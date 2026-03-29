#!/usr/bin/env python3
"""Data Research Agent — CLI entry point.

Usage:
    python main.py collect --mode batch [--query "..."] [--limit N] [--dry-run]
    python main.py collect --mode crawl [--once] [--schedule "cron_expr"]
    python main.py clean [--input path/to/raw/]
    python main.py pipeline [--mode batch|crawl]
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from models import AppConfig, CrawlState, RawRecord, SearchParams
from utils import RateLimiter, ensure_dirs, load_json, save_json, setup_logger
from collectors import create_collector
from cleaner import CleaningPipeline
from scheduler import CrawlScheduler, CrawlStateManager
from geo_export import GeoExporter, MapProvider, MapProviderConfig


def load_config(config_path: str = "config.yaml") -> AppConfig:
    """Load the application config from YAML file."""
    if not Path(config_path).exists():
        print(f"Error: Config file not found: {config_path}")
        print("Create a config.yaml file or specify --config path")
        sys.exit(1)
    return AppConfig.from_yaml(config_path)


def cmd_collect(args, config: AppConfig, logger):
    """Handle the 'collect' command."""
    if args.mode == "batch":
        _collect_batch(args, config, logger)
    elif args.mode == "crawl":
        _collect_crawl(args, config, logger)
    else:
        print(f"Unknown mode: {args.mode}")
        sys.exit(1)


def _collect_batch(args, config: AppConfig, logger):
    """Run batch collection across all configured sources."""
    search_params = config.search_params

    # Override keywords from CLI if provided
    if args.query:
        search_params = SearchParams(
            keywords=args.query.split(),
            location=search_params.location,
            date_range=search_params.date_range,
            filters=search_params.filters,
        )

    limit = args.limit
    total_records = 0
    start_time = time.monotonic()
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    batch_dir = f"data/raw/batch_{timestamp}"
    ensure_dirs(batch_dir)

    for source_config in config.sources:
        rate_limiter = RateLimiter(
            requests_per_second=source_config.rate_limit.requests_per_second,
            requests_per_minute=source_config.rate_limit.requests_per_minute,
        )
        collector = create_collector(source_config, rate_limiter, logger)

        if args.dry_run:
            info = collector.dry_run(search_params)
            print(f"\n[DRY RUN] Source: {info['source']}")
            print(f"  Type: {info['type']}")
            print(f"  URL: {info['base_url']}")
            print(f"  Keywords: {info['keywords']}")
            print(f"  Location: {info['location']}")
            continue

        logger.info("Collecting from source: %s", source_config.name)
        try:
            records = collector.fetch_batch(search_params, limit=limit)
            total_records += len(records)

            # Save raw data
            output_file = f"{batch_dir}/{source_config.name}.json"
            raw_data = [r.model_dump() for r in records]
            save_json(raw_data, output_file)
            logger.info("Saved %d records from %s to %s", len(records), source_config.name, output_file)

        except Exception as e:
            logger.error("Failed to collect from %s: %s", source_config.name, e)

    elapsed = time.monotonic() - start_time

    if args.dry_run:
        print("\n[DRY RUN] No requests were made.")
    else:
        print(f"\nBatch collection complete:")
        print(f"  Sources: {len(config.sources)}")
        print(f"  Records: {total_records}")
        print(f"  Output: {batch_dir}/")
        print(f"  Time: {elapsed:.1f}s")


def _collect_crawl(args, config: AppConfig, logger):
    """Run incremental crawl collection."""
    state_manager = CrawlStateManager(logger=logger)
    state_manager.register_signal_handler()

    def crawl_cycle():
        total = 0
        for source_config in config.sources:
            rate_limiter = RateLimiter(
                requests_per_second=source_config.rate_limit.requests_per_second,
                requests_per_minute=source_config.rate_limit.requests_per_minute,
            )
            collector = create_collector(source_config, rate_limiter, logger)
            state = state_manager.get(source_config.name)

            try:
                records, new_state = collector.fetch_incremental(
                    state, max_records=config.crawl.max_records_per_run
                )
                total += len(records)

                if records:
                    # Save raw data
                    date_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                    output_file = f"data/raw/crawl/{source_config.name}_{date_str}.json"
                    ensure_dirs("data/raw/crawl")
                    raw_data = [r.model_dump() for r in records]
                    save_json(raw_data, output_file)
                    logger.info("Saved %d new records from %s", len(records), source_config.name)

                state_manager.update(new_state)

            except Exception as e:
                logger.error("Crawl failed for %s: %s", source_config.name, e)

        logger.info("Crawl cycle done: %d new records total", total)

    if args.once:
        crawl_cycle()
    else:
        schedule = args.schedule or config.crawl.schedule
        scheduler = CrawlScheduler(crawl_cycle, logger=logger)
        scheduler.run_with_apscheduler(schedule)


def cmd_clean(args, config: AppConfig, logger):
    """Handle the 'clean' command."""
    # Find raw data files
    if args.input:
        input_path = args.input
    else:
        # Find all uncleaned raw files
        input_path = "data/raw"

    raw_files = _find_raw_files(input_path)
    if not raw_files:
        print(f"No raw data files found in {input_path}")
        return

    # Load all raw records
    all_records = []
    for filepath in raw_files:
        data = load_json(filepath)
        if data and isinstance(data, list):
            for item in data:
                try:
                    all_records.append(RawRecord(**item))
                except Exception:
                    # Try treating each item as raw_data directly
                    all_records.append(RawRecord(
                        source="unknown",
                        raw_data=item if isinstance(item, dict) else {"value": item},
                    ))

    if not all_records:
        print("No records found in raw files")
        return

    print(f"Loaded {len(all_records)} raw records from {len(raw_files)} files")

    # Run cleaning pipeline
    pipeline = CleaningPipeline(
        cleaning_config=config.cleaning,
        output_config=config.output,
        logger=logger,
    )

    append = args.input and "crawl" in args.input
    valid, flagged, report = pipeline.run(all_records, append=append)

    print(f"\nCleaning complete:")
    print(f"  Input: {report.input_records}")
    print(f"  Clean: {report.records_cleaned}")
    print(f"  Flagged: {report.records_flagged}")
    print(f"  Duplicates removed: {report.duplicates_removed}")
    if report.fields_fixed:
        print(f"  Fields fixed: {report.fields_fixed}")


def cmd_pipeline(args, config: AppConfig, logger):
    """Handle the 'pipeline' command — collect then clean."""
    mode = args.mode or "batch"

    print(f"Running full pipeline (mode={mode})...")
    print("=" * 50)

    # Step 1: Collect
    print("\n[1/2] Collecting data...")
    collect_args = argparse.Namespace(
        mode=mode, query=None, limit=None, dry_run=False,
        once=True, schedule=None,
    )
    cmd_collect(collect_args, config, logger)

    # Step 2: Clean
    print("\n[2/2] Cleaning data...")
    clean_args = argparse.Namespace(input=None)
    cmd_clean(clean_args, config, logger)

    print("\n" + "=" * 50)
    print("Pipeline complete!")


def cmd_geo(args, config: AppConfig, logger):
    """Handle the 'geo' command — export cleaned data to GeoJSON + map config."""
    input_file = args.input or config.output.clean_file
    if not Path(input_file).exists():
        print(f"Error: Clean data file not found: {input_file}")
        print("Run 'pipeline' or 'clean' first to generate cleaned data.")
        sys.exit(1)

    exporter = GeoExporter()

    # Load based on file extension
    if input_file.endswith(".json"):
        collection = exporter.from_json(input_file)
    else:
        collection = exporter.from_csv(input_file)

    if len(collection) == 0:
        print("No valid station records found (need latitude + longitude).")
        return

    # Save GeoJSON
    output_dir = args.output_dir or "data/geo"
    geojson_path = f"{output_dir}/ev_stations.geojson"
    exporter.save(collection, geojson_path)
    print(f"GeoJSON: {geojson_path} ({len(collection)} stations)")

    # Generate map provider configs
    providers = args.providers.split(",") if args.providers else ["unearth", "mapbox", "leaflet"]
    for provider_name in providers:
        provider_name = provider_name.strip()
        try:
            provider = MapProviderConfig(
                provider=provider_name,
                api_key=args.api_key or "",
            )
            config_path = f"{output_dir}/{provider_name}_config.json"
            provider.generate_config(geojson_path, config_path)
            print(f"  {provider_name} config: {config_path}")
        except ValueError:
            valid = ", ".join(p.value for p in MapProvider)
            print(f"  Warning: unknown provider '{provider_name}' (valid: {valid})")

    print(f"\nReady for map integration. Load the GeoJSON into your map provider.")


def _find_raw_files(path: str) -> list[str]:
    """Find all JSON files in a directory (recursively)."""
    path_obj = Path(path)
    if path_obj.is_file():
        return [str(path_obj)]
    if path_obj.is_dir():
        return sorted(glob.glob(str(path_obj / "**" / "*.json"), recursive=True))
    return []


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    parser = argparse.ArgumentParser(
        description="Data Research Agent — collect and clean data from configured sources",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # collect
    collect_parser = subparsers.add_parser("collect", help="Collect data from sources")
    collect_parser.add_argument("--mode", required=True, choices=["batch", "crawl"], help="Collection mode")
    collect_parser.add_argument("--query", help="Search query (overrides config keywords)")
    collect_parser.add_argument("--limit", type=int, help="Max records to fetch")
    collect_parser.add_argument("--dry-run", action="store_true", help="Show what would be fetched")
    collect_parser.add_argument("--once", action="store_true", help="Run crawl once (no scheduling)")
    collect_parser.add_argument("--schedule", help="Cron expression for crawl schedule")

    # clean
    clean_parser = subparsers.add_parser("clean", help="Clean raw data")
    clean_parser.add_argument("--input", help="Path to raw data directory or file")

    # pipeline
    pipeline_parser = subparsers.add_parser("pipeline", help="Run full collect + clean pipeline")
    pipeline_parser.add_argument("--mode", choices=["batch", "crawl"], help="Collection mode (default: batch)")

    # geo
    geo_parser = subparsers.add_parser("geo", help="Export cleaned data to GeoJSON + map configs")
    geo_parser.add_argument("--input", help="Path to cleaned data file (CSV or JSON)")
    geo_parser.add_argument("--output-dir", help="Output directory for GeoJSON (default: data/geo)")
    geo_parser.add_argument("--providers", help="Comma-separated map providers (default: unearth,mapbox,leaflet)")
    geo_parser.add_argument("--api-key", help="API key for map provider")

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    # Setup logging
    log_level = "DEBUG" if args.verbose else "INFO"
    logger = setup_logger(
        "agent",
        log_file="data/logs/agent.log",
        level=getattr(__import__("logging"), log_level),
    )

    # Load config
    config = load_config(args.config)
    logger.info("Loaded config for topic: %s", config.topic)

    # Dispatch command
    if args.command == "collect":
        cmd_collect(args, config, logger)
    elif args.command == "clean":
        cmd_clean(args, config, logger)
    elif args.command == "pipeline":
        cmd_pipeline(args, config, logger)
    elif args.command == "geo":
        cmd_geo(args, config, logger)


if __name__ == "__main__":
    main()
