#!/usr/bin/env python3
"""Collect EV stations from API-key sources + re-export combined GeoJSON.

Usage:
    # Collect from all sources that have keys set:
    python scripts/collect_with_keys.py

    # Collect from specific sources only:
    python scripts/collect_with_keys.py --sources openchargemap nrel_alt_fuel

    # Skip re-export (just collect raw data):
    python scripts/collect_with_keys.py --no-export

Prerequisites:
    1. Set API keys in .env (see comments in that file for signup URLs)
    2. pip install requests  (if not already installed)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

# Add project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "data_research_agent"))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("collect")

# ── Source definitions ──
# Each entry: (source_name, env_var, estimated_stations, description)
API_KEY_SOURCES = [
    ("openchargemap",     "OPENCHARGEMAP_API_KEY", "~300K", "OpenChargeMap Global"),
    ("nrel_alt_fuel",     "NREL_API_KEY",          "~70K",  "NREL/AFDC US"),
    ("nrel_canada",       "NREL_API_KEY",          "~13K",  "NREL/AFDC Canada"),
    ("nobil",             "NOBIL_API_KEY",          "~25K",  "NOBIL Nordic"),
    ("korea_ev",          "KOREA_DATA_API_KEY",     "~60K",  "Korea MoE"),
    ("ocm_australia",     "OPENCHARGEMAP_API_KEY", "~8K",   "OCM Australia"),
    ("ocm_southeast_asia","OPENCHARGEMAP_API_KEY", "~15K",  "OCM Southeast Asia"),
    ("tomtom_sea",        "TOMTOM_API_KEY",         "varies","TomTom SEA+AU"),
    ("here_sea",          "HERE_API_KEY",            "varies","HERE SEA+AU"),
    ("google_places_sea", "GOOGLE_MAPS_API_KEY",    "varies","Google Places SEA+AU"),
]


def check_available_sources(requested: list[str] | None = None) -> list[str]:
    """Return source names whose API keys are set in the environment."""
    available = []
    for name, env_var, est, desc in API_KEY_SOURCES:
        if requested and name not in requested:
            continue
        key = os.getenv(env_var, "").strip()
        if key:
            logger.info("  ✓ %-25s (%s) — key set", desc, est)
            available.append(name)
        else:
            logger.info("  ✗ %-25s (%s) — %s not set", desc, est, env_var)
    return available


def collect_source(source_name: str, limit: int = 500000) -> list[dict]:
    """Collect from a single source using the existing collector framework."""
    import yaml
    from models import SearchParams, SourceConfig
    from collectors import create_collector

    # Load config
    config_path = PROJECT_ROOT / "data_research_agent" / "config.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Find source config
    source_cfg = None
    for src in config.get("sources", []):
        if src["name"] == source_name:
            source_cfg = src
            break
    if not source_cfg:
        logger.error("Source '%s' not found in config.yaml", source_name)
        return []

    # Build collector
    sc = SourceConfig(**source_cfg)

    # Simple rate limiter
    class SimpleLimiter:
        def __init__(self, rps):
            self.interval = 1.0 / max(rps, 0.1)
            self.last = 0
        def wait(self):
            now = time.monotonic()
            wait = self.interval - (now - self.last)
            if wait > 0:
                time.sleep(wait)
            self.last = time.monotonic()

    rps = source_cfg.get("rate_limit", {}).get("requests_per_second", 1)
    limiter = SimpleLimiter(rps)
    src_logger = logging.getLogger(source_name)

    collector = create_collector(sc, limiter, src_logger)

    # Build search params
    params = SearchParams()
    filters = config.get("search_params", {}).get("filters", {})
    # Source-specific filter adjustments
    if source_name == "nrel_canada":
        filters = dict(filters)
        filters["country"] = "CA"
    params.filters = filters

    logger.info("Collecting from %s (limit=%d)...", source_name, limit)
    start = time.monotonic()

    try:
        records = collector.fetch_batch(params, limit=limit)
        elapsed = time.monotonic() - start
        logger.info("  %s: %d records in %.1fs", source_name, len(records), elapsed)

        # Convert to serializable dicts
        return [
            {
                "source": r.source,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "raw_data": r.raw_data,
                "source_url": r.source_url,
            }
            for r in records
        ]
    except Exception as e:
        logger.error("  %s FAILED: %s", source_name, e)
        import traceback
        traceback.print_exc()
        return []


# ── Normalization for export ──

SC_NUMERIC_TO_ISO = {
    100: "US", 101: "CA", 102: "AT", 103: "DE", 104: "NL", 105: "NO",
    106: "CH", 107: "DK", 108: "GB", 109: "SE", 110: "FR", 111: "SI",
    112: "BE", 113: "IT", 114: "JP", 115: "CN", 116: "AU", 117: "ES",
    118: "LU", 119: "PL", 120: "FI", 121: "HR", 122: "SK", 123: "CZ",
    124: "IE", 125: "PT", 126: "HU", 127: "BG", 128: "TR", 129: "JO",
    131: "MX", 133: "TW", 134: "AE", 136: "EE", 137: "KR", 138: "NZ",
    139: "IN", 140: "LV", 141: "LT", 143: "LI", 144: "KZ", 145: "RO",
    146: "GR", 147: "BA", 148: "RS", 153: "SA", 155: "IS", 156: "SG",
    157: "IL", 158: "MA", 159: "TH", 160: "MY", 161: "QA", 162: "CL",
    163: "OM", 164: "PH", 165: "CO",
}

COUNTRY_TO_ISO = {
    "USA": "US", "United States": "US", "Canada": "CA", "Germany": "DE",
    "France": "FR", "Switzerland": "CH", "United Kingdom": "GB", "Norway": "NO",
    "Sweden": "SE", "Italy": "IT", "Spain": "ES", "Netherlands": "NL",
    "Belgium": "BE", "Austria": "AT", "Denmark": "DK", "Finland": "FI",
    "Poland": "PL", "Czech Republic": "CZ", "Portugal": "PT", "Ireland": "IE",
    "Japan": "JP", "China": "CN", "South Korea": "KR", "Australia": "AU",
    "India": "IN", "Singapore": "SG", "New Zealand": "NZ", "Taiwan": "TW",
    "Turkey": "TR", "Brazil": "BR", "Mexico": "MX", "Thailand": "TH",
    "Malaysia": "MY", "Indonesia": "ID", "Israel": "IL", "South Africa": "ZA",
    "United Arab Emirates": "AE", "Saudi Arabia": "SA", "Greece": "GR",
    "Hungary": "HU", "Romania": "RO", "Croatia": "HR", "Bulgaria": "BG",
    "Slovakia": "SK", "Slovenia": "SI", "Luxembourg": "LU", "Estonia": "EE",
    "Latvia": "LV", "Lithuania": "LT", "Iceland": "IS", "Colombia": "CO",
    "Chile": "CL", "Philippines": "PH", "Qatar": "QA", "Oman": "OM",
    "Jordan": "JO", "Morocco": "MA", "Kazakhstan": "KZ", "Serbia": "RS",
    "Bosnia and Herzegovina": "BA", "Liechtenstein": "LI",
}

DC_PAT = re.compile(r"CCS|CHAdeMO|DC|Combo", re.IGNORECASE)
TESLA_PAT = re.compile(r"Tesla|NACS|Supercharger", re.IGNORECASE)
L2_PAT = re.compile(r"Type.?2|J1772|Type.?1|Mennekes|AC|Typ.?2|IEC", re.IGNORECASE)
L1_PAT = re.compile(r"Schuko|wall|domestic|Level.?1|CEE", re.IGNORECASE)


def normalize_cc(code, country=""):
    if isinstance(code, int) or (isinstance(code, str) and code.isdigit()):
        return SC_NUMERIC_TO_ISO.get(int(code), "")
    if isinstance(code, str):
        c = code.strip().upper()
        if c == "CHE":
            return "CH"
        if len(c) == 2 and c.isalpha():
            return c
    return COUNTRY_TO_ISO.get(country, "") if country else ""


def categorize(ct, net="", pw=None):
    if not ct:
        if pw and pw >= 50:
            return "DC Fast"
        if net and "tesla" in net.lower():
            return "Tesla Supercharger"
        if pw and pw >= 7:
            return "Level 2"
        return "Unknown"
    s = str(ct)
    if TESLA_PAT.search(s):
        return "Tesla Supercharger"
    if DC_PAT.search(s):
        return "DC Fast"
    if L1_PAT.search(s):
        return "Level 1"
    if L2_PAT.search(s):
        return "Level 2"
    if pw:
        if pw >= 50:
            return "DC Fast"
        if pw >= 7:
            return "Level 2"
        return "Level 1"
    return "Unknown"


def export_geojson(batch_dir: Path, output_path: Path) -> int:
    """Load all raw JSON from batch_dir, normalize, dedup, export GeoJSON."""
    all_records = []
    for fname in sorted(os.listdir(batch_dir)):
        if not fname.endswith(".json"):
            continue
        with open(batch_dir / fname) as f:
            data = json.load(f)
        if isinstance(data, list):
            logger.info("  %s: %d records", fname, len(data))
            all_records.extend(data)

    logger.info("Total raw: %d", len(all_records))

    features = []
    seen: set[tuple] = set()

    for rec in all_records:
        rd = rec.get("raw_data", rec)
        source = rec.get("source", "unknown")

        lat, lon = rd.get("latitude"), rd.get("longitude")
        if not lat or not lon:
            continue
        try:
            lat, lon = float(lat), float(lon)
        except (ValueError, TypeError):
            continue
        if lat == 0 and lon == 0:
            continue
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            continue

        cc = normalize_cc(rd.get("country_code", ""), rd.get("country", ""))

        pw = None
        try:
            pw = float(rd["power_kw"]) if rd.get("power_kw") else None
        except (ValueError, TypeError):
            pass

        cat = categorize(rd.get("connector_types", ""), rd.get("network", ""), pw)

        np_val = None
        if rd.get("num_ports"):
            try:
                np_val = int(float(str(rd["num_ports"])))
            except (ValueError, TypeError):
                pass

        name = (rd.get("station_name") or "").strip()
        dk = (round(lat, 4), round(lon, 4), name.lower()[:30])
        if dk in seen:
            continue
        seen.add(dk)

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [round(lon, 6), round(lat, 6)]},
            "properties": {
                "station_id": rd.get("station_id", ""),
                "station_name": name,
                "address": rd.get("address", ""),
                "city": rd.get("city", ""),
                "state": rd.get("state", ""),
                "country": rd.get("country", ""),
                "country_code": cc,
                "postal_code": rd.get("postal_code", ""),
                "network": rd.get("network", ""),
                "operator": rd.get("operator", ""),
                "connector_types": rd.get("connector_types", ""),
                "connector_category": cat,
                "num_ports": np_val,
                "total_ports": np_val,
                "power_kw": pw,
                "status": rd.get("status", ""),
                "access_type": rd.get("access_type", ""),
                "usage_cost": rd.get("usage_cost", ""),
                "source": source,
                "data_provider": rd.get("data_provider", source),
            },
        })

    output_path.parent.mkdir(parents=True, exist_ok=True)
    geojson = {"type": "FeatureCollection", "features": features}
    with open(output_path, "w") as f:
        json.dump(geojson, f)

    size_mb = output_path.stat().st_size / (1024 * 1024)

    sources = Counter(ft["properties"]["source"] for ft in features)
    countries = Counter(ft["properties"]["country_code"] for ft in features)

    logger.info("=" * 50)
    logger.info("EXPORTED: %d stations (%.1f MB)", len(features), size_mb)
    logger.info("=" * 50)
    logger.info("By source:")
    for s, n in sources.most_common():
        logger.info("  %-25s %d", s, n)
    logger.info("Countries: %d", len(countries))
    for c, n in countries.most_common(15):
        logger.info("  %-5s %d", c or "?", n)

    return len(features)


def main():
    parser = argparse.ArgumentParser(description="Collect EV stations from API-key sources")
    parser.add_argument("--sources", nargs="+", help="Specific sources to collect")
    parser.add_argument("--no-export", action="store_true", help="Skip GeoJSON re-export")
    parser.add_argument("--limit", type=int, default=500000, help="Max records per source")
    args = parser.parse_args()

    logger.info("Checking API keys in .env...")
    available = check_available_sources(args.sources)

    if not available:
        logger.warning(
            "\nNo API keys found! Set keys in .env file:\n"
            "  %s/.env\n\n"
            "Signup links (free, ~2 min each):\n"
            "  OpenChargeMap: https://openchargemap.org/site/develop/api\n"
            "  NREL/AFDC:     https://developer.nrel.gov/signup/\n"
            "  NOBIL:         https://info.nobil.no/english\n"
            "  Korea:         https://www.data.go.kr/data/15076352/openapi.do\n",
            PROJECT_ROOT,
        )
        sys.exit(1)

    batch_dir = PROJECT_ROOT / "data_research_agent" / "data" / "raw" / "batch_full_20260329"
    batch_dir.mkdir(parents=True, exist_ok=True)

    # Collect from each available source
    total_new = 0
    for source_name in available:
        records = collect_source(source_name, limit=args.limit)
        if records:
            out_path = batch_dir / f"{source_name}.json"
            with open(out_path, "w") as f:
                json.dump(records, f)
            logger.info("  Saved %d records to %s", len(records), out_path.name)
            total_new += len(records)

    logger.info("\nNew records collected: %d", total_new)

    # Re-export combined GeoJSON
    if not args.no_export and total_new > 0:
        logger.info("\nRe-exporting combined GeoJSON...")
        output_path = PROJECT_ROOT / "frontend" / "data" / "ev_stations.geojson"
        count = export_geojson(batch_dir, output_path)

        # Hot-reload API server if running
        logger.info("\nAttempting API server hot-reload...")
        try:
            import urllib.request
            admin_token = os.getenv("ADMIN_TOKEN", "")
            if admin_token:
                req = urllib.request.Request(
                    "http://localhost:8000/api/admin/reload",
                    method="POST",
                    headers={"Authorization": f"Bearer {admin_token}"},
                )
                with urllib.request.urlopen(req, timeout=10) as resp:
                    logger.info("  Server reloaded: %s", resp.read().decode())
            else:
                logger.info("  No ADMIN_TOKEN set — restart server manually to load new data")
        except Exception as e:
            logger.info("  Server reload skipped (%s) — restart server to load new data", e)


if __name__ == "__main__":
    main()
