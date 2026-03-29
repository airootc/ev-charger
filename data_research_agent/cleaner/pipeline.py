"""Cleaning pipeline: normalize -> dedup -> validate -> enrich."""

from __future__ import annotations

import csv
import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from models import (
    CleaningConfig,
    CleaningReport,
    CleanRecord,
    FieldType,
    FlaggedRecord,
    OutputConfig,
    RawRecord,
)
from utils import ensure_dirs, save_json

from .dedup import deduplicate
from .enricher import enrich_record
from .normalizer import normalize_record
from .validator import validate_records


class CleaningPipeline:
    """Orchestrates the full cleaning pipeline: normalize -> dedup -> validate -> enrich."""

    def __init__(
        self,
        cleaning_config: CleaningConfig,
        output_config: OutputConfig,
        logger: logging.Logger | None = None,
    ):
        self.config = cleaning_config
        self.output = output_config
        self.logger = logger or logging.getLogger("cleaner")

    def run(
        self, records: list[RawRecord], append: bool = False
    ) -> tuple[list[dict], list[FlaggedRecord], CleaningReport]:
        """Run the full cleaning pipeline.

        Args:
            records: Raw records to clean.
            append: If True, append to existing clean dataset instead of overwriting.

        Returns:
            Tuple of (clean_data_dicts, flagged_records, report).
        """
        report = CleaningReport(input_records=len(records))
        self.logger.info("Starting cleaning pipeline with %d records", len(records))

        # Step 1: Normalize
        self.logger.info("Step 1: Normalizing records...")
        normalized_records, total_fields_fixed = self._normalize(records)
        report.fields_fixed = total_fields_fixed

        # Step 2: Deduplicate
        self.logger.info("Step 2: Deduplicating records...")
        unique_records, dup_count = deduplicate(
            normalized_records, self.config.dedup_keys
        )
        report.duplicates_removed = dup_count
        self.logger.info("Removed %d duplicates, %d unique remain", dup_count, len(unique_records))

        # Step 3: Validate
        self.logger.info("Step 3: Validating records...")
        data_dicts = [r.raw_data for r in unique_records]
        valid, flagged = validate_records(
            data_dicts,
            self.config.required_fields,
            self.config.field_types or None,
        )
        report.records_flagged = len(flagged)
        self.logger.info("Valid: %d, Flagged: %d", len(valid), len(flagged))

        # Step 4: Enrich
        if self.config.enrichment:
            self.logger.info("Step 4: Enriching records...")
            for record_data in valid:
                enrich_record(record_data, self.config.enrichment)

        report.records_cleaned = len(valid)
        report.timestamp = datetime.utcnow().isoformat()

        # Save outputs
        self._save_outputs(valid, flagged, report, append)

        self.logger.info(
            "Pipeline complete: %d input -> %d clean, %d flagged, %d dupes removed",
            report.input_records, report.records_cleaned,
            report.records_flagged, report.duplicates_removed,
        )

        return valid, flagged, report

    def _normalize(
        self, records: list[RawRecord]
    ) -> tuple[list[RawRecord], dict[str, int]]:
        """Normalize all records and track field fix counts."""
        total_fixes: dict[str, int] = {}

        for record in records:
            normalized_data, fixes = normalize_record(
                record.raw_data, self.config.field_types
            )
            record.raw_data = normalized_data

            for field, count in fixes.items():
                total_fixes[field] = total_fixes.get(field, 0) + count

        return records, total_fixes

    def _save_outputs(
        self,
        valid: list[dict],
        flagged: list[FlaggedRecord],
        report: CleaningReport,
        append: bool,
    ) -> None:
        """Save clean data, flagged records, and report to disk."""
        # Ensure output directories exist
        for path in (self.output.clean_file, self.output.flagged_file, self.output.report_file):
            ensure_dirs(str(Path(path).parent))

        # Save clean data
        if valid:
            df = pd.DataFrame(valid)
            if append and Path(self.output.clean_file).exists():
                existing = pd.read_csv(self.output.clean_file)
                df = pd.concat([existing, df], ignore_index=True)
                self.logger.info("Appended %d records to existing dataset", len(valid))
            df.to_csv(self.output.clean_file, index=False)
            self.logger.info("Saved %d clean records to %s", len(df), self.output.clean_file)

        # Save flagged records
        if flagged:
            flagged_dicts = [
                {**f.raw_data, "_source": f.source, "_flag_reason": f.flag_reason}
                for f in flagged
            ]
            df_flagged = pd.DataFrame(flagged_dicts)
            df_flagged.to_csv(self.output.flagged_file, index=False)
            self.logger.info("Saved %d flagged records to %s", len(flagged), self.output.flagged_file)

        # Save report
        save_json(report.to_dict(), self.output.report_file)
        self.logger.info("Saved cleaning report to %s", self.output.report_file)
