"""Generic CSV/ZIP download collector for static government datasets.

Handles direct CSV downloads and ZIP files containing CSVs.
Used for: NSW Australia, Singapore LTA, and other static file sources.

Each source provides its own field_mapping in config to map CSV columns
to the common schema.
"""

from __future__ import annotations

import csv
import io
import zipfile
from datetime import datetime

from models import CrawlState, RawRecord, SearchParams
from .base import BaseCollector


class CSVDownloadCollector(BaseCollector):
    """Collector that downloads a CSV (or ZIP containing CSV) and parses it."""

    def fetch_batch(self, params: SearchParams, limit: int | None = None) -> list[RawRecord]:
        self.logger.info("[%s] Downloading CSV dataset", self.config.name)

        response = self._make_request(self.config.base_url, timeout=120)
        content_type = response.headers.get("Content-Type", "")

        # Handle ZIP files
        if self.config.base_url.endswith(".zip") or "zip" in content_type:
            text = self._extract_csv_from_zip(response.content)
        else:
            # Try to decode as text
            text = response.text

        if not text:
            self.logger.error("[%s] No CSV content found", self.config.name)
            return []

        # Strip BOM if present
        text = text.lstrip("\ufeff")

        # Parse CSV
        reader = csv.DictReader(io.StringIO(text))
        records: list[RawRecord] = []

        for row in reader:
            # Apply field mapping if configured
            if self.config.field_mapping:
                mapped = {}
                for csv_col, common_field in self.config.field_mapping.items():
                    val = row.get(csv_col)
                    if val is not None and val.strip():
                        mapped[common_field] = val.strip()
                data = mapped
            else:
                data = {k: v.strip() for k, v in row.items() if v and v.strip()}

            # Skip rows without coordinates
            lat = data.get("latitude")
            lng = data.get("longitude")
            if not lat or not lng:
                continue
            try:
                float(lat)
                float(lng)
            except (ValueError, TypeError):
                continue

            records.append(RawRecord(
                source=self.config.name,
                raw_data=data,
                source_url=self.config.base_url,
            ))

            if limit and len(records) >= limit:
                break

        self.logger.info("[%s] Parsed %d records from CSV", self.config.name, len(records))
        return records

    def fetch_incremental(
        self, state: CrawlState, max_records: int = 500
    ) -> tuple[list[RawRecord], CrawlState]:
        records = self.fetch_batch(SearchParams(), limit=max_records)
        new_state = CrawlState(
            source_name=self.config.name,
            last_run_at=datetime.utcnow().isoformat(),
        )
        return records, new_state

    def _extract_csv_from_zip(self, content: bytes) -> str:
        """Extract the first CSV file from a ZIP archive."""
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                csv_files = [f for f in zf.namelist() if f.lower().endswith(".csv")]
                if not csv_files:
                    self.logger.error("[%s] No CSV file found in ZIP", self.config.name)
                    return ""
                # Use the first (or largest) CSV
                csv_name = csv_files[0]
                self.logger.info("[%s] Extracting %s from ZIP", self.config.name, csv_name)
                return zf.read(csv_name).decode("utf-8", errors="replace")
        except Exception as e:
            self.logger.error("[%s] Failed to extract ZIP: %s", self.config.name, e)
            return ""
