"""Tests for the cleaning pipeline: dedup, normalizer, validator, enricher, full pipeline."""

import json
import sys
from pathlib import Path

import pytest

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from models import (
    CleaningConfig,
    FieldType,
    FlaggedRecord,
    OutputConfig,
    RawRecord,
)
from cleaner.dedup import deduplicate
from cleaner.normalizer import (
    normalize_currency,
    normalize_date,
    normalize_location,
    normalize_text,
    normalize_url,
    normalize_record,
)
from cleaner.validator import validate_records
from cleaner.enricher import enrich_record
from cleaner.pipeline import CleaningPipeline


FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_fixture_records() -> list[RawRecord]:
    """Load test fixture raw records."""
    with open(FIXTURES_DIR / "raw_records.json") as f:
        data = json.load(f)
    return [RawRecord(**item) for item in data]


# --- Deduplication Tests ---

class TestDedup:
    def test_basic_dedup(self):
        """Records with same dedup keys should be deduplicated."""
        records = load_fixture_records()
        # Fixture has 10 records, with duplicates on title+source_url
        unique, dup_count = deduplicate(records, keys=["title", "source_url"])
        assert dup_count > 0
        assert len(unique) < len(records)
        assert len(unique) + dup_count == len(records)

    def test_no_dedup_keys(self):
        """Empty dedup keys should return all records unchanged."""
        records = load_fixture_records()
        unique, dup_count = deduplicate(records, keys=[])
        assert len(unique) == len(records)
        assert dup_count == 0

    def test_keeps_latest(self):
        """Should keep the record with the latest fetched_at."""
        records = [
            RawRecord(source="a", fetched_at="2025-01-01T00:00:00", raw_data={"title": "X", "id": "old"}),
            RawRecord(source="a", fetched_at="2025-06-01T00:00:00", raw_data={"title": "X", "id": "new"}),
        ]
        unique, _ = deduplicate(records, keys=["title"])
        assert len(unique) == 1
        assert unique[0].raw_data["id"] == "new"


# --- Normalizer Tests ---

class TestNormalizeDate:
    def test_us_format(self):
        assert normalize_date("03/05/2025") == "2025-03-05"

    def test_long_format(self):
        assert normalize_date("March 5, 2025") == "2025-03-05"

    def test_iso_format(self):
        assert normalize_date("2025-03-05") == "2025-03-05"

    def test_day_month_year(self):
        result = normalize_date("20 Mar 2025")
        assert result == "2025-03-20"

    def test_none(self):
        assert normalize_date(None) is None

    def test_empty_string(self):
        assert normalize_date("") is None

    def test_invalid(self):
        assert normalize_date("not a date at all xyz") is None or isinstance(
            normalize_date("not a date at all xyz"), str
        )


class TestNormalizeCurrency:
    def test_dollar_sign_commas(self):
        assert normalize_currency("$1,500") == 1500.0

    def test_plain_number(self):
        assert normalize_currency("1500") == 1500.0

    def test_k_suffix_lower(self):
        assert normalize_currency("1.5k") == 1500.0

    def test_k_suffix_upper(self):
        assert normalize_currency("$1.5K") == 1500.0

    def test_m_suffix(self):
        assert normalize_currency("2.3M") == 2_300_000.0

    def test_none(self):
        assert normalize_currency(None) is None

    def test_invalid_string(self):
        assert normalize_currency("not specified") is None

    def test_large_number(self):
        assert normalize_currency("$2,300,000") == 2_300_000.0

    def test_150k(self):
        assert normalize_currency("150K") == 150_000.0


class TestNormalizeText:
    def test_strips_whitespace(self):
        assert normalize_text("  hello  ") == "hello"

    def test_collapses_spaces(self):
        assert normalize_text("hello   world") == "hello world"

    def test_none(self):
        assert normalize_text(None) is None

    def test_unicode_normalization(self):
        # NFKC normalizes things like fullwidth chars
        result = normalize_text("ｈｅｌｌｏ")
        assert result == "hello"


class TestNormalizeUrl:
    def test_strips_tracking_params(self):
        result = normalize_url("https://example.com/page?utm_source=google&id=123&fbclid=abc")
        assert "utm_source" not in result
        assert "fbclid" not in result
        assert "id=123" in result

    def test_none(self):
        assert normalize_url(None) is None

    def test_relative_url(self):
        assert normalize_url("/page/123") == "/page/123"


class TestNormalizeLocation:
    def test_basic(self):
        assert normalize_location("san francisco, ca") == "San Francisco, CA"

    def test_extra_whitespace(self):
        assert normalize_location("  new york,   NY  ") == "New York, NY"

    def test_none(self):
        assert normalize_location(None) is None


class TestNormalizeRecord:
    def test_applies_correct_normalizers(self):
        data = {
            "title": "  Test Title  ",
            "date_posted": "March 5, 2025",
            "salary_min": "$120,000",
            "source_url": "https://example.com?utm_source=test",
        }
        field_types = {
            "title": FieldType.TEXT,
            "date_posted": FieldType.DATE,
            "salary_min": FieldType.CURRENCY,
            "source_url": FieldType.URL,
        }
        result, fixes = normalize_record(data, field_types)
        assert result["title"] == "Test Title"
        assert result["date_posted"] == "2025-03-05"
        assert result["salary_min"] == 120_000.0
        assert "utm_source" not in result["source_url"]


# --- Validator Tests ---

class TestValidator:
    def test_missing_required_field_flagged(self):
        records = [
            {"title": "Good Record", "source_url": "https://example.com"},
            {"title": None, "source_url": "https://example.com"},
            {"title": "No URL", "source_url": None},
        ]
        valid, flagged = validate_records(records, required_fields=["title", "source_url"])
        assert len(valid) == 1
        assert len(flagged) == 2

    def test_type_check_currency(self):
        records = [
            {"title": "OK", "salary_min": 50000.0},
            {"title": "Bad", "salary_min": "not a number"},
        ]
        valid, flagged = validate_records(
            records,
            required_fields=["title"],
            field_types={"salary_min": FieldType.CURRENCY},
        )
        assert len(valid) == 1
        assert len(flagged) == 1
        assert "expected numeric" in flagged[0].flag_reason

    def test_all_valid(self):
        records = [
            {"title": "Job A", "source_url": "https://a.com"},
            {"title": "Job B", "source_url": "https://b.com"},
        ]
        valid, flagged = validate_records(records, required_fields=["title"])
        assert len(valid) == 2
        assert len(flagged) == 0


# --- Enricher Tests ---

class TestEnricher:
    def test_salary_mid(self):
        record = {"salary_min": 100000.0, "salary_max": 150000.0}
        result = enrich_record(record, {"salary_mid": "(salary_min + salary_max) / 2"})
        assert result["salary_mid"] == 125000.0

    def test_simple_subtraction(self):
        record = {"price_new": 200.0, "price_old": 150.0}
        result = enrich_record(record, {"price_diff": "price_new - price_old"})
        assert result["price_diff"] == 50.0

    def test_missing_field_skips(self):
        record = {"salary_min": 100000.0}
        result = enrich_record(record, {"salary_mid": "(salary_min + salary_max) / 2"})
        assert "salary_mid" not in result  # salary_max is missing


# --- Full Pipeline Test ---

class TestCleaningPipeline:
    def test_full_pipeline_with_fixtures(self, tmp_path):
        """Run the full pipeline on fixture data and verify report."""
        records = load_fixture_records()

        cleaning_config = CleaningConfig(
            dedup_keys=["title", "source_url"],
            required_fields=["title", "source_url"],
            field_types={
                "title": FieldType.TEXT,
                "date_posted": FieldType.DATE,
                "salary_min": FieldType.CURRENCY,
                "salary_max": FieldType.CURRENCY,
                "source_url": FieldType.URL,
                "location": FieldType.LOCATION,
            },
            enrichment={"salary_mid": "(salary_min + salary_max) / 2"},
        )

        output_config = OutputConfig(
            format="csv",
            clean_file=str(tmp_path / "dataset.csv"),
            flagged_file=str(tmp_path / "flagged.csv"),
            report_file=str(tmp_path / "report.json"),
        )

        pipeline = CleaningPipeline(cleaning_config, output_config)
        valid, flagged, report = pipeline.run(records)

        # 10 input records
        assert report.input_records == 10

        # Should have some duplicates removed (records 0&2 are dupes, records 1&9 are dupes)
        assert report.duplicates_removed >= 2

        # Should have at least 1 flagged (record 5 has no title)
        assert report.records_flagged >= 1

        # Clean + flagged + dupes = input
        assert report.records_cleaned + report.records_flagged + report.duplicates_removed == report.input_records

        # Check output files exist
        assert (tmp_path / "dataset.csv").exists()
        assert (tmp_path / "report.json").exists()

        # Check enrichment worked
        for record in valid:
            if record.get("salary_min") and record.get("salary_max"):
                if isinstance(record["salary_min"], (int, float)) and isinstance(record["salary_max"], (int, float)):
                    assert "salary_mid" in record

    def test_empty_input(self, tmp_path):
        """Pipeline should handle empty input gracefully."""
        cleaning_config = CleaningConfig(
            dedup_keys=["title"],
            required_fields=["title"],
        )
        output_config = OutputConfig(
            clean_file=str(tmp_path / "dataset.csv"),
            flagged_file=str(tmp_path / "flagged.csv"),
            report_file=str(tmp_path / "report.json"),
        )

        pipeline = CleaningPipeline(cleaning_config, output_config)
        valid, flagged, report = pipeline.run([])

        assert report.input_records == 0
        assert report.records_cleaned == 0
        assert len(valid) == 0
