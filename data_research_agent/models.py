"""Pydantic data models for the data research agent."""

from __future__ import annotations

import re
from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator


# --- Enums ---

class SourceType(str, Enum):
    API = "api"
    SCRAPER = "scraper"
    RSS = "rss"
    OSM_OVERPASS = "osm_overpass"
    ARCGIS = "arcgis"
    SUPERCHARGER = "supercharger"
    FRANCE_IRVE = "france_irve"
    KOREA_EV = "korea_ev"
    CSV_DOWNLOAD = "csv_download"
    WFS = "wfs"
    TOMTOM_EV = "tomtom_ev"
    HERE_EV = "here_ev"
    GOOGLE_PLACES_EV = "google_places_ev"


class PaginationStyle(str, Enum):
    PAGE_NUMBER = "page_number"
    OFFSET = "offset"
    CURSOR = "cursor"


class AuthType(str, Enum):
    API_KEY = "api_key"
    BEARER = "bearer"
    NONE = "none"


class OutputFormat(str, Enum):
    CSV = "csv"
    PARQUET = "parquet"
    JSON = "json"


class FieldType(str, Enum):
    TEXT = "text"
    DATE = "date"
    CURRENCY = "currency"
    URL = "url"
    LOCATION = "location"
    NUMBER = "number"


# --- Source Config ---

class RateLimitConfig(BaseModel):
    requests_per_second: Optional[float] = None
    requests_per_minute: Optional[float] = None


class AuthConfig(BaseModel):
    type: AuthType = AuthType.NONE
    key_env: Optional[str] = None
    key_param: Optional[str] = None
    token_env: Optional[str] = None


class PaginationConfig(BaseModel):
    style: PaginationStyle = PaginationStyle.PAGE_NUMBER
    page_param: str = "page"
    offset_param: str = "offset"
    cursor_param: str = "cursor"
    per_page: int = 100
    per_page_param: str = "per_page"


class ScraperSelectors(BaseModel):
    list_item: str = ""
    fields: dict[str, str] = Field(default_factory=dict)
    next_page: Optional[str] = None


class SourceConfig(BaseModel):
    """Configuration for a single data source.

    The ``extra_config`` dict carries source-specific settings that do not
    fit into the common schema (e.g. ``regions`` for OSM Overpass).  When
    a YAML source block contains keys not declared on this model they are
    captured here automatically via ``model_config``.
    """

    model_config = {"extra": "allow"}

    name: str
    type: SourceType
    base_url: str
    rate_limit: RateLimitConfig = Field(default_factory=RateLimitConfig)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    pagination: PaginationConfig = Field(default_factory=PaginationConfig)
    response_path: Optional[str] = None  # dot-notation path to records in API response
    selectors: Optional[ScraperSelectors] = None  # for scraper type
    field_mapping: dict[str, str] = Field(default_factory=dict)  # source_field -> common_field
    extra_config: Optional[dict[str, Any]] = Field(default=None)  # e.g. {"regions": {...}}


# --- Search & Crawl ---

class DateRange(BaseModel):
    from_date: Optional[str] = Field(None, alias="from")
    to_date: Optional[str] = Field(None, alias="to")

    model_config = {"populate_by_name": True}


class SearchParams(BaseModel):
    keywords: list[str] = Field(default_factory=list)
    location: Optional[str] = None
    date_range: Optional[DateRange] = None
    filters: dict[str, Any] = Field(default_factory=dict)


class CrawlConfig(BaseModel):
    schedule: str = "0 */6 * * *"
    max_records_per_run: int = 500000


class CrawlState(BaseModel):
    source_name: str
    last_id: Optional[str] = None
    last_timestamp: Optional[str] = None
    cursor: Optional[str] = None
    last_run_at: Optional[str] = None


# --- Records ---

class RawRecord(BaseModel):
    source: str
    fetched_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    raw_data: dict[str, Any]
    source_url: Optional[str] = None


class CleanRecord(BaseModel):
    """A cleaned and validated record. Fields are dynamic based on config,
    but these are the metadata fields always present."""
    source: str
    cleaned_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    original_id: Optional[str] = None
    data: dict[str, Any] = Field(default_factory=dict)

    def to_flat_dict(self) -> dict[str, Any]:
        """Flatten data fields into top-level dict for CSV export."""
        result = {"source": self.source, "cleaned_at": self.cleaned_at, "original_id": self.original_id}
        result.update(self.data)
        return result


class FlaggedRecord(BaseModel):
    source: str
    raw_data: dict[str, Any]
    flag_reason: str


# --- Cleaning Report ---

class CleaningReport(BaseModel):
    input_records: int = 0
    duplicates_removed: int = 0
    records_cleaned: int = 0
    records_flagged: int = 0
    fields_fixed: dict[str, int] = Field(default_factory=dict)
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> dict:
        return self.model_dump()


# --- Cleaning Config ---

class CleaningConfig(BaseModel):
    dedup_keys: list[str] = Field(default_factory=list)
    required_fields: list[str] = Field(default_factory=list)
    field_types: dict[str, FieldType] = Field(default_factory=dict)
    enrichment: dict[str, str] = Field(default_factory=dict)


# --- Output Config ---

class OutputConfig(BaseModel):
    format: OutputFormat = OutputFormat.CSV
    clean_file: str = "data/clean/dataset.csv"
    flagged_file: str = "data/clean/flagged.csv"
    report_file: str = "data/clean/cleaning_report.json"


# --- Top-level App Config ---

class AppConfig(BaseModel):
    topic: str = "default_topic"
    sources: list[SourceConfig] = Field(default_factory=list)
    search_params: SearchParams = Field(default_factory=SearchParams)
    crawl: CrawlConfig = Field(default_factory=CrawlConfig)
    cleaning: CleaningConfig = Field(default_factory=CleaningConfig)
    output: OutputConfig = Field(default_factory=OutputConfig)

    @classmethod
    def from_yaml(cls, path: str) -> "AppConfig":
        import yaml
        with open(path, "r") as f:
            data = yaml.safe_load(f)
        return cls(**data)
