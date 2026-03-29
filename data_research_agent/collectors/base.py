"""Abstract base collector with shared request logic."""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from logging import Logger

import requests

from models import CrawlState, RawRecord, SearchParams, SourceConfig
from utils import RateLimiter, retry_request


class BaseCollector(ABC):
    """Base class for all data collectors."""

    def __init__(self, config: SourceConfig, rate_limiter: RateLimiter, logger: Logger):
        self.config = config
        self.rate_limiter = rate_limiter
        self.logger = logger
        self._session = requests.Session()
        self._setup_auth()

    def _setup_auth(self):
        """Configure authentication on the session based on config."""
        auth = self.config.auth
        if auth.type == "bearer" and auth.token_env:
            token = os.environ.get(auth.token_env, "")
            if token:
                self._session.headers["Authorization"] = f"Bearer {token}"
            else:
                self.logger.warning("Bearer token env var %s not set", auth.token_env)
        elif auth.type == "api_key" and auth.key_env:
            # API key is added per-request as a query param in _make_request
            pass

    @retry_request(max_retries=3, base_delay=1.0)
    def _make_request(
        self,
        url: str,
        params: dict | None = None,
        headers: dict | None = None,
        method: str = "GET",
        data: dict | str | None = None,
        timeout: int = 30,
    ) -> requests.Response:
        """Make an HTTP request with rate limiting, retry, and logging."""
        self.rate_limiter.wait()

        params = params or {}
        # Inject API key as query param if configured
        auth = self.config.auth
        if auth.type == "api_key" and auth.key_env and auth.key_param:
            api_key = os.environ.get(auth.key_env, "")
            if api_key:
                params[auth.key_param] = api_key

        self.logger.debug("Request: %s %s params=%s", method, url, params)

        response = self._session.request(
            method, url, params=params, headers=headers, data=data, timeout=timeout,
        )

        self.logger.info(
            "[%s] %s %s -> %d (%d bytes)",
            self.config.name, method, url, response.status_code, len(response.content),
        )

        response.raise_for_status()
        return response

    @abstractmethod
    def fetch_batch(self, params: SearchParams, limit: int | None = None) -> list[RawRecord]:
        """Fetch all records matching params. Handle pagination internally.

        Args:
            params: Search parameters (keywords, filters, etc.)
            limit: Max records to fetch. None = fetch all available.

        Returns:
            List of raw records.
        """

    @abstractmethod
    def fetch_incremental(
        self, state: CrawlState, max_records: int = 500
    ) -> tuple[list[RawRecord], CrawlState]:
        """Fetch only new records since the last crawl state.

        Args:
            state: Previous crawl state for this source.
            max_records: Max records to fetch in this run.

        Returns:
            Tuple of (new records, updated crawl state).
        """

    def apply_field_mapping(self, raw_data: dict) -> dict:
        """Map source-specific field names to common schema fields.

        Uses the field_mapping config: {"source_field": "common_field"}.
        Dot-notation supported for nested source fields (e.g., "AddressInfo.Latitude").
        """
        mapping = self.config.field_mapping
        if not mapping:
            return raw_data

        mapped = {}
        for source_field, common_field in mapping.items():
            value = self._get_dot_value(raw_data, source_field)
            if value is not None:
                mapped[common_field] = value

        return mapped

    def _get_dot_value(self, data: dict, dotted_key: str):
        """Get a value from a nested dict using dot notation."""
        keys = dotted_key.split(".")
        current = data
        for key in keys:
            if isinstance(current, dict):
                current = current.get(key)
            elif isinstance(current, list) and key.isdigit():
                idx = int(key)
                current = current[idx] if idx < len(current) else None
            else:
                return None
            if current is None:
                return None
        return current

    def dry_run(self, params: SearchParams) -> dict:
        """Return info about what would be fetched without making requests."""
        return {
            "source": self.config.name,
            "type": self.config.type.value,
            "base_url": self.config.base_url,
            "keywords": params.keywords,
            "location": params.location,
            "date_range": params.date_range.model_dump() if params.date_range else None,
        }
