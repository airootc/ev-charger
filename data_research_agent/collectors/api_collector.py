"""Generic REST API collector with configurable pagination."""

from __future__ import annotations

from datetime import datetime

from models import CrawlState, RawRecord, SearchParams
from utils import get_nested_value

from .base import BaseCollector


class APICollector(BaseCollector):
    """Collects data from REST APIs with configurable pagination and auth."""

    def fetch_batch(self, params: SearchParams, limit: int | None = None) -> list[RawRecord]:
        records: list[RawRecord] = []
        page = 1
        offset = 0
        cursor = None
        per_page = self.config.pagination.per_page

        while True:
            query_params = self._build_query_params(params, page, offset, cursor)
            response = self._make_request(self.config.base_url, params=query_params)
            data = response.json()

            items = self._extract_items(data)
            if not items:
                self.logger.info("[%s] No more items at page/offset %s/%s", self.config.name, page, offset)
                break

            for item in items:
                mapped = self.apply_field_mapping(item) if self.config.field_mapping else item
                records.append(RawRecord(
                    source=self.config.name,
                    raw_data=mapped,
                    source_url=self.config.base_url,
                ))
                if limit and len(records) >= limit:
                    self.logger.info("[%s] Reached limit of %d records", self.config.name, limit)
                    return records

            self.logger.info("[%s] Fetched %d records so far", self.config.name, len(records))

            # Advance pagination
            style = self.config.pagination.style
            if style == "page_number":
                page += 1
            elif style == "offset":
                offset += per_page
            elif style == "cursor":
                cursor = self._extract_cursor(data)
                if not cursor:
                    break

            # Safety: if we got fewer items than per_page, we're likely on the last page
            if len(items) < per_page:
                break

        return records

    def fetch_incremental(
        self, state: CrawlState, max_records: int = 500
    ) -> tuple[list[RawRecord], CrawlState]:
        records: list[RawRecord] = []
        page = 1
        offset = 0
        cursor = state.cursor
        per_page = self.config.pagination.per_page
        latest_id = state.last_id
        latest_timestamp = state.last_timestamp

        while len(records) < max_records:
            query_params = self._build_query_params(SearchParams(), page, offset, cursor)

            # Add date filter if we have a last timestamp
            if state.last_timestamp:
                query_params["since"] = state.last_timestamp
                query_params["sort"] = "created_at"
                query_params["order"] = "asc"

            response = self._make_request(self.config.base_url, params=query_params)
            data = response.json()
            items = self._extract_items(data)

            if not items:
                break

            for item in items:
                item_id = str(item.get("id", ""))
                item_ts = item.get("created_at") or item.get("updated_at") or item.get("date")

                # Skip if we've seen this record before
                if item_id and item_id == state.last_id:
                    continue

                mapped = self.apply_field_mapping(item) if self.config.field_mapping else item
                records.append(RawRecord(
                    source=self.config.name,
                    raw_data=mapped,
                    source_url=self.config.base_url,
                ))

                # Track the latest record
                if item_id:
                    latest_id = item_id
                if item_ts:
                    latest_timestamp = str(item_ts)

                if len(records) >= max_records:
                    break

            # Advance pagination
            style = self.config.pagination.style
            if style == "page_number":
                page += 1
            elif style == "offset":
                offset += per_page
            elif style == "cursor":
                cursor = self._extract_cursor(data)
                if not cursor:
                    break

            if len(items) < per_page:
                break

        new_state = CrawlState(
            source_name=self.config.name,
            last_id=latest_id,
            last_timestamp=latest_timestamp,
            cursor=cursor if self.config.pagination.style == "cursor" else None,
            last_run_at=datetime.utcnow().isoformat(),
        )

        return records, new_state

    def _build_query_params(
        self, params: SearchParams, page: int, offset: int, cursor: str | None
    ) -> dict:
        """Build query parameters for the API request."""
        query: dict = {}
        pag = self.config.pagination

        # Search params
        if params.keywords:
            query["q"] = " ".join(params.keywords)
        if params.location:
            query["location"] = params.location
        if params.date_range:
            if params.date_range.from_date:
                query["date_from"] = params.date_range.from_date
            if params.date_range.to_date:
                query["date_to"] = params.date_range.to_date

        # Extra filters — only include filters relevant to this source.
        # Each source in config.yaml has specific params; the global filters
        # contain params for ALL sources.
        #
        # IMPORTANT: We use exact source name matching to avoid prefix
        # collisions (e.g. "nrel" matching both "nrel_alt_fuel" and
        # "nrel_canada" which need different country filters).
        source_name = self.config.name.lower()
        source_filter_map: dict[str, dict[str, set[str]]] = {
            # exact name -> allowed filter keys
            "openchargemap":    {"keys": {"output", "compact", "verbose"}},
            "ocm_australia":    {"keys": {"output", "compact", "verbose"}},
            "ocm_southeast_asia": {"keys": {"output", "compact", "verbose"}},
            "nrel_alt_fuel":    {"keys": {"fuel_type", "status", "country", "access"}},
            "nrel_canada":      {"keys": {"fuel_type", "status", "access"},
                                 "overrides": {"country": "CA"}},
            "nobil":            {"keys": {"apiversion", "action", "format"}},
            "korea_ev":         {"keys": {"dataType"}},
        }
        entry = source_filter_map.get(source_name)
        if entry is not None:
            allowed_keys = entry["keys"]
            overrides = entry.get("overrides", {})
            for k, v in params.filters.items():
                if k in allowed_keys:
                    query[k] = v
            # Apply source-specific overrides (e.g. country=CA for nrel_canada)
            query.update(overrides)
        else:
            # Unknown source type: don't send any global filters
            pass

        # Pagination
        query[pag.per_page_param] = pag.per_page
        if pag.style == "page_number":
            query[pag.page_param] = page
        elif pag.style == "offset":
            query[pag.offset_param] = offset
        elif pag.style == "cursor" and cursor:
            query[pag.cursor_param] = cursor

        return query

    def _extract_items(self, data: dict | list) -> list[dict]:
        """Extract the list of records from the API response."""
        if isinstance(data, list):
            return data

        if self.config.response_path:
            items = get_nested_value(data, self.config.response_path)
            return items if isinstance(items, list) else []

        # Common patterns: try "results", "data", "items"
        for key in ("results", "data", "items", "records", "entries"):
            if key in data and isinstance(data[key], list):
                return data[key]

        return []

    def _extract_cursor(self, data: dict) -> str | None:
        """Extract the next-page cursor from the API response."""
        if isinstance(data, dict):
            for key in ("next_cursor", "cursor", "next_page", "next"):
                if key in data and data[key]:
                    return str(data[key])
            # Check nested pagination metadata
            meta = data.get("meta") or data.get("pagination") or {}
            if isinstance(meta, dict):
                for key in ("next_cursor", "cursor", "next_page"):
                    if key in meta and meta[key]:
                        return str(meta[key])
        return None
