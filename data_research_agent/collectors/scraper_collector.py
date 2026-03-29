"""HTML scraping collector with configurable CSS selectors."""

from __future__ import annotations

from datetime import datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from models import CrawlState, RawRecord, SearchParams

from .base import BaseCollector


class ScraperCollector(BaseCollector):
    """Collects data by scraping HTML pages with CSS selectors."""

    def fetch_batch(self, params: SearchParams, limit: int | None = None) -> list[RawRecord]:
        records: list[RawRecord] = []
        selectors = self.config.selectors
        if not selectors:
            self.logger.error("[%s] No selectors configured", self.config.name)
            return records

        url = self._build_url(params)
        page_num = 0

        while url:
            page_num += 1
            self.logger.info("[%s] Scraping page %d: %s", self.config.name, page_num, url)

            response = self._make_request(url)
            soup = BeautifulSoup(response.text, "lxml")

            items = soup.select(selectors.list_item)
            if not items:
                self.logger.info("[%s] No items found on page %d", self.config.name, page_num)
                break

            for item in items:
                record_data = self._extract_fields(item, selectors.fields)
                records.append(RawRecord(
                    source=self.config.name,
                    raw_data=record_data,
                    source_url=url,
                ))
                if limit and len(records) >= limit:
                    return records

            self.logger.info("[%s] Fetched %d records so far", self.config.name, len(records))

            # Find next page
            url = self._find_next_page(soup, selectors.next_page, url)

        return records

    def fetch_incremental(
        self, state: CrawlState, max_records: int = 500
    ) -> tuple[list[RawRecord], CrawlState]:
        records: list[RawRecord] = []
        selectors = self.config.selectors
        if not selectors:
            self.logger.error("[%s] No selectors configured", self.config.name)
            return records, state

        url = self.config.base_url
        latest_id = state.last_id
        hit_known = False

        while url and len(records) < max_records and not hit_known:
            response = self._make_request(url)
            soup = BeautifulSoup(response.text, "lxml")
            items = soup.select(selectors.list_item)

            if not items:
                break

            for item in items:
                record_data = self._extract_fields(item, selectors.fields)
                # Use the first field value as a simple ID for dedup
                item_id = self._compute_item_id(record_data)

                if item_id and item_id == state.last_id:
                    hit_known = True
                    break

                records.append(RawRecord(
                    source=self.config.name,
                    raw_data=record_data,
                    source_url=url,
                ))

                # Track the first (newest) item as latest
                if latest_id is None or latest_id == state.last_id:
                    latest_id = item_id

                if len(records) >= max_records:
                    break

            if not hit_known:
                url = self._find_next_page(soup, selectors.next_page, url)

        new_state = CrawlState(
            source_name=self.config.name,
            last_id=latest_id,
            last_run_at=datetime.utcnow().isoformat(),
        )

        return records, new_state

    def _build_url(self, params: SearchParams) -> str:
        """Build the initial URL with search params as query string."""
        url = self.config.base_url
        query_parts = []
        if params.keywords:
            query_parts.append(f"q={'+'.join(params.keywords)}")
        if params.location:
            query_parts.append(f"location={params.location}")
        if query_parts:
            separator = "&" if "?" in url else "?"
            url += separator + "&".join(query_parts)
        return url

    def _extract_fields(self, element, field_selectors: dict[str, str]) -> dict:
        """Extract fields from an HTML element using CSS selectors.

        Selector format:
            "css_selector::text"       -> get text content
            "css_selector::attr(name)" -> get attribute value
            "css_selector"             -> get text content (default)
        """
        data = {}
        for field_name, selector in field_selectors.items():
            css_sel, extract_type = self._parse_selector(selector)

            if css_sel:
                found = element.select_one(css_sel)
            else:
                found = element

            if not found:
                data[field_name] = None
                continue

            if extract_type == "text":
                data[field_name] = found.get_text(strip=True)
            elif extract_type.startswith("attr("):
                attr_name = extract_type[5:-1]
                data[field_name] = found.get(attr_name)
            else:
                data[field_name] = found.get_text(strip=True)

        return data

    def _parse_selector(self, selector: str) -> tuple[str, str]:
        """Parse 'css_selector::extract_type' into (css, type)."""
        if "::" in selector:
            parts = selector.rsplit("::", 1)
            return parts[0].strip(), parts[1].strip()
        return selector, "text"

    def _find_next_page(self, soup: BeautifulSoup, next_selector: str | None, current_url: str) -> str | None:
        """Find the next page URL from the page."""
        if not next_selector:
            return None

        css_sel, extract_type = self._parse_selector(next_selector)
        link = soup.select_one(css_sel)
        if not link:
            return None

        if extract_type.startswith("attr("):
            attr_name = extract_type[5:-1]
            href = link.get(attr_name)
        else:
            href = link.get("href")

        if not href:
            return None

        return urljoin(current_url, href)

    def _compute_item_id(self, record_data: dict) -> str | None:
        """Compute a simple ID from the record data for dedup tracking."""
        # Use first non-None field value as the ID
        for value in record_data.values():
            if value:
                return str(value)[:100]
        return None
