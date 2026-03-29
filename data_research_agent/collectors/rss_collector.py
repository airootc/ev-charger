"""RSS/Atom feed collector."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime

from models import CrawlState, RawRecord, SearchParams

from .base import BaseCollector


class RSSCollector(BaseCollector):
    """Collects data from RSS and Atom feeds."""

    # Common XML namespaces for Atom feeds
    NAMESPACES = {
        "atom": "http://www.w3.org/2005/Atom",
        "dc": "http://purl.org/dc/elements/1.1/",
        "content": "http://purl.org/rss/1.0/modules/content/",
    }

    def fetch_batch(self, params: SearchParams, limit: int | None = None) -> list[RawRecord]:
        response = self._make_request(self.config.base_url)
        entries = self._parse_feed(response.text)

        records = []
        for entry in entries:
            # Filter by keywords if provided
            if params.keywords and not self._matches_keywords(entry, params.keywords):
                continue

            records.append(RawRecord(
                source=self.config.name,
                raw_data=entry,
                source_url=entry.get("link", self.config.base_url),
            ))

            if limit and len(records) >= limit:
                break

        self.logger.info("[%s] Fetched %d records from feed", self.config.name, len(records))
        return records

    def fetch_incremental(
        self, state: CrawlState, max_records: int = 500
    ) -> tuple[list[RawRecord], CrawlState]:
        response = self._make_request(self.config.base_url)
        entries = self._parse_feed(response.text)

        records = []
        latest_id = state.last_id
        latest_timestamp = state.last_timestamp

        for entry in entries:
            entry_id = entry.get("id") or entry.get("link", "")

            # Stop at previously seen entry
            if entry_id and entry_id == state.last_id:
                break

            # Stop at entries older than last timestamp
            entry_date = entry.get("published") or entry.get("updated")
            if state.last_timestamp and entry_date:
                if entry_date <= state.last_timestamp:
                    break

            records.append(RawRecord(
                source=self.config.name,
                raw_data=entry,
                source_url=entry.get("link", self.config.base_url),
            ))

            # Track the newest entry (first in list)
            if latest_id is None or latest_id == state.last_id:
                latest_id = entry_id
                if entry_date:
                    latest_timestamp = entry_date

            if len(records) >= max_records:
                break

        new_state = CrawlState(
            source_name=self.config.name,
            last_id=latest_id,
            last_timestamp=latest_timestamp,
            last_run_at=datetime.utcnow().isoformat(),
        )

        self.logger.info("[%s] Fetched %d new records from feed", self.config.name, len(records))
        return records, new_state

    def _parse_feed(self, xml_text: str) -> list[dict]:
        """Parse RSS or Atom feed XML into a list of entry dicts."""
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            self.logger.error("[%s] Failed to parse feed XML: %s", self.config.name, e)
            return []

        # Detect feed type
        tag = root.tag.lower()
        if "rss" in tag:
            return self._parse_rss(root)
        elif "feed" in tag:
            return self._parse_atom(root)
        else:
            # Try RSS first, then Atom
            entries = self._parse_rss(root)
            if not entries:
                entries = self._parse_atom(root)
            return entries

    def _parse_rss(self, root: ET.Element) -> list[dict]:
        """Parse RSS 2.0 feed."""
        entries = []
        for item in root.iter("item"):
            entry = {
                "title": self._get_text(item, "title"),
                "link": self._get_text(item, "link"),
                "description": self._get_text(item, "description"),
                "published": self._get_text(item, "pubDate"),
                "id": self._get_text(item, "guid") or self._get_text(item, "link"),
                "author": self._get_text(item, "author") or self._get_text(item, "dc:creator"),
            }
            # Add any category tags
            categories = [cat.text for cat in item.findall("category") if cat.text]
            if categories:
                entry["categories"] = categories
            entries.append(entry)
        return entries

    def _parse_atom(self, root: ET.Element) -> list[dict]:
        """Parse Atom feed."""
        ns = self.NAMESPACES
        entries = []
        for item in root.findall("atom:entry", ns):
            link_el = item.find("atom:link[@rel='alternate']", ns)
            if link_el is None:
                link_el = item.find("atom:link", ns)

            entry = {
                "title": self._get_text(item, "atom:title", ns),
                "link": link_el.get("href") if link_el is not None else None,
                "description": self._get_text(item, "atom:summary", ns),
                "published": self._get_text(item, "atom:published", ns),
                "updated": self._get_text(item, "atom:updated", ns),
                "id": self._get_text(item, "atom:id", ns),
                "author": self._get_text(
                    item.find("atom:author", ns), "atom:name", ns
                ) if item.find("atom:author", ns) is not None else None,
            }
            entries.append(entry)
        return entries

    def _get_text(self, element: ET.Element | None, tag: str, ns: dict | None = None) -> str | None:
        """Get text content of a child element."""
        if element is None:
            return None
        child = element.find(tag, ns) if ns else element.find(tag)
        if child is not None and child.text:
            return child.text.strip()
        return None

    def _matches_keywords(self, entry: dict, keywords: list[str]) -> bool:
        """Check if entry contains any of the keywords."""
        searchable = " ".join(
            str(v).lower() for v in entry.values() if v and isinstance(v, str)
        )
        return any(kw.lower() in searchable for kw in keywords)
