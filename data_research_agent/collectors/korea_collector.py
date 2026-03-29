"""South Korea Ministry of Environment EV Charger API collector.

Uses the data.go.kr open API for EV charging station data.
Free API key required — register at https://www.data.go.kr/data/15076352/openapi.do

~60k+ public stations with real-time status available.
"""

from __future__ import annotations

from datetime import datetime

from models import CrawlState, RawRecord, SearchParams
from .base import BaseCollector


# Charger type code mapping
CHARGER_TYPE_MAP = {
    "01": "DC Slow",
    "02": "AC Slow",
    "03": "DC Fast (CHAdeMO)",
    "04": "AC 3-phase",
    "05": "DC Fast (CCS1)",
    "06": "DC Fast (CCS1) + AC",
    "07": "AC Slow (Type 2)",
    "08": "DC (CCS1) + CHAdeMO + AC",
    "09": "DC (CCS2)",
    "10": "DC (CCS2) + AC (Type 2)",
    "11": "DC (CHAdeMO) + AC",
}

# Status code mapping
STATUS_MAP = {
    "1": "Communication Error",
    "2": "Available",
    "3": "Charging",
    "4": "Unavailable",
    "5": "Under Maintenance",
    "9": "Not Confirmed",
}


def _parse_korea_item(item: dict) -> dict:
    """Parse a Korean API item into common schema."""
    charger_type_code = str(item.get("chgerType", ""))
    connector_type = CHARGER_TYPE_MAP.get(charger_type_code, f"Type {charger_type_code}")

    status_code = str(item.get("stat", ""))
    status = STATUS_MAP.get(status_code, f"Code {status_code}")

    # Power capacity
    power_kw = None
    try:
        power_kw = float(item.get("output", 0))
    except (ValueError, TypeError):
        pass

    return {
        "station_id": f"kr_{item.get('statId', '')}_{item.get('chgerId', '')}",
        "station_name": item.get("statNm", ""),
        "address": item.get("addr", ""),
        "city": "",  # Extractable from addr or zcode
        "state": "",
        "country": "South Korea",
        "country_code": "KR",
        "postal_code": "",
        "latitude": item.get("lat"),
        "longitude": item.get("lng"),
        "network": item.get("busiNm", ""),
        "operator": item.get("busiNm", ""),
        "connector_types": connector_type,
        "num_ports": 1,  # Each record is a single charger
        "power_kw": power_kw,
        "status": status,
        "access_type": "Public" if item.get("limitYn") == "N" else "Restricted",
        "usage_cost": "",
        "facility_type": item.get("kindDetail", ""),
        "phone": item.get("busiCall", ""),
        "date_last_updated": item.get("statUpdDt", ""),
        "data_provider": "data.go.kr (Korea ME)",
    }


class KoreaEVCollector(BaseCollector):
    """Collector for South Korea Ministry of Environment EV charger API."""

    def fetch_batch(self, params: SearchParams, limit: int | None = None) -> list[RawRecord]:
        records: list[RawRecord] = []
        page = 1
        per_page = self.config.pagination.per_page
        max_total = limit or 100000

        while len(records) < max_total:
            query_params = {
                "serviceKey": "",  # Will be injected by _make_request via auth config
                "pageNo": page,
                "numOfRows": per_page,
                "dataType": "JSON",
            }

            # Add any extra filters (like zcode for region filtering)
            query_params.update(params.filters)

            try:
                response = self._make_request(self.config.base_url, params=query_params)
                data = response.json()
            except Exception as e:
                self.logger.error("[%s] Request failed at page %d: %s", self.config.name, page, e)
                break

            # Navigate response structure
            items = self._extract_items(data)
            if not items:
                break

            for item in items:
                parsed = _parse_korea_item(item)
                records.append(RawRecord(
                    source=self.config.name,
                    raw_data=parsed,
                    source_url=self.config.base_url,
                ))
                if len(records) >= max_total:
                    break

            self.logger.info("[%s] Fetched %d records (page %d)", self.config.name, len(records), page)

            if len(items) < per_page:
                break
            page += 1

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

    def _extract_items(self, data: dict) -> list[dict]:
        """Extract items from Korea API nested response structure."""
        # Structure: {response: {header: {...}, body: {items: {item: [...]}}}}
        try:
            body = data.get("response", data).get("body", data)
            items_wrapper = body.get("items", {})
            if isinstance(items_wrapper, dict):
                items = items_wrapper.get("item", [])
            elif isinstance(items_wrapper, list):
                items = items_wrapper
            else:
                items = []
            return items if isinstance(items, list) else [items]
        except (AttributeError, TypeError):
            return []
