"""Supercharge.info collector for Tesla Supercharger stations globally.

Returns all sites in a single JSON response — no pagination, no auth.
Community-maintained, open data.
Source: https://supercharge.info
"""

from __future__ import annotations

from datetime import datetime

from models import CrawlState, RawRecord, SearchParams
from .base import BaseCollector


def _parse_supercharger_site(site: dict) -> dict:
    """Parse a Supercharge.info site into common schema."""
    address = site.get("address", {})
    gps = site.get("gps", {})
    stalls = site.get("stalls", {})
    plugs = site.get("plugs", {})

    # Build connector types from plugs
    connector_list = []
    plug_map = {
        "nacs": "NACS",
        "ccs1": "CCS1",
        "ccs2": "CCS2",
        "type2": "Type 2",
        "gbt": "GB/T",
    }
    for key, label in plug_map.items():
        count = plugs.get(key, 0)
        if count and count > 0:
            connector_list.append(f"{label} ({count})")

    # Total stall count
    stall_count = site.get("stallCount", 0)

    return {
        "station_id": f"sc_{site.get('id', '')}",
        "station_name": site.get("name", ""),
        "address": address.get("street", ""),
        "city": address.get("city", ""),
        "state": address.get("state", ""),
        "country": address.get("country", ""),
        "country_code": address.get("countryId", ""),
        "postal_code": address.get("zip", ""),
        "latitude": gps.get("latitude"),
        "longitude": gps.get("longitude"),
        "network": "Tesla Supercharger",
        "operator": "Tesla",
        "connector_types": ", ".join(connector_list),
        "num_ports": stall_count,
        "num_dc_fast_ports": stall_count,
        "power_kw": site.get("powerKilowatt"),
        "status": site.get("status", ""),
        "access_type": "Public" if site.get("otherEVs") else "Tesla Only",
        "usage_cost": "",
        "facility_type": site.get("facilityName", ""),
        "date_opened": site.get("dateOpened", ""),
        "data_provider": "supercharge.info",
        "date_last_updated": None,
    }


class SuperchargerCollector(BaseCollector):
    """Collector for Supercharge.info API (all Tesla Supercharger sites)."""

    def fetch_batch(self, params: SearchParams, limit: int | None = None) -> list[RawRecord]:
        self.logger.info("[supercharger] Fetching all sites from supercharge.info")

        response = self._make_request(self.config.base_url, timeout=60)
        sites = response.json()

        if not isinstance(sites, list):
            self.logger.error("[supercharger] Unexpected response format")
            return []

        records: list[RawRecord] = []
        country_filter = params.filters.get("countrycode", "").upper()

        for site in sites:
            # Optional country filter
            if country_filter:
                site_country = (site.get("address", {}).get("countryId", "") or "").upper()
                if site_country != country_filter:
                    continue

            parsed = _parse_supercharger_site(site)
            records.append(RawRecord(
                source="supercharger_info",
                raw_data=parsed,
                source_url=f"https://supercharge.info/changes/{site.get('id', '')}",
            ))

            if limit and len(records) >= limit:
                break

        self.logger.info("[supercharger] Fetched %d sites", len(records))
        return records

    def fetch_incremental(
        self, state: CrawlState, max_records: int = 500
    ) -> tuple[list[RawRecord], CrawlState]:
        # Use the changes endpoint for incremental
        changes_url = self.config.base_url.replace("allSites", "allChanges")
        records = self.fetch_batch(SearchParams(), limit=max_records)
        new_state = CrawlState(
            source_name=self.config.name,
            last_run_at=datetime.utcnow().isoformat(),
        )
        return records, new_state
