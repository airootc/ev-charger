"""Google Places API (New) collector for EV charging stations.

Uses Nearby Search with type "ev_charging_station" and proximity-based
grid tiling across SEA + Australia.

Coverage: Global — strong in SG, TH, MY; growing in VN, PH, ID.
Free tier: 5,000-10,000 events/month per SKU.
Docs: https://developers.google.com/maps/documentation/places/web-service/nearby-search

Setup:
    export GOOGLE_MAPS_API_KEY="your_key_here"
    Enable Places API (New) at console.cloud.google.com
"""

from __future__ import annotations

import json
import os
from datetime import datetime

from models import CrawlState, RawRecord, SearchParams
from .base import BaseCollector


# Grid of center points for SEA + Australia (lat, lng, radius_meters)
# Google Places returns max 20 results per request, so use smaller radii
SEA_GRID = [
    # Thailand
    ("bangkok_central", 13.75, 100.50, 15000),
    ("bangkok_north", 13.88, 100.55, 15000),
    ("bangkok_east", 13.72, 100.65, 15000),
    ("chiang_mai", 18.79, 98.98, 20000),
    ("phuket", 7.88, 98.39, 20000),
    ("pattaya", 12.93, 100.88, 20000),
    ("hat_yai", 7.00, 100.47, 20000),
    # Malaysia
    ("kl_central", 3.15, 101.71, 15000),
    ("kl_north", 3.22, 101.68, 15000),
    ("petaling_jaya", 3.11, 101.61, 15000),
    ("penang", 5.41, 100.33, 20000),
    ("johor_bahru", 1.49, 103.74, 20000),
    ("kota_kinabalu", 5.98, 116.07, 20000),
    ("kuching", 1.55, 110.35, 20000),
    # Singapore
    ("singapore_central", 1.30, 103.85, 10000),
    ("singapore_north", 1.38, 103.83, 10000),
    ("singapore_east", 1.33, 103.94, 10000),
    ("singapore_west", 1.34, 103.73, 10000),
    # Indonesia
    ("jakarta_central", -6.18, 106.83, 15000),
    ("jakarta_south", -6.28, 106.80, 15000),
    ("surabaya", -7.25, 112.75, 20000),
    ("bandung", -6.91, 107.61, 20000),
    ("bali", -8.65, 115.22, 20000),
    # Philippines
    ("manila_makati", 14.55, 121.02, 15000),
    ("manila_north", 14.65, 121.03, 15000),
    ("cebu", 10.31, 123.89, 20000),
    # Vietnam
    ("hcmc_central", 10.78, 106.70, 15000),
    ("hcmc_7", 10.73, 106.72, 15000),
    ("hanoi", 21.03, 105.85, 15000),
    ("da_nang", 16.05, 108.22, 20000),
    # Taiwan
    ("taipei", 25.03, 121.57, 15000),
    ("taichung", 24.15, 120.67, 20000),
    ("kaohsiung", 22.63, 120.30, 20000),
    # Hong Kong
    ("hk_central", 22.28, 114.16, 10000),
    ("hk_kowloon", 22.32, 114.17, 10000),
    ("hk_nt", 22.38, 114.12, 15000),
    # Australia
    ("sydney_cbd", -33.87, 151.21, 15000),
    ("sydney_west", -33.83, 151.00, 15000),
    ("melbourne_cbd", -37.81, 144.96, 15000),
    ("brisbane", -27.47, 153.03, 15000),
    ("perth", -31.95, 115.86, 15000),
    ("adelaide", -34.93, 138.60, 15000),
]


def _parse_google_place(place: dict) -> dict:
    """Parse a Google Places API response into common schema."""
    location = place.get("location", {})
    display_name = place.get("displayName", {})
    ev_options = place.get("evChargeOptions", {})

    # Parse connector info from evChargeOptions
    connector_list = []
    total_ports = 0
    max_power = 0
    num_dc = 0
    num_ac = 0

    for conn_agg in ev_options.get("connectorAggregation", []):
        ctype = conn_agg.get("type", "UNKNOWN")
        count = conn_agg.get("count", 0) or conn_agg.get("availableCount", 0) or 1
        power = conn_agg.get("maxChargeRateKw", 0) or 0
        avail = conn_agg.get("availableCount")
        avail_str = f", {avail} avail" if avail is not None else ""

        # Normalize connector names
        label = ctype.replace("EV_CONNECTOR_TYPE_", "").replace("_", " ").title()
        connector_list.append(f"{label} ({power}kW x{count}{avail_str})")

        total_ports += count
        max_power = max(max_power, power)
        if power >= 50:
            num_dc += count
        else:
            num_ac += count

    # Overall count from evChargeOptions
    if not total_ports:
        total_ports = ev_options.get("connectorCount", 0)

    return {
        "station_id": f"goog_{place.get('id', '')}",
        "station_name": display_name.get("text", ""),
        "address": place.get("formattedAddress", ""),
        "city": "",
        "state": "",
        "country": "",
        "country_code": "",
        "postal_code": "",
        "latitude": location.get("latitude"),
        "longitude": location.get("longitude"),
        "network": "",
        "operator": "",
        "connector_types": ", ".join(connector_list),
        "num_ports": total_ports,
        "num_level2_ports": num_ac,
        "num_dc_fast_ports": num_dc,
        "power_kw": max_power if max_power > 0 else None,
        "status": "Operational" if place.get("businessStatus") == "OPERATIONAL" else place.get("businessStatus", ""),
        "access_type": "Public",
        "usage_cost": "",
        "phone": place.get("nationalPhoneNumber", ""),
        "data_provider": "Google Places",
        "date_last_updated": None,
    }


class GooglePlacesEVCollector(BaseCollector):
    """Collector for Google Places API (New) — EV charging stations.

    Uses POST-based Nearby Search with proximity grid tiling.
    Max 20 results per request, no pagination token.
    """

    FIELD_MASK = ",".join([
        "places.id",
        "places.displayName",
        "places.formattedAddress",
        "places.location",
        "places.evChargeOptions",
        "places.businessStatus",
        "places.nationalPhoneNumber",
    ])

    def fetch_batch(self, params: SearchParams, limit: int | None = None) -> list[RawRecord]:
        records: list[RawRecord] = []
        max_total = limit or 50000
        seen_ids: set[str] = set()

        grid = self._select_grid(params)

        # Get API key
        api_key = os.environ.get(self.config.auth.key_env or "", "")
        if not api_key:
            self.logger.error("[google] API key not set: %s", self.config.auth.key_env)
            return []

        for name, lat, lng, radius in grid:
            if len(records) >= max_total:
                break

            self.logger.info("[google] Querying: %s (%d records so far)", name, len(records))

            body = {
                "includedTypes": ["ev_charging_station"],
                "maxResultCount": 20,
                "locationRestriction": {
                    "circle": {
                        "center": {"latitude": lat, "longitude": lng},
                        "radius": float(radius),
                    }
                },
                "rankPreference": "DISTANCE",
            }

            try:
                self.rate_limiter.wait()
                response = self._session.post(
                    self.config.base_url,
                    json=body,
                    headers={
                        "X-Goog-Api-Key": api_key,
                        "X-Goog-FieldMask": self.FIELD_MASK,
                        "Content-Type": "application/json",
                    },
                    timeout=30,
                )
                self.logger.info(
                    "[%s] POST %s -> %d (%d bytes)",
                    self.config.name, self.config.base_url,
                    response.status_code, len(response.content),
                )
                response.raise_for_status()
                data = response.json()
            except Exception as e:
                self.logger.error("[google] Failed for %s: %s", name, e)
                continue

            places = data.get("places", [])
            for place in places:
                parsed = _parse_google_place(place)
                pid = parsed["station_id"]
                if pid in seen_ids:
                    continue
                seen_ids.add(pid)

                records.append(RawRecord(
                    source=self.config.name,
                    raw_data=parsed,
                    source_url="https://places.googleapis.com/v1/places:searchNearby",
                ))

                if len(records) >= max_total:
                    break

            self.logger.info("[google] %s: %d places", name, len(places))

        self.logger.info("[google] Total: %d unique stations from %d grid points", len(records), len(grid))
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

    def _select_grid(self, params: SearchParams) -> list[tuple]:
        """Select grid points based on search params."""
        region = params.filters.get("region", "").lower()
        if region:
            return [g for g in SEA_GRID if region in g[0]]
        return SEA_GRID
