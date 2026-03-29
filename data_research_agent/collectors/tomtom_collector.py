"""TomTom EV Charging Stations collector.

Uses TomTom's Search API (Category Search) to find EV charging stations
with connector details, then optionally enriches with real-time availability.

Coverage: 2.1M+ charge points, 90+ countries, strong in SEA.
Free tier: 2,500 non-tile requests/day.
Docs: https://developer.tomtom.com/search-api/documentation/search-service/category-search

Setup:
    export TOMTOM_API_KEY="your_key_here"
    Register free at https://developer.tomtom.com
"""

from __future__ import annotations

from datetime import datetime

from models import CrawlState, RawRecord, SearchParams
from .base import BaseCollector


# TomTom connector type mapping
CONNECTOR_MAP = {
    "IEC62196Type1": "Type 1 (J1772)",
    "IEC62196Type1CCS": "CCS1",
    "IEC62196Type2CableAttached": "Type 2 (Cable)",
    "IEC62196Type2Outlet": "Type 2 (Socket)",
    "IEC62196Type2CCS": "CCS2",
    "IEC62196Type3": "Type 3",
    "Chademo": "CHAdeMO",
    "Tesla": "Tesla",
    "StandardHouseholdCountrySpecific": "Domestic",
    "GBT20234Part2": "GB/T AC",
    "GBT20234Part3": "GB/T DC",
    "IEC60309AC1PhaseBlue": "Blue Commando",
    "IEC60309DCWhite": "White Commando",
    "NACS": "NACS",
}

# Bounding boxes for SEA + Australia regions
# (topLeft_lat, topLeft_lon, btmRight_lat, btmRight_lon)
SEA_TILES = {
    "thailand": (20.5, 97.0, 5.5, 106.0),
    "malaysia_peninsular": (7.5, 99.0, 1.0, 105.0),
    "malaysia_borneo": (7.5, 109.0, 0.8, 119.5),
    "singapore": (1.5, 103.6, 1.2, 104.1),
    "indonesia_java": (-5.8, 105.0, -8.8, 115.0),
    "indonesia_sumatra": (6.0, 95.0, -6.0, 106.0),
    "indonesia_other": (-1.0, 115.0, -11.0, 141.0),
    "philippines_luzon": (19.0, 119.0, 13.0, 127.0),
    "philippines_vismin": (13.0, 119.0, 5.0, 127.0),
    "vietnam_north": (23.5, 102.0, 15.5, 110.0),
    "vietnam_south": (15.5, 105.0, 8.5, 110.0),
    "taiwan": (25.5, 119.5, 21.5, 122.5),
    "hong_kong": (22.6, 113.8, 22.1, 114.5),
    "cambodia_laos_myanmar": (28.5, 92.0, 5.5, 108.0),
    "australia_east": (-10.0, 138.0, -44.0, 154.0),
    "australia_west": (-10.0, 112.0, -36.0, 138.0),
}


def _parse_tomtom_result(result: dict) -> dict:
    """Parse a TomTom search result into common schema."""
    poi = result.get("poi", {})
    address = result.get("address", {})
    position = result.get("position", {})
    charging_park = result.get("chargingPark", {})
    data_sources = result.get("dataSources", {})

    # Parse connectors
    connectors = charging_park.get("connectors", [])
    connector_list = []
    total_power = 0
    max_power = 0
    num_ports = 0

    for conn in connectors:
        raw_type = conn.get("connectorType", "")
        label = CONNECTOR_MAP.get(raw_type, raw_type)
        power = conn.get("ratedPowerKW", 0) or 0
        connector_list.append(f"{label} ({power}kW)" if power else label)
        total_power += power
        max_power = max(max_power, power)
        num_ports += 1

    # Classify DC fast vs Level 2
    num_dc_fast = sum(1 for c in connectors if (c.get("ratedPowerKW", 0) or 0) >= 50)
    num_level2 = sum(1 for c in connectors if 0 < (c.get("ratedPowerKW", 0) or 0) < 50)

    # Availability ID for optional real-time enrichment
    avail_id = ""
    charging_avail = data_sources.get("chargingAvailability", {})
    if isinstance(charging_avail, dict):
        avail_id = charging_avail.get("id", "")

    return {
        "station_id": f"tt_{result.get('id', '')}",
        "station_name": poi.get("name", ""),
        "address": address.get("freeformAddress", ""),
        "city": address.get("municipality", ""),
        "state": address.get("countrySubdivision", ""),
        "country": address.get("country", ""),
        "country_code": address.get("countryCode", ""),
        "postal_code": address.get("postalCode", ""),
        "latitude": position.get("lat"),
        "longitude": position.get("lon"),
        "network": poi.get("brands", [{}])[0].get("name", "") if poi.get("brands") else "",
        "operator": poi.get("name", ""),
        "connector_types": ", ".join(connector_list),
        "num_ports": num_ports,
        "num_level2_ports": num_level2,
        "num_dc_fast_ports": num_dc_fast,
        "power_kw": max_power if max_power > 0 else None,
        "status": "Operational",
        "access_type": "Public",
        "usage_cost": "",
        "phone": poi.get("phone", ""),
        "facility_type": "",
        "availability_id": avail_id,
        "data_provider": "TomTom",
        "date_last_updated": None,
    }


class TomTomEVCollector(BaseCollector):
    """Collector for TomTom EV charging station search API.

    Uses Category Search with EV category (7309) and regional bounding boxes
    to tile across Southeast Asia and Australia.
    """

    EV_CATEGORY = "7309"  # TomTom category for EV charging stations
    MAX_PER_REQUEST = 100  # TomTom max results per request

    def fetch_batch(self, params: SearchParams, limit: int | None = None) -> list[RawRecord]:
        records: list[RawRecord] = []
        max_total = limit or 100000

        # Determine which tiles to query
        tiles = self._select_tiles(params)

        for tile_name, bbox in tiles:
            if len(records) >= max_total:
                break

            self.logger.info("[tomtom] Querying tile: %s (%d records so far)", tile_name, len(records))
            tile_records = self._fetch_tile(bbox, max_total - len(records))
            records.extend(tile_records)

        self.logger.info("[tomtom] Total: %d stations from %d tiles", len(records), len(tiles))
        return records

    def _fetch_tile(self, bbox: tuple, remaining: int) -> list[RawRecord]:
        """Fetch all stations within a bounding box using offset pagination."""
        records: list[RawRecord] = []
        offset = 0
        top_lat, left_lon, btm_lat, right_lon = bbox

        while len(records) < remaining:
            query_params = {
                "categorySet": self.EV_CATEGORY,
                "topLeft": f"{top_lat},{left_lon}",
                "btmRight": f"{btm_lat},{right_lon}",
                "limit": self.MAX_PER_REQUEST,
                "ofs": offset,
            }

            try:
                response = self._make_request(
                    self.config.base_url,
                    params=query_params,
                    timeout=30,
                )
                data = response.json()
            except Exception as e:
                self.logger.error("[tomtom] Request failed at offset %d: %s", offset, e)
                break

            results = data.get("results", [])
            if not results:
                break

            for result in results:
                parsed = _parse_tomtom_result(result)
                records.append(RawRecord(
                    source=self.config.name,
                    raw_data=parsed,
                    source_url=self.config.base_url,
                ))

            total_results = data.get("summary", {}).get("totalResults", 0)
            offset += len(results)

            self.logger.info(
                "[tomtom] Fetched %d/%d results (offset=%d)",
                len(records), total_results, offset,
            )

            # Stop if we've fetched all available or reached page limit
            if offset >= total_results or len(results) < self.MAX_PER_REQUEST:
                break

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

    def _select_tiles(self, params: SearchParams) -> list[tuple[str, tuple]]:
        """Select which regional tiles to query."""
        region = params.filters.get("region", "").lower()

        if region == "sea":
            keys = [k for k in SEA_TILES if not k.startswith("australia")]
        elif region == "australia":
            keys = [k for k in SEA_TILES if k.startswith("australia")]
        elif region:
            # Match specific tile name
            keys = [k for k in SEA_TILES if region in k]
        else:
            # Default: all tiles
            keys = list(SEA_TILES.keys())

        return [(k, SEA_TILES[k]) for k in keys]
