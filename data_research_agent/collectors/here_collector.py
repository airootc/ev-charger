"""HERE Technologies EV Charge Points API collector.

Uses HERE's EV Charge Points API v2 (browse by proximity) to find
EV charging stations with connector details and availability.

Coverage: Global, strong SEA presence with dedicated local teams.
Free tier: 250,000 transactions/month.
Docs: https://developer.here.com/documentation/charging-stations/

Setup:
    export HERE_API_KEY="your_key_here"
    Register free at https://platform.here.com
"""

from __future__ import annotations

from datetime import datetime

from models import CrawlState, RawRecord, SearchParams
from .base import BaseCollector


# HERE connector type IDs to names
CONNECTOR_TYPE_MAP = {
    "2": "CHAdeMO",
    "6": "Type 1 (J1772)",
    "25": "Type 2 (Socket)",
    "29": "Type 2 (Cable)",
    "31": "Type 2 (Mennekes)",
    "32": "CCS1",
    "33": "CCS2",
    "36": "Tesla",
    "50": "NACS",
}

# Grid of center points for SEA + Australia (lat, lng, radius_meters)
# Using 50km radius circles for dense coverage
SEA_GRID = [
    # Thailand
    ("bangkok", 13.75, 100.50, 50000),
    ("chiang_mai", 18.79, 98.98, 50000),
    ("phuket", 7.88, 98.39, 50000),
    ("pattaya", 12.93, 100.88, 50000),
    ("nakhon_ratchasima", 14.97, 102.10, 50000),
    ("hat_yai", 7.00, 100.47, 50000),
    # Malaysia
    ("kuala_lumpur", 3.14, 101.69, 50000),
    ("penang", 5.41, 100.33, 50000),
    ("johor_bahru", 1.49, 103.74, 50000),
    ("kota_kinabalu", 5.98, 116.07, 50000),
    ("kuching", 1.55, 110.35, 50000),
    ("ipoh", 4.60, 101.08, 50000),
    # Singapore
    ("singapore", 1.35, 103.82, 25000),
    # Indonesia
    ("jakarta", -6.21, 106.85, 50000),
    ("surabaya", -7.25, 112.75, 50000),
    ("bandung", -6.91, 107.61, 50000),
    ("bali", -8.65, 115.22, 50000),
    ("medan", 3.60, 98.67, 50000),
    ("semarang", -6.97, 110.42, 50000),
    # Philippines
    ("manila", 14.60, 120.98, 50000),
    ("cebu", 10.31, 123.89, 50000),
    ("davao", 7.07, 125.61, 50000),
    ("clark", 15.19, 120.54, 50000),
    # Vietnam
    ("ho_chi_minh", 10.82, 106.63, 50000),
    ("hanoi", 21.03, 105.85, 50000),
    ("da_nang", 16.05, 108.22, 50000),
    # Taiwan
    ("taipei", 25.03, 121.57, 50000),
    ("taichung", 24.15, 120.67, 50000),
    ("kaohsiung", 22.63, 120.30, 50000),
    # Hong Kong
    ("hong_kong", 22.32, 114.17, 25000),
    # Australia major cities
    ("sydney", -33.87, 151.21, 50000),
    ("melbourne", -37.81, 144.96, 50000),
    ("brisbane", -27.47, 153.03, 50000),
    ("perth", -31.95, 115.86, 50000),
    ("adelaide", -34.93, 138.60, 50000),
    ("canberra", -35.28, 149.13, 50000),
    ("gold_coast", -28.02, 153.43, 50000),
]


def _parse_here_station(station: dict) -> dict:
    """Parse a HERE EV station response into common schema."""
    address = station.get("address", {})
    position = station.get("position", [{}])
    if isinstance(position, list) and position:
        pos = position[0]
    elif isinstance(position, dict):
        pos = position
    else:
        pos = {}

    # Parse connectors
    connectors_data = station.get("connectors", [])
    connector_list = []
    total_ports = 0
    max_power = 0
    num_dc = 0
    num_ac = 0

    for conn in connectors_data:
        ctype_id = str(conn.get("connectorType", {}).get("id", ""))
        ctype_name = conn.get("connectorType", {}).get("name", "")
        label = CONNECTOR_TYPE_MAP.get(ctype_id, ctype_name or f"Type_{ctype_id}")

        power = conn.get("maxPowerLevel", 0) or 0
        n_connectors = conn.get("numberOfConnectors", 1) or 1
        total_ports += n_connectors

        if power >= 50:
            num_dc += n_connectors
        else:
            num_ac += n_connectors

        max_power = max(max_power, power)
        connector_list.append(f"{label} ({power}kW x{n_connectors})" if power else label)

    return {
        "station_id": f"here_{station.get('poolId', '')}",
        "station_name": station.get("supplierName", address.get("street", "Unknown")),
        "address": f"{address.get('street', '')} {address.get('houseNumber', '')}".strip(),
        "city": address.get("city", ""),
        "state": address.get("state", ""),
        "country": address.get("country", ""),
        "country_code": address.get("countryCode", ""),
        "postal_code": address.get("postalCode", ""),
        "latitude": pos.get("lat"),
        "longitude": pos.get("lng"),
        "network": station.get("supplierName", ""),
        "operator": station.get("supplierName", ""),
        "connector_types": ", ".join(connector_list),
        "num_ports": total_ports,
        "num_level2_ports": num_ac,
        "num_dc_fast_ports": num_dc,
        "power_kw": max_power if max_power > 0 else None,
        "status": "Operational",
        "access_type": "Public" if not station.get("privateAccess") else "Private",
        "usage_cost": "",
        "data_provider": "HERE Technologies",
        "date_last_updated": None,
    }


class HEREEVCollector(BaseCollector):
    """Collector for HERE Technologies EV Charge Points API."""

    def fetch_batch(self, params: SearchParams, limit: int | None = None) -> list[RawRecord]:
        records: list[RawRecord] = []
        max_total = limit or 100000
        seen_ids: set[str] = set()

        grid = self._select_grid(params)

        for name, lat, lng, radius in grid:
            if len(records) >= max_total:
                break

            self.logger.info("[here] Querying: %s (%d records so far)", name, len(records))

            query_params = {
                "prox": f"{lat},{lng},{radius}",
            }

            try:
                response = self._make_request(self.config.base_url, params=query_params, timeout=30)
                data = response.json()
            except Exception as e:
                self.logger.error("[here] Failed for %s: %s", name, e)
                continue

            # Navigate response
            ev_stations = data.get("evStations", {}).get("evStation", [])
            if not ev_stations:
                continue

            for station in ev_stations:
                parsed = _parse_here_station(station)
                sid = parsed["station_id"]
                if sid in seen_ids:
                    continue
                seen_ids.add(sid)

                records.append(RawRecord(
                    source=self.config.name,
                    raw_data=parsed,
                    source_url=self.config.base_url,
                ))

                if len(records) >= max_total:
                    break

            self.logger.info("[here] %s: %d stations", name, len(ev_stations))

        self.logger.info("[here] Total: %d unique stations from %d grid points", len(records), len(grid))
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
