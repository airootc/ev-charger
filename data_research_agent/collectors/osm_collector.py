"""OpenStreetMap Overpass API collector for EV charging stations.

The Overpass API uses POST with a custom query language (Overpass QL),
which doesn't fit the generic API collector's GET + pagination pattern.

Usage is free, no API key required. Rate limit: ~2 req/s recommended.
Docs: https://wiki.openstreetmap.org/wiki/Overpass_API
"""

from __future__ import annotations

from datetime import datetime

from models import CrawlState, RawRecord, SearchParams
from .base import BaseCollector


# Bounding boxes for global tiling (lat_south, lon_west, lat_north, lon_east)
# Split the world into manageable regions to avoid timeouts
REGION_TILES = {
    "europe_west": (35, -15, 60, 15),
    "europe_east": (35, 15, 60, 45),
    "europe_north": (60, -15, 72, 45),
    "north_america_east": (25, -100, 50, -60),
    "north_america_west": (25, -130, 50, -100),
    "east_asia": (20, 100, 50, 145),
    "south_asia": (5, 65, 35, 100),
    "southeast_asia": (-10, 95, 20, 145),
    "oceania": (-50, 110, -10, 180),
    "south_america": (-55, -80, 15, -35),
    "africa_north": (0, -20, 37, 55),
    "africa_south": (-35, 10, 0, 55),
    "middle_east": (12, 25, 42, 65),
}


def _build_overpass_query(bbox: tuple[float, float, float, float], limit: int = 5000) -> str:
    """Build an Overpass QL query for charging stations in a bounding box."""
    south, west, north, east = bbox
    # Use a shorter timeout for large regions to fail fast and move on
    area_deg2 = abs(north - south) * abs(east - west)
    timeout = 60 if area_deg2 > 400 else 120
    return f"""
[out:json][timeout:{timeout}];
(
  node["amenity"="charging_station"]({south},{west},{north},{east});
  way["amenity"="charging_station"]({south},{west},{north},{east});
);
out body {limit};
"""


def _parse_osm_element(element: dict) -> dict:
    """Parse an OSM element into a flat dict matching our common schema."""
    tags = element.get("tags", {})

    # Build address from parts
    address_parts = []
    if tags.get("addr:street"):
        street = tags["addr:street"]
        if tags.get("addr:housenumber"):
            street = f"{tags['addr:housenumber']} {street}"
        address_parts.append(street)
    address = ", ".join(address_parts) if address_parts else tags.get("name", "")

    # Count connectors from socket:* tags
    connector_types = []
    num_ports = 0
    socket_map = {
        "socket:type2": "Type 2",
        "socket:type2_combo": "CCS (Type 2)",
        "socket:chademo": "CHAdeMO",
        "socket:nacs": "NACS (Tesla)",
        "socket:type1": "Type 1 (J1772)",
        "socket:type1_combo": "CCS (Type 1)",
        "socket:schuko": "Schuko",
        "socket:tesla_supercharger": "Tesla Supercharger",
    }
    for tag_key, connector_name in socket_map.items():
        count_str = tags.get(tag_key)
        if count_str is not None:
            connector_types.append(connector_name)
            try:
                num_ports += int(count_str)
            except ValueError:
                num_ports += 1  # tag exists but value isn't numeric

    # Fallback to capacity tag
    if num_ports == 0:
        try:
            num_ports = int(tags.get("capacity", 0))
        except (ValueError, TypeError):
            pass

    return {
        "station_id": f"osm_{element.get('type', 'node')}_{element.get('id', '')}",
        "station_name": tags.get("name", tags.get("operator", "Unknown")),
        "address": address,
        "city": tags.get("addr:city", ""),
        "state": tags.get("addr:state", ""),
        "country": tags.get("addr:country", ""),
        "country_code": tags.get("addr:country", ""),
        "postal_code": tags.get("addr:postcode", ""),
        "latitude": element.get("lat"),
        "longitude": element.get("lon"),
        "network": tags.get("network", ""),
        "operator": tags.get("operator", ""),
        "connector_types": ", ".join(connector_types) if connector_types else "",
        "num_ports": num_ports if num_ports > 0 else None,
        "status": "Operational" if tags.get("disused") != "yes" else "Not Operational",
        "access_type": tags.get("access", "public"),
        "usage_cost": "Free" if tags.get("fee") == "no" else (tags.get("charge", "") if tags.get("fee") == "yes" else ""),
        "data_provider": "OpenStreetMap",
        "date_last_updated": None,
    }


class OSMOverpassCollector(BaseCollector):
    """Collector for OpenStreetMap Overpass API (EV charging stations)."""

    def fetch_batch(self, params: SearchParams, limit: int | None = None) -> list[RawRecord]:
        records: list[RawRecord] = []
        max_total = limit or 50000

        # Determine which regions to query
        regions = self._select_regions(params)

        for region_name, bbox in regions:
            if len(records) >= max_total:
                break

            per_region_limit = min(5000, max_total - len(records))
            query = _build_overpass_query(bbox, limit=per_region_limit)

            self.logger.info("[osm] Querying region: %s (%d records so far)", region_name, len(records))

            try:
                response = self._make_request(
                    self.config.base_url,
                    method="POST",
                    data={"data": query},
                    timeout=120,
                )
                data = response.json()
                elements = data.get("elements", [])

                for element in elements:
                    # Skip ways without coordinates
                    if "lat" not in element or "lon" not in element:
                        continue
                    parsed = _parse_osm_element(element)
                    records.append(RawRecord(
                        source="osm_overpass",
                        raw_data=parsed,
                        source_url=f"https://www.openstreetmap.org/{element.get('type', 'node')}/{element.get('id', '')}",
                    ))

                self.logger.info("[osm] Region %s: %d stations", region_name, len(elements))

            except Exception as e:
                self.logger.error("[osm] Failed for region %s: %s", region_name, e)

        self.logger.info("[osm] Total: %d stations from %d regions", len(records), len(regions))
        return records

    def fetch_incremental(
        self, state: CrawlState, max_records: int = 500
    ) -> tuple[list[RawRecord], CrawlState]:
        # Overpass doesn't support incremental well; do a full fetch
        records = self.fetch_batch(SearchParams(), limit=max_records)
        new_state = CrawlState(
            source_name=self.config.name,
            last_run_at=datetime.utcnow().isoformat(),
        )
        return records, new_state

    def _get_all_regions(self) -> dict[str, tuple]:
        """Merge hardcoded REGION_TILES with any config-defined regions.

        Regions defined in config.yaml under the ``regions`` key of the
        osm_overpass source are loaded from three possible locations:

        1. ``self.config.extra_config["regions"]`` — explicit extra_config dict
        2. ``self.config.model_extra["regions"]`` — Pydantic v2 extra fields
           captured by ``model_config = {"extra": "allow"}``
        3. ``self.config.regions`` — direct attribute (backwards compat)

        Config-defined regions *override* hardcoded defaults when the name
        matches, allowing the YAML to refine bounding boxes without code
        changes.
        """
        all_regions = dict(REGION_TILES)

        # Try multiple access paths for config-defined regions
        config_regions: dict | None = None

        # Path 1: explicit extra_config dict field
        if hasattr(self.config, "extra_config") and isinstance(self.config.extra_config, dict):
            config_regions = self.config.extra_config.get("regions")

        # Path 2: Pydantic v2 model_extra (captures undeclared YAML keys)
        if not config_regions and hasattr(self.config, "model_extra"):
            extras = self.config.model_extra or {}
            if isinstance(extras, dict):
                config_regions = extras.get("regions")

        # Path 3: direct attribute (e.g. if SourceConfig gains a regions field)
        if not config_regions:
            config_regions = getattr(self.config, "regions", None)

        if isinstance(config_regions, dict):
            for name, bbox in config_regions.items():
                if isinstance(bbox, (list, tuple)) and len(bbox) == 4:
                    all_regions[name] = tuple(bbox)

        return all_regions

    def _select_regions(self, params: SearchParams) -> list[tuple[str, tuple]]:
        """Select which region tiles to query based on search params."""
        all_regions = self._get_all_regions()

        # If a country filter is set, try to match to relevant regions
        country_filter = params.filters.get("countrycode", "").upper()
        if country_filter:
            region_map = {
                "US": ["north_america_east", "north_america_west", "north_america_full"],
                "CA": ["north_america_east", "north_america_west", "north_america_full"],
                "GB": ["europe_west", "uk_ireland"],
                "DE": ["europe_west"],
                "FR": ["europe_west"],
                "NO": ["europe_north", "scandinavia"],
                "SE": ["europe_north", "scandinavia"],
                "DK": ["scandinavia"],
                "FI": ["scandinavia"],
                "JP": ["east_asia", "japan_korea"],
                "CN": ["east_asia"],
                "KR": ["east_asia", "japan_korea"],
                "AU": ["oceania"],
                "NZ": ["oceania"],
                "BR": ["south_america", "south_america_ext"],
                "IN": ["south_asia", "india"],
                "IE": ["uk_ireland"],
                "ZA": ["africa_south", "africa_south_east"],
                "KE": ["africa_south_east"],
                "SA": ["middle_east", "middle_east_core"],
                "AE": ["middle_east", "middle_east_core"],
                "IL": ["middle_east", "middle_east_core"],
            }
            selected = region_map.get(country_filter, list(all_regions.keys()))
            return [(name, all_regions[name]) for name in selected if name in all_regions]

        # Default: all regions
        return list(all_regions.items())
