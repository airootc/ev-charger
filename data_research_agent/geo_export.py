"""GeoJSON export and map-provider abstraction for EV charging station data.

Converts cleaned CSV/JSON station data into GeoJSON FeatureCollections,
ready for any map platform (Unearth Insights, Mapbox, Leaflet, Google Maps, etc.).

Usage:
    from geo_export import GeoExporter, MapProviderConfig

    exporter = GeoExporter()
    geojson = exporter.from_csv("data/clean/ev_stations_global.csv")
    exporter.save(geojson, "data/geo/ev_stations.geojson")

    # Generate provider-specific config
    provider = MapProviderConfig("mapbox", api_key="pk.xxx")
    provider.generate_config("data/geo/ev_stations.geojson", "data/geo/mapbox_config.json")
"""

from __future__ import annotations

import csv
import json
import os
import tempfile
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Optional


# --- GeoJSON Models ---

class ConnectorCategory(str, Enum):
    """Standardized connector categories for map display."""
    LEVEL1 = "Level 1"
    LEVEL2 = "Level 2"
    DC_FAST = "DC Fast"
    TESLA = "Tesla Supercharger"
    UNKNOWN = "Unknown"


@dataclass
class GeoFeature:
    """A single GeoJSON Feature representing an EV charging station."""
    latitude: float
    longitude: float
    properties: dict[str, Any]

    def to_dict(self) -> dict:
        return {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [self.longitude, self.latitude],
            },
            "properties": self.properties,
        }


@dataclass
class GeoFeatureCollection:
    """A GeoJSON FeatureCollection."""
    features: list[GeoFeature] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        result = {
            "type": "FeatureCollection",
            "features": [f.to_dict() for f in self.features],
        }
        if self.metadata:
            result["metadata"] = self.metadata
        return result

    def __len__(self) -> int:
        return len(self.features)


# --- Property Schema ---

# Standard properties every station feature should have.
# Map providers use these for popups, filtering, clustering, and styling.
STATION_PROPERTIES = [
    "station_id",
    "station_name",
    "address",
    "city",
    "state",
    "country",
    "country_code",
    "postal_code",
    "network",
    "operator",
    "connector_types",
    "connector_category",  # derived: Level 1 / Level 2 / DC Fast / Tesla
    "num_ports",
    "num_level1_ports",
    "num_level2_ports",
    "num_dc_fast_ports",
    "total_ports",          # derived: sum of all port types
    "status",
    "access_type",
    "usage_cost",
    "phone",
    "facility_type",
    "owner_type",
    "data_provider",
    "source",
    "date_opened",
    "date_last_verified",
    "date_last_updated",
]


# --- GeoJSON Exporter ---

class GeoExporter:
    """Converts cleaned EV station data to GeoJSON."""

    def __init__(
        self,
        lat_field: str = "latitude",
        lng_field: str = "longitude",
        include_fields: list[str] | None = None,
        exclude_fields: list[str] | None = None,
    ):
        self.lat_field = lat_field
        self.lng_field = lng_field
        self.include_fields = include_fields
        self.exclude_fields = set(exclude_fields or [])

    def from_csv(self, filepath: str) -> GeoFeatureCollection:
        """Load a cleaned CSV and convert to GeoJSON FeatureCollection."""
        features = []
        with open(filepath, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                feature = self._row_to_feature(row)
                if feature:
                    features.append(feature)

        return GeoFeatureCollection(
            features=features,
            metadata={
                "source_file": filepath,
                "total_stations": len(features),
                "schema": "ev_charging_station",
            },
        )

    def from_json(self, filepath: str) -> GeoFeatureCollection:
        """Load cleaned JSON records and convert to GeoJSON."""
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        records = data if isinstance(data, list) else data.get("records", data.get("features", []))
        features = []
        for record in records:
            # Handle both flat dicts and {data: {...}} wrapped records
            row = record.get("data", record) if isinstance(record, dict) else record
            feature = self._row_to_feature(row)
            if feature:
                features.append(feature)

        return GeoFeatureCollection(
            features=features,
            metadata={
                "source_file": filepath,
                "total_stations": len(features),
                "schema": "ev_charging_station",
            },
        )

    def from_records(self, records: list[dict]) -> GeoFeatureCollection:
        """Convert a list of flat dicts to GeoJSON."""
        features = [f for r in records if (f := self._row_to_feature(r))]
        return GeoFeatureCollection(
            features=features,
            metadata={"total_stations": len(features), "schema": "ev_charging_station"},
        )

    def save(self, collection: GeoFeatureCollection, filepath: str) -> str:
        """Atomically write GeoJSON to disk."""
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=str(Path(filepath).parent), suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(collection.to_dict(), f, indent=2, default=str)
            os.replace(tmp_path, filepath)
        except Exception:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise
        return filepath

    def _row_to_feature(self, row: dict) -> GeoFeature | None:
        """Convert a single record to a GeoFeature."""
        try:
            lat = float(row.get(self.lat_field, 0))
            lng = float(row.get(self.lng_field, 0))
        except (ValueError, TypeError):
            return None

        if lat == 0 and lng == 0:
            return None
        if not (-90 <= lat <= 90 and -180 <= lng <= 180):
            return None

        props = self._build_properties(row)
        return GeoFeature(latitude=lat, longitude=lng, properties=props)

    def _build_properties(self, row: dict) -> dict[str, Any]:
        """Build the properties dict for a feature, including derived fields."""
        props: dict[str, Any] = {}

        # Select fields
        if self.include_fields:
            keys = self.include_fields
        else:
            keys = [k for k in row.keys() if k not in {self.lat_field, self.lng_field}]

        for key in keys:
            if key in self.exclude_fields:
                continue
            val = row.get(key)
            if val is not None and val != "":
                props[key] = val

        # Derive total_ports
        port_fields = ["num_ports", "num_level1_ports", "num_level2_ports", "num_dc_fast_ports"]
        total = 0
        for pf in port_fields:
            try:
                total += int(row.get(pf, 0) or 0)
            except (ValueError, TypeError):
                pass
        if total > 0:
            props["total_ports"] = total

        # Derive connector_category for map styling/filtering
        props["connector_category"] = self._classify_connector(row)

        return props

    @staticmethod
    def _classify_connector(row: dict) -> str:
        """Classify station by its highest-level connector for map layer styling."""
        try:
            dc_fast = int(row.get("num_dc_fast_ports", 0) or 0)
        except (ValueError, TypeError):
            dc_fast = 0
        try:
            level2 = int(row.get("num_level2_ports", 0) or 0)
        except (ValueError, TypeError):
            level2 = 0

        connector_str = str(row.get("connector_types", "") or "").lower()
        network_str = str(row.get("network", "") or "").lower()

        if "tesla" in network_str or "supercharger" in connector_str or "nacs" in connector_str:
            return ConnectorCategory.TESLA.value
        if dc_fast > 0 or "ccs" in connector_str or "chademo" in connector_str:
            return ConnectorCategory.DC_FAST.value
        if level2 > 0 or "j1772" in connector_str or "type 2" in connector_str:
            return ConnectorCategory.LEVEL2.value
        if int(row.get("num_level1_ports", 0) or 0) > 0:
            return ConnectorCategory.LEVEL1.value
        return ConnectorCategory.UNKNOWN.value


# --- Map Provider Abstraction ---

class MapProvider(str, Enum):
    """Supported map providers."""
    UNEARTH = "unearth"
    MAPBOX = "mapbox"
    LEAFLET = "leaflet"
    GOOGLE = "google"
    DECKGL = "deckgl"


@dataclass
class MapProviderConfig:
    """Generates provider-specific configuration for loading GeoJSON data."""
    provider: MapProvider
    api_key: str = ""
    style_url: str = ""

    def __post_init__(self):
        if isinstance(self.provider, str):
            self.provider = MapProvider(self.provider)

    def generate_config(self, geojson_path: str, output_path: str | None = None) -> dict:
        """Generate a map config dict for the chosen provider."""
        config = getattr(self, f"_config_{self.provider.value}")(geojson_path)
        if output_path:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w") as f:
                json.dump(config, f, indent=2)
        return config

    def _config_unearth(self, geojson_path: str) -> dict:
        """Unearth Insights config — GeoJSON layer with clustering and popups."""
        return {
            "provider": "unearth_insights",
            "version": "1.0",
            "note": "Unearth Insights API is coming soon. This config is ready for integration.",
            "data_source": {
                "type": "geojson",
                "path": geojson_path,
                "format": "FeatureCollection",
            },
            "layers": [
                {
                    "id": "ev-stations-cluster",
                    "type": "cluster",
                    "source": "ev-stations",
                    "cluster_radius": 50,
                    "cluster_properties": {
                        "total_ports": ["+", ["get", "total_ports"]],
                    },
                },
                {
                    "id": "ev-stations-points",
                    "type": "circle",
                    "source": "ev-stations",
                    "paint": {
                        "circle-color": [
                            "match", ["get", "connector_category"],
                            "DC Fast", "#e74c3c",
                            "Tesla Supercharger", "#c0392b",
                            "Level 2", "#3498db",
                            "Level 1", "#95a5a6",
                            "#7f8c8d",
                        ],
                        "circle-radius": 6,
                    },
                },
            ],
            "popup": {
                "fields": ["station_name", "address", "network", "connector_category", "total_ports", "status", "usage_cost"],
            },
            "filters": [
                {"field": "connector_category", "type": "categorical"},
                {"field": "network", "type": "categorical"},
                {"field": "access_type", "type": "categorical"},
                {"field": "country", "type": "categorical"},
            ],
        }

    def _config_mapbox(self, geojson_path: str) -> dict:
        """Mapbox GL JS config."""
        return {
            "provider": "mapbox",
            "version": "1.0",
            "access_token": self.api_key or "YOUR_MAPBOX_TOKEN",
            "style": self.style_url or "mapbox://styles/mapbox/dark-v11",
            "data_source": {
                "id": "ev-stations",
                "type": "geojson",
                "data": geojson_path,
                "cluster": True,
                "clusterMaxZoom": 14,
                "clusterRadius": 50,
            },
            "layers": [
                {
                    "id": "ev-clusters",
                    "type": "circle",
                    "source": "ev-stations",
                    "filter": ["has", "point_count"],
                    "paint": {
                        "circle-color": ["step", ["get", "point_count"], "#51bbd6", 10, "#f1f075", 50, "#f28cb1"],
                        "circle-radius": ["step", ["get", "point_count"], 20, 10, 30, 50, 40],
                    },
                },
                {
                    "id": "ev-cluster-count",
                    "type": "symbol",
                    "source": "ev-stations",
                    "filter": ["has", "point_count"],
                    "layout": {"text-field": "{point_count_abbreviated}", "text-size": 12},
                },
                {
                    "id": "ev-unclustered",
                    "type": "circle",
                    "source": "ev-stations",
                    "filter": ["!", ["has", "point_count"]],
                    "paint": {
                        "circle-color": [
                            "match", ["get", "connector_category"],
                            "DC Fast", "#e74c3c",
                            "Tesla Supercharger", "#c0392b",
                            "Level 2", "#3498db",
                            "Level 1", "#95a5a6",
                            "#7f8c8d",
                        ],
                        "circle-radius": 6,
                        "circle-stroke-width": 1,
                        "circle-stroke-color": "#fff",
                    },
                },
            ],
        }

    def _config_leaflet(self, geojson_path: str) -> dict:
        """Leaflet / OpenStreetMap config."""
        return {
            "provider": "leaflet",
            "version": "1.0",
            "tile_layer": {
                "url": "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
                "attribution": "OpenStreetMap contributors",
            },
            "data_source": {
                "type": "geojson",
                "path": geojson_path,
            },
            "marker_cluster": {
                "enabled": True,
                "max_cluster_radius": 50,
            },
            "style": {
                "color_field": "connector_category",
                "color_map": {
                    "DC Fast": "#e74c3c",
                    "Tesla Supercharger": "#c0392b",
                    "Level 2": "#3498db",
                    "Level 1": "#95a5a6",
                    "Unknown": "#7f8c8d",
                },
                "radius": 6,
            },
            "popup_fields": ["station_name", "address", "network", "connector_category", "total_ports", "status"],
        }

    def _config_google(self, geojson_path: str) -> dict:
        """Google Maps Platform config."""
        return {
            "provider": "google_maps",
            "version": "1.0",
            "api_key": self.api_key or "YOUR_GOOGLE_MAPS_KEY",
            "map_id": "YOUR_MAP_ID",
            "data_source": {
                "type": "geojson",
                "path": geojson_path,
            },
            "marker_clustering": {
                "enabled": True,
                "algorithm": "SuperClusterAlgorithm",
            },
            "style_rules": [
                {
                    "filter": {"connector_category": "DC Fast"},
                    "icon": {"color": "#e74c3c", "scale": 1.2},
                },
                {
                    "filter": {"connector_category": "Tesla Supercharger"},
                    "icon": {"color": "#c0392b", "scale": 1.2},
                },
                {
                    "filter": {"connector_category": "Level 2"},
                    "icon": {"color": "#3498db", "scale": 1.0},
                },
                {
                    "filter": {"connector_category": "Level 1"},
                    "icon": {"color": "#95a5a6", "scale": 0.8},
                },
            ],
            "info_window_fields": ["station_name", "address", "network", "connector_category", "total_ports", "status", "usage_cost"],
        }

    def _config_deckgl(self, geojson_path: str) -> dict:
        """Deck.gl config for large-scale data viz."""
        return {
            "provider": "deckgl",
            "version": "1.0",
            "mapbox_token": self.api_key or "YOUR_MAPBOX_TOKEN",
            "data_source": {
                "type": "geojson",
                "path": geojson_path,
            },
            "layers": [
                {
                    "type": "GeoJsonLayer",
                    "id": "ev-stations",
                    "data": geojson_path,
                    "filled": True,
                    "pointRadiusMinPixels": 3,
                    "pointRadiusMaxPixels": 12,
                    "getPointRadius": "total_ports * 2",
                    "getFillColor": {
                        "field": "connector_category",
                        "map": {
                            "DC Fast": [231, 76, 60],
                            "Tesla Supercharger": [192, 57, 43],
                            "Level 2": [52, 152, 219],
                            "Level 1": [149, 165, 166],
                            "Unknown": [127, 140, 141],
                        },
                    },
                    "pickable": True,
                },
            ],
            "tooltip_fields": ["station_name", "address", "network", "connector_category", "total_ports", "status"],
        }
