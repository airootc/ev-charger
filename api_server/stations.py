"""Stations API endpoint with spatial index and bbox queries."""

from __future__ import annotations

import gzip
import json
import logging
import math
import os
import threading
import time
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from . import db
from .auth import require_api_key
from .canary import generate_canaries_for_key, get_canary_features_in_bbox
from .config import settings
from .fingerprint import fingerprint_engine
from .rate_limit import check_rate_limit

logger = logging.getLogger("api_server.stations")

# Threshold (in bytes) above which we attempt streaming JSON parsing via
# ijson.  If ijson is not installed we fall back to the standard json.load.
_LARGE_FILE_THRESHOLD_BYTES: int = 100 * 1024 * 1024  # 100 MB

router = APIRouter(prefix="/api", tags=["stations"])


# ── Spatial Index ──

class GridSpatialIndex:
    """Simple grid-based spatial index for fast bbox queries.

    Divides the world into 1-degree cells. Each cell stores indices
    of features whose coordinates fall in that cell.
    """

    def __init__(self, features: list[dict], cell_size: float = 1.0):
        self.features = features
        self.cell_size = cell_size
        self.grid: dict[tuple[int, int], list[int]] = {}
        self._build()

    def _build(self):
        """Build the grid index."""
        for i, f in enumerate(self.features):
            coords = f.get("geometry", {}).get("coordinates", [])
            if len(coords) >= 2:
                lng, lat = coords[0], coords[1]
                cell = self._cell_key(lat, lng)
                self.grid.setdefault(cell, []).append(i)

    def _cell_key(self, lat: float, lng: float) -> tuple[int, int]:
        return (int(math.floor(lat / self.cell_size)), int(math.floor(lng / self.cell_size)))

    def query_bbox(self, west: float, south: float, east: float, north: float) -> list[dict]:
        """Get all features within a bounding box."""
        results = []
        min_row = int(math.floor(south / self.cell_size))
        max_row = int(math.floor(north / self.cell_size))
        min_col = int(math.floor(west / self.cell_size))
        max_col = int(math.floor(east / self.cell_size))

        seen = set()
        for row in range(min_row, max_row + 1):
            for col in range(min_col, max_col + 1):
                for idx in self.grid.get((row, col), []):
                    if idx not in seen:
                        seen.add(idx)
                        f = self.features[idx]
                        coords = f["geometry"]["coordinates"]
                        lng, lat = coords[0], coords[1]
                        if west <= lng <= east and south <= lat <= north:
                            results.append(f)

        return results

    @property
    def total_count(self) -> int:
        return len(self.features)


# ── Global state (loaded on startup) ──
#
# Both ``_index`` and ``_metadata`` are replaced atomically under
# ``_data_lock``.  Query functions take a *snapshot* of the reference at
# the start of each call so they never see a half-updated state.

_data_lock = threading.Lock()
_index: GridSpatialIndex | None = None
_metadata: dict = {}


def _load_features(path: str) -> list[dict]:
    """Load GeoJSON features from *path*.

    Supports both plain .geojson and gzip-compressed .geojson.gz files.
    If only the .gz version exists, it will be used automatically.

    For files larger than ``_LARGE_FILE_THRESHOLD_BYTES`` this attempts to
    use ``ijson`` for streaming parsing which keeps peak memory lower.
    Falls back to ``json.load`` when ijson is unavailable.
    """
    # Handle gzip-compressed files
    if path.endswith(".gz"):
        logger.info("Loading compressed file: %s", path)
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            geojson = json.load(fh)
        return geojson.get("features", [])

    # Auto-detect compressed version
    gz_path = path + ".gz"
    if not os.path.exists(path) and os.path.exists(gz_path):
        logger.info("Using compressed file: %s", gz_path)
        with gzip.open(gz_path, "rt", encoding="utf-8") as fh:
            geojson = json.load(fh)
        return geojson.get("features", [])

    file_size = os.path.getsize(path)

    if file_size > _LARGE_FILE_THRESHOLD_BYTES:
        try:
            import ijson  # type: ignore[import-untyped]

            logger.info(
                "File size %d MB exceeds threshold; using ijson streaming parser",
                file_size // (1024 * 1024),
            )
            features: list[dict] = []
            with open(path, "rb") as fh:
                for feature in ijson.items(fh, "features.item"):
                    features.append(feature)
            return features
        except ImportError:
            logger.warning(
                "ijson not installed; falling back to json.load for %d MB file",
                file_size // (1024 * 1024),
            )

    with open(path, "r", encoding="utf-8") as fh:
        geojson = json.load(fh)
    return geojson.get("features", [])


def load_geojson() -> int:
    """Load GeoJSON file and build spatial index.  Called on server startup.

    The new index and metadata are built in local variables and then swapped
    into the module-level references under ``_data_lock`` so that concurrent
    readers always see a consistent snapshot.
    """
    global _index, _metadata

    path = settings.GEOJSON_PATH
    if not Path(path).exists():
        # Try compressed version
        gz_path = path + ".gz"
        if Path(gz_path).exists():
            logger.info("Uncompressed file not found, using %s", gz_path)
            path = gz_path
        else:
            raise FileNotFoundError(f"GeoJSON file not found: {path} (also tried {gz_path})")

    features = _load_features(path)

    # Build index in a local variable -- no lock needed yet
    new_index = GridSpatialIndex(features)

    # Build metadata for filter options + counts for charts
    network_counts: dict[str, int] = {}
    country_counts: dict[str, int] = {}
    category_counts: dict[str, int] = {}

    for f in features:
        p = f.get("properties", {})
        net = p.get("network")
        cty = p.get("country")
        cat = p.get("connector_category")
        if net:
            network_counts[net] = network_counts.get(net, 0) + 1
        if cty:
            country_counts[cty] = country_counts.get(cty, 0) + 1
        if cat:
            category_counts[cat] = category_counts.get(cat, 0) + 1

    sorted_networks = sorted(network_counts.keys(), key=lambda k: -network_counts[k])
    sorted_countries = sorted(country_counts.keys(), key=lambda k: -country_counts[k])

    new_metadata: dict[str, Any] = {
        "total_stations": len(features),
        "networks": sorted_networks,
        "countries": sorted_countries,
        "connector_categories": sorted(category_counts.keys()),
        "network_counts": network_counts,
        "country_counts": country_counts,
        "connector_counts": category_counts,
    }

    # Atomic swap -- readers that already grabbed the old reference continue
    # using it safely; new readers pick up the new data.
    with _data_lock:
        _index = new_index
        _metadata = new_metadata

    return len(features)


def get_real_stations_summary() -> list[dict]:
    """Get a simplified list of real stations for canary generation."""
    # Snapshot: grab the reference once so a concurrent reload cannot
    # swap the object out from under us mid-iteration.
    index = _index
    if not index:
        return []
    stations = []
    for f in index.features[:200]:  # Sample up to 200
        p = f.get("properties", {})
        coords = f.get("geometry", {}).get("coordinates", [0, 0])
        stations.append({
            "station_name": p.get("station_name", ""),
            "address": p.get("address", ""),
            "city": p.get("city", ""),
            "country": p.get("country", ""),
            "country_code": p.get("country_code", ""),
            "connector_types": p.get("connector_types", ""),
            "connector_category": p.get("connector_category", ""),
            "latitude": coords[1],
            "longitude": coords[0],
        })
    return stations


# ── Endpoints ──

@router.get("/stations/meta")
async def stations_meta():
    """Get filter options and dataset metadata. No auth required."""
    # Snapshot the reference so a concurrent reload is safe.
    return _metadata


@router.get("/stations/overview")
async def stations_overview():
    """Return a sampled subset of all stations for the global map view.

    No auth required — returns at most ~2000 evenly-sampled stations so
    the map can show cluster dots at every zoom level without requiring
    the user to zoom in first.
    """
    index = _index
    if not index:
        return {"type": "FeatureCollection", "features": []}

    features = index.features
    total = len(features)

    # Sample evenly to cap at ~2000 features
    max_overview = 2000
    if total <= max_overview:
        sampled = features
    else:
        step = total / max_overview
        sampled = [features[int(i * step)] for i in range(max_overview)]

    # Strip heavy properties — keep only what clustering needs
    lite = []
    for f in sampled:
        p = f.get("properties", {})
        lite.append({
            "type": "Feature",
            "geometry": f["geometry"],
            "properties": {
                "station_id": p.get("station_id", ""),
                "station_name": p.get("station_name", ""),
                "connector_category": p.get("connector_category", ""),
                "network": p.get("network", ""),
                "country_code": p.get("country_code", ""),
                "total_ports": p.get("total_ports"),
            },
        })

    return {"type": "FeatureCollection", "features": lite}


@router.get("/stations")
async def get_stations(
    request: Request,
    key_record: dict = Depends(require_api_key),
    _rl: None = Depends(check_rate_limit),
    bbox: str = Query(..., description="west,south,east,north in degrees"),
    connector_category: Optional[str] = Query(None),
    network: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
    min_power_kw: Optional[float] = Query(None),
    search: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Get EV stations within a bounding box. Requires API key."""
    start_time = time.monotonic()

    # Snapshot the index reference once -- safe under Python's GIL and
    # immune to a concurrent hot-reload swapping the module-level variable.
    index = _index
    if not index:
        raise HTTPException(status_code=503, detail="Data not loaded yet")

    # Parse bbox
    try:
        parts = [float(x.strip()) for x in bbox.split(",")]
        if len(parts) != 4:
            raise ValueError()
        west, south, east, north = parts
    except (ValueError, IndexError):
        raise HTTPException(status_code=400, detail="Invalid bbox format. Use: west,south,east,north")

    # Validate bbox
    if not (-180 <= west <= 180 and -180 <= east <= 180 and -90 <= south <= 90 and -90 <= north <= 90):
        raise HTTPException(status_code=400, detail="Bbox coordinates out of range")

    # Check bbox area
    area = abs(east - west) * abs(north - south)
    if area > settings.MAX_BBOX_AREA_DEGREES:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Bbox area too large",
                "max_area_degrees": settings.MAX_BBOX_AREA_DEGREES,
                "requested_area": round(area, 1),
                "hint": "Zoom in or use a smaller bounding box",
            },
        )

    # Bot detection
    client_id = key_record["id"]
    suspicion_score = fingerprint_engine.score_request(request, client_id, bbox)
    fp_hash = fingerprint_engine.compute_fingerprint_hash(request)

    if suspicion_score >= 90:
        raise HTTPException(status_code=403, detail="Request blocked due to suspicious activity")

    # Query spatial index (using snapshot)
    features = index.query_bbox(west, south, east, north)

    # Apply filters
    if connector_category:
        features = [f for f in features if f["properties"].get("connector_category") == connector_category]
    if network:
        features = [f for f in features if f["properties"].get("network") == network]
    if country:
        features = [f for f in features if f["properties"].get("country") == country]
    if min_power_kw:
        features = [
            f for f in features
            if (f["properties"].get("power_kw") or 0) >= min_power_kw
        ]
    if search:
        search_lower = search.lower()
        features = [
            f for f in features
            if search_lower in (f["properties"].get("station_name", "") or "").lower()
            or search_lower in (f["properties"].get("address", "") or "").lower()
        ]

    total_in_bbox = len(features)

    # Degrade data for suspicious requests
    if suspicion_score >= 70:
        # Reduce coordinate precision and omit some fields
        degraded = []
        for f in features:
            coords = f["geometry"]["coordinates"]
            df = {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [round(coords[0], 2), round(coords[1], 2)]},
                "properties": {
                    "station_name": f["properties"].get("station_name"),
                    "connector_category": f["properties"].get("connector_category"),
                    "country": f["properties"].get("country"),
                },
            }
            degraded.append(df)
        features = degraded

    # Inject canary points
    canary_features = get_canary_features_in_bbox(key_record["id"], west, south, east, north)
    if not canary_features:
        # Generate canaries on first request
        real_stations = get_real_stations_summary()
        generate_canaries_for_key(key_record["id"], real_stations)
        canary_features = get_canary_features_in_bbox(key_record["id"], west, south, east, north)

    features = features + canary_features

    # Paginate
    paginated = features[offset: offset + limit]
    has_more = (offset + limit) < len(features)

    elapsed_ms = (time.monotonic() - start_time) * 1000

    # Log request
    db.log_request(
        api_key_id=key_record["id"],
        ip=request.client.host if request.client else "unknown",
        endpoint="/api/stations",
        bbox=bbox,
        user_agent=request.headers.get("user-agent"),
        referer=request.headers.get("referer"),
        features_returned=len(paginated),
        response_time_ms=elapsed_ms,
        fingerprint_hash=fp_hash,
        suspicion_score=suspicion_score,
    )

    return {
        "type": "FeatureCollection",
        "features": paginated,
        "metadata": {
            "total_in_bbox": total_in_bbox,
            "returned": len(paginated),
            "offset": offset,
            "has_more": has_more,
            "bbox": [west, south, east, north],
            "response_time_ms": round(elapsed_ms, 1),
        },
    }
