"""Data quality report endpoint for EV charging station data."""

from __future__ import annotations

import hashlib
import logging
import math
import time
import threading
from typing import Any

from fastapi import APIRouter, Depends

from .auth import require_admin
from .stations import _index, _metadata

logger = logging.getLogger("api_server.quality")

router = APIRouter(prefix="/api/admin", tags=["quality"], dependencies=[Depends(require_admin)])

# ── Cache ──

_cache_lock = threading.Lock()
_cached_report: dict[str, Any] | None = None
_cached_data_hash: str | None = None


def _compute_data_hash() -> str:
    """Compute a lightweight hash of the current dataset to detect changes."""
    from .stations import _index as idx, _metadata as meta
    if idx is None:
        return ""
    # Hash based on total count and a sample of coordinates for change detection
    parts = [str(meta.get("total_stations", 0))]
    step = max(1, len(idx.features) // 50)
    for i in range(0, len(idx.features), step):
        coords = idx.features[i].get("geometry", {}).get("coordinates", [])
        if coords:
            parts.append(f"{coords[0]:.6f},{coords[1]:.6f}")
    return hashlib.md5("|".join(parts).encode()).hexdigest()


# ── Haversine ──

_EARTH_RADIUS_M = 6_371_000.0


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return distance in meters between two WGS-84 points."""
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return 2 * _EARTH_RADIUS_M * math.asin(math.sqrt(a))


# ── String similarity ──

def _normalise(name: str | None) -> str:
    """Lowercase, strip whitespace and common suffixes for comparison."""
    if not name:
        return ""
    return name.lower().strip()


def _similar(a: str, b: str) -> bool:
    """Simple similarity check: one name contains the other, or high token overlap."""
    if not a or not b:
        return False
    if a == b:
        return True
    # Containment check
    if a in b or b in a:
        return True
    # Token overlap (Jaccard on words)
    tokens_a = set(a.split())
    tokens_b = set(b.split())
    if not tokens_a or not tokens_b:
        return False
    intersection = tokens_a & tokens_b
    union = tokens_a | tokens_b
    return len(intersection) / len(union) >= 0.6


# ── Report builder ──

_COMPLETENESS_FIELDS = ("network", "connector_category", "power_kw", "address")
_DUPLICATE_RADIUS_M = 50.0
_LOW_COVERAGE_THRESHOLD = 10


def _build_report() -> dict[str, Any]:
    """Build the full quality report from the spatial index."""
    from .stations import _index as idx, _metadata as meta

    if idx is None or not idx.features:
        return {
            "summary": {"total_stations": 0, "generated_at": time.time()},
            "duplicates": [],
            "coverage_gaps": [],
            "completeness": {},
        }

    features = idx.features
    total = len(features)
    start = time.monotonic()

    # ── Completeness ──
    missing_counts: dict[str, int] = {field: 0 for field in _COMPLETENESS_FIELDS}
    for f in features:
        props = f.get("properties", {})
        for field in _COMPLETENESS_FIELDS:
            val = props.get(field)
            if val is None or val == "":
                missing_counts[field] += 1

    completeness = {
        field: {
            "missing_count": missing_counts[field],
            "missing_pct": round(missing_counts[field] / total * 100, 2),
            "present_pct": round((total - missing_counts[field]) / total * 100, 2),
        }
        for field in _COMPLETENESS_FIELDS
    }

    # ── Coverage gaps ──
    country_counts: dict[str, int] = meta.get("country_counts", {})
    coverage_gaps = [
        {"country": country, "station_count": count}
        for country, count in sorted(country_counts.items(), key=lambda x: x[1])
        if count < _LOW_COVERAGE_THRESHOLD
    ]

    # ── Duplicates (grid-cell-local scan for efficiency) ──
    duplicates: list[dict[str, Any]] = []
    seen_pairs: set[tuple[int, int]] = set()

    for cell_indices in idx.grid.values():
        if len(cell_indices) < 2:
            continue
        # Only check pairs within the same cell (and avoid O(N^2) on huge cells)
        check_limit = min(len(cell_indices), 200)
        for i in range(check_limit):
            idx_a = cell_indices[i]
            fa = features[idx_a]
            coords_a = fa.get("geometry", {}).get("coordinates", [])
            if len(coords_a) < 2:
                continue
            name_a = _normalise(fa.get("properties", {}).get("station_name"))
            lat_a, lon_a = coords_a[1], coords_a[0]

            for j in range(i + 1, check_limit):
                idx_b = cell_indices[j]
                pair_key = (min(idx_a, idx_b), max(idx_a, idx_b))
                if pair_key in seen_pairs:
                    continue

                fb = features[idx_b]
                coords_b = fb.get("geometry", {}).get("coordinates", [])
                if len(coords_b) < 2:
                    continue
                lat_b, lon_b = coords_b[1], coords_b[0]

                # Quick lat/lon pre-filter (~50m is roughly 0.0005 degrees)
                if abs(lat_a - lat_b) > 0.001 or abs(lon_a - lon_b) > 0.001:
                    continue

                dist = _haversine_m(lat_a, lon_a, lat_b, lon_b)
                if dist > _DUPLICATE_RADIUS_M:
                    continue

                name_b = _normalise(fb.get("properties", {}).get("station_name"))
                if _similar(name_a, name_b):
                    seen_pairs.add(pair_key)
                    duplicates.append({
                        "station_a": {
                            "index": idx_a,
                            "name": fa.get("properties", {}).get("station_name"),
                            "coordinates": coords_a,
                        },
                        "station_b": {
                            "index": idx_b,
                            "name": fb.get("properties", {}).get("station_name"),
                            "coordinates": coords_b,
                        },
                        "distance_m": round(dist, 1),
                    })

    elapsed_ms = round((time.monotonic() - start) * 1000, 1)

    return {
        "summary": {
            "total_stations": total,
            "duplicate_pairs": len(duplicates),
            "countries_with_low_coverage": len(coverage_gaps),
            "build_time_ms": elapsed_ms,
            "generated_at": time.time(),
        },
        "duplicates": duplicates[:500],  # Cap output size
        "coverage_gaps": coverage_gaps,
        "completeness": completeness,
    }


# ── Endpoint ──

@router.get("/quality-report")
async def quality_report():
    """Generate a data quality report for the loaded station dataset.

    The report is cached and only rebuilt when the underlying data changes.
    """
    global _cached_report, _cached_data_hash

    current_hash = _compute_data_hash()

    with _cache_lock:
        if _cached_report is not None and _cached_data_hash == current_hash:
            return _cached_report

    report = _build_report()

    with _cache_lock:
        _cached_report = report
        _cached_data_hash = current_hash

    logger.info(
        "Quality report built: %d stations, %d duplicate pairs, %.1f ms",
        report["summary"]["total_stations"],
        report["summary"]["duplicate_pairs"],
        report["summary"]["build_time_ms"],
    )
    return report
