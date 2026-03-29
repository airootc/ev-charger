"""Honeypot/canary data injection for leak detection."""

from __future__ import annotations

import hashlib
import json
import logging
import random
import secrets
import threading
from typing import Any

from . import db
from .config import settings

logger = logging.getLogger("api_server.canary")

# Per-key locks prevent two concurrent requests for the same API key from
# double-generating canaries.  The outer ``_canary_gen_meta_lock`` guards
# the dictionary of per-key locks itself.
_canary_gen_locks: dict[str, threading.Lock] = {}
_canary_gen_meta_lock = threading.Lock()


def _get_key_lock(api_key_id: str) -> threading.Lock:
    """Return (or create) a per-key lock for canary generation."""
    with _canary_gen_meta_lock:
        if api_key_id not in _canary_gen_locks:
            _canary_gen_locks[api_key_id] = threading.Lock()
        return _canary_gen_locks[api_key_id]

# ── Name-generation building blocks ──
# These components are mixed to produce realistic-looking station names that
# blend with genuine data.  A seeded RNG per API key ensures consistent
# canaries for the same key while varying across keys.

_STREET_NAMES: list[str] = [
    "Oak", "Elm", "Maple", "Cedar", "Pine", "Birch", "Walnut", "Spruce",
    "Willow", "Ash", "Poplar", "Chestnut", "Magnolia", "Sycamore", "Laurel",
    "Hickory", "Alder", "Cypress", "Hazel", "Linden",
]

_STREET_SUFFIXES: list[str] = [
    "Street", "Avenue", "Boulevard", "Drive", "Road", "Lane", "Way", "Place",
]

_CITY_NAMES: list[str] = [
    "Riverside", "Lakewood", "Fairview", "Springfield", "Greenville",
    "Madison", "Franklin", "Clinton", "Georgetown", "Burlington",
    "Ashland", "Brookfield", "Chester", "Dayton", "Eastport",
    "Glendale", "Hampton", "Kingston", "Milton", "Newport",
]

_STATION_PATTERNS: list[str] = [
    "{street} {suffix} Charging Hub",
    "{city} EV Station",
    "{city} {street} Supercharger",
    "{street} {suffix} EV Center",
    "{city} Fast Charge Plaza",
    "{street} Power Station",
    "{city} Green Energy Hub",
    "{street} {suffix} Charge Point",
]

_OPERATOR_PREFIXES: list[str] = [
    "Green", "Eco", "Volt", "Power", "Charge", "Electra", "Ion", "Amp",
    "Spark", "Flux", "Bolt", "Watt", "Surge", "Grid", "Current",
]

_OPERATOR_SUFFIXES: list[str] = [
    "Energy", "Power", "Charge", "Networks", "Systems", "Grid", "Go",
    "Stream", "Mobility", "Connect", "Drive", "Link", "Hub", "Wave",
]

# Number of distinct operator names to generate per key
_OPERATORS_PER_KEY = 8

# Prefix for canary station IDs (matches the real-data convention)
_STATION_ID_PREFIX = "st_"

# Bytes of randomness in generated station IDs (produces 12 hex chars)
_STATION_ID_BYTES = 6


def _seed_rng_for_key(api_key_id: str) -> random.Random:
    """Return a deterministic RNG seeded from *api_key_id*.

    Using a per-key seed means the same key always gets the same canary
    names/operators, but different keys get different ones.
    """
    digest = hashlib.sha256(api_key_id.encode()).digest()
    seed = int.from_bytes(digest[:8], "big")
    return random.Random(seed)


def _generate_operators(rng: random.Random) -> list[str]:
    """Generate a list of realistic operator names using *rng*."""
    operators: list[str] = []
    prefixes = list(_OPERATOR_PREFIXES)
    suffixes = list(_OPERATOR_SUFFIXES)
    rng.shuffle(prefixes)
    rng.shuffle(suffixes)
    for i in range(_OPERATORS_PER_KEY):
        prefix = prefixes[i % len(prefixes)]
        suffix = suffixes[i % len(suffixes)]
        operators.append(f"{prefix}{suffix}")
    return operators


def _generate_station_name(rng: random.Random) -> str:
    """Generate a single realistic-looking station name using *rng*."""
    pattern = rng.choice(_STATION_PATTERNS)
    return pattern.format(
        street=rng.choice(_STREET_NAMES),
        suffix=rng.choice(_STREET_SUFFIXES),
        city=rng.choice(_CITY_NAMES),
    )


def generate_canaries_for_key(api_key_id: str, real_stations: list[dict]) -> list[dict]:
    """Generate unique canary stations for an API key.

    Creates fake stations near real stations with unique identifiers
    tied to this specific API key.  If canaries already exist for this
    key, returns the existing ones.

    Thread-safety: a per-key lock prevents two concurrent requests from
    double-generating canaries for the same key (double-checked locking
    pattern).  Additionally, ``db.store_canary`` uses INSERT OR IGNORE
    so even in a multi-process deployment duplicate rows are silently
    skipped.

    The station names, IDs, and operator names are derived from a
    per-key seeded RNG so they look realistic and are consistent across
    calls for the same key, yet distinct between keys.
    """
    # Fast path (no lock) -- canaries already generated
    existing = db.get_canaries_for_key(api_key_id)
    if existing:
        return existing

    # Slow path -- acquire a per-key lock and double-check
    lock = _get_key_lock(api_key_id)
    with lock:
        existing = db.get_canaries_for_key(api_key_id)
        if existing:
            return existing

        return _generate_canaries_locked(api_key_id, real_stations)


def _generate_canaries_locked(api_key_id: str, real_stations: list[dict]) -> list[dict]:
    """Internal helper: generate canaries while the per-key lock is held."""
    if not real_stations:
        return []

    num_canaries = min(
        max(settings.CANARY_MIN_PER_KEY, int(len(real_stations) * settings.CANARY_RATIO)),
        settings.CANARY_MAX_PER_KEY,
    )

    rng = _seed_rng_for_key(api_key_id)
    operators = _generate_operators(rng)

    # Pick random real stations as base for canaries
    bases = rng.sample(real_stations, min(num_canaries, len(real_stations)))
    canaries: list[dict] = []

    for base in bases:
        # Offset coordinates 50-200 meters (roughly 0.0005-0.002 degrees)
        lat_offset = rng.uniform(0.0005, 0.002) * rng.choice([-1, 1])
        lng_offset = rng.uniform(0.0005, 0.002) * rng.choice([-1, 1])

        canary_name = _generate_station_name(rng)
        canary_lat = base["latitude"] + lat_offset
        canary_lng = base["longitude"] + lng_offset
        station_id = f"{_STATION_ID_PREFIX}{secrets.token_hex(_STATION_ID_BYTES)}"

        props: dict[str, Any] = {
            "station_id": station_id,
            "station_name": canary_name,
            "address": base.get("address", ""),
            "city": base.get("city", ""),
            "country": base.get("country", ""),
            "country_code": base.get("country_code", ""),
            "network": rng.choice(operators),
            "operator": rng.choice(operators),
            "connector_types": base.get("connector_types", "CCS2"),
            "connector_category": base.get("connector_category", "DC Fast"),
            "num_ports": rng.randint(2, 8),
            "total_ports": rng.randint(2, 8),
            "power_kw": rng.choice([50, 100, 150, 200, 250]),
            "status": "Operational",
            "access_type": "Public",
            "_canary_key": api_key_id,  # Internal marker, stripped before serving
        }

        db.store_canary(api_key_id, canary_name, canary_lat, canary_lng, props)
        canaries.append({
            "api_key_id": api_key_id,
            "station_name": canary_name,
            "latitude": canary_lat,
            "longitude": canary_lng,
            "properties": json.dumps(props),
        })

    return canaries


def get_canary_features_in_bbox(
    api_key_id: str,
    west: float, south: float, east: float, north: float,
) -> list[dict]:
    """Get canary GeoJSON features within a bounding box for a specific key."""
    canaries = db.get_canaries_for_key(api_key_id)
    features: list[dict] = []

    for c in canaries:
        lat = c["latitude"]
        lng = c["longitude"]
        if south <= lat <= north and west <= lng <= east:
            props = json.loads(c["properties"]) if isinstance(c["properties"], str) else c["properties"]
            # Remove internal marker
            props.pop("_canary_key", None)

            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lng, lat],
                },
                "properties": props,
            })

    return features


def check_leak(features: list[dict]) -> list[dict]:
    """Check a list of features for canary matches. Returns leak sources."""
    matches: list[dict] = []
    for f in features:
        coords = f.get("geometry", {}).get("coordinates", [])
        if len(coords) >= 2:
            lng, lat = coords[0], coords[1]
            results = db.check_canary_match(
                f.get("properties", {}).get("station_name", ""),
                lat, lng,
            )
            for r in results:
                matches.append({
                    "canary_station": r["station_name"],
                    "api_key_id": r["api_key_id"],
                    "api_key_name": r.get("key_name", "unknown"),
                    "injected_at": r["injected_at"],
                })
    return matches
