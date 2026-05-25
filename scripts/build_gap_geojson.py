#!/usr/bin/env python3
"""
Build ev_gap.geojson from ev_ownership.json + ev_stations.geojson.gz

Reads ownership/gap data, geocodes each area to a lat/lng centroid,
and outputs a GeoJSON FeatureCollection for the frontend map layer.

Centroid sources:
  - US ZIPs: computed from station coordinates in ev_stations.geojson.gz
  - Countries: computed from station coordinates + hardcoded fallback table
  - UK local authorities: approximate match from station postcodes
"""

import gzip
import json
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
FRONTEND_DATA = SCRIPT_DIR.parent / "frontend" / "data"
OWNERSHIP_PATH = FRONTEND_DATA / "ev_ownership.json"
STATIONS_PATH = FRONTEND_DATA / "ev_stations.geojson.gz"
OUTPUT_PATH = FRONTEND_DATA / "ev_gap.geojson"
ZIP_CENTROIDS_CSV = SCRIPT_DIR / "us_zip_centroids.csv"

# Hardcoded country centroids (ISO 2-letter → [lat, lng])
# Used as fallback when no stations exist for a country
COUNTRY_CENTROIDS = {
    "US": [39.8283, -98.5795],
    "GB": [55.3781, -1.4360],
    "DE": [51.1657, 10.4515],
    "FR": [46.2276, 2.2137],
    "NL": [52.1326, 5.2913],
    "NO": [60.4720, 8.4689],
    "SE": [60.1282, 18.6435],
    "DK": [56.2639, 9.5018],
    "FI": [61.9241, 25.7482],
    "IT": [41.8719, 12.5674],
    "ES": [40.4637, -3.7492],
    "PT": [39.3999, -8.2245],
    "AT": [47.5162, 14.5501],
    "CH": [46.8182, 8.2275],
    "BE": [50.5039, 4.4699],
    "PL": [51.9194, 19.1451],
    "CZ": [49.8175, 15.4730],
    "IE": [53.1424, -7.6921],
    "GR": [39.0742, 21.8243],
    "HU": [47.1625, 19.5033],
    "RO": [45.9432, 24.9668],
    "BG": [42.7339, 25.4858],
    "HR": [45.1000, 15.2000],
    "SK": [48.6690, 19.6990],
    "SI": [46.1512, 14.9955],
    "LT": [55.1694, 23.8813],
    "LV": [56.8796, 24.6032],
    "EE": [58.5953, 25.0136],
    "CY": [35.1264, 33.4299],
    "MT": [35.9375, 14.3754],
    "LU": [49.8153, 6.1296],
    "IS": [64.9631, -19.0208],
    "CN": [35.8617, 104.1954],
    "JP": [36.2048, 138.2529],
    "KR": [35.9078, 127.7669],
    "IN": [20.5937, 78.9629],
    "AU": [-25.2744, 133.7751],
    "NZ": [-40.9006, 174.8860],
    "CA": [56.1304, -106.3468],
    "MX": [23.6345, -102.5528],
    "BR": [-14.2350, -51.9253],
    "CL": [-35.6751, -71.5430],
    "CO": [4.5709, -74.2973],
    "AR": [-38.4161, -63.6167],
    "ZA": [-30.5595, 22.9375],
    "TH": [15.8700, 100.9925],
    "MY": [4.2105, 101.9758],
    "ID": [-0.7893, 113.9213],
    "PH": [12.8797, 121.7740],
    "VN": [14.0583, 108.2772],
    "TR": [38.9637, 35.2433],
    "IL": [31.0461, 34.8516],
    "AE": [23.4241, 53.8478],
    "SA": [23.8859, 45.0792],
    "EG": [26.8206, 30.8025],
    "KE": [-0.0236, 37.9062],
    "NG": [9.0820, 8.6753],
    "TW": [23.6978, 120.9605],
    "SG": [1.3521, 103.8198],
    "HK": [22.3193, 114.1694],
    "RU": [61.5240, 105.3188],
    "UA": [48.3794, 31.1656],
}

# IEA aggregate regions to skip (not real countries)
SKIP_AREAS = {
    "World", "Europe", "USA", "Other Europe",
    "Rest of the world", "Advanced Economies",
}


def load_zip_centroids_csv():
    """Load US ZIP centroids from the bundled CSV file."""
    import csv
    centroids = {}
    if not ZIP_CENTROIDS_CSV.exists():
        log.warning("ZIP centroids CSV not found: %s", ZIP_CENTROIDS_CSV)
        return centroids
    with open(ZIP_CENTROIDS_CSV) as f:
        reader = csv.DictReader(f)
        for row in reader:
            z = row.get("ZIP", "").strip()
            lat = row.get("LAT", "").strip()
            lng = row.get("LNG", "").strip()
            if z and lat and lng:
                try:
                    centroids[z] = [float(lat), float(lng)]
                except ValueError:
                    pass
    log.info("  ZIP centroids from CSV: %d", len(centroids))
    return centroids


def load_station_centroids():
    """
    Read the compressed station GeoJSON and build centroid lookups:
      - zip_centroids: { "98034": [lat, lng], ... }  (CSV + station-derived)
      - country_centroids: { "DE": [lat, lng], ... }
    """
    # Start with CSV-based ZIP centroids (33K+ US ZIPs)
    zip_centroids = load_zip_centroids_csv()

    log.info("Loading station centroids from %s ...", STATIONS_PATH.name)

    zip_acc = {}    # zip -> {lat_sum, lng_sum, count}
    cc_acc = {}     # country_code -> {lat_sum, lng_sum, count}

    with gzip.open(STATIONS_PATH, "rt", encoding="utf-8") as f:
        data = json.load(f)

    for feat in data.get("features", []):
        coords = feat.get("geometry", {}).get("coordinates", [])
        if len(coords) < 2:
            continue
        lng, lat = coords[0], coords[1]
        props = feat.get("properties", {})
        postal = props.get("postal_code", "") or ""
        cc = props.get("country_code", "") or ""

        # US ZIP centroids (station-derived, override CSV for better precision)
        if postal and cc in ("US",):
            z = postal[:5]
            if z not in zip_acc:
                zip_acc[z] = [0.0, 0.0, 0]
            zip_acc[z][0] += lat
            zip_acc[z][1] += lng
            zip_acc[z][2] += 1

        # Country centroids from station positions
        if cc and len(cc) == 2:
            if cc not in cc_acc:
                cc_acc[cc] = [0.0, 0.0, 0]
            cc_acc[cc][0] += lat
            cc_acc[cc][1] += lng
            cc_acc[cc][2] += 1

    # Station-derived ZIP centroids override CSV-based ones
    for z, (lat_s, lng_s, n) in zip_acc.items():
        zip_centroids[z] = [round(lat_s / n, 5), round(lng_s / n, 5)]

    station_cc = {}
    for cc, (lat_s, lng_s, n) in cc_acc.items():
        station_cc[cc] = [round(lat_s / n, 5), round(lng_s / n, 5)]

    # Merge station-derived country centroids with hardcoded fallbacks
    merged_cc = dict(COUNTRY_CENTROIDS)
    merged_cc.update(station_cc)  # station-derived overrides hardcoded

    log.info("  ZIP centroids total: %d", len(zip_centroids))
    log.info("  Country centroids: %d (%d from stations, %d hardcoded fallback)",
             len(merged_cc), len(station_cc), len(COUNTRY_CENTROIDS))

    return zip_centroids, merged_cc


def build_geojson(ownership, zip_centroids, country_centroids):
    """Convert ownership areas to GeoJSON features with coordinates."""
    features = []
    skipped = 0
    by_type = {"zip": 0, "country": 0, "local_authority": 0, "state": 0}

    for area in ownership.get("areas", []):
        area_type = area.get("area_type", "")
        area_code = area.get("area_code", "")
        area_name = area.get("area_name", "")
        cc = area.get("country_code", "")

        # Skip aggregate regions
        if area_name in SKIP_AREAS or area_code in SKIP_AREAS:
            skipped += 1
            continue

        # Determine coordinates
        lat, lng = None, None

        if area_type == "zip":
            centroid = zip_centroids.get(area_code)
            if centroid:
                lat, lng = centroid
        elif area_type == "country":
            # Try country_code first, then area_code as ISO code
            centroid = country_centroids.get(cc)
            if not centroid and len(area_code) == 2:
                centroid = country_centroids.get(area_code)
            if centroid:
                lat, lng = centroid
        elif area_type == "local_authority":
            # UK local authorities — use country centroid as approximate
            # (individual LA geocoding would require a separate lookup)
            centroid = country_centroids.get(cc or "GB")
            if centroid:
                lat, lng = centroid
                # Skip UK LAs for now — they'd all cluster at the same point
                skipped += 1
                continue
        elif area_type == "state":
            centroid = country_centroids.get(cc or "US")
            if centroid:
                lat, lng = centroid

        if lat is None or lng is None:
            skipped += 1
            continue

        by_type[area_type] = by_type.get(area_type, 0) + 1

        ev_count = area.get("ev_count", 0) or 0
        stations = area.get("stations", 0) or 0
        gap_score = area.get("gap_score")
        gap_category = area.get("gap_category", "no_data")
        evs_per_station = area.get("evs_per_station")

        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [lng, lat],
            },
            "properties": {
                "area_type": area_type,
                "area_code": area_code,
                "area_name": area_name,
                "country_code": cc,
                "state": area.get("state", ""),
                "ev_count": ev_count,
                "bev_count": area.get("bev_count", 0) or 0,
                "phev_count": area.get("phev_count", 0) or 0,
                "stations": stations,
                "ports": area.get("ports", 0) or 0,
                "dc_fast_stations": area.get("dc_fast_stations", 0) or 0,
                "evs_per_station": evs_per_station,
                "gap_score": gap_score,
                "gap_category": gap_category,
                "source": area.get("source", ""),
                "year": area.get("year", ""),
                # Pre-compute display radius (log scale, clamped)
                "display_radius": _display_radius(ev_count, area_type),
            },
        }
        features.append(feature)

    log.info("  Features by type: %s", by_type)
    log.info("  Skipped (no coords / aggregate): %d", skipped)

    return {
        "type": "FeatureCollection",
        "properties": {
            "generated_at": ownership.get("generated_at", ""),
            "benchmark_evs_per_charger": ownership.get("benchmark_evs_per_charger", 20),
            "total_evs_tracked": ownership.get("total_evs_tracked", 0),
        },
        "features": features,
    }


def _display_radius(ev_count, area_type):
    """Compute a display radius for the circle marker (in pixels at zoom ~10)."""
    import math
    if ev_count <= 0:
        return 4
    if area_type == "country":
        # Countries get larger circles
        return min(max(int(math.log10(max(ev_count, 1)) * 8), 10), 50)
    else:
        # ZIP-level
        return min(max(int(math.log10(max(ev_count, 1)) * 4), 4), 25)


def main():
    log.info("Building gap analysis GeoJSON...")

    if not OWNERSHIP_PATH.exists():
        log.error("Missing %s — run collect_ev_ownership.py first", OWNERSHIP_PATH)
        sys.exit(1)
    if not STATIONS_PATH.exists():
        log.error("Missing %s", STATIONS_PATH)
        sys.exit(1)

    # Load inputs
    with open(OWNERSHIP_PATH) as f:
        ownership = json.load(f)
    log.info("Loaded %d ownership areas", len(ownership.get("areas", [])))

    zip_centroids, cc_centroids = load_station_centroids()

    # Build GeoJSON
    geojson = build_geojson(ownership, zip_centroids, cc_centroids)
    n_features = len(geojson["features"])

    # Write output
    with open(OUTPUT_PATH, "w") as f:
        json.dump(geojson, f, separators=(",", ":"))

    size_mb = OUTPUT_PATH.stat().st_size / 1024 / 1024
    log.info("\nWrote %d features to %s (%.1f MB)", n_features, OUTPUT_PATH.name, size_mb)

    # Summary by gap category
    cats = {}
    for feat in geojson["features"]:
        cat = feat["properties"]["gap_category"]
        cats[cat] = cats.get(cat, 0) + 1
    log.info("\nGap categories:")
    for cat in ["critical", "underserved", "adequate", "well_served", "no_data"]:
        log.info("  %-15s %d", cat, cats.get(cat, 0))


if __name__ == "__main__":
    main()
