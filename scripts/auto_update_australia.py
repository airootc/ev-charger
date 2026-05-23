#!/usr/bin/env python3
"""Automated Australia EV charging station data updater.

Collects from all Australian sources, deduplicates, merges into the main
GeoJSON dataset, and optionally pushes to GitHub (triggering Render redeploy).

Usage:
    # Collect + merge only
    python scripts/auto_update_australia.py

    # Collect + merge + push to GitHub + restart server
    python scripts/auto_update_australia.py --push

    # Dry run (collect + dedup, don't modify GeoJSON)
    python scripts/auto_update_australia.py --dry-run
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import logging
import math
import os
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import requests

# ── Config ──

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data_research_agent" / "data" / "raw" / "batch_australia_operators"
GEOJSON_PATH = PROJECT_ROOT / "frontend" / "data" / "ev_stations.geojson"
GZ_PATH = PROJECT_ROOT / "frontend" / "data" / "ev_stations.geojson.gz"
LOG_DIR = PROJECT_ROOT / "scripts" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "australia_update.log"),
    ],
)
logger = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "UnearthAI-DataResearch/1.0 (EV Station Auto-Update)"
})

GRID_SIZE = 0.001  # ~111m for dedup proximity


# ═══════════════════════════════════════════════
#  COLLECTORS — one function per data source
# ═══════════════════════════════════════════════

def collect_nsw_tfnsw() -> list[dict]:
    """Transport for NSW — EV Charging Stations (CSV)."""
    url = "https://opendata.transport.nsw.gov.au/data/dataset/be1c4de4-4517-4bd0-8a09-2965ddfc7179/resource/7bbb6461-e52d-4fe7-ace4-a15c30198de0/download/ev_chargers_consolidated_sep25.csv"
    resp = SESSION.get(url, timeout=60)
    resp.raise_for_status()
    reader = csv.DictReader(io.StringIO(resp.text))
    records = []
    for row in reader:
        records.append({
            "source": "nsw_tfnsw",
            "station_name": row.get("Name", ""),
            "operator": row.get("Operator", ""),
            "address": row.get("Address", ""),
            "suburb": row.get("Suburb", ""),
            "state": "NSW",
            "latitude": row.get("Latitude", ""),
            "longitude": row.get("Longitude", ""),
            "charger_type": row.get("Charger Type", ""),
            "power_kw": row.get("Power (kW)", ""),
            "num_ports": row.get("Number of Chargers", ""),
            "connector_type": row.get("Connector Type", ""),
            "status": row.get("Status", ""),
        })
    return records


def collect_queensland() -> list[dict]:
    """Queensland Government — EV Charging Stations."""
    urls = [
        "https://www.tmr.qld.gov.au/-/media/aboutus/corpinfo/Open%20data/findachargingev/csl_ev.csv",
    ]
    for url in urls:
        try:
            resp = SESSION.get(url, timeout=60, allow_redirects=True)
            resp.raise_for_status()
            content = resp.text
            if content.strip().startswith("{"):
                data = json.loads(content)
                raw_records = data.get("result", {}).get("records", [])
            else:
                raw_records = list(csv.DictReader(io.StringIO(content)))

            records = []
            for row in raw_records:
                records.append({
                    "source": "qld_gov",
                    "station_name": row.get("Location", row.get("Name", "")),
                    "operator": row.get("Operator", ""),
                    "address": row.get("Address", ""),
                    "suburb": row.get("Town", ""),
                    "state": "QLD",
                    "latitude": row.get("Latitude", ""),
                    "longitude": row.get("Longitude", ""),
                    "charger_type": row.get("Charger Type", ""),
                    "power_kw": row.get("Power", ""),
                    "num_ports": "",
                })
            if records:
                return records
        except Exception as e:
            logger.warning("QLD %s failed: %s", url, e)

    # Fallback: CKAN datastore API
    try:
        api_url = "https://data.qld.gov.au/api/3/action/datastore_search?resource_id=a34d4b5f-8e3c-4995-8950-2e84fd7bb4d5&limit=5000"
        resp = SESSION.get(api_url, timeout=60)
        resp.raise_for_status()
        raw_records = resp.json().get("result", {}).get("records", [])
        records = []
        for row in raw_records:
            records.append({
                "source": "qld_gov",
                "station_name": row.get("Location", ""),
                "operator": row.get("Operator", ""),
                "address": row.get("Address", ""),
                "suburb": row.get("Town", ""),
                "state": "QLD",
                "latitude": row.get("Latitude", ""),
                "longitude": row.get("Longitude", ""),
                "charger_type": row.get("Charger Type", ""),
                "power_kw": row.get("Power", ""),
                "num_ports": "",
            })
        return records
    except Exception as e:
        logger.error("QLD CKAN failed: %s", e)
        return []


def collect_victoria() -> list[dict]:
    """Victoria Data Vic — Government Funded Public EV Chargers."""
    url = "https://discover.data.vic.gov.au/api/3/action/package_show?id=government-funded-public-ev-chargers"
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
        resources = resp.json().get("result", {}).get("resources", [])
        # Find CSV or GeoJSON
        for res in resources:
            fmt = res.get("format", "").upper()
            if fmt in ("CSV", "GEOJSON", "JSON"):
                data_url = res["url"]
                resp2 = SESSION.get(data_url, timeout=60)
                resp2.raise_for_status()

                if fmt in ("GEOJSON", "JSON"):
                    features = resp2.json().get("features", [])
                    records = []
                    for f in features:
                        props = f.get("properties", {})
                        coords = f.get("geometry", {}).get("coordinates", [None, None])
                        records.append({
                            "source": "vic_gov",
                            "station_name": props.get("name", props.get("Name", "")),
                            "operator": props.get("Owner", props.get("operator", "")),
                            "address": props.get("Address", ""),
                            "suburb": props.get("Suburb", ""),
                            "state": "VIC",
                            "latitude": coords[1] if len(coords) > 1 else "",
                            "longitude": coords[0] if len(coords) > 0 else "",
                            "power_kw": props.get("Size_kW", ""),
                            "connector_type": props.get("Plug_Type", ""),
                            "num_ports": "",
                        })
                    return records
                else:
                    reader = csv.DictReader(io.StringIO(resp2.text))
                    records = []
                    for row in reader:
                        records.append({
                            "source": "vic_gov",
                            "station_name": row.get("Name", row.get("Site Name", "")),
                            "operator": row.get("Owner", row.get("Operator", "")),
                            "address": row.get("Address", ""),
                            "suburb": row.get("Suburb", row.get("Town", "")),
                            "state": "VIC",
                            "latitude": row.get("Latitude", row.get("lat", "")),
                            "longitude": row.get("Longitude", row.get("lon", "")),
                            "power_kw": row.get("Size_kW", row.get("Power", "")),
                            "connector_type": row.get("Plug_Type", ""),
                            "num_ports": "",
                        })
                    return records
    except Exception as e:
        logger.error("Victoria failed: %s", e)
    return []


def collect_opendatasoft() -> list[dict]:
    """OpenDataSoft — EV Charging Stations Australia."""
    url = "https://data.opendatasoft.com/api/explore/v2.1/catalog/datasets/ev-charging-stations@australiademo/records"
    all_records = []
    offset = 0

    while True:
        resp = SESSION.get(url, params={"limit": 100, "offset": offset}, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        if not results:
            break
        for row in results:
            geo = row.get("geopoint", row.get("geo_point_2d", {})) or {}
            all_records.append({
                "source": "opendatasoft_au",
                "station_name": row.get("station_name", row.get("name", "")),
                "operator": row.get("operator", ""),
                "address": row.get("station_address", row.get("address", "")),
                "suburb": row.get("suburb_2", row.get("suburb", "")),
                "state": row.get("state", ""),
                "latitude": row.get("latitude", geo.get("lat", "")),
                "longitude": row.get("longitude", geo.get("lon", "")),
                "charger_type": row.get("type", ""),
                "power_kw": row.get("charger_capacities", ""),
                "num_ports": row.get("number_of_plugs", ""),
            })
        offset += 100
        if offset >= data.get("total_count", 0):
            break
        time.sleep(0.3)

    return all_records


def collect_osm_australia() -> list[dict]:
    """OpenStreetMap Overpass — Australia EV charging with operator tags."""
    query = """
    [out:json][timeout:300];
    area["ISO3166-1"="AU"]->.au;
    (
      node["amenity"="charging_station"](area.au);
      way["amenity"="charging_station"](area.au);
    );
    out center body;
    """
    resp = SESSION.post("https://overpass-api.de/api/interpreter", data={"data": query}, timeout=360)
    resp.raise_for_status()
    elements = resp.json().get("elements", [])
    records = []
    for el in elements:
        tags = el.get("tags", {})
        lat = el.get("lat", el.get("center", {}).get("lat", ""))
        lon = el.get("lon", el.get("center", {}).get("lon", ""))
        records.append({
            "source": "osm_australia",
            "station_name": tags.get("name", ""),
            "operator": tags.get("operator", tags.get("network", tags.get("brand", ""))),
            "address": f"{tags.get('addr:street', '')} {tags.get('addr:housenumber', '')}".strip(),
            "suburb": tags.get("addr:suburb", tags.get("addr:city", "")),
            "state": tags.get("addr:state", ""),
            "latitude": lat,
            "longitude": lon,
            "charger_type": "",
            "power_kw": tags.get("charging_station:output", ""),
            "num_ports": tags.get("capacity", ""),
            "connector_type": "",
            "socket_chademo": tags.get("socket:chademo", ""),
            "socket_ccs2": tags.get("socket:type2_combo", ""),
            "socket_type2": tags.get("socket:type2", ""),
        })
    return records


# All collectors
COLLECTORS = [
    ("NSW TfNSW", collect_nsw_tfnsw),
    ("Queensland Gov", collect_queensland),
    ("Victoria Gov", collect_victoria),
    ("OpenDataSoft AU", collect_opendatasoft),
    ("OSM Australia", collect_osm_australia),
]


# ═══════════════════════════════════════════════
#  NORMALIZE + DEDUP + MERGE
# ═══════════════════════════════════════════════

def normalize_record(rec: dict) -> dict | None:
    """Normalize a raw record to a standard format. Returns None if invalid."""
    try:
        lat = float(rec.get("latitude", 0))
        lng = float(rec.get("longitude", 0))
    except (ValueError, TypeError):
        return None

    if not (-90 <= lat <= 90 and -180 <= lng <= 180):
        return None
    if lat == 0 and lng == 0:
        return None

    operator = (rec.get("operator", "") or "").strip() or "Unknown"
    name = (rec.get("station_name", "") or "").strip()
    charger_type = (rec.get("charger_type", "") or "").lower()
    connector = (rec.get("connector_type", "") or "").lower()

    try:
        pw = str(rec.get("power_kw", "") or "")
        power_val = float(pw.replace("kW", "").strip().split(",")[0].split("/")[0]) if pw else 0
    except (ValueError, TypeError):
        power_val = 0

    op_lower = operator.lower()
    if "tesla" in op_lower and ("supercharg" in op_lower or power_val >= 50):
        cat = "Tesla Supercharger"
    elif power_val >= 50 or "dc" in charger_type or "dc fast" in connector:
        cat = "DC Fast"
    elif power_val >= 3 or "level 2" in charger_type or "type 2" in connector or "ac" in charger_type:
        cat = "Level 2"
    elif 0 < power_val < 3:
        cat = "Level 1"
    else:
        cat = "Level 2"

    try:
        num_ports = int(rec.get("num_ports", "")) if rec.get("num_ports") else None
    except (ValueError, TypeError):
        num_ports = None

    return {
        "lat": round(lat, 6),
        "lng": round(lng, 6),
        "name": name,
        "operator": operator,
        "address": (rec.get("address", "") or "").strip(),
        "suburb": (rec.get("suburb", "") or "").strip(),
        "state": (rec.get("state", "") or "").strip(),
        "connector_category": cat,
        "power_kw": power_val,
        "num_ports": num_ports,
        "source": rec.get("source", "unknown"),
    }


def quality_score(rec: dict) -> int:
    """Higher = better data quality."""
    s = 0
    if rec["name"]:
        s += 10
    if rec["operator"] != "Unknown":
        s += 5
    if rec["address"]:
        s += 3
    if rec["power_kw"] > 0:
        s += 2
    if rec["num_ports"]:
        s += 1
    return s


def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(min(1, math.sqrt(a)))


def deduplicate(records: list[dict], threshold_m: float = 100) -> list[dict]:
    """Grid-based spatial dedup. Keeps the highest quality record per cluster."""
    grid: dict[tuple, list[dict]] = defaultdict(list)
    for rec in records:
        key = (round(rec["lat"] / GRID_SIZE), round(rec["lng"] / GRID_SIZE))
        grid[key].append(rec)

    unique = []
    used_coords = set()

    # Process grid cells
    for key, cell_recs in grid.items():
        cell_recs.sort(key=quality_score, reverse=True)
        for rec in cell_recs:
            coord_key = (round(rec["lat"], 4), round(rec["lng"], 4))
            if coord_key in used_coords:
                continue

            # Check proximity against already-accepted records in nearby cells
            is_dup = False
            for dx in range(-1, 2):
                if is_dup:
                    break
                for dy in range(-1, 2):
                    if is_dup:
                        break
                    nkey = (key[0] + dx, key[1] + dy)
                    for existing in grid.get(nkey, []):
                        ex_coord = (round(existing["lat"], 4), round(existing["lng"], 4))
                        if ex_coord in used_coords and ex_coord != coord_key:
                            if haversine_m(rec["lat"], rec["lng"], existing["lat"], existing["lng"]) < threshold_m:
                                is_dup = True
                                break

            if not is_dup:
                used_coords.add(coord_key)
                unique.append(rec)

    return unique


def merge_into_geojson(new_stations: list[dict], dry_run: bool = False) -> int:
    """Merge new AU stations into main GeoJSON. Returns count of new stations added."""
    # Load existing
    if GZ_PATH.exists():
        with gzip.open(GZ_PATH, "rt", encoding="utf-8") as f:
            geojson = json.load(f)
    elif GEOJSON_PATH.exists():
        with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
            geojson = json.load(f)
    else:
        geojson = {"type": "FeatureCollection", "features": []}

    existing = geojson.get("features", [])
    logger.info("Existing features in GeoJSON: %d", len(existing))

    # Build coord index of existing AU stations
    existing_au_coords = set()
    for feat in existing:
        props = feat.get("properties", {})
        cc = (props.get("country_code", "") or props.get("country", "")).upper()
        if cc in ("AU", "AUSTRALIA"):
            coords = feat.get("geometry", {}).get("coordinates", [])
            if len(coords) >= 2:
                existing_au_coords.add((round(coords[1], 4), round(coords[0], 4)))

    logger.info("Existing AU station coords: %d", len(existing_au_coords))

    # Add new stations not already present
    added = 0
    for rec in new_stations:
        coord_key = (round(rec["lat"], 4), round(rec["lng"], 4))
        if coord_key in existing_au_coords:
            continue

        existing_au_coords.add(coord_key)
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [rec["lng"], rec["lat"]],
            },
            "properties": {
                "station_id": f"au_{rec['source']}_{added}",
                "station_name": rec["name"] or f"{rec['operator']} - {rec['suburb'] or 'Station'}",
                "address": ", ".join(filter(None, [rec["address"], rec["suburb"], rec["state"]])),
                "network": rec["operator"],
                "connector_types": rec["connector_category"],
                "connector_category": rec["connector_category"],
                "total_ports": rec["num_ports"],
                "power_kw": rec["power_kw"] if rec["power_kw"] > 0 else None,
                "country": "Australia",
                "country_code": "AU",
                "source": f"australia_{rec['source']}",
            },
        }
        existing.append(feature)
        added += 1

    if dry_run:
        logger.info("[DRY RUN] Would add %d new AU stations (total would be %d)", added, len(existing))
        return added

    geojson["features"] = existing

    logger.info("Writing %s ...", GEOJSON_PATH)
    with open(GEOJSON_PATH, "w", encoding="utf-8") as f:
        json.dump(geojson, f)

    logger.info("Writing %s ...", GZ_PATH)
    with gzip.open(GZ_PATH, "wt", encoding="utf-8") as f:
        json.dump(geojson, f)

    logger.info("Added %d new AU stations. Total features: %d", added, len(existing))
    return added


# ═══════════════════════════════════════════════
#  GIT PUSH (optional)
# ═══════════════════════════════════════════════

def git_push():
    """Commit updated GeoJSON and push to GitHub (triggers Render redeploy)."""
    os.chdir(PROJECT_ROOT)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Stage the updated files
    subprocess.run(["git", "add", "frontend/data/ev_stations.geojson.gz"], check=True)
    subprocess.run(["git", "add", "frontend/data/ev_stations.geojson"], check=False)  # May be gitignored

    # Check if there are changes
    result = subprocess.run(["git", "diff", "--cached", "--quiet"], capture_output=True)
    if result.returncode == 0:
        logger.info("No changes to commit.")
        return

    msg = f"Auto-update Australia EV data ({now})"
    subprocess.run(["git", "commit", "-m", msg], check=True)
    subprocess.run(["git", "push", "origin", "main"], check=True)
    logger.info("Pushed to GitHub — Render will auto-redeploy.")


# ═══════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Auto-update Australia EV charging data")
    parser.add_argument("--push", action="store_true", help="Push to GitHub after merge")
    parser.add_argument("--dry-run", action="store_true", help="Collect + dedup only, don't modify GeoJSON")
    args = parser.parse_args()

    timestamp = datetime.now(timezone.utc).isoformat()
    logger.info("=" * 60)
    logger.info("Australia EV Auto-Update — %s", timestamp)
    logger.info("=" * 60)

    # ── Step 1: Collect ──
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    all_raw = []
    results = {}

    for name, collector in COLLECTORS:
        try:
            logger.info("Collecting: %s", name)
            records = collector()
            count = len(records)
            results[name] = count
            logger.info("  -> %d records", count)

            # Save raw
            safe_name = name.lower().replace(" ", "_").replace(".", "")
            path = RAW_DIR / f"{safe_name}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(records, f, ensure_ascii=False, indent=2)

            all_raw.extend(records)
            time.sleep(1)
        except Exception as e:
            logger.error("  -> FAILED: %s", e)
            results[name] = 0

    logger.info("Total raw records: %d", len(all_raw))

    # ── Step 2: Normalize ──
    normalized = []
    for rec in all_raw:
        n = normalize_record(rec)
        if n:
            normalized.append(n)
    logger.info("Valid records after normalization: %d", len(normalized))

    # ── Step 3: Deduplicate ──
    unique = deduplicate(normalized)
    logger.info("Unique stations after dedup: %d", len(unique))

    # Operator summary
    op_counts: dict[str, int] = defaultdict(int)
    for rec in unique:
        op_counts[rec["operator"]] += 1
    logger.info("Top operators:")
    for op, cnt in sorted(op_counts.items(), key=lambda x: -x[1])[:20]:
        logger.info("  %-30s %5d", op, cnt)

    # ── Step 4: Merge ──
    added = merge_into_geojson(unique, dry_run=args.dry_run)

    # ── Step 5: Push (optional) ──
    if args.push and added > 0 and not args.dry_run:
        try:
            git_push()
        except Exception as e:
            logger.error("Git push failed: %s", e)

    # ── Summary ──
    logger.info("")
    logger.info("=" * 60)
    logger.info("UPDATE COMPLETE")
    logger.info("=" * 60)
    for name, count in results.items():
        logger.info("  %-20s %5d records", name, count)
    logger.info("  %-20s %5d unique (deduped)", "TOTAL", len(unique))
    logger.info("  %-20s %5d new stations added", "MERGED", added)


if __name__ == "__main__":
    main()
