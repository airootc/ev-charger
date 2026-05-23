#!/usr/bin/env python3
"""Collect EV charging station data for Australia by operator/source.

Sources:
  1. NSW Open Data (Transport for NSW) — all NSW stations
  2. Queensland Open Data (data.qld.gov.au) — QLD Electric Super Highway
  3. Victoria Data Vic — Government funded EV chargers
  4. OpenDataSoft Australia — aggregated national dataset
  5. Chargefox — scrape from map API
  6. Evie Networks — scrape from map page
  7. Exploren — scrape from map API
  8. Peclet Data Portal — aggregated EV stations
  9. Data.NSW — EV charging locations (alternate dataset)
"""

import csv
import io
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data_research_agent" / "data" / "raw" / "batch_australia_operators"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "UnearthAI-DataResearch/1.0 (EV Station Data Collection)"
})


def save_json(name: str, data) -> int:
    """Save data as JSON and return record count."""
    path = OUTPUT_DIR / f"{name}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    count = len(data) if isinstance(data, list) else 0
    logger.info("Saved %s: %d records -> %s", name, count, path)
    return count


# ── 1. NSW Open Data (Transport for NSW) ──

def collect_nsw_tfnsw():
    """Transport for NSW — EV Charging Stations dataset (CSV)."""
    logger.info("Collecting NSW TfNSW data...")
    url = "https://opendata.transport.nsw.gov.au/data/dataset/be1c4de4-4517-4bd0-8a09-2965ddfc7179/resource/7bbb6461-e52d-4fe7-ace4-a15c30198de0/download/ev_chargers_consolidated_sep25.csv"
    try:
        resp = SESSION.get(url, timeout=60)
        resp.raise_for_status()
        reader = csv.DictReader(io.StringIO(resp.text))
        records = []
        for row in reader:
            records.append({
                "source": "nsw_tfnsw",
                "operator": row.get("Operator", row.get("operator", "")),
                "station_name": row.get("Name", row.get("name", "")),
                "address": row.get("Address", row.get("address", "")),
                "suburb": row.get("Suburb", row.get("suburb", "")),
                "state": "NSW",
                "country": "Australia",
                "country_code": "AU",
                "latitude": row.get("Latitude", row.get("latitude", "")),
                "longitude": row.get("Longitude", row.get("longitude", "")),
                "charger_type": row.get("Charger Type", row.get("charger_type", "")),
                "power_kw": row.get("Power (kW)", row.get("power_kw", "")),
                "num_chargers": row.get("Number of Chargers", row.get("num_chargers", "")),
                "connector_type": row.get("Connector Type", row.get("connector_type", "")),
                "status": row.get("Status", row.get("status", "")),
                "raw_data": row,
            })
        return save_json("nsw_tfnsw", records)
    except Exception as e:
        logger.error("NSW TfNSW failed: %s", e)
        return 0


# ── 2. NSW Data.NSW — EV Charging Locations ──

def collect_nsw_datansw():
    """Data.NSW portal — EV Charging Locations."""
    logger.info("Collecting NSW Data.NSW data...")
    # Try the CKAN API
    url = "https://data.nsw.gov.au/data/api/3/action/package_show?id=2-ev-charging-locations"
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
        pkg = resp.json()
        resources = pkg.get("result", {}).get("resources", [])
        csv_resources = [r for r in resources if r.get("format", "").upper() == "CSV"]
        if not csv_resources:
            # Try JSON resources
            json_resources = [r for r in resources if r.get("format", "").upper() in ("JSON", "GEOJSON")]
            if json_resources:
                data_url = json_resources[0]["url"]
                resp2 = SESSION.get(data_url, timeout=60)
                resp2.raise_for_status()
                data = resp2.json()
                features = data.get("features", data) if isinstance(data, dict) else data
                records = []
                for f in (features if isinstance(features, list) else []):
                    props = f.get("properties", f)
                    geom = f.get("geometry", {})
                    coords = geom.get("coordinates", [None, None])
                    records.append({
                        "source": "nsw_datansw",
                        "station_name": props.get("name", props.get("Name", "")),
                        "operator": props.get("operator", props.get("Operator", "")),
                        "address": props.get("address", props.get("Address", "")),
                        "state": "NSW",
                        "country": "Australia",
                        "country_code": "AU",
                        "latitude": coords[1] if len(coords) > 1 else "",
                        "longitude": coords[0] if len(coords) > 0 else "",
                        "raw_data": props,
                    })
                return save_json("nsw_datansw", records)
            logger.warning("No CSV or JSON resources found in Data.NSW")
            return 0

        data_url = csv_resources[0]["url"]
        resp2 = SESSION.get(data_url, timeout=60)
        resp2.raise_for_status()
        reader = csv.DictReader(io.StringIO(resp2.text))
        records = []
        for row in reader:
            records.append({
                "source": "nsw_datansw",
                "station_name": row.get("Name", row.get("name", "")),
                "operator": row.get("Operator", row.get("operator", "")),
                "address": row.get("Address", row.get("address", "")),
                "state": "NSW",
                "country": "Australia",
                "country_code": "AU",
                "latitude": row.get("Latitude", row.get("latitude", "")),
                "longitude": row.get("Longitude", row.get("longitude", "")),
                "raw_data": row,
            })
        return save_json("nsw_datansw", records)
    except Exception as e:
        logger.error("NSW Data.NSW failed: %s", e)
        return 0


# ── 3. Queensland Open Data ──

def collect_queensland():
    """Queensland Government — EV Charging Stations dataset."""
    logger.info("Collecting Queensland data...")
    # Try the direct CSV first
    urls = [
        "https://www.tmr.qld.gov.au/-/media/aboutus/corpinfo/Open%20data/findachargingev/csl_ev.csv",
        "https://data.qld.gov.au/dataset/find-a-charging-station-electric-vehicle/resource/a34d4b5f-8e3c-4995-8950-2e84fd7bb4d5",
    ]
    for url in urls:
        try:
            resp = SESSION.get(url, timeout=60, allow_redirects=True)
            resp.raise_for_status()
            content = resp.text
            if content.strip().startswith("{"):
                # JSON response from CKAN
                data = json.loads(content)
                if "result" in data and "records" in data["result"]:
                    raw_records = data["result"]["records"]
                    records = []
                    for row in raw_records:
                        records.append({
                            "source": "qld_gov",
                            "station_name": row.get("Location", row.get("location", "")),
                            "operator": row.get("Operator", row.get("operator", "")),
                            "address": row.get("Address", row.get("address", "")),
                            "suburb": row.get("Town", row.get("town", "")),
                            "state": "QLD",
                            "country": "Australia",
                            "country_code": "AU",
                            "latitude": row.get("Latitude", row.get("latitude", "")),
                            "longitude": row.get("Longitude", row.get("longitude", "")),
                            "charger_type": row.get("Charger Type", ""),
                            "power_kw": row.get("Power", row.get("power_kw", "")),
                            "raw_data": row,
                        })
                    return save_json("queensland_gov", records)
            else:
                # CSV response
                reader = csv.DictReader(io.StringIO(content))
                records = []
                for row in reader:
                    records.append({
                        "source": "qld_gov",
                        "station_name": row.get("Location", row.get("Name", "")),
                        "operator": row.get("Operator", row.get("operator", "")),
                        "address": row.get("Address", row.get("address", "")),
                        "suburb": row.get("Town", row.get("town", "")),
                        "state": "QLD",
                        "country": "Australia",
                        "country_code": "AU",
                        "latitude": row.get("Latitude", row.get("latitude", "")),
                        "longitude": row.get("Longitude", row.get("longitude", "")),
                        "charger_type": row.get("Charger Type", ""),
                        "power_kw": row.get("Power", ""),
                        "raw_data": row,
                    })
                if records:
                    return save_json("queensland_gov", records)
        except Exception as e:
            logger.warning("QLD URL %s failed: %s", url, e)
            continue

    # Fallback: CKAN API
    try:
        api_url = "https://data.qld.gov.au/api/3/action/datastore_search?resource_id=a34d4b5f-8e3c-4995-8950-2e84fd7bb4d5&limit=5000"
        resp = SESSION.get(api_url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        raw_records = data.get("result", {}).get("records", [])
        records = []
        for row in raw_records:
            records.append({
                "source": "qld_gov",
                "station_name": row.get("Location", ""),
                "operator": row.get("Operator", ""),
                "address": row.get("Address", ""),
                "suburb": row.get("Town", ""),
                "state": "QLD",
                "country": "Australia",
                "country_code": "AU",
                "latitude": row.get("Latitude", ""),
                "longitude": row.get("Longitude", ""),
                "charger_type": row.get("Charger Type", ""),
                "power_kw": row.get("Power", ""),
                "raw_data": row,
            })
        return save_json("queensland_gov", records)
    except Exception as e:
        logger.error("Queensland CKAN API failed: %s", e)
        return 0


# ── 4. Victoria Data Vic ──

def collect_victoria():
    """Victorian Government — Government Funded Public EV Chargers."""
    logger.info("Collecting Victoria data...")
    # Try CKAN API
    url = "https://discover.data.vic.gov.au/api/3/action/package_show?id=government-funded-public-ev-chargers"
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
        pkg = resp.json()
        resources = pkg.get("result", {}).get("resources", [])
        csv_resources = [r for r in resources if r.get("format", "").upper() == "CSV"]
        if not csv_resources:
            # Try any downloadable resource
            csv_resources = [r for r in resources if r.get("url", "").endswith((".csv", ".CSV"))]
        if not csv_resources:
            logger.warning("No CSV resource found for Victoria dataset")
            # Try GeoJSON
            geojson_resources = [r for r in resources if r.get("format", "").upper() in ("GEOJSON", "JSON")]
            if geojson_resources:
                csv_resources = geojson_resources

        if csv_resources:
            data_url = csv_resources[0]["url"]
            resp2 = SESSION.get(data_url, timeout=60)
            resp2.raise_for_status()

            if data_url.endswith(".json") or data_url.endswith(".geojson"):
                data = resp2.json()
                features = data.get("features", [])
                records = []
                for f in features:
                    props = f.get("properties", {})
                    coords = f.get("geometry", {}).get("coordinates", [None, None])
                    records.append({
                        "source": "vic_gov",
                        "station_name": props.get("name", props.get("Name", "")),
                        "operator": props.get("operator", props.get("Owner", "")),
                        "address": props.get("address", props.get("Address", "")),
                        "state": "VIC",
                        "country": "Australia",
                        "country_code": "AU",
                        "latitude": coords[1] if coords and len(coords) > 1 else "",
                        "longitude": coords[0] if coords and len(coords) > 0 else "",
                        "power_kw": props.get("power_kw", props.get("Size_kW", "")),
                        "connector_type": props.get("plug_type", props.get("Plug_Type", "")),
                        "raw_data": props,
                    })
                return save_json("victoria_gov", records)
            else:
                reader = csv.DictReader(io.StringIO(resp2.text))
                records = []
                for row in reader:
                    lat = row.get("Latitude", row.get("latitude", row.get("lat", "")))
                    lng = row.get("Longitude", row.get("longitude", row.get("lon", row.get("lng", ""))))
                    records.append({
                        "source": "vic_gov",
                        "station_name": row.get("Name", row.get("name", row.get("Site Name", ""))),
                        "operator": row.get("Owner", row.get("Operator", row.get("operator", ""))),
                        "address": row.get("Address", row.get("address", "")),
                        "suburb": row.get("Suburb", row.get("suburb", row.get("Town", ""))),
                        "state": "VIC",
                        "country": "Australia",
                        "country_code": "AU",
                        "latitude": lat,
                        "longitude": lng,
                        "power_kw": row.get("Size_kW", row.get("Power", row.get("power_kw", ""))),
                        "connector_type": row.get("Plug_Type", row.get("Connector", "")),
                        "raw_data": row,
                    })
                return save_json("victoria_gov", records)
        return 0
    except Exception as e:
        logger.error("Victoria Data Vic failed: %s", e)
        return 0


# ── 5. OpenDataSoft Australia ──

def collect_opendatasoft():
    """OpenDataSoft — EV Charging Stations Australia dataset."""
    logger.info("Collecting OpenDataSoft Australia data...")
    url = "https://data.opendatasoft.com/api/explore/v2.1/catalog/datasets/ev-charging-stations@australiademo/records"
    all_records = []
    offset = 0
    limit = 100

    try:
        while True:
            params = {"limit": limit, "offset": offset}
            resp = SESSION.get(url, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])
            if not results:
                break

            for row in results:
                geo = row.get("geo_point_2d", {}) or {}
                records_entry = {
                    "source": "opendatasoft_au",
                    "station_name": row.get("name", ""),
                    "operator": row.get("operator", row.get("network", "")),
                    "address": row.get("address", ""),
                    "suburb": row.get("suburb", row.get("city", "")),
                    "state": row.get("state", ""),
                    "country": "Australia",
                    "country_code": "AU",
                    "latitude": geo.get("lat", ""),
                    "longitude": geo.get("lon", ""),
                    "charger_type": row.get("charger_type", ""),
                    "power_kw": row.get("power_kw", ""),
                    "connector_type": row.get("connector_type", row.get("plug_type", "")),
                    "status": row.get("status", ""),
                    "num_chargers": row.get("num_chargers", ""),
                    "raw_data": row,
                }
                all_records.append(records_entry)

            offset += limit
            if offset >= data.get("total_count", 0):
                break
            time.sleep(0.5)

        return save_json("opendatasoft_au", all_records)
    except Exception as e:
        logger.error("OpenDataSoft failed: %s", e)
        return 0


# ── 6. Peclet Data Portal ──

def collect_peclet():
    """Peclet Data Portal — EV Charging Stations dataset."""
    logger.info("Collecting Peclet data...")
    url = "https://data.peclet.com.au/api/explore/v2.1/catalog/datasets/ev-charging-stations/records"
    all_records = []
    offset = 0
    limit = 100

    try:
        while True:
            params = {"limit": limit, "offset": offset}
            resp = SESSION.get(url, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])
            if not results:
                break

            for row in results:
                geo = row.get("geo_point_2d", {}) or {}
                records_entry = {
                    "source": "peclet_au",
                    "station_name": row.get("name", row.get("station_name", "")),
                    "operator": row.get("operator", row.get("network", "")),
                    "address": row.get("address", ""),
                    "suburb": row.get("suburb", row.get("city", "")),
                    "state": row.get("state", ""),
                    "country": "Australia",
                    "country_code": "AU",
                    "latitude": geo.get("lat", ""),
                    "longitude": geo.get("lon", ""),
                    "charger_type": row.get("charger_type", ""),
                    "power_kw": row.get("power_kw", ""),
                    "connector_type": row.get("connector_type", ""),
                    "raw_data": row,
                }
                all_records.append(records_entry)

            offset += limit
            total = data.get("total_count", 0)
            if offset >= total:
                break
            time.sleep(0.5)

        return save_json("peclet_au", all_records)
    except Exception as e:
        logger.error("Peclet failed: %s", e)
        return 0


# ── 7. Chargefox — try map tile API ──

def collect_chargefox():
    """Attempt to collect Chargefox station data from their public map API."""
    logger.info("Collecting Chargefox data...")
    # Chargefox uses a map at https://www.chargefox.com/charging-network
    # Try common API patterns
    urls_to_try = [
        "https://api.chargefox.com/v2/sites?limit=5000",
        "https://api.chargefox.com/v1/sites?limit=5000",
        "https://www.chargefox.com/api/sites",
        "https://app.chargefox.com/api/v2/public/sites?per_page=5000",
    ]
    for url in urls_to_try:
        try:
            resp = SESSION.get(url, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                sites = data if isinstance(data, list) else data.get("data", data.get("sites", data.get("results", [])))
                if sites and isinstance(sites, list) and len(sites) > 0:
                    records = []
                    for site in sites:
                        lat = site.get("latitude", site.get("lat", ""))
                        lng = site.get("longitude", site.get("lng", site.get("lon", "")))
                        if not lat and "location" in site:
                            loc = site["location"]
                            lat = loc.get("latitude", loc.get("lat", ""))
                            lng = loc.get("longitude", loc.get("lng", ""))
                        records.append({
                            "source": "chargefox",
                            "station_name": site.get("name", site.get("title", "")),
                            "operator": "Chargefox",
                            "address": site.get("address", ""),
                            "suburb": site.get("suburb", site.get("city", "")),
                            "state": site.get("state", ""),
                            "country": "Australia",
                            "country_code": "AU",
                            "latitude": lat,
                            "longitude": lng,
                            "power_kw": site.get("max_power", site.get("power_kw", "")),
                            "num_chargers": site.get("evse_count", site.get("num_chargers", "")),
                            "status": site.get("status", ""),
                            "raw_data": site,
                        })
                    return save_json("chargefox", records)
        except Exception:
            continue

    logger.warning("Chargefox: No public API found, skipping (try OpenChargeMap for Chargefox data)")
    return 0


# ── 8. OSM Overpass — Australia-specific with operator tags ──

def collect_osm_australia():
    """OpenStreetMap Overpass — EV charging stations in Australia with operator info."""
    logger.info("Collecting OSM Australia data (with operator tags)...")
    overpass_url = "https://overpass-api.de/api/interpreter"
    query = """
    [out:json][timeout:300];
    area["ISO3166-1"="AU"]->.au;
    (
      node["amenity"="charging_station"](area.au);
      way["amenity"="charging_station"](area.au);
    );
    out center body;
    """
    try:
        resp = SESSION.post(overpass_url, data={"data": query}, timeout=360)
        resp.raise_for_status()
        data = resp.json()
        elements = data.get("elements", [])
        records = []
        for el in elements:
            tags = el.get("tags", {})
            lat = el.get("lat", el.get("center", {}).get("lat", ""))
            lon = el.get("lon", el.get("center", {}).get("lon", ""))
            records.append({
                "source": "osm_australia",
                "osm_id": el.get("id"),
                "osm_type": el.get("type"),
                "station_name": tags.get("name", ""),
                "operator": tags.get("operator", tags.get("network", "")),
                "brand": tags.get("brand", ""),
                "address": f"{tags.get('addr:street', '')} {tags.get('addr:housenumber', '')}".strip(),
                "suburb": tags.get("addr:suburb", tags.get("addr:city", "")),
                "state": tags.get("addr:state", ""),
                "country": "Australia",
                "country_code": "AU",
                "latitude": lat,
                "longitude": lon,
                "socket_chademo": tags.get("socket:chademo", ""),
                "socket_ccs2": tags.get("socket:type2_combo", tags.get("socket:ccs", "")),
                "socket_type2": tags.get("socket:type2", ""),
                "power_kw": tags.get("charging_station:output", tags.get("capacity", "")),
                "num_chargers": tags.get("capacity", ""),
                "access": tags.get("access", ""),
                "fee": tags.get("fee", ""),
                "opening_hours": tags.get("opening_hours", ""),
                "raw_tags": tags,
            })
        return save_json("osm_australia", records)
    except Exception as e:
        logger.error("OSM Australia failed: %s", e)
        return 0


# ── Main ──

def main():
    logger.info("=" * 60)
    logger.info("Australia EV Charging Station Collection by Operator")
    logger.info("=" * 60)

    totals = {}
    collectors = [
        ("NSW TfNSW", collect_nsw_tfnsw),
        ("NSW Data.NSW", collect_nsw_datansw),
        ("Queensland Gov", collect_queensland),
        ("Victoria Gov", collect_victoria),
        ("OpenDataSoft AU", collect_opendatasoft),
        ("Peclet Portal", collect_peclet),
        ("Chargefox", collect_chargefox),
        ("OSM Australia", collect_osm_australia),
    ]

    for name, collector in collectors:
        try:
            count = collector()
            totals[name] = count
            logger.info("  ✓ %s: %d records", name, count)
        except Exception as e:
            logger.error("  ✗ %s: FAILED - %s", name, e)
            totals[name] = 0
        time.sleep(1)  # Be polite between sources

    # Summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("COLLECTION SUMMARY")
    logger.info("=" * 60)
    grand_total = 0
    for name, count in totals.items():
        status = "✓" if count > 0 else "✗"
        logger.info("  %s %-20s %5d records", status, name, count)
        grand_total += count
    logger.info("-" * 40)
    logger.info("  TOTAL: %d records (pre-dedup)", grand_total)

    # Analyze operators across all collected data
    logger.info("")
    logger.info("OPERATOR ANALYSIS")
    logger.info("=" * 60)
    operator_counts = {}
    for f in OUTPUT_DIR.glob("*.json"):
        with open(f, "r") as fh:
            data = json.load(fh)
        if isinstance(data, list):
            for rec in data:
                op = rec.get("operator", "") or rec.get("brand", "") or "Unknown"
                op = op.strip()
                if op:
                    operator_counts[op] = operator_counts.get(op, 0) + 1

    for op, count in sorted(operator_counts.items(), key=lambda x: -x[1])[:30]:
        logger.info("  %-35s %5d stations", op, count)

    return grand_total


if __name__ == "__main__":
    total = main()
    sys.exit(0 if total > 0 else 1)
