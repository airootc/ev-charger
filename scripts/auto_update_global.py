#!/usr/bin/env python3
"""Automated global EV charging station data updater — by country & operator.

Collects from government open data portals, aggregator APIs, and OSM
for each supported country. Deduplicates, merges into the main GeoJSON,
and optionally pushes to GitHub (triggering Render redeploy).

Usage:
    python scripts/auto_update_global.py                      # All countries
    python scripts/auto_update_global.py --countries AU NZ UK  # Specific countries
    python scripts/auto_update_global.py --dry-run             # Preview only
    python scripts/auto_update_global.py --push                # Push to GitHub after merge

Supported countries: AU, NZ, UK, DE, FR, NL, NO, SE, DK, CH, IE, CA, US, SG, JP, KR
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
RAW_DIR = PROJECT_ROOT / "data_research_agent" / "data" / "raw" / "batch_global_operators"
GEOJSON_PATH = PROJECT_ROOT / "frontend" / "data" / "ev_stations.geojson"
GZ_PATH = PROJECT_ROOT / "frontend" / "data" / "ev_stations.geojson.gz"
LOG_DIR = PROJECT_ROOT / "scripts" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "global_update.log"),
    ],
)
logger = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "UnearthAI-DataResearch/1.0"})

GRID_SIZE = 0.001  # ~111m


# ═══════════════════════════════════════════════════
#  SHARED HELPERS
# ═══════════════════════════════════════════════════

def osm_query(area_tag: str, area_value: str) -> list[dict]:
    """Run an Overpass query for EV charging stations in a country."""
    query = f"""
    [out:json][timeout:300];
    area["{area_tag}"="{area_value}"]->.a;
    (
      node["amenity"="charging_station"](area.a);
      way["amenity"="charging_station"](area.a);
    );
    out center body;
    """
    resp = SESSION.post("https://overpass-api.de/api/interpreter",
                        data={"data": query}, timeout=360)
    resp.raise_for_status()
    elements = resp.json().get("elements", [])
    records = []
    for el in elements:
        tags = el.get("tags", {})
        lat = el.get("lat", el.get("center", {}).get("lat", ""))
        lon = el.get("lon", el.get("center", {}).get("lon", ""))

        # Build connector type string from socket tags
        sockets = []
        if tags.get("socket:chademo"): sockets.append("CHAdeMO")
        if tags.get("socket:type2_combo") or tags.get("socket:ccs"): sockets.append("CCS2")
        if tags.get("socket:type2"): sockets.append("Type 2")
        if tags.get("socket:type1"): sockets.append("Type 1")
        if tags.get("socket:type1_combo"): sockets.append("CCS1")
        if tags.get("socket:tesla_supercharger"): sockets.append("Tesla")

        records.append({
            "station_name": tags.get("name", ""),
            "operator": tags.get("operator", tags.get("network", tags.get("brand", ""))),
            "address": f"{tags.get('addr:street', '')} {tags.get('addr:housenumber', '')}".strip(),
            "suburb": tags.get("addr:suburb", tags.get("addr:city", "")),
            "city": tags.get("addr:city", tags.get("addr:suburb", "")),
            "state": tags.get("addr:state", ""),
            "postal_code": tags.get("addr:postcode", ""),
            "latitude": lat,
            "longitude": lon,
            "power_kw": tags.get("charging_station:output", tags.get("maxpower", "")),
            "num_ports": tags.get("capacity", ""),
            "charger_type": "",
            "connector_type": ", ".join(sockets) if sockets else "",
            "access_type": tags.get("access", ""),
            "fee": tags.get("fee", ""),
            "opening_hours": tags.get("opening_hours", ""),
            "status": "Operational",
            "usage_cost": "Free" if tags.get("fee") == "no" else ("Paid" if tags.get("fee") == "yes" else ""),
            "brand": tags.get("brand", ""),
        })
    return records


def fetch_csv(url: str, field_map: dict, defaults: dict = None, timeout: int = 60) -> list[dict]:
    """Fetch a CSV URL and map fields to our standard schema."""
    resp = SESSION.get(url, timeout=timeout, allow_redirects=True)
    resp.raise_for_status()
    reader = csv.DictReader(io.StringIO(resp.text))
    records = []
    for row in reader:
        rec = {}
        for our_field, csv_fields in field_map.items():
            if isinstance(csv_fields, str):
                csv_fields = [csv_fields]
            for cf in csv_fields:
                val = row.get(cf, "")
                if val:
                    rec[our_field] = val
                    break
            else:
                rec[our_field] = ""
        if defaults:
            for k, v in defaults.items():
                rec.setdefault(k, v)
        records.append(rec)
    return records


def fetch_opendatasoft(dataset_id: str, domain: str, defaults: dict = None) -> list[dict]:
    """Fetch all records from an OpenDataSoft dataset."""
    url = f"https://{domain}/api/explore/v2.1/catalog/datasets/{dataset_id}/records"
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
            rec = {
                "station_name": row.get("station_name", row.get("name", row.get("nom_station", ""))),
                "operator": row.get("operator", row.get("network", row.get("nom_operateur", row.get("operateur", "")))),
                "address": row.get("station_address", row.get("address", row.get("adresse_station", ""))),
                "suburb": row.get("suburb_2", row.get("suburb", row.get("city", row.get("commune", "")))),
                "state": row.get("state", row.get("region", "")),
                "latitude": row.get("latitude", geo.get("lat", "")),
                "longitude": row.get("longitude", geo.get("lon", "")),
                "power_kw": row.get("charger_capacities", row.get("power_kw", row.get("puissance_nominale", ""))),
                "num_ports": row.get("number_of_plugs", row.get("nbre_pdc", "")),
                "charger_type": row.get("type", ""),
                "connector_type": row.get("connector_type", ""),
            }
            if defaults:
                rec.update(defaults)
            all_records.append(rec)
        offset += 100
        if offset >= data.get("total_count", 0):
            break
        time.sleep(0.3)
    return all_records


# ═══════════════════════════════════════════════════
#  COUNTRY COLLECTORS
# ═══════════════════════════════════════════════════

# ── AUSTRALIA ──
def collect_AU() -> list[dict]:
    records = []
    # NSW TfNSW
    try:
        logger.info("  AU: NSW TfNSW...")
        r = fetch_csv(
            "https://opendata.transport.nsw.gov.au/data/dataset/be1c4de4-4517-4bd0-8a09-2965ddfc7179/resource/7bbb6461-e52d-4fe7-ace4-a15c30198de0/download/ev_chargers_consolidated_sep25.csv",
            {"station_name": ["Name"], "operator": ["Operator"], "address": ["Address"],
             "suburb": ["Suburb"], "city": ["Suburb"], "latitude": ["Latitude"],
             "longitude": ["Longitude"], "charger_type": ["Charger Type"],
             "power_kw": ["Power (kW)"], "num_ports": ["Number of Chargers"],
             "connector_type": ["Connector Type"], "status": ["Status"],
             "postal_code": ["Postcode"], "access_type": ["Access Type"]},
            defaults={"state": "NSW", "source": "nsw_tfnsw"},
        )
        records.extend(r)
        logger.info("    -> %d", len(r))
    except Exception as e:
        logger.error("    NSW TfNSW failed: %s", e)

    # QLD
    try:
        logger.info("  AU: Queensland...")
        resp = SESSION.get("https://data.qld.gov.au/api/3/action/datastore_search?resource_id=a34d4b5f-8e3c-4995-8950-2e84fd7bb4d5&limit=5000", timeout=60)
        resp.raise_for_status()
        for row in resp.json().get("result", {}).get("records", []):
            records.append({"source": "qld_gov", "station_name": row.get("Location", ""),
                            "operator": row.get("Operator", ""), "address": row.get("Address", ""),
                            "suburb": row.get("Town", ""), "state": "QLD",
                            "latitude": row.get("Latitude", ""), "longitude": row.get("Longitude", ""),
                            "charger_type": row.get("Charger Type", ""), "power_kw": row.get("Power", ""),
                            "num_ports": "", "connector_type": ""})
        logger.info("    -> %d QLD records", len([r for r in records if r.get("source") == "qld_gov"]))
    except Exception as e:
        logger.error("    QLD failed: %s", e)

    # OpenDataSoft AU
    try:
        logger.info("  AU: OpenDataSoft...")
        r = fetch_opendatasoft("ev-charging-stations@australiademo", "data.opendatasoft.com", {"source": "opendatasoft_au"})
        records.extend(r)
        logger.info("    -> %d", len(r))
    except Exception as e:
        logger.error("    OpenDataSoft AU failed: %s", e)

    # OSM
    try:
        logger.info("  AU: OSM...")
        r = osm_query("ISO3166-1", "AU")
        for rec in r:
            rec["source"] = "osm_au"
        records.extend(r)
        logger.info("    -> %d", len(r))
    except Exception as e:
        logger.error("    OSM AU failed: %s", e)

    for r in records:
        r["country"] = "Australia"
        r["country_code"] = "AU"
    return records


# ── NEW ZEALAND ──
def collect_NZ() -> list[dict]:
    records = []
    # EVRoam — NZ government EV charging data
    try:
        logger.info("  NZ: EVRoam...")
        resp = SESSION.get("https://evroam.nz/api/coredata/availableconnectors", timeout=60)
        if resp.status_code == 200:
            data = resp.json()
            items = data if isinstance(data, list) else data.get("data", data.get("results", []))
            for item in items:
                records.append({"source": "evroam_nz", "station_name": item.get("name", ""),
                                "operator": item.get("owner", item.get("operator", "")),
                                "address": item.get("address", ""), "suburb": item.get("city", ""),
                                "state": "", "latitude": item.get("latitude", item.get("lat", "")),
                                "longitude": item.get("longitude", item.get("lng", "")),
                                "power_kw": item.get("maxPower", ""), "num_ports": item.get("numberOfConnectors", ""),
                                "charger_type": "", "connector_type": item.get("connectorType", "")})
            logger.info("    -> %d", len(records))
    except Exception as e:
        logger.warning("    EVRoam API failed: %s", e)

    # ChargeNet scrape attempt
    try:
        logger.info("  NZ: ChargeNet...")
        for url in ["https://charge.net.nz/api/stations", "https://charge.net.nz/api/v1/stations"]:
            try:
                resp = SESSION.get(url, timeout=30)
                if resp.status_code == 200:
                    data = resp.json()
                    sites = data if isinstance(data, list) else data.get("data", data.get("stations", []))
                    for s in sites:
                        loc = s.get("location", s)
                        records.append({"source": "chargenet_nz", "station_name": s.get("name", ""),
                                        "operator": "ChargeNet", "address": s.get("address", loc.get("address", "")),
                                        "suburb": s.get("city", ""), "state": "",
                                        "latitude": loc.get("latitude", loc.get("lat", s.get("latitude", ""))),
                                        "longitude": loc.get("longitude", loc.get("lng", s.get("longitude", ""))),
                                        "power_kw": s.get("maxPower", ""), "num_ports": s.get("connectorCount", ""),
                                        "charger_type": "", "connector_type": ""})
                    logger.info("    -> %d ChargeNet", len(sites))
                    break
            except Exception:
                continue
    except Exception as e:
        logger.warning("    ChargeNet failed: %s", e)

    # OSM
    try:
        logger.info("  NZ: OSM...")
        r = osm_query("ISO3166-1", "NZ")
        for rec in r:
            rec["source"] = "osm_nz"
        records.extend(r)
        logger.info("    -> %d", len(r))
    except Exception as e:
        logger.error("    OSM NZ failed: %s", e)

    for r in records:
        r["country"] = "New Zealand"
        r["country_code"] = "NZ"
    return records


# ── UNITED KINGDOM ──
def collect_UK() -> list[dict]:
    records = []
    # UK gov NCR was decommissioned Nov 2024, try archived data or OpenChargeMap UK
    # OpenDataSoft UK Power Networks
    try:
        logger.info("  UK: UK Power Networks OpenDataSoft...")
        r = fetch_opendatasoft("ozev-ukpn-national-chargepoint-register",
                               "ukpowernetworks.opendatasoft.com", {"source": "ukpn_ncr"})
        records.extend(r)
        logger.info("    -> %d", len(r))
    except Exception as e:
        logger.warning("    UKPN failed: %s", e)

    # OSM UK
    try:
        logger.info("  UK: OSM...")
        r = osm_query("ISO3166-1", "GB")
        for rec in r:
            rec["source"] = "osm_uk"
        records.extend(r)
        logger.info("    -> %d", len(r))
    except Exception as e:
        logger.error("    OSM UK failed: %s", e)

    for r in records:
        r["country"] = "United Kingdom"
        r["country_code"] = "GB"
    return records


# ── GERMANY ──
def collect_DE() -> list[dict]:
    records = []
    # Bundesnetzagentur
    try:
        logger.info("  DE: Bundesnetzagentur...")
        url = "https://data.bundesnetzagentur.de/Bundesnetzagentur/SharedDocs/Downloads/DE/Sachgebiete/Energie/Unternehmen_Institutionen/E_Mobilitaet/Ladesaeulenregister.csv"
        resp = SESSION.get(url, timeout=120, allow_redirects=True)
        resp.raise_for_status()
        # Try different encodings
        for enc in ["utf-8", "latin-1", "cp1252"]:
            try:
                text = resp.content.decode(enc)
                # Detect delimiter
                delim = ";" if ";" in text[:500] else ","
                reader = csv.DictReader(io.StringIO(text), delimiter=delim)
                for row in reader:
                    records.append({
                        "source": "bundesnetzagentur",
                        "station_name": row.get("Standort", row.get("Location", "")),
                        "operator": row.get("Betreiber", row.get("Operator", "")),
                        "address": f"{row.get('Straße', row.get('Street', ''))} {row.get('Hausnummer', '')}".strip(),
                        "suburb": row.get("Ort", row.get("City", "")),
                        "state": row.get("Bundesland", row.get("State", "")),
                        "latitude": (row.get("Breitengrad", row.get("Latitude", "")) or "").replace(",", "."),
                        "longitude": (row.get("Längengrad", row.get("Longitude", "")) or "").replace(",", "."),
                        "power_kw": row.get("Nennleistung Ladeeinrichtung [kW]", row.get("Power", "")),
                        "num_ports": row.get("Anzahl Ladepunkte", ""),
                        "charger_type": "", "connector_type": row.get("Steckertypen1", ""),
                    })
                logger.info("    -> %d", len(records))
                break
            except UnicodeDecodeError:
                continue
    except Exception as e:
        logger.warning("    Bundesnetzagentur failed: %s", e)

    # OSM Germany
    try:
        logger.info("  DE: OSM...")
        r = osm_query("ISO3166-1", "DE")
        for rec in r:
            rec["source"] = "osm_de"
        records.extend(r)
        logger.info("    -> %d", len(r))
    except Exception as e:
        logger.error("    OSM DE failed: %s", e)

    for r in records:
        r["country"] = "Germany"
        r["country_code"] = "DE"
    return records


# ── FRANCE ──
def collect_FR() -> list[dict]:
    records = []
    # IRVE — official French EV charging registry
    try:
        logger.info("  FR: IRVE (data.gouv.fr)...")
        url = "https://www.data.gouv.fr/fr/datasets/r/eb76d20a-8501-400e-b336-d85c3c4a8e44"
        resp = SESSION.get(url, timeout=120, allow_redirects=True)
        resp.raise_for_status()
        text = resp.content.decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            lat = row.get("consolidated_latitude", row.get("Ylat", ""))
            lng = row.get("consolidated_longitude", row.get("Xlong", ""))
            records.append({
                "source": "irve_france",
                "station_name": row.get("nom_station", row.get("n_station", "")),
                "operator": row.get("nom_operateur", row.get("n_operateur", "")),
                "address": row.get("adresse_station", row.get("ad_station", "")),
                "suburb": row.get("consolidated_commune", row.get("commune", "")),
                "state": "",
                "latitude": lat, "longitude": lng,
                "power_kw": row.get("puissance_nominale", ""),
                "num_ports": row.get("nbre_pdc", ""),
                "charger_type": "", "connector_type": "",
            })
        logger.info("    -> %d", len(records))
    except Exception as e:
        logger.warning("    IRVE failed: %s, trying OSM fallback", e)

    # OSM France
    try:
        logger.info("  FR: OSM...")
        r = osm_query("ISO3166-1", "FR")
        for rec in r:
            rec["source"] = "osm_fr"
        records.extend(r)
        logger.info("    -> %d", len(r))
    except Exception as e:
        logger.error("    OSM FR failed: %s", e)

    for r in records:
        r["country"] = "France"
        r["country_code"] = "FR"
    return records


# ── NETHERLANDS ──
def collect_NL() -> list[dict]:
    records = []
    # RVO open data
    try:
        logger.info("  NL: OpenDataSoft...")
        r = fetch_opendatasoft("ev-charging-stations@public", "data.opendatasoft.com",
                               {"source": "opendatasoft_nl"})
        nl_only = [rec for rec in r if "NL" in str(rec.get("state", "")).upper() or
                   "netherlands" in str(rec.get("suburb", "")).lower()]
        records.extend(r)  # Keep all — filter later by coords
        logger.info("    -> %d", len(r))
    except Exception as e:
        logger.warning("    NL OpenDataSoft failed: %s", e)

    # OSM Netherlands
    try:
        logger.info("  NL: OSM...")
        r = osm_query("ISO3166-1", "NL")
        for rec in r:
            rec["source"] = "osm_nl"
        records.extend(r)
        logger.info("    -> %d", len(r))
    except Exception as e:
        logger.error("    OSM NL failed: %s", e)

    for r in records:
        r["country"] = "Netherlands"
        r["country_code"] = "NL"
    return records


# ── NORWAY ──
def collect_NO() -> list[dict]:
    records = []
    # NOBIL — Norwegian EV registry (needs API key, try without)
    try:
        logger.info("  NO: NOBIL...")
        nobil_key = os.getenv("NOBIL_API_KEY", "")
        if nobil_key:
            url = f"https://nobil.no/api/server/datadump.php?apikey={nobil_key}&countrycode=NOR&format=json&file=false"
            resp = SESSION.get(url, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            stations = data.get("chargerstations", [])
            for s in stations:
                attrs = s.get("csmd", {})
                records.append({
                    "source": "nobil_no",
                    "station_name": attrs.get("name", ""),
                    "operator": attrs.get("Owned_by", attrs.get("Operator", "")),
                    "address": attrs.get("Street", ""),
                    "suburb": attrs.get("City", ""),
                    "state": attrs.get("County", ""),
                    "latitude": attrs.get("Position", "").split(",")[0] if "," in str(attrs.get("Position", "")) else "",
                    "longitude": attrs.get("Position", "").split(",")[1] if "," in str(attrs.get("Position", "")) else "",
                    "power_kw": "", "num_ports": attrs.get("Number_charging_points", ""),
                    "charger_type": "", "connector_type": "",
                })
            logger.info("    -> %d", len(stations))
        else:
            logger.info("    NOBIL: No API key, skipping")
    except Exception as e:
        logger.warning("    NOBIL failed: %s", e)

    # OSM Norway
    try:
        logger.info("  NO: OSM...")
        r = osm_query("ISO3166-1", "NO")
        for rec in r:
            rec["source"] = "osm_no"
        records.extend(r)
        logger.info("    -> %d", len(r))
    except Exception as e:
        logger.error("    OSM NO failed: %s", e)

    for r in records:
        r["country"] = "Norway"
        r["country_code"] = "NO"
    return records


# ── SWEDEN ──
def collect_SE() -> list[dict]:
    records = []
    try:
        logger.info("  SE: OSM...")
        r = osm_query("ISO3166-1", "SE")
        for rec in r:
            rec["source"] = "osm_se"
        records.extend(r)
        logger.info("    -> %d", len(r))
    except Exception as e:
        logger.error("    OSM SE failed: %s", e)

    for r in records:
        r["country"] = "Sweden"
        r["country_code"] = "SE"
    return records


# ── DENMARK ──
def collect_DK() -> list[dict]:
    records = []
    # Copenhagen open data
    try:
        logger.info("  DK: Copenhagen...")
        resp = SESSION.get("https://wfs-kbhkort.kk.dk/k101/ows?service=WFS&version=2.0.0&request=GetFeature&typeName=k101:ladestandere&outputFormat=application/json", timeout=60)
        if resp.status_code == 200:
            features = resp.json().get("features", [])
            for f in features:
                props = f.get("properties", {})
                coords = f.get("geometry", {}).get("coordinates", [None, None])
                records.append({
                    "source": "copenhagen_dk",
                    "station_name": props.get("vejnavn", ""),
                    "operator": props.get("udbyder", props.get("operatoer", "")),
                    "address": props.get("vejnavn", ""),
                    "suburb": "Copenhagen", "state": "",
                    "latitude": coords[1] if len(coords) > 1 else "",
                    "longitude": coords[0] if len(coords) > 0 else "",
                    "power_kw": props.get("effekt", ""), "num_ports": props.get("antal_ladepunkter", ""),
                    "charger_type": "", "connector_type": "",
                })
            logger.info("    -> %d", len(features))
    except Exception as e:
        logger.warning("    Copenhagen failed: %s", e)

    # OSM Denmark
    try:
        logger.info("  DK: OSM...")
        r = osm_query("ISO3166-1", "DK")
        for rec in r:
            rec["source"] = "osm_dk"
        records.extend(r)
        logger.info("    -> %d", len(r))
    except Exception as e:
        logger.error("    OSM DK failed: %s", e)

    for r in records:
        r["country"] = "Denmark"
        r["country_code"] = "DK"
    return records


# ── SWITZERLAND ──
def collect_CH() -> list[dict]:
    records = []
    # Swiss Federal Office of Energy (BFE)
    try:
        logger.info("  CH: BFE / ich-tanke-strom...")
        resp = SESSION.get("https://data.geo.admin.ch/ch.bfe.ladestellen-elektromobilitaet/data/odata/ch.bfe.ladestellen-elektromobilitaet.ods?$format=json&$top=10000", timeout=120)
        if resp.status_code == 200:
            data = resp.json()
            items = data.get("value", data.get("d", {}).get("results", []))
            for item in items:
                records.append({
                    "source": "bfe_ch",
                    "station_name": item.get("StationName", item.get("City", "")),
                    "operator": item.get("OperatorName", item.get("Operator", "")),
                    "address": item.get("Address", ""),
                    "suburb": item.get("City", ""),
                    "state": item.get("Canton", ""),
                    "latitude": item.get("Latitude", ""),
                    "longitude": item.get("Longitude", ""),
                    "power_kw": item.get("MaxPower", ""),
                    "num_ports": item.get("NumberOfChargingPoints", ""),
                    "charger_type": "", "connector_type": "",
                })
            logger.info("    -> %d", len(items))
    except Exception as e:
        logger.warning("    BFE failed: %s", e)

    # OSM Switzerland
    try:
        logger.info("  CH: OSM...")
        r = osm_query("ISO3166-1", "CH")
        for rec in r:
            rec["source"] = "osm_ch"
        records.extend(r)
        logger.info("    -> %d", len(r))
    except Exception as e:
        logger.error("    OSM CH failed: %s", e)

    for r in records:
        r["country"] = "Switzerland"
        r["country_code"] = "CH"
    return records


# ── IRELAND ──
def collect_IE() -> list[dict]:
    records = []
    # ESB eCars
    try:
        logger.info("  IE: ESB eCars...")
        resp = SESSION.get("https://www.esb.ie/ecars/charge-point-map", timeout=30)
        # Try API endpoints
        for url in ["https://www.esb.ie/ecars/api/chargepoints", "https://api.esbecars.com/v1/chargepoints"]:
            try:
                resp = SESSION.get(url, timeout=30)
                if resp.status_code == 200:
                    data = resp.json()
                    points = data if isinstance(data, list) else data.get("data", data.get("chargepoints", []))
                    for p in points:
                        records.append({
                            "source": "esb_ie",
                            "station_name": p.get("name", ""),
                            "operator": "ESB eCars",
                            "address": p.get("address", ""),
                            "suburb": p.get("town", p.get("city", "")),
                            "state": p.get("county", ""),
                            "latitude": p.get("latitude", p.get("lat", "")),
                            "longitude": p.get("longitude", p.get("lng", "")),
                            "power_kw": p.get("maxPower", ""), "num_ports": p.get("connectorCount", ""),
                            "charger_type": "", "connector_type": "",
                        })
                    logger.info("    -> %d ESB", len(points))
                    break
            except Exception:
                continue
    except Exception as e:
        logger.warning("    ESB failed: %s", e)

    # OSM Ireland
    try:
        logger.info("  IE: OSM...")
        r = osm_query("ISO3166-1", "IE")
        for rec in r:
            rec["source"] = "osm_ie"
        records.extend(r)
        logger.info("    -> %d", len(r))
    except Exception as e:
        logger.error("    OSM IE failed: %s", e)

    for r in records:
        r["country"] = "Ireland"
        r["country_code"] = "IE"
    return records


# ── CANADA ──
def collect_CA() -> list[dict]:
    records = []
    # Montreal
    try:
        logger.info("  CA: Montreal...")
        resp = SESSION.get("https://donnees.montreal.ca/api/3/action/datastore_search?resource_id=2d13e6b8-6b86-4a7c-8e0f-6ea2e019ffe3&limit=5000", timeout=60)
        if resp.status_code == 200:
            for row in resp.json().get("result", {}).get("records", []):
                records.append({
                    "source": "montreal_ca",
                    "station_name": row.get("NOM_BORNE", row.get("NAME", "")),
                    "operator": row.get("RESEAU", row.get("NETWORK", "")),
                    "address": row.get("ADRESSE", row.get("ADDRESS", "")),
                    "suburb": "Montreal", "state": "QC",
                    "latitude": row.get("LATITUDE", ""), "longitude": row.get("LONGITUDE", ""),
                    "power_kw": row.get("PUISSANCE", ""), "num_ports": row.get("NB_BORNES", ""),
                    "charger_type": "", "connector_type": "",
                })
            logger.info("    -> %d Montreal", len([r for r in records if r["source"] == "montreal_ca"]))
    except Exception as e:
        logger.warning("    Montreal failed: %s", e)

    # Vancouver
    try:
        logger.info("  CA: Vancouver...")
        resp = SESSION.get("https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/electric-vehicle-charging-stations/records?limit=100", timeout=60)
        if resp.status_code == 200:
            for row in resp.json().get("results", []):
                geo = row.get("geo_point_2d", {}) or {}
                records.append({
                    "source": "vancouver_ca",
                    "station_name": row.get("name", ""),
                    "operator": row.get("operator", ""),
                    "address": row.get("address", ""),
                    "suburb": "Vancouver", "state": "BC",
                    "latitude": geo.get("lat", ""), "longitude": geo.get("lon", ""),
                    "power_kw": "", "num_ports": "",
                    "charger_type": "", "connector_type": "",
                })
            logger.info("    -> %d Vancouver", len([r for r in records if r["source"] == "vancouver_ca"]))
    except Exception as e:
        logger.warning("    Vancouver failed: %s", e)

    # OSM Canada
    try:
        logger.info("  CA: OSM...")
        r = osm_query("ISO3166-1", "CA")
        for rec in r:
            rec["source"] = "osm_ca"
        records.extend(r)
        logger.info("    -> %d", len(r))
    except Exception as e:
        logger.error("    OSM CA failed: %s", e)

    for r in records:
        r["country"] = "Canada"
        r["country_code"] = "CA"
    return records


# ── SINGAPORE ──
def collect_SG() -> list[dict]:
    records = []
    # LTA DataMall
    try:
        logger.info("  SG: LTA / data.gov.sg...")
        resp = SESSION.get("https://data.gov.sg/api/action/datastore_search?resource_id=85207289-6ae7-4a56-9066-e6090a3684a5&limit=5000", timeout=60)
        if resp.status_code == 200:
            for row in resp.json().get("result", {}).get("records", []):
                records.append({
                    "source": "lta_sg",
                    "station_name": row.get("Description", row.get("description", "")),
                    "operator": row.get("Operator", row.get("operator", "")),
                    "address": row.get("Address", row.get("address", "")),
                    "suburb": "", "state": "",
                    "latitude": row.get("Latitude", row.get("latitude", "")),
                    "longitude": row.get("Longitude", row.get("longitude", "")),
                    "power_kw": row.get("Power", ""), "num_ports": row.get("No. of Chargers", ""),
                    "charger_type": row.get("Type", ""), "connector_type": "",
                })
            logger.info("    -> %d", len(records))
    except Exception as e:
        logger.warning("    SG LTA failed: %s", e)

    # OSM Singapore
    try:
        logger.info("  SG: OSM...")
        r = osm_query("ISO3166-1", "SG")
        for rec in r:
            rec["source"] = "osm_sg"
        records.extend(r)
        logger.info("    -> %d", len(r))
    except Exception as e:
        logger.error("    OSM SG failed: %s", e)

    for r in records:
        r["country"] = "Singapore"
        r["country_code"] = "SG"
    return records


# ── JAPAN ──
def collect_JP() -> list[dict]:
    records = []
    # OSM Japan (primary free source)
    try:
        logger.info("  JP: OSM...")
        r = osm_query("ISO3166-1", "JP")
        for rec in r:
            rec["source"] = "osm_jp"
        records.extend(r)
        logger.info("    -> %d", len(r))
    except Exception as e:
        logger.error("    OSM JP failed: %s", e)

    for r in records:
        r["country"] = "Japan"
        r["country_code"] = "JP"
    return records


# ── SOUTH KOREA ──
def collect_KR() -> list[dict]:
    records = []
    # Korea data.go.kr (needs API key)
    kr_key = os.getenv("KOREA_DATA_API_KEY", "")
    if kr_key:
        try:
            logger.info("  KR: data.go.kr...")
            url = f"https://apis.data.go.kr/B552584/EvCharger/getChargerInfo?serviceKey={kr_key}&numOfRows=9999&pageNo=1&dataType=JSON"
            resp = SESSION.get(url, timeout=120)
            resp.raise_for_status()
            items = resp.json().get("items", resp.json().get("body", {}).get("items", []))
            if isinstance(items, dict):
                items = items.get("item", [])
            for item in items:
                records.append({
                    "source": "korea_gov",
                    "station_name": item.get("statNm", ""),
                    "operator": item.get("busiNm", ""),
                    "address": item.get("addr", ""),
                    "suburb": "", "state": "",
                    "latitude": item.get("lat", ""), "longitude": item.get("lng", ""),
                    "power_kw": item.get("output", ""), "num_ports": "",
                    "charger_type": item.get("chgerType", ""), "connector_type": "",
                })
            logger.info("    -> %d", len(items))
        except Exception as e:
            logger.warning("    Korea API failed: %s", e)

    # OSM Korea
    try:
        logger.info("  KR: OSM...")
        r = osm_query("ISO3166-1", "KR")
        for rec in r:
            rec["source"] = "osm_kr"
        records.extend(r)
        logger.info("    -> %d", len(r))
    except Exception as e:
        logger.error("    OSM KR failed: %s", e)

    for r in records:
        r["country"] = "South Korea"
        r["country_code"] = "KR"
    return records


# ── US (supplemental — main data comes from NREL/AFDC) ──
def collect_US() -> list[dict]:
    records = []
    # NREL AFDC (needs key)
    nrel_key = os.getenv("NREL_API_KEY", "")
    if nrel_key:
        try:
            logger.info("  US: NREL/AFDC...")
            url = f"https://developer.nrel.gov/api/alt-fuel-stations/v1.json?api_key={nrel_key}&fuel_type=ELEC&country=US&limit=all"
            resp = SESSION.get(url, timeout=300)
            resp.raise_for_status()
            stations = resp.json().get("fuel_stations", [])
            for s in stations:
                records.append({
                    "source": "nrel_us",
                    "station_name": s.get("station_name", ""),
                    "operator": s.get("ev_network", ""),
                    "address": s.get("street_address", ""),
                    "suburb": s.get("city", ""), "state": s.get("state", ""),
                    "latitude": s.get("latitude", ""), "longitude": s.get("longitude", ""),
                    "power_kw": "", "num_ports": s.get("ev_level2_evse_num", ""),
                    "charger_type": "", "connector_type": "",
                })
            logger.info("    -> %d", len(stations))
        except Exception as e:
            logger.warning("    NREL failed: %s", e)
    else:
        logger.info("  US: No NREL_API_KEY, using OSM only")

    # OSM US (just sample — full US is huge)
    # Skip full US OSM to avoid Overpass timeout, rely on NREL
    for r in records:
        r["country"] = "United States"
        r["country_code"] = "US"
    return records


# ═══════════════════════════════════════════════════
#  REGISTRY
# ═══════════════════════════════════════════════════

COUNTRY_COLLECTORS = {
    "AU": ("Australia", collect_AU),
    "NZ": ("New Zealand", collect_NZ),
    "UK": ("United Kingdom", collect_UK),
    "DE": ("Germany", collect_DE),
    "FR": ("France", collect_FR),
    "NL": ("Netherlands", collect_NL),
    "NO": ("Norway", collect_NO),
    "SE": ("Sweden", collect_SE),
    "DK": ("Denmark", collect_DK),
    "CH": ("Switzerland", collect_CH),
    "IE": ("Ireland", collect_IE),
    "CA": ("Canada", collect_CA),
    "SG": ("Singapore", collect_SG),
    "JP": ("Japan", collect_JP),
    "KR": ("South Korea", collect_KR),
    "US": ("United States", collect_US),
}


# ═══════════════════════════════════════════════════
#  NORMALIZE + DEDUP + MERGE (same logic as Australia script)
# ═══════════════════════════════════════════════════

def normalize_record(rec: dict) -> dict | None:
    try:
        lat = float(str(rec.get("latitude", "0")).replace(",", "."))
        lng = float(str(rec.get("longitude", "0")).replace(",", "."))
    except (ValueError, TypeError):
        return None
    if not (-90 <= lat <= 90 and -180 <= lng <= 180) or (lat == 0 and lng == 0):
        return None

    operator = (rec.get("operator", "") or "").strip() or "Unknown"
    name = (rec.get("station_name", "") or "").strip()
    charger_type = (rec.get("charger_type", "") or "").lower()
    connector = (rec.get("connector_type", "") or "").lower()

    try:
        pw = str(rec.get("power_kw", "") or "").replace(",", ".")
        power_val = float(pw.split("/")[0].split(";")[0].replace("kW", "").strip()) if pw else 0
    except (ValueError, TypeError):
        power_val = 0

    op_lower = operator.lower()
    if "tesla" in op_lower and ("supercharg" in op_lower or power_val >= 50):
        cat = "Tesla Supercharger"
    elif power_val >= 50 or "dc" in charger_type or "dc fast" in connector or "ccs" in connector or "chademo" in connector:
        cat = "DC Fast"
    elif power_val >= 3 or "level 2" in charger_type or "type 2" in connector or "type2" in connector:
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
        "lat": round(lat, 6), "lng": round(lng, 6),
        "name": name, "operator": operator,
        "address": (rec.get("address", "") or "").strip(),
        "suburb": (rec.get("suburb", "") or "").strip(),
        "city": (rec.get("city", "") or rec.get("suburb", "") or "").strip(),
        "state": (rec.get("state", "") or "").strip(),
        "postal_code": (rec.get("postal_code", "") or "").strip(),
        "country": rec.get("country", ""), "country_code": rec.get("country_code", ""),
        "connector_category": cat,
        "connector_type": (rec.get("connector_type", "") or connector).strip(),
        "power_kw": power_val,
        "num_ports": num_ports,
        "status": (rec.get("status", "") or "").strip(),
        "access_type": (rec.get("access_type", "") or "").strip(),
        "fee": (rec.get("fee", "") or "").strip(),
        "usage_cost": (rec.get("usage_cost", "") or "").strip(),
        "opening_hours": (rec.get("opening_hours", "") or "").strip(),
        "brand": (rec.get("brand", "") or "").strip(),
        "source": rec.get("source", "unknown"),
    }


def quality_score(rec: dict) -> int:
    s = 0
    if rec["name"]: s += 10
    if rec["operator"] != "Unknown": s += 5
    if rec["address"]: s += 3
    if rec["power_kw"] > 0: s += 2
    if rec["num_ports"]: s += 1
    return s


def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(min(1, math.sqrt(a)))


def deduplicate(records: list[dict], threshold_m: float = 100) -> list[dict]:
    grid: dict[tuple, list[dict]] = defaultdict(list)
    for rec in records:
        key = (round(rec["lat"] / GRID_SIZE), round(rec["lng"] / GRID_SIZE))
        grid[key].append(rec)

    unique = []
    used_coords = set()

    for key, cell_recs in grid.items():
        cell_recs.sort(key=quality_score, reverse=True)
        for rec in cell_recs:
            coord_key = (round(rec["lat"], 4), round(rec["lng"], 4))
            if coord_key in used_coords:
                continue
            is_dup = False
            for dx in range(-1, 2):
                if is_dup: break
                for dy in range(-1, 2):
                    if is_dup: break
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


def _build_properties(rec: dict, idx: int = 0) -> dict:
    """Build GeoJSON properties dict from a normalized record."""
    return {
        "station_id": f"{rec['country_code'].lower()}_{rec['source']}_{idx}",
        "station_name": rec["name"] or f"{rec['operator']} - {rec['city'] or rec['suburb'] or 'Station'}",
        "address": ", ".join(filter(None, [rec["address"], rec["city"] or rec["suburb"], rec["state"]])),
        "city": rec.get("city", "") or rec.get("suburb", ""),
        "state": rec.get("state", ""),
        "postal_code": rec.get("postal_code", ""),
        "operator": rec["operator"],
        "network": rec["operator"],
        "connector_types": rec.get("connector_type", "") or rec["connector_category"],
        "connector_category": rec["connector_category"],
        "total_ports": rec["num_ports"],
        "num_ports": str(rec["num_ports"]) if rec["num_ports"] else "",
        "power_kw": rec["power_kw"] if rec["power_kw"] > 0 else None,
        "status": rec.get("status", "") or "Operational",
        "access_type": rec.get("access_type", ""),
        "usage_cost": rec.get("usage_cost", ""),
        "opening_hours": rec.get("opening_hours", ""),
        "country": rec["country"],
        "country_code": rec["country_code"],
        "data_provider": rec["source"],
        "source": rec["source"],
    }


def _enrich_existing(existing_props: dict, new_props: dict) -> bool:
    """Fill in blank fields on an existing record from new data. Returns True if anything changed."""
    changed = False
    # Fields to enrich if currently empty/missing
    ENRICH_FIELDS = [
        "city", "state", "postal_code", "operator", "network",
        "connector_types", "access_type", "usage_cost", "opening_hours",
        "data_provider", "source",
    ]
    for field in ENRICH_FIELDS:
        old_val = existing_props.get(field, "") or ""
        new_val = new_props.get(field, "") or ""
        if not str(old_val).strip() and str(new_val).strip():
            existing_props[field] = new_val
            changed = True
    # Upgrade power_kw if existing is null/0 and new has value
    if not existing_props.get("power_kw") and new_props.get("power_kw"):
        existing_props["power_kw"] = new_props["power_kw"]
        changed = True
    return changed


def merge_into_geojson(new_stations: list[dict], dry_run: bool = False) -> int:
    if GZ_PATH.exists():
        with gzip.open(GZ_PATH, "rt", encoding="utf-8") as f:
            geojson = json.load(f)
    elif GEOJSON_PATH.exists():
        with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
            geojson = json.load(f)
    else:
        geojson = {"type": "FeatureCollection", "features": []}

    existing = geojson.get("features", [])
    logger.info("Existing features: %d", len(existing))

    # Build index of existing features by coordinate for enrichment
    coord_to_feat: dict[tuple, dict] = {}
    for feat in existing:
        coords = feat.get("geometry", {}).get("coordinates", [])
        if len(coords) >= 2:
            key = (round(coords[1], 4), round(coords[0], 4))
            coord_to_feat[key] = feat

    added = 0
    enriched = 0
    for rec in new_stations:
        coord_key = (round(rec["lat"], 4), round(rec["lng"], 4))
        new_props = _build_properties(rec, added)

        if coord_key in coord_to_feat:
            # Existing record — try to enrich with new data
            if _enrich_existing(coord_to_feat[coord_key]["properties"], new_props):
                enriched += 1
            continue

        # New record — add it
        feature = {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [rec["lng"], rec["lat"]]},
            "properties": new_props,
        }
        existing.append(feature)
        coord_to_feat[coord_key] = feature
        added += 1

    if dry_run:
        logger.info("[DRY RUN] Would add %d new, enrich %d existing (total would be %d)", added, enriched, len(existing))
        return added

    geojson["features"] = existing
    logger.info("Writing GeoJSON (%d features)...", len(existing))
    with open(GEOJSON_PATH, "w", encoding="utf-8") as f:
        json.dump(geojson, f)
    with gzip.open(GZ_PATH, "wt", encoding="utf-8") as f:
        json.dump(geojson, f)
    logger.info("Added %d new, enriched %d existing. Total: %d", added, enriched, len(existing))
    return added


def git_push():
    os.chdir(PROJECT_ROOT)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    subprocess.run(["git", "add", "frontend/data/ev_stations.geojson.gz"], check=True)
    subprocess.run(["git", "add", "frontend/data/ev_stations.geojson"], check=False)
    result = subprocess.run(["git", "diff", "--cached", "--quiet"], capture_output=True)
    if result.returncode == 0:
        logger.info("No changes to commit.")
        return
    subprocess.run(["git", "commit", "-m", f"Auto-update global EV data ({now})"], check=True)
    subprocess.run(["git", "push", "origin", "main"], check=True)
    logger.info("Pushed to GitHub — Render will auto-redeploy.")


# ═══════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Auto-update global EV charging data")
    parser.add_argument("--countries", nargs="+", default=list(COUNTRY_COLLECTORS.keys()),
                        help="Country codes to collect (default: all)")
    parser.add_argument("--push", action="store_true", help="Push to GitHub after merge")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    args = parser.parse_args()

    timestamp = datetime.now(timezone.utc).isoformat()
    logger.info("=" * 60)
    logger.info("Global EV Auto-Update — %s", timestamp)
    logger.info("Countries: %s", ", ".join(args.countries))
    logger.info("=" * 60)

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    all_normalized = []
    country_stats = {}

    for code in args.countries:
        code = code.upper()
        if code not in COUNTRY_COLLECTORS:
            logger.warning("Unknown country code: %s (available: %s)", code, ", ".join(COUNTRY_COLLECTORS))
            continue

        country_name, collector = COUNTRY_COLLECTORS[code]
        logger.info("\n── %s (%s) ──", country_name, code)
        try:
            raw = collector()
            # Save raw
            path = RAW_DIR / f"{code.lower()}_raw.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(raw, f, ensure_ascii=False, indent=2)

            # Normalize
            normalized = [n for r in raw if (n := normalize_record(r))]
            country_stats[code] = {"raw": len(raw), "valid": len(normalized)}
            all_normalized.extend(normalized)
            logger.info("  %s: %d raw -> %d valid", code, len(raw), len(normalized))
            time.sleep(2)  # Be polite between countries
        except Exception as e:
            logger.error("  %s FAILED: %s", code, e)
            country_stats[code] = {"raw": 0, "valid": 0}

    logger.info("\nTotal valid records: %d", len(all_normalized))

    # Deduplicate
    unique = deduplicate(all_normalized)
    logger.info("After dedup: %d unique stations", len(unique))

    # Top operators globally
    op_counts: dict[str, int] = defaultdict(int)
    for rec in unique:
        op_counts[rec["operator"]] += 1
    logger.info("\nTop 30 operators (global):")
    for op, cnt in sorted(op_counts.items(), key=lambda x: -x[1])[:30]:
        logger.info("  %-35s %5d", op, cnt)

    # Merge
    added = merge_into_geojson(unique, dry_run=args.dry_run)

    # Push
    if args.push and added > 0 and not args.dry_run:
        try:
            git_push()
        except Exception as e:
            logger.error("Git push failed: %s", e)

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    for code, stats in sorted(country_stats.items()):
        name = COUNTRY_COLLECTORS[code][0]
        logger.info("  %s %-20s  raw: %5d  valid: %5d", code, name, stats["raw"], stats["valid"])
    logger.info("  %-23s  unique: %5d  added: %5d", "TOTAL", len(unique), added)


if __name__ == "__main__":
    main()
