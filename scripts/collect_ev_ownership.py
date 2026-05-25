#!/usr/bin/env python3
"""Collect EV ownership/registration data by geographic area for supply-demand gap analysis.

Sources (postcode/ZIP-level where available):
  - US: Washington State EV registrations (by ZIP), California DMV (by ZIP),
        Connecticut, Maryland, Colorado, New York (by county)
  - UK: DfT VEH0145 — Plug-in vehicles by LSOA (mapped to postcode)
  - AU: Data.gov.au — Registered vehicles by postcode & fuel type
  - IEA: Global EV stock by country (national-level fallback)

Output:
  frontend/data/ev_ownership.json — aggregated EV counts per geographic area
  Used by the frontend to show supply-demand heatmaps

Usage:
    python scripts/collect_ev_ownership.py                  # Collect all
    python scripts/collect_ev_ownership.py --countries US    # US only
    python scripts/collect_ev_ownership.py --dry-run         # Preview
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
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import requests

# ── Config ──
PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = PROJECT_ROOT / "frontend" / "data" / "ev_ownership.json"
GEOJSON_PATH = PROJECT_ROOT / "frontend" / "data" / "ev_stations.geojson"
GZ_PATH = PROJECT_ROOT / "frontend" / "data" / "ev_stations.geojson.gz"
LOG_DIR = PROJECT_ROOT / "scripts" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "ev_ownership.log"),
    ],
)
logger = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "UnearthAI-DataResearch/1.0"})


# ═══════════════════════════════════════════════════
#  DATA COLLECTORS — EV REGISTRATIONS BY AREA
# ═══════════════════════════════════════════════════

def collect_us_washington() -> list[dict]:
    """Washington State — EV population by ZIP code (~180K records)."""
    logger.info("  US-WA: Fetching Washington State EV registrations by ZIP...")
    url = "https://data.wa.gov/api/views/f6w7-q2d2/rows.csv?accessType=DOWNLOAD"
    try:
        resp = SESSION.get(url, timeout=120, stream=True)
        resp.raise_for_status()
        # Stream to avoid memory issues — aggregate by ZIP
        zip_counts = defaultdict(lambda: {"bev": 0, "phev": 0, "total": 0})
        reader = csv.DictReader(io.StringIO(resp.text))
        for row in reader:
            zip_code = (row.get("Postal Code", row.get("zip_code", "")) or "").strip()[:5]
            if not zip_code or not zip_code.isdigit():
                continue
            ev_type = (row.get("Electric Vehicle Type", row.get("ev_type", "")) or "").upper()
            if "BATTERY" in ev_type or "BEV" in ev_type:
                zip_counts[zip_code]["bev"] += 1
            elif "PLUG" in ev_type or "PHEV" in ev_type:
                zip_counts[zip_code]["phev"] += 1
            zip_counts[zip_code]["total"] += 1

        records = []
        for zip_code, counts in zip_counts.items():
            records.append({
                "area_type": "zip",
                "area_code": zip_code,
                "area_name": f"ZIP {zip_code}",
                "country_code": "US",
                "state": "WA",
                "ev_count": counts["total"],
                "bev_count": counts["bev"],
                "phev_count": counts["phev"],
                "source": "wa_dol",
                "year": 2026,
            })
        logger.info("    -> %d ZIP areas, %d total EVs", len(records),
                     sum(r["ev_count"] for r in records))
        return records
    except Exception as e:
        logger.error("    WA failed: %s", e)
        return []


def collect_us_california() -> list[dict]:
    """California DMV — Vehicle fuel type count by ZIP code."""
    logger.info("  US-CA: Fetching California DMV EV registrations by ZIP...")
    # Try latest year first, fall back
    urls = [
        ("2024", "https://data.ca.gov/dataset/15179472-adeb-4df6-920a-20640d02b08c/resource/66b0121e-5eab-4fcf-aa0d-2b1dfb5510ab/download/vehicle-fuel-type-counts-2024.csv"),
        ("2024_alt", "https://data.ca.gov/dataset/vehicle-fuel-type-count-by-zip-code/resource/66b0121e-5eab-4fcf-aa0d-2b1dfb5510ab/download/vehicle-fuel-type-count-by-zip-code.csv"),
    ]
    for year, url in urls:
        try:
            resp = SESSION.get(url, timeout=120)
            if resp.status_code == 200:
                zip_counts = defaultdict(lambda: {"bev": 0, "phev": 0, "total": 0})
                # Log first few lines to understand schema
                lines = resp.text.split("\n")
                if lines:
                    logger.info("    CA CSV header: %s", lines[0][:200])
                reader = csv.DictReader(io.StringIO(resp.text))
                for row in reader:
                    fuel = (row.get("Fuel", row.get("fuel", row.get("Fuel Type", ""))) or "").upper()
                    zip_code = (row.get("ZIP Code", row.get("Zip Code", row.get("zip_code", ""))) or "").strip()[:5]
                    if not zip_code or not zip_code.isdigit():
                        continue
                    count = 0
                    try:
                        count = int(row.get("Vehicles", row.get("vehicles", row.get("Number of Vehicles", row.get("Count", 0)))) or 0)
                    except (ValueError, TypeError):
                        continue
                    if "BATTERY" in fuel or "BEV" in fuel or fuel == "ELECTRIC":
                        zip_counts[zip_code]["bev"] += count
                        zip_counts[zip_code]["total"] += count
                    elif "PLUG" in fuel or "PHEV" in fuel:
                        zip_counts[zip_code]["phev"] += count
                        zip_counts[zip_code]["total"] += count

                records = []
                for zip_code, counts in zip_counts.items():
                    if counts["total"] > 0:
                        records.append({
                            "area_type": "zip",
                            "area_code": zip_code,
                            "area_name": f"ZIP {zip_code}",
                            "country_code": "US",
                            "state": "CA",
                            "ev_count": counts["total"],
                            "bev_count": counts["bev"],
                            "phev_count": counts["phev"],
                            "source": "ca_dmv",
                            "year": int(year),
                        })
                logger.info("    -> %d ZIP areas (%s), %d total EVs", len(records), year,
                             sum(r["ev_count"] for r in records))
                return records
        except Exception as e:
            logger.warning("    CA %s failed: %s", year, e)
    return []


def collect_us_multistate() -> list[dict]:
    """Collect from other US states that publish EV data on data.gov/Socrata."""
    records = []

    # Connecticut — EV registrations via Socrata JSON API (aggregate by city+type)
    try:
        logger.info("  US-CT: Connecticut EV registrations...")
        url = ("https://data.ct.gov/resource/y7ky-5wcz.json"
               "?$select=primarycustomercity,type,count(*) as cnt"
               "&$group=primarycustomercity,type"
               "&$limit=50000")
        resp = SESSION.get(url, timeout=60)
        if resp.status_code == 200:
            city_counts = defaultdict(lambda: {"bev": 0, "phev": 0, "total": 0})
            for row in resp.json():
                city = (row.get("primarycustomercity", "") or "").strip()
                if not city:
                    continue
                ev_type = (row.get("type", "") or "").upper()
                cnt = int(row.get("cnt", 0) or 0)
                if ev_type in ("BEV", "BEMC", "FCEV"):
                    city_counts[city]["bev"] += cnt
                elif ev_type == "PHEV":
                    city_counts[city]["phev"] += cnt
                city_counts[city]["total"] += cnt

            for city, counts in city_counts.items():
                records.append({
                    "area_type": "city",
                    "area_code": f"CT_{city}",
                    "area_name": f"{city.title()}, CT",
                    "country_code": "US",
                    "state": "CT",
                    "ev_count": counts["total"],
                    "bev_count": counts["bev"],
                    "phev_count": counts["phev"],
                    "source": "ct_dmv",
                    "year": 2025,
                })
            logger.info("    -> %d CT cities, %d EVs", len(city_counts),
                         sum(c["total"] for c in city_counts.values()))
        else:
            logger.warning("    CT API returned %d", resp.status_code)
    except Exception as e:
        logger.warning("    CT failed: %s", e)

    # New York — EV registrations via Socrata JSON API (aggregate by ZIP)
    # Field: fuel_type = 'ELECTRIC' (uppercase); zip = ZIP code
    try:
        logger.info("  US-NY: New York EV registrations (aggregated API)...")
        # Use SoQL to aggregate by ZIP directly — much faster than fetching all rows
        url = ("https://data.ny.gov/resource/w4pv-hbkt.json"
               "?$select=zip,count(*) as cnt"
               "&$where=fuel_type='ELECTRIC'"
               "&$group=zip"
               "&$limit=50000")
        resp = SESSION.get(url, timeout=120)
        if resp.status_code == 200:
            zip_counts = {}
            for row in resp.json():
                zip_code = (row.get("zip", "") or "").strip()[:5]
                if not zip_code or not zip_code.isdigit():
                    continue
                cnt = int(row.get("cnt", 0) or 0)
                zip_counts[zip_code] = {"bev": cnt, "phev": 0, "total": cnt}

            for zip_code, counts in zip_counts.items():
                records.append({
                    "area_type": "zip",
                    "area_code": zip_code,
                    "area_name": f"ZIP {zip_code}",
                    "country_code": "US",
                    "state": "NY",
                    "ev_count": counts["total"],
                    "bev_count": counts["bev"],
                    "phev_count": counts["phev"],
                    "source": "ny_dmv",
                    "year": 2025,
                })
            logger.info("    -> %d NY ZIP areas, %d EVs", len(zip_counts),
                         sum(c["total"] for c in zip_counts.values()))
        else:
            logger.warning("    NY API returned %d: %s", resp.status_code, resp.text[:200])
    except Exception as e:
        logger.warning("    NY failed: %s", e)

    # Colorado — no longer available as open data (Socrata dataset removed)
    # EValuateCO dashboard exists but has no public API
    logger.info("  US-CO: Skipped (no public API available)")

    return records


def collect_uk() -> list[dict]:
    """UK DfT VEH0145 — Plug-in vehicles by LSOA, aggregated to local authority.

    Format is wide: LSOA21CD, LSOA21NM, Fuel, Keepership, 2025 Q4, 2025 Q3, ...
    Values are EV counts per LSOA per quarter. We take the latest quarter (first data column).
    Values of '[c]' mean suppressed for confidentiality — treat as 0.
    """
    logger.info("  UK: Fetching DfT VEH0145 (plug-in vehicles by LSOA)...")
    url = "https://assets.publishing.service.gov.uk/media/69ef3dcf08ecdb5c6f34afaa/df_VEH0145.csv"
    try:
        resp = SESSION.get(url, timeout=300)
        resp.raise_for_status()

        text = resp.content.decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(text))

        # Get the latest quarter column name (first quarter column after Keepership)
        fieldnames = reader.fieldnames or []
        quarter_cols = [c for c in fieldnames if "Q" in c and any(y in c for y in ["2025", "2024", "2023"])]
        latest_quarter = quarter_cols[0] if quarter_cols else ""
        logger.info("    Using latest quarter column: %s", latest_quarter)

        if not latest_quarter:
            logger.warning("    No quarter columns found in VEH0145")
            return []

        # Aggregate by local authority (extracted from LSOA name)
        # LSOA21NM format: "City of London 001A" or "Barking and Dagenham 015A"
        # IMPORTANT: Exclude Fuel="Total" and Keepership="Total"/"DISPOSAL" to avoid double-counting
        la_counts = defaultdict(lambda: {"bev": 0, "phev": 0, "total": 0})

        for row in reader:
            lsoa_name = row.get("LSOA21NM", "")
            fuel = (row.get("Fuel", "") or "").upper()
            keepership = (row.get("Keepership", "") or "").upper()
            count_str = row.get(latest_quarter, "0")

            # Skip aggregate/double-count rows
            if "TOTAL" in fuel or keepership in ("TOTAL", "DISPOSAL"):
                continue

            # Handle suppressed values '[c]' and non-numeric
            try:
                count = int(count_str)
            except (ValueError, TypeError):
                count = 0

            if count <= 0:
                continue

            # Extract local authority name from LSOA name (remove trailing code like "001A")
            la_name = " ".join(lsoa_name.split()[:-1]) if lsoa_name else "Unknown"
            if not la_name:
                la_name = "Unknown"

            if "BATTERY" in fuel:
                la_counts[la_name]["bev"] += count
            elif "PLUG" in fuel or "RANGE" in fuel:
                la_counts[la_name]["phev"] += count
            la_counts[la_name]["total"] += count

        records = []
        for la_name, counts in la_counts.items():
            if counts["total"] > 0 and la_name != "Unknown":
                records.append({
                    "area_type": "local_authority",
                    "area_code": la_name,
                    "area_name": la_name,
                    "country_code": "GB",
                    "state": "",
                    "ev_count": counts["total"],
                    "bev_count": counts["bev"],
                    "phev_count": counts["phev"],
                    "source": "dft_veh0145",
                    "year": 2025,
                })
        logger.info("    -> %d local authorities, %d total EVs", len(records),
                     sum(r["ev_count"] for r in records))
        return records
    except Exception as e:
        logger.error("    UK VEH0145 failed: %s", e)
        return []


def collect_australia() -> list[dict]:
    """Australia — Registered vehicles by postcode from Data.gov.au (Jan 2025 census)."""
    logger.info("  AU: Fetching registered vehicles by postcode (Jan 2025)...")
    # Road vehicles Australia, January 2025 — by vehicle type, state, postcode, motive power
    # Columns: vehicle_type, state_abb, registered_postcode, motive_power, no_vehicles
    download_url = (
        "https://data.gov.au/data/dataset/f6e0a290-7d47-4b88-ac3b-34824b0ab334/"
        "resource/0271e694-0f99-4db4-a397-1d2c48f0dcc1/download/"
        "rva-2025-mvs-vehtype-streg-poareg-mtvpwr-rpc.csv"
    )

    try:
        resp = SESSION.get(download_url, timeout=300)
        resp.raise_for_status()

        text = resp.content.decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(text))

        postcode_counts = defaultdict(lambda: {"bev": 0, "phev": 0, "total": 0, "state": ""})
        for row in reader:
            motive = (row.get("motive_power", "") or "").strip().upper()
            postcode = (row.get("registered_postcode", "") or "").strip()
            state = (row.get("state_abb", "") or "").strip()
            if not postcode:
                continue

            try:
                count = int(row.get("no_vehicles", 0) or 0)
            except (ValueError, TypeError):
                count = 0
            if count <= 0:
                continue

            # "Battery/Fuel-cell electric" = BEV+FCEV
            # "Hybrid electric" = HEV (non-plug-in) — excluded
            # AU data does not separate PHEV from HEV
            if motive == "BATTERY/FUEL-CELL ELECTRIC":
                postcode_counts[postcode]["bev"] += count
                postcode_counts[postcode]["total"] += count

            if state and not postcode_counts[postcode]["state"]:
                postcode_counts[postcode]["state"] = state

        records = []
        for postcode, counts in postcode_counts.items():
            if counts["total"] > 0:
                records.append({
                    "area_type": "postcode",
                    "area_code": postcode,
                    "area_name": f"Postcode {postcode}",
                    "country_code": "AU",
                    "state": counts["state"],
                    "ev_count": counts["total"],
                    "bev_count": counts["bev"],
                    "phev_count": counts["phev"],
                    "source": "abs_rva_2025",
                    "year": 2025,
                })
        logger.info("    -> %d postcodes, %d total EVs", len(records),
                     sum(r["ev_count"] for r in records))
        return records

    except Exception as e:
        logger.error("    AU data.gov.au failed: %s", e)
        return []


def collect_iea_global() -> list[dict]:
    """IEA Global EV Data — national-level EV stock by country (fallback)."""
    logger.info("  IEA: Fetching global EV stock data...")
    # IEA provides downloadable CSV from their data explorer
    url = "https://api.iea.org/evs?parameters=EV+stock&category=Historical&mode=Cars&csv=true"
    try:
        resp = SESSION.get(url, timeout=60, headers={"Accept": "text/csv"})
        if resp.status_code == 200 and len(resp.text) > 100:
            reader = csv.DictReader(io.StringIO(resp.text))
            # Filter to EV stock with unit=Vehicles only, separate by powertrain
            # IMPORTANT: powertrain "EV" = total (BEV+PHEV+FCEV), so only use BEV/PHEV rows
            # to avoid double-counting
            country_data = {}  # region -> year -> {bev, phev, total}
            for row in reader:
                region = row.get("region", row.get("Region", ""))
                param = row.get("parameter", "")
                unit = row.get("unit", "")
                year = int(row.get("year", row.get("Year", 0)) or 0)
                value = float(row.get("value", row.get("Value", 0)) or 0)
                powertrain = row.get("powertrain", row.get("Powertrain", ""))

                # Only EV stock in vehicles
                if param != "EV stock" or unit != "Vehicles":
                    continue
                if not region or year <= 0 or value <= 0:
                    continue

                key = (region, year)
                if key not in country_data:
                    country_data[key] = {"bev": 0, "phev": 0, "total": 0}

                pt = powertrain.upper()
                if pt == "BEV":
                    country_data[key]["bev"] = int(value)
                elif pt == "PHEV":
                    country_data[key]["phev"] = int(value)
                elif pt == "EV":
                    # "EV" = total (BEV + PHEV + FCEV), use as the total
                    country_data[key]["total"] = int(value)

            # Keep only latest year per country
            latest_by_country = {}
            for (region, year), counts in country_data.items():
                # If total wasn't set by "EV" row, compute from BEV+PHEV
                if counts["total"] == 0:
                    counts["total"] = counts["bev"] + counts["phev"]
                if region not in latest_by_country or year > latest_by_country[region]["year"]:
                    latest_by_country[region] = {**counts, "year": year}

            records = []
            # Map IEA region names to ISO codes
            iea_to_iso = {
                "Australia": "AU", "New Zealand": "NZ", "United Kingdom": "GB",
                "Germany": "DE", "France": "FR", "Netherlands": "NL",
                "Norway": "NO", "Sweden": "SE", "Denmark": "DK",
                "Switzerland": "CH", "Ireland": "IE", "Canada": "CA",
                "Singapore": "SG", "Japan": "JP", "Korea": "KR",
                "United States": "US", "China": "CN", "India": "IN",
                "Brazil": "BR", "Italy": "IT", "Spain": "ES",
                "Belgium": "BE", "Austria": "AT", "Finland": "FI",
                "Portugal": "PT", "Poland": "PL",
            }
            for region, data in latest_by_country.items():
                iso = iea_to_iso.get(region, "")
                records.append({
                    "area_type": "country",
                    "area_code": iso or region,
                    "area_name": region,
                    "country_code": iso or region,
                    "state": "",
                    "ev_count": data["total"],
                    "bev_count": data["bev"],
                    "phev_count": data["phev"],
                    "source": "iea_gevo",
                    "year": data["year"],
                })
            logger.info("    -> %d countries", len(records))
            return records
    except Exception as e:
        logger.warning("    IEA API failed: %s", e)

    # Fallback: hard-coded IEA data (2024 actuals from Global EV Outlook 2025)
    logger.info("    Using IEA 2024 actuals...")
    iea_2024 = [
        ("CN", "China", 32000000, 24000000, 8000000),
        ("US", "United States", 6300000, 4700000, 1600000),
        ("DE", "Germany", 2800000, 1700000, 1100000),
        ("FR", "France", 2100000, 1400000, 700000),
        ("GB", "United Kingdom", 1700000, 1200000, 500000),
        ("NO", "Norway", 1100000, 890000, 210000),
        ("NL", "Netherlands", 750000, 450000, 300000),
        ("SE", "Sweden", 700000, 420000, 280000),
        ("CA", "Canada", 650000, 450000, 200000),
        ("KR", "Korea", 690000, 630000, 60000),
        ("JP", "Japan", 530000, 370000, 160000),
        ("DK", "Denmark", 350000, 250000, 100000),
        ("AU", "Australia", 300000, 230000, 70000),
        ("CH", "Switzerland", 290000, 190000, 100000),
        ("IE", "Ireland", 130000, 90000, 40000),
        ("NZ", "New Zealand", 100000, 70000, 30000),
        ("SG", "Singapore", 40000, 33000, 7000),
    ]
    records = []
    for iso, name, total, bev, phev in iea_2024:
        records.append({
            "area_type": "country",
            "area_code": iso,
            "area_name": name,
            "country_code": iso,
            "state": "",
            "ev_count": total,
            "bev_count": bev,
            "phev_count": phev,
            "source": "iea_2024_est",
            "year": 2024,
        })
    logger.info("    -> %d countries (estimates)", len(records))
    return records


# ═══════════════════════════════════════════════════
#  SUPPLY-DEMAND GAP ANALYSIS
# ═══════════════════════════════════════════════════

def load_station_counts() -> dict:
    """Load EV charger station counts by geographic area from GeoJSON."""
    logger.info("Loading station counts from GeoJSON...")
    path = GZ_PATH if GZ_PATH.exists() else GEOJSON_PATH
    if not path.exists():
        logger.warning("No GeoJSON found at %s", path)
        return {}

    opener = gzip.open if str(path).endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as f:
        data = json.load(f)

    # Count stations by various area groupings
    counts = {
        "by_zip": defaultdict(lambda: {"stations": 0, "ports": 0, "dc_fast": 0}),
        "by_postcode": defaultdict(lambda: {"stations": 0, "ports": 0, "dc_fast": 0}),
        "by_country": defaultdict(lambda: {"stations": 0, "ports": 0, "dc_fast": 0}),
        "by_state": defaultdict(lambda: {"stations": 0, "ports": 0, "dc_fast": 0}),
    }

    for feat in data.get("features", []):
        props = feat.get("properties", {})
        cc = props.get("country_code", "")
        postal = props.get("postal_code", "")
        state = props.get("state", "")
        category = props.get("connector_category", "")
        ports = 0
        try:
            ports = int(props.get("total_ports") or props.get("num_ports") or 0)
        except (ValueError, TypeError):
            pass

        is_dc = category in ("DC Fast", "Tesla Supercharger")

        # Country level
        if cc:
            counts["by_country"][cc]["stations"] += 1
            counts["by_country"][cc]["ports"] += ports
            if is_dc:
                counts["by_country"][cc]["dc_fast"] += 1

        # State level (for US/AU)
        if state and cc in ("US", "AU"):
            key = f"{cc}_{state}"
            counts["by_state"][key]["stations"] += 1
            counts["by_state"][key]["ports"] += ports
            if is_dc:
                counts["by_state"][key]["dc_fast"] += 1

        # Postal code level
        if postal:
            if cc == "US":
                zip5 = postal[:5]
                counts["by_zip"][zip5]["stations"] += 1
                counts["by_zip"][zip5]["ports"] += ports
                if is_dc:
                    counts["by_zip"][zip5]["dc_fast"] += 1
            else:
                counts["by_postcode"][f"{cc}_{postal}"]["stations"] += 1
                counts["by_postcode"][f"{cc}_{postal}"]["ports"] += ports
                if is_dc:
                    counts["by_postcode"][f"{cc}_{postal}"]["dc_fast"] += 1

    logger.info("  Stations by country: %d countries", len(counts["by_country"]))
    logger.info("  Stations by ZIP: %d ZIPs", len(counts["by_zip"]))
    logger.info("  Stations by postcode: %d postcodes", len(counts["by_postcode"]))
    return counts


def compute_gap_analysis(ownership: list[dict], station_counts: dict) -> list[dict]:
    """Compute supply-demand gap for each area."""
    results = []

    for rec in ownership:
        area_type = rec["area_type"]
        area_code = rec["area_code"]
        cc = rec["country_code"]
        ev_count = rec["ev_count"]

        # Find matching station count
        stations = 0
        ports = 0
        dc_fast = 0

        if area_type == "zip" and area_code in station_counts["by_zip"]:
            sc = station_counts["by_zip"][area_code]
            stations = sc["stations"]
            ports = sc["ports"]
            dc_fast = sc["dc_fast"]
        elif area_type == "postcode":
            key = f"{cc}_{area_code}"
            if key in station_counts["by_postcode"]:
                sc = station_counts["by_postcode"][key]
                stations = sc["stations"]
                ports = sc["ports"]
                dc_fast = sc["dc_fast"]
        elif area_type == "country" and cc in station_counts["by_country"]:
            sc = station_counts["by_country"][cc]
            stations = sc["stations"]
            ports = sc["ports"]
            dc_fast = sc["dc_fast"]
        elif area_type in ("county", "local_authority"):
            # Try state-level matching
            state = rec.get("state", "")
            key = f"{cc}_{state}"
            if key in station_counts["by_state"]:
                sc = station_counts["by_state"][key]
                stations = sc["stations"]
                ports = sc["ports"]
                dc_fast = sc["dc_fast"]

        # Compute ratios
        evs_per_station = round(ev_count / stations, 1) if stations > 0 else None
        evs_per_port = round(ev_count / ports, 1) if ports > 0 else None

        # Gap score: higher = more underserved
        # Industry benchmark: ~20 EVs per public charger is considered adequate
        # >50 EVs per charger = underserved, <10 = well-served
        BENCHMARK = 20  # EVs per public charger
        if evs_per_station is not None:
            gap_score = round(evs_per_station / BENCHMARK, 2)
        else:
            gap_score = None  # No stations = infinite gap

        result = {
            **rec,
            "stations": stations,
            "ports": ports,
            "dc_fast_stations": dc_fast,
            "evs_per_station": evs_per_station,
            "evs_per_port": evs_per_port,
            "gap_score": gap_score,
            "gap_category": (
                "critical" if gap_score is None and ev_count > 0 else
                "critical" if gap_score and gap_score > 5 else
                "underserved" if gap_score and gap_score > 2.5 else
                "adequate" if gap_score and gap_score >= 0.5 else
                "well_served" if gap_score and gap_score < 0.5 else
                "no_data"
            ),
        }
        results.append(result)

    return results


# ═══════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════

COLLECTORS = {
    "US": [
        ("Washington State", collect_us_washington),
        ("California", collect_us_california),
        ("Multi-State (CT, NY, CO)", collect_us_multistate),
    ],
    "UK": [("DfT VEH0145", collect_uk)],
    "AU": [("Data.gov.au", collect_australia)],
    "IEA": [("IEA Global", collect_iea_global)],
}


def main():
    parser = argparse.ArgumentParser(description="Collect EV ownership data for supply-demand analysis")
    parser.add_argument("--countries", nargs="+", default=list(COLLECTORS.keys()),
                        help="Which collectors to run (US, UK, AU, IEA)")
    parser.add_argument("--dry-run", action="store_true", help="Preview only, don't write output")
    args = parser.parse_args()

    timestamp = datetime.now(timezone.utc).isoformat()
    logger.info("=" * 60)
    logger.info("EV Ownership Data Collection — %s", timestamp)
    logger.info("Collectors: %s", ", ".join(args.countries))
    logger.info("=" * 60)

    all_ownership = []
    for code in args.countries:
        code = code.upper()
        if code not in COLLECTORS:
            logger.warning("Unknown collector: %s", code)
            continue
        for name, collector in COLLECTORS[code]:
            logger.info("\n── %s ──", name)
            try:
                records = collector()
                all_ownership.extend(records)
            except Exception as e:
                logger.error("  %s FAILED: %s", name, e)

    logger.info("\n" + "=" * 60)
    logger.info("Total ownership records: %d", len(all_ownership))
    total_evs = sum(r["ev_count"] for r in all_ownership)
    logger.info("Total EVs tracked: %s", f"{total_evs:,}")

    # Load station counts and compute gap
    station_counts = load_station_counts()
    gap_results = compute_gap_analysis(all_ownership, station_counts)

    # Summary stats
    categories = defaultdict(int)
    for r in gap_results:
        categories[r["gap_category"]] += 1
    logger.info("\nGap Analysis Summary:")
    for cat, cnt in sorted(categories.items(), key=lambda x: -x[1]):
        logger.info("  %-15s %5d areas", cat, cnt)

    # Top underserved areas
    underserved = sorted(
        [r for r in gap_results if r["gap_score"] and r["gap_score"] > 2.5 and r["ev_count"] > 50],
        key=lambda x: -(x["gap_score"] or 0)
    )
    if underserved:
        logger.info("\nTop 20 most underserved areas (EVs per station vs benchmark):")
        for r in underserved[:20]:
            logger.info("  %s (%s) — %d EVs, %d stations, %.0f EVs/station (%.1fx benchmark)",
                         r["area_name"], r["country_code"],
                         r["ev_count"], r["stations"],
                         r["evs_per_station"] or 0, r["gap_score"] or 0)

    if args.dry_run:
        logger.info("\n[DRY RUN] Would write %d records to %s", len(gap_results), OUTPUT_PATH)
        return

    # Write output
    output = {
        "generated_at": timestamp,
        "total_areas": len(gap_results),
        "total_evs_tracked": total_evs,
        "benchmark_evs_per_charger": 20,
        "gap_categories": dict(categories),
        "areas": gap_results,
    }
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    logger.info("\nWrote %d records to %s (%.1f MB)",
                 len(gap_results), OUTPUT_PATH, OUTPUT_PATH.stat().st_size / 1024 / 1024)


if __name__ == "__main__":
    main()
