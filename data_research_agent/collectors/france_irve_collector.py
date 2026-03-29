"""France IRVE (Infrastructure de Recharge pour Véhicules Électriques) collector.

Uses the data.gouv.fr consolidated dataset — the official French government
open data for EV charging infrastructure. ~100k+ stations, updated daily.

Primary method: CSV download from data.gouv.fr stable redirect URL.
Fallback: GeoJSON download.
Source: https://www.data.gouv.fr/en/datasets/fichier-consolide-des-bornes-de-recharge-pour-vehicules-electriques/
"""

from __future__ import annotations

import csv
import io
from datetime import datetime

from models import CrawlState, RawRecord, SearchParams
from .base import BaseCollector


def _parse_irve_row(row: dict) -> dict:
    """Parse an IRVE CSV row into common schema."""
    # Coordinates are in "consolidated_longitude" / "consolidated_latitude"
    # or "[lng, lat]" in coordonneesXY
    lat = None
    lng = None

    try:
        lat = float(row.get("consolidated_latitude", ""))
        lng = float(row.get("consolidated_longitude", ""))
    except (ValueError, TypeError):
        # Try parsing coordonneesXY "[lng, lat]"
        coord_str = row.get("coordonneesXY", "")
        if coord_str and "," in coord_str:
            coord_str = coord_str.strip("[] ")
            parts = coord_str.split(",")
            if len(parts) == 2:
                try:
                    lng = float(parts[0].strip())
                    lat = float(parts[1].strip())
                except (ValueError, TypeError):
                    pass

    # Build connector types from boolean columns
    connectors = []
    connector_map = {
        "prise_type_2": "Type 2",
        "prise_type_combo_ccs": "CCS",
        "prise_type_chademo": "CHAdeMO",
        "prise_type_ef": "Type E/F (Schuko)",
        "prise_type_autre": "Other",
    }
    for field, label in connector_map.items():
        val = row.get(field, "").strip().lower()
        if val in ("true", "1", "oui", "yes"):
            connectors.append(label)

    # Power
    power_kw = None
    try:
        power_kw = float(row.get("puissance_nominale", ""))
    except (ValueError, TypeError):
        pass

    # Free or paid
    gratuit = row.get("gratuit", "").strip().lower()
    if gratuit in ("true", "1", "oui"):
        usage_cost = "Free"
    elif row.get("paiement_acte", "").strip().lower() in ("true", "1", "oui"):
        usage_cost = "Pay per use"
    else:
        usage_cost = ""

    return {
        "station_id": row.get("id_station_itinerance", row.get("id_pdc_itinerance", "")),
        "station_name": row.get("nom_station", ""),
        "address": row.get("adresse_station", ""),
        "city": row.get("consolidated_commune", row.get("nom_commune", "")),
        "state": row.get("consolidated_region", ""),
        "country": "France",
        "country_code": "FR",
        "postal_code": row.get("consolidated_code_postal", row.get("code_insee_commune", "")),
        "latitude": lat,
        "longitude": lng,
        "network": row.get("nom_enseigne", ""),
        "operator": row.get("nom_operateur", ""),
        "connector_types": ", ".join(connectors),
        "num_ports": row.get("nbre_pdc"),
        "power_kw": power_kw,
        "status": row.get("consolidated_is_open", ""),
        "access_type": row.get("condition_acces", ""),
        "usage_cost": usage_cost,
        "facility_type": row.get("implantation_station", ""),
        "date_opened": row.get("date_mise_en_service", ""),
        "date_last_updated": row.get("date_maj", ""),
        "data_provider": "data.gouv.fr IRVE",
    }


class FranceIRVECollector(BaseCollector):
    """Collector for France IRVE consolidated CSV dataset."""

    def fetch_batch(self, params: SearchParams, limit: int | None = None) -> list[RawRecord]:
        self.logger.info("[france_irve] Downloading consolidated IRVE dataset")

        response = self._make_request(
            self.config.base_url,
            timeout=120,
        )

        # Parse CSV from response text
        text = response.text
        reader = csv.DictReader(io.StringIO(text))

        records: list[RawRecord] = []
        for row in reader:
            parsed = _parse_irve_row(row)
            # Skip rows without coordinates
            if parsed["latitude"] is None or parsed["longitude"] is None:
                continue

            records.append(RawRecord(
                source="france_irve",
                raw_data=parsed,
                source_url="https://www.data.gouv.fr/en/datasets/fichier-consolide-des-bornes-de-recharge-pour-vehicules-electriques/",
            ))

            if limit and len(records) >= limit:
                break

        self.logger.info("[france_irve] Parsed %d stations from CSV", len(records))
        return records

    def fetch_incremental(
        self, state: CrawlState, max_records: int = 500
    ) -> tuple[list[RawRecord], CrawlState]:
        # CSV is a full dump; fetch all and deduplicate downstream
        records = self.fetch_batch(SearchParams(), limit=max_records)
        new_state = CrawlState(
            source_name=self.config.name,
            last_run_at=datetime.utcnow().isoformat(),
        )
        return records, new_state
