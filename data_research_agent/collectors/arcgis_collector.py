"""ArcGIS REST API collector for Bundesnetzagentur (Germany) EV charging data.

Uses the ArcGIS FeatureServer query endpoint with offset pagination.
No auth required on the public endpoint.

Endpoint: services2.arcgis.com/jUpNdisbWqRpMo35/arcgis/rest/services/
          Ladesaeulen_in_Deutschland/FeatureServer/0/query
"""

from __future__ import annotations

from datetime import datetime

from models import CrawlState, RawRecord, SearchParams
from .base import BaseCollector


def _parse_bnetza_feature(feature: dict) -> dict:
    """Parse a Bundesnetzagentur ArcGIS feature into common schema."""
    attrs = feature.get("attributes", {})
    geom = feature.get("geometry", {})

    # Build connector_types from Steckertypen1-6
    connectors = []
    for i in range(1, 7):
        ct = attrs.get(f"Steckertypen{i}")
        if ct:
            connectors.append(ct)

    # Build address
    address_parts = [attrs.get("Straße", "")]
    if attrs.get("Hausnummer"):
        address_parts.append(attrs["Hausnummer"])
    if attrs.get("Adresszusatz"):
        address_parts.append(f"({attrs['Adresszusatz']})")
    address = " ".join(p for p in address_parts if p).strip()

    # Sum power ratings for informational purposes
    total_power_kw = attrs.get("Nennleistung_Ladeeinrichtung__k")

    return {
        "station_id": str(attrs.get("Ladeeinrichtungs_ID", "")),
        "station_name": attrs.get("Anzeigename__Karte_", address or "Unknown"),
        "address": address,
        "city": attrs.get("Ort", ""),
        "state": attrs.get("Bundesland", ""),
        "country": "Germany",
        "country_code": "DE",
        "postal_code": str(attrs.get("Postleitzahl", "")),
        "latitude": attrs.get("Breitengrad") or geom.get("y"),
        "longitude": attrs.get("Längengrad") or geom.get("x"),
        "network": "",
        "operator": attrs.get("Betreiber", ""),
        "connector_types": ", ".join(connectors),
        "num_ports": attrs.get("Anzahl_Ladepunkte"),
        "power_kw": total_power_kw,
        "status": attrs.get("Status", ""),
        "access_type": attrs.get("Öffnungszeiten", ""),
        "usage_cost": attrs.get("Bezahlsysteme", ""),
        "facility_type": attrs.get("Art_der_Ladeeinrichtung", ""),
        "date_opened": attrs.get("Inbetriebnahmedatum"),
        "data_provider": "Bundesnetzagentur",
        "date_last_updated": None,
    }


class ArcGISCollector(BaseCollector):
    """Collector for ArcGIS FeatureServer REST endpoints (offset pagination)."""

    MAX_PER_PAGE = 2000  # ArcGIS max

    def fetch_batch(self, params: SearchParams, limit: int | None = None) -> list[RawRecord]:
        records: list[RawRecord] = []
        offset = 0
        per_page = min(self.MAX_PER_PAGE, self.config.pagination.per_page)
        max_total = limit or 200000

        while len(records) < max_total:
            query_params = self._build_query(params, offset, per_page)

            try:
                response = self._make_request(self.config.base_url, params=query_params)
                data = response.json()
            except Exception as e:
                self.logger.error("[%s] Request failed at offset %d: %s", self.config.name, offset, e)
                break

            features = data.get("features", [])
            if not features:
                break

            for feature in features:
                parsed = _parse_bnetza_feature(feature)
                records.append(RawRecord(
                    source=self.config.name,
                    raw_data=parsed,
                    source_url=self.config.base_url,
                ))

            self.logger.info("[%s] Fetched %d records (offset=%d)", self.config.name, len(records), offset)

            # Check if there are more records
            exceeded = data.get("exceededTransferLimit", False)
            if not exceeded or len(features) < per_page:
                break

            offset += per_page

        return records

    def fetch_incremental(
        self, state: CrawlState, max_records: int = 500
    ) -> tuple[list[RawRecord], CrawlState]:
        # ArcGIS doesn't have a good incremental mechanism; fetch all
        records = self.fetch_batch(SearchParams(), limit=max_records)
        new_state = CrawlState(
            source_name=self.config.name,
            last_run_at=datetime.utcnow().isoformat(),
        )
        return records, new_state

    def _build_query(self, params: SearchParams, offset: int, per_page: int) -> dict:
        """Build ArcGIS REST query parameters."""
        where_clause = params.filters.get("where", "1=1")

        return {
            "where": where_clause,
            "outFields": "*",
            "outSR": "4326",
            "f": "json",
            "returnGeometry": "true",
            "resultRecordCount": per_page,
            "resultOffset": offset,
        }
