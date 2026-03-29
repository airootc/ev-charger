"""WFS (Web Feature Service) collector for OGC-compliant geospatial endpoints.

Used for: Victoria (Australia) government EV charging data via GeoServer WFS.
Returns GeoJSON features with offset-based pagination.
"""

from __future__ import annotations

from datetime import datetime

from models import CrawlState, RawRecord, SearchParams
from .base import BaseCollector


class WFSCollector(BaseCollector):
    """Collector for OGC WFS endpoints that return GeoJSON."""

    def fetch_batch(self, params: SearchParams, limit: int | None = None) -> list[RawRecord]:
        records: list[RawRecord] = []
        start_index = 0
        max_features = min(self.config.pagination.per_page, 1000)
        max_total = limit or 50000
        type_name = params.filters.get("typeName", self.config.field_mapping.get("_typeName", ""))

        while len(records) < max_total:
            query_params = {
                "service": "WFS",
                "version": "1.1.0",
                "request": "GetFeature",
                "typeName": type_name,
                "outputFormat": "application/json",
                "maxFeatures": max_features,
                "startIndex": start_index,
            }

            # Add any spatial filter from params
            if params.filters.get("bbox"):
                query_params["bbox"] = params.filters["bbox"]

            try:
                response = self._make_request(self.config.base_url, params=query_params, timeout=60)
                data = response.json()
            except Exception as e:
                self.logger.error("[%s] WFS request failed at index %d: %s", self.config.name, start_index, e)
                break

            features = data.get("features", [])
            if not features:
                break

            for feature in features:
                props = feature.get("properties", {})
                geom = feature.get("geometry", {})

                # Extract coordinates from geometry
                coords = geom.get("coordinates", [])
                if geom.get("type") == "Point" and len(coords) >= 2:
                    lng, lat = coords[0], coords[1]
                else:
                    lat = props.get("latitude")
                    lng = props.get("longitude")

                if lat is None or lng is None:
                    continue

                # Apply field mapping
                mapped = {"latitude": lat, "longitude": lng}
                if self.config.field_mapping:
                    for src_field, common_field in self.config.field_mapping.items():
                        if src_field.startswith("_"):
                            continue  # Skip internal config fields
                        val = props.get(src_field)
                        if val is not None:
                            mapped[common_field] = val
                else:
                    mapped.update(props)

                records.append(RawRecord(
                    source=self.config.name,
                    raw_data=mapped,
                    source_url=self.config.base_url,
                ))

                if len(records) >= max_total:
                    break

            self.logger.info("[%s] Fetched %d records (startIndex=%d)", self.config.name, len(records), start_index)

            if len(features) < max_features:
                break
            start_index += max_features

        return records

    def fetch_incremental(
        self, state: CrawlState, max_records: int = 500
    ) -> tuple[list[RawRecord], CrawlState]:
        records = self.fetch_batch(SearchParams(), limit=max_records)
        new_state = CrawlState(
            source_name=self.config.name,
            last_run_at=datetime.utcnow().isoformat(),
        )
        return records, new_state
