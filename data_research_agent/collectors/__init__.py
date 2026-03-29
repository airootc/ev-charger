from .base import BaseCollector
from .api_collector import APICollector
from .scraper_collector import ScraperCollector
from .rss_collector import RSSCollector
from .osm_collector import OSMOverpassCollector
from .arcgis_collector import ArcGISCollector
from .supercharger_collector import SuperchargerCollector
from .france_irve_collector import FranceIRVECollector
from .korea_collector import KoreaEVCollector
from .csv_download_collector import CSVDownloadCollector
from .wfs_collector import WFSCollector
from .tomtom_collector import TomTomEVCollector
from .here_collector import HEREEVCollector
from .google_places_collector import GooglePlacesEVCollector

COLLECTOR_TYPES = {
    "api": APICollector,
    "scraper": ScraperCollector,
    "rss": RSSCollector,
    "osm_overpass": OSMOverpassCollector,
    "arcgis": ArcGISCollector,
    "supercharger": SuperchargerCollector,
    "france_irve": FranceIRVECollector,
    "korea_ev": KoreaEVCollector,
    "csv_download": CSVDownloadCollector,
    "wfs": WFSCollector,
    "tomtom_ev": TomTomEVCollector,
    "here_ev": HEREEVCollector,
    "google_places_ev": GooglePlacesEVCollector,
}


def create_collector(config, rate_limiter, logger):
    """Factory function to create a collector based on source type."""
    collector_cls = COLLECTOR_TYPES.get(config.type)
    if not collector_cls:
        raise ValueError(f"Unknown collector type: {config.type}")
    return collector_cls(config=config, rate_limiter=rate_limiter, logger=logger)
