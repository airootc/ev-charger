"""Configuration loaded from environment variables / .env file."""

from __future__ import annotations

import logging
import os
import secrets
from pathlib import Path

from dotenv import load_dotenv

# Load .env from project root
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")

logger = logging.getLogger("api_server.config")

# Sentinel values that must be replaced before production use
_DEFAULT_API_SECRET_KEY = "change-me-in-production"
_DEFAULT_ADMIN_TOKEN = "admin-change-me"

# Length of auto-generated tokens (in bytes of entropy before base64 encoding)
_AUTO_TOKEN_LENGTH = 32


class Settings:
    """Application settings resolved from environment variables.

    Call ``validate()`` during startup to enforce secret hygiene.
    """

    # Paths
    PROJECT_ROOT: Path = _PROJECT_ROOT
    DB_PATH: str = os.getenv("DB_PATH", str(_PROJECT_ROOT / "api_server" / "data.db"))
    GEOJSON_PATH: str = os.getenv("GEOJSON_PATH", str(_PROJECT_ROOT / "frontend" / "data" / "ev_stations.geojson"))
    FRONTEND_DIR: str = str(_PROJECT_ROOT / "frontend")

    # Environment
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "development").lower()

    # Security
    API_SECRET_KEY: str = os.getenv("API_SECRET_KEY", _DEFAULT_API_SECRET_KEY)
    ADMIN_TOKEN: str = os.getenv("ADMIN_TOKEN", "")

    # CORS
    CORS_ORIGINS: list[str] = os.getenv(
        "CORS_ORIGINS", "http://localhost:8000,http://localhost:3000,http://localhost:3001"
    ).split(",")

    # Rate limiting
    RATE_LIMIT_FREE_PER_MINUTE: int = int(os.getenv("RATE_LIMIT_FREE_PER_MINUTE", "60"))
    RATE_LIMIT_PRO_PER_MINUTE: int = int(os.getenv("RATE_LIMIT_PRO_PER_MINUTE", "300"))
    RATE_LIMIT_IP_PER_MINUTE: int = int(os.getenv("RATE_LIMIT_IP_PER_MINUTE", "30"))

    # Bbox / pagination
    MAX_BBOX_AREA_DEGREES: float = float(os.getenv("MAX_BBOX_AREA_DEGREES", "100"))
    MAX_FEATURES_PER_RESPONSE: int = int(os.getenv("MAX_FEATURES_PER_RESPONSE", "500"))

    # Canary
    CANARY_RATIO: float = float(os.getenv("CANARY_RATIO", "0.02"))
    CANARY_MIN_PER_KEY: int = 5
    CANARY_MAX_PER_KEY: int = 15

    # Server
    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "8000"))

    @classmethod
    def validate(cls) -> None:
        """Validate that security-critical settings are properly configured.

        In **production** (``ENVIRONMENT=production``), raises ``RuntimeError``
        if default/placeholder secrets are still in use.

        In **development**, logs warnings so developers are aware but allows
        startup to continue.
        """
        is_production = cls.ENVIRONMENT == "production"

        # --- ADMIN_TOKEN ---
        if not cls.ADMIN_TOKEN or cls.ADMIN_TOKEN == _DEFAULT_ADMIN_TOKEN:
            if is_production:
                raise RuntimeError(
                    "ADMIN_TOKEN is not set or still uses the default value. "
                    "Set a strong ADMIN_TOKEN environment variable before running in production."
                )
            # Auto-generate a random token for development convenience
            cls.ADMIN_TOKEN = secrets.token_urlsafe(_AUTO_TOKEN_LENGTH)
            logger.warning(
                "ADMIN_TOKEN was not set — auto-generated a random token for this session. "
                "Set the ADMIN_TOKEN environment variable to silence this warning."
            )

        # --- API_SECRET_KEY ---
        if cls.API_SECRET_KEY == _DEFAULT_API_SECRET_KEY:
            if is_production:
                raise RuntimeError(
                    "API_SECRET_KEY is still set to the default placeholder. "
                    "Set a strong API_SECRET_KEY environment variable before running in production."
                )
            logger.warning(
                "API_SECRET_KEY is using the default placeholder value. "
                "Set the API_SECRET_KEY environment variable before deploying."
            )


settings = Settings()
