"""FastAPI server — serves both the secure API and the frontend."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .config import settings
from .security_headers import SecurityHeadersMiddleware
from . import db
from .stations import load_geojson, router as stations_router
from .admin import router as admin_router
from .quality import router as quality_router
from .submissions import router as submissions_router

logger = logging.getLogger("api_server")

# Number of leading characters to show when logging truncated keys
_KEY_LOG_PREFIX_LENGTH = 8


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown logic."""
    # Validate configuration before anything else
    settings.validate()

    # Startup
    logger.info("Initializing database...")
    db.init_db()

    logger.info("Loading GeoJSON data...")
    count = load_geojson()
    logger.info("Loaded %d stations into spatial index", count)

    logger.info("Pruning old request log entries...")
    pruned = db.prune_request_log(max_age_days=30)
    logger.info("Pruned %d old request log entries", pruned)

    # Create a default frontend API key if none exists
    keys = db.list_api_keys()
    frontend_keys = [k for k in keys if k["name"] == "frontend-default"]
    if not frontend_keys:
        raw_key, record = db.create_api_key(
            name="frontend-default",
            tier="free",
            daily_limit=5000,
            expires_in_days=365,
        )
        logger.info(
            "Created frontend API key: %s...",
            raw_key[:_KEY_LOG_PREFIX_LENGTH],
        )
    else:
        logger.info("Frontend API key already exists (prefix: %s)", frontend_keys[0]["key_prefix"])

    yield

    # Shutdown
    logger.info("Server shutting down")


app = FastAPI(
    title="EV Charging Stations API",
    description="Global EV charging station data API with 14,000+ stations from 17 sources",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
    lifespan=lifespan,
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_methods=["GET", "POST", "DELETE", "PATCH"],
    allow_headers=["X-API-Key", "Content-Type", "Authorization"],
    allow_credentials=False,
)

# Security headers (added after CORS so CORS is the outermost middleware)
app.add_middleware(SecurityHeadersMiddleware)

# API routes
app.include_router(stations_router)
app.include_router(admin_router)
app.include_router(quality_router)
app.include_router(submissions_router)


# Health check
@app.get("/api/health")
async def health():
    return {"status": "ok", "service": "ev-charging-api"}


# Block direct access to the GeoJSON data file
@app.get("/data/{path:path}")
async def block_data_access(path: str):
    return JSONResponse(
        status_code=403,
        content={"error": "Direct data access is not allowed. Use the /api/stations endpoint with an API key."},
    )


# Serve frontend static files (but NOT the data/ subdirectory)
_frontend_dir = Path(settings.FRONTEND_DIR)
if _frontend_dir.exists():
    # Serve index.html for root
    @app.get("/")
    async def serve_index():
        index_path = _frontend_dir / "index.html"
        if index_path.exists():
            return FileResponse(index_path)
        return JSONResponse({"error": "Frontend not found"}, status_code=404)

    # Mount static files (CSS, JS, etc.) — excluding data/
    app.mount("/", StaticFiles(directory=str(_frontend_dir), html=True), name="frontend")


# ── Entry point ──

def main():
    """Run the server directly: python -m api_server.server"""
    import uvicorn

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    logger.info("Starting EV Charging Station API on %s:%d", settings.HOST, settings.PORT)

    uvicorn.run(
        "api_server.server:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=False,
    )


if __name__ == "__main__":
    main()
