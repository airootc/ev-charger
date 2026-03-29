"""Admin API endpoints for key management and monitoring."""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from pydantic import BaseModel

from . import db
from .auth import require_admin
from .canary import check_leak
from .stations import load_geojson

logger = logging.getLogger("api_server")

router = APIRouter(prefix="/api/admin", tags=["admin"], dependencies=[Depends(require_admin)])


class CreateKeyRequest(BaseModel):
    name: str
    tier: str = "free"
    daily_limit: int = 1000
    expires_in_days: int | None = 365


class UpdateKeyRequest(BaseModel):
    name: str | None = None
    tier: str | None = None
    expires_at: str | None = None


@router.post("/keys")
async def create_key(req: CreateKeyRequest):
    """Create a new API key. Returns the plaintext key (shown only once)."""
    raw_key, record = db.create_api_key(
        name=req.name,
        tier=req.tier,
        daily_limit=req.daily_limit,
        expires_in_days=req.expires_in_days,
    )
    return {
        "key": raw_key,
        "warning": "Save this key now — it will not be shown again.",
        **record,
    }


@router.get("/keys")
async def list_keys():
    """List all API keys (redacted)."""
    return {"keys": db.list_api_keys()}


@router.delete("/keys/{key_id}")
async def revoke_key(key_id: str):
    """Revoke an API key."""
    if db.revoke_api_key(key_id):
        return {"status": "revoked", "key_id": key_id}
    raise HTTPException(status_code=404, detail="Key not found")


@router.post("/keys/{key_id}/rotate")
async def rotate_key(key_id: str):
    """Rotate an API key. Returns the new plaintext key (shown only once).

    The old key is immediately invalidated.
    """
    new_key = db.rotate_api_key(key_id)
    if not new_key:
        raise HTTPException(status_code=404, detail="Key not found or inactive")
    logger.info("API key rotated: key_id=%s", key_id)
    return {
        "key": new_key,
        "key_id": key_id,
        "key_prefix": new_key[:10],
        "warning": "Save this key now — it will not be shown again. The previous key is now invalid.",
    }


@router.patch("/keys/{key_id}")
async def update_key(key_id: str, req: UpdateKeyRequest):
    """Update key metadata (name, tier, expires_at)."""
    updates = req.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    updated = db.update_api_key(key_id, **updates)
    if not updated:
        raise HTTPException(status_code=404, detail="Key not found")
    logger.info("API key updated: key_id=%s fields=%s", key_id, list(updates.keys()))
    return {"status": "updated", "key": updated}


@router.get("/audit")
async def audit_log(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    key_id: str | None = Query(default=None),
    min_suspicion: int | None = Query(default=None, ge=0),
):
    """Enhanced audit log with filtering by key, suspicion score, and pagination."""
    logs = db.get_audit_log(
        limit=limit,
        offset=offset,
        key_id=key_id,
        min_suspicion=min_suspicion,
    )
    return {
        "logs": logs,
        "count": len(logs),
        "limit": limit,
        "offset": offset,
    }


@router.get("/keys/{key_id}/usage")
async def key_usage(key_id: str):
    """Get usage stats for an API key."""
    daily = db.get_key_daily_usage(key_id)
    recent = db.get_recent_requests(key_id, limit=20)
    canaries = db.get_canaries_for_key(key_id)
    return {
        "key_id": key_id,
        "daily_requests": daily,
        "canary_count": len(canaries),
        "recent_requests": recent,
    }


@router.get("/logs")
async def recent_logs(limit: int = 100):
    """Get recent request logs."""
    return {"logs": db.get_recent_requests(limit=limit)}


@router.post("/canary-check")
async def canary_check(body: dict = Body(...)):
    """Check uploaded GeoJSON for canary points to identify leak sources.

    Body: { "features": [...] } — a GeoJSON FeatureCollection or feature list.
    """
    features = body.get("features", [])
    if not features:
        raise HTTPException(status_code=400, detail="No features provided")

    matches = check_leak(features)
    return {
        "checked": len(features),
        "canary_matches": len(matches),
        "leak_sources": matches,
    }


@router.post("/reload")
async def reload_data():
    """Hot-reload the GeoJSON data without restarting the server.

    Reads the GeoJSON file from disk and rebuilds the spatial index.
    Use this after updating the data file (e.g., from the daily refresh pipeline).

    Requires admin bearer token: Authorization: Bearer <ADMIN_TOKEN>
    """
    try:
        count = load_geojson()
        logger.info("Hot-reload: reloaded %d stations into spatial index", count)
        return {
            "status": "ok",
            "message": f"Reloaded {count} stations",
            "stations_loaded": count,
        }
    except FileNotFoundError as e:
        logger.error("Hot-reload failed: %s", e)
        raise HTTPException(status_code=404, detail=f"GeoJSON file not found: {e}")
    except Exception as e:
        logger.error("Hot-reload failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Reload failed: {e}")
