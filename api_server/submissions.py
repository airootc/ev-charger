"""Station submission endpoints — Phase 1 'Report a Station' feature."""

from __future__ import annotations

import json
import gzip
import logging
import os
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, field_validator

from . import db
from .auth import require_admin
from .config import settings
from .stations import load_geojson

logger = logging.getLogger("api_server.submissions")

router = APIRouter(prefix="/api", tags=["submissions"])


# ── Pydantic Models ──

class StationSubmission(BaseModel):
    station_name: str
    latitude: float
    longitude: float
    connector_type: Optional[str] = None
    network: Optional[str] = None
    num_ports: Optional[int] = None
    address: Optional[str] = None
    submitter_email: Optional[str] = None
    notes: Optional[str] = None

    @field_validator("latitude")
    @classmethod
    def validate_latitude(cls, v: float) -> float:
        if not -90 <= v <= 90:
            raise ValueError("Latitude must be between -90 and 90")
        return v

    @field_validator("longitude")
    @classmethod
    def validate_longitude(cls, v: float) -> float:
        if not -180 <= v <= 180:
            raise ValueError("Longitude must be between -180 and 180")
        return v


# ── Public Endpoint ──

@router.post("/stations/submit")
async def submit_station(submission: StationSubmission):
    """Submit a new station report. No authentication required for Phase 1."""
    submission_id = f"sub_{secrets.token_hex(8)}"
    now = datetime.now(timezone.utc).isoformat()

    with db.get_db() as conn:
        conn.execute(
            """INSERT INTO station_submissions
               (id, station_name, latitude, longitude, connector_type, network,
                num_ports, address, submitter_email, notes, status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)""",
            (
                submission_id,
                submission.station_name,
                submission.latitude,
                submission.longitude,
                submission.connector_type,
                submission.network,
                submission.num_ports,
                submission.address,
                submission.submitter_email,
                submission.notes,
                now,
            ),
        )

    logger.info("New station submission: id=%s name=%s", submission_id, submission.station_name)

    return {
        "status": "success",
        "message": "Station submitted for review",
        "submission_id": submission_id,
    }


# ── Admin Endpoints ──

@router.get("/admin/submissions", dependencies=[Depends(require_admin)])
async def list_submissions(status: str = "pending"):
    """List station submissions filtered by status. Requires admin token.

    Query params:
        status: 'pending' (default), 'approved', 'rejected', or 'all'
    """
    with db.get_db() as conn:
        if status == "all":
            rows = conn.execute(
                "SELECT * FROM station_submissions ORDER BY created_at DESC"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM station_submissions WHERE status = ? ORDER BY created_at DESC",
                (status,),
            ).fetchall()

        # Always fetch counts for the dashboard
        counts = {}
        for s in ("pending", "approved", "rejected"):
            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM station_submissions WHERE status = ?", (s,)
            ).fetchone()
            counts[s] = row["cnt"] if row else 0

    return {"submissions": [dict(r) for r in rows], "counts": counts}


@router.post("/admin/submissions/{submission_id}/approve", dependencies=[Depends(require_admin)])
async def approve_submission(submission_id: str):
    """Approve a submission and add it to the main GeoJSON data. Requires admin token."""
    with db.get_db() as conn:
        row = conn.execute(
            "SELECT * FROM station_submissions WHERE id = ?", (submission_id,)
        ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Submission not found")

    submission = dict(row)
    if submission["status"] != "pending":
        raise HTTPException(status_code=400, detail=f"Submission already {submission['status']}")

    # Add to GeoJSON file
    _add_station_to_geojson(submission)

    # Update status
    now = datetime.now(timezone.utc).isoformat()
    with db.get_db() as conn:
        conn.execute(
            "UPDATE station_submissions SET status = 'approved', reviewed_at = ? WHERE id = ?",
            (now, submission_id),
        )

    # Reload spatial index so the new station appears immediately
    try:
        count = load_geojson()
        logger.info("Reloaded %d stations after approving submission %s", count, submission_id)
    except Exception as e:
        logger.error("Failed to reload GeoJSON after approval: %s", e)

    return {
        "status": "approved",
        "submission_id": submission_id,
        "message": "Station added to the map",
    }


@router.post("/admin/submissions/{submission_id}/reject", dependencies=[Depends(require_admin)])
async def reject_submission(submission_id: str):
    """Reject a station submission. Requires admin token."""
    with db.get_db() as conn:
        row = conn.execute(
            "SELECT * FROM station_submissions WHERE id = ?", (submission_id,)
        ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Submission not found")

    submission = dict(row)
    if submission["status"] != "pending":
        raise HTTPException(status_code=400, detail=f"Submission already {submission['status']}")

    now = datetime.now(timezone.utc).isoformat()
    with db.get_db() as conn:
        conn.execute(
            "UPDATE station_submissions SET status = 'rejected', reviewed_at = ? WHERE id = ?",
            (now, submission_id),
        )

    return {
        "status": "rejected",
        "submission_id": submission_id,
        "message": "Submission rejected",
    }


# ── Helpers ──

def _add_station_to_geojson(submission: dict) -> None:
    """Append an approved station to the GeoJSON data file."""
    path = settings.GEOJSON_PATH
    gz_path = path + ".gz"

    # Determine which file to read
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as fh:
            geojson = json.load(fh)
    elif os.path.exists(gz_path):
        with gzip.open(gz_path, "rt", encoding="utf-8") as fh:
            geojson = json.load(fh)
    else:
        raise FileNotFoundError(f"GeoJSON file not found: {path}")

    # Build the new feature
    new_feature = {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [submission["longitude"], submission["latitude"]],
        },
        "properties": {
            "station_id": submission["id"],
            "station_name": submission["station_name"],
            "address": submission["address"] or "",
            "network": submission["network"] or "",
            "connector_types": submission["connector_type"] or "",
            "connector_category": submission["connector_type"] or "Unknown",
            "total_ports": submission["num_ports"],
            "country": "",
            "source": "user_submission",
            "notes": submission["notes"] or "",
        },
    }

    geojson.setdefault("features", []).append(new_feature)

    # Write back — write uncompressed file, and also compressed if it existed
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(geojson, fh)

    if os.path.exists(gz_path):
        with gzip.open(gz_path, "wt", encoding="utf-8") as fh:
            json.dump(geojson, fh)
