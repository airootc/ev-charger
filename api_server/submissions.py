"""Station submission endpoints — Phase 1 'Report a Station' feature."""

from __future__ import annotations

import json
import gzip
import logging
import os
import re
import secrets
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, field_validator

from . import db
from .auth import require_admin
from .config import settings
from .stations import load_geojson

logger = logging.getLogger("api_server.submissions")

router = APIRouter(prefix="/api", tags=["submissions"])


# ── Anti-Spam Config ──

_SUBMIT_RATE_LIMIT_PER_IP = 5       # max submissions per IP per window
_SUBMIT_RATE_WINDOW_SECONDS = 3600  # 1 hour window
_SUBMIT_COOLDOWN_SECONDS = 30       # min seconds between submissions from same IP
_DUPLICATE_RADIUS_DEGREES = 0.001   # ~111 meters — reject near-duplicate locations
_DUPLICATE_WINDOW_SECONDS = 86400   # 24 hours for duplicate check

# Spam patterns in text fields
_SPAM_URL_PATTERN = re.compile(r'https?://|www\.|\.com/|\.net/|\.org/|bit\.ly|tinyurl', re.IGNORECASE)
_SPAM_KEYWORDS = re.compile(
    r'\b(buy now|click here|free money|crypto|casino|viagra|lottery|winner|prize|act now)\b',
    re.IGNORECASE,
)

# In-memory rate limiter
_ip_submissions: dict[str, list[float]] = {}
_ip_lock = threading.Lock()


def _check_rate_limit(ip: str) -> None:
    """Enforce per-IP submission rate limits."""
    now = time.time()
    with _ip_lock:
        timestamps = _ip_submissions.get(ip, [])
        # Clean old entries
        timestamps = [t for t in timestamps if now - t < _SUBMIT_RATE_WINDOW_SECONDS]
        _ip_submissions[ip] = timestamps

        # Check cooldown (too fast)
        if timestamps and (now - timestamps[-1]) < _SUBMIT_COOLDOWN_SECONDS:
            wait = int(_SUBMIT_COOLDOWN_SECONDS - (now - timestamps[-1])) + 1
            raise HTTPException(
                status_code=429,
                detail=f"Please wait {wait} seconds before submitting again.",
            )

        # Check rate limit
        if len(timestamps) >= _SUBMIT_RATE_LIMIT_PER_IP:
            raise HTTPException(
                status_code=429,
                detail="Too many submissions. Please try again later.",
            )

        # Record this submission
        timestamps.append(now)
        _ip_submissions[ip] = timestamps


def _check_spam_content(submission: "StationSubmission") -> None:
    """Reject submissions with spammy content."""
    text_fields = [
        submission.station_name,
        submission.network or "",
        submission.address or "",
        submission.notes or "",
    ]
    combined = " ".join(text_fields)

    # Check for URLs in non-URL fields
    if _SPAM_URL_PATTERN.search(combined):
        raise HTTPException(status_code=400, detail="URLs are not allowed in submissions.")

    # Check for spam keywords
    if _SPAM_KEYWORDS.search(combined):
        raise HTTPException(status_code=400, detail="Submission contains prohibited content.")

    # Station name too short or too long
    name = submission.station_name.strip()
    if len(name) < 3:
        raise HTTPException(status_code=400, detail="Station name must be at least 3 characters.")
    if len(name) > 200:
        raise HTTPException(status_code=400, detail="Station name is too long.")

    # Check for repetitive characters (e.g. "aaaaaaa")
    if re.match(r'^(.)\1{5,}$', name):
        raise HTTPException(status_code=400, detail="Invalid station name.")


def _check_honeypot(website: Optional[str]) -> None:
    """Reject if the hidden honeypot field is filled (bots fill all fields)."""
    if website:
        logger.warning("Honeypot triggered — bot submission blocked")
        raise HTTPException(status_code=400, detail="Submission rejected.")


def _check_duplicate_location(lat: float, lng: float) -> None:
    """Reject if a submission with nearly the same coordinates exists recently."""
    try:
        with db.get_db() as conn:
            row = conn.execute(
                """SELECT COUNT(*) as cnt FROM station_submissions
                   WHERE status = 'pending'
                   AND ABS(latitude - ?) < ?
                   AND ABS(longitude - ?) < ?""",
                (lat, _DUPLICATE_RADIUS_DEGREES, lng, _DUPLICATE_RADIUS_DEGREES),
            ).fetchone()
            if row and row["cnt"] > 0:
                raise HTTPException(
                    status_code=400,
                    detail="A station at this location has already been submitted and is pending review.",
                )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Duplicate check failed: %s", e)


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
    website: Optional[str] = None  # Honeypot field — should always be empty

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
async def submit_station(submission: StationSubmission, request: Request):
    """Submit a new station report. No authentication required for Phase 1."""
    # Get client IP
    ip = request.headers.get("x-forwarded-for", request.client.host or "unknown").split(",")[0].strip()

    # Anti-spam checks
    _check_honeypot(submission.website)
    _check_rate_limit(ip)
    _check_spam_content(submission)
    _check_duplicate_location(submission.latitude, submission.longitude)

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
