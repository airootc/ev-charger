"""API key authentication middleware."""

from __future__ import annotations

import hmac

from fastapi import HTTPException, Request, Security
from fastapi.security import APIKeyHeader

from . import db
from .brute_force import brute_force_guard
from .config import settings

# API keys must be sent via the X-API-Key header — never in query parameters.
_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def require_api_key(
    request: Request,
    header_key: str | None = Security(_api_key_header),
) -> dict:
    """FastAPI dependency: validate API key and return key record.

    Keys are accepted **only** via the ``X-API-Key`` header to avoid
    leaking credentials in URLs, server logs, and browser history.

    Raises:
        HTTPException 401: No valid key provided or key expired.
        HTTPException 429: Daily limit exceeded or brute-force lockout.
    """
    raw_key = header_key
    ip = request.client.host if request.client else "unknown"

    # Block IPs with too many recent auth failures
    blocked, retry_after = brute_force_guard.is_blocked(ip)
    if blocked:
        raise HTTPException(
            status_code=429,
            detail={"error": "Too many failed authentication attempts", "retry_after_seconds": retry_after},
            headers={"Retry-After": str(retry_after)},
        )

    if not raw_key:
        brute_force_guard.record_failure(ip)
        raise HTTPException(
            status_code=401,
            detail={"error": "API key required", "hint": "Pass via X-API-Key header"},
        )

    key_record = db.validate_api_key(raw_key)
    if not key_record:
        brute_force_guard.record_failure(ip)
        raise HTTPException(
            status_code=401,
            detail={"error": "Invalid or inactive API key"},
        )

    # Check if the key has expired
    if db.check_key_expiry(key_record["id"]):
        raise HTTPException(
            status_code=401,
            detail={"error": "API key has expired"},
        )

    # Check daily usage limit
    daily_usage = db.get_key_daily_usage(key_record["id"])
    if daily_usage >= key_record["daily_limit"]:
        raise HTTPException(
            status_code=429,
            detail={
                "error": "Daily request limit exceeded",
                "limit": key_record["daily_limit"],
                "used": daily_usage,
            },
        )

    # Valid key — clear any failure history for this IP
    brute_force_guard.record_success(ip)

    # Attach to request state for downstream use
    request.state.api_key = key_record
    return key_record


async def require_admin(request: Request) -> bool:
    """FastAPI dependency: require admin bearer token.

    Uses constant-time comparison to prevent timing side-channel attacks.
    """
    ip = request.client.host if request.client else "unknown"

    # Block IPs with too many recent auth failures
    blocked, retry_after = brute_force_guard.is_blocked(ip)
    if blocked:
        raise HTTPException(
            status_code=429,
            detail={"error": "Too many failed authentication attempts", "retry_after_seconds": retry_after},
            headers={"Retry-After": str(retry_after)},
        )

    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        brute_force_guard.record_failure(ip)
        raise HTTPException(status_code=401, detail="Admin bearer token required")

    token = auth_header[7:]
    if not hmac.compare_digest(token, settings.ADMIN_TOKEN):
        brute_force_guard.record_failure(ip)
        raise HTTPException(status_code=403, detail="Invalid admin token")

    brute_force_guard.record_success(ip)
    return True
