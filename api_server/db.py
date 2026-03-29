"""SQLite database helpers for API keys, request logs, and canary points."""

from __future__ import annotations

import hashlib
import json
import logging
import secrets
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from .config import settings

logger = logging.getLogger("api_server.db")

_local = threading.local()

# ── Usage Cache ──
# Maps key_id -> (count, expiry_timestamp) to avoid hitting the DB on every
# rate-limit check.  The cache is intentionally approximate: after a write the
# counter will be stale for up to _USAGE_CACHE_TTL_SECONDS seconds.
_USAGE_CACHE_TTL_SECONDS: int = 60
_usage_cache: dict[str, tuple[int, float]] = {}
_usage_cache_lock = threading.Lock()

# ── Pruning Defaults ──
DEFAULT_PRUNE_MAX_AGE_DAYS: int = 30


def _get_conn() -> sqlite3.Connection:
    """Get a thread-local SQLite connection."""
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = sqlite3.connect(settings.DB_PATH, check_same_thread=False)
        _local.conn.row_factory = sqlite3.Row
        _local.conn.execute("PRAGMA journal_mode=WAL")
        _local.conn.execute("PRAGMA foreign_keys=ON")
    return _local.conn


@contextmanager
def get_db():
    """Context manager for database access."""
    conn = _get_conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def init_db():
    """Create tables if they don't exist."""
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS api_keys (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                key_hash TEXT NOT NULL UNIQUE,
                key_prefix TEXT NOT NULL,
                tier TEXT NOT NULL DEFAULT 'free',
                created_at TEXT NOT NULL,
                expires_at TEXT,
                active INTEGER NOT NULL DEFAULT 1,
                daily_limit INTEGER NOT NULL DEFAULT 1000,
                metadata TEXT DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS request_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                api_key_id TEXT,
                ip TEXT,
                timestamp TEXT NOT NULL,
                endpoint TEXT,
                bbox TEXT,
                user_agent TEXT,
                referer TEXT,
                features_returned INTEGER DEFAULT 0,
                response_time_ms REAL,
                fingerprint_hash TEXT,
                suspicion_score INTEGER DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_request_log_key_ts
                ON request_log(api_key_id, timestamp);
            CREATE INDEX IF NOT EXISTS idx_request_log_ip_ts
                ON request_log(ip, timestamp);

            CREATE TABLE IF NOT EXISTS canary_points (
                id TEXT PRIMARY KEY,
                api_key_id TEXT NOT NULL,
                station_name TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                injected_at TEXT NOT NULL,
                properties TEXT NOT NULL DEFAULT '{}',
                FOREIGN KEY (api_key_id) REFERENCES api_keys(id)
            );

            CREATE INDEX IF NOT EXISTS idx_canary_key
                ON canary_points(api_key_id);

            CREATE INDEX IF NOT EXISTS idx_request_log_timestamp
                ON request_log(timestamp);
        """)


# ── API Key Operations ──

def hash_key(raw_key: str) -> str:
    """SHA-256 hash of a raw API key."""
    return hashlib.sha256(raw_key.encode()).hexdigest()


def generate_api_key() -> str:
    """Generate a new API key with 'di_' prefix."""
    return f"di_{secrets.token_hex(32)}"


def create_api_key(
    name: str,
    tier: str = "free",
    daily_limit: int = 1000,
    expires_in_days: int | None = 365,
) -> tuple[str, dict]:
    """Create a new API key. Returns (plaintext_key, key_record)."""
    raw_key = generate_api_key()
    key_id = secrets.token_hex(8)
    key_h = hash_key(raw_key)
    now = datetime.now(timezone.utc).isoformat()
    expires = None
    if expires_in_days:
        expires = (datetime.now(timezone.utc) + timedelta(days=expires_in_days)).isoformat()

    with get_db() as conn:
        conn.execute(
            """INSERT INTO api_keys (id, name, key_hash, key_prefix, tier, created_at, expires_at, active, daily_limit)
               VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)""",
            (key_id, name, key_h, raw_key[:10], tier, now, expires, daily_limit),
        )

    record = {
        "id": key_id,
        "name": name,
        "tier": tier,
        "created_at": now,
        "expires_at": expires,
        "daily_limit": daily_limit,
        "key_prefix": raw_key[:10],
    }
    return raw_key, record


def validate_api_key(raw_key: str) -> dict | None:
    """Validate an API key and return the key record, or None if invalid/inactive.

    Note: this no longer rejects expired keys — callers should check
    ``key_record["expires_at"]`` or use :func:`check_key_expiry` so they can
    return a more specific error to the client.
    """
    key_h = hash_key(raw_key)

    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM api_keys WHERE key_hash = ? AND active = 1",
            (key_h,),
        ).fetchone()

    if not row:
        return None

    return dict(row)


def list_api_keys() -> list[dict]:
    """List all API keys (redacted)."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, name, key_prefix, tier, created_at, expires_at, active, daily_limit FROM api_keys ORDER BY created_at DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def revoke_api_key(key_id: str) -> bool:
    """Revoke an API key."""
    with get_db() as conn:
        cursor = conn.execute("UPDATE api_keys SET active = 0 WHERE id = ?", (key_id,))
    return cursor.rowcount > 0


def rotate_api_key(key_id: str) -> str | None:
    """Rotate an API key: generate a new key and update the hash.

    Returns the new plaintext key (shown once), or None if key_id not found.
    """
    with get_db() as conn:
        row = conn.execute(
            "SELECT id FROM api_keys WHERE id = ? AND active = 1", (key_id,)
        ).fetchone()
        if not row:
            return None

        new_key = generate_api_key()
        new_hash = hash_key(new_key)
        conn.execute(
            "UPDATE api_keys SET key_hash = ?, key_prefix = ? WHERE id = ?",
            (new_hash, new_key[:10], key_id),
        )
    return new_key


def update_api_key(key_id: str, **fields) -> dict | None:
    """Update mutable fields on an API key (name, tier, expires_at).

    Returns the updated key record, or None if key_id not found.
    """
    allowed = {"name", "tier", "expires_at"}
    updates = {k: v for k, v in fields.items() if k in allowed and v is not None}
    if not updates:
        return None

    set_clause = ", ".join(f"{col} = ?" for col in updates)
    values = list(updates.values()) + [key_id]

    with get_db() as conn:
        cursor = conn.execute(
            f"UPDATE api_keys SET {set_clause} WHERE id = ?", values
        )
        if cursor.rowcount == 0:
            return None
        row = conn.execute(
            "SELECT id, name, key_prefix, tier, created_at, expires_at, active, daily_limit FROM api_keys WHERE id = ?",
            (key_id,),
        ).fetchone()
    return dict(row) if row else None


def check_key_expiry(key_id: str) -> bool:
    """Return True if the key is expired. Returns False if not expired or key not found."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT expires_at FROM api_keys WHERE id = ?", (key_id,)
        ).fetchone()
    if not row or not row["expires_at"]:
        return False
    now = datetime.now(timezone.utc).isoformat()
    return row["expires_at"] < now


def get_key_daily_usage(key_id: str) -> int:
    """Get today's request count for a key.

    Uses an in-memory cache with a TTL of ``_USAGE_CACHE_TTL_SECONDS`` to
    avoid running a COUNT(*) query on every single request.  The count may
    therefore be stale by up to one TTL window, which is acceptable for
    quota enforcement (the hard limit is checked via rate_limit.py).
    """
    now = time.monotonic()

    with _usage_cache_lock:
        cached = _usage_cache.get(key_id)
        if cached is not None:
            count, expiry = cached
            if now < expiry:
                return count

    # Cache miss or expired -- query the database
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with get_db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM request_log WHERE api_key_id = ? AND timestamp >= ?",
            (key_id, today),
        ).fetchone()
    count = row["cnt"] if row else 0

    with _usage_cache_lock:
        _usage_cache[key_id] = (count, now + _USAGE_CACHE_TTL_SECONDS)

    return count


# ── Request Logging ──

def log_request(
    api_key_id: str | None,
    ip: str,
    endpoint: str,
    bbox: str | None = None,
    user_agent: str | None = None,
    referer: str | None = None,
    features_returned: int = 0,
    response_time_ms: float = 0,
    fingerprint_hash: str | None = None,
    suspicion_score: int = 0,
):
    """Log an API request."""
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        conn.execute(
            """INSERT INTO request_log
               (api_key_id, ip, timestamp, endpoint, bbox, user_agent, referer,
                features_returned, response_time_ms, fingerprint_hash, suspicion_score)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (api_key_id, ip, now, endpoint, bbox, user_agent, referer,
             features_returned, response_time_ms, fingerprint_hash, suspicion_score),
        )


def prune_request_log(max_age_days: int = DEFAULT_PRUNE_MAX_AGE_DAYS) -> int:
    """Delete request_log entries older than *max_age_days*.

    Returns the number of rows deleted.  Intended to be called once at
    server startup (and optionally on a periodic schedule) to prevent
    unbounded table growth.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=max_age_days)).isoformat()
    with get_db() as conn:
        cursor = conn.execute(
            "DELETE FROM request_log WHERE timestamp < ?", (cutoff,)
        )
        deleted = cursor.rowcount
    logger.info(
        "Pruned %d request_log entries older than %d days", deleted, max_age_days
    )
    return deleted


def get_recent_requests(key_id: str | None = None, limit: int = 100) -> list[dict]:
    """Get recent request logs."""
    with get_db() as conn:
        if key_id:
            rows = conn.execute(
                "SELECT * FROM request_log WHERE api_key_id = ? ORDER BY timestamp DESC LIMIT ?",
                (key_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM request_log ORDER BY timestamp DESC LIMIT ?",
                (limit,),
            ).fetchall()
    return [dict(r) for r in rows]


def get_audit_log(
    limit: int = 100,
    offset: int = 0,
    key_id: str | None = None,
    min_suspicion: int | None = None,
) -> list[dict]:
    """Query the request_log with optional filters, enriched with key name.

    Returns log entries ordered by timestamp descending.
    """
    query = """
        SELECT rl.id, rl.api_key_id, ak.name as key_name,
               rl.endpoint, rl.ip, rl.timestamp,
               rl.user_agent, rl.referer, rl.bbox,
               rl.features_returned, rl.response_time_ms,
               rl.fingerprint_hash, rl.suspicion_score
        FROM request_log rl
        LEFT JOIN api_keys ak ON rl.api_key_id = ak.id
        WHERE 1=1
    """
    params: list[Any] = []

    if key_id is not None:
        query += " AND rl.api_key_id = ?"
        params.append(key_id)
    if min_suspicion is not None:
        query += " AND rl.suspicion_score >= ?"
        params.append(min_suspicion)

    query += " ORDER BY rl.timestamp DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    with get_db() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


# ── Canary Operations ──

def store_canary(api_key_id: str, station_name: str, lat: float, lng: float, props: dict) -> str:
    """Store a canary point for an API key.

    Uses INSERT OR IGNORE so that a duplicate primary key (extremely
    unlikely with random IDs, but possible under race conditions) is
    silently skipped rather than raising an integrity error.
    """
    canary_id = f"canary_{secrets.token_hex(8)}"
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        conn.execute(
            """INSERT OR IGNORE INTO canary_points
               (id, api_key_id, station_name, latitude, longitude, injected_at, properties)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (canary_id, api_key_id, station_name, lat, lng, now, json.dumps(props)),
        )
    return canary_id


def get_canaries_for_key(api_key_id: str) -> list[dict]:
    """Get all canary points for an API key."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM canary_points WHERE api_key_id = ?",
            (api_key_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def check_canary_match(station_name: str, lat: float, lng: float, tolerance: float = 0.001) -> list[dict]:
    """Check if a point matches any canary. Returns matching key IDs."""
    with get_db() as conn:
        rows = conn.execute(
            """SELECT cp.*, ak.name as key_name FROM canary_points cp
               JOIN api_keys ak ON cp.api_key_id = ak.id
               WHERE ABS(cp.latitude - ?) < ? AND ABS(cp.longitude - ?) < ?""",
            (lat, tolerance, lng, tolerance),
        ).fetchall()
    return [dict(r) for r in rows]
