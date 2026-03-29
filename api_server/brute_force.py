"""Brute force protection for authentication endpoints.

Tracks failed auth attempts per IP and applies escalating lockouts:
  - 5 failures in 15 min  -> blocked 30 min
  - 10 failures in 1 hour -> blocked 2 hours
  - 20 failures in 24 hours -> blocked 24 hours
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field

logger = logging.getLogger("api_server")

# Escalation tiers: (threshold, window_seconds, block_seconds)
_TIERS: list[tuple[int, float, float]] = [
    (5, 15 * 60, 30 * 60),       # 5 failures in 15 min  -> block 30 min
    (10, 60 * 60, 2 * 60 * 60),  # 10 failures in 1 hour -> block 2 hours
    (20, 24 * 60 * 60, 24 * 60 * 60),  # 20 failures in 24 hours -> block 24 hours
]

_MAX_ENTRY_AGE = 24 * 60 * 60  # 24 hours
_CLEANUP_INTERVAL = 300  # Run cleanup at most every 5 minutes


@dataclass
class _IPRecord:
    """Tracks failure timestamps and current block state for a single IP."""

    failures: list[float] = field(default_factory=list)
    blocked_until: float = 0.0


class BruteForceGuard:
    """In-memory brute force protection keyed by IP address."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._records: dict[str, _IPRecord] = {}
        self._last_cleanup = time.monotonic()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record_failure(self, ip: str) -> None:
        """Record a failed authentication attempt for *ip*.

        If the new failure count crosses an escalation tier threshold,
        the IP is blocked for the corresponding duration.
        """
        now = time.monotonic()
        with self._lock:
            rec = self._records.setdefault(ip, _IPRecord())
            rec.failures.append(now)
            self._evaluate_block(rec, now)

        self._maybe_cleanup()
        logger.warning("Brute-force: auth failure from %s (total recent: %d)", ip, len(rec.failures))

    def record_success(self, ip: str) -> None:
        """Record a successful authentication, clearing failure history."""
        with self._lock:
            self._records.pop(ip, None)

    def is_blocked(self, ip: str) -> tuple[bool, int]:
        """Check whether *ip* is currently blocked.

        Returns:
            (blocked, retry_after_seconds) -- *retry_after_seconds* is 0
            when not blocked.
        """
        now = time.monotonic()
        with self._lock:
            rec = self._records.get(ip)
            if rec is None:
                return False, 0
            if rec.blocked_until > now:
                remaining = int(rec.blocked_until - now) + 1
                return True, remaining
            return False, 0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _evaluate_block(self, rec: _IPRecord, now: float) -> None:
        """Apply the most severe matching escalation tier (lock held)."""
        for threshold, window, block_duration in reversed(_TIERS):
            recent = [t for t in rec.failures if (now - t) <= window]
            if len(recent) >= threshold:
                new_blocked_until = now + block_duration
                if new_blocked_until > rec.blocked_until:
                    rec.blocked_until = new_blocked_until
                # Keep only timestamps within the largest window so the
                # list does not grow unbounded.
                rec.failures = recent
                return

    def _maybe_cleanup(self) -> None:
        """Periodically purge records older than 24 hours."""
        now = time.monotonic()
        if now - self._last_cleanup < _CLEANUP_INTERVAL:
            return

        with self._lock:
            self._last_cleanup = now
            cutoff = now - _MAX_ENTRY_AGE
            stale_ips = [
                ip
                for ip, rec in self._records.items()
                if rec.blocked_until < now
                and (not rec.failures or rec.failures[-1] < cutoff)
            ]
            for ip in stale_ips:
                del self._records[ip]

        if stale_ips:
            logger.info("Brute-force: cleaned up %d stale IP records", len(stale_ips))


# Singleton instance shared across auth and admin modules.
brute_force_guard = BruteForceGuard()
