"""Token-bucket rate limiter -- per API key and per IP.

IMPORTANT: Scalability limitation
---------------------------------
This module stores all rate-limit state **in-process memory**.  It works
correctly for a single-worker / single-process deployment, but does NOT
share state across multiple Uvicorn workers or multiple server instances.

For **multi-worker or horizontally-scaled deployments** the token-bucket
state should be moved to a shared store such as Redis (e.g. using the
``redis`` or ``aioredis`` libraries with a Lua-script based token bucket).
Until then, each worker maintains its own independent counters, which means
the effective per-key/per-IP rate limit is multiplied by the number of
workers.
"""

from __future__ import annotations

import time
import threading
from dataclasses import dataclass, field

from fastapi import HTTPException, Request

from .config import settings


@dataclass
class TokenBucket:
    """Simple token bucket rate limiter."""
    capacity: float
    refill_rate: float  # tokens per second
    tokens: float = 0.0
    last_refill: float = field(default_factory=time.monotonic)

    def __post_init__(self):
        self.tokens = self.capacity

    def consume(self) -> bool:
        """Try to consume one token. Returns True if allowed, False if rate limited."""
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now

        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False

    @property
    def retry_after(self) -> float:
        """Seconds until next token is available."""
        if self.tokens >= 1.0:
            return 0.0
        return (1.0 - self.tokens) / self.refill_rate


class RateLimiterStore:
    """Thread-safe store of per-key and per-IP rate limiters."""

    def __init__(self):
        self._lock = threading.Lock()
        self._key_buckets: dict[str, TokenBucket] = {}
        self._ip_buckets: dict[str, TokenBucket] = {}
        self._last_cleanup = time.monotonic()

    def check_key(self, key_id: str, tier: str = "free") -> bool:
        """Check rate limit for an API key. Returns True if allowed."""
        rpm = settings.RATE_LIMIT_PRO_PER_MINUTE if tier == "pro" else settings.RATE_LIMIT_FREE_PER_MINUTE
        rps = rpm / 60.0

        with self._lock:
            if key_id not in self._key_buckets:
                self._key_buckets[key_id] = TokenBucket(capacity=rpm, refill_rate=rps)
            return self._key_buckets[key_id].consume()

    def check_ip(self, ip: str) -> bool:
        """Check rate limit for an IP address. Returns True if allowed."""
        rpm = settings.RATE_LIMIT_IP_PER_MINUTE
        rps = rpm / 60.0

        with self._lock:
            if ip not in self._ip_buckets:
                self._ip_buckets[ip] = TokenBucket(capacity=rpm, refill_rate=rps)
            return self._ip_buckets[ip].consume()

    def get_retry_after(self, key_id: str | None, ip: str) -> float:
        """Get the retry-after time in seconds."""
        with self._lock:
            times = []
            if key_id and key_id in self._key_buckets:
                times.append(self._key_buckets[key_id].retry_after)
            if ip in self._ip_buckets:
                times.append(self._ip_buckets[ip].retry_after)
            return max(times) if times else 1.0

    def reset(self) -> None:
        """Clear all rate-limit state.  Intended for use in tests."""
        with self._lock:
            self._key_buckets.clear()
            self._ip_buckets.clear()
            self._last_cleanup = time.monotonic()

    def cleanup(self, max_age: float = 600):
        """Remove stale buckets older than max_age seconds."""
        now = time.monotonic()
        if now - self._last_cleanup < 60:
            return
        self._last_cleanup = now

        with self._lock:
            cutoff = now - max_age
            self._key_buckets = {
                k: v for k, v in self._key_buckets.items()
                if v.last_refill > cutoff
            }
            self._ip_buckets = {
                k: v for k, v in self._ip_buckets.items()
                if v.last_refill > cutoff
            }


# Singleton store
rate_limiter = RateLimiterStore()


async def check_rate_limit(request: Request):
    """FastAPI dependency: enforce rate limits. Call AFTER auth."""
    ip = request.client.host if request.client else "unknown"
    key_record = getattr(request.state, "api_key", None)
    key_id = key_record["id"] if key_record else None
    tier = key_record["tier"] if key_record else "free"

    # Check IP rate limit
    if not rate_limiter.check_ip(ip):
        retry = rate_limiter.get_retry_after(key_id, ip)
        raise HTTPException(
            status_code=429,
            detail={"error": "Rate limit exceeded (IP)", "retry_after_seconds": round(retry, 1)},
            headers={"Retry-After": str(int(retry) + 1)},
        )

    # Check key rate limit
    if key_id and not rate_limiter.check_key(key_id, tier):
        retry = rate_limiter.get_retry_after(key_id, ip)
        raise HTTPException(
            status_code=429,
            detail={"error": "Rate limit exceeded (API key)", "retry_after_seconds": round(retry, 1)},
            headers={"Retry-After": str(int(retry) + 1)},
        )

    # Periodic cleanup
    rate_limiter.cleanup()
