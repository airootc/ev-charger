"""Tests for collector utilities: rate limiting, retry, base collector."""

import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils import RateLimiter, retry_request
from models import SourceConfig, SearchParams, CrawlState, RateLimitConfig, AuthConfig


class TestRateLimiter:
    def test_no_limit(self):
        """RateLimiter with no limits should not block."""
        limiter = RateLimiter()
        start = time.monotonic()
        for _ in range(5):
            limiter.wait()
        elapsed = time.monotonic() - start
        assert elapsed < 0.1

    def test_per_second_limit(self):
        """RateLimiter should enforce per-second rate."""
        limiter = RateLimiter(requests_per_second=10)
        # 10 req/s = 0.1s between requests
        start = time.monotonic()
        limiter.wait()
        limiter.wait()
        elapsed = time.monotonic() - start
        # Should take at least ~0.1s for 2 requests
        assert elapsed >= 0.08

    def test_per_minute_limit(self):
        """RateLimiter with per-minute config computes correct interval."""
        limiter = RateLimiter(requests_per_minute=60)
        # 60 req/min = 1 req/s = 1.0s interval
        assert abs(limiter._min_interval - 1.0) < 0.01


class TestRetryRequest:
    def test_success_no_retry(self):
        """Function that succeeds on first call should not retry."""
        call_count = 0

        @retry_request(max_retries=3, base_delay=0.01)
        def succeeding_func():
            nonlocal call_count
            call_count += 1
            return "ok"

        result = succeeding_func()
        assert result == "ok"
        assert call_count == 1

    def test_retry_then_success(self):
        """Function that fails twice then succeeds should retry."""
        call_count = 0

        @retry_request(max_retries=3, base_delay=0.01)
        def flaky_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("transient failure")
            return "ok"

        result = flaky_func()
        assert result == "ok"
        assert call_count == 3

    def test_all_retries_exhausted(self):
        """Function that always fails should raise after max retries."""
        call_count = 0

        @retry_request(max_retries=2, base_delay=0.01)
        def failing_func():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("permanent failure")

        with pytest.raises(ConnectionError, match="permanent failure"):
            failing_func()
        assert call_count == 3  # 1 initial + 2 retries


class TestBaseCollector:
    def test_subclass_must_implement_abstract_methods(self):
        """Verify BaseCollector can't be instantiated directly."""
        from collectors.base import BaseCollector

        with pytest.raises(TypeError):
            BaseCollector(
                config=MagicMock(),
                rate_limiter=RateLimiter(),
                logger=MagicMock(),
            )

    def test_dry_run(self):
        """Verify dry_run returns expected info."""
        from collectors.api_collector import APICollector

        config = SourceConfig(
            name="test",
            type="api",
            base_url="https://api.test.com/data",
            rate_limit=RateLimitConfig(requests_per_second=5),
            auth=AuthConfig(type="none"),
        )
        collector = APICollector(
            config=config,
            rate_limiter=RateLimiter(),
            logger=MagicMock(),
        )

        params = SearchParams(keywords=["python", "data"])
        info = collector.dry_run(params)

        assert info["source"] == "test"
        assert info["type"] == "api"
        assert info["base_url"] == "https://api.test.com/data"
        assert info["keywords"] == ["python", "data"]
