"""Request fingerprinting and bot/scraper detection."""

from __future__ import annotations

import hashlib
import re
import time
import threading
from collections import deque
from dataclasses import dataclass, field

from fastapi import Request

# Known bot/scraper User-Agent patterns
BOT_PATTERNS = re.compile(
    r"(curl|wget|python-requests|scrapy|httpie|axios|node-fetch|"
    r"go-http-client|java|httpclient|libwww|lwp|mechanize|"
    r"headlesschrome|phantomjs|selenium|puppeteer|playwright|"
    r"bot|spider|crawler|scraper)",
    re.IGNORECASE,
)


@dataclass
class ClientProfile:
    """Tracks request patterns for a single client."""
    request_times: deque = field(default_factory=lambda: deque(maxlen=50))
    bboxes: deque = field(default_factory=lambda: deque(maxlen=20))
    last_score: int = 0


class FingerprintEngine:
    """Analyzes requests for bot/scraper behavior."""

    def __init__(self):
        self._lock = threading.Lock()
        self._profiles: dict[str, ClientProfile] = {}  # keyed by key_id or IP

    def _get_profile(self, client_id: str) -> ClientProfile:
        with self._lock:
            if client_id not in self._profiles:
                self._profiles[client_id] = ClientProfile()
            return self._profiles[client_id]

    def score_request(self, request: Request, client_id: str, bbox: str | None = None) -> int:
        """Score a request for suspicion (0-100). Higher = more suspicious."""
        score = 0
        profile = self._get_profile(client_id)
        now = time.monotonic()

        # 1. User-Agent analysis
        ua = request.headers.get("user-agent", "")
        if not ua:
            score += 30
        elif BOT_PATTERNS.search(ua):
            score += 40

        # 2. Missing standard browser headers
        if not request.headers.get("accept-language"):
            score += 15
        if not request.headers.get("accept"):
            score += 10

        # 3. Request timing analysis
        profile.request_times.append(now)
        if len(profile.request_times) >= 5:
            intervals = [
                profile.request_times[i] - profile.request_times[i - 1]
                for i in range(1, len(profile.request_times))
            ]
            recent = intervals[-5:]
            avg_interval = sum(recent) / len(recent)
            if avg_interval < 0.5:  # More than 2 req/sec sustained
                score += 25
            elif avg_interval < 1.0:
                score += 10

        # 4. Bbox scanning pattern detection
        if bbox:
            profile.bboxes.append(bbox)
            if len(profile.bboxes) >= 5:
                grid_score = self._detect_grid_pattern(list(profile.bboxes))
                score += grid_score

        # 5. Missing referer (from our frontend)
        referer = request.headers.get("referer", "")
        if not referer:
            score += 10

        # Cap at 100
        score = min(score, 100)
        profile.last_score = score
        return score

    def _detect_grid_pattern(self, bboxes: list[str]) -> int:
        """Detect if bboxes form a systematic grid scan. Returns 0-35 score."""
        try:
            parsed = []
            for b in bboxes[-10:]:
                parts = [float(x) for x in b.split(",")]
                if len(parts) == 4:
                    parsed.append(parts)

            if len(parsed) < 4:
                return 0

            # Check for regular width/height across bboxes
            widths = [p[2] - p[0] for p in parsed]
            heights = [p[3] - p[1] for p in parsed]

            # If widths and heights are suspiciously uniform
            if widths and heights:
                w_variance = max(widths) - min(widths)
                h_variance = max(heights) - min(heights)
                if w_variance < 0.01 and h_variance < 0.01 and len(parsed) >= 4:
                    return 35  # Very likely a grid scan
                if w_variance < 0.1 and h_variance < 0.1 and len(parsed) >= 6:
                    return 20  # Likely systematic

            return 0
        except (ValueError, IndexError):
            return 0

    def compute_fingerprint_hash(self, request: Request) -> str:
        """Create a hash fingerprint from browser characteristics."""
        parts = [
            request.headers.get("user-agent", ""),
            request.headers.get("accept-language", ""),
            request.headers.get("accept-encoding", ""),
            request.headers.get("accept", ""),
        ]
        raw = "|".join(parts)
        return hashlib.md5(raw.encode()).hexdigest()[:16]

    def cleanup(self, max_age: float = 600):
        """Remove stale profiles."""
        now = time.monotonic()
        with self._lock:
            stale = [
                k for k, v in self._profiles.items()
                if v.request_times and (now - v.request_times[-1]) > max_age
            ]
            for k in stale:
                del self._profiles[k]


# Singleton
fingerprint_engine = FingerprintEngine()
