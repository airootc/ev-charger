"""Middleware that adds security headers to every HTTP response."""

from __future__ import annotations

import os

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Injects standard security headers into all responses."""

    # Resolved once at import time; override with ENVIRONMENT env-var.
    _is_production: bool = os.getenv("ENVIRONMENT", "development").lower() == "production"

    async def dispatch(self, request: Request, call_next) -> Response:
        response: Response = await call_next(request)

        # 1. Prevent MIME-type sniffing
        response.headers["X-Content-Type-Options"] = "nosniff"

        # 2. Deny framing (clickjacking protection)
        response.headers["X-Frame-Options"] = "DENY"

        # 3. XSS filter (legacy browsers)
        response.headers["X-XSS-Protection"] = "1; mode=block"

        # 4. HSTS — only when the connection is actually TLS *or* we are
        #    running in a production environment (typically behind a TLS
        #    terminating proxy).
        is_https = request.url.scheme == "https"
        if is_https or self._is_production:
            response.headers["Strict-Transport-Security"] = (
                "max-age=31536000; includeSubDomains"
            )

        # 5. Content-Security-Policy
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' https://unpkg.com; "
            "style-src 'self' 'unsafe-inline' https://unpkg.com; "
            "img-src 'self' data: blob: https://*.basemaps.cartocdn.com https://*.openstreetmap.org; "
            "connect-src 'self' https://*.basemaps.cartocdn.com https://unpkg.com; "
            "worker-src 'self' blob:; "
            "child-src blob:; "
            "frame-ancestors 'none'"
        )

        # 6. Referrer policy
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"

        # 7. Permissions policy
        response.headers["Permissions-Policy"] = (
            "geolocation=(self), camera=(), microphone=()"
        )

        # 8. Remove the Server header if present
        if "Server" in response.headers:
            del response.headers["Server"]

        return response
