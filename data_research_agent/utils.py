"""Shared utilities: logging, rate limiting, retry, file I/O."""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
import time
from functools import wraps
from pathlib import Path
from typing import Any, Callable


def setup_logger(name: str, log_file: str | None = None, level: int = logging.INFO) -> logging.Logger:
    """Create a logger that writes to console and optionally to a file."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if log_file:
        ensure_dirs(str(Path(log_file).parent))
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


class RateLimiter:
    """Thread-safe rate limiter supporting per-second or per-minute limits."""

    def __init__(
        self,
        requests_per_second: float | None = None,
        requests_per_minute: float | None = None,
    ):
        if requests_per_second:
            self._min_interval = 1.0 / requests_per_second
        elif requests_per_minute:
            self._min_interval = 60.0 / requests_per_minute
        else:
            self._min_interval = 0.0

        self._last_request_time = 0.0
        self._lock = threading.Lock()

    def wait(self):
        """Block until it's safe to make the next request."""
        if self._min_interval <= 0:
            return

        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            wait_time = self._min_interval - elapsed
            if wait_time > 0:
                time.sleep(wait_time)
            self._last_request_time = time.monotonic()


def retry_request(max_retries: int = 3, base_delay: float = 1.0):
    """Decorator: retry a function with exponential backoff on exception."""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        delay = base_delay * (2 ** attempt)
                        logging.getLogger("retry").warning(
                            "Attempt %d/%d failed for %s: %s. Retrying in %.1fs",
                            attempt + 1, max_retries + 1, func.__name__, e, delay,
                        )
                        time.sleep(delay)
            raise last_exception

        return wrapper

    return decorator


def save_json(data: Any, filepath: str) -> None:
    """Atomic JSON write: write to temp file then rename."""
    ensure_dirs(str(Path(filepath).parent))
    fd, tmp_path = tempfile.mkstemp(
        dir=str(Path(filepath).parent), suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2, default=str)
        os.replace(tmp_path, filepath)
    except Exception:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


def load_json(filepath: str) -> Any:
    """Load JSON file with error handling. Returns None if file doesn't exist."""
    try:
        with open(filepath, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as e:
        logging.getLogger("utils").error("Failed to parse JSON from %s: %s", filepath, e)
        return None


def ensure_dirs(*paths: str) -> None:
    """Create directories if they don't exist."""
    for path in paths:
        Path(path).mkdir(parents=True, exist_ok=True)


def get_nested_value(data: dict, dotted_path: str) -> Any:
    """Extract a value from a nested dict using dot notation.
    e.g., get_nested_value({"a": {"b": [1,2]}}, "a.b") -> [1, 2]
    """
    keys = dotted_path.split(".")
    current = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        else:
            return None
        if current is None:
            return None
    return current
