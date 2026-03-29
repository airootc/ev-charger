"""Crawl state management and scheduling."""

from __future__ import annotations

import json
import logging
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Callable

from models import CrawlState
from utils import ensure_dirs, load_json, save_json

STATE_FILE = "data/state/crawl_state.json"


class CrawlStateManager:
    """Manages crawl state: load, save, update per source."""

    def __init__(self, state_file: str = STATE_FILE, logger: logging.Logger | None = None):
        self.state_file = state_file
        self.logger = logger or logging.getLogger("crawl_state")
        self._states: dict[str, CrawlState] = {}
        self._dirty = False
        self._load()

    def _load(self):
        """Load state from disk."""
        data = load_json(self.state_file)
        if data and isinstance(data, dict):
            for name, state_data in data.items():
                self._states[name] = CrawlState(**state_data)
            self.logger.info("Loaded crawl state for %d sources", len(self._states))
        else:
            self.logger.info("No existing crawl state found, starting fresh")

    def save(self):
        """Save current state to disk atomically."""
        data = {name: state.model_dump() for name, state in self._states.items()}
        save_json(data, self.state_file)
        self._dirty = False
        self.logger.debug("Saved crawl state for %d sources", len(self._states))

    def get(self, source_name: str) -> CrawlState:
        """Get state for a source, creating a new one if not found."""
        if source_name not in self._states:
            self._states[source_name] = CrawlState(source_name=source_name)
        return self._states[source_name]

    def update(self, state: CrawlState):
        """Update state for a source and save to disk."""
        self._states[state.source_name] = state
        self._dirty = True
        self.save()  # Save immediately after each source completes

    def register_signal_handler(self):
        """Register SIGINT handler to save state on interrupt."""
        original_handler = signal.getsignal(signal.SIGINT)

        def handler(signum, frame):
            self.logger.info("SIGINT received, saving crawl state...")
            if self._dirty:
                self.save()
            self.logger.info("State saved. Exiting.")
            # Restore and re-raise
            signal.signal(signal.SIGINT, original_handler)
            sys.exit(0)

        signal.signal(signal.SIGINT, handler)


class CrawlScheduler:
    """Runs the crawl pipeline on a schedule."""

    def __init__(
        self,
        crawl_func: Callable[[], None],
        logger: logging.Logger | None = None,
    ):
        self.crawl_func = crawl_func
        self.logger = logger or logging.getLogger("scheduler")

    def run_once(self):
        """Run the crawl function once."""
        self.logger.info("Running single crawl cycle...")
        start = time.monotonic()
        self.crawl_func()
        elapsed = time.monotonic() - start
        self.logger.info("Crawl cycle completed in %.1f seconds", elapsed)

    def run_with_apscheduler(self, cron_expression: str):
        """Run the crawl function on a cron schedule using APScheduler."""
        try:
            from apscheduler.schedulers.blocking import BlockingScheduler
            from apscheduler.triggers.cron import CronTrigger
        except ImportError:
            self.logger.warning(
                "APScheduler not installed, falling back to simple loop. "
                "Install with: pip install apscheduler"
            )
            self._run_simple_loop(cron_expression)
            return

        scheduler = BlockingScheduler()
        trigger = CronTrigger.from_crontab(cron_expression)

        scheduler.add_job(self.crawl_func, trigger, id="crawl_job", name="data_crawl")

        self.logger.info("Scheduled crawl with cron: %s", cron_expression)
        self.logger.info("Press Ctrl+C to stop")

        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            self.logger.info("Scheduler stopped")
            scheduler.shutdown()

    def _run_simple_loop(self, cron_expression: str):
        """Fallback: simple loop with interval parsed from cron expression."""
        interval = self._parse_interval(cron_expression)
        self.logger.info(
            "Running crawl every %d seconds (parsed from cron: %s)",
            interval, cron_expression,
        )

        try:
            while True:
                self.run_once()
                self.logger.info("Next crawl in %d seconds...", interval)
                time.sleep(interval)
        except KeyboardInterrupt:
            self.logger.info("Loop stopped by user")

    def _parse_interval(self, cron_expression: str) -> int:
        """Parse a rough interval in seconds from a cron expression.

        This is a best-effort fallback — only handles simple patterns:
            "*/N * * * *" -> every N minutes
            "0 */N * * *" -> every N hours
            "0 0 * * *"   -> daily (86400s)
        """
        parts = cron_expression.strip().split()
        if len(parts) != 5:
            return 3600  # default: hourly

        minute, hour = parts[0], parts[1]

        if minute.startswith("*/"):
            try:
                return int(minute[2:]) * 60
            except ValueError:
                pass

        if hour.startswith("*/"):
            try:
                return int(hour[2:]) * 3600
            except ValueError:
                pass

        # Default to hourly
        return 3600
