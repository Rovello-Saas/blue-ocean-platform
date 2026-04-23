"""
Scheduler daemon entrypoint.

Run this as a long-lived process to drive all recurring automation:
research pipeline (daily), agent-cost polling (30 min), ad performance
pulls (2h), decision engine (2h15), label sync (2h), daily counters
(midnight), competitor price checks (weekly), stock checks (daily),
feedback updates (weekly), and ready-product processing (hourly).

Usage:
    python -m src.scheduler               # foreground, Ctrl-C to stop
    python -m src.scheduler --once <job>  # run a single job and exit

The `--once` flag is useful for:
    - Manual kick-off of the research pipeline without waiting 24h
    - Smoke-testing a specific job during development
    - Triggering jobs from an external cron if you don't want the daemon

Deployment:
    - Railway / Heroku: add a `worker:` entry in the Procfile running this module
    - Systemd: invoke as `ExecStart=/path/to/python -m src.scheduler`
    - Docker: use as CMD

The scheduler uses APScheduler's BackgroundScheduler, so jobs run in a
thread pool. The main thread sleeps in a keep-alive loop and prints a
short health line every 5 minutes so `tail -f` output is intelligible.
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
from typing import Optional

from dotenv import load_dotenv

# `.env` overrides are already handled by src.core.config's load_dotenv call
# on import, but we call it again here before any other imports to ensure
# credentials are available when this is run as the primary entrypoint.
load_dotenv(override=True)

from src.core.config import AppConfig
from src.scheduler.jobs import JobScheduler
from src.sheets.manager import get_data_store

logger = logging.getLogger(__name__)


# Jobs that can be invoked via `--once <name>`. Keys mirror JobScheduler
# method names (minus the `job_` prefix) so the CLI reads naturally.
ONE_SHOT_JOBS = {
    "research": "job_research_pipeline",
    "agent": "job_poll_agent_costs",
    "performance": "job_pull_performance",
    "decisions": "job_run_decisions",
    "labels": "job_sync_labels",
    "counters": "job_update_daily_counters",
    "prices": "job_check_competitor_prices",
    "stock": "job_check_stock",
    "feedback": "job_update_feedback",
    "ready": "job_process_ready_products",
}


def _configure_logging(verbose: bool = False) -> None:
    """Configure root logger. Picks a format that reads cleanly in `tail -f`."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-5s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    # APScheduler is noisy at DEBUG; keep it at INFO even in verbose mode.
    logging.getLogger("apscheduler").setLevel(logging.INFO)


def _load_merged_config(store) -> AppConfig:
    """Load defaults.yaml + overlay Sheet config. Falls back to defaults on read failure."""
    config = AppConfig()
    try:
        sheet_config = store.get_config()
        if sheet_config:
            config.merge_sheet_config(sheet_config)
            logger.info("Merged config overlay from Google Sheet")
    except Exception as e:
        logger.warning("Could not load sheet config (using defaults only): %s", e)
    return config


def run_once(job_name: str) -> int:
    """Execute a single job by short name and return an exit code."""
    if job_name not in ONE_SHOT_JOBS:
        logger.error(
            "Unknown job '%s'. Valid: %s",
            job_name, ", ".join(sorted(ONE_SHOT_JOBS.keys())),
        )
        return 2

    method_name = ONE_SHOT_JOBS[job_name]
    store = get_data_store()
    config = _load_merged_config(store)
    scheduler = JobScheduler(store, config)

    job_fn = getattr(scheduler, method_name, None)
    if not callable(job_fn):
        logger.error("JobScheduler has no method '%s'", method_name)
        return 2

    logger.info("--- Running job '%s' (%s) once ---", job_name, method_name)
    start = time.monotonic()
    try:
        job_fn()
    except Exception as e:
        logger.error("Job '%s' raised: %s", job_name, e, exc_info=True)
        return 1
    logger.info("--- Job '%s' finished in %.1fs ---", job_name, time.monotonic() - start)
    return 0


def run_daemon() -> int:
    """Start the BackgroundScheduler and keep the process alive until signaled."""
    store = get_data_store()
    config = _load_merged_config(store)

    scheduler = JobScheduler(store, config)
    scheduler.start()

    # Make SIGTERM trigger a clean shutdown (Railway / systemd / Docker stop
    # sends SIGTERM by default; without this, APScheduler leaves its worker
    # threads hanging and the container is killed with SIGKILL after grace).
    stop_requested = {"flag": False}
    def _handle_signal(signum, _frame):
        logger.info("Received signal %d; shutting down scheduler...", signum)
        stop_requested["flag"] = True
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    # Dump the job manifest once at startup so it's easy to verify cadence
    # from the logs without having to tail for hours.
    for job in scheduler.get_job_status():
        logger.info("Scheduled: %-30s next=%s  trigger=%s",
                    job["name"], job["next_run"], job["trigger"])

    logger.info("Scheduler daemon ready — %d jobs", len(scheduler.get_job_status()))
    try:
        heartbeat_every = 300  # 5 minutes
        last_heartbeat = 0.0
        while not stop_requested["flag"]:
            time.sleep(1)
            now = time.monotonic()
            if now - last_heartbeat >= heartbeat_every:
                jobs = scheduler.get_job_status()
                logger.info("heartbeat: %d jobs scheduled", len(jobs))
                last_heartbeat = now
    finally:
        scheduler.stop()
        logger.info("Scheduler stopped.")
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.scheduler",
        description="Blue Ocean Platform — scheduler daemon",
    )
    parser.add_argument(
        "--once",
        metavar="JOB",
        help=(
            "Run a single job and exit. Valid values: "
            + ", ".join(sorted(ONE_SHOT_JOBS.keys()))
        ),
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable DEBUG logging.",
    )
    args = parser.parse_args(argv)

    _configure_logging(verbose=args.verbose)

    if args.once:
        return run_once(args.once)
    return run_daemon()


if __name__ == "__main__":
    sys.exit(main())
