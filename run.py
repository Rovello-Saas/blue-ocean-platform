"""
Main entry point for the Qoveliqo Ads automation system.
Starts the background scheduler and the Streamlit dashboard.

Usage:
    # Run the full system (scheduler + dashboard):
    python run.py

    # Run only the dashboard:
    streamlit run dashboard/app.py

    # Run only the scheduler (no UI):
    python run.py --scheduler-only
"""

import sys
import os
import logging
import argparse
import subprocess
import threading
import time

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.core.config import AppConfig
from src.sheets.manager import get_data_store
from src.scheduler.jobs import JobScheduler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("qoveliqo_ads.log"),
    ],
)
logger = logging.getLogger(__name__)


def start_scheduler():
    """Start the background job scheduler."""
    logger.info("Starting background scheduler...")

    try:
        store = get_data_store()
        config = AppConfig()

        # Load config from Sheet
        try:
            sheet_config = store.get_config()
            if sheet_config:
                config.merge_sheet_config(sheet_config)
                logger.info("Loaded config from Google Sheet")
        except Exception as e:
            logger.warning("Could not load sheet config: %s. Using defaults.", e)

        scheduler = JobScheduler(store, config)
        scheduler.start()

        logger.info("Scheduler started successfully!")
        return scheduler

    except Exception as e:
        logger.error("Failed to start scheduler: %s", e)
        return None


def start_dashboard(port: str = None):
    """Start the Streamlit dashboard in a subprocess."""
    logger.info("Starting Streamlit dashboard...")

    dashboard_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "dashboard", "app.py"
    )

    # Use PORT env var (Railway), --port arg, or default 8501
    serve_port = port or os.environ.get("PORT", "8501")

    process = subprocess.Popen(
        [
            sys.executable, "-m", "streamlit", "run",
            dashboard_path,
            "--server.port", serve_port,
            "--server.address", "0.0.0.0",
            "--server.headless", "true",
        ],
        cwd=os.path.dirname(os.path.abspath(__file__)),
    )

    logger.info("Dashboard started on http://0.0.0.0:%s", serve_port)
    return process


def main():
    parser = argparse.ArgumentParser(description="Qoveliqo Ads Automation System")
    parser.add_argument(
        "--scheduler-only",
        action="store_true",
        help="Run only the scheduler without the dashboard",
    )
    parser.add_argument(
        "--dashboard-only",
        action="store_true",
        help="Run only the dashboard without the scheduler",
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("  Qoveliqo Ads - AI Product Research & Ads Automation")
    logger.info("=" * 60)

    scheduler = None
    dashboard_process = None

    try:
        if not args.dashboard_only:
            scheduler = start_scheduler()

        if not args.scheduler_only:
            dashboard_process = start_dashboard()

        # Keep running
        logger.info("System is running. Press Ctrl+C to stop.")

        while True:
            time.sleep(60)

            # Health check
            if scheduler:
                jobs = scheduler.get_job_status()
                running_jobs = len([j for j in jobs if j.get("next_run")])
                logger.debug("Scheduler health: %d jobs active", running_jobs)

    except KeyboardInterrupt:
        logger.info("Shutting down...")

        if scheduler:
            scheduler.stop()
            logger.info("Scheduler stopped.")

        if dashboard_process:
            dashboard_process.terminate()
            dashboard_process.wait()
            logger.info("Dashboard stopped.")

        logger.info("Goodbye!")

    except Exception as e:
        logger.error("Fatal error: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
