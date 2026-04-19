"""
Background worker for the Blue Ocean Platform.
Runs the scheduler for automated product discovery, performance tracking,
and decision engine.

Usage (Railway worker service):
    python worker.py
"""

import sys
import os
import logging
import time

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from src.core.config import AppConfig
from src.sheets.manager import get_data_store
from src.scheduler.jobs import JobScheduler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def main():
    logger.info("=" * 60)
    logger.info("  Blue Ocean Platform - Background Worker")
    logger.info("=" * 60)

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

        logger.info("Scheduler started successfully! Waiting for jobs...")

        # Keep running
        while True:
            time.sleep(60)

            # Health check
            jobs = scheduler.get_job_status()
            running_jobs = len([j for j in jobs if j.get("next_run")])
            logger.debug("Scheduler health: %d jobs active", running_jobs)

    except KeyboardInterrupt:
        logger.info("Shutting down scheduler...")
        if 'scheduler' in locals():
            scheduler.stop()
        logger.info("Goodbye!")

    except Exception as e:
        logger.error("Fatal error: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
