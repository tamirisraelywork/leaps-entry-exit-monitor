"""
LEAPS Monitor Engine — Entry point.

Run:
    python -m monitor_engine.main

This starts the background scheduler and runs forever.
All secrets must be set as environment variables (see .env.example).
"""

from __future__ import annotations

import logging
import signal
import sys
import time
from pathlib import Path

# Add project root to sys.path so all imports resolve correctly
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("leaps.monitor")


def main():
    from monitor_engine.monitor_service import get_scheduler

    logger.info("=" * 60)
    logger.info("LEAPS Monitor Engine starting…")
    logger.info("Communicates with UI via BigQuery — no Streamlit needed.")
    logger.info("=" * 60)

    sched = get_scheduler()

    def _shutdown(signum, frame):
        logger.info("Shutdown signal received — stopping scheduler…")
        sched.shutdown(wait=False)
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    logger.info("Monitor engine running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(30)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Monitor engine stopped.")


if __name__ == "__main__":
    main()
