"""
Railway / Heroku worker entrypoint.

Thin shim over `python -m src.scheduler`. Kept at project root because some
PaaS setups (Railway's default Procfile behaviour) expect a top-level
script file rather than a module invocation. The daemon logic — job list,
signal handling, heartbeat, `--once` escape hatch — lives in
`src/scheduler/__main__.py` so both entrypoints stay in sync.

Usage:
    python worker.py                    # Railway / Heroku worker service
    python -m src.scheduler             # equivalent, preferred locally
    python -m src.scheduler --once research   # run one job and exit
"""

import os
import sys

# Ensure the project root is on sys.path when invoked via bare `python
# worker.py` from any directory.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.scheduler.__main__ import main  # noqa: E402  (must come after sys.path fix)

if __name__ == "__main__":
    sys.exit(main())
