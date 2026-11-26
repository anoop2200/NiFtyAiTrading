"""Restore NiftyTradingAI.db from a backup. Stop the recorder and health API first.

Usage: .venv\\Scripts\\python scripts\\restore_db.py <backup-file> --yes
The current database is kept as NiftyTradingAI.pre-restore-<stamp>.db.
"""
import argparse
from pathlib import Path

import _bootstrap  # noqa: F401
from app.config import settings
from app.database.backup import restore

ap = argparse.ArgumentParser()
ap.add_argument("backup")
ap.add_argument("--yes", action="store_true", help="confirm the recorder and API are stopped")
a = ap.parse_args()
if not a.yes:
    raise SystemExit("refusing without --yes (stop the recorder and health API first)")
print(f"restored: {restore(Path(a.backup), settings().path('database'))}")
