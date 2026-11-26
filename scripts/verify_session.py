"""Verify a recorded day of raw Parquet and save a JSON report.

Usage: .venv\\Scripts\\python scripts\\verify_session.py [YYYY-MM-DD]   (default: today)
Exit code 0 = all checks passed.
"""
import json
import sys

import _bootstrap  # noqa: F401
from app.clock import now_ist
from app.config import settings
from app.data.verify import verify_day
from app.database.db import make_engine

cfg = settings()
day = sys.argv[1] if len(sys.argv) > 1 else now_ist().date().isoformat()
res = verify_day(cfg.path("raw"), day, make_engine(), liveness_s=cfg.get("recorder.feed_liveness_s"))
path = cfg.path("reports") / f"verify_{day}.json"
path.write_text(json.dumps(res, indent=2, default=str), encoding="utf-8")
print(json.dumps(res, indent=2, default=str))
print(f"\n{'PASSED' if res['passed'] else 'FAILED'} — report {path}")
raise SystemExit(0 if res["passed"] else 1)
