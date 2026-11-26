"""Explicit, audited registry status change (v1.3 §5.4 / §18: no automatic research-to-production promotion).

Usage:
  .venv\\Scripts\\python scripts\\set_indicator_status.py --list
  .venv\\Scripts\\python scripts\\set_indicator_status.py ema 1.0.0 APPROVED --actor anoop --note "reviewed"
APPROVED requires a passed validation report (scripts/validate_indicators.py).
"""
import argparse

import _bootstrap  # noqa: F401
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.database.db import init_db, make_engine
from app.database.models import IndicatorDefinitions
from app.indicators.registry import STATUSES, set_status, sync_definitions


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("indicator", nargs="?")
    ap.add_argument("version", nargs="?")
    ap.add_argument("status", nargs="?", choices=STATUSES)
    ap.add_argument("--actor", default="")
    ap.add_argument("--note", default="")
    ap.add_argument("--list", action="store_true")
    a = ap.parse_args()
    engine = make_engine()
    init_db(engine)
    sync_definitions(engine)
    if a.list or not a.indicator:
        with Session(engine) as s:
            for r in s.scalars(select(IndicatorDefinitions).order_by(IndicatorDefinitions.indicator_id)):
                print(f"{r.indicator_id:<18} {r.version:<8} {r.status:<10} {r.family:<13} {r.name}")
        return 0
    if not (a.version and a.status and a.actor):
        ap.error("indicator, version, status and --actor are required")
    set_status(engine, a.indicator, a.version, a.status, actor=a.actor, note=a.note)
    print(f"{a.indicator}@{a.version} -> {a.status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
