"""Import historical option-chain snapshots from the existing PostgreSQL DB (read-only) to Parquet.

Needs PG_HOST / PG_PORT / PG_DB / PG_USER / PG_PASSWORD in .env or Credential Manager.
Prefer a dedicated read-only role; the session is forced READ ONLY regardless.

Usage:
  .venv\\Scripts\\python scripts\\import_pg_history.py --list            # show available days only
  .venv\\Scripts\\python scripts\\import_pg_history.py                   # import all missing days
  .venv\\Scripts\\python scripts\\import_pg_history.py --from 2026-06-01 --to 2026-06-30 [--force]
"""
import argparse
import json
from datetime import date

import _bootstrap  # noqa: F401
from loguru import logger

from app.clock import now_ist
from app.config import settings
from app.database.db import init_db, log_event, make_engine
from app.historical.pg_import import PgConfig, check_source, connect_read_only, import_day, list_days
from app.logging_setup import setup_logging


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--from", dest="d_from", type=date.fromisoformat)
    ap.add_argument("--to", dest="d_to", type=date.fromisoformat)
    ap.add_argument("--force", action="store_true", help="re-import days already imported")
    a = ap.parse_args()

    cfg = settings()
    setup_logging("historical-import")
    hc = cfg["historical_import"]
    engine = make_engine()
    init_db(engine)
    conn = connect_read_only(PgConfig.from_secrets())
    try:
        cols = check_source(conn, hc["table"])
        days = list_days(conn, hc["table"], hc["symbol_filter"])
        today = now_ist().date()
        days = [d for d in days if d < today]  # never import a session that may still be in progress
        if a.d_from:
            days = [d for d in days if d >= a.d_from]
        if a.d_to:
            days = [d for d in days if d <= a.d_to]
        logger.info(f"source ok ({len(cols)} columns); {len(days)} completed days "
                    f"{days[0] if days else '-'} .. {days[-1] if days else '-'}")
        if a.list:
            print("\n".join(d.isoformat() for d in days))
            return 0
        out_root = cfg.path("historical")
        summary = []
        for d in days:
            r = import_day(conn, engine, out_root, hc["table"], hc["symbol_filter"], d, hc["dataset"],
                           hc["data_tier"], hc["fetch_batch_rows"], a.force)
            if r is not None:
                summary.append(r)
        log_event(engine, "INFO", "historical-import", "pg_import_done",
                  {"days_imported": len(summary), "rows": sum(s.get("rows", 0) for s in summary)})
        report = cfg.path("reports") / f"pg_import_{now_ist():%Y%m%d-%H%M%S}.json"
        report.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
        print(f"imported {len(summary)} day(s); skipped {len(days) - len(summary)} already present; report {report}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
