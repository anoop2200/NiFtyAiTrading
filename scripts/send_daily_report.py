"""Daily paper P&L report with capital usage (v1.3 §9.7, §14.2) — run after 15:25 IST.

  .venv\\Scripts\\python scripts\\send_daily_report.py                      # latest run, latest session, dry run
  .venv\\Scripts\\python scripts\\send_daily_report.py --db main --send     # paper run in the trading DB, to Telegram
  .venv\\Scripts\\python scripts\\send_daily_report.py --run-id <id> --date 2026-11-25

Without --send (or without TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID) the message is printed and written to
data/runtime/telegram_outbox.jsonl. The databases are opened read-only.
"""
import argparse
import json
import sys

import _bootstrap  # noqa: F401
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.config import PROJECT_ROOT, settings
from app.database.db import make_ro_engine, research_db_path
from app.database.models import CapitalUsage
from app.notify.telegram import INFO, Alert, Notifier, OutboxTransport, make_transport
from app.reports.daily import build_daily_report, format_report


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", choices=("research", "main"), default="research")
    ap.add_argument("--run-id")
    ap.add_argument("--date")
    ap.add_argument("--send", action="store_true", help="send to Telegram when credentials are configured")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--combined", action="store_true", help="the combined all-strategy report (rules + book)")
    ap.add_argument("--book-run", help="book run id for --combined (default: config book.run_id; research: book_replay)")
    ap.add_argument("--note", action="append", default=[], help="extra line for the report's Notes (repeatable)")
    a = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    eng = make_ro_engine(research_db_path() if a.db == "research" else settings().path("database"))
    if a.combined:
        return _combined(a, eng)
    with Session(eng) as s:
        run_id = a.run_id or s.scalar(select(CapitalUsage.run_id).order_by(CapitalUsage.id.desc()).limit(1))
        if not run_id:
            print("no CapitalUsage rows in this database")
            return 1
        date = a.date or s.scalar(select(func.max(CapitalUsage.session_date)).where(CapitalUsage.run_id == run_id))
    d = build_daily_report(eng, run_id, date)
    text = format_report(d)
    print(json.dumps(d, indent=2, default=str) if a.json else text)
    transport = make_transport() if a.send else OutboxTransport(PROJECT_ROOT / "data" / "runtime" / "telegram_outbox.jsonl")
    outcome = Notifier(transport).send(Alert("daily_pnl", INFO, text, key=f"daily|{run_id}|{date}"))
    print(f"\n[{transport.name}] {outcome}")
    return 0 if outcome == "SENT" else 2


def _combined(a, eng) -> int:
    import yaml
    from app.reports.combined import build_combined, format_combined
    pc = yaml.safe_load((PROJECT_ROOT / "config" / "paper.yaml").read_text(encoding="utf-8"))
    book = pc.get("book") or {}
    rules_run = a.run_id or (pc["run_id"] if a.db == "main" else None)
    book_run = a.book_run or (book.get("run_id") if a.db == "main" else "book_replay")
    with Session(eng) as s:
        date = a.date or s.scalar(select(func.max(CapitalUsage.session_date)).where(
            CapitalUsage.run_id.in_([x for x in (rules_run, book_run) if x])))
    if not date:
        print("no sessions for these runs")
        return 1
    d = build_combined(eng, date, rules_run, list(pc.get("strategies") or []) if rules_run else [], book_run,
                       list(book.get("codes") or []), notes=a.note)
    parts = format_combined(d)
    print(json.dumps(d, indent=2, default=str) if a.json else "\n\n----\n\n".join(parts))
    transport = make_transport() if a.send else OutboxTransport(PROJECT_ROOT / "data" / "runtime" / "telegram_outbox.jsonl")
    n = Notifier(transport)
    out = [n.send(Alert("daily_pnl", INFO, t, key=f"daily|combined|{date}|{i}")) for i, t in enumerate(parts, 1)]
    print(f"\n[{transport.name}] {out}")
    return 0 if all(o == "SENT" for o in out) else 2


if __name__ == "__main__":
    raise SystemExit(main())
