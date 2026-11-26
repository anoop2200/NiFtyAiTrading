"""Phase 9 paper trading engine (rule-only). PAPER ONLY — there is no order API anywhere in this project.

  live     .venv\\Scripts\\python scripts\\run_paper.py live                 # today: recorder + paper engine, one process
  recover  .venv\\Scripts\\python scripts\\run_paper.py live --recover --actor NAME   # after a crash / forced restart
  replay   .venv\\Scripts\\python scripts\\run_paper.py replay [--from D] [--to D] [--run-id ID]   # history "as live"
  status   .venv\\Scripts\\python scripts\\run_paper.py status [--db main|research]
  resolve  .venv\\Scripts\\python scripts\\run_paper.py resolve --run-id ID --date D --actor NAME --note "..."
  labels   .venv\\Scripts\\python scripts\\run_paper.py labels [--db main|research]   # Phase 7 label readiness

live: the market-data recorder keeps the only Dhan connection and hands every feed batch to the paper engine
(same process). Every engine input is journaled first (data/paper/<run>/<date>.jsonl). If an unfinished journal
exists for today the engine refuses to start until `--recover --actor NAME`: the journal is replayed, verified
against its checkpoints, and only then does the engine continue (v1.3 §12.3). At record_end the day is closed,
labelled, reconciled, persisted, and the daily P&L report is sent.

replay: history days pushed through the same engine exactly as live would push them, into the research DB
(mode paper_replay). It exercises journal, closeout, reconciliation and labels; results equal the backtest.
"""
import argparse
import asyncio
import json
import sys
import time
from dataclasses import asdict
from pathlib import Path

import _bootstrap  # noqa: F401
import yaml
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.backtest.fees import FeeSchedule
from app.config import PROJECT_ROOT, settings
from app.control.actions import COMMANDS, KILL_FILE, OperatorLog, reset_loss_latch
from app.database.db import init_db, make_engine, research_db_path
from app.database.models import PaperSessions, TradeLabels
from app.engine.closeout import closeout, open_session_row, resolve_incident
from app.engine.feeds import history_day, history_events, prior_daily, push, warmup_bars
from app.engine.journal import Journal
from app.engine.session import PaperSession, SessionSpec
from app.market.calendar import EventCalendar, TradingCalendar

PAPER_CFG = PROJECT_ROOT / "config" / "paper.yaml"
CARRY = PROJECT_ROOT / "data" / "runtime" / "paper_risk_carry.json"


def paper_cfg() -> dict:
    return yaml.safe_load(PAPER_CFG.read_text(encoding="utf-8"))


def journal_path(run_id: str, day: str) -> Path:
    return PROJECT_ROOT / "data" / "paper" / run_id / f"{day}.jsonl"


def code_fingerprint() -> str:
    from app.research.registry import code_fingerprint as fp
    return fp()["source_sha256"]


def _notifier():
    from app.notify.telegram import Notifier, make_transport
    return Notifier(make_transport())


def _report(db, run_id, day, notifier) -> None:
    from app.notify.telegram import INFO, Alert
    from app.reports.daily import build_daily_report, format_report
    text = format_report(build_daily_report(db, run_id, day))
    print(text)
    if notifier:
        notifier.send(Alert("daily_pnl", INFO, text, key=f"daily|{run_id}|{day}"))


def _summary(day, r) -> str:
    rec = r["reconciliation"]
    return (f"{day}: {r['status']} · trades {r['trades']} · signals {r['signals']} · labels {r['labels']} · "
            f"reconciled {rec['ok']} (peak ₹{rec['peak_capital_broker']:,.0f})")


# ---------------------------------------------------------------------------------------------- replay
def cmd_replay(a) -> int:
    from app.marketdata.pipeline import history_days
    from app.research.simulate import HistoryContext
    cfg, pc = settings(), paper_cfg()
    ds = cfg.get("historical_import.dataset")
    days = [d for d in history_days(cfg.path("historical"), ds)
            if (not a.d_from or d >= a.d_from) and (not a.d_to or d <= a.d_to)]
    ctx = HistoryContext.load(cfg, ds, history_days(cfg.path("historical"), ds))
    db = make_engine(research_db_path())
    init_db(db)
    run_id = a.run_id or f"replay_{pc['run_id']}"
    runtime = PROJECT_ROOT / "data" / "paper" / run_id / "_runtime"      # never the live kill switch / carry
    carry = runtime / "carry.json"
    if carry.exists() and not a.keep_carry:
        carry.unlink()
    for day in days:
        q, idx, lot = history_day(ctx, day)
        ok, why = ctx.quality.get(day, (False, "NOT_RECONCILED"))
        spec = SessionSpec(run_id=run_id, session=day, lot_size=lot, source=f"replay:{ds}", data_label=ctx.label,
                           mode="paper_replay", strategies=tuple(pc["strategies"]),
                           overrides={"require_futures": False}, prior_daily=prior_daily(ctx.index, day),
                           data_ok=ok, data_reason=why, feed_liveness_s=15.0,
                           selection_quote_age_s=pc["selection_quote_age_s"], feed_outage_s=a.feed_outage_s,
                           fill=asdict(ctx.fill), checkpoint_every=pc["checkpoint_every"])
        jp = journal_path(run_id, day)
        jp.unlink(missing_ok=True)
        Path(str(jp) + ".gz").unlink(missing_ok=True)
        s = PaperSession(spec, jp, ctx.fees, ctx.cal, ctx.events, kill_file=runtime / "kill.json",
                         commands=runtime / "commands", carry_file=carry,
                         warmup=warmup_bars(ctx.index, day, a.warmup_sessions))
        # like the backtest, a simulated operator resets a carried consecutive-loss latch at the next session
        # (through the same audited, journaled command path the real operator uses)
        if (s.carry_in.get("loss_latch_session") or "9999") < day:
            reset_loss_latch("replay-operator-sim", "replay: reset at next session", commands=runtime / "commands",
                             log=OperatorLog(runtime / "operator_actions.jsonl"))
        open_session_row(db, s, code_fingerprint())
        push(s, history_events(q, idx, tick_s=5.0, vix_bars=ctx.vix_day(day)))
        print(_summary(day, closeout(db, s)))
    return 0


# ---------------------------------------------------------------------------------------------- live
async def _live(a) -> int:
    from app.clock import now_ist
    from app.data.recorder import Recorder, end_time_today, prepare
    from app.engine.feeds import LiveAdapter
    from app.logging_setup import setup_logging
    from loguru import logger

    cfg, pc = settings(), paper_cfg()
    setup_logging("trading-engine")
    db = make_engine()
    init_db(db)
    today = now_ist().date()
    day = today.isoformat()
    cal, fees, events = TradingCalendar.load(), FeeSchedule.load(), EventCalendar.load()
    if not cal.is_trading_day(today):
        logger.error(f"{day} is not a trading session")
        return 2
    run_id = pc["run_id"]
    jp = journal_path(run_id, day)
    notifier = _notifier()
    unfinished = jp.exists() and not any(r["k"] == "end" for r in Journal.read(jp))
    if Journal.closed(jp) or (jp.exists() and not unfinished):
        logger.error(f"session {day} of {run_id} already closed ({jp})")
        return 2
    if unfinished and not a.recover:
        logger.error(f"unfinished journal {jp}: the engine stopped uncleanly. Restart with --recover --actor NAME "
                     "after checking why (it is replayed and verified before trading continues).")
        return 3
    if a.recover and not a.actor:
        logger.error("--recover needs --actor")
        return 2
    end_at = end_time_today(cfg, a.duration_min)
    p = await prepare(cfg, db, today, a.spot)
    lot = p.selection.report.get("nearest_lot_size")
    if lot not in (25, 50, 65, 75):
        logger.error(f"implausible lot size {lot} from the instrument master")
        return 2
    future = p.universe.futures[0].security_id if p.universe.futures else None
    if unfinished:
        s = PaperSession.recover(jp, fees, cal, events, kill_file=KILL_FILE, commands=COMMANDS, carry_file=CARRY,
                                 notifier=notifier)
        s._incident("OPERATOR_RECOVERY", {"actor": a.actor, "recovery": s.recovery}, notify=True)
        logger.warning(f"recovered {s.recovery}")
    else:
        spec = SessionSpec(run_id=run_id, session=day, lot_size=int(lot), source="live_feed",
                           data_label="observed_feed", mode="paper", strategies=tuple(pc["strategies"]),
                           overrides={"require_futures": bool(pc["require_futures"])},
                           prior_daily=_prior_daily_live(cfg, day, p.universe.index.security_id),
                           feed_liveness_s=pc["feed_liveness_s"], selection_quote_age_s=pc["selection_quote_age_s"],
                           feed_outage_s=pc["feed_outage_s"], checkpoint_every=pc["checkpoint_every"])
        s = PaperSession(spec, jp, fees, cal, events, kill_file=KILL_FILE, commands=COMMANDS, carry_file=CARRY,
                         notifier=notifier)
    open_session_row(db, s, code_fingerprint())
    adapter = LiveAdapter(s, today, p.universe.index.security_id, future, p.universe.options,
                          vix_id=p.universe.vix.security_id if p.universe.vix else None)
    rec = Recorder(cfg, db, p, end_at, chain_enabled=not a.no_chain)
    rec.taps.append(adapter.on_rows)

    async def clock():
        while now_ist() < end_at:
            try:
                adapter.tick(time.time_ns())
            except Exception as e:  # noqa: BLE001
                logger.error(f"engine tick failed: {e!r}")
                s.kill.trip("RISK_EVALUATION_ERROR", repr(e)[:200])
            await asyncio.sleep(pc["tick_s"])

    ticker = asyncio.create_task(clock())
    status = await rec.run()
    ticker.cancel()
    r = closeout(db, s)
    logger.info(f"recorder {status}; {_summary(day, r)}; adapter errors {adapter.errors}")
    _report(db, run_id, day, notifier)
    notifier.flush_digest()
    return 0 if r["status"] == "COMPLETED" else 1


def _prior_daily_live(cfg, day: str, index_id: int) -> list[dict]:
    import pandas as pd
    from app.indicators.datasets import pg_spot_stream
    from app.marketdata.pipeline import history_days
    days = [d for d in history_days(cfg.path("derived"), "pg_spot_bars_1m") if d < day]
    rows = prior_daily(pg_spot_stream(cfg.path("derived"), days), day) if days else []
    have = {r["session"] for r in rows}
    feed_root = cfg.path("derived") / "feed_bars_1m"
    for f in sorted(feed_root.glob("date=*/part-0.parquet")) if feed_root.exists() else []:
        d = f.parent.name[5:]
        if d >= day or d in have:
            continue
        b = pd.read_parquet(f)
        b = b[b["security_id"] == index_id]
        if len(b):
            rows.append({"session": d, "high": float(b["high"].max()), "low": float(b["low"].min()),
                         "close": float(b.sort_values("ts")["close"].iloc[-1])})
    return sorted(rows, key=lambda r: r["session"])[-20:]


# ---------------------------------------------------------------------------------------------- status / labels
def _db(name):
    from app.database.db import make_ro_engine
    return make_ro_engine(research_db_path() if name == "research" else settings().path("database"))


def cmd_status(a) -> int:
    db = _db(a.db)
    with Session(db) as s:
        rows = s.scalars(select(PaperSessions).order_by(PaperSessions.session_date)).all()
    if not rows:
        print("no paper sessions yet")
        return 0
    streak = 0
    for r in rows:
        inc = [i for i in json.loads(r.incidents or "[]") if not i["resolved"]]
        ok = r.status == "COMPLETED" and not inc
        streak = streak + 1 if ok else 0
        rec = json.loads(r.reconciliation or "{}")
        print(f"{r.session_date} {r.run_id:<28} {r.status:<26} restarts {r.restarts} reconciled {rec.get('ok')} "
              f"unresolved {len(inc)}")
    need = paper_cfg()["operational_run_sessions"]
    print(f"\nconsecutive clean sessions: {streak} / {need} (§17 phase 9 operational run)")
    return 0


def cmd_labels(a) -> int:
    from app.labels.labeller import adequacy
    db = _db(a.db)
    with Session(db) as s:
        rows = [{c.name: getattr(r, c.name) for c in TradeLabels.__table__.columns}
                for r in s.scalars(select(TradeLabels)).all()]
        by = s.execute(select(TradeLabels.data_tier, TradeLabels.horizon_min, TradeLabels.label, func.count())
                       .group_by(TradeLabels.data_tier, TradeLabels.horizon_min, TradeLabels.label)).all()
    for t in by:
        print(*t)
    for h in (5, 10, 15, 30):
        print(json.dumps(adequacy(rows, h)))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    lv = sub.add_parser("live")
    lv.add_argument("--recover", action="store_true")
    lv.add_argument("--actor")
    lv.add_argument("--duration-min", type=float)
    lv.add_argument("--no-chain", action="store_true")
    lv.add_argument("--spot", type=float)
    rp = sub.add_parser("replay")
    rp.add_argument("--from", dest="d_from")
    rp.add_argument("--to", dest="d_to")
    rp.add_argument("--run-id")
    rp.add_argument("--keep-carry", action="store_true")
    rp.add_argument("--warmup-sessions", type=int, default=10)
    rp.add_argument("--feed-outage-s", type=float, default=120.0,
                    help="history snapshots arrive every ~10-15 s, so the live 30 s watchdog is relaxed to 120 s")
    for name in ("status", "labels"):
        x = sub.add_parser(name)
        x.add_argument("--db", choices=("main", "research"), default="main")
    rs = sub.add_parser("resolve")
    rs.add_argument("--run-id", required=True)
    rs.add_argument("--date", required=True)
    rs.add_argument("--actor", required=True)
    rs.add_argument("--note", required=True)
    rs.add_argument("--db", choices=("main", "research"), default="main")
    a = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if a.cmd == "replay":
        return cmd_replay(a)
    if a.cmd == "live":
        try:
            return asyncio.run(_live(a))
        except KeyboardInterrupt:
            return 130
    if a.cmd == "status":
        return cmd_status(a)
    if a.cmd == "labels":
        return cmd_labels(a)
    db = make_engine(research_db_path() if a.db == "research" else settings().path("database"))
    resolve_incident(db, a.run_id, a.date, a.actor, a.note)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
