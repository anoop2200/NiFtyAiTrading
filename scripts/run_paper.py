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
import pandas as pd
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
    smoke = bool(a.smoke)
    if smoke and not a.duration_min:
        logger.error("--smoke needs --duration-min")
        return 2
    # --smoke: an isolated rehearsal (own run id, runtime dir, research DB, Telegram outbox) that can never touch
    # the real run's journal, kill switch, risk carry or main database.
    db = make_engine(research_db_path() if smoke else None)
    init_db(db)
    today = now_ist().date()
    day = today.isoformat()
    cal, fees, events = TradingCalendar.load(), FeeSchedule.load(), EventCalendar.load()
    if not cal.is_trading_day(today):
        logger.error(f"{day} is not a trading session")
        return 2
    run_id = f"smoke_{pc['run_id']}" if smoke else pc["run_id"]
    jp = journal_path(run_id, day)
    kill_file, commands, carry_file = KILL_FILE, COMMANDS, CARRY
    if smoke:
        from app.notify.telegram import Notifier, make_transport
        rt = PROJECT_ROOT / "data" / "paper" / run_id / "_runtime"
        kill_file, commands, carry_file = rt / "kill.json", rt / "commands", rt / "carry.json"
        carry_file.unlink(missing_ok=True)
        jp.unlink(missing_ok=True)
        Path(str(jp) + ".gz").unlink(missing_ok=True)
        notifier = Notifier(make_transport(outbox=rt / "telegram_outbox.jsonl"))
    else:
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
    book_cfg = pc.get("book") or {}
    book_on = bool(book_cfg.get("enabled"))
    n_hist = max(20, int(book_cfg.get("warmup_sessions", 35)) + 1) if book_on else 20
    hist = pd.DataFrame() if unfinished else await _index_history_live(cfg, cal, today, logger, n_hist)
    if unfinished:
        s = PaperSession.recover(jp, fees, cal, events, kill_file=kill_file, commands=commands, carry_file=carry_file,
                                 notifier=notifier)
        s._incident("OPERATOR_RECOVERY", {"actor": a.actor, "recovery": s.recovery}, notify=True)
        logger.warning(f"recovered {s.recovery}")
    else:
        strategies = late_start_strategies(tuple(pc["strategies"]), now_ist())
        if strategies != tuple(pc["strategies"]):
            logger.warning(f"late start ({now_ist():%H:%M}): the 09:15-09:30 opening range cannot be observed, "
                           f"so ORB is off today; running {strategies}")
        spec = SessionSpec(run_id=run_id, session=day, lot_size=int(lot), source="live_feed",
                           data_label="observed_feed", mode="paper", strategies=strategies,
                           overrides={"require_futures": bool(pc["require_futures"])},
                           prior_daily=prior_daily(hist, day) if len(hist) else None,
                           feed_liveness_s=pc["feed_liveness_s"], selection_quote_age_s=pc["selection_quote_age_s"],
                           feed_outage_s=pc["feed_outage_s"], checkpoint_every=pc["checkpoint_every"],
                           book=_book_spec(book_cfg, smoke) if book_on else None)
        book_warmup = (await _book_warmup(cfg, hist, day, int(book_cfg.get("warmup_sessions", 35)), logger)
                       if book_on else None)
        s = PaperSession(spec, jp, fees, cal, events, kill_file=kill_file, commands=commands, carry_file=carry_file,
                         notifier=notifier, warmup=warmup_bars(hist, day, pc.get("warmup_sessions", 10)) if len(hist)
                         else None, book_warmup=book_warmup)
        logger.info(f"indicator warm-up: {s.warmup_bars} index bars; prior days: {len(spec.prior_daily or [])}")
    open_session_row(db, s, code_fingerprint())
    adapter = LiveAdapter(s, today, p.universe.index.security_id, future, p.universe.options,
                          vix_id=p.universe.vix.security_id if p.universe.vix else None)
    rec = Recorder(cfg, db, p, end_at, chain_enabled=not a.no_chain)
    rec.taps.append(adapter.on_rows)

    async def clock():
        last_snap = 0.0
        while now_ist() < end_at:
            try:
                adapter.tick(time.time_ns())
            except Exception as e:  # noqa: BLE001
                logger.error(f"engine tick failed: {e!r}")
                s.kill.trip("RISK_EVALUATION_ERROR", repr(e)[:200])
            if s.book is not None and time.monotonic() - last_snap >= 5:
                last_snap = time.monotonic()
                try:
                    write_book_snapshot(s.book, day)
                except Exception as e:  # noqa: BLE001 - display only
                    logger.warning(f"book snapshot failed: {e!r}")
            await asyncio.sleep(pc["tick_s"])

    ticker = asyncio.create_task(clock())
    status = await rec.run()
    ticker.cancel()
    r = closeout(db, s)
    logger.info(f"recorder {status}; {_summary(day, r)}; adapter errors {adapter.errors}")
    try:                                               # detailed reports: console / log (the dashboard has them)
        _report(db, run_id, day, None)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"rules report: {e!r}")
    if s.book is not None:
        write_book_snapshot(s.book, day)
        try:
            _book_report(db, s.book, day, None, r.get("book"))
        except Exception as e:  # noqa: BLE001
            logger.warning(f"book report: {e!r}")
    _combined_report(db, s, day, notifier, r)          # the one daily P&L message to Telegram
    notifier.flush_digest()
    return 0 if r["status"] == "COMPLETED" else 1


def _combined_report(db, s, day, notifier, r) -> None:
    """NiftyTradingAI daily P&L report: every strategy (rules + book) in one Telegram message after closeout."""
    from app.notify.telegram import INFO, Alert
    from app.reports.combined import build_combined, format_combined
    notes = []
    if r.get("status") != "COMPLETED":
        notes.append(f"closeout status {r.get('status')}")
    if (r.get("book") or {}).get("error"):
        notes.append(f"strategy book closeout error: {r['book']['error']}")
    try:
        why = s.kill.blocking_reason()
        if why:
            notes.append(f"kill switch ON at close: {why}")
    except Exception:  # noqa: BLE001 - a note only
        pass
    try:
        d = build_combined(db, day, s.spec.run_id, list(s.spec.strategies),
                           s.book.run_id if s.book is not None else None,
                           list(s.book.codes) if s.book is not None else [], notes)
        parts = format_combined(d)
    except Exception as e:  # noqa: BLE001 - never lose the closeout over the report; say so instead
        parts = [f"📊 NiftyTradingAI — Daily P&L Report {day}\nreport failed: {e!r}"[:500]]
    for i, text in enumerate(parts, 1):
        print(text)
        if notifier:
            notifier.send(Alert("daily_pnl", INFO, text, key=f"daily|combined|{day}|{i}"))


def _book_spec(book_cfg: dict, smoke: bool) -> dict:
    rid = book_cfg.get("run_id", "book_v1")
    return {"run_id": f"smoke_{rid}" if smoke else rid, "codes": list(book_cfg.get("codes") or [])}


def book_snapshot_path(run_id: str, day: str) -> Path:
    return PROJECT_ROOT / "data" / "paper" / run_id / f"{day}.live.json"


def write_book_snapshot(book, day: str) -> None:
    """Display-only state of the strategy book for the dashboard (authoritative results: closeout -> database)."""
    from app.clock import now_ist
    p = book_snapshot_path(book.run_id, day)
    p.parent.mkdir(parents=True, exist_ok=True)
    snap = {"at": now_ist().isoformat(timespec="seconds"), "session": day, "run_id": book.run_id,
            "summary": book.summary(), "status": book.status(), "signals": book.signals[-200:],
            "trades": [{k: v for k, v in t.items() if k not in ("features", "cost")} for t in book.trades],
            "finished": book.finished}
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(snap, default=str), encoding="utf-8")
    tmp.replace(p)


async def _book_warmup(cfg, hist, day: str, sessions: int, logger) -> dict:
    """35 sessions of index bars + (late start) today's completed bars so far, so VWAP / opening range / the
    first-candle rule see the whole day. Stored in the journal's start record -> recovery warms up identically."""
    from app.book.engine import warmup_columns
    from app.clock import now_ist
    w = (warmup_columns(hist, day, sessions) if len(hist)
         else {"ts": [], "open": [], "high": [], "low": [], "close": [], "sessions": []})
    if len(w["sessions"]) < sessions:
        logger.warning(f"strategy book warm-up: only {len(w['sessions'])} of {sessions} sessions available")
    now = now_ist()
    if (now.hour, now.minute) <= (9, 15):
        return w
    from app.data.dhan_rest import DhanRest
    from app.historical import vix as store
    from app.secrets import get_secret
    rest = DhanRest(cfg.get("dhan.rest_base"), get_secret("DHAN_CLIENT_ID"), get_secret("DHAN_ACCESS_TOKEN"),
                    cfg.get("dhan.option_chain_min_interval_s"), cfg.get("dhan.market_quote_min_interval_s"))
    try:
        r = await rest.intraday_candles(13, "IDX_I", "INDEX", f"{day} 09:00:00", now.strftime("%Y-%m-%d %H:%M:%S"))
        bars = store.candles_to_bars(r.body, day) if r.status == 200 else None
        if bars is not None and len(bars):
            done = bars[bars["ts"] + 60 <= int(now.timestamp()) - 2]          # completed minutes only
            w["ts"] += [int(x) for x in done["ts"]]
            for c in ("open", "high", "low", "close"):
                w[c] += [round(float(x), 2) for x in done[c]]
            w["today_bars"] = int(len(done))
            logger.info(f"strategy book: late start, {len(done)} of today's index bars loaded")
        else:
            logger.warning(f"strategy book: today's bars unavailable (HTTP {r.status}); "
                           "VWAP / opening range start from the first live bar")
    except Exception as e:  # noqa: BLE001
        logger.warning(f"strategy book: today's bars fetch failed: {e!r}")
    finally:
        await rest.close()
    return w


def _book_report(db, book, day, notifier, closeout_info) -> None:
    from app.book.persist import strategy_report
    from app.notify.telegram import INFO, Alert
    hist = None
    try:
        from app.dashboard.data import q
        hist = q(db, "SELECT code, net_pnl FROM BookTrades WHERE run_id = :r", r=book.run_id)
    except Exception:  # noqa: BLE001
        pass
    text = strategy_report(book.run_id, day, book.trades, book.signals, book.codes, hist)
    if closeout_info and closeout_info.get("error"):
        text += f"\nbook closeout error: {closeout_info['error']}"
    print(text)
    if notifier:
        notifier.send(Alert("daily_pnl", INFO, text, key=f"daily|{book.run_id}|{day}"))


def late_start_strategies(strategies: tuple, now) -> tuple:
    """A fresh (non-recovery) start after 09:17 IST misses opening-range bars: ORB would see a partial or empty
    range (the approved opening_range indicator assumes it saw the open), so it sits the day out."""
    if (now.hour, now.minute) > (9, 17):
        return tuple(x for x in strategies if x != "orb")
    return strategies


async def _index_history_live(cfg, cal, today, logger, sessions: int = 20):
    """The sessions before today from the NIFTY 1-minute store, fetching any missing ones from Dhan first."""
    import pandas as pd
    from app.data.dhan_rest import DhanRest
    from app.engine import live_history as lh
    from app.secrets import get_secret
    root = cfg.path("historical")
    want = lh.needed_sessions(cal, today, sessions)
    gaps = lh.missing(root, want)
    if gaps:
        rest = DhanRest(cfg.get("dhan.rest_base"), get_secret("DHAN_CLIENT_ID"), get_secret("DHAN_ACCESS_TOKEN"),
                        cfg.get("dhan.option_chain_min_interval_s"), cfg.get("dhan.market_quote_min_interval_s"))
        try:
            logger.info(f"index history: fetching {gaps}: {await lh.fetch_missing(rest, root, gaps)}")
        except Exception as e:  # noqa: BLE001 - a warm-up gap must not stop the session; it is reported
            logger.error(f"index history fetch failed: {e!r}")
        finally:
            await rest.close()
    hist = lh.index_frame(root, want)
    info = lh.summary(want, hist)
    (logger.warning if info["missing"] or lh.stale(cal, today, hist) else logger.info)(f"index history {info}")
    return hist if len(hist) else pd.DataFrame()


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
    lv.add_argument("--smoke", action="store_true",
                    help="isolated rehearsal (run id smoke_*, research DB, outbox alerts); needs --duration-min")
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
