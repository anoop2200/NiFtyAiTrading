"""Phase 5 real-data run: ORB + trend pullback through the full decision pipeline (contract ranking +
independent risk engine) on the optionchain_db history, with the Phase 4 replay/paper broker.

HISTORY LABEL: the PG history has no futures bars, so futures participation (ORB) and futures VWAP
(pullback) are switched off (`require_futures=False`) and every result is labelled
"history_without_futures". Production/paper configs keep them required. Quotes are top-of-book snapshots
(~10-13 s cadence), so feed liveness is 15 s and fills use the snapshot fill model. Kill switch: in-memory
(never the live state file). One RiskEngine spans all sessions (the consecutive-loss latch carries over and is
reset by a *simulated operator* at the next session -- the earliest the rules allow; --no-latch-reset keeps it
latched for the rest of the run).

Usage: .venv\\Scripts\\python scripts\\run_strategies.py [--from D] [--to D] [--verify-replay] [--no-persist]
       [--include-holdout --actor NAME]   (the sealed Phase 6 hold-out is excluded by default)
"""
import argparse
import hashlib
import json

import _bootstrap  # noqa: F401
import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.backtest.fees import FeeSchedule
from app.backtest.history import TIER
from app.backtest.persist import persist_session, persist_signals
from app.backtest.report import summarize
from app.clock import now_ist
from app.config import settings
from app.database.db import init_db, make_engine
from app.database.models import Holdouts
from app.indicators.datasets import pg_spot_stream
from app.logging_setup import setup_logging
from app.market.calendar import EventCalendar, TradingCalendar
from app.marketdata.pipeline import history_days
from app.pipeline.decision import gap_ratios
from app.research.simulate import FILL, LABEL, HistoryContext, day_quality, simulate
from app.research.registry import record_exposure
from app.research.spec import build_strategies


def run_all(cfg, ds, days, cal, fees, events, run_id, research=None, latch_reset=True):
    """One continuous run of the default ORB + pullback configs (same simulator as Phase 6 experiments)."""
    idx = pg_spot_stream(cfg.path("derived"), days)
    ctx = HistoryContext(cfg, ds, days, cal, fees, events, idx, gap_ratios(idx), day_quality(cfg))

    def persist(_vid, day, broker, pipe, risk):
        persist_session(research, broker, run_id, day, mode="backtest")
        persist_signals(research, run_id, day, pipe.signals, broker.trades, risk.state.events)

    strategies = build_strategies(("orb", "pullback"), {}, {"require_futures": False})
    run = simulate(ctx, {"default": strategies}, latch_reset=latch_reset,
                   on_session=persist if research is not None else None)["default"]
    return run.trades, run.sessions, run.signals, run.heads, run.risk


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="d_from")
    ap.add_argument("--to", dest="d_to")
    ap.add_argument("--verify-replay", action="store_true")
    ap.add_argument("--no-persist", action="store_true")
    ap.add_argument("--include-holdout", action="store_true",
                    help="also run the sealed hold-out sessions (records an exposure and retires the hold-out)")
    ap.add_argument("--actor", help="operator name, required with --include-holdout")
    ap.add_argument("--no-latch-reset", action="store_true",
                    help="never reset the consecutive-loss latch (default: simulated operator resets next session)")
    a = ap.parse_args()
    cfg = settings()
    setup_logging("strategies")
    ds = cfg.get("historical_import.dataset")
    days = [d for d in history_days(cfg.path("historical"), ds)
            if (not a.d_from or d >= a.d_from) and (not a.d_to or d <= a.d_to)]
    cal, fees, events = TradingCalendar.load(), FeeSchedule.load(), EventCalendar.load()
    run_id = now_ist().strftime("%Y%m%d-%H%M%S") + "_rules_v1"
    out = cfg.path("derived").parent / "backtests" / run_id
    out.mkdir(parents=True, exist_ok=True)
    research_db = make_engine(cfg.path("database").with_name("NiftyTradingAI_research.db"))
    init_db(research_db)
    research = None if a.no_persist else research_db
    # Phase 6 hold-out custody: a sealed hold-out is excluded unless explicitly included, and including it is
    # recorded as an exposure (which retires the hold-out).
    with Session(research_db) as s:
        sealed = s.scalars(select(Holdouts).where(Holdouts.status == "SEALED")).first()
    if sealed is not None:
        held = set(json.loads(sealed.sessions))
        if a.include_holdout:
            if not a.actor:
                raise SystemExit("--include-holdout needs --actor (the exposure is recorded and retires the hold-out)")
            if held & set(days):
                record_exposure(research_db, sealed.id, a.actor, f"run_strategies.py full run {run_id}")
        else:
            excluded = sorted(held & set(days))
            days = [d for d in days if d not in held]
            if excluded:
                print(f"sealed hold-out {sealed.id}: excluded {len(excluded)} sessions "
                      f"{excluded[0]}..{excluded[-1]} (use --include-holdout --actor NAME to override)")

    trades, sessions, signals, heads, risk = run_all(cfg, ds, days, cal, fees, events, run_id, research,
                                                       not a.no_latch_reset)
    combined = hashlib.sha256("".join(heads).encode()).hexdigest()
    checks = {"audit_chains_valid": all(s["audit_valid"] for s in sessions),
              "kill_switch_auto": risk.kill.auto}
    if a.verify_replay:
        _, _, sig2, h2, _ = run_all(cfg, ds, days, cal, fees, events, run_id, None, not a.no_latch_reset)
        c2 = hashlib.sha256("".join(h2).encode()).hexdigest()
        checks["deterministic_replay"] = {"hash_1": combined, "hash_2": c2,
                                          "passed": combined == c2 and len(sig2) == len(signals)}
    df = pd.DataFrame(trades)
    if not df.empty:
        df.to_parquet(out / "trades.parquet", index=False)
    sdf = pd.DataFrame(signals)
    with open(out / "signals.jsonl", "w", encoding="utf-8") as f:
        for s in signals:
            f.write(json.dumps(s, default=str) + "\n")
    reasons = {}
    for s in signals:
        for r in s["reasons"]:
            k = f"{s['strategy_id']}:{s['outcome']}:{r.split(':')[0]}"
            reasons[k] = reasons.get(k, 0) + 1
    funnel = (sdf.groupby(["strategy_id", "outcome"]).size().unstack(fill_value=0).to_dict("index")
              if not sdf.empty else {})
    report = {"run_id": run_id, "label": LABEL, "data_tier": TIER, "risk_config": risk.cfg.version,
              "loss_latch_reset": "none" if a.no_latch_reset else "simulated operator reset at next session",
              "fill_model": FILL.__dict__, "fee_version": fees.version, "days": [days[0], days[-1], len(days)],
              "combined_audit_hash": combined, "checks": checks, "signal_funnel": funnel,
              "reason_counts": dict(sorted(reasons.items(), key=lambda kv: -kv[1])),
              "summary": summarize(df, sessions)}
    (out / "report.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    s = report["summary"]
    print(json.dumps({"funnel": funnel, "all": s.get("all"), "by_dte": s.get("by_dte"),
                      "by_exit_reason": {k: v["trades"] for k, v in s.get("by_exit_reason", {}).items()},
                      "checks": checks}, indent=1, default=str))
    print(json.dumps(dict(list(report["reason_counts"].items())[:20]), indent=1))
    print(f"report: {out / 'report.json'}")
    ok = checks["audit_chains_valid"] and checks.get("deterministic_replay", {}).get("passed", True)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
