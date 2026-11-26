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
"""
import argparse
import hashlib
import json
from dataclasses import replace

import _bootstrap  # noqa: F401
import pandas as pd
from sqlalchemy import delete
from sqlalchemy.orm import Session

from app.backtest.audit import AuditLog
from app.backtest.broker import PaperBroker
from app.backtest.fees import FeeSchedule
from app.backtest.fills import FillConfig
from app.backtest.history import TIER, lot_size_evidence, snapshot_quotes
from app.backtest.persist import persist_session
from app.backtest.replay import ReplayEngine
from app.backtest.report import summarize
from app.clock import now_ist
from app.config import settings
from app.database.db import init_db, make_engine
from app.database.models import BarBuilds, RiskEvents, TradingSignals
from app.indicators.datasets import pg_spot_stream
from app.indicators.engine import IndicatorEngine
from app.logging_setup import setup_logging
from app.market.calendar import EventCalendar, TradingCalendar
from app.marketdata.pipeline import history_days
from app.pipeline.decision import DecisionPipeline, PipelineConfig, gap_ratios
from app.risk.engine import RiskEngine
from app.risk.killswitch import KillSwitch
from app.strategies.contracts import SelectionConfig
from app.strategies.orb import OpeningRangeBreakout, ORBConfig
from app.strategies.pullback import PullbackConfig, TrendPullback

FILL = FillConfig(max_quote_age_s=15.0, version="fill-1/snapshot")
LABEL = "history_without_futures/observed_top_of_book_snapshot"


def day_quality(cfg) -> dict[str, tuple[bool, str | None]]:
    out = {}
    with Session(make_engine(cfg.path("database"))) as s:
        for b in s.query(BarBuilds).filter(BarBuilds.dataset == "pg_spot_bars_1m"):
            r = json.loads(b.reconciliation) if b.reconciliation else None
            ok = bool(r and r.get("passed"))
            out[b.trade_date] = (ok, None if ok else "SPOT_BARS_FAILED_RECONCILIATION" if r else "NOT_RECONCILED")
    return out


def run_all(cfg, ds, days, cal, fees, events, run_id, research=None, latch_reset=True):
    risk = RiskEngine(fees=fees, events=events, kill=KillSwitch(None))
    strategies = [OpeningRangeBreakout(ORBConfig(require_futures=False)),
                  TrendPullback(PullbackConfig(require_futures=False))]
    all_index = pg_spot_stream(cfg.path("derived"), days)
    gaps = gap_ratios(all_index)
    quality = day_quality(cfg)
    trades, sessions, signals, heads = [], [], [], []
    for day in days:
        path = cfg.path("historical") / ds / f"date={day}" / "part-0.parquet"
        lot = lot_size_evidence(path)
        if lot not in (25, 50, 65, 75):
            raise ValueError(f"{day}: implausible lot size evidence {lot}")
        pipe = DecisionPipeline(strategies, risk, cal, lot,
                                PipelineConfig(feed_liveness_s=15.0, data_label=LABEL,
                                               selection=replace(SelectionConfig(), max_quote_age_s=30.0)))
        ok, why = quality.get(day, (False, "NOT_RECONCILED"))
        pipe.start_session(day, gaps.get(day), ok, why)
        if latch_reset and risk.state.loss_latch_session and risk.state.loss_latch_session < day:
            # simulated operator: resets the consecutive-loss latch at the first allowed moment (next session)
            risk.reset_consecutive_loss_latch("backtest-operator-sim", day)
        broker = PaperBroker(FILL, fees, AuditLog())
        pipe.attach(broker)
        index = all_index[all_index["session"] == day]
        ReplayEngine(broker, pipe, IndicatorEngine(pipe.bindings)).run({"index": index}, snapshot_quotes(path))
        sig = [s | {"session": day} for s in pipe.signals]
        if research is not None:
            persist_session(research, broker, run_id, day, mode="backtest")
            with Session(research) as s:
                for m in (TradingSignals, RiskEvents):
                    s.execute(delete(m).where(m.run_id == run_id, m.session_date == day))
                tid_by_cycle = {t["cycle"]: t["trade_id"] for t in broker.trades}
                for x in sig:
                    s.add(TradingSignals(run_id=run_id, session_date=day, seq=x["seq"], ts=x["ts"],
                                         strategy_id=x["strategy_id"], setup_id=x["setup_id"], cycle_key=x["cycle_key"],
                                         direction=x["direction"], outcome=x["outcome"], reasons=json.dumps(x["reasons"]),
                                         contract=x["contract"], dte=x["dte"], lots=x["lots"],
                                         planned_r_inr=x["planned_r_inr"], shadow_lots=x["shadow_lots"],
                                         trade_id=tid_by_cycle.get(x["cycle_key"]),
                                         payload=json.dumps(x["payload"], default=str),
                                         risk_config_version=x["risk_config_version"]))
                for e in risk.state.events:   # state (and its events) is per session
                    s.add(RiskEvents(run_id=run_id, session_date=day, kind=e["kind"], payload=json.dumps(e, default=str)))
                s.commit()
        trades += [t | {"session": day} for t in broker.trades]
        signals += sig
        heads.append(broker.audit.head)
        sessions.append({"date": day, "lot_size_evidence": lot, "trades": len(broker.trades),
                         "gap_ratio": gaps.get(day), "data_ok": ok, "peak_capital": round(broker.capital["peak"], 2),
                         "peak_capital_ns": broker.capital["peak_ns"], "peak_trades": broker.capital["peak_trades"],
                         "risk_events": [e["kind"] for e in risk.state.events], "audit_head": broker.audit.head,
                         "audit_valid": AuditLog.verify(broker.audit.records)})
    return trades, sessions, signals, heads, risk


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="d_from")
    ap.add_argument("--to", dest="d_to")
    ap.add_argument("--verify-replay", action="store_true")
    ap.add_argument("--no-persist", action="store_true")
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
    research = None
    if not a.no_persist:
        research = make_engine(cfg.path("database").with_name("NiftyTradingAI_research.db"))
        init_db(research)

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
