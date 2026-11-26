"""Phase 4 real-data replay: event-driven backtest over the optionchain_db history with the paper broker.

Strategy: the ORB *plumbing check* (app/backtest/demo.py) — exercises replay, fills, fees, audit and
capital tracking on real quotes; its P&L is NOT evidence for any strategy. One broker per session (positions
are flat by 15:25). Outputs: data/backtests/<run_id>/ (trades.parquet, audit.jsonl, report.json).

Usage: .venv\\Scripts\\python scripts\\run_backtest.py [--from D] [--to D] [--verify-replay]
"""
import argparse
import hashlib
import json

import _bootstrap  # noqa: F401
import pandas as pd

from app.backtest.audit import AuditLog
from app.backtest.broker import PaperBroker
from app.backtest.demo import ORBDemo
from app.backtest.fees import FeeSchedule
from app.backtest.fills import FillConfig
from app.backtest.persist import persist_session
from app.backtest.history import TIER, lot_size_evidence, snapshot_quotes
from app.backtest.replay import ReplayEngine
from app.backtest.report import summarize
from app.clock import now_ist
from app.config import settings
from app.database.db import init_db, make_engine
from app.indicators.datasets import pg_spot_stream
from app.indicators.engine import IndicatorEngine, binding
from app.logging_setup import setup_logging
from app.market.calendar import TradingCalendar
from app.marketdata.pipeline import history_days

# snapshot cadence is ~10-13 s, so a quote up to 15 s old is the freshest the history can offer
FILL = FillConfig(max_quote_age_s=15.0, version="fill-1/snapshot")


def run_day(cfg, ds, day, cal, fees):
    path = cfg.path("historical") / ds / f"date={day}" / "part-0.parquet"
    lot = lot_size_evidence(path)
    if lot not in (25, 50, 65, 75):
        raise ValueError(f"{day}: implausible lot size evidence {lot}")
    quotes = snapshot_quotes(path)
    index = pg_spot_stream(cfg.path("derived"), [day])
    b = binding("opening_range", "index", 1, minutes=15)
    strat = ORBDemo(cal, lot, b.key)
    broker = PaperBroker(FILL, fees, AuditLog())
    ReplayEngine(broker, strat, IndicatorEngine([b])).run({"index": index}, quotes)
    session = {"date": day, "lot_size_evidence": lot, "quotes": len(quotes), "trades": len(broker.trades),
               "peak_capital": round(broker.capital["peak"], 2), "peak_capital_ns": broker.capital["peak_ns"],
               "peak_trades": broker.capital["peak_trades"], "skips": strat.skips,
               "audit_head": broker.audit.head}
    return broker, session


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="d_from")
    ap.add_argument("--to", dest="d_to")
    ap.add_argument("--verify-replay", action="store_true")
    a = ap.parse_args()
    cfg = settings()
    setup_logging("backtest")
    ds = cfg.get("historical_import.dataset")
    days = [d for d in history_days(cfg.path("historical"), ds)
            if (not a.d_from or d >= a.d_from) and (not a.d_to or d <= a.d_to)]
    cal, fees = TradingCalendar.load(), FeeSchedule.load()
    run_id = now_ist().strftime("%Y%m%d-%H%M%S") + "_orb_demo"
    out = cfg.path("derived").parent / "backtests" / run_id
    out.mkdir(parents=True, exist_ok=True)

    research = make_engine(cfg.path("database").with_name("NiftyTradingAI_research.db"))
    init_db(research)

    def run_all(persist=False):
        trades, sessions, audits = [], [], []
        for day in days:
            br, s = run_day(cfg, ds, day, cal, fees)
            if persist:
                persist_session(research, br, run_id, day, mode="backtest")
            trades += [t | {"session": day} for t in br.trades]
            sessions.append(s)
            audits.append(br.audit)
        return trades, sessions, audits

    trades, sessions, audits = run_all(persist=True)
    combined = hashlib.sha256("".join(s["audit_head"] for s in sessions).encode()).hexdigest()
    checks = {}
    if a.verify_replay:
        _, s2, _ = run_all()
        c2 = hashlib.sha256("".join(s["audit_head"] for s in s2).encode()).hexdigest()
        checks["deterministic_replay"] = {"hash_1": combined, "hash_2": c2, "passed": combined == c2}
    checks["audit_chains_valid"] = all(AuditLog.verify(x.records) for x in audits)
    df = pd.DataFrame(trades)
    if not df.empty:
        df.to_parquet(out / "trades.parquet", index=False)
    with open(out / "audit.jsonl", "w", encoding="utf-8") as f:
        for au in audits:
            for r in au.records:
                f.write(json.dumps(r, default=str) + "\n")
    report = {"run_id": run_id, "strategy": "orb_demo_plumbing (NOT a strategy result)", "data_tier": TIER,
              "fill_model": FILL.__dict__, "fee_version": fees.version, "days": [days[0], days[-1], len(days)],
              "combined_audit_hash": combined, "checks": checks, "summary": summarize(df, sessions)}
    (out / "report.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    s = report["summary"]
    print(json.dumps({k: s.get(k) for k in ("all", "by_dte", "fidelity", "max_drawdown_inr", "capital")},
                     indent=2, default=str))
    print(json.dumps({"by_exit_reason": {k: v["trades"] for k, v in s.get("by_exit_reason", {}).items()},
                      "checks": checks}, default=str))
    print(f"report: {out / 'report.json'}")
    ok = checks["audit_chains_valid"] and checks.get("deterministic_replay", {}).get("passed", True)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
