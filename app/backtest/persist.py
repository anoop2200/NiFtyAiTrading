"""Persist one session of a PaperBroker (trades, hash-chained events, capital usage) to SQLite (schema v4).
Used by backtest runs now and by paper trading later (single writer)."""
from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy import delete
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.backtest.broker import PaperBroker
from app.backtest.fills import NS
from app.clock import IST
from app.database.models import CapitalUsage, PaperTradeEvents, PaperTrades


def _iso(ns: int | None) -> str | None:
    return None if ns is None else datetime.fromtimestamp(ns / NS, IST).isoformat(timespec="milliseconds")


def session_capital(broker: PaperBroker) -> dict:
    t = sorted(broker.trades, key=lambda x: x["exit_ns"])
    eq, peak, dd = 0.0, 0.0, 0.0
    for x in t:
        eq += x["net_pnl"]
        peak = max(peak, eq)
        dd = max(dd, peak - eq)
    return {"net_pnl": round(eq, 2), "max_intraday_drawdown": round(dd, 2),
            "largest_trade_risk": max((x["risk_inr"] for x in t if x["dte"] != 0), default=0.0),
            "largest_0dte_risk": max((x["risk_inr"] for x in t if x["dte"] == 0), default=0.0), "trades": len(t)}


def persist_session(engine: Engine, broker: PaperBroker, run_id: str, session_date: str, mode: str = "backtest") -> None:
    cap = session_capital(broker)
    with Session(engine) as s:
        for model in (PaperTrades, PaperTradeEvents, CapitalUsage):
            s.execute(delete(model).where(model.run_id == run_id, model.session_date == session_date))
        for t in broker.trades:
            s.add(PaperTrades(run_id=run_id, mode=mode, session_date=session_date, trade_id=t["trade_id"],
                              cycle_key=t["cycle"], strategy_id=t["strategy"], contract=t["contract"], dte=t["dte"],
                              qty=t["qty"], entry_ts=_iso(t["entry_ns"]), exit_ts=_iso(t["exit_ns"]),
                              entry_px=t["entry_px"], exit_px=t["exit_px"], exit_reason=t["exit_reason"],
                              gross_pnl=t["gross_pnl"], charges=t["charges"], net_pnl=t["net_pnl"],
                              risk_inr=t["risk_inr"], r_multiple=t["r_multiple"], low_fidelity=int(t["low_fidelity"]),
                              fee_version=t["fee_version"], fill_model=t["fill_model"]))
        for r in broker.audit.records:
            payload = {k: v for k, v in r.items() if k not in ("seq", "ts_ns", "kind", "prev", "hash")}
            s.add(PaperTradeEvents(run_id=run_id, session_date=session_date, seq=r["seq"], ts=_iso(r["ts_ns"]),
                                   kind=r["kind"], trade_id=r.get("trade_id"),
                                   payload=json.dumps(payload, default=str), prev_hash=r["prev"], hash=r["hash"]))
        c = broker.capital
        s.add(CapitalUsage(run_id=run_id, mode=mode, session_date=session_date, peak_capital=round(c["peak"], 2),
                           peak_capital_ts=_iso(c["peak_ns"]), peak_trade_ids=json.dumps(c["peak_trades"]), **cap))
        s.commit()
