"""Persist one day of the strategy book (single writer: the paper engine at closeout, or a replay run).

Written under the book's own run id (default `book_v1`) so the existing pages (Signals, Paper trades, Performance,
daily report) work unchanged:
  PaperTrades     model trades (spec fills and costs)
  TradingSignals  every signal: TRADED / NO_TRADE (reason) / MISSED
  CapitalUsage    the day's row (premium deployed, P&L, drawdown) - also what lists the run
  BookTrades      model + executable (ask / bid) prices and P&L, costs, features
"""
from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy import delete
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.clock import IST
from app.database.models import BookTrades, CapitalUsage, PaperTrades, TradingSignals

FILL_MODEL = "book-1m-bar"


def _iso_ns(ns: int) -> str:
    return datetime.fromtimestamp(ns / 1e9, IST).isoformat(timespec="seconds")


def capital(trades: list[dict]) -> dict:
    """Premium deployed over time (entry value + charges while open), P&L curve and drawdown of the day."""
    ev = []
    for t in trades:
        ev.append((t["entry_ns"], 1, t))
        ev.append((t["exit_ns"], 0, t))
    deployed = peak = 0.0
    peak_ns, open_ids, peak_ids = None, set(), []
    for ns, kind, t in sorted(ev, key=lambda e: (e[0], e[1])):
        v = t["entry_px"] * t["qty"]
        if kind == 1:
            deployed += v
            open_ids.add(t["trade_id"])
        else:
            deployed -= v
            open_ids.discard(t["trade_id"])
        if deployed > peak:
            peak, peak_ns, peak_ids = deployed, ns, sorted(open_ids)
    eq = top = dd = 0.0
    for t in sorted(trades, key=lambda x: x["exit_ns"]):
        eq += t["net_pnl"]
        top = max(top, eq)
        dd = max(dd, top - eq)
    return {"peak": round(peak, 2), "peak_ts": _iso_ns(peak_ns) if peak_ns else None, "peak_ids": peak_ids,
            "net": round(eq, 2), "dd": round(dd, 2),
            "largest_risk": max((t["risk_inr"] for t in trades), default=0.0)}


def persist_day(db: Engine, run_id: str, session_date: str, trades: list[dict], signals: list[dict],
                mode: str = "paper", fee_version: str | None = None) -> None:
    cap = capital(trades)
    with Session(db) as s:
        for model in (PaperTrades, TradingSignals, CapitalUsage, BookTrades):
            s.execute(delete(model).where(model.run_id == run_id, model.session_date == session_date))
        for t in trades:
            dte = (datetime.fromisoformat(t["expiry"]).date() - datetime.fromisoformat(session_date).date()).days
            s.add(PaperTrades(run_id=run_id, mode=mode, session_date=session_date, trade_id=t["trade_id"],
                              cycle_key=f"BOOK|{session_date}|{t['code']}|{t['trade_id']}|{t['side']}",
                              strategy_id=t["strategy_id"], contract=t["contract"], dte=dte, qty=t["qty"],
                              entry_ts=t["entry_ts"], exit_ts=t["exit_ts"], entry_px=t["entry_px"],
                              exit_px=t["exit_px"], exit_reason=t["exit_reason"], gross_pnl=t["gross_pnl"],
                              charges=t["charges"], net_pnl=t["net_pnl"], risk_inr=t["risk_inr"],
                              r_multiple=t["r_multiple"], low_fidelity=0,
                              fee_version=fee_version or t.get("fee_version", "book-cost-1"),
                              fill_model=FILL_MODEL))
            s.add(BookTrades(run_id=run_id, session_date=session_date, trade_id=t["trade_id"], code=t["code"],
                             strategy_id=t["strategy_id"], name=t["name"], side=t["side"], contract=t["contract"],
                             strike=t["strike"], expiry=t["expiry"], qty=t["qty"], exit_profile=t["exit_profile"],
                             signal_ts=t["signal_ts"], entry_ts=t["entry_ts"], exit_ts=t["exit_ts"],
                             entry_px=t["entry_px"], exit_px=t["exit_px"], exit_reason=t["exit_reason"],
                             peak_px=t["peak_px"], gross_pnl=t["gross_pnl"], charges=t["charges"],
                             net_pnl=t["net_pnl"], r_multiple=t["r_multiple"], exec_entry_px=t["exec_entry_px"],
                             exec_exit_px=t["exec_exit_px"], exec_net_pnl=t["exec_net_pnl"],
                             exec_note=t["exec_note"],
                             payload=json.dumps({"cost": t["cost"], "features": t["features"], "trail": t["trail"]},
                                                default=str)))
        for sg in signals:
            traded = sg["outcome"] == "TRADED"
            s.add(TradingSignals(run_id=run_id, session_date=session_date, seq=sg["seq"], ts=sg["ts"],
                                 strategy_id=sg["strategy_id"], setup_id=sg["code"],
                                 cycle_key=f"BOOK|{session_date}|{sg['code']}|{sg.get('trade_id')}|{sg['side']}"
                                 if traded else None,
                                 direction=sg["direction"], outcome="APPROVED" if traded else sg["outcome"],
                                 reasons=json.dumps([] if traded else [sg.get("reason")]),
                                 contract=sg.get("contract"), dte=None, lots=1 if traded else None,
                                 planned_r_inr=None, shadow_lots=None, trade_id=sg.get("trade_id"),
                                 payload=json.dumps({"name": sg["name"], "features": sg.get("features"),
                                                     "entry_px": sg.get("entry_px")}, default=str),
                                 risk_config_version="book: kill switch only"))
        s.add(CapitalUsage(run_id=run_id, mode=mode, session_date=session_date, peak_capital=cap["peak"],
                           peak_capital_ts=cap["peak_ts"], peak_trade_ids=json.dumps(cap["peak_ids"]),
                           net_pnl=cap["net"], max_intraday_drawdown=cap["dd"],
                           largest_trade_risk=cap["largest_risk"], largest_0dte_risk=0.0, trades=len(trades)))
        s.commit()


def strategy_report(run_id: str, session_date: str, trades: list[dict], signals: list[dict], codes: list[str],
                    history=None) -> str:
    """Telegram / console text: today's result of every strategy, plus run-to-date per strategy if given."""
    from app.book.strategies import STRATEGIES
    L = [f"📒 Strategy book — {session_date} (paper, run {run_id})"]
    net = sum(t["net_pnl"] for t in trades)
    ex = [t["exec_net_pnl"] for t in trades if t["exec_net_pnl"] is not None]
    L.append(f"Net ₹{net:,.0f} on {len(trades)} trades" + (f" · executable ₹{sum(ex):,.0f}" if ex else ""))
    for code in codes:
        sd = STRATEGIES[code]
        t = next((x for x in trades if x["code"] == code), None)
        if t:
            L.append(f"• {sd.name} ({code}): {t['contract'].split('|', 2)[2].replace('|', ' ')} "
                     f"{t['entry_ts'][11:16]}→{t['exit_ts'][11:16]} {t['entry_px']:.2f}→{t['exit_px']:.2f} "
                     f"{t['exit_reason']} ₹{t['net_pnl']:,.0f}"
                     + (f" (exec ₹{t['exec_net_pnl']:,.0f})" if t["exec_net_pnl"] is not None else ""))
        else:
            sg = [x for x in signals if x["code"] == code]
            why = f"signal not traded ({sg[-1].get('reason')})" if sg else "no signal"
            L.append(f"• {sd.name} ({code}): {why}")
    if history is not None and len(history):
        L.append("Run to date (model P&L):")
        for code in codes:
            h = history[history["code"] == code]
            if len(h):
                wins = int((h["net_pnl"] > 0).sum())
                L.append(f"  {code} {len(h)} trades · {wins / len(h):.0%} win · ₹{h['net_pnl'].sum():,.0f}")
    return "\n".join(L)
