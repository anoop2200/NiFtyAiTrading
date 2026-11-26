"""End-of-session closeout for the paper engine (v1.3 §17 phase 9 acceptance).

reconcile()      independent checks, every session:
                   * audit chain valid (hash-linked)
                   * every trade's quantities / prices / charges re-derived from the audit fill records
                   * peak capital re-derived from the audit fill sequence equals the broker's figure
                   * no position left open; every APPROVED signal has a matching broker decision
labels           every candidate labelled from the session journal's quotes (Phase 7 track)
persist          trades, audit, capital usage, signals, risk events, labels, PaperSessions row
"""
from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.backtest.audit import AuditLog
from app.backtest.broker import PaperBroker
from app.backtest.persist import persist_labels, persist_session, persist_signals
from app.clock import iso
from app.database.models import PaperSessions
from app.engine.journal import Journal, rec_quote
from app.labels.labeller import LabelConfig, label_session

TOL = 0.011   # rupee rounding tolerance per figure


def reconcile(broker: PaperBroker, signals: list[dict]) -> dict:
    recs = broker.audit.records
    checks = {"audit_chain_valid": AuditLog.verify(recs)}
    orders = {r["order_id"]: r for r in recs if r["kind"] == "order_submitted"}
    fills: dict[int, dict] = {}
    for r in recs:
        if r["kind"] == "fill":
            f = fills.setdefault(r["trade_id"], {"BUY": [0, 0.0], "SELL": [0, 0.0]})
            f[r["side"]][0] += r["qty"]
            f[r["side"]][1] += r["px"] * r["qty"]
    marks = {r["trade_id"]: r for r in recs if r["kind"] == "forced_mark"}
    bad = []
    for t in broker.trades:
        f = fills.get(t["trade_id"], {"BUY": [0, 0.0], "SELL": [0, 0.0]})
        sell_q, sell_v = f["SELL"]
        if t["trade_id"] in marks:
            m = marks[t["trade_id"]]
            sell_q, sell_v = sell_q + m["qty"], sell_v + m["px"] * m["qty"]
        ok = (f["BUY"][0] == t["qty"] == sell_q
              and abs(f["BUY"][1] / max(f["BUY"][0], 1) - t["entry_px"]) < 1e-3
              and abs(sell_v - f["BUY"][1] - t["gross_pnl"]) < TOL * 2
              and abs(t["gross_pnl"] - t["charges"] - t["net_pnl"]) < TOL)
        if not ok:
            bad.append(t["trade_id"])
    checks["trades_match_fills"] = not bad
    checks["mismatched_trades"] = bad
    # charges: entry + exit charges re-computed from fill turnover per order
    fee_bad = []
    by_order: dict[int, float] = {}
    for r in recs:
        if r["kind"] == "fill":
            by_order[r["order_id"]] = by_order.get(r["order_id"], 0.0) + r["px"] * r["qty"]
    per_trade: dict[int, float] = {}
    for oid, turnover in by_order.items():
        o = orders[oid]
        per_trade[o["trade_id"]] = per_trade.get(o["trade_id"], 0.0) + broker.fees.charges(o["side"], turnover)["total"]
    for t in broker.trades:
        extra = 0.0
        if t["trade_id"] in marks:
            m = marks[t["trade_id"]]
            extra = broker.fees.charges("SELL", m["px"] * m["qty"])["total"]
        if abs(per_trade.get(t["trade_id"], 0.0) + extra - t["charges"]) > TOL * 4:
            fee_bad.append(t["trade_id"])
    checks["charges_match_fee_schedule"] = not fee_bad
    checks["charge_mismatches"] = fee_bad
    # capital (§9.7): re-derive deployed capital from the audit records alone, in audit order
    deployed, peak = 0.0, 0.0
    held: dict[int, list] = {}          # trade -> [buy qty, buy value, entry charges]
    for r in recs:
        k = r["kind"]
        if k == "fill" and r["side"] == "BUY":
            h = held.setdefault(r["trade_id"], [0, 0.0, 0.0])
            h[0] += r["qty"]
            h[1] += r["px"] * r["qty"]
            deployed += r["px"] * r["qty"]
        elif k == "order_closed" and orders[r["order_id"]]["purpose"] == "ENTRY" and r["filled_qty"] > 0:
            held[orders[r["order_id"]]["trade_id"]][2] += r["charges"]["total"]
            deployed += r["charges"]["total"]
        elif (k == "fill" and r["side"] == "SELL") or k == "forced_mark":
            h = held[r["trade_id"]]
            deployed -= (h[1] / h[0]) * r["qty"] + h[2] * r["qty"] / h[0]
        peak = max(peak, deployed)
    checks["peak_capital_broker"] = round(broker.capital["peak"], 2)
    checks["peak_capital_from_audit"] = round(peak, 2)
    checks["capital_reconciles"] = abs(peak - broker.capital["peak"]) < TOL
    checks["open_positions_after_close"] = sorted(broker.open_trade_ids())
    approved = {s["cycle_key"] for s in signals if s["outcome"] == "APPROVED"}
    decided = {r["cycle"] for r in recs if r["kind"] in ("decision", "entry_rejected")}
    checks["approved_without_decision"] = sorted(approved - decided)
    checks["ok"] = (checks["audit_chain_valid"] and checks["trades_match_fills"] and checks["charges_match_fee_schedule"]
                    and checks["capital_reconciles"] and not checks["open_positions_after_close"]
                    and not checks["approved_without_decision"])
    return checks


def journal_quotes(path: Path, keys: set[str]) -> list:
    """Quotes of the given contracts from a session journal (streamed; the journal can be large)."""
    out = []
    if not keys:
        return out
    with open(path, encoding="utf-8") as f:
        for line in f:
            if not line.startswith('{"k":"q"'):
                continue
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if r["key"] in keys:
                out.append(rec_quote(r))
    return out


def label_from_journal(session, cfg: LabelConfig = LabelConfig()) -> list[dict]:
    sigs = [s | {"session": session.spec.session} for s in session.pipe.signals]
    keys = {s["contract"] for s in sigs if s.get("contract") and s["outcome"] in ("APPROVED", "REJECTED")}
    quotes = journal_quotes(session.journal.path, keys)
    tier = "observed_feed" if session.spec.data_label == "observed_feed" else session.spec.data_label
    return label_session(sigs, quotes, session.spec.lot_size, session.fill, session.fees,
                         {t["cycle"] for t in session.broker.trades}, tier, cfg)


def open_session_row(db: Engine, session, code_fingerprint: str | None = None) -> None:
    """RUNNING row at session start. A fresh session replaces any earlier row of the same run/day (a re-run of a
    replay); a recovered session keeps it and counts the restart."""
    recovered = getattr(session, "recovery", None) is not None
    with Session(db) as s:
        row = s.query(PaperSessions).filter_by(run_id=session.spec.run_id, session_date=session.spec.session).first()
        if row is not None and not recovered:
            s.delete(row)
            s.flush()
            row = None
        if row is None:
            row = PaperSessions(run_id=session.spec.run_id, session_date=session.spec.session, source=session.spec.source,
                                started_at=iso(), status="RUNNING", restarts=0, journal_path=str(session.journal.path),
                                journal_events=0, code_fingerprint=code_fingerprint,
                                risk_config_version=session.risk.cfg["version"] if session.risk.cfg else None)
            s.add(row)
        else:
            row.status, row.restarts = "RUNNING", (row.restarts or 0) + 1
        s.commit()


def closeout(db: Engine, session) -> dict:
    """Finish the day: close, label, reconcile, persist (single writer = this engine process)."""
    session.finish()
    sp = session.spec
    rec = reconcile(session.broker, session.pipe.signals)
    labels = label_from_journal(session)
    persist_session(db, session.broker, sp.run_id, sp.session, mode=sp.mode)
    persist_signals(db, sp.run_id, sp.session, session.pipe.signals, session.broker.trades, session.risk.state.events)
    persist_labels(db, sp.run_id, sp.session, labels)
    session.write_carry()
    unresolved = [i for i in session.incidents if not i["resolved"]]
    status = "COMPLETED" if rec["ok"] and not unresolved else "COMPLETED_WITH_INCIDENTS"
    with Session(db) as s:
        row = s.query(PaperSessions).filter_by(run_id=sp.run_id, session_date=sp.session).first()
        if row is None:
            row = PaperSessions(run_id=sp.run_id, session_date=sp.session, source=sp.source, started_at=iso(),
                                restarts=0, journal_path=str(session.journal.path))
            s.add(row)
        row.ended_at, row.status = iso(), status
        row.journal_events = session.applied
        row.journal_sha256 = Journal.sha256(session.journal.path)      # of the uncompressed journal
        row.journal_path = str(Journal.compress(session.journal.path))
        row.audit_head = session.broker.audit.head
        row.reconciliation = json.dumps(rec, default=str)
        row.incidents = json.dumps(session.incidents, default=str)
        row.risk_config_version = session.risk.cfg["version"] if session.risk.cfg else None
        if getattr(session, "recovery", None):
            row.restarts = session.recovery["restart"]
        s.commit()
    return {"status": status, "reconciliation": rec, "labels": len(labels), "trades": len(session.broker.trades),
            "signals": len(session.pipe.signals), "incidents": session.incidents}


def resolve_incident(db: Engine, run_id: str, session_date: str, actor: str, note: str) -> None:
    """Operator resolution of a critical incident (recorded on the session row)."""
    with Session(db) as s:
        row = s.query(PaperSessions).filter_by(run_id=run_id, session_date=session_date).one()
        inc = json.loads(row.incidents or "[]")
        for i in inc:
            if not i["resolved"]:
                i |= {"resolved": True, "resolved_by": actor, "note": note, "resolved_at": iso()}
        row.incidents = json.dumps(inc)
        if row.status == "COMPLETED_WITH_INCIDENTS" and json.loads(row.reconciliation or "{}").get("ok"):
            row.status = "COMPLETED"
        s.commit()


