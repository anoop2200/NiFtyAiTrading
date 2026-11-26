"""Candidate meta-labels (v1.3 §6.3) — the Phase 7 data track, run after every session.

For every strategy candidate (APPROVED *and* risk-REJECTED, i.e. traded and counterfactual) and every holding
horizon (5 / 10 / 15 / 30 min) a fresh PaperBroker opens ONE lot of the selected contract at the decision time,
with the candidate's own stop / target in option-premium space and `time_stop = horizon`. The same fill model
(ask-side entry after latency, bid-side exits, stale/spread/protection rules, top-of-book slippage) and the same
fee schedule as paper trading are used, so a label is exactly what the paper broker would have done:

    STOP      -> STOP_FIRST        TARGET -> TARGET_FIRST        TIME_STOP / SESSION_FLAT_1525 -> TIMEOUT
    entry never filled -> NO_ENTRY (reason kept)           no contract / template -> UNLABELLABLE

Timeout P&L is the bid-side exit net of round-trip charges. Features are copied from the signal payload, which
is computed at decision time (no forward information). `session_date` is the grouping key for chronological,
day-grouped validation. Labels from the history snapshot tier are kept apart from prospective live-feed labels
(`data_tier`); only the latter count toward the §11 minimum of 500.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime

from app.backtest.audit import AuditLog
from app.backtest.broker import EntryIntent, PaperBroker
from app.backtest.fees import FeeSchedule
from app.backtest.fills import NS, FillConfig, Quote
from app.backtest.replay import ReplayEngine
from app.clock import IST

LABEL_VERSION = "label-1"
HORIZONS_MIN = (5, 10, 15, 30)
OUTCOME = {"STOP": "STOP_FIRST", "TARGET": "TARGET_FIRST", "TIME_STOP": "TIMEOUT", "SESSION_FLAT_1525": "TIMEOUT"}
PROSPECTIVE_TIERS = ("observed_feed",)


@dataclass(frozen=True)
class LabelConfig:
    horizons_min: tuple[int, ...] = HORIZONS_MIN
    lookback_s: float = 60.0          # quotes before the decision that may form the book at order arrival
    version: str = LABEL_VERSION


def _iso(ns: int | None) -> str | None:
    return None if ns is None else datetime.fromtimestamp(ns / NS, IST).isoformat(timespec="milliseconds")


def label_features(sig: dict) -> dict:
    """Decision-time features only: candidate features, chosen-contract features, time of day, DTE, direction."""
    p = sig.get("payload") or {}
    chosen = ((p.get("selection") or {}).get("chosen") or {})
    t = datetime.fromtimestamp(sig["ts_ns"] / NS, IST)
    f = {f"cand_{k}": v for k, v in (p.get("features") or {}).items() if isinstance(v, (int, float, bool))}
    f |= {f"opt_{k}": v for k, v in chosen.items()
          if k in ("delta", "iv", "gamma", "theta_day", "theta_burn_pct_hr", "spread_pct", "bid", "ask",
                   "quote_age_s", "score", "eligible") and isinstance(v, (int, float))}
    f |= {"minute_of_day": t.hour * 60 + t.minute, "dte": sig.get("dte"), "direction": sig.get("direction"),
          "strategy_id": sig.get("strategy_id")}
    return f


def _quotes_by_contract(quotes: list[Quote]) -> dict[str, list[Quote]]:
    out: dict[str, list[Quote]] = {}
    for q in quotes:
        out.setdefault(q.key, []).append(q)
    for v in out.values():
        v.sort(key=lambda q: q.ts_ns)
    return out


def _window(series: list[Quote], start_ns: int, end_ns: int) -> list[Quote]:
    import bisect
    ts = [q.ts_ns for q in series]
    return series[bisect.bisect_left(ts, start_ns):bisect.bisect_right(ts, end_ns)]


def label_session(signals: list[dict], quotes: list[Quote], lot_size: int, fill: FillConfig, fees: FeeSchedule,
                  traded_cycles: set[str], data_tier: str, cfg: LabelConfig = LabelConfig()) -> list[dict]:
    """Labels for one session. `signals` are pipeline signal dicts (with ts_ns and payload)."""
    by_key = _quotes_by_contract(quotes)
    rows = []
    for sig in signals:
        if sig["outcome"] not in ("APPROVED", "REJECTED") or not sig.get("setup_id"):
            continue                                           # NO_TRADE: no candidate exists
        tpl = (sig.get("payload") or {}).get("template")
        base = {"session_date": sig["session"], "signal_seq": sig["seq"], "signal_ts": sig["ts"],
                "strategy_id": sig["strategy_id"], "cycle_key": sig.get("cycle_key"), "direction": sig.get("direction"),
                "signal_outcome": sig["outcome"], "reject_reasons": json.dumps(sig.get("reasons") or []),
                "traded": int(sig.get("cycle_key") in traded_cycles), "contract": sig.get("contract"),
                "dte": sig.get("dte"), "stop_pct": tpl and tpl["stop_pct"], "target_pct": tpl and tpl["target_pct"],
                "features": json.dumps(label_features(sig), default=str, sort_keys=True), "data_tier": data_tier,
                "label_version": cfg.version}
        for h in cfg.horizons_min:
            row = base | {"horizon_min": h}
            if not sig.get("contract") or not tpl:
                rows.append(row | {"label": "UNLABELLABLE", "label_reason": (sig.get("reasons") or ["NO_CONTRACT"])[0]})
                continue
            rows.append(row | _one(sig, tpl, h, by_key.get(sig["contract"], []), lot_size, fill, fees, cfg))
    return rows


def _one(sig, tpl, horizon_min, series, lot_size, fill, fees, cfg) -> dict:
    dec = sig["ts_ns"]
    end = dec + int((horizon_min * 60 + 2 * fill.order_timeout_s + fill.latency_ms / 1000 + 1) * NS)
    window = _window(series, dec - int(cfg.lookback_s * NS), end)
    broker = PaperBroker(fill, fees, AuditLog())
    tid = broker.submit_entry(EntryIntent(cycle_key=f"label|{sig['cycle_key']}|{horizon_min}",
                                          strategy_id=sig["strategy_id"], contract=sig["contract"], lots=1,
                                          lot_size=lot_size, decision_ns=dec, stop_pct=tpl["stop_pct"],
                                          target_pct=tpl["target_pct"], time_stop_s=horizon_min * 60,
                                          dte=sig.get("dte") or 0, meta={"label": True}))
    if tid is None:
        return {"label": "NO_ENTRY", "label_reason": "AFTER_FLAT_TIME"}
    # the book must not see quotes from after the decision before the replay reaches them
    ReplayEngine(broker).run({}, window)
    if not broker.trades:
        entry = next((o for o in broker.orders.values() if o.purpose == "ENTRY"), None)
        p = broker.positions[tid]
        if p.qty:   # filled but no exit inside the data window
            return {"label": "UNLABELLABLE", "label_reason": "NO_EXIT_QUOTE", "entry_px": p.entry_px}
        return {"label": "NO_ENTRY", "label_reason": (entry.last_reason if entry else None) or "NO_FILL"}
    t = broker.trades[0]
    reason = t["exit_reason"]
    base_reason = reason.split("|")[0]
    if base_reason not in OUTCOME:      # END_OF_DATA: the quote stream ended before any barrier or the horizon
        return {"label": "UNLABELLABLE", "label_reason": "NO_EXIT_QUOTE", "entry_px": t["entry_px"]}
    return {"label": OUTCOME[base_reason], "label_reason": reason,
            "entry_ts": _iso(t["entry_ns"]), "entry_px": t["entry_px"], "exit_ts": _iso(t["exit_ns"]),
            "exit_px": t["exit_px"], "gross_pnl_per_lot": round(t["gross_pnl"] * lot_size / t["qty"], 2),
            "charges_per_lot": round(t["charges"] * lot_size / t["qty"], 2),
            "net_pnl_per_lot": round(t["net_pnl"] * lot_size / t["qty"], 2),
            "r_multiple": t["r_multiple"]}


def adequacy(labels: list[dict], horizon_min: int = 15, min_labels: int = 500, min_per_class: int = 100,
             min_sessions: int = 60) -> dict:
    """§11 training-readiness check on PROSPECTIVE labels only."""
    rows = [r for r in labels if r["horizon_min"] == horizon_min and r["data_tier"] in PROSPECTIVE_TIERS
            and r["label"] in ("TARGET_FIRST", "STOP_FIRST", "TIMEOUT")]
    classes = {c: sum(r["label"] == c for r in rows) for c in ("TARGET_FIRST", "STOP_FIRST", "TIMEOUT")}
    sessions = len({r["session_date"] for r in rows})
    return {"horizon_min": horizon_min, "labels": len(rows), "classes": classes, "sessions": sessions,
            "counterfactual": sum(not r["traded"] for r in rows),
            "ready": len(rows) >= min_labels and sessions >= min_sessions
                     and all(v >= min_per_class for v in classes.values())}
