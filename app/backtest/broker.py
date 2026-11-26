"""Paper broker (v1.3 §8.3, §9.6, §9.7): latency, ladder fills, partial fills, order timeouts, premium-based
stop/target on the executable bid, time stops, the 15:25 flat rule, versioned charges, capital usage and a
hash-chained audit of every decision, order attempt, fill and cost.

Long options only (paper MVP). The broker never holds a position to settlement.
"""
from __future__ import annotations

import heapq
import itertools
from dataclasses import dataclass, field
from datetime import datetime, time

from app.backtest.audit import AuditLog
from app.backtest.fees import FeeSchedule
from app.backtest.fills import NS, Attempt, FillConfig, Quote, attempt_fill
from app.clock import IST

FLAT_BY = time(15, 25)


@dataclass(frozen=True)
class EntryIntent:
    cycle_key: str
    strategy_id: str
    contract: str
    lots: int
    lot_size: int
    decision_ns: int
    stop_pct: float                 # stop = entry * (1 - stop_pct/100), evaluated on bid
    target_pct: float               # target = entry * (1 + target_pct/100), evaluated on bid
    time_stop_s: int = 1800
    dte: int | None = None
    meta: dict = field(default_factory=dict)


@dataclass
class Order:
    id: int
    purpose: str                    # ENTRY / EXIT
    side: str                       # BUY / SELL
    contract: str
    qty: int
    decision_ns: int
    arrival_ns: int
    trade_id: int
    exit_reason: str | None = None
    status: str = "PENDING"        # PENDING WORKING FILLED PARTIAL_CANCELLED NO_FILL
    fills: list[tuple[int, float, int, bool]] = field(default_factory=list)  # (ts, px, qty, low_fidelity)
    attempts: int = 0
    last_reason: str | None = None
    deadline_ns: int = 0
    charges: dict | None = None

    @property
    def filled_qty(self) -> int:
        return sum(f[2] for f in self.fills)

    @property
    def avg_px(self) -> float | None:
        q = self.filled_qty
        return sum(f[1] * f[2] for f in self.fills) / q if q else None

    @property
    def turnover(self) -> float:
        return sum(f[1] * f[2] for f in self.fills)


@dataclass
class Position:
    trade_id: int
    intent: EntryIntent
    qty: int = 0
    entry_px: float | None = None
    entry_charges: float = 0.0
    stop: float | None = None
    target: float | None = None
    opened_ns: int | None = None
    status: str = "PENDING_ENTRY"   # PENDING_ENTRY OPEN EXITING CLOSED CANCELLED
    exit_reason: str | None = None
    exit_fills: list[tuple[int, float, int, bool]] = field(default_factory=list)
    exit_charges: float = 0.0
    low_fidelity: bool = False
    best_bid: float | None = None
    worst_bid: float | None = None


class PaperBroker:
    def __init__(self, cfg: FillConfig | None = None, fees: FeeSchedule | None = None, audit: AuditLog | None = None):
        self.cfg = cfg or FillConfig()
        self.fees = fees or FeeSchedule.load()
        self.audit = audit or AuditLog()
        self.book: dict[str, Quote] = {}
        self.orders: dict[int, Order] = {}
        self.positions: dict[int, Position] = {}
        self.trades: list[dict] = []
        self.consumed: dict[str, dict[int, int]] = {}
        self.timers: list[tuple[int, int, str, int]] = []   # (ts, seq, kind, ref)
        self._ids = itertools.count(1)
        self._seq = itertools.count()
        self.capital = {"deployed": 0.0, "peak": 0.0, "peak_ns": None, "peak_trades": []}
        self.cycles_open: dict[str, int] = {}
        self.listeners: list = []   # objects with on_broker_event(kind, payload): risk engine, persistence

    def _notify(self, kind: str, **payload) -> None:
        for listener in self.listeners:
            listener.on_broker_event(kind, payload)

    # ------------------------------------------------------------------ timers (driven by the replay engine)
    def _timer(self, ts: int, kind: str, ref: int) -> None:
        heapq.heappush(self.timers, (ts, next(self._seq), kind, ref))

    def next_timer_ns(self) -> int | None:
        return self.timers[0][0] if self.timers else None

    def run_timers(self, until_ns: int) -> None:
        """Fire every timer due at or before `until_ns` in time order."""
        while self.timers and self.timers[0][0] <= until_ns:
            ts, _, kind, ref = heapq.heappop(self.timers)
            if kind == "arrival":
                self._work(self.orders[ref], ts)
            elif kind == "order_timeout":
                self._order_timeout(self.orders[ref], ts)
            elif kind == "time_stop":
                p = self.positions[ref]
                if p.status == "OPEN":
                    self._exit(p, ts, "TIME_STOP")
            elif kind == "flat":
                p = self.positions[ref]
                if p.status == "OPEN":
                    self._exit(p, ts, "SESSION_FLAT_1525")

    # ------------------------------------------------------------------ entry / exit requests
    def submit_entry(self, it: EntryIntent) -> int | None:
        if it.decision_ns >= flat_ns(it.decision_ns):
            self.audit.add(it.decision_ns, "entry_rejected", cycle=it.cycle_key, reason="AFTER_FLAT_TIME")
            self._notify("entry_not_filled", cycle_key=it.cycle_key, trade_id=None, reason="AFTER_FLAT_TIME")
            return None
        if it.cycle_key in self.cycles_open:
            self.audit.add(it.decision_ns, "entry_rejected", cycle=it.cycle_key, reason="CYCLE_ALREADY_ACTIVE")
            self._notify("entry_not_filled", cycle_key=it.cycle_key, trade_id=None, reason="CYCLE_ALREADY_ACTIVE")
            return None
        tid = next(self._ids)
        p = self.positions[tid] = Position(tid, it)
        self.cycles_open[it.cycle_key] = tid
        self.audit.add(it.decision_ns, "decision", trade_id=tid, cycle=it.cycle_key, strategy=it.strategy_id,
                       contract=it.contract, lots=it.lots, lot_size=it.lot_size, stop_pct=it.stop_pct,
                       target_pct=it.target_pct, time_stop_s=it.time_stop_s, dte=it.dte, meta=it.meta)
        self._new_order(p, "ENTRY", "BUY", it.lots * it.lot_size, it.decision_ns)
        return tid

    def open_trade_ids(self) -> set[int]:
        return {t for t, p in self.positions.items() if p.status in ("OPEN", "EXITING")}

    def unrealized_inr(self) -> dict[int, float]:
        """Mark-to-bid P&L of open positions net of entry charges (no exit charges)."""
        out = {}
        for t, p in self.positions.items():
            if p.status in ("OPEN", "EXITING") and p.qty:
                q = self.book.get(p.intent.contract)
                if q is not None and q.bid and q.bid > 0:
                    left = p.qty - sum(f[2] for f in p.exit_fills)
                    out[t] = (q.bid - p.entry_px) * left - p.entry_charges
        return out

    def request_exit(self, trade_id: int, now_ns: int, reason: str) -> None:
        p = self.positions[trade_id]
        if p.status == "OPEN":
            self._exit(p, now_ns, reason)

    def _new_order(self, p: Position, purpose: str, side: str, qty: int, decision_ns: int,
                   exit_reason: str | None = None) -> Order:
        oid = next(self._ids)
        arrival = decision_ns + self.cfg.latency_ms * 1_000_000
        o = self.orders[oid] = Order(oid, purpose, side, p.intent.contract, qty, decision_ns, arrival, p.trade_id,
                                     exit_reason, deadline_ns=arrival + int(self.cfg.order_timeout_s * NS))
        self.audit.add(decision_ns, "order_submitted", order_id=oid, trade_id=p.trade_id, purpose=purpose, side=side,
                       qty=qty, arrival_ns=arrival, exit_reason=exit_reason, fill_model=self.cfg.version)
        self._timer(arrival, "arrival", oid)
        self._timer(o.deadline_ns, "order_timeout", oid)
        return o

    def _exit(self, p: Position, now_ns: int, reason: str) -> None:
        # an entry still working (partially filled) is cancelled first, so the exit covers the final quantity
        for o in self.orders.values():
            if o.trade_id == p.trade_id and o.purpose == "ENTRY" and o.status in ("PENDING", "WORKING"):
                o.last_reason = "CANCELLED_FOR_EXIT"
                self._close_order(o, now_ns, "PARTIAL_CANCELLED")
        p.status, p.exit_reason = "EXITING", reason
        self._new_order(p, "EXIT", "SELL", p.qty - sum(f[2] for f in p.exit_fills), now_ns, reason)

    # ------------------------------------------------------------------ market data
    def on_quote(self, q: Quote) -> None:
        self.book[q.key] = q
        for o in [o for o in self.orders.values() if o.contract == q.key and o.status == "WORKING"]:
            self._work(o, q.ts_ns)
        for p in [p for p in self.positions.values() if p.intent.contract == q.key and p.status == "OPEN"]:
            self._check_exit(p, q)

    def _check_exit(self, p: Position, q: Quote) -> None:
        b = q.bid
        if not b or b <= 0 or not q.bids or q.bids[0][1] <= 0:
            return  # quote withdrawn: no executable bid, keep monitoring
        p.best_bid = b if p.best_bid is None else max(p.best_bid, b)
        p.worst_bid = b if p.worst_bid is None else min(p.worst_bid, b)
        if b <= p.stop:          # stop checked first (conservative)
            self._exit(p, q.ts_ns, "STOP")
        elif b >= p.target:
            self._exit(p, q.ts_ns, "TARGET")

    # ------------------------------------------------------------------ order working
    def _work(self, o: Order, now_ns: int) -> None:
        if o.status in ("FILLED", "PARTIAL_CANCELLED", "NO_FILL"):
            return
        if now_ns < o.arrival_ns:
            return
        o.status = "WORKING"
        left = o.qty - o.filled_qty
        q = self.book.get(o.contract)
        if q is not None and q.ts_ns > now_ns:
            q = None  # never use a quote from the future
        a: Attempt = attempt_fill(o.side, left, q, now_ns, self.cfg, self.consumed)
        o.attempts += 1
        o.last_reason = a.reason
        self.audit.add(now_ns, "attempt", order_id=o.id, quote_ns=q.ts_ns if q else None, touch=a.touch,
                       limit=a.limit, filled=a.filled, reason=a.reason, low_fidelity=a.low_fidelity,
                       quote_tier=q.tier if q else None)
        for px, qty in a.filled:
            o.fills.append((now_ns, px, qty, a.low_fidelity))
            self.audit.add(now_ns, "fill", order_id=o.id, trade_id=o.trade_id, side=o.side, px=px, qty=qty,
                           low_fidelity=a.low_fidelity)
            self._apply_fill(o, now_ns, px, qty, a.low_fidelity)
        if o.filled_qty >= o.qty:
            self._close_order(o, now_ns, "FILLED")

    def _order_timeout(self, o: Order, now_ns: int) -> None:
        if o.status not in ("PENDING", "WORKING"):
            return
        p = self.positions[o.trade_id]
        if o.purpose == "ENTRY":
            self._close_order(o, now_ns, "PARTIAL_CANCELLED" if o.filled_qty else "NO_FILL")
            return
        # exits are never abandoned: close this order and re-submit the remainder (audited)
        self._close_order(o, now_ns, "PARTIAL_CANCELLED" if o.filled_qty else "NO_FILL")
        if p.status == "EXITING":
            self.audit.add(now_ns, "exit_resubmitted", trade_id=p.trade_id, last_reason=o.last_reason)
            self._new_order(p, "EXIT", "SELL", p.qty - sum(f[2] for f in p.exit_fills), now_ns, p.exit_reason)

    def _close_order(self, o: Order, now_ns: int, status: str) -> None:
        o.status = status
        o.charges = self.fees.charges(o.side, o.turnover, executed=o.filled_qty > 0)
        self.audit.add(now_ns, "order_closed", order_id=o.id, status=status, filled_qty=o.filled_qty,
                       avg_px=o.avg_px, charges=o.charges, last_reason=o.last_reason)
        p = self.positions[o.trade_id]
        if o.purpose == "ENTRY":
            p.entry_charges += o.charges["total"]
            if o.filled_qty == 0:
                p.status = "CANCELLED"
                self.cycles_open.pop(p.intent.cycle_key, None)
                self.audit.add(now_ns, "entry_not_filled", trade_id=p.trade_id, reason=o.last_reason)
                self._notify("entry_not_filled", cycle_key=p.intent.cycle_key, trade_id=p.trade_id,
                             reason=o.last_reason)
            else:
                self._capital(now_ns, o.charges["total"])
        else:
            p.exit_charges += o.charges["total"]
            if sum(f[2] for f in p.exit_fills) >= p.qty:
                self._finish(p, now_ns)

    def _apply_fill(self, o: Order, ts: int, px: float, qty: int, lowfi: bool) -> None:
        p = self.positions[o.trade_id]
        p.low_fidelity |= lowfi
        if o.purpose == "ENTRY":
            cost = (p.entry_px or 0) * p.qty + px * qty
            p.qty += qty
            p.entry_px = cost / p.qty
            first = p.status == "PENDING_ENTRY"
            p.stop = round(p.entry_px * (1 - p.intent.stop_pct / 100), 2)
            p.target = round(p.entry_px * (1 + p.intent.target_pct / 100), 2)
            self._capital(ts, px * qty)
            if first:
                p.status, p.opened_ns = "OPEN", ts
                self.audit.add(ts, "position_opened", trade_id=p.trade_id, stop=p.stop, target=p.target)
                self._notify("position_opened", cycle_key=p.intent.cycle_key, trade_id=p.trade_id, entry_px=px,
                             qty=qty)
                self._timer(ts + p.intent.time_stop_s * NS, "time_stop", p.trade_id)
                self._timer(flat_ns(ts), "flat", p.trade_id)
        else:
            p.exit_fills.append((ts, px, qty, lowfi))
            self._capital(ts, -(p.entry_px * qty + p.entry_charges * qty / p.qty))

    def _capital(self, ts: int, delta: float) -> None:
        c = self.capital
        c["deployed"] = round(c["deployed"] + delta, 6)
        if c["deployed"] > c["peak"] + 1e-9:
            c["peak"], c["peak_ns"] = c["deployed"], ts
            c["peak_trades"] = sorted(t for t, p in self.positions.items() if p.status in ("OPEN", "EXITING")
                                      or (p.status == "PENDING_ENTRY" and p.qty))

    def _finish(self, p: Position, ts: int) -> None:
        p.status = "CLOSED"
        self.cycles_open.pop(p.intent.cycle_key, None)
        exit_qty = sum(f[2] for f in p.exit_fills)
        exit_px = sum(f[1] * f[2] for f in p.exit_fills) / exit_qty
        gross = (exit_px - p.entry_px) * p.qty
        charges = round(p.entry_charges + p.exit_charges, 2)
        risk = (p.entry_px - p.stop) * p.qty
        t = {"trade_id": p.trade_id, "cycle": p.intent.cycle_key, "strategy": p.intent.strategy_id,
             "contract": p.intent.contract, "dte": p.intent.dte, "qty": p.qty, "lots": p.qty // p.intent.lot_size,
             "requested_lots": p.intent.lots, "decision_ns": p.intent.decision_ns, "entry_ns": p.opened_ns,
             "exit_ns": ts, "entry_px": round(p.entry_px, 4), "exit_px": round(exit_px, 4), "stop": p.stop,
             "target": p.target, "exit_reason": p.exit_reason, "gross_pnl": round(gross, 2), "charges": charges,
             "net_pnl": round(gross - charges, 2), "risk_inr": round(risk, 2),
             "r_multiple": round((gross - charges) / risk, 4) if risk > 0 else None,
             "holding_s": round((ts - p.opened_ns) / NS, 3), "low_fidelity": p.low_fidelity,
             "best_bid": p.best_bid, "worst_bid": p.worst_bid, "fee_version": self.fees.version,
             "fill_model": self.cfg.version}
        self.trades.append(t)
        self.audit.add(ts, "trade_closed", **t)
        self._notify("trade_closed", trade=t)

    def mark_unfilled_at_close(self, now_ns: int) -> None:
        """End of data: anything still exiting is closed at the last valid bid (flagged)."""
        for p in self.positions.values():
            if p.status in ("OPEN", "EXITING"):
                q = self.book.get(p.intent.contract)
                left = p.qty - sum(f[2] for f in p.exit_fills)
                px = q.bid if q and q.bid else 0.0
                self.audit.add(now_ns, "forced_mark", trade_id=p.trade_id, qty=left, px=px)
                p.exit_reason = (p.exit_reason or "END_OF_DATA") + "|MARKED"
                p.exit_fills.append((now_ns, px, left, True))
                p.low_fidelity = True
                self._capital(now_ns, -(p.entry_px * left + p.entry_charges * left / p.qty))
                p.exit_charges += self.fees.charges("SELL", px * left)["total"]
                self._finish(p, now_ns)


def flat_ns(ts_ns: int) -> int:
    d = datetime.fromtimestamp(ts_ns / NS, IST).date()
    return int(datetime.combine(d, FLAT_BY, IST).timestamp()) * NS


# ---------------------------------------------------------------------------------------- bar-mode exits
def resolve_bar_exit(stop: float, target: float, bar: dict, half_spread: float, slippage: float) -> tuple[str, float] | None:
    """OHLC-only option data (no quotes): conservative intrabar resolution (v1.3 §10.2).

    Bars are mid/LTP-based, so executable bid = price - half_spread - slippage. If the bar's range touches
    both stop and target, the stop is assumed to have come first. Gaps through a level fill at the open."""
    o, h, low = bar["open"], bar["high"], bar["low"]
    adj = half_spread + slippage
    if o - adj <= stop:
        return "STOP", round(o - adj, 2)
    if o - adj >= target:
        return "TARGET", round(o - adj, 2)
    hit_stop, hit_target = low - adj <= stop, h - adj >= target
    if hit_stop:
        return "STOP", round(stop - slippage, 2)   # both touched -> stop first
    if hit_target:
        return "TARGET", round(target, 2)
    return None
