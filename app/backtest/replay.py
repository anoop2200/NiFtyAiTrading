"""Event-driven chronological replay (v1.3 §10.2).

Merges three event kinds in time order:
  * quotes       (ts = quote receive/snapshot time)  -> broker.on_quote
  * bar closes   (ts = bar start + timeframe: a bar is usable only after it ends) -> indicators -> strategy
  * broker timers (order arrival, order timeout, time stop, 15:25 flat)
At equal timestamps market data is applied before timers and timers before strategy decisions, so a
decision never sees a quote from after its own time and an order never fills on a quote from after arrival.
The same IndicatorEngine and PaperBroker classes are used by paper trading.
"""
from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from typing import Iterable, Protocol

import pandas as pd

from app.backtest.broker import EntryIntent, PaperBroker
from app.backtest.fills import NS, Quote
from app.indicators.engine import IndicatorEngine


@dataclass(frozen=True)
class ExitRequest:
    trade_id: int
    reason: str


class Strategy(Protocol):
    strategy_id: str

    def on_bar(self, stream: str, bar: dict, indicators: dict[str, dict], ctx: "Context") -> list: ...


@dataclass
class Context:
    now_ns: int
    broker: PaperBroker
    extra: dict = field(default_factory=dict)

    def quote(self, key: str) -> Quote | None:
        q = self.broker.book.get(key)
        return q if q is not None and q.ts_ns <= self.now_ns else None

    def quotes(self) -> dict[str, Quote]:
        return {k: q for k, q in self.broker.book.items() if q.ts_ns <= self.now_ns}


QUOTE, TIMER, BAR = 0, 1, 2   # priority at equal timestamps


@dataclass
class ReplayEngine:
    broker: PaperBroker
    strategy: Strategy | None = None
    indicators: IndicatorEngine | None = None
    bar_seconds: int = 60
    decisions: list[dict] = field(default_factory=list)

    def run(self, bars: dict[str, pd.DataFrame], quotes: Iterable[Quote]) -> PaperBroker:
        heap: list = []
        seq = 0
        for name, df in bars.items():
            for r in df.to_dict("records"):
                heapq.heappush(heap, ((int(r["ts"]) + self.bar_seconds) * NS, BAR, seq, name, r))
                seq += 1
        qit = iter(sorted(quotes, key=lambda q: q.ts_ns))
        nxt_q = next(qit, None)
        last_ns = 0
        while heap or nxt_q is not None or self.broker.next_timer_ns() is not None:
            cands = []
            if nxt_q is not None:
                cands.append((nxt_q.ts_ns, QUOTE))
            t = self.broker.next_timer_ns()
            if t is not None:
                cands.append((t, TIMER))
            if heap:
                cands.append((heap[0][0], BAR))
            ts, kind = min(cands)
            if ts < last_ns:
                raise RuntimeError("replay went backwards in time")
            last_ns = ts
            if kind == QUOTE:
                self.broker.on_quote(nxt_q)
                nxt_q = next(qit, None)
            elif kind == TIMER:
                self.broker.run_timers(ts)
            else:
                _, _, _, name, bar = heapq.heappop(heap)
                self._on_bar(ts, name, bar)
            if not heap and nxt_q is None:
                # data exhausted: let pending arrivals/timeouts resolve on the last known book for a bounded
                # horizon (exit re-submits could otherwise loop forever), then mark what is still open
                horizon = ts + int((2 * self.broker.cfg.order_timeout_s + 1) * NS)
                while self.broker.next_timer_ns() is not None and self.broker.next_timer_ns() <= horizon:
                    t = self.broker.next_timer_ns()
                    self.broker.run_timers(t)
                    last_ns = t
                break
        self.broker.mark_unfilled_at_close(last_ns)
        return self.broker

    def _on_bar(self, now_ns: int, name: str, bar: dict) -> None:
        outs = {}
        if self.indicators is not None:
            for row in self.indicators.on_bar(name, bar):
                outs[row["binding"]] = row
        if self.strategy is None:
            return
        ctx = Context(now_ns, self.broker)
        for act in self.strategy.on_bar(name, bar, outs, ctx) or []:
            if isinstance(act, EntryIntent):
                tid = self.broker.submit_entry(act)
                self.decisions.append({"ts_ns": now_ns, "type": "entry", "cycle": act.cycle_key, "trade_id": tid})
            elif isinstance(act, ExitRequest):
                self.broker.request_exit(act.trade_id, now_ns, act.reason)
                self.decisions.append({"ts_ns": now_ns, "type": "exit", "trade_id": act.trade_id,
                                       "reason": act.reason})
