"""Push-driven twin of the backtest ReplayEngine (v1.3 §10.2 "same code as paper trading").

The live feed pushes quotes, final bars and clock ticks one at a time; this class applies them with exactly
the ReplayEngine's ordering rules, so a recorded day pushed through it gives the same decisions, fills and
audit chain as a backtest replay of that day (tests/test_phase9.py proves it on real history):

  * at equal timestamps: quotes  <  broker timers  <  bar decisions
  * a quote at time t first fires every timer strictly before t, then updates the book
  * a bar available at time t first fires every timer at or before t, then runs indicators + strategy
  * a clock tick at time t fires every timer at or before t (timers keep running while the feed is quiet)
Time never goes backwards: a late event is applied at the current engine time and counted.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from app.backtest.broker import PaperBroker
from app.backtest.fills import NS, Quote
from app.backtest.replay import ReplayEngine
from app.indicators.engine import IndicatorEngine


@dataclass
class StreamEngine:
    broker: PaperBroker
    strategy: object | None = None
    indicators: IndicatorEngine | None = None
    now_ns: int = 0
    late_events: int = 0
    finished: bool = False
    _replay: ReplayEngine = field(init=False)

    def __post_init__(self):
        # bar handling (indicators -> strategy -> entry / exit actions) is shared with the backtest engine
        self._replay = ReplayEngine(self.broker, self.strategy, self.indicators)

    @property
    def decisions(self) -> list[dict]:
        return self._replay.decisions

    def _clock(self, ts: int) -> int:
        if self.finished:
            raise RuntimeError("engine already finished")
        if ts < self.now_ns:
            self.late_events += 1
            ts = self.now_ns
        return ts

    def _fire(self, until_ns: int, inclusive: bool) -> None:
        while True:
            t = self.broker.next_timer_ns()
            if t is None or t > until_ns or (not inclusive and t == until_ns):
                return
            self.broker.run_timers(t)
            self.now_ns = max(self.now_ns, t)

    def on_quote(self, q: Quote) -> None:
        ts = self._clock(q.ts_ns)
        self._fire(ts, inclusive=False)
        self.now_ns = ts
        self.broker.on_quote(q if q.ts_ns == ts else Quote(ts, q.key, q.bids, q.asks, q.ltp, q.tier, q.has_depth))

    def on_bar(self, stream: str, bar: dict, available_ns: int) -> None:
        ts = self._clock(available_ns)
        self._fire(ts, inclusive=True)
        self.now_ns = ts
        self._replay._on_bar(ts, stream, bar)

    def tick(self, now_ns: int) -> None:
        ts = self._clock(now_ns)
        self._fire(ts, inclusive=True)
        self.now_ns = ts

    def finish(self) -> PaperBroker:
        """End of session data: resolve pending orders within the same bounded horizon as the replay engine,
        then mark anything still open at the last valid bid (flagged)."""
        last = self.now_ns
        horizon = last + int((2 * self.broker.cfg.order_timeout_s + 1) * NS)
        while self.broker.next_timer_ns() is not None and self.broker.next_timer_ns() <= horizon:
            t = self.broker.next_timer_ns()
            self.broker.run_timers(t)
            last = t
        self.broker.mark_unfilled_at_close(last)
        self.now_ns, self.finished = last, True
        return self.broker
