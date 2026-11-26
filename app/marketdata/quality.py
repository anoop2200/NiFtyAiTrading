"""Daily data-quality matrix (v1.3 §4.2): one row per instrument per day.

Pure accumulation over normalized events plus the bar builder output, so it can run live (fed event by
event) or in batch over a recorded day.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from app.marketdata.normalize import Q

NS = 1_000_000_000


@dataclass
class _Inst:
    events: int = 0
    trades: int = 0
    book_events: int = 0
    flags: dict[str, int] = field(default_factory=dict)
    first_ns: int | None = None
    last_ns: int | None = None
    max_change_gap_s: float = 0.0
    stale_s: float = 0.0          # seconds inside the session spent with change age > stale limit
    spread_sum: float = 0.0
    spread_n: int = 0


@dataclass
class QualityMatrix:
    session_start_ns: int
    session_end_ns: int
    stale_after_s: float = 30.0
    insts: dict[int, _Inst] = field(default_factory=dict)

    def on_event(self, ev: dict) -> None:
        t = ev["recv_ns"]
        if not (self.session_start_ns <= t <= self.session_end_ns):
            return
        st = self.insts.setdefault(ev["security_id"], _Inst())
        st.events += 1
        flags = Q(ev.get("flags", 0))
        for f in Q:
            if f and f in flags:
                st.flags[f.name] = st.flags.get(f.name, 0) + 1
        if (ev.get("trade_qty") or 0) > 0:
            st.trades += 1
        if ev.get("spread") is not None and not (flags & Q.CROSSED):
            st.book_events += 1
            st.spread_sum += ev["spread"]
            st.spread_n += 1
        if Q.DUPLICATE in flags:
            return
        prev = st.last_ns if st.last_ns is not None else self.session_start_ns
        self._gap(st, (t - prev) / NS)
        if st.first_ns is None:
            st.first_ns = t
        st.last_ns = t

    def _gap(self, st: _Inst, gap: float) -> None:
        st.max_change_gap_s = max(st.max_change_gap_s, gap)
        if gap > self.stale_after_s:
            st.stale_s += gap - self.stale_after_s

    def finalize(self, bars_per_sid: dict[int, int] | None = None) -> list[dict]:
        span = (self.session_end_ns - self.session_start_ns) / NS
        minutes = round(span / 60)
        out = []
        for sid, st in sorted(self.insts.items()):
            tail = (self.session_end_ns - (st.last_ns or self.session_start_ns)) / NS
            gap, stale = st.max_change_gap_s, st.stale_s
            gap = max(gap, tail)
            if tail > self.stale_after_s:
                stale += tail - self.stale_after_s
            bars = (bars_per_sid or {}).get(sid)
            out.append({
                "security_id": sid, "events": st.events, "trades": st.trades, "book_events": st.book_events,
                "flags": dict(sorted(st.flags.items())),
                "max_change_gap_s": round(gap, 3),
                "pct_time_fresh": round(100 * (1 - stale / span), 2) if span else None,
                "mean_spread": round(st.spread_sum / st.spread_n, 4) if st.spread_n else None,
                "bars": bars, "bar_coverage_pct": round(100 * bars / minutes, 2) if bars is not None and minutes else None,
            })
        return out
