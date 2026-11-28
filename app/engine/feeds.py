"""Event sources for the paper engine.

HistoryFeed   a recorded history day pushed through the engine exactly as the live feed would push it
              (quotes at their receive time, each 1-minute bar when it becomes final, clock ticks) — used for
              dry runs, the 30-session operational rehearsal and the recovery tests.
LiveAdapter   turns normalized Dhan feed rows into option quotes (5-level depth) and final index / futures bars
              for a PaperSession; wired into the market-data recorder's row callback (same process, so the
              recorder keeps the only Dhan connection).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime

import pandas as pd

from app.backtest.fills import NS, Quote
from app.backtest.history import contract_key, lot_size_evidence, snapshot_quotes
from app.clock import IST
from app.marketdata.bars import BAD, BarBuilder
from app.marketdata.normalize import Normalizer


# ---------------------------------------------------------------------------------------------- history
def history_events(quotes: list[Quote], index_bars: pd.DataFrame, tick_s: float = 5.0,
                   vix_bars: pd.DataFrame | None = None) -> list[tuple]:
    """Ordered (ts_ns, kind, payload) events: quotes, bar closes, ticks. Equal timestamps: quote < tick < bar
    (and a VIX bar before the index bar of the same minute; the pipeline only uses VIX bars closed a minute
    earlier, so this order cannot change a decision)."""
    ev = [(q.ts_ns, 0, i, "q", q) for i, q in enumerate(quotes)]
    if vix_bars is not None and len(vix_bars):
        ev += [((int(r["ts"]) + 60) * NS, 2, -1 - i, "v", r) for i, r in enumerate(vix_bars.to_dict("records"))]
    recs = index_bars.to_dict("records")
    ev += [((int(r["ts"]) + 60) * NS, 2, i, "b", r) for i, r in enumerate(recs)]
    if ev and tick_s:
        lo, hi = min(e[0] for e in ev), max(e[0] for e in ev)
        step = int(tick_s * NS)
        ev += [(t, 1, 0, "t", None) for t in range(lo - lo % step + step, hi, step)]
    ev.sort(key=lambda e: e[:3])
    return [(ts, k, x) for ts, _, _, k, x in ev]


def push(session, events, stop_after: int | None = None) -> int:
    """Feed events into a PaperSession; returns how many were pushed (stop_after simulates a crash)."""
    n = 0
    for ts, k, x in events:
        if stop_after is not None and n >= stop_after:
            break
        if k == "q":
            session.quote(x)
        elif k == "b":
            session.bar("index", x, ts)
        elif k == "v":
            session.bar("vix", x, ts)
        else:
            session.tick(ts)
        n += 1
    return n


def prior_daily(index: pd.DataFrame, day: str, n: int = 20) -> list[dict]:
    d = index[index["session"] < day].groupby("session").agg(high=("high", "max"), low=("low", "min"),
                                                             close=("close", "last")).tail(n)
    return [{"session": s, "high": float(r.high), "low": float(r.low), "close": float(r.close)} for s, r in d.iterrows()]


def warmup_bars(index: pd.DataFrame, day: str, sessions: int = 10) -> dict:
    """Index 1-minute bars of the `sessions` sessions before `day`, as journal-safe dicts."""
    prior = sorted(index.loc[index["session"] < day, "session"].unique())[-sessions:]
    df = index[index["session"].isin(prior)].sort_values("ts")
    return {"index": [{k: (v.item() if hasattr(v, "item") else v) for k, v in r.items()}
                      for r in df.to_dict("records")]}


def history_day(ctx, day: str) -> tuple[list[Quote], pd.DataFrame, int]:
    path = ctx.day_path(day)
    return snapshot_quotes(path), ctx.index[ctx.index["session"] == day], lot_size_evidence(path)


# ---------------------------------------------------------------------------------------------- live
@dataclass
class LiveAdapter:
    session: object                        # PaperSession
    today: date
    index_id: int
    future_id: int | None
    options: dict                          # security_id -> universe Sub (expiry, strike, option_type); live view
    tf_s: int = 60
    grace_s: float = 2.0
    vix_id: int | None = None
    norm: Normalizer = field(default_factory=Normalizer)
    errors: int = 0
    quotes: int = 0
    bars: int = 0

    def __post_init__(self):
        self.bb = BarBuilder(tf_s=self.tf_s, grace_s=self.grace_s,
                             index_ids={self.index_id} | ({self.vix_id} if self.vix_id else set()))
        self.session_open_ts = int(datetime.combine(self.today, datetime.min.time(), IST)
                                   .replace(hour=9, minute=15).timestamp())

    def on_rows(self, rows: list[dict]) -> None:
        for r in rows:
            try:
                self._row(r)
            except Exception:  # noqa: BLE001 - one bad packet must not stop the engine; counted + surfaced
                self.errors += 1

    def _row(self, r: dict) -> None:
        ev = self.norm.process(r)
        if ev is None:
            return
        sid = ev["security_id"]
        if sid in self.options:
            if ev["flags"] & BAD or ev.get("bid") is None:
                return
            sub = self.options[sid]
            exp, k, ot = sub.expiry, sub.strike, sub.option_type
            bids = tuple((float(r[f"bid_px_{i}"]), int(r[f"bid_qty_{i}"])) for i in range(1, 6)
                         if r.get(f"bid_px_{i}") and r.get(f"bid_qty_{i}"))
            asks = tuple((float(r[f"ask_px_{i}"]), int(r[f"ask_qty_{i}"])) for i in range(1, 6)
                         if r.get(f"ask_px_{i}") and r.get(f"ask_qty_{i}"))
            ltt = ev.get("ltt")
            self.session.quote(Quote(ev["recv_ns"], contract_key(exp, k, ot), bids, asks, ev.get("ltp"),
                                     "observed_feed", True, ev.get("volume"), int(ltt) if ltt else None))
            self.quotes += 1
            return
        if sid in (self.index_id, self.future_id) or (self.vix_id and sid == self.vix_id):
            self._bars(self.bb.on_event(ev), ev["recv_ns"])

    def _bars(self, rows: list[dict], now_ns: int) -> None:
        for b in rows:
            if b["open"] is None:
                continue
            sid = b["security_id"]
            stream = "index" if sid == self.index_id else ("vix" if self.vix_id and sid == self.vix_id else "future")
            bar = b | {"session": self.today.isoformat(), "session_open_ts": self.session_open_ts}
            self.session.bar(stream, bar, max(now_ns, (int(b["ts"]) + self.tf_s) * NS))
            self.bars += 1

    def tick(self, now_ns: int) -> None:
        """Clock tick: close bars on a quiet feed, then let the engine run its timers / watchdog."""
        self.bb.watermark_ns = max(self.bb.watermark_ns, now_ns)
        self._bars(self.bb._finalize(), now_ns)
        self.session.tick(now_ns)
