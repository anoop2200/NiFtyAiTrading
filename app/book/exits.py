"""Exit management and costs of the strategy book (Implementation Specification §2.5, §4).

`Position.on_bar` is the specification's `manage_exit` made incremental: it is called with each FINAL 1-minute
option bar of the traded contract, in time order, starting with the entry bar, and applies the checks in the
specification's (pessimistic) order:

  1. bar starts at/after 15:15            -> sell at the bar open (SQUARE_OFF)
  2. not the entry bar, bar start >= flip  -> sell at the open (ST_FLIP)
  3. not the entry bar, open <= stop       -> sell at the open (gap through the stop)
  4. low <= stop                           -> sell at the stop (the entry bar itself can stop out)
  5. only then: peak = max(peak, high), break-even, trail (a raised stop acts from the next bar)
"""
from __future__ import annotations

from dataclasses import dataclass, field

EXIT_PROFILES = {
    "X1": dict(sl=0.15, trig=0.15, step=0.15, be=None),   # stop -15%; from +15% trail 15% below the high
    "E5": dict(sl=0.15, trig=0.40, step=0.25, be=0.20),   # stop -15%; +20% -> break-even; +40% trail 25% below
    "X2": dict(sl=0.15, trig=None, step=None, be=None),   # fixed stop -15%
}
SQUARE_OFF_MIN = 15 * 60 + 15                             # 15:15 IST


@dataclass
class CostModel:
    """Spec §2.5: per leg on the (slippage-adjusted) fill x quantity; floored per round trip."""
    tick: float = 0.05
    slippage_ticks: int = 1
    illiquid_slippage_ticks: int = 3
    illiquid_volume: int = 500             # bar volume under this -> illiquid slippage
    brokerage_cap: float = 20.0
    brokerage_rate: float = 0.0003
    stt_sell_rate: float = 0.001
    exchange_rate: float = 0.00035
    sebi_rate: float = 0.000001
    stamp_buy_rate: float = 0.00003
    gst_rate: float = 0.18
    floor: float = 85.0
    version: str = "book-cost-1"

    def ticks(self, volume) -> int:
        return self.illiquid_slippage_ticks if volume is not None and volume < self.illiquid_volume else self.slippage_ticks

    def round_trip(self, entry_px: float, exit_px: float, qty: int, entry_vol=None, exit_vol=None,
                   slippage: bool = True) -> dict:
        si = self.ticks(entry_vol) * self.tick if slippage else 0.0
        so = self.ticks(exit_vol) * self.tick if slippage else 0.0
        buy, sell = entry_px + si, max(exit_px - so, 0.0)
        tb, ts = buy * qty, sell * qty
        brk = min(self.brokerage_cap, self.brokerage_rate * tb) + min(self.brokerage_cap, self.brokerage_rate * ts)
        exch = self.exchange_rate * (tb + ts)
        sebi = self.sebi_rate * (tb + ts)
        parts = {"slippage": (si + so) * qty, "brokerage": brk, "stt": self.stt_sell_rate * ts, "exchange": exch,
                 "sebi": sebi, "stamp": self.stamp_buy_rate * tb, "gst": self.gst_rate * (brk + exch + sebi)}
        modelled = sum(parts.values())
        total = max(modelled, self.floor)
        return {"total": round(total, 2), "modelled": round(modelled, 2), "floored": total > modelled,
                "parts": {k: round(v, 2) for k, v in parts.items()}}


@dataclass
class Position:
    key: str
    side: str
    profile: str
    entry_ts: int                       # entry bar start (epoch s)
    entry: float                        # entry bar open (model fill before slippage)
    qty: int
    entry_volume: int | None = None
    stop: float = 0.0
    peak: float = 0.0
    armed: bool = False
    flip_ts: int | None = None          # epoch s of the index candle close where the Supertrend turned against
    bars_seen: int = 0
    exit_ts: int | None = None
    exit_px: float | None = None
    exit_reason: str | None = None
    exit_volume: int | None = None
    stop_at_exit_bar: float | None = None
    trail_history: list = field(default_factory=list)

    def __post_init__(self):
        p = EXIT_PROFILES[self.profile]
        if not self.stop:
            self.stop = self.entry * (1 - p["sl"])
        if not self.peak:
            self.peak = self.entry

    @property
    def open(self) -> bool:
        return self.exit_reason is None

    def _close(self, ts, px, reason, volume) -> tuple:
        self.exit_ts, self.exit_px, self.exit_reason, self.exit_volume = ts, round(px, 2), reason, volume
        return ts, self.exit_px, reason

    def on_bar(self, bar: dict, minute_of_day: int):
        """One final option bar (ts = start, epoch s). Returns (ts, price, reason) when the position exits."""
        if not self.open or bar["ts"] < self.entry_ts:
            return None
        first = bar["ts"] == self.entry_ts
        self.bars_seen += 1
        self.stop_at_exit_bar = self.stop
        o, h, l, v = bar["open"], bar["high"], bar["low"], bar.get("volume")
        if minute_of_day >= SQUARE_OFF_MIN:
            return self._close(bar["ts"], o, "SQUARE_OFF", v)
        if self.flip_ts is not None and not first and bar["ts"] >= self.flip_ts:
            return self._close(bar["ts"], o, "ST_FLIP", v)
        if not first and o <= self.stop:
            return self._close(bar["ts"], o, "TRAIL_SL" if self.armed else "SL", v)
        if l <= self.stop:
            return self._close(bar["ts"], self.stop, "TRAIL_SL" if self.armed else "SL", v)
        p = EXIT_PROFILES[self.profile]
        old = self.stop
        self.peak = max(self.peak, h)
        if p["be"] is not None and self.peak >= self.entry * (1 + p["be"]):
            self.stop = max(self.stop, self.entry)
        if p["trig"] is not None and self.peak >= self.entry * (1 + p["trig"]):
            self.armed = True
            self.stop = max(self.stop, self.peak * (1 - p["step"]))
        if self.stop != old:
            self.trail_history.append((bar["ts"], round(self.stop, 2)))
        return None

    def force_close(self, ts: int, px: float, reason: str, volume=None):
        return self._close(ts, px, reason, volume)
