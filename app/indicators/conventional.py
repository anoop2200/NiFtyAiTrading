"""Conventional indicators. Each has a streaming `_update` and, where practical, an independent vectorised
`_vector` (pandas) used by the batch path, so the batch-vs-stream test compares two implementations."""
from __future__ import annotations

import math
from collections import deque

import numpy as np
import pandas as pd

from app.indicators.base import Indicator, IndicatorSpec, SignalState, SourceRequirement
from app.indicators.registry import register

S = SignalState
PRICE = SourceRequirement("index", ("close",))
OHLC = SourceRequirement("index", ("high", "low", "close"))


@register
class EMA(Indicator):
    SPEC = IndicatorSpec("ema", "Exponential moving average", "1.0.0", "trend",
                         "ema_t = a*close_t + (1-a)*ema_{t-1}, a = 2/(n+1), seeded with first close",
                         PRICE, ("ema", "slope"), "price", "close above a rising EMA = BULLISH, below a falling one = BEARISH",
                         recovery="snapshot")
    DEFAULTS = {"period": 21}

    def warmup(self):
        return self.params["period"]

    def init_state(self):
        self.ema = None

    def _update(self, bar):
        a = 2 / (self.params["period"] + 1)
        prev = self.ema
        self.ema = bar["close"] if prev is None else a * bar["close"] + (1 - a) * prev
        slope = None if prev is None else self.ema - prev
        st = S.NEUTRAL
        if slope is not None:
            st = S.BULLISH if bar["close"] > self.ema and slope > 0 else S.BEARISH if bar["close"] < self.ema and slope < 0 else S.NEUTRAL
        return {"ema": self.ema, "slope": slope, "state": st.value}

    def _vector(self, d):
        ema = d["close"].ewm(span=self.params["period"], adjust=False).mean()
        slope = ema.diff()
        st = np.where((d["close"] > ema) & (slope > 0), S.BULLISH.value,
                      np.where((d["close"] < ema) & (slope < 0), S.BEARISH.value, S.NEUTRAL.value))
        return pd.DataFrame({"ema": ema, "slope": slope, "state": st}, index=d.index)


@register
class SMA(Indicator):
    SPEC = IndicatorSpec("sma", "Simple moving average", "1.0.0", "trend", "mean(close, n)", PRICE, ("sma",),
                         "price", "reference level", recovery="replay")
    DEFAULTS = {"period": 20}

    def warmup(self):
        return self.params["period"]

    def init_state(self):
        self.win = deque(maxlen=self.params["period"])

    def _update(self, bar):
        self.win.append(bar["close"])
        return {"sma": sum(self.win) / len(self.win) if len(self.win) == self.win.maxlen else None}

    def _vector(self, d):
        return pd.DataFrame({"sma": d["close"].rolling(self.params["period"]).mean()}, index=d.index)


@register
class RSI(Indicator):
    SPEC = IndicatorSpec("rsi", "Relative strength index (Wilder)", "1.0.0", "momentum",
                         "100 - 100/(1+RS), RS = wilder(gain,n)/wilder(loss,n), Wilder = EMA alpha 1/n seeded with first change",
                         PRICE, ("rsi",), "0-100", ">= overbought -> OVERBOUGHT, <= oversold -> OVERSOLD")
    DEFAULTS = {"period": 14, "overbought": 70.0, "oversold": 30.0}

    def warmup(self):
        return self.params["period"] + 1

    def init_state(self):
        self.prev = None
        self.ag = self.al = None

    def _rsi(self, ag, al):
        if al == 0:
            return 100.0 if ag > 0 else 50.0
        return 100 - 100 / (1 + ag / al)

    def _state(self, r):
        p = self.params
        return S.OVERBOUGHT if r >= p["overbought"] else S.OVERSOLD if r <= p["oversold"] else S.NEUTRAL

    def _update(self, bar):
        c, prev = bar["close"], self.prev
        self.prev = c
        if prev is None:
            return {"rsi": None}
        g, l = max(c - prev, 0.0), max(prev - c, 0.0)
        a = 1 / self.params["period"]
        if self.ag is None:
            self.ag, self.al = g, l
        else:
            self.ag, self.al = a * g + (1 - a) * self.ag, a * l + (1 - a) * self.al
        r = self._rsi(self.ag, self.al)
        return {"rsi": r, "state": self._state(r).value}

    def _vector(self, d):
        ch = d["close"].diff()
        a = 1 / self.params["period"]
        ag = ch.clip(lower=0).iloc[1:].ewm(alpha=a, adjust=False).mean().reindex(d.index)
        al = (-ch).clip(lower=0).iloc[1:].ewm(alpha=a, adjust=False).mean().reindex(d.index)
        r = pd.Series([None if pd.isna(x) else self._rsi(x, y) for x, y in zip(ag, al)], index=d.index, dtype=float)
        st = [S.NO_SIGNAL.value if pd.isna(x) else self._state(x).value for x in r]
        return pd.DataFrame({"rsi": r, "state": st}, index=d.index)


@register
class ATR(Indicator):
    SPEC = IndicatorSpec("atr", "Average true range (Wilder)", "1.0.0", "volatility",
                         "TR = max(h-l, |h-pc|, |l-pc|) (first bar h-l); ATR = Wilder(TR, n)", OHLC, ("atr", "atr_pct"),
                         "points / % of close", "volatility scale for stops and regime")
    DEFAULTS = {"period": 14}

    def warmup(self):
        return self.params["period"]

    def init_state(self):
        self.pc = None
        self.atr = None

    def _update(self, bar):
        h, l, c = bar["high"], bar["low"], bar["close"]
        tr = h - l if self.pc is None else max(h - l, abs(h - self.pc), abs(l - self.pc))
        self.pc = c
        a = 1 / self.params["period"]
        self.atr = tr if self.atr is None else a * tr + (1 - a) * self.atr
        return {"atr": self.atr, "atr_pct": 100 * self.atr / c}

    def _vector(self, d):
        pc = d["close"].shift(1)
        tr = pd.concat([d["high"] - d["low"], (d["high"] - pc).abs(), (d["low"] - pc).abs()], axis=1).max(axis=1)
        atr = tr.ewm(alpha=1 / self.params["period"], adjust=False).mean()
        return pd.DataFrame({"atr": atr, "atr_pct": 100 * atr / d["close"]}, index=d.index)


@register
class Bollinger(Indicator):
    SPEC = IndicatorSpec("bbands", "Bollinger bands", "1.0.0", "volatility",
                         "mid = SMA(n); upper/lower = mid +/- k*std(n, ddof=0); width = (upper-lower)/mid; %b",
                         PRICE, ("mid", "upper", "lower", "width", "pct_b"), "price / ratio",
                         "close above upper = BREAKOUT; below lower = REVERSAL_CANDIDATE", recovery="replay")
    DEFAULTS = {"period": 20, "k": 2.0}

    def warmup(self):
        return self.params["period"]

    def init_state(self):
        self.win = deque(maxlen=self.params["period"])

    def _calc(self, c, mid, sd):
        up, lo = mid + self.params["k"] * sd, mid - self.params["k"] * sd
        pb = (c - lo) / (up - lo) if up > lo else 0.5
        st = S.BREAKOUT if c > up else S.REVERSAL_CANDIDATE if c < lo else S.NEUTRAL
        return {"mid": mid, "upper": up, "lower": lo, "width": (up - lo) / mid, "pct_b": pb, "state": st.value}

    def _update(self, bar):
        self.win.append(bar["close"])
        if len(self.win) < self.win.maxlen:
            return {}
        m = sum(self.win) / len(self.win)
        sd = math.sqrt(sum((x - m) ** 2 for x in self.win) / len(self.win))
        return self._calc(bar["close"], m, sd)

    def _vector(self, d):
        n = self.params["period"]
        m, sd = d["close"].rolling(n).mean(), d["close"].rolling(n).std(ddof=0)
        rows = [self._calc(c, a, b) if not pd.isna(a) else {} for c, a, b in zip(d["close"], m, sd)]
        return pd.DataFrame(rows, index=d.index, columns=[*self.SPEC.outputs, "state"])


@register
class ROC(Indicator):
    SPEC = IndicatorSpec("roc", "Rate of change", "1.0.0", "momentum", "100*(close/close[n]-1)", PRICE, ("roc",),
                         "%", "momentum sign / magnitude", recovery="replay")
    DEFAULTS = {"period": 10}

    def warmup(self):
        return self.params["period"] + 1

    def init_state(self):
        self.win = deque(maxlen=self.params["period"] + 1)

    def _update(self, bar):
        self.win.append(bar["close"])
        if len(self.win) < self.win.maxlen:
            return {}
        r = 100 * (self.win[-1] / self.win[0] - 1)
        return {"roc": r, "state": (S.BULLISH if r > 0 else S.BEARISH if r < 0 else S.NEUTRAL).value}

    def _vector(self, d):
        r = 100 * (d["close"] / d["close"].shift(self.params["period"]) - 1)
        st = np.where(r > 0, S.BULLISH.value, np.where(r < 0, S.BEARISH.value, S.NEUTRAL.value))
        return pd.DataFrame({"roc": r, "state": st}, index=d.index)


@register
class SessionVWAP(Indicator):
    SPEC = IndicatorSpec("vwap", "Session VWAP", "1.0.0", "trend",
                         "sum(typical*volume)/sum(volume) since session start, typical = (h+l+c)/3",
                         SourceRequirement("future", ("high", "low", "close", "volume"),
                                           ("observed_feed", "observed_broker_candles")),
                         ("vwap", "dist_pct"), "price / %", "close above VWAP = BULLISH",
                         session_reset=True, missing_data="carry", recovery="snapshot")

    def init_state(self):
        self.pv = 0.0
        self.v = 0.0

    def _update(self, bar):
        self.pv += (bar["high"] + bar["low"] + bar["close"]) / 3 * bar["volume"]
        self.v += bar["volume"]
        if self.v <= 0:
            return {}
        vw = self.pv / self.v
        return {"vwap": vw, "dist_pct": 100 * (bar["close"] / vw - 1),
                "state": (S.BULLISH if bar["close"] > vw else S.BEARISH if bar["close"] < vw else S.NEUTRAL).value}

    def _vector(self, d):
        pv = ((d["high"] + d["low"] + d["close"]) / 3 * d["volume"]).cumsum()
        v = d["volume"].cumsum()
        vw = (pv / v).where(v > 0)
        st = np.where(d["close"] > vw, S.BULLISH.value, np.where(d["close"] < vw, S.BEARISH.value, S.NEUTRAL.value))
        return pd.DataFrame({"vwap": vw, "dist_pct": 100 * (d["close"] / vw - 1), "state": st}, index=d.index)


@register
class OpeningRange(Indicator):
    SPEC = IndicatorSpec("opening_range", "Opening range levels", "1.0.0", "price_action",
                         "high/low of the first m minutes of the session; breakout when close leaves the range",
                         OHLC, ("or_high", "or_low", "or_width_pct", "direction"), "price / % / -1,0,1",
                         "first close above OR high / below OR low after the range completes = BREAKOUT "
                         "(direction +1/-1); while still outside = BULLISH / BEARISH",
                         session_reset=True, missing_data="carry")
    DEFAULTS = {"minutes": 15}

    def warmup(self):
        return 1

    def init_state(self):
        self.start = None
        self.hi = self.lo = None
        self.done = False
        self.prev_dir = 0

    def _update(self, bar):
        if self.start is None:  # anchor to the official session open, not to the first bar received
            self.start = int(bar.get("session_open_ts") or bar["ts"])
        if bar["ts"] < self.start + 60 * self.params["minutes"]:
            self.hi = bar["high"] if self.hi is None else max(self.hi, bar["high"])
            self.lo = bar["low"] if self.lo is None else min(self.lo, bar["low"])
            return {}  # range still forming: no output yet (never uses later bars)
        self.done = True
        c = bar["close"]
        d = 1 if c > self.hi else -1 if c < self.lo else 0
        # BREAKOUT only on the bar that first leaves the range on that side; afterwards BULLISH / BEARISH
        st = S.NEUTRAL
        if d:
            st = S.BREAKOUT if d != self.prev_dir else (S.BULLISH if d > 0 else S.BEARISH)
        self.prev_dir = d
        return {"or_high": self.hi, "or_low": self.lo, "or_width_pct": 100 * (self.hi - self.lo) / self.lo,
                "direction": d, "state": st.value}
