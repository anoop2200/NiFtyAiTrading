"""Custom indicators (v1.3 §5.1). Research concepts: names do not imply verified institutional activity or
profitability; they enter production only after validation and explicit approval."""
from __future__ import annotations

import math
from collections import deque

from app.indicators.base import Indicator, IndicatorSpec, SignalState, SourceRequirement
from app.indicators.conventional import EMA, RSI
from app.indicators.registry import register

S = SignalState
PRICE = SourceRequirement("index", ("close",))
OHLC = SourceRequirement("index", ("high", "low", "close"))


@register
class KAMA(Indicator):
    SPEC = IndicatorSpec("kama", "Adaptive EMA (Kaufman)", "1.0.0", "trend",
                         "ER = |close-close[n]| / sum|diff|; sc = (ER*(2/(f+1)-2/(s+1)) + 2/(s+1))^2; kama += sc*(close-kama)",
                         PRICE, ("kama", "efficiency"), "price / 0-1",
                         "efficiency near 1 = trending, near 0 = noisy; close vs KAMA gives direction")
    DEFAULTS = {"period": 10, "fast": 2, "slow": 30}

    def warmup(self):
        return self.params["period"] + 1

    def init_state(self):
        self.win = deque(maxlen=self.params["period"] + 1)
        self.kama = None

    def _update(self, bar):
        c = bar["close"]
        self.win.append(c)
        if len(self.win) < self.win.maxlen:
            self.kama = c
            return {}
        w = list(self.win)
        noise = sum(abs(b - a) for a, b in zip(w, w[1:]))
        er = abs(w[-1] - w[0]) / noise if noise > 0 else 0.0
        f, s = 2 / (self.params["fast"] + 1), 2 / (self.params["slow"] + 1)
        sc = (er * (f - s) + s) ** 2
        self.kama += sc * (c - self.kama)
        st = S.NEUTRAL if er < 0.3 else S.BULLISH if c > self.kama else S.BEARISH
        return {"kama": self.kama, "efficiency": er, "state": st.value}


@register
class ADX(Indicator):
    SPEC = IndicatorSpec("adx", "Trend strength (ADX, Wilder)", "1.0.0", "trend",
                         "+DM/-DM, TR Wilder-smoothed over n; DX = 100|+DI--DI|/(+DI+-DI); ADX = Wilder(DX, n)",
                         OHLC, ("adx", "plus_di", "minus_di"), "0-100",
                         "ADX >= threshold: trend; direction from DI cross")
    DEFAULTS = {"period": 14, "threshold": 25.0}

    def warmup(self):
        return 2 * self.params["period"]

    def init_state(self):
        self.prev = None
        self.tr = self.pdm = self.mdm = None
        self.adx = None

    def _update(self, bar):
        h, l, c = bar["high"], bar["low"], bar["close"]
        if self.prev is None:
            self.prev = (h, l, c)
            return {}
        ph, pl, pc = self.prev
        self.prev = (h, l, c)
        up, dn = h - ph, pl - l
        pdm = up if up > dn and up > 0 else 0.0
        mdm = dn if dn > up and dn > 0 else 0.0
        tr = max(h - l, abs(h - pc), abs(l - pc))
        a = 1 / self.params["period"]
        if self.tr is None:
            self.tr, self.pdm, self.mdm = tr, pdm, mdm
        else:
            self.tr = a * tr + (1 - a) * self.tr
            self.pdm = a * pdm + (1 - a) * self.pdm
            self.mdm = a * mdm + (1 - a) * self.mdm
        if self.tr <= 0:
            return {}
        pdi, mdi = 100 * self.pdm / self.tr, 100 * self.mdm / self.tr
        dx = 100 * abs(pdi - mdi) / (pdi + mdi) if pdi + mdi > 0 else 0.0
        self.adx = dx if self.adx is None else a * dx + (1 - a) * self.adx
        st = S.NEUTRAL
        if self.adx >= self.params["threshold"]:
            st = S.BULLISH if pdi > mdi else S.BEARISH
        return {"adx": self.adx, "plus_di": pdi, "minus_di": mdi, "state": st.value}


@register
class RealizedVol(Indicator):
    SPEC = IndicatorSpec("realized_vol", "Realized volatility (annualised)", "1.0.0", "volatility",
                         "std(log returns, n, ddof=1) * sqrt(375*252) for 1-minute bars",
                         PRICE, ("rv",), "annualised fraction", "input to vol regime / IV-RV spread",
                         recovery="replay")
    DEFAULTS = {"period": 30, "bars_per_year": 375 * 252}

    def warmup(self):
        return self.params["period"] + 1

    def init_state(self):
        self.prev = None
        self.rets = deque(maxlen=self.params["period"])

    def _update(self, bar):
        c, p = bar["close"], self.prev
        self.prev = c
        if p is None or p <= 0 or c <= 0:
            return {}
        self.rets.append(math.log(c / p))
        if len(self.rets) < self.rets.maxlen:
            return {}
        m = sum(self.rets) / len(self.rets)
        var = sum((r - m) ** 2 for r in self.rets) / (len(self.rets) - 1)
        return {"rv": math.sqrt(var * self.params["bars_per_year"])}


@register
class VolCompression(Indicator):
    SPEC = IndicatorSpec("vol_compression", "Volatility compression / expansion", "1.0.0", "volatility",
                         "Bollinger width percentile rank over lookback L; compression when rank <= low, "
                         "VOLATILITY_EXPANSION when rank crosses above high within m bars of a compression",
                         PRICE, ("width", "width_rank"), "ratio / 0-1",
                         "squeeze then expansion often precedes directional moves (research hypothesis)",
                         recovery="snapshot")
    DEFAULTS = {"period": 20, "k": 2.0, "lookback": 120, "low": 0.2, "high": 0.8, "memory": 30}

    def warmup(self):
        return self.params["period"] + self.params["lookback"] - 1

    def init_state(self):
        self.win = deque(maxlen=self.params["period"])
        self.widths = deque(maxlen=self.params["lookback"])
        self.since_squeeze = None

    def _update(self, bar):
        self.win.append(bar["close"])
        if len(self.win) < self.win.maxlen:
            return {}
        m = sum(self.win) / len(self.win)
        sd = math.sqrt(sum((x - m) ** 2 for x in self.win) / len(self.win))
        w = 2 * self.params["k"] * sd / m
        self.widths.append(w)
        rank = sum(1 for x in self.widths if x <= w) / len(self.widths)
        st = S.NEUTRAL
        if rank <= self.params["low"]:
            self.since_squeeze = 0
        elif self.since_squeeze is not None:
            self.since_squeeze += 1
            if rank >= self.params["high"] and self.since_squeeze <= self.params["memory"]:
                st = S.VOLATILITY_EXPANSION
                self.since_squeeze = None
            elif self.since_squeeze > self.params["memory"]:
                self.since_squeeze = None
        return {"width": w, "width_rank": rank, "state": st.value}


@register
class SwingPivots(Indicator):
    SPEC = IndicatorSpec("swing_pivots", "Confirmed swing highs / lows", "1.0.0", "price_action",
                         "bar i is a swing high when high[i] > highs of k bars each side; reported only at bar i+k "
                         "(confirmation lag) so no future bar is used",
                         OHLC, ("last_swing_high", "last_swing_low", "structure"), "price / -1,0,1",
                         "higher highs and higher lows = 1 (BULLISH), lower lows and lower highs = -1 (BEARISH)",
                         recovery="snapshot")
    DEFAULTS = {"k": 3}

    def warmup(self):
        return 2 * self.params["k"] + 1

    def init_state(self):
        self.win = deque(maxlen=2 * self.params["k"] + 1)
        self.highs: list[float] = []
        self.lows: list[float] = []

    def _update(self, bar):
        self.win.append((bar["high"], bar["low"]))
        k = self.params["k"]
        if len(self.win) == self.win.maxlen:
            w = list(self.win)
            ch, cl = w[k]
            others = w[:k] + w[k + 1:]
            if all(ch > h for h, _ in others):
                self.highs = (self.highs + [ch])[-2:]
            if all(cl < low for _, low in others):
                self.lows = (self.lows + [cl])[-2:]
        if not self.highs or not self.lows:
            return {}
        struct = 0
        if len(self.highs) == 2 and len(self.lows) == 2:
            if self.highs[1] > self.highs[0] and self.lows[1] > self.lows[0]:
                struct = 1
            elif self.highs[1] < self.highs[0] and self.lows[1] < self.lows[0]:
                struct = -1
        st = S.BULLISH if struct == 1 else S.BEARISH if struct == -1 else S.NEUTRAL
        return {"last_swing_high": self.highs[-1], "last_swing_low": self.lows[-1], "structure": struct,
                "state": st.value}


@register
class FuturesRelVolume(Indicator):
    SPEC = IndicatorSpec("fut_rel_volume", "Futures relative volume", "1.0.0", "participation",
                         "volume / mean(volume of previous n bars); acceleration = rel_vol - rel_vol[1]",
                         SourceRequirement("future", ("volume",), ("observed_feed", "observed_broker_candles")),
                         ("rel_vol", "accel"), "ratio",
                         "index volume is not real volume (v1.3 §3.2): participation must use futures volume",
                         recovery="replay")
    DEFAULTS = {"period": 20, "high": 2.0}

    def warmup(self):
        return self.params["period"] + 1

    def init_state(self):
        self.win = deque(maxlen=self.params["period"])
        self.prev_rv = None

    def _update(self, bar):
        v = bar["volume"]
        out = {}
        if len(self.win) == self.win.maxlen:
            m = sum(self.win) / len(self.win)
            rv = v / m if m > 0 else None
            if rv is not None:
                out = {"rel_vol": rv, "accel": None if self.prev_rv is None else rv - self.prev_rv,
                       "state": (S.BREAKOUT if rv >= self.params["high"] else S.NEUTRAL).value}
                self.prev_rv = rv
        self.win.append(v)
        return out


@register
class MarketStrengthIndex(Indicator):
    SPEC = IndicatorSpec("market_strength", "Market Strength Index (composite)", "1.0.0", "composite",
                         "mean of: sign(EMA fast - EMA slow), (RSI-50)/50, clipped ROC-style EMA slope z; "
                         "scaled to -100..100. Research composite: not a verified signal",
                         PRICE, ("msi",), "-100..100", ">= +40 BULLISH, <= -40 BEARISH",
                         dependencies=("ema@1.0.0", "rsi@1.0.0"))
    DEFAULTS = {"fast": 9, "slow": 21, "rsi": 14, "threshold": 40.0}

    def warmup(self):
        return max(self.params["slow"], self.params["rsi"] + 1)

    def init_state(self):
        self.fast = EMA(period=self.params["fast"])
        self.slow = EMA(period=self.params["slow"])
        self.rsi = RSI(period=self.params["rsi"])

    def get_state(self):
        return {"key": self.key, "state": {"n": self.n, "last_ts": self.last_ts, "session": self.session,
                                           "reset_count": self.reset_count,
                                           "children": [c.get_state() for c in (self.fast, self.slow, self.rsi)]}}

    def set_state(self, snap):
        if snap["key"] != self.key:
            raise ValueError(f"state belongs to {snap['key']}, not {self.key}")
        st = snap["state"]
        self.n, self.last_ts, self.session, self.reset_count = st["n"], st["last_ts"], st["session"], st["reset_count"]
        for c, s in zip((self.fast, self.slow, self.rsi), st["children"]):
            c.set_state(s)

    def _update(self, bar):
        # children see the same (already final) bar; their own gap/session rules are bypassed because
        # this composite resets them together through init_state.
        child_bar = {k: bar[k] for k in ("ts", "close")}
        f, s, r = self.fast._update(child_bar), self.slow._update(child_bar), self.rsi._update(child_bar)
        if r.get("rsi") is None:
            return {}
        trend = 1.0 if f["ema"] > s["ema"] else -1.0 if f["ema"] < s["ema"] else 0.0
        mom = (r["rsi"] - 50) / 50
        slope = s["slope"] or 0.0
        slope_n = max(-1.0, min(1.0, slope / (0.0005 * s["ema"])))
        msi = 100 * (trend + mom + slope_n) / 3
        th = self.params["threshold"]
        return {"msi": msi, "state": (S.BULLISH if msi >= th else S.BEARISH if msi <= -th else S.NEUTRAL).value}
