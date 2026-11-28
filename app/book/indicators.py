"""Indicators of the NIFTY options strategy book (Implementation Specification §3, 28 Nov 2026).

These are the specification's reference implementations, kept numerically identical so the live book reproduces
the backtest: vectorised over a frame of closed candles (IST, start-stamped). Only CCI's mean absolute deviation is
computed with a sliding window instead of `rolling().apply` (same arithmetic, ~100x faster).

`Wilder` smoothing = ewm(alpha=1/n, adjust=False), seeded with the first value (matches TradingView's RMA after
~3n bars of warm-up; the live book warms up on 35 sessions).
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from numpy.lib.stride_tricks import sliding_window_view


def resample(m1: pd.DataFrame, tf: int) -> pd.DataFrame:
    if tf == 1:
        return m1[["open", "high", "low", "close"]].copy()
    g = m1.groupby(m1.index.floor(f"{tf}min"))
    return pd.DataFrame({"open": g["open"].first(), "high": g["high"].max(),
                         "low": g["low"].min(), "close": g["close"].last()})


def to_base(series_by_start: pd.Series, htf: int, base_index: pd.DatetimeIndex, base_tf: int) -> np.ndarray:
    """Higher-timeframe value known at the HTF candle CLOSE, read at each base candle close (last completed)."""
    s = pd.Series(series_by_start.to_numpy(), index=series_by_start.index + pd.Timedelta(minutes=htf))
    return s.reindex(base_index + pd.Timedelta(minutes=base_tf), method="ffill").to_numpy()


def wilder(x, n: int) -> np.ndarray:
    return pd.Series(np.asarray(x, float)).ewm(alpha=1 / n, adjust=False).mean().to_numpy()


def true_range(df: pd.DataFrame) -> np.ndarray:
    h, l, c = df.high.to_numpy(), df.low.to_numpy(), df.close.to_numpy()
    pc = np.r_[c[0], c[:-1]]
    return np.maximum.reduce([h - l, np.abs(h - pc), np.abs(l - pc)])


def supertrend(df: pd.DataFrame, p: int = 10, m: float = 3.0) -> np.ndarray:
    """+1 = up (green), -1 = down (red)."""
    h, l, c = df.high.to_numpy(), df.low.to_numpy(), df.close.to_numpy()
    if not len(c):
        return np.ones(0, int)
    atr = wilder(true_range(df), p)
    hl2 = (h + l) / 2
    up, dn = (hl2 - m * atr).tolist(), (hl2 + m * atr).tolist()     # plain floats: same IEEE math, ~5x faster loop
    c = c.tolist()
    fu, fd, d = list(up), list(dn), [1] * len(c)
    for i in range(1, len(c)):
        fu[i] = max(up[i], fu[i - 1]) if c[i - 1] > fu[i - 1] else up[i]
        fd[i] = min(dn[i], fd[i - 1]) if c[i - 1] < fd[i - 1] else dn[i]
        if d[i - 1] == -1 and c[i] > fd[i - 1]:
            d[i] = 1
        elif d[i - 1] == 1 and c[i] < fu[i - 1]:
            d[i] = -1
        else:
            d[i] = d[i - 1]
    return np.asarray(d, int)


def halftrend(df: pd.DataFrame, amp: int = 2) -> np.ndarray:
    """TradingView HalfTrend (everget) direction: +1 up, -1 down."""
    h, l, c = df.high.to_numpy(), df.low.to_numpy(), df.close.to_numpy()
    if not len(c):
        return np.ones(0, int)
    hp = pd.Series(h).rolling(amp, min_periods=1).max().to_numpy()
    lp = pd.Series(l).rolling(amp, min_periods=1).min().to_numpy()
    hma = pd.Series(h).rolling(amp, min_periods=1).mean().to_numpy()
    lma = pd.Series(l).rolling(amp, min_periods=1).mean().to_numpy()
    h, l, c, hp, lp, hma, lma = (x.tolist() for x in (h, l, c, hp, lp, hma, lma))
    trend, nxt, maxlow, minhigh = 0, 0, l[0], h[0]
    out = [1] * len(c)
    for i in range(1, len(c)):
        if nxt == 1:
            maxlow = max(lp[i], maxlow)
            if hma[i] < maxlow and c[i] < l[i - 1]:
                trend, nxt, minhigh = 1, 0, hp[i]
        else:
            minhigh = min(hp[i], minhigh)
            if lma[i] > minhigh and c[i] > h[i - 1]:
                trend, nxt, maxlow = 0, 1, lp[i]
        out[i] = 1 if trend == 0 else -1
    return np.asarray(out, int)


def volstop(df: pd.DataFrame, n: int = 20, mult: float = 2.0) -> np.ndarray:
    """TradingView Volatility Stop, source close: +1 uptrend, -1 downtrend."""
    c = df.close.to_numpy()
    if not len(c):
        return np.ones(0, int)
    atr = (wilder(true_range(df), n) * mult).tolist()
    c = c.tolist()
    up, mx, mn, stop = True, c[0], c[0], c[0] - atr[0]
    out = [1] * len(c)
    for i in range(len(c)):
        mx, mn = max(mx, c[i]), min(mn, c[i])
        stop = max(stop, mx - atr[i]) if up else min(stop, mn + atr[i])
        new_up = c[i] - stop >= 0
        if new_up != up:
            up, mx, mn = new_up, c[i], c[i]
            stop = mx - atr[i] if up else mn + atr[i]
        out[i] = 1 if up else -1
    return np.asarray(out, int)


def rsi(c: pd.Series, n: int = 14) -> pd.Series:
    d = c.diff().fillna(0)
    g, l_ = wilder(d.clip(lower=0), n), wilder((-d).clip(lower=0), n)
    with np.errstate(all="ignore"):
        return pd.Series(np.nan_to_num(100 - 100 / (1 + g / l_), nan=50.0), index=c.index)


def bollinger(c: pd.Series, n: int = 20, k: float = 2.0):
    mid, sd = c.rolling(n).mean(), c.rolling(n).std(ddof=0)
    return mid + k * sd, mid, mid - k * sd


def williams_r(df: pd.DataFrame, n: int = 14) -> pd.Series:
    hh, ll = df.high.rolling(n).max(), df.low.rolling(n).min()
    return (-100 * (hh - df.close) / (hh - ll).replace(0, np.nan)).fillna(-50)


def stoch_rsi_k(c: pd.Series, n: int = 14, smooth: int = 3) -> pd.Series:
    r = rsi(c, n)
    lo, hi = r.rolling(n).min(), r.rolling(n).max()
    return (100 * (r - lo) / (hi - lo).replace(0, np.nan)).rolling(smooth).mean().fillna(50)


def _mad(tp: pd.Series, n: int) -> pd.Series:
    a = tp.to_numpy(float)
    out = np.full(len(a), np.nan)
    if len(a) >= n:
        w = sliding_window_view(a, n)
        out[n - 1:] = np.abs(w - w.mean(axis=1, keepdims=True)).mean(axis=1)
    return pd.Series(out, index=tp.index)


def cci(df: pd.DataFrame, n: int = 20) -> pd.Series:
    tp = (df.high + df.low + df.close) / 3
    sma = tp.rolling(n).mean()
    md = _mad(tp, n)
    return ((tp - sma) / (0.015 * md.replace(0, np.nan))).fillna(0)


def cmo(c: pd.Series, n: int = 14) -> pd.Series:
    d = c.diff()
    su, sd = d.clip(lower=0).rolling(n).sum(), (-d).clip(lower=0).rolling(n).sum()
    return (100 * (su - sd) / (su + sd).replace(0, np.nan)).fillna(0)


def awesome(df: pd.DataFrame) -> pd.Series:
    hl2 = (df.high + df.low) / 2
    return hl2.rolling(5).mean() - hl2.rolling(34).mean()


def adx_di(df: pd.DataFrame, n: int = 14):
    h, l = df.high, df.low
    tr = true_range(df)
    up, dn = h.diff().fillna(0).to_numpy(), -l.diff().fillna(0).to_numpy()
    atr = wilder(tr, n)
    with np.errstate(all="ignore"):
        pdi = 100 * wilder(np.where((up > dn) & (up > 0), up, 0.0), n) / atr
        ndi = 100 * wilder(np.where((dn > up) & (dn > 0), dn, 0.0), n) / atr
        adx = np.nan_to_num(wilder(np.nan_to_num(100 * np.abs(pdi - ndi) / (pdi + ndi)), n))
    return adx, pdi, ndi


def session_vwap_index(df: pd.DataFrame) -> pd.Series:
    """Index has no volume: equal-weight cumulative mean of typical price since 09:15."""
    tp = (df.high + df.low + df.close) / 3
    day = df.index.normalize()
    return tp.groupby(day).cumsum() / tp.groupby(day).cumcount().add(1)


def prev_day_pivots(df: pd.DataFrame):
    day = df.index.normalize()
    dd = df.groupby(day).agg(h=("high", "max"), l=("low", "min"), c=("close", "last"))
    p = (dd.h + dd.l + dd.c) / 3
    r1 = (2 * p - dd.l).shift(1).reindex(day).to_numpy()   # yesterday's values, mapped to today
    s1 = (2 * p - dd.h).shift(1).reindex(day).to_numpy()
    return r1, s1


def opening_range(df: pd.DataFrame):
    mins = df.index.hour * 60 + df.index.minute
    day = df.index.normalize()
    w = df[(mins >= 555) & (mins < 585)]                     # 09:15 .. 09:44
    orh = w.groupby(w.index.normalize()).high.max().reindex(day).to_numpy()
    orl = w.groupby(w.index.normalize()).low.min().reindex(day).to_numpy()
    return orh, orl, np.asarray(mins >= 585)                  # usable from 09:45
