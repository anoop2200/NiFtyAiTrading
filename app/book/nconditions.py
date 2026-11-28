"""Conditions of the parallel strategies N1-N8 ("NIFTY Parallel Strategies N1-N8 - Implementation Specification" §3).

Each condition returns (call_bool, put_bool) arrays over the candles of the strategy's chart; it is computed lazily
and cached per chart evaluation. The formulas follow the specification's reference `features()` line by line
(continuous history, EMAs adjust=False, Wilder smoothing alpha 1/n); the only change is that the weighted moving
average is a convolution instead of a rolling apply (same values, much faster). Every condition is causal: a
candle's value depends only on that candle and earlier ones, and the 3m/5m filters use completed candles only.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from app.book import indicators as I


def _both(a, b):
    f = lambda x: np.nan_to_num(np.asarray(x, float)).astype(bool)      # noqa: E731
    return f(a), f(b)


def linreg_end(y, n: int):
    """TradingView linreg(y, n, 0) and its slope."""
    y = np.asarray(y, float)
    x = np.arange(n, dtype=float)
    xm, sxx = x.mean(), ((x - x.mean()) ** 2).sum()
    ym = pd.Series(y).rolling(n).mean().to_numpy()
    sxy = np.full(len(y), np.nan)
    if len(y) >= n:
        sxy[n - 1:] = np.convolve(np.nan_to_num(y), x[::-1], mode="valid")
    slope = (sxy - n * xm * ym) / sxx
    return ym + slope * (n - 1 - xm), slope


def wma(s: pd.Series, n: int) -> pd.Series:
    """Linearly weighted MA (weights 1..n, newest heaviest); NaN until n values, NaN-propagating like rolling."""
    v = s.to_numpy(float)
    w = np.arange(1, n + 1, dtype=float)
    out = np.full(len(v), np.nan)
    if len(v) >= n:
        out[n - 1:] = np.convolve(v, w[::-1], mode="valid") / w.sum()     # newest bar gets weight n
    return pd.Series(out, index=s.index)


def hma(c: pd.Series, n: int = 21) -> pd.Series:
    return wma(2 * wma(c, n // 2) - wma(c, n), int(np.sqrt(n)))


def psar(df: pd.DataFrame, step: float = 0.02, mx: float = 0.2) -> np.ndarray:   # +1 bullish, -1 bearish
    h, l_ = df.high.to_numpy().tolist(), df.low.to_numpy().tolist()
    out = [1] * len(h)
    if not h:
        return np.asarray(out, int)
    bull, af, ep, sar = True, step, h[0], l_[0]
    for i in range(1, len(h)):
        sar = sar + af * (ep - sar)
        if bull:
            sar = min(sar, l_[i - 1], l_[i - 2] if i > 1 else l_[i - 1])
            if l_[i] < sar:
                bull, sar, ep, af = False, ep, l_[i], step
            elif h[i] > ep:
                ep, af = h[i], min(af + step, mx)
        else:
            sar = max(sar, h[i - 1], h[i - 2] if i > 1 else h[i - 1])
            if h[i] > sar:
                bull, sar, ep, af = True, ep, h[i], step
            elif l_[i] < ep:
                ep, af = l_[i], min(af + step, mx)
        out[i] = 1 if bull else -1
    return np.asarray(out, int)


def heikin(df: pd.DataFrame, need: int = 2) -> np.ndarray:   # +1 last `need` HA candles green, -1 red, 0 mixed
    o, h, l_, c = (df[k].to_numpy() for k in ("open", "high", "low", "close"))
    if not len(c):
        return np.zeros(0, int)
    hc = (o + h + l_ + c) / 4
    hcl = hc.tolist()
    ho = [0.0] * len(c)
    ho[0] = (o[0] + c[0]) / 2
    for i in range(1, len(c)):
        ho[i] = (ho[i - 1] + hcl[i - 1]) / 2
    ho = np.asarray(ho)
    up = pd.Series((hc > ho).astype(int)).rolling(need).sum().to_numpy() == need
    dn = pd.Series((hc < ho).astype(int)).rolling(need).sum().to_numpy() == need
    return np.where(up, 1, np.where(dn, -1, 0))


def kama(c: pd.Series, n: int = 10, fast: int = 2, slow: int = 30) -> pd.Series:
    v = c.to_numpy(float)
    if not len(v):
        return pd.Series(dtype=float)
    ch = np.abs(v - np.r_[np.full(n, np.nan), v][: len(v)])
    vol = pd.Series(np.abs(np.diff(v, prepend=v[0]))).rolling(n).sum().to_numpy()
    with np.errstate(all="ignore"):
        er = np.nan_to_num(ch / vol)
    sc = ((er * (2 / (fast + 1) - 2 / (slow + 1)) + 2 / (slow + 1)) ** 2).tolist()
    vl = v.tolist()
    k = [0.0] * len(vl)
    k[0] = vl[0]
    for i in range(1, len(vl)):
        k[i] = k[i - 1] + sc[i] * (vl[i] - k[i - 1])
    return pd.Series(k, index=c.index)


def ichimoku(df: pd.DataFrame):
    h, l_ = df.high, df.low
    ten = (h.rolling(9).max() + l_.rolling(9).min()) / 2
    kij = (h.rolling(26).max() + l_.rolling(26).min()) / 2
    sa = ((ten + kij) / 2).shift(26)
    sb = ((h.rolling(52).max() + l_.rolling(52).min()) / 2).shift(26)
    return ten, kij, np.maximum(sa, sb), np.minimum(sa, sb)


def fisher(df: pd.DataFrame, n: int = 9) -> np.ndarray:
    hl2 = ((df.high + df.low) / 2).to_numpy()
    hh = pd.Series(hl2).rolling(n).max().to_numpy().tolist()
    ll = pd.Series(hl2).rolling(n).min().to_numpy().tolist()
    hl = hl2.tolist()
    fv, fish = 0.0, [0.0] * len(hl)
    for i in range(len(hl)):
        den = hh[i] - ll[i]
        raw = 0.0 if not den > 0 else 0.66 * ((hl[i] - ll[i]) / den - 0.5) + 0.67 * fv
        fv = min(max(raw, -0.999), 0.999)
        fish[i] = 0.5 * float(np.log((1 + fv) / (1 - fv))) + 0.5 * (fish[i - 1] if i else 0.0)
    return np.asarray(fish)


def n_condition(name: str, m1: pd.DataFrame, df: pd.DataFrame, tf: int, cache: dict):
    """(call_bool, put_bool) for one N condition on the tf chart `df` (built from 1-minute bars `m1`)."""
    key = "n:" + name
    if key in cache:
        return cache[key]
    c, h, l_, o = df.close, df.high, df.low, df.open
    idx = df.index
    day = idx.normalize()
    mins = np.asarray(idx.hour * 60 + idx.minute)
    cv, pcv = c.to_numpy(), c.shift(1).to_numpy()
    pc = c.shift(1)
    with np.errstate(all="ignore"):
        if name == "psarf":
            ps = psar(df)
            pp = np.r_[ps[:1], ps[:-1]]
            out = _both((ps == 1) & (pp == -1), (ps == -1) & (pp == 1))
        elif name == "cam4":
            dd = df.groupby(day).agg(h=("high", "max"), l=("low", "min"), c=("close", "last"))
            rng = dd.h - dd.l
            h4 = (dd.c + 1.1 * rng / 2).shift(1).reindex(day).to_numpy()
            l4 = (dd.c - 1.1 * rng / 2).shift(1).reindex(day).to_numpy()
            out = _both((cv > h4) & ~(pcv > h4), (cv < l4) & ~(pcv < l4))
        elif name == "orb15":
            orw = df[(mins >= 555) & (mins < 570)]
            orh = orw.groupby(orw.index.normalize()).high.max().reindex(day).to_numpy()
            orl = orw.groupby(orw.index.normalize()).low.min().reindex(day).to_numpy()
            aft = mins >= 570
            out = _both(aft & (cv > orh) & ~(pcv > orh), aft & (cv < orl) & ~(pcv < orl))
        elif name == "ichi":
            ten, kij, ctop, cbot = ichimoku(df)
            out = _both((c > ctop) & (ten > kij), (c < cbot) & (ten < kij))
        elif name == "hma":
            hm = hma(c, 21)
            out = _both(hm > hm.shift(1), hm < hm.shift(1))
        elif name == "kama":
            km = kama(c)
            out = _both(km > km.shift(1), km < km.shift(1))
        elif name == "lrs":
            _, sl50 = linreg_end(cv, 50)
            out = _both(sl50 > 0, sl50 < 0)
        elif name == "emastack":
            e8, e21, e55 = (c.ewm(span=s, adjust=False).mean() for s in (8, 21, 55))
            out = _both((e8 > e21) & (e21 > e55), (e8 < e21) & (e21 < e55))
        elif name == "ha":
            ha = heikin(df)
            out = _both(ha == 1, ha == -1)
        elif name in ("ha3m", "hma5m"):
            htf = 3 if name == "ha3m" else 5
            fn = heikin if name == "ha3m" else \
                (lambda d: np.sign(hma(d.close, 21).diff().fillna(0).to_numpy()).astype(int))
            hd = I.resample(m1, htf)
            v = I.to_base(pd.Series(fn(hd), index=hd.index), htf, idx, tf)
            out = _both(v == 1, v == -1)
        elif name == "trix":
            e3 = c.ewm(span=15, adjust=False).mean().ewm(span=15, adjust=False).mean().ewm(span=15, adjust=False).mean()
            trix = e3.pct_change() * 10000
            out = _both((trix > 0) & (trix > trix.shift(1)), (trix < 0) & (trix < trix.shift(1)))
        elif name == "fisher":
            fish = fisher(df)
            fp = np.r_[0.0, fish[:-1]]
            out = _both((fish > 0) & (fish > fp), (fish < 0) & (fish < fp))
        elif name == "uo":
            mn = np.minimum(l_, pc.fillna(l_))
            bp = c - mn
            trr = np.maximum(h, pc.fillna(h)) - mn
            av = lambda n: bp.rolling(n).sum() / trr.rolling(n).sum().replace(0, np.nan)   # noqa: E731
            uo = 100 * (4 * av(7) + 2 * av(14) + av(28)) / 7
            out = _both(uo > 60, uo < 40)
        elif name == "kst":
            roc = lambda n: (c / c.shift(n) - 1) * 100                                        # noqa: E731
            kst = (roc(10).rolling(10).mean() + 2 * roc(15).rolling(10).mean() + 3 * roc(20).rolling(10).mean()
                   + 4 * roc(30).rolling(15).mean())
            ksig = kst.rolling(9).mean()
            out = _both((kst > 0) & (kst > ksig), (kst < 0) & (kst < ksig))
        elif name == "rvi":
            sw = lambda x: (x + 2 * x.shift(1) + 2 * x.shift(2) + x.shift(3)) / 6                # noqa: E731
            rvi = sw(c - o).rolling(10).sum() / sw(h - l_).rolling(10).sum().replace(0, np.nan)
            rsg = sw(rvi)
            out = _both((rvi > 0) & (rvi > rsg), (rvi < 0) & (rvi < rsg))
        elif name == "sqzmom":
            mid = c.rolling(20).mean()
            dmid = ((h.rolling(20).max() + l_.rolling(20).min()) / 2 + mid) / 2
            sm = pd.Series(linreg_end((c - dmid).to_numpy(), 20)[0], index=idx)
            out = _both((sm > 0) & (sm > sm.shift(1)), (sm < 0) & (sm < sm.shift(1)))
        elif name == "chop38":
            tr = pd.Series(I.true_range(df), index=idx)
            chop = 100 * np.log10(tr.rolling(14).sum() / (h.rolling(14).max() - l_.rolling(14).min())
                                  .replace(0, np.nan)) / np.log10(14)
            out = _both(chop < 38.2, chop < 38.2)
        elif name == "atrx":
            tr = I.true_range(df)
            ax = I.wilder(tr, 5) / I.wilder(tr, 20)
            out = _both(ax > 1.2, ax > 1.2)
        elif name == "h1":
            out = _both(mins < 615, mins < 615)
        elif name == "late":
            out = _both(mins >= 810, mins >= 810)
        else:
            raise KeyError(name)
    cache[key] = out
    return out
