"""The strategies of the NIFTY options strategy book: the base ten (Implementation Specification §5, §6) and
the parallel set N1-N8 (N1-N8 Implementation Specification §3, §5).

Each strategy has a stable machine id (used in the database), a plain-English name for people, and its code in the
specification (U1, PB, Q1..Q8, N1..N8) for traceability. Signals are computed on CLOSED candles of the NIFTY index chart;
only the option is traded. `chart_signals` evaluates every strategy of one timeframe over a frame of 1-minute index
bars and returns per-candle arrays; the book engine reads the candle that just closed.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from app.book import indicators as I
from app.book.nconditions import n_condition


@dataclass(frozen=True)
class StrategyDef:
    code: str                  # specification code
    sid: str                   # machine id (database strategy_id)
    name: str                  # human name
    tf: int                    # chart minutes
    side: str                  # CE | PE
    rule: str                  # "fresh" (U1, PB: first rising-edge signal, retried) | "first" (Q: first candle only)
    target: float              # premium target, INR
    band: tuple[float, float]  # accepted entry premium
    scan: tuple[int, int]      # m range (inclusive): calls ATM + 50m, puts ATM - 50m
    exit: str                  # X1 | E5 | X2
    flip: tuple[int, float]    # Supertrend (period, multiplier) on the strategy's chart for the flip exit
    summary: str = ""
    fees: str = "book"         # cost schedule: "book" (base specification) | "n" (N1-N8 specification, dated STT)
    family: str = "base"       # "base" (U1, PB, Q1-Q8) | "parallel" (N1-N8)

    @property
    def direction(self) -> int:
        return 1 if self.side == "CE" else -1


def _q(code, sid, name, tf, side, target, exit_, summary):
    return StrategyDef(code, sid, name, tf, side, "first", target, (round(0.6 * target, 2), round(1.4 * target, 2)),
                       (-10, 6), exit_, (10, 3.0), summary)


STRATEGIES: dict[str, StrategyDef] = {s.code: s for s in [
    StrategyDef("U1", "call_pivot_breakout", "Call · Pivot Breakout", 3, "CE", "fresh", 250, (150, 350), (-9, 4),
                "X1", (10, 3.0), "BB(20,3.5) breakout + Supertrend up + RSI>70 + W%R>-20 above yesterday's R1; "
                                 "skipped if NIFTY rose >0.91% in 30 min"),
    StrategyDef("PB", "put_or_breakdown", "Put · Opening-Range Breakdown", 3, "PE", "fresh", 250, (150, 350), (-9, 4),
                "E5", (14, 3.0), "BB(20,2.5) breakdown + Supertrend(14,3) down + RSI<25 + W%R<-90 below the opening-"
                                 "range low and EMA200; only when the day did not gap up more than 0.14%"),
    _q("Q1", "call_momentum_surge", "Call · Momentum Surge", 3, "CE", 200, "X1",
       "BB(20,3.0) breakout + ADX + 5-min HalfTrend up + CCI>200 + CMO>50, close near EMA20"),
    _q("Q2", "call_afternoon_trend_ride", "Call · Afternoon Trend Ride", 3, "CE", 250, "E5",
       "BB(20,3.0) breakout + ADX + 5-min HalfTrend up + rising AO + momentum, from 12:00"),
    _q("Q3", "call_afternoon_vwap_burst", "Call · Afternoon VWAP Burst", 5, "CE", 150, "X2",
       "5-min BB(20,3.0) breakout above VWAP + ADX + W%R(50)>-20 + CCI>200, from 12:00"),
    _q("Q4", "call_vwap_breakout", "Call · VWAP Breakout", 3, "CE", 250, "X1",
       "BB(20,3.5) breakout above VWAP + ADX + RSI>60, close near EMA20"),
    _q("Q5", "put_morning_vwap_flush", "Put · Morning VWAP Flush", 1, "PE", 250, "X1",
       "1-min BB(20,3.5) breakdown below VWAP + 5-min Volatility Stop down + StochRSI<20 + 5-min %B<0.2, "
       "before 12:00"),
    _q("Q6", "put_afternoon_slide", "Put · Afternoon Slide", 1, "PE", 250, "E5",
       "1-min BB(20,3.5) breakdown + 3-min Supertrend down + 5-min HalfTrend down + StochRSI<20 + momentum, "
       "from 12:00"),
    _q("Q7", "put_trend_breakdown", "Put · Trend Breakdown", 5, "PE", 250, "X1",
       "5-min BB(20,3.0) breakdown + ADX + 3-min Supertrend down + W%R(50)<-80 + CMO<-50, close near EMA20"),
    _q("Q8", "call_afternoon_scalp", "Call · Afternoon Scalp", 1, "CE", 150, "X1",
       "1-min BB(20,3.5) breakout + HalfTrend up + 5-min Supertrend up + rising AO + 5-min %B>0.8, from 12:00"),
]}



def _n(code, sid, name, tf, side, target, exit_, summary):
    return StrategyDef(code, sid, name, tf, side, "first", target, (round(0.6 * target, 2), round(1.4 * target, 2)),
                       (-10, 6), exit_, (10, 3.0), summary, fees="n", family="parallel")


# Parallel strategies N1-N8 (N specification §5): paper forward test only (the spec does not recommend live money)
for _s in [
    _n("N1", "call_sar_volatility_burst", "Call · SAR Flip Volatility Burst", 3, "CE", 150, "X2",
       "3-min Parabolic SAR flips up + Ichimoku bullish + 5-min HMA rising + KST and RVI above signal + ATR5/ATR20>1.2"),
    _n("N2", "put_sar_volatility_drop", "Put · SAR Flip Volatility Drop", 5, "PE", 250, "X1",
       "5-min Parabolic SAR flips down + Ichimoku bearish + two red 3-min Heikin-Ashi + UO<40 + RVI below signal "
       "+ ATR5/ATR20>1.2"),
    _n("N3", "call_camarilla_breakout", "Call · Camarilla H4 Breakout", 5, "CE", 150, "E5",
       "5-min close crosses above yesterday's Camarilla H4 + EMA 8>21>55 + two green Heikin-Ashi + UO>60 + RVI "
       "above signal + ATR5/ATR20>1.2"),
    _n("N4", "put_sar_trend_slide", "Put · SAR Flip Trend Slide", 3, "PE", 150, "X1",
       "3-min Parabolic SAR flips down + Ichimoku bearish + 5-min HMA falling + UO<40 + squeeze momentum falling "
       "+ trending market (CHOP<38.2)"),
    _n("N5", "put_fisher_trend_drop", "Put · Fisher Trend Drop", 5, "PE", 200, "E5",
       "5-min Parabolic SAR flips down + KAMA falling + two red 3-min Heikin-Ashi + Fisher falling below 0 + RVI "
       "below signal + trending market (CHOP<38.2)"),
    _n("N6", "call_late_or_breakout", "Call · Late Opening-Range Breakout", 5, "CE", 200, "X1",
       "5-min close crosses above the 09:15-09:30 high + Ichimoku bullish + EMA 8>21>55 + TRIX rising + UO>60, "
       "from 13:30"),
    _n("N7", "call_late_sar_flip", "Call · Late SAR Flip", 5, "CE", 250, "E5",
       "5-min Parabolic SAR flips up + HMA rising + EMA 8>21>55 + Fisher rising above 0 + UO>60, from 13:30"),
    _n("N8", "call_early_sar_flip", "Call · Early SAR Flip", 3, "CE", 150, "E5",
       "3-min Parabolic SAR flips up + 50-bar regression slope up + two green Heikin-Ashi + RVI above signal + "
       "squeeze momentum rising, before 10:15"),
]:
    STRATEGIES[_s.code] = _s

# N conditions (N specification §5): trigger + trend 1, trend 2, momentum 1, momentum 2, context
N_RULES = {
    "N1": ("psarf", ["ichi", "hma5m", "kst", "rvi", "atrx"]),
    "N2": ("psarf", ["ichi", "ha3m", "uo", "rvi", "atrx"]),
    "N3": ("cam4", ["emastack", "ha", "uo", "rvi", "atrx"]),
    "N4": ("psarf", ["ichi", "hma5m", "uo", "sqzmom", "chop38"]),
    "N5": ("psarf", ["kama", "ha3m", "fisher", "rvi", "chop38"]),
    "N6": ("orb15", ["ichi", "emastack", "trix", "uo", "late"]),
    "N7": ("psarf", ["hma", "emastack", "fisher", "uo", "late"]),
    "N8": ("psarf", ["lrs", "ha", "rvi", "sqzmom", "h1"]),
}

# Q conditions (spec §6.2): trigger + list of condition names
Q_RULES = {
    "Q1": ("bb30", ["adx", "ht5m", "cci200", "cmo", "ema20near"]),
    "Q2": ("bb30", ["adx", "ht5m", "ao", "mom10", "pm"]),
    "Q3": ("bb30", ["vwap", "adx", "wr50", "cci200", "pm"]),
    "Q4": ("bb35", ["vwap", "adx", "rsi60", "ema20near"]),
    "Q5": ("bb35", ["vwap", "vst5m", "srsi", "pctb5", "am"]),
    "Q6": ("bb35", ["st3m", "ht5m", "srsi", "mom10", "pm"]),
    "Q7": ("bb30", ["adx", "st3m", "wr50", "cmo", "ema20near"]),
    "Q8": ("bb35", ["ht", "st5m", "ao", "pctb5", "pm"]),
}

SIGNAL_FIRST_MIN, SIGNAL_LAST_MIN = 555, 900      # candle START 09:15 .. 15:00


def by_code_or_id(x: str) -> StrategyDef:
    if x in STRATEGIES:
        return STRATEGIES[x]
    for s in STRATEGIES.values():
        if s.sid == x:
            return s
    raise KeyError(x)


def _mins(idx: pd.DatetimeIndex) -> np.ndarray:
    return np.asarray(idx.hour * 60 + idx.minute)


def _q_condition(name: str, m1: pd.DataFrame, df: pd.DataFrame, tf: int, cache: dict):
    """(call_bool, put_bool) arrays for one condition on the tf chart (spec q_conditions, computed lazily)."""
    if name in cache:
        return cache[name]
    c, idx = df.close, df.index
    B = lambda a, b: (np.asarray(a, bool), np.asarray(b, bool))     # noqa: E731
    if name in ("bb30", "bb35"):
        mid, sd = cache.setdefault("_bb", (c.rolling(20).mean(), c.rolling(20).std(ddof=0)))
        k = 3.0 if name == "bb30" else 3.5
        out = B(c > mid + k * sd, c < mid - k * sd)
    elif name == "ht":
        ht = I.halftrend(df)
        out = B(ht == 1, ht == -1)
    elif name == "vwap":
        vw = I.session_vwap_index(df)
        out = B(c > vw, c < vw)
    elif name == "adx":
        adx, pdi, ndi = I.adx_di(df, 14)
        out = B((adx > 25) & (pdi > ndi), (adx > 25) & (ndi > pdi))
    elif name in ("st3m", "st5m", "vst5m", "ht5m"):
        htf = 3 if name == "st3m" else 5
        fn = {"st3m": lambda d: I.supertrend(d, 10, 3.0), "st5m": lambda d: I.supertrend(d, 10, 3.0),
              "vst5m": I.volstop, "ht5m": I.halftrend}[name]
        hd = I.resample(m1, htf)
        v = I.to_base(pd.Series(fn(hd), index=hd.index), htf, idx, tf)
        out = B(v == 1, v == -1)
    elif name == "rsi60":
        r = I.rsi(c)
        out = B(r > 60, r < 40)
    elif name == "srsi":
        k = I.stoch_rsi_k(c)
        out = B(k > 80, k < 20)
    elif name == "wr50":
        w = I.williams_r(df, 50)
        out = B(w > -20, w < -80)
    elif name == "cci200":
        cc = I.cci(df, 20)
        out = B(cc > 200, cc < -200)
    elif name == "cmo":
        cm = I.cmo(c, 14)
        out = B(cm > 50, cm < -50)
    elif name == "ao":
        ao = I.awesome(df)
        out = B((ao > 0) & (ao > ao.shift(1)), (ao < 0) & (ao < ao.shift(1)))
    elif name == "mom10":
        m10 = (c / c.shift(10) - 1) * 100
        th = 0.1 * np.sqrt(tf)
        out = B(m10 > th, m10 < -th)
    elif name == "pctb5":
        d5 = I.resample(m1, 5)
        m5, s5 = d5.close.rolling(20).mean(), d5.close.rolling(20).std(ddof=0)
        pb = I.to_base((d5.close - (m5 - 2 * s5)) / (4 * s5).replace(0, np.nan), 5, idx, tf)
        out = B(pb > 0.8, pb < 0.2)
    elif name == "ema20near":
        de = (c / c.ewm(span=20, adjust=False).mean() - 1) * 100
        out = B(de <= 0.4, de >= -0.4)
    elif name in ("am", "pm"):
        mins = _mins(idx)
        out = B(mins < 720, mins < 720) if name == "am" else B(mins >= 720, mins >= 720)
    else:
        raise KeyError(name)
    cache[name] = out
    return out


def chart_signals(m1: pd.DataFrame, tf: int, codes: list[str]) -> tuple[pd.DataFrame, dict]:
    """Evaluate the strategies `codes` (all on chart `tf`) over 1-minute index bars `m1` (IST index, start-stamped).

    Returns (chart candles, {code: {"signal": bool[], "blocked": bool[] | None, "st": int[], "detail": {...}}}):
      signal  = the candle is a tradeable signal (Q: all conditions; U1/PB: fresh rising edge) inside 09:15-15:00
      blocked = U1's ROC filter vetoed this fresh signal (later fresh signals may still trade)
      st      = the strategy's flip-exit Supertrend direction per candle
    """
    df = I.resample(m1, tf)
    out: dict[str, dict] = {}
    if df.empty:
        return df, out
    c = df.close
    mins = _mins(df.index)
    window = (mins >= SIGNAL_FIRST_MIN) & (mins <= SIGNAL_LAST_MIN)
    cache: dict = {}
    st_cache: dict = {}

    def st(p, m):
        if (p, m) not in st_cache:
            st_cache[(p, m)] = I.supertrend(df, p, m)
        return st_cache[(p, m)]

    for code in codes:
        sd = STRATEGIES[code]
        if code == "U1":
            up35, _, _ = I.bollinger(c, 20, 3.5)
            s = st(10, 3.0)
            r = I.rsi(c, 14)
            wr = I.williams_r(df, 14)
            r1, _ = I.prev_day_pivots(df)
            k = (c > up35).to_numpy() & (s == 1) & (r > 70).to_numpy() & (wr > -20).to_numpy() & (c.to_numpy() > r1)
            fresh = k & ~np.r_[False, k[:-1]]
            roc10 = ((c / c.shift(10) - 1) * 100).to_numpy()
            out[code] = {"signal": fresh & window, "blocked": roc10 > 0.91, "st": st(*sd.flip),
                         "detail": {"roc10": roc10, "r1": r1, "rsi": r.to_numpy(), "wr": wr.to_numpy()}}
        elif code == "PB":
            _, _, lo25 = I.bollinger(c, 20, 2.5)
            s14 = st(14, 3.0)
            r = I.rsi(c, 14)
            wr = I.williams_r(df, 14)
            _, orl, after = I.opening_range(df)
            e200 = c.ewm(span=200, adjust=False).mean()
            day = df.index.normalize()
            dd = df.groupby(day).agg(o=("open", "first"), c=("close", "last"))
            gap = ((dd.o / dd.c.shift(1) - 1) * 100).reindex(day).to_numpy()
            k = ((c < lo25).to_numpy() & (s14 == -1) & (r < 25).to_numpy() & (wr < -90).to_numpy()
                 & after & (c.to_numpy() < orl) & (c < e200).to_numpy() & (gap <= 0.14))
            fresh = k & ~np.r_[False, k[:-1]]
            out[code] = {"signal": fresh & (mins <= SIGNAL_LAST_MIN), "blocked": None, "st": st(*sd.flip),
                         "detail": {"gap_pct": gap, "orl": orl, "rsi": r.to_numpy(), "wr": wr.to_numpy()}}
        elif code in N_RULES:
            trig, conds = N_RULES[code]
            si = 0 if sd.side == "CE" else 1
            ok = n_condition(trig, m1, df, tf, cache)[si] & window
            for cnd in conds:
                ok = ok & n_condition(cnd, m1, df, tf, cache)[si]
            out[code] = {"signal": ok, "blocked": None, "st": st(*sd.flip), "detail": {}}
        else:
            trig, conds = Q_RULES[code]
            si = 0 if sd.side == "CE" else 1
            ok = _q_condition(trig, m1, df, tf, cache)[si] & window
            for cnd in conds:
                ok = ok & _q_condition(cnd, m1, df, tf, cache)[si]
            out[code] = {"signal": ok, "blocked": None, "st": st(*sd.flip), "detail": {}}
    return df, out


def flip_close_ts(df: pd.DataFrame, st_dir: np.ndarray, side: str, tf: int, after_ts: pd.Timestamp):
    """Close time of the first candle closing after `after_ts` where the Supertrend turns against the trade."""
    against = -1 if side == "CE" else 1
    if len(st_dir) < 2:
        return None
    chg = np.r_[False, (st_dir[1:] != st_dir[:-1]) & (st_dir[1:] == against)]
    closes = df.index[chg] + pd.Timedelta(minutes=tf)
    later = closes[closes > after_ts]
    return later[0] if len(later) else None
