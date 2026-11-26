"""Single pricing / Greeks implementation for history and live (v1.3 §4.4).

Model: Black-Scholes-Merton on the index spot with continuous risk-free rate r and dividend yield q,
European exercise (NIFTY index options). Assumptions are versioned in ASSUMPTIONS and stored with every
computed value; they are a reproducible baseline, not a market estimate, and must be stress-tested.

Time to expiry: calendar seconds from the observation to expiry at 15:30 IST, / (365 * 86400).
IV inversion uses the observed premium: mid only when both sides are valid and not crossed, else
LTP (flagged). Illiquid / crossed / intrinsic-violating quotes return None (invalid), never a forced value.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime, time

from app.clock import IST

EXPIRY_TIME = time(15, 30)
SECONDS_PER_YEAR = 365 * 86400


@dataclass(frozen=True)
class Assumptions:
    version: str = "bsm-1"
    r: float = 0.065        # flat annual risk-free rate (approx. 91-day T-bill); versioned assumption
    q: float = 0.012        # flat annual dividend yield of NIFTY 50; versioned assumption
    min_t_seconds: int = 60  # below this the model is not evaluated
    iv_lo: float = 0.01
    iv_hi: float = 3.0


ASSUMPTIONS = Assumptions()


def year_fraction(obs: datetime, expiry: date) -> float:
    exp = datetime.combine(expiry, EXPIRY_TIME, IST)
    return (exp - obs.astimezone(IST)).total_seconds() / SECONDS_PER_YEAR


def _ncdf(x: float) -> float:
    return 0.5 * math.erfc(-x / math.sqrt(2.0))


def _npdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def price(s: float, k: float, t: float, sigma: float, cp: str, a: Assumptions = ASSUMPTIONS) -> float:
    if t <= 0 or sigma <= 0:
        return max(0.0, (s - k) if cp == "CE" else (k - s))
    st = sigma * math.sqrt(t)
    d1 = (math.log(s / k) + (a.r - a.q + 0.5 * sigma * sigma) * t) / st
    d2 = d1 - st
    df_r, df_q = math.exp(-a.r * t), math.exp(-a.q * t)
    if cp == "CE":
        return s * df_q * _ncdf(d1) - k * df_r * _ncdf(d2)
    return k * df_r * _ncdf(-d2) - s * df_q * _ncdf(-d1)


def greeks(s: float, k: float, t: float, sigma: float, cp: str, a: Assumptions = ASSUMPTIONS) -> dict:
    """delta, gamma, theta (per calendar day), vega (per 1 vol point)."""
    st = sigma * math.sqrt(t)
    d1 = (math.log(s / k) + (a.r - a.q + 0.5 * sigma * sigma) * t) / st
    d2 = d1 - st
    df_r, df_q = math.exp(-a.r * t), math.exp(-a.q * t)
    gamma = df_q * _npdf(d1) / (s * st)
    vega = s * df_q * _npdf(d1) * math.sqrt(t) / 100
    common = -s * df_q * _npdf(d1) * sigma / (2 * math.sqrt(t))
    if cp == "CE":
        delta = df_q * _ncdf(d1)
        theta = common - a.r * k * df_r * _ncdf(d2) + a.q * s * df_q * _ncdf(d1)
    else:
        delta = -df_q * _ncdf(-d1)
        theta = common + a.r * k * df_r * _ncdf(-d2) - a.q * s * df_q * _ncdf(-d1)
    return {"delta": delta, "gamma": gamma, "theta": theta / 365, "vega": vega}


def implied_vol(premium: float, s: float, k: float, t: float, cp: str,
                a: Assumptions = ASSUMPTIONS, tol: float = 1e-6) -> float | None:
    """Bisection on [iv_lo, iv_hi] (monotone in sigma). None when no-arbitrage bounds are violated."""
    if not (premium and premium > 0 and s > 0 and k > 0) or t * SECONDS_PER_YEAR < a.min_t_seconds:
        return None
    lo_px, hi_px = price(s, k, t, a.iv_lo, cp, a), price(s, k, t, a.iv_hi, cp, a)
    if not (lo_px - 1e-9 <= premium <= hi_px + 1e-9):
        return None
    lo, hi = a.iv_lo, a.iv_hi
    for _ in range(100):
        mid = 0.5 * (lo + hi)
        if price(s, k, t, mid, cp, a) > premium:
            hi = mid
        else:
            lo = mid
        if hi - lo < tol:
            break
    return 0.5 * (lo + hi)


def premium_for_iv(ltp: float | None, bid: float | None, ask: float | None) -> tuple[float | None, str]:
    """Price side used for IV: mid only with a valid, uncrossed book; else LTP (flagged)."""
    if bid and ask and bid > 0 and ask > 0 and ask >= bid:
        return (bid + ask) / 2, "mid"
    if ltp and ltp > 0:
        return ltp, "ltp"
    return None, "none"
