"""Deterministic contract eligibility and ranking (v1.3 §6 step 5, §7.3, §9.6).

Universe: CE for bullish / PE for bearish candidates, ATM +/- 5 strikes, nearest expiry; on an expiry day
both the 0DTE and the next expiry are eligible. Eligibility uses executable prices (ask for entry, bid for
exit feasibility); LTP is never used. Rank = weighted sum of |delta - 0.5|, spread % of mid and theta burn
(% of premium per hour); ties broken by strike distance then contract key. Every evaluated contract and the
reason it was rejected is returned for the audit."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime

from app.backtest.fills import NS, Quote
from app.clock import IST
from app.pricing.bs import greeks, implied_vol, year_fraction


@dataclass(frozen=True)
class SelectionConfig:
    strikes_each_side: int = 5
    strike_step: float = 50.0
    min_premium: float = 30.0
    max_quote_age_s: float = 30.0
    max_spread_ticks: int = 2
    max_spread_pct_of_mid: float = 1.0
    tick: float = 0.05
    delta_range: tuple[float, float] = (0.35, 0.65)
    w_delta: float = 1.0
    w_spread: float = 0.5
    w_theta: float = 0.2
    version: str = "rank-1"


@dataclass
class Selection:
    key: str | None
    dte: int | None = None
    quote: Quote | None = None
    features: dict = field(default_factory=dict)
    evaluated: list[dict] = field(default_factory=list)
    reason: str | None = None


def parse(key: str) -> tuple[date, float, str]:
    _, exp, k, ot = key.split("|")
    return date.fromisoformat(exp), float(k), ot


def select_contract(direction: int, spot: float, now_ns: int, quotes: dict[str, Quote], cal,
                    cfg: SelectionConfig = SelectionConfig()) -> Selection:
    now = datetime.fromtimestamp(now_ns / NS, IST)
    today = now.date()
    cp = "CE" if direction > 0 else "PE"
    expiries = sorted({parse(k)[0] for k in quotes if parse(k)[0] >= today})
    if not expiries:
        return Selection(None, reason="NO_LIVE_EXPIRY")
    allowed = expiries[:2] if expiries[0] == today else expiries[:1]
    atm = round(spot / cfg.strike_step) * cfg.strike_step
    lo, hi = atm - cfg.strikes_each_side * cfg.strike_step, atm + cfg.strikes_each_side * cfg.strike_step
    out = Selection(None)
    best = None
    for key, q in sorted(quotes.items()):
        exp, k, ot = parse(key)
        if ot != cp or exp not in allowed or not (lo <= k <= hi):
            continue
        row = {"key": key, "expiry": exp.isoformat(), "strike": k, "bid": q.bid, "ask": q.ask}
        age = (now_ns - q.ts_ns) / NS
        reason = None
        if age > cfg.max_quote_age_s or age < 0:
            reason = "QUOTE_STALE"
        elif not (q.bid and q.ask) or q.bid <= 0 or q.ask <= 0 or q.ask < q.bid or q.bids[0][1] <= 0 or q.asks[0][1] <= 0:
            reason = "QUOTE_INVALID"
        elif q.ask < cfg.min_premium:
            reason = "BELOW_MIN_PREMIUM"
        else:
            mid = (q.bid + q.ask) / 2
            spread_pct = 100 * (q.ask - q.bid) / mid
            if q.ask - q.bid > max(cfg.max_spread_ticks * cfg.tick, mid * cfg.max_spread_pct_of_mid / 100) + 1e-9:
                reason = "SPREAD_TOO_WIDE"
            else:
                t = year_fraction(now, exp)
                iv = implied_vol(mid, spot, k, t, cp)
                if iv is None:
                    reason = "IV_UNAVAILABLE"
                else:
                    g = greeks(spot, k, t, iv, cp)
                    d = abs(g["delta"])
                    theta_burn = 100 * abs(g["theta"]) / 6.25 / mid   # % of premium per trading hour
                    row |= {"delta": round(g["delta"], 4), "iv": round(iv, 4), "gamma": g["gamma"],
                            "theta_day": round(g["theta"], 4), "theta_burn_pct_hr": round(theta_burn, 4),
                            "spread_pct": round(spread_pct, 4), "dte": cal.dte(exp, today), "quote_age_s": round(age, 3)}
                    if not (cfg.delta_range[0] <= d <= cfg.delta_range[1]):
                        reason = "DELTA_OUT_OF_RANGE"
                    else:
                        score = cfg.w_delta * abs(d - 0.5) * 10 + cfg.w_spread * spread_pct + cfg.w_theta * theta_burn
                        row["score"] = round(score, 6)
                        rank_key = (round(score, 9), abs(k - spot), key)
                        if best is None or rank_key < best[0]:
                            best = (rank_key, key, q, row)
        row["rejected"] = reason
        out.evaluated.append(row)
    if best is None:
        out.reason = "NO_ELIGIBLE_CONTRACT"
        return out
    _, key, q, row = best
    out.key, out.quote, out.dte, out.features = key, q, row["dte"], row | {"rank_version": cfg.version,
                                                                         "eligible": sum(1 for r in out.evaluated if not r["rejected"])}
    return out
