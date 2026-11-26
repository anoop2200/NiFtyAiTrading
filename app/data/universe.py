"""Subscription universe: index + futures (current/next) + ATM ± N options with a transition buffer.

On expiry days both the expiring (0DTE) and next-expiry option series are included (v1.3 §9.6).
Strike step and expiries come from the instrument master — never hard-coded.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

import pandas as pd

from app.data.instruments import Selection


@dataclass(frozen=True)
class Sub:
    segment: str          # IDX_I / NSE_FNO
    security_id: int
    mode: str             # ticker / quote / full
    role: str             # index / future / option
    expiry: str | None = None
    strike: float | None = None
    option_type: str | None = None


@dataclass
class Universe:
    index: Sub
    futures: list[Sub]
    option_expiries: list[date]
    strike_step: float
    atm: float
    each_side: int
    buffer: int
    options: dict[int, Sub] = field(default_factory=dict)
    vix: Sub | None = None

    def all(self) -> list[Sub]:
        return [self.index, *([self.vix] if self.vix else []), *self.futures, *self.options.values()]

    def describe(self) -> dict:
        return {"index": self.index.security_id, "vix": self.vix.security_id if self.vix else None,
                "futures": [f.security_id for f in self.futures],
                "option_expiries": [d.isoformat() for d in self.option_expiries],
                "strike_step": self.strike_step, "atm": self.atm, "options": len(self.options),
                "each_side": self.each_side, "buffer": self.buffer}


def strike_step(options: pd.DataFrame, expiry: date, near: float | None = None) -> float:
    strikes = sorted(options.loc[options["expiry"].dt.date == expiry, "strike"].dropna().unique())
    if len(strikes) < 3:
        raise ValueError(f"not enough strikes for {expiry}")
    s = pd.Series(strikes)
    if near is not None:  # use strikes around the money; far OTM steps may be wider
        s = s[(s - near).abs() <= (s - near).abs().nsmallest(min(len(s), 21)).max()]
    diffs = s.diff().dropna()
    return float(diffs.mode().min())


def option_expiries(options: pd.DataFrame, today: date) -> list[date]:
    exps = sorted(d for d in options["expiry"].dt.date.unique() if d >= today)
    if not exps:
        raise ValueError("no live option expiries")
    return exps[:2] if exps[0] == today else exps[:1]


def round_to_step(price: float, step: float) -> float:
    return round(price / step) * step


def _option_subs(options: pd.DataFrame, expiries: list[date], atm: float, step: float, n: int) -> dict[int, Sub]:
    lo, hi = atm - n * step, atm + n * step
    sel = options[options["expiry"].dt.date.isin(expiries) & options["strike"].between(lo, hi)]
    return {int(r.security_id): Sub("NSE_FNO", int(r.security_id), "full", "option",
                                    r.expiry.date().isoformat(), float(r.strike), r.option_type)
            for r in sel.itertuples(index=False)}


def build_universe(sel: Selection, spot: float, today: date, each_side: int = 5, buffer: int = 2,
                   futures_months: int = 2) -> Universe:
    exps = option_expiries(sel.options, today)
    step = strike_step(sel.options, exps[0], near=spot)
    atm = round_to_step(spot, step)
    idx = Sub("IDX_I", int(sel.index["security_id"]), "ticker", "index")
    fut = sel.futures[sel.futures["expiry"].dt.date >= today].sort_values("expiry").head(futures_months)
    futures = [Sub("NSE_FNO", int(r.security_id), "full", "future", r.expiry.date().isoformat())
               for r in fut.itertuples(index=False)]
    vix = Sub("IDX_I", int(sel.vix["security_id"]), "ticker", "vix") if sel.vix else None
    u = Universe(idx, futures, exps, step, atm, each_side, buffer, vix=vix)
    u.options = _option_subs(sel.options, exps, atm, step, each_side + buffer)
    return u


def recenter(u: Universe, sel: Selection, spot: float) -> list[Sub]:
    """If ATM moved, return newly required option subscriptions (existing ones are kept for the session)."""
    new_atm = round_to_step(spot, u.strike_step)
    if new_atm == u.atm:
        return []
    u.atm = new_atm
    wanted = _option_subs(sel.options, u.option_expiries, new_atm, u.strike_step, u.each_side + u.buffer)
    added = [s for sid, s in wanted.items() if sid not in u.options]
    u.options.update({s.security_id: s for s in added})
    return added
