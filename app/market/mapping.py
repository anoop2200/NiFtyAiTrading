"""Dated instrument map (security_id <-> contract facts) and the futures roll rule (v1.3 §3.2).

Always built from the instrument master stored for the trading day being processed, never from
today's master, so historical contracts resolve to the facts valid on that day.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.database.models import Instruments
from app.market.calendar import TradingCalendar


@dataclass(frozen=True)
class Contract:
    security_id: int
    segment: str
    role: str                 # index / future / option
    trading_symbol: str
    expiry: date | None = None
    strike: float | None = None
    option_type: str | None = None
    lot_size: int | None = None
    tick_size: float | None = None


ROLE = {"INDEX": "index", "FUTIDX": "future", "OPTIDX": "option", "VIX": "vix"}


class InstrumentMap:
    def __init__(self, master_date: date, contracts: list[Contract]):
        self.master_date = master_date
        self.by_id = {c.security_id: c for c in contracts}
        self.by_key = {(c.expiry, c.strike, c.option_type): c for c in contracts if c.role == "option"}
        self.futures = sorted((c for c in contracts if c.role == "future"), key=lambda c: c.expiry)
        self.index = next((c for c in contracts if c.role == "index"), None)

    @classmethod
    def from_db(cls, engine: Engine, master_date: date) -> "InstrumentMap":
        with Session(engine) as s:
            rows = s.scalars(select(Instruments).where(Instruments.master_date == master_date.isoformat())).all()
        if not rows:
            raise LookupError(f"no instrument master stored for {master_date}")
        return cls(master_date, [Contract(
            r.security_id, r.exchange_segment, ROLE.get(r.instrument_type, r.instrument_type.lower()),
            r.trading_symbol, date.fromisoformat(r.expiry[:10]) if r.expiry else None, r.strike, r.option_type,
            r.lot_size, r.tick_size) for r in rows])

    def get(self, security_id: int) -> Contract | None:
        return self.by_id.get(security_id)

    def option(self, expiry: date, strike: float, option_type: str) -> Contract | None:
        return self.by_key.get((expiry, float(strike), option_type))

    def live_futures(self, on: date) -> list[Contract]:
        return [f for f in self.futures if f.expiry and f.expiry >= on]


def active_future(fut: list[Contract], on: date, cal: TradingCalendar,
                  prev_session_volume: dict[int, int] | None = None, days_before_expiry: int = 2) -> Contract:
    """Roll rule (v1.3 §3.2): use the next-month contract from the start of the first session after
    next-month volume exceeded current-month volume, or `days_before_expiry` trading days before the
    current contract's expiry, whichever comes first. Intraday features never mix two contracts."""
    live = [f for f in fut if f.expiry and f.expiry >= on]
    if not live:
        raise LookupError(f"no live futures on {on}")
    if len(live) == 1:
        return live[0]
    cur, nxt = live[0], live[1]
    if cal.dte(cur.expiry, on) <= days_before_expiry:
        return nxt
    v = prev_session_volume or {}
    if v.get(nxt.security_id, 0) > v.get(cur.security_id, 0) > 0:
        return nxt
    return cur
