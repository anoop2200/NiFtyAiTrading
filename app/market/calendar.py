"""Exchange trading calendar, expiry adjustment, DTE tagging and event blackouts (v1.3 §3.3, §9.3, §9.6).

The calendar is the source of truth for trading days. A year without a calendar file is unknown:
`is_trading_day` raises CalendarGap so callers fail closed instead of guessing.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path

import yaml

from app.clock import IST, parse_hhmm
from app.config import PROJECT_ROOT

CAL_DIR = PROJECT_ROOT / "config" / "calendar"


class CalendarGap(RuntimeError):
    """No calendar data for the requested year."""


@dataclass(frozen=True)
class SessionTimes:
    day: date
    open: dtime
    close: dtime
    kind: str            # regular | special
    name: str = ""

    def open_dt(self) -> datetime:
        return datetime.combine(self.day, self.open, IST)

    def close_dt(self) -> datetime:
        return datetime.combine(self.day, self.close, IST)

    def contains(self, ts: datetime) -> bool:
        return self.open_dt() <= ts.astimezone(IST) < self.close_dt()


class TradingCalendar:
    def __init__(self, years: dict[int, dict]):
        self._years = years
        self.holidays: dict[date, str] = {}
        self.special: dict[date, dict] = {}
        for y in years.values():
            self.holidays.update({_d(k): v for k, v in (y.get("holidays") or {}).items()})
            for s in y.get("special_sessions") or []:
                self.special[_d(s["date"])] = s

    @classmethod
    def load(cls, directory: Path = CAL_DIR) -> "TradingCalendar":
        years = {}
        for f in sorted(Path(directory).glob("nse_*.yaml")):
            data = yaml.safe_load(f.read_text(encoding="utf-8"))
            years[int(data["year"])] = data
        return cls(years)

    # ---- coverage ------------------------------------------------------
    def covers(self, d: date) -> bool:
        return d.year in self._years

    def verification(self, year: int) -> str:
        return (self._years.get(year) or {}).get("verification", {}).get("status", "missing")

    def _require(self, d: date) -> dict:
        if d.year not in self._years:
            raise CalendarGap(f"no NSE calendar for {d.year}: add config/calendar/nse_{d.year}.yaml")
        return self._years[d.year]

    # ---- trading days ----------------------------------------------------
    def is_trading_day(self, d: date, include_special: bool = False) -> bool:
        self._require(d)
        if include_special and d in self.special and self.special[d].get("tradable"):
            return True
        return d.weekday() < 5 and d not in self.holidays

    def session(self, d: date) -> SessionTimes | None:
        y = self._require(d)
        if d in self.special and self.special[d].get("tradable") and self.special[d].get("open"):
            s = self.special[d]
            return SessionTimes(d, parse_hhmm(s["open"]), parse_hhmm(s["close"]), "special", s["name"])
        if not self.is_trading_day(d):
            return None
        rs = y["regular_session"]
        return SessionTimes(d, parse_hhmm(rs["open"]), parse_hhmm(rs["close"]), "regular")

    def next_trading_day(self, d: date) -> date:
        d += timedelta(days=1)
        while not self.is_trading_day(d):
            d += timedelta(days=1)
        return d

    def prev_trading_day(self, d: date) -> date:
        d -= timedelta(days=1)
        while not self.is_trading_day(d):
            d -= timedelta(days=1)
        return d

    def trading_days(self, start: date, end: date) -> list[date]:
        out, d = [], start
        while d <= end:
            if self.is_trading_day(d):
                out.append(d)
            d += timedelta(days=1)
        return out

    # ---- expiries --------------------------------------------------------
    def adjust_expiry(self, nominal: date) -> date:
        """If the nominal expiry day is not a trading day, expiry moves to the previous trading day."""
        return nominal if self.is_trading_day(nominal) else self.prev_trading_day(nominal)

    def weekly_expiry_on_or_after(self, d: date, weekday: int = 1) -> date:
        """Nominal weekly expiry weekday (Tuesday=1 for NIFTY) adjusted for holidays. The instrument
        master stays authoritative; this is used for checks and planning."""
        nominal = d + timedelta(days=(weekday - d.weekday()) % 7)
        exp = self.adjust_expiry(nominal)
        if exp < d:  # adjusted expiry fell before d: take the next week's
            exp = self.adjust_expiry(nominal + timedelta(days=7))
        return exp

    def dte(self, expiry: date, on: date) -> int:
        """Trading days from `on` until expiry: 0 on expiry day (0DTE)."""
        if expiry < on:
            raise ValueError(f"expiry {expiry} before {on}")
        return len(self.trading_days(on, expiry)) - (1 if self.is_trading_day(on) else 0)

    @staticmethod
    def dte_bucket(dte: int) -> str:
        return "0" if dte == 0 else "1" if dte == 1 else "2+"


def _d(v) -> date:
    return v if isinstance(v, date) else date.fromisoformat(str(v))


@dataclass(frozen=True)
class MarketEvent:
    when: datetime
    type: str
    name: str
    source: str
    whole_day: bool
    before_min: int
    after_min: int

    def window(self) -> tuple[datetime, datetime]:
        if self.whole_day:
            start = self.when.replace(hour=0, minute=0)
            return start, start + timedelta(days=1)
        return self.when - timedelta(minutes=self.before_min), self.when + timedelta(minutes=self.after_min)


class EventCalendar:
    def __init__(self, events: list[MarketEvent]):
        self.events = sorted(events, key=lambda e: e.when)

    @classmethod
    def load(cls, path: Path = CAL_DIR / "events.yaml") -> "EventCalendar":
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
        bm = data.get("blackout_minutes") or {}
        default = bm.get("default", [15, 15])
        out = []
        for e in data.get("events") or []:
            if not e.get("source"):
                raise ValueError(f"event without source: {e}")
            d = _d(e["date"])
            t = parse_hhmm(e["time"]) if e.get("time") else dtime(0, 0)
            b, a = bm.get(e["type"], default)
            out.append(MarketEvent(datetime.combine(d, t, IST), e["type"], e["name"], e["source"],
                                   not e.get("time"), int(b), int(a)))
        return cls(out)

    def blackout(self, ts: datetime) -> list[MarketEvent]:
        ts = ts.astimezone(IST)
        return [e for e in self.events if e.window()[0] <= ts < e.window()[1]]
