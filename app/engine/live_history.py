"""Index history for a LIVE paper session: indicator warm-up and prior-day levels (v1.3 §5 state recovery).

Replay sessions warm their indicators on the previous sessions' index bars; a live session must start in the same
state. Source: the NIFTY 50 1-minute store (`data/historical/nifty_index`, Dhan /charts/intraday, read-only
market-data API). Sessions missing from the store are fetched before the open; whatever cannot be fetched is
reported, never invented — the engine then starts with a shorter warm-up and says so.
"""
from __future__ import annotations

from datetime import date

import pandas as pd

from app.historical import vix as store
from app.indicators.datasets import _epoch_s

NIFTY_INDEX_ID = 13
NAME = "nifty_index"


def needed_sessions(cal, day: date, n: int) -> list[str]:
    """The `n` trading sessions before `day`, oldest first."""
    out, d = [], day
    while len(out) < n:
        d = cal.prev_trading_day(d)
        out.append(d.isoformat())
    return sorted(out)


def missing(historical_root, sessions: list[str], min_bars: int = 370) -> list[str]:
    df = store.load(historical_root, sessions, name=NAME)
    per = df.groupby("session").size().to_dict() if len(df) else {}
    return [s for s in sessions if per.get(s, 0) < min_bars]


async def fetch_missing(rest, historical_root, sessions: list[str], security_id: int = NIFTY_INDEX_ID) -> dict:
    """Fetch sessions from Dhan intraday candles into the store; returns {session: bars | error}."""
    out = {}
    for day in sessions:
        r = await rest.intraday_candles(security_id, "IDX_I", "INDEX", f"{day} 09:15:00", f"{day} 15:30:00")
        if r.status != 200:
            out[day] = f"HTTP {r.status}"
            continue
        bars = store.candles_to_bars(r.body, day)
        if len(bars):
            store.upsert(historical_root, bars, name=NAME)
        out[day] = len(bars)
    return out


def index_frame(historical_root, sessions: list[str]) -> pd.DataFrame:
    """Store bars in the shape replay feeds the indicators (ts, session, session_open_ts, OHLC)."""
    df = store.load(historical_root, sessions, name=NAME)
    if df.empty:
        return pd.DataFrame(columns=["ts", "session", "session_open_ts", "open", "high", "low", "close"])
    opens = pd.to_datetime(df["session"] + " 09:15").dt.tz_localize("Asia/Kolkata")
    df = df.assign(session_open_ts=_epoch_s(opens))
    df = df[["ts", "session", "session_open_ts", "open", "high", "low", "close"]]
    return df.astype({"ts": "int64", "open": "float64", "high": "float64", "low": "float64", "close": "float64"})


def summary(sessions: list[str], frame: pd.DataFrame) -> dict:
    have = sorted(frame["session"].unique()) if len(frame) else []
    return {"wanted": len(sessions), "have": len(have), "last": have[-1] if have else None,
            "missing": [s for s in sessions if s not in have]}


def stale(cal, day: date, frame: pd.DataFrame) -> bool:
    """True when the previous trading session is absent (prior-day levels would be a day old)."""
    prev = cal.prev_trading_day(day).isoformat()
    return not len(frame) or prev not in set(frame["session"])


__all__ = ["needed_sessions", "missing", "fetch_missing", "index_frame", "summary", "stale"]
