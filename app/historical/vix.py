"""India VIX 1-minute history (v1.3 decision 2026-11-26: India VIX is the market volatility-level input).

Source: Dhan /charts/intraday for the India VIX index (IDX_I, security id from the instrument master; 21 at the
time of writing) — a read-only market-data API. One Parquet file of 1-minute bars, keyed by bar-start epoch
seconds, upserted per session so re-running a backfill is idempotent.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from app.clock import IST
from app.marketdata.reconcile import parse_candles

TIER = "dhan_intraday_candles"
COLUMNS = ["ts", "session", "open", "high", "low", "close", "complete"]


def vix_path(historical_root: Path) -> Path:
    return Path(historical_root) / "india_vix" / "india_vix_1m.parquet"


def candles_to_bars(body, day: str) -> pd.DataFrame:
    """Dhan candle arrays -> bars of `day` within 09:15-15:29 IST (bar start times)."""
    rows = []
    for ts, c in sorted(parse_candles(body).items()):
        t = datetime.fromtimestamp(ts, IST)
        if t.date().isoformat() != day or not ((9, 15) <= (t.hour, t.minute) <= (15, 29)):
            continue
        if min(c["open"], c["high"], c["low"], c["close"]) <= 0 or c["high"] < c["low"]:
            continue
        rows.append({"ts": int(ts), "session": day, "open": c["open"], "high": c["high"], "low": c["low"],
                     "close": c["close"], "complete": True})
    return pd.DataFrame(rows, columns=COLUMNS)


def load(historical_root: Path, days: list[str] | None = None) -> pd.DataFrame:
    p = vix_path(historical_root)
    if not p.exists():
        return pd.DataFrame(columns=COLUMNS)
    df = pd.read_parquet(p)
    if days is not None:
        df = df[df["session"].isin(days)]
    return df.sort_values("ts").reset_index(drop=True)


def upsert(historical_root: Path, bars: pd.DataFrame) -> int:
    p = vix_path(historical_root)
    p.parent.mkdir(parents=True, exist_ok=True)
    old = load(historical_root)
    if len(bars):
        old = old[~old["session"].isin(set(bars["session"]))]
    df = pd.concat([x for x in (old, bars) if len(x)], ignore_index=True) if len(old) or len(bars) else bars
    df = df.sort_values("ts").drop_duplicates("ts", keep="last").reset_index(drop=True)
    df.to_parquet(p, index=False)
    return len(df)


def coverage(historical_root: Path, days: list[str]) -> dict:
    df = load(historical_root, days)
    per = df.groupby("session").size().to_dict() if len(df) else {}
    return {"days": len(days), "with_vix": sum(1 for d in days if per.get(d, 0) > 0),
            "full_days": sum(1 for d in days if per.get(d, 0) >= 370),
            "missing": [d for d in days if not per.get(d)]}
