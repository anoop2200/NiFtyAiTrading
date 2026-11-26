"""Load indicator input streams (1-minute bars) from derived/historical datasets."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from app.indicators.chain import chain_bars


def _epoch_s(t: pd.Series) -> pd.Series:
    # unit-agnostic (pandas may hold s/us/ns resolution)
    return ((t - pd.Timestamp(0, tz="UTC")) // pd.Timedelta(seconds=1)).astype("int64")


def _session_cols(df: pd.DataFrame, ts_col: str = "ts") -> pd.DataFrame:
    t = pd.to_datetime(df[ts_col], utc=True).dt.tz_convert("Asia/Kolkata")
    out = df.copy()
    out["ts"] = _epoch_s(t)
    out["session"] = t.dt.date.astype(str)
    opens = pd.to_datetime(out["session"] + " 09:15").dt.tz_localize("Asia/Kolkata")
    out["session_open_ts"] = _epoch_s(opens)
    return out


def pg_spot_stream(derived: Path, days: list[str]) -> pd.DataFrame:
    parts = [pq.read_table(Path(derived) / "pg_spot_bars_1m" / f"date={d}" / "part-0.parquet").to_pandas()
             for d in days if (Path(derived) / "pg_spot_bars_1m" / f"date={d}").exists()]
    if not parts:
        return pd.DataFrame()
    df = _session_cols(pd.concat(parts, ignore_index=True))
    return df.sort_values("ts").reset_index(drop=True)


def pg_chain_stream(historical: Path, dataset: str, days: list[str], cache: Path | None = None) -> pd.DataFrame:
    """Chain bars per day; cached as Parquet under `cache` (rebuilt when the source file is newer)."""
    parts = []
    for d in days:
        f = Path(historical) / dataset / f"date={d}" / "part-0.parquet"
        if not f.exists():
            continue
        c = Path(cache) / f"date={d}" / "part-0.parquet" if cache else None
        if c is not None and c.exists() and c.stat().st_mtime >= f.stat().st_mtime:
            parts.append(pd.read_parquet(c))
            continue
        cb = chain_bars(pq.read_table(f))
        if c is not None and not cb.empty:
            c.parent.mkdir(parents=True, exist_ok=True)
            cb.to_parquet(c, index=False)
        parts.append(cb)
    parts = [p for p in parts if not p.empty]
    return pd.concat(parts, ignore_index=True).sort_values("ts").reset_index(drop=True) if parts else pd.DataFrame()


def pg_option_stream(derived: Path, day: str, expiry: str, strike: float, option_type: str) -> pd.DataFrame:
    """One contract's reconstructed 1-minute bars (for option-stream indicators)."""
    t = pq.read_table(Path(derived) / "pg_option_bars_1m" / f"date={day}" / "part-0.parquet").to_pandas()
    t = t[(t["expiry"].astype(str) == expiry) & (t["strike"] == strike) & (t["option_type"] == option_type)]
    return _session_cols(t).sort_values("ts").reset_index(drop=True)
