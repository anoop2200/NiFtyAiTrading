"""Quotes from the optionchain_db snapshot history (tier observed_top_of_book_snapshot, ~10 s cadence).

Only the best bid/ask and their quantities are known, so every quote is `has_depth=False`: fills get the
low-fidelity slippage and flag. Contract facts come from the snapshot rows themselves (strike, expiry,
type), never from today's instrument master."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from app.backtest.fills import NS, Quote

TIER = "observed_top_of_book_snapshot"


def contract_key(expiry: str, strike: float, option_type: str) -> str:
    return f"NIFTY|{expiry}|{int(strike) if float(strike).is_integer() else strike}|{option_type}"


def parse_key(key: str) -> tuple[str, float, str]:
    _, exp, k, ot = key.split("|")
    return exp, float(k), ot


def snapshot_quotes(path: Path) -> list[Quote]:
    t = pq.read_table(path, columns=["ts", "expiry_ts", "strike", "option_type", "ltp", "bid", "ask", "bid_qty",
                                     "ask_qty"]).to_pandas()
    ts_ns = (t["ts"] - pd.Timestamp(0, tz="UTC")) // pd.Timedelta(1, "ns")
    exp = t["expiry_ts"].dt.date.astype(str)
    out = []
    for i, e, k, ot, ltp, b, a, bq, aq in zip(ts_ns, exp, t["strike"], t["option_type"], t["ltp"], t["bid"], t["ask"],
                                              t["bid_qty"], t["ask_qty"]):
        out.append(Quote(int(i), contract_key(e, k, ot), ((float(b), int(bq)),), ((float(a), int(aq)),),
                         ltp=float(ltp), tier=TIER, has_depth=False))
    out.sort(key=lambda q: q.ts_ns)
    return out


def lot_size_evidence(path: Path) -> int:
    """GCD of observed top-of-book quantities: the lot size actually in force that day."""
    import math
    t = pq.read_table(path, columns=["bid_qty", "ask_qty"]).to_pandas()
    vals = [int(v) for v in list(t["bid_qty"]) + list(t["ask_qty"]) if v and v > 0]
    g = 0
    for v in vals:
        g = math.gcd(g, v)
    return g


__all__ = ["NS", "TIER", "contract_key", "parse_key", "snapshot_quotes", "lot_size_evidence"]
