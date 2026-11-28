"""Historical replay of the strategy book through the SAME BookEngine the live forward test runs.

Inputs are research-tier data (never a substitute for observed quotes): the NIFTY 1-minute index store and the
Dhan expired-option ("rolling") 1-minute OHLCV of the nearest weekly expiry, ATM +/- the imported strikes. Events
are pushed in live order: at time M+60 the index bar of minute M, then the option bars of minute M (an option bar
can only be used once final). Used for the specification §7.3 validation and the period comparison.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.book.engine import NS, BookEngine, contract_key, warmup_columns
from app.book.strategies import STRATEGIES


def load_index(historical_root: Path) -> pd.DataFrame:
    from app.historical import vix as store
    return store.load(historical_root, None, name="nifty_index")


def load_options(historical_root: Path, day: str) -> pd.DataFrame:
    from app.historical import rolling_options as ro
    df = ro.load_day(historical_root, day)
    if df.empty:
        return df
    df = df[(df["expiry_flag"] == "WEEK") & (df["expiry_code"] == 1) & df["expiry"].notna()]
    df = df.drop_duplicates(["ts", "strike", "option_type"]).sort_values("ts")
    df["key"] = [contract_key(e, k, t) for e, k, t in zip(df["expiry"], df["strike"], df["option_type"])]
    return df


def run_day(day: str, index: pd.DataFrame, options: pd.DataFrame, codes=tuple(STRATEGIES), qty: int = 65,
            warmup_sessions: int = 35, kill_fn=None, fast: bool = True) -> BookEngine:
    """fast=True computes each chart once per day (equal to the live per-candle path, see precompute_day)."""
    eng = BookEngine(day, qty, codes, warmup_columns(index, day, warmup_sessions), kill_fn=kill_fn)
    today = index[index["session"] == day].sort_values("ts")
    if fast:
        eng.precompute_day(today)
    by_min: dict[int, list] = {}
    for r in options.itertuples(index=False):
        by_min.setdefault(int(r.ts), []).append(
            (r.key, {"ts": int(r.ts), "open": float(r.open), "high": float(r.high), "low": float(r.low),
                     "close": float(r.close), "volume": int(r.volume) if r.volume == r.volume else None}))
    minutes = sorted(set(int(x) for x in today["ts"]) | set(by_min))
    idx = {int(r["ts"]): r for r in today.to_dict("records")}
    for m in minutes:
        if m in idx:
            eng.on_index_bar(idx[m], (m + 60) * NS)
        for key, bar in by_min.get(m, ()):
            eng.on_option_bar(key, bar)
        eng.advance_options(m)
    eng.finish()
    return eng


def run_days(days: list[str], historical_root: Path, codes=tuple(STRATEGIES), qty: int = 65,
             progress=None) -> tuple[list[dict], list[dict]]:
    index = load_index(historical_root)
    trades, signals = [], []
    for i, day in enumerate(days, 1):
        opts = load_options(historical_root, day)
        if opts.empty or not (index["session"] == day).any():
            continue
        e = run_day(day, index, opts, codes, qty)
        trades += [t | {"session": day} for t in e.trades]
        signals += [s | {"session": day} for s in e.signals]
        if progress:
            progress(i, len(days), day, e)
    return trades, signals
