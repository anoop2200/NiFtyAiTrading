"""Per-minute `chain` stream bars from option-chain snapshots (history: optionchain_db import; live: the
recorded `chain` stream once available). Uses the nearest expiry on or after the bar's date and the last
snapshot inside each minute (no later snapshot is ever used for an earlier minute)."""
from __future__ import annotations

import pandas as pd
import pyarrow as pa


def chain_bars(t: pa.Table, band: int = 5, step: float = 50.0) -> pd.DataFrame:
    """Columns: ts (epoch s, minute start), session, session_open_ts, expiry, spot, atm_strike, ce_oi, pe_oi,
    atm_ce_ltp/bid/ask, atm_pe_ltp/bid/ask, provider_iv_ce, provider_iv_pe, tier."""
    df = t.select(["snapshot_ts", "expiry_ts", "strike", "option_type", "ltp", "oi", "bid", "ask", "iv",
                   "spot"]).to_pandas()
    hm = df["snapshot_ts"].dt.strftime("%H:%M")
    df = df[(hm >= "09:15") & (hm < "15:30")].copy()
    if df.empty:
        return pd.DataFrame()
    df["day"] = df["snapshot_ts"].dt.date
    df["expiry"] = df["expiry_ts"].dt.date
    df = df[df["expiry"] >= df["day"]]
    near = df.groupby("snapshot_ts")["expiry"].transform("min")
    df = df[df["expiry"] == near]
    df["minute"] = df["snapshot_ts"].dt.floor("60s")
    last = df.groupby("minute")["snapshot_ts"].transform("max")
    df = df[df["snapshot_ts"] == last]  # last snapshot of each minute
    rows = []
    for minute, g in df.groupby("minute", sort=True):
        spot = g["spot"].dropna()
        if spot.empty:
            continue
        s = float(spot.iloc[0])
        atm = round(s / step) * step
        lo, hi = atm - band * step, atm + band * step
        b = g[(g["strike"] >= lo) & (g["strike"] <= hi)]
        ce, pe = b[b["option_type"] == "CE"], b[b["option_type"] == "PE"]
        a_ce = g[(g["strike"] == atm) & (g["option_type"] == "CE")]
        a_pe = g[(g["strike"] == atm) & (g["option_type"] == "PE")]

        def pick(x, c):
            v = x[c].iloc[0] if len(x) else None
            return None if v is None or pd.isna(v) or v <= 0 else float(v)

        day = minute.date()
        rows.append({
            "ts": int(minute.timestamp()), "session": day.isoformat(),
            "session_open_ts": int(pd.Timestamp(f"{day} 09:15", tz="Asia/Kolkata").timestamp()),
            "expiry": g["expiry"].iloc[0].isoformat(), "spot": s, "atm_strike": atm,
            "ce_oi": int(ce["oi"].sum()), "pe_oi": int(pe["oi"].sum()), "strikes_in_band": int(b["strike"].nunique()),
            "atm_ce_ltp": pick(a_ce, "ltp"), "atm_ce_bid": pick(a_ce, "bid"), "atm_ce_ask": pick(a_ce, "ask"),
            "atm_pe_ltp": pick(a_pe, "ltp"), "atm_pe_bid": pick(a_pe, "bid"), "atm_pe_ask": pick(a_pe, "ask"),
            "provider_iv_ce": pick(a_ce, "iv"), "provider_iv_pe": pick(a_pe, "iv"),
            "tier": "reconstructed_from_snapshots"})
    return pd.DataFrame(rows)
