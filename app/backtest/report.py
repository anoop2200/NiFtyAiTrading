"""Backtest summary: after-cost results with DTE, exit-reason and fidelity segmentation, plus capital usage
per session (v1.3 §9.7, §10.2)."""
from __future__ import annotations

import pandas as pd


def _seg(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"trades": 0}
    return {"trades": int(len(df)), "win_rate_pct": round(100 * (df["net_pnl"] > 0).mean(), 2),
            "gross_pnl": round(df["gross_pnl"].sum(), 2), "charges": round(df["charges"].sum(), 2),
            "net_pnl": round(df["net_pnl"].sum(), 2), "avg_net_pnl": round(df["net_pnl"].mean(), 2),
            "avg_r": round(df["r_multiple"].mean(), 4) if df["r_multiple"].notna().any() else None,
            "avg_holding_s": round(df["holding_s"].mean(), 1)}


def summarize(trades: pd.DataFrame, sessions: list[dict]) -> dict:
    out = {"all": _seg(trades)}
    if trades.empty:
        return out | {"sessions": sessions}
    t = trades.copy()
    t["dte_bucket"] = t["dte"].map(lambda d: "0" if d == 0 else "1" if d == 1 else "2+")
    out["by_dte"] = {k: _seg(g) for k, g in t.groupby("dte_bucket")}
    out["by_exit_reason"] = {k: _seg(g) for k, g in t.groupby("exit_reason")}
    out["fidelity"] = {"low_fidelity_trades": int(t["low_fidelity"].sum()),
                       "partial_entries": int((t["lots"] < t["requested_lots"]).sum())}
    eq = t.sort_values("exit_ns")["net_pnl"].cumsum()
    out["max_drawdown_inr"] = round(float((eq.cummax().clip(lower=0) - eq).max()), 2)
    peaks = [s["peak_capital"] for s in sessions]
    out["capital"] = {"max_session_peak_capital": max(peaks) if peaks else 0.0,
                      "largest_trade_risk_inr": round(float(t["risk_inr"].max()), 2),
                      "shadow_capital_0p50pct_rule": round(float(t.loc[t["dte"] != 0, "risk_inr"].max() / 0.005), 2)
                      if (t["dte"] != 0).any() else None,
                      "shadow_capital_0p25pct_0dte_rule": round(float(t.loc[t["dte"] == 0, "risk_inr"].max() / 0.0025), 2)
                      if (t["dte"] == 0).any() else None}
    out["sessions"] = sessions
    return out
