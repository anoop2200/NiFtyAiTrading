"""Evaluation metrics, bootstrap intervals, Deflated Sharpe Ratio and promotion-gate screening
(v1.3 §10.2-§10.4). Pure functions over a trades DataFrame (one row per closed paper trade, with `session`).

R for a trade = net P&L / planned risk at entry (`risk_inr`: entry - stop, times quantity). Sharpe is per
session (daily net P&L, sessions without trades count as 0) annualised with 252 sessions.
"""
from __future__ import annotations

import math
import random
from statistics import NormalDist

import numpy as np
import pandas as pd

ND = NormalDist()
EULER = 0.5772156649015329


def _r(t: pd.DataFrame) -> pd.Series:
    return (t["net_pnl"] / t["risk_inr"].where(t["risk_inr"] > 0)).fillna(0.0)


def _exit_share(t: pd.DataFrame, prefix: str) -> float:
    return round(float(t["exit_reason"].astype(str).str.startswith(prefix).mean()), 4) if len(t) else 0.0


def daily_pnl(t: pd.DataFrame, sessions: list[str]) -> pd.Series:
    by = t.groupby("session")["net_pnl"].sum() if len(t) else pd.Series(dtype=float)
    return pd.Series([float(by.get(s, 0.0)) for s in sessions], index=sessions)


def max_drawdown(values: pd.Series) -> float:
    eq = values.cumsum()
    return float((eq.cummax().clip(lower=0) - eq).max()) if len(eq) else 0.0


def core(t: pd.DataFrame, sessions: list[str]) -> dict:
    """Headline metrics for a set of trades over a set of sessions."""
    n = len(t)
    out = {"trades": n, "sessions": len(sessions), "sessions_with_trades": int(t["session"].nunique()) if n else 0}
    if n == 0:
        return out | {"net_pnl": 0.0, "net_expectancy_inr": None, "net_expectancy_r": None, "profit_factor": None,
                      "win_rate_pct": None, "max_drawdown_inr": 0.0, "max_drawdown_r": 0.0, "sharpe_session": None,
                      "charges": 0.0, "turnover_inr": 0.0}
    t = t.sort_values(["exit_ns", "trade_id"]) if "exit_ns" in t else t
    r = _r(t)
    gains, losses = t.loc[t["net_pnl"] > 0, "net_pnl"].sum(), -t.loc[t["net_pnl"] < 0, "net_pnl"].sum()
    d = daily_pnl(t, sessions)
    sd = d.std(ddof=1) if len(d) > 1 else 0.0
    return out | {
        "net_pnl": round(float(t["net_pnl"].sum()), 2),
        "gross_pnl": round(float(t["gross_pnl"].sum()), 2),
        "charges": round(float(t["charges"].sum()), 2),
        "turnover_inr": round(float((t["entry_px"] * t["qty"] + t["exit_px"] * t["qty"]).sum()), 2),
        "net_expectancy_inr": round(float(t["net_pnl"].mean()), 2),
        "net_expectancy_r": round(float(r.mean()), 4),
        "profit_factor": round(float(gains / losses), 4) if losses > 0 else None,
        "win_rate_pct": round(100 * float((t["net_pnl"] > 0).mean()), 2),
        "max_drawdown_inr": round(max_drawdown(t["net_pnl"]), 2),
        "max_drawdown_r": round(max_drawdown(r), 4),
        "sharpe_session": round(float(d.mean() / sd * math.sqrt(252)), 4) if sd > 0 else None,
        "target_share": _exit_share(t, "TARGET"), "stop_share": _exit_share(t, "STOP"),
        "timeout_share": _exit_share(t, "TIME_STOP"), "invalidated_share": _exit_share(t, "SETUP_INVALIDATED"),
        "low_fidelity_share": round(float(t["low_fidelity"].mean()), 4) if "low_fidelity" in t else None,
        "largest_session_profit_share": largest_session_share(t),
    }


def largest_session_share(t: pd.DataFrame) -> float | None:
    """Share of total net profit contributed by the best session (None when total profit <= 0)."""
    by = t.groupby("session")["net_pnl"].sum()
    total = by.sum()
    return round(float(by.max() / total), 4) if total > 0 else None


def dte_bucket(dte) -> str:
    return "0DTE" if dte == 0 else "1DTE" if dte == 1 else "2+DTE"


def segmented(t: pd.DataFrame, sessions: list[str], regimes: dict[str, str]) -> dict:
    out = {"by_dte": {}, "by_strategy": {}, "by_regime": {}, "by_time_of_day": {}}
    if t.empty:
        return out
    t = t.assign(dte_bucket=t["dte"].map(dte_bucket), regime=t["session"].map(lambda s: regimes.get(s, "unknown")),
                 tod=pd.to_datetime(t["entry_ns"], unit="ns", utc=True).dt.tz_convert("Asia/Kolkata").dt.hour
                 .map(lambda h: "09-11" if h < 11 else "11-13" if h < 13 else "13-15"))
    for col, key in (("dte_bucket", "by_dte"), ("strategy", "by_strategy"), ("regime", "by_regime"),
                     ("tod", "by_time_of_day")):
        for v, g in t.groupby(col):
            ss = sorted(g["session"].unique()) if col != "regime" else [s for s in sessions if regimes.get(s) == v]
            out[key][str(v)] = core(g, ss)
    return out


# ---------------------------------------------------------------------------------------------- regimes
def session_regimes(index: pd.DataFrame, atr_days: int = 14, min_days: int = 5, trend_ratio: float = 0.5) -> dict:
    """Deterministic, after-the-fact session label for *segmentation only* (never a trading input):
    |close - open| / prior daily ATR >= trend_ratio -> trend_up / trend_down, else range."""
    d = index.sort_values("ts").groupby("session").agg(open=("open", "first"), high=("high", "max"),
                                                        low=("low", "min"), close=("close", "last"))
    pc = d["close"].shift(1)
    tr = pd.concat([d["high"] - d["low"], (d["high"] - pc).abs(), (d["low"] - pc).abs()], axis=1).max(axis=1)
    atr = tr.shift(1).rolling(atr_days, min_periods=min_days).mean()
    out = {}
    for s in d.index:
        a = atr.get(s)
        if pd.isna(a) or a <= 0:
            out[str(s)] = "unknown"
            continue
        x = (d.at[s, "close"] - d.at[s, "open"]) / a
        out[str(s)] = "trend_up" if x >= trend_ratio else "trend_down" if x <= -trend_ratio else "range"
    return out


# ---------------------------------------------------------------------------------------------- inference
def session_bootstrap(t: pd.DataFrame, sessions: list[str], n: int = 2000, seed: int = 0, level: float = 0.95) -> dict:
    """Resample whole sessions (trades within a day are correlated). CI for net expectancy (INR and R per
    trade) and for max drawdown (INR)."""
    if t.empty or not sessions:
        return {"n": 0}
    t = t.assign(r=_r(t))
    groups = {s: g for s, g in t.groupby("session")}
    rng = random.Random(seed)
    exp_inr, exp_r, dd = [], [], []
    for _ in range(n):
        pick = [rng.choice(sessions) for _ in sessions]
        parts = [groups[s] for s in pick if s in groups]
        if not parts:
            exp_inr.append(0.0), exp_r.append(0.0), dd.append(0.0)
            continue
        b = pd.concat(parts)
        exp_inr.append(float(b["net_pnl"].mean()))
        exp_r.append(float(b["r"].mean()))
        dd.append(max_drawdown(b["net_pnl"]))
    lo, hi = (1 - level) / 2, 1 - (1 - level) / 2

    def ci(x):
        return [round(float(np.quantile(x, lo)), 4), round(float(np.quantile(x, hi)), 4)]
    return {"n": n, "seed": seed, "level": level, "net_expectancy_inr": ci(exp_inr), "net_expectancy_r": ci(exp_r),
            "max_drawdown_inr": ci(dd)}


def deflated_sharpe(daily: pd.Series, trial_sharpes: list[float], n_trials: int) -> dict:
    """Deflated Sharpe Ratio (Bailey & Lopez de Prado 2014) on per-session returns (non-annualised SR).
    SR0 is the expected maximum SR of `n_trials` independent unskilled trials with the observed variance of
    trial Sharpe ratios; DSR = P(true SR > SR0)."""
    x = daily.astype(float)
    T = len(x)
    if T < 10 or x.std(ddof=1) == 0:
        return {"evaluable": False, "reason": "fewer than 10 sessions or zero variance", "n_trials": n_trials}
    sr = float(x.mean() / x.std(ddof=1))
    skew = float(((x - x.mean()) ** 3).mean() / x.std(ddof=0) ** 3)
    kurt = float(((x - x.mean()) ** 4).mean() / x.std(ddof=0) ** 4)
    v = float(np.var(trial_sharpes, ddof=1)) if len(trial_sharpes) > 1 else 0.0
    if n_trials > 1 and v > 0:
        sr0 = math.sqrt(v) * ((1 - EULER) * ND.inv_cdf(1 - 1 / n_trials) + EULER * ND.inv_cdf(1 - 1 / (n_trials * math.e)))
    else:
        sr0 = 0.0
    den = 1 - skew * sr + (kurt - 1) / 4 * sr * sr
    if den <= 0:
        return {"evaluable": False, "reason": "non-positive variance term", "n_trials": n_trials}
    z = (sr - sr0) * math.sqrt(T - 1) / math.sqrt(den)
    return {"evaluable": True, "sr_per_session": round(sr, 6), "sr0": round(sr0, 6), "skew": round(skew, 4),
            "kurtosis": round(kurt, 4), "sessions": T, "n_trials": n_trials, "trial_sr_variance": round(v, 8),
            "dsr": round(ND.cdf(z), 6)}


# ---------------------------------------------------------------------------------------------- gates
def promotion_screen(oos: dict, oos_seg: dict, boot: dict, dsr: dict, data: dict, observed_tier_trades: int,
                     observed_tier_sessions: int, n_trials: int, holdout_reused: bool) -> dict:
    """§10.4 screening gates on the walk-forward OOS aggregate. PASS / FAIL / NOT_EVALUABLE / HUMAN."""
    g = {}

    def gate(name, ok, detail):
        g[name] = {"status": "NOT_EVALUABLE" if ok is None else ("PASS" if ok else "FAIL"), "detail": detail}

    gate("data_integrity", not data.get("quality_failed_days"),
         {"quality_failed_days": data.get("quality_failed_days")})
    regimes = [k for k, v in oos_seg.get("by_regime", {}).items() if k != "unknown" and v["trades"] > 0]
    gate("oos_sample", oos["trades"] >= 200 and oos["sessions"] >= 60 and len(regimes) >= 3
         and observed_tier_trades >= 60 and observed_tier_sessions >= 30,
         {"trades": oos["trades"], "sessions": oos["sessions"], "regime_buckets": regimes,
          "observed_feed_trades": observed_tier_trades, "observed_feed_sessions": observed_tier_sessions})
    lb = (boot.get("net_expectancy_inr") or [None])[0]
    gate("net_performance", None if lb is None else (oos.get("net_expectancy_inr") or 0) > 0 and lb > 0,
         {"net_expectancy_inr": oos.get("net_expectancy_inr"), "bootstrap_95_lower": lb})
    pf = oos.get("profit_factor")
    gate("profit_factor", None if pf is None else pf >= 1.20, {"profit_factor": pf})
    gate("drawdown", oos.get("max_drawdown_r", 0) <= 8.0,
         {"max_drawdown_r": oos.get("max_drawdown_r"), "max_drawdown_inr": oos.get("max_drawdown_inr")})
    share = oos.get("largest_session_profit_share")
    pos_regimes = [k for k, v in oos_seg.get("by_regime", {}).items()
                   if k != "unknown" and (v.get("net_expectancy_inr") or 0) > 0]
    gate("stability", None if share is None else share <= 0.20 and len(pos_regimes) >= 2,
         {"largest_session_profit_share": share, "positive_regimes": pos_regimes})
    gate("expiry_day_segment", "0DTE" in oos_seg.get("by_dte", {}) or oos["trades"] == 0,
         {"dte_buckets": sorted(oos_seg.get("by_dte", {}))})
    g["meta_model_calibration"] = {"status": "NOT_APPLICABLE", "detail": "rule-only strategies (Phase 7)"}
    gate("multiple_testing", None if not dsr.get("evaluable") else (not holdout_reused and dsr["dsr"] >= 0.95),
         {"n_trials": n_trials, "dsr": dsr.get("dsr"), "holdout_reused": holdout_reused})
    g["operational_paper_test"] = {"status": "NOT_EVALUABLE", "detail": "30-session paper run is Phase 9"}
    g["approval"] = {"status": "HUMAN", "detail": "written sign-off required; never automatic"}
    fails = [k for k, v in g.items() if v["status"] == "FAIL"]
    pending = [k for k, v in g.items() if v["status"] in ("NOT_EVALUABLE", "HUMAN")]
    return {"gates": g, "failed": fails, "pending": pending,
            "verdict": "REJECT" if fails else "INCOMPLETE" if pending else "READY_FOR_HUMAN_REVIEW"}
