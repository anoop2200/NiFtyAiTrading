"""Meta-model dataset from TradeLabels (v1.3 §6.3, §11).

One row per (candidate, horizon) with a resolved label (TARGET_FIRST / STOP_FIRST / TIMEOUT); NO_ENTRY and
UNLABELLABLE are excluded (no executable outcome). Features come only from the decision-time `features` JSON
the labeller stored. Leakage guards:

  * an explicit allow-list of feature names (anything new must be added here deliberately)
  * outcome columns (entry/exit prices, P&L, R, label, exit times) and risk-engine outcomes (traded, reject
    reasons) are never features — the model judges market conditions, not what the risk engine allowed
  * absolute price levels (spot, range edges) are replaced by relative versions (non-stationary otherwise)
  * `session_date` is carried as the CV group; validation never puts one session in train and test
"""
from __future__ import annotations

import hashlib
import json

import pandas as pd
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.database.models import TradeLabels

CLASSES = ("STOP_FIRST", "TIMEOUT", "TARGET_FIRST")
NUMERIC = (
    "minute_of_day", "dte", "direction",
    "cand_atr_pct", "cand_bar_range_atr", "cand_or_width_pct", "cand_close_vs_or",
    "cand_adx", "cand_rsi", "cand_ema_gap_atr", "cand_pullback_depth_atr",
    "opt_delta_abs", "opt_iv", "opt_gamma", "opt_theta_burn_pct_hr", "opt_spread_pct", "opt_quote_age_s",
    "opt_score", "opt_eligible", "opt_premium",
    "mkt_vix", "mkt_vix_chg_pct_session",
)
CATEGORICAL = ("strategy_id",)
OUTCOME_COLUMNS = ("label", "label_reason", "entry_ts", "entry_px", "exit_ts", "exit_px", "gross_pnl_per_lot",
                   "charges_per_lot", "net_pnl_per_lot", "r_multiple", "traded", "reject_reasons", "signal_outcome")


def _derive(f: dict) -> dict:
    """Decision-time raw features -> stationary model features (missing ones stay NaN)."""
    g = lambda k: f.get(k)  # noqa: E731
    out = {"minute_of_day": g("minute_of_day"), "dte": g("dte"), "direction": g("direction"),
           "cand_or_width_pct": g("cand_or_width_pct"), "cand_adx": g("cand_adx"), "cand_rsi": g("cand_rsi"),
           "opt_iv": g("opt_iv"), "opt_gamma": g("opt_gamma"), "opt_theta_burn_pct_hr": g("opt_theta_burn_pct_hr"),
           "opt_spread_pct": g("opt_spread_pct"), "opt_quote_age_s": g("opt_quote_age_s"),
           "opt_score": g("opt_score"), "opt_eligible": g("opt_eligible"), "opt_premium": g("opt_ask"),
           "mkt_vix": g("mkt_vix"), "mkt_vix_chg_pct_session": g("mkt_vix_chg_pct_session")}
    close, atr = g("cand_close"), g("cand_atr")
    if close and atr:
        out["cand_atr_pct"] = 100 * atr / close
        if g("cand_bar_range") is not None:
            out["cand_bar_range_atr"] = g("cand_bar_range") / atr
        hi, lo = g("cand_or_high"), g("cand_or_low")
        if hi is not None and lo is not None and hi > lo:
            out["cand_close_vs_or"] = (close - (hi + lo) / 2) / (hi - lo)
        if g("cand_ema9") is not None and g("cand_ema21") is not None:
            out["cand_ema_gap_atr"] = (g("cand_ema9") - g("cand_ema21")) / atr
        ph, pl = g("cand_prev_high"), g("cand_prev_low")
        if ph is not None and pl is not None:   # where the confirmation bar closed vs the pullback bar's range
            out["cand_pullback_depth_atr"] = (close - (ph + pl) / 2) / atr
    if g("opt_delta") is not None:
        out["opt_delta_abs"] = abs(g("opt_delta"))
    return out


def load_labels(engine: Engine, horizon_min: int, tiers: tuple[str, ...] | None = None,
                run_ids: tuple[str, ...] | None = None) -> pd.DataFrame:
    with Session(engine) as s:
        q = select(TradeLabels).where(TradeLabels.horizon_min == horizon_min, TradeLabels.label.in_(CLASSES))
        if tiers:
            q = q.where(TradeLabels.data_tier.in_(tiers))
        if run_ids:
            q = q.where(TradeLabels.run_id.in_(run_ids))
        rows = s.scalars(q).all()
        return pd.DataFrame([{c.name: getattr(r, c.name) for c in TradeLabels.__table__.columns} for r in rows])


def build(labels: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame]:
    """-> X (allow-listed features), y (class index), meta (group, outcome and identity columns; never features)."""
    if labels.empty:
        return pd.DataFrame(columns=list(NUMERIC) + list(CATEGORICAL)), pd.Series(dtype=int), pd.DataFrame()
    df = labels.sort_values(["session_date", "signal_ts", "signal_seq"]).reset_index(drop=True)
    raw = df["features"].map(json.loads)
    X = pd.DataFrame([_derive(f) for f in raw], columns=list(NUMERIC)).astype(float)
    X["strategy_id"] = pd.Categorical([f.get("strategy_id") for f in raw])
    y = df["label"].map({c: i for i, c in enumerate(CLASSES)}).astype(int)
    meta = df[["run_id", "session_date", "signal_seq", "signal_ts", "strategy_id", "cycle_key", "traded",
               "signal_outcome", "net_pnl_per_lot", "r_multiple", "data_tier", "label"]].copy()
    return X, y, meta


def fingerprint(labels: pd.DataFrame) -> str:
    cols = ["run_id", "session_date", "signal_seq", "horizon_min", "label", "net_pnl_per_lot", "features"]
    h = hashlib.sha256()
    for r in labels.sort_values(["run_id", "session_date", "signal_seq"])[cols].itertuples(index=False):
        h.update(json.dumps(list(r), default=str).encode())
    return h.hexdigest()


def leakage_report(X: pd.DataFrame) -> dict:
    bad = [c for c in X.columns if c in OUTCOME_COLUMNS or c not in NUMERIC + CATEGORICAL]
    constant = [c for c in NUMERIC if c in X and X[c].nunique(dropna=True) <= 1]
    return {"forbidden_features": bad, "constant_features": constant, "ok": not bad,
            "missing_share": {c: round(float(X[c].isna().mean()), 3) for c in NUMERIC if c in X}}


def class_counts(y: pd.Series) -> dict:
    return {c: int((y == i).sum()) for i, c in enumerate(CLASSES)}


__all__ = ["CLASSES", "NUMERIC", "CATEGORICAL", "load_labels", "build", "fingerprint", "leakage_report",
           "class_counts"]
