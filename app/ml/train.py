"""Meta-model training: chronological day-grouped walk-forward, LightGBM, calibration, baseline (v1.3 §6.3, §11).

Target: `win` = the candidate's labelled one-lot trade closed with positive net P&L after charges within the
horizon (TARGET_FIRST, or a TIMEOUT that ended in profit). The model outputs a calibrated P(win).

Validation (no leakage by construction):
  * sessions are sorted; each fold tests a contiguous block of later sessions
  * training uses only sessions strictly before the test block, minus `embargo_sessions` adjacent sessions
  * inside the training window the LAST `calib_share` of sessions is held back to fit the calibrator, so the
    calibrator never sees rows the booster trained on, and neither ever sees the test block
  * the take/skip threshold comes from training-window statistics only (break-even probability from the
    training window's average win and average loss), never tuned on the test block

Baseline: the training window's base rate as a constant probability (calibration baseline) and the rule-only
policy that takes every candidate (economic baseline).
"""
from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, field

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression

from app.ml.dataset import NUMERIC

TARGET = "win = net_pnl_per_lot > 0 within horizon"


@dataclass(frozen=True)
class TrainConfig:
    n_folds: int = 5
    min_train_sessions: int = 20
    embargo_sessions: int = 1
    calib_share: float = 0.25
    calibration: str = "sigmoid"          # sigmoid (Platt) or isotonic; isotonic needs >= 200 calibration rows
    seed: int = 7
    lgb_params: dict = field(default_factory=lambda: {
        "objective": "binary", "learning_rate": 0.05, "num_leaves": 7, "min_data_in_leaf": 10,
        "feature_fraction": 0.8, "bagging_fraction": 0.8, "bagging_freq": 1, "lambda_l2": 1.0,
        "verbose": -1, "deterministic": True, "num_threads": 1})
    num_boost_round: int = 150
    ece_bins: int = 10


# ------------------------------------------------------------------------------------------ features
def design_matrix(X: pd.DataFrame, strategies: list[str]) -> pd.DataFrame:
    """Numeric model input: allow-listed numerics + one-hot strategy (fixed category list from training)."""
    M = X[list(NUMERIC)].astype(float).copy()
    sid = X["strategy_id"].astype(str)
    for s in strategies:
        M[f"strat_{s}"] = (sid == s).astype(float)
    return M


def target(meta: pd.DataFrame) -> np.ndarray:
    return (meta["net_pnl_per_lot"].astype(float) > 0).astype(int).to_numpy()


# ------------------------------------------------------------------------------------------ calibration
class Calibrator:
    def __init__(self, method: str):
        self.method, self.a, self.b, self.iso = method, 1.0, 0.0, None

    @staticmethod
    def _logit(p):
        p = np.clip(p, 1e-6, 1 - 1e-6)
        return np.log(p / (1 - p))

    def fit(self, raw: np.ndarray, y: np.ndarray) -> "Calibrator":
        if len(set(y)) < 2:                     # degenerate calibration set -> constant base rate
            self.method, self.a, self.b = "constant", 0.0, float(self._logit(np.array([y.mean() if len(y) else .5]))[0])
            return self
        if self.method == "isotonic":
            self.iso = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0).fit(raw, y)
        else:
            lr = LogisticRegression(C=1e6, max_iter=1000).fit(self._logit(raw).reshape(-1, 1), y)
            self.a, self.b = float(lr.coef_[0, 0]), float(lr.intercept_[0])
        return self

    def predict(self, raw: np.ndarray) -> np.ndarray:
        if self.method == "isotonic":
            return self.iso.predict(raw)
        return 1 / (1 + np.exp(-(self.a * self._logit(raw) + self.b)))

    def to_dict(self) -> dict:
        if self.method == "isotonic":
            return {"method": "isotonic", "x": self.iso.X_thresholds_.tolist(), "y": self.iso.y_thresholds_.tolist()}
        return {"method": self.method, "a": self.a, "b": self.b}

    @classmethod
    def from_dict(cls, d: dict) -> "Calibrator":
        c = cls(d["method"])
        if d["method"] == "isotonic":
            c.iso = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
            c.iso.fit(d["x"], d["y"])
        else:
            c.a, c.b = d["a"], d["b"]
        return c


# ------------------------------------------------------------------------------------------ metrics
def brier(p, y) -> float:
    return float(np.mean((np.asarray(p) - np.asarray(y)) ** 2)) if len(y) else float("nan")


def log_loss(p, y) -> float:
    p = np.clip(np.asarray(p, float), 1e-6, 1 - 1e-6)
    y = np.asarray(y)
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p))) if len(y) else float("nan")


def ece(p, y, bins: int = 10) -> float:
    p, y = np.asarray(p, float), np.asarray(y, float)
    if not len(y):
        return float("nan")
    idx = np.minimum((p * bins).astype(int), bins - 1)
    return float(sum(abs(p[idx == b].mean() - y[idx == b].mean()) * (idx == b).sum()
                     for b in range(bins) if (idx == b).any()) / len(y))


def breakeven_p(net: np.ndarray) -> float:
    """P(win) at which a candidate's expected net P&L is zero, from training-window wins and losses."""
    wins, losses = net[net > 0], net[net <= 0]
    if not len(wins):
        return 1.0
    if not len(losses):
        return 0.0
    w, lo = wins.mean(), -losses.mean()
    return float(lo / (w + lo)) if (w + lo) > 0 else 0.5


# ------------------------------------------------------------------------------------------ CV
def walk_forward_folds(sessions: list[str], cfg: TrainConfig) -> list[dict]:
    """Expanding-window folds over sorted unique sessions."""
    s = sorted(set(sessions))
    test_pool = s[cfg.min_train_sessions + cfg.embargo_sessions:]
    if len(test_pool) < cfg.n_folds:
        return []
    blocks = np.array_split(np.array(test_pool), cfg.n_folds)
    folds = []
    for i, blk in enumerate(blocks):
        first = s.index(blk[0])
        train = s[:max(0, first - cfg.embargo_sessions)]
        n_cal = max(1, int(round(len(train) * cfg.calib_share)))
        folds.append({"fold": i, "fit": train[:-n_cal], "calib": train[-n_cal:],
                      "embargo": s[len(train):first], "test": list(blk)})
    return folds


def _fit(Xd: pd.DataFrame, y: np.ndarray, cfg: TrainConfig) -> lgb.Booster:
    params = cfg.lgb_params | {"seed": cfg.seed}
    return lgb.train(params, lgb.Dataset(Xd, label=y, free_raw_data=False), num_boost_round=cfg.num_boost_round)


def fit_calibrated(X: pd.DataFrame, meta: pd.DataFrame, fit_sessions, calib_sessions, strategies,
                   cfg: TrainConfig) -> tuple[lgb.Booster, Calibrator, float, float]:
    """-> booster, calibrator, base rate, break-even threshold (all from the training window only)."""
    y = target(meta)
    fit_m = meta["session_date"].isin(fit_sessions).to_numpy()
    cal_m = meta["session_date"].isin(calib_sessions).to_numpy()
    Xd = design_matrix(X, strategies)
    booster = _fit(Xd[fit_m], y[fit_m], cfg)
    method = cfg.calibration if (cfg.calibration != "isotonic" or cal_m.sum() >= 200) else "sigmoid"
    cal = Calibrator(method).fit(booster.predict(Xd[cal_m]), y[cal_m])
    window = fit_m | cal_m
    net = meta["net_pnl_per_lot"].astype(float).to_numpy()[window]
    return booster, cal, float(y[window].mean()), breakeven_p(net)


def evaluate(X: pd.DataFrame, meta: pd.DataFrame, cfg: TrainConfig = TrainConfig()) -> dict:
    """Walk-forward out-of-fold predictions + model vs baseline metrics."""
    strategies = sorted(X["strategy_id"].astype(str).unique().tolist())
    folds = walk_forward_folds(meta["session_date"].tolist(), cfg)
    if not folds:
        return {"ok": False, "reason": f"not enough sessions for {cfg.n_folds} folds after "
                                       f"{cfg.min_train_sessions} training sessions", "folds": [], "oof": None}
    y = target(meta)
    net = meta["net_pnl_per_lot"].astype(float).to_numpy()
    Xd = design_matrix(X, strategies)
    oof = []
    fold_out = []
    for f in folds:
        test_m = meta["session_date"].isin(f["test"]).to_numpy()
        fit_y = y[meta["session_date"].isin(f["fit"]).to_numpy()]
        if test_m.sum() == 0 or len(set(fit_y)) < 2:
            fold_out.append({**_fold_desc(f), "skipped": "no test rows or one-class training window"})
            continue
        booster, cal, base, thr = fit_calibrated(X, meta, f["fit"], f["calib"], strategies, cfg)
        p = cal.predict(booster.predict(Xd[test_m]))
        take = (p > thr).astype(int)
        idx = np.flatnonzero(test_m)
        oof.append(pd.DataFrame({"row": idx, "fold": f["fold"], "p_win": p, "p_base": base, "threshold": thr,
                                 "take": take, "y": y[idx], "net": net[idx]}))
        fold_out.append({**_fold_desc(f), "rows": int(test_m.sum()), "base_rate": round(base, 4),
                         "threshold": round(thr, 4), "brier": round(brier(p, y[idx]), 4),
                         "brier_base": round(brier(np.full(len(idx), base), y[idx]), 4),
                         "taken": int(take.sum()), "net_rule_only": round(float(net[idx].sum()), 2),
                         "net_model": round(float(net[idx][take == 1].sum()), 2)})
    if not oof:
        return {"ok": False, "reason": "every fold skipped", "folds": fold_out, "oof": None}
    o = pd.concat(oof, ignore_index=True)
    o["session_date"] = meta["session_date"].to_numpy()[o["row"]]
    return {"ok": True, "folds": fold_out, "oof": o, "strategies": strategies,
            "metrics": oof_metrics(o, cfg)}


def _fold_desc(f: dict) -> dict:
    span = lambda xs: [xs[0], xs[-1], len(xs)] if xs else [None, None, 0]  # noqa: E731
    return {"fold": f["fold"], "fit": span(f["fit"]), "calib": span(f["calib"]),
            "embargo": span(f["embargo"]), "test": span(f["test"])}


def oof_metrics(o: pd.DataFrame, cfg: TrainConfig, n_boot: int = 2000) -> dict:
    p, y, base = o["p_win"].to_numpy(), o["y"].to_numpy(), o["p_base"].to_numpy()
    by_day = o.groupby("session_date").apply(
        lambda d: pd.Series({"rule": d["net"].sum(), "model": d.loc[d["take"] == 1, "net"].sum()}),
        include_groups=False)
    diff = (by_day["model"] - by_day["rule"]).to_numpy()
    rng = np.random.default_rng(cfg.seed)
    boots = np.array([rng.choice(diff, len(diff)).sum() for _ in range(n_boot)]) if len(diff) else np.array([0.0])
    taken = o[o["take"] == 1]
    r = lambda v: None if v is None or (isinstance(v, float) and math.isnan(v)) else round(float(v), 4)  # noqa: E731
    return {
        "rows": int(len(o)), "sessions": int(o["session_date"].nunique()), "win_rate": r(y.mean()),
        "brier_model": r(brier(p, y)), "brier_baseline": r(brier(base, y)),
        "logloss_model": r(log_loss(p, y)), "logloss_baseline": r(log_loss(base, y)),
        "ece_model": r(ece(p, y, cfg.ece_bins)), "ece_baseline": r(ece(base, y, cfg.ece_bins)),
        "taken": int(len(taken)), "take_rate": r(len(taken) / len(o)),
        "net_rule_only": r(o["net"].sum()), "net_model": r(taken["net"].sum()),
        "net_per_candidate_rule_only": r(o["net"].mean()),
        "net_per_trade_model": r(taken["net"].mean()) if len(taken) else None,
        "net_improvement": r(taken["net"].sum() - o["net"].sum()),
        "net_improvement_p05": r(np.percentile(boots, 5)), "net_improvement_p95": r(np.percentile(boots, 95)),
        "days_model_better": int((diff > 0).sum()), "days_model_worse": int((diff < 0).sum()),
    }


# ------------------------------------------------------------------------------------------ gate
@dataclass(frozen=True)
class GateConfig:
    max_ece: float = 0.05
    min_oof_sessions: int = 30
    require_bootstrap_positive: bool = True


def promotion_gate(*, adequacy: dict, prospective_only: bool, leakage: dict, metrics: dict | None,
                   cfg: GateConfig = GateConfig()) -> dict:
    """§11: meta-model enters production only if every check passes. History-trained models never do."""
    m = metrics or {}
    lt = lambda a, b: a is not None and b is not None and a < b  # noqa: E731
    checks = {
        "prospective_data_only": prospective_only,
        "label_adequacy": bool(adequacy.get("ready")),
        "no_leakage": bool(leakage.get("ok")),
        "enough_oof_sessions": m.get("sessions", 0) >= cfg.min_oof_sessions,
        "brier_beats_baseline": lt(m.get("brier_model"), m.get("brier_baseline")),
        "logloss_beats_baseline": lt(m.get("logloss_model"), m.get("logloss_baseline")),
        "calibrated_ece": m.get("ece_model") is not None and m["ece_model"] <= cfg.max_ece,
        "net_after_cost_improves": (m.get("net_improvement") or 0) > 0,
        "net_improvement_robust": (not cfg.require_bootstrap_positive) or (m.get("net_improvement_p05") or 0) > 0,
    }
    return {"passed": all(checks.values()), "checks": checks, "config": asdict(cfg)}


# ------------------------------------------------------------------------------------------ artifact
def artifact(booster: lgb.Booster, cal: Calibrator, strategies: list[str], threshold: float, base_rate: float,
             cfg: TrainConfig, horizon_min: int, extra: dict) -> dict:
    """JSON-only artifact (no pickle): booster text + calibrator parameters + feature contract."""
    return {"format": "nifty-meta-model-1", "target": TARGET, "horizon_min": horizon_min,
            "features": list(NUMERIC), "strategies": strategies, "threshold": threshold, "base_rate": base_rate,
            "calibrator": cal.to_dict(), "train_config": asdict(cfg), "booster": booster.model_to_string(), **extra}


class MetaModel:
    def __init__(self, art: dict):
        self.art = art
        self.booster = lgb.Booster(model_str=art["booster"])
        self.cal = Calibrator.from_dict(art["calibrator"])

    @classmethod
    def load(cls, path) -> "MetaModel":
        with open(path, encoding="utf-8") as f:
            return cls(json.load(f))

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        return self.cal.predict(self.booster.predict(design_matrix(X, self.art["strategies"])))

    def decide(self, X: pd.DataFrame) -> np.ndarray:
        return (self.predict(X) > self.art["threshold"]).astype(int)
