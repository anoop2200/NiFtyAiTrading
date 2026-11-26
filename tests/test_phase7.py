"""Phase 7: meta-model dataset, leakage guards, walk-forward CV, calibration, gate, registry."""
import json
import random
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest
from sqlalchemy.orm import Session

from app.control.actions import OperatorLog
from app.database.db import init_db, make_engine
from app.database.models import ModelPredictions, TradeLabels
from app.ml import dataset as ds
from app.ml.registry import RegistryError, approve, list_versions, train_and_register, verify_artifact
from app.ml.train import (Calibrator, GateConfig, MetaModel, TrainConfig, breakeven_p, ece, evaluate,
                          promotion_gate, walk_forward_folds)

FAST = TrainConfig(n_folds=4, min_train_sessions=20, num_boost_round=60)


def _sessions(n, start=date(2027, 1, 4)):
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _labels(n_sessions=80, per=10, tier="observed_feed", run_id="paper_rules_v1", seed=1, signal=True):
    """Synthetic candidates where opt_score drives the outcome (signal=True) or nothing does (signal=False)."""
    rng = random.Random(seed)
    rows = []
    for d in _sessions(n_sessions):
        for k in range(per):
            score = rng.random()
            pwin = (0.85 if score > 0.5 else 0.15) if signal else 0.45
            win = rng.random() < pwin
            label = ("TARGET_FIRST" if rng.random() < 0.7 else "TIMEOUT") if win else \
                    ("STOP_FIRST" if rng.random() < 0.7 else "TIMEOUT")
            net = rng.uniform(300, 700) if win else -rng.uniform(250, 600)
            sid = "orb" if k % 2 else "pullback"
            f = {"strategy_id": sid, "direction": rng.choice([-1, 1]), "dte": rng.randint(0, 6),
                 "minute_of_day": 560 + 20 * k, "cand_close": 24000 + rng.gauss(0, 50), "cand_atr": 12 + rng.random(),
                 "cand_bar_range": 10 + 5 * rng.random(), "opt_score": score, "opt_delta": -0.5 * rng.random(),
                 "opt_iv": 0.12, "opt_spread_pct": 0.3 * rng.random(), "opt_ask": 80.0, "opt_eligible": 3}
            if sid == "orb":
                f |= {"cand_or_high": 24050.0, "cand_or_low": 23950.0, "cand_or_width_pct": 0.4}
            else:
                f |= {"cand_ema9": 24010.0, "cand_ema21": 24000.0, "cand_prev_high": 24020.0,
                      "cand_prev_low": 24005.0, "cand_adx": 30.0, "cand_rsi": 55.0}
            rows.append(dict(run_id=run_id, session_date=d, signal_seq=k, signal_ts=f"{d}T09:{20 + k:02d}:00+05:30",
                             strategy_id=sid, cycle_key=f"{sid}:{k}", direction=f["direction"],
                             signal_outcome="APPROVED", reject_reasons="[]", traded=1, contract="NIFTY X",
                             dte=f["dte"], horizon_min=15, stop_pct=20.0, target_pct=30.0, label=label,
                             label_reason=label, entry_ts=None, entry_px=80.0, exit_ts=None, exit_px=90.0,
                             gross_pnl_per_lot=net + 60, charges_per_lot=60.0, net_pnl_per_lot=round(net, 2),
                             r_multiple=net / 400, features=json.dumps(f), data_tier=tier, label_version="label-1"))
    return rows


def _db(tmp_path, rows, name="t.db"):
    e = make_engine(tmp_path / name)
    init_db(e)
    with Session(e) as s:
        s.add_all(TradeLabels(**r) for r in rows)
        s.commit()
    return e


# ------------------------------------------------------------------------------------------ dataset
def test_dataset_excludes_unresolved_and_has_no_outcome_features(tmp_path):
    rows = _labels(5, 6)
    rows[0]["label"], rows[1]["label"] = "NO_ENTRY", "UNLABELLABLE"
    e = _db(tmp_path, rows)
    labels = ds.load_labels(e, 15)
    assert len(labels) == len(rows) - 2
    X, y, meta = ds.build(labels)
    assert set(X.columns) == set(ds.NUMERIC) | set(ds.CATEGORICAL)
    assert not set(X.columns) & set(ds.OUTCOME_COLUMNS)
    assert ds.leakage_report(X)["ok"]
    assert list(meta["session_date"]) == sorted(meta["session_date"])           # chronological order
    # absolute price levels are never inputs; only relative transforms
    assert "cand_close" not in X.columns and X["cand_atr_pct"].between(0, 1).all()
    assert X.loc[meta["strategy_id"] == "orb", "cand_close_vs_or"].notna().all()
    assert X.loc[meta["strategy_id"] == "pullback", "cand_ema_gap_atr"].notna().all()


def test_leakage_report_flags_outcome_columns():
    X = pd.DataFrame({"opt_score": [0.1], "net_pnl_per_lot": [5.0], "strategy_id": ["orb"]})
    rep = ds.leakage_report(X)
    assert not rep["ok"] and rep["forbidden_features"] == ["net_pnl_per_lot"]


def test_tier_filter_and_fingerprint(tmp_path):
    e = _db(tmp_path, _labels(4, 3, tier="history_x") + _labels(4, 3, run_id="r2"))
    assert len(ds.load_labels(e, 15, ("observed_feed",))) == 12
    a = ds.load_labels(e, 15)
    assert ds.fingerprint(a) == ds.fingerprint(a.sample(frac=1, random_state=3))   # order-independent


# ------------------------------------------------------------------------------------------ CV
def test_walk_forward_folds_are_chronological_and_disjoint():
    s = _sessions(60)
    folds = walk_forward_folds(s, TrainConfig(n_folds=5, min_train_sessions=20, embargo_sessions=1))
    assert len(folds) == 5
    tested = []
    for f in folds:
        parts = [f["fit"], f["calib"], f["embargo"], f["test"]]
        assert all(parts[0]) and f["calib"] and f["test"] and len(f["embargo"]) == 1
        flat = [d for p in parts for d in p]
        assert flat == sorted(flat) and len(set(flat)) == len(flat)              # fit < calib < embargo < test
        assert max(f["fit"]) < min(f["calib"]) and max(f["calib"]) < min(f["test"])
        tested += f["test"]
    assert len(tested) == len(set(tested)) == 60 - 21


def test_too_few_sessions_gives_no_folds():
    assert walk_forward_folds(_sessions(22), TrainConfig(n_folds=5, min_train_sessions=20)) == []


def test_future_outcomes_do_not_change_earlier_predictions(tmp_path):
    """Scrambling the last fold's outcomes must not move any earlier fold's predictions or decisions."""
    X, _, meta = ds.build(pd.DataFrame(_labels(60, 8)))
    a = evaluate(X, meta, FAST)
    last = a["folds"][-1]["test"][0]
    meta2 = meta.copy()
    m = meta2["session_date"] >= last
    meta2.loc[m, "net_pnl_per_lot"] = -meta2.loc[m, "net_pnl_per_lot"]
    b = evaluate(X, meta2, FAST)
    oa, ob = a["oof"], b["oof"]
    early = oa["fold"] < oa["fold"].max()
    assert np.allclose(oa.loc[early, "p_win"], ob.loc[early, "p_win"])
    assert (oa.loc[early, "take"] == ob.loc[early, "take"]).all()


# ------------------------------------------------------------------------------------------ model
def test_signal_beats_baseline_and_noise_does_not():
    X, _, meta = ds.build(pd.DataFrame(_labels(80, 10)))
    m = evaluate(X, meta, FAST)["metrics"]
    assert m["brier_model"] < m["brier_baseline"] and m["logloss_model"] < m["logloss_baseline"]
    assert m["net_improvement"] > 0 and m["net_improvement_p05"] > 0
    Xn, _, metan = ds.build(pd.DataFrame(_labels(80, 10, signal=False, seed=5)))
    n = evaluate(Xn, metan, FAST)["metrics"]
    assert n["brier_model"] >= n["brier_baseline"] - 0.005           # no edge to find


def test_calibrator_roundtrip_and_ece():
    rng = np.random.default_rng(0)
    raw = rng.random(500)
    y = (rng.random(500) < raw ** 2).astype(int)
    for method in ("sigmoid", "isotonic"):
        c = Calibrator(method).fit(raw, y)
        c2 = Calibrator.from_dict(json.loads(json.dumps(c.to_dict())))
        assert np.allclose(c.predict(raw), c2.predict(raw))
    assert ece(np.full(4, 0.5), np.array([0, 1, 0, 1])) == 0
    assert ece(np.full(4, 0.9), np.array([0, 0, 0, 0])) == pytest.approx(0.9)
    assert Calibrator("sigmoid").fit(raw[:5], np.zeros(5, int)).method == "constant"


def test_breakeven_threshold():
    assert breakeven_p(np.array([400.0, -400.0])) == pytest.approx(0.5)
    assert breakeven_p(np.array([300.0, -600.0])) == pytest.approx(2 / 3)
    assert breakeven_p(np.array([-1.0])) == 1.0 and breakeven_p(np.array([1.0])) == 0.0


def test_gate_requires_every_check():
    good = {"sessions": 40, "brier_model": .2, "brier_baseline": .25, "logloss_model": .6, "logloss_baseline": .69,
            "ece_model": .03, "net_improvement": 100.0, "net_improvement_p05": 5.0}
    ok = dict(adequacy={"ready": True}, prospective_only=True, leakage={"ok": True})
    assert promotion_gate(**ok, metrics=good)["passed"]
    for k, bad in [("ece_model", .2), ("net_improvement", -1.0), ("net_improvement_p05", -1.0),
                   ("brier_model", .3), ("sessions", 10)]:
        assert not promotion_gate(**ok, metrics=good | {k: bad})["passed"], k
    assert not promotion_gate(**ok | {"prospective_only": False}, metrics=good)["passed"]
    assert not promotion_gate(**ok | {"adequacy": {"ready": False}}, metrics=good)["passed"]
    assert not promotion_gate(**ok, metrics=None)["passed"]


# ------------------------------------------------------------------------------------------ registry
def test_history_model_is_research_only_and_never_approvable(tmp_path):
    e = _db(tmp_path, _labels(80, 10, tier="history_without_futures/observed_top_of_book_snapshot"))
    out = train_and_register(e, tiers=None, cfg=FAST, models_dir=tmp_path / "m")
    assert out["status"] == "RESEARCH_ONLY" and "never promotable" in out["status_reason"]
    assert not out["gate"]["checks"]["prospective_data_only"]
    with pytest.raises(RegistryError, match="only a CANDIDATE"):
        approve(e, out["model_version_id"], "anoop", "try", OperatorLog(tmp_path / "ops.jsonl"))
    assert not (tmp_path / "ops.jsonl").exists()


def test_prospective_model_candidate_approve_and_supersede(tmp_path):
    e = _db(tmp_path, _labels(80, 10))
    gate = GateConfig(max_ece=0.1)
    out = train_and_register(e, tiers=("observed_feed",), cfg=FAST, gate_cfg=gate, models_dir=tmp_path / "m")
    assert out["adequacy"]["ready"] and out["gate"]["passed"], out["gate"]
    assert out["status"] == "CANDIDATE"
    with Session(e) as s:
        n = s.query(ModelPredictions).filter_by(model_version_id=out["model_version_id"], kind="oof").count()
    assert n == out["metrics"]["rows"]
    # artifact is JSON, hashed, and reproduces the registry's feature contract
    mm = MetaModel.load(out["artifact"])
    X, _, meta = ds.build(ds.load_labels(e, 15))
    p = mm.predict(X)
    assert ((p >= 0) & (p <= 1)).all() and p[X["opt_score"] > .5].mean() > p[X["opt_score"] <= .5].mean()
    log = OperatorLog(tmp_path / "ops.jsonl")
    with pytest.raises(Exception):
        approve(e, out["model_version_id"], "", "no actor", log)
    r = approve(e, out["model_version_id"], "anoop", "gate passed", log)
    assert r["status"] == "APPROVED" and log.records()[-1]["action"] == "approve_meta_model"
    out2 = train_and_register(e, tiers=("observed_feed",), cfg=FAST, gate_cfg=gate, models_dir=tmp_path / "m")
    approve(e, out2["model_version_id"], "anoop", "newer", log)
    st = {v["version"]: v["status"] for v in list_versions(e)}
    assert st == {1: "RETIRED", 2: "APPROVED"}


def test_tampered_artifact_blocks_approval(tmp_path):
    e = _db(tmp_path, _labels(80, 10))
    out = train_and_register(e, tiers=("observed_feed",), cfg=FAST, gate_cfg=GateConfig(max_ece=0.1),
                             models_dir=tmp_path / "m")
    assert out["status"] == "CANDIDATE"
    with open(out["artifact"], "a", encoding="utf-8") as f:
        f.write(" ")
    assert not verify_artifact(e, out["model_version_id"])
    with pytest.raises(RegistryError, match="sha256"):
        approve(e, out["model_version_id"], "anoop", "x", OperatorLog(tmp_path / "ops.jsonl"))


def test_trading_path_does_not_use_the_meta_model():
    """Rule-only until an approved model AND reviewed shadow wiring exist."""
    import pathlib
    from app.pipeline.decision import PipelineConfig
    assert "rule_only" in str(PipelineConfig().meta_label)
    root = pathlib.Path(__file__).resolve().parents[1] / "app"
    for sub in ("engine", "strategies", "risk", "backtest", "pipeline"):
        for f in (root / sub).rglob("*.py"):
            assert "app.ml" not in f.read_text(encoding="utf-8"), f
