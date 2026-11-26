"""Phase 6 acceptance: same experiment reruns reproduce metrics; hold-out access is logged (and single-use);
failed experiments are retained. Plus spec / split / metric / registry unit tests."""
from __future__ import annotations

import hashlib
import json
import random
from datetime import date, timedelta

import pandas as pd
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import PROJECT_ROOT
from app.database.db import init_db, make_engine
from app.database.models import ExperimentResults, Experiments, HoldoutAccessLog, Holdouts, StrategyTrials
from app.research import metrics as M
from app.research import runner as R
from app.research.registry import (HoldoutError, current_holdout, record_exposure, register_trials,
                                   request_holdout)
from app.research.simulate import VariantRun
from app.research.spec import ExperimentSpec, SpecError, WalkForward, build_strategies, variant_id
from app.research.splits import holdout_size, propose_holdout, walk_forward

SPEC_FILE = PROJECT_ROOT / "config" / "experiments" / "orb_invalidation_v1.yaml"


def sessions(n: int, start: date = date(2026, 1, 1)) -> list[str]:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


@pytest.fixture
def db(tmp_path):
    e = make_engine(tmp_path / "research.db")
    init_db(e)
    return e


# ====================================================================== spec
def test_example_spec_loads_with_six_variants():
    s = ExperimentSpec.load(SPEC_FILE)
    v = s.variants()
    assert len(v) == 6 and all(k.startswith("v_") for k in v)
    assert {p["orb.invalidation"] for p in v.values()} == {"range_edge", "range_mid", "opposite_edge"}


def test_variant_ids_are_content_hashes_independent_of_order():
    assert variant_id({"a.x": 1, "b.y": 2}) == variant_id({"b.y": 2, "a.x": 1})
    assert variant_id({"a.x": 1}) != variant_id({"a.x": 2})
    d = ExperimentSpec.load(SPEC_FILE).to_dict()
    d2 = {**d, "grid": dict(reversed(list(d["grid"].items())))}
    assert ExperimentSpec.from_dict(d).variants().keys() == ExperimentSpec.from_dict(d2).variants().keys()


def test_spec_hash_changes_with_content():
    d = ExperimentSpec.load(SPEC_FILE).to_dict()
    assert ExperimentSpec.from_dict(d).spec_hash == ExperimentSpec.from_dict(json.loads(json.dumps(d))).spec_hash
    assert ExperimentSpec.from_dict({**d, "hypothesis": "other"}).spec_hash != ExperimentSpec.from_dict(d).spec_hash


@pytest.mark.parametrize("patch,msg", [
    ({"grid": {"orb.nope": [1]}}, "unknown parameter"),
    ({"grid": {"pullback.min_adx": [20.0]}, "strategies": ["orb"]}, "not enabled"),
    ({"grid": {"orb.confirm_atr": ["x"]}}, "expected float"),
    ({"grid": {"orb.normal.nope": [1]}}, "exit-template"),
    ({"grid": {"orb.confirm_atr": []}}, "non-empty"),
    ({"surprise": 1}, "unknown spec keys"),
    ({"selection_metric": "luck"}, "selection_metric"),
    ({"strategies": ["martingale"]}, "unknown strategy"),
    ({"walk_forward": {"train_sessions": 0}}, "positive"),
])
def test_bad_specs_rejected(patch, msg):
    d = {"name": "t", "hypothesis": "h", "strategies": ["orb", "pullback"]} | patch
    with pytest.raises(SpecError, match=msg):
        ExperimentSpec.from_dict(d)


def test_params_are_applied_including_exit_templates():
    orb, pb = build_strategies(("orb", "pullback"), {"orb.confirm_atr": 0.5, "orb.normal.stop_pct": 18.0,
                                                     "pullback.min_adx": 25}, {"require_futures": False})
    assert orb.cfg.confirm_atr == 0.5 and orb.cfg.normal.stop_pct == 18.0 and orb.cfg.normal.target_pct == 30.0
    assert pb.cfg.min_adx == 25 and not orb.cfg.require_futures and not pb.cfg.require_futures


# ====================================================================== splits
def test_holdout_is_last_20pct_min_20_and_fixed_after_retirement():
    s = sessions(59)
    assert holdout_size(59) == 20 and holdout_size(200) == 40
    assert propose_holdout(s, None) == s[-20:]
    assert propose_holdout(sessions(39), None) is None              # dev would be smaller than hold-out
    assert propose_holdout(s, s[-1]) is None                        # retired: needs fresh sessions after it
    more = sessions(110)
    assert propose_holdout(more, s[-1]) == more[-holdout_size(110):]


def test_walk_forward_rolling_and_anchored():
    s = sessions(39)
    f = walk_forward(s, WalkForward(15, 8, 8))
    assert [(x.train[0], x.test[0], len(x.train), len(x.test)) for x in f] == \
        [(s[0], s[15], 15, 8), (s[8], s[23], 15, 8), (s[16], s[31], 15, 8)]
    a = walk_forward(s, WalkForward(15, 8, 8, anchored=True))
    assert all(x.train[0] == s[0] for x in a) and len(a[2].train) == 31
    for x in f:
        assert max(x.train) < min(x.test)                            # strictly chronological
    with pytest.raises(ValueError):
        walk_forward(s, WalkForward(10, 8, 4))


# ====================================================================== metrics
def trades_df(rows):
    base = {"trade_id": 0, "strategy": "orb", "dte": 2, "gross_pnl": 0.0, "charges": 50.0, "risk_inr": 1000.0,
            "entry_px": 100.0, "exit_px": 100.0, "qty": 65, "exit_reason": "TIME_STOP", "entry_ns": 0, "exit_ns": 0,
            "low_fidelity": True}
    out = []
    for i, (sess, pnl) in enumerate(rows):
        out.append(base | {"trade_id": i, "session": sess, "net_pnl": pnl, "exit_ns": i,
                           "entry_ns": int(pd.Timestamp(f"{sess} 10:00", tz="Asia/Kolkata").value)})
    return pd.DataFrame(out)


def test_core_metrics_hand_computed():
    s = sessions(4)
    t = trades_df([(s[0], 300.0), (s[0], -100.0), (s[1], -200.0), (s[2], 500.0)])
    m = M.core(t, s)
    assert m["trades"] == 4 and m["net_pnl"] == 500.0
    assert m["profit_factor"] == pytest.approx(800 / 300, rel=1e-4)
    assert m["net_expectancy_inr"] == 125.0 and m["net_expectancy_r"] == 0.125
    assert m["win_rate_pct"] == 50.0
    assert m["max_drawdown_inr"] == 300.0                            # +300 -> 0 after -100, -200
    assert m["largest_session_profit_share"] == 1.0                  # s[2] 500 / total 500
    assert M.core(t.iloc[0:0], s)["trades"] == 0


def test_bootstrap_is_seeded_and_brackets_mean():
    s = sessions(30)
    rng = random.Random(1)
    t = trades_df([(x, rng.gauss(50, 300)) for x in s for _ in range(2)])
    a, b = M.session_bootstrap(t, s, n=300, seed=7), M.session_bootstrap(t, s, n=300, seed=7)
    assert a == b
    lo, hi = a["net_expectancy_inr"]
    assert lo < t["net_pnl"].mean() < hi


def test_deflated_sharpe_penalises_more_trials():
    rng = random.Random(3)
    daily = pd.Series([rng.gauss(30, 100) for _ in range(60)])
    one = M.deflated_sharpe(daily, [0.1], 1)
    many = M.deflated_sharpe(daily, [0.0, 0.1, 0.2, -0.1, 0.3], 50)
    assert one["sr0"] == 0.0 and many["sr0"] > 0 and many["dsr"] < one["dsr"]
    assert not M.deflated_sharpe(pd.Series([1.0] * 5), [], 1)["evaluable"]


def test_promotion_screen_rejects_losing_small_sample():
    s = sessions(24)
    t = trades_df([(x, -100.0) for x in s])
    oos = M.core(t, s)
    seg = M.segmented(t, s, {x: "range" for x in s})
    scr = M.promotion_screen(oos, seg, M.session_bootstrap(t, s, n=100), M.deflated_sharpe(M.daily_pnl(t, s), [], 1),
                             {"quality_failed_days": []}, 0, 0, 6, False)
    assert scr["verdict"] == "REJECT"
    assert {"oos_sample", "net_performance"} <= set(scr["failed"])
    assert scr["gates"]["approval"]["status"] == "HUMAN"


def test_session_regimes_label_only_after_enough_history():
    rows = []
    for i, d in enumerate(sessions(10)):
        o = 24000 + i
        c = o + (300 if i % 3 == 0 else -300 if i % 3 == 1 else 5)
        rows += [{"ts": i * 1000, "session": d, "open": o, "high": max(o, c) + 50, "low": min(o, c) - 50, "close": o},
                 {"ts": i * 1000 + 60, "session": d, "open": c, "high": c, "low": c, "close": c}]
    r = M.session_regimes(pd.DataFrame(rows))
    v = list(r.values())
    assert v[:5] == ["unknown"] * 5 and set(v[6:]) <= {"trend_up", "trend_down", "range"}


# ====================================================================== registry
def test_trial_counter_counts_distinct_variants_across_experiments(db):
    assert register_trials(db, "orb+pullback", {"v_a": {}, "v_b": {}}, "e1") == 2
    assert register_trials(db, "orb+pullback", {"v_b": {}, "v_c": {}}, "e2") == 3
    assert register_trials(db, "orb", {"v_a": {}}, "e3") == 1
    with Session(db) as s:
        b = s.scalars(select(StrategyTrials).where(StrategyTrials.variant_id == "v_b")).one()
        assert b.times_tested == 2 and b.first_experiment == "e1"


def test_holdout_sealed_is_stable_and_single_use_with_logging(db):
    s59 = sessions(59)
    h = current_holdout(db, s59)
    assert json.loads(h.sessions) == s59[-20:] and h.status == "SEALED"
    assert current_holdout(db, sessions(70)).id == h.id               # not recomputed as data grows
    with pytest.raises(HoldoutError):
        request_holdout(db, h.id, "e1", "anoop", "")                  # purpose required
    got = request_holdout(db, h.id, "e2", "anoop", "promotion review")
    assert got.status == "RETIRED"
    with pytest.raises(HoldoutError, match="already inspected"):
        request_holdout(db, h.id, "e3", "anoop", "peek again")
    with Session(db) as s:
        log = s.scalars(select(HoldoutAccessLog).order_by(HoldoutAccessLog.id)).all()
        assert [(x.experiment_id, x.granted) for x in log] == [("e1", 0), ("e2", 1), ("e3", 0)]
    assert current_holdout(db, s59) is None                          # no fresh sessions after retirement


def test_accidental_exposure_is_documented_and_retires(db):
    h = current_holdout(db, sessions(59))
    record_exposure(db, h.id, "anoop", "plotted hold-out equity curve by mistake")
    with Session(db) as s:
        row = s.get(Holdouts, h.id)
        assert row.status == "RETIRED" and "plotted" in row.accidental_exposure


def test_documented_exposure_without_retiring_stays_on_record(db):
    h = current_holdout(db, sessions(59))
    record_exposure(db, h.id, "anoop", "aggregate run seen before sealing", retire=False)
    with Session(db) as s:
        row = s.get(Holdouts, h.id)
        assert row.status == "SEALED" and "before sealing" in row.accidental_exposure
        assert s.scalars(select(HoldoutAccessLog)).one().purpose == "exposure (documented)"


# ====================================================================== runner (simulation stubbed)
class FakeCtx:
    label = "synthetic"

    def __init__(self, tmp, n=59):
        self.days = sessions(n)
        rows = []
        for i, d in enumerate(self.days):
            o = 24000 + 10 * i
            rows += [{"ts": i * 1000, "session": d, "open": o, "high": o + 120, "low": o - 120, "close": o},
                     {"ts": i * 1000 + 60, "session": d, "open": o, "high": o, "low": o,
                      "close": o + (200 if i % 3 == 0 else -200 if i % 3 == 1 else 0)}]
        self.index = pd.DataFrame(rows)
        self.fees = type("F", (), {"version": "fees-test"})()
        self.fill = type("X", (), {"version": "fill-test", "__dict__": {"version": "fill-test"}})()
        self.tmp = tmp

    def data_fingerprint(self):
        return {"sha256": "synthetic", "quality_failed_days": [], "sessions": len(self.days)}


def fake_simulate(ctx, variants, days=None, latch_reset=True, on_session=None):
    out = {}
    for vid in variants:
        rng = random.Random(int(hashlib.sha256(vid.encode()).hexdigest()[:8], 16))
        trades = []
        for d in days:
            for k in range(rng.randint(0, 3)):
                trades.append({"trade_id": len(trades) + 1, "session": d, "strategy": "orb", "dte": rng.choice([0, 1, 3]),
                               "net_pnl": round(rng.gauss(20, 400), 2), "gross_pnl": 0.0, "charges": 60.0,
                               "risk_inr": 1200.0, "entry_px": 100.0, "exit_px": 101.0, "qty": 65,
                               "exit_reason": rng.choice(["TARGET", "STOP", "TIME_STOP"]),
                               "entry_ns": int(pd.Timestamp(f"{d} 10:{10 + k}", tz="Asia/Kolkata").value),
                               "exit_ns": int(pd.Timestamp(f"{d} 10:{30 + k}", tz="Asia/Kolkata").value),
                               "low_fidelity": True})
        out[vid] = VariantRun(vid, variants[vid], None, trades=trades, heads=[vid])
    return out


@pytest.fixture
def stubbed(monkeypatch):
    monkeypatch.setattr(R, "simulate", fake_simulate)


def small_spec(**kw) -> ExperimentSpec:
    d = {"name": "t", "hypothesis": "h", "strategies": ["orb", "pullback"],
         "grid": {"orb.invalidation": ["range_edge", "range_mid"], "orb.confirm_atr": [0.25, 0.5]},
         "walk_forward": {"train_sessions": 15, "test_sessions": 8, "step_sessions": 8, "min_train_trades": 5},
         "holdout_variant": {"orb.invalidation": "range_mid"}} | kw
    return ExperimentSpec.from_dict(d)


def test_rerun_reproduces_metrics(db, tmp_path, stubbed):
    ctx = FakeCtx(tmp_path)
    a = R.run_experiment(small_spec(), db, ctx, "tester", out_root=tmp_path, bootstrap_n=200)
    b = R.run_experiment(small_spec(), db, ctx, "tester", out_root=tmp_path, rerun_of=a["experiment_id"],
                         bootstrap_n=200)
    assert a["results_hash"] == b["results_hash"] and b["reproduced"] == 1
    with Session(db) as s:
        assert s.get(Experiments, b["experiment_id"]).reproduced == 1
        assert s.get(Experiments, a["experiment_id"]).status == "COMPLETED"
        n = s.scalars(select(ExperimentResults).where(ExperimentResults.experiment_id == a["experiment_id"])).all()
        assert len(n) == 4 * (1 + 2 * 3) + 1                         # full_dev + 3 folds x (train,test) + wf_oos
    assert (tmp_path / a["experiment_id"] / "report.md").read_text(encoding="utf-8").startswith("# Experiment")
    assert a["sessions"]["holdout"] == [ctx.days[-20], ctx.days[-1], 20]


def test_changed_result_is_flagged_not_reproduced(db, tmp_path, monkeypatch):
    ctx = FakeCtx(tmp_path)
    monkeypatch.setattr(R, "simulate", fake_simulate)
    a = R.run_experiment(small_spec(), db, ctx, "tester", out_root=tmp_path, bootstrap_n=100)

    def drift(*args, **kw):
        out = fake_simulate(*args, **kw)
        for r in out.values():
            for t in r.trades[:1]:
                t["net_pnl"] += 1.0
        return out
    monkeypatch.setattr(R, "simulate", drift)
    b = R.run_experiment(small_spec(), db, ctx, "tester", out_root=tmp_path, rerun_of=a["experiment_id"],
                         bootstrap_n=100)
    assert b["reproduced"] == 0


def test_failed_experiment_is_retained_with_trials_counted(db, tmp_path, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("simulated crash")
    monkeypatch.setattr(R, "simulate", boom)
    with pytest.raises(RuntimeError):
        R.run_experiment(small_spec(), db, FakeCtx(tmp_path), "tester", out_root=tmp_path)
    with Session(db) as s:
        e = s.scalars(select(Experiments)).one()
        assert e.status == "FAILED" and "simulated crash" in e.error and e.finished_at
        assert e.trials_total == 4
        assert len(s.scalars(select(StrategyTrials)).all()) == 4      # variants still count


def test_holdout_evaluated_once_logged_and_retired(db, tmp_path, stubbed):
    ctx = FakeCtx(tmp_path)
    r = R.run_experiment(small_spec(), db, ctx, "tester", evaluate_holdout=True, holdout_purpose="review",
                         out_root=tmp_path, bootstrap_n=100)
    h = r["results"]["holdout"]
    assert h["sessions"] == 20 and h["sessions_range"] == [ctx.days[-20], ctx.days[-1]]
    with pytest.raises(HoldoutError):
        R.run_experiment(small_spec(), db, ctx, "tester", evaluate_holdout=True, holdout_purpose="again",
                         out_root=tmp_path, bootstrap_n=100)
    with Session(db) as s:
        log = s.scalars(select(HoldoutAccessLog)).all()
        assert [x.granted for x in log] == [1, 0]
        assert [e.status for e in s.scalars(select(Experiments).order_by(Experiments.started_at))] == \
            ["COMPLETED", "FAILED"]


def test_holdout_sessions_never_used_in_development(db, tmp_path, monkeypatch):
    seen = []

    def spy(ctx, variants, days=None, **kw):
        seen.append(list(days))
        return fake_simulate(ctx, variants, days=days, **kw)
    monkeypatch.setattr(R, "simulate", spy)
    ctx = FakeCtx(tmp_path)
    R.run_experiment(small_spec(), db, ctx, "tester", out_root=tmp_path, bootstrap_n=100)
    assert not set(seen[0]) & set(ctx.days[-20:]) and len(seen) == 1


def test_walk_forward_selection_uses_train_window_only(db, tmp_path, stubbed):
    r = R.run_experiment(small_spec(), db, FakeCtx(tmp_path), "tester", out_root=tmp_path, bootstrap_n=100)
    for w in r["results"]["walk_forward"]:
        scores = {v: s for v, s in w["train_score"].items() if s is not None}
        if scores:
            assert w["selected"] == max(scores, key=lambda v: (scores[v], v))
