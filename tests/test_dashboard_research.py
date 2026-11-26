"""Dashboard additions: meta-model registry, India VIX, OI-03 research tier — all read-only."""
import json

import pandas as pd
import pytest
from sqlalchemy.orm import Session

from app.dashboard import data as D
from app.database.db import make_ro_engine
from app.database.models import TradingSignals
from app.historical import rolling_options as ro
from app.historical import vix as vixh
from app.ml.registry import train_and_register
from test_phase7 import FAST, _db, _labels
from test_rolling_options import _body


def test_model_registry_views_on_read_only_engine(tmp_path):
    e = _db(tmp_path, _labels(80, 10, tier="history_x"))
    out = train_and_register(e, tiers=None, cfg=FAST, models_dir=tmp_path / "m")
    ro_e = make_ro_engine(tmp_path / "t.db")
    mv = D.model_versions(ro_e)
    assert len(mv) == 1 and mv.iloc[0]["status"] == "RESEARCH_ONLY"
    assert "prospective_data_only" in mv.iloc[0]["gate_failed"] and mv.iloc[0]["data_tiers"] == "history_x"
    folds = D.model_folds(ro_e, out["model_version_id"])
    assert len(folds) == FAST.n_folds and {"test", "brier", "net_model"} <= set(folds.columns)
    cal = D.model_calibration(ro_e, out["model_version_id"])
    assert cal["n"].sum() == out["metrics"]["rows"] and cal["predicted"].between(0, 1).all()


def test_model_registry_absent_tables_give_empty(tmp_path):
    from app.database.db import make_engine
    from sqlalchemy import text
    e = make_engine(tmp_path / "old.db")
    with e.begin() as c:
        c.execute(text("CREATE TABLE x (a int)"))
    assert D.model_versions(make_ro_engine(tmp_path / "old.db")).empty


def test_signals_view_shows_recorded_vix(tmp_path):
    e = _db(tmp_path, [])
    with Session(e) as s:
        for i, mkt in enumerate([{"vix": 13.4, "vix_bar_ts": 1}, {"vix": None}]):
            s.add(TradingSignals(run_id="r", session_date="2026-11-25", seq=i, ts="t", strategy_id="orb",
                                 outcome="APPROVED", reasons="[]", payload=json.dumps({"market": mkt})))
        s.commit()
    df = D.signals(make_ro_engine(tmp_path / "t.db"), "r")
    assert "payload" not in df.columns and df["vix"].tolist()[0] == 13.4 and pd.isna(df["vix"].tolist()[1])


def test_vix_views(tmp_path, monkeypatch):
    bars = pd.DataFrame([{"ts": 1795578300 + 60 * i, "session": "2026-11-25", "open": 13 + i / 10,
                          "high": 13.2 + i / 10, "low": 12.9 + i / 10, "close": 13.1 + i / 10, "complete": True}
                         for i in range(5)])
    vixh.upsert(tmp_path, bars)
    monkeypatch.setattr(D, "settings", lambda: type("S", (), {"path": lambda self, k: tmp_path})())
    daily = D.vix_daily()
    assert daily.iloc[0]["close"] == pytest.approx(13.5) and daily.iloc[0]["bars"] == 5
    intr = D.vix_intraday("2026-11-25")
    assert len(intr) == 5 and intr["time"].iloc[0] == "09:15"


def test_rolling_views(tmp_path, monkeypatch):
    day = "2026-11-25"
    df = pd.concat([ro.parse(_body(day, side=s, n=375), day, "WEEK", 1, 0, ot)
                    for s, ot in (("ce", "CALL"), ("pe", "PUT"))])
    ro.write_day(tmp_path, day, df)
    ro.write_manifest(tmp_path, {"days": {day: {"status": "ok", "rows": len(df), "series": 2},
                                          "2026-11-24": {"status": "empty", "rows": 0, "series": 0}},
                                 "expiries": {}})
    ro.finalize(tmp_path)
    monkeypatch.setattr(D, "settings", lambda: type("S", (), {"path": lambda self, k: tmp_path})())
    r = D.rolling_coverage()
    assert r["coverage"]["trading_days"] == 1 and r["coverage"]["finalized"] and r["days"] == [day]
    assert r["by_month"].iloc[0]["holidays_or_empty"] == 1 and set(r["expiries"]["series"]) == {"WEEK1"}
    d = D.rolling_day(day)
    assert set(d["option_type"]) == {"CE", "PE"} and d["time"].iloc[0] == "09:15" and d["expiry"].notna().all()
