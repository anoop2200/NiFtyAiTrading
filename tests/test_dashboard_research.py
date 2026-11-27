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


def test_live_journal_incremental_and_chain(tmp_path):
    import json as _j
    from app.dashboard import data as D
    p = tmp_path / "2026-11-27.jsonl"
    t0 = 1795751520 * 10**9
    recs = [{"k": "start", "at": "2026-11-27T09:21:58+05:30"},
            {"k": "b", "s": "index", "bar": {"ts": 1795751520, "open": 1, "high": 2, "low": 0.5, "close": 25810.0}}]
    for i, strike in enumerate(range(25500, 26150, 50)):
        for ot in ("CE", "PE"):
            recs.append({"k": "q", "t": t0 + i, "key": f"NIFTY|2026-12-01|{strike}|{ot}", "b": [[100.0, 65]],
                         "a": [[100.5, 65]], "l": 100.2})
    p.write_text("\n".join(_j.dumps(r) for r in recs) + "\n" + '{"k":"q","t":1', encoding="utf-8")  # partial tail
    j = D.LiveJournal(p).refresh()
    assert j.records == len(recs) and len(j.quotes) == 26
    wide, meta = D.live_chain(j, now_ns=t0 + 2 * 10**9)
    assert meta["atm"] == 25800 and meta["spot"] == 25810.0 and len(wide) == 11
    assert wide["CE_spread"].iloc[0] == 0.5 and 1.9 <= wide["CE_age_s"].iloc[0] <= 2.0
    with open(p, "a", encoding="utf-8") as f:                # the partial line completes; one new record
        f.write('000,"key":"NIFTY|2026-12-01|25800|CE","b":[[90.0,65]],"a":[[91.0,65]],"l":90.5}\n')
    j.refresh()
    assert j.records == len(recs) + 1 and j.quotes["NIFTY|2026-12-01|25800|CE"]["l"] == 90.5
    assert list(D.live_bars(j, "index")["close"]) == [25810.0] and D.live_bars(j, "vix").empty


def test_closed_day_is_detected_and_read_from_db(tmp_path, monkeypatch):
    """After the 15:35 closeout the journal is gzipped: the Live page must show the day as CLOSED, not 'not started'."""
    import pandas as pd
    from app.dashboard import data as D
    p = tmp_path / "2026-11-27.jsonl"
    monkeypatch.setattr(D, "live_journal_path", lambda day, run_id=None: p)
    assert not D.live_journal_closed("2026-11-27")             # nothing yet
    p.write_text("{}\n")
    assert not D.live_journal_closed("2026-11-27")             # running
    p.unlink()
    (tmp_path / "2026-11-27.jsonl.gz").write_bytes(b"")
    assert D.live_journal_closed("2026-11-27")                 # closed out
    tr = pd.DataFrame({"session_date": ["2026-11-26", "2026-11-27"], "net_pnl": [1.0, 2.0]})
    ps = pd.DataFrame({"session_date": ["2026-11-27"], "run_id": ["r1"], "status": ["COMPLETED"]})
    monkeypatch.setattr(D, "trades", lambda e, r: tr)
    monkeypatch.setattr(D, "signals", lambda e, r, d=None: pd.DataFrame({"x": [1]}))
    monkeypatch.setattr(D, "paper_sessions", lambda e: ps)
    d = D.closed_day(None, "2026-11-27", "r1")
    assert list(d["trades"]["net_pnl"]) == [2.0] and d["session"]["status"] == "COMPLETED" and len(d["signals"]) == 1
    assert D.closed_day(None, "2026-11-27", "other")["session"] is None
