"""Live start path: index warm-up history, late-start strategy set, pre-flight helpers, smoke isolation."""
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from app.clock import IST
from app.engine import live_history as lh
from app.engine.feeds import prior_daily, warmup_bars
from app.historical import vix as store
from app.market.calendar import TradingCalendar

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import preflight  # noqa: E402
import run_paper  # noqa: E402


def _day_bars(day: str, n: int = 375, base: float = 25000.0) -> pd.DataFrame:
    t0 = int(datetime.fromisoformat(f"{day}T09:15:00").replace(tzinfo=IST).timestamp())
    return pd.DataFrame([{"ts": t0 + 60 * i, "session": day, "open": base + i, "high": base + i + 2,
                          "low": base + i - 2, "close": base + i + 1, "complete": True} for i in range(n)])


def test_needed_sessions_skip_holidays_and_weekends():
    cal = TradingCalendar.load()
    s = lh.needed_sessions(cal, date(2026, 11, 27), 3)
    assert s == ["2026-11-23", "2026-11-25", "2026-11-26"]          # 2026-11-24 is a holiday


def test_index_frame_session_open_and_warmup(tmp_path):
    for d in ("2026-11-25", "2026-11-26"):
        store.upsert(tmp_path, _day_bars(d), name=lh.NAME)
    f = lh.index_frame(tmp_path, ["2026-11-25", "2026-11-26"])
    first = f.iloc[0]
    assert first["session_open_ts"] == first["ts"]                 # 09:15 bar starts at the session open (epoch s)
    assert f["session_open_ts"].nunique() == 2 and f["ts"].dtype == "int64"
    pd_ = prior_daily(f, "2026-11-27")
    assert [r["session"] for r in pd_] == ["2026-11-25", "2026-11-26"] and pd_[-1]["high"] == 25000.0 + 374 + 2
    assert len(warmup_bars(f, "2026-11-27", 10)["index"]) == 750
    assert lh.missing(tmp_path, ["2026-11-24", "2026-11-25"]) == ["2026-11-24"]
    cal = TradingCalendar.load()
    assert not lh.stale(cal, date(2026, 11, 27), f) and lh.stale(cal, date(2026, 11, 30), f)
    assert lh.summary(["2026-11-24", "2026-11-25"], f)["missing"] == ["2026-11-24"]


def test_warmup_runs_through_the_indicator_engine(tmp_path):
    """The live warm-up frame must be accepted by the real indicators (ORB's opening range included)."""
    from app.strategies.orb import OpeningRangeBreakout, ORBConfig
    from app.indicators.engine import IndicatorEngine
    store.upsert(tmp_path, _day_bars("2026-11-26"), name=lh.NAME)
    f = lh.index_frame(tmp_path, ["2026-11-26"])
    eng = IndicatorEngine(OpeningRangeBreakout(ORBConfig(require_futures=False)).bindings)
    outs = [eng.on_bar("index", b) for b in warmup_bars(f, "2026-11-27")["index"]]
    or_rows = [r for o in outs for r in o if "opening_range" in r["binding"] and r.get("valid")]
    assert or_rows and or_rows[0]["or_high"] == 25000.0 + 14 + 2


def test_late_fresh_start_drops_orb_only():
    s = ("orb", "pullback")
    at = lambda h, m: datetime(2026, 11, 27, h, m, tzinfo=IST)  # noqa: E731
    assert run_paper.late_start_strategies(s, at(8, 50)) == s
    assert run_paper.late_start_strategies(s, at(9, 17)) == s
    assert run_paper.late_start_strategies(s, at(9, 18)) == ("pullback",)
    assert run_paper.late_start_strategies(("pullback",), at(11, 0)) == ("pullback",)


def test_preflight_target_session():
    cal = TradingCalendar.load()
    assert preflight.target_session(cal, datetime(2026, 11, 26, 22, 0, tzinfo=IST)) == date(2026, 11, 27)
    assert preflight.target_session(cal, datetime(2026, 11, 27, 8, 50, tzinfo=IST)) == date(2026, 11, 27)
    assert preflight.target_session(cal, datetime(2026, 11, 28, 10, 0, tzinfo=IST)) == date(2026, 11, 30)


def test_preflight_report_codes():
    r = preflight.Report()
    r.add("a", preflight.OK, "x")
    assert r.code == 0
    r.add("b", preflight.WARN, "y")
    assert r.code == 1
    r.add("c", preflight.FAIL, {"z": 1})
    assert r.code == 2


def test_smoke_mode_is_isolated_in_source():
    src = (Path(run_paper.__file__)).read_text(encoding="utf-8")
    assert "smoke_" in src and 'rt / "kill.json"' in src and "research_db_path() if smoke" in src
    assert "--smoke needs --duration-min" in src
