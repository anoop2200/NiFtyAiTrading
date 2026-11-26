"""OI-03 research tier: Dhan rolling (expired) option history — parsing, storage, expiry inference."""
import asyncio
from datetime import date, datetime

import pytest

from app.clock import IST
from app.data.dhan_rest import RestResult
from app.historical import rolling_options as ro
from app.pricing.bs import price

DAY = "2026-11-25"          # Wednesday; NIFTY weekly expiry Tuesday 2026-12-01


def _ts(day, hh, mm):
    return int(datetime.fromisoformat(day).replace(hour=hh, minute=mm, tzinfo=IST).timestamp())


def _body(day, side="ce", dte_days=6.0, n=300, spot=25750.0, strike=25750.0, iv=14.0):
    """Synthetic rolling response whose prices are consistent with `dte_days` to expiry (close 15:30)."""
    ts, close = [], []
    for i in range(n):
        t = _ts(day, 9, 15) + 60 * i
        ts.append(t)
        left = dte_days + (_ts(day, 15, 30) - t) / 86400
        close.append(round(price(spot, strike, left / 365, iv / 100, "CE" if side == "ce" else "PE"), 2))
    data = {"timestamp": ts, "open": close, "high": [c + 1 for c in close], "low": [c - 1 for c in close],
            "close": close, "volume": [100] * n, "oi": [5000] * n, "iv": [iv] * n, "strike": [strike] * n,
            "spot": [spot] * n}
    return {"data": {side: data, ("pe" if side == "ce" else "ce"): None}}


def test_parse_keeps_session_minutes_and_drops_bad_bars():
    b = _body(DAY, n=380)
    b["data"]["ce"]["timestamp"][0] = _ts(DAY, 9, 14)            # pre-open
    b["data"]["ce"]["low"][5] = 0                                # bad bar
    b["data"]["ce"]["timestamp"].append(_ts("2026-11-26", 9, 15))  # next day (inclusive toDate)
    for k in ("open", "high", "low", "close", "volume", "oi", "iv", "strike", "spot"):
        b["data"]["ce"][k].append(b["data"]["ce"][k][-1])
    df = ro.parse(b, DAY, "WEEK", 1, 0, "CALL")
    assert len(df) == 375 - 2                    # 09:15..15:29, minus the 09:15 bar moved pre-open and the bad bar
    assert set(df["session"]) == {DAY} and set(df["option_type"]) == {"CE"} and (df["rel_strike"] == 0).all()
    assert list(df.columns) == ro.COLUMNS
    assert ro.parse({"data": {"ce": None, "pe": None}}, DAY, "WEEK", 1, 0, "CALL").empty
    assert ro.parse(None, DAY, "WEEK", 1, 0, "PUT").empty


def test_spec_series_and_labels():
    s = ro.RollingSpec(each_side=2)
    assert len(s.series()) == 5 * 2
    assert [ro.rel_label(k) for k in (-2, 0, 3)] == ["ATM-2", "ATM", "ATM+3"]


def test_fetch_day_write_load_roundtrip(tmp_path):
    class Rest:
        def __init__(self):
            self.calls = []

        async def rolling_option(self, uid, flag, code, strike, ot, d1, d2):
            self.calls.append((strike, ot, d1, d2))
            return RestResult(200, _body(DAY, "ce" if ot == "CALL" else "pe"), "", 0, 0)

    rest = Rest()
    df, info = asyncio.run(ro.fetch_day(rest, DAY, ro.RollingSpec(each_side=1)))
    assert len(rest.calls) == 6 and all(c[2] == c[3] == DAY for c in rest.calls)   # one day per request
    assert info["status"] == "ok" and info["series"] == 6
    ro.write_day(tmp_path, DAY, df)
    m = ro.read_manifest(tmp_path)
    m["days"][DAY] = info
    ro.write_manifest(tmp_path, m)
    assert len(ro.load_day(tmp_path, DAY)) == len(df)
    assert ro.coverage(tmp_path)["trading_days"] == 1


def test_implied_days_recovers_time_to_expiry():
    df = ro.parse(_body(DAY, dte_days=6.0, n=375), DAY, "WEEK", 1, 0, "CALL")
    imp = ro.implied_days(df)[("WEEK", 1)]
    assert 6.0 < imp < 7.0                                   # midday: 6 days + the rest of the session


def test_expiry_weekday_switch_and_monthly_candidates():
    assert ro.expiry_weekday(date(2025, 8, 20)) == 3 and ro.expiry_weekday(date(2025, 9, 2)) == 1
    weekly = ro._candidates(date(2025, 11, 26), "WEEK")
    assert weekly[0] == date(2025, 12, 2) and all(c.weekday() == 1 for c in weekly)
    monthly = ro._candidates(date(2025, 11, 26), "MONTH")
    assert monthly[0] == date(2025, 12, 30)              # last Tuesday of Nov 2025 (25th) is already past
    assert all(c.weekday() == 1 and date.fromordinal(c.toordinal() + 7).month != c.month for c in monthly)


def test_infer_expiry_uses_implied_time_and_holiday_shift():
    trading = {"2026-11-25", "2026-11-26", "2026-11-27", "2026-11-30", "2026-12-01", "2026-12-02"}
    e = ro.infer_expiry(DAY, "WEEK", 1, 6.6, trading)
    assert e["expiry"] == "2026-12-01" and e["dte"] == 6
    # Tuesday 2026-12-01 a holiday (no data) -> expiry Monday 2026-11-30
    e = ro.infer_expiry(DAY, "WEEK", 1, 5.6, trading - {"2026-12-01"})
    assert e["expiry"] == "2026-11-30"
    # Thursday era: Monday 2021-09-13 with ~3.4 days implied -> Thursday 2021-09-16
    t21 = {"2021-09-13", "2021-09-14", "2021-09-15", "2021-09-16", "2021-09-17"}
    assert ro.infer_expiry("2021-09-13", "WEEK", 1, 3.4, t21)["expiry"] == "2021-09-16"
    # expiry day itself
    assert ro.infer_expiry("2026-12-01", "WEEK", 1, 0.15, trading)["dte"] == 0
    # no implied time: calendar fallback, labelled
    e = ro.infer_expiry(DAY, "WEEK", 1, None, trading)
    assert e["expiry"] == "2026-12-01" and e["method"] == "calendar_only"


def test_finalize_writes_expiries_and_flags_inconsistency(tmp_path):
    days = {"2026-11-25": 6.0, "2026-12-01": 0.0}
    m = {"days": {}, "expiries": {}}
    for d, dte in days.items():
        df = ro.parse(_body(d, dte_days=dte, n=375), d, "WEEK", 1, 0, "CALL")
        ro.write_day(tmp_path, d, df)
        m["days"][d] = {"status": "ok", "rows": len(df)}
    ro.write_manifest(tmp_path, m)
    r = ro.finalize(tmp_path)
    assert r["problems"] == 0
    loaded = ro.load_day(tmp_path, "2026-11-25")
    assert set(loaded["expiry"]) == {"2026-12-01"} and set(loaded["tier"]) == {ro.TIER}


def test_research_tier_is_not_used_by_the_trading_or_backtest_path():
    import pathlib
    root = pathlib.Path(__file__).resolve().parents[1] / "app"
    for sub in ("engine", "strategies", "risk", "backtest", "pipeline", "research"):
        for f in (root / sub).rglob("*.py"):
            assert "rolling_options" not in f.read_text(encoding="utf-8"), f


@pytest.mark.parametrize("each_side", [0, 10])
def test_spec_bounds(each_side):
    assert len(ro.RollingSpec(each_side=each_side).series()) == (2 * each_side + 1) * 2
