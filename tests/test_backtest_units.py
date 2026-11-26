import pytest

from app.backtest.audit import AuditLog
from app.backtest.broker import resolve_bar_exit
from app.backtest.fees import FeeSchedule
from app.backtest.fills import FillConfig, Quote, attempt_fill


def q(bids, asks, ts=0, depth=True):
    return Quote(ts, "K", tuple(bids), tuple(asks), has_depth=depth)


def test_never_uses_ltp_and_needs_a_side():
    r = attempt_fill("SELL", 65, Quote(0, "K", (), ((100.0, 65),), ltp=105.0), 0, FillConfig(), {})
    assert r.qty == 0 and r.reason == "SIDE_WITHDRAWN"


def test_crossed_and_stale_books_refused():
    assert attempt_fill("BUY", 65, q([(101, 65)], [(100, 65)]), 0, FillConfig(), {}).reason == "CROSSED_BOOK"
    assert attempt_fill("BUY", 65, q([(99, 65)], [(100, 65)], ts=0), 4 * 10**9, FillConfig(), {}).reason == "STALE_QUOTE"


def test_price_protection_stops_walking_the_ladder():
    r = attempt_fill("BUY", 195, q([(99.9, 650)], [(100, 65), (100.9, 65), (102, 65)]), 0, FillConfig(), {})
    assert r.filled == [(100, 65), (100.9, 65)] and r.reason == "PRICE_PROTECTION" and r.limit == 101.0


def test_consumed_liquidity_is_not_reused():
    used = {}
    quote = q([(99.9, 650)], [(100, 65)])
    assert attempt_fill("BUY", 65, quote, 0, FillConfig(), used).qty == 65
    assert attempt_fill("BUY", 65, quote, 0, FillConfig(), used).reason == "NO_LIQUIDITY"


def test_fee_schedule_versioned_and_side_specific():
    f = FeeSchedule.load("nse_opt_2024_10_v1")
    buy, sell = f.charges("BUY", 100000), f.charges("SELL", 100000)
    assert buy["stt"] == 0 and buy["stamp"] == 3.0 and sell["stt"] == 100.0 and sell["stamp"] == 0
    assert buy["fee_version"] == "nse_opt_2024_10_v1"
    assert f.charges("BUY", 0, executed=False)["total"] == 0


@pytest.mark.parametrize("bar,expected", [
    ({"open": 100, "high": 135, "low": 75, "close": 110}, ("STOP", 79.9)),     # both touched -> stop first
    ({"open": 100, "high": 135, "low": 95, "close": 110}, ("TARGET", 130.0)),
    ({"open": 70, "high": 75, "low": 65, "close": 72}, ("STOP", 69.6)),        # gap through stop: at open
    ({"open": 100, "high": 110, "low": 95, "close": 105}, None),
])
def test_conservative_intrabar_resolution(bar, expected):
    assert resolve_bar_exit(80.0, 130.0, bar, half_spread=0.3, slippage=0.1) == expected


def test_audit_chain_detects_tampering():
    a = AuditLog()
    a.add(1, "fill", px=100.0, qty=65)
    a.add(2, "fill", px=101.0, qty=65)
    assert AuditLog.verify(a.records)
    a.records[0]["px"] = 99.0
    assert not AuditLog.verify(a.records)
