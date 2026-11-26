"""Research-tier strategy pre-screen: modelled quotes, lot evidence, summary, isolation, calendars."""
from datetime import date

import pandas as pd
import pytest

from app.backtest.fills import NS
from app.market.calendar import TradingCalendar
from app.screen import ltp

MODEL = {"table": {"0": {0: 0.3, 1: 0.3, 6: 0.4}, "1": {0: 0.25}, "2-3": {0: 0.24}, "4+": {0: 0.22, 6: 0.26}},
         "sha256": "x"}


def _bars(close=100.0, expiry="2026-12-01", strike=25750.0, spot=25750.0, n=3, lot=65):
    """`lot`: open interest is a multiple of it on every row (varied, so the GCD is the lot itself)."""
    return pd.DataFrame([{"ts": 1795578300 + 60 * i, "session": "2026-11-25", "expiry_flag": "WEEK",
                          "expiry_code": 1, "rel_strike": 0, "option_type": "CE", "strike": strike,
                          "open": close, "high": close + 1, "low": close - 1, "close": close + i, "volume": 10,
                          "oi": lot * (3, 7, 11)[i % 3], "iv": 14.0, "spot": spot, "expiry": expiry} for i in range(n)])


def test_quotes_straddle_the_trade_price_and_arrive_at_bar_end():
    qs = ltp.modelled_quotes(_bars(), MODEL, 1.0, lot=65, quote_lots=20)
    assert len(qs) == 3
    q = qs[0]
    assert q.ts_ns == (1795578300 + 60) * NS                 # the close is known only when the minute ends
    assert q.bid < 100.0 < q.ask and not q.has_depth and q.tier == ltp.TIER
    assert q.bids[0][1] == q.asks[0][1] == 65 * 20
    assert q.key == "NIFTY|2026-12-01|25750|CE"
    assert round(q.bid / 0.05, 6).is_integer() and round(q.ask / 0.05, 6).is_integer()


def test_spread_scales_with_multiplier_dte_and_moneyness():
    w = lambda qs: qs[0].ask - qs[0].bid  # noqa: E731
    base = w(ltp.modelled_quotes(_bars(close=200.0), MODEL, 1.0, 65))
    assert w(ltp.modelled_quotes(_bars(close=200.0), MODEL, 4.0, 65)) > base
    assert ltp.spread_pct(MODEL, 0, 0) > ltp.spread_pct(MODEL, 5, 0)          # expiry day wider
    assert ltp.spread_pct(MODEL, 5, 9) == ltp.spread_pct(MODEL, 5, 6)         # moneyness capped
    assert ltp.spread_pct(MODEL, 2, 0) == 0.24
    tiny = ltp.modelled_quotes(_bars(close=0.5), MODEL, 1.0, 65)[0]
    assert tiny.ask - tiny.bid >= 0.05 - 1e-9 and tiny.bid >= 0.05             # at least one tick


def test_rows_without_expiry_are_skipped():
    assert ltp.modelled_quotes(_bars(expiry=None), MODEL, 1.0, 65) == []


def test_lot_from_open_interest_or_revision_date():
    assert ltp.lot_for_day(_bars(lot=75), "2025-12-01") == (75, "oi_gcd")
    assert ltp.lot_for_day(_bars(lot=65), "2026-02-01") == (65, "oi_gcd")
    lot, how = ltp.lot_for_day(_bars(lot=5), "2025-12-29")     # roll week: mixed lots -> GCD 5
    assert lot == 75 and how.startswith("revision_date")
    assert ltp.lot_for_day(_bars(lot=5), "2026-03-25")[0] == 65


def test_summarize_and_vix_breakdown():
    trades = [{"session": f"2026-0{1 + i % 3}-0{1 + i % 5}", "cycle": f"c{i}", "strategy": "orb" if i % 2 else "pb",
               "net_pnl": (100.0 if i % 3 == 0 else -50.0), "gross_pnl": 0.0, "charges": 40.0,
               "exit_reason": "TIME_STOP"} for i in range(12)]
    signals = [{"outcome": "APPROVED", "cycle_key": t["cycle"], "session": t["session"],
                "payload": {"market": {"vix": 10 + i}}} for i, t in enumerate(trades)]
    s = ltp.summarize(trades, signals, 10)
    assert s["trades"] == 12 and s["net_pnl"] == 4 * 100 - 8 * 50
    assert set(s["by_strategy"]) == {"orb", "pb"} and sum(v["trades"] for v in s["by_vix_tercile"].values()) == 12
    assert s["max_drawdown"] <= 0 and s["exit_reasons"] == {"TIME_STOP": 12}
    assert ltp.summarize([], [], 3) == {"sessions": 3, "trades": 0, "net_pnl": 0.0}


def test_screen_is_isolated_from_trading_and_evidence_code():
    import pathlib
    root = pathlib.Path(__file__).resolve().parents[1] / "app"
    for sub in ("engine", "strategies", "risk", "backtest", "pipeline", "research", "ml", "labels"):
        for f in (root / sub).rglob("*.py"):
            src = f.read_text(encoding="utf-8")
            assert "app.screen" not in src and "rolling_options" not in src, f


def test_calendars_cover_the_screen_year():
    cal = TradingCalendar.load()
    assert cal.covers(date(2025, 12, 1)) and not cal.is_trading_day(date(2025, 12, 25))
    assert not cal.is_trading_day(date(2026, 1, 15))          # closure missing from the published lists
    assert not cal.is_trading_day(date(2025, 10, 21))         # Muhurat-only day
    assert cal.is_trading_day(date(2025, 11, 26))
