"""Strategy book (NIFTY Options Strategy Book - Implementation Specification, 28 Nov 2026).

  * costs and the exit rules reproduce the specification's worked examples and reference P&L
  * every indicator / signal is causal: the replay fast path (one computation per day) equals the live path
  * live option bars are built from traded volume at the exchange trade time and become final after a grace
  * the book runs inside the paper engine: entry at the bar open, executable shadow from ask / bid, Supertrend
    flip exit, closeout persistence under its own run, and an identical book after a crash recovery
"""
from __future__ import annotations

import random

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.book import engine as BE
from app.book.engine import NS, BookEngine
from app.book.exits import CostModel, Position
from app.book.strategies import STRATEGIES, chart_signals
from app.clock import IST


# ---------------------------------------------------------------------------------------------- costs / exits
def test_cost_floor_reproduces_reference_pnl():
    c = CostModel()
    # spec §7.3 Q4: 233.40 -> 198.39 stop, 65 qty: modelled cost < 85 -> floored, net -2,361
    r = c.round_trip(233.40, 198.39, 65, 10_000, 10_000)
    assert r["floored"] and r["total"] == 85.0
    assert round((198.39 - 233.40) * 65 - r["total"]) == -2361
    # illiquid bar (< 500 volume): 3 ticks of slippage instead of 1
    assert c.round_trip(100, 100, 65, 100, 100)["parts"]["slippage"] == pytest.approx(0.15 * 2 * 65)
    assert c.round_trip(100, 100, 65, 1000, 1000)["parts"]["slippage"] == pytest.approx(0.05 * 2 * 65)


def _bar(ts, o, h, l, c):
    return {"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": 1000}


T0 = int(pd.Timestamp("2026-09-02 10:00", tz=IST).timestamp())


def test_x1_and_e5_trail_match_worked_examples():
    p = Position("k", "CE", "X1", T0, 250.0, 65)
    assert p.stop == pytest.approx(212.5)
    p.on_bar(_bar(T0, 250, 287.5, 249, 280), 600)
    assert p.stop == pytest.approx(244.375) and p.armed
    p.on_bar(_bar(T0 + 60, 290, 400, 289, 390), 601)
    assert p.stop == pytest.approx(340.0)
    e = Position("k", "CE", "E5", T0, 250.0, 65)
    e.on_bar(_bar(T0, 250, 300, 249, 290), 600)
    assert e.stop == pytest.approx(250.0) and not e.armed          # break-even at +20%
    e.on_bar(_bar(T0 + 60, 300, 350, 299, 340), 601)
    assert e.stop == pytest.approx(262.5) and e.armed              # trail from +40%: 25% below the high
    e.on_bar(_bar(T0 + 120, 350, 500, 349, 480), 602)
    assert e.stop == pytest.approx(375.0)


def test_exit_check_order_is_pessimistic():
    # the entry bar itself can stop out (at the stop price); a gap through the stop fills at the open
    p = Position("k", "CE", "X1", T0, 200.0, 65)
    assert p.on_bar(_bar(T0, 200, 260, 160, 250), 600) == (T0, 170.0, "SL")
    p = Position("k", "CE", "X1", T0, 200.0, 65)
    p.on_bar(_bar(T0, 200, 205, 195, 200), 600)
    assert p.on_bar(_bar(T0 + 60, 150, 210, 140, 200), 601) == (T0 + 60, 150.0, "SL")
    # a stop raised by a bar's high only acts from the next bar
    p = Position("k", "CE", "X1", T0, 200.0, 65)
    assert p.on_bar(_bar(T0, 200, 300, 200, 210), 600) is None and p.stop == pytest.approx(255.0)
    assert p.on_bar(_bar(T0 + 60, 260, 262, 250, 255), 601) == (T0 + 60, 255.0, "TRAIL_SL")
    # flip: not on the entry bar; from the first bar starting at/after the flip candle close; 15:15 square-off first
    p = Position("k", "CE", "X2", T0, 200.0, 65, flip_ts=T0)
    assert p.on_bar(_bar(T0, 200, 205, 195, 200), 600) is None
    assert p.on_bar(_bar(T0 + 60, 201, 205, 195, 200), 601) == (T0 + 60, 201.0, "ST_FLIP")
    p = Position("k", "CE", "X2", T0, 200.0, 65, flip_ts=T0)
    assert p.on_bar(_bar(T0 + 60, 201, 205, 100, 200), 915)[2] == "SQUARE_OFF"


# ---------------------------------------------------------------------------------------------- causality
def _walk(days=8, seed=3):
    rnd = random.Random(seed)
    rows, px = [], 24000.0
    for d in pd.bdate_range("2026-08-03", periods=days):
        t0 = int(pd.Timestamp(f"{d.date()} 09:15", tz=IST).timestamp())
        for i in range(375):
            o = px
            px += rnd.gauss(0, 6) + (8 if (i // 40) % 3 == 0 else -4)    # bursts so that conditions do fire
            rows.append({"ts": t0 + 60 * i, "open": o, "high": max(o, px) + rnd.random() * 3,
                         "low": min(o, px) - rnd.random() * 3, "close": px, "session": str(d.date())})
    return pd.DataFrame(rows)


def _m1(df):
    idx = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert(IST)
    return pd.DataFrame(df[["open", "high", "low", "close"]].to_numpy(), columns=["open", "high", "low", "close"],
                        index=pd.DatetimeIndex(idx))


@pytest.mark.parametrize("tf", [1, 3, 5])
def test_signals_are_causal_so_fast_replay_equals_live(tf):
    m1 = _m1(_walk())
    codes = [c for c, s in STRATEGIES.items() if s.tf == tf]
    full_df, full = chart_signals(m1, tf, codes)
    from app.book.indicators import resample
    from app.book.strategies import Q_RULES, _q_condition
    names = sorted({n for c in codes if c in Q_RULES for n in [Q_RULES[c][0], *Q_RULES[c][1]]})
    fcache: dict = {}
    fcond = {n: _q_condition(n, m1, resample(m1, tf), tf, fcache) for n in names}
    varied = 0
    for cut in range(len(full_df) - 120, len(full_df), 7):            # last day, every 7th candle
        start = full_df.index[cut]
        pm1 = m1[m1.index < start + pd.Timedelta(minutes=tf)]
        part_df, part = chart_signals(pm1, tf, codes)
        assert part_df.index[-1] == start
        for c in codes:
            for k in ("signal", "st"):
                assert np.array_equal(np.asarray(part[c][k]), np.asarray(full[c][k][:cut + 1])), (c, k, start)
        pcache: dict = {}
        for n in names:                                               # every single condition, both sides
            p = _q_condition(n, pm1, resample(pm1, tf), tf, pcache)
            for side in (0, 1):
                assert np.array_equal(p[side], fcond[n][side][:cut + 1]), (n, side, start)
    for n in names:                                                   # the comparison is not vacuous
        varied += int(fcond[n][0].any() or fcond[n][1].any())
    assert varied >= len(names) - 2


# ---------------------------------------------------------------------------------------------- live option bars
def test_option_bars_from_traded_volume_at_exchange_time():
    e = BookEngine("2026-09-02", 65, ["Q8"], {})
    k = "NIFTY|2026-09-08|24000|CE"
    m = T0
    e.on_quote(k, (m + 1) * NS, 100.0, 99.9, 100.1, volume=5000, ltt=m - 30)     # first sight: earlier trade
    e.on_quote(k, (m + 2) * NS, 101.0, 100.9, 101.1, volume=5065, ltt=m + 2)     # trade
    e.on_quote(k, (m + 3) * NS, 101.0, 100.0, 101.2, volume=5065, ltt=m + 2)     # depth change only
    e.on_quote(k, (m + 20) * NS, 99.0, 98.9, 99.1, volume=5195, ltt=m + 20)
    e.on_quote(k, (m + 61) * NS, 105.0, 104.9, 105.1, volume=5260, ltt=m + 59)   # late report of minute m
    e.on_time((m + 62) * NS)
    assert not e.bars.get(k)                                                     # still inside the grace
    e.on_time((m + 64) * NS)
    assert e.bars[k] == [{"ts": m, "open": 101.0, "high": 105.0, "low": 99.0, "close": 105.0, "volume": 260}]
    assert e.watermark == m


# ---------------------------------------------------------------------------------------------- in the paper engine
@pytest.fixture
def book_session(tmp_path, monkeypatch):
    """Paper engine on the Phase 9 synthetic day with the book on and a stub signal: Q8 (1-min call) fires on the
    10:00 candle; its flip Supertrend turns down on the 10:02 candle (closes 10:03)."""
    import test_phase9 as P9

    def stub(m1, tf, codes):
        df = m1[["open", "high", "low", "close"]].copy()
        mins = np.asarray(df.index.hour * 60 + df.index.minute)
        out = {}
        for c in codes:
            sig = (mins == 600) if c == "Q8" else np.zeros(len(df), bool)
            st = np.where(mins >= 602, -1, 1)
            out[c] = {"signal": sig, "blocked": None, "st": st, "detail": {}}
        return df, out
    monkeypatch.setattr(BE, "chart_signals", stub)
    sp = P9.spec(book={"run_id": "book_t", "codes": ["Q8", "Q1"]})
    return P9, sp


def test_book_trades_inside_paper_engine_and_persists(tmp_path, book_session, engine):
    P9, sp = book_session
    s = P9.new_session(tmp_path, sp=sp, book_warmup={"ts": [], "open": [], "high": [], "low": [], "close": []})
    P9.push(s, P9.EVENTS)
    b = s.book
    assert b is not None and b.index_bars_today > 300
    from app.engine.closeout import closeout
    r = closeout(engine, s)
    assert r["book"]["run_id"] == "book_t" and r["book"]["trades"] == 1
    t = b.trades[0]
    assert t["code"] == "Q8" and t["entry_ts"][11:16] == "10:01" and t["exit_reason"] == "ST_FLIP"
    assert t["exit_ts"][11:16] == "10:03"                                       # first bar at/after the flip close
    assert 90 <= t["entry_px"] <= 210
    opt = {x["ts"]: x for x in b.bars[t["contract"]]}
    ten01 = int(pd.Timestamp("2026-09-02 10:01", tz=IST).timestamp())
    assert t["entry_px"] == opt[ten01]["open"] and t["exit_px"] == opt[ten01 + 120]["open"]
    # executable shadow: live ask at signal detection, live bid at the exit
    assert t["exec_entry_px"] is not None and t["exec_exit_px"] is not None and t["exec_net_pnl"] is not None
    assert t["exec_entry_px"] > t["exec_exit_px"] - 1000
    from app.database.models import BookTrades, CapitalUsage, PaperTrades, TradingSignals
    with Session(engine) as db:
        assert db.scalars(select(PaperTrades).where(PaperTrades.run_id == "book_t")).one().strategy_id == \
            "call_afternoon_scalp"
        bt = db.scalars(select(BookTrades).where(BookTrades.run_id == "book_t")).one()
        assert bt.name == "Call · Afternoon Scalp" and bt.exec_net_pnl == t["exec_net_pnl"]
        assert db.scalars(select(CapitalUsage).where(CapitalUsage.run_id == "book_t")).one().trades == 1
        assert db.scalars(select(TradingSignals).where(TradingSignals.run_id == "book_t")).one().outcome == "APPROVED"
        # the main run is untouched by the book
        assert all(x.strategy_id == "stub" for x in db.scalars(select(PaperTrades).where(PaperTrades.run_id == "t")))


def test_book_recovers_identically_after_crash(tmp_path, book_session):
    P9, sp = book_session
    warm = {"ts": [], "open": [], "high": [], "low": [], "close": []}
    full = P9.new_session(tmp_path / "a", sp=sp, book_warmup=warm)
    P9.push(full, P9.EVENTS)
    full.book.finish()
    in_trade = int(pd.Timestamp("2026-09-02 10:02:10", tz=IST).timestamp()) * NS        # entry 10:01, exit 10:03
    cut = next(i for i, e in enumerate(P9.EVENTS) if e[0] >= in_trade)
    s = P9.new_session(tmp_path / "b", sp=sp, book_warmup=warm)
    P9.push(s, P9.EVENTS[:cut])
    s.journal.close()
    assert s.book.state["Q8"]["pos"] is not None and s.book.state["Q8"]["pos"].open
    r = P9.recover(tmp_path / "b")
    assert r.book is not None and r.book.state["Q8"]["pos"].entry == s.book.state["Q8"]["pos"].entry
    P9.push(r, P9.EVENTS[cut:])
    r.book.finish()
    strip = lambda ts: [{k: v for k, v in t.items() if k != "features"} for t in ts]   # noqa: E731
    assert strip(r.book.trades) == strip(full.book.trades)
    assert [x["outcome"] for x in r.book.signals] == [x["outcome"] for x in full.book.signals]


def test_kill_switch_blocks_book_entries(tmp_path, book_session):
    P9, sp = book_session
    s = P9.new_session(tmp_path, sp=sp, book_warmup={"ts": [], "open": [], "high": [], "low": [], "close": []})
    s.kill.trip("FEED_OUTAGE", "test")
    P9.push(s, P9.EVENTS)
    assert not s.book.trades
    sig = [x for x in s.book.signals if x["code"] == "Q8"]
    assert sig and sig[0]["outcome"] == "NO_TRADE" and sig[0]["reason"].startswith("KILL_SWITCH")
    assert s.book.state["Q8"]["done"]                                     # Q: first qualifying candle ends the day
