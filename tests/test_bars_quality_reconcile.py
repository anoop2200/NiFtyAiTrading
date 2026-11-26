from datetime import datetime, timedelta

import pyarrow as pa
import pytest
from conftest import full_packet, ticker_packet

from app.clock import IST
from app.data.feed_parser import decode
from app.data.parquet_store import FEED_SCHEMA, PartitionedWriter
from app.marketdata.bars import BarBuilder, bars_table, snapshot_bars
from app.marketdata.normalize import Q
from app.marketdata.pipeline import build_feed_day, minute_dict
from app.marketdata.quality import QualityMatrix
from app.marketdata.reconcile import Tolerance, parse_candles, reconcile

NS = 1_000_000_000
M0 = int(datetime(2026, 11, 26, 10, 0, tzinfo=IST).timestamp())  # minute-aligned epoch seconds


def ev(t, sid=1, ltp=100.0, qty=10, ltt=None, flags=0, bid=None, ask=None, high=None, low=None, oi=None):
    return {"recv_ns": int(t * NS), "security_id": sid, "ltp": ltp, "trade_qty": qty,
            "ltt": ltt if ltt is not None else int(t), "flags": flags, "bid": bid, "ask": ask,
            "spread": round(ask - bid, 4) if bid and ask else None, "day_high": high, "day_low": low, "oi": oi}


def run(events, **kw):
    b = BarBuilder(**kw)
    out = []
    for e in events:
        out += b.on_event(e)
    return out + b.flush(), b


def test_ohlcv_vwap_and_book_fields():
    bars, _ = run([ev(M0 + 1, ltp=100, qty=10, bid=99.9, ask=100.1),
                   ev(M0 + 20, ltp=103, qty=20, bid=102.9, ask=103.2),
                   ev(M0 + 40, ltp=98, qty=10),
                   ev(M0 + 59, ltp=101, qty=10, oi=5000)])
    assert len(bars) == 1
    b = bars[0]
    assert (b["open"], b["high"], b["low"], b["close"], b["volume"], b["trades"]) == (100, 103, 98, 101, 50, 4)
    assert b["vwap"] == pytest.approx((100 * 10 + 103 * 20 + 98 * 10 + 101 * 10) / 50)
    assert (b["bid"], b["ask"], b["quote_updates"], b["oi"]) == (102.9, 103.2, 2, 5000)
    assert b["spread_mean"] == pytest.approx(0.25)


def test_quote_only_events_do_not_set_prices_and_missing_minutes_emit_no_bar():
    bars, _ = run([ev(M0 + 1, ltp=100, qty=5), ev(M0 + 30, ltp=100.5, qty=0, bid=100, ask=101),
                   ev(M0 + 181, ltp=104, qty=5)])
    assert [b["ts"] - M0 for b in bars] == [0, 180]           # minutes 1 and 2 have no bar (no ffill)
    assert bars[0]["close"] == 100 and bars[0]["quote_updates"] == 1


def test_trade_bucketed_by_exchange_time_not_receive_time():
    # traded at 10:00:59 exchange time, received 10:01:00.5 -> belongs to the 10:00 bar
    bars, _ = run([ev(M0 + 10, ltp=100, qty=1), ev(M0 + 60.5, ltp=105, qty=1, ltt=M0 + 59)])
    assert len(bars) == 1 and bars[0]["close"] == 105


def test_implausible_ltt_falls_back_to_receive_time():
    bars, _ = run([ev(M0 + 10, ltp=100, qty=1, ltt=M0 - 3600)])
    assert bars[0]["ts"] == M0


def test_late_event_after_finalization_is_counted_not_applied():
    bars, b = run([ev(M0 + 10, ltp=100, qty=1),
                   ev(M0 + 65, sid=2, ltp=50, qty=1),     # advances watermark past 10:01:02
                   ev(M0 + 66, ltp=101, qty=1, ltt=M0 + 61),
                   ev(M0 + 66.5, ltp=999, qty=1, ltt=M0 + 30)])  # LTT in finalized minute -> late
    first = [x for x in bars if x["security_id"] == 1 and x["ts"] == M0][0]
    assert first["high"] == 100 and b.late_total == 1
    assert [x for x in bars if x["security_id"] == 1 and x["ts"] == M0 + 60][0]["late_events"] == 1


def test_bad_events_excluded_but_counted():
    bars, _ = run([ev(M0 + 1, ltp=100, qty=1),
                   ev(M0 + 2, ltp=500, qty=1, flags=int(Q.OUT_OF_ORDER)),
                   ev(M0 + 3, ltp=101, qty=1, bid=102, ask=101, flags=int(Q.CROSSED))])
    b = bars[0]
    assert b["high"] == 101 and b["flagged_events"] == 2 and b["crossed"] == 1


def test_day_high_low_recovers_extremes_between_packets():
    bars, _ = run([ev(M0 + 1, ltp=100, qty=1, high=101, low=95),
                   ev(M0 + 30, ltp=100.5, qty=1, high=104, low=95),   # 104 traded between packets
                   ev(M0 + 50, ltp=100, qty=1, high=104, low=94)])
    b = bars[0]
    assert (b["high"], b["low"], b["extreme_from_day_range"]) == (104, 94, True)


def test_index_bars_from_ticker_ltp_without_volume():
    bars, _ = run([ev(M0 + 1, sid=13, ltp=24000, qty=0), ev(M0 + 2, sid=13, ltp=24010, qty=0)], index_ids={13})
    assert (bars[0]["open"], bars[0]["close"], bars[0]["volume"]) == (24000, 24010, 0)


def test_streaming_equals_batch():
    events = [ev(M0 + i * 7.3, sid=1 + i % 3, ltp=100 + (i % 11), qty=1 + i % 4,
                 bid=99 + (i % 11), ask=100.5 + (i % 11)) for i in range(400)]
    batch, _ = run(events)
    live = BarBuilder()
    emitted = []
    for e in events:  # as if each event arrived over the socket
        emitted.extend(live.on_event(e))
    emitted.extend(live.flush())
    assert bars_table(batch).equals(bars_table(emitted))
    assert bars_table(batch).num_rows > 0


def test_quality_matrix():
    s, e = M0 * NS, (M0 + 600) * NS
    q = QualityMatrix(s, e, stale_after_s=30)
    for t in range(0, 300, 5):
        q.on_event(ev(M0 + t, qty=1, bid=99, ask=100))
    q.on_event(ev(M0 + 400, flags=int(Q.DUPLICATE), qty=0))
    q.on_event(ev(M0 + 401, flags=int(Q.CROSSED), qty=0, bid=101, ask=100))
    r = q.finalize({1: 6})[0]
    assert r["events"] == 62 and r["flags"] == {"CROSSED": 1, "DUPLICATE": 1}
    assert r["max_change_gap_s"] == pytest.approx(199)        # last change 401 -> session end 600
    assert r["pct_time_fresh"] == pytest.approx(100 * (1 - ((106 - 30) + (199 - 30)) / 600), abs=0.01)
    assert r["mean_spread"] == 1 and r["bar_coverage_pct"] == 60.0


def test_reconcile_tolerance():
    tol = Tolerance(tick=0.05)
    ref = {M0 + 60 * i: {"open": 26000, "high": 26010, "low": 25990, "close": 26005, "volume": 1000}
           for i in range(100)}
    ours = {t: dict(v) for t, v in ref.items()}
    ours[M0]["close"] += 5.1          # > 0.02% of 26005 (5.2)? no: within
    ours[M0 + 60]["close"] += 6.0     # outside
    ours[M0 + 120]["high"] = 26011   # above ref high beyond one tick -> fail
    ours[M0 + 180]["volume"] = 1040  # 4% -> ok
    ours[M0 + 240]["volume"] = 1200  # fail
    del ours[M0 + 300]
    r = reconcile(ours, ref, tol)
    assert r["field_failures"] == {"open": 0, "close": 1, "high_low": 1, "volume": 1}
    assert r["common_minutes"] == 99 and r["within_tolerance_pct"] == pytest.approx(100 * 96 / 99, abs=0.01)
    assert r["passed"] is True
    ours2 = {t: v for t, v in ours.items() if t < M0 + 60 * 90}
    assert reconcile(ours2, ref, tol)["passed"] is False   # coverage below 95%


def test_parse_candles_shapes():
    body = {"open": [1, 2], "high": [3, 4], "low": [0.5, 1], "close": [2, 3], "volume": [10, 20],
            "timestamp": [M0, M0 + 60]}
    assert parse_candles(body)[M0 + 60]["close"] == 3
    assert parse_candles({"data": body})[M0]["volume"] == 10
    assert parse_candles({"errorCode": "DH-905"}) == {}


def test_feed_day_pipeline_end_to_end(tmp_path):
    day = "2026-11-26"
    w = PartitionedWriter(tmp_path, "feed", FEED_SCHEMA, 10_000, 9999)
    seq = 0
    for k in range(0, 180):   # 10:00:00 .. 10:02:59, one packet each per second
        t = M0 + k
        for msg, extra in ((ticker_packet(13, 24000.0 + k, ltt=t), {}),
                           (full_packet(49081, ltp=100 + k % 7, ltt=t), {"volume": 1000 + 10 * k})):
            r = decode(msg)[0]
            seq += 1
            r.update(recv_ns=t * NS, seq=seq, conn_id=1, **extra)
            w.add(r)
    w.close()
    res = build_feed_day(tmp_path, day, known_ids={13, 49081})
    bars = res["bars"]
    assert res["index_ids"] == [13]
    m13 = minute_dict(bars.filter(pa.compute.equal(bars["security_id"], 13)), price_only=True)
    assert len(m13) == 3 and m13[M0]["open"] == 24000 and m13[M0]["close"] == 24059
    m = minute_dict(bars.filter(pa.compute.equal(bars["security_id"], 49081)))
    assert m[M0 + 60]["volume"] == 600 and m[M0]["volume"] == 590   # first packet not attributable
    q = {r["security_id"]: r for r in res["quality"]}
    assert q[49081]["events"] == 180 and q[49081]["bars"] == 3


def test_snapshot_bars_from_history_rows():
    base = datetime(2026, 11, 26, 9, 15, 3, tzinfo=IST)
    rows = []
    for i in range(18):  # 3 minutes x 6 snapshots
        ts = base + timedelta(seconds=10 * i)
        for ot, ltp in (("CE", 100 + i), ("PE", 80 - i)):
            rows.append({"snapshot_ts": ts, "expiry_ts": datetime(2026, 12, 1, tzinfo=IST), "strike": 26000.0,
                         "option_type": ot, "ltp": float(ltp), "volume": 1000 + 100 * i, "oi": 5000,
                         "bid": float(ltp) - 0.5, "ask": float(ltp) + 0.5 if i != 4 else float(ltp) - 1,
                         "iv": 15.0, "delta": 0.5, "spot": 26000.0 + i})
    t = pa.Table.from_pylist(rows)
    opt, spot = snapshot_bars(t)
    assert spot.num_rows == 3 and opt.num_rows == 6
    s0 = spot.slice(0, 1).to_pylist()[0]
    assert (s0["open"], s0["close"], s0["samples"]) == (26000, 26005, 6)
    ce = [r for r in opt.to_pylist() if r["option_type"] == "CE"]
    assert [r["volume"] for r in ce] == [0, 600, 600]
    assert ce[0]["spread_mean"] == pytest.approx(1.0)   # crossed sample (i=4) excluded
    assert ce[0]["tier"] == "reconstructed_from_snapshots"
