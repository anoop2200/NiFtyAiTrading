from conftest import full_packet, ticker_packet

from app.data.feed_parser import decode
from app.marketdata.normalize import Q, Normalizer
from app.marketdata.staleness import Freshness, FreshnessTracker

NS = 1_000_000_000
T0 = 1_700_000_000


def row(msg: bytes, recv_s: float, seq: int = 0, **over) -> dict:
    r = decode(msg)[0]
    r.update(recv_ns=int(recv_s * NS), seq=seq, conn_id=1)
    r.update(over)
    return r


def flags(ev) -> Q:
    return Q(ev["flags"])


def test_clean_quote_and_trade_qty_from_cumulative_volume():
    n = Normalizer({49081})
    e1 = n.process(row(full_packet(49081, ltt=T0), T0, volume=1000))
    e2 = n.process(row(full_packet(49081, ltp=100.5, ltt=T0 + 1), T0 + 1, volume=1300))
    assert flags(e1) == Q.NONE and e1["trade_qty"] == 0          # first packet: not attributable
    assert e2["trade_qty"] == 300 and e2["spread"] == 0.2 and e2["mid"] == 100.0
    assert e2["bid_qty_5"] == sum(75 * (i + 1) for i in range(5))


def test_duplicate_payload_flagged():
    n = Normalizer()
    n.process(row(full_packet(1, ltt=T0), T0))
    e = n.process(row(full_packet(1, ltt=T0), T0 + 0.5))
    assert Q.DUPLICATE in flags(e)


def test_out_of_order_ltt_and_volume_do_not_poison_state():
    n = Normalizer()
    n.process(row(full_packet(1, ltt=T0 + 10), T0 + 10, volume=5000))
    back = n.process(row(full_packet(1, ltp=101, ltt=T0 + 5), T0 + 11, volume=4000))
    assert Q.OUT_OF_ORDER in flags(back)
    nxt = n.process(row(full_packet(1, ltp=102, ltt=T0 + 12), T0 + 12, volume=5200))
    assert flags(nxt) == Q.NONE and nxt["trade_qty"] == 200   # measured against the last clean packet


def test_crossed_book():
    n = Normalizer()
    e = n.process(row(full_packet(1, bid=101.0, ask=100.0), T0))
    assert Q.CROSSED in flags(e)


def test_zero_quantity_book_side():
    n = Normalizer()
    e = n.process(row(full_packet(1), T0, ask_qty_1=0))
    assert Q.ZERO_QTY in flags(e)


def test_invalid_instrument():
    n = Normalizer({13})
    e = n.process(row(full_packet(777), T0))
    assert Q.INVALID_INSTRUMENT in flags(e)
    assert n.counts["INVALID_INSTRUMENT"] == 1


def test_clock_skew():
    n = Normalizer()
    e = n.process(row(full_packet(1, ltt=T0 + 30), T0))
    assert Q.CLOCK_SKEW in flags(e)


def test_ticker_carries_no_book_and_oi_packets_update_state_only():
    n = Normalizer()
    e = n.process(row(ticker_packet(13, 24000.0, ltt=T0), T0))
    assert e["bid"] is None and e["spread"] is None and flags(e) == Q.NONE
    assert n.process({"code": 5, "security_id": 1, "oi": 777, "recv_ns": T0 * NS}) is None
    e2 = n.process(row(full_packet(1), T0 + 1))
    assert e2["oi"] is not None


def test_quiet_vs_stale_vs_feed_down():
    tr = FreshnessTracker(underlying_ids={13})
    tr.on_event({"recv_ns": T0 * NS, "security_id": 1, "flags": 0})
    tr.on_event({"recv_ns": T0 * NS, "security_id": 13, "flags": 0})
    for k in range(1, 41):  # feed stays alive through other instruments
        tr.on_event({"recv_ns": (T0 + k) * NS, "security_id": 2, "flags": 0})
        if k == 3:
            assert tr.evaluate((T0 + k) * NS, 1) == Freshness.FRESH
        if k == 8:
            assert tr.evaluate((T0 + k) * NS, 1) == Freshness.QUIET_CONFIRM
            assert tr.evaluate((T0 + k) * NS, 13) == Freshness.FRESH
        if k == 12:
            assert tr.evaluate((T0 + k) * NS, 13) == Freshness.STALE   # underlying: tighter limit
        if k == 35:
            assert tr.evaluate((T0 + k) * NS, 1) == Freshness.STALE
    assert tr.evaluate((T0 + 40) * NS, 99) == Freshness.NO_DATA
    assert tr.evaluate((T0 + 45) * NS, 2) == Freshness.FEED_DOWN


def test_duplicates_do_not_refresh_change_age():
    tr = FreshnessTracker()
    tr.on_event({"recv_ns": T0 * NS, "security_id": 1, "flags": 0})
    tr.on_event({"recv_ns": (T0 + 31) * NS, "security_id": 1, "flags": int(Q.DUPLICATE)})
    assert tr.evaluate((T0 + 31) * NS, 1) == Freshness.STALE
