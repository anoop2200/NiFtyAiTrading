import struct

import pytest
from conftest import disconnect_packet, full_packet, header, ticker_packet

from app.data.feed_parser import decode, parse_packet, split_message


def test_ticker():
    r = parse_packet(ticker_packet(13, 24123.45, 1_750_000_000))
    assert r["code"] == 2 and r["segment"] == 0 and r["security_id"] == 13
    assert r["ltp"] == pytest.approx(24123.45, abs=0.01)
    assert r["ltt"] == 1_750_000_000


def test_quote():
    body = struct.pack("<fhifiiiffff", 101.5, 75, 1_750_000_001, 100.2, 9000, 300, 400, 99.0, 0.0, 103.0, 98.0)
    r = parse_packet(header(4, 2, 49081, 50) + body)
    assert r["ltq"] == 75 and r["volume"] == 9000 and r["total_buy_qty"] == 400
    assert r["open"] == 99.0 and r["high"] == 103.0 and r["low"] == 98.0


def test_full_with_depth():
    r = parse_packet(full_packet(49081, ltp=120.25, bid=120.2, ask=120.3))
    assert r["code"] == 8 and r["security_id"] == 49081
    assert r["oi"] == 1_000_000 and r["oi_high"] == 1_100_000 and r["oi_low"] == 900_000
    assert r["open"] == 95.0 and r["high"] == 105.0
    assert r["bid_px_1"] == pytest.approx(120.2, abs=1e-3) and r["ask_px_1"] == pytest.approx(120.3, abs=1e-3)
    assert r["bid_qty_5"] == 375 and r["ask_qty_5"] == 750 and r["ask_orders_5"] == 6


def test_oi_prevclose_disconnect():
    assert parse_packet(header(5, 2, 7, 12) + struct.pack("<i", 4242))["oi"] == 4242
    pc = parse_packet(header(6, 2, 7, 16) + struct.pack("<fi", 88.5, 777))
    assert pc["prev_close"] == 88.5 and pc["prev_oi"] == 777
    assert parse_packet(disconnect_packet(807))["disconnect_code"] == 807


def test_split_concatenated_and_unknown():
    msg = ticker_packet(13, 1.0) + full_packet(5) + header(7, 0, 0, 12) + b"\x01\x02\x03\x04"
    parts = split_message(msg)
    assert [len(p) for p in parts] == [16, 162, 12]
    rows = decode(msg)
    assert [r["code"] for r in rows] == [2, 8, 7]
    assert rows[2]["raw"].endswith(b"\x01\x02\x03\x04")  # unknown layout kept raw
    assert all("raw" in r for r in rows)


def test_truncated_packet_flagged():
    rows = decode(full_packet(5)[:100])
    assert len(rows) == 1 and rows[0]["code"] == 8 and rows[0]["parse_error"] is True
