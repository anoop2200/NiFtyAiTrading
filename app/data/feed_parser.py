"""Dhan v2 live-feed binary decoder (little-endian), per Dhan live-market-feed documentation.

Header (8 bytes): code u8 | message length i16 | exchange segment u8 | security id i32.
Unknown packet types (index=1, market status=7) are kept with raw bytes and no parsed fields.
Every row keeps the raw packet so files can be re-parsed if a layout assumption proves wrong.
"""
from __future__ import annotations

import struct
from typing import Any

HEADER = struct.Struct("<BhBi")
TICKER = struct.Struct("<fi")                  # ltp, ltt
QUOTE = struct.Struct("<fhifiiiffff")          # ltp ltq ltt atp vol tsq tbq open close high low
OI = struct.Struct("<i")
PREV_CLOSE = struct.Struct("<fi")              # prev close, prev oi
FULL = struct.Struct("<fhifiiiiiiffff")        # ltp ltq ltt atp vol tsq tbq oi oi_hi oi_lo open close high low
DEPTH = struct.Struct("<iihhff")               # bid_qty ask_qty bid_orders ask_orders bid_px ask_px
DISCONNECT = struct.Struct("<h")

CODE_INDEX, CODE_TICKER, CODE_QUOTE, CODE_OI, CODE_PREV_CLOSE, CODE_STATUS, CODE_FULL, CODE_DISCONNECT = \
    1, 2, 4, 5, 6, 7, 8, 50
SIZES = {CODE_TICKER: 16, CODE_QUOTE: 50, CODE_OI: 12, CODE_PREV_CLOSE: 16, CODE_FULL: 162, CODE_DISCONNECT: 10}
DEPTH_LEVELS = 5

FIELDS = ["ltp", "ltq", "ltt", "atp", "volume", "total_sell_qty", "total_buy_qty", "oi", "oi_high", "oi_low",
          "open", "close", "high", "low", "prev_close", "prev_oi", "disconnect_code"]
DEPTH_FIELDS = [f"{k}_{i}" for i in range(1, DEPTH_LEVELS + 1)
                for k in ("bid_qty", "ask_qty", "bid_orders", "ask_orders", "bid_px", "ask_px")]


def _px(x: float) -> float:
    # float32 on the wire: 25834.40 arrives as 25834.4004. NSE ticks are >= 0.05, so 2 dp is exact.
    return round(float(x), 2)


def parse_packet(buf: bytes) -> dict[str, Any]:
    code, msg_len, seg, sid = HEADER.unpack_from(buf, 0)
    row: dict[str, Any] = {"code": code, "msg_len": msg_len, "segment": seg, "security_id": sid}
    if code == CODE_TICKER:
        ltp, ltt = TICKER.unpack_from(buf, 8)
        row.update(ltp=_px(ltp), ltt=ltt)
    elif code == CODE_QUOTE:
        ltp, ltq, ltt, atp, vol, tsq, tbq, o, c, h, lo = QUOTE.unpack_from(buf, 8)
        row.update(ltp=_px(ltp), ltq=ltq, ltt=ltt, atp=_px(atp), volume=vol, total_sell_qty=tsq,
                   total_buy_qty=tbq, open=_px(o), close=_px(c), high=_px(h), low=_px(lo))
    elif code == CODE_OI:
        row["oi"] = OI.unpack_from(buf, 8)[0]
    elif code == CODE_PREV_CLOSE:
        pc, poi = PREV_CLOSE.unpack_from(buf, 8)
        row.update(prev_close=_px(pc), prev_oi=poi)
    elif code == CODE_FULL:
        (ltp, ltq, ltt, atp, vol, tsq, tbq, oi, oi_hi, oi_lo, o, c, h, lo) = FULL.unpack_from(buf, 8)
        row.update(ltp=_px(ltp), ltq=ltq, ltt=ltt, atp=_px(atp), volume=vol, total_sell_qty=tsq,
                   total_buy_qty=tbq, oi=oi, oi_high=oi_hi, oi_low=oi_lo, open=_px(o), close=_px(c),
                   high=_px(h), low=_px(lo))
        for i in range(DEPTH_LEVELS):
            bq, aq, bo, ao, bp, ap = DEPTH.unpack_from(buf, 62 + i * DEPTH.size)
            n = i + 1
            row.update({f"bid_qty_{n}": bq, f"ask_qty_{n}": aq, f"bid_orders_{n}": bo,
                        f"ask_orders_{n}": ao, f"bid_px_{n}": _px(bp), f"ask_px_{n}": _px(ap)})
    elif code == CODE_DISCONNECT:
        row["disconnect_code"] = DISCONNECT.unpack_from(buf, 8)[0]
    return row


def split_message(msg: bytes) -> list[bytes]:
    """A WebSocket message normally holds one packet; split defensively if several are concatenated."""
    out, pos = [], 0
    while pos + HEADER.size <= len(msg):
        code = msg[pos]
        size = SIZES.get(code)
        if size is None or pos + size > len(msg):
            out.append(msg[pos:])  # unknown layout: keep the remainder as one raw packet
            break
        out.append(msg[pos:pos + size])
        pos += size
    return out


def decode(msg: bytes) -> list[dict[str, Any]]:
    rows = []
    for pkt in split_message(msg):
        try:
            row = parse_packet(pkt)
        except struct.error:
            row = {"code": pkt[0] if pkt else -1, "msg_len": None, "segment": None, "security_id": None,
                   "parse_error": True}
        row["raw"] = pkt
        rows.append(row)
    return rows
