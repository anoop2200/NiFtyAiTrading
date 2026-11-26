"""Normalize raw feed packets into per-instrument quote/trade events with data-quality flags (v1.3 §4.2).

Nothing is dropped silently: every packet produces a NormEvent (or a state update for OI / prev-close)
carrying flags. Downstream consumers decide what to exclude. Bid/ask are never forward-filled: an
event only carries a book when its packet carried one.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntFlag
from typing import Any

from app.data.feed_parser import CODE_FULL, CODE_OI, CODE_PREV_CLOSE, CODE_QUOTE, CODE_TICKER


class Q(IntFlag):
    NONE = 0
    DUPLICATE = 1            # identical payload to the previous packet for this instrument
    OUT_OF_ORDER = 2         # exchange trade time or cumulative volume moved backwards
    CROSSED = 4              # best bid > best ask (both > 0)
    ZERO_QTY = 8             # a side of the top of book has price but zero quantity, or no price at all
    INVALID_INSTRUMENT = 16  # id not in the dated instrument map
    CLOCK_SKEW = 32          # exchange time ahead of local receive time beyond tolerance
    PARSE_ERROR = 64
    LATE = 128               # arrived after its bar was finalized (set by the bar builder)


MAX_FUTURE_SKEW_S = 2.0


@dataclass
class InstrumentState:
    last_payload: bytes | None = None
    last_ltt: int | None = None
    last_volume: int | None = None
    oi: int | None = None
    prev_close: float | None = None
    day_high: float | None = None
    day_low: float | None = None
    last_change_ns: int | None = None


@dataclass
class Normalizer:
    known_ids: set[int] | None = None        # None = do not validate ids
    states: dict[int, InstrumentState] = field(default_factory=dict)
    counts: dict[str, int] = field(default_factory=dict)

    def _count(self, flags: Q) -> None:
        for f in Q:
            if f and f in flags:
                self.counts[f.name] = self.counts.get(f.name, 0) + 1

    def process(self, row: dict[str, Any]) -> dict | None:
        code, sid = row.get("code"), row.get("security_id")
        if code not in (CODE_TICKER, CODE_QUOTE, CODE_FULL, CODE_OI, CODE_PREV_CLOSE):
            return None  # disconnect / status / unknown packets: kept in raw, not quote events
        st = self.states.setdefault(sid, InstrumentState())
        if code == CODE_OI:
            st.oi = row.get("oi")
            return None
        if code == CODE_PREV_CLOSE:
            st.prev_close = row.get("prev_close")
            return None

        flags = Q.NONE
        if row.get("parse_error"):
            flags |= Q.PARSE_ERROR
        if self.known_ids is not None and sid not in self.known_ids:
            flags |= Q.INVALID_INSTRUMENT
        payload = (row.get("raw") or b"")[8:]
        if payload and payload == st.last_payload:
            flags |= Q.DUPLICATE
        ltt, recv = row.get("ltt"), row["recv_ns"]
        if ltt:
            if st.last_ltt and ltt < st.last_ltt:
                flags |= Q.OUT_OF_ORDER
            if ltt - recv / 1e9 > MAX_FUTURE_SKEW_S:
                flags |= Q.CLOCK_SKEW
        vol = row.get("volume")
        trade_qty = 0
        if vol is not None and code in (CODE_QUOTE, CODE_FULL):
            if st.last_volume is not None and vol < st.last_volume:
                flags |= Q.OUT_OF_ORDER
            elif st.last_volume is not None:
                trade_qty = vol - st.last_volume
            # first packet of the session: cumulative volume so far is not attributable to a bar

        ev = {"recv_ns": recv, "seq": row.get("seq"), "security_id": sid, "code": code,
              "ltp": row.get("ltp"), "ltt": ltt, "volume": vol, "trade_qty": trade_qty,
              "oi": row.get("oi") if row.get("oi") is not None else st.oi,
              "day_high": row.get("high"), "day_low": row.get("low"),
              "bid": None, "ask": None, "bid_qty": None, "ask_qty": None, "mid": None, "spread": None,
              "bid_qty_5": None, "ask_qty_5": None}
        if code == CODE_FULL:
            bid, ask = row.get("bid_px_1"), row.get("ask_px_1")
            bq, aq = row.get("bid_qty_1"), row.get("ask_qty_1")
            ev.update(bid=bid, ask=ask, bid_qty=bq, ask_qty=aq,
                      bid_qty_5=sum(row.get(f"bid_qty_{i}") or 0 for i in range(1, 6)),
                      ask_qty_5=sum(row.get(f"ask_qty_{i}") or 0 for i in range(1, 6)))
            if bid and ask and bid > 0 and ask > 0:
                if bid > ask:
                    flags |= Q.CROSSED
                ev["mid"] = round((bid + ask) / 2, 4)
                ev["spread"] = round(ask - bid, 4)
            if not bid or not ask or not bq or not aq:
                flags |= Q.ZERO_QTY

        # state advances only on clean ordering, so one bad packet cannot poison later ones
        if not (flags & (Q.OUT_OF_ORDER | Q.PARSE_ERROR)):
            if ltt:
                st.last_ltt = ltt
            if vol is not None and code in (CODE_QUOTE, CODE_FULL):
                st.last_volume = vol
        if not (flags & Q.DUPLICATE):
            st.last_change_ns = recv
        st.last_payload = payload or st.last_payload
        ev["flags"] = int(flags)
        self._count(flags)
        return ev
