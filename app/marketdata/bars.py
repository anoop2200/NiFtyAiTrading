"""1-minute bars.

Feed bars (tier `observed_feed`) — trade-based, built by one streaming implementation that batch
processing also uses, so live and replayed bars are identical:
  * a trade is observed when cumulative volume rises between packets; price = LTP, qty = Δvolume;
  * trades are bucketed by exchange trade time (LTT, UTC epoch) when plausible, else receive time;
  * index bars use ticker LTP updates (no volume);
  * extremes traded between packets are recovered from the packet's running day high / low;
  * book fields (bid/ask/spread) come only from packets that carried a book — never forward-filled;
  * a bar is final once the receive-time watermark passes its end + grace; later events are LATE;
  * minutes with no events produce no bar (gaps stay visible).

Snapshot bars (tier `reconstructed_from_snapshots`) — from the ~10 s optionchain_db history.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import pyarrow as pa
import pyarrow.compute as pc

from app.marketdata.normalize import Q

NS = 1_000_000_000
LTT_MAX_LAG_S = 120
BAD = Q.PARSE_ERROR | Q.INVALID_INSTRUMENT | Q.OUT_OF_ORDER | Q.DUPLICATE

BAR_SCHEMA = pa.schema([
    ("security_id", pa.int64()), ("ts", pa.timestamp("s", tz="Asia/Kolkata")),
    ("open", pa.float64()), ("high", pa.float64()), ("low", pa.float64()), ("close", pa.float64()),
    ("volume", pa.int64()), ("vwap", pa.float64()), ("trades", pa.int32()), ("oi", pa.int64()),
    ("bid", pa.float64()), ("ask", pa.float64()), ("spread_mean", pa.float64()), ("quote_updates", pa.int32()),
    ("crossed", pa.int32()), ("flagged_events", pa.int32()), ("late_events", pa.int32()),
    ("extreme_from_day_range", pa.bool_()), ("tier", pa.string())])


@dataclass
class _Acc:
    sid: int
    start: int  # epoch seconds
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    volume: int = 0
    pv: float = 0.0
    trades: int = 0
    oi: int | None = None
    bid: float | None = None
    ask: float | None = None
    spread_sum: float = 0.0
    spread_n: int = 0
    quotes: int = 0
    crossed: int = 0
    flagged: int = 0
    late: int = 0
    from_range: bool = False

    def price(self, p: float) -> None:
        if self.open is None:
            self.open = self.high = self.low = p
        else:
            self.high, self.low = max(self.high, p), min(self.low, p)
        self.close = p

    def row(self) -> dict:
        return {"security_id": self.sid, "ts": self.start, "open": self.open, "high": self.high, "low": self.low,
                "close": self.close, "volume": self.volume,
                "vwap": round(self.pv / self.volume, 4) if self.volume else None, "trades": self.trades,
                "oi": self.oi, "bid": self.bid, "ask": self.ask,
                "spread_mean": round(self.spread_sum / self.spread_n, 4) if self.spread_n else None,
                "quote_updates": self.quotes, "crossed": self.crossed, "flagged_events": self.flagged,
                "late_events": self.late, "extreme_from_day_range": self.from_range, "tier": "observed_feed"}


@dataclass
class BarBuilder:
    tf_s: int = 60
    grace_s: float = 2.0
    index_ids: set[int] = field(default_factory=set)
    open_bars: dict[tuple[int, int], _Acc] = field(default_factory=dict)
    final_until: dict[int, int] = field(default_factory=dict)  # sid -> first bucket start still open
    day_hl: dict[int, tuple[float | None, float | None]] = field(default_factory=dict)
    watermark_ns: int = 0
    late_total: int = 0

    def _bucket(self, ev: dict, traded: bool) -> int:
        recv_s = ev["recv_ns"] / NS
        t = recv_s
        ltt = ev.get("ltt")
        if traded and ltt and recv_s - LTT_MAX_LAG_S <= ltt <= recv_s + 2:
            t = ltt
        return int(t // self.tf_s * self.tf_s)

    def on_event(self, ev: dict) -> list[dict]:
        sid, flags = ev["security_id"], ev.get("flags", 0)
        is_index = sid in self.index_ids
        traded = bool(ev.get("ltp")) and (is_index or (ev.get("trade_qty") or 0) > 0) and not (flags & BAD)
        start = self._bucket(ev, traded)
        self.watermark_ns = max(self.watermark_ns, ev["recv_ns"])
        if start < self.final_until.get(sid, 0):
            self.late_total += 1
            nxt = self.open_bars.get((sid, self.final_until[sid]))
            if nxt:
                nxt.late += 1
            return self._finalize()
        acc = self.open_bars.get((sid, start))
        if acc is None:
            acc = self.open_bars[(sid, start)] = _Acc(sid, start)
        if flags:
            acc.flagged += 1
        if traded:
            acc.price(ev["ltp"])
            if not is_index:
                q = ev["trade_qty"]
                acc.volume += q
                acc.pv += ev["ltp"] * q
                acc.trades += 1
        # extremes traded between two packets show up as a new running day high / low
        if not (flags & BAD) and not is_index:
            ph, pl = self.day_hl.get(sid, (None, None))
            h, lo = ev.get("day_high"), ev.get("day_low")
            if acc.open is not None:
                if h and ph and h > ph and h > acc.high:
                    acc.high, acc.from_range = h, True
                if lo and pl and 0 < lo < pl and lo < acc.low:
                    acc.low, acc.from_range = lo, True
            self.day_hl[sid] = (h or ph, lo or pl)
        if ev.get("oi") is not None:
            acc.oi = ev["oi"]
        if ev.get("bid") is not None and not (flags & BAD):
            acc.quotes += 1
            acc.bid, acc.ask = ev["bid"], ev["ask"]
            if ev.get("spread") is not None:
                acc.spread_sum += ev["spread"]
                acc.spread_n += 1
            if flags & Q.CROSSED:
                acc.crossed += 1
        return self._finalize()

    def _finalize(self, force: bool = False) -> list[dict]:
        out = []
        limit = self.watermark_ns / NS - self.grace_s
        for key in sorted(self.open_bars, key=lambda k: k[1]):
            sid, start = key
            if force or start + self.tf_s <= limit:
                out.append(self.open_bars.pop(key).row())
                self.final_until[sid] = max(self.final_until.get(sid, 0), start + self.tf_s)
        return out

    def flush(self) -> list[dict]:
        return self._finalize(force=True)


def bars_table(rows: list[dict]) -> pa.Table:
    t = pa.Table.from_pylist(rows, schema=BAR_SCHEMA)
    return t.sort_by([("security_id", "ascending"), ("ts", "ascending")])


# --------------------------------------------------------------------------- snapshot history bars
SNAP_SCHEMA = pa.schema([
    ("expiry", pa.date32()), ("strike", pa.float64()), ("option_type", pa.string()),
    ("ts", pa.timestamp("s", tz="Asia/Kolkata")), ("open", pa.float64()), ("high", pa.float64()),
    ("low", pa.float64()), ("close", pa.float64()), ("volume", pa.int64()), ("oi", pa.int64()),
    ("bid", pa.float64()), ("ask", pa.float64()), ("spread_mean", pa.float64()), ("iv", pa.float64()),
    ("delta", pa.float64()), ("spot", pa.float64()), ("samples", pa.int32()), ("negative_volume", pa.bool_()),
    ("tier", pa.string())])

SPOT_SCHEMA = pa.schema([
    ("ts", pa.timestamp("s", tz="Asia/Kolkata")), ("open", pa.float64()), ("high", pa.float64()),
    ("low", pa.float64()), ("close", pa.float64()), ("samples", pa.int32()), ("tier", pa.string())])


def snapshot_bars(t: pa.Table, tf_s: int = 60) -> tuple[pa.Table, pa.Table]:
    """Option bars per (expiry, strike, type) and NIFTY spot bars from one day of snapshot rows.

    OHLC are of ~10 s samples, so true intra-minute extremes can be missed; volume is the change of
    the cumulative day volume between the last samples of consecutive minutes."""
    import pandas as pd

    df = t.select(["snapshot_ts", "expiry_ts", "strike", "option_type", "ltp", "volume", "oi", "bid", "ask",
                   "iv", "delta", "spot"]).to_pandas()
    df = df[(df["snapshot_ts"].dt.strftime("%H:%M") >= "09:15") & (df["snapshot_ts"].dt.strftime("%H:%M") < "15:30")]
    df["minute"] = df["snapshot_ts"].dt.floor(f"{tf_s}s")
    df["expiry"] = df["expiry_ts"].dt.date
    df = df.sort_values("snapshot_ts")
    valid_book = (df["bid"] > 0) & (df["ask"] > 0) & (df["ask"] >= df["bid"])
    df["spread"] = (df["ask"] - df["bid"]).where(valid_book)
    df.loc[~valid_book, ["bid", "ask"]] = None
    g = df[df["ltp"] > 0].groupby(["expiry", "strike", "option_type", "minute"], sort=True)
    bars = g.agg(open=("ltp", "first"), high=("ltp", "max"), low=("ltp", "min"), close=("ltp", "last"),
                 cumvol=("volume", "last"), oi=("oi", "last"), bid=("bid", "last"), ask=("ask", "last"),
                 spread_mean=("spread", "mean"), iv=("iv", "last"), delta=("delta", "last"),
                 spot=("spot", "last"), samples=("ltp", "size")).reset_index()
    prev = bars.groupby(["expiry", "strike", "option_type"])["cumvol"].shift(1)
    vol = bars["cumvol"] - prev
    bars["negative_volume"] = vol < 0
    bars["volume"] = vol.clip(lower=0).fillna(0).astype("int64")  # first minute: not attributable
    bars = bars.rename(columns={"minute": "ts"}).drop(columns=["cumvol"])
    bars["tier"] = "reconstructed_from_snapshots"
    opt = pa.Table.from_pandas(bars[[f.name for f in SNAP_SCHEMA]], schema=SNAP_SCHEMA, preserve_index=False)

    s = df.drop_duplicates("snapshot_ts")[["snapshot_ts", "minute", "spot"]].dropna()
    sg = s.groupby("minute").agg(open=("spot", "first"), high=("spot", "max"), low=("spot", "min"),
                                 close=("spot", "last"), samples=("spot", "size")).reset_index()
    sg = sg.rename(columns={"minute": "ts"})
    sg["tier"] = "reconstructed_from_snapshots"
    spot = pa.Table.from_pandas(sg[[f.name for f in SPOT_SCHEMA]], schema=SPOT_SCHEMA, preserve_index=False)
    return opt, spot


def ts_seconds(col: pa.ChunkedArray) -> list[int]:
    return pc.cast(pc.cast(col, pa.timestamp("s", tz="UTC")), pa.int64()).to_pylist()
