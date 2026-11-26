"""Daily-partitioned Parquet writer: <root>/date=YYYY-MM-DD/stream=<name>/part-<HHMMSS>-<n>.parquet.

Files are written to a temp name and renamed, so a crash never leaves a half-written part file.
"""
from __future__ import annotations

import os
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from app.clock import ist_date_of_ns, ns_to_ist
from app.data.feed_parser import DEPTH_FIELDS

_i64, _i32, _i16, _f64 = pa.int64(), pa.int32(), pa.int16(), pa.float64()

FEED_SCHEMA = pa.schema(
    [("recv_ns", _i64), ("seq", _i64), ("conn_id", _i16), ("code", _i16), ("msg_len", _i32),
     ("segment", _i16), ("security_id", _i64), ("ltp", _f64), ("ltq", _i32), ("ltt", _i64), ("atp", _f64),
     ("volume", _i64), ("total_sell_qty", _i64), ("total_buy_qty", _i64), ("oi", _i64), ("oi_high", _i64),
     ("oi_low", _i64), ("open", _f64), ("close", _f64), ("high", _f64), ("low", _f64), ("prev_close", _f64),
     ("prev_oi", _i64), ("disconnect_code", _i16), ("parse_error", pa.bool_())]
    + [(f, _f64 if "_px_" in f else (_i32 if "orders" in f else _i64)) for f in DEPTH_FIELDS]
    + [("raw", pa.binary())])

CHAIN_SCHEMA = pa.schema(
    [("recv_ns", _i64), ("request_ns", _i64), ("snapshot_seq", _i64), ("underlying_scrip", _i64),
     ("expiry", pa.string()), ("underlying_ltp", _f64), ("strike", _f64), ("option_type", pa.string()),
     ("security_id", _i64), ("last_price", _f64), ("average_price", _f64), ("previous_close_price", _f64),
     ("oi", _i64), ("previous_oi", _i64), ("volume", _i64), ("previous_volume", _i64),
     ("implied_volatility", _f64), ("delta", _f64), ("gamma", _f64), ("theta", _f64), ("vega", _f64),
     ("top_bid_price", _f64), ("top_bid_quantity", _i64), ("top_ask_price", _f64), ("top_ask_quantity", _i64)])

CHAIN_RAW_SCHEMA = pa.schema(
    [("recv_ns", _i64), ("request_ns", _i64), ("snapshot_seq", _i64), ("underlying_scrip", _i64),
     ("expiry", pa.string()), ("http_status", _i16), ("body", pa.string())])


class PartitionedWriter:
    def __init__(self, root: Path, stream: str, schema: pa.Schema, flush_rows: int = 5000,
                 flush_seconds: float = 10.0, ts_field: str = "recv_ns"):
        self.root, self.stream, self.schema = Path(root), stream, schema
        self.flush_rows, self.flush_seconds, self.ts_field = flush_rows, flush_seconds, ts_field
        self._buf: list[dict] = []
        self._last_flush = time.monotonic()
        self._part = 0
        self.rows_written = 0
        self.files: list[Path] = []

    def add(self, row: dict) -> None:
        self._buf.append(row)
        if len(self._buf) >= self.flush_rows or time.monotonic() - self._last_flush >= self.flush_seconds:
            self.flush()

    def add_many(self, rows: list[dict]) -> None:
        for r in rows:
            self.add(r)

    def flush(self) -> None:
        self._last_flush = time.monotonic()
        if not self._buf:
            return
        rows, self._buf = self._buf, []
        by_date: dict[str, list[dict]] = {}
        for r in rows:  # a buffer may straddle midnight
            by_date.setdefault(ist_date_of_ns(r[self.ts_field]).isoformat(), []).append(r)
        for day, part_rows in by_date.items():
            d = self.root / f"date={day}" / f"stream={self.stream}"
            d.mkdir(parents=True, exist_ok=True)
            self._part += 1
            stamp = ns_to_ist(part_rows[0][self.ts_field]).strftime("%H%M%S")
            dest = d / f"part-{stamp}-{os.getpid()}-{self._part:05d}.parquet"
            tmp = dest.with_suffix(".tmp")
            table = pa.Table.from_pylist(part_rows, schema=self.schema)
            pq.write_table(table, tmp, compression="zstd")
            tmp.replace(dest)
            self.files.append(dest)
            self.rows_written += len(part_rows)

    def close(self) -> None:
        self.flush()


def read_stream(root: Path, day: str, stream: str) -> pa.Table:
    d = Path(root) / f"date={day}" / f"stream={stream}"
    files = sorted(d.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(d)
    return pa.concat_tables([pq.read_table(f) for f in files])
