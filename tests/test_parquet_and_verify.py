from datetime import datetime

from conftest import full_packet, ticker_packet

from app.clock import IST
from app.data.feed_parser import decode
from app.data.parquet_store import FEED_SCHEMA, PartitionedWriter, read_stream
from app.data.verify import verify_day


def _ns(day: str, hh: int, mm: int, ss: int = 0) -> int:
    return int(datetime(*map(int, day.split("-")), hh, mm, ss, tzinfo=IST).timestamp() * 1e9)


def _rows(day, start, end, step_s=1, gap=None):
    rows, seq = [], 0
    t = _ns(day, *start)
    stop = _ns(day, *end)
    while t <= stop:
        if gap and gap[0] <= t < gap[1]:
            t += int(step_s * 1e9)
            continue
        for msg in (ticker_packet(13, 24000.0, ltt=t // 10**9), full_packet(49081, ltt=t // 10**9)):
            for r in decode(msg):
                seq += 1
                r.update(recv_ns=t, seq=seq, conn_id=1)
                rows.append(r)
        t += int(step_s * 1e9)
    return rows


def test_writer_roundtrip_and_midnight_split(tmp_path):
    w = PartitionedWriter(tmp_path, "feed", FEED_SCHEMA, flush_rows=50, flush_seconds=999)
    rows = _rows("2026-11-25", (23, 59, 30), (23, 59, 59)) + _rows("2026-11-26", (0, 0, 0), (0, 0, 20))
    w.add_many(rows)
    w.close()
    a = read_stream(tmp_path, "2026-11-25", "feed")
    b = read_stream(tmp_path, "2026-11-26", "feed")
    assert a.num_rows + b.num_rows == len(rows) == w.rows_written
    assert not list(tmp_path.rglob("*.tmp"))
    assert b.column("raw")[0].as_py()[:1] in (b"\x02", b"\x08")


def test_verify_full_session_passes(tmp_path):
    day = "2026-11-26"
    w = PartitionedWriter(tmp_path, "feed", FEED_SCHEMA, flush_rows=20000)
    w.add_many(_rows(day, (9, 14, 0), (15, 31, 0), step_s=2))
    w.close()
    res = verify_day(tmp_path, day)
    assert res["passed"], res["checks"]
    assert res["ltt_epoch_interpretation"] == "UTC epoch"
    assert res["packets_by_code"] == {"2": res["feed_rows"] // 2, "8": res["feed_rows"] // 2}


def test_verify_detects_gap_and_late_start(tmp_path):
    day = "2026-11-26"
    w = PartitionedWriter(tmp_path, "feed", FEED_SCHEMA, flush_rows=20000)
    w.add_many(_rows(day, (9, 40, 0), (15, 31, 0), step_s=2, gap=(_ns(day, 11, 0), _ns(day, 11, 5))))
    w.close()
    res = verify_day(tmp_path, day)
    assert not res["passed"]
    assert res["checks"]["covers_market_open"] is False
    assert res["checks"]["no_gap_over_60s"] is False
