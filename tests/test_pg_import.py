"""PG importer against a fake read-only connection (no database needed)."""
from datetime import date, datetime, timedelta
from decimal import Decimal

import pyarrow.parquet as pq
from sqlalchemy.orm import Session

from app.clock import IST
from app.database.models import HistoricalImports
from app.historical.pg_import import COLUMNS, add_snapshot_ts, import_day, quality, rows_to_table

DAY = date(2026, 11, 20)


def fake_rows(n_snaps=30, step=10, gap_after=None):
    rows, rid = [], 0
    t = datetime(2026, 11, 20, 9, 15, 5)  # naive IST wall time, as returned by AT TIME ZONE
    for k in range(n_snaps):
        if gap_after is not None and k == gap_after:
            t += timedelta(seconds=120)
        for off in range(-5, 6):
            for ot in ("CE", "PE"):
                rid += 1
                bid = Decimal("100.10") if not (k == 3 and off == 0) else Decimal("101.00")  # one crossed
                rows.append((t + timedelta(microseconds=3 * rid % 900), "NIFTY", datetime(2026, 11, 25, 15, 30), Decimal("24000"), off,
                             Decimal(24000 + 50 * off), ot, Decimal("100.20"), 150000, 9000, Decimal("12.5"),
                             Decimal("100.0"), Decimal("0.5"), Decimal("0.001"), Decimal("-12.1"), Decimal("8.2"),
                             bid, Decimal("100.30"), 650, 975, "24010.55", rid))
        t += timedelta(seconds=step)
    return rows


class FakeCursor:
    def __init__(self, rows):
        self.rows, self.pos, self.itersize = rows, 0, 0

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        assert sql.lstrip().upper().startswith("SELECT"), "importer must only SELECT"

    def fetchmany(self, n):
        chunk = self.rows[self.pos:self.pos + n]
        self.pos += n
        return chunk


class FakeConn:
    def __init__(self, rows):
        self.rows = rows

    def cursor(self, name=None):
        return FakeCursor(self.rows)

    def commit(self):
        pass


def test_rows_to_table_and_quality():
    rows = fake_rows(gap_after=10)
    t = rows_to_table(rows, "observed_top_of_book_snapshot", "postgresql:filtered_option_data")
    assert t.num_rows == len(rows) and t.schema.field("ts").type.tz == "Asia/Kolkata"
    assert t["ts"][0].as_py().tzinfo is not None and t["ts"][0].as_py().hour == 9
    t = add_snapshot_ts(t)
    assert t.schema.names[-3] == "snapshot_ts"
    q = quality(t)
    assert q["rows_per_snapshot_median"] == 22
    assert q["snapshots"] == 30 and q["median_interval_s"] == 10.0
    assert q["gaps_over_60s"] == 1 and q["crossed_quotes"] == 2
    assert q["snapshots_outside_0900_1545_ist"] == 0
    assert len(COLUMNS) + 3 == len(t.schema)


def test_import_day_idempotent(tmp_path, engine):
    conn = FakeConn(fake_rows())
    r = import_day(conn, engine, tmp_path, "filtered_option_data", "NIFTY", DAY, "pg_test", "tier", 100)
    assert r["rows"] == 30 * 22
    f = tmp_path / "pg_test" / f"date={DAY}" / "part-0.parquet"
    assert pq.read_table(f).num_rows == 660
    assert import_day(conn, engine, tmp_path, "filtered_option_data", "NIFTY", DAY, "pg_test", "tier", 100) is None
    again = import_day(conn, engine, tmp_path, "filtered_option_data", "NIFTY", DAY, "pg_test", "tier", 100,
                       force=True)
    assert again["rows"] == 660
    with Session(engine) as s:
        recs = s.query(HistoricalImports).all()
        assert len(recs) == 1 and recs[0].snapshots == 30 and len(recs[0].sha256) == 64
        assert datetime.fromisoformat(recs[0].first_ts).utcoffset() == IST.utcoffset(datetime(2026, 1, 1))
