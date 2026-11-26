"""One-time / incremental import of option-chain snapshots from the existing PostgreSQL database
(optionchain_db.filtered_option_data) into date-partitioned Parquet under data/historical.

STRICTLY READ-ONLY on the source: the session is set READ ONLY before any query, and only SELECTs on
information_schema and the source table are issued. Each imported day is recorded in the
HistoricalImports table with row counts, a checksum and data-quality metrics. Re-running skips days
already imported unless --force is given.

Data tier: observed top-of-book snapshots (~10 s cadence, ATM ± 5) — bid/ask observed, no depth.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterator

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.clock import IST, iso
from app.database.models import HistoricalImports
from app.secrets import get_secret

# source column -> (target column, arrow type). Quoted identifiers keep the source's mixed case.
COLUMNS: list[tuple[str, str, pa.DataType]] = [
    # Same IST conversion the existing OptionEngine reader uses (OptionChainDb.SelectColumns).
    ('("timestamp" AT TIME ZONE \'Asia/Kolkata\')', "ts", pa.timestamp("us", tz="Asia/Kolkata")),
    ('"Symbol"', "symbol", pa.string()),
    ('("ExpiryDate" AT TIME ZONE \'Asia/Kolkata\')', "expiry_ts", pa.timestamp("us", tz="Asia/Kolkata")),
    ("atm_strike", "atm_strike", pa.float64()),
    ("strike_offset", "strike_offset", pa.int32()),
    ("strike_price", "strike", pa.float64()),
    ("option_type", "option_type", pa.string()),
    ("ltp", "ltp", pa.float64()),
    ("oi", "oi", pa.int64()),
    ("volume", "volume", pa.int64()),
    ("iv", "iv", pa.float64()),
    ("vwap", "vwap", pa.float64()),
    ("delta", "delta", pa.float64()),
    ("gamma", "gamma", pa.float64()),
    ("theta", "theta", pa.float64()),
    ("vega", "vega", pa.float64()),
    ("bid_price", "bid", pa.float64()),
    ("ask_price", "ask", pa.float64()),
    ("bid_qty", "bid_qty", pa.int64()),
    ("ask_qty", "ask_qty", pa.int64()),
    ("(analysis_snapshot->>'Spot')", "spot", pa.float64()),
    ("id", "src_id", pa.int64()),
]
REQUIRED_SOURCE_COLUMNS = {"timestamp", "Symbol", "ExpiryDate", "atm_strike", "strike_offset", "strike_price",
                           "option_type", "ltp", "oi", "volume", "iv", "vwap", "delta", "gamma", "theta", "vega",
                           "bid_price", "ask_price", "bid_qty", "ask_qty", "analysis_snapshot", "id"}
SCHEMA = pa.schema([(t, ty) for _, t, ty in COLUMNS]
                   + [("snapshot_ts", pa.timestamp("us", tz="Asia/Kolkata")), ("data_tier", pa.string()),
                      ("source", pa.string())])


@dataclass
class PgConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str

    @classmethod
    def from_secrets(cls) -> "PgConfig":
        return cls(get_secret("PG_HOST"), int(get_secret("PG_PORT", required=False) or 5432),
                   get_secret("PG_DB"), get_secret("PG_USER"), get_secret("PG_PASSWORD"))


def connect_read_only(cfg: PgConfig):
    import psycopg

    conn = psycopg.connect(host=cfg.host, port=cfg.port, dbname=cfg.dbname, user=cfg.user,
                           password=cfg.password, options="-c default_transaction_read_only=on",
                           application_name="NiftyTradingAI-import (read-only)")
    with conn.cursor() as cur:
        cur.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
        cur.execute("SHOW transaction_read_only")
        if cur.fetchone()[0] != "on":
            conn.close()
            raise RuntimeError("could not make the PostgreSQL session read-only; aborting")
    conn.commit()
    return conn


def check_source(conn, table: str) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_schema='public' AND table_name=%s", (table,))
        cols = dict(cur.fetchall())
    if not cols:
        raise RuntimeError(f"source table public.{table} not found")
    missing = sorted(REQUIRED_SOURCE_COLUMNS - set(cols))
    if missing:
        raise RuntimeError(f"source table {table} is missing columns {missing}; found {sorted(cols)}")
    return cols


def list_days(conn, table: str, symbol: str) -> list[date]:
    with conn.cursor() as cur:
        cur.execute(f'SELECT DISTINCT ("timestamp" AT TIME ZONE \'Asia/Kolkata\')::date d FROM {table} '
                    'WHERE upper("Symbol") = upper(%s) ORDER BY d', (symbol,))
        return [r[0] for r in cur.fetchall()]


def _fetch_day(conn, table: str, symbol: str, day: date, batch: int) -> Iterator[list[tuple]]:
    cols = ", ".join(src for src, _, _ in COLUMNS)
    sql = (f"SELECT {cols} FROM {table} WHERE upper(\"Symbol\") = upper(%s) "
           f"AND (\"timestamp\" AT TIME ZONE 'Asia/Kolkata')::date = %s ORDER BY \"timestamp\", id")
    with conn.cursor(name=f"imp_{day:%Y%m%d}") as cur:  # server-side cursor: bounded memory
        cur.itersize = batch
        cur.execute(sql, (symbol, day))
        while True:
            rows = cur.fetchmany(batch)
            if not rows:
                break
            yield rows
    conn.commit()


def _to_ist(v):
    if v is None:
        return None
    return v.replace(tzinfo=IST) if v.tzinfo is None else v.astimezone(IST)


def rows_to_table(rows: list[tuple], tier: str, source: str) -> pa.Table:
    cols: dict[str, list] = {t: [] for _, t, _ in COLUMNS}
    names = [t for _, t, _ in COLUMNS]
    for r in rows:
        for name, v in zip(names, r):
            if name in ("ts", "expiry_ts"):
                v = _to_ist(v)
            elif v is not None and name in ("atm_strike", "strike", "ltp", "iv", "vwap", "delta", "gamma",
                                             "theta", "vega", "bid", "ask", "spot"):
                v = float(v)
            elif v is not None and name in ("oi", "volume", "bid_qty", "ask_qty", "src_id", "strike_offset"):
                v = int(v)
            elif name == "option_type" and v is not None:
                v = str(v).upper()
            cols[name].append(v)
    n = len(rows)
    cols["data_tier"] = [tier] * n
    cols["source"] = [source] * n
    schema = pa.schema([f for f in SCHEMA if f.name != "snapshot_ts"])
    return pa.Table.from_pydict(cols, schema=schema)


SNAPSHOT_GAP_US = 1_000_000  # rows written within 1 s of each other belong to one chain snapshot


def add_snapshot_ts(table: pa.Table) -> pa.Table:
    """Source rows of one snapshot are inserted microseconds apart (~22 rows every ~10.7 s). Tag each
    row with the timestamp of the first row of its batch so a snapshot can be selected as a unit."""
    import numpy as np

    table = table.sort_by([("ts", "ascending"), ("src_id", "ascending")])
    us = table["ts"].cast(pa.timestamp("us", tz="Asia/Kolkata")).cast(pa.int64()).to_numpy(zero_copy_only=False)
    starts = np.ones(len(us), dtype=bool)
    starts[1:] = np.diff(us) > SNAPSHOT_GAP_US
    snap_us = us[np.maximum.accumulate(np.where(starts, np.arange(len(us)), 0))]
    snap = pa.array(snap_us, type=pa.int64()).cast(pa.timestamp("us", tz="Asia/Kolkata"))
    return table.append_column("snapshot_ts", snap).select(SCHEMA.names)


def quality(table: pa.Table) -> dict:
    import numpy as np
    import pyarrow.compute as pc

    n = table.num_rows
    if n == 0:
        return {"rows": 0}
    snap_us = np.unique(table["snapshot_ts"].cast(pa.int64()).to_numpy(zero_copy_only=False))
    gaps = np.diff(snap_us) / 1e6
    sizes = np.unique(table["snapshot_ts"].cast(pa.int64()).to_numpy(zero_copy_only=False), return_counts=True)[1]
    stamps = table["snapshot_ts"]
    first, last = stamps[0].as_py(), stamps[-1].as_py()
    hhmm = [(t.hour, t.minute) for t in pc.unique(stamps).to_pylist()]
    bid, ask = table["bid"], table["ask"]
    crossed = pc.sum(pc.cast(pc.and_(pc.greater(bid, 0), pc.greater(bid, ask)), pa.int64())).as_py() or 0
    zero_bid = pc.sum(pc.cast(pc.less_equal(pc.fill_null(bid, 0), 0), pa.int64())).as_py() or 0
    # market-hours view (09:15-15:30 IST): what backtests actually use
    snap_dt = pc.unique(stamps).to_pylist()
    mh = sorted(t for t in snap_dt if (9, 15) <= (t.hour, t.minute) < (15, 30))
    mh_gaps = np.diff([t.timestamp() for t in mh]) if len(mh) > 1 else np.array([])
    return {
        "mh_snapshots": len(mh),
        "mh_first": mh[0].isoformat() if mh else None, "mh_last": mh[-1].isoformat() if mh else None,
        "mh_median_interval_s": round(float(np.median(mh_gaps)), 2) if len(mh_gaps) else None,
        "mh_max_gap_s": round(float(mh_gaps.max()), 1) if len(mh_gaps) else None,
        "mh_gaps_over_60s": int((mh_gaps > 60).sum()),
        "mh_complete": bool(mh) and (mh[0].hour, mh[0].minute) <= (9, 16)
                       and (mh[-1].hour, mh[-1].minute) >= (15, 29) and not (mh_gaps > 300).any(),
        "rows": n, "snapshots": int(len(snap_us)),
        "rows_per_snapshot_median": int(np.median(sizes)),
        "first_ts": first.isoformat(), "last_ts": last.isoformat(),
        "pre_open_snapshots": sum(1 for x in hhmm if x < (9, 15)),
        "snapshots_outside_0900_1545_ist": sum(1 for x in hhmm if not ((9, 0) <= x <= (15, 45))),
        "median_interval_s": round(float(np.median(gaps)), 2) if len(gaps) else None,
        "max_gap_s": round(float(gaps.max()), 1) if len(gaps) else None,
        "gaps_over_60s": int((gaps > 60).sum()),
        "crossed_quotes": int(crossed), "zero_or_null_bid": int(zero_bid),
        "null_spot": table["spot"].null_count, "null_iv": table["iv"].null_count,
        "expiries": sorted({e.date().isoformat() for e in table["expiry_ts"].to_pylist() if e}),
    }


def import_day(conn, engine: Engine, out_root: Path, table: str, symbol: str, day: date, dataset: str,
               tier: str, batch: int, force: bool = False) -> dict | None:
    with Session(engine) as s:
        done = s.scalar(select(HistoricalImports).where(HistoricalImports.dataset == dataset,
                                                        HistoricalImports.trade_date == day.isoformat()))
        if done and not force:
            return None
    source = f"postgresql:{table}"
    dest_dir = out_root / dataset / f"date={day.isoformat()}"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / "part-0.parquet"
    tmp = dest.with_suffix(".tmp")
    parts = [rows_to_table(rows, tier, source)
             for rows in _fetch_day(conn, table, symbol, day, batch)]
    if not parts:
        return {"day": day.isoformat(), "rows": 0}
    full = add_snapshot_ts(pa.concat_tables(parts))  # one day ~40k rows: fits in memory
    pq.write_table(full, tmp, compression="zstd")
    tmp.replace(dest)
    q = quality(full)
    sha = hashlib.sha256(dest.read_bytes()).hexdigest()
    with Session(engine) as s:
        if done:
            s.delete(s.merge(done))
            s.flush()
        s.add(HistoricalImports(dataset=dataset, source=source, data_tier=tier, trade_date=day.isoformat(),
                                rows=q["rows"], snapshots=q["snapshots"], first_ts=q["first_ts"],
                                last_ts=q["last_ts"], file_path=str(dest), sha256=sha,
                                quality=json.dumps(q), imported_at=iso()))
        s.commit()
    logger.info(f"{day}: {q['rows']} rows, {q['snapshots']} snapshots, median {q['median_interval_s']}s")
    return {"day": day.isoformat(), **q}
