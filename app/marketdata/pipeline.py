"""Batch pipelines: recorded feed day -> normalized events -> 1-minute bars + quality matrix, and
optionchain_db snapshot history -> reconstructed bars. Outputs go to <derived>/<dataset>/date=YYYY-MM-DD/."""
from __future__ import annotations

import hashlib
import json
import os
from datetime import date, datetime, time as dtime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from sqlalchemy import delete
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.clock import IST, iso
from app.data.dhan_rest import SEGMENT_CODES
from app.data.parquet_store import read_stream
from app.database.models import BarBuilds, DataQualityDaily
from app.marketdata.bars import BarBuilder, bars_table, snapshot_bars
from app.marketdata.normalize import Normalizer
from app.marketdata.quality import QualityMatrix

CODE_VERSION = "phase2-1"


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def write_partition(table: pa.Table, root: Path, dataset: str, day: str) -> Path:
    d = Path(root) / dataset / f"date={day}"
    d.mkdir(parents=True, exist_ok=True)
    final, tmp = d / "part-0.parquet", d / "part-0.parquet.tmp"
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, final)
    return final


def record_build(engine: Engine, dataset: str, day: str, tier: str, source: str, table: pa.Table, path: Path,
                 instruments: int, params: dict, reconciliation: dict | None = None) -> None:
    with Session(engine) as s:
        s.execute(delete(BarBuilds).where(BarBuilds.dataset == dataset, BarBuilds.trade_date == day))
        s.add(BarBuilds(dataset=dataset, trade_date=day, data_tier=tier, source=source, rows=table.num_rows,
                        instruments=instruments, file_path=str(path), sha256=_sha(path),
                        params=json.dumps(params | {"code_version": CODE_VERSION}),
                        reconciliation=json.dumps(reconciliation, default=str) if reconciliation else None,
                        built_at=iso()))
        s.commit()


def _ns(day: str, t: dtime) -> int:
    return int(datetime.combine(date.fromisoformat(day), t, IST).timestamp() * 1e9)


def build_feed_day(raw_root: Path, day: str, known_ids: set[int] | None = None, grace_s: float = 2.0,
                   tf_s: int = 60, market_open: dtime = dtime(9, 15), market_close: dtime = dtime(15, 30),
                   stale_after_s: float = 30.0) -> dict:
    """Replay a recorded day in receive order through the same streaming code used live."""
    feed = read_stream(raw_root, day, "feed")
    feed = feed.sort_by([("recv_ns", "ascending"), ("seq", "ascending")])
    index_ids = set(pc.unique(pc.filter(feed["security_id"],
                                        pc.equal(feed["segment"], SEGMENT_CODES["IDX_I"]))).to_pylist())
    norm = Normalizer(known_ids)
    bb = BarBuilder(tf_s=tf_s, grace_s=grace_s, index_ids=index_ids)
    qm = QualityMatrix(_ns(day, market_open), _ns(day, market_close), stale_after_s)
    open_s, close_s = _ns(day, market_open) / 1e9, _ns(day, market_close) / 1e9
    rows = []
    for r in feed.to_pylist():
        ev = norm.process(r)
        if ev is None:
            continue
        qm.on_event(ev)
        rows.extend(bb.on_event(ev))
    rows.extend(bb.flush())
    rows = [r for r in rows if open_s <= r["ts"] < close_s]
    bars = bars_table(rows)
    per_sid: dict[int, int] = {}
    for r in rows:
        per_sid[r["security_id"]] = per_sid.get(r["security_id"], 0) + 1
    return {"bars": bars, "quality": qm.finalize(per_sid), "normalizer_counts": dict(norm.counts),
            "late_events": bb.late_total, "index_ids": sorted(index_ids)}


def save_quality(engine: Engine, day: str, quality: list[dict], roles: dict[int, str] | None = None) -> None:
    with Session(engine) as s:
        s.execute(delete(DataQualityDaily).where(DataQualityDaily.trade_date == day))
        for q in quality:
            s.add(DataQualityDaily(trade_date=day, role=(roles or {}).get(q["security_id"]),
                                   **{k: (json.dumps(v) if k == "flags" else v) for k, v in q.items()}))
        s.commit()


def history_day(historical_root: Path, dataset: str, day: str) -> pa.Table:
    return pq.read_table(Path(historical_root) / dataset / f"date={day}" / "part-0.parquet")


def history_days(historical_root: Path, dataset: str) -> list[str]:
    return sorted(p.name[5:] for p in (Path(historical_root) / dataset).glob("date=*"))


def build_history_day(historical_root: Path, dataset: str, day: str, tf_s: int = 60) -> tuple[pa.Table, pa.Table]:
    return snapshot_bars(history_day(historical_root, dataset, day), tf_s)


def minute_dict(table: pa.Table, price_only: bool = False) -> dict[int, dict]:
    """{epoch_s: {open, high, low, close, volume}} from a bar table with a `ts` column."""
    secs = pc.cast(pc.cast(table["ts"], pa.timestamp("s", tz="UTC")), pa.int64()).to_pylist()
    cols = {c: table[c].to_pylist() for c in ("open", "high", "low", "close")}
    vols = table["volume"].to_pylist() if ("volume" in table.column_names and not price_only) else None
    return {t: {"open": cols["open"][i], "high": cols["high"][i], "low": cols["low"][i],
                "close": cols["close"][i], "volume": vols[i] if vols else None} for i, t in enumerate(secs)}
