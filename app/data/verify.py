"""Validate one recorded day of raw Parquet (Phase 1 acceptance: timestamps/IDs validated, files readable)."""
from __future__ import annotations

from datetime import datetime, time as dtime
from pathlib import Path

import pyarrow.compute as pc
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.clock import IST
from app.data.parquet_store import read_stream
from app.database.models import Instruments

IST_OFFSET_S = 19800


def _ns_at(day: str, t: dtime) -> int:
    return int(datetime.combine(datetime.fromisoformat(day).date(), t, IST).timestamp() * 1e9)


def verify_day(root: Path, day: str, engine: Engine | None = None, market_open: dtime = dtime(9, 15),
               market_close: dtime = dtime(15, 30), liveness_s: float = 3.0) -> dict:
    checks: dict[str, bool] = {}
    out: dict = {"day": day}
    feed = read_stream(root, day, "feed")
    checks["feed_files_readable"] = True
    n = feed.num_rows
    out["feed_rows"] = n

    order = pc.sort_indices(feed, sort_keys=[("seq", "ascending")])
    seq = pc.take(feed["seq"], order).to_pylist()
    recv = pc.take(feed["recv_ns"], order).to_pylist()
    checks["seq_unique"] = len(set(seq)) == n
    checks["recv_monotonic_with_seq"] = all(b >= a for a, b in zip(recv, recv[1:]))

    out["packets_by_code"] = {str(x["values"]): x["counts"] for x in pc.value_counts(feed["code"]).to_pylist()}
    perr = pc.sum(pc.cast(pc.fill_null(feed["parse_error"], False), "int64")).as_py() or 0
    out["parse_errors"] = perr
    checks["parse_errors_below_0.01pct"] = perr <= max(1, n // 10000)
    out["server_disconnects"] = int(pc.sum(pc.cast(pc.equal(feed["code"], 50), "int64")).as_py() or 0)

    # market-hours coverage and liveness gaps
    o_ns, c_ns = _ns_at(day, market_open), _ns_at(day, market_close)
    mh = sorted(r for r in recv if o_ns <= r <= c_ns)
    gaps = [(b - a) / 1e9 for a, b in zip(mh, mh[1:])]
    out["first_packet"] = datetime.fromtimestamp(recv[0] / 1e9, IST).isoformat() if recv else None
    out["last_packet"] = datetime.fromtimestamp(recv[-1] / 1e9, IST).isoformat() if recv else None
    out["market_hours_max_gap_s"] = round(max(gaps), 2) if gaps else None
    out["market_hours_gaps_over_liveness"] = sum(1 for g in gaps if g > liveness_s)
    checks["covers_market_open"] = bool(mh) and mh[0] - o_ns <= 60e9
    checks["covers_market_close"] = bool(mh) and c_ns - mh[-1] <= 60e9
    checks["no_gap_over_60s"] = bool(gaps) and max(gaps) <= 60

    # instrument ids known to the dated instrument master
    ids = set(pc.unique(feed["security_id"]).to_pylist()) - {None}
    out["instruments_seen"] = len(ids)
    if engine is not None:
        with Session(engine) as s:
            known = set(s.scalars(select(Instruments.security_id).where(Instruments.master_date == day)))
        unknown = sorted(ids - known)
        out["unknown_security_ids"] = unknown[:20]
        checks["all_ids_in_master"] = bool(known) and not unknown

    full = feed.filter(pc.equal(feed["code"], 8))
    if full.num_rows:
        bid, ask = full["bid_px_1"], full["ask_px_1"]
        crossed = pc.and_(pc.greater(ask, 0), pc.greater(bid, ask))
        out["full_packets_crossed_top"] = int(pc.sum(pc.cast(crossed, "int64")).as_py() or 0)
        # exchange time vs local receive: reveals whether LTT is UTC epoch or IST-shifted epoch
        sample = full.slice(0, min(full.num_rows, 5000))
        diffs = sorted(r / 1e9 - l for r, l in zip(sample["recv_ns"].to_pylist(), sample["ltt"].to_pylist())
                       if l)
        if diffs:
            med = diffs[len(diffs) // 2]
            out["ltt_minus_recv_median_s"] = round(-med, 1)
            out["ltt_epoch_interpretation"] = ("IST-shifted (+19800 s)" if abs(-med - IST_OFFSET_S) < 3600
                                               else "UTC epoch" if abs(med) < 3600 else "unclear")

    try:
        chain = read_stream(root, day, "chain")
        out["chain_rows"] = chain.num_rows
        out["chain_snapshots"] = len(set(chain["snapshot_seq"].to_pylist()))
    except FileNotFoundError:
        out["chain_rows"] = 0

    out["checks"] = checks
    out["passed"] = all(checks.values())
    return out
