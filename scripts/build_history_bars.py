"""Build 1-minute bars from the imported optionchain_db snapshot history (tier reconstructed_from_snapshots).

Optionally reconciles our reconstructed NIFTY spot bars against Dhan's index intraday candles
(read-only data API) as a real-data check of the bar logic and of the snapshot history itself.

Usage:
  .venv\\Scripts\\python scripts\\build_history_bars.py                      # all imported days
  .venv\\Scripts\\python scripts\\build_history_bars.py --from 2026-11-01 --reconcile
"""
import argparse
import asyncio
import json

import _bootstrap  # noqa: F401
from loguru import logger

from app.config import settings
from app.data.dhan_rest import DhanRest
from app.database.db import init_db, make_engine
from app.logging_setup import setup_logging
from app.marketdata.pipeline import (build_history_day, history_days, minute_dict, record_build,
                                     write_partition)
from app.marketdata.reconcile import SNAPSHOT_TOLERANCE, parse_candles, reconcile
from app.secrets import get_secret

SRC_TIER = "reconstructed_from_snapshots"


async def fetch_index_candles(cfg, day: str):
    rc = cfg.get("marketdata.reconcile")
    rest = DhanRest(cfg.get("dhan.rest_base"), get_secret("DHAN_CLIENT_ID"), get_secret("DHAN_ACCESS_TOKEN"))
    try:
        r = await rest.intraday_candles(rc["index_security_id"], rc["index_segment"], "INDEX",
                                        f"{day} 09:15:00", f"{day} 15:30:00")
    finally:
        await rest.close()
    if r.status != 200:
        raise RuntimeError(f"intraday candles HTTP {r.status}: {r.text[:300]}")
    return r.body


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="d_from")
    ap.add_argument("--to", dest="d_to")
    ap.add_argument("--reconcile", action="store_true", help="compare spot bars with Dhan index candles")
    a = ap.parse_args()
    cfg = settings()
    setup_logging("history_bars")
    engine = make_engine()
    init_db(engine)
    ds = cfg.get("historical_import.dataset")
    tf = cfg.get("marketdata.bar_seconds")
    days = [d for d in history_days(cfg.path("historical"), ds)
            if (not a.d_from or d >= a.d_from) and (not a.d_to or d <= a.d_to)]
    summary = []
    for day in days:
        opt, spot = build_history_day(cfg.path("historical"), ds, day, tf)
        rec = None
        if a.reconcile:
            body = asyncio.run(fetch_index_candles(cfg, day))
            raw_dir = cfg.path("derived") / "reconcile_raw"
            raw_dir.mkdir(parents=True, exist_ok=True)
            (raw_dir / f"index_{day}.json").write_text(json.dumps(body), encoding="utf-8")
            rec = reconcile(minute_dict(spot, price_only=True), parse_candles(body), SNAPSHOT_TOLERANCE,
                            check_volume=False)
        p_opt = write_partition(opt, cfg.path("derived"), "pg_option_bars_1m", day)
        p_spot = write_partition(spot, cfg.path("derived"), "pg_spot_bars_1m", day)
        n_inst = len(set(zip(opt["expiry"].to_pylist(), opt["strike"].to_pylist(),
                             opt["option_type"].to_pylist())))
        src = f"historical/{ds}/date={day}"
        record_build(engine, "pg_option_bars_1m", day, SRC_TIER, src, opt, p_opt, n_inst, {"tf_s": tf})
        record_build(engine, "pg_spot_bars_1m", day, SRC_TIER, src, spot, p_spot, 1, {"tf_s": tf}, rec)
        line = {"day": day, "option_bars": opt.num_rows, "spot_bars": spot.num_rows}
        if rec:
            line |= {k: rec[k] for k in ("ref_minutes", "common_minutes", "coverage_pct", "within_tolerance_pct",
                                         "passed", "field_failures", "mean_abs_err")}
        logger.info(json.dumps(line))
        summary.append(line)
    out = cfg.path("reports") / "history_bars_summary.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    if a.reconcile:
        passed = sum(1 for s in summary if s.get("passed"))
        print(f"reconciliation: {passed}/{len(summary)} days passed - {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
