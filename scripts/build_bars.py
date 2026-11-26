"""Build 1-minute feed bars + the daily data-quality matrix for a recorded day (tier observed_feed).

Usage: .venv\\Scripts\\python scripts\\build_bars.py [YYYY-MM-DD] [--reconcile]
--reconcile compares index and futures bars with Dhan intraday candles for the same day.
"""
import argparse
import asyncio
import json
from datetime import date

import _bootstrap  # noqa: F401
import pyarrow.compute as pc

from app.clock import now_ist
from app.config import settings
from app.data.dhan_rest import DhanRest
from app.database.db import init_db, make_engine
from app.logging_setup import setup_logging
from app.market.mapping import InstrumentMap
from app.marketdata.pipeline import build_feed_day, minute_dict, record_build, save_quality, write_partition
from app.marketdata.reconcile import parse_candles, reconcile
from app.secrets import get_secret


async def candles(cfg, items: list[tuple[int, str, str]], day: str) -> dict:
    rest = DhanRest(cfg.get("dhan.rest_base"), get_secret("DHAN_CLIENT_ID"), get_secret("DHAN_ACCESS_TOKEN"))
    out = {}
    try:
        for sid, seg, inst in items:
            r = await rest.intraday_candles(sid, seg, inst, f"{day} 09:15:00", f"{day} 15:30:00")
            out[sid] = r.body if r.status == 200 else {"http_status": r.status, "text": r.text[:300]}
    finally:
        await rest.close()
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("day", nargs="?", default=now_ist().date().isoformat())
    ap.add_argument("--reconcile", action="store_true")
    a = ap.parse_args()
    cfg = settings()
    setup_logging("build_bars")
    engine = make_engine()
    init_db(engine)
    md = cfg.get("marketdata")
    try:
        imap = InstrumentMap.from_db(engine, date.fromisoformat(a.day))
        known, roles = set(imap.by_id), {k: c.role for k, c in imap.by_id.items()}
    except LookupError:
        imap, known, roles = None, None, {}
    res = build_feed_day(cfg.path("raw"), a.day, known, md["bar_grace_s"], md["bar_seconds"],
                         stale_after_s=md["staleness"]["max_change_age_s"])
    bars = res["bars"]
    path = write_partition(bars, cfg.path("derived"), "feed_bars_1m", a.day)
    save_quality(engine, a.day, res["quality"], roles)
    rec = None
    if a.reconcile:
        items = [(sid, "IDX_I", "INDEX") for sid in res["index_ids"]]
        if imap:
            items += [(f.security_id, "NSE_FNO", "FUTIDX") for f in imap.live_futures(date.fromisoformat(a.day))]
        bodies = asyncio.run(candles(cfg, items, a.day))
        rec = {}
        for sid, body in bodies.items():
            ours = bars.filter(pc.equal(bars["security_id"], sid))
            is_idx = sid in res["index_ids"]
            rec[str(sid)] = reconcile(minute_dict(ours, price_only=is_idx), parse_candles(body),
                                      check_volume=not is_idx)
    record_build(engine, "feed_bars_1m", a.day, "observed_feed", f"raw/date={a.day}/stream=feed", bars, path,
                 len(set(bars["security_id"].to_pylist())),
                 {"tf_s": md["bar_seconds"], "grace_s": md["bar_grace_s"]}, rec)
    report = {"day": a.day, "bars": bars.num_rows, "normalizer_flags": res["normalizer_counts"],
              "late_events": res["late_events"], "quality": res["quality"], "reconciliation": rec}
    out = cfg.path("reports") / f"bars_{a.day}.json"
    out.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(json.dumps({k: v for k, v in report.items() if k != "quality"}, indent=2, default=str)[:4000])
    print(f"report: {out}")
    ok = not rec or all(r["passed"] for r in rec.values())
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
