"""Run the market-data recorder for today's session (until session.record_end, or --duration-min).

Usage: .venv\\Scripts\\python scripts\\record_session.py [--duration-min 20] [--no-chain]
Stop early with Ctrl+C: buffers are flushed and the session is marked INTERRUPTED.
"""
import argparse
import asyncio

import _bootstrap  # noqa: F401
from loguru import logger

from app.clock import now_ist
from app.config import settings
from app.data.recorder import Recorder, end_time_today, prepare
from app.database.db import init_db, make_engine
from app.logging_setup import setup_logging


async def main(args) -> int:
    cfg = settings()
    setup_logging("market-data-engine")
    engine = make_engine()
    init_db(engine)
    end_at = end_time_today(cfg, args.duration_min)
    if end_at <= now_ist():
        logger.error(f"session end {end_at:%H:%M} already passed; use --duration-min for a test run")
        return 2
    if now_ist().weekday() >= 5:
        logger.warning("weekend: the feed will be quiet (exchange calendar arrives in Phase 2)")
    p = await prepare(cfg, engine, now_ist().date(), args.spot)
    logger.info(f"universe {p.universe.describe()}; recording until {end_at:%H:%M:%S}")
    status = await Recorder(cfg, engine, p, end_at, chain_enabled=not args.no_chain).run()
    logger.info(f"session {status}")
    return 0 if status == "COMPLETED" else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration-min", type=float, default=None)
    ap.add_argument("--no-chain", action="store_true")
    ap.add_argument("--spot", type=float, default=None, help="override spot if the LTP API is unavailable")
    try:
        raise SystemExit(asyncio.run(main(ap.parse_args())))
    except KeyboardInterrupt:
        raise SystemExit(130)
