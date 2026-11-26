r"""Backfill India VIX 1-minute bars from Dhan's intraday candles (read-only market-data API).

  .venv\Scripts\python scripts\backfill_vix.py                    # every history session (PG import days)
  .venv\Scripts\python scripts\backfill_vix.py --from 2026-11-01 --to 2026-11-25
  .venv\Scripts\python scripts\backfill_vix.py --coverage         # report only, no API calls

Bars go to data/historical/india_vix/india_vix_1m.parquet (upsert per session). Backtests, experiments and paper
replays pick them up automatically and record VIX with every decision; missing VIX is recorded as None.
"""
import argparse
import asyncio
import json

import _bootstrap  # noqa: F401

from app.config import settings
from app.data.dhan_rest import DhanRest
from app.historical import vix
from app.marketdata.pipeline import history_days
from app.secrets import get_secret

VIX_SECURITY_ID = 21     # India VIX in the Dhan master (IDX_I); verified against instrument_master_2026-11-26


async def fetch(cfg, days: list[str], security_id: int) -> list[dict]:
    rest = DhanRest(cfg.get("dhan.rest_base"), get_secret("DHAN_CLIENT_ID"), get_secret("DHAN_ACCESS_TOKEN"),
                    cfg.get("dhan.option_chain_min_interval_s"), cfg.get("dhan.market_quote_min_interval_s"))
    out = []
    try:
        for day in days:
            r = await rest.intraday_candles(security_id, "IDX_I", "INDEX", f"{day} 09:15:00", f"{day} 15:30:00")
            if r.status != 200:
                out.append({"day": day, "error": f"HTTP {r.status}: {r.text[:160]}"})
                continue
            bars = vix.candles_to_bars(r.body, day)
            vix.upsert(cfg.path("historical"), bars)
            out.append({"day": day, "bars": len(bars),
                        "close": float(bars["close"].iloc[-1]) if len(bars) else None})
    finally:
        await rest.close()
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--from", dest="d_from")
    ap.add_argument("--to", dest="d_to")
    ap.add_argument("--security-id", type=int, default=VIX_SECURITY_ID)
    ap.add_argument("--coverage", action="store_true")
    a = ap.parse_args()
    cfg = settings()
    days = [d for d in history_days(cfg.path("historical"), cfg.get("historical_import.dataset"))
            if (not a.d_from or d >= a.d_from) and (not a.d_to or d <= a.d_to)]
    if not a.coverage:
        for line in asyncio.run(fetch(cfg, days, a.security_id)):
            print(json.dumps(line))
    print(json.dumps(vix.coverage(cfg.path("historical"), days)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
