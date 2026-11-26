"""Phase 1 API smoke test (closes the evidence for OI-01, OI-02, OI-13).

Checks: token via /profile, instrument master download + validation, NIFTY spot via LTP, expiry list,
one option-chain call, and a short WebSocket capture. Saves market-data samples (never account data
or tokens) under data/reports/smoke/<timestamp>/.

Usage: .venv\\Scripts\\python scripts\\smoke_test.py [--ws-seconds 30]
"""
import argparse
import asyncio
import json
from collections import Counter

import _bootstrap  # noqa: F401
from app.clock import now_ist
from app.config import settings
from app.data.feed_client import FeedClient
from app.data.recorder import prepare
from app.database.db import init_db, make_engine
from app.logging_setup import setup_logging


async def main(ws_seconds: float) -> int:
    cfg = settings()
    setup_logging("smoke")
    engine = make_engine()
    init_db(engine)
    out = cfg.path("reports") / "smoke" / now_ist().strftime("%Y%m%d-%H%M%S")
    out.mkdir(parents=True, exist_ok=True)
    result: dict = {"started": now_ist().isoformat()}

    p = await prepare(cfg, engine, now_ist().date())
    result["auth"] = "ok"
    result["master"] = p.selection.report
    result["spot"] = p.spot
    result["universe"] = p.universe.describe()

    exp = await p.rest.expiry_list(p.universe.index.security_id)
    (out / "expiry_list.json").write_text(exp.text, encoding="utf-8")
    result["expiry_list_http"] = exp.status

    chain = await p.rest.option_chain(p.universe.index.security_id, p.universe.option_expiries[0].isoformat())
    (out / "option_chain_sample.json").write_text(chain.text, encoding="utf-8")
    result["option_chain_http"] = chain.status
    try:
        body = chain.body["data"]["oc"]
        k = next(iter(body))
        result["option_chain_fields"] = {"strike_key_example": k, "ce_keys": sorted(body[k].get("ce", {}))}
    except Exception as e:
        result["option_chain_fields"] = f"UNEXPECTED SHAPE: {e!r} (inspect option_chain_sample.json)"

    codes: Counter = Counter()
    per_instrument: Counter = Counter()
    samples: dict[int, str] = {}

    def on_rows(rows):
        for r in rows:
            codes[r["code"]] += 1
            per_instrument[r.get("security_id")] += 1
            samples.setdefault(r["code"], r["raw"].hex())

    url = (f"{cfg.get('dhan.feed_url')}?version=2&token={p.token}&clientId={p.client_id}&authType=2")
    feed = FeedClient(url, on_rows, lambda e, d: print("feed:", e, d))
    await feed.subscribe(p.universe.all())
    task = asyncio.create_task(feed.run())
    await asyncio.sleep(ws_seconds)
    feed.stop()
    await asyncio.gather(task, return_exceptions=True)
    await p.rest.close()

    result["ws"] = {"seconds": ws_seconds, "packets_by_code": dict(codes),
                    "instruments_seen": len(per_instrument), "instruments_subscribed": len(feed.subs),
                    "min_packets_per_instrument": min(per_instrument.values()) if per_instrument else 0}
    (out / "ws_packet_samples_hex.json").write_text(json.dumps(samples, indent=2), encoding="utf-8")
    (out / "smoke_result.json").write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print(json.dumps(result, indent=2, default=str))
    print(f"\nsaved to {out}")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--ws-seconds", type=float, default=30)
    raise SystemExit(asyncio.run(main(ap.parse_args().ws_seconds)))
