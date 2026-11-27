"""Option-chain REST poller. Raw JSON is always stored; the flattened view is best-effort because the
response field names are not yet confirmed against a live payload (open issue OI-02)."""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Callable

from loguru import logger

from app.data.dhan_rest import DhanAuthError, DhanRest

RATE_LIMIT_BACKOFF_S = (15.0, 120.0)   # pause after HTTP 429: 15 s, doubling to 2 min; reset on the next 200


def _f(x: Any) -> float | None:
    try:
        return None if x is None else float(x)
    except (TypeError, ValueError):
        return None


def _i(x: Any) -> int | None:
    try:
        return None if x is None else int(float(x))
    except (TypeError, ValueError):
        return None


def flatten_chain(body: Any, base: dict) -> list[dict]:
    data = (body or {}).get("data") or {}
    oc = data.get("oc") or {}
    ul = _f(data.get("last_price"))
    rows = []
    for strike_key, sides in oc.items():
        for side_key, opt_type in (("ce", "CE"), ("pe", "PE")):
            o = (sides or {}).get(side_key)
            if not o:
                continue
            g = o.get("greeks") or {}
            rows.append({**base, "underlying_ltp": ul, "strike": _f(strike_key), "option_type": opt_type,
                         "security_id": _i(o.get("security_id")), "last_price": _f(o.get("last_price")),
                         "average_price": _f(o.get("average_price")),
                         "previous_close_price": _f(o.get("previous_close_price")),
                         "oi": _i(o.get("oi")), "previous_oi": _i(o.get("previous_oi")),
                         "volume": _i(o.get("volume")), "previous_volume": _i(o.get("previous_volume")),
                         "implied_volatility": _f(o.get("implied_volatility")),
                         "delta": _f(g.get("delta")), "gamma": _f(g.get("gamma")),
                         "theta": _f(g.get("theta")), "vega": _f(g.get("vega")),
                         "top_bid_price": _f(o.get("top_bid_price")),
                         "top_bid_quantity": _i(o.get("top_bid_quantity")),
                         "top_ask_price": _f(o.get("top_ask_price")),
                         "top_ask_quantity": _i(o.get("top_ask_quantity"))})
    return rows


class ChainPoller:
    def __init__(self, rest: DhanRest, underlying_scrip: int, expiries: list[str],
                 on_raw: Callable[[dict], None], on_rows: Callable[[list[dict]], None],
                 on_event: Callable[[str, dict], None]):
        self.rest, self.scrip, self.expiries = rest, underlying_scrip, expiries
        self.on_raw, self.on_rows, self.on_event = on_raw, on_rows, on_event
        self.snapshots = 0
        self.errors = 0
        self.last_ok_ns = 0
        self.backoff_s = 0.0
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    def age_s(self) -> float | None:
        return None if not self.last_ok_ns else (time.time_ns() - self.last_ok_ns) / 1e9

    async def run(self) -> None:
        while not self._stop.is_set():
            for exp in list(self.expiries):
                if self._stop.is_set():
                    return
                try:
                    res = await self.rest.option_chain(self.scrip, exp)  # rate gate enforces 1 / 3 s per expiry
                except DhanAuthError as e:
                    self.on_event("chain_auth_error", {"error": str(e)})
                    return
                except Exception as e:  # network errors: record and continue
                    self.errors += 1
                    self.on_event("chain_error", {"expiry": exp, "error": repr(e)})
                    await asyncio.sleep(3)
                    continue
                recv = time.time_ns()
                self.snapshots += 1
                base = {"recv_ns": recv, "request_ns": res.request_ns, "snapshot_seq": self.snapshots,
                        "underlying_scrip": self.scrip, "expiry": exp}
                self.on_raw({**base, "http_status": res.status, "body": res.text})
                if res.status == 200:
                    rows = flatten_chain(res.body, base)
                    if rows:
                        self.last_ok_ns = recv
                        self.on_rows(rows)
                    else:
                        self.on_event("chain_unparsed", {"expiry": exp, "sample": res.text[:300]})
                else:
                    self.errors += 1
                    self.on_event("chain_http_error", {"expiry": exp, "status": res.status,
                                                       "body": res.text[:300]})
                    logger.warning(f"option chain {exp} HTTP {res.status}")
                    if res.status == 429:
                        # Dhan: "Further requests may result in the user being blocked" - back off, don't hammer
                        self.backoff_s = min(max(self.backoff_s * 2, RATE_LIMIT_BACKOFF_S[0]), RATE_LIMIT_BACKOFF_S[1])
                        self.on_event("chain_rate_limited", {"expiry": exp, "pause_s": self.backoff_s})
                        try:
                            await asyncio.wait_for(self._stop.wait(), timeout=self.backoff_s)
                        except asyncio.TimeoutError:
                            pass
                        continue
                if res.status == 200:
                    self.backoff_s = 0.0
            # with a single expiry the per-expiry rate gate paces the loop; nothing else to wait for
            await asyncio.sleep(0)


def raw_json(body: Any) -> str:
    return json.dumps(body, separators=(",", ":"))
