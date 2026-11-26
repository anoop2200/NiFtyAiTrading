"""Minimal async Dhan v2 REST client for data APIs (no order endpoints — paper trading only).

Field names of option-chain / quote responses are not fully confirmed (open issue OI-01/OI-02), so
callers keep the raw JSON alongside anything they parse.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

import httpx

SEGMENT_CODES = {"IDX_I": 0, "NSE_EQ": 1, "NSE_FNO": 2, "BSE_EQ": 4, "MCX_COMM": 5, "BSE_FNO": 8}
SEGMENT_NAMES = {v: k for k, v in SEGMENT_CODES.items()}


class DhanAuthError(RuntimeError):
    pass


class RateGate:
    """Enforce a minimum interval between calls sharing a key."""

    def __init__(self) -> None:
        self._last: dict[str, float] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    async def wait(self, key: str, min_interval: float) -> None:
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            gap = time.monotonic() - self._last.get(key, 0.0)
            if gap < min_interval:
                await asyncio.sleep(min_interval - gap)
            self._last[key] = time.monotonic()


@dataclass
class RestResult:
    status: int
    body: Any
    text: str
    request_ns: int
    response_ns: int


class DhanRest:
    def __init__(self, base_url: str, client_id: str, access_token: str,
                 chain_interval_s: float = 3.2, quote_interval_s: float = 1.05,
                 transport: httpx.AsyncBaseTransport | None = None):
        self._client = httpx.AsyncClient(
            base_url=base_url, timeout=10.0, transport=transport,
            headers={"access-token": access_token, "client-id": client_id,
                     "Content-Type": "application/json", "Accept": "application/json"})
        self._gate = RateGate()
        self.chain_interval_s = chain_interval_s
        self.quote_interval_s = quote_interval_s

    async def close(self) -> None:
        await self._client.aclose()

    async def _call(self, method: str, path: str, json_body: Any = None) -> RestResult:
        t0 = time.time_ns()
        r = await self._client.request(method, path, json=json_body)
        t1 = time.time_ns()
        try:
            body = r.json()
        except ValueError:
            body = None
        if r.status_code in (401, 403):
            raise DhanAuthError(f"Dhan auth rejected ({r.status_code}); token expired or invalid")
        return RestResult(r.status_code, body, r.text, t0, t1)

    async def profile(self) -> RestResult:
        return await self._call("GET", "/profile")

    async def ltp(self, instruments: dict[str, list[int]]) -> RestResult:
        await self._gate.wait("marketquote", self.quote_interval_s)
        return await self._call("POST", "/marketfeed/ltp", instruments)

    async def quote(self, instruments: dict[str, list[int]]) -> RestResult:
        await self._gate.wait("marketquote", self.quote_interval_s)
        return await self._call("POST", "/marketfeed/quote", instruments)

    async def expiry_list(self, underlying_scrip: int, underlying_seg: str = "IDX_I") -> RestResult:
        await self._gate.wait(f"chain:{underlying_scrip}:list", self.chain_interval_s)
        return await self._call("POST", "/optionchain/expirylist",
                                {"UnderlyingScrip": underlying_scrip, "UnderlyingSeg": underlying_seg})

    async def option_chain(self, underlying_scrip: int, expiry: str,
                           underlying_seg: str = "IDX_I") -> RestResult:
        await self._gate.wait(f"chain:{underlying_scrip}:{expiry}", self.chain_interval_s)
        return await self._call("POST", "/optionchain",
                                {"UnderlyingScrip": underlying_scrip, "UnderlyingSeg": underlying_seg,
                                 "Expiry": expiry})


def extract_ltp(body: Any, segment: str, security_id: int) -> float | None:
    """Best-effort LTP extraction from /marketfeed/ltp: {"data": {SEG: {"<id>": {"last_price": x}}}}."""
    try:
        node = body["data"][segment][str(security_id)]
        return float(node.get("last_price") or node.get("LTP"))
    except (KeyError, TypeError, ValueError):
        return None
