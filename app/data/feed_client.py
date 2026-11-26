"""Dhan live-feed WebSocket client with liveness tracking and automatic reconnect + resubscribe.

Each decoded packet gets a local receive timestamp (ns) and a session-wide sequence number before it
is handed to `on_rows`. Disconnect packets with auth codes (807/808/809/810) stop the client rather
than retry-looping on a dead token.
"""
from __future__ import annotations

import asyncio
import json
import time
from collections import Counter
from typing import Awaitable, Callable, Iterable

import websockets
from loguru import logger

from app.data.feed_parser import CODE_DISCONNECT, decode
from app.data.universe import Sub

MODE_REQUEST = {"ticker": 15, "quote": 17, "full": 21}
FATAL_DISCONNECT = {806, 807, 808, 809, 810}


class FeedFatal(RuntimeError):
    pass


class FeedClient:
    def __init__(self, url: str, on_rows: Callable[[list[dict]], None],
                 on_event: Callable[[str, dict], None] | None = None,
                 max_per_message: int = 100, backoff: Iterable[float] = (1, 2, 5, 10, 20, 30)):
        self.url = url
        self.on_rows = on_rows
        self.on_event = on_event or (lambda e, d: None)
        self.max_per_message = max_per_message
        self.backoff = list(backoff)
        self.subs: dict[tuple[str, int], Sub] = {}
        self.seq = 0
        self.conn_id = 0
        self.connected = False
        self.last_packet_ns = 0
        self.packets = 0
        self.by_code: Counter = Counter()
        self.reconnects = 0
        self._ws = None
        self._stop = asyncio.Event()

    # ---- subscriptions -------------------------------------------------
    def _messages(self, subs: list[Sub]) -> list[str]:
        msgs = []
        for mode, code in MODE_REQUEST.items():
            group = [s for s in subs if s.mode == mode]
            for i in range(0, len(group), self.max_per_message):
                chunk = group[i:i + self.max_per_message]
                msgs.append(json.dumps({"RequestCode": code, "InstrumentCount": len(chunk), "InstrumentList": [
                    {"ExchangeSegment": s.segment, "SecurityId": str(s.security_id)} for s in chunk]}))
        return msgs

    async def subscribe(self, subs: list[Sub]) -> None:
        new = [s for s in subs if (s.segment, s.security_id) not in self.subs]
        for s in new:
            self.subs[(s.segment, s.security_id)] = s
        if self._ws is not None and self.connected and new:
            for m in self._messages(new):
                await self._ws.send(m)
            self.on_event("subscribed", {"count": len(new), "total": len(self.subs)})

    # ---- liveness ------------------------------------------------------
    def feed_age_s(self) -> float | None:
        return None if not self.last_packet_ns else (time.time_ns() - self.last_packet_ns) / 1e9

    async def force_reconnect(self, reason: str) -> None:
        """Close a connection that is open but silent (half-open socket); run() then reconnects."""
        if self._ws is not None:
            self.on_event("forced_reconnect", {"reason": reason, "conn_id": self.conn_id})
            await self._ws.close()

    # ---- run loop ------------------------------------------------------
    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        attempt = 0
        while not self._stop.is_set():
            try:
                async with websockets.connect(self.url, ping_interval=None, max_size=2**22,
                                              open_timeout=15, close_timeout=5) as ws:
                    # server sends pings every ~10 s; websockets answers them automatically
                    self._ws, self.connected, self.conn_id = ws, True, self.conn_id + 1
                    attempt = 0
                    self.on_event("connected", {"conn_id": self.conn_id, "subscriptions": len(self.subs)})
                    for m in self._messages(list(self.subs.values())):
                        await ws.send(m)
                    await self._receive(ws)
            except FeedFatal:
                raise
            except (OSError, websockets.WebSocketException, asyncio.TimeoutError) as e:
                self.on_event("disconnected", {"conn_id": self.conn_id, "error": repr(e)})
            finally:
                self.connected, self._ws = False, None
            if self._stop.is_set():
                break
            delay = self.backoff[min(attempt, len(self.backoff) - 1)]
            attempt += 1
            self.reconnects += 1
            logger.warning(f"feed reconnect #{self.reconnects} in {delay}s")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    async def _receive(self, ws) -> None:
        stop_task = asyncio.create_task(self._stop.wait())
        try:
            while True:
                recv_task = asyncio.create_task(ws.recv())
                done, _ = await asyncio.wait({recv_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
                if stop_task in done:
                    recv_task.cancel()
                    # the socket may already have failed (e.g. 1011 on disconnect): retrieve it so asyncio does not
                    # print "Task exception was never retrieved" at shutdown
                    recv_task.add_done_callback(lambda t: t.cancelled() or t.exception())
                    try:
                        await ws.send(json.dumps({"RequestCode": 12}))
                    except Exception:
                        pass
                    return
                msg = recv_task.result()
                now = time.time_ns()
                if isinstance(msg, str):
                    self.on_event("text_message", {"text": msg[:500]})
                    continue
                rows = decode(msg)
                for r in rows:
                    self.seq += 1
                    r["recv_ns"], r["seq"], r["conn_id"] = now, self.seq, self.conn_id
                    self.by_code[r["code"]] += 1
                    if r["code"] == CODE_DISCONNECT:
                        code = r.get("disconnect_code")
                        self.on_event("server_disconnect", {"code": code})
                        if code in FATAL_DISCONNECT:
                            self.on_rows(rows)
                            raise FeedFatal(f"Dhan feed disconnect code {code} (auth/subscription)")
                self.packets += len(rows)
                self.last_packet_ns = now
                self.on_rows(rows)
        finally:
            stop_task.cancel()
