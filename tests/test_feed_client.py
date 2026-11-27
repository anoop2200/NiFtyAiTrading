"""Reconnect + resubscribe against a local fake Dhan feed server (Phase 1 acceptance: reconnect tested)."""
import asyncio
import json

import pytest
import websockets
from conftest import disconnect_packet, full_packet, ticker_packet

from app.data.feed_client import FeedClient, FeedFatal
from app.data.universe import Sub

SUBS = [Sub("IDX_I", 13, "ticker", "index"), Sub("NSE_FNO", 49081, "full", "option")]


async def _serve(handler):
    server = await websockets.serve(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return server, f"ws://127.0.0.1:{port}/?version=2&token=x&clientId=y&authType=2"


@pytest.mark.asyncio
async def test_reconnect_and_resubscribe():
    connections: list[list[dict]] = []

    async def handler(ws):
        subs = []
        connections.append(subs)
        for _ in range(2):  # ticker group + full group
            subs.append(json.loads(await ws.recv()))
        await ws.send(ticker_packet(13, 24000.0))
        await ws.send(full_packet(49081))
        if len(connections) == 1:
            await ws.close()  # drop the first connection
            return
        await asyncio.sleep(5)

    server, url = await _serve(handler)
    rows: list[dict] = []
    client = FeedClient(url, rows.extend, backoff=[0.05])
    await client.subscribe(SUBS)
    task = asyncio.create_task(client.run())
    for _ in range(100):
        if len(rows) >= 4:
            break
        await asyncio.sleep(0.05)
    client.stop()
    await asyncio.wait_for(task, 5)
    server.close()

    assert len(connections) == 2 and client.reconnects >= 1
    for subs in connections:  # every connection re-sent both subscription groups
        assert sorted(m["RequestCode"] for m in subs) == [15, 21]
    assert [r["seq"] for r in rows] == sorted(r["seq"] for r in rows) == list(range(1, len(rows) + 1))
    assert {r["conn_id"] for r in rows} == {1, 2}


@pytest.mark.asyncio
async def test_auth_disconnect_is_fatal():
    async def handler(ws):
        await ws.recv()
        await ws.send(disconnect_packet(807))
        await asyncio.sleep(1)

    server, url = await _serve(handler)
    client = FeedClient(url, lambda r: None, backoff=[0.05])
    await client.subscribe(SUBS[:1])
    with pytest.raises(FeedFatal):
        await asyncio.wait_for(client.run(), 5)
    server.close()


def test_subscription_batching_limit():
    client = FeedClient("ws://unused", lambda r: None, max_per_message=100)
    subs = [Sub("NSE_FNO", i, "full", "option") for i in range(250)]
    msgs = [json.loads(m) for m in client._messages(subs)]
    assert [m["InstrumentCount"] for m in msgs] == [100, 100, 50]
    assert all(isinstance(x["SecurityId"], str) for m in msgs for x in m["InstrumentList"])



@pytest.mark.asyncio
async def test_cancelled_receive_leaves_no_unretrieved_exception():
    """15:35 on 2026-11-27: the recorder cancels the feed task; the orphaned recv later failed with 1011 and asyncio
    printed 'Task exception was never retrieved'."""
    import gc
    loop = asyncio.get_running_loop()
    seen = []
    old = loop.get_exception_handler()
    loop.set_exception_handler(lambda lp, ctx: seen.append(ctx.get("message")))
    closed = asyncio.Event()

    class _WS:
        async def recv(self):
            await closed.wait()                     # socket closes only after the task was cancelled
            raise websockets.exceptions.ConnectionClosedError(None, None)

    client = FeedClient("ws://unused", lambda rows: None)
    try:
        t = asyncio.create_task(client._receive(_WS()))
        await asyncio.sleep(0.01)
        client.stop()
        t.cancel()                                  # exactly what Recorder.run's finally does
        await asyncio.gather(t, return_exceptions=True)
        del t                                       # its CancelledError traceback would keep the recv task alive
        closed.set()
        for _ in range(5):
            await asyncio.sleep(0)
        gc.collect()
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(old)
    assert not [m for m in seen if m and "never retrieved" in m], seen
