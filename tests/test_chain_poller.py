import asyncio

import app.data.chain_poller as cp
from app.data.dhan_rest import RestResult


class _Rest:
    def __init__(self, statuses):
        self.statuses, self.calls = list(statuses), 0

    async def option_chain(self, scrip, exp):
        self.calls += 1
        st = self.statuses.pop(0) if self.statuses else 200
        return RestResult(st, {}, '{"data":{"805":"Too many requests"}}' if st == 429 else "{}", 0, 0)


def test_rate_limit_backs_off_instead_of_hammering(monkeypatch):
    """HTTP 429 pauses the poller (Dhan warns the account may be blocked) and doubles the pause; 200 resets it."""
    monkeypatch.setattr(cp, "RATE_LIMIT_BACKOFF_S", (0.05, 0.08))
    monkeypatch.setattr(cp, "flatten_chain", lambda body, base: [])
    events = []
    rest = _Rest([429, 429, 429])
    p = cp.ChainPoller(rest, 13, ["2026-12-01"], lambda r: None, lambda r: None, lambda k, d: events.append((k, d)))

    async def go():
        t = asyncio.create_task(p.run())
        while rest.calls < 4:
            await asyncio.sleep(0.01)
        p.stop()
        await t
    asyncio.run(asyncio.wait_for(go(), 5))
    pauses = [d["pause_s"] for k, d in events if k == "chain_rate_limited"]
    assert pauses == [0.05, 0.08, 0.08]                         # 15 s -> doubling -> capped (scaled down here)
    assert p.backoff_s == 0.0                                   # reset by the next 200
