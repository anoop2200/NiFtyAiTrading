"""Plumbing-check strategy for the Phase 4 replay: NOT a trading strategy and not tuned.

Opening-range breakout on the index (15-minute range) -> buy the ATM CE (up) or PE (down) of the nearest
expiry with a live quote, 1 lot, premium-based stop/target, with the v1.3 0DTE profile on expiry days.
It exists to exercise the replay engine, broker, fees and audit on real history; strategy logic with the
full confirmation and risk rules is Phase 5.
"""
from __future__ import annotations

from datetime import date, datetime, time

from app.backtest.broker import EntryIntent
from app.backtest.fills import NS
from app.backtest.history import parse_key
from app.clock import IST
from app.market.calendar import TradingCalendar

NORMAL = {"stop_pct": 20.0, "target_pct": 30.0, "time_stop_s": 1800, "last_entry": time(15, 0)}
ZERO_DTE = {"stop_pct": 25.0, "target_pct": 40.0, "time_stop_s": 900, "last_entry": time(14, 30)}
MIN_PREMIUM = 30.0


class ORBDemo:
    strategy_id = "orb_demo_plumbing"

    def __init__(self, cal: TradingCalendar, lot_size: int, binding_key: str):
        self.cal, self.lot_size, self.binding = cal, lot_size, binding_key
        self.done: set[tuple[str, int]] = set()
        self.skips: dict[str, int] = {}

    def _skip(self, why: str) -> list:
        self.skips[why] = self.skips.get(why, 0) + 1
        return []

    def on_bar(self, stream, bar, ind, ctx):
        o = ind.get(self.binding)
        if stream != "index" or not o or not o["valid"] or o["state"] != "BREAKOUT":
            return []
        direction = int(o["direction"])
        now = datetime.fromtimestamp(ctx.now_ns / NS, IST)
        day = now.date()
        if (str(day), direction) in self.done:
            return []
        spot = bar["close"]
        cp = "CE" if direction > 0 else "PE"
        best = None
        for key, q in ctx.quotes().items():
            exp, k, ot = parse_key(key)
            e = date.fromisoformat(exp)
            if ot != cp or e < day or (ctx.now_ns - q.ts_ns) / NS > 15:
                continue
            rank = (e, abs(k - spot))
            if best is None or rank < best[0]:
                best = (rank, key, q, e)
        if best is None:
            return self._skip("no_fresh_contract_quote")
        _, key, q, exp = best
        if not q.ask or q.ask < MIN_PREMIUM:
            return self._skip("below_min_premium")
        dte = self.cal.dte(exp, day)
        prof = ZERO_DTE if dte == 0 else NORMAL
        if now.time() >= prof["last_entry"]:
            return self._skip("after_last_entry_time")
        self.done.add((str(day), direction))
        return [EntryIntent(cycle_key=f"NIFTY|{day}|{self.strategy_id}|orb|{cp}", strategy_id=self.strategy_id,
                            contract=key, lots=1, lot_size=self.lot_size, decision_ns=ctx.now_ns,
                            stop_pct=prof["stop_pct"], target_pct=prof["target_pct"],
                            time_stop_s=prof["time_stop_s"], dte=dte,
                            meta={"spot": spot, "or_high": o["or_high"], "or_low": o["or_low"],
                                  "profile": "0DTE" if dte == 0 else "normal", "ask": q.ask, "bid": q.bid})]
