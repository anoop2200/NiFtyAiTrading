"""Quotes and the paper fill model (v1.3 §8.3).

* Entry (BUY) consumes the ask ladder, exit (SELL) the bid ladder; LTP is never a fill price.
* Each attempt is a marketable limit (IOC): it walks the ladder level by level up to a protection price
  (touch +/- max(protection_ticks, protection_pct)). Quantity at each level caps the fill -> partial fills
  and volume-weighted average price.
* Liquidity consumed from a quote is remembered, so retries only take liquidity from newer quotes.
* With top-of-book only (no depth ladder) the fill gets extra adverse slippage and is flagged low_fidelity.
* Attempts are refused (no fill) when the quote is missing/stale, a side is withdrawn (price or qty 0),
  the book is crossed, or no level lies within the protection price (spread widening).
"""
from __future__ import annotations

from dataclasses import dataclass, field

NS = 1_000_000_000


@dataclass(frozen=True)
class Quote:
    ts_ns: int
    key: str                               # contract key
    bids: tuple[tuple[float, int], ...]    # best first: (price, qty)
    asks: tuple[tuple[float, int], ...]
    ltp: float | None = None
    tier: str = "observed_feed"           # observed_feed (depth) | observed_top_of_book_snapshot | ...
    has_depth: bool = True                 # False -> only top of book is known

    @property
    def bid(self) -> float | None:
        return self.bids[0][0] if self.bids else None

    @property
    def ask(self) -> float | None:
        return self.asks[0][0] if self.asks else None

    @property
    def qid(self) -> str:
        return f"{self.key}@{self.ts_ns}"


@dataclass(frozen=True)
class FillConfig:
    tick: float = 0.05
    latency_ms: int = 250                  # decision -> simulated order arrival
    max_quote_age_s: float = 3.0           # a quote older than this at arrival is stale
    protection_pct: float = 1.0            # max adverse distance from touch, % of touch
    protection_ticks: int = 4              # ... but at least this many ticks
    low_fidelity_slippage_ticks: int = 1   # extra adverse slippage per unit when no depth ladder
    order_timeout_s: float = 30.0          # entry: give up remainder; exit: re-submit
    max_entry_spread_pct: float = 5.0      # entries refused while (ask-bid)/mid exceeds this (spread widening)
    version: str = "fill-1"


REASONS = ("NO_QUOTE", "STALE_QUOTE", "SIDE_WITHDRAWN", "CROSSED_BOOK", "SPREAD_TOO_WIDE", "PRICE_PROTECTION",
           "NO_LIQUIDITY")


@dataclass
class Attempt:
    filled: list[tuple[float, int]] = field(default_factory=list)   # (price, qty)
    reason: str | None = None                                       # why nothing (more) filled
    low_fidelity: bool = False
    touch: float | None = None
    limit: float | None = None

    @property
    def qty(self) -> int:
        return sum(q for _, q in self.filled)


def round_tick(px: float, tick: float, up: bool) -> float:
    n = px / tick
    n = int(n) + (1 if up and abs(n - int(n)) > 1e-9 else 0)
    return round(n * tick, 2)


def attempt_fill(side: str, qty: int, quote: Quote | None, now_ns: int, cfg: FillConfig,
                 consumed: dict[str, dict[int, int]]) -> Attempt:
    """One IOC attempt against `quote` (the latest quote known at `now_ns`)."""
    if quote is None:
        return Attempt(reason="NO_QUOTE")
    if (now_ns - quote.ts_ns) / NS > cfg.max_quote_age_s:
        return Attempt(reason="STALE_QUOTE")
    if quote.bid and quote.ask and quote.bid > quote.ask:
        return Attempt(reason="CROSSED_BOOK")
    ladder = quote.asks if side == "BUY" else quote.bids
    if not ladder or ladder[0][0] <= 0 or ladder[0][1] <= 0:
        return Attempt(reason="SIDE_WITHDRAWN")
    touch = ladder[0][0]
    if side == "BUY" and quote.bid and quote.bid > 0:  # long-only: BUY is always an entry
        mid = (quote.bid + touch) / 2
        if 100 * (touch - quote.bid) / mid > cfg.max_entry_spread_pct:
            return Attempt(reason="SPREAD_TOO_WIDE", touch=touch)
    room = max(cfg.protection_ticks * cfg.tick, touch * cfg.protection_pct / 100)
    limit = round(touch + room, 2) if side == "BUY" else round(max(touch - room, cfg.tick), 2)
    used = consumed.setdefault(quote.qid, {})
    a = Attempt(touch=touch, limit=limit, low_fidelity=not quote.has_depth)
    left = qty
    for lvl, (px, avail) in enumerate(ladder):
        if left <= 0:
            break
        if px <= 0 or avail <= 0:
            break
        if (side == "BUY" and px > limit + 1e-9) or (side == "SELL" and px < limit - 1e-9):
            a.reason = a.reason or "PRICE_PROTECTION"
            break
        free = avail - used.get(lvl, 0)
        if free <= 0:
            continue
        take = min(free, left)
        fill_px = px
        if not quote.has_depth:  # top of book only: conservative extra slippage, flagged
            slip = cfg.low_fidelity_slippage_ticks * cfg.tick
            fill_px = round(px + slip, 2) if side == "BUY" else round(max(px - slip, cfg.tick), 2)
        a.filled.append((fill_px, take))
        used[lvl] = used.get(lvl, 0) + take
        left -= take
    if left > 0 and a.reason is None:
        a.reason = "NO_LIQUIDITY"
    return a
