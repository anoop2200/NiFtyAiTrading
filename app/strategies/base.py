"""Strategy candidate contract (v1.3 §6 step 3, §7.2).

A strategy is deterministic: from final bars + indicator outputs it emits Candidates with an explicit
direction, setup key, thesis, underlying invalidation level and exit templates. It never sizes, never
checks risk and never talks to the broker."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ExitTemplate:
    stop_pct: float
    target_pct: float
    time_stop_s: int


@dataclass(frozen=True)
class Candidate:
    strategy_id: str
    setup_id: str                  # unique per setup instance within the session
    direction: int                 # +1 bullish underlying -> CE ; -1 bearish -> PE
    thesis: str
    invalidation: float            # underlying level; a close beyond it invalidates the setup
    normal: ExitTemplate
    zero_dte: ExitTemplate
    fresh_confirmation: bool = True
    needs_chain: bool = False
    features: dict = field(default_factory=dict)

    def cycle_key(self, session: str, profile: str = "NIFTY") -> str:
        side = "CE" if self.direction > 0 else "PE"
        return f"{profile}|{session}|{self.strategy_id}|{self.setup_id}|{side}"


@dataclass(frozen=True)
class NoTrade:
    strategy_id: str
    reason: str
    detail: dict = field(default_factory=dict)


def invalidated(direction: int, close: float, level: float) -> bool:
    return close < level if direction > 0 else close > level
