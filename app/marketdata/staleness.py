"""Staleness model (v1.3 §4.3): connection-level feed liveness is separate from per-instrument quote
change age. A quote that has not changed on a live feed is QUIET (confirm via REST before entry), not
STALE."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Freshness(str, Enum):
    FRESH = "FRESH"
    QUIET_CONFIRM = "QUIET_CONFIRM"   # live feed, quote unchanged > confirm_after_s: confirm with REST snapshot
    STALE = "STALE"                   # live feed, quote unchanged > max_change_age_s
    FEED_DOWN = "FEED_DOWN"           # connection silent > liveness_s
    NO_DATA = "NO_DATA"


@dataclass
class StalenessPolicy:
    liveness_s: float = 3.0
    confirm_after_s: float = 5.0
    max_change_age_s: float = 30.0
    underlying_max_change_age_s: float = 10.0


@dataclass
class FreshnessTracker:
    policy: StalenessPolicy = field(default_factory=StalenessPolicy)
    last_packet_ns: int | None = None
    last_change_ns: dict[int, int] = field(default_factory=dict)
    underlying_ids: set[int] = field(default_factory=set)

    def on_event(self, ev: dict) -> None:
        self.last_packet_ns = ev["recv_ns"]
        if not (ev.get("flags", 0) & 1):  # not a duplicate
            self.last_change_ns[ev["security_id"]] = ev["recv_ns"]

    def feed_alive(self, now_ns: int) -> bool:
        return self.last_packet_ns is not None and (now_ns - self.last_packet_ns) / 1e9 <= self.policy.liveness_s

    def evaluate(self, now_ns: int, security_id: int) -> Freshness:
        if not self.feed_alive(now_ns):
            return Freshness.FEED_DOWN
        last = self.last_change_ns.get(security_id)
        if last is None:
            return Freshness.NO_DATA
        age = (now_ns - last) / 1e9
        limit = (self.policy.underlying_max_change_age_s if security_id in self.underlying_ids
                 else self.policy.max_change_age_s)
        if age > limit:
            return Freshness.STALE
        if age > self.policy.confirm_after_s and security_id not in self.underlying_ids:
            return Freshness.QUIET_CONFIRM
        return Freshness.FRESH
