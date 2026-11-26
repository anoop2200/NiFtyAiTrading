"""Opening-range breakout (v1.3 §7.1).

Range = 09:15-09:30 (opening_range indicator, 15 minutes). A candidate is created only for a *confirmed*
break: close beyond the range by >= confirm_atr x ATR, the breakout bar's range >= expansion_atr x ATR, and
futures participation (relative futures volume >= min_rel_volume) when `require_futures` is on. Failed or
uncertain breaks return NO TRADE. A break that fails (close back inside the range) ends that setup; a later
break on either side is a new setup with a fresh confirmation. Invalidation: a close back inside the range.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from app.indicators.engine import binding
from app.strategies.base import Candidate, ExitTemplate, NoTrade



@dataclass
class ORBConfig:
    confirm_atr: float = 0.25
    expansion_atr: float = 1.0
    min_rel_volume: float = 1.2
    require_futures: bool = True
    min_range_pct: float = 0.10       # opening range narrower than this is noise
    max_range_pct: float = 1.20       # wider than this: move already spent
    normal: ExitTemplate = field(default_factory=lambda: ExitTemplate(20.0, 30.0, 1800))
    zero_dte: ExitTemplate = field(default_factory=lambda: ExitTemplate(25.0, 40.0, 900))
    version: str = "orb-1.0.0"


class OpeningRangeBreakout:
    strategy_id = "orb"

    def __init__(self, cfg: ORBConfig | None = None):
        self.cfg = cfg or ORBConfig()
        self.bindings = [binding("opening_range", "index", minutes=15), binding("atr", "index", period=14)]
        self.or_key, self.atr_key = (b.key for b in self.bindings)
        fr = binding("fut_rel_volume", "future", period=20, high=2.0)
        self.frv_key = fr.key
        self.bindings.append(fr)
        self.session = None
        self.setup_n = 0
        self.inside = True          # price currently inside the range (a new break can start a setup)
        self.active_dir = 0         # direction of the setup in progress (0 = none)

    def _new_session(self, session: str) -> None:
        self.session, self.setup_n, self.inside, self.active_dir = session, 0, True, 0

    def on_bar(self, session: str, bar: dict, ind: dict) -> list:
        if session != self.session:
            self._new_session(session)
        o, a = ind.get(self.or_key), ind.get(self.atr_key)
        if not o or not o.get("valid"):
            return []
        c = bar["close"]
        hi, lo = o["or_high"], o["or_low"]
        if lo <= c <= hi:
            self.inside, self.active_dir = True, 0     # back inside: any running setup is over
            return []
        d = 1 if c > hi else -1
        if not self.inside:
            return []                                  # already outside: only the first break is a setup
        if not a or not a.get("valid"):
            return [NoTrade(self.strategy_id, "ATR_NOT_READY")]
        atr = a["atr"]
        width_pct = o["or_width_pct"]
        feats = {"or_high": hi, "or_low": lo, "close": c, "atr": atr, "or_width_pct": width_pct,
                 "bar_range": bar["high"] - bar["low"]}
        self.inside = False
        if not (self.cfg.min_range_pct <= width_pct <= self.cfg.max_range_pct):
            return [NoTrade(self.strategy_id, "RANGE_OUT_OF_BOUNDS", feats)]
        beyond = (c - hi) if d > 0 else (lo - c)
        if beyond < self.cfg.confirm_atr * atr:
            return [NoTrade(self.strategy_id, "BREAK_NOT_CONFIRMED", feats)]
        if bar["high"] - bar["low"] < self.cfg.expansion_atr * atr:
            return [NoTrade(self.strategy_id, "NO_RANGE_EXPANSION", feats)]
        f = ind.get(self.frv_key)
        if f and f.get("valid"):
            feats["fut_rel_volume"] = f["rel_vol"]
            if f["rel_vol"] < self.cfg.min_rel_volume:
                return [NoTrade(self.strategy_id, "NO_FUTURES_PARTICIPATION", feats)]
        elif self.cfg.require_futures:
            return [NoTrade(self.strategy_id, "FUTURES_DATA_MISSING", feats)]
        else:
            feats["futures_check"] = "skipped (require_futures=false)"
        self.setup_n += 1
        fresh = self.active_dir != -d
        self.active_dir = d
        return [Candidate(self.strategy_id, f"orb{self.setup_n}", d,
                          thesis=f"confirmed {'up' if d > 0 else 'down'} break of 15m opening range",
                          invalidation=hi if d > 0 else lo, normal=self.cfg.normal, zero_dte=self.cfg.zero_dte,
                          fresh_confirmation=fresh, features=feats | {"strategy_version": self.cfg.version})]
