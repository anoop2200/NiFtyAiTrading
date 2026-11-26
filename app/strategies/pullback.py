"""Trend pullback (v1.3 §7.1), evaluated on final 5-minute index bars.

Trend: EMA9 > EMA21 (up) or EMA9 < EMA21 (down) with ADX >= min_adx and the matching DI on top.
Pullback: a bar whose low (up) / high (down) touches EMA21 arms a setup. Resumption: a later bar closes
beyond the previous bar's high (up) / low (down) with RSI on the trend side of 50 -> candidate. Futures
VWAP agreement (futures close on the trend side of session VWAP) is required when `require_futures` is on.
Invalidation: a close beyond EMA21 -/+ invalidation_atr x ATR. Each touch is a new setup, so every candidate
carries a fresh confirmation; a setup disarms when the trend condition is lost or after `max_wait_bars`.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from app.indicators.engine import Resampler, binding
from app.strategies.base import Candidate, ExitTemplate, NoTrade


@dataclass
class PullbackConfig:
    tf_min: int = 5
    min_adx: float = 20.0
    invalidation_atr: float = 0.5
    max_wait_bars: int = 6
    require_futures: bool = True
    normal: ExitTemplate = field(default_factory=lambda: ExitTemplate(20.0, 30.0, 1800))
    zero_dte: ExitTemplate = field(default_factory=lambda: ExitTemplate(25.0, 40.0, 900))
    version: str = "pullback-1.0.0"


class TrendPullback:
    strategy_id = "pullback"

    def __init__(self, cfg: PullbackConfig | None = None):
        self.cfg = cfg or PullbackConfig()
        tf = self.cfg.tf_min
        b = {"fast": binding("ema", "index", tf, period=9), "slow": binding("ema", "index", tf, period=21),
             "adx": binding("adx", "index", tf, period=14), "rsi": binding("rsi", "index", tf, period=14),
             "atr": binding("atr", "index", tf, period=14), "vwap": binding("vwap", "future", 1)}
        self.keys = {k: v.key for k, v in b.items()}
        self.bindings = list(b.values())
        self.resampler = Resampler(tf * 60)
        self.session = None
        self._reset_session(None)

    def _reset_session(self, session) -> None:
        self.session, self.setup_n, self.armed, self.prev = session, 0, None, None

    def on_bar(self, session: str, bar: dict, ind: dict) -> list:
        """`bar` is a final 1-minute index bar; `ind` the latest output of every binding."""
        if session != self.session:
            self._reset_session(session)
            self.resampler = Resampler(self.cfg.tf_min * 60)
        out = []
        for xb in self.resampler.on_bar(bar):
            if xb.get("complete"):
                out += self._on_tf_bar(xb, ind)
            self.prev = xb
        return out

    def _on_tf_bar(self, b: dict, ind: dict) -> list:
        g = {k: ind.get(v) for k, v in self.keys.items()}
        need = ("fast", "slow", "adx", "rsi", "atr")
        if any(not g[k] or not g[k].get("valid") or g[k].get("ts") != b["ts"] for k in need):
            return []                                   # indicators not ready / not from this bar
        fast, slow, atr = g["fast"]["ema"], g["slow"]["ema"], g["atr"]["atr"]
        adx, pdi, mdi, rsi = g["adx"]["adx"], g["adx"]["plus_di"], g["adx"]["minus_di"], g["rsi"]["rsi"]
        d = 1 if fast > slow and pdi > mdi else -1 if fast < slow and mdi > pdi else 0
        if d == 0 or adx < self.cfg.min_adx:
            self.armed = None
            return []
        if self.armed and (self.armed["dir"] != d or b["ts"] - self.armed["ts"] > self.cfg.max_wait_bars * self.cfg.tf_min * 60):
            self.armed = None
        touched = b["low"] <= slow if d > 0 else b["high"] >= slow
        if touched and self.armed is None:
            self.armed = {"dir": d, "ts": b["ts"]}
            return []
        if self.armed is None or self.prev is None or touched:
            return []
        resumed = (b["close"] > self.prev["high"] and rsi > 50) if d > 0 else (b["close"] < self.prev["low"] and rsi < 50)
        if not resumed:
            return []
        self.armed = None
        feats = {"ema9": fast, "ema21": slow, "adx": adx, "rsi": rsi, "atr": atr, "close": b["close"],
                 "prev_high": self.prev["high"], "prev_low": self.prev["low"], "tf_min": self.cfg.tf_min}
        v = g["vwap"]
        if v and v.get("valid"):
            feats["fut_vwap_dist_pct"] = v["dist_pct"]
            if (v["dist_pct"] <= 0 if d > 0 else v["dist_pct"] >= 0):
                return [NoTrade(self.strategy_id, "FUTURES_VWAP_DISAGREES", feats)]
        elif self.cfg.require_futures:
            return [NoTrade(self.strategy_id, "FUTURES_DATA_MISSING", feats)]
        else:
            feats["futures_check"] = "skipped (require_futures=false)"
        self.setup_n += 1
        inval = slow - self.cfg.invalidation_atr * atr if d > 0 else slow + self.cfg.invalidation_atr * atr
        return [Candidate(self.strategy_id, f"pb{self.setup_n}", d,
                          thesis=f"{'up' if d > 0 else 'down'}trend pullback to EMA21 resumed",
                          invalidation=inval, normal=self.cfg.normal, zero_dte=self.cfg.zero_dte,
                          fresh_confirmation=True, features=feats | {"strategy_version": self.cfg.version})]
