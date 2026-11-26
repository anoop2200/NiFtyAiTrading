"""Decision pipeline (v1.3 §6): data-quality gate -> strategy candidates -> contract ranking -> independent
risk engine -> EntryIntent for the paper broker. Implements the replay `Strategy` protocol, so the same
object runs in backtest replay and (later) paper trading.

ML meta-labelling (§6 step 6) is Phase 7; until then every approved candidate is sent with the
rule-only label. Every candidate and NO TRADE is recorded in `signals` with its features, contract
selection, every risk check and the outcome.

Exits owned here (the broker owns stop/target/time-stop/15:25): setup invalidation on the underlying,
daily-loss breach (close everything) and a manual kill-switch emergency close.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import pandas as pd

from app.backtest.broker import EntryIntent, PaperBroker
from app.backtest.fills import NS
from app.backtest.replay import Context, ExitRequest
from app.clock import IST
from app.risk.engine import RiskEngine, RiskRequest
from app.strategies.base import Candidate, NoTrade, invalidated
from app.strategies.contracts import SelectionConfig, select_contract


@dataclass
class PipelineConfig:
    profile: str = "NIFTY"
    feed_liveness_s: float = 3.0       # no quote anywhere for this long = feed dead (history: ~15 s cadence)
    data_label: str = "observed_feed"
    selection: SelectionConfig = field(default_factory=SelectionConfig)
    meta_label: str = "rule_only (meta-model is Phase 7)"


class DecisionPipeline:
    strategy_id = "pipeline"

    def __init__(self, strategies: list, risk: RiskEngine, cal, lot_size: int, cfg: PipelineConfig | None = None):
        self.strategies, self.risk, self.cal, self.lot_size = strategies, risk, cal, lot_size
        self.cfg = cfg or PipelineConfig()
        self.latest: dict[str, dict] = {}
        self.signals: list[dict] = []
        self.session: str | None = None
        self.data_ok, self.data_reason = True, None
        self.setups: dict[str, Candidate] = {}        # cycle_key -> candidate (for invalidation exits)
        self.broker: PaperBroker | None = None
        self.exits_requested: set[int] = set()

    # ------------------------------------------------------------------ wiring
    @property
    def bindings(self) -> list:
        seen, out = set(), []
        for s in self.strategies:
            for b in s.bindings:
                if b.key not in seen:
                    seen.add(b.key)
                    out.append(b)
        return out

    def attach(self, broker: PaperBroker) -> None:
        """Risk bookkeeping is driven by broker events (fills, no-fills, closes)."""
        self.broker = broker
        if self.risk not in broker.listeners:
            broker.listeners.append(self.risk)

    def start_session(self, session: str, gap_ratio: float | None, data_ok: bool = True,
                      data_reason: str | None = None) -> None:
        self.session, self.data_ok, self.data_reason = session, data_ok, data_reason
        self.latest, self.setups, self.exits_requested = {}, {}, set()
        self.risk.start_session(session, gap_ratio)

    # ------------------------------------------------------------------ replay entry point
    def on_bar(self, stream: str, bar: dict, outs: dict[str, dict], ctx: Context) -> list:
        self.latest.update(outs)
        if stream == "vix":
            self._on_vix(bar)
            return []
        if stream != "index":
            return []                                  # futures bars only feed indicators
        if bar.get("session") != self.session:
            raise RuntimeError(f"bar session {bar.get('session')} but pipeline session {self.session}")
        acts: list = []
        broker = ctx.broker
        self.risk.check_consistency(broker.open_trade_ids())
        unreal = broker.unrealized_inr()
        acts += self._risk_exits(broker, unreal, ctx.now_ns)
        acts += self._invalidation_exits(broker, bar)
        gate = self._data_gate(bar)
        for s in self.strategies:
            for res in s.on_bar(self.session, bar, self.latest) or []:
                if isinstance(res, NoTrade):
                    self._record(ctx.now_ns, res.strategy_id, None, "NO_TRADE", [res.reason], payload=res.detail)
                elif gate:
                    self._record(ctx.now_ns, res.strategy_id, res, "NO_TRADE", [gate], payload=res.features)
                else:
                    acts += self._candidate(res, bar, ctx, unreal)
        return acts

    # ------------------------------------------------------------------ market context
    def _on_vix(self, bar: dict) -> None:
        if bar.get("close") is None or pd.isna(bar.get("close")):
            return
        vb = getattr(self, "_vix_bars", None)
        if vb is None or vb.get("session") != bar.get("session"):
            vb = self._vix_bars = {"session": bar.get("session"), "open": float(bar.get("open") or bar["close"]),
                                   "bars": []}
        vb["bars"].append((int(bar["ts"]), float(bar["close"])))

    def market_context(self, bar: dict) -> dict:
        """India VIX as of the minute BEFORE the decision bar (order-independent: a VIX bar of the same minute
        may arrive just before or after the index bar). Missing VIX is recorded as None, never guessed."""
        vb = getattr(self, "_vix_bars", None)
        if not vb or vb.get("session") != self.session:
            return {"vix": None}
        cutoff = int(bar["ts"]) - 60
        prior = [(t, c) for t, c in vb["bars"] if t <= cutoff]
        if not prior:
            return {"vix": None}
        t, c = prior[-1]
        return {"vix": c, "vix_bar_ts": t, "vix_chg_pct_session": round(100 * (c / vb["open"] - 1), 4)
                if vb["open"] else None}

    # ------------------------------------------------------------------ steps
    def _data_gate(self, bar: dict) -> str | None:
        if not self.data_ok:
            return f"DATA_QUALITY_DAY:{self.data_reason}"
        if not bar.get("complete", True) or any(pd.isna(bar.get(k)) for k in ("open", "high", "low", "close")):
            return "DATA_QUALITY_BAR"
        return None

    def _risk_exits(self, broker: PaperBroker, unreal: dict, now_ns: int) -> list:
        reason = None
        if self.risk.kill.blocking_reason() and self.risk.kill.emergency_close:
            reason = "KILL_SWITCH_EMERGENCY_CLOSE"
        elif self.risk.daily_loss_breached(unreal) and \
                (self.risk.cfg is None or self.risk.cfg["limits"]["daily_loss_open_policy"] == "close"):
            reason = "DAILY_LOSS_LIMIT"
        if reason is None:
            return []
        out = []
        for tid in sorted(broker.open_trade_ids()):
            if broker.positions[tid].status == "OPEN" and tid not in self.exits_requested:
                self.exits_requested.add(tid)
                out.append(ExitRequest(tid, reason))
        return out

    def _invalidation_exits(self, broker: PaperBroker, bar: dict) -> list:
        out = []
        for tid, p in sorted(broker.positions.items()):
            c = self.setups.get(p.intent.cycle_key)
            if p.status == "OPEN" and c and tid not in self.exits_requested \
                    and invalidated(c.direction, bar["close"], c.invalidation):
                self.exits_requested.add(tid)
                out.append(ExitRequest(tid, "SETUP_INVALIDATED"))
        return out

    def _feed_alive(self, ctx: Context) -> bool:
        latest = max((q.ts_ns for q in ctx.broker.book.values() if q.ts_ns <= ctx.now_ns), default=None)
        return latest is not None and (ctx.now_ns - latest) / NS <= self.cfg.feed_liveness_s

    def _candidate(self, c: Candidate, bar: dict, ctx: Context, unreal: dict) -> list:
        cycle = c.cycle_key(self.session, self.cfg.profile)
        sel = select_contract(c.direction, bar["close"], ctx.now_ns, ctx.quotes(), self.cal, self.cfg.selection)
        base = {"features": c.features, "thesis": c.thesis, "invalidation": c.invalidation,
                "selection": {"chosen": sel.features, "evaluated": sel.evaluated}, "data_label": self.cfg.data_label,
                "market": self.market_context(bar)}
        if sel.key is None:
            self.risk.on_rejected_cycle(cycle)
            self._record(ctx.now_ns, c.strategy_id, c, "REJECTED", [sel.reason], cycle, payload=base)
            return []
        tpl = c.zero_dte if sel.dte == 0 else c.normal
        q = sel.quote
        req = RiskRequest(now_ns=ctx.now_ns, cycle_key=cycle, strategy_id=c.strategy_id, setup_id=c.setup_id,
                          direction=c.direction, fresh_confirmation=c.fresh_confirmation, contract=sel.key,
                          dte=sel.dte, ask=q.ask, bid=q.bid, quote_age_s=(ctx.now_ns - q.ts_ns) / NS,
                          feed_alive=self._feed_alive(ctx), lot_size=self.lot_size, stop_pct=tpl.stop_pct,
                          target_pct=tpl.target_pct, time_stop_s=tpl.time_stop_s, needs_chain=c.needs_chain)
        dec = self.risk.evaluate(req, unreal)
        rec = {**base, "risk_checks": dec.checks, "template": tpl.__dict__}
        if not dec.approved:
            self.risk.on_rejected(req)
            self._record(ctx.now_ns, c.strategy_id, c, "REJECTED", dec.reasons, cycle, sel, dec, payload=rec)
            return []
        self.risk.on_approved(req, dec)
        self.setups[cycle] = c
        self._record(ctx.now_ns, c.strategy_id, c, "APPROVED", [], cycle, sel, dec, payload=rec)
        return [EntryIntent(cycle_key=cycle, strategy_id=c.strategy_id, contract=sel.key, lots=dec.lots,
                            lot_size=self.lot_size, decision_ns=ctx.now_ns, stop_pct=tpl.stop_pct,
                            target_pct=tpl.target_pct, time_stop_s=tpl.time_stop_s, dte=sel.dte,
                            meta={"setup": c.setup_id, "spot": bar["close"], "ask": q.ask, "bid": q.bid,
                                  "planned_r_inr": dec.planned_r_inr, "shadow_lots": dec.shadow_lots,
                                  "meta_label": self.cfg.meta_label, "risk_config": dec.config_version,
                                  "data_label": self.cfg.data_label})]

    def _record(self, now_ns, strategy_id, c, outcome, reasons, cycle=None, sel=None, dec=None, payload=None):
        self.signals.append({
            "seq": len(self.signals), "ts": datetime.fromtimestamp(now_ns / NS, IST).isoformat(timespec="seconds"),
            "ts_ns": now_ns, "session": self.session, "strategy_id": strategy_id,
            "setup_id": c.setup_id if c else None, "cycle_key": cycle, "direction": c.direction if c else None,
            "outcome": outcome, "reasons": reasons, "contract": sel.key if sel else None,
            "dte": sel.dte if sel else None, "lots": dec.lots if dec else None,
            "planned_r_inr": dec.planned_r_inr if dec else None, "shadow_lots": dec.shadow_lots if dec else None,
            "risk_config_version": dec.config_version if dec else None, "payload": payload or {}})


# ---------------------------------------------------------------------------------------------- session inputs
def gap_ratios(index: pd.DataFrame, atr_days: int = 14, min_days: int = 5) -> dict[str, float | None]:
    """|open - prior close| / ATR of the prior `atr_days` sessions (daily true range). None when there is not
    enough prior history -> the risk engine treats the day as a gap day (fail closed)."""
    d = index.sort_values("ts").groupby("session").agg(open=("open", "first"), high=("high", "max"),
                                                        low=("low", "min"), close=("close", "last"))
    pc = d["close"].shift(1)
    tr = pd.concat([d["high"] - d["low"], (d["high"] - pc).abs(), (d["low"] - pc).abs()], axis=1).max(axis=1)
    atr = tr.shift(1).rolling(atr_days, min_periods=min_days).mean()
    out = {}
    for s in d.index:
        a, p = atr.get(s), pc.get(s)
        out[str(s)] = None if pd.isna(a) or pd.isna(p) or a <= 0 else round(abs(d.at[s, "open"] - p) / a, 4)
    return out
