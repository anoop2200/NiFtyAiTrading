"""Independent, deterministic risk engine with final authority over new entries (v1.3 §9).

It does not import strategies, indicators or models. Every check is evaluated (not short-circuited) so the
audit shows all reasons a candidate failed. Any exception inside evaluation is NO TRADE and trips the
automatic kill switch. A missing or invalid risk config is NO TRADE for every candidate.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, time
from pathlib import Path

from app.backtest.fees import FeeSchedule
from app.clock import IST
from app.market.calendar import EventCalendar
from app.risk.config import RiskConfig, RiskConfigError
from app.risk.killswitch import KillSwitch

NS = 1_000_000_000


@dataclass(frozen=True)
class RiskRequest:
    now_ns: int
    cycle_key: str
    strategy_id: str
    setup_id: str
    direction: int                  # +1 long underlying (CE) / -1 short underlying (PE)
    fresh_confirmation: bool        # required for a new opposite-direction cycle
    contract: str
    dte: int
    ask: float | None
    bid: float | None
    quote_age_s: float | None
    feed_alive: bool
    lot_size: int
    stop_pct: float
    target_pct: float
    time_stop_s: int
    needs_chain: bool = False
    chain_age_s: float | None = None


@dataclass
class RiskDecision:
    approved: bool
    reasons: list[str]
    lots: int = 0
    planned_r_inr: float | None = None
    shadow_lots: int | None = None
    checks: dict = field(default_factory=dict)
    config_version: str | None = None


@dataclass
class OpenRisk:
    trade_id: int
    cycle_key: str
    strategy_id: str
    direction: int
    dte: int
    r_inr: float
    entry_px: float | None = None
    qty: int = 0


@dataclass
class RiskState:
    session: str | None = None
    entries: int = 0
    entries_0dte: int = 0
    rejected_orders: int = 0
    consecutive_losses: int = 0
    loss_latch_session: str | None = None     # set when the consecutive-loss limit is hit
    realized_r: float = 0.0
    daily_loss_hit: bool = False
    cycles_seen: set = field(default_factory=set)
    pending: dict = field(default_factory=dict)       # cycle_key -> OpenRisk (approved, not yet filled)
    open: dict = field(default_factory=dict)          # trade_id -> OpenRisk
    cooldown_until: dict = field(default_factory=dict)  # (strategy, direction) -> ns
    last_cycle_by_strategy: dict = field(default_factory=dict)  # strategy -> (direction, cycle, closed?)
    gap_block_until_ns: int | None = None
    events: list[dict] = field(default_factory=list)


class RiskEngine:
    def __init__(self, config_path: Path | None = None, fees: FeeSchedule | None = None,
                 events: EventCalendar | None = None, kill: KillSwitch | None = None, config: RiskConfig | None = None):
        self.config_error: str | None = None
        self.cfg: RiskConfig | None = config
        if self.cfg is None:
            try:
                self.cfg = RiskConfig.load(config_path) if config_path else RiskConfig.load()
            except RiskConfigError as e:
                self.config_error = str(e)
        self.fees = fees or FeeSchedule.load()
        self.events = events or EventCalendar([])
        state_file = None
        if self.cfg is not None:
            from app.config import PROJECT_ROOT
            state_file = PROJECT_ROOT / self.cfg["kill_switch"]["manual_state_file"]
        self.kill = kill or KillSwitch(state_file)
        self.state = RiskState()

    # ------------------------------------------------------------------ session lifecycle
    def start_session(self, session: str, gap_ratio: float | None = None) -> None:
        """gap_ratio = |open - prior close| / prior-day ATR; None = unknown (treated as a gap day)."""
        latch = self.state.loss_latch_session
        self.state = RiskState(session=session, loss_latch_session=latch)
        if self.cfg is None:
            return
        w = self.cfg["windows"]
        if gap_ratio is None or gap_ratio > w["gap_atr_multiple"]:
            until = self._at(session, self.cfg.t("windows", "gap_day_entry_after"))
            self.state.gap_block_until_ns = until
            self._event("gap_day_block", {"gap_ratio": gap_ratio, "until_ns": until})

    def reset_consecutive_loss_latch(self, actor: str, session: str) -> None:
        """Manual reset, only allowed in a later session than the one that hit the limit."""
        if not actor:
            raise PermissionError("reset needs an operator name")
        if self.state.loss_latch_session and session <= self.state.loss_latch_session:
            raise PermissionError("consecutive-loss latch can only be reset in the next session")
        self._event("loss_latch_reset", {"actor": actor, "latched": self.state.loss_latch_session})
        self.state.loss_latch_session = None
        self.state.consecutive_losses = 0

    # ------------------------------------------------------------------ evaluation
    def evaluate(self, req: RiskRequest, unrealized_inr: dict[int, float] | None = None) -> RiskDecision:
        try:
            return self._evaluate(req, unrealized_inr or {})
        except Exception as e:  # fail safe: an error in risk is NO TRADE and a critical event
            self.kill.trip("RISK_EVALUATION_ERROR", repr(e))
            self._event("risk_evaluation_error", {"error": repr(e), "cycle": req.cycle_key})
            return RiskDecision(False, ["RISK_EVALUATION_ERROR"])

    def _evaluate(self, r: RiskRequest, unreal: dict[int, float]) -> RiskDecision:
        if self.cfg is None:
            return RiskDecision(False, ["RISK_CONFIG_MISSING"], checks={"config": self.config_error})
        c, s = self.cfg, self.state
        now = datetime.fromtimestamp(r.now_ns / NS, IST)
        if s.session != now.date().isoformat():
            raise RuntimeError(f"risk session {s.session} does not match decision date {now.date()}")
        zero = r.dte == 0
        checks: dict[str, tuple[bool, object]] = {}

        def chk(name, ok, detail=None):
            checks[name] = (bool(ok), detail)

        # --- switches and session-level limits
        chk("kill_switch", self.kill.blocking_reason() is None, self.kill.blocking_reason())
        chk("consecutive_loss_latch", s.loss_latch_session is None, s.loss_latch_session)
        day_r = s.realized_r + sum(unreal.get(t, 0.0) / o.r_inr for t, o in s.open.items() if o.r_inr > 0)
        chk("daily_loss", not s.daily_loss_hit and day_r > -c["limits"]["max_daily_loss_r"], round(day_r, 4))
        # --- time windows
        t = now.time()
        chk("opening_window", t >= c.t("windows", "no_entry_open_until"), str(t))
        chk("closing_window", t < c.t("windows", "last_entry"), str(t))
        if zero:
            chk("zero_dte_last_entry", t < c.t("zero_dte", "last_entry"), str(t))
        chk("gap_day", s.gap_block_until_ns is None or r.now_ns >= s.gap_block_until_ns, s.gap_block_until_ns)
        ev = self.events.blackout(now)
        chk("event_blackout", not ev, [e.name for e in ev])
        # --- counts and exposure
        used = s.entries + len(s.pending)
        chk("entries_per_day", used < c["limits"]["max_entries_per_day"], used)
        if zero:
            used0 = s.entries_0dte + sum(1 for p in s.pending.values() if p.dte == 0)
            chk("zero_dte_entries", used0 < c["zero_dte"]["max_entries"], used0)
        n_open = len(s.open) + len(s.pending)
        chk("concurrent_positions", n_open < c["limits"]["max_concurrent_positions"], n_open)
        open_r = sum(max(0.0, 1.0 + unreal.get(t, 0.0) / o.r_inr) if o.r_inr > 0 else 1.0 for t, o in s.open.items())
        open_r += len(s.pending)
        chk("open_risk", open_r + 1.0 <= c["limits"]["max_open_risk_r"] + 1e-9, round(open_r, 4))
        # --- cycle rules
        chk("cycle_unique", r.cycle_key not in s.cycles_seen, r.cycle_key)
        last = s.last_cycle_by_strategy.get(r.strategy_id)
        opp_active = any(o.strategy_id == r.strategy_id and o.direction == -r.direction
                         for o in [*s.open.values(), *s.pending.values()])
        chk("no_opposite_active", not opp_active, None)
        needs_fresh = last is not None and last[0] == -r.direction
        chk("opposite_needs_fresh_setup", not needs_fresh or r.fresh_confirmation, last)
        cd = s.cooldown_until.get((r.strategy_id, r.direction))
        chk("cooldown", cd is None or r.now_ns >= cd, cd)
        # --- data and quote
        q = c["quotes"]
        chk("feed_alive", r.feed_alive, None)
        chk("quote_fresh", r.quote_age_s is not None and r.quote_age_s <= q["max_quote_age_s"], r.quote_age_s)
        if r.needs_chain:
            chk("chain_fresh", r.chain_age_s is not None and r.chain_age_s <= q["max_chain_age_s"], r.chain_age_s)
        valid_q = bool(r.ask and r.bid and r.ask > 0 and r.bid > 0 and r.ask >= r.bid)
        chk("quote_valid", valid_q, (r.bid, r.ask))
        chk("min_premium", bool(r.ask) and r.ask >= q["min_premium"], r.ask)
        if valid_q:
            mid, spread = (r.ask + r.bid) / 2, r.ask - r.bid
            lim = max(q["max_spread_ticks"] * q["tick"], mid * q["max_spread_pct_of_mid"] / 100)
            tgt = r.ask * r.target_pct / 100 * q["max_spread_pct_of_target"] / 100
            chk("spread", spread <= lim + 1e-9 and spread <= tgt + 1e-9, {"spread": round(spread, 4),
                                                                             "limit": round(min(lim, tgt), 4)})
        # --- exit template and holding
        tpl = c["zero_dte"] if zero else c["exit_templates"]["normal"]
        chk("exit_template", tpl["stop_pct"][0] <= r.stop_pct <= tpl["stop_pct"][1]
            and tpl["target_pct"][0] <= r.target_pct <= tpl["target_pct"][1], (r.stop_pct, r.target_pct))
        max_hold = (c["zero_dte"]["max_holding_min"] if zero else c["limits"]["max_holding_min"]) * 60
        chk("max_holding", 0 < r.time_stop_s <= max_hold, r.time_stop_s)

        # --- sizing (never reduced for capital in paper mode) + planned R
        lots = c["sizing"]["lots_per_trade_0dte"] if zero else c["sizing"]["lots_per_trade"]
        planned_r = shadow = None
        if valid_q:
            qty = lots * r.lot_size
            stop_bid = r.ask * (1 - r.stop_pct / 100)
            slip = c["sizing"]["slippage_ticks_per_side"] * q["tick"] * qty * 2
            charges = (self.fees.charges("BUY", r.ask * qty)["total"] + self.fees.charges("SELL", stop_bid * qty)["total"])
            planned_r = round((r.ask - stop_bid) * qty + charges + slip, 2)
            pct = c["sizing"]["shadow_risk_pct_0dte" if zero else "shadow_risk_pct"]
            per_lot = planned_r / lots
            shadow = math.floor(c["sizing"]["shadow_capital_inr"] * pct / 100 / per_lot) if per_lot > 0 else 0
        reasons = [k.upper() for k, (ok, _) in checks.items() if not ok]
        return RiskDecision(not reasons, reasons, lots if not reasons else 0, planned_r, shadow,
                            {k: {"ok": ok, "detail": d} for k, (ok, d) in checks.items()}, c.version)

    # ------------------------------------------------------------------ bookkeeping (from broker events)
    def on_broker_event(self, kind: str, payload: dict) -> None:
        if kind == "position_opened":
            self.on_entry_filled(payload["cycle_key"], payload["trade_id"], payload["entry_px"], payload["qty"])
        elif kind == "entry_not_filled":
            self.on_entry_not_filled(payload["cycle_key"])
        elif kind == "trade_closed":
            self.on_trade_closed(payload["trade"])

    def on_approved(self, req: RiskRequest, dec: RiskDecision) -> None:
        s = self.state
        s.cycles_seen.add(req.cycle_key)
        s.pending[req.cycle_key] = OpenRisk(-1, req.cycle_key, req.strategy_id, req.direction, req.dte,
                                            dec.planned_r_inr or 0.0)
        s.last_cycle_by_strategy[req.strategy_id] = (req.direction, req.cycle_key)

    def on_rejected(self, req: RiskRequest) -> None:
        self.on_rejected_cycle(req.cycle_key)

    def on_rejected_cycle(self, cycle_key: str) -> None:
        self.state.cycles_seen.add(cycle_key)  # a rejected candidate ends its cycle (no re-entry)

    def on_entry_filled(self, cycle_key: str, trade_id: int, entry_px: float, qty: int) -> None:
        s = self.state
        p = s.pending.pop(cycle_key, None)
        if p is None:
            self._inconsistent(f"fill for unknown cycle {cycle_key}")
            return
        p.trade_id, p.entry_px, p.qty = trade_id, entry_px, qty
        s.open[trade_id] = p
        s.entries += 1
        s.entries_0dte += 1 if p.dte == 0 else 0

    def on_entry_not_filled(self, cycle_key: str) -> None:
        s = self.state
        if s.pending.pop(cycle_key, None) is None:
            self._inconsistent(f"no-fill for unknown cycle {cycle_key}")
        s.rejected_orders += 1

    def on_trade_closed(self, trade: dict) -> None:
        s, c = self.state, self.cfg
        o = s.open.pop(trade["trade_id"], None)
        if o is None:
            self._inconsistent(f"close for unknown trade {trade['trade_id']}")
            return
        r = trade["net_pnl"] / o.r_inr if o.r_inr > 0 else 0.0
        s.realized_r += r
        s.consecutive_losses = s.consecutive_losses + 1 if trade["net_pnl"] < 0 else 0
        if c and s.consecutive_losses >= c["limits"]["max_consecutive_losses"] and s.loss_latch_session is None:
            s.loss_latch_session = s.session
            self._event("consecutive_loss_latch", {"losses": s.consecutive_losses})
        if c and s.realized_r <= -c["limits"]["max_daily_loss_r"]:
            s.daily_loss_hit = True
            self._event("daily_loss_limit", {"realized_r": round(s.realized_r, 4)})
        if trade["exit_reason"].startswith("STOP") and c:
            s.cooldown_until[(o.strategy_id, o.direction)] = trade["exit_ns"] + int(
                c["limits"]["cooldown_after_stop_min"] * 60 * NS)

    def daily_loss_breached(self, unrealized_inr: dict[int, float]) -> bool:
        if self.cfg is None:
            return True
        s = self.state
        day_r = s.realized_r + sum(unrealized_inr.get(t, 0.0) / o.r_inr for t, o in s.open.items() if o.r_inr > 0)
        if day_r <= -self.cfg["limits"]["max_daily_loss_r"] and not s.daily_loss_hit:
            s.daily_loss_hit = True
            self._event("daily_loss_limit", {"day_r": round(day_r, 4)})
        return s.daily_loss_hit

    def check_consistency(self, broker_open_trade_ids: set[int]) -> bool:
        mine = set(self.state.open)
        if mine != broker_open_trade_ids:
            self._inconsistent(f"risk open {sorted(mine)} != broker open {sorted(broker_open_trade_ids)}")
            return False
        return True

    # ------------------------------------------------------------------ helpers
    def _inconsistent(self, detail: str) -> None:
        self.kill.trip("RISK_STATE_INCONSISTENT", detail)
        self._event("risk_state_inconsistent", {"detail": detail})

    def _event(self, kind: str, detail: dict) -> None:
        self.state.events.append({"kind": kind, **detail})

    @staticmethod
    def _at(session: str, t: time) -> int:
        from datetime import date
        return int(datetime.combine(date.fromisoformat(session), t, IST).timestamp()) * NS
