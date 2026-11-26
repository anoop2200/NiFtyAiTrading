"""Paper-trading session: the trading-engine core of Phase 9 (v1.3 §11, §12, §17 phase 9).

One PaperSession = one trading day of one paper run. Inputs (quotes, final bars, clock ticks, operator
commands, kill-switch changes) arrive one at a time, are written to the write-ahead journal, then applied to the
same DecisionPipeline / RiskEngine / PaperBroker used by backtests (through StreamEngine, which is proven equal to
the replay engine). At the end of the session it

  * closes the day (pending orders resolved, anything open marked),
  * labels every candidate (traded and risk-rejected counterfactuals) for the Phase 7 track,
  * reconciles fills vs audit chain vs trades vs capital usage,
  * persists trades / audit / capital / signals / risk events / labels / session record (single writer),
  * carries the consecutive-loss latch to the next session.

Determinism for recovery: the risk engine uses an IN-MEMORY kill switch. The real kill-switch file and the
operator command inbox are polled before every bar and tick; any change is journaled first and then applied,
so a journal replay reproduces every kill-switch effect exactly, and a manual kill still blocks the very next
decision. Automatic trips (feed outage) are decided inside journaled ticks, so they replay too.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, time as dtime
from pathlib import Path

from app.backtest.audit import AuditLog
from app.backtest.broker import PaperBroker
from app.backtest.fees import FeeSchedule
from app.backtest.fills import NS, FillConfig, Quote
from app.clock import IST, iso
from app.engine.journal import Journal, quote_rec, rec_quote
from app.engine.stream import StreamEngine
from app.indicators.engine import IndicatorEngine
from app.pipeline.decision import DecisionPipeline, PipelineConfig
from app.risk.engine import RiskEngine
from app.risk.killswitch import KillSwitch
from app.strategies.contracts import SelectionConfig

ENGINE_COMMANDS = ("ack_auto", "reset_loss_latch")


@dataclass
class SessionSpec:
    run_id: str
    session: str
    lot_size: int
    source: str                                   # live_feed | replay:<dataset>
    data_label: str = "observed_feed"
    mode: str = "paper"                           # paper | paper_replay
    strategies: tuple = ("orb", "pullback")
    overrides: dict = field(default_factory=lambda: {"require_futures": False})
    gap_ratio: float | None = None
    prior_daily: list | None = None               # [{session, high, low, close}] -> gap ratio from the first bar
    data_ok: bool = True
    data_reason: str | None = None
    feed_liveness_s: float = 3.0
    selection_quote_age_s: float = 30.0
    feed_outage_s: float = 30.0                   # no quote for this long in market hours -> automatic kill
    market_open: str = "09:15"
    market_close: str = "15:30"
    fill: dict = field(default_factory=lambda: asdict(FillConfig()))
    checkpoint_every: int = 2000

    def to_json(self) -> dict:
        d = asdict(self)
        d["strategies"] = list(self.strategies)
        return d

    @classmethod
    def from_json(cls, d: dict) -> "SessionSpec":
        d = dict(d)
        d["strategies"] = tuple(d["strategies"])
        return cls(**d)


def gap_ratio_from(prior_daily: list[dict], today_open: float, atr_days: int = 14, min_days: int = 5) -> float | None:
    """Same definition as pipeline.decision.gap_ratios: |open - prior close| / ATR(prior sessions)."""
    if not prior_daily:
        return None
    rows = sorted(prior_daily, key=lambda r: r["session"])
    trs, pc = [], None
    for r in rows:
        cands = [r["high"] - r["low"]] + ([abs(r["high"] - pc), abs(r["low"] - pc)] if pc is not None else [])
        trs.append(max(cands))
        pc = r["close"]
    window = trs[-atr_days:]
    if len(window) < min_days:
        return None
    atr = sum(window) / len(window)
    return None if atr <= 0 else round(abs(today_open - pc) / atr, 4)


class PaperSession:
    def __init__(self, spec: SessionSpec, journal_path: Path, fees: FeeSchedule, cal, events,
                 kill_file: Path | None = None, commands: Path | None = None, carry_file: Path | None = None,
                 notifier=None, risk_config_path: Path | None = None, warmup: dict | None = None,
                 strategy_factory=None, _recovering: bool = False):
        from app.research.spec import build_strategies
        self.spec, self.fees, self.cal = spec, fees, cal
        self.kill_file, self.commands, self.carry_file = kill_file, commands, carry_file
        self.notifier, self.muted = notifier, _recovering
        self.fill = FillConfig(**spec.fill)
        self.kill = KillSwitch(None)                       # in-memory, driven by journaled records only
        self.risk = RiskEngine(config_path=risk_config_path, fees=fees, events=events, kill=self.kill)
        # strategy_factory: tests only (production builds the registered strategies named in the spec)
        self.strategies = strategy_factory() if strategy_factory else build_strategies(spec.strategies, {},
                                                                                       spec.overrides)
        self.pipe = DecisionPipeline(self.strategies, self.risk, cal, spec.lot_size,
                                     PipelineConfig(feed_liveness_s=spec.feed_liveness_s, data_label=spec.data_label,
                                                    selection=replace(SelectionConfig(),
                                                                      max_quote_age_s=spec.selection_quote_age_s)))
        self.broker = PaperBroker(self.fill, fees, AuditLog())
        self.pipe.attach(self.broker)
        self.broker.listeners.append(self)
        self.engine = StreamEngine(self.broker, self.pipe, IndicatorEngine(self.pipe.bindings))
        self.applied = 0
        self.last_quote_ns: int | None = None
        self.started = False                               # risk session opened (needs gap ratio)
        self.incidents: list[dict] = []
        self.kill_seen = {"manual": False, "emergency_close": False}
        self._signals_alerted = 0
        self._latch_reset: dict | None = None
        self.recovery: dict | None = None
        self.carry_in = self._read_carry()
        self.journal = Journal(journal_path)
        self.warmup_bars = self._warm(warmup or {})
        if not _recovering:
            self._write({"k": "start", "at": iso(), "spec": spec.to_json(), "carry": self.carry_in,
                         "warmup": warmup or {}})
            self._open_risk_session(spec.gap_ratio if spec.prior_daily is None else None, defer=spec.prior_daily is not None)

    # ------------------------------------------------------------------ session lifecycle helpers
    def _warm(self, warmup: dict) -> int:
        """Indicator warm-up (v1.3 §5 state recovery): prior sessions' final 1-minute bars go through the
        indicators only (strategies and risk never see them), so ATR / EMA / ADX start the day in the same state
        as a continuous run. The bars are part of the journal's start record, so recovery warms up identically."""
        n = 0
        for stream in sorted(warmup):
            for bar in warmup[stream]:
                self.engine.indicators.on_bar(stream, bar)
                n += 1
        return n

    def _read_carry(self) -> dict:
        if self.carry_file and Path(self.carry_file).exists():
            return json.loads(Path(self.carry_file).read_text(encoding="utf-8"))
        return {}

    def _open_risk_session(self, gap_ratio, defer: bool = False) -> None:
        if defer:
            return                                         # opened by the first index bar (gap ratio needs today's open)
        latch = self.carry_in.get("loss_latch_session")
        self.risk.state.loss_latch_session = latch
        self.pipe.start_session(self.spec.session, gap_ratio, self.spec.data_ok, self.spec.data_reason)
        self.started = True
        if self._latch_reset:
            self.risk._event("loss_latch_reset", self._latch_reset)
        if latch:
            self._incident("LOSS_LATCH_CARRIED", {"latched_session": latch}, notify=False)

    def _incident(self, kind: str, detail: dict, notify: bool = True, critical: bool = False) -> None:
        self.incidents.append({"kind": kind, "ns": self.engine.now_ns, "detail": detail,
                               "resolved": not critical})
        if notify and self.notifier and not self.muted:
            from app.notify.telegram import CRITICAL, WARNING, Alert
            self.notifier.send(Alert("incident", CRITICAL if critical else WARNING,
                                     f"Paper engine {self.spec.session}: {kind} {json.dumps(detail, default=str)[:300]}",
                                     key=f"incident|{kind}"))

    # ------------------------------------------------------------------ inputs (journal first, then apply)
    def _write(self, rec: dict) -> None:
        self.journal.write(rec)

    def quote(self, q: Quote) -> None:
        r = quote_rec(q)
        self._write(r)
        self._apply(r)

    def bar(self, stream: str, bar: dict, available_ns: int) -> None:
        self._poll_operator(available_ns)
        r = {"k": "b", "t": available_ns, "s": stream, "bar": bar}
        self._write(r)
        self._apply(r)

    def tick(self, now_ns: int) -> None:
        self._poll_operator(now_ns)
        r = {"k": "t", "t": now_ns}
        self._write(r)
        self._apply(r)

    def _poll_operator(self, now_ns: int) -> None:
        """Read the kill-switch file and the command inbox; journal changes before they take effect."""
        if self.muted:
            return
        if self.kill_file is not None:
            ks = KillSwitch(self.kill_file)
            ks.refresh()
            state = {"manual": ks.manual, "emergency_close": ks.emergency_close}
            if state != self.kill_seen:
                r = {"k": "kill", "t": now_ns, **state}
                self._write(r)
                self._apply(r)
        if self.commands is not None and Path(self.commands).exists():
            for f in sorted(Path(self.commands).glob("*.json")):
                try:
                    cmd = json.loads(f.read_text(encoding="utf-8"))
                except (OSError, ValueError):
                    continue
                if cmd.get("command") not in ENGINE_COMMANDS:
                    continue
                r = {"k": "cmd", "t": now_ns, "cmd": cmd, "file": f.name}
                self._write(r)
                self._apply(r)
                f.replace(f.with_suffix(".done"))

    def _apply(self, r: dict) -> None:
        k = r["k"]
        if k == "q":
            q = rec_quote(r)
            self.engine.on_quote(q)
            self.last_quote_ns = q.ts_ns
        elif k == "b":
            if not self.started and r["s"] == "index":
                self._open_risk_session(gap_ratio_from(self.spec.prior_daily or [], r["bar"]["open"]))
            if self.started or r["s"] == "vix":
                self.engine.on_bar(r["s"], r["bar"], r["t"])
                self._alert_new_signals()
            else:
                self.engine.tick(r["t"])                   # futures bar before the first index bar
        elif k == "t":
            self.engine.tick(r["t"])
            self._watchdog(r["t"])
        elif k == "kill":
            self.kill.manual, self.kill.emergency_close = r["manual"], r["emergency_close"] and r["manual"]
            self.kill_seen = {"manual": r["manual"], "emergency_close": r["emergency_close"]}
            self.kill.history.append({"at": r["t"], "event": "manual_on" if r["manual"] else "manual_off"})
            self.risk._event("kill_switch_manual", {"on": r["manual"], "emergency_close": r["emergency_close"]})
        elif k == "cmd":
            self._command(r["cmd"], r["t"])
        elif k == "ck":
            return
        elif k in ("start", "end", "recovered"):
            return
        else:
            raise ValueError(f"unknown journal record {k}")
        self.applied += 1
        if not self.muted and self.applied % self.spec.checkpoint_every == 0:
            self._write({"k": "ck", "n": self.applied, "head": self.broker.audit.head})

    def _command(self, cmd: dict, now_ns: int) -> None:
        if cmd["command"] == "ack_auto":
            self.kill.acknowledge(cmd["actor"])
            self.risk._event("auto_kill_acknowledged", {"actor": cmd["actor"]})
        elif cmd["command"] == "reset_loss_latch":
            latched = self.risk.state.loss_latch_session if self.started else self.carry_in.get("loss_latch_session")
            if not latched:
                return
            if self.spec.session <= latched:
                self._incident("LOSS_LATCH_RESET_REFUSED", {"latched": latched, "reason": "same session"})
            elif self.started:
                self.risk.reset_consecutive_loss_latch(cmd["actor"], self.spec.session)
            else:   # before the first bar: clear the carried latch; the reset is recorded when the session opens
                self.carry_in = {**self.carry_in, "loss_latch_session": None}
                self._latch_reset = {"actor": cmd["actor"], "latched": latched}

    def _in_market(self, now_ns: int) -> bool:
        t = datetime.fromtimestamp(now_ns / NS, IST).time()
        return dtime.fromisoformat(self.spec.market_open) <= t < dtime.fromisoformat(self.spec.market_close)

    def _watchdog(self, now_ns: int) -> None:
        if not self._in_market(now_ns):
            return
        day = datetime.fromtimestamp(now_ns / NS, IST).date()
        open_ns = int(datetime.combine(day, dtime.fromisoformat(self.spec.market_open), IST).timestamp()) * NS
        age = (now_ns - max(self.last_quote_ns or 0, open_ns)) / NS     # pre-open quiet is not an outage
        if age > self.spec.feed_outage_s and (self.kill.auto is None or self.kill.auto.get("acked_by")):
            self.kill.trip("FEED_OUTAGE", f"no option quote for {age:.0f}s")
            self.risk._event("auto_kill", {"reason": "FEED_OUTAGE", "age_s": round(age, 1)})
            self._incident("FEED_OUTAGE", {"age_s": round(age, 1)}, critical=True)

    # ------------------------------------------------------------------ notifications (broker listener)
    def on_broker_event(self, kind: str, payload: dict) -> None:
        if self.muted or not self.notifier:
            return
        from app.notify.telegram import INFO, Alert, trade_alert
        if kind == "position_opened":
            self.notifier.send(Alert("entry", INFO, f"Paper entry #{payload.get('trade_id')} {payload.get('cycle_key')} "
                                                     f"@ {payload.get('entry_px')}", key=f"entry|{payload.get('trade_id')}"))
        elif kind == "trade_closed":
            self.notifier.send(trade_alert(payload["trade"]))

    def _alert_new_signals(self) -> None:
        new = self.pipe.signals[self._signals_alerted:]
        self._signals_alerted = len(self.pipe.signals)
        if self.muted or not self.notifier:
            return
        from app.notify.telegram import candidate_alert
        for s in new:
            if s["outcome"] in ("APPROVED", "REJECTED") and s.get("setup_id"):
                self.notifier.send(candidate_alert(s))

    # ------------------------------------------------------------------ recovery
    @classmethod
    def recover(cls, journal_path: Path, fees, cal, events, **kw) -> "PaperSession":
        """Rebuild a crashed session from its journal and verify it against every checkpoint (v1.3 §12.3)."""
        recs = Journal.read(journal_path)
        if not recs or recs[0]["k"] != "start":
            raise ValueError("journal has no start record")
        if any(r["k"] == "end" for r in recs):
            raise ValueError("session already ended cleanly; nothing to recover")
        spec = SessionSpec.from_json(recs[0]["spec"])
        notifier = kw.pop("notifier", None)
        s = cls(spec, journal_path, fees, cal, events, _recovering=True, warmup=recs[0].get("warmup"), **kw)
        s.carry_in = recs[0].get("carry") or {}
        s._open_risk_session(spec.gap_ratio if spec.prior_daily is None else None, defer=spec.prior_daily is not None)
        mismatches = []
        for r in recs[1:]:
            if r["k"] == "ck":
                if r["n"] != s.applied or r["head"] != s.broker.audit.head:
                    mismatches.append({"n": r["n"], "applied": s.applied, "expected": r["head"],
                                       "got": s.broker.audit.head})
                continue
            s._apply(r)
        s.muted, s.notifier = False, notifier
        s._signals_alerted = len(s.pipe.signals)
        restarts = sum(r["k"] == "recovered" for r in recs) + 1
        ok = not mismatches and AuditLog.verify(s.broker.audit.records)
        s._write({"k": "recovered", "at": iso(), "events": s.applied, "head": s.broker.audit.head, "ok": ok,
                  "restart": restarts})
        s.recovery = {"events": s.applied, "checkpoints_failed": mismatches, "audit_valid": ok, "restart": restarts,
                      "open_trades": sorted(s.broker.open_trade_ids())}
        if not ok:
            s.kill.trip("RISK_STATE_INCONSISTENT", "journal replay does not reproduce the checkpoints")
            s._incident("RECOVERY_MISMATCH", s.recovery, critical=True)
        else:
            s._incident("RESTART_RECOVERED", {"events": s.applied, "restart": restarts,
                                              "open_trades": s.recovery["open_trades"]})
        return s

    # ------------------------------------------------------------------ end of session
    def finish(self) -> None:
        self.engine.finish()
        self._write({"k": "end", "at": iso(), "events": self.applied, "head": self.broker.audit.head})
        self.journal.close()

    def write_carry(self) -> None:
        if self.carry_file:
            p = Path(self.carry_file)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(json.dumps({"session": self.spec.session,
                                     "loss_latch_session": self.risk.state.loss_latch_session}), encoding="utf-8")
