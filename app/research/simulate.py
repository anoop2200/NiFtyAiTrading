"""Multi-variant simulation over history with the production decision pipeline (v1.3 §10.2).

Days are the outer loop (each day's quotes are loaded once) and variants the inner loop; every variant
keeps its own strategies, indicator state and RiskEngine across days, exactly as one continuous paper run
would. Kill switch: in-memory only. The consecutive-loss latch is reset by a simulated operator at the next
session (the earliest the rules allow) unless `latch_reset=False`.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, replace

import pandas as pd
from sqlalchemy.orm import Session

from app.backtest.audit import AuditLog
from app.backtest.broker import PaperBroker
from app.backtest.fees import FeeSchedule
from app.backtest.fills import FillConfig
from app.backtest.history import TIER, lot_size_evidence, snapshot_quotes
from app.backtest.replay import ReplayEngine
from app.database.db import make_engine
from app.database.models import BarBuilds
from app.indicators.datasets import pg_spot_stream
from app.indicators.engine import IndicatorEngine
from app.market.calendar import EventCalendar, TradingCalendar
from app.pipeline.decision import DecisionPipeline, PipelineConfig, gap_ratios
from app.risk.engine import RiskEngine
from app.risk.killswitch import KillSwitch
from app.strategies.contracts import SelectionConfig

FILL = FillConfig(max_quote_age_s=15.0, version="fill-1/snapshot")
LABEL = "history_without_futures/observed_top_of_book_snapshot"


@dataclass
class HistoryContext:
    """Everything a simulation reads, with fingerprints for the experiment record."""
    cfg: object
    dataset: str
    days: list[str]
    cal: TradingCalendar
    fees: FeeSchedule
    events: EventCalendar
    index: pd.DataFrame
    gaps: dict
    quality: dict
    fill: FillConfig = FILL
    label: str = LABEL

    @classmethod
    def load(cls, cfg, dataset: str, days: list[str]) -> "HistoryContext":
        index = pg_spot_stream(cfg.path("derived"), days)
        return cls(cfg, dataset, days, TradingCalendar.load(), FeeSchedule.load(), EventCalendar.load(), index,
                   gap_ratios(index), day_quality(cfg))

    def day_path(self, day: str):
        return self.cfg.path("historical") / self.dataset / f"date={day}" / "part-0.parquet"

    def data_fingerprint(self) -> dict:
        h = hashlib.sha256()
        for d in self.days:
            h.update(d.encode())
            h.update(self.day_path(d).read_bytes())
        h.update(pd.util.hash_pandas_object(self.index[["ts", "open", "high", "low", "close"]],
                                            index=False).values.tobytes())
        return {"dataset": self.dataset, "tier": TIER, "label": self.label, "sessions": len(self.days),
                "first": self.days[0], "last": self.days[-1], "sha256": h.hexdigest(),
                "quality_failed_days": sorted(d for d in self.days if not self.quality.get(d, (False,))[0])}


def day_quality(cfg) -> dict[str, tuple[bool, str | None]]:
    out = {}
    with Session(make_engine(cfg.path("database"))) as s:
        for b in s.query(BarBuilds).filter(BarBuilds.dataset == "pg_spot_bars_1m"):
            r = json.loads(b.reconciliation) if b.reconciliation else None
            ok = bool(r and r.get("passed"))
            out[b.trade_date] = (ok, None if ok else "SPOT_BARS_FAILED_RECONCILIATION" if r else "NOT_RECONCILED")
    return out


@dataclass
class VariantRun:
    variant_id: str
    strategies: list
    risk: RiskEngine
    trades: list[dict] = field(default_factory=list)
    signals: list[dict] = field(default_factory=list)
    sessions: list[dict] = field(default_factory=list)
    heads: list[str] = field(default_factory=list)
    audit_ok: bool = True

    @property
    def audit_hash(self) -> str:
        return hashlib.sha256("".join(self.heads).encode()).hexdigest()


def simulate(ctx: HistoryContext, variants: dict[str, list], days: list[str] | None = None,
             latch_reset: bool = True, on_session=None) -> dict[str, VariantRun]:
    """variants: variant_id -> freshly built strategy objects. Returns one VariantRun per variant."""
    runs = {vid: VariantRun(vid, strats, RiskEngine(fees=ctx.fees, events=ctx.events, kill=KillSwitch(None)))
            for vid, strats in variants.items()}
    for day in days or ctx.days:
        path = ctx.day_path(day)
        lot = lot_size_evidence(path)
        if lot not in (25, 50, 65, 75):
            raise ValueError(f"{day}: implausible lot size evidence {lot}")
        quotes = snapshot_quotes(path)
        index = ctx.index[ctx.index["session"] == day]
        ok, why = ctx.quality.get(day, (False, "NOT_RECONCILED"))
        for vid, run in runs.items():
            risk = run.risk
            pipe = DecisionPipeline(run.strategies, risk, ctx.cal, lot,
                                    PipelineConfig(feed_liveness_s=15.0, data_label=ctx.label,
                                                   selection=replace(SelectionConfig(), max_quote_age_s=30.0)))
            pipe.start_session(day, ctx.gaps.get(day), ok, why)
            if latch_reset and risk.state.loss_latch_session and risk.state.loss_latch_session < day:
                risk.reset_consecutive_loss_latch("backtest-operator-sim", day)
            broker = PaperBroker(ctx.fill, ctx.fees, AuditLog())
            pipe.attach(broker)
            ReplayEngine(broker, pipe, IndicatorEngine(pipe.bindings)).run({"index": index}, quotes)
            run.trades += [t | {"session": day} for t in broker.trades]
            run.signals += [s | {"session": day} for s in pipe.signals]
            run.heads.append(broker.audit.head)
            valid = AuditLog.verify(broker.audit.records)
            run.audit_ok &= valid
            run.sessions.append({"date": day, "lot_size_evidence": lot, "trades": len(broker.trades), "data_ok": ok,
                                 "gap_ratio": ctx.gaps.get(day), "peak_capital": round(broker.capital["peak"], 2),
                                 "peak_capital_ns": broker.capital["peak_ns"],
                                 "peak_trades": broker.capital["peak_trades"],
                                 "risk_events": [e["kind"] for e in risk.state.events],
                                 "audit_head": broker.audit.head, "audit_valid": valid})
            if on_session:
                on_session(vid, day, broker, pipe, risk)
    return runs
