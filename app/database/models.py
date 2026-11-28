"""Phase 1 operational schema (v1.3 §13). Later phases add their tables through new schema versions.

High-frequency quote/depth events are NOT stored here; they go to daily Parquet partitions.
"""
from __future__ import annotations

from sqlalchemy import Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

SCHEMA_VERSION = 9
SCHEMA_DESCRIPTIONS = {
    1: "Phase 1: instruments, sessions, events, historical imports",
    2: "Phase 2: derived bar builds and daily data-quality matrix",
    3: "Phase 3: indicator definitions (registry) and indicator runs",
    4: "Phase 4: paper trades, paper trade events (audit) and capital usage",
    5: "Phase 5: trading signals (candidates + risk decisions) and risk events",
    6: "Phase 6: experiments, experiment results, strategy trial counter, hold-outs and hold-out access log",
    7: "Phase 9: paper-engine sessions and candidate trade labels (Phase 7 track)",
    8: "Phase 7: meta-model registry (training runs, model versions, predictions)",
    9: "Strategy book: per-strategy forward-test trades with model and executable (ask/bid) P&L",
}


class Base(DeclarativeBase):
    pass


class SchemaVersion(Base):
    __tablename__ = "SchemaVersion"
    version: Mapped[int] = mapped_column(Integer, primary_key=True)
    applied_at: Mapped[str] = mapped_column(String(40))
    description: Mapped[str] = mapped_column(Text, default="")


class InstrumentMasters(Base):
    """One row per downloaded instrument-master file (dated copy kept on disk)."""
    __tablename__ = "InstrumentMasters"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    master_date: Mapped[str] = mapped_column(String(10), unique=True)
    downloaded_at: Mapped[str] = mapped_column(String(40))
    source_url: Mapped[str] = mapped_column(Text)
    file_path: Mapped[str] = mapped_column(Text)
    sha256: Mapped[str] = mapped_column(String(64))
    total_rows: Mapped[int] = mapped_column(Integer)
    selected_rows: Mapped[int] = mapped_column(Integer)
    validation: Mapped[str] = mapped_column(Text)  # JSON


class Instruments(Base):
    """Dated instrument facts. Never reuse today's master to reconstruct historical contracts."""
    __tablename__ = "Instruments"
    __table_args__ = (UniqueConstraint("master_id", "exchange_segment", "security_id"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    master_id: Mapped[int] = mapped_column(ForeignKey("InstrumentMasters.id"), index=True)
    master_date: Mapped[str] = mapped_column(String(10), index=True)
    security_id: Mapped[int] = mapped_column(Integer)
    exchange_segment: Mapped[str] = mapped_column(String(12))
    instrument_type: Mapped[str] = mapped_column(String(12))  # INDEX / FUTIDX / OPTIDX
    underlying: Mapped[str] = mapped_column(String(20))
    trading_symbol: Mapped[str] = mapped_column(String(80))
    expiry: Mapped[str | None] = mapped_column(String(25), index=True)
    strike: Mapped[float | None] = mapped_column(Float)
    option_type: Mapped[str | None] = mapped_column(String(2))
    lot_size: Mapped[int | None] = mapped_column(Integer)
    tick_size: Mapped[float | None] = mapped_column(Float)
    expiry_flag: Mapped[str | None] = mapped_column(String(2))


class DataSessions(Base):
    __tablename__ = "DataSessions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    session_date: Mapped[str] = mapped_column(String(10), index=True)
    started_at: Mapped[str] = mapped_column(String(40))
    ended_at: Mapped[str | None] = mapped_column(String(40))
    status: Mapped[str] = mapped_column(String(20))  # RUNNING / COMPLETED / FAILED / INTERRUPTED
    master_id: Mapped[int | None] = mapped_column(ForeignKey("InstrumentMasters.id"))
    subscribed_instruments: Mapped[int] = mapped_column(Integer, default=0)
    packets: Mapped[int] = mapped_column(Integer, default=0)
    chain_snapshots: Mapped[int] = mapped_column(Integer, default=0)
    reconnects: Mapped[int] = mapped_column(Integer, default=0)
    universe: Mapped[str] = mapped_column(Text, default="{}")  # JSON
    notes: Mapped[str] = mapped_column(Text, default="")


class SystemEvents(Base):
    __tablename__ = "SystemEvents"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ts: Mapped[str] = mapped_column(String(40), index=True)
    level: Mapped[str] = mapped_column(String(10))
    component: Mapped[str] = mapped_column(String(40))
    event: Mapped[str] = mapped_column(String(60), index=True)
    detail: Mapped[str] = mapped_column(Text, default="{}")  # JSON
    session_id: Mapped[int | None] = mapped_column(ForeignKey("DataSessions.id"))


class HistoricalImports(Base):
    """Provenance of imported historical datasets (one row per dataset/day partition)."""
    __tablename__ = "HistoricalImports"
    __table_args__ = (UniqueConstraint("dataset", "trade_date"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dataset: Mapped[str] = mapped_column(String(60))
    source: Mapped[str] = mapped_column(Text)
    data_tier: Mapped[str] = mapped_column(String(40))
    trade_date: Mapped[str] = mapped_column(String(10), index=True)
    rows: Mapped[int] = mapped_column(Integer)
    snapshots: Mapped[int] = mapped_column(Integer)
    first_ts: Mapped[str] = mapped_column(String(40))
    last_ts: Mapped[str] = mapped_column(String(40))
    file_path: Mapped[str] = mapped_column(Text)
    sha256: Mapped[str] = mapped_column(String(64))
    quality: Mapped[str] = mapped_column(Text)  # JSON
    imported_at: Mapped[str] = mapped_column(String(40))


class BarBuilds(Base):
    """Provenance of each derived bar partition (v2)."""
    __tablename__ = "BarBuilds"
    __table_args__ = (UniqueConstraint("dataset", "trade_date"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dataset: Mapped[str] = mapped_column(String(60))        # feed_bars_1m / pg_option_bars_1m / pg_spot_bars_1m
    trade_date: Mapped[str] = mapped_column(String(10), index=True)
    data_tier: Mapped[str] = mapped_column(String(40))
    source: Mapped[str] = mapped_column(Text)
    rows: Mapped[int] = mapped_column(Integer)
    instruments: Mapped[int] = mapped_column(Integer)
    file_path: Mapped[str] = mapped_column(Text)
    sha256: Mapped[str] = mapped_column(String(64))
    params: Mapped[str] = mapped_column(Text, default="{}")   # JSON (timeframe, grace, code version)
    reconciliation: Mapped[str | None] = mapped_column(Text)  # JSON result vs broker candles
    built_at: Mapped[str] = mapped_column(String(40))


class DataQualityDaily(Base):
    """Daily per-instrument data-quality matrix (v2)."""
    __tablename__ = "DataQualityDaily"
    __table_args__ = (UniqueConstraint("trade_date", "security_id"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    trade_date: Mapped[str] = mapped_column(String(10), index=True)
    security_id: Mapped[int] = mapped_column(Integer)
    role: Mapped[str | None] = mapped_column(String(10))
    events: Mapped[int] = mapped_column(Integer)
    trades: Mapped[int] = mapped_column(Integer)
    book_events: Mapped[int] = mapped_column(Integer)
    flags: Mapped[str] = mapped_column(Text)  # JSON counts per flag
    max_change_gap_s: Mapped[float] = mapped_column(Float)
    pct_time_fresh: Mapped[float | None] = mapped_column(Float)
    mean_spread: Mapped[float | None] = mapped_column(Float)
    bars: Mapped[int | None] = mapped_column(Integer)
    bar_coverage_pct: Mapped[float | None] = mapped_column(Float)


class IndicatorDefinitions(Base):
    """Indicator registry (v1.3 §5.4). One row per (indicator_id, version); approval is explicit (v3)."""
    __tablename__ = "IndicatorDefinitions"
    __table_args__ = (UniqueConstraint("indicator_id", "version"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    indicator_id: Mapped[str] = mapped_column(String(60), index=True)
    version: Mapped[str] = mapped_column(String(20))
    name: Mapped[str] = mapped_column(String(120))
    family: Mapped[str] = mapped_column(String(30))
    code_ref: Mapped[str] = mapped_column(Text)
    code_sha256: Mapped[str] = mapped_column(String(64))
    spec: Mapped[str] = mapped_column(Text)                      # JSON: full contract + defaults
    status: Mapped[str] = mapped_column(String(12))              # DRAFT / VALIDATED / APPROVED / RETIRED
    experiment_id: Mapped[str | None] = mapped_column(String(60))
    validation: Mapped[str | None] = mapped_column(Text)         # JSON validation report
    created_at: Mapped[str] = mapped_column(String(40))
    updated_at: Mapped[str] = mapped_column(String(40))


class IndicatorRuns(Base):
    """One row per indicator computation run over a dataset (values themselves live in Parquet) (v3)."""
    __tablename__ = "IndicatorRuns"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_at: Mapped[str] = mapped_column(String(40))
    dataset: Mapped[str] = mapped_column(String(60))
    date_from: Mapped[str] = mapped_column(String(10))
    date_to: Mapped[str] = mapped_column(String(10))
    bindings: Mapped[str] = mapped_column(Text)                  # JSON list of binding keys
    skipped: Mapped[str] = mapped_column(Text, default="{}")     # JSON binding -> reason (source requirements)
    output_path: Mapped[str] = mapped_column(Text)
    output_sha256: Mapped[str] = mapped_column(String(64))
    output_hash: Mapped[str] = mapped_column(String(64))         # content hash for deterministic replay
    checks: Mapped[str] = mapped_column(Text)                    # JSON: batch=stream, replay, restart results


class PaperTrades(Base):
    """Closed paper trades (v4). `mode` = backtest / paper; values are after charges."""
    __tablename__ = "PaperTrades"
    __table_args__ = (UniqueConstraint("run_id", "session_date", "trade_id"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[str] = mapped_column(String(60), index=True)
    mode: Mapped[str] = mapped_column(String(10))
    session_date: Mapped[str] = mapped_column(String(10), index=True)
    trade_id: Mapped[int] = mapped_column(Integer)
    cycle_key: Mapped[str] = mapped_column(Text)
    strategy_id: Mapped[str] = mapped_column(String(60))
    contract: Mapped[str] = mapped_column(String(60))
    dte: Mapped[int | None] = mapped_column(Integer)
    qty: Mapped[int] = mapped_column(Integer)
    entry_ts: Mapped[str] = mapped_column(String(40))
    exit_ts: Mapped[str] = mapped_column(String(40))
    entry_px: Mapped[float] = mapped_column(Float)
    exit_px: Mapped[float] = mapped_column(Float)
    exit_reason: Mapped[str] = mapped_column(String(40))
    gross_pnl: Mapped[float] = mapped_column(Float)
    charges: Mapped[float] = mapped_column(Float)
    net_pnl: Mapped[float] = mapped_column(Float)
    risk_inr: Mapped[float] = mapped_column(Float)
    r_multiple: Mapped[float | None] = mapped_column(Float)
    low_fidelity: Mapped[int] = mapped_column(Integer)
    fee_version: Mapped[str] = mapped_column(String(40))
    fill_model: Mapped[str] = mapped_column(String(40))


class BookTrades(Base):
    """Strategy-book trades (schema v9): the spec-model P&L (also in PaperTrades) plus the executable shadow."""
    __tablename__ = "BookTrades"
    __table_args__ = (UniqueConstraint("run_id", "session_date", "trade_id"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[str] = mapped_column(String(60), index=True)
    session_date: Mapped[str] = mapped_column(String(10), index=True)
    trade_id: Mapped[int] = mapped_column(Integer)
    code: Mapped[str] = mapped_column(String(8), index=True)            # specification code (U1, PB, Q1..Q8)
    strategy_id: Mapped[str] = mapped_column(String(60), index=True)
    name: Mapped[str] = mapped_column(String(80))
    side: Mapped[str] = mapped_column(String(2))
    contract: Mapped[str] = mapped_column(String(60))
    strike: Mapped[float] = mapped_column(Float)
    expiry: Mapped[str] = mapped_column(String(10))
    qty: Mapped[int] = mapped_column(Integer)
    exit_profile: Mapped[str] = mapped_column(String(4))
    signal_ts: Mapped[str] = mapped_column(String(40))
    entry_ts: Mapped[str] = mapped_column(String(40))
    exit_ts: Mapped[str] = mapped_column(String(40))
    entry_px: Mapped[float] = mapped_column(Float)
    exit_px: Mapped[float] = mapped_column(Float)
    exit_reason: Mapped[str] = mapped_column(String(20))
    peak_px: Mapped[float] = mapped_column(Float)
    gross_pnl: Mapped[float] = mapped_column(Float)
    charges: Mapped[float] = mapped_column(Float)
    net_pnl: Mapped[float] = mapped_column(Float)
    r_multiple: Mapped[float | None] = mapped_column(Float)
    exec_entry_px: Mapped[float | None] = mapped_column(Float)          # live ask at signal detection
    exec_exit_px: Mapped[float | None] = mapped_column(Float)           # live bid when the model exit fired
    exec_net_pnl: Mapped[float | None] = mapped_column(Float)
    exec_note: Mapped[str | None] = mapped_column(Text)
    payload: Mapped[str] = mapped_column(Text)                          # JSON: costs, features, trail


class PaperTradeEvents(Base):
    """Hash-chained audit records (decision, order, attempt, fill, charges, close) (v4)."""
    __tablename__ = "PaperTradeEvents"
    __table_args__ = (UniqueConstraint("run_id", "session_date", "seq"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[str] = mapped_column(String(60), index=True)
    session_date: Mapped[str] = mapped_column(String(10))
    seq: Mapped[int] = mapped_column(Integer)
    ts: Mapped[str] = mapped_column(String(40))
    kind: Mapped[str] = mapped_column(String(30), index=True)
    trade_id: Mapped[int | None] = mapped_column(Integer)
    payload: Mapped[str] = mapped_column(Text)
    prev_hash: Mapped[str] = mapped_column(String(64))
    hash: Mapped[str] = mapped_column(String(64))


class CapitalUsage(Base):
    """Per-session capital usage and live-capital planning inputs (v1.3 §9.7) (v4)."""
    __tablename__ = "CapitalUsage"
    __table_args__ = (UniqueConstraint("run_id", "session_date"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[str] = mapped_column(String(60), index=True)
    mode: Mapped[str] = mapped_column(String(10))
    session_date: Mapped[str] = mapped_column(String(10), index=True)
    peak_capital: Mapped[float] = mapped_column(Float)
    peak_capital_ts: Mapped[str | None] = mapped_column(String(40))
    peak_trade_ids: Mapped[str] = mapped_column(Text)           # JSON
    net_pnl: Mapped[float] = mapped_column(Float)
    max_intraday_drawdown: Mapped[float] = mapped_column(Float)
    largest_trade_risk: Mapped[float] = mapped_column(Float)
    largest_0dte_risk: Mapped[float] = mapped_column(Float)
    trades: Mapped[int] = mapped_column(Integer)


class TradingSignals(Base):
    """Every strategy candidate / no-trade with contract selection and the full risk decision (v5)."""
    __tablename__ = "TradingSignals"
    __table_args__ = (UniqueConstraint("run_id", "session_date", "seq"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[str] = mapped_column(String(60), index=True)
    session_date: Mapped[str] = mapped_column(String(10), index=True)
    seq: Mapped[int] = mapped_column(Integer)
    ts: Mapped[str] = mapped_column(String(40))
    strategy_id: Mapped[str] = mapped_column(String(40), index=True)
    setup_id: Mapped[str | None] = mapped_column(String(40))
    cycle_key: Mapped[str | None] = mapped_column(String(120))
    direction: Mapped[int | None] = mapped_column(Integer)
    outcome: Mapped[str] = mapped_column(String(20), index=True)     # APPROVED / REJECTED / NO_TRADE
    reasons: Mapped[str] = mapped_column(Text)                         # JSON list
    contract: Mapped[str | None] = mapped_column(String(60))
    dte: Mapped[int | None] = mapped_column(Integer)
    lots: Mapped[int | None] = mapped_column(Integer)
    planned_r_inr: Mapped[float | None] = mapped_column(Float)
    shadow_lots: Mapped[int | None] = mapped_column(Integer)
    trade_id: Mapped[int | None] = mapped_column(Integer)
    payload: Mapped[str] = mapped_column(Text)                         # JSON: features, checks, selection
    risk_config_version: Mapped[str | None] = mapped_column(String(40))


class RiskEvents(Base):
    """Risk-engine and kill-switch events: latches, limits, inconsistencies, acknowledgements (v5)."""
    __tablename__ = "RiskEvents"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[str] = mapped_column(String(60), index=True)
    session_date: Mapped[str] = mapped_column(String(10), index=True)
    kind: Mapped[str] = mapped_column(String(40), index=True)
    payload: Mapped[str] = mapped_column(Text)


class Experiments(Base):
    """One experiment run: hypothesis, spec, fingerprints, status. Failed runs are kept (v6)."""
    __tablename__ = "Experiments"
    id: Mapped[str] = mapped_column(String(80), primary_key=True)
    name: Mapped[str] = mapped_column(String(80), index=True)
    hypothesis: Mapped[str] = mapped_column(Text)
    spec: Mapped[str] = mapped_column(Text)                       # canonical JSON
    spec_hash: Mapped[str] = mapped_column(String(64), index=True)
    strategy_set: Mapped[str] = mapped_column(String(80))
    code_fingerprint: Mapped[str] = mapped_column(Text)           # JSON: git head/dirty + source sha
    data_fingerprint: Mapped[str | None] = mapped_column(Text)    # JSON
    risk_config: Mapped[str | None] = mapped_column(String(120))  # version + sha
    fee_version: Mapped[str | None] = mapped_column(String(60))
    fill_model: Mapped[str | None] = mapped_column(String(60))
    status: Mapped[str] = mapped_column(String(12), index=True)   # RUNNING / COMPLETED / FAILED
    error: Mapped[str | None] = mapped_column(Text)
    actor: Mapped[str] = mapped_column(String(60))
    started_at: Mapped[str] = mapped_column(String(40))
    finished_at: Mapped[str | None] = mapped_column(String(40))
    variants_tested: Mapped[int] = mapped_column(Integer, default=0)
    trials_total: Mapped[int | None] = mapped_column(Integer)     # strategy-set variant counter at run time
    holdout_id: Mapped[int | None] = mapped_column(Integer)
    holdout_evaluated: Mapped[int] = mapped_column(Integer, default=0)
    results_hash: Mapped[str | None] = mapped_column(String(64))
    rerun_of: Mapped[str | None] = mapped_column(String(80))
    reproduced: Mapped[int | None] = mapped_column(Integer)       # rerun: 1 identical results, 0 differs
    verdict: Mapped[str | None] = mapped_column(String(30))
    report_path: Mapped[str | None] = mapped_column(Text)


class ExperimentResults(Base):
    """Metrics per experiment x variant x scope (full_dev, fold:N:train, fold:N:test, wf_oos, holdout) (v6)."""
    __tablename__ = "ExperimentResults"
    __table_args__ = (UniqueConstraint("experiment_id", "variant_id", "scope"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    experiment_id: Mapped[str] = mapped_column(String(80), index=True)
    variant_id: Mapped[str] = mapped_column(String(40))
    scope: Mapped[str] = mapped_column(String(40))
    params: Mapped[str] = mapped_column(Text)
    metrics: Mapped[str] = mapped_column(Text)


class StrategyTrials(Base):
    """Variant counter for multiple-testing control: every distinct parameter set ever tested per strategy
    set, including those from failed or discarded experiments (v6)."""
    __tablename__ = "StrategyTrials"
    __table_args__ = (UniqueConstraint("strategy_set", "variant_id"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    strategy_set: Mapped[str] = mapped_column(String(80), index=True)
    variant_id: Mapped[str] = mapped_column(String(40))
    params: Mapped[str] = mapped_column(Text)
    first_experiment: Mapped[str] = mapped_column(String(80))
    first_at: Mapped[str] = mapped_column(String(40))
    times_tested: Mapped[int] = mapped_column(Integer, default=1)


class Holdouts(Base):
    """Fixed chronological hold-out windows. SEALED until inspected once, then RETIRED (v6)."""
    __tablename__ = "Holdouts"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sessions: Mapped[str] = mapped_column(Text)                   # JSON list
    first_session: Mapped[str] = mapped_column(String(10))
    last_session: Mapped[str] = mapped_column(String(10))
    created_at: Mapped[str] = mapped_column(String(40))
    status: Mapped[str] = mapped_column(String(10), index=True)
    inspected_by: Mapped[str | None] = mapped_column(String(60))
    inspected_at: Mapped[str | None] = mapped_column(String(40))
    inspected_experiment: Mapped[str | None] = mapped_column(String(80))
    accidental_exposure: Mapped[str | None] = mapped_column(Text)  # documented exposures (JSON list)


class HoldoutAccessLog(Base):
    """Every request to read hold-out results, granted or refused (v6)."""
    __tablename__ = "HoldoutAccessLog"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    holdout_id: Mapped[int | None] = mapped_column(Integer, index=True)
    experiment_id: Mapped[str | None] = mapped_column(String(80))
    actor: Mapped[str] = mapped_column(String(60))
    purpose: Mapped[str] = mapped_column(Text)
    at: Mapped[str] = mapped_column(String(40))
    granted: Mapped[int] = mapped_column(Integer)
    reason: Mapped[str | None] = mapped_column(Text)


class PaperSessions(Base):
    """One paper-engine session: lifecycle, journal, recovery and reconciliation (v7, Phase 9)."""
    __tablename__ = "PaperSessions"
    __table_args__ = (UniqueConstraint("run_id", "session_date"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[str] = mapped_column(String(60), index=True)
    session_date: Mapped[str] = mapped_column(String(10), index=True)
    source: Mapped[str] = mapped_column(String(40))                # live_feed / replay:<dataset>
    started_at: Mapped[str] = mapped_column(String(40))
    ended_at: Mapped[str | None] = mapped_column(String(40))
    status: Mapped[str] = mapped_column(String(20))                # RUNNING / COMPLETED / FAILED / RECOVERED
    restarts: Mapped[int] = mapped_column(Integer, default=0)
    journal_path: Mapped[str] = mapped_column(Text)
    journal_events: Mapped[int] = mapped_column(Integer, default=0)
    journal_sha256: Mapped[str | None] = mapped_column(String(64))
    audit_head: Mapped[str | None] = mapped_column(String(64))
    reconciliation: Mapped[str | None] = mapped_column(Text)       # JSON: audit/fills/capital checks
    incidents: Mapped[str | None] = mapped_column(Text)            # JSON list
    code_fingerprint: Mapped[str | None] = mapped_column(String(64))
    risk_config_version: Mapped[str | None] = mapped_column(String(40))


class TradeLabels(Base):
    """Meta-labels for every strategy candidate (v1.3 §6.3): first barrier in option-premium space within a
    holding horizon, for traded AND risk-rejected (counterfactual) candidates (v7)."""
    __tablename__ = "TradeLabels"
    __table_args__ = (UniqueConstraint("run_id", "session_date", "signal_seq", "horizon_min"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[str] = mapped_column(String(60), index=True)
    session_date: Mapped[str] = mapped_column(String(10), index=True)   # grouping key for CV (no day overlap)
    signal_seq: Mapped[int] = mapped_column(Integer)
    signal_ts: Mapped[str] = mapped_column(String(40))
    strategy_id: Mapped[str] = mapped_column(String(40), index=True)
    cycle_key: Mapped[str | None] = mapped_column(String(120))
    direction: Mapped[int | None] = mapped_column(Integer)
    signal_outcome: Mapped[str] = mapped_column(String(20))             # APPROVED / REJECTED
    reject_reasons: Mapped[str] = mapped_column(Text)                   # JSON list
    traded: Mapped[int] = mapped_column(Integer)                        # 0 = counterfactual
    contract: Mapped[str | None] = mapped_column(String(60))
    dte: Mapped[int | None] = mapped_column(Integer)
    horizon_min: Mapped[int] = mapped_column(Integer)
    stop_pct: Mapped[float | None] = mapped_column(Float)
    target_pct: Mapped[float | None] = mapped_column(Float)
    label: Mapped[str] = mapped_column(String(20), index=True)          # TARGET_FIRST / STOP_FIRST / TIMEOUT / NO_ENTRY / UNLABELLABLE
    label_reason: Mapped[str | None] = mapped_column(String(80))
    entry_ts: Mapped[str | None] = mapped_column(String(40))
    entry_px: Mapped[float | None] = mapped_column(Float)
    exit_ts: Mapped[str | None] = mapped_column(String(40))
    exit_px: Mapped[float | None] = mapped_column(Float)
    gross_pnl_per_lot: Mapped[float | None] = mapped_column(Float)
    charges_per_lot: Mapped[float | None] = mapped_column(Float)
    net_pnl_per_lot: Mapped[float | None] = mapped_column(Float)
    r_multiple: Mapped[float | None] = mapped_column(Float)
    features: Mapped[str] = mapped_column(Text)                         # JSON, decision-time only
    data_tier: Mapped[str] = mapped_column(String(60))                  # prospective (live feed) vs history
    label_version: Mapped[str] = mapped_column(String(20))


class ModelTrainingRuns(Base):
    """One walk-forward training + evaluation of the meta-model (v1.3 §6.3, §11) (v8)."""
    __tablename__ = "ModelTrainingRuns"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[str] = mapped_column(String(40))
    model_name: Mapped[str] = mapped_column(String(60), index=True)
    horizon_min: Mapped[int] = mapped_column(Integer)
    target: Mapped[str] = mapped_column(String(60))
    data_tiers: Mapped[str] = mapped_column(Text)                       # JSON list
    label_runs: Mapped[str] = mapped_column(Text)                       # JSON list of TradeLabels.run_id
    dataset_fingerprint: Mapped[str] = mapped_column(String(64))
    rows: Mapped[int] = mapped_column(Integer)
    sessions: Mapped[int] = mapped_column(Integer)
    first_session: Mapped[str | None] = mapped_column(String(10))
    last_session: Mapped[str | None] = mapped_column(String(10))
    params: Mapped[str] = mapped_column(Text)                           # JSON (model + CV + calibration)
    folds: Mapped[str] = mapped_column(Text)                            # JSON per-fold sessions / metrics
    metrics: Mapped[str] = mapped_column(Text)                          # JSON out-of-fold model vs baseline
    leakage: Mapped[str] = mapped_column(Text)                          # JSON
    gate: Mapped[str] = mapped_column(Text)                             # JSON promotion-gate checks
    code_fingerprint: Mapped[str | None] = mapped_column(String(64))


class ModelVersions(Base):
    """A trained, calibrated model artifact. Status: RESEARCH_ONLY (never promotable: history data or failed
    gate) / CANDIDATE (gate passed, awaiting operator approval) / APPROVED / RETIRED (v8)."""
    __tablename__ = "ModelVersions"
    __table_args__ = (UniqueConstraint("model_name", "version"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    model_name: Mapped[str] = mapped_column(String(60), index=True)
    version: Mapped[int] = mapped_column(Integer)
    training_run_id: Mapped[int] = mapped_column(ForeignKey("ModelTrainingRuns.id"))
    created_at: Mapped[str] = mapped_column(String(40))
    artifact_path: Mapped[str] = mapped_column(Text)
    artifact_sha256: Mapped[str] = mapped_column(String(64))
    features: Mapped[str] = mapped_column(Text)                         # JSON list (model input order)
    calibration: Mapped[str] = mapped_column(String(20))
    status: Mapped[str] = mapped_column(String(20), index=True)
    status_reason: Mapped[str] = mapped_column(Text, default="")
    approved_by: Mapped[str | None] = mapped_column(String(60))
    approved_at: Mapped[str | None] = mapped_column(String(40))


class ModelPredictions(Base):
    """Calibrated probabilities per candidate: out-of-fold (kind=oof) from training, later shadow/live (v8)."""
    __tablename__ = "ModelPredictions"
    __table_args__ = (UniqueConstraint("model_version_id", "kind", "label_run_id", "session_date", "signal_seq"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    model_version_id: Mapped[int] = mapped_column(ForeignKey("ModelVersions.id"), index=True)
    kind: Mapped[str] = mapped_column(String(10))                       # oof / shadow / live
    fold: Mapped[int | None] = mapped_column(Integer)
    label_run_id: Mapped[str] = mapped_column(String(60))
    session_date: Mapped[str] = mapped_column(String(10), index=True)
    signal_seq: Mapped[int] = mapped_column(Integer)
    strategy_id: Mapped[str] = mapped_column(String(40))
    p_win: Mapped[float] = mapped_column(Float)
    take: Mapped[int] = mapped_column(Integer)
    outcome_label: Mapped[str | None] = mapped_column(String(20))
    net_pnl_per_lot: Mapped[float | None] = mapped_column(Float)
