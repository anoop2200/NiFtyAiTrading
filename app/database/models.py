"""Phase 1 operational schema (v1.3 §13). Later phases add their tables through new schema versions.

High-frequency quote/depth events are NOT stored here; they go to daily Parquet partitions.
"""
from __future__ import annotations

from sqlalchemy import Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

SCHEMA_VERSION = 3
SCHEMA_DESCRIPTIONS = {
    1: "Phase 1: instruments, sessions, events, historical imports",
    2: "Phase 2: derived bar builds and daily data-quality matrix",
    3: "Phase 3: indicator definitions (registry) and indicator runs",
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
