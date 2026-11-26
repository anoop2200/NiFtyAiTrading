"""Read-only data access for the dashboard (v1.3 §11 single writer, §14.1).

Every database is opened through `make_ro_engine` (SQLite `mode=ro`), so the dashboard physically cannot write a
table. State changes go only through app.control.actions (audited files the trading engine reads).
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.clock import IST
from app.config import settings
from app.database.db import make_ro_engine, research_db_path
from app.marketdata.pipeline import history_day, history_days
from app.risk.config import RISK_FILE

CHAIN_DATASET = "pg_filtered_option_data"


def engines() -> dict[str, Engine]:
    out = {}
    for name, path in (("main", settings().path("database")), ("research", research_db_path())):
        if Path(path).exists():
            out[name] = make_ro_engine(path)
    return out


def q(engine: Engine, sql: str, **params) -> pd.DataFrame:
    with engine.connect() as c:
        return pd.read_sql_query(text(sql), c, params=params)


# ---------------------------------------------------------------------------------------------- runs / trades
def runs(engine: Engine) -> pd.DataFrame:
    return q(engine, """SELECT run_id, mode, COUNT(*) AS sessions, MIN(session_date) AS first, MAX(session_date) AS last,
                               SUM(trades) AS trades, ROUND(SUM(net_pnl), 2) AS net_pnl
                        FROM CapitalUsage GROUP BY run_id ORDER BY MAX(id) DESC""")


def trades(engine: Engine, run_id: str) -> pd.DataFrame:
    return q(engine, "SELECT * FROM PaperTrades WHERE run_id = :r ORDER BY session_date, exit_ts, trade_id", r=run_id)


def capital(engine: Engine, run_id: str) -> pd.DataFrame:
    return q(engine, "SELECT * FROM CapitalUsage WHERE run_id = :r ORDER BY session_date", r=run_id)


def signals(engine: Engine, run_id: str, session_date: str | None = None) -> pd.DataFrame:
    sql = ("SELECT session_date, seq, ts, strategy_id, setup_id, direction, outcome, reasons, contract, dte, lots, "
           "planned_r_inr, trade_id, risk_config_version, payload FROM TradingSignals WHERE run_id = :r")
    if session_date:
        sql += " AND session_date = :d"
    df = q(engine, sql + " ORDER BY session_date, seq", r=run_id, d=session_date)
    if not df.empty:                      # India VIX as recorded with the decision (None where unavailable)
        mkt = df.pop("payload").map(lambda x: (json.loads(x or "{}") or {}).get("market") or {})
        df.insert(df.columns.get_loc("dte") + 1, "vix", mkt.map(lambda m: m.get("vix")))
    return df


def risk_events(engine: Engine, run_id: str) -> pd.DataFrame:
    return q(engine, "SELECT session_date, kind, payload FROM RiskEvents WHERE run_id = :r ORDER BY id", r=run_id)


def performance(tr: pd.DataFrame) -> dict:
    if tr.empty:
        return {"trades": 0}
    pnl = tr["net_pnl"]
    eq = pnl.cumsum()
    dd = (eq.cummax().clip(lower=0) - eq)
    wins, losses = pnl[pnl > 0], pnl[pnl < 0]
    return {"trades": int(len(tr)), "net_pnl": round(float(pnl.sum()), 2), "charges": round(float(tr["charges"].sum()), 2),
            "win_rate": round(float((pnl > 0).mean()), 3), "expectancy_inr": round(float(pnl.mean()), 2),
            "expectancy_r": round(float(tr["r_multiple"].fillna(0).mean()), 3),
            "profit_factor": round(float(wins.sum() / -losses.sum()), 2) if len(losses) and losses.sum() else None,
            "max_drawdown": round(float(dd.max()), 2)}


def breakdown(tr: pd.DataFrame, by: str) -> pd.DataFrame:
    if tr.empty:
        return pd.DataFrame()
    t = tr.copy()
    if by == "hour":
        t["hour"] = pd.to_datetime(t["entry_ts"], format="ISO8601").dt.strftime("%H:00")
    if by == "dte":
        t["dte"] = t["dte"].map(lambda d: "0DTE" if d == 0 else "1DTE" if d == 1 else "2+DTE")
    g = t.groupby(by)["net_pnl"]
    return pd.DataFrame({"trades": g.size(), "net_pnl": g.sum().round(2), "win_rate": g.apply(lambda s: (s > 0).mean()).round(3),
                         "avg_r": t.groupby(by)["r_multiple"].mean().round(3)}).reset_index()


def paper_sessions(engine: Engine) -> pd.DataFrame:
    try:
        return q(engine, """SELECT session_date, run_id, source, status, restarts, journal_events, audit_head,
                                   reconciliation, incidents FROM PaperSessions ORDER BY session_date DESC, run_id""")
    except Exception:  # noqa: BLE001 - table appears with schema v7 (first paper-engine start)
        return pd.DataFrame()


def label_summary(engine: Engine) -> pd.DataFrame:
    try:
        return q(engine, """SELECT data_tier, horizon_min, label, COUNT(*) AS n, SUM(traded) AS traded
                            FROM TradeLabels GROUP BY data_tier, horizon_min, label ORDER BY 1, 2, 3""")
    except Exception:  # noqa: BLE001
        return pd.DataFrame()


# ---------------------------------------------------------------------------------------------- research
def experiments(engine: Engine) -> pd.DataFrame:
    return q(engine, """SELECT id, name, status, verdict, actor, started_at, finished_at, variants_tested, trials_total,
                               holdout_evaluated, reproduced, rerun_of, results_hash, report_path FROM Experiments ORDER BY started_at DESC""")


def holdouts(engine: Engine) -> pd.DataFrame:
    return q(engine, "SELECT * FROM Holdouts ORDER BY id DESC")


def indicators(engine: Engine) -> pd.DataFrame:
    return q(engine, "SELECT indicator_id, version, name, family, status, updated_at FROM IndicatorDefinitions ORDER BY indicator_id")


def system_events(engine: Engine, limit: int = 50) -> pd.DataFrame:
    return q(engine, "SELECT ts, level, component, event, detail FROM SystemEvents ORDER BY id DESC LIMIT :n", n=limit)


# ---------------------------------------------------------------------------------------------- market monitor
def chain_days() -> list[str]:
    return history_days(settings().path("historical"), CHAIN_DATASET)


def chain_snapshot(day: str, at: str | None = None) -> tuple[pd.DataFrame, dict]:
    """ATM±5 chain at the last snapshot at or before `at` (HH:MM, IST) of a history day."""
    df = history_day(settings().path("historical"), CHAIN_DATASET, day).to_pandas()
    snaps = sorted(df["snapshot_ts"].unique())
    pick = snaps[-1]
    if at:
        cut = pd.Timestamp(f"{day} {at}", tz=IST)
        before = [s for s in snaps if s <= cut]
        pick = before[-1] if before else snaps[0]
    s = df[df["snapshot_ts"] == pick].copy()
    s["quote_age_s"] = (pick - s["ts"]).dt.total_seconds().clip(lower=0).round(1)
    s["flags"] = [",".join(f for f, bad in (("NO_BIDASK", b <= 0 or a <= 0), ("NO_LTP", l <= 0)) if bad)
                  for b, a, l in zip(s["bid"], s["ask"], s["ltp"])]
    wide = s.pivot_table(index="strike", columns="option_type",
                         values=["ltp", "bid", "ask", "oi", "iv", "delta", "quote_age_s"], aggfunc="last")
    wide.columns = [f"{t}_{v}" for v, t in wide.columns]
    cols = [c for c in ("CE_oi", "CE_iv", "CE_delta", "CE_bid", "CE_ask", "CE_ltp", "CE_quote_age_s") if c in wide] + \
           [c for c in ("PE_ltp", "PE_bid", "PE_ask", "PE_delta", "PE_iv", "PE_oi", "PE_quote_age_s") if c in wide]
    meta = {"snapshot": str(pick), "spot": float(s["spot"].iloc[0]), "atm": float(s["atm_strike"].iloc[0]),
            "expiry": str(s["expiry_ts"].iloc[0].date()), "snapshots_in_day": len(snaps),
            "rows_flagged": int((s["flags"] != "").sum()), "tier": str(s["data_tier"].iloc[0])}
    return wide[cols].reset_index(), meta


# ---------------------------------------------------------------------------------------------- config / ops
def risk_config_text() -> str:
    return Path(RISK_FILE).read_text(encoding="utf-8")


def risk_config() -> dict:
    return yaml.safe_load(risk_config_text())


def notifications_tail(path: Path | None = None, n: int = 30) -> list[dict]:
    path = path or settings().path("runtime") / "telegram_outbox.jsonl"
    if not Path(path).exists():
        return []
    return [json.loads(x) for x in Path(path).read_text(encoding="utf-8").splitlines()[-n:] if x.strip()]


def now_ist() -> str:
    return datetime.now(IST).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------------------------- Phase 7 models
def model_versions(engine: Engine) -> pd.DataFrame:
    """Meta-model registry (schema v8): one row per version with its training run's gate and metrics."""
    try:
        df = q(engine, """SELECT v.id, v.model_name, v.version, v.status, v.status_reason, v.created_at, v.calibration,
                                 v.approved_by, v.approved_at, v.artifact_sha256, r.horizon_min, r.rows, r.sessions,
                                 r.first_session, r.last_session, r.data_tiers, r.metrics, r.gate
                          FROM ModelVersions v JOIN ModelTrainingRuns r ON r.id = v.training_run_id
                          ORDER BY v.model_name, v.version DESC""")
    except Exception:  # noqa: BLE001 - tables appear with schema v8
        return pd.DataFrame()
    if df.empty:
        return df
    m = df.pop("metrics").map(lambda x: json.loads(x or "{}"))
    g = df.pop("gate").map(lambda x: json.loads(x or "{}"))
    for k in ("brier_model", "brier_baseline", "ece_model", "net_improvement", "net_improvement_p05", "taken"):
        df[k] = m.map(lambda d, k=k: d.get(k))
    df["gate_passed"] = g.map(lambda d: d.get("passed"))
    df["gate_failed"] = g.map(lambda d: ", ".join(k for k, v in (d.get("checks") or {}).items() if not v))
    df["data_tiers"] = df["data_tiers"].map(lambda x: ", ".join(json.loads(x or "[]")))
    return df


def model_folds(engine: Engine, model_version_id: int) -> pd.DataFrame:
    df = q(engine, """SELECT r.folds FROM ModelVersions v JOIN ModelTrainingRuns r ON r.id = v.training_run_id
                      WHERE v.id = :i""", i=model_version_id)
    if df.empty:
        return df
    rows = []
    for f in json.loads(df.iloc[0]["folds"] or "[]"):
        rows.append({"fold": f.get("fold"), "test": f"{f['test'][0]} .. {f['test'][1]} ({f['test'][2]})",
                     "train_sessions": (f.get("fit") or [0, 0, 0])[2] + (f.get("calib") or [0, 0, 0])[2],
                     **{k: f.get(k) for k in ("rows", "base_rate", "threshold", "brier", "brier_base", "taken",
                                              "net_rule_only", "net_model", "skipped")}})
    return pd.DataFrame(rows)


def model_calibration(engine: Engine, model_version_id: int, bins: int = 10) -> pd.DataFrame:
    """Out-of-fold reliability table: mean predicted P(win) vs observed win rate per probability bin."""
    df = q(engine, "SELECT p_win, net_pnl_per_lot FROM ModelPredictions WHERE model_version_id = :i AND kind = 'oof'",
           i=model_version_id)
    if df.empty:
        return df
    df["win"] = (df["net_pnl_per_lot"] > 0).astype(int)
    df["bin"] = (df["p_win"] * bins).clip(upper=bins - 1).astype(int)
    out = df.groupby("bin").agg(predicted=("p_win", "mean"), observed=("win", "mean"), n=("win", "size")).reset_index()
    out["range"] = out["bin"].map(lambda b: f"{b / bins:.1f}-{(b + 1) / bins:.1f}")
    return out[["range", "predicted", "observed", "n"]].round(3)


# ---------------------------------------------------------------------------------------------- India VIX
def vix_daily() -> pd.DataFrame:
    from app.historical import vix as vixh
    df = vixh.load(settings().path("historical"))
    if df.empty:
        return df
    return (df.groupby("session").agg(open=("open", "first"), high=("high", "max"), low=("low", "min"),
                                      close=("close", "last"), bars=("close", "size")).reset_index())


def vix_intraday(day: str) -> pd.DataFrame:
    from app.historical import vix as vixh
    df = vixh.load(settings().path("historical"), [day])
    if df.empty:
        return df
    df["time"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert(IST).dt.strftime("%H:%M")
    return df[["time", "open", "high", "low", "close"]]


# ---------------------------------------------------------------------------------------------- OI-03 research tier
def rolling_coverage() -> dict:
    from app.historical import rolling_options as ro
    root = settings().path("historical")
    m = ro.read_manifest(root)
    cov = ro.coverage(root)
    days = pd.DataFrame([{"day": d, **{k: v.get(k) for k in ("status", "rows", "series")}}
                         for d, v in m.get("days", {}).items()])
    if not days.empty:
        days["month"] = days["day"].str[:7]
        by_month = (days.groupby("month").agg(trading_days=("status", lambda s: int((s == "ok").sum())),
                                              holidays_or_empty=("status", lambda s: int((s == "empty").sum())),
                                              errors=("status", lambda s: int((s == "error").sum())),
                                              rows=("rows", "sum")).reset_index())
    else:
        by_month = pd.DataFrame()
    exp = pd.DataFrame([{"day": d, "series": k, **v} for d, per in m.get("expiries", {}).items()
                        for k, v in per.items()])
    return {"coverage": cov, "by_month": by_month, "expiries": exp,
            "expiry_problems": pd.DataFrame((m.get("expiry_check") or {}).get("problems") or []),
            "days": sorted(days.loc[days["status"] == "ok", "day"]) if not days.empty else []}


def rolling_day(day: str) -> pd.DataFrame:
    """One research-tier day: ATM±5 close per minute and type (trade prices; no bid/ask)."""
    from app.historical import rolling_options as ro
    df = ro.load_day(settings().path("historical"), day)
    if df.empty:
        return df
    df["time"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert(IST).dt.strftime("%H:%M")
    return df
