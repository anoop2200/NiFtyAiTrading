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


# ---------------------------------------------------------------------------------------------- live journal view
class LiveJournal:
    """Incremental READ-ONLY reader of the paper engine's journal (data/paper/<run>/<day>.jsonl).

    The live journal grows all day (100+ MB), so each refresh reads only the bytes appended since the last one,
    up to the last complete line, and keeps the latest quote per contract and the 1-minute bars per stream. The
    file is opened read-only; the engine stays the single writer."""

    def __init__(self, path: Path):
        self.path, self.offset = Path(path), 0
        self.quotes: dict[str, dict] = {}
        self.bars: dict[str, list[dict]] = {}
        self.last_t, self.records, self.started, self.ended = 0, 0, None, False
        self._lock = __import__("threading").Lock()       # shared by every browser tab's thread

    def refresh(self) -> "LiveJournal":
        with self._lock:
            return self._refresh()

    def _refresh(self) -> "LiveJournal":
        if not self.path.exists():
            return self
        with open(self.path, "rb") as f:
            f.seek(self.offset)
            chunk = f.read()
        end = chunk.rfind(b"\n")
        if end < 0:
            return self
        self.offset += end + 1
        for line in chunk[:end].splitlines():
            try:
                r = json.loads(line)
            except ValueError:
                continue
            self.records += 1
            k = r.get("k")
            if r.get("t"):
                self.last_t = max(self.last_t, int(r["t"]))
            if k == "q":
                self.quotes[r["key"]] = r
            elif k == "b":
                self.bars.setdefault(r.get("s") or "?", []).append(r["bar"])
            elif k == "start":
                self.started = r.get("at")
            elif k == "end":
                self.ended = True
        return self


_LIVE: dict[str, LiveJournal] = {}


def paper_run_id() -> str:
    from app.config import PROJECT_ROOT
    return yaml.safe_load((PROJECT_ROOT / "config" / "paper.yaml").read_text(encoding="utf-8"))["run_id"]


def live_journal_path(day: str, run_id: str | None = None) -> Path:
    from app.config import PROJECT_ROOT
    return PROJECT_ROOT / "data" / "paper" / (run_id or paper_run_id()) / f"{day}.jsonl"


def book_cfg() -> dict:
    from app.config import PROJECT_ROOT
    return yaml.safe_load((PROJECT_ROOT / "config" / "paper.yaml").read_text(encoding="utf-8")).get("book") or {}


def book_snapshot(day: str, run_id: str | None = None) -> dict | None:
    """The running engine's display snapshot of the strategy book (written every 5 s; final after closeout)."""
    from app.config import PROJECT_ROOT
    p = PROJECT_ROOT / "data" / "paper" / (run_id or book_cfg().get("run_id", "book_v1")) / f"{day}.live.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def book_trades(engine: Engine, run_id: str) -> pd.DataFrame:
    try:
        return q(engine, "SELECT * FROM BookTrades WHERE run_id = :r ORDER BY session_date, trade_id", r=run_id)
    except Exception:  # noqa: BLE001 - table appears with schema v9
        return pd.DataFrame()


def book_summary(bt: pd.DataFrame) -> pd.DataFrame:
    """Per-strategy forward-test summary (model and executable P&L)."""
    from app.book.strategies import STRATEGIES
    rows = []
    for code in [c for c in STRATEGIES if c in set(bt["code"])]:
        g = bt[bt["code"] == code].sort_values(["session_date", "exit_ts"])
        w, lo = g[g.net_pnl > 0], g[g.net_pnl <= 0]
        eq = g.net_pnl.cumsum()
        ex = g.exec_net_pnl.dropna()
        rows.append({"code": code, "strategy": STRATEGIES[code].name, "trades": len(g), "win %": round(100 * len(w) / len(g)),
                     "profit factor": round(w.net_pnl.sum() / -lo.net_pnl.sum(), 2) if lo.net_pnl.sum() < 0 else None,
                     "net ₹": round(g.net_pnl.sum()), "executable ₹": round(ex.sum()) if len(ex) else None,
                     "max drawdown ₹": round((eq - eq.cummax().clip(lower=0)).min()),
                     "avg win ₹": round(w.net_pnl.mean()) if len(w) else None,
                     "avg loss ₹": round(lo.net_pnl.mean()) if len(lo) else None,
                     "last trade": g.session_date.iloc[-1]})
    return pd.DataFrame(rows)


def live_journal_closed(day: str, run_id: str | None = None) -> bool:
    """True once the day's journal was closed out: the engine gzips it at 15:35 and removes the plain file."""
    p = live_journal_path(day, run_id)
    return not p.exists() and Path(str(p) + ".gz").exists()


def closed_day(engine: Engine, day: str, run_id: str | None = None) -> dict:
    """The persisted results of a closed paper day (what closeout wrote to the main database)."""
    run = run_id or paper_run_id()
    tr = trades(engine, run)
    tr = tr[tr["session_date"] == day] if not tr.empty else tr
    ps = paper_sessions(engine)
    ps = ps[(ps["session_date"] == day) & (ps["run_id"] == run)] if not ps.empty else ps
    return {"run_id": run, "trades": tr, "signals": signals(engine, run, day),
            "session": ps.iloc[0].to_dict() if len(ps) else None}


def live_journal(day: str, run_id: str | None = None) -> LiveJournal:
    p = live_journal_path(day, run_id)
    with _SHADOW_LOCK:
        j = _LIVE.get(str(p))
        if j is None or (p.exists() and p.stat().st_size < j.offset):    # new or rewritten file
            j = _LIVE[str(p)] = LiveJournal(p)
    return j.refresh()


def live_chain(j: LiveJournal, now_ns: int | None = None, each_side: int = 5) -> tuple[pd.DataFrame, dict]:
    """ATM±each_side chain from the latest journal quotes of the nearest expiry (top of book + LTP + age)."""
    import time as _t
    now_ns = now_ns or _t.time_ns()
    idx = j.bars.get("index") or []
    spot = float(idx[-1]["close"]) if idx else None
    rows = []
    for key, r in j.quotes.items():
        try:
            _, expiry, strike, ot = key.split("|")
        except ValueError:
            continue
        b, a = (r.get("b") or [[None, 0]])[0], (r.get("a") or [[None, 0]])[0]
        rows.append({"expiry": expiry, "strike": float(strike), "type": ot, "bid": b[0], "bid_qty": b[1],
                     "ask": a[0], "ask_qty": a[1], "ltp": r.get("l"), "age_s": round((now_ns - r["t"]) / 1e9, 1)})
    meta = {"spot": spot, "atm": None, "expiry": None, "quotes": len(j.quotes), "records": j.records,
            "last_event_age_s": round((now_ns - j.last_t) / 1e9, 1) if j.last_t else None,
            "started": j.started, "ended": j.ended, "bar_streams": {s: len(v) for s, v in j.bars.items()}}
    if not rows:
        return pd.DataFrame(), meta
    df = pd.DataFrame(rows)
    expiry = sorted(df["expiry"].unique())[0]
    df = df[df["expiry"] == expiry]
    strikes = sorted(df["strike"].unique())
    ref = spot if spot is not None else strikes[len(strikes) // 2]
    atm = min(strikes, key=lambda s: abs(s - ref))
    i = strikes.index(atm)
    keep = strikes[max(0, i - each_side): i + each_side + 1]
    df = df[df["strike"].isin(keep)]
    df["spread"] = (df["ask"] - df["bid"]).round(2)
    wide = df.pivot_table(index="strike", columns="type", values=["bid", "ask", "ltp", "spread", "age_s"],
                          aggfunc="last")
    wide.columns = [f"{t}_{v}" for v, t in wide.columns]
    cols = [c for c in ("CE_age_s", "CE_spread", "CE_bid", "CE_ask", "CE_ltp") if c in wide] + \
           [c for c in ("PE_ltp", "PE_bid", "PE_ask", "PE_spread", "PE_age_s") if c in wide]
    meta.update(atm=atm, expiry=expiry)
    return wide[cols].reset_index(), meta


def live_bars(j: LiveJournal, stream: str) -> pd.DataFrame:
    b = j.bars.get(stream) or []
    if not b:
        return pd.DataFrame(columns=["time", "open", "high", "low", "close"])
    df = pd.DataFrame(b)
    df["time"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert(IST).dt.strftime("%H:%M")
    return df[["time", "open", "high", "low", "close"]]


# ---------------------------------------------------------------------------------------------- live session shadow
_SHADOW: dict[str, object] = {}
_SHADOW_RETRIED: dict[str, str] = {}
_SHADOW_LOCK = __import__("threading").Lock()


def live_shadow(day: str, run_id: str | None = None):
    """Muted journal replay of today's paper session (app.engine.shadow), cached and refreshed incrementally."""
    from app.engine.shadow import ShadowSession
    p = live_journal_path(day, run_id)
    with _SHADOW_LOCK:
        sh = _SHADOW.get(str(p))
        if sh is not None and sh.error and not _SHADOW_RETRIED.get(str(p)):
            _SHADOW_RETRIED[str(p)] = sh.error                # one clean rebuild; a repeat failure stays visible
            sh = None
        if sh is None or (p.exists() and p.stat().st_size < sh.offset):
            sh = _SHADOW[str(p)] = ShadowSession(p)
    return sh.refresh()


def engine_state_label(day: str) -> tuple[str, str]:
    """(icon+text, detail) for the sidebar: manual kill file + the running engine's automatic latch (shadow)."""
    ks = __import__("app.control.actions", fromlist=["kill_status"]).kill_status()
    if ks["manual"]:
        return "🛑 SAFE (kill switch on)", "manual kill switch"
    if live_journal_path(day).exists():
        st = live_shadow(day).state()
        auto = (st.get("kill") or {}).get("auto")
        if auto and not auto.get("acked_by"):
            return "🛑 ENTRIES BLOCKED", f"automatic kill: {auto.get('reason')} — {auto.get('detail')}"
        if st.get("error") or st.get("mismatches"):
            return "⚠️ CHECK ENGINE", st.get("error") or "shadow replay does not match engine checkpoints"
        age = st.get("last_event_age_s")
        if not st.get("ended") and age is not None and age > 30:
            return "⚠️ ENGINE SILENT", f"no journal event for {age:.0f}s"
    elif live_journal_closed(day):
        return "⏹ CLOSED", f"session {day} closed out"
    return "🟢 ACTIVE", ""
