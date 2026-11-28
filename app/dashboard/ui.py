"""NiftyTradingAI operator dashboard (v1.3 Phase 8, §14.1). PAPER TRADING ONLY.

Run (localhost only; settings in .streamlit/config.toml):
    .venv\\Scripts\\python -m streamlit run app\\dashboard\\ui.py

Read-only: every database is opened with SQLite mode=ro. The only state-changing controls are on the
"Config & kill switch" page and go through app.control.actions (operator name + reason + confirmation phrase,
hash-chained operator log).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402
import plotly.express as px  # noqa: E402
import streamlit as st  # noqa: E402

from datetime import datetime  # noqa: E402

from app.clock import IST  # noqa: E402
from app.control import actions  # noqa: E402
from app.dashboard import data as D  # noqa: E402
from app.notify.telegram import Notifier, kill_switch_alert, make_transport  # noqa: E402
from app.reports.daily import build_daily_report, format_report  # noqa: E402
from app.services.health import read_health  # noqa: E402

st.set_page_config(page_title="NiftyTradingAI", page_icon="📈", layout="wide")


@st.cache_resource
def _engines():
    return D.engines()


def _run_picker(eng, key: str):
    r = D.runs(eng)
    if r.empty:
        st.info("No paper / backtest runs in this database yet.")
        return None, r
    run = st.selectbox("Run", r["run_id"], key=key,
                       format_func=lambda x: f"{x} ({r.set_index('run_id').at[x, 'mode']}, "
                                             f"{r.set_index('run_id').at[x, 'sessions']} sessions)")
    return run, r


def _db_picker(key: str):
    engs = _engines()
    names = [n for n in ("research", "main") if n in engs]
    if not names:
        st.error("No database found.")
        st.stop()
    name = st.radio("Database", names, horizontal=True, key=key,
                    help="research = backtests/experiments; main = live paper trading (Phase 9)")
    return engs[name]


# ---------------------------------------------------------------------------------------------- pages
def page_home():
    st.header("System health")
    ks = actions.kill_status()
    c = st.columns(2) + st.columns(2)
    c[0].metric("Trading state", "SAFE" if ks["manual"] else "ACTIVE",
                help="SAFE = manual kill switch on, new entries blocked")
    h = read_health()
    c[1].metric("Recorder", h.get("state", "?"))
    c[2].metric("Clock (IST)", D.now_ist()[11:19])
    try:
        import psutil
        c[3].metric("CPU / memory", f"{psutil.cpu_percent():.0f}% / {psutil.virtual_memory().percent:.0f}%")
    except ImportError:
        c[3].metric("CPU / memory", "n/a", help="install psutil to show host load")
    if ks["manual"]:
        st.error(f"Kill switch ON since {ks['changed_at']} by {ks['changed_by']}"
                 + (" — emergency close requested" if ks["emergency_close"] else ""))
    st.subheader("Feed / recorder")
    st.json(h, expanded=False)
    engs = _engines()
    st.subheader("Databases")
    st.write({n: str(e.url).split("///")[-1].split("?")[0].removeprefix("file:") for n, e in engs.items()})
    if "main" in engs:
        st.subheader("Recent system events")
        st.dataframe(D.system_events(engs["main"], 30), width="stretch", hide_index=True)
    log = actions.OperatorLog()
    st.caption(f"Operator log: {len(log.records())} records, chain intact = {log.verify()}")


def page_market():
    st.header("Market monitor")
    today = datetime.now(IST).date().isoformat()
    has_live = D.live_journal_path(today).exists()
    mode = st.radio("Source", ["Live (today's paper engine)", "History (imported PG snapshots)"],
                    index=0 if has_live else 1, horizontal=True)
    if mode.startswith("Live"):
        if D.live_journal_closed(today):
            st.info(f"The {today} session is closed (journal compressed at closeout). "
                    "Switch to History for the imported chain snapshots.")
            return
        if not has_live:
            st.info(f"No paper-engine journal for {today} yet — start the engine (start_trading_day.bat).")
            return
        _live_market(today)
        return
    st.caption("Imported HISTORY chain (PostgreSQL top-of-book snapshots), ATM±5.")
    days = D.chain_days()
    if not days:
        st.info("No history chain imported.")
        return
    c = st.columns(2)
    day = c[0].selectbox("Session", days[::-1])
    at = c[1].text_input("Time (HH:MM IST, blank = last snapshot)", "")
    try:
        wide, meta = D.chain_snapshot(day, at.strip() or None)
    except Exception as e:  # noqa: BLE001
        st.error(f"cannot load chain: {e}")
        return
    m = st.columns(5)
    m[0].metric("Spot", f"{meta['spot']:,.2f}")
    m[1].metric("ATM", f"{meta['atm']:,.0f}")
    m[2].metric("Expiry", meta["expiry"])
    m[3].metric("Snapshot", meta["snapshot"][11:19])
    m[4].metric("Quality flags", meta["rows_flagged"])
    st.dataframe(wide, width="stretch", hide_index=True)
    st.caption(f"tier: {meta['tier']} · {meta['snapshots_in_day']} snapshots that day")
    _vix_section(day)


@st.fragment(run_every=5)
def _live_market(day: str):
    """Live view from the engine's journal (read-only, incremental); refreshes every 5 s."""
    j = D.live_journal(day)
    wide, meta = D.live_chain(j)
    age = meta["last_event_age_s"]
    st.caption(f"Run {D.paper_run_id()} · journal {meta['records']:,} records · started {str(meta['started'])[11:19]}"
               f" IST · {'session ENDED' if meta['ended'] else 'running'} · refreshes every 5 s")
    m = st.columns(5)
    m[0].metric("Spot", f"{meta['spot']:,.0f}" if meta["spot"] else "–")
    m[1].metric("ATM", f"{meta['atm']:,.0f}" if meta["atm"] else "–")
    m[2].metric("Expiry", meta["expiry"] or "–")
    m[3].metric("Last event", f"{age:.0f}s ago" if age is not None else "–")
    m[4].metric("Contracts", meta["quotes"])
    if age is not None and age > 30 and not meta["ended"]:
        st.error(f"No engine event for {age:.0f} s — check the engine window / feed.")
    if wide.empty:
        st.info("No option quotes in the journal yet.")
    else:
        st.dataframe(wide, width="stretch", hide_index=True)
    c = st.columns(2)
    for col, stream, title in ((c[0], "index", "NIFTY 50 (1-min close)"), (c[1], "vix", "India VIX (1-min close)")):
        b = D.live_bars(j, stream)
        col.caption(f"{title} · {len(b)} bars" + (f" · last {b['close'].iloc[-1]:,.2f} at {b['time'].iloc[-1]}"
                                                   if len(b) else ""))
        if len(b):
            col.line_chart(b.set_index("time")["close"], height=220)
    st.caption("Top of book from the engine's own quote stream (what the paper broker fills against). IV, delta "
               "and OI are not in the journal; see the History view for those.")


def _vix_section(day: str):
    st.subheader("India VIX")
    st.caption("Market volatility-level input (owner decision 2026-11-26). Recorded with every decision as the VIX "
               "bar of the minute before the decision bar; no rule uses it yet. History: Dhan intraday candles.")
    daily = D.vix_daily()
    if daily.empty:
        st.info("No India VIX history — run scripts\backfill_vix.py.")
        return
    intr = D.vix_intraday(day)
    m = st.columns(4)
    if not intr.empty:
        m[0].metric(f"VIX close {day}", f"{intr['close'].iloc[-1]:.2f}",
                    f"{intr['close'].iloc[-1] - intr['open'].iloc[0]:+.2f} on the day")
        m[1].metric("Day range", f"{intr['low'].min():.2f} – {intr['high'].max():.2f}")
    m[2].metric("History", f"{len(daily)} sessions")
    m[3].metric("Range over history", f"{daily['low'].min():.2f} – {daily['high'].max():.2f}")
    c = st.columns(2)
    if not intr.empty:
        c[0].caption(f"Intraday {day} (1-minute close)")
        c[0].line_chart(intr.set_index("time")["close"], height=220)
    c[1].caption("Daily close")
    c[1].line_chart(daily.set_index("session")["close"], height=220)


def page_signals():
    st.header("Signals")
    eng = _db_picker("sig_db")
    run, _ = _run_picker(eng, "sig_run")
    if not run:
        return
    sig = D.signals(eng, run)
    days = sorted(sig["session_date"].unique())[::-1]
    day = st.selectbox("Session", ["(all)"] + days)
    if day != "(all)":
        sig = sig[sig["session_date"] == day]
    outs = st.multiselect("Outcome", sorted(sig["outcome"].unique()), default=sorted(sig["outcome"].unique()))
    sig = sig[sig["outcome"].isin(outs)]
    c = st.columns(3)
    for i, (k, v) in enumerate(sig["outcome"].value_counts().items()):
        c[i % 3].metric(k, int(v))
    rej = sig[sig["outcome"] != "APPROVED"]["reasons"].dropna().map(json.loads).explode().value_counts()
    if len(rej):
        st.subheader("Rejection / no-trade reasons")
        st.bar_chart(rej)
    st.dataframe(sig, width="stretch", hide_index=True)


def page_trades():
    st.header("Paper trades")
    eng = _db_picker("tr_db")
    run, _ = _run_picker(eng, "tr_run")
    if not run:
        return
    tr = D.trades(eng, run)
    day = st.selectbox("Session", ["(all)"] + sorted(tr["session_date"].unique())[::-1])
    if day != "(all)":
        tr = tr[tr["session_date"] == day]
    cols = ["session_date", "trade_id", "strategy_id", "contract", "dte", "qty", "entry_ts", "entry_px", "exit_ts",
            "exit_px", "exit_reason", "gross_pnl", "charges", "net_pnl", "risk_inr", "r_multiple", "low_fidelity"]
    st.dataframe(tr[[c for c in cols if c in tr]], width="stretch", hide_index=True)
    cap = D.capital(eng, run)
    if day != "(all)":
        cap = cap[cap["session_date"] == day]
    ps = D.paper_sessions(eng)
    if not ps.empty:
        ps = ps[ps["run_id"] == run]
        if len(ps):
            st.subheader("Paper-engine sessions (journal, recovery, reconciliation)")
            ps = ps.assign(reconciled=ps["reconciliation"].map(lambda x: json.loads(x or "{}").get("ok")),
                           unresolved=ps["incidents"].map(lambda x: sum(not i["resolved"] for i in json.loads(x or "[]"))))
            st.dataframe(ps.drop(columns=["reconciliation", "incidents"]), width="stretch", hide_index=True)
    st.subheader("Capital usage per session (§9.7)")
    st.dataframe(cap.drop(columns=["id", "run_id"]), width="stretch", hide_index=True)


def page_performance():
    st.header("Performance")
    eng = _db_picker("pf_db")
    run, _ = _run_picker(eng, "pf_run")
    if not run:
        return
    tr = D.trades(eng, run)
    p = D.performance(tr)
    if not p["trades"]:
        st.info("No trades.")
        return
    c = st.columns(3) + st.columns(3)
    c[0].metric("Net P&L", f"₹{p['net_pnl']:,.0f}")
    c[1].metric("Trades", p["trades"])
    c[2].metric("Win rate", f"{p['win_rate']:.0%}")
    c[3].metric("Expectancy", f"₹{p['expectancy_inr']:,.0f} / {p['expectancy_r']:+.2f}R")
    c[4].metric("Profit factor", p["profit_factor"] if p["profit_factor"] is not None else "–")
    c[5].metric("Max drawdown", f"₹{p['max_drawdown']:,.0f}")
    t = tr.sort_values(["session_date", "exit_ts"]).reset_index(drop=True)
    t["equity"] = t["net_pnl"].cumsum()
    t["drawdown"] = t["equity"] - t["equity"].cummax().clip(lower=0)
    st.plotly_chart(px.line(t, x=t.index, y=["equity", "drawdown"], labels={"x": "trade #", "value": "₹"},
                            title="Equity and drawdown (net of charges)"), width="stretch")
    daily = t.groupby("session_date")["net_pnl"].sum().reset_index()
    st.plotly_chart(px.bar(daily, x="session_date", y="net_pnl", title="Net P&L per session"), width="stretch")
    a, b = st.columns(2)
    for col, by, title in ((a, "hour", "By entry hour"), (b, "dte", "By DTE")):
        col.subheader(title)
        col.dataframe(D.breakdown(tr, by), width="stretch", hide_index=True)
    a, b = st.columns(2)
    a.subheader("By strategy")
    a.dataframe(D.breakdown(tr, "strategy_id"), width="stretch", hide_index=True)
    b.subheader("By exit reason")
    b.dataframe(D.breakdown(tr, "exit_reason"), width="stretch", hide_index=True)
    st.subheader("R calibration")
    st.plotly_chart(px.histogram(tr, x="r_multiple", nbins=40, title="Distribution of realised R"),
                    width="stretch")
    st.subheader("Daily report (§14.2)")
    day = st.selectbox("Report session", sorted(tr["session_date"].unique())[::-1])
    st.code(format_report(build_daily_report(eng, run, day)), language=None)


def page_research():
    st.header("Backtests & research")
    engs = _engines()
    if "research" in engs:
        st.subheader("Experiments")
        st.dataframe(D.experiments(engs["research"]), width="stretch", hide_index=True)
        st.subheader("Hold-outs")
        st.dataframe(D.holdouts(engs["research"]), width="stretch", hide_index=True)
        st.subheader("Backtest runs")
        st.dataframe(D.runs(engs["research"]), width="stretch", hide_index=True)
    for name in ("main", "research"):
        if name in engs:
            ls = D.label_summary(engs[name])
            if not ls.empty:
                st.subheader(f"Candidate labels — Phase 7 track ({name} DB)")
                st.caption("Only prospective live-feed labels (data_tier observed_feed) count toward the 500-label "
                           "training threshold; history-tier labels exercise the pipeline.")
                st.dataframe(ls, width="stretch", hide_index=True)
    for name in ("main", "research"):
        if name in engs:
            _models_section(engs[name], name)
    _rolling_section()
    if "main" in engs:
        st.subheader("Indicator registry")
        st.dataframe(D.indicators(engs["main"]), width="stretch", hide_index=True)


def _models_section(eng, name: str):
    mv = D.model_versions(eng)
    if mv.empty:
        return
    st.subheader(f"Meta-model registry — Phase 7 ({name} DB)")
    st.caption("RESEARCH_ONLY = trained on history-tier labels or failed the promotion gate; never approvable. "
               "CANDIDATE = every gate check passed, awaiting operator approval (CLI: train_meta_model.py approve). "
               "No model is in the trading path.")
    cols = ["id", "model_name", "version", "status", "horizon_min", "rows", "sessions", "data_tiers", "brier_model",
            "brier_baseline", "ece_model", "net_improvement", "net_improvement_p05", "gate_passed", "created_at"]
    st.dataframe(mv[cols], width="stretch", hide_index=True)
    pick = st.selectbox("Model version", mv["id"].tolist(), key=f"mv_{name}",
                        format_func=lambda i: f"{mv.loc[mv['id'] == i, 'model_name'].iloc[0]} "
                                              f"v{mv.loc[mv['id'] == i, 'version'].iloc[0]}")
    row = mv[mv["id"] == pick].iloc[0]
    if row["status_reason"]:
        st.warning(row["status_reason"])
    if row["gate_failed"]:
        st.caption(f"Failed gate checks: {row['gate_failed']}")
    c = st.columns(2)
    c[0].caption("Walk-forward folds (test blocks are later sessions than everything the fold trained on)")
    c[0].dataframe(D.model_folds(eng, int(pick)), width="stretch", hide_index=True)
    cal = D.model_calibration(eng, int(pick))
    c[1].caption("Out-of-fold calibration: predicted P(win) vs observed win rate")
    if not cal.empty:
        c[1].dataframe(cal, width="stretch", hide_index=True)
        c[1].line_chart(cal.set_index("range")[["predicted", "observed"]], height=200)


def _rolling_section():
    r = D.rolling_coverage()
    cov = r["coverage"]
    if not cov.get("days_requested"):
        return
    st.subheader("Expired-option history — Dhan rolling options (OI-03, research tier)")
    st.caption("1-minute TRADE prices (no bid/ask) for ATM±5 of the near weekly expiry; strike relative to ATM, "
               "re-evaluated every minute. v1.3 §18: never used for fills, promotion or hold-outs — pre-screening "
               "and regime research only. Expiries are inferred (implied time-to-expiry + expiry weekday + holidays).")
    m = st.columns(5)
    m[0].metric("Trading days", cov["trading_days"])
    m[1].metric("Holidays / empty", cov["empty_days"])
    m[2].metric("Range", f"{cov['first'][:7]} → {cov['last'][:7]}" if cov["first"] else "—",
                help=f"{cov['first']} → {cov['last']}" if cov["first"] else None)
    m[3].metric("Rows", f"{cov['rows']:,}")
    m[4].metric("Expiry check problems", "not finalized" if not cov["finalized"] else cov["expiry_problems"])
    c = st.columns(2)
    c[0].caption("Coverage by month")
    c[0].dataframe(r["by_month"], width="stretch", hide_index=True)
    if not r["expiries"].empty:
        e = r["expiries"]
        c[1].caption("Inferred expiries (DTE distribution of the near weekly series)")
        c[1].bar_chart(e["dte"].value_counts().sort_index(), height=200)
        if not r["expiry_problems"].empty:
            st.dataframe(r["expiry_problems"], width="stretch", hide_index=True)
    if r["days"]:
        day = st.selectbox("Inspect a day", r["days"][::-1], key="ro_day")
        df = D.rolling_day(day)
        if not df.empty:
            exp = df["expiry"].dropna().iloc[0] if df["expiry"].notna().any() else "not inferred"
            st.caption(f"expiry {exp} · spot {df['spot'].iloc[0]:,.2f} → {df['spot'].iloc[-1]:,.2f}")
            atm = df[df["rel_strike"] == 0].pivot_table(index="time", columns="option_type", values="close")
            st.line_chart(atm, height=220)


def page_config():
    st.header("Configuration & kill switch")
    st.caption("Every action needs your name, a reason and a confirmation; it is written to the hash-chained "
               "operator log. The dashboard never writes the database.")
    ks = actions.kill_status()
    st.subheader(f"Kill switch — {ks['state']}")
    with st.form("kill"):
        actor = st.text_input("Operator")
        reason = st.text_input("Reason")
        if not ks["manual"]:
            emergency = st.checkbox("Also EMERGENCY CLOSE open paper positions at the next valid bid")
            ok = st.form_submit_button("Turn kill switch ON (block new entries now)", type="primary")
            if ok:
                try:
                    actions.kill_on(actor, reason, emergency_close=emergency)
                    Notifier(make_transport()).send(kill_switch_alert(True, actor, reason, emergency))
                    st.success("Kill switch ON — new entries are blocked.")
                    st.rerun()
                except actions.ActionError as e:
                    st.error(str(e))
        else:
            confirm = st.text_input(f"Type {actions.RESUME_PHRASE} to allow new entries again")
            if st.form_submit_button("Turn kill switch OFF"):
                try:
                    actions.kill_off(actor, reason, confirm)
                    Notifier(make_transport()).send(kill_switch_alert(False, actor, reason))
                    st.success("Kill switch OFF.")
                    st.rerun()
                except actions.ActionError as e:
                    st.error(str(e))
    with st.expander("Acknowledge an AUTOMATIC kill-switch trip"):
        with st.form("ack"):
            a_actor = st.text_input("Operator", key="ack_actor")
            a_reason = st.text_input("What you checked", key="ack_reason")
            if st.form_submit_button("Request acknowledgement"):
                try:
                    actions.acknowledge_auto(a_actor, a_reason)
                    st.success("Acknowledgement posted; the trading engine applies it on its next risk check.")
                except actions.ActionError as e:
                    st.error(str(e))

    st.subheader("Risk configuration")
    cfg = D.risk_config()
    st.write(f"Current version **{cfg['version']}** — changes take effect at the next trading-engine session start.")
    with st.form("cfg"):
        changes = {}
        cols = st.columns(3)
        for i, (sec, key) in enumerate(sorted(actions.EDITABLE)):
            cur = cfg.get(sec, {}).get(key)
            label = f"{sec}.{key}"
            col = cols[i % 3]
            if isinstance(cur, bool) or cur is None:
                continue
            if isinstance(cur, int):
                v = col.number_input(label, value=cur, step=1)
            elif isinstance(cur, float):
                v = col.number_input(label, value=cur, step=0.5, format="%.2f")
            else:
                v = col.text_input(label, value=str(cur))
            if v != cur:
                changes[label] = v
        c_actor = st.text_input("Operator", key="cfg_actor")
        c_reason = st.text_input("Reason", key="cfg_reason")
        c_confirm = st.text_input("Type APPLY to confirm")
        if st.form_submit_button("Apply changes"):
            try:
                rec = actions.change_risk_config(c_actor, c_reason, changes, c_confirm)
                st.success(f"Saved {rec['detail']['to_version']}: {rec['detail']['changes']}")
            except actions.ActionError as e:
                st.error(str(e))
    with st.expander("risk.yaml (read-only view)"):
        st.code(D.risk_config_text(), language="yaml")

    st.subheader("Operator log")
    log = actions.OperatorLog()
    st.caption(f"chain intact = {log.verify()}")
    recs = log.records()[-30:][::-1]
    if recs:
        st.dataframe(pd.DataFrame([{k: r[k] for k in ("seq", "at", "action", "actor", "reason")} for r in recs]),
                     width="stretch", hide_index=True)
    st.subheader("Recent notifications")
    st.dataframe(pd.DataFrame(D.notifications_tail()), width="stretch", hide_index=True)


def page_live():
    st.header("Live session")
    today = datetime.now(IST).date().isoformat()
    if D.live_journal_closed(today):
        _closed_session(today)
        return
    if not D.live_journal_path(today).exists():
        st.info(f"No paper-engine journal for {today} — the engine has not started today (start_trading_day.bat).")
        return
    _live_session(today)


def _closed_session(day: str):
    engs = _engines()
    if "main" not in engs:
        st.warning(f"{day} was closed out, but the main database was not found.")
        return
    d = D.closed_day(engs["main"], day)
    ps, tr, sig = d["session"], d["trades"], d["signals"]
    if ps is None:
        st.warning(f"The {day} journal is closed (compressed), but the database has no paper-session row for run "
                   f"{d['run_id']} — closeout may not have finished. Check the engine console log in data/logs.")
        return
    rec = json.loads(ps.get("reconciliation") or "{}")
    inc = json.loads(ps.get("incidents") or "[]")
    st.success(f"Session {day} CLOSED ({ps['status']}). Results below are what closeout saved to the database; "
               "the Signals, Paper trades and Performance pages have the full history.")
    net = float(tr["net_pnl"].sum()) if len(tr) else 0.0
    wins = int((tr["net_pnl"] > 0).sum()) if len(tr) else 0
    m = st.columns(6)
    m[0].metric("Engine", "CLOSED")
    m[1].metric("Signals", len(sig))
    m[2].metric("Trades (won / lost)", f"{len(tr)} ({wins} / {len(tr) - wins})")
    m[3].metric("Net P&L ₹", f"{net:,.0f}")
    m[4].metric("R total", f"{tr['r_multiple'].sum():+.2f}" if len(tr) else "–")
    m[5].metric("Reconciled", "yes" if rec.get("ok") else "NO")
    st.caption(f"run {d['run_id']} · journal {int(ps['journal_events']):,} events · restarts {ps['restarts']} · "
               f"unresolved incidents {sum(not i.get('resolved') for i in inc)}")
    if rec and not rec.get("ok"):
        st.error("Closeout reconciliation FAILED: " + json.dumps(rec))
    cols = ["trade_id", "strategy_id", "contract", "qty", "entry_ts", "entry_px", "exit_ts", "exit_px", "exit_reason",
            "gross_pnl", "charges", "net_pnl", "r_multiple"]
    st.subheader(f"Trades ({len(tr)})")
    if len(tr):
        st.dataframe(tr[[c for c in cols if c in tr]], width="stretch", hide_index=True)
    st.subheader(f"Signals ({len(sig)})")
    if len(sig):
        st.dataframe(sig, width="stretch", hide_index=True)
    if inc:
        st.subheader("Incidents")
        st.dataframe(pd.DataFrame(inc), width="stretch", hide_index=True)


@st.fragment(run_every=5)
def _live_session(day: str):
    sh = D.live_shadow(day)
    s = sh.state()
    st.caption(f"Rebuilt from the engine's journal by a muted read-only replay (same mechanism as crash recovery); "
               f"refreshes every 5 s. Signals, trades and P&L reach the database only at the 15:35 closeout.")
    if s.get("error"):
        st.error(s["error"])
        return
    if not s.get("built"):
        st.info("Journal has no start record yet.")
        return
    auto = (s.get("kill") or {}).get("auto")
    if s["kill"].get("manual"):
        st.error("Manual kill switch ON — entries blocked (Config & kill switch page).")
    if auto and not auto.get("acked_by"):
        st.error(f"Automatic kill switch TRIPPED: {auto['reason']} — {auto.get('detail')} (at {auto.get('at', '')[11:19]}). "
                 "New entries are blocked until an operator acknowledges it: "
                 r"`.venv\Scripts\python scripts\kill_switch.py ack --actor NAME --reason '...'`")
    if s["mismatches"]:
        st.error(f"Shadow replay does NOT match {len(s['mismatches'])} engine checkpoint(s) — the view may be wrong.")
    age = s["last_event_age_s"]
    if not s["ended"] and age is not None and age > 30:
        st.error(f"No engine event for {age:.0f} s — check the engine window.")
    m = st.columns(6)
    m[0].metric("Engine", "ENDED" if s["ended"] else "RUNNING")
    m[1].metric("Last event", f"{age:.0f}s ago" if age is not None else "–")
    m[2].metric("Strategies", ", ".join(s["strategies"]))
    m[3].metric("Signals", s["signals"])
    m[4].metric("Open / closed", f"{len(s['open_trades'])} / {s['closed_trades']}")
    m[5].metric("P&L ₹ (real. + unreal.)", f"{s['realized_inr'] + s['unrealized_inr']:,.0f}")
    st.caption(f"journal {s['records']:,} records · {s['checkpoints_ok']} engine checkpoints reproduced · "
               f"warm-up {s['warmup_bars']} bars · risk session {'open' if s['risk_session_open'] else 'waiting for first index bar'}"
               f" · entries {s['entries']} · consecutive losses {s['consecutive_losses']} · realized R {s['realized_r']:+.2f}"
               + (" · DAILY LOSS LIMIT HIT" if s["daily_loss_hit"] else "") + (f" · loss latch {s['loss_latch']}" if s["loss_latch"] else ""))
    lb = s.get("last_bars") or {}
    if lb:
        st.caption("Last 1-min bars: " + " · ".join(
            f"{k} {v['close']:,.2f} @ {datetime.fromtimestamp(v['ts'], IST):%H:%M}" for k, v in sorted(lb.items()) if v.get("ts")))
    pos = sh.positions()
    st.subheader(f"Open positions ({len(pos)})")
    if pos:
        st.dataframe(pd.DataFrame(pos), width="stretch", hide_index=True)
    tr = sh.trades()
    st.subheader(f"Closed trades today ({len(tr)})")
    if tr:
        st.dataframe(pd.DataFrame(tr), width="stretch", hide_index=True)
    sig = sh.signals()
    st.subheader(f"Signals today ({len(sig)})")
    if sig:
        df = pd.DataFrame(sig)
        keep = [c for c in ("ts", "strategy", "direction", "outcome", "reason", "cycle_key", "contract", "trade_id")
                if c in df]
        st.dataframe(df[keep] if keep else df, width="stretch", hide_index=True)
    else:
        st.caption("No candidate setup yet. The strategy inputs below show what the rules currently see.")
    ind = sh.indicators()
    if ind:
        st.subheader("What the strategies see (latest indicator values)")
        rows = []
        for key, v in ind.items():
            vals = {k: (round(x, 2) if isinstance(x, float) else x) for k, x in v.items()
                    if k not in ("ts", "binding", "valid", "state")}
            rows.append({"indicator": key, "bar": datetime.fromtimestamp(v["ts"], IST).strftime("%H:%M") if v.get("ts") else "",
                         "state": v.get("state"), "valid": v.get("valid"), "values": json.dumps(vals)})
        st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
    if s["incidents"]:
        st.subheader("Incidents")
        st.dataframe(pd.DataFrame(s["incidents"]), width="stretch", hide_index=True)
    ev = sh.risk_events(30)
    if ev:
        st.subheader("Risk events (latest 30)")
        st.dataframe(pd.DataFrame(ev), width="stretch", hide_index=True)


def page_book():
    from app.book.strategies import STRATEGIES
    st.header("Strategy book")
    cfg = D.book_cfg()
    run = cfg.get("run_id", "book_v1")
    st.caption(f"{len(cfg.get('codes') or [])} independent NIFTY option-buying strategies — base book U1, PB, Q1–Q8 and "
               f"the parallel set N1–N8 (run `{run}`, paper). Signals come from the NIFTY index "
               "chart, one trade per strategy per day; guard: kill switch only. **Model P&L** follows the "
               "specification (1-minute option bar fills, spec costs) and is comparable with the backtest; "
               "**executable** uses the live ask at the signal and the live bid at the exit.")
    if not cfg.get("enabled"):
        st.warning("The book is disabled in config/paper.yaml (book.enabled).")
    tab_today, tab_hist, tab_rep, tab_ref = st.tabs(["Today", "Forward-test results", "Daily report", "Strategies"])
    with tab_today:
        _book_today(datetime.now(IST).date().isoformat(), run)
    with tab_hist:
        engs = _engines()
        src = st.radio("Results", [f"Forward test ({run}, main database)",
                                   "Historical replay (book_replay, research database)"], horizontal=True)
        name, rid = ("main", run) if src.startswith("Forward") else ("research", "book_replay")
        bt = D.book_trades(engs[name], rid) if name in engs else pd.DataFrame()
        if bt.empty:
            st.info("No closed book trades yet. Forward-test results reach the database at the 15:35 closeout; "
                    "replays: scripts/run_book.py replay --persist.")
        else:
            days = sorted(bt["session_date"].unique())
            st.caption(f"{len(days)} sessions · {days[0]} → {days[-1]}")
            m = st.columns(4)
            m[0].metric("Trades", len(bt))
            m[1].metric("Win rate", f"{100 * (bt.net_pnl > 0).mean():.0f}%")
            m[2].metric("Net ₹ (model)", f"{bt.net_pnl.sum():,.0f}")
            ex = bt.exec_net_pnl.dropna()
            m[3].metric("Net ₹ (executable)", f"{ex.sum():,.0f}" if len(ex) else "–",
                        help="same trades priced at the live ask / bid")
            st.subheader("Per strategy")
            st.dataframe(D.book_summary(bt), width="stretch", hide_index=True)
            st.subheader("Daily P&L per strategy (model, ₹)")
            names = {c: f"{c} {s.name}" for c, s in STRATEGIES.items()}
            piv = bt.assign(strategy=bt["code"].map(names)).pivot_table(
                index="session_date", columns="strategy", values="net_pnl", aggfunc="sum", fill_value=0.0)
            piv["TOTAL"] = piv.sum(axis=1)
            st.dataframe(piv.sort_index(ascending=False).round(0), width="stretch")
            st.subheader("Cumulative P&L (model)")
            st.line_chart(piv.drop(columns="TOTAL").cumsum(), height=280)
            st.subheader("All trades")
            cols = ["session_date", "code", "name", "contract", "signal_ts", "entry_ts", "exit_ts", "entry_px",
                    "exit_px", "exit_reason", "net_pnl", "exec_entry_px", "exec_exit_px", "exec_net_pnl", "exec_note"]
            st.dataframe(bt[cols].sort_values(["session_date", "entry_ts"], ascending=False), width="stretch",
                         hide_index=True)
    with tab_rep:
        _book_daily_report(cfg)
    with tab_ref:
        st.dataframe(pd.DataFrame([{"code": s.code, "set": "parallel N" if s.family == "parallel" else "base",
                                    "strategy": s.name, "chart": f"{s.tf} min", "option": s.side,
                                    "premium target ₹": f"{s.target:g} ({s.band[0]:g}–{s.band[1]:g})",
                                    "exit": {"X1": "stop −15%, trail 15% from +15%",
                                             "E5": "stop −15%, break-even +20%, trail 25% from +40%",
                                             "X2": "fixed stop −15%"}[s.exit] + " · Supertrend flip · 15:15",
                                    "signal": s.summary, "enabled": s.code in set(cfg.get("codes") or [])}
                                   for s in STRATEGIES.values()]), width="stretch", hide_index=True)


def _book_daily_report(cfg: dict):
    """The combined NiftyTradingAI daily P&L report exactly as Telegram receives it after the 15:35 closeout."""
    import yaml
    from app.reports.combined import build_combined, format_combined
    from app.config import PROJECT_ROOT
    pc = yaml.safe_load((PROJECT_ROOT / "config" / "paper.yaml").read_text(encoding="utf-8"))
    engs = _engines()
    src = st.radio("Source", ["Paper run (main database)", "Historical replay (research database)"], horizontal=True,
                   key="book_rep_src")
    main = src.startswith("Paper")
    eng = engs.get("main" if main else "research")
    if eng is None:
        st.info("database not found")
        return
    rules_run = pc["run_id"] if main else None
    book_run = cfg.get("run_id", "book_v1") if main else "book_replay"
    days = D.q(eng, "SELECT DISTINCT session_date FROM CapitalUsage WHERE run_id IN (:a, :b) ORDER BY session_date DESC",
               a=rules_run or "", b=book_run)
    if days.empty:
        st.info("No closed session yet: the report is built at the 15:35 closeout.")
        return
    day = st.selectbox("Session", days["session_date"].tolist(), key="book_rep_day")
    d = build_combined(eng, day, rules_run, list(pc.get("strategies") or []) if rules_run else [], book_run,
                       list(cfg.get("codes") or []))
    for part in format_combined(d):
        st.code(part, language=None)
    st.caption("Re-send: `.venv\\Scripts\\python scripts\\send_daily_report.py --combined --db "
               f"{'main' if main else 'research'} --date {day} --send`")


@st.fragment(run_every=5)
def _book_today(day: str, run: str):
    snap = D.book_snapshot(day, run)
    if not snap:
        st.info(f"No book activity for {day} yet: the engine writes it while it runs (start_trading_day.bat).")
        return
    sm = snap["summary"]
    age = (datetime.now(IST) - datetime.fromisoformat(snap["at"])).total_seconds()
    st.caption(f"engine snapshot {snap['at'][11:19]} ({age:.0f}s ago)" + (" · day closed" if snap["finished"] else "")
               + f" · warm-up {sm.get('warmup_bars')} index bars · today {sm.get('index_bars_today')} bars")
    if not snap["finished"] and age > 30:
        st.error(f"Book snapshot is {age:.0f}s old — check the engine window.")
    m = st.columns(4)
    m[0].metric("Trades today", sm["trades"])
    m[1].metric("Open now", len(sm.get("open") or []))
    m[2].metric("Net ₹ (model)", f"{sm['net_pnl']:,.0f}")
    m[3].metric("Net ₹ (executable)", "–" if sm.get("exec_net_pnl") is None else f"{sm['exec_net_pnl']:,.0f}")
    st.subheader("Strategies")
    st.dataframe(pd.DataFrame(snap["status"]), width="stretch", hide_index=True)
    if snap["trades"]:
        st.subheader("Closed trades")
        st.dataframe(pd.DataFrame(snap["trades"]).drop(columns=["trail", "entry_ns", "exit_ns"], errors="ignore"),
                     width="stretch", hide_index=True)
    if snap["signals"]:
        st.subheader("Signals")
        sg = pd.DataFrame(snap["signals"])
        keep = [c for c in ("ts", "code", "name", "outcome", "reason", "contract", "entry_px") if c in sg]
        st.dataframe(sg[keep].iloc[::-1], width="stretch", hide_index=True)


PAGES = {"Live session": page_live, "Strategy book": page_book, "Home / health": page_home, "Market monitor": page_market, "Signals": page_signals,
         "Paper trades": page_trades, "Performance": page_performance, "Backtests & research": page_research,
         "Config & kill switch": page_config}

st.sidebar.title("NiftyTradingAI")
st.sidebar.caption("PAPER TRADING ONLY · localhost")
choice = st.sidebar.radio("Page", list(PAGES))
try:
    _label, _detail = D.engine_state_label(datetime.now(IST).date().isoformat())
except Exception as _e:  # noqa: BLE001
    _label, _detail = "⚠️ UNKNOWN", repr(_e)[:200]
st.sidebar.markdown(f"**State:** {_label}")
if _detail:
    st.sidebar.caption(_detail)
PAGES[choice]()
