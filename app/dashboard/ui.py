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
    st.caption("Live feed arrives with Phase 9 paper trading. Until then this shows the imported HISTORY chain "
               "(PostgreSQL top-of-book snapshots), ATM±5.")
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
    if "main" in engs:
        st.subheader("Indicator registry")
        st.dataframe(D.indicators(engs["main"]), width="stretch", hide_index=True)


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


PAGES = {"Home / health": page_home, "Market monitor": page_market, "Signals": page_signals,
         "Paper trades": page_trades, "Performance": page_performance, "Backtests & research": page_research,
         "Config & kill switch": page_config}

st.sidebar.title("NiftyTradingAI")
st.sidebar.caption("PAPER TRADING ONLY · localhost")
choice = st.sidebar.radio("Page", list(PAGES))
_ks = actions.kill_status()
st.sidebar.markdown(f"**State:** {'🛑 SAFE (kill switch on)' if _ks['manual'] else '🟢 ACTIVE'}")
PAGES[choice]()
