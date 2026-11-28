"""NiftyTradingAI combined daily P&L report: every paper strategy of the day in one Telegram message.

Covers the rule run (ORB, pullback; run `paper_rules_v1`) and the strategy book (base U1, PB, Q1-Q8 and parallel
N1-N8; run `book_v1`). Built only from the database after the 15:35 closeout (PaperTrades / BookTrades /
TradingSignals), so the same text can be re-sent or previewed later (scripts/send_daily_report.py, dashboard).

Layout (plain text, Telegram-safe; split into numbered parts only if it exceeds one message):
  header           app name, date, paper-only notice
  day total        net (model), executable net, gross, charges, wins/losses, best and worst trade, peak capital
  by group         rules / base book / parallel N: trades and net
  by strategy      one block per strategy that traded (contract, times, prices, exit reason, net, executable),
                   then one line per group listing strategies with no signal / a skipped signal and why
  to date          per strategy since the run started: trades, win rate, net, profit factor, max drawdown, MTD
  notes            kill switch, closeout problems
"""
from __future__ import annotations

import json
from datetime import date

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

APP_NAME = "NiftyTradingAI"
TELEGRAM_SAFE = 3900

RULE_NAMES = {"orb": ("ORB", "Rules · Opening-Range Breakout"), "pullback": ("PULL", "Rules · Trend Pullback")}
GROUPS = [("rules", "Rules strategies"), ("base", "Strategy book · base (U1, PB, Q1–Q8)"),
          ("parallel", "Strategy book · parallel (N1–N8)")]
EXIT_TEXT = {"SL": "stop loss 15%", "BE_SL": "break-even stop", "TRAIL_SL": "trailing stop", "ST_FLIP": "Supertrend flip",
             "SQUARE_OFF": "15:15 square-off", "END": "end of data"}
SKIP_TEXT = {"NO_OPTION_IN_BAND": "no option near the premium target", "ROC_FILTER": "ROC filter",
             "ENGINE_STARTED_LATE": "engine started after the signal"}


def _q(engine: Engine, sql: str, **p) -> pd.DataFrame:
    try:
        with engine.connect() as c:
            return pd.read_sql_query(text(sql), c, params=p)
    except Exception:  # noqa: BLE001 - a table missing in an old database means "nothing to report"
        return pd.DataFrame()


def _inr(x, signed: bool = True) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return "–"
    s = f"₹{abs(x):,.0f}"
    if not signed:
        return s
    return ("+" if x > 0 else "−" if x < 0 else "") + s


def _hm(ts) -> str:
    return str(ts)[11:16] if ts else "–"


def _max_dd(pnl: pd.Series) -> float:
    eq = pnl.cumsum()
    return float((eq - eq.cummax().clip(lower=0)).min()) if len(eq) else 0.0


def _pf(p: pd.Series):
    w, lo = p[p > 0].sum(), -p[p < 0].sum()
    return round(float(w / lo), 2) if lo > 0 else (float("inf") if w > 0 else None)


def _pf_txt(x) -> str:
    return "–" if x is None else "∞" if x == float("inf") else f"{x:.2f}"


def _peak_capital(tr: pd.DataFrame) -> tuple[float, str | None]:
    """Largest premium outlay held at the same time (entry price x qty of the open trades), and when."""
    if tr.empty:
        return 0.0, None
    ev = []
    for r in tr.itertuples(index=False):
        amt = float(r.entry_px) * int(r.qty)
        ev += [(str(r.entry_ts), 1, amt), (str(r.exit_ts), 0, -amt)]     # an exit at T frees before an entry at T
    cur, peak, at = 0.0, 0.0, None
    for ts, _, amt in sorted(ev):
        cur += amt
        if cur > peak:
            peak, at = cur, ts
    return peak, at


def build_combined(engine: Engine, session_date: str, rules_run: str | None, rules_strategies: list[str],
                   book_run: str | None, book_codes: list[str], notes: list[str] | None = None) -> dict:
    from app.book.strategies import STRATEGIES
    rows: list[dict] = []                     # one per traded strategy-trade (day)
    hist: list[pd.DataFrame] = []             # run-to-date trades, normalised
    strat: list[dict] = []                    # every configured strategy with its day status

    if rules_run:
        pt = _q(engine, "SELECT * FROM PaperTrades WHERE run_id = :r AND session_date <= :d", r=rules_run, d=session_date)
        sg = _q(engine, "SELECT strategy_id, outcome, reasons FROM TradingSignals WHERE run_id = :r AND session_date = :d",
                r=rules_run, d=session_date)
        if len(pt):
            h = pt.assign(code=pt["strategy_id"].map(lambda s: RULE_NAMES.get(s, (s.upper(), s))[0]), exec_net_pnl=None)
            hist.append(h[["code", "session_date", "exit_ts", "net_pnl", "exec_net_pnl"]])
        for sid in rules_strategies:
            code, name = RULE_NAMES.get(sid, (sid.upper(), f"Rules · {sid}"))
            day = pt[(pt["session_date"] == session_date) & (pt["strategy_id"] == sid)] if len(pt) else pt
            for t in day.itertuples(index=False):
                ctr = str(t.contract).split("|")
                rows.append({"group": "rules", "code": code, "name": name, "contract": " ".join(ctr[2:4]) if len(ctr) >= 4
                             else str(t.contract), "entry_ts": t.entry_ts, "exit_ts": t.exit_ts, "entry_px": t.entry_px,
                             "exit_px": t.exit_px, "qty": t.qty, "reason": str(t.exit_reason), "gross": t.gross_pnl,
                             "charges": t.charges, "net": t.net_pnl, "exec": None})
            mine = sg[sg["strategy_id"] == sid] if len(sg) else sg
            strat.append({"group": "rules", "code": code, "name": name, "traded": len(day) > 0,
                          "skip": _rule_skip(mine) if not len(day) else None})

    if book_run:
        bt = _q(engine, "SELECT * FROM BookTrades WHERE run_id = :r AND session_date <= :d", r=book_run, d=session_date)
        sg = _q(engine, "SELECT setup_id AS code, outcome, reasons FROM TradingSignals WHERE run_id = :r "
                        "AND session_date = :d ORDER BY seq", r=book_run, d=session_date)
        if len(bt):
            hist.append(bt[["code", "session_date", "exit_ts", "net_pnl", "exec_net_pnl"]])
        for code in book_codes:
            sd = STRATEGIES.get(code)
            if sd is None:
                continue
            day = bt[(bt["session_date"] == session_date) & (bt["code"] == code)] if len(bt) else bt
            for t in day.itertuples(index=False):
                rows.append({"group": sd.family, "code": code, "name": sd.name,
                             "contract": f"{int(t.strike)} {t.side}", "entry_ts": t.entry_ts, "exit_ts": t.exit_ts,
                             "entry_px": t.entry_px, "exit_px": t.exit_px, "qty": t.qty, "reason": t.exit_reason,
                             "gross": t.gross_pnl, "charges": t.charges, "net": t.net_pnl,
                             "exec": None if pd.isna(t.exec_net_pnl) else float(t.exec_net_pnl)})
            mine = sg[sg["code"] == code] if len(sg) else sg
            skip = None
            if not len(day) and len(mine):
                r = json.loads(mine.iloc[-1]["reasons"] or "[]")
                why = (r[0] if r else mine.iloc[-1]["outcome"]) or ""
                skip = SKIP_TEXT.get(why, "kill switch" if why.startswith("KILL_SWITCH") else why.replace("_", " ").lower())
            strat.append({"group": sd.family, "code": code, "name": sd.name, "traded": len(day) > 0, "skip": skip})

    day = pd.DataFrame(rows)
    H = pd.concat(hist, ignore_index=True) if hist else pd.DataFrame(columns=["code", "session_date", "exit_ts", "net_pnl"])
    month = session_date[:7]
    to_date = []
    for s in strat:
        h = H[H["code"] == s["code"]].sort_values(["session_date", "exit_ts"])
        if not len(h):
            continue
        p = h["net_pnl"].astype(float)
        to_date.append({"group": s["group"], "code": s["code"], "trades": len(h), "win": float((p > 0).mean()),
                        "net": float(p.sum()), "pf": _pf(p), "max_dd": _max_dd(p),
                        "mtd": float(p[h["session_date"].astype(str).str.startswith(month)].sum()),
                        "sessions": int(h["session_date"].nunique())})
    ran_q = _q(engine, "SELECT DISTINCT run_id FROM CapitalUsage WHERE session_date = :d", d=session_date)
    ran_ids = set(ran_q["run_id"]) if len(ran_q) else set()
    ran = {"rules": rules_run in ran_ids, "base": book_run in ran_ids, "parallel": book_run in ran_ids}
    sess = _q(engine, "SELECT DISTINCT session_date FROM CapitalUsage WHERE run_id IN (:a, :b) AND session_date <= :d",
              a=rules_run or "", b=book_run or "", d=session_date)
    allp = H.sort_values(["session_date", "exit_ts"])["net_pnl"].astype(float) if len(H) else pd.Series(dtype=float)
    daily = H.groupby("session_date")["net_pnl"].sum() if len(H) else pd.Series(dtype=float)
    peak, peak_at = _peak_capital(day) if len(day) else (0.0, None)
    ex = day["exec"].dropna() if len(day) else pd.Series(dtype=float)
    return {
        "app": APP_NAME, "session": session_date, "rules_run": rules_run, "book_run": book_run,
        "trades": rows, "strategies": strat, "to_date": to_date, "notes": list(notes or []), "ran": ran,
        "net": float(day["net"].sum()) if len(day) else 0.0,
        "gross": float(day["gross"].sum()) if len(day) else 0.0,
        "charges": float(day["charges"].sum()) if len(day) else 0.0,
        "exec_net": float(ex.sum()) if len(ex) else None, "exec_count": int(len(ex)),
        "wins": int((day["net"] > 0).sum()) if len(day) else 0,
        "losses": int((day["net"] <= 0).sum()) if len(day) else 0,
        "peak_capital": peak, "peak_capital_at": peak_at,
        "run_net": float(allp.sum()), "run_trades": int(len(allp)), "run_pf": _pf(allp),
        "run_max_dd": _max_dd(allp), "run_trade_days": int(len(daily)),
        "run_sessions": int(len(sess)) if len(sess) else int(len(daily)),
        "run_since": str(sess["session_date"].min()) if len(sess) else (str(daily.index.min()) if len(daily) else None),
        "run_mtd": float(daily[daily.index.astype(str).str.startswith(month)].sum()) if len(daily) else 0.0,
        "best_day": (str(daily.idxmax()), float(daily.max())) if len(daily) else None,
        "worst_day": (str(daily.idxmin()), float(daily.min())) if len(daily) else None,
    }


def _rule_skip(sg: pd.DataFrame) -> str | None:
    if sg is None or not len(sg):
        return None
    r = json.loads(sg.iloc[-1]["reasons"] or "[]")
    return (str(r[0]) if r else str(sg.iloc[-1]["outcome"])).replace("_", " ").lower()


def _reason(r: str) -> str:
    return EXIT_TEXT.get(r) or str(r).replace("_", " ").lower()


def _n(k: int, word: str) -> str:
    return f"{k} {word}" + ("" if k == 1 else "s")


def format_combined(d: dict) -> list[str]:
    """The report as Telegram messages (normally one; numbered parts when it is longer than one message)."""
    dt = date.fromisoformat(d["session"])
    tr = d["trades"]
    head = [f"📊 {d['app']} — Daily P&L Report", f"{dt:%a %d %b %Y} · paper trading (no real orders)"]
    n_strat = len(d["strategies"])
    ran = d.get("ran") or {}
    tot = [f"💰 Day net {_inr(d['net'])}  ({_n(len(tr), 'trade')} from {n_strat} strategies)"]
    if tr:
        wr = d["wins"] / len(tr)
        tot.append(f"Gross {_inr(d['gross'])} · charges {_inr(d['charges'], False)} · "
                   f"won {d['wins']} / lost {d['losses']} ({wr:.0%})")
        if d["exec_net"] is not None:
            tot.append(f"Executable (live bid/ask) {_inr(d['exec_net'])} on {d['exec_count']} book trades")
        best = max(tr, key=lambda t: t["net"])
        worst = min(tr, key=lambda t: t["net"])
        tot.append(f"Best {best['code']} {_inr(best['net'])}"
                   + (f" · worst {worst['code']} {_inr(worst['net'])}" if worst["net"] < 0 else ""))
        tot.append(f"Peak capital in use {_inr(d['peak_capital'], False)} at {_hm(d['peak_capital_at'])}")
    groups = ["📂 By group"]
    for g, label in GROUPS:
        gs = [s for s in d["strategies"] if s["group"] == g]
        if not gs:
            continue
        gt = [t for t in tr if t["group"] == g]
        if ran.get(g, True) or gt:
            groups.append(f"• {label}: {_inr(sum(t['net'] for t in gt))} · {_n(len(gt), 'trade')}")
        else:
            groups.append(f"• {label}: not running this session")

    blocks = []
    for g, label in GROUPS:
        gs = [s for s in d["strategies"] if s["group"] == g]
        if not gs:
            continue
        L = [f"— {label} —"]
        if not ran.get(g, True) and not any(t["group"] == g for t in tr):
            continue
        for s in gs:
            for t in (t for t in tr if t["code"] == s["code"]):
                icon = "🟢" if t["net"] > 0 else "🔴"
                ex = f" · exec {_inr(t['exec'])}" if t.get("exec") is not None else ""
                L.append(f"{icon} {t['code']} {s['name']}  {_inr(t['net'])}")
                L.append(f"   {t['contract']} · {_hm(t['entry_ts'])}→{_hm(t['exit_ts'])} · "
                         f"{t['entry_px']:.2f}→{t['exit_px']:.2f} · {_reason(t['reason'])}{ex}")
        skipped = [s for s in gs if not s["traded"] and s["skip"]]
        quiet = [s for s in gs if not s["traded"] and not s["skip"]]
        if skipped:
            L.append("⏭ Signal, no trade: " + "; ".join(f"{s['code']} ({s['skip']})" for s in skipped))
        if quiet:
            L.append("⚪ No signal: " + ", ".join(s["code"] for s in quiet))
        blocks.append("\n".join(L))

    td = ["📅 Run to date (model P&L)"]
    if d["run_trades"]:
        td.append(f"All: {_n(d['run_trades'], 'trade')} in {_n(d['run_sessions'], 'session')}"
                  f" (since {d['run_since']}) · net {_inr(d['run_net'])} · "
                  f"MTD {_inr(d['run_mtd'])} · PF {_pf_txt(d['run_pf'])} · "
                  f"max DD {_inr(d['run_max_dd'])}")
        if d["best_day"] and d["run_trade_days"] > 1:
            td.append(f"Best day {d['best_day'][0][5:]} {_inr(d['best_day'][1])} · "
                      f"worst day {d['worst_day'][0][5:]} {_inr(d['worst_day'][1])}")
        td.append("strategy: trades · win · net · PF · max DD · MTD")
        names = {s["code"]: s["name"] for s in d["strategies"]}
        for r in sorted(d["to_date"], key=lambda r: -r["net"]):
            td.append(f"{r['code']} {names.get(r['code'], '')}: {r['trades']} · {r['win']:.0%} · {_inr(r['net'])} · "
                      f"{_pf_txt(r['pf'])} · {_inr(r['max_dd'])} · {_inr(r['mtd'])}")
        none = [s["code"] for s in d["strategies"] if s["code"] not in {r["code"] for r in d["to_date"]}]
        if none:
            td.append("No trades yet: " + ", ".join(none))
    else:
        td.append("No closed trades yet.")
    foot = []
    if d["notes"]:
        foot = ["⚠️ Notes"] + [f"• {n}" for n in d["notes"]]
    foot.append("Model P&L = specification fills and costs (backtest-comparable); executable = live ask in, bid out.")

    sections = ["\n".join(head), "\n".join(tot), "\n".join(groups), *blocks, "\n".join(td), "\n".join(foot)]
    chunks = []                                       # a section longer than one message is cut at line breaks
    for sec in sections:
        buf = ""
        for line in sec.split("\n"):
            if buf and len(buf) + 1 + len(line) > TELEGRAM_SAFE:
                chunks.append(buf)
                buf = line
            else:
                buf = line if not buf else buf + "\n" + line
        chunks.append(buf)
    parts, cur = [], ""
    for sec in chunks:
        add = sec if not cur else cur + "\n\n" + sec
        if len(add) > TELEGRAM_SAFE and cur:
            parts.append(cur)
            cur = sec
        else:
            cur = add
    parts.append(cur)
    if len(parts) > 1:
        parts = [p if i == 0 else f"📊 {d['app']} — Daily P&L Report {dt:%d %b} (part {i + 1}/{len(parts)})\n\n" + p
                 for i, p in enumerate(parts)]
        parts[0] = parts[0].replace(" — Daily P&L Report", f" — Daily P&L Report (part 1/{len(parts)})", 1)
    return parts
