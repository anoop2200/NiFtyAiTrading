"""Daily paper P&L report with capital usage and live-capital planning (v1.3 §9.7, §14.2).

Built from PaperTrades + CapitalUsage of one run (paper mode in the trading DB, or a backtest run in the
research DB). "To date" figures cover every session of the run up to and including the report date.

  capital deployed      = Σ(entry fill × qty + entry charges) over open positions (tracked by the broker)
  shadow capital        = largest normal-trade R / 0.005 and largest 0DTE-trade R / 0.0025 (to date)
  planning figure       = max(peak capital to date, shadow capital to date) + cumulative max drawdown to date
The planning figure is an estimate from paper results only — not a recommendation.
"""
from __future__ import annotations

import json
from dataclasses import dataclass

import pandas as pd
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.database.models import CapitalUsage, PaperTrades

SHADOW_NORMAL, SHADOW_0DTE = 0.005, 0.0025


@dataclass
class Window:
    sessions: int = 20


def _trades(engine: Engine, run_id: str, upto: str) -> pd.DataFrame:
    with Session(engine) as s:
        rows = s.scalars(select(PaperTrades).where(PaperTrades.run_id == run_id, PaperTrades.session_date <= upto)).all()
        return pd.DataFrame([{c.name: getattr(r, c.name) for c in PaperTrades.__table__.columns} for r in rows])


def _capital(engine: Engine, run_id: str, upto: str) -> pd.DataFrame:
    with Session(engine) as s:
        rows = s.scalars(select(CapitalUsage).where(CapitalUsage.run_id == run_id, CapitalUsage.session_date <= upto)
                         .order_by(CapitalUsage.session_date)).all()
        return pd.DataFrame([{c.name: getattr(r, c.name) for c in CapitalUsage.__table__.columns} for r in rows])


def _max_dd(pnl: pd.Series) -> float:
    eq = pnl.cumsum()
    return float((eq.cummax().clip(lower=0) - eq).max()) if len(eq) else 0.0


def build_daily_report(engine: Engine, run_id: str, session_date: str, is_expiry_day: bool | None = None,
                       window: Window = Window()) -> dict:
    cap = _capital(engine, run_id, session_date)
    if cap.empty or session_date not in set(cap["session_date"]):
        raise LookupError(f"no CapitalUsage row for run {run_id} on {session_date}")
    tr = _trades(engine, run_id, session_date)
    day = tr[tr["session_date"] == session_date] if len(tr) else tr
    row = cap[cap["session_date"] == session_date].iloc[0]
    last = cap.tail(window.sessions)

    def peak(df):
        i = df["peak_capital"].idxmax()
        return {"inr": round(float(df.at[i, "peak_capital"]), 2), "session": df.at[i, "session_date"]}

    if len(tr):
        tr = tr.sort_values(["exit_ts", "session_date", "trade_id"])
        normal_r = tr.loc[tr["dte"] != 0, "risk_inr"]
        zero_r = tr.loc[tr["dte"] == 0, "risk_inr"]
    else:
        normal_r = zero_r = pd.Series(dtype=float)
    largest_normal = float(normal_r.max()) if len(normal_r) else 0.0
    largest_0dte = float(zero_r.max()) if len(zero_r) else 0.0
    shadow_normal, shadow_0dte = largest_normal / SHADOW_NORMAL, largest_0dte / SHADOW_0DTE
    cum_dd = max(_max_dd(tr["net_pnl"]) if len(tr) else 0.0, float(cap["max_intraday_drawdown"].max()))
    peak_to_date = float(cap["peak_capital"].max())
    planning = max(peak_to_date, shadow_normal, shadow_0dte) + cum_dd

    def seg(df):
        if df.empty:
            return {"trades": 0}
        return {"trades": int(len(df)), "net_pnl": round(float(df["net_pnl"].sum()), 2),
                "wins": int((df["net_pnl"] > 0).sum()), "losses": int((df["net_pnl"] < 0).sum()),
                "r_total": round(float(df["r_multiple"].fillna(0).sum()), 3)}

    exp_day = bool(is_expiry_day) if is_expiry_day is not None else bool(len(day) and (day["dte"] == 0).any())
    by_dte = {}
    if exp_day and len(day):
        for name, g in day.assign(b=day["dte"].map(lambda d: "0DTE" if d == 0 else "1DTE" if d == 1 else "2+DTE")).groupby("b"):
            by_dte[name] = seg(g)
    d = {
        "run_id": run_id, "session": session_date, "mode": row["mode"],
        "net_pnl": round(float(day["net_pnl"].sum()), 2) if len(day) else 0.0,
        "gross_pnl": round(float(day["gross_pnl"].sum()), 2) if len(day) else 0.0,
        "charges": round(float(day["charges"].sum()), 2) if len(day) else 0.0,
        "trades": int(len(day)),
        "wins": int((day["net_pnl"] > 0).sum()) if len(day) else 0,
        "losses": int((day["net_pnl"] < 0).sum()) if len(day) else 0,
        "timeouts": int(day["exit_reason"].astype(str).str.startswith("TIME_STOP").sum()) if len(day) else 0,
        "r_total": round(float(day["r_multiple"].fillna(0).sum()), 3) if len(day) else 0.0,
        "peak_capital": {"inr": round(float(row["peak_capital"]), 2), "at": row["peak_capital_ts"],
                         "open_trades": json.loads(row["peak_trade_ids"] or "[]")},
        "peak_capital_last_n": peak(last) | {"sessions": int(len(last))},
        "peak_capital_since_inception": peak(cap) | {"sessions": int(len(cap))},
        "max_intraday_drawdown": round(float(row["max_intraday_drawdown"]), 2),
        "cumulative_max_drawdown": round(cum_dd, 2),
        "largest_trade_r_today": round(float(day["risk_inr"].max()), 2) if len(day) else 0.0,
        "largest_normal_r_to_date": round(largest_normal, 2), "largest_0dte_r_to_date": round(largest_0dte, 2),
        "shadow_capital_0p50pct": round(shadow_normal, 2), "shadow_capital_0p25pct_0dte": round(shadow_0dte, 2),
        "live_capital_planning_figure": round(planning, 2),
        "expiry_day": exp_day, "by_dte": by_dte,
    }
    return d


def _inr(x) -> str:
    return f"₹{x:,.0f}"


def format_report(d: dict) -> str:
    L = [f"📊 Daily paper P&L — {d['session']} ({d['mode']}, run {d['run_id']})",
         f"Net {_inr(d['net_pnl'])} · gross {_inr(d['gross_pnl'])} · charges {_inr(d['charges'])}",
         f"Trades {d['trades']} · wins {d['wins']} · losses {d['losses']} · timeouts {d['timeouts']} · R total {d['r_total']:+.2f}",
         "", "Capital usage (paper — no capital limit):",
         f"• Peak capital today {_inr(d['peak_capital']['inr'])} at {d['peak_capital']['at'] or '–'}"
         f" · open trades {d['peak_capital']['open_trades'] or '–'}",
         f"• Peak last {d['peak_capital_last_n']['sessions']} sessions {_inr(d['peak_capital_last_n']['inr'])}"
         f" ({d['peak_capital_last_n']['session']}) · since inception {_inr(d['peak_capital_since_inception']['inr'])}"
         f" ({d['peak_capital_since_inception']['session']})",
         f"• Max intraday drawdown {_inr(d['max_intraday_drawdown'])} · cumulative max drawdown {_inr(d['cumulative_max_drawdown'])}",
         f"• Largest trade R today {_inr(d['largest_trade_r_today'])}",
         f"• Shadow capital: 0.50% rule {_inr(d['shadow_capital_0p50pct'])} · 0.25% 0DTE rule {_inr(d['shadow_capital_0p25pct_0dte'])}",
         f"• Live-capital PLANNING ESTIMATE {_inr(d['live_capital_planning_figure'])}",
         "  (paper results only, not a recommendation; real fills, slippage and margin rules may differ)"]
    if d["expiry_day"] and d["by_dte"]:
        L += ["", "Expiry day — by DTE:"]
        for k, s in d["by_dte"].items():
            L.append(f"• {k}: {s['trades']} trades, net {_inr(s['net_pnl'])}, R {s['r_total']:+.2f}")
    return "\n".join(L)
