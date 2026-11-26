r"""Strategy PRE-SCREEN on a year of Dhan rolling-option trade prices (research tier; not promotion evidence).

  .venv\Scripts\python scripts\screen_strategies.py bias                      # screen vs quote backtest, 59 PG days
  .venv\Scripts\python scripts\screen_strategies.py run [--from D] [--to D] [--multipliers 1,2,4]

Quotes are MODELLED from 1-minute trade prices with the PG-calibrated spread (x multiplier); see
app/screen/ltp.py. Reports go to data/reports/screen_<stamp>/ (report.json + report.md). Nothing is written
to any database, no trial is consumed and no hold-out is touched.
"""
import argparse
import json
from datetime import datetime

import _bootstrap  # noqa: F401
import pandas as pd

from app.clock import IST
from app.config import settings
from app.historical import rolling_options as ro
from app.marketdata.pipeline import history_days
from app.research.simulate import HistoryContext, simulate
from app.research.spec import build_strategies
from app.screen import ltp


def strategies():
    return build_strategies(("orb", "pullback"), {}, {"require_futures": False})


def rolling_days(cfg, d_from=None, d_to=None) -> list[str]:
    m = ro.read_manifest(cfg.path("historical"))
    return [d for d in sorted(m["days"]) if m["days"][d].get("status") == "ok"
            and (not d_from or d >= d_from) and (not d_to or d <= d_to)]


def _out_dir(cfg, kind: str):
    out = cfg.path("reports") / f"screen_{kind}_{datetime.now(IST).strftime('%Y%m%d-%H%M%S')}"
    out.mkdir(parents=True, exist_ok=True)
    return out


def cmd_run(cfg, a) -> dict:
    days = rolling_days(cfg, a.d_from, a.d_to)
    model = ltp.calibrate_spreads(cfg.path("derived"))
    res = {"kind": "screen", "tier": ltp.TIER, "not_evidence": "research pre-screen: modelled spreads on trade "
           "prices; cannot approve, promote or consume a trial (v1.3 §18)", "spread_model": model,
           "strategies": ["orb", "pullback"], "runs": {}}
    for mult in [float(x) for x in a.multipliers.split(",")]:
        ctx = ltp.ScreenContext.build(cfg, days, model, mult)
        r = ltp.run_screen(ctx, strategies)
        res["runs"][f"{mult:g}x"] = {"fingerprint": ctx.data_fingerprint(), "audit_ok": r["audit_ok"],
                                     "audit_hash": r["audit_hash"], "signals": r["signals"], **r["summary"]}
        res.setdefault("lots", r["lots"])
        print(json.dumps({"spread": f"{mult:g}x", **{k: r["summary"].get(k) for k in
                          ("sessions", "trades", "net_pnl", "win_rate", "profit_factor", "max_drawdown")}}))
    return res


def cmd_bias(cfg, a) -> dict:
    """Same days, same code: quote-based backtest (observed bid/ask) vs the screen (modelled quotes)."""
    ds = cfg.get("historical_import.dataset")
    overlap = sorted(set(history_days(cfg.path("historical"), ds)) & set(rolling_days(cfg)))
    model = ltp.calibrate_spreads(cfg.path("derived"))
    hctx = HistoryContext.load(cfg, ds, overlap)
    real = simulate(hctx, {"real": strategies()})["real"]
    out = {"kind": "bias_check", "days": len(overlap), "first": overlap[0], "last": overlap[-1],
           "quote_backtest": ltp.summarize(real.trades, real.signals, len(overlap)), "screen": {}}
    rt = pd.DataFrame(real.trades)
    for mult in (1.0, 2.0):
        sctx = ltp.ScreenContext.build(cfg, overlap, model, mult)
        r = ltp.run_screen(sctx, strategies)
        st = pd.DataFrame(r["trades"])
        both = set(zip(rt.get("session", []), rt.get("cycle", []))) & set(zip(st.get("session", []),
                                                                             st.get("cycle", [])))
        out["screen"][f"{mult:g}x"] = {**r["summary"], "same_cycles_traded": len(both),
                                       "only_in_backtest": len(rt) - len(both), "only_in_screen": len(st) - len(both)}
        print(json.dumps({"spread": f"{mult:g}x", "screen_net": r["summary"].get("net_pnl"),
                          "screen_trades": r["summary"].get("trades"), "backtest_net": out["quote_backtest"]["net_pnl"],
                          "backtest_trades": out["quote_backtest"]["trades"], "same_cycles": len(both)}))
    return out


def markdown(res: dict) -> str:
    lines = [f"# Strategy pre-screen ({res['kind']})", "",
             "**Research tier — modelled spreads on Dhan 1-minute trade prices. Not evidence for promotion "
             "(v1.3 §18).**", ""]
    if res["kind"] == "bias_check":
        q = res["quote_backtest"]
        lines += [f"Days: {res['days']} ({res['first']} .. {res['last']})", "",
                  "| source | trades | net ₹ | win rate | profit factor | same cycles as backtest |",
                  "|---|---|---|---|---|---|",
                  f"| quote backtest (observed bid/ask) | {q['trades']} | {q['net_pnl']:,.0f} | {q.get('win_rate')} | "
                  f"{q.get('profit_factor')} | — |"]
        for k, s in res["screen"].items():
            lines.append(f"| screen {k} spread | {s['trades']} | {s['net_pnl']:,.0f} | {s.get('win_rate')} | "
                         f"{s.get('profit_factor')} | {s['same_cycles_traded']} |")
        return "\n".join(lines) + "\n"
    lines += ["| spread | sessions | trades | net ₹ | win rate | profit factor | max DD ₹ | +days / −days |",
              "|---|---|---|---|---|---|---|---|"]
    for k, s in res["runs"].items():
        lines.append(f"| {k} | {s['sessions']} | {s['trades']} | {s['net_pnl']:,.0f} | {s.get('win_rate')} | "
                     f"{s.get('profit_factor')} | {s.get('max_drawdown', 0):,.0f} | "
                     f"{s.get('positive_days')} / {s.get('negative_days')} |")
    base = res["runs"].get("1x") or next(iter(res["runs"].values()))
    for title, key in (("By strategy (1x)", "by_strategy"), ("By VIX at entry, exploratory (1x)", "by_vix_tercile")):
        lines += ["", f"## {title}", "", "| | trades | net ₹ | win rate |", "|---|---|---|---|"]
        for k, v in (base.get(key) or {}).items():
            lines.append(f"| {k} | {v['trades']} | {v['net_pnl']:,.0f} | {v['win_rate']} |")
    lines += ["", "## By month (1x)", "", "| month | net ₹ |", "|---|---|"]
    lines += [f"| {k} | {v:,.0f} |" for k, v in (base.get("by_month") or {}).items()]
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--from", dest="d_from")
    r.add_argument("--to", dest="d_to")
    r.add_argument("--multipliers", default="1,2,4")
    sub.add_parser("bias")
    a = ap.parse_args()
    cfg = settings()
    res = cmd_run(cfg, a) if a.cmd == "run" else cmd_bias(cfg, a)
    out = _out_dir(cfg, a.cmd)
    (out / "report.json").write_text(json.dumps(res, indent=1, default=str), encoding="utf-8")
    (out / "report.md").write_text(markdown(res), encoding="utf-8")
    print(f"report: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
