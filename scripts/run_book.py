r"""Strategy book: validation against the specification and historical replays (research tier).

  .venv\Scripts\python scripts\run_book.py validate                       # spec §7.3 reference trades
  .venv\Scripts\python scripts\run_book.py replay --from 2026-07-01 --to 2026-10-16 [--persist]
  .venv\Scripts\python scripts\run_book.py strategies                     # names, codes, rules

The live forward test runs inside the paper engine (config/paper.yaml `book:`); this script replays the SAME
BookEngine over the NIFTY 1-minute store and the Dhan expired-option history (ATM +/- 5 strikes only, trade prices,
no bid/ask - research tier: it can miss strikes the specification's data had, and a strike that drifts outside
the imported range loses its bars, delaying exits). --persist writes run `book_replay` to the RESEARCH database.
"""
import argparse
import json
import sys
from pathlib import Path

import _bootstrap  # noqa: F401
import pandas as pd

from app.book import replay as R
from app.book.strategies import STRATEGIES
from app.config import PROJECT_ROOT, settings

# spec §7.3: (code, date, side, strike, entry, exit, entry_px, exit_px, reason, net)
REFERENCE = [
    ("Q4", "2026-10-12", "CE", 22450, "09:18", "09:22", 233.40, 198.39, "SL", -2361),
    ("U1", "2026-10-12", "CE", 22450, "09:18", "09:22", 233.40, 198.39, "SL", -2361),
    ("PB", "2026-10-14", "PE", None, "13:06", "13:48", None, None, "ST_FLIP", -2571),
    ("Q1", "2026-09-29", "CE", 22500, "11:15", "12:22", 210.95, 228.22, "TRAIL_SL", 1038),
    ("Q2", "2026-09-25", "CE", 22950, "14:03", "15:15", 240.25, 266.55, "SQUARE_OFF", 1625),
    ("Q3", "2026-09-25", "CE", 23150, "14:05", "14:09", 148.60, 126.31, "SL", -1534),
    ("Q5", "2026-09-09", "PE", 23750, "09:16", "10:17", 252.25, 249.40, "ST_FLIP", -270),
    ("Q6", "2026-09-24", "PE", 23350, "13:27", "14:36", 240.40, 290.05, "ST_FLIP", 3142),
    ("Q7", "2026-08-19", "PE", 24300, "09:20", "11:53", 214.75, 221.34, "TRAIL_SL", 343),
    ("Q8", "2026-08-12", "CE", 24400, "14:55", "15:15", 152.85, 145.10, "SQUARE_OFF", -589),
    # N1-N8 specification §7 (N4, N7, N8 fall before the imported option history starts on 2025-11-26)
    ("N1", "2026-10-15", "CE", 22700, "09:18", "14:33", 134.45, 338.00, "ST_FLIP", 13146),
    ("N2", "2026-06-29", "PE", 24250, "09:35", "09:40", 239.60, 203.66, "SL", -2421),
    ("N3", "2026-10-15", "CE", 22700, "09:30", "14:50", 160.00, 302.65, "ST_FLIP", 9187),
    ("N4", "2025-07-24", "PE", 25250, "11:57", "12:16", 163.80, 203.11, "TRAIL_SL", 2470),
    ("N5", "2026-09-15", "PE", 23600, "09:55", "15:15", 203.35, 398.30, "SQUARE_OFF", 12587),
    ("N6", "2026-08-14", "CE", 24250, "13:50", "15:15", 208.45, 202.00, "SQUARE_OFF", -504),
    ("N7", "2025-07-30", "CE", 24650, "14:00", "15:15", 237.40, 231.80, "SQUARE_OFF", -449),
    ("N8", "2025-10-06", "CE", 24800, "09:42", "13:51", 156.70, 284.10, "ST_FLIP", 8196),
]


def cmd_validate(a) -> int:
    root = settings().path("historical")
    index = R.load_index(root)
    rows, ok_all = [], True
    only = set(a.codes.split(",")) if getattr(a, "codes", None) else None
    for code, day, side, strike, e_t, x_t, e_px, x_px, reason, net in REFERENCE:
        if only and code not in only:
            continue
        opts = R.load_options(root, day)
        if opts.empty:
            rows.append({"code": code, "name": STRATEGIES[code].name, "date": day, "match": None,
                         "spec": f"{strike} {e_t}->{x_t} {e_px}->{x_px} {reason} {net}", "ours": "-",
                         "note": "not checkable: no imported option history for this day"})
            continue
        e = R.run_day(day, index, opts, codes=[code])
        t = e.trades[0] if e.trades else None
        got = {"strike": None, "entry": None, "exit": None, "entry_px": None, "exit_px": None, "reason": None,
               "net": None}
        if t:
            got = {"strike": t["strike"], "entry": t["entry_ts"][11:16], "exit": t["exit_ts"][11:16],
                   "entry_px": t["entry_px"], "exit_px": t["exit_px"], "reason": t["exit_reason"],
                   "net": round(t["net_pnl"])}
        match = bool(t) and (strike is None or got["strike"] == strike) and got["entry"] == e_t and \
            got["exit"] == x_t and got["reason"] == reason and abs(got["net"] - net) <= 1 and \
            (e_px is None or abs(got["entry_px"] - e_px) <= 0.05) and (x_px is None or abs(got["exit_px"] - x_px) <= 0.05)
        ok_all &= match
        note = ""
        if not t:
            sig = [f"{s['ts'][11:16]} {s['outcome']} {s.get('reason') or ''}".strip() for s in e.signals]
            idx_bars = index[index["session"] == day]
            first = pd.Timestamp(int(idx_bars["ts"].min()), unit="s", tz="Asia/Kolkata").strftime("%H:%M") \
                if len(idx_bars) else None
            note = f"no trade; signals {sig}; first index bar {first}"
            if first and first > "09:15" and e_t <= "09:20":
                note += " (the 09:15 bar is missing from the index store - refetch with backfill_vix.py --nifty)"
        elif not match and strike is not None and got["strike"] != strike:
            note = "different strike chosen: the imported option history (ATM +/- 5) differs from the spec's data"
        elif not match and got["entry"] == e_t and got["exit"] > x_t:
            k = opts[opts["key"] == t["contract"]]
            mins = {pd.Timestamp(int(x), unit="s", tz="Asia/Kolkata").strftime("%H:%M") for x in k["ts"]}
            if x_t not in mins:
                note = (f"same entry; the contract has no imported bar at the spec's exit {x_t} (strike drifted "
                        f"outside the imported ATM +/- 5), so the exit is delayed")
        rows.append({"code": code, "name": STRATEGIES[code].name, "date": day, "match": match,
                     "spec": f"{strike or ''} {e_t}->{x_t} {e_px or ''}->{x_px or ''} {reason} {net}",
                     "ours": f"{got['strike'] or ''} {got['entry']}->{got['exit']} {got['entry_px']}->{got['exit_px']} "
                             f"{got['reason']} {got['net']}" if t else "-", "note": note})
    df = pd.DataFrame(rows)
    with pd.option_context("display.max_colwidth", 120, "display.width", 250):
        print(df.to_string(index=False))
    chk = df[df["match"].notna()]
    print(f"\n{int(chk['match'].astype(bool).sum())} of {len(chk)} checkable reference trades reproduced exactly"
          f" ({len(df) - len(chk)} not checkable)")
    return 0 if ok_all else 1


def summarize(trades: pd.DataFrame) -> pd.DataFrame:
    out = []
    for code, g in trades.groupby("code", sort=False):
        w, lo = g[g.net_pnl > 0], g[g.net_pnl <= 0]
        eq = g.sort_values("exit_ts").net_pnl.cumsum()
        out.append({"code": code, "strategy": STRATEGIES[code].name, "trades": len(g),
                    "win_rate": round(len(w) / len(g), 3), "pf": round(w.net_pnl.sum() / -lo.net_pnl.sum(), 2)
                    if len(lo) and lo.net_pnl.sum() < 0 else None,
                    "net": round(g.net_pnl.sum()), "max_dd": round((eq - eq.cummax().clip(lower=0)).min()),
                    "avg_win": round(w.net_pnl.mean()) if len(w) else None,
                    "avg_loss": round(lo.net_pnl.mean()) if len(lo) else None})
    return pd.DataFrame(out).set_index("code").reindex([c for c in STRATEGIES if c in set(trades["code"])])


def cmd_replay(a) -> int:
    from app.historical import rolling_options as ro
    cfg = settings()
    root = cfg.path("historical")
    m = ro.read_manifest(root)
    days = sorted(d for d, v in m["days"].items() if v.get("status") == "ok"
                  and (not a.d_from or d >= a.d_from) and (not a.d_to or d <= a.d_to))
    codes = a.codes.split(",") if a.codes else list(STRATEGIES)

    def prog(i, n, day, e):
        if i % 20 == 0 or i == n:
            print(f"  {i}/{n} {day}: {len(e.trades)} trades", file=sys.stderr)
    trades, signals = R.run_days(days, root, codes, progress=prog)
    out = PROJECT_ROOT / "data" / "backtests" / "book"
    out.mkdir(parents=True, exist_ok=True)
    tag = f"{days[0]}_{days[-1]}" if days else "none"
    tdf = pd.DataFrame(trades)
    if tdf.empty:
        print("no trades")
        return 0
    tdf.drop(columns=["features", "cost", "trail"]).to_csv(out / f"trades_{tag}.csv", index=False)
    pd.DataFrame(signals).drop(columns=["features"]).to_csv(out / f"signals_{tag}.csv", index=False)
    s = summarize(tdf)
    print(s.to_string())
    allg = tdf.net_pnl
    print(f"\nAll: {len(tdf)} trades · win {float((allg > 0).mean()):.0%} · PF "
          f"{allg[allg > 0].sum() / -allg[allg <= 0].sum():.2f} · net ₹{allg.sum():,.0f} · days {len(days)}")
    print(f"files: {out}")
    if a.persist:
        from app.book.persist import persist_day
        from app.database.db import init_db, make_engine, research_db_path
        db = make_engine(research_db_path())
        init_db(db)
        for day in days:
            persist_day(db, "book_replay", day, [t for t in trades if t["session"] == day],
                        [x for x in signals if x["session"] == day], mode="backtest")
        print("persisted run book_replay to the research database")
    return 0


def cmd_strategies(a) -> int:
    for s in STRATEGIES.values():
        print(json.dumps({"code": s.code, "id": s.sid, "name": s.name, "chart": f"{s.tf} min", "side": s.side,
                          "premium": f"{s.target:g} ({s.band[0]:g}-{s.band[1]:g})", "exit": s.exit,
                          "rule": s.summary}, ensure_ascii=False))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    va = sub.add_parser("validate")
    va.add_argument("--codes", help="comma-separated codes (default all)")
    rp = sub.add_parser("replay")
    rp.add_argument("--from", dest="d_from")
    rp.add_argument("--to", dest="d_to")
    rp.add_argument("--codes", help="comma-separated codes (default all)")
    rp.add_argument("--persist", action="store_true")
    sub.add_parser("strategies")
    a = ap.parse_args()
    return {"validate": cmd_validate, "replay": cmd_replay, "strategies": cmd_strategies}[a.cmd](a)


if __name__ == "__main__":
    raise SystemExit(main())
