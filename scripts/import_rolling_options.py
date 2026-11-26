r"""Import expired NIFTY option history from Dhan (OI-03) into a RESEARCH-ONLY data tier.

  .venv\Scripts\python scripts\import_rolling_options.py fetch --from 2021-09-01 --to 2026-11-25   # resumable
  .venv\Scripts\python scripts\import_rolling_options.py finalize      # infer + check expiries
  .venv\Scripts\python scripts\import_rolling_options.py coverage
  .venv\Scripts\python scripts\import_rolling_options.py validate      # vs PG snapshot history (overlap days)

Read-only historical data API, one request per (day, series); default ATM±5 CE+PE of the near weekly expiry
= 22 requests per weekday at <= 4 requests/s. Days already in the manifest are skipped, so an interrupted run
just continues. Data: data/historical/dhan_rolling_options/date=D/part-0.parquet (gitignored).
Trade prices only (no bid/ask): never a substitute for the quote-based backtest (v1.3 §18).
"""
import argparse
import asyncio
import json
from datetime import date

import _bootstrap  # noqa: F401
import pandas as pd

from app.config import settings
from app.data.dhan_rest import DhanAuthError, DhanRest
from app.historical import rolling_options as ro
from app.marketdata.pipeline import history_days
from app.secrets import get_secret


async def fetch(cfg, days: list[str], spec: ro.RollingSpec, interval_s: float, refetch: bool) -> None:
    hist = cfg.path("historical")
    m = ro.read_manifest(hist)
    todo = [d for d in days if refetch or d not in m["days"] or m["days"][d].get("status") == "error"]
    print(json.dumps({"weekdays": len(days), "to_fetch": len(todo), "requests_per_day": len(spec.series())}))
    rest = DhanRest(cfg.get("dhan.rest_base"), get_secret("DHAN_CLIENT_ID"), get_secret("DHAN_ACCESS_TOKEN"),
                    quote_interval_s=interval_s)
    try:
        for i, day in enumerate(todo, 1):
            df, info = await ro.fetch_day(rest, day, spec)
            if len(df):
                ro.write_day(hist, day, df)
            m["days"][day] = info
            ro.write_manifest(hist, m)
            if i % 10 == 0 or info["status"] != "ok":
                print(json.dumps({"n": i, "of": len(todo), "day": day, "status": info["status"],
                                  "rows": info["rows"], "errors": info["errors"][:1]}), flush=True)
            if info["status"] == "error" and all(e.get("http") == 429 for e in info["errors"]):
                await asyncio.sleep(30)
    except DhanAuthError as e:
        print(json.dumps({"stopped": str(e)}))
    finally:
        await rest.close()


def validate(cfg) -> dict:
    """Overlap with the PG snapshot history: expiry inference and price agreement."""
    hist, derived = cfg.path("historical"), cfg.path("derived")
    pg_days = history_days(hist, cfg.get("historical_import.dataset"))
    out, diffs = [], []
    for day in pg_days:
        df = ro.load_day(hist, day)
        p = derived / "pg_option_bars_1m" / f"date={day}" / "part-0.parquet"
        if df.empty or not p.exists():
            continue
        op = pd.read_parquet(p)
        op["tsec"] = pd.to_datetime(op["ts"]).map(lambda t: int(t.timestamp()))
        op["expiry"] = op["expiry"].astype(str).str[:10]
        near = sorted(op["expiry"].unique())[0]
        w = df[(df["expiry_flag"] == "WEEK") & (df["expiry_code"] == 1)]
        inferred = w["expiry"].iloc[0] if len(w) else None
        mm = w.merge(op.rename(columns={"close": "pg_close"})[["tsec", "strike", "option_type", "expiry", "pg_close",
                                                               "bid", "ask"]],
                     left_on=["ts", "strike", "option_type", "expiry"],
                     right_on=["tsec", "strike", "option_type", "expiry"])
        rel = ((mm["close"] - mm["pg_close"]).abs() / mm["pg_close"]).dropna()
        diffs += rel.tolist()
        out.append({"day": day, "pg_near_expiry": near, "inferred": inferred, "match": near == inferred,
                    "common_bars": int(len(mm)), "median_rel_diff_pct": round(100 * float(rel.median()), 2)
                    if len(rel) else None})
    s = pd.Series(diffs)
    return {"days": len(out), "expiry_matches": sum(o["match"] for o in out),
            "mismatches": [o for o in out if not o["match"]][:10],
            "price_rel_diff_pct": {"median": round(100 * s.median(), 2), "p95": round(100 * s.quantile(.95), 2)}
            if len(s) else None, "per_day": out}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fetch")
    f.add_argument("--from", dest="d_from", required=True)
    f.add_argument("--to", dest="d_to", required=True)
    f.add_argument("--each-side", type=int, default=5)
    f.add_argument("--flags", default="WEEK", help="comma list: WEEK,MONTH")
    f.add_argument("--codes", default="1", help="comma list: 1,2")
    f.add_argument("--interval-s", type=float, default=0.25, help="seconds between requests (Data APIs: 5/s)")
    f.add_argument("--refetch", action="store_true")
    sub.add_parser("finalize")
    sub.add_parser("coverage")
    sub.add_parser("validate")
    a = ap.parse_args()
    cfg = settings()
    if a.cmd == "fetch":
        if not 0 <= a.each_side <= 10:
            raise SystemExit("--each-side must be 0..10 (API limit near expiry)")
        spec = ro.RollingSpec(flags=tuple(a.flags.split(",")), codes=tuple(int(x) for x in a.codes.split(",")),
                              each_side=a.each_side)
        days = ro.weekdays(date.fromisoformat(a.d_from), date.fromisoformat(a.d_to))
        asyncio.run(fetch(cfg, days, spec, a.interval_s, a.refetch))
        print(json.dumps(ro.coverage(cfg.path("historical"))))
    elif a.cmd == "finalize":
        print(json.dumps(ro.finalize(cfg.path("historical")), default=str))
    elif a.cmd == "coverage":
        print(json.dumps(ro.coverage(cfg.path("historical"))))
    else:
        r = validate(cfg)
        r.pop("per_day")
        print(json.dumps(r, indent=1, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
