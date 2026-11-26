"""Phase 3 acceptance: validate every registered indicator and record the report in the registry.

Checks per indicator: batch == stream (independent vectorised implementation where one exists),
deterministic replay, no look-ahead (perturb future bars), restart from JSON snapshot, and warm-up
replay for recovery='replay' indicators. Passing indicators move DRAFT -> VALIDATED; approval for
production remains a separate, explicit human action (scripts/set_indicator_status.py).

Data: real history (index + chain streams, one option contract). Futures-stream indicators have no
futures history yet, so they are validated on a clearly labelled synthetic stream.
Also compares internal BSM IV with the provider IV stored in optionchain_db.

Usage: .venv\\Scripts\\python scripts\\validate_indicators.py [--days 10]
"""
import argparse
import json

import _bootstrap  # noqa: F401
import numpy as np
import pandas as pd

from app.config import settings
from app.database.db import init_db, make_engine
from app.indicators.datasets import pg_chain_stream, pg_option_stream, pg_spot_stream
from app.indicators.registry import load_all, set_status, sync_definitions
from app.indicators.validation import validate
from app.logging_setup import setup_logging
from app.marketdata.pipeline import history_days
from app.pricing.bs import ASSUMPTIONS, implied_vol, premium_for_iv, year_fraction


def synthetic_futures(n_days: int = 3, seed: int = 11) -> pd.DataFrame:
    """SYNTHETIC test stream (never stored or used as market data)."""
    rng = np.random.default_rng(seed)
    rows, px = [], 26000.0
    for d in range(n_days):
        day = (pd.Timestamp("2026-11-02") + pd.Timedelta(days=d)).date()
        open_ts = int(pd.Timestamp(f"{day} 09:15", tz="Asia/Kolkata").timestamp())
        for m in range(375):
            o = px
            px *= float(np.exp(rng.normal(0, 0.0006)))
            hi, lo = max(o, px) * (1 + abs(rng.normal(0, 2e-4))), min(o, px) * (1 - abs(rng.normal(0, 2e-4)))
            rows.append({"ts": open_ts + 60 * m, "session": str(day), "session_open_ts": open_ts, "open": o,
                         "high": hi, "low": lo, "close": px, "volume": int(rng.lognormal(8, 0.6)),
                         "tier": "synthetic"})
    return pd.DataFrame(rows)


def iv_check(chain: pd.DataFrame) -> dict:
    """Internal BSM IV vs provider IV (provider values in % in optionchain_db)."""
    diffs = {f"{cp}_{b}": [] for cp in ("CE", "PE") for b in ("0dte", "1plus")}
    for r in chain.to_dict("records"):
        obs = pd.Timestamp(r["ts"] + 60, unit="s", tz="Asia/Kolkata").to_pydatetime()
        t = year_fraction(obs, pd.Timestamp(r["expiry"]).date())
        for cp in ("CE", "PE"):
            g = lambda k: None if pd.isna(r.get(k)) else r.get(k)  # noqa: E731
            prov = g(f"provider_iv_{cp.lower()}")
            px, _ = premium_for_iv(g(f"atm_{cp.lower()}_ltp"), g(f"atm_{cp.lower()}_bid"), g(f"atm_{cp.lower()}_ask"))
            if not prov or px is None:
                continue
            iv = implied_vol(px, r["spot"], r["atm_strike"], t, cp)
            if iv is not None:
                diffs[f"{cp}_{'0dte' if t * 365 < 1 else '1plus'}"].append(100 * iv - prov)
    out = {"assumptions": ASSUMPTIONS.__dict__,
           "note": "0dte = under one calendar day to the 15:30 expiry. Large 0DTE gaps point to a different "
                   "time-to-expiry convention at the provider (open item); ours is calendar seconds to 15:30."}
    for cp, d in diffs.items():
        a = np.array(d)
        out[cp] = {"n": len(a), "median_diff_vol_pts": round(float(np.median(a)), 3) if len(a) else None,
                   "median_abs_diff_vol_pts": round(float(np.median(np.abs(a))), 3) if len(a) else None,
                   "p90_abs_diff_vol_pts": round(float(np.percentile(np.abs(a), 90)), 3) if len(a) else None}
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=10, help="most recent history days to validate on")
    a = ap.parse_args()
    cfg = settings()
    setup_logging("validate_indicators")
    engine = make_engine()
    init_db(engine)
    sync_definitions(engine)
    ds = cfg.get("historical_import.dataset")
    days = history_days(cfg.path("historical"), ds)[-a.days:]
    index = pg_spot_stream(cfg.path("derived"), days)
    chain = pg_chain_stream(cfg.path("historical"), ds, days, cfg.path("derived") / "pg_chain_bars_1m")
    last = chain[chain["session"] == days[-1]].iloc[0]
    option = pg_option_stream(cfg.path("derived"), days[-1], last["expiry"], float(last["atm_strike"]), "CE")
    streams = {"index": (index, "history"), "chain": (chain, "history"), "option": (option, "history: 1 contract"),
               "future": (synthetic_futures(), "SYNTHETIC")}
    report = {"days": [days[0], days[-1]], "indicators": {}}
    ok = True
    for iid, versions in sorted(load_all().items()):
        for ver, cls in sorted(versions.items()):
            df, label = streams[cls.SPEC.source.stream]
            rep = validate(cls, df)
            rep["data"] = label
            report["indicators"][f"{iid}@{ver}"] = rep
            ok &= rep["passed"]
            real = label != "SYNTHETIC"
            set_status(engine, iid, ver, "VALIDATED" if rep["passed"] and real else "DRAFT",
                       actor="validate_indicators.py",
                       validation={**rep, "passed": rep["passed"] and label != "SYNTHETIC",
                                   "note": "synthetic data: needs real futures history before approval"
                                   if label == "SYNTHETIC" else ""})
            print(f"{iid}@{ver:<6} {'PASS' if rep['passed'] else 'FAIL'}  rows={rep['rows']:<6} "
                  f"vectorised={rep['batch_vs_stream']['vectorised']!s:<5} data={label}")
    report["iv_check"] = iv_check(chain)
    print("IV internal vs provider:", json.dumps({k: v for k, v in report["iv_check"].items() if k != "assumptions"}))
    out = cfg.path("reports") / "indicator_validation.json"
    out.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(f"{'ALL PASSED' if ok else 'FAILURES'} - {out}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
