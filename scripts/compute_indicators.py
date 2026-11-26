"""Compute the configured indicator bindings (config/indicators.yaml) over the imported history.

Streams: index = reconstructed NIFTY spot bars, chain = per-minute ATM-band chain bars (both
tier reconstructed_from_snapshots). Bindings whose source requirement is not met are skipped and logged.
Output: data/derived/indicators/date=YYYY-MM-DD/part-0.parquet (long format) + an IndicatorRuns row.

Usage: .venv\\Scripts\\python scripts\\compute_indicators.py [--from D] [--to D] [--verify-replay]
"""
import argparse
import json

import _bootstrap  # noqa: F401
import pandas as pd
import yaml

from app.clock import iso
from app.config import PROJECT_ROOT as ROOT, settings
from app.database.db import init_db, make_engine
from app.database.models import IndicatorRuns
from app.indicators.datasets import pg_chain_stream, pg_spot_stream
from app.indicators.engine import IndicatorEngine, binding, check_source, output_hash
from app.indicators.registry import sync_definitions
from app.logging_setup import setup_logging
from app.marketdata.pipeline import _sha, history_days
from sqlalchemy.orm import Session

TIER = "reconstructed_from_snapshots"


def make_bindings(cfg_path):
    spec = yaml.safe_load(open(cfg_path, encoding="utf-8"))["bindings"]
    return [binding(b["id"], b["stream"], b.get("tf_min", 1), b.get("version"), **(b.get("params") or {}))
            for b in spec]


def to_long(rows: pd.DataFrame) -> pd.DataFrame:
    meta = {"ts", "binding", "state", "valid"}
    val_cols = [c for c in rows.columns if c not in meta]
    long = rows.melt(id_vars=["ts", "binding", "state", "valid"], value_vars=val_cols, var_name="field",
                     value_name="value").dropna(subset=["value"])
    long["value"] = long["value"].astype(float)
    long["valid"] = long["valid"].astype(bool)
    return long.sort_values(["binding", "field", "ts"]).reset_index(drop=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="d_from")
    ap.add_argument("--to", dest="d_to")
    ap.add_argument("--verify-replay", action="store_true", help="run twice and require identical output hash")
    a = ap.parse_args()
    cfg = settings()
    setup_logging("indicators")
    engine = make_engine()
    init_db(engine)
    sync_definitions(engine)
    ds = cfg.get("historical_import.dataset")
    days = [d for d in history_days(cfg.path("historical"), ds)
            if (not a.d_from or d >= a.d_from) and (not a.d_to or d <= a.d_to)]
    streams = {"index": pg_spot_stream(cfg.path("derived"), days),
               "chain": pg_chain_stream(cfg.path("historical"), ds, days, cfg.path("derived") / "pg_chain_bars_1m")}
    cfg_path = ROOT / "config" / "indicators.yaml"

    def run_once():
        bs, skipped = [], {}
        for b in make_bindings(cfg_path):
            df = streams.get(b.stream)
            why = f"no '{b.stream}' stream in this dataset" if df is None or df.empty else \
                check_source(b, TIER, set(df.columns))
            if why:
                skipped[b.key] = why
            else:
                bs.append(b)
        return IndicatorEngine(bs).run({k: v for k, v in streams.items() if not v.empty}), bs, skipped

    rows, bs, skipped = run_once()
    h = output_hash(rows)
    checks = {}
    if a.verify_replay:
        rows2, _, _ = run_once()
        checks["deterministic_replay"] = {"hash_1": h, "hash_2": output_hash(rows2), "passed": h == output_hash(rows2)}
    long = to_long(rows)
    long["date"] = pd.to_datetime(long["ts"], unit="s", utc=True).dt.tz_convert("Asia/Kolkata").dt.date.astype(str)
    out_root = cfg.path("derived") / "indicators"
    for d, part in long.groupby("date"):
        p = out_root / f"date={d}"
        p.mkdir(parents=True, exist_ok=True)
        part.drop(columns="date").to_parquet(p / "part-0.parquet", index=False)
    summary = (rows.groupby("binding")["valid"].agg(["size", "sum"]).rename(columns={"size": "rows", "sum": "valid"})
               .reset_index().to_dict("records"))
    manifest = out_root / f"manifest_{days[0]}_{days[-1]}.json"
    manifest.write_text(json.dumps({"days": [days[0], days[-1], len(days)], "output_hash": h, "skipped": skipped,
                                    "bindings": summary, "checks": checks}, indent=2, default=str), encoding="utf-8")
    with Session(engine) as s:
        s.add(IndicatorRuns(run_at=iso(), dataset=ds, date_from=days[0], date_to=days[-1],
                            bindings=json.dumps([b.key for b in bs]), skipped=json.dumps(skipped),
                            output_path=str(out_root), output_sha256=_sha(manifest), output_hash=h,
                            checks=json.dumps(checks)))
        s.commit()
    print(json.dumps({"days": len(days), "bindings_run": len(bs), "skipped": skipped, "rows": len(rows),
                      "output_hash": h[:16], "checks": checks}, indent=2, default=str))
    return 0 if all(c.get("passed", True) for c in checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
