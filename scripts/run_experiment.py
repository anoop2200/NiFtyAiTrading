"""Phase 6 experiment runner CLI.

  run      .venv\\Scripts\\python scripts\\run_experiment.py run config\\experiments\\orb_invalidation_v1.yaml --actor anoop
  rerun    .venv\\Scripts\\python scripts\\run_experiment.py rerun <experiment_id> --actor anoop   # reproducibility check
  holdout  .venv\\Scripts\\python scripts\\run_experiment.py run <spec> --actor anoop --holdout --purpose "promotion review"
  list     .venv\\Scripts\\python scripts\\run_experiment.py list
  status   .venv\\Scripts\\python scripts\\run_experiment.py holdouts

The hold-out is evaluated only with --holdout, an actor and a purpose, for the spec's frozen
`holdout_variant`; the access is logged and the hold-out retired (it can never be evaluated twice).
Results go to data/experiments/<id>/ (report.json, report.md) and NiftyTradingAI_research.db.
"""
import argparse
import json

import _bootstrap  # noqa: F401
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import settings
from app.database.db import init_db, make_engine
from app.database.models import Experiments, HoldoutAccessLog, Holdouts
from app.logging_setup import setup_logging
from app.marketdata.pipeline import history_days
from app.research.runner import run_experiment
from app.research.simulate import HistoryContext
from app.research.spec import ExperimentSpec


def main() -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("spec")
    r.add_argument("--actor", required=True)
    r.add_argument("--holdout", action="store_true")
    r.add_argument("--purpose")
    rr = sub.add_parser("rerun")
    rr.add_argument("experiment_id")
    rr.add_argument("--actor", required=True)
    sub.add_parser("list")
    sub.add_parser("holdouts")
    a = ap.parse_args()
    cfg = settings()
    setup_logging("experiments")
    research = make_engine(cfg.path("database").with_name("NiftyTradingAI_research.db"))
    init_db(research)

    if a.cmd == "list":
        with Session(research) as s:
            for e in s.scalars(select(Experiments).order_by(Experiments.started_at)):
                print(f"{e.id:60s} {e.status:9s} {e.verdict or '-':22s} variants={e.variants_tested} "
                      f"trials={e.trials_total} reproduced={e.reproduced} rerun_of={e.rerun_of or '-'}")
        return 0
    if a.cmd == "holdouts":
        with Session(research) as s:
            for h in s.scalars(select(Holdouts)):
                print(f"holdout {h.id}: {h.first_session}..{h.last_session} {h.status} inspected_by={h.inspected_by} "
                      f"exp={h.inspected_experiment}")
            for x in s.scalars(select(HoldoutAccessLog)):
                print(f"  access {x.at} holdout={x.holdout_id} actor={x.actor} granted={x.granted} "
                      f"purpose={x.purpose!r} reason={x.reason}")
        return 0

    rerun_of = None
    if a.cmd == "rerun":
        with Session(research) as s:
            e = s.get(Experiments, a.experiment_id)
            if e is None:
                raise SystemExit(f"unknown experiment {a.experiment_id}")
            spec = ExperimentSpec.from_dict(json.loads(e.spec))
        rerun_of = a.experiment_id
    else:
        spec = ExperimentSpec.load(a.spec)
    ds = cfg.get("historical_import.dataset")
    ctx = HistoryContext.load(cfg, ds, history_days(cfg.path("historical"), ds))
    rep = run_experiment(spec, research, ctx, a.actor, evaluate_holdout=getattr(a, "holdout", False),
                         holdout_purpose=getattr(a, "purpose", None), rerun_of=rerun_of)
    res = rep["results"]
    print(json.dumps({"experiment": rep["experiment_id"], "verdict": res["screen"]["verdict"],
                      "failed_gates": res["screen"]["failed"], "oos": {k: res["oos"].get(k) for k in
                      ("trades", "sessions", "net_pnl", "net_expectancy_r", "profit_factor", "max_drawdown_r")},
                      "selected": [w["selected"] for w in res["walk_forward"]], "dsr": res["dsr"].get("dsr"),
                      "trials": rep["trials_total_for_strategy_set"], "results_hash": rep["results_hash"],
                      "reproduced": rep["reproduced"], "holdout": res["holdout"] and {k: res["holdout"].get(k) for k in
                      ("trades", "net_pnl", "net_expectancy_r", "profit_factor")}}, indent=1, default=str))
    print(f"report: {rep['report_dir']}\\report.md")
    return 0 if rep["reproduced"] in (None, 1) else 2


if __name__ == "__main__":
    raise SystemExit(main())
