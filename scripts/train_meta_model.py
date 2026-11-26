r"""Phase 7 meta-model: train / evaluate / register / approve (v1.3 §6.3, §11). Offline only; nothing here trades.

  train    .venv\Scripts\python scripts\train_meta_model.py train [--horizon 15] [--db main|research]
  research .venv\Scripts\python scripts\train_meta_model.py train --db research --history   # pipeline dry run
  list     .venv\Scripts\python scripts\train_meta_model.py list [--db main|research]
  approve  .venv\Scripts\python scripts\train_meta_model.py approve --id N --actor NAME --reason "..."

By default only prospective (live-feed, `observed_feed`) labels are used. `--history` trains on history-tier
labels too; such a model is registered RESEARCH_ONLY and can never be approved, whatever its metrics.
The registry lives in the same DB as the labels; artifacts are JSON files under data/models/ (sha256 in the DB).
"""
import argparse
import json

import _bootstrap  # noqa: F401

from app.database.db import init_db, make_engine, research_db_path
from app.labels.labeller import PROSPECTIVE_TIERS
from app.ml.registry import approve, list_versions, train_and_register
from app.ml.train import GateConfig, TrainConfig
from app.research.registry import code_fingerprint


def _engine(db: str):
    e = make_engine(research_db_path() if db == "research" else None)
    init_db(e)
    return e


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    t = sub.add_parser("train")
    t.add_argument("--db", choices=["main", "research"], default="main")
    t.add_argument("--horizon", type=int, default=15)
    t.add_argument("--model-name", default="meta_v1")
    t.add_argument("--run-id", action="append", help="restrict to these TradeLabels run ids")
    t.add_argument("--history", action="store_true", help="include history-tier labels (RESEARCH_ONLY model)")
    t.add_argument("--folds", type=int, default=5)
    t.add_argument("--min-train-sessions", type=int, default=20)
    t.add_argument("--calibration", choices=["sigmoid", "isotonic"], default="sigmoid")
    l_ = sub.add_parser("list")
    l_.add_argument("--db", choices=["main", "research"], default="main")
    a = sub.add_parser("approve")
    a.add_argument("--db", choices=["main", "research"], default="main")
    a.add_argument("--id", type=int, required=True)
    a.add_argument("--actor", required=True)
    a.add_argument("--reason", required=True)
    args = ap.parse_args()
    e = _engine(args.db)
    if args.cmd == "train":
        cfg = TrainConfig(n_folds=args.folds, min_train_sessions=args.min_train_sessions,
                          calibration=args.calibration)
        out = train_and_register(e, model_name=args.model_name, horizon_min=args.horizon,
                                 tiers=None if args.history else PROSPECTIVE_TIERS,
                                 run_ids=tuple(args.run_id) if args.run_id else None, cfg=cfg,
                                 gate_cfg=GateConfig(), code_fingerprint=code_fingerprint()["source_sha256"])
        print(json.dumps(out, indent=2, default=str))
    elif args.cmd == "list":
        for r in list_versions(e):
            print(json.dumps(r, default=str))
    else:
        print(json.dumps(approve(e, args.id, args.actor, args.reason), indent=2))


if __name__ == "__main__":
    main()
