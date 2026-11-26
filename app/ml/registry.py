"""Meta-model registry (v1.3 §6.3, §11): training runs, versioned artifacts, out-of-fold predictions, approval.

Status rules (enforced here, not by convention):
  * RESEARCH_ONLY — trained on anything but prospective (live-feed) labels, or the promotion gate failed.
    Never approvable, whatever the metrics say.
  * CANDIDATE     — every gate check passed; waits for an operator decision.
  * APPROVED      — an operator approved a CANDIDATE (audited in the operator log). Only one APPROVED version
    per model name; approving a new one RETIRES the previous one.
  * RETIRED       — superseded or withdrawn.

Nothing in the trading path reads this registry yet: `PipelineConfig.meta_label` stays rule-only until an
APPROVED model exists AND the shadow-mode wiring (a later step) is built and reviewed.
"""
from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.config import PROJECT_ROOT
from app.database.models import ModelPredictions, ModelTrainingRuns, ModelVersions
from app.labels.labeller import PROSPECTIVE_TIERS, adequacy
from app.ml import dataset as ds
from app.ml.train import TARGET, GateConfig, TrainConfig, artifact, evaluate, fit_calibrated, promotion_gate
from app.clock import iso

MODELS_DIR = PROJECT_ROOT / "data" / "models"


class RegistryError(PermissionError):
    pass


def _sha256(path: Path) -> str:
    import hashlib
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def train_and_register(engine: Engine, *, model_name: str = "meta_v1", horizon_min: int = 15,
                       tiers: tuple[str, ...] | None = PROSPECTIVE_TIERS, run_ids: tuple[str, ...] | None = None,
                       cfg: TrainConfig = TrainConfig(), gate_cfg: GateConfig = GateConfig(),
                       models_dir: Path = MODELS_DIR, code_fingerprint: str | None = None) -> dict:
    labels = ds.load_labels(engine, horizon_min, tiers, run_ids)
    if labels.empty:
        raise ValueError(f"no resolved labels for horizon {horizon_min} (tiers={tiers}, runs={run_ids})")
    X, _, meta = ds.build(labels)
    leak = ds.leakage_report(X)
    if not leak["ok"]:
        raise ValueError(f"leakage guard: forbidden features {leak['forbidden_features']}")
    data_tiers = sorted(meta["data_tier"].unique().tolist())
    prospective = all(t in PROSPECTIVE_TIERS for t in data_tiers)
    adq = adequacy(labels.to_dict("records"), horizon_min=horizon_min)
    ev = evaluate(X, meta, cfg)
    gate = promotion_gate(adequacy=adq, prospective_only=prospective, leakage=leak,
                          metrics=ev.get("metrics"), cfg=gate_cfg)
    sessions = sorted(meta["session_date"].unique().tolist())
    with Session(engine) as s:
        run = ModelTrainingRuns(
            created_at=iso(), model_name=model_name, horizon_min=horizon_min, target=TARGET,
            data_tiers=json.dumps(data_tiers), label_runs=json.dumps(sorted(meta["run_id"].unique().tolist())),
            dataset_fingerprint=ds.fingerprint(labels), rows=len(meta), sessions=len(sessions),
            first_session=sessions[0], last_session=sessions[-1],
            params=json.dumps({"train": cfg.__dict__, "gate": gate_cfg.__dict__}, default=str),
            folds=json.dumps(ev["folds"], default=str),
            metrics=json.dumps(ev.get("metrics") or {"error": ev.get("reason")}, default=str),
            leakage=json.dumps(leak | {"adequacy": adq}, default=str), gate=json.dumps(gate, default=str),
            code_fingerprint=code_fingerprint)
        s.add(run)
        s.flush()
        version = (s.scalar(select(func.max(ModelVersions.version)).where(ModelVersions.model_name == model_name))
                   or 0) + 1
        # final model: same recipe as one fold, on the full window (last calib_share of sessions -> calibrator)
        n_cal = max(1, int(round(len(sessions) * cfg.calib_share)))
        strategies = sorted(X["strategy_id"].astype(str).unique().tolist())
        booster, cal, base, thr = fit_calibrated(X, meta, sessions[:-n_cal], sessions[-n_cal:], strategies, cfg)
        reasons = []
        if not prospective:
            reasons.append(f"trained on non-prospective labels {data_tiers}: research only, never promotable")
        if not gate["passed"]:
            reasons.append("gate failed: " + ", ".join(k for k, v in gate["checks"].items() if not v))
        status = "CANDIDATE" if not reasons else "RESEARCH_ONLY"
        models_dir = Path(models_dir)
        models_dir.mkdir(parents=True, exist_ok=True)
        path = models_dir / f"{model_name}_v{version}.json"
        art = artifact(booster, cal, strategies, thr, base, cfg, horizon_min,
                       {"model_name": model_name, "version": version, "training_run_id": run.id,
                        "dataset_fingerprint": run.dataset_fingerprint, "status_at_creation": status})
        path.write_text(json.dumps(art), encoding="utf-8")
        mv = ModelVersions(model_name=model_name, version=version, training_run_id=run.id, created_at=iso(),
                           artifact_path=str(path), artifact_sha256=_sha256(path),
                           features=json.dumps(list(ds.NUMERIC) + [f"strat_{x}" for x in strategies]),
                           calibration=cal.method, status=status, status_reason="; ".join(reasons))
        s.add(mv)
        s.flush()
        if ev.get("oof") is not None:
            o = ev["oof"]
            for r in o.itertuples(index=False):
                m = meta.iloc[r.row]
                s.add(ModelPredictions(model_version_id=mv.id, kind="oof", fold=int(r.fold),
                                       label_run_id=m["run_id"], session_date=m["session_date"],
                                       signal_seq=int(m["signal_seq"]), strategy_id=m["strategy_id"],
                                       p_win=float(r.p_win), take=int(r.take), outcome_label=m["label"],
                                       net_pnl_per_lot=float(r.net)))
        s.commit()
        return {"training_run_id": run.id, "model_version_id": mv.id, "model": f"{model_name} v{version}",
                "status": status, "status_reason": mv.status_reason, "artifact": str(path),
                "rows": len(meta), "sessions": len(sessions), "data_tiers": data_tiers, "adequacy": adq,
                "leakage": leak, "folds": ev["folds"], "metrics": ev.get("metrics"), "gate": gate,
                "evaluation_error": None if ev["ok"] else ev.get("reason")}


def verify_artifact(engine: Engine, model_version_id: int) -> bool:
    with Session(engine) as s:
        mv = s.get(ModelVersions, model_version_id)
        return mv is not None and Path(mv.artifact_path).exists() and _sha256(Path(mv.artifact_path)) == mv.artifact_sha256


def approve(engine: Engine, model_version_id: int, actor: str, reason: str, log=None) -> dict:
    """Operator approval of a CANDIDATE. RESEARCH_ONLY can never be approved."""
    from app.control.actions import OperatorLog
    log = log or OperatorLog()
    with Session(engine) as s:
        mv = s.get(ModelVersions, model_version_id)
        if mv is None:
            raise RegistryError(f"no model version {model_version_id}")
        if mv.status != "CANDIDATE":
            raise RegistryError(f"{mv.model_name} v{mv.version} is {mv.status}; only a CANDIDATE can be approved"
                                + (f" ({mv.status_reason})" if mv.status_reason else ""))
        if not verify_artifact(engine, model_version_id):
            raise RegistryError("artifact missing or its sha256 does not match the registry")
        run = s.get(ModelTrainingRuns, mv.training_run_id)
        if not json.loads(run.gate)["passed"] or any(t not in PROSPECTIVE_TIERS for t in json.loads(run.data_tiers)):
            raise RegistryError("training run does not pass the promotion gate on prospective data")
        rec = log.append("approve_meta_model", actor, reason, model=mv.model_name, version=mv.version,
                         artifact_sha256=mv.artifact_sha256)
        for old in s.scalars(select(ModelVersions).where(ModelVersions.model_name == mv.model_name,
                                                         ModelVersions.status == "APPROVED")):
            old.status, old.status_reason = "RETIRED", f"superseded by v{mv.version}"
        mv.status, mv.approved_by, mv.approved_at = "APPROVED", actor, iso()
        s.commit()
        return {"model": f"{mv.model_name} v{mv.version}", "status": mv.status, "operator_log_seq": rec["seq"]}


def list_versions(engine: Engine) -> list[dict]:
    with Session(engine) as s:
        out = []
        for mv in s.scalars(select(ModelVersions).order_by(ModelVersions.model_name, ModelVersions.version)):
            run = s.get(ModelTrainingRuns, mv.training_run_id)
            m = json.loads(run.metrics)
            out.append({"id": mv.id, "model": mv.model_name, "version": mv.version, "status": mv.status,
                        "horizon_min": run.horizon_min, "rows": run.rows, "sessions": run.sessions,
                        "tiers": json.loads(run.data_tiers), "gate_passed": json.loads(run.gate)["passed"],
                        "brier": [m.get("brier_model"), m.get("brier_baseline")],
                        "net_improvement": m.get("net_improvement"), "created_at": mv.created_at,
                        "reason": mv.status_reason})
        return out
