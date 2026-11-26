"""Hold-out custody, variant (trial) counter and code fingerprint for experiments (v1.3 §10.3)."""
from __future__ import annotations

import hashlib
import json
import subprocess

from sqlalchemy import func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.clock import iso
from app.config import PROJECT_ROOT
from app.database.models import HoldoutAccessLog, Holdouts, StrategyTrials
from app.research.splits import propose_holdout

FINGERPRINT_GLOBS = ("app/**/*.py", "config/risk.yaml", "config/fees.yaml", "config/indicators.yaml")


class HoldoutError(PermissionError):
    pass


# ---------------------------------------------------------------------------------------------- code
def code_fingerprint() -> dict:
    """Content hash of everything that can change a result, plus git head/dirty for humans."""
    h = hashlib.sha256()
    files = sorted({p for g in FINGERPRINT_GLOBS for p in PROJECT_ROOT.glob(g) if p.is_file()})
    for p in files:
        h.update(str(p.relative_to(PROJECT_ROOT)).replace("\\", "/").encode())
        h.update(p.read_bytes().replace(b"\r\n", b"\n"))
    git = {}
    try:
        git["head"] = subprocess.run(["git", "rev-parse", "HEAD"], cwd=PROJECT_ROOT, capture_output=True,
                                     text=True, timeout=10).stdout.strip() or None
        dirty = subprocess.run(["git", "status", "--porcelain", "--", "app", "config"], cwd=PROJECT_ROOT,
                               capture_output=True, text=True, timeout=10).stdout.strip()
        git["dirty"] = bool(dirty)
    except (OSError, subprocess.SubprocessError):
        git = {"head": None, "dirty": None}
    return {"source_sha256": h.hexdigest(), "files": len(files), **git}


# ---------------------------------------------------------------------------------------------- trials
def register_trials(engine: Engine, strategy_set: str, variants: dict[str, dict], experiment_id: str) -> int:
    """Count every variant before it runs (so failed experiments still count). Returns the total number of
    distinct variants ever tested for this strategy set."""
    with Session(engine) as s:
        for vid, params in variants.items():
            row = s.scalars(select(StrategyTrials).where(StrategyTrials.strategy_set == strategy_set,
                                                         StrategyTrials.variant_id == vid)).first()
            if row is None:
                s.add(StrategyTrials(strategy_set=strategy_set, variant_id=vid, params=json.dumps(params, sort_keys=True),
                                     first_experiment=experiment_id, first_at=iso(), times_tested=1))
            else:
                row.times_tested += 1
        s.commit()
        return s.scalar(select(func.count()).select_from(StrategyTrials)
                        .where(StrategyTrials.strategy_set == strategy_set))


# ---------------------------------------------------------------------------------------------- hold-out
def current_holdout(engine: Engine, sessions: list[str]) -> Holdouts | None:
    """The sealed hold-out; creates one when none is sealed and enough fresh sessions exist."""
    with Session(engine, expire_on_commit=False) as s:
        sealed = s.scalars(select(Holdouts).where(Holdouts.status == "SEALED").order_by(Holdouts.id)).first()
        if sealed is not None:
            return sealed
        retired = s.scalars(select(Holdouts).where(Holdouts.status == "RETIRED")
                            .order_by(Holdouts.last_session.desc())).first()
        chosen = propose_holdout(sessions, retired.last_session if retired else None)
        if chosen is None:
            return None
        h = Holdouts(sessions=json.dumps(chosen), first_session=chosen[0], last_session=chosen[-1], created_at=iso(),
                     status="SEALED")
        s.add(h)
        s.commit()
        return h


def request_holdout(engine: Engine, holdout_id: int | None, experiment_id: str, actor: str, purpose: str) -> Holdouts:
    """Grant one-time access to the sealed hold-out, logging every request (granted or refused). A granted
    access retires the hold-out: its sessions become development data for all later experiments."""
    with Session(engine, expire_on_commit=False) as s:
        h = s.get(Holdouts, holdout_id) if holdout_id is not None else None
        reason = None
        if not actor or not purpose:
            reason = "actor and purpose are required"
        elif h is None:
            reason = "no hold-out exists (not enough sessions yet)"
        elif h.status != "SEALED":
            reason = f"hold-out {h.id} already inspected by {h.inspected_by} at {h.inspected_at} ({h.inspected_experiment})"
        s.add(HoldoutAccessLog(holdout_id=holdout_id, experiment_id=experiment_id, actor=actor or "", purpose=purpose or "",
                               at=iso(), granted=int(reason is None), reason=reason))
        if reason is None:
            h.status, h.inspected_by, h.inspected_at, h.inspected_experiment = "RETIRED", actor, iso(), experiment_id
        s.commit()
        if reason:
            raise HoldoutError(reason)
        return h


def record_exposure(engine: Engine, holdout_id: int, actor: str, description: str, retire: bool = True) -> None:
    """Document a look at hold-out data outside a granted review (§10.3). By default the hold-out is retired;
    `retire=False` only documents it (an operator decision, e.g. for aggregate results seen before the
    hold-out existed) and the note stays on the hold-out for every later review."""
    with Session(engine) as s:
        h = s.get(Holdouts, holdout_id)
        log = json.loads(h.accidental_exposure or "[]") + [{"at": iso(), "actor": actor, "what": description}]
        h.accidental_exposure = json.dumps(log)
        if retire and h.status == "SEALED":
            h.status, h.inspected_by, h.inspected_at, h.inspected_experiment = "RETIRED", actor, iso(), "EXPOSURE"
        s.add(HoldoutAccessLog(holdout_id=holdout_id, experiment_id=None, actor=actor, purpose="exposure (retired)" if retire else "exposure (documented)",
                               at=iso(), granted=1, reason=description))
        s.commit()
