"""Indicator registry (v1.3 §5.4): code registry (id -> version -> class) plus the persisted definition rows.

Approval is explicit and audited: only APPROVED definitions may feed production paper decisions; DRAFT
and VALIDATED ones run in research/replay only. Once APPROVED, any logic change must bump SPEC.version —
`sync_definitions` refuses a changed code hash under an approved (id, version); a changed VALIDATED
definition falls back to DRAFT and must be revalidated.
"""
from __future__ import annotations

import json

from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.clock import iso

REGISTRY: dict[str, dict[str, type]] = {}
STATUSES = ("DRAFT", "VALIDATED", "APPROVED", "RETIRED")


def register(cls: type) -> type:
    spec = cls.SPEC
    versions = REGISTRY.setdefault(spec.id, {})
    if spec.version in versions and versions[spec.version] is not cls:
        raise ValueError(f"duplicate indicator {spec.id}@{spec.version}")
    versions[spec.version] = cls
    return cls


def load_all() -> dict[str, dict[str, type]]:
    # importing the modules runs their @register decorators
    from app.indicators import conventional, custom, options  # noqa: F401
    return REGISTRY


def get(indicator_id: str, version: str | None = None) -> type:
    load_all()
    versions = REGISTRY[indicator_id]
    if version is None:
        version = max(versions, key=lambda v: tuple(int(x) for x in v.split(".")))
    return versions[version]


def create(indicator_id: str, version: str | None = None, **params):
    return get(indicator_id, version)(**params)


class CodeChangedWithoutVersionBump(RuntimeError):
    pass


def sync_definitions(engine: Engine) -> list[str]:
    """Upsert one IndicatorDefinitions row per registered (id, version). New rows start as DRAFT."""
    from app.database.models import IndicatorDefinitions
    load_all()
    changed = []
    with Session(engine) as s:
        for iid, versions in sorted(REGISTRY.items()):
            for ver, cls in sorted(versions.items()):
                d = cls.describe()
                row = s.scalars(select(IndicatorDefinitions).where(
                    IndicatorDefinitions.indicator_id == iid, IndicatorDefinitions.version == ver)).first()
                if row is None:
                    s.add(IndicatorDefinitions(indicator_id=iid, version=ver, name=d["name"], family=d["family"],
                                               code_ref=d["code_ref"], code_sha256=d["code_sha256"],
                                               spec=json.dumps(d, default=str), status="DRAFT", created_at=iso(),
                                               updated_at=iso()))
                    changed.append(f"{iid}@{ver} added")
                elif row.code_sha256 != d["code_sha256"]:
                    if row.status == "APPROVED":
                        raise CodeChangedWithoutVersionBump(
                            f"{iid}@{ver} is APPROVED but its code changed: bump SPEC.version "
                            "(approved versions stay available for replay)")
                    # not yet approved: the old validation no longer describes this code
                    if row.status == "VALIDATED":
                        row.status, row.validation = "DRAFT", None
                    row.code_sha256, row.spec, row.updated_at = d["code_sha256"], json.dumps(d, default=str), iso()
                    changed.append(f"{iid}@{ver} code changed -> DRAFT, revalidate")
        s.commit()
    return changed


def set_status(engine: Engine, indicator_id: str, version: str, status: str, actor: str, note: str = "",
               validation: dict | None = None) -> None:
    from app.database.db import log_event
    from app.database.models import IndicatorDefinitions
    if status not in STATUSES:
        raise ValueError(status)
    with Session(engine) as s:
        row = s.scalars(select(IndicatorDefinitions).where(IndicatorDefinitions.indicator_id == indicator_id,
                                                           IndicatorDefinitions.version == version)).one()
        if status == "APPROVED" and not (validation or (row.validation and json.loads(row.validation).get("passed"))):
            raise PermissionError("approval needs a passed validation report")
        old = row.status
        row.status, row.updated_at = status, iso()
        if validation is not None:
            row.validation = json.dumps(validation, default=str)
        s.commit()
    log_event(engine, "INFO", "indicators", "indicator_status_changed",
              {"indicator": f"{indicator_id}@{version}", "from": old, "to": status, "actor": actor, "note": note})


def approved(engine: Engine) -> set[tuple[str, str]]:
    from app.database.models import IndicatorDefinitions
    with Session(engine) as s:
        return {(r.indicator_id, r.version) for r in s.scalars(
            select(IndicatorDefinitions).where(IndicatorDefinitions.status == "APPROVED"))}
