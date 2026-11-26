"""SQLite engine: WAL, foreign keys, single controlled writer per process.

Phase 1 note: the recorder / import scripts are the only writers. When the trading-engine arrives
(Phase 5) it becomes the sole writer and other processes send it commands (v1.3 §12.1).
"""
from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import create_engine, event, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.clock import iso
from app.config import settings
from app.database.models import SCHEMA_VERSION, Base, SchemaVersion, SystemEvents


def _sqlite_pragmas(dbapi_conn, _record):
    cur = dbapi_conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("PRAGMA foreign_keys=ON")
    cur.execute("PRAGMA synchronous=NORMAL")
    cur.execute("PRAGMA busy_timeout=5000")
    cur.close()


def make_engine(db_path: Path | str | None = None) -> Engine:
    path = Path(db_path) if db_path else settings().path("database")
    path.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(f"sqlite:///{path.as_posix()}", future=True)
    event.listen(engine, "connect", _sqlite_pragmas)
    return engine


def init_db(engine: Engine) -> None:
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        if s.get(SchemaVersion, SCHEMA_VERSION) is None:
            s.add(SchemaVersion(version=SCHEMA_VERSION, applied_at=iso(),
                                description="Phase 1: instruments, sessions, events, historical imports"))
            s.commit()


def session_factory(engine: Engine) -> sessionmaker:
    return sessionmaker(engine, expire_on_commit=False)


def log_event(engine: Engine, level: str, component: str, event_name: str,
              detail: dict | None = None, session_id: int | None = None) -> None:
    with Session(engine) as s:
        s.add(SystemEvents(ts=iso(), level=level, component=component, event=event_name,
                           detail=json.dumps(detail or {}, default=str), session_id=session_id))
        s.commit()


def schema_version(engine: Engine) -> int | None:
    with Session(engine) as s:
        return s.scalar(select(SchemaVersion.version).order_by(SchemaVersion.version.desc()))
