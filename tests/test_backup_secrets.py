import sqlite3

from sqlalchemy.orm import Session

from app.database.models import SCHEMA_VERSION
from app.database.backup import backup, integrity_ok, restore
from app.database.db import init_db, log_event, make_engine, schema_version
from app.database.models import SystemEvents
from app.secrets import get_secret, mask


def test_backup_and_restore_roundtrip(tmp_path):
    db = tmp_path / "NiftyTradingAI.db"
    e = make_engine(db)
    init_db(e)
    assert schema_version(e) == SCHEMA_VERSION
    log_event(e, "INFO", "test", "before_backup")
    b = backup(db, tmp_path / "backups", stamp="t1")
    assert integrity_ok(b)
    log_event(e, "INFO", "test", "after_backup")
    e.dispose()
    restore(b, db)
    e2 = make_engine(db)
    with Session(e2) as s:
        events = [r.event for r in s.query(SystemEvents).all()]
    assert events == ["before_backup"]
    assert list(tmp_path.glob("NiftyTradingAI.pre-restore-*.db"))


def test_wal_and_foreign_keys(tmp_path):
    e = make_engine(tmp_path / "x.db")
    init_db(e)
    with e.connect() as c:
        assert c.exec_driver_sql("PRAGMA journal_mode").scalar() == "wal"
        assert c.exec_driver_sql("PRAGMA foreign_keys").scalar() == 1


def test_restore_refuses_corrupt_backup(tmp_path):
    bad = tmp_path / "bad.db"
    bad.write_bytes(b"not a database")
    try:
        restore(bad, tmp_path / "target.db")
    except (RuntimeError, sqlite3.DatabaseError):
        pass
    else:
        raise AssertionError("corrupt backup was restored")


def test_secret_env_precedence(monkeypatch):
    monkeypatch.setenv("DHAN_CLIENT_ID", "from-env")
    assert get_secret("DHAN_CLIENT_ID") == "from-env"
    assert mask("abcdef") == "<set, 6 chars>" and mask(None) == "<unset>"
