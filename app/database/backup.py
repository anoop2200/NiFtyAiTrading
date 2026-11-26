"""Online SQLite backup (sqlite3 backup API, safe while WAL is active) and verified restore."""
from __future__ import annotations

import shutil
import sqlite3
from datetime import datetime
from pathlib import Path


def integrity_ok(db_path: Path) -> bool:
    con = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
    try:
        return con.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        con.close()


def backup(db_path: Path, backup_dir: Path, stamp: str | None = None) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = stamp or datetime.now().strftime("%Y%m%d-%H%M%S")
    dest = backup_dir / f"{db_path.stem}-{stamp}.db"
    src = sqlite3.connect(db_path.as_posix())
    dst = sqlite3.connect(dest.as_posix())
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()
    if not integrity_ok(dest):
        dest.unlink(missing_ok=True)
        raise RuntimeError(f"backup failed integrity check: {dest}")
    return dest


def restore(backup_path: Path, db_path: Path) -> Path:
    """Restore a backup over db_path. Caller must ensure no process has the DB open.

    The current DB (if any) is first saved as <name>.pre-restore-<stamp>.db.
    """
    if not integrity_ok(backup_path):
        raise RuntimeError(f"backup is corrupt, refusing to restore: {backup_path}")
    if db_path.exists():
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        shutil.copy2(db_path, db_path.with_name(f"{db_path.stem}.pre-restore-{stamp}.db"))
        for suffix in ("-wal", "-shm"):
            Path(str(db_path) + suffix).unlink(missing_ok=True)
    shutil.copy2(backup_path, db_path)
    if not integrity_ok(db_path):
        raise RuntimeError("restored database failed integrity check")
    return db_path


def prune(backup_dir: Path, stem: str, keep_daily: int = 30) -> list[Path]:
    files = sorted(backup_dir.glob(f"{stem}-*.db"))
    removed = files[:-keep_daily] if len(files) > keep_daily else []
    for f in removed:
        f.unlink()
    return removed
