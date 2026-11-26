"""Back up NiftyTradingAI.db (online, integrity-checked) and prune to the newest 30 daily copies.

Usage: .venv\\Scripts\\python scripts\\backup_db.py
"""
import _bootstrap  # noqa: F401
from app.config import settings
from app.database.backup import backup, prune

cfg = settings()
db = cfg.path("database")
if not db.exists():
    raise SystemExit(f"no database at {db}")
dest = backup(db, cfg.path("backups"))
removed = prune(cfg.path("backups"), db.stem)
print(f"backup ok: {dest}" + (f" (pruned {len(removed)})" if removed else ""))
