"""Health snapshot written atomically by the recorder and read by the health API / CLI."""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

from app.config import settings

HEALTH_FILE = "recorder_health.json"


def health_path() -> Path:
    return settings().path("runtime") / HEALTH_FILE


def write_health(data: dict, path: Path | None = None) -> None:
    path = path or health_path()
    tmp = path.with_suffix(f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps({**data, "written_epoch": time.time()}, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


def read_health(path: Path | None = None, stale_after_s: float = 10.0) -> dict:
    path = path or health_path()
    if not path.exists():
        return {"state": "NOT_RUNNING", "detail": "no health file yet"}
    data = json.loads(path.read_text(encoding="utf-8"))
    age = time.time() - data.get("written_epoch", 0)
    data["health_file_age_s"] = round(age, 1)
    if data.get("state") == "RUNNING" and age > stale_after_s:
        data["state"] = "STALE"  # recorder stopped updating: crashed or hung
    return data
