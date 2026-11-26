"""Minimal health view (Phase 1). Read-only; bind to 127.0.0.1 only (v1.3 §14.3).

Run: .venv\\Scripts\\python -m uvicorn app.api.main:app --host 127.0.0.1 --port 8765
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.database.db import init_db, make_engine
from app.database.models import DataSessions, SystemEvents
from app.services.health import read_health

app = FastAPI(title="NiftyTradingAI health", version="0.1.0")
_engine = None


def engine():
    global _engine
    if _engine is None:
        _engine = make_engine()
        init_db(_engine)  # idempotent; lets the view start before the first recording
    return _engine


@app.get("/health")
def health() -> dict:
    return read_health()


@app.get("/sessions")
def sessions(limit: int = 10) -> list[dict]:
    with Session(engine()) as s:
        rows = s.scalars(select(DataSessions).order_by(DataSessions.id.desc()).limit(limit)).all()
        return [{c: getattr(r, c) for c in ("id", "session_date", "started_at", "ended_at", "status",
                                             "subscribed_instruments", "packets", "chain_snapshots",
                                             "reconnects", "notes")} for r in rows]


@app.get("/events")
def events(limit: int = 50) -> list[dict]:
    with Session(engine()) as s:
        rows = s.scalars(select(SystemEvents).order_by(SystemEvents.id.desc()).limit(limit)).all()
        return [{"ts": r.ts, "level": r.level, "event": r.event, "detail": r.detail} for r in rows]


@app.get("/", response_class=HTMLResponse)
def page() -> str:
    h = read_health()
    color = {"RUNNING": "#1a7f37", "COMPLETED": "#555", "STALE": "#b35900"}.get(h.get("state"), "#b42318")
    items = "".join(f"<tr><td>{k}</td><td>{v}</td></tr>" for k, v in h.items())
    return (f"<html><head><meta http-equiv='refresh' content='2'><title>Recorder health</title></head>"
            f"<body style='font-family:system-ui;margin:24px'><h2 style='color:{color}'>"
            f"Recorder: {h.get('state')}</h2><table cellpadding='4'>{items}</table>"
            f"<p><a href='/sessions'>sessions</a> · <a href='/events'>events</a></p></body></html>")
