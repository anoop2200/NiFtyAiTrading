"""Structured logging: human-readable console + JSON-lines file per day."""
from __future__ import annotations

import sys

from loguru import logger

from app.config import settings

_configured = False


def setup_logging(component: str, level: str = "INFO") -> None:
    global _configured
    if _configured:
        return
    log_dir = settings().path("logs")
    logger.remove()
    logger.add(sys.stderr, level=level,
               format="<green>{time:HH:mm:ss.SSS}</green> | <level>{level: <7}</level> | "
                      "{extra[component]} | {message}")
    logger.add(log_dir / f"{component}_{{time:YYYY-MM-DD}}.jsonl", level="DEBUG", serialize=True,
               rotation="00:00", retention="90 days", enqueue=True)
    logger.configure(extra={"component": component})
    _configured = True
