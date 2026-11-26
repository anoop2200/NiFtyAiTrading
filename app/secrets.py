"""Secret lookup: environment / .env first, then Windows Credential Manager (keyring).

Secrets are never logged. Use `mask()` when a value must be referenced in output.
"""
from __future__ import annotations

import os

from dotenv import load_dotenv

from app.config import PROJECT_ROOT

SERVICE = "NiftyTradingAI"
_loaded = False


class MissingSecret(RuntimeError):
    pass


def _ensure_env() -> None:
    global _loaded
    if not _loaded:
        load_dotenv(PROJECT_ROOT / ".env", override=False)
        _loaded = True


def get_secret(name: str, required: bool = True) -> str | None:
    _ensure_env()
    val = os.environ.get(name)
    if not val:
        try:
            import keyring

            val = keyring.get_password(SERVICE, name)
        except Exception:  # keyring backend unavailable
            val = None
    if required and not val:
        raise MissingSecret(
            f"Secret {name} not set. Add it to .env or run: "
            f".venv\\Scripts\\python scripts\\set_secret.py {name}"
        )
    return val


def set_secret(name: str, value: str) -> None:
    import keyring

    keyring.set_password(SERVICE, name, value)


def mask(value: str | None) -> str:
    if not value:
        return "<unset>"
    return f"<set, {len(value)} chars>"
