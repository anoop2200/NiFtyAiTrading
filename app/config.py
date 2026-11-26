"""Load non-secret settings from config/settings.yaml. Paths are resolved against the project root."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SETTINGS = PROJECT_ROOT / "config" / "settings.yaml"


class Settings:
    def __init__(self, data: dict[str, Any], root: Path = PROJECT_ROOT):
        self._data = data
        self.root = root

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def get(self, dotted: str, default: Any = None) -> Any:
        node: Any = self._data
        for part in dotted.split("."):
            if not isinstance(node, dict) or part not in node:
                return default
            node = node[part]
        return node

    def path(self, name: str) -> Path:
        p = Path(self._data["paths"][name])
        p = p if p.is_absolute() else self.root / p
        if p.suffix:  # file path
            p.parent.mkdir(parents=True, exist_ok=True)
        else:
            p.mkdir(parents=True, exist_ok=True)
        return p


def load_settings(path: Path | str | None = None, root: Path | None = None) -> Settings:
    with open(path or DEFAULT_SETTINGS, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return Settings(data, root or PROJECT_ROOT)


@lru_cache(maxsize=1)
def settings() -> Settings:
    return load_settings()
