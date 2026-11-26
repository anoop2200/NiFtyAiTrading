"""Strict loader for config/risk.yaml. Any missing key, unknown key or out-of-range value makes the config
invalid; the risk engine then rejects every entry (RISK_CONFIG_MISSING / RISK_CONFIG_INVALID)."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import time
from pathlib import Path
from typing import Any

import yaml

from app.clock import parse_hhmm
from app.config import PROJECT_ROOT

RISK_FILE = PROJECT_ROOT / "config" / "risk.yaml"

SCHEMA: dict[str, dict[str, type | tuple]] = {
    "sizing": {"lots_per_trade": int, "lots_per_trade_0dte": int, "shadow_risk_pct": (int, float),
               "shadow_risk_pct_0dte": (int, float), "shadow_capital_inr": (int, float),
               "slippage_ticks_per_side": int},
    "limits": {"max_daily_loss_r": (int, float), "daily_loss_open_policy": str, "max_open_risk_r": (int, float),
               "max_concurrent_positions": int, "max_entries_per_day": int, "max_consecutive_losses": int,
               "cooldown_after_stop_min": (int, float), "max_holding_min": (int, float)},
    "zero_dte": {"max_entries": int, "last_entry": str, "max_holding_min": (int, float), "stop_pct": list,
                 "target_pct": list},
    "windows": {"no_entry_open_until": str, "last_entry": str, "flat_by": str, "gap_day_entry_after": str,
                "gap_atr_multiple": (int, float)},
    "quotes": {"min_premium": (int, float), "max_spread_ticks": int, "max_spread_pct_of_mid": (int, float),
               "max_spread_pct_of_target": (int, float), "tick": (int, float), "max_quote_age_s": (int, float),
               "max_chain_age_s": (int, float), "feed_liveness_s": (int, float)},
    "exit_templates": {"normal": dict},
    "kill_switch": {"auto": dict, "manual_state_file": str},
}


class RiskConfigError(ValueError):
    pass


@dataclass(frozen=True)
class RiskConfig:
    version: str
    sha256: str
    raw: dict

    def __getitem__(self, section: str) -> dict:
        return self.raw[section]

    def t(self, section: str, key: str) -> time:
        return parse_hhmm(self.raw[section][key])

    @classmethod
    def load(cls, path: Path = RISK_FILE) -> "RiskConfig":
        p = Path(path)
        if not p.exists():
            raise RiskConfigError(f"risk config missing: {p}")
        text = p.read_text(encoding="utf-8")
        try:
            data = yaml.safe_load(text)
        except yaml.YAMLError as e:
            raise RiskConfigError(f"risk config unreadable: {e}") from e
        return cls.from_dict(data, hashlib.sha256(text.encode()).hexdigest())

    @classmethod
    def from_dict(cls, data: Any, sha: str = "") -> "RiskConfig":
        if not isinstance(data, dict) or not data.get("version"):
            raise RiskConfigError("risk config has no version")
        extra = set(data) - set(SCHEMA) - {"version"}
        if extra:
            raise RiskConfigError(f"unknown sections {sorted(extra)}")
        for sec, keys in SCHEMA.items():
            block = data.get(sec)
            if not isinstance(block, dict):
                raise RiskConfigError(f"missing section '{sec}'")
            missing, unknown = set(keys) - set(block), set(block) - set(keys)
            if missing or unknown:
                raise RiskConfigError(f"section '{sec}': missing {sorted(missing)} unknown {sorted(unknown)}")
            for k, ty in keys.items():
                v = block[k]
                if not isinstance(v, ty) or isinstance(v, bool):
                    raise RiskConfigError(f"{sec}.{k} has wrong type {type(v).__name__}")
        cls._validate(data)
        return cls(str(data["version"]), sha, data)

    @staticmethod
    def _validate(d: dict) -> None:
        def pos(sec, *keys):
            for k in keys:
                if d[sec][k] <= 0:
                    raise RiskConfigError(f"{sec}.{k} must be > 0")
        pos("sizing", "lots_per_trade", "lots_per_trade_0dte", "shadow_risk_pct", "shadow_risk_pct_0dte")
        pos("limits", "max_daily_loss_r", "max_open_risk_r", "max_concurrent_positions", "max_entries_per_day",
            "max_consecutive_losses", "max_holding_min")
        pos("quotes", "min_premium", "tick", "max_quote_age_s", "max_chain_age_s")
        if d["limits"]["daily_loss_open_policy"] not in ("monitor", "close"):
            raise RiskConfigError("limits.daily_loss_open_policy must be monitor|close")
        for sec, key in (("zero_dte", "last_entry"), ("windows", "no_entry_open_until"), ("windows", "last_entry"),
                         ("windows", "flat_by"), ("windows", "gap_day_entry_after")):
            try:
                parse_hhmm(d[sec][key])
            except Exception as e:
                raise RiskConfigError(f"{sec}.{key} is not HH:MM") from e
        if not (parse_hhmm(d["windows"]["no_entry_open_until"]) < parse_hhmm(d["windows"]["last_entry"])
                < parse_hhmm(d["windows"]["flat_by"])):
            raise RiskConfigError("windows must satisfy open_until < last_entry < flat_by")
        for name, tpl in [("zero_dte", d["zero_dte"]), *[(f"exit_templates.{k}", v)
                                                          for k, v in d["exit_templates"].items()]]:
            for k in ("stop_pct", "target_pct"):
                r = tpl.get(k)
                if not (isinstance(r, list) and len(r) == 2 and 0 < r[0] <= r[1] < 100):
                    raise RiskConfigError(f"{name}.{k} must be [lo, hi] with 0 < lo <= hi < 100")
        # fields editable from the dashboard (Phase 8) get explicit bounds too
        if d["zero_dte"]["max_entries"] < 0:
            raise RiskConfigError("zero_dte.max_entries must be >= 0")
        if d["limits"]["cooldown_after_stop_min"] < 0:
            raise RiskConfigError("limits.cooldown_after_stop_min must be >= 0")
        if parse_hhmm(d["zero_dte"]["last_entry"]) > parse_hhmm(d["windows"]["last_entry"]):
            raise RiskConfigError("zero_dte.last_entry cannot be after windows.last_entry")
        if d["zero_dte"]["max_entries"] > d["limits"]["max_entries_per_day"]:
            raise RiskConfigError("zero_dte.max_entries cannot exceed limits.max_entries_per_day")
