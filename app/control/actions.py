"""Audited operator actions for the dashboard and console (v1.3 §9.5, §11 single writer, §14.3).

The dashboard and the console never write database tables. Every state-changing action goes through this
module, which
  * appends a hash-chained record to `data/runtime/operator_actions.jsonl` (who, what, why, before/after),
  * applies the action through the channel the trading engine already reads:
      - manual kill switch / emergency close  -> kill-switch state file (re-read on every risk evaluation,
        so new entries are blocked immediately),
      - acknowledgement of an automatic trip   -> command file in `data/runtime/commands/`, consumed by the
        engine's KillSwitch (only the engine clears its own automatic latch),
      - guarded risk-config change             -> a new, validated, version-bumped `config/risk.yaml`
        (comments preserved); the engine loads it at its next session start and re-validates it.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from dataclasses import dataclass
from pathlib import Path

import yaml

from app.clock import iso
from app.config import PROJECT_ROOT
from app.risk.config import RISK_FILE, RiskConfig, RiskConfigError
from app.risk.killswitch import KillSwitch

RUNTIME = PROJECT_ROOT / "data" / "runtime"
LOG_FILE = RUNTIME / "operator_actions.jsonl"
COMMANDS = RUNTIME / "commands"
KILL_FILE = RUNTIME / "kill_switch.json"
RESUME_PHRASE = "RESUME"

# Only these risk settings can be changed from the UI; everything else needs a code review + commit.
EDITABLE = {
    ("limits", "max_daily_loss_r"), ("limits", "max_open_risk_r"), ("limits", "max_concurrent_positions"),
    ("limits", "max_entries_per_day"), ("limits", "max_consecutive_losses"), ("limits", "cooldown_after_stop_min"),
    ("limits", "daily_loss_open_policy"), ("zero_dte", "max_entries"), ("zero_dte", "last_entry"),
    ("windows", "no_entry_open_until"), ("windows", "last_entry"), ("quotes", "min_premium"),
    ("sizing", "lots_per_trade"), ("sizing", "lots_per_trade_0dte"),
}


class ActionError(ValueError):
    pass


# ---------------------------------------------------------------------------------------------- audit log
@dataclass
class OperatorLog:
    path: Path = LOG_FILE

    def records(self) -> list[dict]:
        if not Path(self.path).exists():
            return []
        return [json.loads(x) for x in Path(self.path).read_text(encoding="utf-8").splitlines() if x.strip()]

    def head(self) -> str:
        r = self.records()
        return r[-1]["hash"] if r else "genesis"

    def append(self, action: str, actor: str, reason: str, **detail) -> dict:
        if not actor or not actor.strip():
            raise ActionError("an operator name is required")
        if not reason or not reason.strip():
            raise ActionError("a reason is required")
        prev = self.head()
        rec = {"seq": len(self.records()) + 1, "at": iso(), "action": action, "actor": actor.strip(),
               "reason": reason.strip(), "detail": detail}
        body = json.dumps(rec, sort_keys=True, separators=(",", ":"), default=str)
        rec["prev"] = prev
        rec["hash"] = hashlib.sha256((prev + body).encode()).hexdigest()
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, default=str) + "\n")
            f.flush()
            os.fsync(f.fileno())
        return rec

    def verify(self) -> bool:
        head = "genesis"
        for r in self.records():
            body = {k: v for k, v in r.items() if k not in ("prev", "hash")}
            h = hashlib.sha256((head + json.dumps(body, sort_keys=True, separators=(",", ":"), default=str))
                               .encode()).hexdigest()
            if r["prev"] != head or r["hash"] != h:
                return False
            head = h
        return True


# ---------------------------------------------------------------------------------------------- kill switch
def kill_status(kill_file: Path = KILL_FILE) -> dict:
    ks = KillSwitch(kill_file)
    ks.refresh()
    d = json.loads(Path(kill_file).read_text(encoding="utf-8")) if Path(kill_file).exists() else {}
    return {"manual": ks.manual, "emergency_close": ks.emergency_close, "changed_by": d.get("actor"),
            "changed_at": d.get("at"), "state": "SAFE (entries blocked)" if ks.manual else "ACTIVE"}


def kill_on(actor: str, reason: str, emergency_close: bool = False, kill_file: Path = KILL_FILE,
            log: OperatorLog | None = None) -> dict:
    log = log or OperatorLog()
    before = kill_status(kill_file)
    rec = log.append("kill_switch_on", actor, reason, emergency_close=emergency_close, before=before)
    KillSwitch(kill_file).set_manual(True, actor, emergency_close=emergency_close)
    return rec


def kill_off(actor: str, reason: str, confirm: str, kill_file: Path = KILL_FILE, log: OperatorLog | None = None) -> dict:
    if confirm != RESUME_PHRASE:
        raise ActionError(f"type {RESUME_PHRASE} to confirm that entries may resume")
    log = log or OperatorLog()
    before = kill_status(kill_file)
    rec = log.append("kill_switch_off", actor, reason, before=before)
    KillSwitch(kill_file).set_manual(False, actor)
    return rec


def acknowledge_auto(actor: str, reason: str, commands: Path = COMMANDS, log: OperatorLog | None = None) -> dict:
    """Ask the engine to acknowledge its automatic kill-switch latch (it applies it on its next refresh)."""
    log = log or OperatorLog()
    rec = log.append("auto_kill_ack_requested", actor, reason)
    post_command({"command": "ack_auto", "actor": actor.strip(), "reason": reason.strip(),
                  "operator_log_hash": rec["hash"]}, commands)
    return rec


def reset_loss_latch(actor: str, reason: str, commands: Path = COMMANDS, log: OperatorLog | None = None) -> dict:
    """Ask the paper engine to reset the consecutive-loss latch (allowed only in a later session than the one
    that hit the limit; the engine checks that and records the reset as a risk event)."""
    log = log or OperatorLog()
    rec = log.append("loss_latch_reset_requested", actor, reason)
    post_command({"command": "reset_loss_latch", "actor": actor.strip(), "reason": reason.strip(),
                  "operator_log_hash": rec["hash"]}, commands)
    return rec


def post_command(cmd: dict, commands: Path = COMMANDS) -> Path:
    commands.mkdir(parents=True, exist_ok=True)
    name = f"{iso().replace(':', '').replace('+', 'p')}_{uuid.uuid4().hex[:8]}.json"
    tmp = commands / (name + ".tmp")
    tmp.write_text(json.dumps(cmd), encoding="utf-8")
    os.replace(tmp, commands / name)
    return commands / name


# ---------------------------------------------------------------------------------------------- risk config
def _fmt(v) -> str:
    if isinstance(v, str):
        return f'"{v}"' if re.fullmatch(r"\d{1,2}:\d{2}", v) else v
    return repr(v) if isinstance(v, float) else str(v)


def _set_line(text: str, sec: str, key: str, value: str, name: str) -> str:
    """Replace the value of `key` inside top-level block `sec`, keeping indentation and trailing comment."""
    lines = text.split("\n")
    current = None
    line_re = re.compile(rf"^([ \t]+{re.escape(key)}:[ \t]*)([^#]*?)([ \t]*#.*)?$")
    for i, line in enumerate(lines):
        top = re.match(r"^([A-Za-z_][\w]*):", line)
        if top:
            current = top.group(1)
            continue
        if current == sec:
            m = line_re.match(line.rstrip("\r"))
            if m:
                cr = "\r" if line.endswith("\r") else ""
                lines[i] = f"{m.group(1)}{value}{m.group(3) or ''}{cr}"
                return "\n".join(lines)
    raise ActionError(f"{sec}.{key} not found in {name}")


def _bump(version: str) -> str:
    m = re.fullmatch(r"(.*?)(\d+)\.(\d+)\.(\d+)", version)
    if not m:
        raise ActionError(f"cannot bump version {version!r}")
    return f"{m.group(1)}{m.group(2)}.{m.group(3)}.{int(m.group(4)) + 1}"


def change_risk_config(actor: str, reason: str, changes: dict[str, object], confirm: str,
                       risk_file: Path = RISK_FILE, log: OperatorLog | None = None) -> dict:
    """Apply whitelisted risk-setting changes ("section.key" -> value). Validates the whole new config with the
    engine's own validator before writing; bumps the version; keeps every comment; logs before/after."""
    if confirm != "APPLY":
        raise ActionError("type APPLY to confirm the configuration change")
    if not changes:
        raise ActionError("no changes")
    path = Path(risk_file)
    text = path.read_text(encoding="utf-8")
    old = yaml.safe_load(text)
    new_text = text
    diff = {}
    for dotted, value in changes.items():
        sec, _, key = dotted.partition(".")
        if (sec, key) not in EDITABLE:
            raise ActionError(f"{dotted} is not editable from the UI")
        cur = old.get(sec, {}).get(key)
        if cur == value:
            continue
        new_text = _set_line(new_text, sec, key, _fmt(value), path.name)
        diff[dotted] = {"from": cur, "to": value}
    if not diff:
        raise ActionError("values are unchanged")
    new_version = _bump(old["version"])
    new_text, n = re.subn(r"^version:[ \t]*\S+", f"version: {new_version}", new_text, count=1, flags=re.M)
    if n != 1:
        raise ActionError("version line not found")
    parsed = yaml.safe_load(new_text)
    try:
        RiskConfig.from_dict(parsed)
    except RiskConfigError as e:
        raise ActionError(f"rejected by risk validation: {e}") from e
    for dotted, d in diff.items():
        sec, _, key = dotted.partition(".")
        if parsed[sec][key] != d["to"]:
            raise ActionError(f"{dotted} did not round-trip (got {parsed[sec][key]!r})")
    log = log or OperatorLog()
    rec = log.append("risk_config_change", actor, reason, from_version=old["version"], to_version=new_version,
                     changes=diff, old_sha256=hashlib.sha256(text.encode()).hexdigest(),
                     new_sha256=hashlib.sha256(new_text.encode()).hexdigest(),
                     takes_effect="next trading-engine session start")
    tmp = path.with_suffix(".yaml.tmp")
    tmp.write_text(new_text, encoding="utf-8")
    os.replace(tmp, path)
    return rec
