"""Manual and automatic kill switch (v1.3 §9, §9.5).

Manual: a JSON state file (data/runtime/kill_switch.json) that the dashboard, Telegram or a console command
writes; it is re-read on every risk evaluation, so toggling it blocks new entries immediately. An
`emergency_close` flag additionally asks for every open paper position to be closed at the next valid bid.

Automatic: feed outage, clock skew, DB write failure, repeated engine crash or risk-state inconsistency
latch the switch. A latched automatic switch blocks entries until an operator acknowledges it.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

from app.clock import iso

AUTO_REASONS = ("FEED_OUTAGE", "CLOCK_SKEW", "DB_WRITE_FAILURE", "ENGINE_CRASH_LOOP", "RISK_STATE_INCONSISTENT",
                "RISK_EVALUATION_ERROR")


@dataclass
class KillSwitch:
    state_file: Path | None = None
    manual: bool = False
    emergency_close: bool = False
    auto: dict | None = None            # {"reason", "detail", "at", "acked_by"}
    history: list[dict] = field(default_factory=list)
    inbox: Path | None = None           # operator command directory (data/runtime/commands)

    # ---- manual ----------------------------------------------------------------------------------
    def refresh(self) -> None:
        """Pick up changes made by other processes (dashboard / console)."""
        self._consume_commands()
        if self.state_file and Path(self.state_file).exists():
            try:
                d = json.loads(Path(self.state_file).read_text(encoding="utf-8"))
                self.manual = bool(d.get("manual", False))
                self.emergency_close = bool(d.get("emergency_close", False))
            except (OSError, ValueError):
                self.manual = True  # unreadable state file: fail safe (blocked)

    def set_manual(self, on: bool, actor: str, emergency_close: bool = False) -> None:
        self.manual, self.emergency_close = on, (emergency_close and on)
        self.history.append({"at": iso(), "event": "manual_on" if on else "manual_off", "actor": actor,
                             "emergency_close": self.emergency_close})
        if self.state_file:
            p = Path(self.state_file)
            p.parent.mkdir(parents=True, exist_ok=True)
            tmp = p.with_suffix(".tmp")
            tmp.write_text(json.dumps({"manual": self.manual, "emergency_close": self.emergency_close,
                                       "actor": actor, "at": iso()}), encoding="utf-8")
            os.replace(tmp, p)

    # ---- automatic -------------------------------------------------------------------------------
    def trip(self, reason: str, detail: str = "") -> None:
        if reason not in AUTO_REASONS:
            raise ValueError(reason)
        if self.auto is None or self.auto.get("acked_by"):
            self.auto = {"reason": reason, "detail": detail, "at": iso(), "acked_by": None}
            self.history.append({"at": iso(), "event": "auto_trip", "reason": reason, "detail": detail})

    def acknowledge(self, actor: str) -> None:
        if self.auto is None:
            return
        if not actor:
            raise PermissionError("acknowledgement needs an operator name")
        self.auto["acked_by"] = actor
        self.history.append({"at": iso(), "event": "auto_ack", "actor": actor, "reason": self.auto["reason"]})

    def _consume_commands(self) -> None:
        """Apply operator commands posted by the dashboard / console (app.control.actions). Only the engine
        that owns this switch clears its automatic latch; processed commands are renamed *.done."""
        if not self.inbox or not Path(self.inbox).exists():
            return
        for f in sorted(Path(self.inbox).glob("*.json")):
            try:
                cmd = json.loads(f.read_text(encoding="utf-8"))
                if cmd.get("command") != "ack_auto":
                    continue   # other commands belong to the paper engine (app.engine.session)
                if cmd.get("actor"):
                    self.acknowledge(cmd["actor"])
                    self.history.append({"at": iso(), "event": "command_applied", "command": cmd, "file": f.name})
                os.replace(f, f.with_suffix(".done"))
            except (OSError, ValueError):
                continue   # a half-written or unreadable command is retried on the next refresh

    # ---- status ----------------------------------------------------------------------------------
    def blocking_reason(self) -> str | None:
        self.refresh()
        if self.manual:
            return "KILL_SWITCH_MANUAL"
        if self.auto and not self.auto.get("acked_by"):
            return f"KILL_SWITCH_AUTO:{self.auto['reason']}"
        return None
