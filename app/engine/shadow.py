"""Read-only SHADOW of a running paper session, rebuilt from its journal (for the dashboard).

During the day the engine keeps signals, positions, trades and risk state in memory and persists them to the
database only at closeout. The journal holds every input, and replaying it is deterministic (the same mechanism
as crash recovery, v1.3 §12.3), so the dashboard can reconstruct the engine's exact state without talking to it:

- the live journal is opened read-only and read incrementally (only bytes appended since the last refresh);
- the shadow session is MUTED: no notifier, no kill-switch / command files, no carry file, and its own journal
  handle points at a throw-away scratch file, so it can never write anything the engine or the run owns;
- every engine checkpoint (`ck`: events applied + broker audit head) is compared with the shadow, which makes the
  view a continuous integrity check: a mismatch means the shadow does NOT reproduce the engine.
"""
from __future__ import annotations

import json
import tempfile
import threading
import time
from pathlib import Path

from app.backtest.fees import FeeSchedule
from app.engine.session import PaperSession, SessionSpec
from app.market.calendar import EventCalendar, TradingCalendar


class ShadowSession:
    def __init__(self, journal_path: Path, strategy_factory=None):
        self.path, self.offset = Path(journal_path), 0
        self.strategy_factory = strategy_factory          # tests only (production rebuilds the spec's strategies)
        self.session: PaperSession | None = None
        self.records = 0
        self.checkpoints_ok = 0
        self.mismatches: list[dict] = []
        self.ended = False
        self.recoveries = 0
        self.last_t = 0
        self.last_bar: dict[str, dict] = {}
        self.error: str | None = None
        # the dashboard serves every browser tab on its own thread and they share this object: without the lock
        # two refreshes read the same appended bytes and feed them twice (2026-11-27: duplicate bar in vwap)
        self._lock = threading.Lock()
        self._scratch = Path(tempfile.mkdtemp(prefix="shadow_")) / "shadow.jsonl"

    # ------------------------------------------------------------------ replay
    def _build(self, start: dict) -> None:
        spec = SessionSpec.from_json(start["spec"])
        s = PaperSession(spec, self._scratch, FeeSchedule.load(), TradingCalendar.load(), EventCalendar.load(),
                         _recovering=True, warmup=start.get("warmup"), strategy_factory=self.strategy_factory,
                         with_book=False)   # the book is shown from the engine snapshot (a full replay is slow)
        s.carry_in = start.get("carry") or {}
        s._open_risk_session(spec.gap_ratio if spec.prior_daily is None else None,
                             defer=spec.prior_daily is not None)
        self.session = s

    def refresh(self, max_records: int | None = None) -> "ShadowSession":
        with self._lock:
            return self._refresh(max_records)

    def _refresh(self, max_records: int | None = None) -> "ShadowSession":
        if self.error or not self.path.exists():
            return self
        with open(self.path, "rb") as f:
            f.seek(self.offset)
            chunk = f.read()
        end = chunk.rfind(b"\n")
        if end < 0:
            return self
        lines = chunk[:end].splitlines()
        if max_records is not None and len(lines) > max_records:
            lines = lines[:max_records]
            end = sum(len(x) + 1 for x in lines) - 1
        self.offset += end + 1
        try:
            for line in lines:
                if not line.strip():
                    continue
                self._feed(json.loads(line))
        except Exception as e:  # noqa: BLE001 - shown on the dashboard, never raised into the UI
            self.error = f"replay failed at record {self.records}: {e!r}"[:400]
        return self

    def _feed(self, r: dict) -> None:
        self.records += 1
        k = r["k"]
        if r.get("t"):
            self.last_t = max(self.last_t, int(r["t"]))
        if k == "start":
            self._build(r)
            return
        if self.session is None:
            return
        if k == "ck":
            s = self.session
            if r["n"] == s.applied and r["head"] == s.broker.audit.head:
                self.checkpoints_ok += 1
            else:
                self.mismatches.append({"n": r["n"], "shadow_applied": s.applied})
            return
        if k == "end":
            self.ended = True
            return
        if k == "recovered":
            self.recoveries += 1
            self.session._apply(r)                     # re-anchors the feed watchdog exactly as the engine did
            return
        if k == "b":
            self.last_bar[r["s"]] = r["bar"]
        self.session._apply(r)

    # ------------------------------------------------------------------ views
    def state(self, now_ns: int | None = None) -> dict:
        now_ns = now_ns or time.time_ns()
        s = self.session
        base = {"records": self.records, "checkpoints_ok": self.checkpoints_ok, "mismatches": self.mismatches,
                "ended": self.ended, "recoveries": self.recoveries, "error": self.error,
                "last_event_age_s": round((now_ns - self.last_t) / 1e9, 1) if self.last_t else None}
        if s is None:
            return base | {"built": False}
        rs = s.risk.state
        unreal = s.broker.unrealized_inr() if hasattr(s.broker, "unrealized_inr") else {}
        realized = sum(float(t.get("net_pnl") or 0) for t in s.broker.trades)
        return base | {
            "built": True, "session": s.spec.session, "strategies": list(s.spec.strategies), "lot": s.spec.lot_size,
            "risk_session_open": s.started, "kill": {"manual": s.kill.manual, "auto": s.kill.auto},
            "entries": rs.entries, "consecutive_losses": rs.consecutive_losses, "realized_r": rs.realized_r,
            "daily_loss_hit": rs.daily_loss_hit, "loss_latch": rs.loss_latch_session,
            "open_trades": sorted(s.broker.open_trade_ids()), "pending": len(rs.pending),
            "realized_inr": round(realized, 2), "unrealized_inr": round(sum(unreal.values()), 2),
            "signals": len(s.pipe.signals), "closed_trades": len(s.broker.trades),
            "incidents": s.incidents, "warmup_bars": getattr(s, "warmup_bars", None),
            "last_bars": {k: {"ts": v.get("ts"), "close": v.get("close")} for k, v in self.last_bar.items()},
        }

    def signals(self) -> list[dict]:
        return list(self.session.pipe.signals) if self.session else []

    def trades(self) -> list[dict]:
        return list(self.session.broker.trades) if self.session else []

    def positions(self) -> list[dict]:
        if not self.session:
            return []
        unreal = self.session.broker.unrealized_inr()
        out = []
        for tid, p in self.session.broker.positions.items():
            if p.status in ("OPEN", "EXITING"):
                d = {k: v for k, v in vars(p).items() if isinstance(v, (str, int, float, bool, type(None)))}
                out.append(d | {"trade_id": tid, "unrealized_inr": unreal.get(tid)})
        return out

    def indicators(self) -> dict:
        """Latest indicator outputs the strategies see (binding key -> values)."""
        return dict(self.session.pipe.latest) if self.session else {}

    def risk_events(self, n: int = 50) -> list[dict]:
        return list(self.session.risk.state.events[-n:]) if self.session else []
