"""Telegram notifications with severity, deduplication, throttles, digests and a hard cap (v1.3 §14.2, OI-06).

Rules
  * CRITICAL (risk / kill-switch / data outage) is never suppressed: it is always sent; a failed send stays in
    `pending` and is retried (`retry_pending`).
  * WARNING / INFO are dropped when the same dedup key was sent within `dedup_window_s`, when the alert kind was
    sent less than `throttle_s[kind]` ago, or once `daily_cap` non-critical messages went out this IST day.
    Every suppression is counted and summarised by `flush_digest()` ("repeated warnings" digest), so nothing
    disappears without a trace.
Without TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID the notifier writes to an outbox file (dry run). The token is
never logged or put in an error message.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from app.clock import IST, iso

CRITICAL, WARNING, INFO = "CRITICAL", "WARNING", "INFO"
TELEGRAM_MAX = 4096


@dataclass(frozen=True)
class Alert:
    kind: str                    # candidate / rejected / entry / exit / daily_pnl / data / kill_switch / research ...
    severity: str
    text: str
    key: str | None = None       # dedup key; defaults to kind + text

    @property
    def dedup_key(self) -> str:
        return self.key or f"{self.kind}|{self.text}"


@dataclass
class NotifyPolicy:
    dedup_window_s: float = 600.0
    throttle_s: dict = field(default_factory=lambda: {"rejected": 60.0, "candidate": 10.0, "data": 120.0})
    default_throttle_s: float = 0.0
    daily_cap: int = 150         # non-critical messages per IST day
    cap_exempt: tuple = ("daily_pnl",)   # the end-of-day report is never held back by the daily cap


# ---------------------------------------------------------------------------------------------- transports
class OutboxTransport:
    """Dry run: append messages to a JSONL file."""
    name = "outbox"

    def __init__(self, path: Path):
        self.path = Path(path)

    def send(self, text: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps({"at": iso(), "text": text}, ensure_ascii=False) + "\n")


class TelegramTransport:
    name = "telegram"

    def __init__(self, token: str, chat_id: str, client=None, timeout: float = 10.0):
        self._token, self.chat_id, self.timeout = token, chat_id, timeout
        self._client = client

    def send(self, text: str) -> None:
        import httpx
        client = self._client or httpx
        try:
            r = client.post(f"https://api.telegram.org/bot{self._token}/sendMessage",
                            json={"chat_id": self.chat_id, "text": text[:TELEGRAM_MAX],
                                  "disable_web_page_preview": True}, timeout=self.timeout)
            if r.status_code != 200:
                raise RuntimeError(f"telegram HTTP {r.status_code}")
        except Exception as e:  # never leak the token (it is part of the URL)
            raise RuntimeError(str(e).replace(self._token, "<token>")) from None


def make_transport(outbox: Path | None = None):
    from app.config import PROJECT_ROOT
    from app.secrets import get_secret
    token = get_secret("TELEGRAM_BOT_TOKEN", required=False)
    chat = get_secret("TELEGRAM_CHAT_ID", required=False)
    if token and chat:
        return TelegramTransport(token, chat)
    return OutboxTransport(outbox or PROJECT_ROOT / "data" / "runtime" / "telegram_outbox.jsonl")


# ---------------------------------------------------------------------------------------------- notifier
class Notifier:
    def __init__(self, transport, policy: NotifyPolicy | None = None, clock=time.time):
        self.transport, self.policy, self.clock = transport, policy or NotifyPolicy(), clock
        self.last_by_key: dict[str, float] = {}
        self.last_by_kind: dict[str, float] = {}
        self.sent_today: dict[str, int] = {}
        self.suppressed: dict[str, dict] = {}       # kind -> {"count", "reasons", "last"}
        self.pending: list[Alert] = []
        self.log: list[dict] = []

    def _day(self) -> str:
        return datetime.fromtimestamp(self.clock(), IST).date().isoformat()

    def _record(self, alert: Alert, outcome: str, error: str | None = None) -> str:
        self.log.append({"at": self.clock(), "kind": alert.kind, "severity": alert.severity, "outcome": outcome,
                         "key": alert.dedup_key[:120], **({"error": error} if error else {})})
        return outcome

    def _suppress(self, alert: Alert, why: str) -> str:
        s = self.suppressed.setdefault(alert.kind, {"count": 0, "reasons": {}, "last": None})
        s["count"] += 1
        s["reasons"][why] = s["reasons"].get(why, 0) + 1
        s["last"] = alert.text[:200]
        return self._record(alert, why)

    def _deliver(self, alert: Alert, text: str) -> str:
        try:
            self.transport.send(text)
        except Exception as e:
            if alert.severity == CRITICAL:
                self.pending.append(alert)
            return self._record(alert, "FAILED", str(e)[:200])
        now = self.clock()
        self.last_by_key[alert.dedup_key] = now
        self.last_by_kind[alert.kind] = now
        if alert.severity != CRITICAL:
            d = self._day()
            self.sent_today[d] = self.sent_today.get(d, 0) + 1
        return self._record(alert, "SENT")

    def send(self, alert: Alert) -> str:
        if alert.severity not in (CRITICAL, WARNING, INFO):
            raise ValueError(f"unknown severity {alert.severity}")
        text = f"{'🛑' if alert.severity == CRITICAL else '⚠️' if alert.severity == WARNING else 'ℹ️'} {alert.text}"
        if alert.severity == CRITICAL:
            return self._deliver(alert, text)
        now, p = self.clock(), self.policy
        last = self.last_by_key.get(alert.dedup_key)
        if last is not None and now - last < p.dedup_window_s:
            return self._suppress(alert, "DEDUPED")
        thr = p.throttle_s.get(alert.kind, p.default_throttle_s)
        lk = self.last_by_kind.get(alert.kind)
        if thr and lk is not None and now - lk < thr:
            return self._suppress(alert, "THROTTLED")
        if alert.kind not in p.cap_exempt and self.sent_today.get(self._day(), 0) >= p.daily_cap:
            return self._suppress(alert, "CAPPED")
        return self._deliver(alert, text)

    def flush_digest(self) -> str | None:
        """Send one digest of everything suppressed since the last flush (bypasses throttle/cap: it is the
        record of what was held back)."""
        if not self.suppressed:
            return None
        lines = ["Digest of repeated / held-back alerts:"]
        for kind, s in sorted(self.suppressed.items()):
            why = ", ".join(f"{k.lower()} {v}" for k, v in sorted(s["reasons"].items()))
            lines.append(f"• {kind}: {s['count']} ({why}); last: {s['last']}")
        text = "\n".join(lines)[:TELEGRAM_MAX]
        try:
            self.transport.send(text)
        except Exception as e:
            self.log.append({"at": self.clock(), "kind": "digest", "outcome": "FAILED", "error": str(e)[:200]})
            return "FAILED"
        self.suppressed = {}
        self.log.append({"at": self.clock(), "kind": "digest", "outcome": "SENT"})
        return "SENT"

    def retry_pending(self) -> int:
        """Retry failed CRITICAL alerts; returns how many are still pending."""
        todo, self.pending = self.pending, []
        for a in todo:
            self._deliver(a, f"🛑 {a.text} (retry)")
        return len(self.pending)


# ---------------------------------------------------------------------------------------------- alert builders
def kill_switch_alert(on: bool, actor: str, reason: str, emergency: bool = False) -> Alert:
    state = "ON — new entries blocked" if on else "OFF — entries may resume"
    extra = " · EMERGENCY CLOSE of open paper positions" if emergency else ""
    return Alert("kill_switch", CRITICAL, f"Kill switch {state}{extra}\nby {actor}: {reason}")


def auto_kill_alert(reason: str, detail: str) -> Alert:
    return Alert("kill_switch", CRITICAL, f"AUTOMATIC kill switch: {reason}\n{detail}\nOperator acknowledgement required.")


def data_alert(text: str, critical: bool = False) -> Alert:
    return Alert("data", CRITICAL if critical else WARNING, f"Data: {text}", key=f"data|{text[:60]}")


def candidate_alert(sig: dict) -> Alert:
    side = "CE" if (sig.get("direction") or 0) > 0 else "PE"
    head = f"{sig['strategy_id']} {side} candidate {sig.get('setup_id')}"
    if sig["outcome"] == "APPROVED":
        return Alert("candidate", INFO, f"{head} APPROVED → {sig['contract']} ({sig.get('lots')} lot, planned R "
                                        f"₹{sig.get('planned_r_inr')})", key=f"cand|{sig.get('cycle_key')}")
    return Alert("rejected", INFO, f"{head} {sig['outcome']}: {', '.join(sig['reasons'])}",
                 key=f"rej|{sig['strategy_id']}|{'|'.join(sig['reasons'])}")


def trade_alert(t: dict) -> Alert:
    return Alert("exit", INFO, f"Paper trade #{t['trade_id']} closed {t['contract']}: {t['exit_reason']} "
                               f"net ₹{t['net_pnl']:,.2f} ({t.get('r_multiple')} R)", key=f"exit|{t['trade_id']}")


def research_alert(exp_id: str, verdict: str) -> Alert:
    return Alert("research", INFO, f"Experiment {exp_id} finished: {verdict}", key=f"research|{exp_id}")
