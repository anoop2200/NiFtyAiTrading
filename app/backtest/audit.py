"""Append-only, hash-chained audit log for orders, fills, costs and decisions (v1.3 §8.3, §18).

Each record carries the hash of the previous record, so a replay that produces the same events in the same
order produces the same final hash: that is the reproducibility check for fill and cost audits."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path


def _canon(v):
    if isinstance(v, float):
        return round(v, 6)
    if isinstance(v, dict):
        return {k: _canon(x) for k, x in sorted(v.items())}
    if isinstance(v, (list, tuple)):
        return [_canon(x) for x in v]
    return v


@dataclass
class AuditLog:
    records: list[dict] = field(default_factory=list)
    head: str = "genesis"

    def add(self, ts_ns: int, kind: str, **payload) -> dict:
        rec = {"seq": len(self.records) + 1, "ts_ns": int(ts_ns), "kind": kind, **_canon(payload)}
        body = json.dumps(rec, sort_keys=True, separators=(",", ":"), default=str)
        rec["prev"] = self.head
        rec["hash"] = hashlib.sha256((self.head + body).encode()).hexdigest()
        self.head = rec["hash"]
        self.records.append(rec)
        return rec

    def of(self, kind: str) -> list[dict]:
        return [r for r in self.records if r["kind"] == kind]

    def write_jsonl(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            for r in self.records:
                f.write(json.dumps(r, default=str) + "\n")

    @staticmethod
    def verify(records: list[dict]) -> bool:
        head = "genesis"
        for r in records:
            body = {k: v for k, v in r.items() if k not in ("prev", "hash")}
            h = hashlib.sha256((head + json.dumps(body, sort_keys=True, separators=(",", ":"), default=str))
                               .encode()).hexdigest()
            if r["prev"] != head or r["hash"] != h:
                return False
            head = h
        return True
