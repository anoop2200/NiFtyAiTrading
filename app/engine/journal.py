"""Write-ahead session journal for crash recovery (v1.3 §12.3).

Every input the paper engine applies (quote, final bar, clock tick, operator command) is appended here BEFORE it
is applied. Because the engine is deterministic, replaying the journal into a fresh engine rebuilds the exact
state (open positions, working orders, risk counters, audit chain). Checkpoint records carry the audit-chain head
after N events; recovery verifies the rebuilt chain against every checkpoint, so a journal that does not
reproduce the engine's own history is detected instead of silently trusted.

Durability: records are buffered and flushed + fsynced at least every `flush_s` seconds (a crash loses at most
that window of inputs — the rebuilt engine is then simply in the state it had just before them). A torn last
line from a crash is ignored on read.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path

from app.backtest.fills import Quote


def quote_rec(q: Quote) -> dict:
    return {"k": "q", "t": q.ts_ns, "key": q.key, "b": [list(x) for x in q.bids], "a": [list(x) for x in q.asks],
            "l": q.ltp, "tier": q.tier, "d": q.has_depth}


def rec_quote(r: dict) -> Quote:
    return Quote(r["t"], r["key"], tuple((float(p), int(n)) for p, n in r["b"]),
                 tuple((float(p), int(n)) for p, n in r["a"]), r["l"], r["tier"], r["d"])


class Journal:
    def __init__(self, path: Path, flush_s: float = 1.0, clock=time.monotonic):
        self.path, self.flush_s, self.clock = Path(path), flush_s, clock
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._f = open(self.path, "a", encoding="utf-8")
        self._buf: list[str] = []
        self._last = clock()
        self.records = 0

    def write(self, rec: dict) -> None:
        self._buf.append(json.dumps(rec, separators=(",", ":"), default=str))
        self.records += 1
        if self.clock() - self._last >= self.flush_s:
            self.flush()

    def flush(self) -> None:
        if self._buf:
            self._f.write("\n".join(self._buf) + "\n")
            self._buf = []
        self._f.flush()
        os.fsync(self._f.fileno())
        self._last = self.clock()

    def close(self) -> None:
        self.flush()
        self._f.close()

    @staticmethod
    def read(path: Path) -> list[dict]:
        out = []
        if not Path(path).exists():
            return out
        lines = Path(path).read_text(encoding="utf-8").split("\n")
        for i, line in enumerate(lines):
            if not line.strip():
                continue
            try:
                out.append(json.loads(line))
            except ValueError:
                if i >= len(lines) - 2:     # torn tail from a crash
                    break
                raise
        return out

    @staticmethod
    def compress(path: Path) -> Path:
        """Closed journals are kept gzip-compressed (an unfinished journal is never compressed)."""
        import gzip
        import shutil
        gz = Path(str(path) + ".gz")
        with open(path, "rb") as src, gzip.open(gz, "wb", compresslevel=6) as dst:
            shutil.copyfileobj(src, dst)
        Path(path).unlink()
        return gz

    @staticmethod
    def closed(path: Path) -> bool:
        return Path(str(path) + ".gz").exists()

    @staticmethod
    def sha256(path: Path) -> str:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()
