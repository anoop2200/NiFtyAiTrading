"""Indicator engine: multi-timeframe resampling, source-requirement checks, streaming + batch runs,
checkpoint/restore and warm-up.

Streams carry final 1-minute bars as dicts with at least `ts` (epoch s, bar start), `session` (YYYY-MM-DD)
and `session_open_ts`. Higher timeframes are built by `Resampler`, which only emits a bar once all its
minutes are in (or the next bucket starts), so an N-minute indicator never sees an unfinished bar.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field

import pandas as pd

from app.indicators.base import Indicator
from app.indicators.registry import create


# --------------------------------------------------------------------------------------------- resampler
@dataclass
class Resampler:
    tf_s: int
    cur: dict | None = None

    def _bucket(self, bar: dict) -> int:
        o = int(bar.get("session_open_ts") or 0)
        return o + (int(bar["ts"]) - o) // self.tf_s * self.tf_s

    def on_bar(self, bar: dict) -> list[dict]:
        out = []
        b = self._bucket(bar)
        if self.cur is not None and (b != self.cur["ts"] or bar.get("session") != self.cur.get("session")):
            out.append(self._close())
        if self.cur is None:
            self.cur = {**bar, "ts": b, "minutes": 0}
            self.cur.pop("volume", None)
            self.cur["volume"] = 0
        c = self.cur
        c["high"] = max(c["high"], bar["high"]) if c.get("high") is not None else bar.get("high")
        c["low"] = min(c["low"], bar["low"]) if c.get("low") is not None else bar.get("low")
        for k, v in bar.items():  # last-value fields (close, oi, quotes...) come from the latest minute
            if k not in ("ts", "open", "high", "low", "volume"):
                c[k] = v
        c["volume"] += bar.get("volume") or 0
        c["minutes"] += 1
        if int(bar["ts"]) + 60 >= b + self.tf_s:  # last minute of the bucket arrived: bar is final now
            out.append(self._close())
        return out

    def _close(self) -> dict:
        c, self.cur = self.cur, None
        c["complete"] = c["minutes"] == self.tf_s // 60
        return c

    def flush(self) -> list[dict]:
        return [self._close()] if self.cur else []


def resample_df(df: pd.DataFrame, tf_s: int) -> pd.DataFrame:
    if tf_s == 60:
        return df
    r, rows = Resampler(tf_s), []
    for b in df.to_dict("records"):
        rows += r.on_bar(b)
    rows += r.flush()
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------------------------- engine
@dataclass
class Binding:
    """One indicator instance bound to a stream and timeframe."""
    indicator: Indicator
    stream: str
    tf_s: int = 60

    @property
    def key(self) -> str:
        return f"{self.stream}/{self.tf_s // 60}m/{self.indicator.key}"


def binding(indicator_id: str, stream: str, tf_min: int = 1, version: str | None = None, **params) -> Binding:
    return Binding(create(indicator_id, version, **params), stream, tf_min * 60)


def check_source(b: Binding, tier: str, columns: set[str]) -> str | None:
    """None when the stream satisfies the indicator's declared source requirement, else the reason."""
    req = b.indicator.SPEC.source
    if req.stream != b.stream:
        return f"needs stream '{req.stream}', bound to '{b.stream}'"
    if tier not in req.tiers:
        return f"tier '{tier}' not accepted (needs one of {req.tiers})"
    missing = set(req.fields) - columns
    if missing:
        return f"stream lacks fields {sorted(missing)}"
    if req.needs_bid_ask and not {"bid", "ask"} <= columns:
        return "needs observed bid/ask"
    return None


@dataclass
class IndicatorEngine:
    bindings: list[Binding]
    resamplers: dict[tuple[str, int], Resampler] = field(default_factory=dict)

    def __post_init__(self):
        for b in self.bindings:
            if b.tf_s != 60:
                self.resamplers.setdefault((b.stream, b.tf_s), Resampler(b.tf_s))

    def on_bar(self, stream: str, bar: dict) -> list[dict]:
        """Feed one final 1-minute bar of `stream`; returns output rows for every indicator that updated."""
        out = []
        by_tf: dict[int, list[dict]] = {60: [bar]}
        for (s, tf), r in self.resamplers.items():
            if s == stream:
                by_tf[tf] = r.on_bar(bar)
        for b in self.bindings:
            if b.stream != stream:
                continue
            for xb in by_tf.get(b.tf_s, []):
                res = b.indicator.update(xb)
                out.append({"ts": int(xb["ts"]), "binding": b.key, **res})
        return out

    def run(self, streams: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Streaming replay over whole datasets in time order (ties: stream name)."""
        events = []
        for name, df in streams.items():
            events += [(int(r["ts"]), name, r) for r in df.to_dict("records")]
        events.sort(key=lambda e: (e[0], e[1]))
        rows = []
        for _, name, r in events:
            rows += self.on_bar(name, r)
        return pd.DataFrame(rows)

    # ---- checkpoint / restore -----------------------------------------------------------------------
    def checkpoint(self) -> dict:
        return {"indicators": {b.key: b.indicator.get_state() for b in self.bindings},
                "resamplers": {f"{s}|{tf}": r.cur for (s, tf), r in self.resamplers.items()}}

    def restore(self, snap: dict) -> None:
        for b in self.bindings:
            b.indicator.set_state(snap["indicators"][b.key])
        for (s, tf), r in self.resamplers.items():
            r.cur = snap["resamplers"].get(f"{s}|{tf}")

    def warm_up(self, streams: dict[str, pd.DataFrame]) -> int:
        """Recover by replaying history (for recovery='replay' indicators or when no snapshot exists).
        Outputs are discarded; returns bars replayed."""
        df = self.run(streams)
        return len(df)


def output_hash(df: pd.DataFrame) -> str:
    """Stable hash of indicator outputs for deterministic-replay checks (floats rounded to 1e-9)."""
    d = df.copy()
    for c in d.columns:
        if d[c].dtype.kind == "f":
            d[c] = d[c].round(9)
    payload = d.sort_values(["binding", "ts"]).to_json(orient="records", double_precision=10)
    return hashlib.sha256(payload.encode()).hexdigest()


def compare_frames(a: pd.DataFrame, b: pd.DataFrame, cols: list[str], rel: float = 1e-9, abs_: float = 1e-9) -> dict:
    """Batch vs stream comparison: same validity, states, and values within tolerance where valid."""
    mism = {"valid": int((a["valid"].astype(bool) != b["valid"].astype(bool)).sum()),
            "state": int((a["state"] != b["state"]).sum())}
    worst = 0.0
    both = a["valid"].astype(bool) & b["valid"].astype(bool)
    for c in cols:
        x = pd.to_numeric(a.loc[both, c], errors="coerce").astype(float)
        y = pd.to_numeric(b.loc[both, c], errors="coerce").astype(float)
        diff = (x - y).abs()
        tol = abs_ + rel * y.abs()
        mism[c] = int((diff > tol).sum())
        if len(diff):
            worst = max(worst, float(diff.max()))
    return {"rows": len(a), "valid_rows": int(both.sum()), "mismatches": mism, "max_abs_diff": worst,
            "passed": all(v == 0 for v in mism.values())}


def save_checkpoint(path, snap: dict) -> None:
    import os
    from pathlib import Path
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(snap), encoding="utf-8")
    os.replace(tmp, p)


def load_checkpoint(path) -> dict:
    from pathlib import Path
    return json.loads(Path(path).read_text(encoding="utf-8"))
