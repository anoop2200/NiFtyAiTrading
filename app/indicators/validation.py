"""Phase 3 acceptance checks, shared by unit tests and the validation script:
batch == stream, deterministic replay, no look-ahead, restart (snapshot) and warm-up (replay) recovery."""
from __future__ import annotations

import json

import numpy as np
import pandas as pd

from app.indicators.base import Indicator
from app.indicators.engine import compare_frames

REL, ABS = 1e-9, 1e-7


def batch_vs_stream(cls: type[Indicator], df: pd.DataFrame, **params) -> dict:
    a, b = cls(**params).stream(df), cls(**params).batch(df)
    return compare_frames(a, b, list(cls.SPEC.outputs), REL, ABS) | {"vectorised": cls._vector is not None}


def deterministic(cls: type[Indicator], df: pd.DataFrame, **params) -> dict:
    h = [pd.util.hash_pandas_object(cls(**params).stream(df).astype(str), index=True).sum() for _ in range(2)]
    return {"passed": bool(h[0] == h[1])}


def _perturb(df: pd.DataFrame, cut: int, seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    d = df.copy()
    tail = d.index[cut:]
    for c in d.columns:
        if c in ("ts", "session", "session_open_ts", "expiry", "atm_strike", "tier"):
            continue
        if pd.api.types.is_numeric_dtype(d[c]):
            vals = pd.to_numeric(d.loc[tail, c], errors="coerce").astype(float)
            d[c] = d[c].astype(float)
            d.loc[tail, c] = vals * rng.uniform(0.5, 1.5, len(tail))
    return d


def no_lookahead(cls: type[Indicator], df: pd.DataFrame, cut: int, **params) -> dict:
    """Changing every bar after `cut` must not change any output at or before `cut - 1`."""
    a = cls(**params).stream(df).iloc[:cut]
    b = cls(**params).stream(_perturb(df, cut)).iloc[:cut]
    r = compare_frames(a, b, list(cls.SPEC.outputs), 0.0, 0.0)
    return {"passed": r["passed"], "cut": cut, "mismatches": r["mismatches"]}


def restart_snapshot(cls: type[Indicator], df: pd.DataFrame, cut: int, **params) -> dict:
    """Stop at `cut`, persist state as JSON, restore into a fresh instance: outputs must be identical."""
    full = cls(**params).stream(df)
    first = cls(**params)
    first.stream(df.iloc[:cut])
    snap = json.loads(json.dumps(first.get_state()))
    second = cls(**params)
    second.set_state(snap)
    rest = second.stream(df.iloc[cut:])
    r = compare_frames(full.iloc[cut:], rest, list(cls.SPEC.outputs), 0.0, 0.0)
    return {"passed": r["passed"], "cut": cut, "mismatches": r["mismatches"]}


def restart_replay(cls: type[Indicator], df: pd.DataFrame, cut: int, **params) -> dict:
    """For recovery='replay': a fresh instance warmed up on the `warmup()` bars before `cut` must produce
    identical outputs from `cut` on (requires the window to lie inside one reset segment)."""
    ind = cls(**params)
    w = ind.warmup()
    full = cls(**params).stream(df)
    ind.stream(df.iloc[max(0, cut - w):cut])
    rest = ind.stream(df.iloc[cut:])
    r = compare_frames(full.iloc[cut:], rest, list(cls.SPEC.outputs), REL, ABS)
    return {"passed": r["passed"], "cut": cut, "warmup": w, "mismatches": r["mismatches"]}


def validate(cls: type[Indicator], df: pd.DataFrame, cuts: list[int] | None = None, **params) -> dict:
    cuts = cuts or [len(df) // 3, 2 * len(df) // 3]
    rep = {"indicator": f"{cls.SPEC.id}@{cls.SPEC.version}", "rows": len(df),
           "batch_vs_stream": batch_vs_stream(cls, df, **params), "deterministic": deterministic(cls, df, **params),
           "no_lookahead": [no_lookahead(cls, df, c, **params) for c in cuts],
           "restart_snapshot": [restart_snapshot(cls, df, c, **params) for c in cuts]}
    if cls.SPEC.recovery == "replay":
        rep["restart_replay"] = [restart_replay(cls, df, c, **params) for c in cuts]
    checks = [rep["batch_vs_stream"]["passed"], rep["deterministic"]["passed"],
              *(x["passed"] for x in rep["no_lookahead"]), *(x["passed"] for x in rep["restart_snapshot"]),
              *(x["passed"] for x in rep.get("restart_replay", []))]
    rep["passed"] = all(checks)
    return rep
