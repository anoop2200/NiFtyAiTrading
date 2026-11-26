"""Chronological hold-out and walk-forward layout (v1.3 §10.3).

Hold-out: the most recent 20% of sessions (minimum 20) at the time it is created. It is fixed (stored), not
recomputed as data grows. Once inspected for a decision it is retired and becomes development data; the next
hold-out may only be formed from sessions *after* the retired one, once enough of them have accumulated.

Walk-forward: rolling (or anchored) train/test windows over the development sessions only; test windows never
overlap, so the out-of-sample aggregate counts every development session at most once.
"""
from __future__ import annotations

import math
from dataclasses import dataclass

from app.research.spec import WalkForward

HOLDOUT_FRACTION = 0.20
HOLDOUT_MIN_SESSIONS = 20


def holdout_size(n_sessions: int) -> int:
    return max(HOLDOUT_MIN_SESSIONS, math.ceil(HOLDOUT_FRACTION * n_sessions))


def propose_holdout(sessions: list[str], retired_until: str | None) -> list[str] | None:
    """Sessions for a new hold-out, or None if not enough fresh sessions exist yet.

    `retired_until` = last session of the most recently retired hold-out (None if there never was one).
    Development data must keep at least as many sessions as the hold-out."""
    sessions = sorted(sessions)
    fresh = [s for s in sessions if retired_until is None or s > retired_until]
    need = holdout_size(len(sessions))
    if len(fresh) < need or len(sessions) - need < need:
        return None
    return sessions[-need:]


@dataclass(frozen=True)
class Fold:
    index: int
    train: tuple[str, ...]
    test: tuple[str, ...]


def walk_forward(dev_sessions: list[str], wf: WalkForward) -> list[Fold]:
    s = sorted(dev_sessions)
    folds, start, i = [], 0, 0
    while True:
        tr_lo = 0 if wf.anchored else start
        tr_hi = start + wf.train_sessions
        te_hi = tr_hi + wf.test_sessions
        if te_hi > len(s):
            break
        folds.append(Fold(i, tuple(s[tr_lo:tr_hi]), tuple(s[tr_hi:te_hi])))
        start += wf.step_sessions
        i += 1
    for a, b in zip(folds, folds[1:]):
        if set(a.test) & set(b.test):
            raise ValueError("walk-forward test windows overlap: step_sessions must be >= test_sessions")
    return folds
