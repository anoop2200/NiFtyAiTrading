"""Experiment specification (v1.3 §4.4, §10.3).

An experiment states a hypothesis and a finite grid of strategy-parameter variants, plus the walk-forward
layout and the selection metric. The canonical JSON of the spec is hashed; together with the code, data,
risk-config and fee fingerprints this identifies a run, so a rerun of the same spec on the same inputs
must reproduce the same metrics.

Parameter names are `<strategy>.<field>` (ORBConfig / PullbackConfig fields) or
`<strategy>.<normal|zero_dte>.<stop_pct|target_pct|time_stop_s>` for exit templates. Every variant runs
every enabled strategy; a parameter for a strategy that is not enabled is an error.
"""
from __future__ import annotations

import hashlib
import itertools
import json
from dataclasses import dataclass, field, fields, replace
from pathlib import Path

import yaml

from app.strategies.base import ExitTemplate
from app.strategies.orb import OpeningRangeBreakout, ORBConfig
from app.strategies.pullback import PullbackConfig, TrendPullback

STRATEGIES = {"orb": (ORBConfig, OpeningRangeBreakout), "pullback": (PullbackConfig, TrendPullback)}
SELECTION_METRICS = ("net_expectancy_r", "net_pnl", "profit_factor", "sharpe_session")


class SpecError(ValueError):
    pass


def canonical(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


def sha(obj) -> str:
    return hashlib.sha256(canonical(obj).encode()).hexdigest()


@dataclass(frozen=True)
class WalkForward:
    train_sessions: int = 15
    test_sessions: int = 8
    step_sessions: int = 8
    anchored: bool = False           # True: train window grows from the first development session
    min_train_trades: int = 10       # a variant with fewer in-sample trades cannot be selected


@dataclass(frozen=True)
class ExperimentSpec:
    name: str
    hypothesis: str
    strategies: tuple[str, ...]
    base: dict = field(default_factory=dict)        # fixed parameter overrides for every variant
    grid: dict = field(default_factory=dict)        # parameter -> list of values (cartesian product)
    session_from: str | None = None
    session_to: str | None = None
    walk_forward: WalkForward = field(default_factory=WalkForward)
    selection_metric: str = "net_expectancy_r"
    holdout_variant: dict | None = None             # the single frozen variant allowed to touch the hold-out
    history_overrides: dict = field(default_factory=lambda: {"require_futures": False})
    notes: str = ""

    # ------------------------------------------------------------------ loading / identity
    @classmethod
    def load(cls, path: Path) -> "ExperimentSpec":
        d = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        return cls.from_dict(d)

    @classmethod
    def from_dict(cls, d: dict) -> "ExperimentSpec":
        if not isinstance(d, dict):
            raise SpecError("spec must be a mapping")
        known = {f.name for f in fields(cls)}
        unknown = set(d) - known
        if unknown:
            raise SpecError(f"unknown spec keys {sorted(unknown)}")
        for k in ("name", "hypothesis", "strategies"):
            if not d.get(k):
                raise SpecError(f"spec needs '{k}'")
        wf = d.get("walk_forward") or {}
        bad = set(wf) - {f.name for f in fields(WalkForward)}
        if bad:
            raise SpecError(f"unknown walk_forward keys {sorted(bad)}")
        spec = cls(**{**d, "strategies": tuple(d["strategies"]), "walk_forward": WalkForward(**wf),
                      "base": d.get("base") or {}, "grid": d.get("grid") or {}})
        spec.validate()
        return spec

    def to_dict(self) -> dict:
        d = {f.name: getattr(self, f.name) for f in fields(self)}
        d["strategies"] = list(self.strategies)
        d["walk_forward"] = self.walk_forward.__dict__
        return d

    @property
    def spec_hash(self) -> str:
        return sha(self.to_dict())

    # ------------------------------------------------------------------ validation / variants
    def validate(self) -> None:
        for s in self.strategies:
            if s not in STRATEGIES:
                raise SpecError(f"unknown strategy {s!r}")
        if self.selection_metric not in SELECTION_METRICS:
            raise SpecError(f"selection_metric must be one of {SELECTION_METRICS}")
        for k, v in self.grid.items():
            if not isinstance(v, list) or not v:
                raise SpecError(f"grid.{k} must be a non-empty list")
        wf = self.walk_forward
        if min(wf.train_sessions, wf.test_sessions, wf.step_sessions) <= 0:
            raise SpecError("walk_forward windows must be positive")
        for params in [self.base, *self.variants().values(), self.holdout_variant or {}]:
            build_strategies(self.strategies, params, {})    # raises SpecError on a bad parameter

    def variants(self) -> dict[str, dict]:
        """variant_id -> full parameter dict (base + grid point). Ids are content hashes (order-free)."""
        keys = sorted(self.grid)
        out = {}
        for combo in itertools.product(*(self.grid[k] for k in keys)) if keys else [()]:
            p = {**self.base, **dict(zip(keys, combo))}
            out[variant_id(p)] = p
        return dict(sorted(out.items()))


def variant_id(params: dict) -> str:
    return "v_" + sha(params)[:12]


# ---------------------------------------------------------------------------------------------- building
def build_strategies(enabled, params: dict, history_overrides: dict) -> list:
    """Instantiate enabled strategies with `params` applied (history overrides first, params win)."""
    per: dict[str, dict] = {s: {} for s in enabled}
    for k, v in params.items():
        parts = k.split(".")
        if parts[0] not in STRATEGIES:
            raise SpecError(f"parameter {k!r}: unknown strategy {parts[0]!r}")
        if parts[0] not in per:
            raise SpecError(f"parameter {k!r} is for a strategy that is not enabled")
        per[parts[0]][".".join(parts[1:])] = v
    out = []
    for s in enabled:
        cfg_cls, strat_cls = STRATEGIES[s]
        cfg = cfg_cls()
        names = {f.name for f in fields(cfg_cls)}
        for k, v in history_overrides.items():
            if k in names:
                cfg = replace(cfg, **{k: v})
        for k, v in per[s].items():
            head, _, sub = k.partition(".")
            if head not in names or head == "version":
                raise SpecError(f"{s}.{k}: unknown parameter")
            if sub:
                tpl = getattr(cfg, head)
                if not isinstance(tpl, ExitTemplate) or sub not in ExitTemplate.__dataclass_fields__:
                    raise SpecError(f"{s}.{k}: unknown exit-template field")
                cfg = replace(cfg, **{head: replace(tpl, **{sub: v})})
            else:
                cur = getattr(cfg, head)
                if isinstance(cur, ExitTemplate) or (cur is not None and not isinstance(v, type(cur))
                                                     and not (isinstance(cur, float) and isinstance(v, int))):
                    raise SpecError(f"{s}.{k}: expected {type(cur).__name__}, got {type(v).__name__}")
                cfg = replace(cfg, **{head: v})
        out.append(strat_cls(cfg))
    return out
