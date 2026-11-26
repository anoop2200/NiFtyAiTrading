"""Indicator contract (v1.3 §5.2) and base API.

Every indicator is a small stateful object fed one *completed* bar at a time (`update`). Batch mode is
either the same streaming loop or an independent vectorised implementation (`batch`), and tests require
both to agree. State is plain JSON (`get_state` / `set_state`) so a restart can resume exactly; indicators
whose state is a bounded window may instead recover by replaying `warmup` bars.

No-lookahead rule: `update(bar)` may only use the bar passed in and earlier bars. Bars are passed only
after they are final (the bar builder / resampler guarantee this).
"""
from __future__ import annotations

import copy
import hashlib
from collections import deque
import inspect
import math
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, ClassVar

import pandas as pd


class SignalState(str, Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"
    OVERBOUGHT = "OVERBOUGHT"
    OVERSOLD = "OVERSOLD"
    BREAKOUT = "BREAKOUT"
    REVERSAL_CANDIDATE = "REVERSAL_CANDIDATE"
    VOLATILITY_EXPANSION = "VOLATILITY_EXPANSION"
    NO_SIGNAL = "NO_SIGNAL"


# data tiers an indicator may consume (v1.3 data tiers); synthetic is never accepted in production
TIERS = ("observed_feed", "observed_top_of_book_snapshot", "reconstructed_from_snapshots", "observed_broker_candles")


@dataclass(frozen=True)
class SourceRequirement:
    """What the indicator needs and from where. The engine refuses to run an indicator on a stream that
    does not satisfy it (e.g. futures volume indicators on index-only history)."""
    stream: str                      # index / future / option / chain
    fields: tuple[str, ...]          # bar fields read
    tiers: tuple[str, ...] = TIERS   # acceptable data tiers
    needs_bid_ask: bool = False      # historical bid/ask is unavailable for most history (v1.3 §4.1)


@dataclass(frozen=True)
class IndicatorSpec:
    id: str
    name: str
    version: str                     # semantic version; any logic change -> new version
    family: str                      # trend / momentum / volatility / price_action / participation / options / composite
    formula: str
    source: SourceRequirement
    outputs: tuple[str, ...]         # output columns (besides `state`)
    units: str
    interpretation: str
    timeframe_s: int = 60
    warmup_bars: int = 0             # bars before output is valid
    max_input_age_s: int = 180       # a gap longer than this resets state (missing-data rule)
    missing_data: str = "reset"      # reset | carry (carry only for session-anchored indicators)
    recovery: str = "snapshot"       # snapshot | replay
    session_reset: bool = False      # reset at each session start (VWAP, opening range)
    dependencies: tuple[str, ...] = ()


class Indicator:
    """Subclasses set SPEC and implement `_update(bar) -> dict | None`."""
    SPEC: ClassVar[IndicatorSpec]
    DEFAULTS: ClassVar[dict[str, Any]] = {}

    def __init__(self, **params: Any):
        unknown = set(params) - set(self.DEFAULTS)
        if unknown:
            raise ValueError(f"{self.SPEC.id}: unknown params {sorted(unknown)}")
        self.params = {**self.DEFAULTS, **params}
        self.n = 0                    # bars consumed since last reset
        self.last_ts: int | None = None
        self.session: str | None = None
        self.reset_count = 0
        self.init_state()

    # ---- to implement ----------------------------------------------------------------------------------
    def init_state(self) -> None:
        raise NotImplementedError

    def _update(self, bar: dict) -> dict | None:
        raise NotImplementedError

    def warmup(self) -> int:
        """Bars needed before outputs are valid for the current params (defaults to SPEC.warmup_bars)."""
        return self.SPEC.warmup_bars

    # ---- framework ---------------------------------------------------------------------------------------
    @property
    def key(self) -> str:
        p = ",".join(f"{k}={v}" for k, v in sorted(self.params.items()))
        return f"{self.SPEC.id}@{self.SPEC.version}({p})"

    def gap_limit(self, tf_s) -> int:
        """Largest bar-to-bar gap that is not a data gap: max_input_age_s for 1-minute bars, plus the extra
        bar length for resampled bars (a final 5-minute bar arrives every 300 s)."""
        tf = 60 if tf_s is None or tf_s != tf_s else int(tf_s)
        return self.SPEC.max_input_age_s + tf - 60

    def reset(self) -> None:
        self.n = 0
        self.reset_count += 1
        self.init_state()

    def update(self, bar: dict) -> dict:
        """Feed one final bar; returns {value columns..., state, valid}. Never raises on bad data:
        missing required fields give an invalid output (indicator failures fail safe)."""
        ts = int(bar["ts"])
        if self.last_ts is not None and ts <= self.last_ts:
            raise ValueError(f"{self.key}: non-increasing bar time {ts} <= {self.last_ts}")
        sess = bar.get("session")
        if self.last_ts is not None:
            gap = ts - self.last_ts
            new_session = sess is not None and sess != self.session
            if self.SPEC.session_reset and new_session:
                self.reset()
            elif self.SPEC.missing_data == "reset" and not new_session and gap > self.gap_limit(bar.get("tf_s")):
                # intraday data gap: state built on missing bars is not trusted. Overnight gaps are normal
                # for continuous indicators, so state carries across sessions. Resampled bars carry `tf_s`, so
                # the allowed gap grows with the timeframe (a 5-minute bar arrives every 300 s).
                self.reset()
        self.last_ts, self.session = ts, sess
        if any(_missing(bar.get(f)) for f in self.SPEC.source.fields):
            return self._empty(reason="missing_input")
        out = self._update(bar)
        self.n += 1
        if out is None:
            return self._empty(reason="no_output")
        valid = self.n >= max(self.warmup(), 1) and all(not _missing(out.get(c)) for c in self.SPEC.outputs)
        out.setdefault("state", SignalState.NO_SIGNAL.value)
        if not valid:
            out["state"] = SignalState.NO_SIGNAL.value
        out["valid"] = valid
        return out

    def _empty(self, reason: str) -> dict:
        return {**{c: None for c in self.SPEC.outputs}, "state": SignalState.NO_SIGNAL.value, "valid": False}

    def get_state(self) -> dict:
        """JSON-serialisable snapshot (deques are encoded with their maxlen)."""
        st = {k: _enc(v) for k, v in self.__dict__.items() if k != "params"}
        return {"key": self.key, "state": st}

    def set_state(self, snap: dict) -> None:
        if snap["key"] != self.key:
            raise ValueError(f"state belongs to {snap['key']}, not {self.key}")
        self.__dict__.update({k: _dec(v) for k, v in copy.deepcopy(snap["state"]).items()})

    def stream(self, df: pd.DataFrame) -> pd.DataFrame:
        rows = [self.update(r) for r in df.to_dict("records")]
        return pd.DataFrame(rows, index=df.index, columns=[*self.SPEC.outputs, "state", "valid"])

    # vectorised batch: subclasses implementing `_vector(seg) -> DataFrame[outputs (+ state)]` get an
    # independent batch path; segments follow exactly the same reset rules as streaming.
    _vector = None

    def batch(self, df: pd.DataFrame) -> pd.DataFrame:
        if self._vector is None:
            return type(self)(**self.params).stream(df)
        out = pd.DataFrame(index=df.index, columns=[*self.SPEC.outputs, "state", "valid"], dtype=object)
        ok = df[list(self.SPEC.source.fields)].notna().all(axis=1)
        out["state"], out["valid"] = SignalState.NO_SIGNAL.value, False
        for seg in self.segments(df):
            sub = df.loc[seg][ok.loc[seg]]
            if sub.empty:
                continue
            res = self._vector(sub)
            pos = pd.Series(range(1, len(sub) + 1), index=sub.index)
            valid = (pos >= max(self.warmup(), 1)) & res[list(self.SPEC.outputs)].notna().all(axis=1)
            for c in self.SPEC.outputs:
                out.loc[sub.index, c] = res[c].where(res[c].notna(), None)
            st = res["state"] if "state" in res else pd.Series(SignalState.NO_SIGNAL.value, index=sub.index)
            out.loc[sub.index, "state"] = st.where(valid, SignalState.NO_SIGNAL.value)
            out.loc[sub.index, "valid"] = valid
        return out

    def segments(self, df: pd.DataFrame) -> list[pd.Index]:
        """Split rows where streaming would reset (session start / intraday gap)."""
        ts = df["ts"].astype("int64").to_numpy()
        sess = df["session"].to_numpy() if "session" in df else [None] * len(df)
        tfs = df["tf_s"].to_numpy() if "tf_s" in df else [None] * len(df)
        cuts = [0]
        for i in range(1, len(df)):
            new_session = sess[i] is not None and sess[i] != sess[i - 1]
            gap = ts[i] - ts[i - 1]
            if (self.SPEC.session_reset and new_session) or (
                    self.SPEC.missing_data == "reset" and gap > self.gap_limit(tfs[i]) and not new_session):
                cuts.append(i)
        cuts.append(len(df))
        return [df.index[a:b] for a, b in zip(cuts, cuts[1:]) if b > a]

    @classmethod
    def code_sha(cls) -> str:
        return hashlib.sha256(inspect.getsource(cls).encode()).hexdigest()

    @classmethod
    def describe(cls) -> dict:
        d = asdict(cls.SPEC)
        d["defaults"] = dict(cls.DEFAULTS)
        d["code_ref"] = f"{cls.__module__}:{cls.__qualname__}"
        d["code_sha256"] = cls.code_sha()
        return d


def _enc(v: Any) -> Any:
    if isinstance(v, deque):
        return {"__deque__": [_enc(x) for x in v], "maxlen": v.maxlen}
    if isinstance(v, list):
        return [_enc(x) for x in v]
    if isinstance(v, dict):
        return {k: _enc(x) for k, x in v.items()}
    return v


def _dec(v: Any) -> Any:
    if isinstance(v, dict) and "__deque__" in v:
        return deque((_dec(x) for x in v["__deque__"]), maxlen=v["maxlen"])
    if isinstance(v, list):
        return [_dec(x) for x in v]
    if isinstance(v, dict):
        return {k: _dec(x) for k, x in v.items()}
    return v


def _missing(v: Any) -> bool:
    return v is None or (isinstance(v, float) and math.isnan(v))
