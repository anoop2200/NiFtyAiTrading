"""Reconcile our 1-minute bars against Dhan's historical intraday candles (v1.3 §17 Phase 2 acceptance).

Documented tolerance (per minute, both sides present):
  * open / close: |ours - ref| <= max(2 ticks, 0.02% of ref)
  * high / low:   ours must lie within the reference range (+/- 1 tick) and within 0.05% of the
                  reference extreme (sampled or packet-based bars can miss the true extreme)
  * volume (when both sides have it): within 5% or 2 lots
Reconstructed snapshot bars (~10 s sampling) use a wider, separately documented tolerance because a
minute's true open/close can sit up to one sampling interval away from the first/last sample:
SNAPSHOT_TOLERANCE below (o/c 0.06%, h/l 0.10%, i.e. ~15 / ~26 NIFTY points at 26000).
A day passes when >= 95% of the common minutes are within tolerance and >= 95% of the reference
minutes inside the compared window have a bar of ours.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

PASS_PCT = 95.0


@dataclass(frozen=True)
class Tolerance:
    tick: float = 0.05
    oc_ticks: int = 2
    oc_pct: float = 0.02
    hl_pct: float = 0.05
    vol_pct: float = 5.0
    vol_abs: int = 130  # 2 lots of 65

    def oc_ok(self, ours: float, ref: float) -> bool:
        return abs(ours - ref) <= max(self.oc_ticks * self.tick, ref * self.oc_pct / 100) + 1e-9

    def hl_ok(self, ours_h: float, ours_l: float, ref_h: float, ref_l: float) -> bool:
        inside = ours_h <= ref_h + self.tick + 1e-9 and ours_l >= ref_l - self.tick - 1e-9
        near = (abs(ours_h - ref_h) <= ref_h * self.hl_pct / 100 + 1e-9
                and abs(ours_l - ref_l) <= ref_l * self.hl_pct / 100 + 1e-9)
        return inside and near

    def vol_ok(self, ours: int, ref: int) -> bool:
        return abs(ours - ref) <= max(self.vol_abs, ref * self.vol_pct / 100)


SNAPSHOT_TOLERANCE = Tolerance(tick=0.05, oc_ticks=2, oc_pct=0.06, hl_pct=0.10)


def parse_candles(body: Any) -> dict[int, dict]:
    """Dhan /charts/intraday returns parallel arrays {open, high, low, close, volume, timestamp[, open_interest]}
    with epoch-second timestamps. Returns {epoch_s: {o,h,l,c,v}}; unknown shapes return {}."""
    if isinstance(body, dict) and isinstance(body.get("data"), dict):
        body = body["data"]
    if not isinstance(body, dict):
        return {}
    ts = body.get("timestamp") or body.get("start_Time") or []
    out = {}
    for i, t in enumerate(ts):
        try:
            out[int(t)] = {"open": float(body["open"][i]), "high": float(body["high"][i]),
                           "low": float(body["low"][i]), "close": float(body["close"][i]),
                           "volume": int(body["volume"][i]) if body.get("volume") else None}
        except (KeyError, IndexError, TypeError, ValueError):
            continue
    return out


def reconcile(ours: dict[int, dict], ref: dict[int, dict], tol: Tolerance = Tolerance(),
              window: tuple[int, int] | None = None, check_volume: bool = True) -> dict:
    """`ours` / `ref`: {minute_start_epoch_s: {open, high, low, close, volume}}."""
    if window:
        ref = {t: v for t, v in ref.items() if window[0] <= t < window[1]}
        ours = {t: v for t, v in ours.items() if window[0] <= t < window[1]}
    common = sorted(set(ours) & set(ref))
    fails = {"open": 0, "close": 0, "high_low": 0, "volume": 0}
    ok_minutes, worst = 0, []
    abs_err = {"open": 0.0, "close": 0.0}
    for t in common:
        a, b = ours[t], ref[t]
        abs_err["open"] += abs(a["open"] - b["open"])
        abs_err["close"] += abs(a["close"] - b["close"])
        bad = []
        if not tol.oc_ok(a["open"], b["open"]):
            bad.append("open")
        if not tol.oc_ok(a["close"], b["close"]):
            bad.append("close")
        if not tol.hl_ok(a["high"], a["low"], b["high"], b["low"]):
            bad.append("high_low")
        if check_volume and a.get("volume") is not None and b.get("volume"):
            if not tol.vol_ok(a["volume"], b["volume"]):
                bad.append("volume")
        for k in bad:
            fails[k] += 1
        if bad:
            worst.append({"ts": t, "fields": bad, "ours": a, "ref": b})
        else:
            ok_minutes += 1
    within = 100 * ok_minutes / len(common) if common else 0.0
    coverage = 100 * len(common) / len(ref) if ref else 0.0
    return {"ref_minutes": len(ref), "our_minutes": len(ours), "common_minutes": len(common),
            "coverage_pct": round(coverage, 2), "within_tolerance_pct": round(within, 2),
            "field_failures": fails, "examples": worst[:10],
            "mean_abs_err": {k: round(v / len(common), 4) for k, v in abs_err.items()} if common else None,
            "passed": bool(common) and within >= PASS_PCT and coverage >= PASS_PCT,
            "tolerance": tol.__dict__ | {"pass_pct": PASS_PCT}}
