"""Expired NIFTY option history from Dhan's /charts/rollingoption (OI-03) — RESEARCH TIER ONLY.

What the source is (probe 2026-11-26): 1-minute open/high/low/close, volume, OI, IV and spot for strikes
expressed relative to ATM (ATM±10 near expiry), re-evaluated every minute, back to 2021-09. Trade prices only:
**no bid/ask, no depth**. v1.3 §18 forbids LTP-only fills and invented bid/ask, so this tier can never feed
the quote-based backtest, a promotion decision or a hold-out. Its uses: strategy pre-screening with an explicit,
labelled spread model, and regime / indicator research over many market conditions.

The response carries no expiry date. It is inferred per (session, expiry flag, code):
  1. implied time-to-expiry: invert Black-Scholes for T from Dhan's own ATM call close, spot, strike and IV
     (median over 10:00-14:59), in calendar days
  2. the nearest NIFTY expiry weekday on or after the session — Thursday before EXPIRY_WEEKDAY_SWITCH, Tuesday
     from it (NSE change, Sept 2025); for MONTH the last such weekday of a month
  3. holiday shift: if that date had no trading (no data imported for it), the previous trading day
Inferred expiries are checked: on an inferred expiry day the implied time must be under a day.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from app.clock import IST, iso

TIER = "dhan_rolling_option_ltp"
EXPIRY_WEEKDAY_SWITCH = date(2025, 9, 1)     # NIFTY weekly/monthly expiry Thursday -> Tuesday
COLUMNS = ["ts", "session", "expiry_flag", "expiry_code", "rel_strike", "option_type", "strike", "open", "high",
           "low", "close", "volume", "oi", "iv", "spot"]
FIELDS = ("open", "high", "low", "close", "iv", "volume", "strike", "oi", "spot")


@dataclass(frozen=True)
class RollingSpec:
    flags: tuple[str, ...] = ("WEEK",)
    codes: tuple[int, ...] = (1,)
    each_side: int = 5                        # ATM-5 .. ATM+5 (API allows up to 10 near expiry)
    types: tuple[str, ...] = ("CALL", "PUT")
    underlying_id: int = 13

    def series(self) -> list[tuple[str, int, int, str]]:
        return [(f, c, k, t) for f in self.flags for c in self.codes
                for k in range(-self.each_side, self.each_side + 1) for t in self.types]


def rel_label(k: int) -> str:
    return "ATM" if k == 0 else f"ATM{k:+d}"


def root(historical_root: Path) -> Path:
    return Path(historical_root) / "dhan_rolling_options"


# ------------------------------------------------------------------------------------------ parse / store
def parse(body, day: str, flag: str, code: int, rel: int, option_type: str) -> pd.DataFrame:
    """One response -> rows of `day` (09:15-15:29 bar starts). Bad bars (non-positive / high < low) dropped."""
    d = body.get("data") if isinstance(body, dict) else None
    side = d.get("ce" if option_type == "CALL" else "pe") if isinstance(d, dict) else None
    if not isinstance(side, dict) or not side.get("timestamp"):
        return pd.DataFrame(columns=COLUMNS)
    n = len(side["timestamp"])
    df = pd.DataFrame({"ts": [int(t) for t in side["timestamp"]],
                       **{f: (side.get(f) or [None] * n)[:n] for f in FIELDS}})
    t = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert(IST)
    hm = t.dt.hour * 60 + t.dt.minute
    df = df[(t.dt.date.astype(str) == day) & (hm >= 555) & (hm <= 929)].copy()
    for c in ("open", "high", "low", "close", "spot", "strike", "iv"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df[(df[["open", "high", "low", "close"]].min(axis=1) > 0) & (df["high"] >= df["low"])]
    df["session"], df["expiry_flag"], df["expiry_code"] = day, flag, code
    df["rel_strike"], df["option_type"] = rel, "CE" if option_type == "CALL" else "PE"
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")
    df["oi"] = pd.to_numeric(df["oi"], errors="coerce").fillna(0).astype("int64")
    return df[COLUMNS].reset_index(drop=True)


def day_path(historical_root: Path, day: str) -> Path:
    return root(historical_root) / f"date={day}" / "part-0.parquet"


def write_day(historical_root: Path, day: str, df: pd.DataFrame) -> Path:
    p = day_path(historical_root, day)
    p.parent.mkdir(parents=True, exist_ok=True)
    df.sort_values(["expiry_flag", "expiry_code", "option_type", "rel_strike", "ts"]).to_parquet(p, index=False)
    return p


def load_day(historical_root: Path, day: str, with_expiry: bool = True) -> pd.DataFrame:
    p = day_path(historical_root, day)
    if not p.exists():
        return pd.DataFrame(columns=COLUMNS)
    df = pd.read_parquet(p)
    if with_expiry:
        exp = read_manifest(historical_root).get("expiries", {}).get(day, {})
        df["expiry"] = [exp.get(f"{f}{c}", {}).get("expiry") for f, c in zip(df["expiry_flag"], df["expiry_code"])]
        df["tier"] = TIER
    return df


@dataclass
class Manifest:
    days: dict = field(default_factory=dict)        # day -> {status, rows, series, fetched_at, spec}
    expiries: dict = field(default_factory=dict)    # day -> {"WEEK1": {expiry, implied_days, check}}


def manifest_path(historical_root: Path) -> Path:
    return root(historical_root) / "manifest.json"


def read_manifest(historical_root: Path) -> dict:
    p = manifest_path(historical_root)
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {"days": {}, "expiries": {}}


def write_manifest(historical_root: Path, m: dict) -> None:
    p = manifest_path(historical_root)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(m, indent=1, sort_keys=True), encoding="utf-8")
    tmp.replace(p)


# ------------------------------------------------------------------------------------------ fetch
async def fetch_day(rest, day: str, spec: RollingSpec) -> tuple[pd.DataFrame, dict]:
    parts, errors = [], []
    for flag, code, rel, ot in spec.series():
        r = await rest.rolling_option(spec.underlying_id, flag, code, rel_label(rel), ot, day, day)
        if r.status != 200:
            errors.append({"series": f"{flag}{code}/{rel_label(rel)}/{ot}", "http": r.status, "text": r.text[:120]})
            continue
        df = parse(r.body, day, flag, code, rel, ot)
        if len(df):
            parts.append(df)
    df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=COLUMNS)
    info = {"status": "ok" if len(df) else ("error" if errors else "empty"), "rows": int(len(df)),
            "series": int(df.groupby(["expiry_flag", "expiry_code", "rel_strike", "option_type"]).ngroups)
            if len(df) else 0, "fetched_at": iso(), "spec": spec.__dict__ | {"types": list(spec.types)},
            "errors": errors[:5]}
    return df, info


def weekdays(d_from: date, d_to: date) -> list[str]:
    out, d = [], d_from
    while d <= d_to:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


# ------------------------------------------------------------------------------------------ expiry inference
def implied_days(df: pd.DataFrame) -> dict[tuple[str, int], float]:
    """(flag, code) -> median implied calendar days to expiry from the ATM call between 10:00 and 14:59."""
    from scipy.optimize import brentq

    from app.pricing.bs import price
    out = {}
    t = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert(IST)
    m = (df["rel_strike"] == 0) & (df["option_type"] == "CE") & t.dt.hour.between(10, 14) & (df["iv"] > 0)
    for (flag, code), g in df[m].groupby(["expiry_flag", "expiry_code"]):
        vals = []
        for r in g.itertuples(index=False):
            f = lambda T: price(r.spot, r.strike, T, r.iv / 100, "CE") - r.close  # noqa: E731
            try:
                if f(1e-6) < 0 < f(1.0):
                    vals.append(brentq(f, 1e-6, 1.0) * 365)
            except (ValueError, ZeroDivisionError):
                continue
        if len(vals) >= 5:
            out[(flag, int(code))] = float(np.median(vals))
    return out


def expiry_weekday(d: date) -> int:
    return 1 if d >= EXPIRY_WEEKDAY_SWITCH else 3


def _candidates(day: date, flag: str, horizon_days: int = 70) -> list[date]:
    out = []
    for n in range(horizon_days):
        c = day + timedelta(days=n)
        if c.weekday() != expiry_weekday(c):
            continue
        if flag == "MONTH" and (c + timedelta(days=7)).month == c.month:
            continue                                            # not the last such weekday of its month
        out.append(c)
    return out


def infer_expiry(day: str, flag: str, code: int, implied: float | None, trading_days: set[str]) -> dict:
    d = date.fromisoformat(day)
    cands = _candidates(d, flag)
    # holiday shift first, so a Thursday holiday's Wednesday expiry is a candidate in its own right
    last_known = max(trading_days, default="")
    shifted = []
    for c in cands:
        e = c
        if c.isoformat() <= last_known:                  # beyond the imported range a holiday cannot be seen
            while e.isoformat() not in trading_days and e > d and (c - e).days < 4:
                e -= timedelta(days=1)
        shifted.append(e)
    if not shifted:
        return {"expiry": None, "implied_days": implied, "method": "none"}
    if implied is None:
        e = shifted[code - 1] if len(shifted) >= code else None
        method = "calendar_only"
    else:
        e = min(shifted, key=lambda c: abs((c - d).days + 0.5 - implied))   # implied T counts to the close
        method = "implied_T+weekday"
    return {"expiry": e.isoformat() if e else None, "implied_days": None if implied is None else round(implied, 3),
            "method": method, "dte": (e - d).days if e else None}


def finalize(historical_root: Path) -> dict:
    """Infer expiries for every imported day and check them; writes them into the manifest."""
    m = read_manifest(historical_root)
    trading = {d for d, v in m["days"].items() if v.get("status") == "ok"}
    expiries, problems = {}, []
    for day in sorted(trading):
        df = load_day(historical_root, day, with_expiry=False)
        imp = implied_days(df)
        expiries[day] = {}
        for flag, code in sorted({(f, int(c)) for f, c in zip(df["expiry_flag"], df["expiry_code"])}):
            expiries[day][f"{flag}{code}"] = infer_expiry(day, flag, code, imp.get((flag, code)), trading)
    for day, per in expiries.items():           # on an inferred expiry day the implied time must be < 1 day
        for key, e in per.items():
            if e["expiry"] == day and e["implied_days"] is not None and e["implied_days"] > 1.0:
                problems.append({"day": day, "series": key, "implied_days": e["implied_days"]})
            if e["expiry"] is None:
                problems.append({"day": day, "series": key, "error": "no expiry"})
    m["expiries"] = expiries
    m["finalized_at"] = iso()
    m["expiry_check"] = {"problems": problems[:50], "count": len(problems)}
    write_manifest(historical_root, m)
    return {"days": len(expiries), "problems": len(problems), "examples": problems[:5]}


def coverage(historical_root: Path) -> dict:
    m = read_manifest(historical_root)
    st = pd.Series({d: v.get("status") for d, v in m["days"].items()})
    ok = sorted(st[st == "ok"].index) if len(st) else []
    return {"days_requested": int(len(st)), "trading_days": len(ok),
            "empty_days": int((st == "empty").sum()) if len(st) else 0,
            "error_days": sorted(st[st == "error"].index)[:20] if len(st) else [],
            "first": ok[0] if ok else None, "last": ok[-1] if ok else None,
            "rows": int(sum(v.get("rows", 0) for v in m["days"].values())),
            "finalized": bool(m.get("finalized_at")), "expiry_problems": (m.get("expiry_check") or {}).get("count")}


def now_ist() -> datetime:
    return datetime.now(IST)
