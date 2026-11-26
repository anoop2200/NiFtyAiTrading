"""Strategy PRE-SCREEN on Dhan rolling-option trade prices (OI-03 research tier) — not evidence for promotion.

The quote-based backtest (Phase 4-6) has 59 sessions. Dhan's expired-option endpoint gives a year of 1-minute
TRADE prices but no bid/ask (v1.3 §18 forbids LTP-only fills and invented bid/ask in evidence). The screen runs
the SAME production pipeline (strategies, contract ranking, risk engine, paper broker, fees) on quotes that are
explicitly *modelled* from those trade prices:

  quote at each minute's close (available at bar end, never earlier):
      mid = minute close;  bid = mid - half spread;  ask = mid + half spread (tick-rounded, never < 1 tick)
      spread = median relative spread observed in the PG snapshot history for that DTE bucket x moneyness,
               times a stress multiplier (1x / 2x / 4x reported side by side)
      size   = `quote_lots` lots on each side, top of book only (has_depth=False -> the fill model adds its
               low-fidelity slippage tick)
  index bars: Dhan NIFTY 50 1-minute candles; VIX: Dhan India VIX candles
  lot size: the GCD of that day's open-interest figures when it is 65 or 75, else the NSE revision date

Every number the screen produces is labelled `screen_ltp_modelled_spread`. It can rank ideas worth a proper
experiment on recorded quotes; it cannot approve anything, consume a trial, or touch a hold-out. A bias check
re-runs the screen on the PG overlap days and compares it with the quote-based backtest of the same days.
"""
from __future__ import annotations

import glob
import hashlib
import json
import math
from dataclasses import dataclass, field, replace
from pathlib import Path

import pandas as pd

from app.backtest.fees import FeeSchedule
from app.backtest.fills import NS, FillConfig, Quote
from app.backtest.history import contract_key
from app.historical import rolling_options as ro
from app.historical import vix as vixh
from app.market.calendar import EventCalendar, TradingCalendar
from app.pipeline.decision import gap_ratios
from app.research.simulate import HistoryContext, simulate

TIER = "screen_ltp_modelled_spread"
LABEL = f"{TIER}/dhan_rolling_option_ltp"
LOT_REVISION = "2025-12-31"            # first session with lot 65 per the OI evidence (75 before)
DTE_BUCKETS = ((0, "0"), (1, "1"), (3, "2-3"), (10_000, "4+"))
FILL = FillConfig(max_quote_age_s=61.0, version="fill-1/screen-minute")   # one modelled quote per minute


def dte_bucket(dte: int) -> str:
    for hi, name in DTE_BUCKETS:
        if dte <= hi:
            return name
    return "4+"


# ------------------------------------------------------------------------------------------ spread model
def calibrate_spreads(derived: Path, step: float = 50.0) -> dict:
    """Median relative spread (% of mid) by DTE bucket x moneyness (|strike - spot| / step, capped at 6) from
    the PG snapshot option bars. Returns {"table": {bucket: {mny: pct}}, "sessions": n, "sha256": ...}."""
    rows, days = [], []
    for f in sorted(glob.glob(str(Path(derived) / "pg_option_bars_1m" / "date=*" / "part-0.parquet"))):
        d = f.split("date=")[1][:10]
        sp = Path(derived) / "pg_spot_bars_1m" / f"date={d}" / "part-0.parquet"
        if not sp.exists():
            continue
        o = pd.read_parquet(f, columns=["expiry", "strike", "ts", "bid", "ask"])
        s = pd.read_parquet(sp, columns=["ts", "close"]).rename(columns={"close": "spot"})
        o = o.merge(s, on="ts")
        o = o[(o["bid"] > 0) & (o["ask"] >= o["bid"])]
        dte = (pd.to_datetime(o["expiry"].astype(str).str[:10]) - pd.Timestamp(d)).dt.days
        rows.append(pd.DataFrame({"b": dte.map(dte_bucket),
                                  "m": ((o["strike"] - o["spot"]).abs() / step).round().clip(upper=6).astype(int),
                                  "pct": 100 * (o["ask"] - o["bid"]) / ((o["ask"] + o["bid"]) / 2)}))
        days.append(d)
    if not rows:
        raise FileNotFoundError("no PG option bars to calibrate the spread model from")
    a = pd.concat(rows)
    med = a.groupby(["b", "m"])["pct"].median()
    table = {b: {int(m): round(float(v), 4) for m, v in med[b].items()} for b in med.index.get_level_values(0).unique()}
    blob = json.dumps(table, sort_keys=True)
    return {"table": table, "sessions": len(days), "first": days[0], "last": days[-1],
            "sha256": hashlib.sha256(blob.encode()).hexdigest()}


def spread_pct(model: dict, dte: int, mny: int) -> float:
    t = model["table"].get(dte_bucket(dte)) or model["table"]["4+"]
    mny = max(0, min(int(mny), max(int(k) for k in t)))
    return float(t.get(mny, t.get(str(mny), 0.25)))


def modelled_quotes(df: pd.DataFrame, model: dict, multiplier: float, lot: int, quote_lots: int = 20,
                    tick: float = 0.05, step: float = 50.0) -> list[Quote]:
    """Rolling-option minute bars -> top-of-book quotes at each bar END (when the close is known)."""
    out = []
    if df.empty:
        return out
    day = df["session"].iloc[0]
    qty = lot * quote_lots
    for r in df.itertuples(index=False):
        if not r.expiry or not (r.close > 0):
            continue
        dte = (pd.Timestamp(r.expiry) - pd.Timestamp(day)).days
        mny = round(abs(r.strike - r.spot) / step)
        half = max(tick, r.close * spread_pct(model, dte, mny) * multiplier / 200)
        bid = max(tick, math.floor((r.close - half) / tick + 1e-9) * tick)
        ask = math.ceil((r.close + half) / tick - 1e-9) * tick
        if ask - bid < tick:
            ask = bid + tick
        out.append(Quote((int(r.ts) + 60) * NS, contract_key(r.expiry, r.strike, r.option_type),
                         ((round(bid, 2), qty),), ((round(ask, 2), qty),), ltp=float(r.close), tier=TIER,
                         has_depth=False))
    out.sort(key=lambda q: (q.ts_ns, q.key))
    return out


def lot_for_day(df: pd.DataFrame, day: str) -> tuple[int, str]:
    g = 0
    for v in df["oi"]:
        if v and v > 0:
            g = math.gcd(g, int(v))
    if g in (65, 75):
        return g, "oi_gcd"
    return (75 if day < LOT_REVISION else 65), f"revision_date (oi_gcd {g})"


# ------------------------------------------------------------------------------------------ context
@dataclass
class ScreenContext(HistoryContext):
    """HistoryContext whose quotes are modelled from rolling-option trade prices."""
    spread_model: dict = field(default_factory=dict)
    multiplier: float = 1.0
    quote_lots: int = 20
    lots: dict = field(default_factory=dict)
    _cache: dict = field(default_factory=dict)

    @classmethod
    def build(cls, cfg, days: list[str], spread_model: dict, multiplier: float = 1.0,
              quote_lots: int = 20) -> "ScreenContext":
        hist = cfg.path("historical")
        index = vixh.load(hist, days, name="nifty_index")
        if index.empty:
            raise FileNotFoundError("no NIFTY index bars - run backfill_vix.py --days-from rolling --nifty")
        opens = pd.to_datetime(index["session"] + " 09:15").dt.tz_localize("Asia/Kolkata")
        index["session_open_ts"] = (opens - pd.Timestamp(0, tz="UTC")) // pd.Timedelta(1, "s")
        have = set(index.groupby("session").size().loc[lambda s: s >= 370].index)
        days = [d for d in days if d in have]
        quality = {d: (True, None) for d in days}
        ctx = cls(cfg, "dhan_rolling_options", days, TradingCalendar.load(), FeeSchedule.load(),
                  EventCalendar.load(), index, gap_ratios(index), quality, fill=FILL, label=LABEL,
                  vix=vixh.load(hist, days), spread_model=spread_model, multiplier=multiplier,
                  quote_lots=quote_lots)
        return ctx

    def _day(self, day: str) -> pd.DataFrame:
        if day not in self._cache:
            self._cache.clear()                      # one day at a time (simulate's outer loop)
            self._cache[day] = ro.load_day(self.cfg.path("historical"), day)
        return self._cache[day]

    def day_lot(self, day: str) -> int:
        lot, how = lot_for_day(self._day(day), day)
        self.lots[day] = {"lot": lot, "evidence": how}
        return lot

    def day_quotes(self, day: str) -> list:
        return modelled_quotes(self._day(day), self.spread_model, self.multiplier, self.lots[day]["lot"],
                               self.quote_lots)

    def data_fingerprint(self) -> dict:
        h = hashlib.sha256()
        m = ro.read_manifest(self.cfg.path("historical"))
        h.update(json.dumps({d: m["days"].get(d) for d in self.days}, sort_keys=True, default=str).encode())
        h.update(pd.util.hash_pandas_object(self.index[["ts", "close"]], index=False).values.tobytes())
        return {"tier": TIER, "label": self.label, "sessions": len(self.days), "first": self.days[0],
                "last": self.days[-1], "sha256": h.hexdigest(), "spread_model_sha256": self.spread_model["sha256"],
                "multiplier": self.multiplier}


# ------------------------------------------------------------------------------------------ results
def summarize(trades: list[dict], signals: list[dict], sessions: int) -> dict:
    t = pd.DataFrame(trades)
    if t.empty:
        return {"sessions": sessions, "trades": 0, "net_pnl": 0.0}
    net = t["net_pnl"].astype(float)
    by_day = t.groupby("session")["net_pnl"].sum()
    eq = by_day.cumsum()
    out = {"sessions": sessions, "trades": int(len(t)), "net_pnl": round(float(net.sum()), 2),
           "gross_pnl": round(float(t["gross_pnl"].sum()), 2), "charges": round(float(t["charges"].sum()), 2),
           "win_rate": round(float((net > 0).mean()), 4), "avg_net": round(float(net.mean()), 2),
           "profit_factor": round(float(net[net > 0].sum() / -net[net < 0].sum()), 3) if (net < 0).any() else None,
           "max_drawdown": round(float((eq - eq.cummax()).min()), 2),
           "positive_days": int((by_day > 0).sum()), "negative_days": int((by_day < 0).sum()),
           "exit_reasons": t["exit_reason"].value_counts().to_dict() if "exit_reason" in t else {}}
    out["by_strategy"] = {k: {"trades": int(len(g)), "net_pnl": round(float(g["net_pnl"].sum()), 2),
                              "win_rate": round(float((g["net_pnl"] > 0).mean()), 4)}
                          for k, g in t.groupby("strategy")}
    t["month"] = t["session"].str[:7]
    out["by_month"] = {k: round(float(v), 2) for k, v in t.groupby("month")["net_pnl"].sum().items()}
    out["by_vix_tercile"] = vix_breakdown(t, signals)
    return out


def vix_breakdown(t: pd.DataFrame, signals: list[dict]) -> dict:
    """EXPLORATORY: net P&L by the VIX recorded at the entry decision, split at the year's VIX terciles."""
    vix = {}
    for s in signals:
        if s.get("outcome") == "APPROVED" and s.get("cycle_key"):
            v = ((s.get("payload") or {}).get("market") or {}).get("vix")
            vix[(s["session"], s["cycle_key"])] = v
    t = t.assign(vix=[vix.get((s, c)) for s, c in zip(t["session"], t["cycle"])]).dropna(subset=["vix"])
    if len(t) < 9:
        return {}
    q = t["vix"].quantile([1 / 3, 2 / 3]).tolist()
    t["band"] = pd.cut(t["vix"], [-1, q[0], q[1], 1e9], labels=[f"low (<{q[0]:.1f})", "mid",
                                                                    f"high (>{q[1]:.1f})"])
    return {str(k): {"trades": int(len(g)), "net_pnl": round(float(g["net_pnl"].sum()), 2),
                     "win_rate": round(float((g["net_pnl"] > 0).mean()), 4)}
            for k, g in t.groupby("band", observed=True)}


def run_screen(ctx: ScreenContext, strategies_factory, days: list[str] | None = None) -> dict:
    runs = simulate(ctx, {"screen": strategies_factory()}, days=days)
    run = runs["screen"]
    return {"summary": summarize(run.trades, run.signals, len(days or ctx.days)), "trades": run.trades,
            "signals": len(run.signals), "audit_ok": run.audit_ok, "audit_hash": run.audit_hash,
            "lots": dict(ctx.lots)}


__all__ = ["TIER", "LABEL", "FILL", "calibrate_spreads", "spread_pct", "modelled_quotes", "lot_for_day",
           "ScreenContext", "summarize", "run_screen", "replace"]
