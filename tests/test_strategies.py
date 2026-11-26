"""Phase 5: strategies (ORB, trend pullback), contract ranking, decision pipeline wiring, and the static
guarantee that nothing under app/ can place a live order."""
from __future__ import annotations

import random
import re
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pytest

from app.backtest.audit import AuditLog
from app.backtest.broker import PaperBroker
from app.backtest.fees import FeeSchedule
from app.backtest.fills import NS, FillConfig, Quote
from app.backtest.replay import ReplayEngine
from app.clock import IST
from app.config import PROJECT_ROOT
from app.indicators.engine import IndicatorEngine
from app.market.calendar import TradingCalendar
from app.pipeline.decision import DecisionPipeline, PipelineConfig, gap_ratios
from app.pricing.bs import price, year_fraction
from app.risk.engine import RiskEngine
from app.risk.killswitch import KillSwitch
from app.strategies.base import Candidate, NoTrade
from app.strategies.contracts import SelectionConfig, select_contract
from app.strategies.orb import OpeningRangeBreakout, ORBConfig
from app.strategies.pullback import PullbackConfig, TrendPullback

DAY = "2026-09-02"
EXP = "2026-09-08"
CAL = TradingCalendar.load()
FEES = FeeSchedule.load()


def ts(hhmm: str, day: str = DAY) -> int:
    h, m = map(int, hhmm.split(":"))
    return int(datetime.fromisoformat(day).replace(hour=h, minute=m, tzinfo=IST).timestamp())


# ====================================================================== ORB
def orb_ind(o: OpeningRangeBreakout, atr=10.0, frv=None, hi=24020.0, lo=23980.0):
    d = {o.or_key: {"valid": True, "or_high": hi, "or_low": lo, "or_width_pct": 100 * (hi - lo) / lo},
         o.atr_key: {"valid": True, "atr": atr}}
    if frv is not None:
        d[o.frv_key] = {"valid": True, "rel_vol": frv}
    return d


def bar(c, h=None, l=None, t="09:40"):
    return {"ts": ts(t), "open": c, "high": h if h is not None else c + 2, "low": l if l is not None else c - 20,
            "close": c, "session": DAY}


def test_orb_confirmed_break_creates_candidate_with_invalidation():
    o = OpeningRangeBreakout(ORBConfig(require_futures=False))
    out = o.on_bar(DAY, bar(24030, 24032, 24015), orb_ind(o))
    assert len(out) == 1 and isinstance(out[0], Candidate)
    c = out[0]
    assert c.direction == 1 and c.invalidation == 24020.0 and c.setup_id == "orb1" and c.fresh_confirmation
    assert c.cycle_key(DAY) == f"NIFTY|{DAY}|orb|orb1|CE"
    assert c.features["futures_check"].startswith("skipped")
    assert o.on_bar(DAY, bar(24040, 24045, 24020, "09:41"), orb_ind(o)) == []        # already outside: no repeat


@pytest.mark.parametrize("b,ind_kw,cfg_kw,reason", [
    (bar(24021, 24023, 24000), {}, {"require_futures": False}, "BREAK_NOT_CONFIRMED"),
    (bar(24030, 24031, 24025), {}, {"require_futures": False}, "NO_RANGE_EXPANSION"),
    (bar(24030, 24032, 24015), {}, {"require_futures": True}, "FUTURES_DATA_MISSING"),
    (bar(24030, 24032, 24015), {"frv": 0.8}, {"require_futures": True}, "NO_FUTURES_PARTICIPATION"),
    (bar(24030, 24032, 24015), {"hi": 24002.0, "lo": 24000.0}, {"require_futures": False}, "RANGE_OUT_OF_BOUNDS"),
])
def test_orb_no_trade_reasons(b, ind_kw, cfg_kw, reason):
    o = OpeningRangeBreakout(ORBConfig(**cfg_kw))
    out = o.on_bar(DAY, b, orb_ind(o, **ind_kw))
    assert len(out) == 1 and isinstance(out[0], NoTrade) and out[0].reason == reason


def test_orb_futures_participation_passes_when_present():
    o = OpeningRangeBreakout(ORBConfig(require_futures=True))
    out = o.on_bar(DAY, bar(24030, 24032, 24015), orb_ind(o, frv=1.6))
    assert isinstance(out[0], Candidate) and out[0].features["fut_rel_volume"] == 1.6


def test_orb_failed_break_then_opposite_break_is_new_fresh_setup():
    o = OpeningRangeBreakout(ORBConfig(require_futures=False))
    assert isinstance(o.on_bar(DAY, bar(24030, 24032, 24015), orb_ind(o))[0], Candidate)
    assert o.on_bar(DAY, bar(24000, t="09:45"), orb_ind(o)) == []                     # back inside: setup over
    out = o.on_bar(DAY, bar(23970, 23985, 23965, "09:50"), orb_ind(o))
    assert out[0].direction == -1 and out[0].setup_id == "orb2" and out[0].fresh_confirmation
    assert out[0].cycle_key(DAY).endswith("orb2|PE")


def test_orb_new_session_resets_setup_counter():
    o = OpeningRangeBreakout(ORBConfig(require_futures=False))
    o.on_bar(DAY, bar(24030, 24032, 24015), orb_ind(o))
    out = o.on_bar("2026-09-03", bar(24030, 24032, 24015) | {"session": "2026-09-03"}, orb_ind(o))
    assert out[0].setup_id == "orb1"


# ====================================================================== pullback
def feed_5m(p: TrendPullback, t5: str, o, h, l, c, ind: dict) -> list:
    """Feed one 5-minute bar as five 1-minute bars; indicator outputs carry the 5m bucket ts."""
    start = ts(t5)
    out = []
    for i in range(5):
        b = {"ts": start + 60 * i, "session": DAY, "session_open_ts": ts("09:15"),
             "open": o if i == 0 else c, "high": h, "low": l, "close": c, "volume": 0}
        out += p.on_bar(DAY, b, ind if i == 4 else {})
    return out


def pb_ind(p, t5, fast=24050, slow=24000, adx=28, pdi=30, mdi=15, rsi=58, atr=20, vwap=None):
    t = ts(t5)
    k = p.keys
    d = {k["fast"]: {"ts": t, "valid": True, "ema": fast}, k["slow"]: {"ts": t, "valid": True, "ema": slow},
         k["adx"]: {"ts": t, "valid": True, "adx": adx, "plus_di": pdi, "minus_di": mdi},
         k["rsi"]: {"ts": t, "valid": True, "rsi": rsi}, k["atr"]: {"ts": t, "valid": True, "atr": atr}}
    if vwap is not None:
        d[k["vwap"]] = {"ts": t, "valid": True, "dist_pct": vwap}
    return d


def test_pullback_touch_then_resumption_creates_candidate():
    p = TrendPullback(PullbackConfig(require_futures=False))
    assert feed_5m(p, "10:00", 24040, 24045, 23995, 24010, pb_ind(p, "10:00")) == []     # touches EMA21: arm
    out = feed_5m(p, "10:05", 24010, 24060, 24008, 24055, pb_ind(p, "10:05"))           # closes > prev high
    assert len(out) == 1 and isinstance(out[0], Candidate)
    c = out[0]
    assert c.direction == 1 and c.setup_id == "pb1" and c.invalidation == pytest.approx(24000 - 0.5 * 20)


def test_pullback_needs_trend_strength():
    p = TrendPullback(PullbackConfig(require_futures=False))
    feed_5m(p, "10:00", 24040, 24045, 23995, 24010, pb_ind(p, "10:00", adx=15))
    assert feed_5m(p, "10:05", 24010, 24060, 24008, 24055, pb_ind(p, "10:05", adx=15)) == []


def test_pullback_needs_rsi_on_trend_side():
    p = TrendPullback(PullbackConfig(require_futures=False))
    feed_5m(p, "10:00", 24040, 24045, 23995, 24010, pb_ind(p, "10:00"))
    assert feed_5m(p, "10:05", 24010, 24060, 24008, 24055, pb_ind(p, "10:05", rsi=45)) == []


@pytest.mark.parametrize("vwap,reason", [(None, "FUTURES_DATA_MISSING"), (-0.1, "FUTURES_VWAP_DISAGREES")])
def test_pullback_futures_vwap_rules(vwap, reason):
    p = TrendPullback(PullbackConfig(require_futures=True))
    feed_5m(p, "10:00", 24040, 24045, 23995, 24010, pb_ind(p, "10:00", vwap=vwap))
    out = feed_5m(p, "10:05", 24010, 24060, 24008, 24055, pb_ind(p, "10:05", vwap=vwap))
    assert isinstance(out[0], NoTrade) and out[0].reason == reason


def test_pullback_downtrend_mirror():
    p = TrendPullback(PullbackConfig(require_futures=False))
    kw = dict(fast=23950, slow=24000, pdi=12, mdi=30, rsi=40)
    feed_5m(p, "10:00", 23960, 24005, 23955, 23990, pb_ind(p, "10:00", **kw))
    out = feed_5m(p, "10:05", 23990, 23992, 23940, 23945, pb_ind(p, "10:05", **kw))
    assert out[0].direction == -1 and out[0].invalidation == pytest.approx(24010)


def test_pullback_ignores_stale_indicator_rows():
    p = TrendPullback(PullbackConfig(require_futures=False))
    feed_5m(p, "10:00", 24040, 24045, 23995, 24010, pb_ind(p, "10:00"))
    assert feed_5m(p, "10:05", 24010, 24060, 24008, 24055, pb_ind(p, "10:00")) == []    # rows from older bar


# ====================================================================== contract ranking
def chain(spot: float, now: datetime, expiries=(EXP,), iv=0.12, age_s=1, strikes=range(23700, 24301, 50)):
    out = {}
    for e in expiries:
        t = year_fraction(now, date.fromisoformat(e))
        for k in strikes:
            for cp in ("CE", "PE"):
                p = max(price(spot, k, t, iv, cp), 0.05)
                b = round(round(p / 0.05) * 0.05 - 0.05, 2)
                key = f"NIFTY|{e}|{k}|{cp}"
                out[key] = Quote(int((now.timestamp() - age_s) * NS), key, ((b, 1000),), ((round(b + 0.10, 2), 1000),))
    return out


def test_contract_selection_picks_near_atm_eligible_and_is_deterministic():
    now = datetime(2026, 9, 2, 10, 0, tzinfo=IST)
    q = chain(24010, now)
    s1 = select_contract(1, 24010, int(now.timestamp() * NS), q, CAL)
    assert s1.key and s1.key.endswith("|CE") and s1.dte == CAL.dte(date(2026, 9, 8), date(2026, 9, 2))
    assert 0.35 <= abs(s1.features["delta"]) <= 0.65 and s1.features["eligible"] >= 1
    items = list(q.items())
    random.Random(7).shuffle(items)
    s2 = select_contract(1, 24010, int(now.timestamp() * NS), dict(items), CAL)
    assert s2.key == s1.key
    pe = select_contract(-1, 24010, int(now.timestamp() * NS), q, CAL)
    assert pe.key.endswith("|PE")
    reasons = {r["rejected"] for r in s1.evaluated}
    assert "DELTA_OUT_OF_RANGE" in reasons                # deep ITM/OTM strikes evaluated and rejected


def test_contract_selection_rejects_stale_wide_and_cheap_quotes():
    now = datetime(2026, 9, 2, 10, 0, tzinfo=IST)
    now_ns = int(now.timestamp() * NS)
    assert select_contract(1, 24010, now_ns, chain(24010, now, age_s=60), CAL).reason == "NO_ELIGIBLE_CONTRACT"
    wide = {k: Quote(q.ts_ns, k, ((q.bid - 5, 1000),), q.asks) for k, q in chain(24010, now).items()}
    s = select_contract(1, 24010, now_ns, wide, CAL)
    assert s.key is None and {r["rejected"] for r in s.evaluated} <= {"SPREAD_TOO_WIDE", "BELOW_MIN_PREMIUM"}
    cheap = select_contract(1, 24010, now_ns, chain(24010, now, iv=0.01), CAL, SelectionConfig(min_premium=500))
    assert cheap.key is None


def test_contract_selection_expiry_day_allows_next_expiry_only_then():
    now = datetime(2026, 9, 8, 10, 0, tzinfo=IST)
    q = chain(24010, now, expiries=("2026-09-08", "2026-09-15", "2026-09-22"))
    s = select_contract(1, 24010, int(now.timestamp() * NS), q, CAL)
    assert {r["expiry"] for r in s.evaluated} == {"2026-09-08", "2026-09-15"}
    now2 = datetime(2026, 9, 2, 10, 0, tzinfo=IST)
    s2 = select_contract(1, 24010, int(now2.timestamp() * NS), chain(24010, now2, expiries=(EXP, "2026-09-15")), CAL)
    assert {r["expiry"] for r in s2.evaluated} == {EXP}


# ====================================================================== pipeline (end to end on synthetic data)
def synthetic_day():
    rows, path = [], []
    for m in range(0, 106):                               # 09:15 .. 11:00
        t = ts("09:15") + 60 * m
        if m < 25:
            c = 24000 + (15 if m % 2 else -15)
            o, h, l = 24000, c + 5 if c > 24000 else 24005, c - 5 if c < 24000 else 23995
        elif m == 25:                                     # 09:40 breakout bar
            o, h, l, c = 24010, 24062, 24008, 24060
        else:
            c = min(24060 + 4 * (m - 25), 24250)
            o, h, l = c - 3, c + 2, c - 6
        rows.append({"ts": t, "session": DAY, "session_open_ts": ts("09:15"), "open": o, "high": h, "low": l,
                     "close": c, "volume": 0})
        path.append((t, o, c))
    index = pd.DataFrame(rows)
    quotes = []
    for t, o, c in path:
        for s in range(0, 60, 5):
            spot = o + (c - o) * s / 60
            now = datetime.fromtimestamp(t + s, IST)
            for q in chain(spot, now, strikes=range(23800, 24301, 50), age_s=0).values():
                quotes.append(q)
    return index, quotes


SYN = None


def run_pipeline(risk: RiskEngine, data_ok=True, gap=0.2):
    global SYN
    if SYN is None:
        SYN = synthetic_day()
    index, quotes = SYN
    pipe = DecisionPipeline([OpeningRangeBreakout(ORBConfig(require_futures=False))], risk, CAL, 65,
                            PipelineConfig(feed_liveness_s=15.0))
    pipe.start_session(DAY, gap, data_ok, None if data_ok else "TEST")
    broker = PaperBroker(FillConfig(max_quote_age_s=15.0), FEES, AuditLog())
    pipe.attach(broker)
    ReplayEngine(broker, pipe, IndicatorEngine(pipe.bindings)).run({"index": index}, quotes)
    return pipe, broker


def test_pipeline_end_to_end_approves_trades_and_keeps_risk_consistent():
    risk = RiskEngine(fees=FEES, kill=KillSwitch(None))
    pipe, broker = run_pipeline(risk)
    approved = [s for s in pipe.signals if s["outcome"] == "APPROVED"]
    assert len(approved) == 1 and approved[0]["cycle_key"] == f"NIFTY|{DAY}|orb|orb1|CE"
    assert approved[0]["contract"].endswith("|CE") and approved[0]["planned_r_inr"] > 0
    assert all(c["ok"] for c in approved[0]["payload"]["risk_checks"].values())
    assert len(broker.trades) == 1 and broker.trades[0]["lots"] == 1
    assert risk.state.entries == 1 and not risk.state.open and not risk.state.pending
    assert risk.kill.auto is None
    assert AuditLog.verify(broker.audit.records)


def test_pipeline_missing_risk_config_never_trades(tmp_path):
    risk = RiskEngine(config_path=tmp_path / "missing.yaml", fees=FEES, kill=KillSwitch(None))
    pipe, broker = run_pipeline(risk)
    assert broker.trades == [] and not broker.positions
    assert pipe.signals and all(s["outcome"] != "APPROVED" for s in pipe.signals)
    assert any("RISK_CONFIG_MISSING" in s["reasons"] for s in pipe.signals)


def test_pipeline_manual_kill_switch_blocks_entries():
    ks = KillSwitch(None)
    ks.set_manual(True, "test")
    pipe, broker = run_pipeline(RiskEngine(fees=FEES, kill=ks))
    assert broker.trades == []
    assert any("KILL_SWITCH" in s["reasons"] for s in pipe.signals)


def test_pipeline_data_quality_gate_blocks_candidates():
    pipe, broker = run_pipeline(RiskEngine(fees=FEES, kill=KillSwitch(None)), data_ok=False)
    assert broker.trades == []
    assert any(r.startswith("DATA_QUALITY_DAY") for s in pipe.signals for r in s["reasons"])


def test_pipeline_emergency_close_exits_open_positions():
    ks = KillSwitch(None)
    risk = RiskEngine(fees=FEES, kill=ks)

    class Trip:      # flips the emergency switch right after the position opens
        def on_broker_event(self, kind, payload):
            if kind == "position_opened":
                ks.set_manual(True, "test", emergency_close=True)
    index, quotes = SYN or synthetic_day()
    pipe = DecisionPipeline([OpeningRangeBreakout(ORBConfig(require_futures=False))], risk, CAL, 65,
                            PipelineConfig(feed_liveness_s=15.0))
    pipe.start_session(DAY, 0.2)
    broker = PaperBroker(FillConfig(max_quote_age_s=15.0), FEES, AuditLog())
    pipe.attach(broker)
    broker.listeners.append(Trip())
    ReplayEngine(broker, pipe, IndicatorEngine(pipe.bindings)).run({"index": index}, quotes)
    assert len(broker.trades) == 1 and broker.trades[0]["exit_reason"] == "KILL_SWITCH_EMERGENCY_CLOSE"


def test_gap_ratio_unknown_without_history_and_computed_after():
    rows = []
    for i, d in enumerate(pd.bdate_range("2026-08-03", periods=8)):
        s = d.date().isoformat()
        base = 24000 + 10 * i
        for m, px in enumerate([base, base + 50, base - 50, base + 10]):
            rows.append({"ts": ts("09:15", s) + 60 * m, "session": s, "open": px, "high": px + 1, "low": px - 1,
                         "close": px})
    g = gap_ratios(pd.DataFrame(rows))
    vals = list(g.values())
    assert vals[0] is None and vals[4] is None           # fewer than 5 prior sessions: unknown -> fail closed
    assert vals[6] is not None and vals[6] >= 0


# ====================================================================== safety: no live order paths
ORDER_PATTERNS = [r"/orders?\b", r"place_order", r"modify_order", r"cancel_order", r"dhanhq", r"/super/orders",
                  r"/forever/orders", r"placeOrder"]


def test_no_live_order_calls_anywhere_in_app():
    hits = []
    for f in (PROJECT_ROOT / "app").rglob("*.py"):
        text = f.read_text(encoding="utf-8")
        for p in ORDER_PATTERNS:
            for m in re.finditer(p, text):
                hits.append(f"{f.relative_to(PROJECT_ROOT)}: {m.group(0)}")
    assert hits == [], hits


def test_risk_engine_does_not_import_strategies_or_models():
    src = (PROJECT_ROOT / "app" / "risk" / "engine.py").read_text(encoding="utf-8")
    assert not re.search(r"^from app\.(strategies|indicators|pipeline|ml)", src, re.M)
