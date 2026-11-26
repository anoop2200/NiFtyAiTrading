"""Phase 5 acceptance: every risk limit, missing/invalid config, cycle / duplicate / opposite-direction rules,
kill switch (manual, automatic, acknowledgement), exception -> NO TRADE, loss-latch reset rules."""
from __future__ import annotations

import copy
from datetime import datetime

import pytest
import yaml

from app.backtest.fees import FeeSchedule
from app.market.calendar import EventCalendar, MarketEvent
from app.clock import IST
from app.risk.config import RISK_FILE, RiskConfig, RiskConfigError
from app.risk.engine import NS, RiskEngine, RiskRequest
from app.risk.killswitch import KillSwitch

DAY = "2026-09-02"
RAW = yaml.safe_load(RISK_FILE.read_text(encoding="utf-8"))
FEES = FeeSchedule.load()


def ns(hhmm: str, day: str = DAY, sec: int = 0) -> int:
    h, m = map(int, hhmm.split(":"))
    return int(datetime.fromisoformat(day).replace(hour=h, minute=m, second=sec, tzinfo=IST).timestamp()) * NS


def engine(raw=None, events=None, kill=None, gap=0.2) -> RiskEngine:
    e = RiskEngine(fees=FEES, events=events or EventCalendar([]), kill=kill or KillSwitch(None),
                   config=RiskConfig.from_dict(copy.deepcopy(raw or RAW)))
    e.start_session(DAY, gap)
    return e


def req(t="10:00", cycle="NIFTY|2026-09-02|orb|orb1|CE", strategy="orb", direction=1, dte=3, ask=100.0, bid=99.9,
        **kw) -> RiskRequest:
    base = dict(now_ns=ns(t), cycle_key=cycle, strategy_id=strategy, setup_id=cycle.split("|")[3], direction=direction,
                fresh_confirmation=True, contract="NIFTY|2026-09-08|24000|CE", dte=dte, ask=ask, bid=bid,
                quote_age_s=1.0, feed_alive=True, lot_size=65, stop_pct=20.0, target_pct=30.0, time_stop_s=1800)
    base.update(kw)
    return RiskRequest(**base)


def failed(dec) -> set[str]:
    return set(dec.reasons)


def open_trade(e: RiskEngine, r: RiskRequest, trade_id: int):
    d = e.evaluate(r)
    assert d.approved, d.reasons
    e.on_approved(r, d)
    e.on_entry_filled(r.cycle_key, trade_id, r.ask, 65)
    return d


def close_trade(e: RiskEngine, trade_id: int, net: float, reason="TARGET", t="10:30"):
    e.on_trade_closed({"trade_id": trade_id, "net_pnl": net, "exit_reason": reason, "exit_ns": ns(t)})


# ---------------------------------------------------------------- baseline
def test_clean_request_is_approved_with_planned_r():
    d = engine().evaluate(req())
    assert d.approved and d.lots == 1 and d.reasons == []
    # (100 - 80) * 65 = 1300 + charges + 2 ticks slippage
    assert 1300 < d.planned_r_inr < 1400
    assert d.shadow_lots == int(500000 * 0.005 // d.planned_r_inr)
    assert all(c["ok"] for c in d.checks.values())


# ---------------------------------------------------------------- config
def test_missing_config_means_no_trade(tmp_path):
    e = RiskEngine(config_path=tmp_path / "nope.yaml", fees=FEES, kill=KillSwitch(None))
    e.start_session(DAY, 0.1)
    d = e.evaluate(req())
    assert not d.approved and d.reasons == ["RISK_CONFIG_MISSING"]
    assert e.daily_loss_breached({}) is True


@pytest.mark.parametrize("mutate", [
    lambda r: r.pop("limits"),
    lambda r: r["limits"].pop("max_daily_loss_r"),
    lambda r: r["limits"].__setitem__("surprise", 1),
    lambda r: r["limits"].__setitem__("max_entries_per_day", "5"),
    lambda r: r["limits"].__setitem__("max_open_risk_r", 0),
    lambda r: r["windows"].__setitem__("last_entry", "15:30"),
    lambda r: r["windows"].__setitem__("no_entry_open_until", "9.25"),
    lambda r: r["zero_dte"].__setitem__("stop_pct", [30, 20]),
    lambda r: r["zero_dte"].__setitem__("max_entries", 9),
    lambda r: r["limits"].__setitem__("daily_loss_open_policy", "panic"),
    lambda r: r.pop("version"),
])
def test_invalid_config_rejected(mutate, tmp_path):
    raw = copy.deepcopy(RAW)
    mutate(raw)
    with pytest.raises(RiskConfigError):
        RiskConfig.from_dict(raw)
    p = tmp_path / "risk.yaml"
    p.write_text(yaml.safe_dump(raw), encoding="utf-8")
    e = RiskEngine(config_path=p, fees=FEES, kill=KillSwitch(None))
    e.start_session(DAY, 0.1)
    assert e.evaluate(req()).reasons == ["RISK_CONFIG_MISSING"]


def test_unparseable_yaml_rejected(tmp_path):
    p = tmp_path / "risk.yaml"
    p.write_text("limits: [unclosed", encoding="utf-8")
    with pytest.raises(RiskConfigError):
        RiskConfig.load(p)


# ---------------------------------------------------------------- time windows
@pytest.mark.parametrize("t,ok", [("09:20", False), ("09:25", True), ("14:59", True), ("15:00", False)])
def test_entry_windows(t, ok):
    d = engine().evaluate(req(t=t))
    assert d.approved is ok
    if not ok:
        assert failed(d) & {"OPENING_WINDOW", "CLOSING_WINDOW"}


@pytest.mark.parametrize("t,ok", [("14:29", True), ("14:30", False)])
def test_zero_dte_last_entry(t, ok):
    d = engine().evaluate(req(t=t, dte=0, stop_pct=25.0, target_pct=40.0, time_stop_s=900))
    assert d.approved is ok
    assert ok or "ZERO_DTE_LAST_ENTRY" in failed(d)


@pytest.mark.parametrize("gap,t,ok", [(1.5, "09:40", False), (1.5, "09:45", True), (None, "09:30", False),
                                      (0.5, "09:30", True)])
def test_gap_day_blocks_until_0945(gap, t, ok):
    d = engine(gap=gap).evaluate(req(t=t))
    assert d.approved is ok
    assert ok or "GAP_DAY" in failed(d)


def test_event_blackout():
    ev = EventCalendar([MarketEvent(datetime(2026, 9, 2, 10, 0, tzinfo=IST), "rbi", "RBI policy", "test", False, 15, 30)])
    assert "EVENT_BLACKOUT" in failed(engine(events=ev).evaluate(req(t="10:20")))
    assert engine(events=ev).evaluate(req(t="10:31")).approved


# ---------------------------------------------------------------- counts and exposure
def test_entries_per_day_limit():
    e = engine()
    for i in range(5):
        r = req(t=f"10:{10 * i:02d}", cycle=f"NIFTY|{DAY}|orb|orb{i}|CE")
        open_trade(e, r, i + 1)
        close_trade(e, i + 1, 100.0, t=f"10:{10 * i + 5:02d}")
    d = e.evaluate(req(t="11:00", cycle=f"NIFTY|{DAY}|orb|orb9|CE"))
    assert "ENTRIES_PER_DAY" in failed(d)


def test_zero_dte_entry_limit():
    e = engine()
    z = dict(dte=0, stop_pct=25.0, target_pct=40.0, time_stop_s=900)
    for i in range(2):
        open_trade(e, req(t=f"10:{10 * i:02d}", cycle=f"NIFTY|{DAY}|orb|z{i}|CE", **z), i + 1)
        close_trade(e, i + 1, 50.0, t=f"10:{10 * i + 5:02d}")
    assert "ZERO_DTE_ENTRIES" in failed(e.evaluate(req(t="11:00", cycle=f"NIFTY|{DAY}|orb|z9|CE", **z)))
    assert e.evaluate(req(t="11:00", cycle=f"NIFTY|{DAY}|orb|n9|CE")).approved   # normal expiry still allowed


def test_concurrent_positions_and_pending_count():
    e = engine()
    r = req()
    d = e.evaluate(r)
    e.on_approved(r, d)                         # approved, not filled yet: still counts
    d2 = e.evaluate(req(cycle=f"NIFTY|{DAY}|pullback|pb1|CE", strategy="pullback"))
    assert failed(d2) == {"CONCURRENT_POSITIONS"}          # 1R pending + 1R new = 2R: open-risk limit not hit


def test_open_risk_limit_independent_of_position_count():
    raw = copy.deepcopy(RAW)
    raw["limits"]["max_concurrent_positions"] = 3
    raw["limits"]["max_open_risk_r"] = 2.0
    e = engine(raw)
    open_trade(e, req(cycle=f"NIFTY|{DAY}|orb|a|CE"), 1)
    open_trade(e, req(cycle=f"NIFTY|{DAY}|pullback|b|CE", strategy="pullback"), 2)
    d = e.evaluate(req(cycle=f"NIFTY|{DAY}|x|c|CE", strategy="x"))
    assert failed(d) == {"OPEN_RISK"}


def test_daily_loss_counts_unrealized():
    e = engine()
    d = open_trade(e, req(), 1)
    r_inr = d.planned_r_inr
    close_trade(e, 1, -2 * r_inr)
    open_trade(e, req(t="10:40", cycle=f"NIFTY|{DAY}|pullback|pb1|CE", strategy="pullback"), 2)
    nxt = req(t="10:50", cycle=f"NIFTY|{DAY}|x|x1|CE", strategy="x")
    e.state.open[2].r_inr = r_inr
    assert "DAILY_LOSS" not in failed(e.evaluate(nxt, {2: -0.5 * r_inr}))
    assert "DAILY_LOSS" in failed(e.evaluate(nxt, {2: -1.0 * r_inr}))
    assert e.daily_loss_breached({2: -1.0 * r_inr}) and e.state.daily_loss_hit


def test_realized_daily_loss_latches_for_the_session():
    e = engine()
    d = open_trade(e, req(), 1)
    close_trade(e, 1, -3.1 * d.planned_r_inr)
    assert e.state.daily_loss_hit
    assert "DAILY_LOSS" in failed(e.evaluate(req(t="11:00", cycle=f"NIFTY|{DAY}|orb|orb2|CE")))


# ---------------------------------------------------------------- consecutive losses + latch reset
def test_consecutive_loss_latch_and_reset_rules():
    e = engine()
    for i in range(3):
        open_trade(e, req(t=f"10:{20 * i:02d}", cycle=f"NIFTY|{DAY}|s{i}|x|CE", strategy=f"s{i}"), i + 1)
        close_trade(e, i + 1, -10.0, reason="TIME_STOP", t=f"10:{20 * i + 5:02d}")
    assert e.state.loss_latch_session == DAY
    assert "CONSECUTIVE_LOSS_LATCH" in failed(e.evaluate(req(t="12:00", cycle=f"NIFTY|{DAY}|z|z|CE", strategy="z")))
    with pytest.raises(PermissionError):
        e.reset_consecutive_loss_latch("operator", DAY)          # same session: not allowed
    nxt = "2026-09-03"
    e.start_session(nxt, 0.1)
    assert e.state.loss_latch_session == DAY                     # survives the session change
    with pytest.raises(PermissionError):
        e.reset_consecutive_loss_latch("", nxt)                   # needs an operator
    e.reset_consecutive_loss_latch("operator", nxt)
    assert e.state.loss_latch_session is None and e.state.consecutive_losses == 0


def test_win_resets_consecutive_count():
    e = engine()
    for i, pnl in enumerate([-1.0, -1.0, 5.0, -1.0]):
        open_trade(e, req(t=f"1{i}:00", cycle=f"NIFTY|{DAY}|s{i}|x|CE", strategy=f"s{i}"), i + 1)
        close_trade(e, i + 1, pnl, reason="TIME_STOP", t=f"1{i}:05")
    assert e.state.consecutive_losses == 1 and e.state.loss_latch_session is None


# ---------------------------------------------------------------- cooldown
def test_cooldown_after_stop_per_strategy_direction():
    e = engine()
    open_trade(e, req(t="10:00"), 1)
    close_trade(e, 1, 10.0, reason="STOP", t="10:10")
    c2 = f"NIFTY|{DAY}|orb|orb2|CE"
    assert "COOLDOWN" in failed(e.evaluate(req(t="10:20", cycle=c2)))
    assert e.evaluate(req(t="10:25", cycle=c2)).approved
    # other direction / other strategy is not cooling down
    assert "COOLDOWN" not in failed(e.evaluate(req(t="10:20", cycle=f"NIFTY|{DAY}|orb|orb3|PE", direction=-1)))
    assert "COOLDOWN" not in failed(e.evaluate(req(t="10:20", cycle=f"NIFTY|{DAY}|pb|p|CE", strategy="pb")))


def test_target_exit_has_no_cooldown():
    e = engine()
    open_trade(e, req(t="10:00"), 1)
    close_trade(e, 1, 10.0, reason="TARGET", t="10:10")
    assert e.evaluate(req(t="10:11", cycle=f"NIFTY|{DAY}|orb|orb2|CE")).approved


# ---------------------------------------------------------------- cycles
def test_duplicate_cycle_rejected_after_close_and_after_rejection():
    e = engine()
    r = req()
    open_trade(e, r, 1)
    close_trade(e, 1, 10.0)
    assert "CYCLE_UNIQUE" in failed(e.evaluate(req(t="10:40")))     # no re-entry in the same cycle
    e2 = engine()
    r2 = req(t="09:20")
    assert not e2.evaluate(r2).approved
    e2.on_rejected(r2)
    assert "CYCLE_UNIQUE" in failed(e2.evaluate(req(t="10:00")))


def test_opposite_direction_blocked_while_active_and_needs_fresh_setup():
    raw = copy.deepcopy(RAW)
    raw["limits"]["max_concurrent_positions"] = 3
    raw["limits"]["max_open_risk_r"] = 5.0
    e = engine(raw)
    open_trade(e, req(), 1)
    pe = req(t="10:05", cycle=f"NIFTY|{DAY}|orb|orb2|PE", direction=-1)
    assert "NO_OPPOSITE_ACTIVE" in failed(e.evaluate(pe))
    close_trade(e, 1, 10.0, t="10:06")
    stale = req(t="10:10", cycle=f"NIFTY|{DAY}|orb|orb2|PE", direction=-1, fresh_confirmation=False)
    assert failed(e.evaluate(stale)) == {"OPPOSITE_NEEDS_FRESH_SETUP"}
    assert e.evaluate(req(t="10:10", cycle=f"NIFTY|{DAY}|orb|orb2|PE", direction=-1)).approved


def test_ce_and_pe_cannot_both_be_open_for_same_strategy_even_with_capacity():
    raw = copy.deepcopy(RAW)
    raw["limits"]["max_concurrent_positions"] = 2
    raw["limits"]["max_open_risk_r"] = 5.0
    e = engine(raw)
    r = req()
    e.on_approved(r, e.evaluate(r))                          # pending only
    d = e.evaluate(req(cycle=f"NIFTY|{DAY}|orb|orb1|PE", direction=-1))
    assert "NO_OPPOSITE_ACTIVE" in failed(d)


# ---------------------------------------------------------------- data / quote checks
@pytest.mark.parametrize("kw,reason", [
    (dict(feed_alive=False), "FEED_ALIVE"),
    (dict(quote_age_s=31.0), "QUOTE_FRESH"),
    (dict(quote_age_s=None), "QUOTE_FRESH"),
    (dict(needs_chain=True, chain_age_s=11.0), "CHAIN_FRESH"),
    (dict(needs_chain=True, chain_age_s=None), "CHAIN_FRESH"),
    (dict(bid=None), "QUOTE_VALID"),
    (dict(bid=101.0), "QUOTE_VALID"),
    (dict(ask=29.0, bid=28.95), "MIN_PREMIUM"),
    (dict(ask=100.0, bid=98.5), "SPREAD"),                   # 1.5 > max(0.10, 1% of mid)
    (dict(stop_pct=10.0), "EXIT_TEMPLATE"),
    (dict(target_pct=40.0), "EXIT_TEMPLATE"),
    (dict(time_stop_s=1801), "MAX_HOLDING"),
    (dict(time_stop_s=0), "MAX_HOLDING"),
])
def test_quote_and_template_checks(kw, reason):
    d = engine().evaluate(req(**kw))
    assert not d.approved and reason in failed(d), d.reasons


def test_spread_limited_by_target_distance():
    raw = copy.deepcopy(RAW)
    raw["quotes"]["max_spread_pct_of_target"] = 1.0         # 1% of a 20% target on 40 = 0.08 < 2-tick spread
    d = engine(raw).evaluate(req(ask=40.0, bid=39.9, target_pct=20.0))
    assert failed(d) == {"SPREAD"}
    assert engine().evaluate(req(ask=40.0, bid=39.9, target_pct=20.0)).approved


def test_zero_dte_template_and_holding():
    z = dict(dte=0, stop_pct=25.0, target_pct=40.0)
    assert engine().evaluate(req(time_stop_s=900, **z)).approved
    assert "MAX_HOLDING" in failed(engine().evaluate(req(time_stop_s=901, **z)))
    assert "EXIT_TEMPLATE" in failed(engine().evaluate(req(time_stop_s=900, dte=0, stop_pct=20.0, target_pct=25.0)))


def test_all_failing_checks_are_reported():
    d = engine().evaluate(req(t="09:20", feed_alive=False, ask=20.0, bid=19.0))
    assert {"OPENING_WINDOW", "FEED_ALIVE", "MIN_PREMIUM", "SPREAD"} <= failed(d)


# ---------------------------------------------------------------- kill switch
def test_manual_kill_switch_file_blocks_immediately(tmp_path):
    f = tmp_path / "kill.json"
    ks = KillSwitch(f)
    e = engine(kill=ks)
    assert e.evaluate(req()).approved
    other = KillSwitch(f)                                    # e.g. the dashboard process
    other.set_manual(True, "operator")
    assert "KILL_SWITCH" in failed(e.evaluate(req()))
    other.set_manual(False, "operator")
    assert e.evaluate(req()).approved


def test_unreadable_kill_file_fails_safe(tmp_path):
    f = tmp_path / "kill.json"
    f.write_text("{not json", encoding="utf-8")
    assert KillSwitch(f).blocking_reason() == "KILL_SWITCH_MANUAL"


def test_auto_kill_switch_needs_acknowledgement():
    ks = KillSwitch(None)
    e = engine(kill=ks)
    ks.trip("FEED_OUTAGE", "no ticks 12 s")
    assert "KILL_SWITCH" in failed(e.evaluate(req()))
    with pytest.raises(PermissionError):
        ks.acknowledge("")
    ks.acknowledge("operator")
    assert e.evaluate(req()).approved
    with pytest.raises(ValueError):
        ks.trip("BORED")


def test_exception_in_evaluation_is_no_trade_and_trips_kill_switch():
    e = engine()
    e.state.open[99] = None                                  # corrupt state -> exception inside evaluate
    d = e.evaluate(req())
    assert d.reasons == ["RISK_EVALUATION_ERROR"] and not d.approved
    assert e.kill.blocking_reason() == "KILL_SWITCH_AUTO:RISK_EVALUATION_ERROR"


def test_session_mismatch_is_no_trade():
    e = engine()
    d = e.evaluate(req(t="10:00") .__class__(**{**req().__dict__, "now_ns": ns("10:00", "2026-09-03")}))
    assert d.reasons == ["RISK_EVALUATION_ERROR"]


def test_inconsistent_state_trips_kill_switch():
    e = engine()
    open_trade(e, req(), 1)
    assert e.check_consistency({1})
    assert not e.check_consistency(set())
    assert e.kill.blocking_reason() == "KILL_SWITCH_AUTO:RISK_STATE_INCONSISTENT"
    e2 = engine()
    e2.on_entry_filled("unknown-cycle", 5, 100.0, 65)
    assert e2.kill.auto["reason"] == "RISK_STATE_INCONSISTENT"


def test_broker_rejection_releases_pending():
    e = engine()
    r = req()
    e.on_approved(r, e.evaluate(r))
    e.on_broker_event("entry_not_filled", {"cycle_key": r.cycle_key, "trade_id": None, "reason": "AFTER_FLAT_TIME"})
    assert not e.state.pending and e.state.rejected_orders == 1
    assert e.kill.auto is None
