"""Phase 9 acceptance (rule-only paper validation) + the Phase 7 label pipeline.

  * the push-driven StreamEngine equals the backtest ReplayEngine (same decisions, fills, audit chain)
  * state recovers after a forced restart: journal replay + checkpoint verification, then continue -> identical day
  * paper fills, audit chain, charges and capital usage reconcile for every session
  * kill switch / operator commands are journaled, block entries immediately, and replay identically
  * feed-outage watchdog trips the automatic kill switch (pre-open quiet is not an outage)
  * every candidate is labelled, risk-rejected ones counterfactually, with the broker's own fill rules
Synthetic, deterministic market data; a real-history check runs when the imported history is present.
"""
from __future__ import annotations

import json
import random
from dataclasses import asdict
from datetime import date, datetime

import pandas as pd
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.backtest.audit import AuditLog
from app.backtest.broker import PaperBroker
from app.backtest.fees import FeeSchedule
from app.backtest.fills import NS, FillConfig, Quote
from app.backtest.history import contract_key
from app.backtest.replay import ReplayEngine
from app.clock import IST
from app.control import actions
from app.control.actions import OperatorLog
from app.database.models import PaperSessions, PaperTrades, TradeLabels, TradingSignals
from app.engine.closeout import closeout, open_session_row, reconcile, resolve_incident
from app.engine.feeds import LiveAdapter, history_events, push
from app.engine.journal import Journal
from app.engine.session import PaperSession, SessionSpec, gap_ratio_from
from app.engine.stream import StreamEngine
from app.indicators.engine import IndicatorEngine
from app.labels.labeller import adequacy, label_session
from app.market.calendar import EventCalendar, TradingCalendar
from app.pricing.bs import price, year_fraction
from app.risk.engine import RiskEngine
from app.risk.killswitch import KillSwitch
from app.strategies.base import Candidate, ExitTemplate

DAY = "2026-09-02"
EXPIRY = "2026-09-08"
FEES = FeeSchedule.load()
CAL = TradingCalendar.load()
NOEVENTS = EventCalendar([])
FILL = FillConfig(max_quote_age_s=15.0, version="fill-1/test")
LOT = 65


# ---------------------------------------------------------------------------------------------- synthetic day
def _ns(hh, mm, ss=0):
    return int(datetime.fromisoformat(DAY).replace(hour=hh, minute=mm, second=ss, tzinfo=IST).timestamp()) * NS


def synthetic_day(seed=4):
    """Spot random walk 09:15-15:30 (1 s), 1-minute index bars, ATM±5 CE/PE quotes every 5 s (BS, IV 14%)."""
    rnd = random.Random(seed)
    s, spot = 24000.0, {}
    for t in range(_ns(9, 0) // NS, _ns(15, 30) // NS):
        s += rnd.gauss(0, 4.0)
        spot[t] = s
    bars = []
    for m0 in range(_ns(9, 15) // NS, _ns(15, 30) // NS, 60):
        px = [spot[t] for t in range(m0, m0 + 60)]
        bars.append({"ts": m0, "open": px[0], "high": max(px), "low": min(px), "close": px[-1], "samples": 60,
                     "tier": "synthetic", "session": DAY, "session_open_ts": _ns(9, 15) // NS})
    quotes = []
    exp = date.fromisoformat(EXPIRY)
    for t in range(_ns(9, 1) // NS, _ns(15, 30) // NS, 5):
        atm = round(spot[t] / 50) * 50
        yf = year_fraction(datetime.fromtimestamp(t, IST), exp)
        for k in range(int(atm) - 250, int(atm) + 300, 50):
            for cp in ("CE", "PE"):
                mid = max(price(spot[t], k, yf, 0.14, cp), 0.5)
                bid, ask = round(mid - 0.05, 2), round(mid + 0.05, 2)
                quotes.append(Quote(t * NS + 1000, contract_key(EXPIRY, k, cp), ((bid, 650),), ((ask, 650),),
                                    ltp=round(mid, 2), tier="observed_feed", has_depth=False))
    quotes.sort(key=lambda q: q.ts_ns)
    return quotes, pd.DataFrame(bars)


QUOTES, BARS = synthetic_day()


class EveryN:
    """Stub strategy: a fresh setup every `n` bars from 09:30, alternating direction; tight exits so trades
    open and close many times a day."""
    strategy_id = "stub"

    def __init__(self, n=20):
        self.n, self.bindings, self.session, self.k = n, [], None, 0

    def on_bar(self, session, bar, latest):
        if session != self.session:
            self.session, self.k = session, 0
        self.k += 1
        t = datetime.fromtimestamp(bar["ts"], IST)
        if t.hour * 60 + t.minute < 9 * 60 + 30 or self.k % self.n:
            return []
        d = 1 if (self.k // self.n) % 2 else -1
        return [Candidate("stub", f"s{self.k}", d, "test", bar["close"] - d * 500, ExitTemplate(15.0, 20.0, 600),
                          ExitTemplate(15.0, 20.0, 600), features={"k": self.k, "close": bar["close"]})]


def spec(**kw):
    base = dict(run_id="t", session=DAY, lot_size=LOT, source="synthetic", data_label="observed_feed",
                mode="paper", strategies=("stub",), gap_ratio=0.1, feed_liveness_s=15.0, feed_outage_s=30.0,
                fill=asdict(FILL), checkpoint_every=500)
    return SessionSpec(**(base | kw))


def new_session(tmp_path, name="j", **kw):
    sp = kw.pop("sp", None) or spec()
    rt = tmp_path / "rt"
    return PaperSession(sp, tmp_path / f"{name}.jsonl", FEES, CAL, NOEVENTS, kill_file=rt / "kill.json",
                        commands=rt / "commands", carry_file=rt / "carry.json",
                        strategy_factory=lambda: [EveryN()], **kw)


def recover(tmp_path, name="j", **kw):
    rt = tmp_path / "rt"
    return PaperSession.recover(tmp_path / f"{name}.jsonl", FEES, CAL, NOEVENTS, kill_file=rt / "kill.json",
                                commands=rt / "commands", carry_file=rt / "carry.json",
                                strategy_factory=lambda: [EveryN()], **kw)


EVENTS = history_events(QUOTES, BARS, tick_s=1.0)


@pytest.fixture(scope="module")
def full_run(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("full")
    s = new_session(tmp)
    push(s, EVENTS)
    s.finish()
    return s


# ---------------------------------------------------------------------------------------------- engine parity
def test_stream_engine_equals_replay_engine():
    from app.pipeline.decision import DecisionPipeline, PipelineConfig

    def build():
        risk = RiskEngine(fees=FEES, events=NOEVENTS, kill=KillSwitch(None))
        pipe = DecisionPipeline([EveryN()], risk, CAL, LOT, PipelineConfig(feed_liveness_s=15.0))
        pipe.start_session(DAY, 0.1)
        broker = PaperBroker(FILL, FEES, AuditLog())
        pipe.attach(broker)
        return pipe, broker

    p1, b1 = build()
    ReplayEngine(b1, p1, IndicatorEngine(p1.bindings)).run({"index": BARS}, QUOTES)
    p2, b2 = build()
    se = StreamEngine(b2, p2, IndicatorEngine(p2.bindings))
    for ts, k, x in history_events(QUOTES, BARS, tick_s=0):
        se.on_quote(x) if k == "q" else se.on_bar("index", x, ts)
    se.finish()
    assert len(b1.trades) >= 5
    assert b1.audit.head == b2.audit.head and b1.trades == b2.trades and p1.signals == p2.signals


def test_clock_ticks_do_not_change_results(full_run):
    assert len(full_run.broker.trades) >= 5
    from app.pipeline.decision import DecisionPipeline, PipelineConfig
    risk = RiskEngine(fees=FEES, events=NOEVENTS, kill=KillSwitch(None))
    pipe = DecisionPipeline([EveryN()], risk, CAL, LOT, PipelineConfig(feed_liveness_s=15.0))
    pipe.start_session(DAY, 0.1)
    b = PaperBroker(FILL, FEES, AuditLog())
    pipe.attach(b)
    ReplayEngine(b, pipe, IndicatorEngine(pipe.bindings)).run({"index": BARS}, QUOTES)
    assert full_run.broker.audit.head == b.audit.head


def test_late_event_is_clamped_not_reordered():
    se = StreamEngine(PaperBroker(FILL, FEES, AuditLog()))
    se.tick(_ns(10, 0))
    se.on_quote(QUOTES[0])                       # older than engine time
    assert se.late_events == 1 and se.now_ns == _ns(10, 0)


# ---------------------------------------------------------------------------------------------- recovery
def _open_trade_cut(tmp_path):
    """Index of an event shortly after the first entry fill: a position is open across the crash."""
    s = new_session(tmp_path, name="probe")
    for i, e in enumerate(EVENTS):
        push(s, [e])
        if s.broker.open_trade_ids():
            return i + 200
    raise AssertionError("no position ever opened")


@pytest.mark.parametrize("cut", [3000, "open", 60000])
def test_forced_restart_recovers_identical_state(tmp_path, full_run, cut):
    want_open = cut == "open"
    cut = _open_trade_cut(tmp_path) if want_open else cut
    s1 = new_session(tmp_path)
    push(s1, EVENTS, stop_after=cut)
    open_before = sorted(s1.broker.open_trade_ids())
    assert bool(open_before) or not want_open
    s1.journal.flush()
    del s1                                        # crash: no end record, nothing closed
    done = sum(r["k"] in ("q", "b", "t") for r in Journal.read(tmp_path / "j.jsonl"))
    assert done == cut
    s2 = recover(tmp_path)
    assert s2.recovery["audit_valid"] and not s2.recovery["checkpoints_failed"]
    assert s2.recovery["open_trades"] == open_before
    push(s2, EVENTS[done:])
    s2.finish()
    assert s2.broker.audit.head == full_run.broker.audit.head
    assert s2.broker.trades == full_run.broker.trades
    assert [i["kind"] for i in s2.incidents] == ["RESTART_RECOVERED"]


def test_torn_journal_tail_is_ignored(tmp_path):
    s1 = new_session(tmp_path)
    push(s1, EVENTS, stop_after=2000)
    s1.journal.flush()
    with open(tmp_path / "j.jsonl", "a", encoding="utf-8") as f:
        f.write('{"k":"q","t":12')                 # crash mid-write
    s2 = recover(tmp_path)
    assert s2.recovery["audit_valid"] and s2.applied == 2000


def test_tampered_journal_is_detected_and_blocks_entries(tmp_path):
    s1 = new_session(tmp_path)
    push(s1, EVENTS, stop_after=60000)
    s1.journal.flush()
    first_entry_ns = next(x["ts_ns"] for x in s1.pipe.signals if x["outcome"] == "APPROVED")
    lines = (tmp_path / "j.jsonl").read_text(encoding="utf-8").splitlines()
    # alter the bar behind the first approved entry: its close is part of the audited decision record, so the
    # rebuilt audit chain can no longer match the checkpoints written by the original engine
    for i, line in enumerate(lines):
        r = json.loads(line)
        if r["k"] == "b" and r["t"] == first_entry_ns:
            r["bar"]["close"] += 3.0
            lines[i] = json.dumps(r, separators=(",", ":"))
            break
    else:
        raise AssertionError("decision bar not found")
    (tmp_path / "j.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    s2 = recover(tmp_path)
    assert s2.recovery["checkpoints_failed"] and not s2.recovery["audit_valid"]
    assert s2.kill.blocking_reason() == "KILL_SWITCH_AUTO:RISK_STATE_INCONSISTENT"
    assert any(i["kind"] == "RECOVERY_MISMATCH" and not i["resolved"] for i in s2.incidents)


def test_completed_journal_cannot_be_recovered(tmp_path):
    s = new_session(tmp_path)
    push(s, EVENTS[:500])
    s.finish()
    with pytest.raises(ValueError, match="ended cleanly"):
        recover(tmp_path)


# ---------------------------------------------------------------------------------------------- kill switch / commands
def test_manual_kill_is_journaled_blocks_next_decision_and_replays(tmp_path):
    s = new_session(tmp_path)
    kill_at = next(i for i, (ts, k, _) in enumerate(EVENTS)
                   if k == "b" and datetime.fromtimestamp(ts / NS, IST).strftime("%H:%M") == "11:00")
    push(s, EVENTS[:kill_at])
    before = len([x for x in s.pipe.signals if x["outcome"] == "APPROVED"])
    actions.kill_on("anoop", "test", kill_file=tmp_path / "rt" / "kill.json", log=OperatorLog(tmp_path / "o.jsonl"))
    push(s, EVENTS[kill_at:])
    later = [x for x in s.pipe.signals if x["ts"] >= f"{DAY}T11:00"]
    assert later and all(x["outcome"] != "APPROVED" for x in later)
    assert all("KILL_SWITCH" in x["reasons"] for x in later if x["outcome"] == "REJECTED")
    assert before >= 1
    kills = [r for r in Journal.read(tmp_path / "j.jsonl") if r["k"] == "kill"]
    assert kills and kills[0]["manual"] is True
    s.journal.flush()
    s2 = recover(tmp_path)                       # replays the kill record at the same point
    assert s2.broker.audit.head == s.broker.audit.head and s2.kill.manual


def test_ack_and_latch_reset_commands_are_consumed_and_journaled(tmp_path):
    rt = tmp_path / "rt"
    rt.mkdir(parents=True)
    (rt / "carry.json").write_text(json.dumps({"session": "2026-09-01", "loss_latch_session": "2026-09-01"}))
    s = new_session(tmp_path)
    assert s.risk.state.loss_latch_session == "2026-09-01"
    actions.reset_loss_latch("anoop", "reviewed", commands=rt / "commands", log=OperatorLog(tmp_path / "o.jsonl"))
    s.kill.trip("FEED_OUTAGE", "test")
    actions.acknowledge_auto("anoop", "feed ok", commands=rt / "commands", log=OperatorLog(tmp_path / "o.jsonl"))
    s.tick(_ns(9, 10))                           # pre-open: the watchdog stays quiet
    assert s.risk.state.loss_latch_session is None and s.kill.auto["acked_by"] == "anoop"
    assert not list((rt / "commands").glob("*.json"))
    s.journal.flush()
    cmds = [r["cmd"]["command"] for r in Journal.read(tmp_path / "j.jsonl") if r["k"] == "cmd"]
    assert sorted(cmds) == ["ack_auto", "reset_loss_latch"]


def test_latch_reset_in_same_session_is_refused(tmp_path):
    rt = tmp_path / "rt"
    rt.mkdir(parents=True)
    (rt / "carry.json").write_text(json.dumps({"loss_latch_session": DAY}))
    s = new_session(tmp_path)
    actions.reset_loss_latch("anoop", "x", commands=rt / "commands", log=OperatorLog(tmp_path / "o.jsonl"))
    s.tick(_ns(9, 20))
    assert s.risk.state.loss_latch_session == DAY
    assert any(i["kind"] == "LOSS_LATCH_RESET_REFUSED" for i in s.incidents)


def test_feed_outage_watchdog(tmp_path):
    s = new_session(tmp_path)
    s.tick(_ns(9, 14))                           # pre-open quiet: no outage
    s.tick(_ns(9, 15, 20))
    assert s.kill.auto is None
    s.quote(QUOTES[-1].__class__(_ns(9, 15, 30), QUOTES[0].key, QUOTES[0].bids, QUOTES[0].asks, 1.0, "observed_feed",
                                 False))
    s.tick(_ns(9, 16, 5))
    assert s.kill.blocking_reason() == "KILL_SWITCH_AUTO:FEED_OUTAGE"
    assert any(i["kind"] == "FEED_OUTAGE" and not i["resolved"] for i in s.incidents)


def _q(ns):
    return QUOTES[-1].__class__(ns, QUOTES[0].key, QUOTES[0].bids, QUOTES[0].asks, 1.0, "observed_feed", False)


def test_late_start_is_not_a_feed_outage(tmp_path):
    """2026-11-27: an engine started at 09:21 tripped FEED_OUTAGE (419 s 'since the open') on its first tick."""
    s = new_session(tmp_path)
    s.tick(_ns(9, 21, 58))                       # first tick of a late-started engine, no quote yet
    assert s.kill.auto is None
    s.tick(_ns(9, 22, 20))                       # quotes may take a few seconds after the late start
    assert s.kill.auto is None
    s.tick(_ns(9, 22, 40))                       # 42 s after the start without any quote: a real outage
    assert s.kill.blocking_reason() == "KILL_SWITCH_AUTO:FEED_OUTAGE"


def test_restart_downtime_is_not_a_feed_outage_and_replays_identically(tmp_path):
    s = new_session(tmp_path)
    s.tick(_ns(9, 15, 0))
    s.quote(_q(_ns(9, 30, 0)))
    s.tick(_ns(9, 30, 5))
    s.journal.close()                            # engine killed at 09:30:05
    s2 = recover(tmp_path)
    assert s2.kill.auto is None
    s2.tick(_ns(9, 31, 10))                      # back 65 s later: downtime is not an outage
    assert s2.kill.auto is None and not s2.incidents[:-1]
    s2.quote(_q(_ns(9, 31, 12)))
    s2.tick(_ns(9, 31, 50))                      # 38 s without a quote after the restart: real outage
    assert s2.kill.blocking_reason() == "KILL_SWITCH_AUTO:FEED_OUTAGE"
    s2.journal.close()
    s3 = recover(tmp_path)                       # a second replay reproduces exactly the same kill state
    assert s3.recovery["checkpoints_failed"] == [] and s3.kill.auto["reason"] == "FEED_OUTAGE"


# ---------------------------------------------------------------------------------------------- closeout
def test_closeout_reconciles_and_persists(tmp_path, engine):
    s = new_session(tmp_path)
    open_session_row(engine, s, "fp")
    push(s, EVENTS)
    r = closeout(engine, s)
    rec = r["reconciliation"]
    assert rec["ok"] and rec["capital_reconciles"] and rec["trades_match_fills"] and rec["charges_match_fee_schedule"]
    assert rec["peak_capital_from_audit"] == rec["peak_capital_broker"] > 0
    with Session(engine) as db:
        row = db.scalars(select(PaperSessions)).one()
        assert row.status == "COMPLETED" and row.journal_path.endswith(".jsonl.gz") and row.audit_head
        assert db.query(PaperTrades).count() == r["trades"] >= 5
        assert db.query(TradingSignals).count() == r["signals"]
        assert db.query(TradeLabels).count() == r["labels"] > 0
    assert Journal.closed(tmp_path / "j.jsonl") and not (tmp_path / "j.jsonl").exists()
    assert json.loads((tmp_path / "rt" / "carry.json").read_text())["session"] == DAY


def test_reconcile_detects_tampered_trade(full_run):
    b = full_run.broker
    good = reconcile(b, full_run.pipe.signals)
    assert good["ok"]
    t0 = dict(b.trades[0])
    b.trades[0]["gross_pnl"] += 10
    try:
        bad = reconcile(b, full_run.pipe.signals)
        assert not bad["ok"] and bad["mismatched_trades"] == [t0["trade_id"]]
    finally:
        b.trades[0].update(t0)


def test_incident_resolution(tmp_path, engine):
    s = new_session(tmp_path)
    open_session_row(engine, s)
    s.tick(_ns(9, 16))
    s.quote(QUOTES[0])
    s.tick(_ns(10, 0))                            # 44 min without a quote -> critical incident
    push(s, [e for e in EVENTS if e[0] > _ns(10, 0)])
    r = closeout(engine, s)
    assert r["status"] == "COMPLETED_WITH_INCIDENTS"
    resolve_incident(engine, "t", DAY, "anoop", "synthetic outage, checked")
    with Session(engine) as db:
        row = db.scalars(select(PaperSessions)).one()
        assert row.status == "COMPLETED" and json.loads(row.incidents)[0]["resolved_by"] == "anoop"


# ---------------------------------------------------------------------------------------------- labels
def test_every_candidate_is_labelled_including_counterfactuals(full_run):
    sigs = [x | {"session": DAY} for x in full_run.pipe.signals]
    rows = label_session(sigs, QUOTES, LOT, FILL, FEES, {t["cycle"] for t in full_run.broker.trades}, "observed_feed")
    cands = [x for x in sigs if x["outcome"] in ("APPROVED", "REJECTED") and x["setup_id"]]
    assert len(rows) == 4 * len(cands)
    assert {r["horizon_min"] for r in rows} == {5, 10, 15, 30}
    rejected = [r for r in rows if r["signal_outcome"] == "REJECTED"]
    assert rejected and all(r["traded"] == 0 for r in rejected)
    assert any(r["label"] in ("TARGET_FIRST", "STOP_FIRST", "TIMEOUT") for r in rejected)   # counterfactual
    for r in rows:
        if r["label"] in ("TARGET_FIRST", "STOP_FIRST", "TIMEOUT"):
            assert r["entry_ts"] >= r["signal_ts"] and r["exit_ts"] >= r["entry_ts"]
            assert abs(r["gross_pnl_per_lot"] - r["charges_per_lot"] - r["net_pnl_per_lot"]) < 0.02
            assert json.loads(r["features"])["minute_of_day"] >= 9 * 60 + 15


def test_label_matches_broker_trade_when_exits_coincide(full_run):
    """A traded candidate whose trade ended by stop / target / time stop (10 min template) gets the same outcome
    and exit price from the labeller's 10-minute horizon: same fill model, same fees."""
    sigs = [x | {"session": DAY} for x in full_run.pipe.signals]
    rows = {(r["cycle_key"], r["horizon_min"]): r
            for r in label_session(sigs, QUOTES, LOT, FILL, FEES, {t["cycle"] for t in full_run.broker.trades},
                                   "observed_feed")}
    checked = 0
    for t in full_run.broker.trades:
        if t["exit_reason"] not in ("STOP", "TARGET", "TIME_STOP"):
            continue
        r = rows[(t["cycle"], 10)]
        want = {"STOP": "STOP_FIRST", "TARGET": "TARGET_FIRST", "TIME_STOP": "TIMEOUT"}[t["exit_reason"]]
        assert r["label"] == want and r["entry_px"] == t["entry_px"] and r["exit_px"] == t["exit_px"]
        assert r["net_pnl_per_lot"] == pytest.approx(t["net_pnl"] * LOT / t["qty"], abs=0.02)
        checked += 1
    assert checked >= 3


def test_labels_without_contract_or_after_close():
    sig = {"session": DAY, "seq": 0, "ts": f"{DAY}T10:00:00+05:30", "ts_ns": _ns(10, 0), "strategy_id": "stub",
           "setup_id": "s1", "cycle_key": "c", "direction": 1, "outcome": "REJECTED",
           "reasons": ["NO_ELIGIBLE_CONTRACT"], "contract": None, "dte": None, "payload": {}}
    rows = label_session([sig], QUOTES, LOT, FILL, FEES, set(), "observed_feed")
    assert {r["label"] for r in rows} == {"UNLABELLABLE"} and rows[0]["label_reason"] == "NO_ELIGIBLE_CONTRACT"
    late = sig | {"ts_ns": _ns(15, 26), "contract": QUOTES[0].key, "reasons": [],
                  "payload": {"template": {"stop_pct": 15.0, "target_pct": 20.0, "time_stop_s": 600}}}
    assert {r["label"] for r in label_session([late], QUOTES, LOT, FILL, FEES, set(), "observed_feed")} == {"NO_ENTRY"}


def test_adequacy_counts_only_prospective_labels():
    mk = lambda i, lab, tier: {"horizon_min": 15, "data_tier": tier, "label": lab, "traded": i % 2,  # noqa: E731
                               "session_date": f"2026-{1 + i % 12:02d}-{1 + i % 28:02d}"}
    hist = [mk(i, "TIMEOUT", "history_without_futures/observed_top_of_book_snapshot") for i in range(900)]
    assert adequacy(hist)["labels"] == 0
    live = [mk(i, ("TARGET_FIRST", "STOP_FIRST", "TIMEOUT")[i % 3], "observed_feed") for i in range(600)]
    a = adequacy(live)
    assert a["labels"] == 600 and all(v == 200 for v in a["classes"].values()) and a["sessions"] >= 60 and a["ready"]
    assert not adequacy(live[:450])["ready"]


# ---------------------------------------------------------------------------------------------- misc
def test_gap_ratio_matches_backtest_definition():
    from app.pipeline.decision import gap_ratios
    rnd = random.Random(1)
    rows, c = [], 24000.0
    for i in range(20):
        o = c + rnd.gauss(0, 40)
        h, lo = o + abs(rnd.gauss(80, 20)), o - abs(rnd.gauss(80, 20))
        c = rnd.uniform(lo, h)
        d = f"2026-08-{i + 1:02d}"
        rows += [{"session": d, "ts": i * 100 + j, "open": o if j == 0 else c, "high": h if j == 1 else c,
                  "low": lo if j == 2 else c, "close": c} for j in range(4)]
    idx = pd.DataFrame(rows)
    want = gap_ratios(idx)["2026-08-20"]
    daily = idx[idx["session"] < "2026-08-20"].groupby("session").agg(high=("high", "max"), low=("low", "min"),
                                                                        close=("close", "last"))
    prior = [{"session": s, "high": r.high, "low": r.low, "close": r.close} for s, r in daily.iterrows()]
    got = gap_ratio_from(prior, idx[idx["session"] == "2026-08-20"].sort_values("ts")["open"].iloc[0])
    assert got == want


def test_warmup_is_journaled_and_used_on_recovery(tmp_path):
    from app.indicators.engine import binding

    class WithATR(EveryN):
        def __init__(self):
            super().__init__()
            self.bindings = [binding("atr", "index", period=14)]

    prev = [b | {"session": "2026-09-01", "ts": b["ts"] - 86400} for b in BARS.to_dict("records")[-30:]]
    rt = tmp_path / "rt"
    s = PaperSession(spec(), tmp_path / "j.jsonl", FEES, CAL, NOEVENTS, kill_file=rt / "k.json", commands=rt / "c",
                     carry_file=rt / "carry.json", strategy_factory=lambda: [WithATR()], warmup={"index": prev})
    assert s.warmup_bars == 30
    push(s, EVENTS[:20000])
    s.journal.flush()
    start = Journal.read(tmp_path / "j.jsonl")[0]
    assert len(start["warmup"]["index"]) == 30
    s2 = PaperSession.recover(tmp_path / "j.jsonl", FEES, CAL, NOEVENTS, kill_file=rt / "k.json", commands=rt / "c",
                              carry_file=rt / "carry.json", strategy_factory=lambda: [WithATR()])
    assert s2.warmup_bars == 30 and s2.recovery["audit_valid"] and s2.pipe.latest == s.pipe.latest


def test_live_adapter_builds_depth_quotes_and_final_bars():
    class Sink:
        def __init__(self):
            self.q, self.b, self.t = [], [], []

        def quote(self, q):
            self.q.append(q)

        def bar(self, stream, bar, ns):
            self.b.append((stream, bar, ns))

        def tick(self, ns):
            self.t.append(ns)

    class Sub:
        expiry, strike, option_type = EXPIRY, 24000.0, "CE"

    sink = Sink()
    a = LiveAdapter(sink, date.fromisoformat(DAY), index_id=13, future_id=None, options={555: Sub()})
    t0 = _ns(9, 30)
    row = {"code": 8, "security_id": 555, "recv_ns": t0, "seq": 1, "ltp": 101.0, "ltt": t0 // NS, "volume": 100,
           "raw": b"x" * 20, **{f"bid_px_{i}": 100.0 - 0.05 * i for i in range(1, 6)},
           **{f"bid_qty_{i}": 65 * i for i in range(1, 6)}, **{f"ask_px_{i}": 100.1 + 0.05 * i for i in range(1, 6)},
           **{f"ask_qty_{i}": 130 * i for i in range(1, 6)}}
    a.on_rows([row])
    assert len(sink.q) == 1 and sink.q[0].key == contract_key(EXPIRY, 24000.0, "CE")
    assert sink.q[0].bids[0] == (99.95, 65) and len(sink.q[0].asks) == 5 and sink.q[0].has_depth
    for i, px in enumerate((24000.0, 24005.0, 24010.0)):
        a.on_rows([{"code": 2, "security_id": 13, "recv_ns": t0 + i * 20 * NS, "seq": 10 + i, "ltp": px,
                    "ltt": (t0 + i * 20 * NS) // NS, "raw": bytes([i]) * 16}])
    a.tick(t0 + 63 * NS)                          # quiet feed: the 09:30 bar closes on the clock
    assert [s for s, _, _ in sink.b] == ["index"] and sink.b[0][1]["close"] == 24010.0
    assert sink.b[0][1]["session"] == DAY and sink.b[0][2] >= t0 + 60 * NS and sink.t == [t0 + 63 * NS]


def test_engine_code_places_no_orders():
    import pathlib
    src = "\n".join(p.read_text(encoding="utf-8") for p in pathlib.Path("app/engine").glob("*.py"))
    for bad in ("place_order", "/orders", "modify_order", "cancel_order"):
        assert bad not in src


# ---------------------------------------------------------------------------------------------- real history
def test_real_history_paper_engine_equals_backtest(tmp_path):
    from app.config import settings
    from app.marketdata.pipeline import history_days
    cfg = settings()
    ds = cfg.get("historical_import.dataset")
    try:
        days = history_days(cfg.path("historical"), ds)
    except Exception:  # noqa: BLE001
        days = []
    if len(days) < 12:
        pytest.skip("imported history not available")
    from app.engine.feeds import history_day, prior_daily, warmup_bars
    from app.research.simulate import HistoryContext, simulate
    from app.research.spec import build_strategies
    ctx = HistoryContext.load(cfg, ds, days)
    day = days[11]
    bt = simulate(ctx, {"d": build_strategies(("orb", "pullback"), {}, {"require_futures": False})}, days=days[:12])["d"]
    want = [t for t in bt.trades if t["session"] == day]
    q, idx, lot = history_day(ctx, day)
    ok, why = ctx.quality.get(day, (False, "x"))
    sp = SessionSpec(run_id="r", session=day, lot_size=lot, source="replay", data_label=ctx.label, mode="paper_replay",
                     prior_daily=prior_daily(ctx.index, day), data_ok=ok, data_reason=why, feed_liveness_s=15.0,
                     feed_outage_s=1e9, fill=asdict(ctx.fill))
    s = PaperSession(sp, tmp_path / "j.jsonl", ctx.fees, ctx.cal, ctx.events, kill_file=tmp_path / "k.json",
                     commands=tmp_path / "c", carry_file=tmp_path / "carry.json",
                     warmup=warmup_bars(ctx.index, day, 11))
    push(s, history_events(q, idx))
    s.finish()
    strip = lambda ts: [{k: v for k, v in t.items() if k != "session"} for t in ts]  # noqa: E731
    assert strip(want) == s.broker.trades


def test_shadow_replay_matches_engine_incrementally(tmp_path):
    """Dashboard shadow: a muted journal replay reproduces the engine's signals, trades and checkpoints, reading
    the live journal in pieces, and never writes to it."""
    from app.engine.shadow import ShadowSession
    s = new_session(tmp_path, sp=spec(checkpoint_every=50))
    half = len(EVENTS) // 2
    push(s, EVENTS[:half])
    s.journal.flush()
    jp = tmp_path / "j.jsonl"
    size = jp.stat().st_size
    sh = ShadowSession(jp, strategy_factory=lambda: [EveryN()]).refresh()
    assert jp.stat().st_size == size                                   # read-only
    push(s, EVENTS[half:])
    s.journal.flush()
    sh.refresh()
    st = sh.state()
    assert st["built"] and not st["mismatches"] and st["checkpoints_ok"] > 0 and not st["error"]
    assert len(sh.signals()) == len(s.pipe.signals) and len(sh.trades()) == len(s.broker.trades)
    assert sh.session.broker.audit.head == s.broker.audit.head
    assert st["realized_inr"] == round(sum(float(t.get("net_pnl") or 0) for t in s.broker.trades), 2)


def test_shadow_concurrent_refresh_does_not_double_feed(tmp_path):
    """2026-11-27: two dashboard tabs refreshed one shared shadow at once and fed the same bytes twice."""
    import threading
    from app.engine.shadow import ShadowSession
    s = new_session(tmp_path, sp=spec(checkpoint_every=50))
    sh = ShadowSession(tmp_path / "j.jsonl", strategy_factory=lambda: [EveryN()])
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            sh.refresh()

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for t in threads:
        t.start()
    for i in range(0, len(EVENTS), 25):
        push(s, EVENTS[i:i + 25])
        s.journal.flush()
    stop.set()
    for t in threads:
        t.join()
    sh.refresh()
    st = sh.state()
    assert st["error"] is None and not st["mismatches"] and st["checkpoints_ok"] > 0
    assert sh.session.broker.audit.head == s.broker.audit.head and sh.records == s.journal.records
