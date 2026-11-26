"""Phase 8 acceptance: read-only UI except audited config / kill actions, deduplicated notifications, kill switch
blocks new entries immediately, daily report carries the §9.7 capital-usage metrics."""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from app.control import actions
from app.control.actions import ActionError, OperatorLog
from app.database.db import init_db, make_engine, make_ro_engine
from app.database.models import CapitalUsage, PaperTrades
from app.notify.telegram import (CRITICAL, INFO, WARNING, Alert, Notifier, NotifyPolicy, TelegramTransport,
                                 kill_switch_alert)
from app.reports.daily import Window, build_daily_report, format_report
from app.risk.config import RISK_FILE, RiskConfig
from app.risk.killswitch import KillSwitch
from test_risk import engine as risk_engine, req

ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------- operator log
def test_operator_log_chain_and_tamper(tmp_path):
    log = OperatorLog(tmp_path / "ops.jsonl")
    log.append("a", "anoop", "first", x=1)
    log.append("b", "anoop", "second")
    assert log.verify() and [r["seq"] for r in log.records()] == [1, 2]
    lines = (tmp_path / "ops.jsonl").read_text(encoding="utf-8").splitlines()
    lines[0] = lines[0].replace('"first"', '"edited"')
    (tmp_path / "ops.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    assert not log.verify()


@pytest.mark.parametrize("actor,reason", [("", "r"), ("  ", "r"), ("anoop", ""), ("anoop", " ")])
def test_operator_log_requires_actor_and_reason(tmp_path, actor, reason):
    with pytest.raises(ActionError):
        OperatorLog(tmp_path / "ops.jsonl").append("x", actor, reason)


# ---------------------------------------------------------------- kill switch
def test_kill_on_blocks_risk_engine_immediately(tmp_path):
    f, log = tmp_path / "kill.json", OperatorLog(tmp_path / "ops.jsonl")
    e = risk_engine(kill=KillSwitch(f))
    assert e.evaluate(req()).approved
    actions.kill_on("anoop", "test", kill_file=f, log=log)
    d = e.evaluate(req(cycle="NIFTY|2026-09-02|orb|orb2|CE"))
    assert not d.approved and d.reasons == ["KILL_SWITCH"] and d.checks["kill_switch"]["detail"] == "KILL_SWITCH_MANUAL"
    assert actions.kill_status(f)["state"].startswith("SAFE")
    with pytest.raises(ActionError):
        actions.kill_off("anoop", "resume", "yes", kill_file=f, log=log)
    assert actions.kill_status(f)["manual"]
    actions.kill_off("anoop", "resume", actions.RESUME_PHRASE, kill_file=f, log=log)
    assert e.evaluate(req(cycle="NIFTY|2026-09-02|orb|orb3|CE")).approved
    assert [r["action"] for r in log.records()] == ["kill_switch_on", "kill_switch_off"] and log.verify()


def test_emergency_close_flag(tmp_path):
    f = tmp_path / "kill.json"
    actions.kill_on("anoop", "bad fills", emergency_close=True, kill_file=f, log=OperatorLog(tmp_path / "o.jsonl"))
    assert actions.kill_status(f)["emergency_close"]


def test_ack_command_is_consumed_by_engine_switch(tmp_path):
    inbox = tmp_path / "commands"
    ks = KillSwitch(tmp_path / "kill.json", inbox=inbox)
    ks.trip("FEED_OUTAGE", "no ticks 30s")
    assert ks.blocking_reason() == "KILL_SWITCH_AUTO:FEED_OUTAGE"
    actions.acknowledge_auto("anoop", "feed back", commands=inbox, log=OperatorLog(tmp_path / "o.jsonl"))
    assert ks.blocking_reason() is None and ks.auto["acked_by"] == "anoop"
    assert not list(inbox.glob("*.json")) and len(list(inbox.glob("*.done"))) == 1


def test_risk_engine_reads_the_files_the_dashboard_writes():
    from app.risk.engine import RiskEngine
    e = RiskEngine()
    assert Path(e.kill.state_file).resolve() == actions.KILL_FILE.resolve()
    assert Path(e.kill.inbox).resolve() == actions.COMMANDS.resolve()


# ---------------------------------------------------------------- risk config changes
@pytest.fixture
def risk_copy(tmp_path):
    p = tmp_path / "risk.yaml"
    p.write_bytes(RISK_FILE.read_bytes())
    return p


def _comments(s: str) -> list[str]:
    return [m.group(0) for m in re.finditer(r"#.*", s)]


def test_config_change_keeps_comments_and_bumps_version(tmp_path, risk_copy):
    before = risk_copy.read_text(encoding="utf-8")
    log = OperatorLog(tmp_path / "o.jsonl")
    rec = actions.change_risk_config("anoop", "fewer entries", {"limits.max_entries_per_day": 4,
                                                                 "zero_dte.last_entry": "14:15"}, "APPLY",
                                     risk_file=risk_copy, log=log)
    after = risk_copy.read_text(encoding="utf-8")
    new = yaml.safe_load(after)
    assert new["limits"]["max_entries_per_day"] == 4 and new["zero_dte"]["last_entry"] == "14:15"
    old_v = yaml.safe_load(before)["version"]
    assert new["version"] != old_v and rec["detail"]["from_version"] == old_v
    assert _comments(after) == _comments(before)
    assert len(after.splitlines()) == len(before.splitlines())
    RiskConfig.from_dict(new)
    assert log.verify() and rec["detail"]["changes"]["limits.max_entries_per_day"]["to"] == 4


@pytest.mark.parametrize("changes,confirm", [
    ({"limits.max_entries_per_day": 0}, "APPLY"),                 # rejected by the risk validator
    ({"windows.last_entry": "16:00"}, "APPLY"),
    ({"zero_dte.max_entries": -1}, "APPLY"),
    ({"quotes.tick": 0.1}, "APPLY"),                              # not whitelisted
    ({"limits.max_entries_per_day": 4}, "apply"),                 # wrong confirmation
    ({}, "APPLY"),
])
def test_config_change_rejections_leave_file_untouched(tmp_path, risk_copy, changes, confirm):
    before = risk_copy.read_bytes()
    log = OperatorLog(tmp_path / "o.jsonl")
    with pytest.raises(ActionError):
        actions.change_risk_config("anoop", "x", changes, confirm, risk_file=risk_copy, log=log)
    assert risk_copy.read_bytes() == before and log.records() == []


# ---------------------------------------------------------------- notifier
class FakeTransport:
    name = "fake"

    def __init__(self, fail=False):
        self.sent, self.fail = [], fail

    def send(self, text):
        if self.fail:
            raise RuntimeError("down")
        self.sent.append(text)


class Clock:
    def __init__(self, t=1_790_000_000.0):
        self.t = t

    def __call__(self):
        return self.t


def test_dedup_and_throttle_and_digest():
    tr, clk = FakeTransport(), Clock()
    n = Notifier(tr, NotifyPolicy(dedup_window_s=600, throttle_s={"rejected": 60}), clock=clk)
    a = Alert("data", WARNING, "quote stale 25000CE")
    assert n.send(a) == "SENT"
    clk.t += 30
    assert n.send(a) == "DEDUPED"
    clk.t += 600
    assert n.send(a) == "SENT"
    r1, r2 = Alert("rejected", INFO, "spread", key="k1"), Alert("rejected", INFO, "age", key="k2")
    assert n.send(r1) == "SENT"
    clk.t += 10
    assert n.send(r2) == "THROTTLED"
    clk.t += 60
    assert n.send(r2) == "SENT"
    assert n.flush_digest() == "SENT" and "data: 1 (deduped 1)" in tr.sent[-1] and "rejected: 1 (throttled 1)" in tr.sent[-1]
    assert n.flush_digest() is None


def test_cap_never_suppresses_critical():
    tr, clk = FakeTransport(), Clock()
    n = Notifier(tr, NotifyPolicy(daily_cap=2), clock=clk)
    for i in range(2):
        assert n.send(Alert("info", INFO, f"m{i}")) == "SENT"
    assert n.send(Alert("info", INFO, "m3")) == "CAPPED"
    for _ in range(3):   # identical critical alerts are not deduplicated either
        assert n.send(kill_switch_alert(True, "anoop", "test")) == "SENT"
    assert sum("Kill switch ON" in s for s in tr.sent) == 3


def test_failed_critical_is_retried():
    tr = FakeTransport(fail=True)
    n = Notifier(tr, clock=Clock())
    assert n.send(Alert("kill_switch", CRITICAL, "auto trip")) == "FAILED"
    assert n.send(Alert("info", INFO, "x")) == "FAILED" and len(n.pending) == 1
    assert n.retry_pending() == 1
    tr.fail = False
    assert n.retry_pending() == 0 and "auto trip" in tr.sent[0]


def test_telegram_errors_never_contain_token():
    class Boom:
        def post(self, url, **kw):
            raise ConnectionError(f"failed to reach {url}")
    t = TelegramTransport("123:SECRET-TOKEN", "42", client=Boom())
    with pytest.raises(RuntimeError) as ei:
        t.send("hi")
    assert "SECRET-TOKEN" not in str(ei.value) and "<token>" in str(ei.value)

    class Resp:
        status_code = 401

    class Bad:
        def post(self, url, **kw):
            return Resp()
    with pytest.raises(RuntimeError, match="HTTP 401"):
        TelegramTransport("123:SECRET-TOKEN", "42", client=Bad()).send("hi")


# ---------------------------------------------------------------- daily report
def _trade(i, day, dte, net, risk, reason, exit_ts):
    return PaperTrades(run_id="r1", mode="paper", session_date=day, trade_id=i, cycle_key=f"c{i}", strategy_id="orb",
                       contract="NIFTY|x|25000|CE", dte=dte, qty=65, entry_ts=exit_ts, exit_ts=exit_ts, entry_px=100.0,
                       exit_px=100.0, exit_reason=reason, gross_pnl=net + 50, charges=50.0, net_pnl=net,
                       risk_inr=risk, r_multiple=net / risk, low_fidelity=False, fee_version="f", fill_model="m")


@pytest.fixture
def report_db(tmp_path):
    eng = make_engine(tmp_path / "r.db")
    init_db(eng)
    with Session(eng) as s:
        s.add_all([_trade(1, "2026-11-02", 3, 1000.0, 2000.0, "TARGET", "2026-11-02T10:00:00+05:30"),
                   _trade(2, "2026-11-02", 3, -3000.0, 2500.0, "STOP", "2026-11-02T11:00:00+05:30"),
                   _trade(3, "2026-11-03", 0, -500.0, 1500.0, "TIME_STOP", "2026-11-03T10:00:00+05:30"),
                   _trade(4, "2026-11-03", 1, 200.0, 1000.0, "TARGET", "2026-11-03T11:00:00+05:30")])
        s.add_all([CapitalUsage(run_id="r1", mode="paper", session_date="2026-11-02", peak_capital=20000.0,
                                peak_capital_ts="2026-11-02T10:30:00+05:30", peak_trade_ids="[1, 2]", net_pnl=-2000.0,
                                max_intraday_drawdown=3000.0, largest_trade_risk=2500.0, largest_0dte_risk=0.0, trades=2),
                   CapitalUsage(run_id="r1", mode="paper", session_date="2026-11-03", peak_capital=15000.0,
                                peak_capital_ts="2026-11-03T10:05:00+05:30", peak_trade_ids="[3]", net_pnl=-300.0,
                                max_intraday_drawdown=500.0, largest_trade_risk=1000.0, largest_0dte_risk=1500.0, trades=2)])
        s.commit()
    return eng


def test_daily_report_capital_metrics(report_db):
    d = build_daily_report(report_db, "r1", "2026-11-03", window=Window(sessions=1))
    assert (d["net_pnl"], d["gross_pnl"], d["charges"], d["trades"]) == (-300.0, -200.0, 100.0, 2)
    assert (d["wins"], d["losses"], d["timeouts"]) == (1, 1, 1)
    assert d["peak_capital"] == {"inr": 15000.0, "at": "2026-11-03T10:05:00+05:30", "open_trades": [3]}
    assert d["peak_capital_last_n"] == {"inr": 15000.0, "session": "2026-11-03", "sessions": 1}
    assert d["peak_capital_since_inception"] == {"inr": 20000.0, "session": "2026-11-02", "sessions": 2}
    # equity +1000, -2000, -2500, -2300 -> max drawdown from the +1000 peak = 3500
    assert d["cumulative_max_drawdown"] == 3500.0
    assert d["shadow_capital_0p50pct"] == 2500.0 / 0.005 and d["shadow_capital_0p25pct_0dte"] == 1500.0 / 0.0025
    assert d["live_capital_planning_figure"] == max(20000.0, 500000.0, 600000.0) + 3500.0
    assert d["expiry_day"] and set(d["by_dte"]) == {"0DTE", "1DTE"}
    txt = format_report(d)
    assert "PLANNING ESTIMATE" in txt and "not a recommendation" in txt and "by DTE" in txt and len(txt) < 4096


def test_daily_report_unknown_session(report_db):
    with pytest.raises(LookupError):
        build_daily_report(report_db, "r1", "2026-11-04")


# ---------------------------------------------------------------- read-only dashboard
def test_read_only_engine_rejects_writes(report_db, tmp_path):
    ro = make_ro_engine(tmp_path / "r.db")
    with ro.connect() as c:
        assert c.execute(text("SELECT COUNT(*) FROM PaperTrades")).scalar() == 4
        with pytest.raises(OperationalError, match="readonly"):
            c.execute(text("DELETE FROM PaperTrades"))
            c.commit()


def test_dashboard_code_has_no_write_paths():
    bad = re.compile(r"\.commit\(|session\.add|\.add_all\(|make_engine\(|write_text|write_bytes|open\([^)]*['\"][wa]"
                     r"|init_db|to_sql|\.execute\(\s*text\(\s*['\"]\s*(INSERT|UPDATE|DELETE|DROP|CREATE)", re.I)
    for f in (ROOT / "app" / "dashboard").glob("*.py"):
        src = f.read_text(encoding="utf-8")
        assert not bad.search(src), f"{f.name}: {bad.search(src).group(0)}"
    ui = (ROOT / "app" / "dashboard" / "ui.py").read_text(encoding="utf-8")
    # the only state-changing calls are the audited actions
    calls = set(re.findall(r"actions\.(\w+)\(", ui))
    assert calls <= {"kill_status", "kill_on", "kill_off", "acknowledge_auto", "change_risk_config", "OperatorLog"}


def test_streamlit_binds_localhost():
    cfg = (ROOT / ".streamlit" / "config.toml").read_text(encoding="utf-8")
    assert 'address = "127.0.0.1"' in cfg and "gatherUsageStats = false" in cfg


def test_outbox_dry_run(tmp_path):
    from app.notify.telegram import OutboxTransport
    n = Notifier(OutboxTransport(tmp_path / "out.jsonl"))
    assert n.send(Alert("research", INFO, "done")) == "SENT"
    assert json.loads((tmp_path / "out.jsonl").read_text(encoding="utf-8"))["text"].endswith("done")
