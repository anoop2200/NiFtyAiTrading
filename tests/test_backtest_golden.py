"""Golden replay fixtures (Phase 4 acceptance): expected fills, costs and P&L derived by hand."""
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from app.backtest.audit import AuditLog
from app.backtest.broker import EntryIntent, PaperBroker
from app.backtest.fees import FeeSchedule
from app.backtest.fills import NS, FillConfig, Quote
from app.backtest.replay import ReplayEngine

CASES = json.loads((Path(__file__).parent / "golden" / "replay_cases.json").read_text(encoding="utf-8"))
D = CASES["defaults"]


class Scripted:
    strategy_id = "scripted"

    def __init__(self, actions, t0, key):
        self.by_t = {}
        for a in actions:
            self.by_t.setdefault(t0 + int(round(a["t"] * NS)), []).append(a)
        self.key = key

    def on_bar(self, stream, bar, ind, ctx):
        out = []
        for a in self.by_t.get(ctx.now_ns, []):
            out.append(EntryIntent(cycle_key=a.get("cycle", f"cyc-{a['t']}"), strategy_id="scripted",
                                   contract=self.key, lots=a["lots"], lot_size=D["lot_size"], decision_ns=ctx.now_ns,
                                   stop_pct=a.get("stop_pct", D["stop_pct"]),
                                   target_pct=a.get("target_pct", D["target_pct"]),
                                   time_stop_s=a.get("time_stop_s", D["time_stop_s"]), dte=5))
        return out


def run_case(case):
    t0 = int(datetime.fromisoformat(case.get("t0", D["t0"])).timestamp()) * NS
    key = D["key"]
    quotes = [Quote(t0 + int(round(q["t"] * NS)), key, tuple(map(tuple, q["bids"])), tuple(map(tuple, q["asks"])),
                    has_depth=q.get("depth", True)) for q in case["quotes"]]
    acts = case["actions"]
    if any(a["t"] != int(a["t"]) for a in acts):
        raise ValueError("golden actions must fall on whole seconds")
    broker = PaperBroker(FillConfig(**D["fill"]), FeeSchedule.load(D["fee_version"]), AuditLog())
    # a zero-length "clock" bar per action: processed exactly at the action time
    eng = ReplayEngine(broker, Scripted(acts, t0, key), bar_seconds=0)
    clock = pd.DataFrame({"ts": sorted({t0 // NS + int(a["t"]) for a in acts})})
    eng.run({"clock": clock}, quotes)
    return broker, t0


@pytest.mark.parametrize("case", CASES["cases"], ids=[c["name"] for c in CASES["cases"]])
def test_golden_case(case):
    broker, t0 = run_case(case)
    exp = case["expect"]
    if "trades" in exp:
        assert len(broker.trades) == len(exp["trades"]), broker.trades
        for got, want in zip(broker.trades, exp["trades"]):
            for k, v in want.items():
                if k == "entry_offset_s":
                    assert (got["entry_ns"] - t0) / NS == pytest.approx(v), k
                elif k == "exit_offset_s":
                    assert (got["exit_ns"] - t0) / NS == pytest.approx(v), k
                elif isinstance(v, float):
                    assert got[k] == pytest.approx(v, abs=1e-6), k
                else:
                    assert got[k] == v, k
    orders = list(broker.orders.values())
    for want in exp.get("orders", []):
        o = next(o for o in orders if o.purpose == want["purpose"])
        for k, v in want.items():
            if k == "fills":
                assert [(f[1], f[2]) for f in o.fills] == [tuple(x) for x in v]
            elif k == "first_reason":
                assert broker.audit.of("attempt")[0]["reason"] == v
            elif k != "purpose":
                assert getattr(o, k) == (pytest.approx(v) if isinstance(v, float) else v), k
    for i, want in enumerate(exp.get("order_charges", [])):
        got = [o.charges for o in orders if o.charges and o.charges["total"] > 0][i]
        for k, v in want.items():
            assert got[k] == pytest.approx(v, abs=1e-9), (i, k)
    if "capital_peak" in exp:
        assert broker.capital["peak"] == pytest.approx(exp["capital_peak"], abs=1e-6)
    for kind, n in exp.get("events", {}).items():
        assert len(broker.audit.of(kind)) == n, kind
    if "rejection_reasons" in exp:
        assert [r["reason"] for r in broker.audit.of("entry_rejected")] == exp["rejection_reasons"]
    assert AuditLog.verify(broker.audit.records)


def test_golden_replay_is_reproducible():
    for case in CASES["cases"]:
        a, _ = run_case(case)
        b, _ = run_case(case)
        assert a.audit.head == b.audit.head, case["name"]


def test_persist_session_roundtrip(engine):
    from sqlalchemy import func, select
    from sqlalchemy.orm import Session

    from app.backtest.persist import persist_session
    from app.database.models import CapitalUsage, PaperTradeEvents, PaperTrades
    case = next(c for c in CASES["cases"] if c["name"].startswith("G1"))
    broker, _ = run_case(case)
    for _ in range(2):  # idempotent per (run, session)
        persist_session(engine, broker, "test-run", "2026-11-25")
    with Session(engine) as s:
        assert s.scalar(select(func.count()).select_from(PaperTrades)) == 1
        assert s.scalar(select(func.count()).select_from(PaperTradeEvents)) == len(broker.audit.records)
        cu = s.scalars(select(CapitalUsage)).one()
        assert cu.peak_capital == 6552.51 and cu.net_pnl == 1939.82 and cu.trades == 1
        last = s.scalars(select(PaperTradeEvents).order_by(PaperTradeEvents.seq.desc())).first()
        assert last.hash == broker.audit.head


def test_persist_two_sessions_same_run_trade_ids_restart(engine):
    from sqlalchemy import func, select
    from sqlalchemy.orm import Session

    from app.backtest.persist import persist_session
    from app.database.models import PaperTrades
    case = next(c for c in CASES["cases"] if c["name"].startswith("G1"))
    for day in ("2026-11-24", "2026-11-25"):   # each session's broker numbers trades from 1
        broker, _ = run_case(case)
        persist_session(engine, broker, "run-x", day)
    with Session(engine) as s:
        assert s.scalar(select(func.count()).select_from(PaperTrades)) == 2
