"""India VIX stream: master row, subscription, live bars, history backfill, decision context, label features."""
import json
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from app.backtest.fills import NS
from app.clock import IST
from app.data.instruments import parse_master
from app.data.universe import Sub, Universe
from app.engine.feeds import LiveAdapter, history_events, push
from app.historical import vix as vixh
from app.labels.labeller import label_features
from app.ml import dataset as ds
from app.pipeline.decision import DecisionPipeline

DAY = "2026-09-02"
MASTER = Path(__file__).resolve().parents[1] / "data/historical/instrument_master/instrument_master_2026-11-26.csv"


def _ts(hh, mm):
    return int(datetime.fromisoformat(DAY).replace(hour=hh, minute=mm, tzinfo=IST).timestamp())


@pytest.mark.skipif(not MASTER.exists(), reason="instrument master not downloaded")
def test_master_has_india_vix_separate_from_nifty():
    sel = parse_master(MASTER, date(2026, 11, 26))
    assert sel.vix["security_id"] == 21 and sel.vix["instrument_type"] == "VIX"
    assert sel.index["security_id"] == 13                     # NIFTY stays the one "index" role
    assert parse_master(MASTER, date(2026, 11, 26), vix_symbol=None).vix is None


def test_universe_subscribes_vix():
    idx, v = Sub("IDX_I", 13, "ticker", "index"), Sub("IDX_I", 21, "ticker", "vix")
    u = Universe(idx, [], [], 50.0, 24000.0, 5, 2, vix=v)
    assert u.all()[:2] == [idx, v] and u.describe()["vix"] == 21
    assert Universe(idx, [], [], 50.0, 24000.0, 5, 2).all() == [idx]


def test_live_adapter_builds_vix_bars():
    class Sink:
        def __init__(self):
            self.b, self.t = [], []

        def quote(self, q):
            raise AssertionError("VIX ticks are not option quotes")

        def bar(self, stream, bar, ns):
            self.b.append((stream, bar))

        def tick(self, ns):
            self.t.append(ns)

    sink = Sink()
    a = LiveAdapter(sink, date.fromisoformat(DAY), index_id=13, future_id=None, options={}, vix_id=21)
    t0 = _ts(9, 30) * NS
    for i, (sid, px) in enumerate([(13, 24000.0), (21, 13.10), (21, 13.25), (13, 24010.0)]):
        a.on_rows([{"code": 2, "security_id": sid, "recv_ns": t0 + i * 10 * NS, "seq": i + 1, "ltp": px,
                    "ltt": (t0 + i * 10 * NS) // NS, "raw": bytes([i]) * 16}])
    a.tick(t0 + 63 * NS)
    got = {s: b["close"] for s, b in sink.b}
    assert got == {"index": 24010.0, "vix": 13.25} and a.errors == 0


def test_candles_to_bars_filters_and_upsert_is_idempotent(tmp_path):
    ts = [_ts(9, 14), _ts(9, 15), _ts(9, 16), _ts(15, 30)]
    body = {"data": {"timestamp": ts, "open": [1, 13.0, 13.1, 9], "high": [1, 13.2, 13.3, 9],
                     "low": [1, 12.9, 0, 9], "close": [1, 13.1, 13.2, 9], "volume": [0, 0, 0, 0]}}
    bars = vixh.candles_to_bars(body, DAY)
    assert bars["ts"].tolist() == [_ts(9, 15)]                # pre-open, bad (low 0) and 15:30 dropped
    assert vixh.upsert(tmp_path, bars) == 1 and vixh.upsert(tmp_path, bars) == 1
    assert vixh.coverage(tmp_path, [DAY, "2026-09-03"])["missing"] == ["2026-09-03"]


def _pipe(session=DAY):
    return SimpleNamespace(session=session, _on_vix=None)


def test_decision_uses_vix_from_the_previous_minute_only():
    p = _pipe()
    for hh, mm, c in [(9, 15, 13.0), (9, 16, 13.4), (9, 17, 14.0)]:
        DecisionPipeline._on_vix(p, {"ts": _ts(hh, mm), "session": DAY, "open": c, "close": c})
    # index bar 09:17 (09:17-09:18) sees the 09:16 VIX bar, never the same-minute 09:17 bar
    ctx = DecisionPipeline.market_context(p, {"ts": _ts(9, 17)})
    assert ctx["vix"] == 13.4 and ctx["vix_bar_ts"] == _ts(9, 16)
    assert ctx["vix_chg_pct_session"] == pytest.approx(100 * (13.4 / 13.0 - 1), abs=1e-3)
    assert DecisionPipeline.market_context(p, {"ts": _ts(9, 16)})["vix"] == 13.0
    assert DecisionPipeline.market_context(p, {"ts": _ts(9, 15)}) == {"vix": None}     # nothing closed yet
    assert DecisionPipeline.market_context(_pipe("2026-09-03"), {"ts": _ts(9, 18)}) == {"vix": None}
    q = _pipe("2026-09-03")
    DecisionPipeline._on_vix(q, {"ts": _ts(9, 15), "session": DAY, "open": 13, "close": 13})
    assert DecisionPipeline.market_context(q, {"ts": _ts(9, 18)}) == {"vix": None}       # other session's VIX


def test_history_events_push_vix_before_same_minute_index_bar():
    idx = pd.DataFrame([{"ts": _ts(9, 15), "close": 1.0}])
    vix = pd.DataFrame([{"ts": _ts(9, 15), "close": 13.0}])
    ev = history_events([], idx, tick_s=0, vix_bars=vix)
    assert [k for _, k, _ in ev] == ["v", "b"]

    class S:
        def __init__(self):
            self.streams = []

        def bar(self, s, b, t):
            self.streams.append(s)

    s = S()
    push(s, ev)
    assert s.streams == ["vix", "index"]


def test_vix_is_a_label_and_model_feature():
    sig = {"ts_ns": _ts(10, 0) * NS, "dte": 2, "direction": 1, "strategy_id": "orb",
           "payload": {"features": {"close": 24000.0, "atr": 12.0}, "selection": {"chosen": {"ask": 80.0}},
                       "market": {"vix": 13.4, "vix_bar_ts": 1, "vix_chg_pct_session": 3.08}}}
    f = label_features(sig)
    assert f["mkt_vix"] == 13.4 and f["mkt_vix_chg_pct_session"] == 3.08 and "mkt_vix_bar_ts" not in f
    X, _, _ = ds.build(pd.DataFrame([{"run_id": "r", "session_date": DAY, "signal_seq": 0, "signal_ts": "x",
                                      "strategy_id": "orb", "cycle_key": "c", "traded": 1, "signal_outcome": "A",
                                      "net_pnl_per_lot": 1.0, "r_multiple": 0.1, "data_tier": "t",
                                      "label": "TARGET_FIRST", "features": json.dumps(f)}]))
    assert X.loc[0, "mkt_vix"] == 13.4 and ds.leakage_report(X)["ok"]
