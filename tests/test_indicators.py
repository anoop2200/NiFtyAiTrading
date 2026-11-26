import json
import math
from datetime import date, datetime

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from app.clock import IST
from app.indicators.base import Indicator, IndicatorSpec, SourceRequirement
from app.indicators.chain import chain_bars
from app.indicators.engine import (IndicatorEngine, Resampler, binding, check_source, output_hash,
                                   resample_df)
from app.indicators.registry import (CodeChangedWithoutVersionBump, approved, create, load_all, set_status,
                                     sync_definitions)
from app.indicators.validation import no_lookahead, validate
from app.pricing.bs import greeks, implied_vol, premium_for_iv, price, year_fraction

OPEN = int(datetime(2026, 11, 2, 9, 15, tzinfo=IST).timestamp())


def ohlcv(days=2, seed=3, gap_at=None):
    rng = np.random.default_rng(seed)
    rows, px = [], 26000.0
    for d in range(days):
        o_ts = OPEN + d * 86400
        for m in range(375):
            if gap_at and d == 0 and gap_at[0] <= m < gap_at[1]:
                continue
            o = px
            px *= math.exp(rng.normal(0, 0.0007))
            rows.append({"ts": o_ts + 60 * m, "session": str(date.fromtimestamp(o_ts)), "session_open_ts": o_ts,
                         "open": o, "high": max(o, px) + rng.uniform(0, 3), "low": min(o, px) - rng.uniform(0, 3),
                         "close": px, "volume": int(rng.integers(100, 5000)), "oi": int(1e6 + rng.integers(-5e4, 5e4))})
    return pd.DataFrame(rows)


def chain_stream(n=300, seed=5):
    rng = np.random.default_rng(seed)
    rows, s = [], 26000.0
    for m in range(n):
        s += rng.normal(0, 4)
        atm = round(s / 50) * 50
        ts = OPEN + 60 * m
        t = year_fraction(datetime.fromtimestamp(ts + 60, IST), date(2026, 11, 3))
        ce, pe = price(s, atm, t, 0.14, "CE"), price(s, atm, t, 0.15, "PE")
        rows.append({"ts": ts, "session": "2026-11-02", "session_open_ts": OPEN, "expiry": "2026-11-03", "spot": s,
                     "atm_strike": float(atm), "ce_oi": int(5e7 + rng.integers(0, 1e6)),
                     "pe_oi": int(5e7 + rng.integers(0, 1e6)), "atm_ce_ltp": ce, "atm_ce_bid": ce - 0.1,
                     "atm_ce_ask": ce + 0.1, "atm_pe_ltp": pe, "atm_pe_bid": pe - 0.1, "atm_pe_ask": pe + 0.1})
    return pd.DataFrame(rows)


STREAMS = {"index": ohlcv(), "future": ohlcv(seed=4), "option": ohlcv(seed=9), "chain": chain_stream()}


@pytest.mark.parametrize("iid", sorted(load_all()))
def test_every_indicator_passes_phase3_checks(iid):
    cls = create(iid).__class__
    rep = validate(cls, STREAMS[cls.SPEC.source.stream])
    assert rep["passed"], json.dumps(rep, default=str)[:2000]
    assert rep["batch_vs_stream"]["valid_rows"] > 0


def test_vectorised_batch_is_independent_for_core_indicators():
    for iid in ("ema", "sma", "rsi", "atr", "bbands", "roc", "vwap"):
        assert create(iid).__class__._vector is not None


class Leaky(Indicator):
    """Deliberately wrong: reads the next bar's close from the frame it is streamed over."""
    SPEC = IndicatorSpec("leaky", "leaky", "0.0.1", "test", "close[t+1]", SourceRequirement("index", ("close",)),
                         ("x",), "", "")

    def init_state(self):
        pass

    def stream(self, df):
        self._df = df
        return super().stream(df)

    def _update(self, bar):
        nxt = self._df.loc[self._df["ts"] == bar["ts"] + 60, "close"]
        return {"x": float(nxt.iloc[0]) if len(nxt) else bar["close"]}


def test_lookahead_check_catches_a_leaky_indicator():
    df = STREAMS["index"].iloc[:200].reset_index(drop=True)
    assert no_lookahead(Leaky, df, 100)["passed"] is False
    assert no_lookahead(create("ema").__class__, df, 100)["passed"] is True


def test_missing_input_gives_invalid_output_and_keeps_state():
    df = STREAMS["index"].iloc[:60].copy()
    df.loc[30, "close"] = None
    out = create("ema", period=5).stream(df)
    assert not bool(out.loc[30, "valid"]) and out.loc[30, "state"] == "NO_SIGNAL"
    assert bool(out.loc[31, "valid"])


def test_intraday_gap_resets_state_but_overnight_does_not():
    df = ohlcv(gap_at=(100, 110))
    ind = create("ema", period=5)
    out = ind.stream(df)
    i = df.index[df["ts"] == OPEN + 60 * 110][0]
    assert not bool(out.loc[i, "valid"])        # warm-up restarts after a 10-minute hole
    assert ind.reset_count == 1                 # session change overnight did not reset


def test_session_anchored_indicator_resets_each_session():
    out = create("vwap").stream(STREAMS["future"])
    first_day2 = STREAMS["future"].index[STREAMS["future"]["session"] != STREAMS["future"]["session"].iloc[0]][0]
    row = STREAMS["future"].loc[first_day2]
    assert out.loc[first_day2, "vwap"] == pytest.approx((row["high"] + row["low"] + row["close"]) / 3)


def test_opening_range_waits_for_completed_range():
    out = create("opening_range", minutes=15).stream(STREAMS["index"])
    assert not out.iloc[:15]["valid"].any() and bool(out.iloc[15]["valid"])
    hi = STREAMS["index"].iloc[:15]["high"].max()
    assert out.iloc[15]["or_high"] == hi
    assert (out["state"] == "BREAKOUT").sum() < (out["direction"].abs() > 0).sum()


def test_resampler_emits_only_completed_bars():
    df = STREAMS["index"].iloc[:12]
    r, emitted = Resampler(300), []
    for i, b in enumerate(df.to_dict("records")):
        out = r.on_bar(b)
        if out:
            emitted.append((i, out[0]))
    assert [i for i, _ in emitted] == [4, 9]            # emitted on the 5th minute, never earlier
    b0 = emitted[0][1]
    assert b0["open"] == df.iloc[0]["open"] and b0["close"] == df.iloc[4]["close"]
    assert b0["high"] == df.iloc[:5]["high"].max() and b0["volume"] == df.iloc[:5]["volume"].sum()
    assert b0["complete"] and r.flush()[0]["complete"] is False   # 2 of 5 minutes: partial, flagged


def test_engine_checkpoint_restart_matches_uninterrupted_run():
    def eng():
        return IndicatorEngine([binding("ema", "index", 1, period=9), binding("rsi", "index", 5),
                                binding("market_strength", "index", 1), binding("pcr", "chain", 1)])
    streams = {"index": STREAMS["index"], "chain": STREAMS["chain"]}
    full = eng().run(streams)
    cut = OPEN + 60 * 137 + 1   # mid 5-minute bucket: resampler state must survive too
    a = eng()
    first = a.run({k: v[v["ts"] < cut] for k, v in streams.items()})
    snap = json.loads(json.dumps(a.checkpoint()))
    b = eng()
    b.restore(snap)
    second = b.run({k: v[v["ts"] >= cut] for k, v in streams.items()})
    resumed = pd.concat([first, second], ignore_index=True)
    assert output_hash(resumed) == output_hash(full)


def test_engine_run_is_deterministic():
    def run():
        e = IndicatorEngine([binding("adx", "index", 1), binding("atm_iv", "chain", 1)])
        return e.run({"index": STREAMS["index"], "chain": STREAMS["chain"]})
    assert output_hash(run()) == output_hash(run())


def test_source_requirements():
    b = binding("fut_rel_volume", "future", 1)
    assert check_source(b, "observed_feed", {"ts", "volume"}) is None
    assert "tier" in check_source(b, "reconstructed_from_snapshots", {"ts", "volume"})
    assert "fields" in check_source(binding("atr", "index", 1), "observed_feed", {"ts", "close"})
    assert "stream" in check_source(binding("atr", "chain", 1), "observed_feed", {"high", "low", "close"})


def test_unknown_params_rejected_and_keys_versioned():
    with pytest.raises(ValueError):
        create("ema", length=9)
    assert create("ema", period=9).key == "ema@1.0.0(period=9)"


def test_resample_df_matches_streaming_resampler():
    a = resample_df(STREAMS["index"], 300)
    assert len(a) == 150 and a["complete"].all()


# ---------------------------------------------------------------------------------------- registry
def test_registry_lifecycle(engine, monkeypatch):
    added = sync_definitions(engine)
    assert any(x.startswith("ema@1.0.0") for x in added)
    with pytest.raises(PermissionError):
        set_status(engine, "ema", "1.0.0", "APPROVED", actor="test")
    set_status(engine, "ema", "1.0.0", "VALIDATED", actor="test", validation={"passed": True})
    set_status(engine, "ema", "1.0.0", "APPROVED", actor="test")
    assert ("ema", "1.0.0") in approved(engine)
    cls = create("ema").__class__
    monkeypatch.setattr(cls, "code_sha", classmethod(lambda c: "changed"))
    with pytest.raises(CodeChangedWithoutVersionBump):
        sync_definitions(engine)


def test_validated_definition_falls_back_to_draft_on_code_change(engine, monkeypatch):
    sync_definitions(engine)
    set_status(engine, "rsi", "1.0.0", "VALIDATED", actor="test", validation={"passed": True})
    monkeypatch.setattr(create("rsi").__class__, "code_sha", classmethod(lambda c: "changed"))
    assert any("rsi@1.0.0" in x for x in sync_definitions(engine))
    assert ("rsi", "1.0.0") not in approved(engine)


# ---------------------------------------------------------------------------------------- pricing
def test_put_call_parity_and_iv_roundtrip():
    s, k, t = 26010.0, 26000.0, 3 / 365
    from app.pricing.bs import ASSUMPTIONS as A
    c, p = price(s, k, t, 0.15, "CE"), price(s, k, t, 0.15, "PE")
    assert c - p == pytest.approx(s * math.exp(-A.q * t) - k * math.exp(-A.r * t), abs=1e-6)
    for cp, px in (("CE", c), ("PE", p)):
        assert implied_vol(px, s, k, t, cp) == pytest.approx(0.15, abs=1e-5)


def test_iv_invalid_cases():
    assert implied_vol(0.0, 26000, 26000, 0.01, "CE") is None
    assert implied_vol(5.0, 26500, 26000, 0.01, "CE") is None             # below intrinsic
    assert implied_vol(100.0, 26000, 26000, 30 / (365 * 86400), "CE") is None  # under the minimum T
    assert premium_for_iv(100, 101, 100) == (100, "ltp")                  # crossed book -> LTP, flagged
    assert premium_for_iv(None, 0, 10) == (None, "none")
    assert premium_for_iv(100, 99, 101) == (100, "mid")


def test_greeks_match_finite_differences():
    s, k, t, v = 26000.0, 26100.0, 5 / 365, 0.14
    g = greeks(s, k, t, v, "CE")
    h = 1.0
    assert g["delta"] == pytest.approx((price(s + h, k, t, v, "CE") - price(s - h, k, t, v, "CE")) / (2 * h), rel=1e-4)
    gamma_fd = (price(s + h, k, t, v, "CE") - 2 * price(s, k, t, v, "CE") + price(s - h, k, t, v, "CE")) / h ** 2
    assert g["gamma"] == pytest.approx(gamma_fd, rel=1e-3)
    assert g["vega"] == pytest.approx((price(s, k, t, v + 0.005, "CE") - price(s, k, t, v - 0.005, "CE")), rel=1e-3)
    dt = 1 / 365
    assert g["theta"] == pytest.approx(price(s, k, t - dt, v, "CE") - price(s, k, t, v, "CE"), rel=0.05)


def test_year_fraction_uses_1530_expiry():
    obs = datetime(2026, 11, 3, 15, 0, tzinfo=IST)
    assert year_fraction(obs, date(2026, 11, 3)) == pytest.approx(1800 / (365 * 86400))


# ---------------------------------------------------------------------------------------- chain bars
def test_chain_bars_use_last_snapshot_of_minute_and_nearest_expiry():
    rows = []
    for sec, spot in ((5, 26010.0), (55, 26040.0), (65, 26060.0)):
        ts = datetime(2026, 11, 2, 9, 15, tzinfo=IST) + pd.Timedelta(seconds=sec)
        for exp in (date(2026, 11, 3), date(2026, 11, 10)):
            for k in (25950.0, 26000.0, 26050.0, 26100.0):
                for ot in ("CE", "PE"):
                    rows.append({"snapshot_ts": ts, "expiry_ts": datetime.combine(exp, datetime.min.time(), IST),
                                 "strike": k, "option_type": ot, "ltp": 100.0 + sec, "oi": 1000 if ot == "CE" else 2000,
                                 "bid": 99.0, "ask": 101.0, "iv": 14.0, "spot": spot})
    cb = chain_bars(pa.Table.from_pylist(rows), band=1)
    assert len(cb) == 2
    first = cb.iloc[0]
    assert first["spot"] == 26040.0 and first["atm_strike"] == 26050.0    # last snapshot inside 09:15
    assert first["expiry"] == "2026-11-03"                                # nearest expiry only
    assert first["pe_oi"] / first["ce_oi"] == 2.0 and first["atm_ce_ltp"] == 155.0
