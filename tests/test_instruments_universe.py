from datetime import date, timedelta

import pandas as pd
import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.data.instruments import MasterInvalid, parse_master, persist, validate
from app.data.universe import build_universe, recenter
from app.database.models import Instruments

TODAY = date(2026, 11, 24)  # a Tuesday: weekly expiry day -> 0DTE + next expiry


def make_master(path, today=TODAY, lot=65, bad_lot=False, drop_col=None):
    rows = [dict(SEM_EXM_EXCH_ID="NSE", SEM_SMST_SECURITY_ID=13, SEM_INSTRUMENT_NAME="INDEX",
                 SEM_TRADING_SYMBOL="NIFTY", SEM_LOT_UNITS=1, SEM_EXPIRY_DATE="", SEM_STRIKE_PRICE="",
                 SEM_OPTION_TYPE="", SEM_TICK_SIZE=5, SEM_EXPIRY_FLAG=""),
            dict(SEM_EXM_EXCH_ID="NSE", SEM_SMST_SECURITY_ID=26009, SEM_INSTRUMENT_NAME="INDEX",
                 SEM_TRADING_SYMBOL="BANKNIFTY", SEM_LOT_UNITS=1, SEM_EXPIRY_DATE="", SEM_STRIKE_PRICE="",
                 SEM_OPTION_TYPE="", SEM_TICK_SIZE=5, SEM_EXPIRY_FLAG="")]
    sid = 40000
    for m, exp in enumerate([today + timedelta(days=7), today + timedelta(days=35)]):
        sid += 1
        rows.append(dict(SEM_EXM_EXCH_ID="NSE", SEM_SMST_SECURITY_ID=sid, SEM_INSTRUMENT_NAME="FUTIDX",
                         SEM_TRADING_SYMBOL=f"NIFTY-{exp:%b%Y}-FUT", SEM_LOT_UNITS=lot,
                         SEM_EXPIRY_DATE=f"{exp} 14:30:00", SEM_STRIKE_PRICE=-0.01, SEM_OPTION_TYPE="XX",
                         SEM_TICK_SIZE=10, SEM_EXPIRY_FLAG="M"))
    for k, exp in enumerate([today, today + timedelta(days=7), today + timedelta(days=14)]):
        for strike in range(23000, 25050, 50):
            for ot in ("CE", "PE"):
                sid += 1
                lot_here = lot + (1 if bad_lot and k == 0 and strike == 24000 else 0)
                rows.append(dict(SEM_EXM_EXCH_ID="NSE", SEM_SMST_SECURITY_ID=sid, SEM_INSTRUMENT_NAME="OPTIDX",
                                 SEM_TRADING_SYMBOL=f"NIFTY-{exp:%d%b%Y}-{strike}-{ot}", SEM_LOT_UNITS=lot_here,
                                 SEM_EXPIRY_DATE=f"{exp} 14:30:00", SEM_STRIKE_PRICE=float(strike),
                                 SEM_OPTION_TYPE=ot, SEM_TICK_SIZE=5, SEM_EXPIRY_FLAG="W"))
    # a BANKNIFTY option that must be excluded by the "NIFTY-" prefix
    rows.append(dict(rows[-1], SEM_SMST_SECURITY_ID=99999, SEM_TRADING_SYMBOL="BANKNIFTY-X-50000-CE"))
    df = pd.DataFrame(rows)
    if drop_col:
        df = df.drop(columns=[drop_col])
    df.to_csv(path, index=False)
    return path


def test_parse_validate_persist(tmp_path, engine):
    sel = parse_master(make_master(tmp_path / "m.csv"), TODAY)
    assert sel.index["security_id"] == 13
    assert 99999 not in set(sel.options["security_id"])
    rep = validate(sel, TODAY)
    assert rep["nearest_lot_size"] == 65 and rep["expiries"][0] == TODAY.isoformat()
    mid = persist(engine, sel, "file://test", tmp_path / "m.csv", "abc")
    assert persist(engine, sel, "file://test", tmp_path / "m.csv", "abc") == mid  # idempotent
    with Session(engine) as s:
        assert s.scalar(select(func.count()).select_from(Instruments)) == sel.selected_rows


def test_validation_fails_closed(tmp_path):
    with pytest.raises(MasterInvalid):
        validate(parse_master(make_master(tmp_path / "a.csv", bad_lot=True), TODAY), TODAY)
    with pytest.raises(MasterInvalid):
        parse_master(make_master(tmp_path / "b.csv", drop_col="SEM_TICK_SIZE"), TODAY)


def test_universe_expiry_day_and_recenter(tmp_path):
    sel = parse_master(make_master(tmp_path / "m.csv"), TODAY)
    u = build_universe(sel, spot=24012.0, today=TODAY, each_side=5, buffer=2)
    assert u.strike_step == 50 and u.atm == 24000
    assert [d.isoformat() for d in u.option_expiries] == [TODAY.isoformat(), (TODAY + timedelta(days=7)).isoformat()]
    assert len(u.options) == 2 * 2 * 15  # 2 expiries x CE/PE x (5+2)*2+1 strikes
    assert len(u.futures) == 2 and u.index.mode == "ticker"
    assert recenter(u, sel, 24020.0) == []
    added = recenter(u, sel, 24110.0)  # ATM 24000 -> 24100: two new strikes each side of the window
    assert u.atm == 24100 and len(added) == 2 * 2 * 2


def test_universe_normal_day_single_expiry(tmp_path):
    sel = parse_master(make_master(tmp_path / "m.csv"), TODAY)
    u = build_universe(sel, spot=24000.0, today=TODAY + timedelta(days=1))
    assert len(u.option_expiries) == 1
