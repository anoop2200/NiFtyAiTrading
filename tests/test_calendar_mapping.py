from datetime import date, datetime

import pytest

from app.clock import IST
from app.market.calendar import CalendarGap, EventCalendar, TradingCalendar
from app.market.mapping import Contract, InstrumentMap, active_future


@pytest.fixture(scope="module")
def cal():
    return TradingCalendar.load()


def test_holidays_and_weekends(cal):
    assert not cal.is_trading_day(date(2026, 1, 26))      # Republic Day
    assert not cal.is_trading_day(date(2026, 11, 24))     # Tuesday holiday
    assert not cal.is_trading_day(date(2026, 11, 28))     # Saturday
    assert cal.is_trading_day(date(2026, 11, 26))
    assert cal.next_trading_day(date(2026, 11, 23)) == date(2026, 11, 25)
    assert cal.prev_trading_day(date(2026, 11, 25)) == date(2026, 11, 23)


def test_regular_session(cal):
    s = cal.session(date(2026, 11, 26))
    assert (s.open_dt().hour, s.open_dt().minute, s.close_dt().hour, s.close_dt().minute) == (9, 15, 15, 30)
    assert cal.session(date(2026, 11, 24)) is None


def test_special_session_muhurat_not_tradable(cal):
    d = date(2026, 11, 8)  # Sunday Muhurat session: recorded as special, never traded
    assert d in cal.special
    assert not cal.is_trading_day(d)


@pytest.mark.parametrize("nominal,actual", [(date(2026, 10, 20), date(2026, 10, 19)),
                                            (date(2026, 11, 10), date(2026, 11, 9)),
                                            (date(2026, 11, 24), date(2026, 11, 23)),
                                            (date(2026, 12, 1), date(2026, 12, 1))])
def test_tuesday_holiday_expiry_moves_to_previous_trading_day(cal, nominal, actual):
    assert cal.adjust_expiry(nominal) == actual
    assert cal.weekly_expiry_on_or_after(nominal - date.resolution * 3) == actual


def test_expiry_after_holiday_moved_past_d(cal):
    # on Tue 11-24 (holiday) the moved expiry 11-23 is already past -> next week's expiry
    assert cal.weekly_expiry_on_or_after(date(2026, 11, 24)) == date(2026, 12, 1)


def test_dte_counts_trading_days_zero_on_expiry(cal):
    assert cal.dte(date(2026, 12, 1), date(2026, 12, 1)) == 0
    assert cal.dte(date(2026, 12, 1), date(2026, 11, 30)) == 1
    assert cal.dte(date(2026, 12, 1), date(2026, 11, 27)) == 2   # Fri -> Mon -> Tue
    assert cal.dte_bucket(0) == "0" and cal.dte_bucket(1) == "1" and cal.dte_bucket(4) == "2+"


def test_uncovered_year_fails_closed(cal):
    with pytest.raises(CalendarGap):
        cal.is_trading_day(date(2027, 1, 5))


def test_event_blackout(tmp_path):
    p = tmp_path / "events.yaml"
    p.write_text("""
blackout_minutes: {default: [15, 15], rbi_policy: [30, 30]}
events:
  - {date: 2026-12-04, time: "10:00", type: rbi_policy, name: RBI MPC, source: https://example.org/rbi}
  - {date: 2026-12-10, time: null, type: macro, name: whole day, source: https://example.org/x}
""", encoding="utf-8")
    ec = EventCalendar.load(p)
    assert ec.blackout(datetime(2026, 12, 4, 9, 31, tzinfo=IST))
    assert not ec.blackout(datetime(2026, 12, 4, 9, 29, tzinfo=IST))
    assert not ec.blackout(datetime(2026, 12, 4, 10, 30, tzinfo=IST))
    assert ec.blackout(datetime(2026, 12, 10, 15, 0, tzinfo=IST))


def test_event_without_source_rejected(tmp_path):
    p = tmp_path / "events.yaml"
    p.write_text("events:\n  - {date: 2026-12-04, time: '10:00', type: macro, name: x}\n", encoding="utf-8")
    with pytest.raises(ValueError):
        EventCalendar.load(p)


def _fut(sid, exp):
    return Contract(sid, "NSE_FNO", "future", f"NIFTY-{exp}-FUT", exp, None, None, 65, 0.1)


def test_roll_rule(cal):
    cur, nxt = _fut(1, date(2026, 12, 29)), _fut(2, date(2027, 1, 26))
    assert active_future([cur, nxt], date(2026, 12, 1), cal) == cur
    # next-month volume overtook current-month volume in the previous session
    assert active_future([cur, nxt], date(2026, 12, 1), cal, {1: 100, 2: 500}) == nxt
    # within 2 trading days of expiry: rolled regardless of volume
    assert active_future([cur, nxt], date(2026, 12, 24), cal, {1: 900, 2: 1}) == nxt
    # expired contract never selected
    assert active_future([_fut(0, date(2026, 11, 24)), cur], date(2026, 11, 30), cal) == cur


def test_instrument_map_lookup():
    m = InstrumentMap(date(2026, 11, 26), [
        Contract(13, "IDX_I", "index", "NIFTY"),
        Contract(40001, "NSE_FNO", "option", "NIFTY-Dec2026-26000-CE", date(2026, 12, 1), 26000.0, "CE", 65, 0.05),
        _fut(50001, date(2026, 12, 29))])
    assert m.index.security_id == 13
    assert m.option(date(2026, 12, 1), 26000, "CE").security_id == 40001
    assert m.get(99) is None
    assert [f.security_id for f in m.live_futures(date(2026, 11, 26))] == [50001]


def test_instrument_map_requires_dated_master(engine):
    with pytest.raises(LookupError):
        InstrumentMap.from_db(engine, date(2026, 11, 26))
