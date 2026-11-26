r"""OI-03 probe: how much expired NIFTY option history does Dhan's /charts/rollingoption serve?

  .venv\Scripts\python scripts\probe_rolling_options.py

Read-only historical data API. For a ladder of past dates (1 week .. 5 years back) it requests one day of
1-minute ATM CE data for the nearest weekly and monthly expiry, plus ATM±10 / PUT spot checks on a recent day,
and cross-checks the recent day against our PG snapshot history (spot and ATM option close). Raw responses go
to data/derived/oi03_probe/, the summary to data/reports/oi03_rolling_options_probe.json.
"""
import asyncio
import json
from datetime import date, datetime, timedelta

import _bootstrap  # noqa: F401
import pandas as pd

from app.clock import IST
from app.config import settings
from app.data.dhan_rest import DhanRest
from app.secrets import get_secret

NIFTY = 13
RECENT = "2026-11-25"          # a PG history day, for the cross-check


def _weekday_on_or_before(d: date) -> date:
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def ladder(today: date) -> list[str]:
    back = [7, 30, 91, 182, 365, 730, 1095, 1460, 1800, 1830, 1900]
    return [_weekday_on_or_before(today - timedelta(days=n)).isoformat() for n in back]


def summarize(body, key: str) -> dict:
    d = (body or {}).get("data") if isinstance(body, dict) else None
    side = d.get(key) if isinstance(d, dict) else None
    if not isinstance(side, dict):
        return {"rows": 0, "keys": sorted(d.keys()) if isinstance(d, dict) else None}
    ts = side.get("timestamp") or []
    out = {"rows": len(ts), "fields": sorted(k for k, v in side.items() if v)}
    if ts:
        f = lambda t: datetime.fromtimestamp(int(t), IST).strftime("%Y-%m-%d %H:%M")  # noqa: E731
        out |= {"first": f(ts[0]), "last": f(ts[-1]),
                "strikes": sorted(set(side.get("strike") or []))[:6],
                "spot_first": (side.get("spot") or [None])[0], "close_first": (side.get("close") or [None])[0],
                "iv_first": (side.get("iv") or [None])[0], "oi_first": (side.get("oi") or [None])[0]}
    return out


async def main() -> int:
    cfg = settings()
    raw = cfg.path("derived") / "oi03_probe"
    raw.mkdir(parents=True, exist_ok=True)
    rest = DhanRest(cfg.get("dhan.rest_base"), get_secret("DHAN_CLIENT_ID"), get_secret("DHAN_ACCESS_TOKEN"))
    results = []

    async def one(day, flag, code, strike, ot):
        to = (date.fromisoformat(day) + timedelta(days=1)).isoformat()
        r = await rest.rolling_option(NIFTY, flag, code, strike, ot, day, to)
        name = f"{day}_{flag}{code}_{strike}_{ot}.json"
        (raw / name).write_text(json.dumps(r.body if r.body is not None else r.text[:2000]), encoding="utf-8")
        rec = {"day": day, "flag": flag, "code": code, "strike": strike, "type": ot, "http": r.status,
               **summarize(r.body, "ce" if ot == "CALL" else "pe")}
        if r.status != 200:
            rec["error"] = r.text[:200]
        results.append(rec)
        print(json.dumps(rec, default=str))
        return r

    try:
        for day in ladder(date.today()):
            for flag in ("WEEK", "MONTH"):
                await one(day, flag, 1, "ATM", "CALL")
        rc = await one(RECENT, "WEEK", 1, "ATM", "CALL")
        rp = await one(RECENT, "WEEK", 1, "ATM", "PUT")
        for s in ("ATM+10", "ATM-10", "ATM+11"):
            await one(RECENT, "WEEK", 1, s, "CALL")
        await one(RECENT, "WEEK", 2, "ATM", "CALL")
        await one(RECENT, "MONTH", 2, "ATM", "CALL")
    finally:
        await rest.close()

    # cross-check RECENT vs PG snapshot history: spot vs our 1-minute spot bars
    xcheck = {}
    try:
        side = rc.body["data"]["ce"]
        dh = pd.DataFrame({"ts": side["timestamp"], "spot": side["spot"], "close": side["close"],
                           "strike": side["strike"]})
        sp = pd.read_parquet(cfg.path("derived") / "pg_spot_bars_1m" / f"date={RECENT}" / "part-0.parquet")
        sp["ts"] = pd.to_datetime(sp["ts"]).map(lambda t: int(t.timestamp()))   # bar start, epoch seconds
        m = dh.merge(sp[["ts", "close"]].rename(columns={"close": "pg_spot"}), on="ts", how="inner")
        xcheck = {"common_minutes": len(m), "dhan_minutes": len(dh),
                  "spot_mean_abs_diff": round(float((m["spot"] - m["pg_spot"]).abs().mean()), 2) if len(m) else None,
                  "spot_max_abs_diff": round(float((m["spot"] - m["pg_spot"]).abs().max()), 2) if len(m) else None,
                  "strike_changes_in_day": int((dh["strike"].diff().fillna(0) != 0).sum())}
    except Exception as e:  # noqa: BLE001 - report, don't hide
        xcheck = {"error": repr(e)}

    oldest = min((r["day"] for r in results if r.get("rows")), default=None)
    report = {"probed_at": datetime.now(IST).isoformat(timespec="seconds"), "endpoint": "/charts/rollingoption",
              "results": results, "oldest_day_with_data": oldest, "recent_crosscheck": xcheck}
    out = cfg.path("reports") / "oi03_rolling_options_probe.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(json.dumps({"oldest_day_with_data": oldest, "recent_crosscheck": xcheck}, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
