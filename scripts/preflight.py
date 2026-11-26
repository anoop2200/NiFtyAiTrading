r"""Pre-flight check for a live PAPER session - run it the evening before and again before 09:00 IST.

  .venv\Scripts\python scripts\preflight.py                 # checks the next session (today if before 15:30)
  .venv\Scripts\python scripts\preflight.py --fix           # also fetch missing index history + back up the DB
  .venv\Scripts\python scripts\preflight.py --date 2026-11-27

Read-only against Dhan (profile, LTP, intraday candles). Places no orders; writes nothing except, with --fix,
the NIFTY 1-minute history store and a database backup. Exit code: 0 all OK, 1 warnings only, 2 a failure that
would stop or degrade the session.
"""
import argparse
import asyncio
import json
import shutil
import socket
import struct
import sys
import time
from datetime import date, datetime

import _bootstrap  # noqa: F401
import yaml

from app.clock import IST, now_ist
from app.config import PROJECT_ROOT, settings
from app.control.actions import kill_status
from app.engine import live_history as lh
from app.engine.journal import Journal
from app.market.calendar import TradingCalendar

OK, WARN, FAIL = "OK", "WARN", "FAIL"


def token_where() -> str:
    """Where a new Dhan token must go: .env wins over Credential Manager (app.secrets lookup order)."""
    from dotenv import dotenv_values
    env = PROJECT_ROOT / ".env"
    if env.exists() and dotenv_values(env).get("DHAN_ACCESS_TOKEN"):
        return "paste it into the DHAN_ACCESS_TOKEN line of .env (.env overrides Credential Manager)"
    return r"store it with: .venv\Scripts\python scripts\set_secret.py DHAN_ACCESS_TOKEN"
PAPER_CFG = PROJECT_ROOT / "config" / "paper.yaml"


def target_session(cal: TradingCalendar, now: datetime) -> date:
    d = now.date()
    if cal.is_trading_day(d) and (now.hour, now.minute) < (15, 30):
        return d
    return cal.next_trading_day(d)


def ntp_offset_s(server: str = "time.windows.com", timeout: float = 3.0) -> float:
    """Local clock minus NTP time, seconds (SNTP, one packet; no data about the user is sent)."""
    pkt = b"\x1b" + 47 * b"\0"
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.settimeout(timeout)
        t0 = time.time()
        s.sendto(pkt, (server, 123))
        data, _ = s.recvfrom(48)
        t1 = time.time()
    secs, frac = struct.unpack("!II", data[40:48])
    server_t = secs - 2208988800 + frac / 2**32
    return (t0 + t1) / 2 - server_t


class Report:
    def __init__(self):
        self.rows = []

    def add(self, name: str, status: str, detail) -> None:
        self.rows.append({"check": name, "status": status, "detail": detail})
        print(f"[{status:<4}] {name}: {detail if isinstance(detail, str) else json.dumps(detail, default=str)}")

    @property
    def code(self) -> int:
        st = {r["status"] for r in self.rows}
        return 2 if FAIL in st else 1 if WARN in st else 0


async def dhan_checks(cfg, rep: Report, day: date, fix: bool) -> None:
    from app.data.dhan_rest import DhanRest
    from app.data.recorder import master_selection
    from app.database.db import init_db, make_engine
    from app.data.dhan_rest import extract_ltp
    from app.secrets import get_secret

    cid, tok = get_secret("DHAN_CLIENT_ID", required=False), get_secret("DHAN_ACCESS_TOKEN", required=False)
    if not cid or not tok:
        rep.add("dhan secrets", FAIL, "DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN not set - run scripts\\set_secret.py")
        return
    rep.add("dhan secrets", OK, "client id and access token present")
    rest = DhanRest(cfg.get("dhan.rest_base"), cid, tok, cfg.get("dhan.option_chain_min_interval_s"),
                    cfg.get("dhan.market_quote_min_interval_s"))
    try:
        prof = await rest.profile()
        if prof.status != 200:
            rep.add("dhan token", FAIL, f"/profile HTTP {prof.status} - regenerate the access token on web.dhan.co; "
                                        + token_where())
            return
        body = prof.body if isinstance(prof.body, dict) else {}
        validity = body.get("tokenValidity")
        detail = f"/profile OK; token valid until {validity}" if validity else "/profile OK"
        status = OK
        if validity:
            try:
                until = datetime.strptime(validity, "%d/%m/%Y %H:%M").replace(tzinfo=IST)
                end = datetime.combine(day, datetime.min.time(), IST).replace(hour=15, minute=40)
                if until < end:
                    status = FAIL
                    detail = (f"token expires {validity}, before the {day} session ends - after that time "
                              f"regenerate it on web.dhan.co, then {token_where()}")
            except ValueError:
                pass
        rep.add("dhan token", status, detail)

        db = make_engine()
        init_db(db)
        try:
            sel, _ = master_selection(cfg, db, now_ist().date())
            lot = sel.report.get("nearest_lot_size") if hasattr(sel, "report") else None
            vix = getattr(sel, "vix", None)
            rep.add("instrument master", OK if lot in (25, 50, 65, 75) and vix else WARN,
                    {"lot": lot, "vix_row": bool(vix), "index": sel.index.get("security_id"),
                     "note": "master for the session day itself is downloaded at engine start"})
        except Exception as e:  # noqa: BLE001
            rep.add("instrument master", FAIL, repr(e)[:200])

        r = await rest.ltp({"IDX_I": [13]})
        spot = extract_ltp(r.body, "IDX_I", 13)
        rep.add("nifty ltp", OK if spot else FAIL, f"{spot}" if spot else f"HTTP {r.status}")

        root = cfg.path("historical")
        want = lh.needed_sessions(TradingCalendar.load(), day, 20)
        gaps = lh.missing(root, want)
        if gaps and fix:
            print(f"fetching index history {gaps} ...")
            await lh.fetch_missing(rest, root, gaps)
            gaps = lh.missing(root, want)
        rep.add("index warm-up history", OK if not gaps else WARN,
                f"20/20 sessions up to {want[-1]}" if not gaps else
                f"missing {gaps} (engine fetches them at start; or run preflight --fix now)")
    finally:
        await rest.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date")
    ap.add_argument("--fix", action="store_true")
    ap.add_argument("--no-network", action="store_true", help="skip Dhan and NTP checks")
    a = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    cfg, cal, rep = settings(), TradingCalendar.load(), Report()
    now = now_ist()
    day = date.fromisoformat(a.date) if a.date else target_session(cal, now)
    print(f"pre-flight for session {day} (now {now:%Y-%m-%d %H:%M} IST)\n")

    if not cal.covers(day) or not cal.is_trading_day(day):
        rep.add("calendar", FAIL, f"{day} is not a trading session")
    else:
        rep.add("calendar", OK, f"{day} is a trading session ({cal.verification(day.year)})")

    pc = yaml.safe_load(PAPER_CFG.read_text(encoding="utf-8"))
    jp = PROJECT_ROOT / "data" / "paper" / pc["run_id"] / f"{day.isoformat()}.jsonl"
    if Journal.closed(jp) or (jp.exists() and any(r["k"] == "end" for r in Journal.read(jp))):
        rep.add("journal", FAIL, f"session already closed: {jp}")
    elif jp.exists():
        rep.add("journal", WARN, f"unfinished journal {jp} - start with --recover --actor NAME")
    else:
        rep.add("journal", OK, f"no journal yet for {pc['run_id']} {day}")

    ks = kill_status()
    rep.add("kill switch", WARN if ks["manual"] else OK,
            ks["state"] + ("" if not ks["manual"] else " - kill_switch.py off --actor NAME --reason ... --confirm RESUME"))

    free = shutil.disk_usage(PROJECT_ROOT).free / 2**30
    rep.add("disk space", OK if free > 5 else WARN if free > 1 else FAIL, f"{free:.1f} GiB free")

    from app.secrets import get_secret
    tg = get_secret("TELEGRAM_BOT_TOKEN", required=False) and get_secret("TELEGRAM_CHAT_ID", required=False)
    rep.add("telegram", OK if tg else WARN, "configured" if tg else "not configured - alerts go to "
                                                                     "data/runtime/telegram_outbox.jsonl")

    bdir = cfg.path("backups")
    last = max(bdir.glob("*.db"), key=lambda p: p.stat().st_mtime, default=None) if bdir.exists() else None
    if a.fix and cfg.path("database").exists():
        from app.database.backup import backup, prune
        last = backup(cfg.path("database"), bdir)
        prune(bdir, cfg.path("database").stem)
    age_h = (time.time() - last.stat().st_mtime) / 3600 if last else None
    rep.add("database backup", OK if age_h is not None and age_h < 36 else WARN,
            f"latest {last.name}, {age_h:.0f} h old" if last else "no backup yet - preflight --fix makes one")

    if not a.no_network:
        try:
            off = ntp_offset_s()
            rep.add("clock", OK if abs(off) < 1 else WARN if abs(off) < 3 else FAIL,
                    f"local clock {off:+.2f} s vs NTP" + ("" if abs(off) < 1 else " - resync Windows time"))
        except OSError as e:
            rep.add("clock", WARN, f"NTP unreachable ({e}); check Windows time sync manually")
        asyncio.run(dhan_checks(cfg, rep, day, a.fix))

    out = cfg.path("reports") / "preflight"
    out.mkdir(parents=True, exist_ok=True)
    (out / f"preflight_{now:%Y%m%d-%H%M%S}.json").write_text(
        json.dumps({"session": day.isoformat(), "at": now.isoformat(), "checks": rep.rows}, indent=1, default=str),
        encoding="utf-8")
    print(f"\nresult: {['ALL OK', 'WARNINGS', 'FAILED'][rep.code]}")
    return rep.code


if __name__ == "__main__":
    sys.exit(main())
