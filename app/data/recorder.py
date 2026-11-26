"""market-data-engine (Phase 1): auth check, instrument master, universe, WebSocket + option-chain
capture to daily Parquet, health snapshot, session bookkeeping."""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from datetime import date, datetime

from loguru import logger
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.clock import IST, iso, now_ist, parse_hhmm, sntp_offset_ms
from app.config import Settings
from app.data.chain_poller import ChainPoller
from app.data.dhan_rest import DhanRest, extract_ltp
from app.data.feed_client import FeedClient, FeedFatal
from app.data.instruments import Selection, download_master, parse_master, persist, sha256_file, validate
from app.data.parquet_store import CHAIN_RAW_SCHEMA, CHAIN_SCHEMA, FEED_SCHEMA, PartitionedWriter
from app.data.universe import Universe, build_universe, recenter
from app.database.db import log_event
from app.database.models import DataSessions
from app.secrets import get_secret
from app.services.health import write_health

COMPONENT = "market-data-engine"
SILENT_FEED_RECONNECT_S = 40.0  # Dhan closes after 40 s without pong; treat 40 s of silence as half-open


@dataclass
class Prepared:
    rest: DhanRest
    selection: Selection
    master_id: int
    spot: float
    universe: Universe
    client_id: str
    token: str


def master_selection(cfg: Settings, engine: Engine, today: date) -> tuple[Selection, int]:
    url = cfg.get("dhan.instrument_master_url")
    d = cfg.path("historical") / "instrument_master"
    path = d / f"instrument_master_{today.isoformat()}.csv"
    if path.exists():
        sha = sha256_file(path)
    else:
        path, sha = download_master(url, d, today)
    ic = cfg["instrument"]
    sel = parse_master(path, today, ic["name"], ic["index_symbol"], ic["option_symbol_prefix"],
                       ic.get("index_security_id_override"))
    report = validate(sel, today)
    master_id = persist(engine, sel, url, path, sha)
    log_event(engine, "INFO", COMPONENT, "instrument_master_ok", report)
    return sel, master_id


async def prepare(cfg: Settings, engine: Engine, today: date, spot_override: float | None = None) -> Prepared:
    client_id, token = get_secret("DHAN_CLIENT_ID"), get_secret("DHAN_ACCESS_TOKEN")
    rest = DhanRest(cfg.get("dhan.rest_base"), client_id, token,
                    cfg.get("dhan.option_chain_min_interval_s"), cfg.get("dhan.market_quote_min_interval_s"))
    prof = await rest.profile()
    if prof.status != 200:
        await rest.close()
        log_event(engine, "ERROR", COMPONENT, "auth_failed", {"status": prof.status, "body": prof.text[:300]})
        raise RuntimeError(f"Dhan profile check failed: HTTP {prof.status}")
    log_event(engine, "INFO", COMPONENT, "auth_ok", {"status": prof.status})
    sel, master_id = master_selection(cfg, engine, today)
    spot = spot_override
    if spot is None:
        res = await rest.ltp({"IDX_I": [sel.index["security_id"]]})
        spot = extract_ltp(res.body, "IDX_I", sel.index["security_id"])
        if spot is None:
            await rest.close()
            raise RuntimeError(f"could not read NIFTY spot from LTP API (HTTP {res.status}): {res.text[:200]}")
    ic = cfg["instrument"]
    u = build_universe(sel, spot, today, ic["atm_strikes_each_side"], ic["transition_buffer_strikes"],
                       ic["futures_months"])
    return Prepared(rest, sel, master_id, spot, u, client_id, token)


class Recorder:
    def __init__(self, cfg: Settings, engine: Engine, prepared: Prepared, end_at: datetime,
                 chain_enabled: bool = True):
        self.cfg, self.engine, self.p, self.end_at = cfg, engine, prepared, end_at
        rc = cfg["recorder"]
        root = cfg.path("raw")
        self.feed_writer = PartitionedWriter(root, "feed", FEED_SCHEMA, rc["flush_rows"], rc["flush_seconds"])
        self.chain_writer = PartitionedWriter(root, "chain", CHAIN_SCHEMA, rc["flush_rows"], rc["flush_seconds"])
        self.chain_raw_writer = PartitionedWriter(root, "chain_raw", CHAIN_RAW_SCHEMA, 200, rc["flush_seconds"])
        self.health_interval = rc["health_interval_s"]
        self.liveness_s = rc["feed_liveness_s"]
        self.index_id = prepared.universe.index.security_id
        self.index_ltp: float | None = None
        self.clock_offset_ms: float | None = None
        self.last_error: str | None = None
        self.session_id: int | None = None
        self.taps: list = []          # in-process consumers of every feed batch (Phase 9 paper engine)
        self.tap_errors = 0
        url = (f"{cfg.get('dhan.feed_url')}?version=2&token={prepared.token}"
               f"&clientId={prepared.client_id}&authType=2")  # never logged
        self.feed = FeedClient(url, self._on_rows, self._on_event,
                               cfg.get("dhan.max_instruments_per_message"), rc["reconnect_backoff_s"])
        self.chain = None
        if chain_enabled:
            self.chain = ChainPoller(prepared.rest, self.index_id,
                                     [d.isoformat() for d in prepared.universe.option_expiries],
                                     self.chain_raw_writer.add, self.chain_writer.add_many, self._on_event)

    # ---- callbacks -----------------------------------------------------
    def _on_rows(self, rows: list[dict]) -> None:
        for r in rows:
            if r.get("security_id") == self.index_id and r.get("segment") == 0 and r.get("ltp"):
                self.index_ltp = r["ltp"]
        self.feed_writer.add_many(rows)
        for tap in self.taps:
            try:
                tap(rows)
            except Exception as e:  # noqa: BLE001 - a consumer must never stop the recording
                self.tap_errors += 1
                if self.tap_errors <= 5:
                    logger.error(f"feed tap failed: {e!r}")

    def _on_event(self, name: str, detail: dict) -> None:
        level = "WARNING" if "error" in name or "disconnect" in name else "INFO"
        if level == "WARNING":
            self.last_error = f"{name}: {json.dumps(detail, default=str)[:200]}"
        logger.log(level, f"{name} {detail}")
        try:
            log_event(self.engine, level, COMPONENT, name, detail, self.session_id)
        except Exception as e:  # never let bookkeeping kill the feed
            logger.error(f"event log failed: {e!r}")

    # ---- session bookkeeping --------------------------------------------
    def _open_session(self) -> None:
        with Session(self.engine) as s:
            ds = DataSessions(session_date=now_ist().date().isoformat(), started_at=iso(), status="RUNNING",
                              master_id=self.p.master_id, subscribed_instruments=len(self.p.universe.all()),
                              universe=json.dumps(self.p.universe.describe()))
            s.add(ds)
            s.commit()
            self.session_id = ds.id

    def _close_session(self, status: str, notes: str = "") -> None:
        with Session(self.engine) as s:
            ds = s.get(DataSessions, self.session_id)
            ds.ended_at, ds.status, ds.notes = iso(), status, notes
            ds.packets, ds.reconnects = self.feed.packets, self.feed.reconnects
            ds.chain_snapshots = self.chain.snapshots if self.chain else 0
            ds.subscribed_instruments = len(self.feed.subs)
            ds.universe = json.dumps(self.p.universe.describe())
            s.commit()

    def health(self, state: str) -> dict:
        age = self.feed.feed_age_s()
        return {
            "state": state, "component": COMPONENT, "session_id": self.session_id, "now": iso(),
            "end_at": self.end_at.isoformat(), "feed_connected": self.feed.connected,
            "feed_alive": bool(self.feed.connected and age is not None and age <= self.liveness_s),
            "feed_age_s": None if age is None else round(age, 2), "packets": self.feed.packets,
            "packets_by_code": dict(self.feed.by_code), "subscriptions": len(self.feed.subs),
            "reconnects": self.feed.reconnects, "spot": self.index_ltp, "atm": self.p.universe.atm,
            "chain_snapshots": self.chain.snapshots if self.chain else None,
            "chain_age_s": None if not self.chain or self.chain.age_s() is None else round(self.chain.age_s(), 1),
            "chain_errors": self.chain.errors if self.chain else None,
            "rows_written": {"feed": self.feed_writer.rows_written, "chain": self.chain_writer.rows_written},
            "clock_offset_ms": self.clock_offset_ms, "last_error": self.last_error,
        }

    # ---- main loop -------------------------------------------------------
    async def _monitor(self) -> None:
        last_ntp = 0.0
        ntp_every = self.cfg.get("recorder.ntp_check_interval_s", 300)
        server = self.cfg.get("recorder.ntp_server")
        while now_ist() < self.end_at:
            if time.monotonic() - last_ntp > ntp_every:
                last_ntp = time.monotonic()
                try:
                    self.clock_offset_ms = round(await asyncio.to_thread(sntp_offset_ms, server), 1)
                    if abs(self.clock_offset_ms) > 250:
                        self._on_event("clock_skew_warning", {"offset_ms": self.clock_offset_ms})
                except OSError as e:
                    self._on_event("ntp_error", {"error": repr(e)})
            age = self.feed.feed_age_s()
            if self.feed.connected and age is not None and age > SILENT_FEED_RECONNECT_S:
                await self.feed.force_reconnect(f"no packets for {age:.0f}s")
            if self.index_ltp:
                added = recenter(self.p.universe, self.p.selection, self.index_ltp)
                if added:
                    self._on_event("atm_recenter", {"atm": self.p.universe.atm, "added": len(added)})
                    await self.feed.subscribe(added)
            write_health(self.health("RUNNING"))
            await asyncio.sleep(self.health_interval)

    async def run(self) -> str:
        self._open_session()
        await self.feed.subscribe(self.p.universe.all())
        self._on_event("session_started", {"universe": self.p.universe.describe(), "spot": self.p.spot})
        tasks = [asyncio.create_task(self.feed.run(), name="feed")]
        if self.chain:
            tasks.append(asyncio.create_task(self.chain.run(), name="chain"))
        monitor = asyncio.create_task(self._monitor(), name="monitor")
        status, notes = "COMPLETED", ""
        try:
            done, _ = await asyncio.wait([monitor, tasks[0]], return_when=asyncio.FIRST_COMPLETED)
            if tasks[0] in done and tasks[0].exception():
                raise tasks[0].exception()
        except FeedFatal as e:
            status, notes = "FAILED", str(e)
        except asyncio.CancelledError:
            status, notes = "INTERRUPTED", "operator stop"
        finally:
            self.feed.stop()
            if self.chain:
                self.chain.stop()
            for t in [*tasks, monitor]:
                t.cancel()
            await asyncio.gather(*tasks, monitor, return_exceptions=True)
            for w in (self.feed_writer, self.chain_writer, self.chain_raw_writer):
                w.close()
            await self.p.rest.close()
            self._close_session(status, notes)
            write_health(self.health(status))
            self._on_event("session_ended", {"status": status, "packets": self.feed.packets,
                                             "reconnects": self.feed.reconnects, "notes": notes})
        return status


def end_time_today(cfg: Settings, duration_min: float | None = None) -> datetime:
    if duration_min:
        return datetime.fromtimestamp(time.time() + duration_min * 60, IST)
    t = parse_hhmm(cfg.get("session.record_end"))
    return now_ist().replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)
