# NiftyTradingAI — Phase 1 (foundation + data recorder)

Design baseline: `NiftyTradingAI_v1.3_Revised_Architecture.docx`. Paper trading only; no order APIs exist in this code.

## What Phase 1 delivers

| Piece | Where |
|---|---|
| Config (non-secret) | `config/settings.yaml` |
| Secrets (.env or Windows Credential Manager) | `app/secrets.py`, `scripts/set_secret.py` |
| Structured logs (console + daily JSONL) | `app/logging_setup.py` → `logs/` |
| SQLite schema v1 (WAL, FK): InstrumentMasters, Instruments, DataSessions, SystemEvents, HistoricalImports | `app/database/` → `data/database/NiftyTradingAI.db` |
| Backup / restore with integrity check | `scripts/backup_db.py`, `scripts/restore_db.py` |
| Dhan auth smoke test + payload samples | `scripts/smoke_test.py` → `data/reports/smoke/` |
| Instrument master: download, dated copy, validate (fail closed), persist | `app/data/instruments.py` |
| Universe: index + 2 futures + ATM ±5 (+2 buffer); 0DTE + next expiry on expiry days | `app/data/universe.py` |
| WebSocket recorder → daily Parquet (raw bytes kept), reconnect + resubscribe, liveness watchdog, ATM re-centre, NTP offset | `app/data/feed_client.py`, `app/data/recorder.py` → `data/raw/date=…/stream=feed/` |
| Option-chain poller (raw JSON + flattened) | `app/data/chain_poller.py` → `stream=chain`, `stream=chain_raw` |
| Session verification report | `scripts/verify_session.py` → `data/reports/verify_<day>.json` |
| Minimal health view | `app/api/main.py` (http://127.0.0.1:8765) |
| One-time historical import from PostgreSQL (read-only) | `scripts/import_pg_history.py` → `data/historical/pg_filtered_option_data/` |

## Setup

```powershell
py -V:Astral/CPython3.12.13 -m venv .venv      # already done
.venv\Scripts\python -m pip install -r requirements.txt
copy .env.example .env                          # then fill in values, or use set_secret.py
.venv\Scripts\python -m pytest -q
```

Secrets are looked up in `.env` first, then Windows Credential Manager (service `NiftyTradingAI`):

```powershell
.venv\Scripts\python scripts\set_secret.py DHAN_ACCESS_TOKEN
```

The Dhan access token is valid for 24 hours: renew it each morning before 09:00.

## Daily run

```powershell
.venv\Scripts\python scripts\smoke_test.py --ws-seconds 30     # first time, and after any Dhan change
.venv\Scripts\python scripts\record_session.py                  # start ~09:00, runs until 15:35 IST
.venv\Scripts\python -m uvicorn app.api.main:app --host 127.0.0.1 --port 8765   # health view
.venv\Scripts\python scripts\verify_session.py                  # after close
.venv\Scripts\python scripts\backup_db.py
```

Short test run outside market hours: `record_session.py --duration-min 5` (the feed is quiet when the market is closed).

## Historical import from optionchain_db (read-only)

Source: `public.filtered_option_data` — ~10-second NIFTY snapshots, ATM ±5, with LTP, OI, volume, IV, Greeks,
top-of-book bid/ask + quantities and spot. Tagged `observed_top_of_book_snapshot` (no depth).
The importer forces a READ ONLY session and issues only SELECTs. Add `PG_HOST/PG_PORT/PG_DB/PG_USER/PG_PASSWORD`
to `.env` (ideally a dedicated read-only role), then:

```powershell
.venv\Scripts\python scripts\import_pg_history.py --list
.venv\Scripts\python scripts\import_pg_history.py
```

Re-running imports only missing days; `--force` re-imports. Every day is recorded in `HistoricalImports`
with row count, snapshot count, checksum and quality metrics (gaps, crossed quotes, zero bids).

## Phase 1 acceptance checklist (v1.3 §17)

| Criterion | How it is shown |
|---|---|
| One complete session recorded | `record_session.py` for a full trading day, then `verify_session.py` = PASSED |
| Reconnect tested | `tests/test_feed_client.py` (fake server) + `reconnects` in DataSessions on a live day |
| Timestamps / IDs validated | `verify_session.py`: seq/recv ordering, market-hours coverage, gaps, all IDs in dated master, LTT epoch check |
| Raw files readable | `verify_session.py` reads every part file |
| No secrets in Git | `scripts/check_no_secrets.py`; `.env` is git-ignored |
| DB backup / restore tested | `tests/test_backup_secrets.py` + `backup_db.py` |

## Open items this phase must close (v1.3 §21)

- **OI-01 / OI-13** — the smoke test saves sample packets: confirm depth levels and whether updates are change-only.
- **OI-02** — `option_chain_sample.json` confirms the chain response fields; the flattened `chain` stream is
  best-effort until then (raw JSON is always kept in `chain_raw`).
- Instrument-master security-ID column is assumed `SEM_SMST_SECURITY_ID` (falls back to `SECURITY_ID`);
  the master fails closed if neither exists.
- LTT epoch interpretation (UTC vs IST-shifted) is reported by `verify_session.py`.

## Single-writer note

In Phase 1 the recorder and import scripts are the only writers to `NiftyTradingAI.db` (do not run the PG import
while recording). From Phase 5 the trading-engine becomes the sole writer (v1.3 §12.1).
