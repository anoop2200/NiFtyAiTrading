# NiftyTradingAI — Phase 1 (foundation + data recorder) Phase 2 (market data layer), Phase 3 (indicator framework) and Phase 4 (backtester + paper broker)

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

## Phase 2 — market data layer (v1.3 §17)

| Piece | Where |
|---|---|
| NSE trading calendar 2026 (holidays, sessions, Muhurat special session, expiry adjustment, DTE, fail-closed for uncovered years) | `config/calendar/nse_2026.yaml`, `app/market/calendar.py` |
| Event calendar with mandatory source + blackout windows (OI-07 still open: no events loaded yet) | `config/calendar/events.yaml` |
| Dated instrument map + futures roll rule (volume cross-over or 2 trading days before expiry) | `app/market/mapping.py` |
| Quote normalizer with quality flags (DUPLICATE, OUT_OF_ORDER, CROSSED, ZERO_QTY, INVALID_INSTRUMENT, CLOCK_SKEW, PARSE_ERROR, LATE); bid/ask never forward-filled | `app/marketdata/normalize.py` |
| Staleness: feed liveness vs per-instrument change age (FRESH / QUIET_CONFIRM / STALE / FEED_DOWN) | `app/marketdata/staleness.py` |
| Streaming 1-minute bar builder (same code live and batch), exchange-time bucketing, watermark finalization, late-event accounting, day-high/low extreme recovery | `app/marketdata/bars.py` |
| Snapshot-history bars (tier `reconstructed_from_snapshots`) | `bars.snapshot_bars` → `data/derived/pg_option_bars_1m/`, `pg_spot_bars_1m/` |
| Daily data-quality matrix per instrument | `app/marketdata/quality.py` → `DataQualityDaily` |
| Reconciliation vs Dhan `/charts/intraday` candles | `app/marketdata/reconcile.py` |
| SQLite schema v2: `BarBuilds` (provenance + reconciliation JSON), `DataQualityDaily` | `app/database/models.py` |

```
.venv\Scripts\python scriptsuild_history_bars.py --reconcile     # PG history -> bars, spot vs Dhan index candles
.venv\Scripts\python scriptsuild_bars.py 2026-12-01 --reconcile  # recorded feed day -> bars + quality matrix
```

### Documented reconciliation tolerance

Per minute present on both sides; a day passes when >= 95% of common minutes are within tolerance **and**
our bars cover >= 95% of the reference minutes.

| Field | Feed bars (`observed_feed`) | Snapshot bars (`reconstructed_from_snapshots`, ~10 s sampling) |
|---|---|---|
| open / close | max(2 ticks, 0.02%) | max(2 ticks, 0.06%) |
| high / low | inside reference range (±1 tick) and within 0.05% | inside range (±1 tick) and within 0.10% |
| volume | within 5% or 2 lots (index: not compared) | not compared |

Snapshot tolerance is wider because a minute's true open/close can lie up to one sampling interval away from
the first/last sample; NIFTY can move 10–20 points in that time.

**Result on the 59 imported days (2026-08-27 .. 2026-11-25), NIFTY spot vs Dhan index candles:** 56/59 days pass;
median 99.47% of minutes within tolerance, median |close error| 1.15 points. The 3 failing days (11-02, 11-13,
11-19) fail on coverage only (88–93%) — the known gaps in the PG history — while their present minutes are >99%
within tolerance. Report: `data/reports/history_bars_summary.json`.

### Phase 2 acceptance checklist

| Criterion | Evidence |
|---|---|
| Unit tests for out-of-order / duplicate / stale / crossed events | `tests/test_normalize_staleness.py`, `tests/test_bars_quality_reconcile.py` |
| Bars reconcile to source within documented tolerance | `build_history_bars.py --reconcile` (above); feed bars: `build_bars.py --reconcile` on the first recorded live day |
| Holiday / special-session test passes | `tests/test_calendar_mapping.py` (Tuesday-holiday expiries 10-20/11-10/11-24 → Monday, Muhurat not tradable, 2027 fails closed) |

Still open: feed-bar reconciliation needs one recorded live day (same day as Phase 1 acceptance); the 2027 holiday
file must be added once NSE publishes it; Muhurat timings are unconfirmed; OI-07 event source.

## Phase 3 — indicator framework (v1.3 §5, §4.4)

| Piece | Where |
|---|---|
| Indicator contract (id, semantic version, formula, source requirement, outputs, units, warm-up, max input age, missing-data rule, recovery method, dependencies) + standard states | `app/indicators/base.py` |
| 19 indicators: EMA, SMA, RSI, ATR, Bollinger, ROC, session VWAP, opening range (conventional); KAMA, ADX, realized vol, vol compression, confirmed swing pivots, futures relative volume, Market Strength composite (custom); PCR, ATM IV, CE/PE premium pressure, OI buildup (options) | `app/indicators/conventional.py`, `custom.py`, `options.py` |
| Single BSM pricing / IV / Greeks implementation with versioned assumptions (r 6.5%, q 1.2%, T = calendar seconds to 15:30) | `app/pricing/bs.py` |
| Engine: N-minute resampler (emits only completed bars), source-requirement checks, streaming replay, JSON checkpoint/restore, warm-up | `app/indicators/engine.py` |
| Chain stream (nearest expiry, ATM band, last snapshot per minute) | `app/indicators/chain.py` |
| Registry: code registry + `IndicatorDefinitions` (DRAFT → VALIDATED → APPROVED → RETIRED), `IndicatorRuns`; schema v3 | `app/indicators/registry.py` |
| Bindings config | `config/indicators.yaml` |

```
.venv\Scripts\python scripts\validate_indicators.py              # acceptance checks -> VALIDATED + report
.venv\Scripts\python scripts\compute_indicators.py --verify-replay  # all history -> data/derived/indicators/
.venv\Scripts\python scripts\set_indicator_status.py --list
.venv\Scripts\python scripts\set_indicator_status.py ema 1.0.0 APPROVED --actor <name> --note "..."
```

Rules enforced in code:
- Indicators see only final bars; a 5-minute indicator gets a bar only when its last minute is in.
- Missing required input → invalid output and NO_SIGNAL (fail safe), state untouched.
- Intraday data gap > `max_input_age_s` resets state; overnight gaps do not; session-anchored indicators
  (VWAP, opening range) reset each session.
- Bindings whose declared source the dataset cannot satisfy are skipped and logged (futures VWAP / relative
  volume on index-only history; index volume is never used as participation).
- Approval is a human, audited action and needs a passed validation report. An APPROVED definition whose
  code changes is refused until `SPEC.version` is bumped; a VALIDATED one falls back to DRAFT.

### Phase 3 acceptance checklist

| Criterion | Evidence |
|---|---|
| Batch and streaming outputs match within tolerance | `validation.batch_vs_stream` (rel 1e-9 / abs 1e-7); EMA, SMA, RSI, ATR, Bollinger, ROC, VWAP have an independent vectorised implementation; the others' batch path is the streaming loop |
| Deterministic replay | `compute_indicators.py --verify-replay`: 59 days, 20 bindings, 352k rows, identical output hash on two runs |
| No-lookahead tests | perturb every bar after a cut; outputs before it must not change (`tests/test_indicators.py`, incl. a deliberately leaky indicator that the check catches) |
| Restart warm-up test | JSON snapshot restore = uninterrupted run (incl. engine with a half-built 5-minute bar); replay warm-up for `recovery: replay` indicators |

`validate_indicators.py` on the last 10 history days: all 19 pass (17 on real history; futures VWAP and
relative volume on a labelled synthetic stream, so they cannot be approved until real futures data exists).

**IV check vs provider IV in optionchain_db** (ATM, last 10 days): with 1+ days to expiry our IV is within
~0.35 (PE) / ~1.1 (CE) vol points of the provider. On expiry day (0DTE) ours is ~12 vol points higher: the
provider evidently uses a different time-to-expiry convention. This matters because expiry days are traded —
**open item**: decide the 0DTE time convention (calendar seconds vs trading-time / variance-time) and stress-test
the r/q assumptions before any IV-based rule is approved.

## Phase 4 — event-driven backtester + paper broker (v1.3 §8.3, §9.6, §9.7, §10.2)

| Piece | Where |
|---|---|
| Versioned fee schedule (brokerage, exchange txn, SEBI, STT sell, stamp buy, GST) | `config/fees.yaml`, `app/backtest/fees.py` |
| Fill model: ask-entry / bid-exit ladder walk, VWAP, partial fills, price protection, consumed liquidity, top-of-book low-fidelity slippage, refusals (no/stale quote, side withdrawn, crossed, spread too wide) | `app/backtest/fills.py` |
| Paper broker: decision→arrival latency, order timeouts, exits never abandoned (re-submitted), premium stop/target on executable bid, time stop, flat by 15:25, one active position per cycle, capital usage | `app/backtest/broker.py` |
| Conservative intrabar resolution for OHLC-only data (stop first, gaps fill at open) | `broker.resolve_bar_exit` |
| Event-driven replay (quotes → timers → bar-close decisions, never backwards) using the same IndicatorEngine / broker as paper | `app/backtest/replay.py` |
| Hash-chained audit of every decision, attempt, fill, charge and close | `app/backtest/audit.py` |
| Persistence: PaperTrades, PaperTradeEvents, CapitalUsage (schema v4); backtests go to `NiftyTradingAI_research.db` | `app/backtest/persist.py` |
| History quotes (top-of-book snapshots, lot size from observed quantities, never today's master) | `app/backtest/history.py` |

```
.venv\Scripts\python scripts\run_backtest.py --verify-replay   # -> data/backtests/<run_id>/ + research DB
```

### Phase 4 acceptance checklist

| Criterion | Evidence |
|---|---|
| Golden replay fixtures pass | `tests/golden/replay_cases.json` — 13 hand-derived scenarios (target with exact charges, depth VWAP, partial fills, entry timeout, low-fidelity slippage, latency, stale quote, withdrawn bid, spread too wide, duplicate cycle, 15:25 flat, time stop, exit re-submit) |
| Conservative intrabar resolution | `tests/test_backtest_units.py::test_conservative_intrabar_resolution` |
| Ask-entry / bid-exit | fill model; golden cases; LTP is never a fill price (`test_never_uses_ltp_and_needs_a_side`) |
| Fill and cost audit reproducible | audit hash identical across runs (golden + full history); chain verification detects tampering |

**Real-data replay** (59 days, `observed_top_of_book_snapshot` quotes, plumbing-check ORB from `app/backtest/demo.py`):
85 trades, every fill flagged low-fidelity (no depth in history), identical combined audit hash on repeated runs,
all audit chains valid; fills spot-checked against raw snapshots (entry = ask + 1 tick, exit = bid − 1 tick).
The demo's P&L is **not** a strategy result — Phase 5 builds the real strategies with the risk engine.

Open items: verify `config/fees.yaml` against a real Dhan contract note (IPFT currently 0); latency 250 ms is
an assumption until live decision→order timings are measured; history has only top-of-book (depth-ladder
fills get exercised once live sessions are recorded).

## Single-writer note

In Phase 1 the recorder and import scripts are the only writers to `NiftyTradingAI.db` (do not run the PG import
while recording). From Phase 5 the trading-engine becomes the sole writer (v1.3 §12.1).
