# NiftyTradingAI — Phase 1 (foundation + data recorder) Phase 2 (market data layer), Phase 3 (indicator framework), Phase 4 (backtester + paper broker), Phase 5 (rule strategies + risk), Phase 6 (experiment tracking), Phase 8 (dashboard + alerts) and Phase 9 (paper engine + candidate labels)

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
py -V:Astral/CPython3.12.13 -m venv .venv      # already done (base interpreter: see note below)
.venv\Scripts\python -m pip install -r requirements.txt
copy .env.example .env                          # then fill in values, or use set_secret.py
.venv\Scripts\python -m pytest -q
```

**Base interpreter:** `.venv` runs on a project-local copy of CPython 3.12.13 in `.python\` (git-ignored;
`.venv\pyvenv.cfg` `home` points there). The original uv install was made from inside the Claude desktop app,
whose AppData is virtualised and invisible to a normal shell ("No Python at ..."). If `.python\` is lost:
install CPython 3.12.13 anywhere visible and set `home`/`executable` in `.venv\pyvenv.cfg` to it.

Secrets are looked up in `.env` first, then Windows Credential Manager (service `NiftyTradingAI`):

```powershell
.venv\Scripts\python scripts\set_secret.py DHAN_ACCESS_TOKEN
```

The Dhan access token is valid for 24 hours: renew it each morning before 09:00.

## Daily run — live paper trading (market-day runbook)

Paper only: no order API is ever called. The engine (`run_paper.py live`) records the feed itself, so never run
`record_session.py` at the same time (single-writer rule).

**Double-click launchers** (project root): `start_trading_day.bat` = the morning start below (`start_trading_day.bat
check` = pre-flight only, `start_trading_day.bat recover NAME` = restart after a crash); `dashboard.bat` = dashboard
only, opens http://127.0.0.1:8501.

**Evening before** (after 15:30): `powershell -ExecutionPolicy Bypass -File scripts\start_market_day.ps1 -PreflightOnly`
— runs `scripts\preflight.py --fix`: calendar, journal, kill switch, disk, Telegram, DB backup (made now), clock vs
NTP, Dhan token + its expiry time, instrument master (lot, VIX row), NIFTY LTP, and the 20 prior NIFTY sessions the
engine warms its indicators on (fetched if missing). Exit 0 = OK, 1 = warnings, 2 = fix before starting.

**Morning, 08:45–09:10 IST:**
1. If pre-flight said the token expires before the session ends: after that expiry time, generate a new access
   token on web.dhan.co and paste it into the `DHAN_ACCESS_TOKEN` line of `.env` (`.env` overrides Credential
   Manager; if the key is not in `.env`, use `scripts\set_secret.py DHAN_ACCESS_TOKEN`).
2. `powershell -ExecutionPolicy Bypass -File scripts\start_market_day.ps1` — pre-flight again (stops on a FAIL), the
   dashboard at http://127.0.0.1:8501 (minimised window), then the engine in this window until 15:35 IST, with a
   console copy in `data\logs\engine_console_*.log`. At the end it closes out, reconciles and sends the daily report.
3. Watch the dashboard. **Live session** shows the engine's in-memory state (signals, open positions, trades,
   P&L, automatic kill latch, incidents, the indicator values the strategies see), rebuilt every 5 s by a muted
   read-only replay of the journal (`app/engine/shadow.py`) that is checked against every engine checkpoint;
   **Market monitor → Live** shows the chain from the engine's quote stream. The database tables (Signals, Paper
   trades, Performance) fill only at the 15:35 closeout. Alerts go to Telegram.

**During the day:**
- Stop new entries: `.venv\Scripts\python scripts\kill_switch.py on --actor NAME --reason "..."`
  (`--emergency-close` also flattens open paper positions); resume with
  `kill_switch.py off --actor NAME --reason "..." --confirm RESUME`. An automatic trip (feed outage, risk error)
  needs `kill_switch.py ack` before resuming.
- Engine crashed or window closed: `start_market_day.ps1 -Recover -Actor NAME` — the journal is replayed and
  verified before trading continues. A plain restart is refused while an unfinished journal exists. The feed
  watchdog (30 s without an option quote → automatic kill) measures from the engine's first in-market tick and
  re-anchors at every recovery, so a late start or restart downtime is not counted as an outage (2026-11-27 fix).
- A late fresh start (after 09:17 IST) is allowed: indicators warm up on the prior sessions, but the opening range
  cannot be observed, so ORB is left out of that session (logged) and only pullback runs. Start before 09:15.

**After the close:** `.venv\Scripts\python scripts\run_paper.py status`, `scripts\verify_session.py`,
`scripts\backup_db.py`.

**Rehearsal outside market hours** (never touches the real run's journal, kill switch, risk carry or main DB):
`.venv\Scripts\python scripts\run_paper.py live --smoke --duration-min 3 --no-chain` — run id
`smoke_paper_rules_v1`, research DB, alerts to `data\paper\smoke_paper_rules_v1\_runtime\telegram_outbox.jsonl`.
API check: `scripts\smoke_test.py --ws-seconds 30`.

## Strategy book — 18-strategy forward test (run `book_v1`)

Implements two specifications in `app/book/`:
- "NIFTY Options Strategy Book — Implementation Specification": the base ten (U1, PB, Q1–Q8).
- "NIFTY Parallel Strategies N1–N8 — Implementation Specification": the parallel set N1–N8. Its own spec says it
  is for paper testing only: out-of-sample profit factor 1.16, with N2 and N7 losing money from Jul 2025 to Oct 2026.

The book runs inside the same paper engine and feed as its own run (`book_v1`), next to ORB/pullback
(`paper_rules_v1`). Each strategy finds its own signal and takes at most one trade a day. The only guard is the kill
switch: it blocks new entries, and a trade already open still exits by its own rules. Turn the book on or off and
pick strategies in `config/paper.yaml` → `book:`. Remove a code there to pause that strategy.

| Code | Name | Chart | Exit |
|---|---|---|---|
| U1 | Call · Pivot Breakout | 3 min | X1 |
| PB | Put · Opening-Range Breakdown | 3 min | E5 |
| Q1 | Call · Momentum Surge | 3 min | X1 |
| Q2 | Call · Afternoon Trend Ride | 3 min | E5 |
| Q3 | Call · Afternoon VWAP Burst | 5 min | X2 |
| Q4 | Call · VWAP Breakout | 3 min | X1 |
| Q5 | Put · Morning VWAP Flush | 1 min | X1 |
| Q6 | Put · Afternoon Slide | 1 min | E5 |
| Q7 | Put · Trend Breakdown | 5 min | X1 |
| Q8 | Call · Afternoon Scalp | 1 min | X1 |
| N1 | Call · SAR Flip Volatility Burst | 3 min | X2 |
| N2 | Put · SAR Flip Volatility Drop | 5 min | X1 |
| N3 | Call · Camarilla H4 Breakout | 5 min | E5 |
| N4 | Put · SAR Flip Trend Slide | 3 min | X1 |
| N5 | Put · Fisher Trend Drop | 5 min | E5 |
| N6 | Call · Late Opening-Range Breakout | 5 min | X1 |
| N7 | Call · Late SAR Flip | 5 min | E5 |
| N8 | Call · Early SAR Flip | 3 min | E5 |

`scripts\run_book.py strategies` prints the full rules.

Exit styles:
- **X1**: stop −15%, then a 15% trail once the trade is +15%.
- **E5**: stop −15%, move to break-even at +20%, then a 25% trail from +40%.
- **X2**: fixed stop −15%.

- **Signals:**
  - Signals come from closed NIFTY index candles, using 35 warm-up sessions plus today.
  - Entry is at the open of the next 1-minute option bar.
  - The strike is the one whose premium is closest to the strategy's target band.
  - Exits are on the option premium: stop, break-even or trail, a Supertrend flip on the index chart, or the
    15:15 square-off.
  - N1–N8 use their own indicator set (`app/book/nconditions.py`).
- **Costs follow each spec:**
  - The base book uses STT 0.1%.
  - N1–N8 use the N spec's dated schedule, which is STT 0.15% from 2026-04-01.
  - Each trade records its cost version (`book-cost-1` / `n-cost-1`).
- **Two P&L figures per trade:**
  - **model**: the specification's bar fills, slippage and costs, so it is comparable with the backtest;
  - **executable**: the live ask at the signal and the live bid at the exit, with the same costs and no slippage.
- **Where to see it:**
  - Dashboard **Strategy book → Today** shows a live snapshot the engine writes every 5 s.
  - Telegram sends an alert at each entry and exit.
  - At the 15:35 closeout the trades go to `BookTrades`, `PaperTrades`, `TradingSignals` and `CapitalUsage`. The
    **Forward-test results** tab then shows per-strategy daily P&L, cumulative P&L and drawdown.
- **Recovery:** the book is rebuilt from the journal like the rest of the engine. A strategy whose signal fired
  before a late start is marked `ENGINE_STARTED_LATE` for that day.
- **Validation and replays** use research-tier data: trade prices, ATM ±5 strikes only.
  - `.venv\Scripts\python scripts\run_book.py validate [--codes N1,N3]` checks every reference trade in both specs.
  - `run_book.py replay --from 2026-09-01 --to 2026-10-16 [--codes Q1,N3] [--persist]` writes CSVs to
    `data\backtests\book`. With `--persist` it also writes run `book_replay` to the research DB, shown in the
    same tab.

### Daily P&L report (Telegram, right after the 15:35 closeout)

The engine sends one message for the whole system: **"📊 NiftyTradingAI — Daily P&L Report"**. It covers the rule
strategies and all book strategies. The detailed rules and book reports stay in the engine log and on the dashboard.

The message has these sections:
1. **Day total**:
   - net (model), gross, charges, wins and losses;
   - executable net (live bid/ask);
   - best and worst trade;
   - peak capital in use.
2. **By group**: rules, base book and parallel N, each with its net and number of trades.
3. **By strategy**: each trade with its contract, entry→exit times and prices, exit reason, net and executable net.
   Then one line per group listing strategies that had a signal but did not trade (and why), and those with no signal.
4. **Run to date**:
   - net, month to date, profit factor and max drawdown;
   - best and worst day;
   - per strategy: trades, win rate, net, PF, max DD and MTD.
5. **Notes**: kill switch still on, closeout problems.

A long day is sent in numbered parts. The report is exempt from the daily alert cap, so it is never held back.
Preview any past day on **Strategy book → Daily report**. Re-send it with:
`.venv\Scripts\python scripts\send_daily_report.py --combined --db main --date 2026-12-01 --send`
(without `--send`, it goes to the outbox file only).

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
| Event calendar with mandatory source + blackout windows. OI-07 decided: macro events are entered by hand from official publishers (RBI MPC FY27 loaded); the NSE event calendar covers corporate filings only and has no supported API | `config/calendar/events.yaml` |
| Dated instrument map + futures roll rule (volume cross-over or 2 trading days before expiry) | `app/market/mapping.py` |
| Quote normalizer with quality flags (DUPLICATE, OUT_OF_ORDER, CROSSED, ZERO_QTY, INVALID_INSTRUMENT, CLOCK_SKEW, PARSE_ERROR, LATE); bid/ask never forward-filled | `app/marketdata/normalize.py` |
| Staleness: feed liveness vs per-instrument change age (FRESH / QUIET_CONFIRM / STALE / FEED_DOWN) | `app/marketdata/staleness.py` |
| Streaming 1-minute bar builder (same code live and batch), exchange-time bucketing, watermark finalization, late-event accounting, day-high/low extreme recovery | `app/marketdata/bars.py` |
| Snapshot-history bars (tier `reconstructed_from_snapshots`) | `bars.snapshot_bars` → `data/derived/pg_option_bars_1m/`, `pg_spot_bars_1m/` |
| Daily data-quality matrix per instrument | `app/marketdata/quality.py` → `DataQualityDaily` |
| Reconciliation vs Dhan `/charts/intraday` candles | `app/marketdata/reconcile.py` |
| SQLite schema v2: `BarBuilds` (provenance + reconciliation JSON), `DataQualityDaily` | `app/database/models.py` |

```
.venv\Scripts\python scripts\build_history_bars.py --reconcile     # PG history -> bars, spot vs Dhan index candles
.venv\Scripts\python scripts\build_bars.py 2026-12-01 --reconcile  # recorded feed day -> bars + quality matrix
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
**Decision (2026-11-26):** India VIX is the market volatility-level input for rules and regimes. Per-contract
BSM IV is kept for Greeks only. Delta is effectively independent of the time convention, because the premium pins
σ√T: shortening the 0DTE clock by 38% moved IV by ~4 vol points but ATM delta by < 0.005. Contract selection is
therefore unaffected. `atm_iv` must not be compared across DTEs as a level.

**India VIX stream (built 2026-11-26).** The instrument master's `INDIA VIX` row (IDX_I, security id 21) is stored
as role `vix` (never confused with the NIFTY `index` role) and subscribed every session in ticker mode. The live
adapter builds final 1-minute `vix` bars, which are journaled like every other engine input. History:
`scripts/backfill_vix.py` pulls Dhan intraday candles (read-only data API) into
`data/historical/india_vix/india_vix_1m.parquet`; all 59 history sessions are covered (`--coverage` to check).
Backtests, experiments and paper replays load it automatically (it is part of the data fingerprint).
Every strategy decision records `market.vix` (the VIX bar of the minute **before** the decision bar, so
arrival order within a minute cannot change a decision; missing VIX is recorded as `None`, never guessed) and
`market.vix_chg_pct_session`; labels carry them as `mkt_vix` / `mkt_vix_chg_pct_session` and both are on the
meta-model's feature allow-list. **No rule uses VIX yet**: a VIX-based filter or regime is a new strategy
variant and goes through a Phase 6 experiment first.

**Approval (2026-11-26):** after the framework-hash change (`code_sha` now also covers `base.py` and the
`Resampler`), all 19 indicators were revalidated (all pass). The 17 validated on real history were set APPROVED on
the owner's explicit instruction (audited, actor `anoop2200`). `vwap` and `fut_rel_volume` stay DRAFT until real
futures data exists.

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

Open items: verify `config/fees.yaml` against a real Dhan contract note. IPFT (NSE Investor Protection Fund Trust
levy) is currently 0. NSE cut the options IPFT to about ₹0.01/crore of premium from 1 Mar 2026 and raised the
transaction charge to compensate, which may move `exchange_txn_pct` slightly; this is unconfirmed until the
contract note or circular FA73061 is read; latency 250 ms is
an assumption until live decision→order timings are measured; history has only top-of-book (depth-ladder
fills get exercised once live sessions are recorded).

## Phase 5 — rule strategies + independent risk engine (v1.3 §6, §7, §9)

| Piece | Where |
|---|---|
| Risk config (versioned, strictly validated; missing/invalid = NO TRADE for everything) | `config/risk.yaml`, `app/risk/config.py` |
| Risk engine: daily loss −3R (realized + unrealized), open risk 2R, 1 position, 5 entries/day, 3-loss latch (manual reset, next session only), 15-min cooldown after a stop, entry windows (09:25–15:00), gap-day block to 09:45, event blackout, 0DTE limits (2 entries, 14:30, 15-min hold, template ranges), quote/chain/feed freshness, min premium ₹30, spread ≤ max(2 ticks, 1% mid) and ≤ 10% of target, planned R incl. charges + slippage, shadow sizing (report only). Every check is evaluated and audited; an exception is NO TRADE + kill switch | `app/risk/engine.py` |
| Kill switch: manual (state file re-read on every evaluation, optional emergency close) and automatic (feed outage, clock skew, DB write failure, crash loop, inconsistent risk state, evaluation error) with operator acknowledgement | `app/risk/killswitch.py` |
| Cycle rules: key = profile+date+strategy+setup+direction, one position per cycle, no re-entry, no CE+PE for the same strategy, opposite direction needs a fresh confirmation | `RiskEngine` + broker `cycles_open` |
| Opening-range breakout (15-min range, k·ATR confirmation, range expansion, futures participation, invalidation = close back in range) | `app/strategies/orb.py` |
| Trend pullback (5-min EMA9/21 + ADX/DI trend, touch of EMA21, resumption with RSI, futures VWAP agreement, invalidation EMA21 ∓ 0.5·ATR) | `app/strategies/pullback.py` |
| Contract eligibility + deterministic ranking (ATM ±5, nearest expiry + next on expiry day, abs delta 0.35–0.65, spread, theta burn; every rejected contract recorded) | `app/strategies/contracts.py` |
| Decision pipeline (data gate → strategy → contract → risk → EntryIntent), risk fed by broker events, invalidation / daily-loss / emergency exits | `app/pipeline/decision.py` |
| TradingSignals + RiskEvents (schema v5) | `app/database/models.py` |

```
.venv\Scripts\python scripts\run_strategies.py --verify-replay   # -> data/backtests/<run_id>/ + research DB
```

### Phase 5 acceptance checklist

| Criterion | Evidence |
|---|---|
| Risk unit tests cover every limit | `tests/test_risk.py` (61 tests: each limit, windows, 0DTE, gap day, blackout, cooldown, latch + reset rules, quote checks, planned R) |
| Cycle / duplicate / opposite-direction tests | `test_duplicate_cycle_*`, `test_opposite_direction_*`, `test_ce_and_pe_cannot_both_be_open_*` |
| No trade when risk config is missing | `test_missing_config_means_no_trade`, 11 invalid-config cases, `test_pipeline_missing_risk_config_never_trades` |
| No live order calls | `test_no_live_order_calls_anywhere_in_app` (static scan of `app/`), paper broker only |

**Real-data run** (59 sessions, label `history_without_futures/observed_top_of_book_snapshot`): the PG history has
no futures bars, so ORB futures participation and pullback futures-VWAP are switched off for this run only
(production keeps them required). The consecutive-loss latch is reset by a simulated operator at the next session
(`--no-latch-reset` to disable). Result: 127 trades (ORB 82, pullback 45), net −₹14,197 after ₹7,924 charges,
avg −0.03R; 0–1 DTE trades positive, 2+ DTE negative; 66 exits by setup invalidation. Deterministic replay hash
identical across runs, all audit chains valid. These are **untuned first-version rules on low-fidelity data —
not evidence of an edge**; Phase 6 (experiment tracking) is where parameters get evaluated walk-forward.

Open items: futures history is needed to test the strategies as specified; the ORB invalidation (any close back
inside the range) drives most exits and is the first thing to evaluate in Phase 6; the indicator registry hashes
only each indicator class, so framework changes in `app/indicators/base.py` (Phase 5 fixed the missing-data reset
for resampled 5-minute bars) do not demote VALIDATED status automatically.

## Phase 6 — experiment tracking (v1.3 §10.2–§10.4)

| Piece | Where |
|---|---|
| Experiment spec: hypothesis, strategies, parameter grid, walk-forward layout, selection metric, frozen hold-out variant; canonical hash; strict validation of every parameter | `app/research/spec.py`, `config/experiments/*.yaml` |
| Multi-variant simulator over history with the production pipeline + risk engine (days outer, variants inner; shared with `run_strategies.py`) | `app/research/simulate.py` |
| Fixed chronological hold-out (latest 20%, min 20 sessions), rolling/anchored walk-forward on development sessions only | `app/research/splits.py` |
| Metrics: net expectancy (₹ and R), PF, win rate, max DD (₹ and R), session Sharpe, exit-reason shares, costs, turnover; DTE / strategy / regime / time-of-day segments; session-block bootstrap CIs; Deflated Sharpe Ratio with the global trial count; §10.4 promotion screen | `app/research/metrics.py` |
| Hold-out custody (single use, every request logged granted or refused, exposures documented), strategy-set variant counter (counts failed experiments too), code fingerprint | `app/research/registry.py` |
| Runner: RUNNING → COMPLETED / FAILED (traceback kept), results hash, rerun check, report.json + report.md | `app/research/runner.py` |
| Experiments, ExperimentResults, StrategyTrials, Holdouts, HoldoutAccessLog (schema v6, research DB) | `app/database/models.py` |

```
.venv\Scripts\python scripts\run_experiment.py run config\experiments\orb_invalidation_v1.yaml --actor NAME
.venv\Scripts\python scripts\run_experiment.py rerun <experiment_id> --actor NAME
.venv\Scripts\python scripts\run_experiment.py list
.venv\Scripts\python scripts\run_experiment.py holdouts
```

The hold-out is only evaluated with `run <spec> --holdout --actor NAME --purpose "..."` for the spec's
`holdout_variant`; that access is logged and retires the hold-out. `run_strategies.py` now excludes the sealed
hold-out unless `--include-holdout --actor NAME` (recorded as an exposure, which retires it).

### Phase 6 acceptance checklist

| Criterion | Evidence |
|---|---|
| Same experiment reruns reproduce metrics | `test_rerun_reproduces_metrics`, `test_changed_result_is_flagged_not_reproduced`; real data: `orb_invalidation_v1` rerun gave the identical results hash (`reproduced=1`) |
| Hold-out access logged | `test_holdout_sealed_is_stable_and_single_use_with_logging`, `test_holdout_evaluated_once_logged_and_retired`, `test_holdout_sessions_never_used_in_development`; `HoldoutAccessLog` |
| Failed experiments retained | `test_failed_experiment_is_retained_with_trials_counted` (status FAILED, traceback, variants still counted) |

**First experiment — `orb_invalidation_v1`** (6 variants: ORB invalidation range_edge / range_mid /
opposite_edge × confirmation 0.25 / 0.5 ATR; development 2026-08-27..2026-10-23, 39 sessions; hold-out
2026-10-26..2026-11-25 sealed). The hypothesis that a looser invalidation helps is **not supported**: every
looser variant did worse than the Phase 5 default over the development period. Walk-forward OOS (3 folds, 24
sessions): 48 trades, net −₹8,252, −0.04 R, PF 0.72; bootstrap 95% CI of expectancy straddles zero; DSR 0.08.
Verdict REJECT (OOS sample, net performance, PF, multiple testing). 0DTE was the only positive segment (8 trades —
far too few to mean anything).

Hold-out note: the Phase 4/5 full-history runs reported aggregates that include the hold-out sessions before it
was sealed, and this experiment's hypothesis came from those aggregates. Hold-out 1 was therefore **retired by
operator decision on 2026-11-26** (recorded in `HoldoutAccessLog`). All 59 history sessions are now development
data. History is the snapshot tier, which cannot meet the observed-feed promotion gate anyway. The next hold-out
forms automatically from live-recorded sessions after 2026-11-25, once at least 20 of them exist.

## Phase 8 — dashboard, kill switch, alerts, daily report (v1.3 §14, §9.5, §9.7)

Phase 7 (ML) was first deferred; its training stack is now built (see the Phase 7 section) and waits for prospective labels.

| Piece | Where |
|---|---|
| Streamlit dashboard, localhost only: Home/health (state, recorder, clock, CPU/memory, events, operator-log integrity), Market monitor (history chain ATM±5, quote age, quality flags), Signals, Paper trades + capital usage, Performance (equity/drawdown, expectancy, PF, R distribution, hour / DTE / strategy / exit breakdowns, daily report), Backtests & research, Config & kill switch | `app/dashboard/ui.py`, `.streamlit/config.toml` |
| Read-only data access: every DB opened with SQLite `mode=ro` | `app/dashboard/data.py`, `make_ro_engine` in `app/database/db.py` |
| Audited operator actions: hash-chained `data/runtime/operator_actions.jsonl` (name + reason required); kill switch on/off (`RESUME` to turn off), optional emergency close; acknowledgement of an automatic trip via a command file the engine consumes; whitelisted risk-config edits (`APPLY` to confirm), checked by the engine's own validator, version bumped, comments kept, effective at the next session start | `app/control/actions.py`, `app/risk/killswitch.py` |
| Telegram: CRITICAL / WARNING / INFO, dedup keys, per-kind throttles, daily cap on non-critical messages, digest of what was held back, failed CRITICAL retried; token never logged. Without `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` it writes `data/runtime/telegram_outbox.jsonl` | `app/notify/telegram.py` |
| Daily P&L report (§14.2 + §9.7): net/gross/charges, win/loss/timeout counts, R total, session peak capital with time and open trades, peak over the last 20 sessions and since inception, intraday and cumulative max drawdown, largest trade R, shadow capital (0.50% and 0.25% 0DTE rules), live-capital **planning estimate**, DTE breakdown on expiry days | `app/reports/daily.py` |

```
.venv\Scripts\python -m streamlit run app\dashboard\ui.py            # http://127.0.0.1:8501
.venv\Scripts\python scripts\kill_switch.py status
.venv\Scripts\python scripts\kill_switch.py on  --actor NAME --reason "..." [--emergency-close]
.venv\Scripts\python scripts\kill_switch.py off --actor NAME --reason "..." --confirm RESUME
.venv\Scripts\python scripts\kill_switch.py ack --actor NAME --reason "..."
.venv\Scripts\python scripts\kill_switch.py log
.venv\Scripts\python scripts\send_daily_report.py [--db main] [--run-id ID] [--date YYYY-MM-DD] [--send]
.venv\Scripts\python scripts\set_secret.py TELEGRAM_BOT_TOKEN      # and TELEGRAM_CHAT_ID, to send for real
```

Dashboard additions (2026-11-26): India VIX (intraday + daily history) on Market monitor; the VIX recorded with
each decision on Signals; the meta-model registry (versions, failed gate checks, walk-forward folds, out-of-fold
calibration) and the expired-option research tier (coverage, inferred expiries, per-day ATM CE/PE chart) on
Backtests & research. `.claude/launch.json` starts it for the desktop app's browser pane.

Until Phase 9 runs the live paper engine, the dashboard and report read the backtest runs in the research DB, and
the market monitor shows the imported history chain (labelled as such). The risk validator now also bounds the
UI-editable fields (`zero_dte.max_entries >= 0`, `cooldown_after_stop_min >= 0`, 0DTE last entry no later than
the general last entry).

### Phase 8 acceptance checklist

| Criterion | Evidence |
|---|---|
| UI read-only except audited config / kill actions | `test_read_only_engine_rejects_writes`, `test_dashboard_code_has_no_write_paths`, `test_operator_log_chain_and_tamper`, `test_operator_log_requires_actor_and_reason`, `test_config_change_*` (whitelist, validation, file untouched on refusal, comments kept, version bump) |
| Notifications deduplicated | `test_dedup_and_throttle_and_digest`, `test_cap_never_suppresses_critical`, `test_failed_critical_is_retried`, `test_telegram_errors_never_contain_token` |
| Kill switch blocks new entries immediately | `test_kill_on_blocks_risk_engine_immediately` (next evaluation rejected, `RESUME` required), `test_ack_command_is_consumed_by_engine_switch`, `test_risk_engine_reads_the_files_the_dashboard_writes` |
| Daily report includes the capital-usage metrics | `test_daily_report_capital_metrics` (hand-computed peak, cumulative DD, shadow capital, planning figure, DTE split); real run: `send_daily_report.py` on `20261126-190221_rules_v1` |

## Phase 7 — meta-model training stack (v1.3 §6.3, §11) — built, waiting for data

Everything that can be built before the data exists is built and tested; **no model is in the trading path**.
`PipelineConfig.meta_label` stays rule-only and a test forbids any `app.ml` import from the engine, strategies,
risk, pipeline or backtest code.

| Piece | Where |
|---|---|
| Dataset: `TradeLabels` for one horizon → resolved labels only (TARGET_FIRST / STOP_FIRST / TIMEOUT); decision-time features through an explicit allow-list; absolute price levels turned into relative ones (ATR %, position in the opening range, EMA gap / ATR, ...); outcome and risk-engine columns can never be features (`leakage_report`); `session_date` is the CV group; dataset fingerprint (sha256) | `app/ml/dataset.py` |
| Target: P(win), win = labelled one-lot trade closed with positive net P&L after charges within the horizon | `app/ml/train.py` |
| Walk-forward CV: sessions sorted, expanding window, ≥20 training sessions, 1-session embargo, each fold = fit block → later calibration block → embargo → later test block; no session ever in two roles | `walk_forward_folds` |
| LightGBM (small, regularised, deterministic) + Platt (sigmoid) or isotonic calibration fitted on the calibration block only | `fit_calibrated`, `Calibrator` |
| Take/skip threshold = break-even P(win) from the training window's average win and loss (never tuned on test) | `breakeven_p` |
| Baselines: the training window's base rate as a constant probability (Brier, log loss, ECE) and the rule-only policy that takes every candidate (net after costs); day-block bootstrap of the net difference | `evaluate`, `oof_metrics` |
| Promotion gate: prospective labels only, label adequacy (500 / 100 per class / 60 sessions), no leakage, ≥30 out-of-fold sessions, Brier and log loss beat baseline, ECE ≤ 0.05, net improvement > 0 and its bootstrap 5th percentile > 0 | `promotion_gate` |
| Registry (schema v8): `ModelTrainingRuns` (dataset fingerprint, folds, metrics, gate, code fingerprint), `ModelVersions` (JSON artifact under `data/models/` with sha256 — no pickle; status RESEARCH_ONLY / CANDIDATE / APPROVED / RETIRED), `ModelPredictions` (out-of-fold predictions). History-trained or gate-failed models are RESEARCH_ONLY and can never be approved; approval needs a CANDIDATE, a matching artifact hash and an operator name + reason (written to the hash-chained operator log); a new approval retires the old one | `app/ml/registry.py` |

```
.venv\Scripts\python scripts\train_meta_model.py train                         # prospective labels, main DB
.venv\Scripts\python scripts\train_meta_model.py train --db research --history # pipeline dry run (RESEARCH_ONLY)
.venv\Scripts\python scripts\train_meta_model.py list [--db research]
.venv\Scripts\python scripts\train_meta_model.py approve --id N --actor NAME --reason "..."
```

Dry run on the history labels (research DB, horizon 15): 187 resolved candidates over 54 sessions, 104
out-of-fold rows over 33 sessions. The model does **not** beat the baseline (Brier 0.296 vs 0.274, ECE 0.17) and
loses money against rule-only (−6,077 per lot over the test folds) → `meta_v1 v1` RESEARCH_ONLY. Expected with
this little data; it shows the guards work, not that a model is impossible. Re-run with the two VIX
features (`meta_v1 v2`): worse again (Brier 0.313, −11,233 per lot vs rule-only) — more features on 104 rows
mostly add overfitting; VIX's value can only be judged on prospective data.

**Still to do (needs data, not code):** ≥500 prospective labels (`run_paper.py labels` shows progress), then
`train`, then — only for a CANDIDATE — operator approval, then shadow mode (log predictions next to rule-only
decisions, no effect on trades) before the meta-model may filter anything. The shadow wiring is deliberately not
built yet: it should be designed against a real approved model.

### Phase 7 acceptance checklist

| Criterion | Evidence |
|---|---|
| No leakage: decision-time features only, day-grouped chronological validation | `test_dataset_excludes_unresolved_and_has_no_outcome_features`, `test_leakage_report_flags_outcome_columns`, `test_walk_forward_folds_are_chronological_and_disjoint`, `test_future_outcomes_do_not_change_earlier_predictions` |
| Calibrated probabilities better than baseline | `test_signal_beats_baseline_and_noise_does_not`, `test_calibrator_roundtrip_and_ece`, gate checks |
| Improves net after-cost metrics vs rule-only | `net_after_cost_improves` + `net_improvement_robust` gate checks; `test_gate_requires_every_check` |
| Only enough prospective labels count; history never promotes | `test_history_model_is_research_only_and_never_approvable`, `test_adequacy_counts_only_prospective_labels` |
| Registry, artifacts, approval | `test_prospective_model_candidate_approve_and_supersede`, `test_tampered_artifact_blocks_approval` |
| Meta-model outside the trading path until approved | `test_trading_path_does_not_use_the_meta_model` |
| **Model trained on ≥500 prospective labels and passes the gate** | **Not yet — needs live sessions.** |

## Phase 9 — integrated paper trading (rule-only) + the Phase 7 label pipeline (v1.3 §6.3, §12, §17)

| Piece | Where |
|---|---|
| `StreamEngine`: push-driven twin of the backtest replay (same ordering rules: quote < timer < bar at equal times; late events clamped and counted). Proven equal to `ReplayEngine` on all 59 history sessions (identical audit chains) | `app/engine/stream.py` |
| `PaperSession`: one trading day. Every input (quote, final bar, clock tick, kill-switch change, operator command) is written to a write-ahead journal, then applied to the production DecisionPipeline / RiskEngine / PaperBroker. The risk engine's kill switch is in-memory and driven only by journaled records (the real kill-switch file and command inbox are polled before every bar and tick), so recovery replays kill-switch effects exactly, and a manual kill still blocks the very next decision. Feed-outage watchdog (no option quote for 30 s in market hours, pre-open quiet excluded) → automatic kill switch. Indicator warm-up from the previous 10 sessions (stored in the journal's start record). Consecutive-loss latch carried to the next session; reset only by an audited operator command in a later session | `app/engine/session.py`, `app/engine/journal.py` |
| Restart / recovery (§12.3): an unfinished journal blocks the engine until `--recover --actor NAME`; the journal is replayed into a fresh engine and verified against its checkpoints (audit head every 2,000 events); a mismatch trips the automatic kill switch (`RISK_STATE_INCONSISTENT`) and opens a critical incident. Torn last line from a crash is ignored | `PaperSession.recover` |
| Closeout every session: day closed, every candidate labelled, reconciliation (audit chain; each trade's quantities, prices and P&L re-derived from audit fills; charges recomputed from the fee schedule per order; peak capital re-derived from the audit fill sequence), persistence (trades, audit, capital usage, signals, risk events, labels, `PaperSessions`), journal gzip, daily P&L report, carry file | `app/engine/closeout.py` |
| Live wiring: the market-data recorder keeps the only Dhan connection and hands every feed batch to the engine in the same process (`Recorder.taps`); `LiveAdapter` builds 5-level-depth option quotes and final index / futures bars (a quiet feed's bar is closed by the clock) | `app/engine/feeds.py`, `app/data/recorder.py` |
| **Candidate labels (Phase 7 track)**: for every APPROVED and risk-REJECTED candidate and horizons 5 / 10 / 15 / 30 min, a fresh PaperBroker opens one lot of the selected contract at the decision time with the candidate's stop / target → TARGET_FIRST / STOP_FIRST / TIMEOUT (bid-side exit net of charges), NO_ENTRY or UNLABELLABLE. Same fill model and fees as paper trading; decision-time features only; `session_date` is the grouping key; `traded=0` marks counterfactuals; history-tier labels never count toward the 500 prospective labels | `app/labels/labeller.py`, `TradeLabels` (schema v7) |
| Dashboard: paper-engine sessions (status, restarts, reconciliation, unresolved incidents) on Paper trades; label counts on Backtests & research | `app/dashboard/` |

```
.venv\Scripts\python scripts\run_paper.py live                         # each trading day (starts the recorder too)
.venv\Scripts\python scripts\run_paper.py live --recover --actor NAME  # after a crash / forced restart
.venv\Scripts\python scripts\run_paper.py status                       # sessions + 30-session operational streak
.venv\Scripts\python scripts\run_paper.py resolve --run-id paper_rules_v1 --date D --actor NAME --note "..."
.venv\Scripts\python scripts\run_paper.py labels                       # label counts + training readiness
.venv\Scripts\python scripts\run_paper.py replay [--from D] [--to D]    # history pushed through the engine "as live"
```

Configuration: `config/paper.yaml` (run id, strategies, thresholds). **Futures are not required** in paper
(`require_futures: false`): the research that justified ORB / pullback ran without futures and the futures
indicators are still DRAFT; switch it on only after they are validated on recorded feed data and approved.
Journals live under `data/paper/<run>/` (gitignored; gzip after closeout).

Replay rehearsal on all 59 history sessions (`replay_paper_rules_v1`, research DB): every session reconciled;
52 clean; 7 with a FEED_OUTAGE incident where the PG history has gaps of 2+ minutes (the watchdog doing its job,
relaxed to 120 s for 10–15 s snapshot data). With the watchdog off and 10-session warm-up, the paper engine
reproduces the Phase 5 backtest trade for trade except 2026-10-07, where the RBI blackout added in Phase 6
blocks one entry. Labels: 194 candidates × 4 horizons (history tier — pipeline exercise only; 0 of the 500
prospective labels).

Not yet exercised: a live market session (the market was closed when this was built). The first `run_paper.py
live` day is the real test of the adapter, the watchdog thresholds and the journal size with depth data.

### Phase 9 acceptance checklist

| Criterion | Evidence |
|---|---|
| 30 consecutive sessions without unresolved critical incidents | Operational: `run_paper.py status` counts the streak from `PaperSessions`; incidents are raised by the engine (`FEED_OUTAGE`, `RECOVERY_MISMATCH`) and resolved with `run_paper.py resolve`. `test_incident_resolution`. **Needs 30 live sessions.** |
| State recovers after forced restart | `test_forced_restart_recovers_identical_state` (crash early, with a position open, late → identical trades and audit chain), `test_torn_journal_tail_is_ignored`, `test_tampered_journal_is_detected_and_blocks_entries`, `test_completed_journal_cannot_be_recovered`, `test_warmup_is_journaled_and_used_on_recovery` |
| Paper fills and audit reconcile | `test_closeout_reconciles_and_persists`, `test_reconcile_detects_tampered_trade`; every replay session reconciled |
| Capital-usage figures reconcile to fills and charges every session | `capital_reconciles` / `charges_match_fee_schedule` in every closeout; same tests |
| Same code as backtest | `test_stream_engine_equals_replay_engine`, `test_clock_ticks_do_not_change_results`, `test_real_history_paper_engine_equals_backtest` |
| Kill switch / operator commands | `test_manual_kill_is_journaled_blocks_next_decision_and_replays`, `test_ack_and_latch_reset_commands_are_consumed_and_journaled`, `test_latch_reset_in_same_session_is_refused`, `test_feed_outage_watchdog` |
| Labels (Phase 7 data track) | `test_every_candidate_is_labelled_including_counterfactuals`, `test_label_matches_broker_trade_when_exits_coincide`, `test_labels_without_contract_or_after_close`, `test_adequacy_counts_only_prospective_labels` |

## OI-03 — expired NIFTY option history from Dhan (probe 2026-11-26)

`scripts/probe_rolling_options.py` (read-only data API `/charts/rollingoption`; raw responses in
`data/derived/oi03_probe/`, summary in `data/reports/oi03_rolling_options_probe.json`).

| Question | Finding |
|---|---|
| How far back? | Every probe from 1 week to 5 years back returned data; the oldest probed day, **2021-09-13**, has a full session (Dhan states 5 years) |
| Weekly / monthly | Both `WEEK` and `MONTH`, near (`expiryCode` 1) and next (2) expiry |
| Strikes | Relative to ATM, re-evaluated every minute (57 strike changes on 2026-11-25): **ATM±10** for near-expiry index options; `ATM+11` returns nothing. Not a fixed contract series |
| Fields | 1-minute open/high/low/close, volume, OI, IV, spot, strike — **no bid/ask, no depth** |
| Date range | `toDate` is effectively inclusive: a `fromDate`=D, `toDate`=D+1 request returns both days; bars run to 15:39 |
| Agreement with our PG snapshot history (2026-11-25) | Spot: 375 common minutes, mean abs diff 1.96 pts (max 13.6). ATM CE close vs PG option bars: mean abs diff 1.4 (median 1.0, p95 4.5) on a ~220 premium; the Dhan close sat inside the PG bid/ask only 22% of the time (last trade vs sampled quote; PG mean spread 0.52) |

**What it means.** Five years of minute option prices would turn 59 research sessions into ~1,200. But the
data is trade prices only. §18 forbids LTP-only fills and invented bid/ask, so it **cannot replace the
quote-based backtest**. It is usable, under its own data tier (`dhan_rolling_option_ltp`), for:
- strategy *pre-screening* with an explicit, labelled spread model (calibrated from recorded spreads, OI-12),
  whose results never count as evidence for promotion
- regime / indicator research on spot + IV + OI over many market conditions (2021-2026, incl. high-VIX periods)
- candidate-level features for long-horizon studies

It does **not** solve futures history (options only); expired futures would need a separate probe.
OI-03 status: **answered**.

**Imported (owner decision 2026-11-26: one year).** `scripts/import_rolling_options.py fetch --from 2025-11-26
--to 2026-11-25` → 245 trading days (16 weekday holidays came back empty), 2,021,030 rows: ATM±5 CE+PE of the
near weekly expiry, one file per day under `data/historical/dhan_rolling_options/` (gitignored), resumable
via a manifest. The response has no expiry date; `finalize` infers it from the time-to-expiry implied by Dhan's
own IV plus the expiry weekday (Thursday before 2025-09-01, Tuesday after) and a holiday shift taken from the
days that had no trading. Result: 53 weekly expiries, 6 of them Mondays (Tuesday holidays: 3 Mar, 31 Mar,
14 Apr, 20 Oct, 10 Nov, 24 Nov 2026), 0 consistency problems. `validate` against the 59 PG snapshot days:
**expiry 59/59 correct**; matched option closes differ from the PG snapshot close by median 0.43% (p95 2.7%).
Tier `dhan_rolling_option_ltp`; a test forbids any import of it from the engine, strategies, risk, pipeline,
backtest or research (experiment) code. Dashboard: Backtests & research → "Expired-option history".

```
.venv\Scripts\python scripts\import_rolling_options.py fetch --from D --to D [--each-side 5]
.venv\Scripts\python scripts\import_rolling_options.py finalize | coverage | validate
```

## Strategy pre-screen on one year of trade prices (research tier, 2026-11-26)

`scripts/screen_strategies.py` runs the **production** pipeline (ORB + pullback, contract ranking, risk engine,
paper broker, fees) over the 245 sessions of Dhan rolling-option history. Because that source has trade prices
only, quotes are **modelled**: at each minute's end, bid/ask = close ∓ half the median spread observed in the PG
snapshot history for that DTE bucket × moneyness (0.22–0.36% of mid), × a stress multiplier; top of book only
(the fill model adds its low-fidelity tick). Index and VIX bars: Dhan 1-minute candles (`backfill_vix.py
--days-from rolling [--nifty]`). Lot: the GCD of the day's open interest (75 until 2025-12-30, 65 from
2025-12-31; roll weeks fall back to that date). Label `screen_ltp_modelled_spread` — **not evidence**: it
cannot approve or promote anything, consumes no trial, touches no hold-out, writes no database (reports only,
`data/reports/screen_*`); a test keeps `app.screen` out of the engine, strategies, risk, pipeline, backtest,
research, ml and labels code.

**Bias check** (`screen_strategies.py bias`, the 59 days both sources cover): quote-based backtest 127 trades,
−12,854; screen 154 trades, −9,720 at 1× spread and −15,334 at 2×; 83 trades identical. Same sign and scale,
not trade-for-trade (different index-bar source) — usable to rank ideas, not to measure them.

**Full year, current rules** (2025-11-26 .. 2026-11-25):

| spread | trades | net ₹ | win rate | profit factor | max DD ₹ |
|---|---|---|---|---|---|
| 1× | 639 | −80,454 | 29% | 0.73 | −85,909 |
| 2× | 638 | −95,046 | 29% | 0.69 | −99,935 |
| 4× | 522 | −96,834 | 27% | 0.64 | −100,801 |

At 1×: gross −40,040 before charges of 40,414 — the rules lose **before** costs, so cheaper execution alone
cannot rescue them. ORB −58,089 on 469 trades (25% winners); pullback −22,365 on 170 (41%). Exits:
SETUP_INVALIDATED 361, TIME_STOP 217, TARGET 41, STOP 18. 10 of 13 months negative. By VIX at entry
(exploratory, terciles): every band loses, the high-VIX band most. This is the 59-day verdict again with 4×
the sessions: the current ORB / pullback rules have no edge, and a VIX filter on them does not create one.

Calendar: `config/calendar/nse_2025.yaml` added (Aug–Dec data-checked; Jan–Jul from the published list,
marked unverified); `nse_2026.yaml` v2 adds **2026-01-15** — a weekday with no NIFTY bars and no option data in
two Dhan sources that both published holiday lists omit.

```
.venv\Scripts\python scripts\screen_strategies.py bias
.venv\Scripts\python scripts\screen_strategies.py run [--from D] [--to D] [--multipliers 1,2,4]
```

## Single-writer note

In Phase 1 the recorder and import scripts are the only writers to `NiftyTradingAI.db` (do not run the PG import
while recording). From Phase 5 the trading-engine becomes the sole writer (v1.3 §12.1). The Phase 8 dashboard
opens every database read-only and changes state only through the audited files in `data/runtime/`.
