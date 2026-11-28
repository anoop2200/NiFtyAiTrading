"""Incremental engine of the NIFTY options strategy book (Implementation Specification §2, §4, §7.1).

One BookEngine = one trading day. It is driven by the same inputs live and in a historical replay, so the live
forward test runs exactly the code that the validation replays:

  on_index_bar(bar, avail_ns)   a FINAL 1-minute NIFTY index bar (ts = candle start, epoch s)
  on_quote(...)                 live only: an option quote (LTP / cumulative volume / exchange trade time / top of
                                book). Trades build 1-minute option bars; quotes also feed the executable shadow.
  on_option_bar(key, bar)       historical only: a final 1-minute option bar
  on_time(now_ns)               clock: finalises option bars (live) and runs entries / exits
  advance_options(minute_ts)    historical only: option bars up to this minute start are final

Strategy rules (spec §2.3): Q1..Q8 trade the FIRST qualifying candle of the day (no option in the premium band ->
no trade that day); U1/PB trade the first FRESH signal and retry later fresh signals when the band check or U1's
ROC filter blocks one. One trade per strategy per day; strategies are independent (no portfolio risk caps). The
only guard is the paper engine's kill switch (manual on, or an unacknowledged automatic trip): a blocked signal
is recorded and, for Q strategies, ends the day as the first qualifying candle.

P&L (user decision 28 Nov 2026): the MODEL P&L reproduces the backtest (1-minute option bar opens, stop fills at
the stop price, spec costs with slippage and the INR 85 floor). Every trade also carries an EXECUTABLE shadow: the
live ask when the signal was detected and the live bid when the model exit fired (stop: first quote in the exit
minute whose LTP was at/below the stop), costed without the slippage ticks.
"""
from __future__ import annotations

from collections import deque
from dataclasses import asdict
from datetime import datetime

import numpy as np
import pandas as pd

from app.book.exits import CostModel, Position, cost_model_for
from app.book.strategies import STRATEGIES, StrategyDef, chart_signals, flip_close_ts
from app.clock import IST

NS = 1_000_000_000
QUOTE_BUFFER_S = 600            # executable shadow looks back at most this far
OPTION_GRACE_S = 3.0            # an option minute is final this long after it ends (index bars use 2 s)
ENTRY_MAX_DELAY_S = 180         # entry bar must start no more than 3 minutes after the signal candle close


def contract_key(expiry: str, strike: float, option_type: str) -> str:
    return f"NIFTY|{expiry}|{int(strike) if float(strike).is_integer() else strike}|{option_type}"


def _iso(ts_s: float | None) -> str | None:
    return None if ts_s is None else datetime.fromtimestamp(ts_s, IST).isoformat(timespec="seconds")


def _epoch_s(idx: pd.DatetimeIndex) -> np.ndarray:
    """Epoch seconds of a tz-aware index whatever its resolution (pandas may store s, ms or ns)."""
    return idx.tz_convert("UTC").tz_localize(None).to_numpy().astype("datetime64[s]").astype("int64")


def _mod(ts_s: int) -> int:
    d = datetime.fromtimestamp(ts_s, IST)
    return d.hour * 60 + d.minute


class BookEngine:
    def __init__(self, session: str, qty: int, codes: list[str] | tuple = tuple(STRATEGIES),
                 warmup: dict | None = None, cost: CostModel | None = None, kill_fn=None, listener=None,
                 run_id: str = "book_v1"):
        self.session, self.qty, self.run_id = session, int(qty), run_id
        self.codes = [c for c in STRATEGIES if c in set(codes)]
        self.cost = cost                                # None: each strategy's own specification schedule
        self._costs: dict[str, CostModel] = {}
        self.kill_fn = kill_fn or (lambda: None)        # -> reason string when entries are blocked
        self.listener = listener                        # callable(kind, payload) for notifications
        cols = warmup or {}
        n = len(cols.get("ts", []))
        self._ts = [int(x) for x in cols.get("ts", [])]
        self._o = [float(x) for x in cols.get("open", [])]
        self._h = [float(x) for x in cols.get("high", [])]
        self._l = [float(x) for x in cols.get("low", [])]
        self._c = [float(x) for x in cols.get("close", [])]
        self.warmup_bars = n
        self.day_start = int(pd.Timestamp(f"{session} 09:15", tz=IST).timestamp())
        self.tfs = sorted({STRATEGIES[c].tf for c in self.codes})
        self.evaluated: dict[int, int] = {tf: 0 for tf in self.tfs}          # last evaluated candle start per tf
        self.charts: dict[int, tuple] = {}                                    # tf -> (df, {code: st})
        self.state = {c: {"done": False, "pending": None, "pos": None, "first_eval": True} for c in self.codes}
        self.signals: list[dict] = []
        self.trades: list[dict] = []
        self.bars: dict[str, list[dict]] = {}                                 # final option bars today
        self.keys: set[str] = set()                                           # option contracts seen today
        self.watermark = 0                                                    # last final option minute start
        self._build: dict[str, dict] = {}                                     # live option bar builder state
        self._quotes: dict[str, deque] = {}
        self._last_q: dict[str, int] = {}                                     # live: last quote time per contract
        self._processed: dict[str, int] = {}                                  # per open position: last bar ts
        self.index_bars_today = 0
        self.last_index_ts = None
        self.trade_seq = 0
        self.finished = False
        self._full_day: pd.DataFrame | None = None                            # replay only (see precompute_day)
        self._full_charts: dict[int, tuple] = {}

    def cost_for(self, code: str) -> CostModel:
        if self.cost is not None:
            return self.cost
        f = STRATEGIES[code].fees
        if f not in self._costs:
            self._costs[f] = cost_model_for(f, self.session)
        return self._costs[f]

    def precompute_day(self, today_bars: pd.DataFrame) -> None:
        """REPLAY ONLY: hand over the whole day's index bars in advance so each chart is computed once per day.
        Every indicator here is causal (a candle's value depends only on earlier candles) and the warm-up start is
        fixed, so reading the full-day arrays candle by candle equals the live per-candle recomputation; the
        equality is a test (tests/test_book.py). Signals are still taken one closed candle at a time."""
        self._full_day = today_bars
    # ------------------------------------------------------------------ inputs
    def on_index_bar(self, bar: dict, avail_ns: int) -> None:
        ts = int(bar["ts"])
        if self._ts and ts <= self._ts[-1]:
            return                                           # duplicate / out of order (never re-evaluate)
        self._ts.append(ts)
        self._o.append(float(bar["open"]))
        self._h.append(float(bar["high"]))
        self._l.append(float(bar["low"]))
        self._c.append(float(bar["close"]))
        if ts >= self.day_start:
            self.index_bars_today += 1
        self.last_index_ts = ts
        if ts < self.day_start:
            return
        for tf in self.tfs:
            step = tf * 60
            cur = ts - ts % step
            todo = []
            prev_s = self._ts[-2] - self._ts[-2] % step if len(self._ts) > 1 else None
            if prev_s is not None and prev_s < cur and prev_s >= self.day_start and prev_s > self.evaluated[tf]:
                todo.append(prev_s)                          # its last minute never arrived: close it now
            if ts + 60 == cur + step:
                todo.append(cur)
            for s in todo:
                self._evaluate(tf, s, avail_ns)

    def on_quote(self, key: str, t_ns: int, ltp, bid, ask, volume=None, ltt=None) -> None:
        self.keys.add(key)
        self._last_q[key] = t_ns
        dq = self._quotes.setdefault(key, deque())
        dq.append((t_ns, ltp, bid, ask))
        while dq and dq[0][0] < t_ns - QUOTE_BUFFER_S * NS:
            dq.popleft()
        if ltp is None:
            return
        b = self._build.setdefault(key, {"vol": None, "ltp": None, "cur": {}})
        now_min = (t_ns // NS) // 60 * 60
        trade, dv = False, None
        if volume is not None:
            if b["vol"] is None:
                trade = bool(ltt) and int(ltt) // 60 * 60 == now_min and volume > 0
            elif volume > b["vol"]:
                trade, dv = True, int(volume - b["vol"])
            b["vol"] = volume if b["vol"] is None else max(b["vol"], volume)
        else:                                                # journals before 28 Nov 2026 carry no volume
            trade = b["ltp"] is None or ltp != b["ltp"]
        b["ltp"] = ltp
        if not trade:
            return
        m = int(ltt) // 60 * 60 if ltt else now_min
        if m <= self.watermark:
            m = max(now_min, self.watermark + 60)            # trade reported after its minute was final
        bar = b["cur"].get(m)
        if bar is None:
            b["cur"][m] = {"ts": m, "open": ltp, "high": ltp, "low": ltp, "close": ltp,
                           "volume": dv if volume is not None else None}
        else:
            bar["high"], bar["low"], bar["close"] = max(bar["high"], ltp), min(bar["low"], ltp), ltp
            if dv is not None:
                bar["volume"] = (bar["volume"] or 0) + dv

    def on_option_bar(self, key: str, bar: dict) -> None:
        self.keys.add(key)
        self.bars.setdefault(key, []).append(bar)

    def on_time(self, now_ns: int) -> None:
        """Live clock: option minutes that ended more than OPTION_GRACE_S ago become final."""
        last = int((now_ns / NS - OPTION_GRACE_S - 60) // 60 * 60)
        if last <= self.watermark:
            return
        for key, b in self._build.items():
            for m in sorted(x for x in b["cur"] if x <= last):
                self.bars.setdefault(key, []).append(b["cur"].pop(m))
        self.advance_options(last)

    def advance_options(self, minute_ts: int) -> None:
        if minute_ts <= self.watermark:
            return
        self.watermark = minute_ts
        for code in self.codes:
            if self.state[code]["pending"]:
                self._resolve(code)
        for code in self.codes:
            pos = self.state[code]["pos"]
            if pos is not None and pos.open:
                self._run_position(code)

    # ------------------------------------------------------------------ signals
    def _frame(self, upto_excl: int) -> pd.DataFrame:
        n = int(np.searchsorted(np.asarray(self._ts), upto_excl, side="left"))
        idx = pd.to_datetime(np.asarray(self._ts[:n], dtype="int64"), unit="s", utc=True).tz_convert(IST)
        return pd.DataFrame({"open": self._o[:n], "high": self._h[:n], "low": self._l[:n], "close": self._c[:n]},
                            index=idx)

    def _evaluate(self, tf: int, start: int, avail_ns: int) -> None:
        self.evaluated[tf] = start
        close = start + tf * 60
        codes = [c for c in self.codes if STRATEGIES[c].tf == tf]
        live = [c for c in codes if not self.state[c]["done"] or (self.state[c]["pos"] and self.state[c]["pos"].open)]
        if not live:
            return
        if self._full_day is not None:
            df, res = self._full_chart(tf, start)
        else:
            m1 = self._frame(close)
            df, res = chart_signals(m1, tf, live)
        if df.empty or int(df.index[-1].timestamp()) != start:
            return                                            # no index data in this candle
        self.charts[tf] = (df, {c: res[c]["st"] for c in live})
        today = np.asarray(df.index >= pd.Timestamp(self.day_start, unit="s", tz=IST))
        for code in live:
            sd, st = STRATEGIES[code], self.state[code]
            pos = st["pos"]
            if pos is not None and pos.open and pos.flip_ts is None:
                f = flip_close_ts(df, res[code]["st"], sd.side, tf, pd.Timestamp(pos.entry_ts, unit="s", tz=IST))
                if f is not None:
                    pos.flip_ts = int(f.timestamp())
            if st["done"] or st["pending"] or pos is not None:
                continue
            sig, blocked = res[code]["signal"], res[code]["blocked"]
            if st["first_eval"]:
                st["first_eval"] = False
                earlier = np.flatnonzero(sig[:-1] & today[:-1] & (True if blocked is None else ~blocked[:-1]))
                if len(earlier):                              # engine started after a tradeable signal today
                    ts0 = int(df.index[earlier[0]].timestamp())
                    self._signal(sd, ts0 + tf * 60, "MISSED", "ENGINE_STARTED_LATE", {})
                    st["done"] = True
                    continue
            if not sig[-1]:
                continue
            row = {"index_close": float(df.close.iloc[-1])}
            for k, arr in res[code]["detail"].items():
                v = arr[-1]
                row[k] = None if v is None or (isinstance(v, float) and np.isnan(v)) else round(float(v), 4)
            if blocked is not None and blocked[-1]:
                self._signal(sd, close, "NO_TRADE", "ROC_FILTER", row)
                continue
            why = self.kill_fn()
            if why:
                self._signal(sd, close, "NO_TRADE", f"KILL_SWITCH: {why}", row)
                if sd.rule == "first":
                    st["done"] = True
                continue
            atm = int(round(row["index_close"] / 50) * 50)
            st["pending"] = {"close": close, "atm": atm, "t_det": avail_ns, "features": row}
            self._resolve(code)

    def _full_chart(self, tf: int, start: int):
        if tf not in self._full_charts:
            fd = self._full_day
            idx = pd.to_datetime(fd["ts"].astype("int64"), unit="s", utc=True).dt.tz_convert(IST)
            today = pd.DataFrame({c: fd[c].astype(float).to_numpy() for c in ("open", "high", "low", "close")},
                                 index=pd.DatetimeIndex(idx))
            warm = self._frame(self.day_start)
            m1 = pd.concat([warm, today[today.index >= pd.Timestamp(self.day_start, unit="s", tz=IST)]])
            codes = [c for c in self.codes if STRATEGIES[c].tf == tf]
            self._full_charts[tf] = chart_signals(m1, tf, codes)
        df, res = self._full_charts[tf]
        n = int(np.searchsorted(_epoch_s(df.index), start, side="right"))
        cut = {c: {"signal": r["signal"][:n], "blocked": None if r["blocked"] is None else r["blocked"][:n],
                   "st": r["st"][:n], "detail": {k: v[:n] for k, v in r["detail"].items()}} for c, r in res.items()}
        return df.iloc[:n], cut

    def _signal(self, sd: StrategyDef, close_ts: int, outcome: str, reason: str | None, feats: dict,
                **extra) -> dict:
        s = {"seq": len(self.signals) + 1, "ts": _iso(close_ts), "candle_close_ts": close_ts, "code": sd.code,
             "strategy_id": sd.sid, "name": sd.name, "side": sd.side, "direction": sd.direction, "outcome": outcome,
             "reason": reason, "features": feats} | extra
        self.signals.append(s)
        if self.listener:
            self.listener("signal", s)
        return s

    # ------------------------------------------------------------------ entries
    def _expiry(self) -> str | None:
        exps = sorted({k.split("|")[1] for k in self.keys if k.split("|")[1] >= self.session})
        return exps[0] if exps else None

    def _resolve(self, code: str) -> None:
        sd, st = STRATEGIES[code], self.state[code]
        p = st["pending"]
        exp = self._expiry()
        tc = p["close"]
        if self.watermark < tc:
            return                                           # the entry minute's option bars are not final yet
        best, waiting, scanned = None, False, []
        if exp is not None:
            for m in range(sd.scan[0], sd.scan[1] + 1):
                k = p["atm"] + 50 * m if sd.side == "CE" else p["atm"] - 50 * m
                key = contract_key(exp, k, sd.side)
                bars = self.bars.get(key) or []
                f = next((b for b in bars if b["ts"] >= tc), None)
                if f is None or f["ts"] - tc > ENTRY_MAX_DELAY_S:
                    # its first trade after the signal may still come (spec: up to 3 minutes). Live, only a contract
                    # still being quoted since the signal can trade; a replay has no quotes and waits for any known
                    if (key in self.keys and self.watermark < tc + ENTRY_MAX_DELAY_S
                            and (not self._last_q or self._last_q.get(key, 0) >= tc * NS)):
                        waiting = True
                    continue
                px = float(f["open"])
                scanned.append({"strike": k, "open": px, "bar": f["ts"]})
                if best is None or abs(px - sd.target) < abs(best[2] - sd.target):
                    best = (key, f, px, k)
        if waiting:
            return
        st["pending"] = None
        feats = p["features"] | {"atm": p["atm"], "expiry": exp, "scanned": scanned}
        if best is None or not sd.band[0] <= best[2] <= sd.band[1]:
            self._signal(sd, tc, "NO_TRADE", "NO_OPTION_IN_BAND", feats,
                         best_premium=None if best is None else best[2])
            if sd.rule == "first":
                st["done"] = True
            return
        key, f, px, strike = best
        pos = Position(key=key, side=sd.side, profile=sd.exit, entry_ts=int(f["ts"]), entry=px, qty=self.qty,
                       entry_volume=f.get("volume"))
        ch = self.charts.get(sd.tf)
        if ch and code in ch[1]:
            fl = flip_close_ts(ch[0], ch[1][code], sd.side, sd.tf, pd.Timestamp(pos.entry_ts, unit="s", tz=IST))
            pos.flip_ts = None if fl is None else int(fl.timestamp())
        self.trade_seq += 1
        pos_info = {"trade_id": self.trade_seq, "signal_close_ts": tc, "t_det": p["t_det"], "strike": strike,
                    "expiry": exp, "features": feats,
                    "exec_entry": self._exec_price(key, p["t_det"], "ask")}
        st["pos"], st["info"], st["done"] = pos, pos_info, True
        self._processed[code] = int(f["ts"]) - 60
        self._signal(sd, tc, "TRADED", None, feats, contract=key, trade_id=self.trade_seq, entry_px=px,
                     entry_ts=_iso(f["ts"]))
        if self.listener:
            self.listener("entry", {"code": code, "name": sd.name, "contract": key, "entry_px": px,
                                    "entry_ts": _iso(f["ts"]), "trade_id": self.trade_seq,
                                    "exec_ask": pos_info["exec_entry"]["px"]})
        self._run_position(code)

    # ------------------------------------------------------------------ exits
    def _run_position(self, code: str) -> None:
        st = self.state[code]
        pos: Position = st["pos"]
        done_to = self._processed.get(code, 0)
        for b in self.bars.get(pos.key) or []:
            if b["ts"] <= done_to or b["ts"] > self.watermark:
                continue
            self._processed[code] = b["ts"]
            if pos.on_bar(b, _mod(b["ts"])):
                self._close_trade(code)
                return

    def _exec_price(self, key: str, from_ns: int, side: str, until_ns: int | None = None,
                    ltp_at_or_below: float | None = None) -> dict:
        """First live quote at/after from_ns (optionally: within the window and with LTP <= a stop)."""
        for t, ltp, bid, ask in self._quotes.get(key, ()):
            if t < from_ns or (until_ns is not None and t >= until_ns):
                continue
            if ltp_at_or_below is not None and (ltp is None or ltp > ltp_at_or_below):
                continue
            px = ask if side == "ask" else bid
            return {"px": px if px else ltp, "t": t, "note": None if px else f"no {side}: LTP used"}
        if ltp_at_or_below is not None:                       # no qualifying tick seen: first quote of the window
            return self._exec_price(key, from_ns, side, until_ns) | {"note": "stop tick not seen"}
        return {"px": None, "t": None, "note": "no live quote"}

    def _close_trade(self, code: str) -> None:
        sd, st = STRATEGIES[code], self.state[code]
        pos: Position = st["pos"]
        info = st["info"]
        c = self.cost_for(code).round_trip(pos.entry, pos.exit_px, pos.qty, pos.entry_volume, pos.exit_volume)
        gross = (pos.exit_px - pos.entry) * pos.qty
        if pos.exit_reason in ("SL", "BE_SL", "TRAIL_SL"):
            ex = self._exec_price(pos.key, pos.exit_ts * NS, "bid", (pos.exit_ts + 60) * NS, pos.stop_at_exit_bar)
        elif pos.exit_reason == "END":
            q = self._quotes.get(pos.key)
            ex = {"px": q[-1][2] or q[-1][1], "t": q[-1][0], "note": "last quote"} if q else \
                {"px": None, "t": None, "note": "no live quote"}
        else:
            ex = self._exec_price(pos.key, pos.exit_ts * NS, "bid")
        en = info["exec_entry"]
        exec_net = None
        if en["px"] is not None and ex["px"] is not None:
            ec = self.cost_for(code).round_trip(en["px"], ex["px"], pos.qty, slippage=False)
            exec_net = round((ex["px"] - en["px"]) * pos.qty - ec["total"], 2)
        risk = pos.entry * 0.15 * pos.qty
        t = {"trade_id": info["trade_id"], "code": code, "strategy_id": sd.sid, "name": sd.name, "side": sd.side,
             "contract": pos.key, "strike": info["strike"], "expiry": info["expiry"], "qty": pos.qty,
             "exit_profile": sd.exit, "signal_ts": _iso(info["signal_close_ts"]), "entry_ts": _iso(pos.entry_ts),
             "exit_ts": _iso(pos.exit_ts), "entry_px": round(pos.entry, 2), "exit_px": pos.exit_px,
             "exit_reason": pos.exit_reason, "peak_px": round(pos.peak, 2), "gross_pnl": round(gross, 2),
             "charges": c["total"], "net_pnl": round(gross - c["total"], 2), "risk_inr": round(risk, 2),
             "r_multiple": round((gross - c["total"]) / risk, 4) if risk else None,
             "exec_entry_px": en["px"], "exec_exit_px": ex["px"], "exec_net_pnl": exec_net,
             "exec_note": "; ".join(x for x in (en.get("note"), ex.get("note")) if x) or None,
             "cost": c, "fee_version": self.cost_for(code).version, "features": info["features"],
             "trail": pos.trail_history,
             "entry_ns": pos.entry_ts * NS, "exit_ns": pos.exit_ts * NS}
        self.trades.append(t)
        if self.listener:
            self.listener("exit", t)

    def finish(self) -> None:
        """End of day: any position still open (no option bar at/after 15:15) exits at its last bar's open."""
        if self.finished:
            return
        self.finished = True
        for code in self.codes:
            st = self.state[code]
            if st["pending"]:
                st["pending"] = None
                self._signal(STRATEGIES[code], self.session_close(), "NO_TRADE", "SESSION_ENDED_BEFORE_ENTRY", {})
            pos = st["pos"]
            if pos is not None and pos.open:
                bars = [b for b in self.bars.get(pos.key) or [] if b["ts"] >= pos.entry_ts]
                if bars:
                    pos.force_close(bars[-1]["ts"], bars[-1]["open"], "END", bars[-1].get("volume"))
                else:
                    pos.force_close(pos.entry_ts, pos.entry, "END")
                self._close_trade(code)

    def session_close(self) -> int:
        return self.day_start + (15 * 60 + 15 - 555) * 60

    # ------------------------------------------------------------------ views
    def status(self) -> list[dict]:
        out = []
        for code in self.codes:
            sd, st = STRATEGIES[code], self.state[code]
            pos = st["pos"]
            t = next((x for x in self.trades if x["code"] == code), None)
            row = {"code": code, "strategy": sd.name, "chart": f"{sd.tf} min", "side": sd.side,
                   "state": ("CLOSED" if t else "IN TRADE" if pos is not None else "WAITING ENTRY" if st["pending"]
                             else "DONE" if st["done"] else "WATCHING")}
            if pos is not None:
                last = (self.bars.get(pos.key) or [{}])[-1].get("close") if pos.open else pos.exit_px
                row |= {"contract": pos.key, "entry": round(pos.entry, 2), "stop": round(pos.stop, 2),
                        "peak": round(pos.peak, 2), "last": last,
                        "flip_at": _iso(pos.flip_ts), "exit_reason": pos.exit_reason}
                if pos.open and last is not None:
                    row["open_pnl"] = round((last - pos.entry) * pos.qty, 2)
            if t:
                row |= {"net_pnl": t["net_pnl"], "exec_net_pnl": t["exec_net_pnl"]}
            last_sig = next((s for s in reversed(self.signals) if s["code"] == code), None)
            if last_sig:
                row["last_signal"] = f"{last_sig['ts'][11:16]} {last_sig['outcome']}" + (
                    f" ({last_sig['reason']})" if last_sig.get("reason") else "")
            out.append(row)
        return out

    def summary(self) -> dict:
        net = sum(t["net_pnl"] for t in self.trades)
        ex = [t["exec_net_pnl"] for t in self.trades if t["exec_net_pnl"] is not None]
        return {"trades": len(self.trades), "signals": len(self.signals), "net_pnl": round(net, 2),
                "exec_net_pnl": round(sum(ex), 2) if ex else None,
                "open": [c for c in self.codes if self.state[c]["pos"] is not None and self.state[c]["pos"].open],
                "warmup_bars": self.warmup_bars, "index_bars_today": self.index_bars_today}


def warmup_columns(index: pd.DataFrame, day: str, sessions: int = 35) -> dict:
    """Compact journal-safe warm-up: the `sessions` sessions of 1-minute index bars before `day` (column lists)."""
    prior = sorted(index.loc[index["session"] < day, "session"].unique())[-sessions:]
    df = index[index["session"].isin(prior)].sort_values("ts")
    return {"ts": [int(x) for x in df["ts"]], **{c: [round(float(x), 2) for x in df[c]]
                                                  for c in ("open", "high", "low", "close")},
            "sessions": list(prior)}


__all__ = ["BookEngine", "warmup_columns", "contract_key", "asdict"]
