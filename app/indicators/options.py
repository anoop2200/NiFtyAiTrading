"""Option-family indicators. `chain` stream bars are per-minute aggregates of the nearest-expiry ATM ± N
strikes (see app/indicators/chain.py); `option` stream bars are one contract's 1-minute bars."""
from __future__ import annotations

from collections import deque
from datetime import date, datetime

from app.clock import IST
from app.indicators.base import Indicator, IndicatorSpec, SignalState, SourceRequirement
from app.indicators.registry import register
from app.pricing.bs import ASSUMPTIONS, implied_vol, premium_for_iv, year_fraction

S = SignalState


def _num(v):
    """None for missing / NaN (cached Parquet stores missing as NaN)."""
    return None if v is None or v != v else v


CHAIN_TIERS = ("observed_feed", "observed_top_of_book_snapshot", "reconstructed_from_snapshots")


@register
class PCR(Indicator):
    SPEC = IndicatorSpec("pcr", "Put-call OI ratio (ATM band)", "1.0.0", "options",
                         "pcr = sum PE OI / sum CE OI over ATM +/- N strikes; change over n bars",
                         SourceRequirement("chain", ("ce_oi", "pe_oi"), CHAIN_TIERS),
                         ("pcr_oi", "pcr_change"), "ratio",
                         "rising PCR = put writing / support building (research interpretation)",
                         recovery="replay")
    DEFAULTS = {"period": 15, "rise": 0.05}

    def warmup(self):
        return self.params["period"] + 1

    def init_state(self):
        self.win = deque(maxlen=self.params["period"] + 1)

    def _update(self, bar):
        if bar["ce_oi"] <= 0:
            return {}
        p = bar["pe_oi"] / bar["ce_oi"]
        self.win.append(p)
        if len(self.win) < self.win.maxlen:
            return {"pcr_oi": p}
        ch = p - self.win[0]
        r = self.params["rise"]
        return {"pcr_oi": p, "pcr_change": ch,
                "state": (S.BULLISH if ch >= r else S.BEARISH if ch <= -r else S.NEUTRAL).value}


@register
class AtmIV(Indicator):
    SPEC = IndicatorSpec("atm_iv", "ATM implied volatility (internal BSM)", "1.0.0", "options",
                         "IV inverted from ATM CE and PE premiums (mid when book valid, else LTP) with versioned "
                         "BSM assumptions; iv = mean of valid sides; change over n bars",
                         SourceRequirement("chain", ("spot", "atm_strike", "expiry"), CHAIN_TIERS),
                         ("iv", "iv_change"), "annualised fraction",
                         "change >= +x vol pts = VOLATILITY_EXPANSION (IV expansion); <= -x = IV crush (NEUTRAL, crush=-1)",
                         recovery="replay")
    DEFAULTS = {"period": 15, "move": 0.01}

    def warmup(self):
        return self.params["period"] + 1

    def init_state(self):
        self.win = deque(maxlen=self.params["period"] + 1)

    def _side_iv(self, bar, cp, t):
        px, side = premium_for_iv(*(_num(bar.get(f"atm_{cp.lower()}_{f}")) for f in ("ltp", "bid", "ask")))
        if px is None:
            return None
        return implied_vol(px, bar["spot"], bar["atm_strike"], t, cp)

    def _update(self, bar):
        obs = datetime.fromtimestamp(int(bar["ts"]) + 60, IST)  # bar close time
        exp = bar["expiry"] if isinstance(bar["expiry"], date) else date.fromisoformat(str(bar["expiry"])[:10])
        t = year_fraction(obs, exp)
        ce, pe = self._side_iv(bar, "CE", t), self._side_iv(bar, "PE", t)
        vals = [v for v in (ce, pe) if v is not None]
        if not vals:
            return {}
        iv = sum(vals) / len(vals)
        self.win.append(iv)
        out = {"iv": iv}  # assumptions version is recorded with the run (ASSUMPTIONS.version)
        if len(self.win) == self.win.maxlen:
            ch = iv - self.win[0]
            out["iv_change"] = ch
            out["state"] = (S.VOLATILITY_EXPANSION if ch >= self.params["move"] else S.NEUTRAL).value
        return out


@register
class PremiumPressure(Indicator):
    SPEC = IndicatorSpec("premium_pressure", "CE/PE premium momentum (put-call pressure)", "1.0.0", "options",
                         "pressure = %change(ATM CE premium, n) - %change(ATM PE premium, n) at a fixed strike",
                         SourceRequirement("chain", ("atm_strike", "atm_ce_ltp", "atm_pe_ltp"), CHAIN_TIERS),
                         ("pressure",), "% points", ">= +x BULLISH (calls bid up vs puts), <= -x BEARISH",
                         recovery="replay")
    DEFAULTS = {"period": 5, "threshold": 10.0}

    def warmup(self):
        return self.params["period"] + 1

    def init_state(self):
        self.win = deque(maxlen=self.params["period"] + 1)

    def _update(self, bar):
        self.win.append((bar["atm_strike"], bar["atm_ce_ltp"], bar["atm_pe_ltp"]))
        if len(self.win) < self.win.maxlen:
            return {}
        k0, ce0, pe0 = self.win[0]
        k1, ce1, pe1 = self.win[-1]
        # ATM moved: the comparison would mix two strikes; report no value rather than a misleading one
        if k0 != k1 or ce0 <= 0 or pe0 <= 0:
            return {}
        p = 100 * (ce1 / ce0 - 1) - 100 * (pe1 / pe0 - 1)
        th = self.params["threshold"]
        return {"pressure": p, "state": (S.BULLISH if p >= th else S.BEARISH if p <= -th else S.NEUTRAL).value}


BUILDUP = {1: "LONG_BUILDUP", 2: "SHORT_BUILDUP", 3: "SHORT_COVERING", 4: "LONG_UNWINDING", 0: "NONE"}


@register
class OIBuildup(Indicator):
    SPEC = IndicatorSpec("oi_buildup", "OI buildup / unwinding (single contract)", "1.0.0", "options",
                         "over n bars: price up & OI up = long buildup(1); price down & OI up = short buildup(2); "
                         "price up & OI down = short covering(3); price down & OI down = long unwinding(4)",
                         SourceRequirement("option", ("close", "oi"), CHAIN_TIERS),
                         ("buildup", "price_change_pct", "oi_change_pct"), "code / %",
                         "classification of the contract's own flow; side meaning depends on CE vs PE",
                         recovery="replay")
    DEFAULTS = {"period": 5, "min_oi_pct": 0.5}

    def warmup(self):
        return self.params["period"] + 1

    def init_state(self):
        self.win = deque(maxlen=self.params["period"] + 1)

    def _update(self, bar):
        self.win.append((bar["close"], bar["oi"]))
        if len(self.win) < self.win.maxlen:
            return {}
        (p0, o0), (p1, o1) = self.win[0], self.win[-1]
        if p0 <= 0 or o0 <= 0:
            return {}
        dp, do = 100 * (p1 / p0 - 1), 100 * (o1 / o0 - 1)
        code = 0
        if abs(do) >= self.params["min_oi_pct"] and dp != 0:
            code = (1 if dp > 0 else 2) if do > 0 else (3 if dp > 0 else 4)
        return {"buildup": code, "price_change_pct": dp, "oi_change_pct": do, "state": S.NEUTRAL.value}
