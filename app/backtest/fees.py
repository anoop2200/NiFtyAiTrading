"""Versioned fee schedule (config/fees.yaml). Charges are computed per executed order (one order may
consist of several partial fills; brokerage is charged once per order with at least one fill)."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from app.config import PROJECT_ROOT

FEES_FILE = PROJECT_ROOT / "config" / "fees.yaml"


@dataclass(frozen=True)
class FeeSchedule:
    version: str
    brokerage_per_order: float
    exchange_txn_pct: float
    sebi_fee_per_crore: float
    stt_sell_pct: float
    stamp_duty_buy_pct: float
    ipft_per_crore: float
    gst_pct: float
    rounding: int = 2

    @classmethod
    def load(cls, version: str | None = None, path: Path = FEES_FILE) -> "FeeSchedule":
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        v = version or data["active"]
        s = dict(data["schedules"][v])
        s.pop("effective_from", None)
        return cls(version=v, **s)

    def charges(self, side: str, turnover: float, executed: bool = True) -> dict:
        """Charge breakdown for one order. side BUY/SELL, turnover = sum(fill price * qty)."""
        r = self.rounding
        if not executed or turnover <= 0:
            return {"fee_version": self.version, "brokerage": 0.0, "exchange_txn": 0.0, "sebi": 0.0, "ipft": 0.0,
                    "stt": 0.0, "stamp": 0.0, "gst": 0.0, "total": 0.0}
        brokerage = round(self.brokerage_per_order, r)
        txn = round(turnover * self.exchange_txn_pct / 100, r)
        sebi = round(turnover * self.sebi_fee_per_crore / 1e7, r)
        ipft = round(turnover * self.ipft_per_crore / 1e7, r)
        stt = round(turnover * self.stt_sell_pct / 100, r) if side == "SELL" else 0.0
        stamp = round(turnover * self.stamp_duty_buy_pct / 100, r) if side == "BUY" else 0.0
        gst = round((brokerage + txn + sebi + ipft) * self.gst_pct / 100, r)
        total = round(brokerage + txn + sebi + ipft + stt + stamp + gst, r)
        return {"fee_version": self.version, "brokerage": brokerage, "exchange_txn": txn, "sebi": sebi, "ipft": ipft,
                "stt": stt, "stamp": stamp, "gst": gst, "total": total}
