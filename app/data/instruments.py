"""Instrument master: download, dated copy, parse, validate (fail closed), persist (v1.3 §3.1, §3.3)."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import httpx
import pandas as pd
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.clock import iso
from app.database.models import InstrumentMasters, Instruments

SECURITY_ID_COLUMNS = ("SEM_SMST_SECURITY_ID", "SECURITY_ID")
REQUIRED = ("SEM_EXM_EXCH_ID", "SEM_INSTRUMENT_NAME", "SEM_TRADING_SYMBOL", "SEM_LOT_UNITS",
            "SEM_EXPIRY_DATE", "SEM_STRIKE_PRICE", "SEM_OPTION_TYPE", "SEM_TICK_SIZE")


class MasterInvalid(RuntimeError):
    pass


@dataclass
class Selection:
    master_date: date
    index: dict
    futures: pd.DataFrame
    options: pd.DataFrame
    total_rows: int
    report: dict = field(default_factory=dict)

    @property
    def selected_rows(self) -> int:
        return 1 + len(self.futures) + len(self.options)


def download_master(url: str, dest_dir: Path, today: date) -> tuple[Path, str]:
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"instrument_master_{today.isoformat()}.csv"
    tmp = dest.with_suffix(".tmp")
    h = hashlib.sha256()
    with httpx.stream("GET", url, timeout=60.0, follow_redirects=True) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_bytes():
                f.write(chunk)
                h.update(chunk)
    tmp.replace(dest)
    return dest, h.hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _norm(df: pd.DataFrame, sec_col: str, segment: str, underlying: str) -> pd.DataFrame:
    out = pd.DataFrame({
        "security_id": df[sec_col].astype("int64"),
        "exchange_segment": segment,
        "instrument_type": df["SEM_INSTRUMENT_NAME"].str.strip(),
        "underlying": underlying,
        "trading_symbol": df["SEM_TRADING_SYMBOL"].str.strip(),
        "expiry": pd.to_datetime(df["SEM_EXPIRY_DATE"], errors="coerce"),
        "strike": pd.to_numeric(df["SEM_STRIKE_PRICE"], errors="coerce"),
        "option_type": df["SEM_OPTION_TYPE"].where(df["SEM_OPTION_TYPE"].isin(["CE", "PE"])),
        "lot_size": pd.to_numeric(df["SEM_LOT_UNITS"], errors="coerce"),
        "tick_size": pd.to_numeric(df["SEM_TICK_SIZE"], errors="coerce"),
        "expiry_flag": df["SEM_EXPIRY_FLAG"] if "SEM_EXPIRY_FLAG" in df else None,
    })
    return out.reset_index(drop=True)


def parse_master(path: Path, master_date: date, underlying: str = "NIFTY", index_symbol: str = "NIFTY",
                 option_prefix: str = "NIFTY-", index_id_override: int | None = None) -> Selection:
    df = pd.read_csv(path, dtype=str, low_memory=False)
    df.columns = [c.strip() for c in df.columns]
    missing = [c for c in REQUIRED if c not in df.columns]
    sec_col = next((c for c in SECURITY_ID_COLUMNS if c in df.columns), None)
    if missing or not sec_col:
        raise MasterInvalid(f"instrument master missing columns: {missing or ['security id']}; "
                            f"got {list(df.columns)[:20]}")
    nse = df[df["SEM_EXM_EXCH_ID"].str.strip() == "NSE"]
    name = nse["SEM_INSTRUMENT_NAME"].str.strip()
    sym = nse["SEM_TRADING_SYMBOL"].str.strip()

    idx_rows = nse[(name == "INDEX") & (sym == index_symbol)]
    if index_id_override:
        index = {"security_id": int(index_id_override), "trading_symbol": index_symbol}
    elif len(idx_rows) == 1:
        index = {"security_id": int(idx_rows.iloc[0][sec_col]), "trading_symbol": index_symbol}
    else:
        raise MasterInvalid(f"expected exactly 1 INDEX row for {index_symbol}, found {len(idx_rows)}; "
                            "set instrument.index_security_id_override")
    index.update(exchange_segment="IDX_I", instrument_type="INDEX", underlying=underlying)

    fut = _norm(nse[(name == "FUTIDX") & sym.str.startswith(option_prefix)], sec_col, "NSE_FNO", underlying)
    opt = _norm(nse[(name == "OPTIDX") & sym.str.startswith(option_prefix)], sec_col, "NSE_FNO", underlying)
    return Selection(master_date, index, fut, opt, total_rows=len(df))


def validate(sel: Selection, today: date, min_options: int = 200) -> dict:
    """Fail-closed checks (v1.3 §3.1). Raises MasterInvalid; returns a report when valid."""
    problems: list[str] = []
    opt, fut = sel.options, sel.futures
    live_opt = opt[opt["expiry"].dt.date >= today]
    live_fut = fut[fut["expiry"].dt.date >= today]
    if len(live_opt) < min_options:
        problems.append(f"only {len(live_opt)} live options (< {min_options})")
    if live_fut.empty:
        problems.append("no live index futures")
    if live_opt["option_type"].isna().any():
        problems.append("options with missing CE/PE type")
    if (live_opt["strike"].isna() | (live_opt["strike"] <= 0)).any():
        problems.append("options with invalid strike")
    if (live_opt["tick_size"].isna() | (live_opt["tick_size"] <= 0)).any():
        problems.append("options with invalid tick size")
    expiries = sorted(live_opt["expiry"].dt.date.unique())
    lots_near = live_opt[live_opt["expiry"].dt.date == expiries[0]]["lot_size"].unique() if expiries else []
    if len(lots_near) != 1:
        problems.append(f"nearest-expiry lot size not unique: {list(lots_near)}")
    if live_opt.duplicated(["expiry", "strike", "option_type"]).any():
        problems.append("duplicate (expiry, strike, type) contracts")
    report = {
        "live_options": int(len(live_opt)), "live_futures": int(len(live_fut)),
        "expiries": [d.isoformat() for d in expiries[:6]],
        "nearest_lot_size": int(lots_near[0]) if len(lots_near) == 1 else None,
        "index_security_id": sel.index["security_id"], "problems": problems,
    }
    sel.report = report
    if problems:
        raise MasterInvalid("; ".join(problems))
    return report


def persist(engine: Engine, sel: Selection, source_url: str, file_path: Path, sha: str) -> int:
    """Store master metadata + selected instruments. Idempotent per master_date."""
    md = sel.master_date.isoformat()
    with Session(engine) as s:
        existing = s.scalar(select(InstrumentMasters).where(InstrumentMasters.master_date == md))
        if existing is not None:
            if existing.sha256 == sha:
                return existing.id
            s.query(Instruments).filter(Instruments.master_id == existing.id).delete()
            s.delete(existing)
            s.flush()
        m = InstrumentMasters(master_date=md, downloaded_at=iso(), source_url=source_url,
                              file_path=str(file_path), sha256=sha, total_rows=sel.total_rows,
                              selected_rows=sel.selected_rows, validation=json.dumps(sel.report))
        s.add(m)
        s.flush()
        rows = [Instruments(master_id=m.id, master_date=md, security_id=sel.index["security_id"],
                            exchange_segment="IDX_I", instrument_type="INDEX",
                            underlying=sel.index["underlying"], trading_symbol=sel.index["trading_symbol"])]
        for frame in (sel.futures, sel.options):
            for r in frame.itertuples(index=False):
                rows.append(Instruments(
                    master_id=m.id, master_date=md, security_id=int(r.security_id),
                    exchange_segment=r.exchange_segment, instrument_type=r.instrument_type,
                    underlying=r.underlying, trading_symbol=r.trading_symbol,
                    expiry=r.expiry.isoformat() if pd.notna(r.expiry) else None,
                    strike=float(r.strike) if pd.notna(r.strike) else None,
                    option_type=r.option_type if isinstance(r.option_type, str) else None,
                    lot_size=int(r.lot_size) if pd.notna(r.lot_size) else None,
                    tick_size=float(r.tick_size) if pd.notna(r.tick_size) else None,
                    expiry_flag=r.expiry_flag if isinstance(r.expiry_flag, str) else None))
        s.add_all(rows)
        s.commit()
        return m.id
