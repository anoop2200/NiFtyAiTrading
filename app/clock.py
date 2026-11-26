"""Time helpers. All persisted timestamps: ISO 8601 with Asia/Kolkata offset, plus epoch ns where useful."""
from __future__ import annotations

import socket
import struct
import time
from datetime import date, datetime, time as dtime
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
_NTP_DELTA = 2208988800  # seconds between 1900 and 1970


def now_ist() -> datetime:
    return datetime.now(IST)


def now_ns() -> int:
    return time.time_ns()


def iso(dt: datetime | None = None) -> str:
    return (dt or now_ist()).astimezone(IST).isoformat(timespec="milliseconds")


def ns_to_ist(ns: int) -> datetime:
    return datetime.fromtimestamp(ns / 1e9, IST)


def ist_date_of_ns(ns: int) -> date:
    return ns_to_ist(ns).date()


def parse_hhmm(s: str) -> dtime:
    h, m = s.split(":")
    return dtime(int(h), int(m))


def sntp_offset_ms(server: str = "time.google.com", timeout: float = 2.0) -> float:
    """Return local clock offset vs an NTP server in milliseconds (positive = local clock behind).

    Minimal SNTP client; sends no user data.
    """
    packet = b"\x1b" + 47 * b"\0"
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.settimeout(timeout)
        t0 = time.time()
        s.sendto(packet, (server, 123))
        data, _ = s.recvfrom(48)
        t3 = time.time()
    secs, frac = struct.unpack("!II", data[40:48])  # transmit timestamp
    server_time = secs - _NTP_DELTA + frac / 2**32
    return ((server_time - t0) + (server_time - t3)) / 2 * 1000.0
