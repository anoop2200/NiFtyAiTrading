import struct
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.database.db import init_db, make_engine  # noqa: E402


@pytest.fixture
def engine(tmp_path):
    e = make_engine(tmp_path / "test.db")
    init_db(e)
    return e


def header(code: int, seg: int, sid: int, length: int) -> bytes:
    return struct.pack("<BhBi", code, length, seg, sid)


def full_packet(sid: int, ltp: float = 100.0, bid: float = 99.9, ask: float = 100.1, ltt: int = 1_700_000_000,
                seg: int = 2) -> bytes:
    body = struct.pack("<fhifiiiiiiffff", ltp, 75, ltt, 99.5, 12345, 500, 600, 1_000_000, 1_100_000, 900_000,
                       95.0, 0.0, 105.0, 94.0)
    depth = b"".join(struct.pack("<iihhff", 75 * (i + 1), 150 * (i + 1), i + 1, i + 2, bid - 0.05 * i,
                                 ask + 0.05 * i) for i in range(5))
    return header(8, seg, sid, 162) + body + depth


def ticker_packet(sid: int, ltp: float, ltt: int = 1_700_000_000, seg: int = 0) -> bytes:
    return header(2, seg, sid, 16) + struct.pack("<fi", ltp, ltt)


def disconnect_packet(code: int) -> bytes:
    return header(50, 0, 0, 10) + struct.pack("<h", code)
