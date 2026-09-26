"""
Shared utility functions used across generator, streaming, and gold modules.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict


def new_uuid() -> str:
    return str(uuid.uuid4())


def utc_now_iso() -> str:
    """Return the current UTC time as an ISO-8601 string with 'Z' suffix."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def to_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def parse_iso(ts: str) -> datetime:
    """Parse an ISO-8601 timestamp, tolerating a trailing 'Z'."""
    cleaned = ts.replace("Z", "+00:00")
    return datetime.fromisoformat(cleaned)


def safe_json_dumps(obj: Dict[str, Any]) -> str:
    return json.dumps(obj, default=str, separators=(",", ":"))


def safe_json_loads(payload: bytes | str) -> Dict[str, Any] | None:
    try:
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        return json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
        return None


def date_key_from_iso(ts: str) -> int | None:
    """Convert an ISO timestamp to a YYYYMMDD integer date key."""
    try:
        dt = parse_iso(ts)
        return int(dt.strftime("%Y%m%d"))
    except (ValueError, TypeError):
        return None


def chunked(iterable, size: int):
    """Yield successive chunks of `size` from `iterable`."""
    buf = []
    for item in iterable:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf
