"""Exact UTC comparisons for Java/Docker timestamps on every supported Python."""

from __future__ import annotations

import re
from datetime import datetime, timezone

_UTC = re.compile(
    r"(?P<seconds>[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})"
    r"(?:\.(?P<fraction>[0-9]{1,9}))?Z"
)


def parse_utc_timestamp(value: object) -> tuple[datetime, int]:
    """Return UTC whole seconds and nanoseconds without float/truncation.

    Python 3.10's ISO parser rejects some fractional lengths; newer versions
    accept them but truncate to microseconds. Parse only whole seconds with
    datetime and retain the exact fractional part for freshness comparisons.
    """
    match = _UTC.fullmatch(value) if type(value) is str else None
    if match is None:
        raise ValueError("Runner timestamp must be exact UTC with at most nine fractional digits")
    try:
        seconds = datetime.fromisoformat(match.group("seconds")).replace(tzinfo=timezone.utc)
    except ValueError:
        raise ValueError("Runner timestamp has an invalid UTC date or time") from None
    nanoseconds = int((match.group("fraction") or "").ljust(9, "0"))
    return seconds, nanoseconds
