"""Date helpers shared across workflows."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, timedelta


def daterange_inclusive(start: date, end: date) -> Iterator[date]:
    """Yield each calendar day d with ``start <= d < end``.

    Despite the name, the end date is *exclusive* (mirrors :func:`range`).
    Lifted verbatim from ``pipe-gaps`` ``mode_equivalence._daterange_inclusive``.
    Call sites pass ``end + timedelta(days=1)`` when they want the end day
    included; preserving that convention is what keeps the four-mode equivalence
    test byte-equivalent across the move.
    """
    cur = start
    while cur < end:
        yield cur
        cur += timedelta(days=1)
