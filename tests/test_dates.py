"""Tests for ``dit.dates``.

The function ``daterange_inclusive`` is named "inclusive" but, mirroring
:func:`range`, treats the end as **exclusive** (``start <= d < end``). Call
sites that want the end day yielded pass ``end + timedelta(days=1)``. These
tests pin that contract so the four-mode equivalence test stays
byte-equivalent across the move.
"""

from __future__ import annotations

from datetime import date, timedelta

from dit.dates import daterange_inclusive


def test_single_day_yields_start_only_when_end_is_next_day() -> None:
    start = date(2024, 1, 1)
    end = start + timedelta(days=1)
    assert list(daterange_inclusive(start, end)) == [start]


def test_start_equals_end_yields_nothing() -> None:
    d = date(2024, 6, 15)
    assert list(daterange_inclusive(d, d)) == []


def test_end_is_exclusive() -> None:
    start = date(2024, 1, 1)
    end = date(2024, 1, 4)
    assert list(daterange_inclusive(start, end)) == [
        date(2024, 1, 1),
        date(2024, 1, 2),
        date(2024, 1, 3),
    ]


def test_multi_month_range_covers_every_day() -> None:
    start = date(2024, 1, 30)
    end = date(2024, 3, 2)
    days = list(daterange_inclusive(start, end))
    assert days[0] == start
    assert days[-1] == end - timedelta(days=1)
    assert len(days) == (end - start).days
    for i in range(1, len(days)):
        assert days[i] - days[i - 1] == timedelta(days=1)


def test_inclusive_endpoint_via_plus_one_day_convention() -> None:
    start = date(2024, 5, 1)
    end_inclusive = date(2024, 5, 5)
    days = list(daterange_inclusive(start, end_inclusive + timedelta(days=1)))
    assert days[-1] == end_inclusive
    assert days == [start + timedelta(days=i) for i in range(5)]


def test_returns_iterator_not_list() -> None:
    result = daterange_inclusive(date(2024, 1, 1), date(2024, 1, 3))
    assert iter(result) is result
