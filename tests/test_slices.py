"""Tests for ``dit.slices``: generators, spec parsing, the partition check."""
from __future__ import annotations

from datetime import date

import pytest

from dit import slices as s

JAN1, JAN31, DEC31 = date(2020, 1, 1), date(2020, 1, 31), date(2020, 12, 31)


def test_days_inclusive_and_empty_when_reversed() -> None:
    assert s.days(date(2020, 1, 30), date(2020, 2, 1)) == (
        date(2020, 1, 30), date(2020, 1, 31), date(2020, 2, 1))
    assert s.days(JAN31, JAN1) == ()


def test_day_spec_gives_the_31_days_of_january_2020() -> None:
    slices = s.plan(s.parse_spec("day"), JAN1, JAN31)
    assert len(slices) == 31
    assert slices[0] == (JAN1, JAN1) and slices[-1] == (JAN31, JAN31)
    s.check_partition(slices, start=JAN1, end=JAN31)


def test_month_spec_gives_the_12_calendar_months_of_2020() -> None:
    slices = s.plan(s.parse_spec("month"), JAN1, DEC31)
    assert len(slices) == 12
    assert slices[1] == (date(2020, 2, 1), date(2020, 2, 29))  # leap year
    assert slices[-1] == (date(2020, 12, 1), DEC31)
    s.check_partition(slices, start=JAN1, end=DEC31)


def test_month_is_cut_at_the_window_ends() -> None:
    assert s.by_month(date(2020, 1, 15), date(2020, 3, 10)) == (
        (date(2020, 1, 15), JAN31),
        (date(2020, 2, 1), date(2020, 2, 29)),
        (date(2020, 3, 1), date(2020, 3, 10)),
    )


def test_week_is_monday_to_sunday_cut_at_the_ends() -> None:
    # 2020-01-01 is a Wednesday; 2020-01-06 a Monday.
    slices = s.by_week(JAN1, date(2020, 1, 15))
    assert slices == (
        (JAN1, date(2020, 1, 5)),
        (date(2020, 1, 6), date(2020, 1, 12)),
        (date(2020, 1, 13), date(2020, 1, 15)),
    )
    assert all(first.weekday() == 0 for first, _ in slices[1:])


def test_week_starting_on_a_monday_has_no_empty_first_slice() -> None:
    assert s.by_week(date(2020, 1, 6), date(2020, 1, 12)) == (
        (date(2020, 1, 6), date(2020, 1, 12)),)


def test_chunks_of_n_days_last_one_shorter() -> None:
    assert s.plan(s.parse_spec("10d"), JAN1, JAN31) == (
        (JAN1, date(2020, 1, 10)),
        (date(2020, 1, 11), date(2020, 1, 20)),
        (date(2020, 1, 21), date(2020, 1, 30)),
        (JAN31, JAN31),
    )


def test_one_day_chunks_equal_by_day() -> None:
    assert s.by_chunks(JAN1, JAN31, 1) == s.by_day(JAN1, JAN31)


def test_chunks_reject_zero() -> None:
    with pytest.raises(ValueError):
        s.by_chunks(JAN1, JAN31, 0)
    with pytest.raises(ValueError):
        s.parse_spec("0d")


def test_explicit_json_list() -> None:
    spec = s.parse_spec('[["2020-01-16", "2020-01-31"], ["2020-01-01", "2020-01-15"]]')
    assert spec.kind == s.KIND_EXPLICIT
    slices = s.plan(spec, JAN1, JAN31)
    assert slices == ((date(2020, 1, 16), JAN31), (JAN1, date(2020, 1, 15)))  # as given
    s.check_partition(slices, start=JAN1, end=JAN31)  # order does not matter


@pytest.mark.parametrize("text", [
    "fortnight", "d", "-3d", "[]", "[1, 2]", '[["2020-01-01"]]',
    '[["2020-01-02", "2020-01-01"]]', '[["2020-13-01", "2020-12-01"]]', "[not json",
])
def test_bad_specs_are_refused(text: str) -> None:
    with pytest.raises(ValueError):
        s.parse_spec(text)


def test_partition_refuses_overlap_gap_and_window_overrun() -> None:
    with pytest.raises(ValueError, match="overlaps"):
        s.check_partition([(JAN1, date(2020, 1, 10)), (date(2020, 1, 10), JAN31)],
                          start=JAN1, end=JAN31)
    with pytest.raises(ValueError, match="in no slice"):
        s.check_partition([(JAN1, date(2020, 1, 10)), (date(2020, 1, 12), JAN31)],
                          start=JAN1, end=JAN31)
    with pytest.raises(ValueError, match="leaves the window"):
        s.check_partition([(JAN1, date(2020, 2, 1))], start=JAN1, end=JAN31)
    with pytest.raises(ValueError, match="end on"):
        s.check_partition([(JAN1, date(2020, 1, 30))], start=JAN1, end=JAN31)
    with pytest.raises(ValueError, match="start on"):
        s.check_partition([(date(2020, 1, 2), JAN31)], start=JAN1, end=JAN31)
    with pytest.raises(ValueError, match="no slices"):
        s.check_partition([], start=JAN1, end=JAN31)


@pytest.mark.parametrize("text", ["day", "week", "month", "7d", "13d", "45d"])
def test_every_generated_plan_partitions_the_window(text: str) -> None:
    for start, end in ((JAN1, DEC31), (date(2020, 2, 28), date(2020, 3, 2)), (JAN1, JAN1)):
        s.check_partition(s.plan(s.parse_spec(text), start, end), start=start, end=end)


def test_describe() -> None:
    assert s.describe(((JAN1, JAN1), (date(2020, 1, 2), JAN31))) == (
        "2020-01-01, 2020-01-02..2020-01-31")
