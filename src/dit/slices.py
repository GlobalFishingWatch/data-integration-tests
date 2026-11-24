"""Date slices over an inclusive window: generators, a spec string, checks.

A slice is the date range of one job, ``(first, last)``, both days included.
Every function here is pure and returns tuples, so a plan can be computed,
logged and compared without running anything.

Generators, each over the inclusive window ``[start, end]``:

* :func:`by_day`: one slice per day.
* :func:`by_week`: calendar weeks, Monday to Sunday, cut at the window's ends
  (so the first and last week may be shorter).
* :func:`by_month`: calendar months, cut at the window's ends.
* :func:`by_chunks`: consecutive chunks of ``n`` days from ``start``; the last
  one may be shorter.
* :func:`explicit`: a given list of slices, validated.

A spec string selects one of them (:func:`parse_spec`, :func:`plan`), so a
command line can name a whole plan in one short value:

* ``day``, ``week``, ``month``;
* ``<N>d`` (e.g. ``7d``, ``10d``): chunks of N days;
* a JSON list of ``[first, last]`` pairs of ISO dates, both included, e.g.
  ``[["2020-01-01", "2020-01-15"], ["2020-01-16", "2020-01-31"]]``.

``month`` over 2020 is the 12 calendar months; ``day`` over January 2020 is its
31 days.

Two checks:

* :func:`check_partition`: the slices cover the window exactly once, in any
  order. What slices loaded in parallel must satisfy to be comparable with
  one job over the window.
* The segmenter's run-order check (``check_chain`` in
  ``workflows/pipe_segment/ais.py``) is about jobs that run one after the
  other and stays there.
"""
from __future__ import annotations

import dataclasses
import json
import re
from collections.abc import Sequence
from datetime import date, timedelta

Slice = tuple[date, date]  # first and last day of one job, both included

KIND_DAY = "day"
KIND_WEEK = "week"
KIND_MONTH = "month"
KIND_CHUNKS = "chunks"
KIND_EXPLICIT = "explicit"

#: The named spec words, in the order the help lists them.
NAMED_KINDS = (KIND_DAY, KIND_WEEK, KIND_MONTH)

_CHUNKS_RE = re.compile(r"^([1-9]\d*)d$")

#: What the ``--slices`` help of a workflow can say about the spec syntax.
SPEC_HELP = (
    "day | week (calendar weeks, Monday to Sunday) | month (calendar months) | "
    "<N>d (chunks of N days from the start, e.g. 7d) | a JSON list of [first, last] "
    "ISO date pairs, both days included. Weeks, months and the last chunk are cut at "
    "the window's ends."
)


def days(first: date, last: date) -> tuple[date, ...]:
    """Every day from ``first`` to ``last``, both included; empty if
    ``last < first``."""
    return tuple(first + timedelta(days=offset) for offset in range((last - first).days + 1))


def by_day(start: date, end: date) -> tuple[Slice, ...]:
    """One slice per day of ``[start, end]``; empty if ``end < start``."""
    return tuple((day, day) for day in days(start, end))


def _cut(starts: Sequence[date], start: date, end: date) -> tuple[Slice, ...]:
    """Slices beginning at each of ``starts`` (sorted, the first being
    ``start``), each running to the day before the next, the last to ``end``."""
    ends = (*(nxt - timedelta(days=1) for nxt in starts[1:]), end)
    return tuple(zip(starts, ends))


def by_week(start: date, end: date) -> tuple[Slice, ...]:
    """Calendar weeks (Monday to Sunday) of ``[start, end]``, cut at both ends."""
    if end < start:
        return ()
    mondays = tuple(day for day in days(start, end) if day.weekday() == 0 and day != start)
    return _cut((start, *mondays), start, end)


def by_month(start: date, end: date) -> tuple[Slice, ...]:
    """Calendar months of ``[start, end]``, cut at both ends."""
    if end < start:
        return ()
    firsts = tuple(day for day in days(start, end) if day.day == 1 and day != start)
    return _cut((start, *firsts), start, end)


def by_chunks(start: date, end: date, n: int) -> tuple[Slice, ...]:
    """Consecutive chunks of ``n`` days from ``start``; the last one is cut at
    ``end``. ``ValueError`` unless ``n >= 1``."""
    if n < 1:
        raise ValueError(f"a chunk needs at least one day; got {n}")
    if end < start:
        return ()
    starts = days(start, end)[::n]
    return _cut(starts, start, end)


def explicit(pairs: Sequence[Sequence[date]]) -> tuple[Slice, ...]:
    """The given ``[first, last]`` pairs as slices. ``ValueError`` if a pair
    does not have two dates or ends before it starts."""
    def one(index: int, pair: Sequence[date]) -> Slice:
        if len(pair) != 2:
            raise ValueError(f"slice {index} needs [first, last]; got {list(pair)!r}")
        first, last = pair
        if last < first:
            raise ValueError(f"slice {index} ends ({last}) before it starts ({first})")
        return (first, last)

    return tuple(one(index, pair) for index, pair in enumerate(pairs, start=1))


@dataclasses.dataclass(frozen=True)
class SliceSpec:
    """A parsed spec: which generator, and its argument."""

    kind: str
    text: str
    chunk_days: int | None = None
    pairs: tuple[Slice, ...] = ()


def _parse_pairs(text: str) -> tuple[Slice, ...]:
    try:
        raw = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"not a JSON list of [first, last] pairs: {exc}") from None
    if not isinstance(raw, list) or not raw:
        raise ValueError("a JSON slice list must be a non-empty list of [first, last] pairs")

    def to_date(value: object, index: int) -> date:
        if not isinstance(value, str):
            raise ValueError(f"slice {index}: dates must be ISO strings; got {value!r}")
        try:
            return date.fromisoformat(value)
        except ValueError:
            raise ValueError(f"slice {index}: {value!r} is not an ISO date") from None

    def pair(index: int, item: object) -> list[date]:
        if not isinstance(item, list):
            raise ValueError(f"slice {index} needs [first, last]; got {item!r}")
        return [to_date(value, index) for value in item]

    return explicit([pair(index, item) for index, item in enumerate(raw, start=1)])


def parse_spec(text: str) -> SliceSpec:
    """Parse a spec string (see the module docstring). ``ValueError`` naming
    the accepted forms when it is none of them."""
    stripped = text.strip()
    if stripped in NAMED_KINDS:
        return SliceSpec(kind=stripped, text=stripped)
    chunks = _CHUNKS_RE.match(stripped)
    if chunks:
        return SliceSpec(kind=KIND_CHUNKS, text=stripped, chunk_days=int(chunks.group(1)))
    if stripped.startswith("["):
        return SliceSpec(kind=KIND_EXPLICIT, text=stripped, pairs=_parse_pairs(stripped))
    raise ValueError(f"unknown slice spec {text!r}; expected {SPEC_HELP}")


def plan(spec: SliceSpec, start: date, end: date) -> tuple[Slice, ...]:
    """The slices ``spec`` gives over ``[start, end]``. An explicit list is
    returned as given; check it with :func:`check_partition`."""
    if spec.kind == KIND_DAY:
        return by_day(start, end)
    if spec.kind == KIND_WEEK:
        return by_week(start, end)
    if spec.kind == KIND_MONTH:
        return by_month(start, end)
    if spec.kind == KIND_CHUNKS:
        assert spec.chunk_days is not None
        return by_chunks(start, end, spec.chunk_days)
    if spec.kind == KIND_EXPLICIT:
        return spec.pairs
    raise ValueError(f"unknown slice kind {spec.kind!r}")


def check_partition(slices: Sequence[Slice], *, start: date, end: date) -> None:
    """Raise ``ValueError`` unless ``slices`` cover ``[start, end]`` exactly
    once: no day outside the window, no day twice, no day missing. Order does
    not matter (slices loaded in parallel have none)."""
    if not slices:
        raise ValueError("no slices: the window has none, or the list is empty")
    ordered = sorted(slices)
    for first, last in ordered:
        if last < first:
            raise ValueError(f"slice [{first}, {last}] ends before it starts")
        if first < start or last > end:
            raise ValueError(f"slice [{first}, {last}] leaves the window [{start}, {end}]")
    if ordered[0][0] != start:
        raise ValueError(f"the slices start on {ordered[0][0]}, not on the window's first day {start}")
    for (_, last), (nxt, nxt_last) in zip(ordered, ordered[1:]):
        if nxt <= last:
            raise ValueError(f"slice [{nxt}, {nxt_last}] overlaps the slice ending {last}")
        if nxt - timedelta(days=1) != last:
            raise ValueError(
                f"the days from {last + timedelta(days=1)} to {nxt - timedelta(days=1)} "
                f"are in no slice"
            )
    if ordered[-1][1] != end:
        raise ValueError(f"the slices end on {ordered[-1][1]}, not on the window's last day {end}")


def describe(slices: Sequence[Slice]) -> str:
    """``first..last`` per slice, comma-separated (a one-day slice is its day)."""
    return ", ".join(
        first.isoformat() if first == last else f"{first.isoformat()}..{last.isoformat()}"
        for first, last in slices
    )
