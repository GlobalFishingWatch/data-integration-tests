"""Read production's rendered DAGs from composer-dags-production, as data (PIPELINE-4700).

DIT hand-copies what production runs into its workflows: the argv builders in
``workflows/port_visits/ais.py``, the ``ProductionTarget`` in
``cross_version_ais.py``, the image pin of every workflow. Those copies drift
silently when a pipeline changes its CLI or production changes a parameter;
the pipe-anchorages v5 CLI drift went unnoticed for months.

composer-dags-production already keeps a machine-readable record of what its
DAGs run: ``tests/render_dags/snapshots/*.py``. Each file holds one literal
``rendered_dag = {...}`` assignment: one DAG rendered for one fixed run (dag
id, execution date, data interval), with each task's image, command, full
argv and neighbours. composer's CI renders the DAG and checks it against the
snapshot, so a snapshot on composer's main describes main.

**Data, not code.** dit never imports ``gfw.*`` or airflow (working
agreement). This module reads a snapshot with ``git show <commit>:<path>``
(no checkout, so composer's working tree is never touched) and parses it with
``ast.parse`` and ``ast.literal_eval`` on the assignment's value. The file is
never executed; a value that is not a pure literal is rejected.

**What a snapshot cannot say** (details in ``docs/composer-snapshots-2026-10.md``):

* It is ONE run. Dates are concrete and Jinja conditionals are resolved to the
  branch that run takes. :func:`templatize` maps dates back to placeholders
  using the run's data interval, and reports, instead of guessing, a date that
  fits more than one convention (in a one-day interval the first and the last
  day are the same date).
* composer's CI compares lists order-insensitively and checks only the keys a
  snapshot contains, so argv ORDER is not verified. :func:`parse_argv` pairs
  ``--flag value`` by order and is only as good as the generator that wrote
  the file (every snapshot says it is generated, not hand-edited).

Stdlib only and import-light, like :mod:`dit.production`: git through
``subprocess``. The caller fetches composer; nothing here touches the network.
"""
from __future__ import annotations

import ast
import dataclasses
import os
import re
import subprocess
from collections.abc import Collection, Iterator, Mapping, Sequence
from datetime import date, datetime, timedelta
from itertools import chain
from pathlib import Path
from typing import Any

#: Where composer keeps its render snapshots, relative to the repo root.
SNAPSHOT_DIR = "tests/render_dags/snapshots"

#: Default ref to read: what production deploys from, once fetched.
DEFAULT_REF = "origin/main"

#: Env var naming the composer-dags-production checkout to read.
COMPOSER_DIR_ENV = "DIT_COMPOSER_DAGS_DIR"

#: The module-level name every snapshot assigns.
_ASSIGNMENT = "rendered_dag"

#: Timestamp format of the snapshot's dates (composer's own ``_parse_datetime``).
_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


# --------------------------------------------------------------------------
# Data model
# --------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class DataInterval:
    """The half-open Airflow data interval ``[start, end)`` a snapshot was rendered for."""

    start: datetime
    end: datetime


@dataclasses.dataclass(frozen=True)
class RenderedTask:
    """One task of a rendered DAG.

    ``image`` / ``version`` / ``cmds`` / ``arguments`` describe a container
    task; for any other task (sensors, HTTP calls) the image and version are
    ``None`` and the command is empty. ``version`` is the image tag, which is
    how production pins a pipeline version.

    ``arguments`` holds the string argv tokens. A few snapshots (pipe-publications)
    write some arguments as dicts that composer's CI checks only partially;
    those are not argv tokens and are kept apart, in ``structured_arguments``.
    """

    dag_id: str
    task_id: str
    image: str | None
    version: str | None
    cmds: tuple[str, ...]
    arguments: tuple[str, ...]
    data_interval: DataInterval
    execution_date: datetime
    upstream_task_ids: tuple[str, ...] = ()
    downstream_task_ids: tuple[str, ...] = ()
    structured_arguments: tuple[Mapping[str, Any], ...] = ()

    @property
    def argv(self) -> tuple[str, ...]:
        """The container's full command line: ``cmds`` then ``arguments``."""
        return self.cmds + self.arguments

    @property
    def image_name(self) -> str | None:
        """The image's last path component without its tag, e.g. ``pipe-anchorages``."""
        return None if self.image is None else image_name(self.image)


@dataclasses.dataclass(frozen=True)
class RenderedDag:
    """One snapshot: a DAG rendered for one run, read at one composer commit."""

    dag_id: str
    snapshot: str
    commit: str | None
    execution_date: datetime
    data_interval: DataInterval
    tasks: tuple[RenderedTask, ...]

    def task(self, task_id: str) -> RenderedTask:
        """The task ``task_id``; ``KeyError`` naming the snapshot when absent."""
        found = next((t for t in self.tasks if t.task_id == task_id), None)
        if found is None:
            raise KeyError(f"{self.snapshot} ({self.dag_id}) has no task {task_id!r}")
        return found


# --------------------------------------------------------------------------
# Reading snapshots (git, read-only)
# --------------------------------------------------------------------------

def default_composer_dir() -> Path:
    """The composer-dags-production checkout to read.

    ``$DIT_COMPOSER_DAGS_DIR`` if set, else ``$PROJECTS/composer-dags-production``
    (the Makefile's sibling-checkout convention), else a sibling of this repo.
    """
    explicit = os.environ.get(COMPOSER_DIR_ENV)
    if explicit:
        return Path(explicit)
    projects = os.environ.get("PROJECTS")
    if projects:
        return Path(projects) / "composer-dags-production"
    return Path(__file__).resolve().parents[3] / "composer-dags-production"


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True, check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"git {' '.join(args)} failed in {repo} (rc={result.returncode}): "
            f"{result.stderr.strip()}"
        )
    return result.stdout


def resolve_ref(repo: Path, ref: str = DEFAULT_REF) -> str:
    """The commit ``ref`` names in ``repo``. Reads go through the commit, so
    one load is consistent even if ``ref`` moves (a fetch) meanwhile."""
    return _git(repo, "rev-parse", "--verify", f"{ref}^{{commit}}").strip()


def list_snapshots(repo: Path, ref: str = DEFAULT_REF) -> tuple[str, ...]:
    """Names (file stems) of the snapshots at ``ref``, sorted."""
    listing = _git(repo, "ls-tree", "--name-only", ref, f"{SNAPSHOT_DIR}/")
    return tuple(sorted(
        Path(path).stem for path in listing.split() if path.endswith(".py")
    ))


def read_snapshot_source(repo: Path, name: str, ref: str = DEFAULT_REF) -> str:
    """The source of snapshot ``name`` at ``ref``, via ``git show`` (no checkout)."""
    return _git(repo, "show", f"{ref}:{SNAPSHOT_DIR}/{name}.py")


def load_dag(repo: Path, name: str, ref: str = DEFAULT_REF) -> RenderedDag:
    """Snapshot ``name`` at ``ref``, parsed. Records the resolved commit."""
    commit = resolve_ref(repo, ref)
    return dag_from_snapshot(
        parse_rendered_dag(read_snapshot_source(repo, name, commit)),
        snapshot=name, commit=commit,
    )


# --------------------------------------------------------------------------
# Parsing (pure)
# --------------------------------------------------------------------------

def parse_rendered_dag(source: str) -> dict[str, Any]:
    """The literal value of the module-level ``rendered_dag = ...`` assignment.

    ``ast.literal_eval`` accepts only literals (dicts, lists, strings, numbers,
    booleans, ``None``), so nothing in the file runs. ``ValueError`` when the
    assignment is missing, repeated, not a dict, or not a pure literal.
    """
    values = [
        node.value
        for node in ast.parse(source).body
        if isinstance(node, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == _ASSIGNMENT for t in node.targets)
    ]
    if len(values) != 1:
        raise ValueError(f"expected one `{_ASSIGNMENT} = ...` assignment, found {len(values)}")
    try:
        value = ast.literal_eval(values[0])
    except ValueError as exc:
        raise ValueError(f"`{_ASSIGNMENT}` is not a pure literal: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"`{_ASSIGNMENT}` is a {type(value).__name__}, not a dict")
    return value


def _parse_time(value: str) -> datetime:
    return datetime.strptime(value, _TIME_FORMAT)


def image_version(image: str) -> str | None:
    """The tag of an image reference, ignoring any ``@sha256:...`` digest.

    ``None`` for an untagged reference. A ``:`` before the last ``/`` is a
    registry port, not a tag.
    """
    ref = image.split("@", 1)[0]
    last = ref.rsplit("/", 1)[-1]
    return last.split(":", 1)[1] if ":" in last else None


def image_name(image: str) -> str:
    """The last path component of an image reference, without tag or digest."""
    return image.split("@", 1)[0].rsplit("/", 1)[-1].split(":", 1)[0]


def _split_arguments(
    task_id: str, arguments: Sequence[Any],
) -> tuple[tuple[str, ...], tuple[Mapping[str, Any], ...]]:
    unexpected = [a for a in arguments if not isinstance(a, (str, Mapping))]
    if unexpected:
        raise ValueError(f"task {task_id}: unexpected argument types {unexpected!r}")
    return (
        tuple(a for a in arguments if isinstance(a, str)),
        tuple(a for a in arguments if isinstance(a, Mapping)),
    )


def _task_from_snapshot(
    dag_id: str, task_id: str, raw: Mapping[str, Any],
    interval: DataInterval, execution_date: datetime,
) -> RenderedTask:
    image = raw.get("image")
    arguments, structured = _split_arguments(task_id, raw.get("arguments") or ())
    return RenderedTask(
        dag_id=dag_id,
        task_id=task_id,
        image=image,
        version=None if image is None else image_version(image),
        cmds=tuple(raw.get("cmds") or ()),
        arguments=arguments,
        data_interval=interval,
        execution_date=execution_date,
        upstream_task_ids=tuple(raw.get("upstream_task_ids") or ()),
        downstream_task_ids=tuple(raw.get("downstream_task_ids") or ()),
        structured_arguments=structured,
    )


def dag_from_snapshot(
    raw: Mapping[str, Any], *, snapshot: str, commit: str | None = None,
) -> RenderedDag:
    """Build a :class:`RenderedDag` from a parsed ``rendered_dag`` dict."""
    interval = DataInterval(
        start=_parse_time(raw["data_interval_start"]),
        end=_parse_time(raw["data_interval_end"]),
    )
    execution_date = _parse_time(raw["execution_date"])
    return RenderedDag(
        dag_id=raw["dag_id"],
        snapshot=snapshot,
        commit=commit,
        execution_date=execution_date,
        data_interval=interval,
        tasks=tuple(
            _task_from_snapshot(raw["dag_id"], task_id, task, interval, execution_date)
            for task_id, task in raw["tasks"].items()
        ),
    )


def pinned_versions(dag: RenderedDag) -> dict[str, tuple[str, ...]]:
    """Image name -> the distinct tags the DAG's container tasks run, sorted.

    This is how a snapshot shows which pipeline version production pins: one
    DAG can run two versions of the same image (e.g. during a migration).
    Untagged images map to ``()``.
    """
    names = sorted({t.image_name for t in dag.tasks if t.image_name})
    return {
        name: tuple(sorted({
            t.version for t in dag.tasks if t.image_name == name and t.version
        }))
        for name in names
    }


# --------------------------------------------------------------------------
# argv -> flags (pure)
# --------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class Flag:
    """One option of an argv: its name as written (leading dashes kept, since
    ``-start`` and ``--start`` are different flags) and its value, ``None``
    for a bare flag such as ``--wait_for_job``."""

    name: str
    value: str | None


@dataclasses.dataclass(frozen=True)
class ParsedArgv:
    """An argv split into positionals (entrypoint, subcommands) and flags, in order."""

    positionals: tuple[str, ...]
    flags: tuple[Flag, ...]

    def names(self) -> tuple[str, ...]:
        """Flag names in first-seen order, without repeats."""
        return tuple(dict.fromkeys(f.name for f in self.flags))

    def values(self, name: str) -> tuple[str | None, ...]:
        """Every value given for ``name`` (repeated flags such as ``--labels``)."""
        return tuple(f.value for f in self.flags if f.name == name)

    def value(self, name: str) -> str | None:
        """The value of a flag given exactly once; ``KeyError`` when absent,
        ``ValueError`` when repeated."""
        found = self.values(name)
        if not found:
            raise KeyError(name)
        if len(found) > 1:
            raise ValueError(f"{name} is given {len(found)} times")
        return found[0]


_NEGATIVE_NUMBER = re.compile(r"-\d")


def _is_flag(token: str) -> bool:
    return token.startswith("-") and len(token) > 1 and not _NEGATIVE_NUMBER.match(token)


def _argv_items(tokens: Sequence[str], bare: Collection[str]) -> Iterator[str | Flag]:
    """Yield positionals (``str``) and :class:`Flag` items in argv order."""
    i = 0
    while i < len(tokens):
        token = tokens[i]
        if token == "--":
            yield from tokens[i + 1:]
            return
        if not _is_flag(token):
            yield token
            i += 1
        elif "=" in token:
            name, value = token.split("=", 1)
            yield Flag(name, value)
            i += 1
        elif token not in bare and i + 1 < len(tokens) and not _is_flag(tokens[i + 1]):
            yield Flag(token, tokens[i + 1])
            i += 2
        else:
            yield Flag(token, None)
            i += 1


def parse_argv(tokens: Sequence[str], *, bare: Collection[str] = ()) -> ParsedArgv:
    """Split an argv into positionals and flags.

    Rules, in order:

    * ``--name=value`` (or ``-name=value``) is a flag with a value; the value
      is everything after the first ``=``, so ``--labels=k=v`` has value ``k=v``.
    * ``--name value``: a flag followed by a token that is not itself a flag
      takes it as its value, unless ``name`` is in ``bare``.
    * Otherwise the flag is bare (value ``None``).
    * A token after ``--``, and any token that is not a flag, is positional.
      A token starting with ``-`` followed by a digit is a negative number,
      not a flag.

    Without the pipeline's parser this cannot tell a bare flag followed by a
    positional (``-v subcommand``) from ``--name value``; pass the bare flags
    in ``bare`` when the argv has that shape.
    """
    items = tuple(_argv_items(tokens, frozenset(bare)))
    return ParsedArgv(
        positionals=tuple(i for i in items if isinstance(i, str)),
        flags=tuple(i for i in items if isinstance(i, Flag)),
    )


@dataclasses.dataclass(frozen=True)
class FlagDiff:
    """Flag names compared between two argvs. Values of a repeated flag are
    compared as a multiset, so the order of ``--labels`` does not matter."""

    only_left: tuple[str, ...]
    only_right: tuple[str, ...]
    differing: tuple[str, ...]
    same: tuple[str, ...]

    @property
    def identical(self) -> bool:
        return not (self.only_left or self.only_right or self.differing)


def _value_multiset(parsed: ParsedArgv, name: str) -> tuple[tuple[bool, str], ...]:
    return tuple(sorted((v is None, v or "") for v in parsed.values(name)))


def diff_flags(
    left: ParsedArgv, right: ParsedArgv, *, ignore: Collection[str] = (),
) -> FlagDiff:
    """Compare the flags of two argvs by name and value; names in ``ignore`` are left out."""
    names = tuple(n for n in dict.fromkeys(chain(left.names(), right.names())) if n not in ignore)
    in_left = set(left.names())
    in_right = set(right.names())
    both = tuple(n for n in names if n in in_left and n in in_right)
    return FlagDiff(
        only_left=tuple(n for n in names if n not in in_right),
        only_right=tuple(n for n in names if n not in in_left),
        differing=tuple(
            n for n in both if _value_multiset(left, n) != _value_multiset(right, n)
        ),
        same=tuple(n for n in both if _value_multiset(left, n) == _value_multiset(right, n)),
    )


# --------------------------------------------------------------------------
# Concrete dates -> placeholders (pure)
# --------------------------------------------------------------------------
#
# A snapshot's dates are those of the one run it renders. To reuse its argv
# for another run, each date must be traced back to the run's interval. The
# conventions detected, all relative to the half-open data interval
# [start, end) (``*_nodash`` is the same date written YYYYMMDD):
#
#   {start_date}          start's date (Airflow's data_interval_start | ds)
#   {end_date}            the last day inside the interval, i.e. end minus one
#                         day for a midnight-aligned end (inclusive end dates,
#                         e.g. pipe-anchorages v4 --end_date)
#   {end_date_exclusive}  end's date (exclusive end dates, e.g. pipe-events v8
#                         --end-date, pipe-measures --date_range)
#   {execution_date}      the logical date, only where it differs from start's
#                         date. Where they are equal (Airflow 2's standard
#                         timetables) the snapshot cannot tell ``ds`` from
#                         ``data_interval_start``, and both are {start_date}.
#
# An empty interval (start == end; about a fifth of the snapshots, backfills
# whose dates come from params) has no end placeholders: there is no last day.
#
# Dates are recognised as YYYY-MM-DD anywhere, and as YYYYMMDD when not
# preceded by a letter or digit. So `--20260815` (job names) and `_20260815`
# (date shards) are dates, but `_v20240117` is a version (e.g. a
# named-anchorages table) and is never replaced. Times of day are ignored:
# only the date part of an ISO timestamp is replaced.
#
# A date that matches exactly one placeholder is replaced. One that matches
# several (in a one-day interval {start_date} and {end_date} are the same day)
# is left as written and reported as ambiguous. One that matches none is a
# constant (e.g. a history start of 2012-01-01) and is reported as such.

#: Placeholder names, in the order they are reported.
PLACEHOLDERS = ("start_date", "end_date", "end_date_exclusive", "execution_date")

_ISO_DATE = re.compile(r"(?<!\d)\d{4}-\d{2}-\d{2}(?!\d)")
_COMPACT_DATE = re.compile(r"(?<![0-9A-Za-z])\d{8}(?!\d)")
_PLACEHOLDER = re.compile(r"\{(" + "|".join(PLACEHOLDERS) + r")(_nodash)?\}")


def date_placeholders(interval: DataInterval, execution_date: datetime) -> dict[str, date]:
    """The placeholder dates of one run; see the conventions above."""
    start = interval.start.date()
    ends = (
        {
            "end_date": (interval.end - timedelta(microseconds=1)).date(),
            "end_date_exclusive": interval.end.date(),
        }
        if interval.end > interval.start else {}
    )
    logical = (
        {"execution_date": execution_date.date()} if execution_date.date() != start else {}
    )
    return {"start_date": start, **ends, **logical}


def task_placeholders(task: RenderedTask) -> dict[str, date]:
    """:func:`date_placeholders` for the run ``task`` was rendered in."""
    return date_placeholders(task.data_interval, task.execution_date)


@dataclasses.dataclass(frozen=True)
class Ambiguity:
    """A date that fits several placeholders, left as written."""

    literal: str
    candidates: tuple[str, ...]


@dataclasses.dataclass(frozen=True)
class Templated:
    """A string with its run's dates replaced by placeholders where unambiguous."""

    text: str
    ambiguous: tuple[Ambiguity, ...] = ()
    constants: tuple[str, ...] = ()


def _parse_literal_date(literal: str) -> date | None:
    fmt = "%Y-%m-%d" if "-" in literal else "%Y%m%d"
    try:
        return datetime.strptime(literal, fmt).date()
    except ValueError:
        return None


def _literal_index(placeholders: Mapping[str, date]) -> dict[str, tuple[str, ...]]:
    """Each date literal (both spellings) -> the placeholder names it can stand for."""
    spellings = tuple(chain.from_iterable(
        ((d.isoformat(), name), (d.strftime("%Y%m%d"), f"{name}_nodash"))
        for name, d in placeholders.items()
    ))
    return {
        literal: tuple(name for lit, name in spellings if lit == literal)
        for literal in dict.fromkeys(lit for lit, _ in spellings)
    }


def templatize(text: str, placeholders: Mapping[str, date]) -> Templated:
    """Replace the run's dates in ``text`` by ``{placeholder}`` where unambiguous.

    :func:`render_template` with the same dates gives ``text`` back. Ambiguous
    dates stay as written and are listed in ``ambiguous``; real dates that
    match no placeholder are listed in ``constants``. Strings that only look
    like dates (``20261399``) are ignored.
    """
    index = _literal_index(placeholders)
    matches = sorted(
        (m for m in chain(_ISO_DATE.finditer(text), _COMPACT_DATE.finditer(text))
         if _parse_literal_date(m.group()) is not None),
        key=lambda m: m.start(),
    )
    replacements = {
        m.start(): f"{{{index[m.group()][0]}}}"
        for m in matches if len(index.get(m.group(), ())) == 1
    }
    # Each match with the end of the one before it (0 for the first).
    prev_ends = (0, *(m.end() for m in matches))[: len(matches)]
    pieces = chain.from_iterable(
        (text[prev_end:m.start()], replacements.get(m.start(), m.group()))
        for prev_end, m in zip(prev_ends, matches, strict=True)
    )
    tail = text[matches[-1].end():] if matches else text
    return Templated(
        text="".join(pieces) + tail,
        ambiguous=tuple(
            Ambiguity(m.group(), index[m.group()])
            for m in matches if len(index.get(m.group(), ())) > 1
        ),
        constants=tuple(m.group() for m in matches if m.group() not in index),
    )


def render_template(text: str, dates: Mapping[str, date]) -> str:
    """Fill ``{placeholder}`` / ``{placeholder_nodash}`` in ``text`` from ``dates``.

    Only the names in :data:`PLACEHOLDERS` are placeholders, so other braces
    (JSON-valued ``--labels``) are left alone. ``KeyError`` for a placeholder
    missing from ``dates``.
    """
    def fill(match: re.Match[str]) -> str:
        value = dates[match.group(1)]
        return value.strftime("%Y%m%d") if match.group(2) else value.isoformat()

    return _PLACEHOLDER.sub(fill, text)


def templatize_argv(task: RenderedTask) -> tuple[Templated, ...]:
    """:func:`templatize` every token of ``task``'s argv against its own run."""
    placeholders = task_placeholders(task)
    return tuple(templatize(token, placeholders) for token in task.argv)


# --------------------------------------------------------------------------
# JSON-friendly view (for the CLI)
# --------------------------------------------------------------------------

def task_summary(task: RenderedTask, *, templated: bool = False) -> dict[str, Any]:
    """A JSON-serialisable view of ``task``. With ``templated``, the argv's
    run dates are placeholders and the ambiguous dates are listed."""
    summary: dict[str, Any] = {
        "dag_id": task.dag_id,
        "task_id": task.task_id,
        "image": task.image,
        "version": task.version,
        "cmds": list(task.cmds),
        "arguments": list(task.arguments),
        "data_interval": [task.data_interval.start.isoformat(), task.data_interval.end.isoformat()],
        "execution_date": task.execution_date.isoformat(),
        "upstream_task_ids": list(task.upstream_task_ids),
        "downstream_task_ids": list(task.downstream_task_ids),
    }
    if task.structured_arguments:
        summary["structured_arguments"] = [dict(a) for a in task.structured_arguments]
    if templated:
        tokens = templatize_argv(task)
        summary["templated_argv"] = [t.text for t in tokens]
        summary["ambiguous_dates"] = sorted({
            (a.literal, ",".join(a.candidates)) for t in tokens for a in t.ambiguous
        })
    return summary
