"""Mode equivalence integration test for pipe-segment's ``segment`` step (AIS staging).

Runs the segmenter over one date window in several shapes and asserts that
every shape writes the same ``messages_segmented``, ``fragments`` and
``segments`` tables. The segmenter keeps state from day to day (which segment
a day's fragments continue, and each segment's running counts), so "one job
over the window equals one job per day" is the comparison most likely to find
something.

Nothing here is verified by a run yet. What the docstrings say about
pipe-segment was read from its code at v5.0.3 (f4d39fe) and v5.1.0 (4ddde03),
with file and line; what is inferred is labelled.

The unit: (mode, segmenter)
---------------------------
A **mode** is how the window is cut into jobs. A **segmenter** is which of
pipe-segment's two segmenters each job runs. One (mode, segmenter) pair is a
*variant*: it has its own three output tables and its jobs run one after the
other.

Modes (``--modes``):

* ``1_bf``: one job over ``[start, end]``. The reference shape.
* ``2_bfd``: one job over ``[start, end - tail_days]``, then one job per day
  for the last ``tail_days`` days. Mirrors a backfill followed by daily runs.
* ``3_bftruncate``: one job over ``[start, end]``, then the last ``tail_days``
  days rerun one by one, in order. Tests that rerunning days replaces them
  cleanly.
* ``4_daily``: every day of the window as its own job, in order. The
  strictest test of the chaining: every day boundary is a job boundary. It is
  a mode of its own, not ``--tail-days`` set to the window, because
  ``--tail-days`` applies to every mode of a run, and the test that needs
  "every day" also needs "backfill, then three days" in the same run. It is
  not in the default ``--modes``: on the default window it is 31 jobs.

Segmenters (``--segmenter``, default ``incremental``):

* ``incremental``: pipe-segment's ``--incremental`` (v5.1.0, pipe-segment PR
  #227). Each job starts from the segments of the day before its date range
  (``pipeline.py`` lines 159 to 171 at v5.1.0). The AIS v3 DAG passes it since
  2026-11-20 (composer-dags-production PR #3145), which is why it is the
  default here. That is what the DAG is configured for; no production run was
  looked at.
* ``full-history``: pipe-segment's own default, what the AIS DAG passed until
  then and what the VMS v5 DAG, on v5.0.3, still does. Each job reads every
  fragment in its fragments table and matches them all again
  (``transform/full_history_segments.py`` lines 62 to 69 at v5.1.0;
  ``pipeline.py`` at v5.0.3).

``--modes`` entries are variants. A bare mode (``2_bfd``) takes every
segmenter of ``--segmenter``; a qualified one (``1_bf:full-history``) names
its own. So ``--segmenter full-history,incremental`` runs the cross product,
and a list of qualified entries runs exactly the pairs named. One flag holds
the unit, because the cross product is not always what a test needs: the
pipe-segment PR #227 test is five of the eight pairs (44 jobs on the default
window, where the cross product is 80).

The default ``--modes`` is ``1_bf:full-history,1_bf,2_bfd,3_bftruncate``:
with the default segmenter, the three incremental shapes and full history in
one job. Full history in one job is the reference because it carries no state
between jobs at all: every segment is matched from the window's fragments in
one pass. A run of incremental shapes alone would only show that incremental
agrees with itself. ``1_bf:incremental`` separates "the incremental matching
differs" from "the hand-over between jobs differs". Ten jobs on the default
window.

A third part of the unit: the side (``@base``)
----------------------------------------------
A ``--modes`` entry may end in ``@base``. That variant runs on
``--base-image``, an overlay image of an earlier pipe-segment version, as
submitter and worker image; every other variant runs on the run's own images
(the head). So one run can hold three sides of a change such as PR #227: the
base version, the head in the base's mode, and the head in its new mode::

    --modes 1_bf:full-history@base,1_bf:full-history,1_bf:incremental \\
    --base-image <v5.0.3 overlay>

Without the base, "full history equals incremental" is tested on the head
alone, and a change to code both segmenters share would go unseen. PR #227
touched such code (``pipeline.py``, ``transform/read_messages.py``,
``transform/create_segments.py``; ``git diff --stat v5.0.3 v5.1.0``).

* Both sides read the same pinned copy and get the same flags. Nothing is
  added or removed for the older version: a base variant in full history
  passes exactly what a head variant in full history passes. That is enough
  for v5.0.3 against v5.1.0, whose command lines differ only in the three
  flags ``incremental`` adds. A base whose command line differs in another
  way needs per-side adjustments, which this workflow does not have yet.
* A base variant has its own three tables (tag ``…_base``) and sorts first
  within its mode, so ``1_bf:full-history@base`` is the reference when it is
  selected.
* There is no default base image: the base is whichever version the test is
  about. ``@base`` without ``--base-image`` is refused, and so is
  ``--base-image`` without an ``@base`` entry, which would read as a compared
  base. ``--build-from-source`` is refused with ``@base``: the working tree is
  the head.
* ``incremental`` is checked per side (next section but one). A base older
  than v5.1.0 runs full history only.
* The schema definitions of the three tables are the same in both tags
  (``schemas/message_schema.py``, ``schemas/segment_schema.py`` and
  ``transform/fragment.py`` are not in ``git diff v5.0.3 v5.1.0``). Read from
  the code; no table was written.

What the workflow guarantees about the chain
--------------------------------------------
From the PR's notes and the code:

* **A variant's tables start empty and are read by that variant only.** Table
  names end in ``<suffix>_<variant>``, and a run refuses to start if one of
  them already holds rows. A full-history job reads its whole fragments table
  with no date bound (``transform/read_fragments.py`` lines 26 to 41: no
  condition when ``start_date`` is ``None``). Pointed at a table that already
  holds the window, it counts those fragments twice: the PR's author got
  doubled message counts that way.
* **Jobs of a variant are consecutive.** Every job after the first starts no
  later than the day after the last day written, and the last job ends on the
  window's last day (``check_chain``). An incremental job needs the segments
  of the day before its range and fails if they are missing while earlier
  fragments exist (``hooks.py``, ``verify_prior_segments_hook``); the first
  job, on empty tables, starts from an empty state.
* **A rerun rewrites only its date range** (``pipeline.py`` lines 196 to 212
  at v5.1.0 clear the range, then the job appends). So ``3_bftruncate``
  reruns the last days in order, through the end of the window.
* **Every day of the window has messages.** An incremental job fails
  otherwise (``hooks.py``, ``verify_messages_exist_hook``). The default
  window's 31 days each hold 66,000 to 90,000 rows in the staging table
  (partition metadata, 2026-11-20).

``--incremental`` needs an image that knows it
----------------------------------------------
pipe-segment's command line hands every flag it does not know to Beam
(``cli/main.py`` line 47, ``parse_known_args``; ``pipeline.py``,
``PipelineOptions(beam_args)``). So on an image older than v5.1.0,
``--incremental`` would not fail. Inferred, not run: the job would run the
full-history segmenter, and "incremental against full history" would compare
full history with itself and pass. The workflow therefore refuses
``incremental`` before any cloud call unless every image that runs an
incremental variant says it is v5.1.0 or later (per side: the head's images
for the head's variants, ``--base-image`` for the ``@base`` ones). It reads that from the image tag, which must
start with the pipe-segment version (the overlay's ``build.sh`` label, e.g.
``v5.1.0-4ddde03-temp-dataset-…``). A tag that names no version is refused
too; ``--assume-incremental-support`` overrides that for an image known to
hold the flag. With ``--build-from-source`` the submitter is the working
tree, and the check reads its ``segment`` command for the flag.

The default image knows the flag. The refusal is for an explicit older image,
e.g. the overlay of v5.0.3 to test the version the VMS DAG pins: such a run
must name full history for every entry (``--segmenter full-history``).

Comparison
----------
The three tables are compared pairwise across the selected variants
(``dit.compare``, keys as in ``identity_match_key.py``). They are not equally
telling:

* ``segments`` is the **discriminating** one. A row holds the ``seg_id`` a
  fragment was matched to and the segment's cumulative count, identities and
  destinations. Both come from earlier days: from the fragment history in the
  full-history segmenter, from the stored rows of the day before in the
  incremental one.
* ``messages_segmented`` discriminates on ``seg_id`` only: each message gets
  the ``seg_id`` of its fragment.
* ``fragments`` is a **weak** detector of chaining errors. A fragment is built
  from one vessel's messages of one day (``pipeline.py`` lines 326 to 336 at
  v5.1.0) and from no other day's state. It differs between variants only if
  fragmenting is not deterministic or a clear-and-append went wrong.

The array columns (``identities`` and ``destinations`` in ``fragments``; the
four ``daily_*`` and ``cumulative_*`` columns in ``segments``) are
``ARRAY<STRUCT<...>>`` of scalars (``schemas/segment_schema.py``,
``transform/fragment.py``). ``table-check`` compares such columns as
multisets, so the order of their elements does not count as a difference.

Any difference fails the run (exit 1), as in the other mode-equivalence
workflows.

Sources, images, cache
----------------------
* **Source.** ``normalized_messages`` is pinned by copy over the window
  before any job runs (``dit.bq.pin_source_into_experiment``, shared with
  ``identity_match_key.py``). Every job reads the copy.
* **No satellite timing offsets, ever** (owner decision, 2026-11-20). The
  statement and its reasons are in ``identity_match_key.py``'s module
  docstring and hold for this workflow unchanged: no job passes any of the
  four satellite flags, and there is no flag to ask for them.
* **Images.** pipe-segment has no option to name the dataset Beam stages
  query results in, and DIT's service account may not create one. The default
  ``--image-tag`` and ``--worker-image`` are therefore a DIT overlay of
  v5.1.0, the version the AIS v3 DAG is configured for
  (``docker/overlays/pipe-segment-temp-dataset/``; shared with
  ``identity_match_key.py``). The workflow hands the
  overlay its dataset through the ``DIT_BQ_TEMP_DATASET`` container variable.
  As for port visits, DIT never auto-builds an image for this workflow (a
  plain build has no overlay), and a run that would execute the default image
  on unreviewed code refuses to start (``check_images``).
* **No run cache.** A variant writes three comparison tables and the cache
  hands back one, so every run recomputes what it selects.

``--dry-run`` reports the pin estimate, the planned jobs and the comparisons,
and creates nothing.

Dates: ``--start`` and ``--end`` are inclusive, like pipe-segment's
``--date_range``.

Example (the default: full history in one job against three incremental
shapes, January 2020, 10 jobs)::

    dit run workflows/pipe_segment/ais.py --experiment-id segment-modes

Example (the pipe-segment PR #227 test, 44 jobs; see RUN_INSTRUCTIONS.md)::

    dit run workflows/pipe_segment/ais.py --experiment-id pr227 \\
        --modes 1_bf:full-history,2_bfd:full-history,2_bfd,3_bftruncate,4_daily

Example (the same test with the base version as a sixth variant and the
reference, 45 jobs)::

    dit run workflows/pipe_segment/ais.py --experiment-id pr227 \\
        --modes 1_bf:full-history@base,1_bf:full-history,2_bfd:full-history,2_bfd,3_bftruncate,4_daily \\
        --base-image <v5.0.3 overlay>

Example (the version the VMS DAG pins, which has no incremental segmenter)::

    dit run workflows/pipe_segment/ais.py --experiment-id segment-v503 \\
        --segmenter full-history --modes 1_bf,2_bfd,3_bftruncate \\
        --image-tag <v5.0.3 overlay> --worker-image <v5.0.3 overlay>
"""
from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import functools
import itertools
import logging
import os
import re
import sys
import uuid
from collections.abc import Sequence
from datetime import date, timedelta
from pathlib import Path

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from dit import bq as dit_bq
from dit import compare as dit_compare
from dit import composer as dit_composer
from dit import dates as dit_dates
from dit import workflow as dit_workflow
from dit.job_names import make_job_name
from dit.runners import docker as dit_docker
from dit.snapshot import resolve_pipeline_commit
from dit.workflow import (
    add_dataset_args,
    add_experiment_id_arg,
    add_max_num_workers_arg,
    add_modes_arg,
    add_worker_sizing_args,
    log_production_sizing_hint,
    log_worker_caps,
    log_worker_sizings,
    parse_choice_list,
    resolve_run_context,
    resolve_worker_sizing,
    worker_cap,
    worker_sizing_from_args,
)
from workflows.pipe_segment import identity_match_key as imk

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Constants / defaults
# --------------------------------------------------------------------------

PIPELINE_NAME = imk.PIPELINE_NAME
PROJECT = imk.PROJECT

#: The one pipe-segment command this workflow runs; also its Dataflow job-name
#: step and its ``step`` label.
STEP = "segment"

MODE_BF = "1_bf"
MODE_BFD = "2_bfd"
MODE_BFTRUNCATE = "3_bftruncate"
MODE_DAILY = "4_daily"

#: Modes selectable via --modes, in canonical order.
SELECTABLE_MODES = (MODE_BF, MODE_BFD, MODE_BFTRUNCATE, MODE_DAILY)

SEGMENTER_FULL_HISTORY = "full-history"
SEGMENTER_INCREMENTAL = "incremental"

#: Segmenters selectable via --segmenter, in canonical order: full history
#: first, so that the reference variant sorts first.
SEGMENTERS = (SEGMENTER_FULL_HISTORY, SEGMENTER_INCREMENTAL)

#: The segmenter of a ``--modes`` entry that names none: the one the AIS v3
#: DAG's ``segment`` task passes. Its rendered snapshot (core_ais_v3 of
#: composer-dags-production at e4cb29361, read 2026-11-20) has
#: ``--incremental`` since that repository's PR #3145, merged the same day.
#: The VMS v5 DAG's task passes none. This is the DAGs' configuration, not a
#: record of what a production run did.
DEFAULT_SEGMENTER = SEGMENTER_INCREMENTAL

#: What a run without --modes executes: full history in one job, the
#: reference, and the three cheap shapes with the default segmenter. With the
#: default segmenter that is four variants and ten jobs on the default window.
#: ``4_daily`` is one job per day (31 there), so it is opt-in.
DEFAULT_MODES = (
    f"{MODE_BF}:{SEGMENTER_FULL_HISTORY}", MODE_BF, MODE_BFD, MODE_BFTRUNCATE,
)

#: Which code a variant runs on. ``head`` is the run's own images
#: (``--image-tag``, ``--worker-image``); ``base`` is ``--base-image``, an
#: earlier pipe-segment version to compare against. A ``--modes`` entry ends in
#: ``@base`` to run on the base image.
SIDE_HEAD = "head"
SIDE_BASE = "base"

#: Sides in canonical order: base first, so that a base variant, when one is
#: selected, sorts first within its mode and can be the reference.
SIDES = (SIDE_BASE, SIDE_HEAD)

SIDE_SEPARATOR = "@"

#: Each segmenter's short form in table names, job names and compose project
#: names, where ``-`` and ``:`` are not safe everywhere.
_SEGMENTER_TAGS = {SEGMENTER_FULL_HISTORY: "full", SEGMENTER_INCREMENTAL: "inc"}

#: The first pipe-segment version whose ``segment`` command knows
#: ``--incremental`` (pipe-segment PR #227, released as v5.1.0).
INCREMENTAL_SINCE = (5, 1, 0)

#: Staging cohort, January 2020: 363 MB to pin (dry run, 2026-11-20), and each
#: of its 31 days has rows (66,000 to 90,000; partition metadata, same day).
#: The cohort's name carries its snapshot date; its data is 2020.
DEFAULT_START = "2020-01-01"   # inclusive
DEFAULT_END = "2020-01-31"     # inclusive
DEFAULT_TAIL_DAYS = 3

#: Dataflow autoscaling ceiling per job, sized for the staging cohort (142
#: vessels, about 75,000 messages a day). The AIS DAG caps its segment task
#: at 50 (``identity_match_key.STEP_MAX_NUM_WORKERS``); with ``--parallel``
#: several variants submit at once, each up to this cap.
DEFAULT_MAX_NUM_WORKERS = 4

#: Worker sizing, mirroring production's segment task (owner rule,
#: 2026-10-23); shared with ``identity_match_key.py``.
STEP_WORKER_SIZING = {STEP: imk.STEP_WORKER_SIZING[STEP]}

#: The pipe-segment version the default image holds, the overlay directory and
#: the default overlay image (not built yet): one definition for both
#: pipe-segment workflows, in ``identity_match_key.py``, which holds the
#: citations. ``composer_drift.py`` compares the version with the image tag of
#: the DAG's task.
DEFAULT_PIPELINE_VERSION = imk.DEFAULT_PIPELINE_VERSION
OVERLAY_DIR = imk.OVERLAY_DIR
DEFAULT_OVERLAY_IMAGE = imk.DEFAULT_OVERLAY_IMAGE

#: Submitter image: ``docker run`` builds and submits the Beam graph in it. The
#: overlay sets the staging dataset at pipeline construction, so the submitter
#: must be an overlay image.
DEFAULT_IMAGE_TAG = DEFAULT_OVERLAY_IMAGE

#: Dataflow worker image (``--sdk_container_image``): the workers run the
#: transforms the submitter pickled, so it must hold the same code.
DEFAULT_WORKER_IMAGE = DEFAULT_OVERLAY_IMAGE

#: Container variable the overlay patch reads, and the default dataset it
#: names (see OVERLAY_DIR); shared with ``identity_match_key.py``.
TEMP_DATASET_ENV = imk.TEMP_DATASET_ENV
DEFAULT_BQ_TEMP_DATASET = imk.DEFAULT_BQ_TEMP_DATASET

#: pipe-segment's ``--runner`` value for each of this workflow's runners.
_BEAM_RUNNER = {"dataflow": "dataflow", "docker": "DirectRunner"}

#: The tables a variant writes, in the order they are compared, each with what
#: a difference in it means. ``segments`` first: it is the discriminating one.
COMPARED_TABLES = (
    ("segments",
     "DISCRIMINATING: seg_id carried from day to day, cumulative counts and identities"),
    ("messages_segmented",
     "discriminating for seg_id only: each message carries its fragment's seg_id"),
    ("fragments",
     "weak: built per vessel and day, no state from other days; differs only on "
     "nondeterminism or a broken clear-and-append"),
)
assert {name for name, _ in COMPARED_TABLES} == set(imk.SEGMENT_OUTPUT_TABLES)

COMPARE_VIEW_SUFFIX = ""


# --------------------------------------------------------------------------
# Variants: (mode, segmenter, side)
# --------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class Variant:
    """One way to run the window: how it is cut into jobs, which segmenter
    each job runs, and on which code. The unit that is run and compared."""

    mode: str
    segmenter: str
    side: str = SIDE_HEAD

    @property
    def label(self) -> str:
        """As typed in ``--modes`` and shown in logs: ``2_bfd:incremental``,
        ``1_bf:full-history@base``."""
        own = f"{self.mode}:{self.segmenter}"
        return own if self.side == SIDE_HEAD else f"{own}{SIDE_SEPARATOR}{self.side}"

    @property
    def tag(self) -> str:
        """Safe for table names, job names and labels: ``2_bfd_inc``,
        ``1_bf_full_base``."""
        own = f"{self.mode}_{_SEGMENTER_TAGS[self.segmenter]}"
        return own if self.side == SIDE_HEAD else f"{own}_{self.side}"


#: Every variant, in canonical order: by mode, then by side (base first), then
#: by segmenter. The first selected one is the reference:
#: ``1_bf:full-history@base`` when selected, else ``1_bf:full-history``.
ALL_VARIANTS = tuple(
    Variant(mode, segmenter, side)
    for mode in SELECTABLE_MODES for side in SIDES for segmenter in SEGMENTERS
)


def _split_entry(entry: str) -> tuple[str, str | None, str]:
    """One ``--modes`` entry, ``mode[:segmenter][@side]``, as
    ``(mode, segmenter or None, side)``. Nothing is validated here."""
    own, at, side = entry.partition(SIDE_SEPARATOR)
    mode, colon, segmenter = own.partition(":")
    return (
        mode.strip(),
        segmenter.strip() if colon else None,
        side.strip() if at else SIDE_HEAD,
    )


def parse_variants(raw_modes: str, raw_segmenters: str) -> list[Variant]:
    """The variants ``--modes`` and ``--segmenter`` select, in canonical order.

    A bare mode takes every segmenter of ``raw_segmenters``; ``mode:segmenter``
    names its own. Either form may end in ``@base`` to run on ``--base-image``.
    Duplicates collapse. An unknown or empty selection raises ``SystemExit``
    naming the valid choices, before any cloud call: a typo that ran nothing
    would read as a passing run.
    """
    segmenters = parse_choice_list(
        raw_segmenters, choices=SEGMENTERS, flag="--segmenter", noun="segmenter",
    )
    entries = [entry.strip() for entry in raw_modes.split(",") if entry.strip()]
    parts = [_split_entry(entry) for entry in entries]
    valid = (
        f"valid modes: {','.join(SELECTABLE_MODES)}, each optionally followed by "
        f"':' and one of {','.join(SEGMENTERS)}, and then optionally by "
        f"'{SIDE_SEPARATOR}{SIDE_BASE}' to run on --base-image"
    )
    if not parts:
        raise SystemExit(f"error: --modes must name at least one mode; {valid}")
    # Reported as typed, so the message echoes the command line.
    unknown = [
        entry for entry, (mode, own, side) in zip(entries, parts, strict=True)
        if mode not in SELECTABLE_MODES
        or (own is not None and own not in SEGMENTERS)
        or side not in SIDES
    ]
    if unknown:
        raise SystemExit(f"error: unknown mode(s) {','.join(unknown)}; {valid}")
    selected = {
        Variant(mode, segmenter, side)
        for mode, own, side in parts
        for segmenter in (segmenters if own is None else [own])
    }
    return [variant for variant in ALL_VARIANTS if variant in selected]


# --------------------------------------------------------------------------
# The plan: which jobs a variant runs, over which days
# --------------------------------------------------------------------------

Slice = tuple[date, date]  # first and last day of one job, both included


def _days(first: date, last: date) -> list[date]:
    """Every day from ``first`` to ``last``, both included."""
    # dit.dates.daterange_inclusive is half-open despite its name.
    return list(dit_dates.daterange_inclusive(first, last + timedelta(days=1)))


def plan_slices(mode: str, start: date, end: date, tail_days: int) -> tuple[Slice, ...]:
    """The date range of each job of ``mode`` over ``[start, end]``, in run order. Pure."""
    tail = tuple((day, day) for day in _days(end - timedelta(days=tail_days - 1), end))
    if mode == MODE_BF:
        return ((start, end),)
    if mode == MODE_BFD:
        return ((start, end - timedelta(days=tail_days)), *tail)
    if mode == MODE_BFTRUNCATE:
        return ((start, end), *tail)
    if mode == MODE_DAILY:
        return tuple((day, day) for day in _days(start, end))
    raise ValueError(f"unknown mode {mode!r}")


def check_chain(slices: Sequence[Slice], *, start: date, end: date) -> None:
    """Raise ``ValueError`` unless the jobs can run in this order on tables
    that start empty and end up holding ``[start, end]``. Pure.

    A job needs the day before its range to be written already: the
    incremental segmenter reads that day's segments, the full-history one the
    fragments up to it. A job rewrites only its own range, so after a rerun
    the days behind it are stale until they are rerun in order. Hence: the
    first job starts on ``start``; every later job starts no later than the
    day after the last day written; the last job ends on ``end``.
    """
    written_through: date | None = None
    for index, (first, last) in enumerate(slices, start=1):
        if last < first:
            raise ValueError(f"job {index} ends ({last}) before it starts ({first})")
        if first < start or last > end:
            raise ValueError(
                f"job {index} [{first}, {last}] leaves the window [{start}, {end}]"
            )
        if written_through is None:
            if first != start:
                raise ValueError(
                    f"the first job starts on {first}, not on the window's first day {start}"
                )
        elif first - timedelta(days=1) > written_through:
            raise ValueError(
                f"job {index} starts on {first}, but the last day written is "
                f"{written_through}: the days between are a gap"
            )
        written_through = last
    if written_through != end:
        raise ValueError(
            f"the last job ends on {written_through}, not on the window's last day {end}: "
            f"later days would be missing or stale"
        )


@dataclasses.dataclass(frozen=True)
class Job:
    """One ``segment`` invocation of a variant: its place in the variant's
    run order and its inclusive date range."""

    variant: Variant
    iteration: int
    total: int
    start: date
    end: date

    @property
    def first(self) -> bool:
        """The variant's first job: its tables are empty."""
        return self.iteration == 1


def plan_jobs(variant: Variant, start: date, end: date, tail_days: int) -> tuple[Job, ...]:
    """The jobs of ``variant`` over ``[start, end]``, in run order. Pure.
    ``ValueError`` if they would not chain (``check_chain``)."""
    slices = plan_slices(variant.mode, start, end, tail_days)
    check_chain(slices, start=start, end=end)
    return tuple(
        Job(variant, iteration, len(slices), first, last)
        for iteration, (first, last) in enumerate(slices, start=1)
    )


def _plans(args: argparse.Namespace) -> dict[Variant, tuple[Job, ...]]:
    """Every selected variant's jobs."""
    start, end = args.date_range
    return {v: plan_jobs(v, start, end, args.tail_days) for v in args.variants}


# --------------------------------------------------------------------------
# Names
# --------------------------------------------------------------------------

def _resolve_suffix(args: argparse.Namespace) -> str:
    """``<experiment_id>_<commit>_<uuid>``, or ``--suffix`` as given."""
    if args.suffix:
        return args.suffix
    return f"{args.experiment_id}_{args.commit_sha}_{uuid.uuid4().hex[:6]}"


def _table(args: argparse.Namespace, table: str, suffix: str, variant: Variant) -> str:
    """FQN of one of ``variant``'s output tables:
    ``<project>.<dataset>.<table>_<suffix>_<variant tag>``."""
    return imk._output_prefix(
        table, dest_dataset=args.dest_dataset, suffix=f"{suffix}_{variant.tag}",
    )


def _tables(args: argparse.Namespace, suffix: str, variant: Variant) -> dict[str, str]:
    """``variant``'s three output tables, by table name."""
    return {table: _table(args, table, suffix, variant) for table in imk.SEGMENT_OUTPUT_TABLES}


def _job_name(experiment_id: str, job: Job) -> str:
    """``dit-pipe-segment-segment-<experiment>-<variant>-<N>-<M>``; the
    experiment id is shortened when the name would pass 63 characters."""
    return make_job_name(
        repo=PIPELINE_NAME, step=STEP, experiment_id=experiment_id,
        mode=job.variant.tag, iteration=job.iteration, total_iterations=job.total,
    )


def _labels(args: argparse.Namespace, job: Job) -> list[str]:
    """``--labels`` of one job. pipe-segment requires them on every runner
    (``identity_match_key._step_labels``) and puts them on the tables it
    creates, its query jobs and the Dataflow job. No value may hold ``=``:
    pipe-segment splits each entry on it.

    ``dit_run_id`` is what ``make dit-cancel`` and the SIGTERM trap of
    ``dit run`` find a run's Dataflow jobs by.
    """
    variant = job.variant
    return [
        "--labels=environment=develop",
        "--labels=resource_creator=dit",
        f"--labels=step={STEP}",
        f"--labels=experiment_id={imk._safe_label_value(args.experiment_id)}",
        "--labels=dit_workflow=pipe_segment_ais",
        f"--labels=dit_mode={variant.mode}",
        f"--labels=dit_segmenter={variant.segmenter}",
        f"--labels=dit_side={variant.side}",
        f"--labels=dit_iteration={job.iteration}",
        f"--labels=dit_total_iterations={job.total}",
        f"--labels=dit_slice_start={job.start.isoformat()}",
        f"--labels=dit_slice_end={job.end.isoformat()}",
        f"--labels=dit_run_id={imk._safe_label_value(args.run_id)}",
        f"--labels=dit_commit_sha={imk._safe_label_value(args.commit_sha)}",
    ]


# --------------------------------------------------------------------------
# argv of one job
# --------------------------------------------------------------------------

def _segmenter_flags(args: argparse.Namespace, suffix: str, variant: Variant) -> list[str]:
    """The flags that select the segmenter. None for full history, which is
    pipe-segment's default.

    ``--in_segments_table`` and ``--in_fragments_table`` name the variant's
    own tables. Those are pipe-segment's defaults too (``pipeline.py`` lines
    137 to 149 at v5.1.0); they are passed so the argv says where the state
    comes from, and so a change of default upstream cannot move it.
    """
    if variant.segmenter != SEGMENTER_INCREMENTAL:
        return []
    tables = _tables(args, suffix, variant)
    return [
        "--incremental",
        f"--in_segments_table={tables['segments']}",
        f"--in_fragments_table={tables['fragments']}",
    ]


def variant_images(args: argparse.Namespace, variant: Variant) -> tuple[str, str]:
    """``(submitter image, worker image)`` of ``variant``. A base variant runs
    ``--base-image`` as both: the two must hold the same code, and an overlay
    image serves as either."""
    if variant.side == SIDE_BASE:
        return args.base_image, args.base_image
    return args.image_tag, args.worker_image


def segment_argv(
    args: argparse.Namespace, job: Job, *, suffix: str, pinned: imk._PinnedSources,
) -> list[str]:
    """argv of ``pipe-segment segment`` for ``job``. Pure.

    Built by ``identity_match_key._segment_args``, so a full-history job has
    the flags of that workflow's ``segment`` step, which mirror production's
    task: the same options, no satellite flag. What differs is DIT's own: the
    tables, the labels, the job name, and the date range of the job.
    """
    variant = job.variant
    _, worker_image = variant_images(args, variant)
    return imk._segment_args(
        pinned=pinned,
        dest_dataset=args.dest_dataset,
        suffix=f"{suffix}_{variant.tag}",
        experiment_id=args.experiment_id,
        binding_name=variant.tag,
        date_range=(job.start, job.end),
        runner=_BEAM_RUNNER[args.runner],
        worker_image=worker_image,
        ssvid_filter_query=args.ssvid_filter_query,
        service_account=args.service_account,
        max_num_workers=args.max_num_workers,
        worker_sizing=args.worker_sizings[STEP],
        build_from_source=args.build_from_source,
        labels=_labels(args, job),
        job_name=_job_name(args.experiment_id, job),
        extra_flags=_segmenter_flags(args, suffix, variant),
    )


def describe_job(args: argparse.Namespace, job: Job, *, suffix: str) -> str:
    """One line saying what ``job`` covers, reads and writes (for the plan)."""
    variant = job.variant
    tables = _tables(args, suffix, variant)
    days = (job.end - job.start).days + 1
    span = (f"{job.start}" if days == 1 else f"{job.start}..{job.end}") + f" ({days}d)"
    day_before = job.start - timedelta(days=1)
    if variant.segmenter == SEGMENTER_INCREMENTAL:
        state = (
            "an empty state (its tables are empty)" if job.first
            else f"the segments and fragments of {day_before} in its own tables"
        )
    else:
        state = (
            "no fragment history (its tables are empty)" if job.first
            else f"every fragment in {tables['fragments']}"
        )
    return (
        f"{variant.label} job {job.iteration}/{job.total}: {span}; reads the pinned "
        f"messages of that range and {state}; clears and writes that range"
    )


# --------------------------------------------------------------------------
# Running
# --------------------------------------------------------------------------

def _docker_run_kwargs(args: argparse.Namespace, variant: Variant) -> dict:
    """Keyword arguments for ``dit_docker.run``.

    ``GOOGLE_CLOUD_PROJECT``: Beam's BigQuery write builds its own client and
    takes the project from the environment (see
    ``identity_match_key._run_pipe_subcommand``). ``DIT_BQ_TEMP_DATASET``: the
    overlay patch stages every query read in that existing dataset.
    """
    return {
        "container_env": {
            "GOOGLE_CLOUD_PROJECT": PROJECT,
            TEMP_DATASET_ENV: args.bq_temp_dataset,
        },
        "entrypoint": imk.CLI_ENTRYPOINT,
        "volumes": [imk.GCP_VOLUME],
        "service": imk.COMPOSE_SERVICE,
        "build_from_source": args.build_from_source,
        "project_name": f"dit-pipe-segment-{variant.tag}",
    }


def run_variant(
    args: argparse.Namespace, variant: Variant, *, suffix: str, pinned: imk._PinnedSources,
) -> None:
    """Run ``variant``'s jobs one after the other. Each job reads what the
    jobs before it wrote, so the first failure ends the variant
    (``SystemExit``)."""
    start, end = args.date_range
    image_tag, _ = variant_images(args, variant)
    for job in plan_jobs(variant, start, end, args.tail_days):
        logger.info("%s", describe_job(args, job, suffix=suffix))
        rc = dit_docker.run(
            image_tag,
            segment_argv(args, job, suffix=suffix, pinned=pinned),
            **_docker_run_kwargs(args, variant),
        )
        if rc != 0:
            raise SystemExit(
                f"segment failed (rc={rc}): {variant.label} job {job.iteration}/{job.total} "
                f"[{job.start}, {job.end}]. The variant's later jobs were not run; its "
                f"tables hold the days written so far."
            )


def nonempty_tables(fqns: Sequence[str], *, project: str = PROJECT) -> list[str]:
    """Those of ``fqns`` that exist and hold rows. Free metadata reads."""
    client = bigquery.Client(project=project)

    def rows(fqn: str) -> int:
        try:
            return client.get_table(fqn).num_rows or 0
        except NotFound:
            return 0

    return [fqn for fqn in fqns if rows(fqn) > 0]


def require_empty_outputs(args: argparse.Namespace, suffix: str) -> None:
    """Refuse to run a variant whose output tables already hold rows.

    A variant's tables must start empty. A full-history job reads every
    fragment in its fragments table, and an incremental job starts from the
    stored segments, so rows left by another run would be read as this run's
    history. That happens only when ``--suffix`` names an earlier run's
    tables: a generated suffix is new.
    """
    fqns = [
        fqn for variant in args.variants for fqn in _tables(args, suffix, variant).values()
    ]
    found = nonempty_tables(fqns)
    if found:
        raise SystemExit(
            "error: these output tables already hold rows:\n  " + "\n  ".join(found)
            + "\nEvery variant must start from empty tables: a job reads its own "
            "fragments and segments tables as its history. Use another --suffix (or "
            "none), drop the tables, or pass --skip-pipelines to compare them as they are."
        )


# --------------------------------------------------------------------------
# Comparison
# --------------------------------------------------------------------------

#: (table, variant a, variant b) -> table-check verdict (0 identical, else different).
Verdicts = dict[tuple[str, Variant, Variant], int]


def comparison_pairs(variants: Sequence[Variant]) -> list[tuple[Variant, Variant]]:
    """Every pair of the selected variants, the reference's pairs first."""
    return list(itertools.combinations(variants, 2))


def compare_all(args: argparse.Namespace, suffix: str) -> Verdicts:
    """Compare the three tables pairwise across the selected variants."""
    verdicts: Verdicts = {}
    for table, meaning in COMPARED_TABLES:
        for a, b in comparison_pairs(args.variants):
            rc = dit_compare.compare_tables(
                _table(args, table, suffix, a),
                _table(args, table, suffix, b),
                keys=imk.COMPARE_KEYS[table],
                view_suffix=COMPARE_VIEW_SUFFIX,
            )
            verdicts[(table, a, b)] = rc
            logger.info("compare [%s / %s] %s vs %s -> rc=%s",
                        table, meaning, a.label, b.label, rc)
    return verdicts


def summarize(verdicts: Verdicts) -> str:
    """The verdicts as a table, discriminating comparison first."""
    lines = ["", "pipe-segment mode-equivalence summary:"]
    lines += [
        f"  {table:<18}  {a.label} vs {b.label}  -> "
        f"{'IDENTICAL' if rc == 0 else 'DIFFERENT'}"
        for (table, a, b), rc in verdicts.items()
    ]
    return "\n".join(lines)


# --------------------------------------------------------------------------
# Images
# --------------------------------------------------------------------------

_VERSION_TAG_RE = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)(?!\d)")


def image_pipeline_version(image: str) -> tuple[int, int, int] | None:
    """The pipe-segment version an image's tag starts with, or ``None``.

    ``…/pipe-segment:v5.1.0-4ddde03-temp-dataset-…`` gives ``(5, 1, 0)``; so
    does a published ``…:v5.1.0``. A tag that starts with anything else
    (``dit-<commit>``, a branch name) names no version.
    """
    match = _VERSION_TAG_RE.match(dit_composer.image_version(image) or "")
    if match is None:
        return None
    major, minor, patch = map(int, match.groups())
    return major, minor, patch


def _images_executed(
    args: argparse.Namespace, side: str = SIDE_HEAD,
) -> tuple[tuple[str, str], ...]:
    """``(flag, image)`` for each image the variants of ``side`` execute.

    Head: the submitter image runs unless ``--build-from-source`` replaces it,
    and the worker image runs only on Dataflow. Base: ``--base-image`` is both.
    A compare-only run executes none, and neither does a side with no variant
    selected.
    """
    if args.skip_pipelines or not any(v.side == side for v in args.variants):
        return ()
    if side == SIDE_BASE:
        return (("--base-image", args.base_image),)
    candidates = (
        ("--image-tag", args.image_tag, not args.build_from_source),
        ("--worker-image", args.worker_image, args.runner == "dataflow"),
    )
    return tuple((flag, image) for flag, image, executed in candidates if executed)


def _default_images_executed(args: argparse.Namespace) -> tuple[str, ...]:
    """The image flags still at the default whose image this run executes."""
    defaults = {"--image-tag": DEFAULT_IMAGE_TAG, "--worker-image": DEFAULT_WORKER_IMAGE}
    return tuple(flag for flag, image in _images_executed(args) if image == defaults[flag])


def check_images(args: argparse.Namespace, pipeline_commit: str, unreviewed: bool) -> None:
    """Refuse a run that would execute the default image on code it does not hold.

    The same guard as port visits' (``workflows/port_visits/ais.py``), for the
    same reason. The default image is pipe-segment ``DEFAULT_PIPELINE_VERSION``
    with DIT's temp-dataset overlay. For unreviewed code (a dirty tree, a DIT
    snapshot, a commit not merged into ``origin/main``) it is the wrong code,
    and the shared harness would replace a default worker image with a kaniko
    build of plain source: no overlay, so Beam would try to create its own
    BigQuery staging dataset and the Dataflow job would fail. So nothing is
    auto-built for this workflow (``resolve_run_context`` calls this in place
    of the auto-build), and the run stops here, before any registry, Cloud
    Build, BigQuery or Dataflow call. Reviewed code runs the default image;
    explicit images are always respected.
    """
    flags = _default_images_executed(args)
    if not (unreviewed and flags):
        return
    raise SystemExit(
        f"error: this pipe-segment checkout is unreviewed (commit {pipeline_commit}: a "
        f"dirty tree, a DIT snapshot, or not merged into origin/main), and the run would "
        f"execute DIT's default image for {' and '.join(flags)}:\n"
        f"  {DEFAULT_OVERLAY_IMAGE}\n"
        f"That image holds pipe-segment {DEFAULT_PIPELINE_VERSION} with DIT's temp-dataset "
        f"overlay, not this code. DIT does not auto-build an image for this workflow: a "
        f"plain build has no overlay, and its Dataflow job would fail creating a BigQuery "
        f"staging dataset. Choose one:\n"
        f"  - build an overlay image of the commit and pass it as BOTH --image-tag and "
        f"--worker-image:\n"
        f"      {OVERLAY_DIR}/build.sh <pipe-segment dir> {pipeline_commit} <label> --push\n"
        f"    (build.sh builds a commit; uncommitted changes are in it only if "
        f"{pipeline_commit} is a DIT snapshot)\n"
        f"  - check the working tree locally, without Dataflow: --runner docker "
        f"--build-from-source (no overlay there either: Beam creates its own staging "
        f"dataset, with your credentials)\n"
        f"  - run pipe-segment {DEFAULT_PIPELINE_VERSION} itself: use a clean checkout of a "
        f"commit merged into origin/main"
    )


#: Where a pipe-segment checkout declares the ``segment`` command's flags.
_SEGMENT_COMMAND_FILE = Path("src/pipe_segment/cli/commands/segment.py")

_INCREMENTAL_WHY = (
    "pipe-segment hands flags it does not know to Beam, which ignores them, so on older "
    "code --incremental would not fail: the job would run the full-history segmenter and "
    "the comparison with full history would pass without testing anything."
)


def _version_text(version: tuple[int, int, int]) -> str:
    return "v" + ".".join(map(str, version))


def check_base_side(args: argparse.Namespace) -> None:
    """Refuse a selection of ``@base`` variants that cannot run as asked. No
    cloud call.

    * ``@base`` needs ``--base-image``: there is no default, since the base is
      whichever earlier version the test is about.
    * ``--base-image`` without an ``@base`` entry would run nothing on it, and
      the run would read as if the base had been compared.
    * ``--build-from-source`` runs the working tree, which is the head.
    """
    base = [v.label for v in args.variants if v.side == SIDE_BASE]
    if args.base_image and not base:
        raise SystemExit(
            f"error: --base-image is given, but no --modes entry ends in "
            f"'{SIDE_SEPARATOR}{SIDE_BASE}', so nothing would run on it. Add one, e.g. "
            f"{MODE_BF}:{SEGMENTER_FULL_HISTORY}{SIDE_SEPARATOR}{SIDE_BASE}."
        )
    if not base:
        return
    if args.build_from_source:
        raise SystemExit(
            f"error: --build-from-source runs the working tree, and {', '.join(base)} "
            f"must run the base image. Run the head from an image too (--image-tag and "
            f"--worker-image), or drop the '{SIDE_SEPARATOR}{SIDE_BASE}' entries."
        )
    if not args.base_image and not args.skip_pipelines:
        raise SystemExit(
            f"error: {', '.join(base)} run on --base-image, and none is given. Pass an "
            f"overlay image of the base version, built with:\n"
            f"  {OVERLAY_DIR}/build.sh <pipe-segment dir> <commit or tag> <label> --push\n"
            f"with a label that starts with the version (e.g. v5.0.3), so that DIT can "
            f"tell which flags the image knows."
        )


def check_incremental_support(args: argparse.Namespace, repo_dir: str) -> None:
    """Refuse ``incremental`` unless every image that runs an incremental
    variant is known to know ``--incremental``. No cloud call: image tags and
    one local file. Checked per side: a base image older than the flag is fine
    as long as every ``@base`` entry names full history.

    * An image whose tag starts with a version older than
      ``INCREMENTAL_SINCE`` is refused.
    * An image whose tag names no version is refused unless
      ``--assume-incremental-support`` is given.
    * With ``--build-from-source`` the submitter is the working tree at
      ``repo_dir``; its ``segment`` command must declare the flag.

    ``incremental`` is the default segmenter and the default image knows the
    flag, so a default run passes. What is refused is an older or unversioned
    image given by flag while an incremental variant is still selected for it.
    """
    if args.skip_pipelines:
        return
    for side in SIDES:
        incremental = [
            v.label for v in args.variants
            if v.segmenter == SEGMENTER_INCREMENTAL and v.side == side
        ]
        if incremental:
            _check_incremental_support(args, repo_dir, side, incremental)


def _check_incremental_support(
    args: argparse.Namespace, repo_dir: str, side: str, incremental: Sequence[str],
) -> None:
    """``check_incremental_support`` for the incremental variants of one side."""
    since = _version_text(INCREMENTAL_SINCE)
    selected = f"the {SEGMENTER_INCREMENTAL} segmenter ({', '.join(incremental)})"
    image_flags = "--base-image" if side == SIDE_BASE else "--image-tag and --worker-image"
    full_history_only = (
        f"name full history in every '{SIDE_SEPARATOR}{SIDE_BASE}' entry (e.g. "
        f"{MODE_BF}:{SEGMENTER_FULL_HISTORY}{SIDE_SEPARATOR}{SIDE_BASE})"
        if side == SIDE_BASE else
        f"run this image with full history only (--segmenter "
        f"{SEGMENTER_FULL_HISTORY}, and no ':{SEGMENTER_INCREMENTAL}' entry in --modes)"
    )

    if args.build_from_source:
        command_file = Path(repo_dir) / _SEGMENT_COMMAND_FILE
        declared = command_file.is_file() and '"--incremental"' in command_file.read_text()
        if not declared:
            raise SystemExit(
                f"error: {selected} with --build-from-source needs a pipe-segment working "
                f"tree at {since} or later, and {command_file} declares no --incremental "
                f"flag. {_INCREMENTAL_WHY}"
            )

    for flag, image in _images_executed(args, side):
        version = image_pipeline_version(image)
        if version is not None and version >= INCREMENTAL_SINCE:
            continue
        if version is None and args.assume_incremental_support:
            logger.warning(
                "%s %s: the tag names no pipe-segment version; taking its --incremental "
                "support on your word (--assume-incremental-support).", flag, image,
            )
            continue
        found = (
            f"holds pipe-segment {_version_text(version)}" if version is not None
            else "has a tag that does not start with a pipe-segment version, so DIT "
                 "cannot tell what it holds"
        )
        way_out = (
            f"Choose one: {full_history_only}; "
            f"or pass an overlay image of {since} or later as {image_flags} "
            f"({OVERLAY_DIR}/build.sh <pipe-segment dir> <commit> <label> "
            f"--push, with a label that starts with the version, e.g. {since}; the default "
            f"image is one)"
            + ("." if version is not None
               else "; or pass --assume-incremental-support if the image is known to "
                    "hold the flag.")
        )
        raise SystemExit(
            f"error: {selected} needs pipe-segment {since} or later, and {flag} "
            f"{image} {found}. {_INCREMENTAL_WHY} {way_out}"
        )


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def _parse_date(value: str, flag: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise SystemExit(f"{flag}: invalid date {value!r}: {exc}") from exc


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Mode equivalence integration test for pipe-segment's segment step "
                    "(AIS staging).",
    )
    p.add_argument("--runner", choices=["dataflow", "docker"], default="dataflow",
                   help="dataflow: submit to Dataflow from inside the container (default). "
                        "docker: DirectRunner inside the container, for local checks.")
    add_modes_arg(
        p, choices=SELECTABLE_MODES, default=DEFAULT_MODES, cached=False,
        help_suffix=(
            f"An entry is a mode, run with every segmenter of --segmenter, or "
            f"mode:segmenter (e.g. {MODE_BF}:{SEGMENTER_FULL_HISTORY}) for exactly that "
            f"pair. The default is full history in one job, the reference, and three "
            f"shapes of the default segmenter. {MODE_DAILY} runs every day of the window "
            f"as its own job and is not in the default. The three tables are compared "
            f"across every pair of the selected (mode, segmenter) variants."
        ),
    )
    p.add_argument("--segmenter", default=DEFAULT_SEGMENTER,
                   help=f"Comma-separated segmenters for the --modes entries that name "
                        f"none: {', '.join(SEGMENTERS)}. Default: {DEFAULT_SEGMENTER}, which "
                        f"the AIS DAG's segment task passes: pipe-segment's --incremental, "
                        f"which needs an image of {_version_text(INCREMENTAL_SINCE)} or later "
                        f"(the default image is one). {SEGMENTER_FULL_HISTORY} passes no flag; "
                        f"it is pipe-segment's own default and what the VMS DAG runs with.")
    p.add_argument("--assume-incremental-support", action="store_true",
                   help="Accept an image whose tag names no pipe-segment version for an "
                        "incremental variant. Without it such an image is refused: on code "
                        "older than v5.1.0, --incremental is silently ignored.")
    p.add_argument("--start", default=DEFAULT_START,
                   help=f"First day of the window, inclusive. Default {DEFAULT_START}.")
    p.add_argument("--end", default=DEFAULT_END,
                   help=f"Last day of the window, inclusive. Default {DEFAULT_END}.")
    p.add_argument("--tail-days", type=int, default=DEFAULT_TAIL_DAYS,
                   help=f"Days at the end of the window that {MODE_BFD} and "
                        f"{MODE_BFTRUNCATE} run one job each. Default {DEFAULT_TAIL_DAYS}.")
    p.add_argument("--source-normalized-table", default=imk.DEFAULT_SRC_NORMALIZED_TABLE,
                   help=f"Normalized AIS messages, pinned by copy over the window. Default: "
                        f"{imk.DEFAULT_SRC_NORMALIZED_TABLE} (staging cohort). Must be one "
                        f"table partitioned on `timestamp`: not a view, not date-sharded "
                        f"tables. Every day of the window must have messages.")
    p.add_argument("--reuse-pin", default=None, metavar="STAMP",
                   help="Reuse the pinned copy of an earlier run with the same "
                        "--experiment-id, window and source instead of copying again. "
                        "STAMP is the pin stamp that run logged (e.g. 20261120t101500z).")
    p.add_argument("--pin-expiration-days", type=int, default=imk.DEFAULT_PIN_EXPIRATION_DAYS,
                   help=f"Days until the pinned copy expires. Default "
                        f"{imk.DEFAULT_PIN_EXPIRATION_DAYS}.")
    p.add_argument("--pin-max-gb", type=float, default=imk.DEFAULT_PIN_MAX_GB,
                   help=f"Refuse to pin a source whose copy would process more than this "
                        f"many GB (10^9 bytes), checked by a dry run. Default "
                        f"{imk.DEFAULT_PIN_MAX_GB:g}.")
    p.add_argument("--ssvid-filter-query", default=None,
                   help="Optional vessel filter, forwarded as segment's --ssvid_filter_query "
                        "(comma-separated double-quoted ssvids). For small local runs.")
    p.add_argument("--image-tag", default=DEFAULT_IMAGE_TAG,
                   help=f"Submitter image: builds and submits the Beam graph. Default: DIT's "
                        f"overlay of pipe-segment {DEFAULT_PIPELINE_VERSION}, the version "
                        f"the AIS DAG is configured for ({DEFAULT_IMAGE_TAG}). For other code, "
                        f"pass an "
                        f"overlay image of that commit, built with {OVERLAY_DIR}/build.sh, "
                        f"here and as --worker-image.")
    p.add_argument("--worker-image", default=DEFAULT_WORKER_IMAGE,
                   help="Dataflow worker image, passed to Beam as --sdk_container_image. "
                        "Default: the same overlay image as --image-tag. Must hold the same "
                        "code as the submitter.")
    p.add_argument("--base-image", default=None, metavar="IMAGE",
                   help=f"Image of an earlier pipe-segment version, run by the --modes "
                        f"entries that end in '{SIDE_SEPARATOR}{SIDE_BASE}' as submitter and "
                        f"worker image, e.g. --modes "
                        f"{MODE_BF}:{SEGMENTER_FULL_HISTORY}{SIDE_SEPARATOR}{SIDE_BASE},"
                        f"{MODE_BF}:{SEGMENTER_FULL_HISTORY},{MODE_BF}. An overlay image "
                        f"built with {OVERLAY_DIR}/build.sh. No default. Both sides read the "
                        f"same pinned copy and get the same flags; a base older than "
                        f"{_version_text(INCREMENTAL_SINCE)} runs full history only.")
    p.add_argument("--build-from-source", action="store_true",
                   help="Run `docker compose run dev` on the working tree instead of "
                        "--image-tag. That build has no temp-dataset overlay: fine with "
                        "--runner docker, but on Dataflow Beam will try to create its own "
                        "staging dataset, which DIT's service account may not.")
    p.add_argument("--bq-temp-dataset", default=DEFAULT_BQ_TEMP_DATASET,
                   help="Existing BigQuery dataset Beam stages query results in; env-var "
                        "fallback DIT_BQ_TEMP_DATASET (default ${PROJECT}.${DIT_DEST_DATASET}). "
                        "Passed to the overlay image as the DIT_BQ_TEMP_DATASET container "
                        "variable; pipe-segment has no option for it.")
    p.add_argument("--service-account", default=dit_workflow.DEFAULT_DATAFLOW_SA,
                   help="Service account the Dataflow workers run as; env-var fallback "
                        "DIT_DATAFLOW_SA.")
    add_dataset_args(p)
    add_max_num_workers_arg(
        p, default=DEFAULT_MAX_NUM_WORKERS,
        help_detail="The default is sized for the staging cohort; the AIS DAG caps its "
                    "segment task at 50.",
    )
    add_worker_sizing_args(p, defaults=STEP_WORKER_SIZING)
    p.add_argument("--suffix", default=None,
                   help="Output-table suffix; generated from the experiment id, the "
                        "pipe-segment commit and a random part when omitted. A run refuses "
                        "tables that already hold rows.")
    add_experiment_id_arg(p)
    p.add_argument("--require-clean", action="store_true",
                   help="Error on a dirty tree instead of auto-snapshotting.")
    p.add_argument("--skip-pipelines", action="store_true",
                   help="Skip the jobs; only compare the tables of --suffix (required).")
    p.add_argument("--skip-comparisons", action="store_true",
                   help="Skip the comparison phase; only run the jobs.")
    p.add_argument("--parallel", action="store_true",
                   help="Run the variants in parallel threads. The jobs of one variant "
                        "always run one after the other.")
    p.add_argument("--dry-run", action="store_true",
                   help="Report the pin estimate (a BigQuery dry run, checked against "
                        "--pin-max-gb), the planned jobs and the comparisons, and stop. "
                        "Creates nothing: no copy, no table, no image, no job.")
    args = p.parse_args(argv)

    args.variants = parse_variants(args.modes, args.segmenter)
    check_base_side(args)
    args.date_range = (_parse_date(args.start, "--start"), _parse_date(args.end, "--end"))
    start, end = args.date_range
    if end < start:
        raise SystemExit(
            f"--start ({args.start}) must be <= --end ({args.end}); both are inclusive."
        )
    if not 0 <= args.tail_days <= (end - start).days:
        raise SystemExit(
            f"--tail-days must be between 0 and the window's length minus one "
            f"({(end - start).days} for [{args.start}, {args.end}]); got {args.tail_days}."
        )
    if args.reuse_pin is not None:
        try:
            dit_bq.parse_pin_stamp(args.reuse_pin)
        except ValueError as exc:
            raise SystemExit(f"--reuse-pin: {exc}") from exc
    if args.pin_max_gb <= 0:
        raise SystemExit(f"--pin-max-gb must be > 0; got {args.pin_max_gb}")
    if args.pin_expiration_days < 1:
        raise SystemExit(f"--pin-expiration-days must be >= 1; got {args.pin_expiration_days}")
    if args.skip_pipelines and not args.suffix:
        raise SystemExit(
            "--skip-pipelines compares the tables of an earlier run: pass its --suffix."
        )
    if args.skip_pipelines and args.dry_run:
        raise SystemExit("--dry-run plans the jobs; it has nothing to do with --skip-pipelines.")
    # Every plan must chain, and every job name must fit, before anything runs.
    try:
        for jobs in _plans(args).values():
            for job in jobs:
                _job_name(args.experiment_id, job)
    except ValueError as exc:
        raise SystemExit(f"error: {exc}") from exc
    # Flag > ceiling env var > default; see dit.workflow.resolve_max_num_workers.
    args.worker_cap = worker_cap(args.max_num_workers, default=DEFAULT_MAX_NUM_WORKERS)
    args.max_num_workers = args.worker_cap.workers
    # Per field: flag > production's sizing (resolve_worker_sizing).
    args.cli_worker_sizing = worker_sizing_from_args(args)
    args.worker_sizings = {
        step: resolve_worker_sizing(args.cli_worker_sizing, default)
        for step, default in STEP_WORKER_SIZING.items()
    }
    return args


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def log_plan(args: argparse.Namespace, suffix: str) -> None:
    """Log every variant's tables and jobs, and the comparisons that follow."""
    plans = _plans(args)
    pairs = comparison_pairs(args.variants)
    logger.info(
        "plan: %d variant(s), %d segment job(s), %d comparison(s) (%d pair(s) x %d tables)",
        len(plans), sum(map(len, plans.values())), len(pairs) * len(COMPARED_TABLES),
        len(pairs), len(COMPARED_TABLES),
    )
    for variant, jobs in plans.items():
        logger.info("variant %s: %d job(s); image %s; tables %s",
                    variant.label, len(jobs), variant_images(args, variant)[0],
                    ", ".join(_tables(args, suffix, variant).values()))
        for job in jobs:
            logger.info("  %s", describe_job(args, job, suffix=suffix))
    if not pairs:
        logger.info(
            "only one variant selected (%s): no pair to compare. Add a second entry to "
            "--modes, or a second --segmenter, to get a comparison.", args.variants[0].label,
        )
        return
    logger.info("reference: %s (its pairs are compared first)", args.variants[0].label)
    for table, meaning in COMPARED_TABLES:
        logger.info("compare %s on %s, %d pair(s): %s",
                    table, ",".join(imk.COMPARE_KEYS[table]), len(pairs), meaning)


def _log_configuration(args: argparse.Namespace) -> None:
    start, end = args.date_range
    logger.info("experiment_id: %s", args.experiment_id)
    logger.info("variants: %s", ", ".join(v.label for v in args.variants))
    logger.info("window (inclusive): %s -> %s (%d days), tail_days=%d",
                start, end, (end - start).days + 1, args.tail_days)
    logger.info("source: %s", args.source_normalized_table)
    logger.info("runner: %s", args.runner)
    for side in reversed(SIDES):
        for flag, image in _images_executed(args, side):
            logger.info("%s: %s", flag, image)
    log_worker_caps({"every job": args.worker_cap})
    log_worker_sizings(args.worker_sizings)
    if args.runner == "dataflow":
        log_production_sizing_hint(
            args.cli_worker_sizing, args.worker_sizings, [args.source_normalized_table],
        )


def _dry_run(args: argparse.Namespace, repo_dir: str) -> int:
    """Report what a run would do; create nothing.

    The commit is read without snapshotting (``runner="docker"`` never
    snapshots), so for a dirty tree it is HEAD, where a real Dataflow run
    would first snapshot and push the tree and use that commit. The pin is a
    BigQuery dry run of the copy statement.
    """
    commit, unreviewed = resolve_pipeline_commit(
        repo_dir, PIPELINE_NAME, runner="docker", require_clean=False,
    )
    check_images(args, commit, unreviewed)
    args.commit_sha, args.run_id = commit, "dry-run"
    suffix = _resolve_suffix(args)
    logger.info("dry run: nothing is created. commit %s%s; suffix %s%s",
                commit, " (UNREVIEWED)" if unreviewed else "", suffix,
                "" if args.suffix else " (the last part is random per run)")
    require_empty_outputs(args, suffix)
    imk._pin(args)
    log_plan(args, suffix)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    repo_dir = os.getcwd()

    _log_configuration(args)
    # No cloud call yet: image tags and, with --build-from-source, one local file.
    check_incremental_support(args, repo_dir)

    if args.dry_run:
        return _dry_run(args, repo_dir)

    ctx = resolve_run_context(
        repo_dir=repo_dir,
        pipeline_name=PIPELINE_NAME,
        runner=args.runner,
        require_clean=args.require_clean,
        suffix=args.suffix or None,
        worker_image=args.worker_image,
        default_worker_image=DEFAULT_WORKER_IMAGE,
        # No run cache here, so the worker-image digest is unused.
        resolve_digest=False,
        # No auto-build for this workflow: a plain build cannot replace the
        # overlay default. Unreviewed code on a default image is refused.
        check_images=functools.partial(check_images, args),
    )
    if args.build_from_source and args.runner == "dataflow" and not args.skip_pipelines:
        logger.warning(
            "--build-from-source submits from a plain build of the working tree, without "
            "the temp-dataset overlay. Beam will try to create its own BigQuery staging "
            "dataset as %s; DIT's default service account may not. Prefer an overlay "
            "image of the commit as --image-tag (%s/build.sh).",
            args.service_account, OVERLAY_DIR,
        )
    args.run_context = ctx
    args.commit_sha = ctx.pipeline_commit
    args.worker_image = ctx.worker_image
    args.run_id = ctx.run_id

    suffix = _resolve_suffix(args)
    logger.info("suffix: %s  run_id: %s  commit: %s%s", suffix, args.run_id, args.commit_sha,
                " (UNREVIEWED)" if ctx.unreviewed else "")
    log_plan(args, suffix)

    if not args.skip_pipelines:
        require_empty_outputs(args, suffix)
        pinned = imk._pin(args)
        run = functools.partial(run_variant, args, suffix=suffix, pinned=pinned)
        if args.parallel:
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(args.variants)) as pool:
                futures = [pool.submit(run, variant) for variant in args.variants]
                for future in concurrent.futures.as_completed(futures):
                    future.result()  # re-raise
        else:
            for variant in args.variants:
                run(variant)

    if args.skip_comparisons or len(args.variants) < 2:
        return 0
    verdicts = compare_all(args, suffix)
    print(summarize(verdicts), file=sys.stderr)
    return int(any(rc != 0 for rc in verdicts.values()))


if __name__ == "__main__":
    sys.exit(main())
