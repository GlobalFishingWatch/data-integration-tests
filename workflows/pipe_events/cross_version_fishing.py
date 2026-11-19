"""Cross-version experiment for pipe-events fishing events.

Pins the source data by copying it at run start, runs
``workflows/pipe_events/fishing.py`` once per pipeline-version binding (with
each binding pointed at the pinned copies), and then diffs corresponding
output views pairwise across bindings on ``event_id``. Diff results are
reported but do not fail the run -- the *point* of cross-version testing is
to surface behaviour change, so a non-empty diff is information, not error.

Example: validate the ``refactor/fishing_events_incremental_stage_two``
branch by comparing it against ``main``::

    dit run workflows/pipe_events/cross_version_fishing.py \\
        --experiment-id refactor-stage-two \\
        --start 2020-01-01 --end 2021-01-01 \\
        --binding main=main \\
        --binding refactor=refactor/fishing_events_incremental_stage_two \\
        --modes 1_bf \\
        --parallel

Steps:

1. Verify every binding's git ref exists in ``$PROJECTS/pipe-events``.
2. Pin the SEVEN moving sources with ``dit.bq.pin_source_into_experiment``:
   copy each into ``<project>.tech_great_expectations`` (default 7-day TTL).
   What is copied follows what pipe-events reads (its SQL, checked from
   v4.2.17, ``fishing.py``'s default image, to v9.0.0):

   * ``research_messages``: the only source read by date. Step 1 reads
     ``DATE(timestamp)`` from one day before a slice's start up to its
     exclusive end, so the copy holds ``[--start - 1 day, --end)``, which
     covers every slice of every mode (see ``_messages_window``). The copy
     keeps the source's partitioning and clustering.
   * ``segs_activity``, ``segment_vessel``, ``product_vessel_info_summary``,
     ``identity_core``, ``identity_authorization``, ``event_regions``: read
     without a date filter, so copied whole.

   A source that is a view is copied too, as a plain table of the rows it
   returns (on the staging cohort: ``identity_core`` and
   ``identity_authorization``, which select from production's identity
   tables, and ``event_regions``).

   Every copy's name ends in the pin stamp (the run's start time in UTC,
   e.g. ``20261119t171500z``). ``--reuse-pin <stamp>`` reuses an earlier
   run's copies instead, e.g. on a retry. Pinning replaced BigQuery
   snapshots: BigQuery refuses a snapshot of a table in another
   organisation and of a view, and no DIT identity may create one with an
   expiration.

   The eighth ``fishing.py`` source, ``spatial_measures_20201105``, is
   deliberately NOT pinned. Its ``_YYYYMMDD`` filename suffix is a
   content-addressable version literal -- copying a version-pinned static
   table (58 GB) buys nothing. All bindings read the shared default for
   that table.

3. For each binding (in parallel by default; ``--sequential-bindings`` opts
   out): ``git worktree add`` a temp dir at the ref, invoke ``fishing.py``
   from that worktree with E4's per-table FQN flags
   (``--source-research-messages-fqn``,
   ``--source-segs-activity-fqn``,
   ``--source-segment-vessel-fqn``,
   ``--source-product-vessel-info-summary-fqn``,
   ``--source-identity-core-fqn``,
   ``--source-identity-authorization-fqn``,
   ``--source-event-regions-fqn``) pointing at the pinned copies; the
   pinned ``--start`` / ``--end`` / ``--tail-days``; and a binding-scoped
   ``--suffix``. Each subprocess's stdout/stderr is line-prefixed
   ``[<binding>] `` so parallel runs interleave readably.

4. For each mode in ``--modes`` and each pair of bindings, compare the
   corresponding ``<suffix>_<mode>_fishing_events`` view AND the
   ``<suffix>_<mode>_product_events_fishing`` view on ``event_id``. Pairs
   touching a binding that failed (rc != 0) are SKIPPED, not diffed.

The overall exit code is non-zero iff any binding failed; an individual
binding failure does not abort siblings.

``--start`` / ``--end`` (half-open, ``fishing.py``'s convention) and
``--tail-days`` are this wrapper's flags, not pass-through extras: they
decide the dates ``research_messages`` is copied over, and they are forwarded
to every binding so no binding runs outside the copy.

``--dry-run`` skips the ``fishing.py`` invocations and the diff phase, and
only dry-runs the copies (nothing is created; each copy's estimated bytes are
logged and checked against ``--pin-max-gb``), but still performs worktree
setup/teardown -- useful for validating the orchestration and the cost of the
pin without running anything.

Note on cross-version semantics vs port_visits: ``fishing.py`` is
docker-only (no Dataflow submitter/worker split), so this wrapper does NOT
carry the ``--binding-worker-image`` knob its port_visits sibling has.
Per-binding docker image identity comes from the worktree's HEAD:
``_run_binding`` invokes ``fishing.py`` with ``cwd=worktree_dir``, so its
``resolve_run_context`` -> ``ensure_pipeline_image`` reads that worktree's
git ref and auto-builds ``gcr.io/world-fishing-827/dit/pipe-events:dit-<sha>``
via kaniko for unreviewed refs (or reuses the canonical published image for
merged refs). ``--build-from-source`` is ALWAYS stripped from user extras
-- it makes the docker runner ignore ``--image-tag`` and build from the
mounted compose file's working tree, defeating per-binding image identity.

If a binding's ref cannot auto-build (broken Dockerfile, kaniko permission
failure), pre-build and push its image manually to
``gcr.io/world-fishing-827/dit/pipe-events:dit-<sha>`` before invoking this
wrapper -- ``ensure_pipeline_image``'s ``if_existing="skip"`` semantics
will use the pre-built image.
"""
from __future__ import annotations

import argparse
import dataclasses
import itertools
import logging
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dit import bq as dit_bq
from dit import compare as dit_compare
from dit.workflow import parse_modes

# fishing.py owns the canonical mode set + dataset and date defaults; import
# so the two can't drift.
from workflows.pipe_events.fishing import (
    DEFAULT_END,
    DEFAULT_INTERNAL_DS,
    DEFAULT_PIPE_REGIONS_LAYERS,
    DEFAULT_PIPE_STATIC,
    DEFAULT_PUBLISHED_DS,
    DEFAULT_START,
    DEFAULT_TAIL_DAYS,
)
from workflows.pipe_events.fishing import (
    MODES as FISHING_MODES,
)

logger = logging.getLogger(__name__)


PROJECT = "world-fishing-827"
DEFAULT_DEST_DATASET = os.environ.get("DIT_DEST_DATASET", "tech_great_expectations")
DEFAULT_MODES = ",".join(FISHING_MODES)
DEFAULT_PROJECTS_DIR = os.environ.get("PROJECTS") or str(
    Path(__file__).resolve().parents[2].parent
)
DEFAULT_PIPELINE_DIR = os.path.join(DEFAULT_PROJECTS_DIR, "pipe-events")
DEFAULT_PIN_EXPIRATION_DAYS = 7
DEFAULT_PIN_MAX_GB = dit_bq.DEFAULT_PIN_MAX_BYTES / 1e9

FISHING_WORKFLOW = Path(__file__).with_name("fishing.py")

# Binding names flow into ``{experiment_id}-{binding_name}`` which becomes
# a BQ table suffix -- must match the same regex ``add_experiment_id_arg``
# enforces on experiment_id itself. Reject e.g. ``refactor/branch`` (slash
# would produce an invalid table name).
_BINDING_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")


# --------------------------------------------------------------------------
# Argparse
# --------------------------------------------------------------------------

def parse_args(argv: Sequence[str] | None = None) -> tuple[argparse.Namespace, list[str]]:
    p = argparse.ArgumentParser(
        description="Cross-version experiment for pipe-events fishing events.",
    )
    p.add_argument("--experiment-id", required=True,
                   help="Slug for the experiment; appears in pinned-copy names and "
                        "output-table suffixes. Hyphens become underscores in the copies' "
                        "names.")
    # Removed 2026-11 (PIPELINE-4705): sources are pinned by copying them at
    # run start, and a copy cannot be taken at a past time without time
    # travel. --snapshot-expiration-days went with the snapshots. Both are
    # rejected explicitly, so an old invocation fails here rather than
    # leaking into every binding's fishing.py extras after the copies.
    p.add_argument("--pin-source-at", help=argparse.SUPPRESS)
    p.add_argument("--snapshot-expiration-days", help=argparse.SUPPRESS)
    p.add_argument("--binding", action="append", required=True, dest="bindings",
                   help="`name=ref` pair, repeatable. Both must be valid git refs in "
                        "--pipeline-dir. Name must match [a-z0-9][a-z0-9_-]{0,31} because it "
                        "flows into a BQ table suffix.")
    # The wrapper owns the run's dates: they decide the window of the
    # research_messages copy, so they are validated before anything is pinned
    # and forwarded to every binding.
    p.add_argument("--start", default=DEFAULT_START,
                   help=f"Inclusive start date (half-open window [start, end)), forwarded to "
                        f"every binding and used to window the pinned research_messages. "
                        f"Default {DEFAULT_START}.")
    p.add_argument("--end", default=DEFAULT_END,
                   help=f"Exclusive end date (half-open window [start, end)), forwarded to "
                        f"every binding and used to window the pinned research_messages. "
                        f"Default {DEFAULT_END}.")
    p.add_argument("--tail-days", type=int, default=DEFAULT_TAIL_DAYS,
                   help=f"Number of tail days of the bfd / bftruncate daily iteration, "
                        f"forwarded to every binding. Default {DEFAULT_TAIL_DAYS}.")
    p.add_argument("--modes", default=DEFAULT_MODES,
                   help=f"Comma-separated mode names whose output views get diffed pairwise. "
                        f"Default {DEFAULT_MODES}.")
    p.add_argument("--pipeline-dir", default=DEFAULT_PIPELINE_DIR,
                   help="pipe-events checkout used for git worktrees.")
    # Source dataset knobs -- the wrapper's own copies of fishing.py's
    # dataset defaults. They name the tables that are pinned, so the copies
    # are of whichever cohort the user selected.
    p.add_argument("--internal-ds", default=DEFAULT_INTERNAL_DS,
                   help="Source internal AIS dataset to pin from (research_messages, "
                        "segment_vessel).")
    p.add_argument("--published-ds", default=DEFAULT_PUBLISHED_DS,
                   help="Source published AIS dataset to pin from (segs_activity, identity_*, "
                        "vessel info).")
    p.add_argument("--pipe-static", default=DEFAULT_PIPE_STATIC,
                   help="Source pipe_static dataset. NOTE: spatial_measures_20201105 is NOT "
                        "pinned (content-addressable version literal); this knob is accepted "
                        "for parity with fishing.py but has no effect. Every binding reads "
                        "fishing.py's default spatial_measures FQN.")
    p.add_argument("--pipe-regions-layers", default=DEFAULT_PIPE_REGIONS_LAYERS,
                   help="Source pipe_regions_layers dataset to pin from (event_regions).")
    p.add_argument("--dest-dataset", default=DEFAULT_DEST_DATASET,
                   help="BQ dataset holding the output tables produced by each binding's "
                        "fishing.py run.")
    p.add_argument("--reuse-pin", default=None, metavar="STAMP",
                   help="Reuse the pinned copies of an earlier run with the same "
                        "--experiment-id, --start, --end, --tail-days and sources instead of "
                        "copying again. STAMP is the pin stamp that run logged (e.g. "
                        "20261119t171500z). Fails if any copy is missing.")
    p.add_argument("--pin-expiration-days", type=int, default=DEFAULT_PIN_EXPIRATION_DAYS,
                   help=f"Days until the pinned copies expire. Default "
                        f"{DEFAULT_PIN_EXPIRATION_DAYS}. Raise it for a run that could outlive "
                        f"them.")
    p.add_argument("--pin-max-gb", type=float, default=DEFAULT_PIN_MAX_GB,
                   help=f"Refuse to pin a source whose copy would process more than this many "
                        f"GB (10^9 bytes), checked by a dry run. Per copy, not per run. Default "
                        f"{DEFAULT_PIN_MAX_GB:g}.")
    p.add_argument("--dry-run", action="store_true",
                   help="Skip fishing.py invocations and pairwise diffs; dry-run the copies "
                        "only (nothing is created; each estimate is logged against "
                        "--pin-max-gb); still sets up and tears down worktrees.")
    p.add_argument("--sequential-bindings", action="store_true",
                   help="Run bindings serially instead of the default (parallel). Useful "
                        "when debugging a single binding's logs without interleave.")
    args, fishing_extra_args = p.parse_known_args(argv)
    if args.pin_source_at is not None:
        raise SystemExit(
            "--pin-source-at was removed: sources are now pinned by copying them at run "
            "start. Drop the flag; to reuse an earlier run's copies, pass --reuse-pin <stamp>."
        )
    if args.snapshot_expiration_days is not None:
        raise SystemExit(
            "--snapshot-expiration-days was removed with snapshot pinning; "
            "use --pin-expiration-days."
        )
    del args.pin_source_at, args.snapshot_expiration_days
    args.bindings = [_parse_binding(b) for b in args.bindings]
    args.modes = parse_modes(args.modes, choices=FISHING_MODES)
    # Validate binding names against the BQ-safe subset -- they flow into
    # output-table suffixes as ``{experiment_id}-{binding_name}``. A typo
    # like ``--binding refactor/stage_two=...`` would otherwise produce an
    # invalid BQ table name only at write time, deep into the run.
    _validate_binding_names(args.bindings)
    # The same checks fishing.py makes in main(), made here before anything
    # is pinned rather than in every binding afterwards.
    args.start = _parse_date("--start", args.start)
    args.end = _parse_date("--end", args.end)
    if args.end <= args.start:
        raise SystemExit(
            f"--end ({args.end}) must be after --start ({args.start}); the window is "
            f"half-open [start, end)."
        )
    if args.tail_days < 1:
        raise SystemExit(f"--tail-days must be >= 1; got {args.tail_days}")
    if args.reuse_pin is not None:
        try:
            dit_bq.parse_pin_stamp(args.reuse_pin)
        except ValueError as exc:
            raise SystemExit(f"--reuse-pin: {exc}") from exc
    if args.pin_max_gb <= 0:
        raise SystemExit(f"--pin-max-gb must be > 0; got {args.pin_max_gb}")
    if args.pin_expiration_days < 1:
        raise SystemExit(f"--pin-expiration-days must be >= 1; got {args.pin_expiration_days}")
    return args, fishing_extra_args


def _parse_binding(spec: str) -> tuple[str, str]:
    if "=" not in spec:
        raise SystemExit(f"--binding must be name=ref; got: {spec!r}")
    name, ref = spec.split("=", 1)
    if not name or not ref:
        raise SystemExit(f"--binding must be name=ref with both parts non-empty; got: {spec!r}")
    return name, ref


def _validate_binding_names(bindings: list[tuple[str, str]]) -> None:
    """Reject binding names that would produce an invalid or ambiguous
    BQ table suffix.

    Two checks -- both fail at parse time rather than deep into the run:

    1. Regex: must match [a-z0-9][a-z0-9_-]{0,31} (same shape
       ``add_experiment_id_arg`` enforces on ``experiment_id``), because
       binding name flows into ``{experiment_id}-{binding_name}``. A
       ``refactor/branch`` binding name would produce an invalid BQ table.

    2. Uniqueness: ``suffix_by_binding`` is a dict keyed on name, so a
       duplicate binding name silently collapses to one suffix -- but
       ``args.bindings`` still has both entries, so ``_invoke`` runs
       twice against the same suffix (concurrently, since parallel is
       the default). Result: two ``fishing.py`` runs writing the same
       output tables at once, zero diff pairs, exit 0. Plausible typo
       path is ``--binding main=main --binding main=refactor/...`` when
       someone means ``--binding refactor=...`` for the second.
    """
    bad = [name for name, _ in bindings if not _BINDING_NAME_RE.match(name)]
    if bad:
        raise SystemExit(
            f"--binding NAME must match {_BINDING_NAME_RE.pattern} (BQ-table-suffix-safe); "
            f"invalid: {bad}"
        )
    seen = [n for n, _ in bindings]
    dupes = sorted({n for n in seen if seen.count(n) > 1})
    if dupes:
        raise SystemExit(
            f"--binding NAME must be unique (names key the output-table suffix "
            f"AND the diff matrix); duplicated: {dupes}"
        )


def _parse_date(flag: str, s: str) -> date:
    try:
        return date.fromisoformat(s)
    except ValueError as exc:
        raise SystemExit(f"{flag}: invalid date {s!r}; expected YYYY-MM-DD") from exc


# --------------------------------------------------------------------------
# Pre-flight: verify refs
# --------------------------------------------------------------------------

def _verify_refs(pipeline_dir: str, bindings: list[tuple[str, str]]) -> None:
    if not Path(pipeline_dir, ".git").exists():
        raise SystemExit(f"--pipeline-dir {pipeline_dir} is not a git repo.")
    for name, ref in bindings:
        result = subprocess.run(
            ["git", "-C", pipeline_dir, "rev-parse", "--verify", "--quiet", ref + "^{commit}"],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise SystemExit(
                f"binding {name!r} ref {ref!r} not found in {pipeline_dir}. "
                f"Fetch it first (e.g. `git -C {pipeline_dir} fetch origin {ref}`)."
            )
        logger.info("binding %s: ref %s resolves to %s", name, ref, result.stdout.strip())


# --------------------------------------------------------------------------
# Pin the sources (copies in tech_great_expectations, dit.bq.pin_source_into_experiment)
# --------------------------------------------------------------------------

# Role in the pinned-copy names.
#
# The role is workflow-specific -- NOT the bare "cross_version" that
# port_visits/cross_version_ais.py uses -- so the two workflows' copies of a
# shared basename (both pin a `segs_activity`) stay apart by name for one
# --experiment-id, also when their pin stamps happen to coincide.
_SOURCE_PIN_ROLE = "cross_version_fishing"

# Time column of research_messages, and how far before a slice's start
# pipe-events reads it. Step 1 (fishing-events-1-incremental.sql.j2) filters
#     DATE(timestamp) >= DATE_SUB(DATE(start_date), INTERVAL 1 DAY)
#     AND DATE(timestamp) < DATE(end_date)
# for the merge with the previous day's open events. The filter is the same
# in every release from v4.2.17 (fishing.py's default image) to v9.0.0.
_MESSAGES_WINDOW_COLUMN = "timestamp"
_MESSAGES_PAD_DAYS = 1


@dataclasses.dataclass(frozen=True)
class _PinnedSources:
    """What every binding reads: the seven pinned copies, and the run shape
    (``start`` / ``end`` half-open, ``tail_days``) the ``research_messages``
    copy was windowed for. Threaded through ``_run_binding`` ->
    ``_fishing_args_for_binding``, which forwards the run shape too, so a
    binding cannot run over dates its copy does not hold.

    Seven tables, NOT eight: ``spatial_measures_20201105`` is content-
    addressable by its ``_YYYYMMDD`` filename suffix and is not pinned (see
    module docstring). All bindings share fishing.py's default for that
    table.

    Frozen so nothing changes the pins between pinning and the
    ``fishing.py`` invocations.
    """
    start: date
    end: date
    tail_days: int
    research_messages: str
    segs_activity: str
    segment_vessel: str
    product_vessel_info_summary: str
    identity_core: str
    identity_authorization: str
    event_regions: str


@dataclasses.dataclass(frozen=True)
class _PinSpec:
    """One source to pin: the ``_PinnedSources`` field it fills, the table
    (or view) to copy, and its window (None = whole)."""
    field: str
    source_table: str
    window: dit_bq.SourceWindow | None


def _messages_window(start: date, end: date, tail_days: int) -> dit_bq.SourceWindow:
    """The dates of ``research_messages`` that any slice of any mode reads.

    ``fishing.py`` runs these slices over the half-open ``[start, end)``:

    * ``1_bf``: ``[start, end)``;
    * ``2_bfd``: ``[start, end - tail_days)``, then one-day slices
      ``[d, d + 1)`` for the last ``tail_days`` days;
    * ``3_bftruncate``: ``[start, end)``, then the same one-day slices.

    Each slice reads from ``_MESSAGES_PAD_DAYS`` before its start up to its
    exclusive end. The earliest slice start is ``start``, or the first tail
    day ``end - tail_days`` if that is earlier (a tail longer than the run),
    and no slice ends after ``end``. The window does not depend on the
    selected modes, so one set of copies serves any ``--modes`` on a rerun.
    """
    first_slice_start = min(start, end - timedelta(days=tail_days))
    return dit_bq.SourceWindow(
        column=_MESSAGES_WINDOW_COLUMN,
        start=first_slice_start - timedelta(days=_MESSAGES_PAD_DAYS),
        end=end,
    )


def _pin_specs(args: argparse.Namespace) -> list[_PinSpec]:
    """The seven sources a run pins. ``research_messages`` is the only one
    pipe-events reads by date, so the only one windowed; the other six are
    read without a date filter (steps 2 and 3) and copied whole.

    ``spatial_measures_20201105`` is deliberately absent; see the module
    docstring.
    """
    # Use dataset knobs verbatim -- they already carry the project. Splitting
    # ``project.dataset`` on ``.`` and re-prefixing PROJECT silently discards
    # a user-supplied project (e.g. ``--internal-ds gfw-int-vms-v3.some_internal``
    # pinning from world-fishing-827 instead), which best-case produces a
    # mid-run 404 and worst-case copies the wrong data from a same-named
    # dataset in the default project. Match fishing.py's source resolvers
    # (``f"{args.internal_ds}.research_messages"``, etc.) so "the wrapper
    # pins exactly what fishing.py would have read" holds by construction,
    # not by coincidence with the defaults. A copy only reads its source, so
    # a source in another organisation works.
    #
    # (This deliberately diverges from cross_version_ais.py, which DOES need
    # ``PROJECT`` re-prefixing because its ``--source-dataset-stem`` is a
    # bare stem with no project.)
    return [
        _PinSpec(
            "research_messages", f"{args.internal_ds}.research_messages",
            _messages_window(args.start, args.end, args.tail_days),
        ),
        _PinSpec("segs_activity", f"{args.published_ds}.segs_activity", None),
        _PinSpec("segment_vessel", f"{args.internal_ds}.segment_vessel", None),
        _PinSpec(
            "product_vessel_info_summary",
            f"{args.published_ds}.product_vessel_info_summary", None,
        ),
        _PinSpec("identity_core", f"{args.published_ds}.identity_core", None),
        _PinSpec(
            "identity_authorization", f"{args.published_ds}.identity_authorization", None,
        ),
        _PinSpec("event_regions", f"{args.pipe_regions_layers}.event_regions", None),
    ]


def _pin_sources(
    args: argparse.Namespace, pinned_at: datetime, *, reuse: bool,
) -> _PinnedSources:
    """Pin every ``_pin_specs`` source under the stamp of ``pinned_at``, or
    with ``reuse``, check that an earlier run's copies under that stamp exist.

    All copies share one stamp, so ``--reuse-pin <stamp>`` can recompute
    every name. A reused copy must match this run's experiment id, window
    and source table name, since all of them are in the name.

    With ``--dry-run`` nothing is created: each copy is dry-run only, which
    logs its estimated bytes and still refuses one above ``--pin-max-gb``.
    """
    def pin(spec: _PinSpec) -> str:
        if reuse:
            return dit_bq.require_pinned_source(
                dit_bq.pinned_source_fqn(
                    spec.source_table,
                    experiment_id=args.experiment_id,
                    role=_SOURCE_PIN_ROLE,
                    pinned_at=pinned_at,
                    window=spec.window,
                    project=PROJECT,
                ),
                project=PROJECT,
            )
        return dit_bq.pin_source_into_experiment(
            spec.source_table,
            experiment_id=args.experiment_id,
            role=_SOURCE_PIN_ROLE,
            pinned_at=pinned_at,
            window=spec.window,
            expiration_days=args.pin_expiration_days,
            max_bytes=int(args.pin_max_gb * 1e9),
            dry_run=args.dry_run,
            project=PROJECT,
        )

    pinned = {spec.field: pin(spec) for spec in _pin_specs(args)}
    for field, fqn in pinned.items():
        logger.info("pinned %s: %s", field, fqn)
    return _PinnedSources(
        start=args.start, end=args.end, tail_days=args.tail_days, **pinned,
    )


def _pin(args: argparse.Namespace) -> _PinnedSources:
    """Pin what the bindings read: new copies of the sources, stamped with
    the run's start time, or an earlier run's copies with --reuse-pin."""
    reuse = args.reuse_pin is not None
    pinned_at = (
        dit_bq.parse_pin_stamp(args.reuse_pin) if reuse
        else datetime.now(timezone.utc).replace(microsecond=0)
    )
    logger.info("pin stamp: %s (%s)", dit_bq.format_pin_stamp(pinned_at),
                "reusing an earlier run's copies" if reuse else "new copies")
    logger.info("pinning: %s (copies in %s.tech_great_expectations)",
                ", ".join(spec.source_table for spec in _pin_specs(args)), PROJECT)
    pinned = _pin_sources(args, pinned_at, reuse=reuse)
    if not reuse and not args.dry_run:
        logger.info("to rerun on these copies, pass --reuse-pin %s",
                    dit_bq.format_pin_stamp(pinned_at))
    return pinned


# --------------------------------------------------------------------------
# Per-binding run via git worktree
# --------------------------------------------------------------------------

def _fishing_args_for_binding(
    extra_args: list[str],
    *,
    pinned: _PinnedSources,
    suffix: str,
    experiment_id: str,
    dest_dataset: str,
    modes: Sequence[str],
) -> list[str]:
    """Strip user-supplied overrides for fields the wrapper owns, then
    re-inject the wrapper's values.

    Emits E4's per-table FQN flags (added by
    ``feat(pipe-events): per-table source-FQN flags on fishing.py``) so
    each binding reads the pinned copies. The copies live in
    ``tech_great_expectations`` (not in a ``<stem>_internal`` /
    ``<stem>_published`` shape that ``--internal-ds`` / ``--published-ds``
    could address), so per-table FQNs are the only way to route fishing.py
    at them. ``--start`` / ``--end`` / ``--tail-days`` come from the pin
    too: the ``research_messages`` copy holds only the dates that run shape
    reads.

    Note ``spatial_measures_20201105`` is intentionally NOT overridden --
    it's content-addressable by its filename literal; every binding reads
    fishing.py's default for that table.

    Note ``fishing.py`` (unlike port_visits' ais.py) has NO
    ``--binding-name`` flag -- pipe-events is docker-only with no Dataflow
    job names to distinguish; the binding is encoded in the output-table
    ``--suffix`` (``{experiment_id}-{binding_name}``), which is the only
    distinguishability that matters at pipe-events' docker runtime. This
    is why the function takes ``suffix`` and ``experiment_id`` but not
    ``binding_name``: the wrapper composed the suffix from the two and
    threads only what fishing.py can act on.
    """
    # ALL of these are ALWAYS dropped in cross-version runs -- the wrapper
    # exclusively owns them (it pinned the inputs and knows the copies' FQNs
    # and window). A user-supplied --source-*-fqn (etc.) in extras could
    # otherwise leak an unpinned table into one binding, defeating the
    # cross-version pin.
    #
    # Split into value-taking flags (drop_kvs -- swallow the arg AND the
    # next token) vs store_true flags (drop_bare -- swallow only the arg
    # itself; conflating the two eats the following unrelated flag).
    drop_kvs = {
        # Dataset knobs: can only address <stem>_internal / <stem>_published
        # / pipe_static / pipe_regions_layers shapes -- can't reach the
        # copies in tech_great_expectations. Would produce a misleading
        # startup log even if their values were ignored downstream.
        "--internal-ds", "--published-ds", "--pipe-static", "--pipe-regions-layers",
        # All 8 per-table FQN flags. Even --source-spatial-measures-fqn is
        # dropped: the wrapper does not pin spatial_measures, but if a
        # user-extra pointed one binding at a different spatial_measures
        # version the cross-version comparison would silently diverge on
        # region membership. Force every binding to fishing.py's default.
        "--source-research-messages-fqn", "--source-segs-activity-fqn",
        "--source-segment-vessel-fqn", "--source-product-vessel-info-summary-fqn",
        "--source-identity-core-fqn", "--source-identity-authorization-fqn",
        "--source-spatial-measures-fqn", "--source-event-regions-fqn",
        # The run shape decides which dates of research_messages a binding
        # reads; the copy holds only the wrapper's. (parse_args consumes
        # these, so they reach here only from a direct caller.)
        "--start", "--end", "--tail-days",
        # Wrapper-owned run identity + mode selection.
        "--suffix", "--experiment-id",
        # --modes is the wrapper's own flag (it selects which modes to run
        # AND which to diff); a user extra must not desync the two halves.
        "--modes",
        # --image-tag pins a single container image across all bindings.
        # For pipe-events (BQ-SQL-via-container) the image IS the pipeline
        # code: pin it, both bindings run the SAME code, and the diff is
        # empty by construction while the wrapper cheerfully reports
        # IDENTICAL on both view families -- the worst possible failure
        # mode for this tool: a confident false pass on the exact question
        # it exists to answer. Per-binding image identity must come from
        # the worktree HEAD via ensure_pipeline_image, so --image-tag has
        # to be off-limits. Same reasoning as --build-from-source below,
        # via a different mechanism (ensure_pipeline_image short-circuits
        # on any override != default_worker_image).
        "--image-tag",
        # --dest-dataset is a fishing.py knob but the WRAPPER owns it
        # too (this parser accepts it, and _run_diffs reads it via
        # args.dest_dataset). Drop from user extras so the wrapper's
        # value is what gets threaded to fishing.py -- otherwise a user
        # extra could silently point the two bindings' writes at a
        # dataset different from where _run_diffs then reads.
        "--dest-dataset",
    }
    drop_bare = {
        # --build-from-source makes the docker runner ignore --image-tag
        # and build from the compose file's mounted working tree. That
        # breaks per-binding image identity (every container would run
        # whatever the laptop's compose file currently mounts, regardless
        # of ref). Force the auto-build path via ensure_pipeline_image.
        "--build-from-source",
    }
    out: list[str] = []
    skip_next = False
    for arg in extra_args:
        if skip_next:
            skip_next = False
            continue
        if arg in drop_kvs:
            skip_next = True
            continue
        if any(arg.startswith(k + "=") for k in drop_kvs):
            continue
        if arg in drop_bare:
            continue
        out.append(arg)
    out.extend([
        # Per-table FQN overrides route fishing.py at the pinned copies in
        # tech_great_expectations. Each binding reads the SAME copies so any
        # output divergence is attributable to pipeline code, not
        # source-data drift.
        "--source-research-messages-fqn", pinned.research_messages,
        "--source-segs-activity-fqn", pinned.segs_activity,
        "--source-segment-vessel-fqn", pinned.segment_vessel,
        "--source-product-vessel-info-summary-fqn", pinned.product_vessel_info_summary,
        "--source-identity-core-fqn", pinned.identity_core,
        "--source-identity-authorization-fqn", pinned.identity_authorization,
        "--source-event-regions-fqn", pinned.event_regions,
        # The run shape the research_messages copy was windowed for.
        "--start", pinned.start.isoformat(),
        "--end", pinned.end.isoformat(),
        "--tail-days", str(pinned.tail_days),
        # Forward the mode selection so each binding RUNS only the modes we
        # will diff.
        "--modes", ",".join(modes),
        "--suffix", suffix,
        # Threaded so both bindings' startup logs report the same
        # experiment_id (otherwise fishing.py's add_experiment_id_arg
        # auto-generates a distinct solo_<hex> per binding). --suffix
        # already wins for the table names via fishing.py's
        # _resolve_suffix; experiment_id is a label/log consistency knob.
        "--experiment-id", experiment_id,
        # Keep the write side (fishing.py's --dest-dataset) and the diff
        # side (_view_fqn's dest_dataset) in sync. Without this the
        # wrapper's args.dest_dataset would only affect where diffs LOOK,
        # while fishing.py falls back to its own default for where it
        # WRITES -- masked as long as both defaults resolve through
        # DIT_DEST_DATASET to the same value, but breaks the moment
        # anyone passes --dest-dataset explicitly.
        "--dest-dataset", dest_dataset,
    ])
    return out


def _stream_prefixed(stream, prefix: str, sink) -> None:
    """Reader thread: copies lines from ``stream`` to ``sink`` with ``prefix``.
    Python's stdout is GIL-protected at the per-write level, so concurrent
    reader threads on different subprocesses interleave cleanly at line
    granularity."""
    try:
        for line in iter(stream.readline, ""):
            sink.write(f"{prefix}{line}")
            sink.flush()
    finally:
        stream.close()


def _run_binding(
    *,
    name: str,
    ref: str,
    experiment_id: str,
    pinned: _PinnedSources,
    suffix: str,
    dest_dataset: str,
    modes: Sequence[str],
    pipeline_dir: str,
    fishing_extra_args: list[str],
    dry_run: bool,
) -> int:
    """git worktree add + invoke fishing.py from it + tear down.

    Per-binding docker image identity comes from the worktree's HEAD --
    ``Popen(cwd=worktree_dir)`` sets the CWD inside the child, and
    ``fishing.py``'s ``resolve_run_context`` reads that CWD (via
    ``git_info(repo_dir=os.getcwd())`` inside ``ensure_pipeline_image``)
    to compute the ref and, when unreviewed, kaniko-build
    ``pipe-events:dit-<sha>`` from ``git archive <commit>``.
    """
    worktree_dir = tempfile.mkdtemp(prefix=f"dit-xv-{name}-")
    try:
        subprocess.run(
            ["git", "-C", pipeline_dir, "worktree", "add", "--force", worktree_dir, ref],
            check=True, capture_output=True, text=True,
        )
        logger.info("binding %s: worktree at %s @ %s", name, worktree_dir, ref)

        argv = _fishing_args_for_binding(
            fishing_extra_args,
            pinned=pinned, suffix=suffix,
            experiment_id=experiment_id,
            dest_dataset=dest_dataset,
            modes=modes,
        )
        # fishing.py's argparse-based CLI: no click, no --help discovery
        # trick needed. Invoke as ``python -m pipe_events``? No -- fishing.py
        # has its own __main__ block that dispatches to main(argv).
        cmd = [sys.executable, str(FISHING_WORKFLOW), *argv]
        logger.info("binding %s: invoking %s", name, " ".join(shlex.quote(c) for c in cmd))

        if dry_run:
            logger.info("binding %s: --dry-run set; skipping fishing.py invocation", name)
            return 0

        # Stream subprocess output with a [binding-name] prefix so parallel
        # runs interleave readably. stderr is merged into stdout to keep
        # ordering coherent.
        prefix = f"[{name}] "
        env = {**os.environ}
        proc = subprocess.Popen(
            cmd, cwd=worktree_dir, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1,
        )
        reader = threading.Thread(
            target=_stream_prefixed,
            args=(proc.stdout, prefix, sys.stderr),
            daemon=True,
        )
        reader.start()
        rc = proc.wait()
        reader.join()
        return rc
    finally:
        # `git worktree remove --force` works even if the worktree path was modified.
        subprocess.run(
            ["git", "-C", pipeline_dir, "worktree", "remove", "--force", worktree_dir],
            check=False, capture_output=True,
        )
        # Tolerate residue if git's bookkeeping somehow left files behind.
        shutil.rmtree(worktree_dir, ignore_errors=True)
        logger.info("binding %s: worktree torn down", name)


# --------------------------------------------------------------------------
# Pairwise diffs (two view families: fishing_events + product_events_fishing)
# --------------------------------------------------------------------------

# Named-tuple-lite: the two view families a cross-version run diffs, and the
# suffix pattern fishing.py's _fishing_events_view / _product_events_view
# emit. Kept as a plain list so adding a third target later (say, an
# intermediate view) is a one-line change.
_DIFF_TARGETS = ("fishing_events", "product_events_fishing")


def _view_fqn(dest_dataset: str, suffix: str, mode: str, target: str) -> str:
    """Compose the view FQN fishing.py writes for one (suffix, mode, target).

    Mirrors ``fishing.py``'s ``_fishing_events_view`` /
    ``_product_events_view`` (which return
    ``{PROJECT}.{dest_dataset}.{suffix}_{mode}_{target}``).
    """
    return f"{PROJECT}.{dest_dataset}.{suffix}_{mode}_{target}"


def _diff_pair(
    *, dest_dataset: str, a_suffix: str, b_suffix: str, mode: str, target: str,
) -> int:
    return dit_compare.compare_tables(
        _view_fqn(dest_dataset, a_suffix, mode, target),
        _view_fqn(dest_dataset, b_suffix, mode, target),
        keys=["event_id"],
        view_suffix="",
    )


_SKIPPED = -1  # sentinel rc for diff pairs we couldn't run


def _run_diffs(
    *,
    modes: list[str],
    suffix_by_binding: dict[str, str],
    dest_dataset: str,
    failed_bindings: set[str],
) -> dict[tuple[str, str, str, str], int]:
    """Diff every (mode, target, binding_a, binding_b) combination.

    target ranges over `_DIFF_TARGETS` -- two view families per pair per
    mode. Pipe-events writes both a `_fishing_events` view (all authorized
    events) and a `_product_events_fishing` view (restrictive subset used
    downstream); a cross-version pin needs both to agree.
    """
    results: dict[tuple[str, str, str, str], int] = {}
    bindings = list(suffix_by_binding.keys())
    for mode in modes:
        for target in _DIFF_TARGETS:
            for a, b in itertools.combinations(bindings, 2):
                key = (mode, target, a, b)
                if a in failed_bindings or b in failed_bindings:
                    results[key] = _SKIPPED
                    failed_side = a if a in failed_bindings else b
                    logger.info(
                        "diff mode=%s target=%s %s vs %s -> SKIPPED (binding %s failed)",
                        mode, target, a, b, failed_side,
                    )
                    continue
                rc = _diff_pair(
                    dest_dataset=dest_dataset,
                    a_suffix=suffix_by_binding[a],
                    b_suffix=suffix_by_binding[b],
                    mode=mode,
                    target=target,
                )
                results[key] = rc
                verdict = "IDENTICAL" if rc == 0 else f"DIFFERENT (table-check rc={rc})"
                logger.info(
                    "diff mode=%s target=%s %s vs %s -> %s",
                    mode, target, a, b, verdict,
                )
    return results


def _summarize(results: dict[tuple[str, str, str, str], int]) -> str:
    lines = ["", "Cross-version diff summary:"]
    for (mode, target, a, b), rc in results.items():
        if rc == _SKIPPED:
            verdict = "SKIPPED"
        elif rc == 0:
            verdict = "IDENTICAL"
        else:
            verdict = "DIFFERENT"
        lines.append(f"  mode={mode}  target={target}  {a} vs {b}  -> {verdict}  (rc={rc})")
    return "\n".join(lines)


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args, fishing_extra_args = parse_args(argv)

    logger.info("experiment_id: %s", args.experiment_id)
    logger.info("date window (half-open): [%s, %s), tail_days=%d",
                args.start, args.end, args.tail_days)
    logger.info("bindings: %s", args.bindings)
    logger.info("modes: %s", args.modes)
    logger.info("pipeline_dir: %s", args.pipeline_dir)
    logger.info(
        "source: internal=%s  published=%s  regions=%s",
        args.internal_ds, args.published_ds, args.pipe_regions_layers,
    )
    logger.info("spatial_measures NOT pinned (content-addressable version literal); "
                "every binding reads fishing.py's default")

    _verify_refs(args.pipeline_dir, args.bindings)
    pinned = _pin(args)

    # Each binding gets a deterministic suffix the diff step can address.
    suffix_by_binding = {
        name: f"{args.experiment_id}-{name}" for name, _ in args.bindings
    }

    def _invoke(name: str, ref: str) -> tuple[str, int]:
        rc = _run_binding(
            name=name, ref=ref,
            experiment_id=args.experiment_id,
            pinned=pinned,
            suffix=suffix_by_binding[name],
            dest_dataset=args.dest_dataset,
            modes=args.modes,
            pipeline_dir=args.pipeline_dir,
            fishing_extra_args=fishing_extra_args,
            dry_run=args.dry_run,
        )
        return name, rc

    rc_by_binding: dict[str, int] = {}
    if args.sequential_bindings or len(args.bindings) == 1:
        logger.info("running %d binding(s) sequentially", len(args.bindings))
        for name, ref in args.bindings:
            n, rc = _invoke(name, ref)
            rc_by_binding[n] = rc
    else:
        logger.info("running %d bindings in parallel", len(args.bindings))
        with ThreadPoolExecutor(max_workers=len(args.bindings)) as ex:
            for n, rc in ex.map(lambda nr: _invoke(*nr), args.bindings):
                rc_by_binding[n] = rc

    failed_bindings = {n for n, rc in rc_by_binding.items() if rc != 0}
    for name, rc in rc_by_binding.items():
        if rc != 0:
            logger.error("binding %s failed with rc=%d", name, rc)

    if args.dry_run:
        logger.info("--dry-run set; skipping pairwise diffs.")
        return 1 if failed_bindings else 0

    # Single-binding case: no pairs to diff. Say so explicitly rather
    # than producing an empty summary + clean exit 0 that reads like
    # "compared and identical" but never ran a comparison. Same shape
    # as fishing.py's compare_all handles the single-mode case.
    if len(args.bindings) < 2:
        only_name = args.bindings[0][0]
        logger.info(
            "only one binding selected (%s -> suffix %s) -- no pair to diff. "
            "Its output views under `%s.%s` are ready for a follow-up run or "
            "manual inspection. Add a second --binding to trigger a comparison.",
            only_name, suffix_by_binding[only_name], PROJECT, args.dest_dataset,
        )
        return 1 if failed_bindings else 0

    results = _run_diffs(
        modes=args.modes,
        suffix_by_binding=suffix_by_binding,
        dest_dataset=args.dest_dataset,
        failed_bindings=failed_bindings,
    )
    print(_summarize(results), file=sys.stderr)
    return 1 if failed_bindings else 0


if __name__ == "__main__":
    sys.exit(main())
