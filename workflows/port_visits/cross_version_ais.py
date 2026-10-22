"""Cross-version experiment for pipe-anchorages port-visits (AIS).

Pins the source data by copying it at run start, runs
``workflows/port_visits/ais.py`` once per pipeline-version binding (with each
binding pointed at the pinned copies), and then diffs corresponding output
tables pairwise across bindings. Diff results are reported but do not fail the
run — the *point* of cross-version testing is to surface behaviour change, so
a non-empty diff is information, not error.

Example: validate PIPELINE-1465 by comparing v4.6.4 against the fix branch::

    dit run workflows/port_visits/cross_version_ais.py \\
        --experiment-id pipeline-1465 \\
        --start 2020-01-01 --end 2020-12-31 \\
        --binding before=v4.6.4 \\
        --binding after=fix/PIPELINE-1465_port_visit_start_location \\
        --binding-worker-image after=gcr.io/world-fishing-827/pipe-anchorages-1465-after:latest \\
        --modes 1_bf \\
        --runner dataflow --parallel --build-from-source

Steps:

1. Verify every binding's git ref exists in ``$PROJECTS/anchorages_pipeline``.
2. Pin what the selected ``--steps`` read with
   ``dit.bq.pin_source_into_experiment``: copy it into
   ``<project>.tech_great_expectations`` (default 7-day TTL), keeping the
   sources' partitioning and clustering:

   * ``thin_port_messages``: ``messages_positions`` over ``[--start, --end]``
     (the only range it reads);
   * ``port_visits``: ``segment_info`` and ``segs_activity`` whole (it joins
     and filters them without a date range), and without step 1 its input
     ``--thinned-message-table`` over ``[--start, --end]``.

   Every copy's name ends in the pin stamp (the run's start time in UTC,
   e.g. ``20261022t171500z``). ``--reuse-pin <stamp>`` reuses an earlier
   run's copies instead, e.g. on a retry. Pinning replaced BigQuery
   snapshots in 2026-10: BigQuery refuses a snapshot of a table in another
   organisation, and no DIT identity may create one with an expiration.
3. For each binding (in parallel by default; ``--sequential-bindings`` opts out):
   ``git worktree add`` a temp dir at the ref, invoke ``ais.py`` from that
   worktree with M4's per-table FQN flags (``--source-messages-fqn``,
   ``--source-segment-info-fqn``, ``--source-segs-activity-fqn``) pointing
   at the pinned copies; the pinned ``--start`` / ``--end``; a
   binding-scoped ``--suffix``; and optionally
   a per-binding ``--worker-image`` (from ``--binding-worker-image NAME=IMAGE``)
   and submitter ``--image-tag`` (from ``--binding-image-tag NAME=IMAGE``;
   required for ``--pipeline-cli v5`` runs, whose DIT overlay image must also
   be the submitter). Each subprocess's stdout/stderr is line-prefixed ``[<binding>] `` so
   parallel runs interleave readably.
4. For each mode in ``--modes`` and each pair of bindings, compare the
   corresponding outputs of the last selected step:
   ``port_visits_<exp>-<binding>_<mode>`` on ``visit_id``, or with ``--steps
   thin_port_messages`` ``port_events_<exp>-<binding>_<mode>`` on
   ``(identifier, timestamp)``. Pairs touching a binding that failed
   (rc != 0) are SKIPPED, not diffed.

The overall exit code is non-zero iff any binding failed; an individual
binding failure does not abort siblings.

``--dry-run`` skips the ``ais.py`` invocations and the diff phase, and only
dry-runs the copies (reporting their bytes against the ``--pin-max-gb`` cap),
but still performs worktree setup/teardown — useful for validating the
orchestration without burning Dataflow or BigQuery cost.

Note on cross-version semantics: ``ais.py``'s default ``--worker-image`` is a
fixed published path (e.g. ``pipe-anchorages:v4.6.4``). Without
``--binding-worker-image`` overrides, every binding's Dataflow workers run the
SAME published code regardless of the worktree ref — only the submission-side
orchestrator differs. For changes that live in worker code (most pipeline
changes — Beam PTransforms, DoFns), per-binding ``--worker-image`` is
required for the cross-version diff to be meaningful. Manually build + push
each binding's worktree to a registry both you and the Dataflow worker SA can
access (e.g. ``gcr.io/world-fishing-827/...``) and pass it here.
"""
from __future__ import annotations

import argparse
import dataclasses
import itertools
import logging
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Sequence

from dit import bq as dit_bq
from dit import compare as dit_compare
from dit.workflow import parse_choice_list, parse_modes

# ais.py owns the canonical mode and step sets, date defaults, step rules and
# comparison keys; import them so the two can't drift.
from workflows.port_visits.ais import COMPARE_KEYS_BY_STEP, STEP_THIN, STEP_VISITS, validate_steps
from workflows.port_visits.ais import DEFAULT_END as AIS_DEFAULT_END
from workflows.port_visits.ais import DEFAULT_START as AIS_DEFAULT_START
from workflows.port_visits.ais import SELECTABLE_MODES as AIS_SELECTABLE_MODES
from workflows.port_visits.ais import SELECTABLE_STEPS as AIS_SELECTABLE_STEPS

logger = logging.getLogger(__name__)


PROJECT = "world-fishing-827"
DEFAULT_DEST_DATASET = os.environ.get("DIT_DEST_DATASET", "tech_great_expectations")
DEFAULT_SOURCE_DATASET_STEM = "pipe_ais_test_202408290000"
DEFAULT_MODES = "1_bf,2_bfd,3_bftruncate"
DEFAULT_PROJECTS_DIR = os.environ.get("PROJECTS") or str(Path(__file__).resolve().parents[2].parent)
DEFAULT_PIPELINE_DIR = os.path.join(DEFAULT_PROJECTS_DIR, "anchorages_pipeline")
DEFAULT_PIN_EXPIRATION_DAYS = 7
DEFAULT_PIN_MAX_GB = dit_bq.DEFAULT_PIN_MAX_BYTES / 1e9

AIS_WORKFLOW = Path(__file__).with_name("ais.py")


# --------------------------------------------------------------------------
# Argparse
# --------------------------------------------------------------------------

def parse_args(argv: Optional[Sequence[str]] = None) -> tuple[argparse.Namespace, list[str]]:
    p = argparse.ArgumentParser(
        description="Cross-version experiment for pipe-anchorages port-visits.",
    )
    p.add_argument("--experiment-id", required=True,
                   help="Slug for the experiment; appears in pinned-copy names and output-table suffixes.")
    # Removed 2026-10: a copy cannot be taken at a past time without time
    # travel, and time travel loses partition pruning on tables modified
    # inside their time-travel window. Rejected explicitly, so an old
    # invocation fails here rather than leaking into ais.py's extras.
    p.add_argument("--pin-source-at", help=argparse.SUPPRESS)
    p.add_argument("--binding", action="append", required=True, dest="bindings",
                   help="`name=ref` pair, repeatable. Both must be valid git refs in --pipeline-dir.")
    p.add_argument("--start", default=AIS_DEFAULT_START,
                   help=f"Inclusive start date, forwarded to every binding and used to window the "
                        f"pinned messages. Default {AIS_DEFAULT_START}.")
    p.add_argument("--end", default=AIS_DEFAULT_END,
                   help=f"Inclusive end date, forwarded to every binding and used to window the "
                        f"pinned messages. Default {AIS_DEFAULT_END}.")
    p.add_argument("--modes", default=DEFAULT_MODES,
                   help=f"Comma-separated mode names whose output tables get diffed pairwise. Default {DEFAULT_MODES}.")
    p.add_argument("--steps", default=",".join(AIS_SELECTABLE_STEPS),
                   help=f"Comma-separated pipeline steps every binding runs (ais.py's --steps; "
                        f"default {','.join(AIS_SELECTABLE_STEPS)}). Decides what is pinned and "
                        f"which output is diffed (the last step's). {STEP_VISITS} alone needs "
                        f"--thinned-message-table.")
    p.add_argument("--pipeline-dir", default=DEFAULT_PIPELINE_DIR,
                   help="anchorages_pipeline checkout used for git worktrees.")
    p.add_argument("--source-dataset-stem", default=DEFAULT_SOURCE_DATASET_STEM,
                   help="Source dataset stem (in world-fishing-827) to pin from; the --source-*-fqn "
                        "flags override single tables.")
    p.add_argument("--source-messages-fqn", default=None,
                   help="Source messages_positions table to pin, e.g. a production table in another "
                        "organisation. Default: derived from --source-dataset-stem.")
    p.add_argument("--source-segment-info-fqn", default=None,
                   help="Source segment_info table to pin. Default: derived from --source-dataset-stem.")
    p.add_argument("--source-segs-activity-fqn", default=None,
                   help="Source segs_activity table to pin. Default: derived from --source-dataset-stem.")
    p.add_argument("--dest-dataset", default=DEFAULT_DEST_DATASET,
                   help="BQ dataset holding the output tables produced by each binding's ais.py run.")
    p.add_argument("--reuse-pin", default=None, metavar="STAMP",
                   help="Reuse the pinned copies of an earlier run with the same --experiment-id, "
                        "--start, --end and sources instead of copying again. STAMP is the pin "
                        "stamp that run logged (e.g. 20261022t171500z). Fails if any copy is missing.")
    p.add_argument("--pin-expiration-days", type=int, default=DEFAULT_PIN_EXPIRATION_DAYS,
                   help=f"Days until the pinned copies expire. Default {DEFAULT_PIN_EXPIRATION_DAYS}.")
    p.add_argument("--pin-max-gb", type=float, default=DEFAULT_PIN_MAX_GB,
                   help=f"Refuse to pin a source whose copy would process more than this many GB "
                        f"(10^9 bytes), checked by a dry run. Default {DEFAULT_PIN_MAX_GB:g}.")
    p.add_argument("--dry-run", action="store_true",
                   help="Skip ais.py invocations and pairwise diffs; dry-run the copies only "
                        "(nothing is created); still sets up and tears down worktrees.")
    p.add_argument("--sequential-bindings", action="store_true",
                   help="Run bindings serially instead of the default (parallel). Useful when debugging a single binding's logs without interleave.")
    p.add_argument("--binding-worker-image", action="append", default=[],
                   dest="binding_worker_images",
                   help="`name=image` pair, repeatable. Overrides --worker-image (passed to ais.py) for that one binding. "
                        "Lets cross-version actually exercise per-binding worker code, which the default static --worker-image does not. "
                        "Bindings without an override use ais.py's default.")
    p.add_argument("--binding-image-tag", action="append", default=[],
                   dest="binding_image_tags",
                   help="`name=image` pair, repeatable. Overrides --image-tag (the SUBMITTER image "
                        "ais.py runs to build and submit the Beam graph) for that one binding. Needed "
                        "with --pipeline-cli v5, where the DIT overlay image must also be the "
                        "submitter: the BigQuery staging dataset is set at pipeline construction. "
                        "Bindings without an override use ais.py's --image-tag.")
    p.add_argument("--thinned-message-table", default=None,
                   help="Fully-qualified BQ table holding pre-thinned port messages (e.g. "
                        "production's raw_port_events); only with --steps port_visits. The wrapper "
                        "pins it over [--start, --end], then passes the pinned copy to every "
                        "binding's ais.py as --thinned-message-table, so no binding runs step 1 "
                        "(thin_port_messages) -- saves the dominant cost when the change under "
                        "test is in step 2 only -- and messages_positions is not pinned. The "
                        "wrapper exclusively owns this knob in cross-version runs: any "
                        "user-supplied --thinned-message-table in extras is ALWAYS dropped (even "
                        "when this wrapper flag is unset), so the user can't inject an unpinned "
                        "table and bypass the pin.")
    args, ais_extra_args = p.parse_known_args(argv)
    if args.pin_source_at is not None:
        raise SystemExit(
            "--pin-source-at was removed: sources are now pinned by copying them at run "
            "start. Drop the flag; to reuse an earlier run's copies, pass --reuse-pin <stamp>."
        )
    args.bindings = [_parse_binding(b) for b in args.bindings]
    # Validate against ais.py's selectable modes -- previously any string
    # was accepted, so a typo silently produced zero diff pairs (which
    # reads identically to "everything matched").
    args.modes = parse_modes(args.modes, choices=AIS_SELECTABLE_MODES)
    args.steps = parse_choice_list(
        args.steps, choices=AIS_SELECTABLE_STEPS, flag="--steps", noun="step",
    )
    validate_steps(args.steps, args.thinned_message_table)
    args.start = _parse_date("--start", args.start)
    args.end = _parse_date("--end", args.end)
    if args.end < args.start:
        raise SystemExit(f"--end {args.end} is before --start {args.start}")
    if args.reuse_pin is not None:
        try:
            dit_bq.parse_pin_stamp(args.reuse_pin)
        except ValueError as exc:
            raise SystemExit(f"--reuse-pin: {exc}") from exc
    if args.pin_max_gb <= 0:
        raise SystemExit(f"--pin-max-gb must be > 0; got {args.pin_max_gb}")
    args.binding_worker_images = dict(_parse_binding(b) for b in args.binding_worker_images)
    args.binding_image_tags = dict(_parse_binding(b) for b in args.binding_image_tags)
    _binding_names = {n for n, _ in args.bindings}
    for flag, overrides in (
        ("--binding-worker-image", args.binding_worker_images),
        ("--binding-image-tag", args.binding_image_tags),
    ):
        _unknown = set(overrides) - _binding_names
        if _unknown:
            raise SystemExit(
                f"{flag} references unknown binding(s): {sorted(_unknown)}; "
                f"declared bindings: {sorted(_binding_names)}"
            )
    return args, ais_extra_args


def _parse_binding(spec: str) -> tuple[str, str]:
    if "=" not in spec:
        raise SystemExit(f"--binding must be name=ref; got: {spec!r}")
    name, ref = spec.split("=", 1)
    if not name or not ref:
        raise SystemExit(f"--binding must be name=ref with both parts non-empty; got: {spec!r}")
    return name, ref


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

# Roles in the pinned-copy names. Keeping the sources and the optional
# thinned table in DIFFERENT roles prevents a name collision when a caller
# supplies a --thinned-message-table whose basename matches a source's
# (e.g. "messages_positions").
_SOURCE_PIN_ROLE = "cross_version"
_THINNED_PIN_ROLE = "cross_version_thinned"

# Time column of messages_positions and of the thinned table. Both
# pipe-anchorages CLIs read them by DATE(timestamp) over the run's dates.
_WINDOW_COLUMN = "timestamp"


@dataclasses.dataclass(frozen=True)
class _PinnedSources:
    """What every binding reads: the pinned copies and the inclusive date
    range the windowed copies cover. Threaded through `_run_binding` ->
    `_ais_args_for_binding`, which forwards `start` / `end` too, so a
    binding cannot run over dates its copies don't hold.

    A field is None when no selected step reads it: `messages_positions`
    without step 1, `segment_info` / `segs_activity` without step 2, and
    `thinned` unless step 2 runs alone. Frozen so nothing changes the pins
    between pinning and the `ais.py` invocations.
    """
    start: date
    end: date
    messages_positions: Optional[str] = None
    segment_info: Optional[str] = None
    segs_activity: Optional[str] = None
    thinned: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class _PinSpec:
    """One source to pin: the `_PinnedSources` field it fills, the table to
    copy, its role, and its window (None = whole table)."""
    field: str
    source_table: str
    role: str
    window: Optional[dit_bq.SourceWindow]


def _source_tables(args: argparse.Namespace) -> tuple[str, str, str]:
    """(messages_positions, segment_info, segs_activity) FQNs to pin: each
    `--source-*-fqn` override, else derived from `--source-dataset-stem`."""
    stem = args.source_dataset_stem
    return (
        args.source_messages_fqn or f"{PROJECT}.{stem}_internal.messages_positions",
        args.source_segment_info_fqn or f"{PROJECT}.{stem}_published.segment_info",
        args.source_segs_activity_fqn or f"{PROJECT}.{stem}_published.segs_activity",
    )


def _pin_specs(args: argparse.Namespace) -> list[_PinSpec]:
    """What the selected steps read, so what a run pins: step 1 reads
    `messages_positions` over the run's dates; step 2 reads `segment_info` and
    `segs_activity` whole (no date range) and, without step 1, the
    `--thinned-message-table` over the run's dates."""
    messages, segment_info, segs_activity = _source_tables(args)
    window = dit_bq.SourceWindow(
        column=_WINDOW_COLUMN, start=args.start, end=args.end + timedelta(days=1),
    )
    thin_reads = [_PinSpec("messages_positions", messages, _SOURCE_PIN_ROLE, window)]
    external_thinned = (
        [] if STEP_THIN in args.steps
        else [_PinSpec("thinned", args.thinned_message_table, _THINNED_PIN_ROLE, window)]
    )
    visits_reads = [
        *external_thinned,
        _PinSpec("segment_info", segment_info, _SOURCE_PIN_ROLE, None),
        _PinSpec("segs_activity", segs_activity, _SOURCE_PIN_ROLE, None),
    ]
    return [
        *(thin_reads if STEP_THIN in args.steps else []),
        *(visits_reads if STEP_VISITS in args.steps else []),
    ]


def _pin_sources(
    args: argparse.Namespace, pinned_at: datetime, *, reuse: bool,
) -> _PinnedSources:
    """Pin every `_pin_specs` source under the stamp of `pinned_at`, or with
    `reuse`, check that an earlier run's copies under that stamp exist.

    All copies share one stamp, so `--reuse-pin <stamp>` can recompute
    every name. A reused copy must match this run's experiment id, role,
    window and source table name, since all of them are in the name.
    """
    def pin(spec: _PinSpec) -> str:
        if reuse:
            return dit_bq.require_pinned_source(
                dit_bq.pinned_source_fqn(
                    spec.source_table,
                    experiment_id=args.experiment_id,
                    role=spec.role,
                    pinned_at=pinned_at,
                    window=spec.window,
                    project=PROJECT,
                ),
                project=PROJECT,
            )
        return dit_bq.pin_source_into_experiment(
            spec.source_table,
            experiment_id=args.experiment_id,
            role=spec.role,
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
    return _PinnedSources(start=args.start, end=args.end, **pinned)


# --------------------------------------------------------------------------
# Per-binding run via git worktree
# --------------------------------------------------------------------------

def _ais_args_for_binding(
    extra_args: list[str],
    *,
    pinned: _PinnedSources,
    suffix: str,
    experiment_id: str,
    binding_name: str,
    modes: Sequence[str],
    steps: Sequence[str] = AIS_SELECTABLE_STEPS,
    worker_image: Optional[str] = None,
    image_tag: Optional[str] = None,
) -> list[str]:
    """Strip user-supplied overrides for fields the wrapper owns, then
    re-inject the wrapper's values.

    ``steps`` must be the selection ``pinned`` was built for: each source
    flag is emitted only when it was pinned.

    ``--image-tag`` (the submitter image) follows the same rule as
    ``--worker-image``: dropped from the extras and re-injected only when the
    wrapper has a per-binding value (``--binding-image-tag``).

    The pinned copies live in ``tech_great_expectations`` (not in a
    ``<stem>_internal`` / ``<stem>_published`` shape that
    ``--source-dataset-stem`` could address), so M4's per-table FQN flags
    (``--source-messages-fqn`` / ``--source-segment-info-fqn`` /
    ``--source-segs-activity-fqn``) are the only way to route ais.py at
    them. ``--start`` / ``--end`` come from the pin too: the windowed
    copies hold only those dates.

    ``--suffix`` controls table names; ``--experiment-id`` +
    ``--binding-name`` flow into Dataflow job names and BQ labels.
    ``--worker-image`` is dropped only when a wrapper-supplied value is
    present -- otherwise ais.py's default wins (no override, no drop).
    """
    # --thinned-message-table, --source-* and the dates are ALWAYS dropped
    # in cross-version runs -- the wrapper exclusively owns these knobs (it
    # pinned the inputs and knows the copies' FQNs and window). A
    # user-supplied --source-messages-fqn (etc.) in extra_args could
    # otherwise leak an unpinned table into one binding, defeating the
    # cross-version pin.
    drop_kvs = {
        "--source-dataset-stem",
        "--source-messages-fqn",
        "--source-segment-info-fqn",
        "--source-segs-activity-fqn",
        "--start", "--end",
        "--suffix", "--experiment-id", "--binding-name",
        "--thinned-message-table",
        # --modes is the wrapper's own flag (it selects which modes to run
        # AND which to diff); a user extra must not desync the two halves.
        # --steps likewise also decides what is pinned and diffed.
        "--modes", "--steps",
    }
    if worker_image is not None:
        drop_kvs = drop_kvs | {"--worker-image"}
    if image_tag is not None:
        drop_kvs = drop_kvs | {"--image-tag"}
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
        out.append(arg)
    out.extend([
        # Per-table FQN overrides (M4 of canonical-dataset migration) point
        # ais.py at the pinned copies in tech_great_expectations. Each
        # binding reads the SAME copies so any output divergence is
        # attributable to pipeline code, not source-data drift. Each one is
        # emitted below only when a selected step reads it.
        "--start", pinned.start.isoformat(),
        "--end", pinned.end.isoformat(),
        "--steps", ",".join(steps),
        # Forward the mode selection so each binding RUNS only the modes we
        # will diff. Before ais.py grew --modes, a --modes subset narrowed the
        # diff set only -- every binding still ran all three, so the flag
        # saved nothing on the expensive half.
        "--modes", ",".join(modes),
        "--suffix", suffix,
        "--experiment-id", experiment_id,
        "--binding-name", binding_name,
        # No --allow-dirty-tree needed: --suffix makes ais.py record git state
        # as-is (the worktree is a committed ref) and skip the auto-snapshot path.
    ])
    if worker_image is not None:
        out.extend(["--worker-image", worker_image])
    if image_tag is not None:
        out.extend(["--image-tag", image_tag])
    pinned_flags = (
        ("--source-messages-fqn", pinned.messages_positions),
        ("--source-segment-info-fqn", pinned.segment_info),
        ("--source-segs-activity-fqn", pinned.segs_activity),
        ("--thinned-message-table", pinned.thinned),
    )
    out.extend(
        token for flag, fqn in pinned_flags if fqn is not None for token in (flag, fqn)
    )
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
    modes: Sequence[str],
    steps: Sequence[str],
    pipeline_dir: str,
    ais_extra_args: list[str],
    dry_run: bool,
    worker_image: Optional[str] = None,
    image_tag: Optional[str] = None,
) -> int:
    worktree_dir = tempfile.mkdtemp(prefix=f"dit-xv-{name}-")
    try:
        subprocess.run(
            ["git", "-C", pipeline_dir, "worktree", "add", "--force", worktree_dir, ref],
            check=True, capture_output=True, text=True,
        )
        logger.info("binding %s: worktree at %s @ %s", name, worktree_dir, ref)

        argv = _ais_args_for_binding(
            ais_extra_args,
            pinned=pinned, suffix=suffix,
            experiment_id=experiment_id, binding_name=name,
            modes=modes,
            steps=steps,
            worker_image=worker_image,
            image_tag=image_tag,
        )
        cmd = [sys.executable, str(AIS_WORKFLOW), *argv]
        logger.info("binding %s: invoking %s", name, " ".join(shlex.quote(c) for c in cmd))
        if worker_image is not None:
            logger.info("binding %s: --worker-image override -> %s", name, worker_image)
        if image_tag is not None:
            logger.info("binding %s: --image-tag override -> %s", name, image_tag)

        if dry_run:
            logger.info("binding %s: --dry-run set; skipping ais.py invocation", name)
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
# Pairwise diffs
# --------------------------------------------------------------------------

#: Output table prefix per step, as ais.py names them.
_OUTPUT_PREFIX_BY_STEP = {STEP_THIN: "port_events", STEP_VISITS: "port_visits"}


def _output_table(dest_dataset: str, suffix: str, mode: str, step: str) -> str:
    """A binding's output of ``step`` for ``mode``."""
    return f"{PROJECT}.{dest_dataset}.{_OUTPUT_PREFIX_BY_STEP[step]}_{suffix}_{mode}"


def _diff_pair(
    *, dest_dataset: str, a_suffix: str, b_suffix: str, mode: str, step: str,
) -> int:
    """Diff two bindings' outputs of ``step`` on that step's key."""
    return dit_compare.compare_tables(
        _output_table(dest_dataset, a_suffix, mode, step),
        _output_table(dest_dataset, b_suffix, mode, step),
        keys=list(COMPARE_KEYS_BY_STEP[step]),
        view_suffix="",
    )


_SKIPPED = -1  # sentinel rc for diff pairs we couldn't run


def _run_diffs(
    *,
    modes: list[str],
    step: str,
    suffix_by_binding: dict[str, str],
    dest_dataset: str,
    failed_bindings: set[str],
) -> dict[tuple[str, str, str], int]:
    """Diff every pair of bindings' ``step`` outputs, per mode."""
    results: dict[tuple[str, str, str], int] = {}
    bindings = list(suffix_by_binding.keys())
    for mode in modes:
        for a, b in itertools.combinations(bindings, 2):
            if a in failed_bindings or b in failed_bindings:
                results[(mode, a, b)] = _SKIPPED
                failed_side = a if a in failed_bindings else b
                logger.info("diff mode=%s %s vs %s -> SKIPPED (binding %s failed)",
                            mode, a, b, failed_side)
                continue
            rc = _diff_pair(
                dest_dataset=dest_dataset,
                a_suffix=suffix_by_binding[a],
                b_suffix=suffix_by_binding[b],
                mode=mode,
                step=step,
            )
            results[(mode, a, b)] = rc
            verdict = "IDENTICAL" if rc == 0 else f"DIFFERENT (table-check rc={rc})"
            logger.info("diff mode=%s %s vs %s -> %s", mode, a, b, verdict)
    return results


def _summarize(results: dict[tuple[str, str, str], int]) -> str:
    lines = ["", "Cross-version diff summary:"]
    for (mode, a, b), rc in results.items():
        if rc == _SKIPPED:
            verdict = "SKIPPED"
        elif rc == 0:
            verdict = "IDENTICAL"
        else:
            verdict = "DIFFERENT"
        lines.append(f"  mode={mode}  {a} vs {b}  -> {verdict}  (rc={rc})")
    return "\n".join(lines)


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args, ais_extra_args = parse_args(argv)

    reuse = args.reuse_pin is not None
    pinned_at = (
        dit_bq.parse_pin_stamp(args.reuse_pin) if reuse
        else datetime.now(timezone.utc).replace(microsecond=0)
    )
    logger.info("experiment_id: %s", args.experiment_id)
    logger.info("pin stamp: %s (%s)", dit_bq.format_pin_stamp(pinned_at),
                "reusing an earlier run's copies" if reuse else "new copies")
    logger.info("date range (inclusive): %s -> %s", args.start, args.end)
    logger.info("bindings: %s", args.bindings)
    logger.info("modes: %s", args.modes)
    logger.info("steps: %s", ",".join(args.steps))
    logger.info("pipeline_dir: %s", args.pipeline_dir)
    logger.info("pinning: %s (copies in %s.tech_great_expectations)",
                ", ".join(spec.source_table for spec in _pin_specs(args)), PROJECT)

    _verify_refs(args.pipeline_dir, args.bindings)
    pinned = _pin_sources(args, pinned_at, reuse=reuse)
    if not reuse and not args.dry_run:
        logger.info("to rerun on these copies, pass --reuse-pin %s",
                    dit_bq.format_pin_stamp(pinned_at))

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
            modes=args.modes,
            steps=args.steps,
            pipeline_dir=args.pipeline_dir,
            ais_extra_args=ais_extra_args,
            dry_run=args.dry_run,
            worker_image=args.binding_worker_images.get(name),
            image_tag=args.binding_image_tags.get(name),
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

    results = _run_diffs(
        modes=args.modes,
        step=args.steps[-1],
        suffix_by_binding=suffix_by_binding,
        dest_dataset=args.dest_dataset,
        failed_bindings=failed_bindings,
    )
    print(_summarize(results), file=sys.stderr)
    return 1 if failed_bindings else 0


if __name__ == "__main__":
    sys.exit(main())
