"""Cross-version experiment for pipe-events fishing events.

Pins the source data via BQ snapshots at a fixed timestamp, runs
``workflows/pipe_events/fishing.py`` once per pipeline-version binding (with
each binding pointed at the snapshotted inputs), and then diffs corresponding
output views pairwise across bindings on ``event_id``. Diff results are
reported but do not fail the run -- the *point* of cross-version testing is
to surface behaviour change, so a non-empty diff is information, not error.

Example: validate the ``refactor/fishing_events_incremental_stage_two``
branch by comparing it against ``main``::

    dit run workflows/pipe_events/cross_version_fishing.py \\
        --experiment-id refactor-stage-two \\
        --pin-source-at 2026-08-12T09:00:00Z \\
        --binding main=main \\
        --binding refactor=refactor/fishing_events_incremental_stage_two \\
        --modes 1_bf \\
        --parallel

Steps:

1. Verify every binding's git ref exists in ``$PROJECTS/pipe-events``.
2. ``dit.bq.snapshot_into_experiment`` the SEVEN moving source tables
   (``research_messages``, ``segs_activity``, ``segment_vessel``,
   ``product_vessel_info_summary``, ``identity_core``,
   ``identity_authorization``, ``event_regions``) from their configured
   locations at ``--pin-source-at`` into the canonical
   ``<project>.tech_great_expectations`` dataset as
   ``dit_exp_<sanitised(exp)>_cross_version_<source_basename>`` tables with
   per-table ``expiration_timestamp`` (default 7-day TTL).

   The eighth ``fishing.py`` source, ``spatial_measures_20201105``, is
   deliberately NOT snapshotted. Its ``_YYYYMMDD`` filename suffix is a
   content-addressable version literal -- snapshotting a version-pinned
   static table buys nothing (7-day TTL cleanup on a permanently unchanging
   table, gratuitous storage, and it obscures the "this file lives forever
   at this address" contract). All bindings read the shared default for
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
   ``--source-event-regions-fqn``) pointing at the canonical snapshots, plus
   a binding-scoped ``--suffix``. Each subprocess's stdout/stderr is
   line-prefixed ``[<binding>] `` so parallel runs interleave readably.

4. For each mode in ``--modes`` and each pair of bindings, compare the
   corresponding ``<suffix>_<mode>_fishing_events`` view AND the
   ``<suffix>_<mode>_product_events_fishing`` view on ``event_id``. Pairs
   touching a binding that failed (rc != 0) are SKIPPED, not diffed.

The overall exit code is non-zero iff any binding failed; an individual
binding failure does not abort siblings.

``--dry-run`` skips the ``fishing.py`` invocations and the diff phase but
still performs snapshotting and worktree setup/teardown -- useful for
validating the orchestration without burning BQ cost.

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
from datetime import datetime, timezone
from pathlib import Path

from dit import bq as dit_bq
from dit import compare as dit_compare
from dit.workflow import parse_modes

# fishing.py owns the canonical mode set + dataset defaults; import so the
# two can't drift.
from workflows.pipe_events.fishing import (
    DEFAULT_INTERNAL_DS,
    DEFAULT_PIPE_REGIONS_LAYERS,
    DEFAULT_PIPE_STATIC,
    DEFAULT_PUBLISHED_DS,
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
DEFAULT_SNAPSHOT_EXPIRATION_DAYS = 7

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
                   help="Slug for the experiment; appears in snapshot dataset name and "
                        "output-table suffixes. Sanitisation: hyphens become underscores in "
                        "snapshot dest table names.")
    p.add_argument("--pin-source-at", required=True,
                   help="ISO 8601 timestamp for the source-data snapshot "
                        "(e.g. 2026-08-12T10:00:00Z).")
    p.add_argument("--binding", action="append", required=True, dest="bindings",
                   help="`name=ref` pair, repeatable. Both must be valid git refs in "
                        "--pipeline-dir. Name must match [a-z0-9][a-z0-9_-]{0,31} because it "
                        "flows into a BQ table suffix.")
    p.add_argument("--modes", default=DEFAULT_MODES,
                   help=f"Comma-separated mode names whose output views get diffed pairwise. "
                        f"Default {DEFAULT_MODES}.")
    p.add_argument("--pipeline-dir", default=DEFAULT_PIPELINE_DIR,
                   help="pipe-events checkout used for git worktrees.")
    # Source dataset knobs -- the wrapper's own copies of fishing.py's
    # dataset defaults. Threaded into _snapshot_source so the SOURCE side
    # of the snapshot points at whichever cohort the user selected.
    p.add_argument("--internal-ds", default=DEFAULT_INTERNAL_DS,
                   help="Source internal AIS dataset (research_messages, segment_vessel).")
    p.add_argument("--published-ds", default=DEFAULT_PUBLISHED_DS,
                   help="Source published AIS dataset (segs_activity, identity_*, vessel info).")
    p.add_argument("--pipe-static", default=DEFAULT_PIPE_STATIC,
                   help="Source pipe_static dataset. NOTE: spatial_measures_20201105 is NOT "
                        "snapshotted (content-addressable version literal); this knob is "
                        "accepted for parity with fishing.py but has no effect on the snapshot "
                        "phase. Every binding reads fishing.py's default spatial_measures FQN.")
    p.add_argument("--pipe-regions-layers", default=DEFAULT_PIPE_REGIONS_LAYERS,
                   help="Source pipe_regions_layers dataset (event_regions).")
    p.add_argument("--dest-dataset", default=DEFAULT_DEST_DATASET,
                   help="BQ dataset holding the output tables produced by each binding's "
                        "fishing.py run.")
    p.add_argument("--snapshot-expiration-days", type=int,
                   default=DEFAULT_SNAPSHOT_EXPIRATION_DAYS,
                   help="default_table_expiration for the created snapshot tables, in days. "
                        "Bump for pipe3-scale invocations where a single sequential run could "
                        "outlive the default 7-day window.")
    p.add_argument("--dry-run", action="store_true",
                   help="Skip fishing.py invocations and pairwise diffs; still creates "
                        "snapshots and worktrees.")
    p.add_argument("--sequential-bindings", action="store_true",
                   help="Run bindings serially instead of the default (parallel). Useful "
                        "when debugging a single binding's logs without interleave.")
    args, fishing_extra_args = p.parse_known_args(argv)
    args.bindings = [_parse_binding(b) for b in args.bindings]
    args.modes = parse_modes(args.modes, choices=FISHING_MODES)
    args.pin_source_at = _parse_iso8601(args.pin_source_at)
    # Validate binding names against the BQ-safe subset -- they flow into
    # output-table suffixes as ``{experiment_id}-{binding_name}``. A typo
    # like ``--binding refactor/stage_two=...`` would otherwise produce an
    # invalid BQ table name only at snapshot/write time, deep into the run.
    _validate_binding_names(args.bindings)
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


def _parse_iso8601(s: str) -> datetime:
    # Accept both "...Z" and explicit offsets.
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s[:-1]).replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(s)
    except ValueError as exc:
        raise SystemExit(f"--pin-source-at: invalid ISO 8601 timestamp {s!r}: {exc}") from exc


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
# Snapshot tables (canonical-dataset shape)
# --------------------------------------------------------------------------

# Role used by dit.bq.snapshot_into_experiment.
#
# The role is workflow-specific -- NOT the bare "cross_version" that
# port_visits/cross_version_ais.py uses -- because a shared basename can
# collide across workflows for the same --experiment-id. Concrete case:
# both this workflow and cross_version_ais.py snapshot a `segs_activity`
# table, so with role="cross_version" both dest FQNs resolve to
# `...tech_great_expectations.dit_exp_<exp>_cross_version_segs_activity`.
# Because snapshot_into_experiment defaults to if_existing="skip", the
# second workflow silently adopts the first workflow's snapshot -- pinned
# at a different --pin-source-at and possibly derived from a different
# dataset. snapshot_into_experiment's own docstring puts the duty here:
# "Caller is responsible for keeping roles disjoint per workflow so
# concurrent experiments don't collide on a table name." The precedent
# is cross_version_ais.py's own second role (_THINNED_SNAPSHOT_ROLE) --
# same trap, same fix.
_SOURCE_SNAPSHOT_ROLE = "cross_version_fishing"


@dataclasses.dataclass(frozen=True)
class _CrossVersionSnapshotFQNs:
    """The seven canonical snapshot FQNs that ``_snapshot_source`` produces,
    threaded through ``_run_binding`` -> ``_fishing_args_for_binding`` so
    each binding's ``fishing.py`` invocation reads from the canonical-dataset
    snapshots instead of from the live source datasets.

    Seven fields, NOT eight: ``spatial_measures_20201105`` is content-
    addressable by its ``_YYYYMMDD`` filename suffix and is not snapshotted
    (see module docstring). All bindings share fishing.py's default for
    that table.

    Frozen so callers can't accidentally mutate the FQNs between snapshot
    creation and ``fishing.py`` invocation.
    """
    research_messages: str
    segs_activity: str
    segment_vessel: str
    product_vessel_info_summary: str
    identity_core: str
    identity_authorization: str
    event_regions: str


def _snapshot_source(args: argparse.Namespace) -> _CrossVersionSnapshotFQNs:
    """Snapshot the seven moving pipe-events source tables at
    ``--pin-source-at`` into the canonical ``tech_great_expectations``
    dataset via ``dit.bq.snapshot_into_experiment``. Returns the dest FQNs
    as a frozen dataclass for threading downstream.

    ``spatial_measures_20201105`` is deliberately NOT snapshotted; see the
    module docstring for the content-addressable rationale.

    KNOWN FOOTGUN inherited from ``snapshot_into_experiment``:
    ``if_existing="skip"`` (the helper's default) makes per-source-table
    re-runs idempotent BUT silently keeps the prior snapshot if the same
    ``--experiment-id`` is re-run with a different ``--pin-source-at``.
    ``--experiment-id`` is REQUIRED in this workflow (no auto-generated
    default) -- use a fresh experiment id per distinct pin timestamp; the
    7-day TTL on each snapshot table cleans up afterwards.
    """
    # Use dataset knobs verbatim -- they already carry the project. Splitting
    # ``project.dataset`` on ``.`` and re-prefixing PROJECT silently discards
    # a user-supplied project (e.g. ``--internal-ds gfw-int-vms-v3.some_internal``
    # snapshotting from world-fishing-827 instead), which best-case produces
    # a mid-run 404 and worst-case snapshots the wrong data from a same-named
    # dataset in the default project. Match fishing.py's ``_run_slice``
    # (``f"{args.internal_ds}.research_messages"``, etc.) so "the wrapper
    # snapshots exactly what fishing.py would have read" holds by
    # construction, not by coincidence with the defaults. Cross-org sources
    # are already real (encounters/vms.py targets gfw-int-vms-v3) and
    # snapshot_into_experiment carries a ``project`` param for cross-org
    # writes.
    #
    # (This deliberately diverges from cross_version_ais.py, which DOES need
    # ``PROJECT`` re-prefixing because its ``--source-dataset-stem`` is a
    # bare stem with no project.)
    src_research_messages = f"{args.internal_ds}.research_messages"
    src_segs_activity = f"{args.published_ds}.segs_activity"
    src_segment_vessel = f"{args.internal_ds}.segment_vessel"
    src_pvis = f"{args.published_ds}.product_vessel_info_summary"
    src_identity_core = f"{args.published_ds}.identity_core"
    src_identity_auth = f"{args.published_ds}.identity_authorization"
    src_event_regions = f"{args.pipe_regions_layers}.event_regions"

    def _snap(source: str) -> str:
        return dit_bq.snapshot_into_experiment(
            source,
            experiment_id=args.experiment_id,
            role=_SOURCE_SNAPSHOT_ROLE,
            as_of=args.pin_source_at,
            expiration_days=args.snapshot_expiration_days,
            project=PROJECT,
        )

    return _CrossVersionSnapshotFQNs(
        research_messages=_snap(src_research_messages),
        segs_activity=_snap(src_segs_activity),
        segment_vessel=_snap(src_segment_vessel),
        product_vessel_info_summary=_snap(src_pvis),
        identity_core=_snap(src_identity_core),
        identity_authorization=_snap(src_identity_auth),
        event_regions=_snap(src_event_regions),
    )


# --------------------------------------------------------------------------
# Per-binding run via git worktree
# --------------------------------------------------------------------------

def _fishing_args_for_binding(
    extra_args: list[str],
    *,
    snapshot_fqns: _CrossVersionSnapshotFQNs,
    suffix: str,
    experiment_id: str,
    dest_dataset: str,
    modes: Sequence[str],
) -> list[str]:
    """Strip user-supplied overrides for fields the wrapper owns, then
    re-inject the wrapper's values.

    Emits E4's per-table FQN flags (added by
    ``feat(pipe-events): per-table source-FQN flags on fishing.py``) so
    each binding reads from the canonical-dataset snapshots. The snapshots
    live in ``tech_great_expectations`` (not in a ``<stem>_internal`` /
    ``<stem>_published`` shape that ``--internal-ds`` / ``--published-ds``
    could address), so per-table FQNs are the only way to route fishing.py
    at them.

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
    # exclusively owns them (it snapshotted the inputs and knows the
    # canonical dest FQNs). A user-supplied --source-*-fqn (etc.) in extras
    # could otherwise leak an unpinned table into one binding, defeating
    # the cross-version pin.
    #
    # Split into value-taking flags (drop_kvs -- swallow the arg AND the
    # next token) vs store_true flags (drop_bare -- swallow only the arg
    # itself; conflating the two eats the following unrelated flag).
    drop_kvs = {
        # Dataset knobs: can only address <stem>_internal / <stem>_published
        # / pipe_static / pipe_regions_layers shapes -- can't reach
        # tech_great_expectations snapshots. Would produce a misleading
        # startup log even if their values were ignored downstream.
        "--internal-ds", "--published-ds", "--pipe-static", "--pipe-regions-layers",
        # All 8 per-table FQN flags. Even --source-spatial-measures-fqn is
        # dropped: the wrapper does not snapshot spatial_measures, but if a
        # user-extra pointed one binding at a different spatial_measures
        # version the cross-version comparison would silently diverge on
        # region membership. Force every binding to fishing.py's default.
        "--source-research-messages-fqn", "--source-segs-activity-fqn",
        "--source-segment-vessel-fqn", "--source-product-vessel-info-summary-fqn",
        "--source-identity-core-fqn", "--source-identity-authorization-fqn",
        "--source-spatial-measures-fqn", "--source-event-regions-fqn",
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
        # Per-table FQN overrides route fishing.py at the M1-helper-produced
        # snapshots in tech_great_expectations. Each binding reads from the
        # SAME snapshots so any output divergence is attributable to
        # pipeline code, not source-data drift.
        "--source-research-messages-fqn", snapshot_fqns.research_messages,
        "--source-segs-activity-fqn", snapshot_fqns.segs_activity,
        "--source-segment-vessel-fqn", snapshot_fqns.segment_vessel,
        "--source-product-vessel-info-summary-fqn", snapshot_fqns.product_vessel_info_summary,
        "--source-identity-core-fqn", snapshot_fqns.identity_core,
        "--source-identity-authorization-fqn", snapshot_fqns.identity_authorization,
        "--source-event-regions-fqn", snapshot_fqns.event_regions,
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
    snapshot_fqns: _CrossVersionSnapshotFQNs,
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
            snapshot_fqns=snapshot_fqns, suffix=suffix,
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
    logger.info("pin_source_at: %s", args.pin_source_at.isoformat())
    logger.info("bindings: %s", args.bindings)
    logger.info("modes: %s", args.modes)
    logger.info("pipeline_dir: %s", args.pipeline_dir)
    logger.info(
        "source: internal=%s  published=%s  regions=%s "
        "(canonical dest: %s.tech_great_expectations)",
        args.internal_ds, args.published_ds, args.pipe_regions_layers, PROJECT,
    )
    logger.info("spatial_measures NOT snapshotted (content-addressable version literal); "
                "every binding reads fishing.py's default")

    _verify_refs(args.pipeline_dir, args.bindings)
    snapshot_fqns = _snapshot_source(args)

    # Each binding gets a deterministic suffix the diff step can address.
    suffix_by_binding = {
        name: f"{args.experiment_id}-{name}" for name, _ in args.bindings
    }

    def _invoke(name: str, ref: str) -> tuple[str, int]:
        rc = _run_binding(
            name=name, ref=ref,
            experiment_id=args.experiment_id,
            snapshot_fqns=snapshot_fqns,
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
