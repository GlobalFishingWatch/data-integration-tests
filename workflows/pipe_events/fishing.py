"""Mode-equivalence integration test for pipe-events fishing events.

Ports the three pipe-events bash integration scripts
(``pipe-events/integration_tests/staging-bf_bfd_bftruncate_async.sh`` /
``pipe-events/integration_tests/pipe3-bf_bfd_bftruncate.sh`` /
``pipe-events/integration_tests/pipe3-bf_bfd_bftruncate_async.sh``) into a
single dit workflow, and adds the cross-mode comparison the bash never had:
the bash wrote three prefixed outputs for manual inspection; this workflow
asserts they are identical (any divergence is a test failure).

The three bash variants differ only in **CLI-overridable defaults** (date
window, tail days, source AIS datasets); the mode shape, 4-step docker
chain, and comparison contract are identical. So they collapse into ONE
workflow file with overridable CLI defaults. **Defaults follow the staging
script** (2020 calendar year against the AIS test cohort) -- the right thing
for routine validation; the production-scale variants are reachable by CLI
override:

* ``pipe3`` sync (full prod cohort, 1 year of 2012):
  ``--start 2012-01-01 --end 2013-01-01
    --internal-ds world-fishing-827.pipe_ais_v3_internal
    --published-ds world-fishing-827.pipe_ais_v3_published``

* ``pipe3`` async (full prod cohort, 2012 -> 2025-05-04, 15-day tail):
  add ``--end 2025-05-04 --tail-days 15`` to the override above.

The staging script is the **default first run** per pipe-events' own
``CLAUDE.md`` ("Always run staging first; only run pipe3 for final
validation"). Production-scale runs are deliberately gated behind explicit
flags so you don't fire one off by accident.

pipe-events is dit's **third consumer** and the first non-Beam one. It is a
**BQ-SQL pipeline run via docker** (NOT Beam/Dataflow): the container
orchestrates BigQuery jobs, using ``_SESSION.*`` temp tables for isolation
(so ``--parallel`` across modes is safe). Consequences vs the two Beam
consumers:

* No Dataflow, no Beam, no ``worker_image_digest``, no submitter-vs-worker
  split. The pipeline code identity is ``pipeline_commit`` + the container
  image.
* **No run cache** (deferred — the cache key's ``worker_image_digest`` is
  Dataflow-shaped; settle the docker-runner cache-key shape separately).
  ``resolve_run_context`` still runs for provenance (it records
  ``pipeline_commit`` / ``unreviewed``).
* Authenticates via a docker **named volume** ``gcp`` mounted at
  ``/root/.config`` (created out-of-band: ``docker volume create gcp`` +
  ``gcloud auth application-default login`` -- the container reads ADC from
  ``/root/.config/gcloud/application_default_credentials.json``, which only
  ``application-default login`` populates; plain ``auth login`` won't). Threaded
  via the runner's ``volumes`` param.

**Image resolution (symmetric with Beam consumers).** Default ``--image-tag``
is the canonical published pipe-events image
(``us-central1-docker.pkg.dev/gfw-int-infrastructure/publication/...:vX.Y.Z``,
matching what composer-dags-production pins in prod; readable but not
writable by dit per the absolute prod-infra boundary). For *reviewed* code at
the default version the docker runner pulls that canonical image directly.
For *unreviewed* code (snapshot / dirty / unmerged) the harness auto-builds
a content-addressable ``gcr.io/world-fishing-827/dit/pipe-events:dit-<commit>``
via the same M-pivot-4 kaniko machinery the Beam workflows use, and stamps
the FQN onto ``args.image_tag``. An explicit ``--image-tag`` override is
always respected. ``--build-from-source`` opts out entirely: the runner
ignores ``args.image_tag`` and the compose ``pipeline`` service builds the
image from the mounted working tree (laptop's natural inner-loop pattern).

Each mode drives the same **4-step docker chain** per date slice
(``docker compose run --entrypoint pipe pipeline <op> <args>``):

  1. ``incremental_events`` (x2 score fields: nnet_score, night_loitering)
     -> ``{prefix}_<sfield>_merged``
  2. ``incremental_filter_events`` (x2) -> ``{prefix}_<sfield>_filtered``
  3. ``auth_and_regions_fishing_events`` ->
     ``{prefix}_fishing_events_v{YYYYMMDD}`` (versioned table; date = the
     slice's exclusive end with hyphens stripped) + ``{prefix}_fishing_events``
     (view to the latest)
  4. ``fishing_restrictive`` -> ``{prefix}_product_events_fishing_v{YYYYMMDD}``
     + ``{prefix}_product_events_fishing`` (view)

Modes (mirroring the bash):

* ``1_bf``: one full backfill over ``[start, end)`` (a single window). Oracle.
* ``2_bfd``: backfill to ``tail_days`` short of ``end``, then a daily loop of
  1-day slices ``[d, d+1)`` over the last ``tail_days`` days. Steady-state
  daily reprocessing.
* ``3_bftruncate``: a full backfill over ``[start, end)``, then the same daily
  loop — exercises the truncate-and-remerge path on already-processed days.

Date semantics: ``--start`` / ``--end`` are **half-open** ``[start, end)`` —
matching the bash, which passes ``-end $end_d`` as an exclusive bound to the
incremental query and uses ``end_d = current_day + 1`` for each daily slice.
(The bash's ``start_year=N`` / ``end_year_plus_one-01-01`` shape maps to
``start=N-01-01``, ``end=(N+1)-01-01`` — e.g. staging's 2020 becomes
``start=2020-01-01``, ``end=2021-01-01``.)

Comparison (the value-add): the fishing-events schema is keyed by
**``event_id``** with NO SCD-2 columns (no ``valid_from``/``valid_to``/
``is_current``) and no ``_last_versions`` view — versioning is table-level
(date-suffixed ``_v{date}`` + a view to the latest). So this is the
**truncate shape**: ``compare_tables(a, b, keys=("event_id",),
view_suffix="")`` — like port-visits, NOT pipe-gaps. We compare the
``_fishing_events`` **view** (which abstracts the per-mode date suffix; each
mode's final slice has a different ``end`` -> different ``_v{date}`` table
name) and, as a second target, the ``_product_events_fishing`` (restrictive)
view.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import itertools
import logging
import os
import sys
import uuid
from datetime import date, datetime, timedelta
from typing import Optional, Sequence

from dit import compare as dit_compare
from dit import dates as dit_dates
from dit.runners import docker as dit_docker
from dit.workflow import (
    add_dataset_args,
    add_experiment_id_arg,
    resolve_run_context,
)

logger = logging.getLogger(__name__)

# Pipeline repo name; namespaces auto-snapshot refs
# (refs/dit-snapshots/<PIPELINE_NAME>/<sha>) and is recorded for provenance.
PIPELINE_NAME = "pipe-events"

#: Workflow identity (recorded for provenance / logging; no cache here).
WORKFLOW_NAME = "workflows/pipe_events/fishing.py"

# Mode labels. Single source of truth shared by execute_* and compare_all.
MODE_BF = "1_bf"
MODE_BFD = "2_bfd"
MODE_BFTRUNCATE = "3_bftruncate"
MODES = (MODE_BF, MODE_BFD, MODE_BFTRUNCATE)


# --------------------------------------------------------------------------
# Constants / defaults (from the generate script's defaults)
# --------------------------------------------------------------------------

PROJECT = "world-fishing-827"

# Staging cohort source datasets (generate_incremental_fishing_events.sh
# defaults): internal/published AIS test datasets + static + regions layers.
DEFAULT_INTERNAL_DS = f"{PROJECT}.pipe_ais_test_202408290000_internal"
DEFAULT_PUBLISHED_DS = f"{PROJECT}.pipe_ais_test_202408290000_published"
DEFAULT_PIPE_STATIC = f"{PROJECT}.pipe_static"
DEFAULT_PIPE_REGIONS_LAYERS = f"{PROJECT}.pipe_regions_layers"

# Defaults follow ``staging-bf_bfd_bftruncate_async.sh``: 2020 calendar year
# against the AIS test cohort (the generate-script's own defaults). Half-open
# window ``[start, end)``. pipe-events' own CLAUDE.md says: always run
# staging first (smaller cohort, cheaper); pipe3 is for final validation
# only. Override at the CLI for pipe3 (see module docstring above).
DEFAULT_START = "2020-01-01"   # inclusive
DEFAULT_END = "2021-01-01"     # exclusive
DEFAULT_TAIL_DAYS = 3

# Canonical published pipe-events image, pinned at the prod version that
# composer-dags-production currently runs (``Versions.PIPE_EVENTS = "v4.2.17"``
# in ``dags/core/ais/v3.py``). Read-only to dit by IAM per the absolute
# prod-infra boundary; the docker runner pulls from here for reviewed code.
# ``--build-from-source`` short-circuits this entirely (the runner builds the
# compose ``pipeline`` service from the mounted working tree instead).
DEFAULT_IMAGE_TAG = (
    "us-central1-docker.pkg.dev/gfw-int-infrastructure/publication/"
    "github-globalfishingwatch-pipe-events:v4.2.17"
)

# The compose service + entrypoint the bash uses:
#   docker compose run --entrypoint pipe pipeline <op> <args>
COMPOSE_SERVICE = "pipeline"
CLI_ENTRYPOINT = "pipe"

# GCP auth: shared named volume mounted into the container at /root/.config.
GCP_VOLUME = "gcp:/root/.config"

# Score fields the incremental + filter steps each run twice over.
SCORE_FIELDS = ("nnet_score", "night_loitering")

# Comparison contract for fishing events (truncate shape, keyed by event_id;
# NOT SCD-2 — the schema has no valid_from/valid_to/is_current).
COMPARE_KEYS = ("event_id",)
COMPARE_VIEW_SUFFIX = ""

# Labels stamped on every table the chain writes (mirrors the bash's -labels).
_LABELS_JSON = (
    '{"environment": "integration_test", "resource_creator": "dit", '
    '"project": "core_pipeline", "version": "v3", "step": "fishing_events", '
    '"stage": "testing"}'
)


# --------------------------------------------------------------------------
# Date helpers
# --------------------------------------------------------------------------

def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _date_shard(d: date) -> str:
    """The version-table date suffix: ISO date with hyphens stripped.

    Mirrors pipe-events' ``parse.py`` (``reference_date.replace('-', '')``)
    and the bash's ``end_d_shard=${end_d//-/}``.
    """
    return d.isoformat().replace("-", "")


# --------------------------------------------------------------------------
# Table / prefix names
# --------------------------------------------------------------------------

def _mode_prefix(suffix: str, mode: str) -> str:
    """Per-mode pipeline prefix (the bash's ``${pipeline_prefix}_<mode>``)."""
    return f"{suffix}_{mode}"


def _dest_ds(args: argparse.Namespace) -> str:
    return f"{PROJECT}.{args.dest_dataset}"


def _fishing_events_view(args: argparse.Namespace, suffix: str, mode: str) -> str:
    """The ``_fishing_events`` view FQN (latest versioned auth output)."""
    return f"{_dest_ds(args)}.{_mode_prefix(suffix, mode)}_fishing_events"


def _product_events_view(args: argparse.Namespace, suffix: str, mode: str) -> str:
    """The ``_product_events_fishing`` view FQN (latest restrictive output)."""
    return f"{_dest_ds(args)}.{_mode_prefix(suffix, mode)}_product_events_fishing"


# --------------------------------------------------------------------------
# The 4-step docker chain (one slice)
# --------------------------------------------------------------------------

def _run_step(args: argparse.Namespace, step_args: list[str], *, label: str) -> None:
    """Run one ``pipe`` operation in the container; raise on non-zero exit."""
    rc = dit_docker.run(
        args.image_tag,
        step_args,
        entrypoint=CLI_ENTRYPOINT,
        volumes=[GCP_VOLUME],
        service=COMPOSE_SERVICE,
        build_from_source=args.build_from_source,
    )
    if rc != 0:
        raise SystemExit(f"pipe-events step failed (rc={rc}, {label})")


def _run_slice(
    args: argparse.Namespace,
    *,
    mode: str,
    slice_start: date,
    slice_end: date,
    suffix: str,
    iteration: int,
    total_iterations: int,
) -> None:
    """One date slice = the full 4-step incremental fishing-events chain.

    Replicates ``scripts/generate_incremental_fishing_events.sh`` for one
    ``[slice_start, slice_end)`` window (``slice_end`` is exclusive — the
    bash passes it as ``-end $end_d`` and ``-rdate $end_d``).

    ``iteration`` / ``total_iterations`` are 1-indexed within the mode (logged
    for per-slice provenance, like the Beam consumers' job-name suffixes; the
    docker runner has no Dataflow job to label).
    """
    prefix = _mode_prefix(suffix, mode)
    dest_ds = _dest_ds(args)
    end_shard = _date_shard(slice_end)
    common = ["--project", args.pipeline_project, "--labels", args.labels]
    logger.info(
        "pipe-events chain mode=%s slice=[%s, %s) iter=%d/%d prefix=%s",
        mode, slice_start, slice_end, iteration, total_iterations, prefix,
    )

    # 1. Merged (incremental_events) — once per score field.
    for sfield in SCORE_FIELDS:
        _run_step(
            args,
            [
                *common,
                "--table_description", f"Incremental fishing events based on {sfield}",
                "incremental_events",
                "-start", slice_start.isoformat(),
                "-end", slice_end.isoformat(),
                "-messages", f"{args.internal_ds}.research_messages",
                "-sfield", sfield,
                "-dest", dest_ds,
                "-dest_tbl_prefix", f"{prefix}_{sfield}",
            ],
            label=f"incremental_events sfield={sfield} mode={mode}",
        )

    # 2. Filtered (incremental_filter_events) — once per score field.
    for sfield in SCORE_FIELDS:
        _run_step(
            args,
            [
                *common,
                "--table_description", f"Filtered fishing events based on {sfield}",
                "incremental_filter_events",
                "-sfield", sfield,
                "-segsact", f"{args.published_ds}.segs_activity",
                "-segvessel", f"{args.internal_ds}.segment_vessel",
                "-pvesselinfo", f"{args.published_ds}.product_vessel_info_summary",
                "-mtbl", f"{dest_ds}.{prefix}_{sfield}_merged",
                "-dest", dest_ds,
                "-dest_tbl_prefix", f"{prefix}_{sfield}",
            ],
            label=f"incremental_filter_events sfield={sfield} mode={mode}",
        )

    # 3. Authorizations (auth_and_regions_fishing_events).
    _run_step(
        args,
        [
            *common,
            "--table_description", "Fishing events with authorizations",
            "auth_and_regions_fishing_events",
            "-source_fishing", f"{dest_ds}.{prefix}_nnet_score_filtered",
            "-source_nl", f"{dest_ds}.{prefix}_night_loitering_filtered",
            "-idcore", f"{args.published_ds}.identity_core",
            "-idauth", f"{args.published_ds}.identity_authorization",
            "-measures", f"{args.pipe_static}.spatial_measures_20201105",
            "-regions", f"{args.pipe_regions_layers}.event_regions",
            "-allvessels", f"{args.published_ds}.product_vessel_info_summary",
            "-dest", f"{dest_ds}.{prefix}_fishing_events_v",
            "-dest_view", f"{dest_ds}.{prefix}_fishing_events",
            "-rdate", slice_end.isoformat(),
        ],
        label=f"auth_and_regions_fishing_events mode={mode} shard={end_shard}",
    )

    # 4. Restrictive (fishing_restrictive).
    _run_step(
        args,
        [
            *common,
            "--table_description", "Restrictive fishing events used in products",
            "fishing_restrictive",
            "-source_events", f"{dest_ds}.{prefix}_fishing_events_v",
            "-destrest", f"{dest_ds}.{prefix}_product_events_fishing_v",
            "-destrestview", f"{dest_ds}.{prefix}_product_events_fishing",
            "-rdate", slice_end.isoformat(),
        ],
        label=f"fishing_restrictive mode={mode} shard={end_shard}",
    )


# --------------------------------------------------------------------------
# Modes (date-slice arithmetic mirroring the staging / pipe3 bash scripts —
# identical across all three variants; only CLI-overridable defaults differ)
# --------------------------------------------------------------------------

def _daily_slices(end: date, tail_days: int) -> list[date]:
    """The exclusive ends of the daily tail slices.

    The bash loops ``current_day`` over the last ``tail_days`` days and runs a
    1-day slice ``[current_day, current_day + 1)`` for each. ``end`` is the
    exclusive overall end, so the last processed day is ``end - 1`` and the
    daily slice ends are ``[end - tail_days + 1, ..., end]`` (each used as the
    exclusive ``slice_end``). Returns those exclusive ends.
    """
    # daterange_inclusive is half-open; this yields end - tail_days + 1 .. end.
    return list(
        dit_dates.daterange_inclusive(
            end - timedelta(days=tail_days - 1), end + timedelta(days=1)
        )
    )


def execute_bf(args: argparse.Namespace, suffix: str) -> None:
    """One full backfill over [start, end)."""
    start = _parse_date(args.start)
    end = _parse_date(args.end)
    _run_slice(args, mode=MODE_BF, slice_start=start, slice_end=end, suffix=suffix,
               iteration=1, total_iterations=1)


def execute_bfd(args: argparse.Namespace, suffix: str) -> None:
    """Backfill to tail_days short of end, then a daily loop over the tail."""
    start = _parse_date(args.start)
    end = _parse_date(args.end)
    initial_end = end - timedelta(days=args.tail_days)
    daily_ends = _daily_slices(end, args.tail_days)
    total = 1 + len(daily_ends)
    _run_slice(args, mode=MODE_BFD, slice_start=start, slice_end=initial_end, suffix=suffix,
               iteration=1, total_iterations=total)
    for i, day_end in enumerate(daily_ends, start=2):
        day_start = day_end - timedelta(days=1)
        _run_slice(args, mode=MODE_BFD, slice_start=day_start, slice_end=day_end, suffix=suffix,
                   iteration=i, total_iterations=total)


def execute_bftruncate(args: argparse.Namespace, suffix: str) -> None:
    """Full backfill over [start, end), then the same daily loop (truncate)."""
    start = _parse_date(args.start)
    end = _parse_date(args.end)
    daily_ends = _daily_slices(end, args.tail_days)
    total = 1 + len(daily_ends)
    _run_slice(args, mode=MODE_BFTRUNCATE, slice_start=start, slice_end=end, suffix=suffix,
               iteration=1, total_iterations=total)
    for i, day_end in enumerate(daily_ends, start=2):
        day_start = day_end - timedelta(days=1)
        _run_slice(args, mode=MODE_BFTRUNCATE, slice_start=day_start, slice_end=day_end,
                   suffix=suffix, iteration=i, total_iterations=total)


# --------------------------------------------------------------------------
# Comparisons (the value-add the bash never had)
# --------------------------------------------------------------------------

def compare_all(mode_fqns: dict[str, str], *, label: str) -> int:
    """Pairwise-compare the three modes on ``event_id`` (truncate shape).

    ``mode_fqns`` maps each mode to the FQN to compare. Returns 0 iff all
    pairwise comparisons are identical; non-zero on any divergence (a
    divergence IS a test failure — the modes must be equivalent).
    """
    overall = 0
    for a, b in itertools.combinations(MODES, 2):
        rc = dit_compare.compare_tables(
            mode_fqns[a],
            mode_fqns[b],
            keys=COMPARE_KEYS,
            view_suffix=COMPARE_VIEW_SUFFIX,
        )
        logger.info("compare [%s] %s vs %s -> rc=%s", label, a, b, rc)
        overall = overall or rc
    return overall


# --------------------------------------------------------------------------
# Suffix
# --------------------------------------------------------------------------

def _resolve_suffix(args: argparse.Namespace) -> str:
    """Output prefix: <experiment_id>_<commit>_<uuid>; --suffix overrides."""
    if args.suffix:
        return args.suffix
    return f"{args.experiment_id}_{args.commit_sha}_{uuid.uuid4().hex[:6]}"


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Mode-equivalence integration test for pipe-events fishing events.",
    )
    # pipe-events is BQ-SQL via docker; the only runner is docker.
    p.add_argument("--pipeline-project", default=PROJECT,
                   help="GCP project the BQ jobs run in (--project to the CLI).")
    p.add_argument("--internal-ds", default=DEFAULT_INTERNAL_DS,
                   help="Internal AIS dataset (research_messages, segment_vessel).")
    p.add_argument("--published-ds", default=DEFAULT_PUBLISHED_DS,
                   help="Published AIS dataset (segs_activity, identity_*, vessel info).")
    p.add_argument("--pipe-static", default=DEFAULT_PIPE_STATIC,
                   help="pipe_static dataset (spatial_measures_*).")
    p.add_argument("--pipe-regions-layers", default=DEFAULT_PIPE_REGIONS_LAYERS,
                   help="pipe_regions_layers dataset (event_regions).")
    p.add_argument("--start", default=DEFAULT_START,
                   help="Inclusive start date (half-open window [start, end)).")
    p.add_argument("--end", default=DEFAULT_END,
                   help="Exclusive end date (half-open window [start, end)).")
    p.add_argument("--tail-days", type=int, default=DEFAULT_TAIL_DAYS,
                   help="Number of tail days for bfd / bftruncate daily iteration.")
    p.add_argument("--image-tag", default=DEFAULT_IMAGE_TAG,
                   help="Docker image identifier (docker-compose service image).")
    p.add_argument("--labels", default=_LABELS_JSON,
                   help="JSON labels stamped on every output table.")
    p.add_argument("--build-from-source", action="store_true",
                   help="Build + run the local compose service instead of the published image.")
    p.add_argument("--suffix", default=None,
                   help="Output-table prefix; auto-generated from git HEAD when omitted.")
    p.add_argument("--require-clean", action="store_true",
                   help="Error on a dirty tree instead of auto-snapshotting "
                        "(for CI / strict-provenance callers).")
    p.add_argument("--skip-pipelines", action="store_true",
                   help="Skip the pipeline phase; only run comparisons.")
    p.add_argument("--skip-comparisons", action="store_true",
                   help="Skip the comparison phase; only run pipelines.")
    p.add_argument("--parallel", action="store_true",
                   help="Run the three modes in parallel threads "
                        "(_SESSION temp tables isolate concurrent runs).")
    add_experiment_id_arg(p)
    # Only the dataset/SA knobs — pipe-events runs no Dataflow (Phase 3 split).
    add_dataset_args(p)
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)

    if args.tail_days < 1:
        raise SystemExit(
            f"--tail-days must be >= 1; got {args.tail_days}. "
            "The bfd / bftruncate daily loop needs at least one tail day."
        )
    if _parse_date(args.end) <= _parse_date(args.start):
        raise SystemExit(
            f"--end ({args.end}) must be after --start ({args.start}); the "
            "window is half-open [start, end)."
        )

    repo_dir = os.getcwd()

    # Resolve the committed ref + provenance via the shared harness. No run
    # cache for pipe-events (no Dataflow worker image to digest), so
    # resolve_digest=False — skips the gcloud describe. ensure_pipeline_image
    # (called inside resolve_run_context) returns the canonical default tag
    # for reviewed code, an auto-built dit/pipe-events:dit-<commit> for
    # unreviewed code, or an explicit override unchanged. Same trigger as the
    # Beam consumers; we still record pipeline_commit / unreviewed for
    # provenance regardless.
    ctx = resolve_run_context(
        repo_dir=repo_dir,
        pipeline_name=PIPELINE_NAME,
        runner="docker",
        require_clean=args.require_clean,
        suffix=args.suffix or None,
        worker_image=args.image_tag,
        default_worker_image=DEFAULT_IMAGE_TAG,
        resolve_digest=False,
        build_from_source=args.build_from_source,
    )
    args.run_context = ctx
    args.commit_sha = ctx.pipeline_commit
    args.unreviewed = ctx.unreviewed
    args.run_id = ctx.run_id
    # Stamp the harness-resolved image (canonical / auto-built / override) onto
    # args.image_tag so _run_step's dit_docker.run pulls THAT image. Ignored
    # by the docker runner when build_from_source=True (compose builds the
    # pipeline service from the mounted working tree).
    args.image_tag = ctx.worker_image

    suffix = _resolve_suffix(args)

    logger.info("experiment_id: %s", args.experiment_id)
    logger.info("suffix (table prefix): %s", suffix)
    logger.info("run_id: %s  commit_sha: %s%s",
                args.run_id, args.commit_sha,
                " (UNREVIEWED)" if args.unreviewed else "")
    logger.info("internal_ds: %s  published_ds: %s", args.internal_ds, args.published_ds)
    logger.info("date window (half-open): [%s, %s), tail_days=%d",
                args.start, args.end, args.tail_days)

    if not args.skip_pipelines:
        mode_execs = [
            (MODE_BF, execute_bf),
            (MODE_BFD, execute_bfd),
            (MODE_BFTRUNCATE, execute_bftruncate),
        ]
        if args.parallel:
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(mode_execs)) as pool:
                futures = {
                    pool.submit(fn, args, suffix): mode for mode, fn in mode_execs
                }
                # Surface the first exception (if any) after all complete.
                for fut in concurrent.futures.as_completed(futures):
                    fut.result()
        else:
            for _mode, fn in mode_execs:
                fn(args, suffix)

    if args.skip_comparisons:
        return 0

    # Two comparison targets, both keyed by event_id on the latest-version
    # VIEW (the view abstracts each mode's differing _v{date} table suffix).
    fishing_fqns = {m: _fishing_events_view(args, suffix, m) for m in MODES}
    product_fqns = {m: _product_events_view(args, suffix, m) for m in MODES}
    rc_fishing = compare_all(fishing_fqns, label="fishing_events")
    rc_product = compare_all(product_fqns, label="product_events_fishing")
    return rc_fishing or rc_product


if __name__ == "__main__":
    sys.exit(main())
