"""Mode equivalence integration test for the pipe-gaps raw-gaps pipeline.

Drives the same pipeline several different ways ("modes") and asserts all
modes produce identical output on their latest-state views (``bq_out_gaps``;
the rows are in ``<view>_versioned``). Ported from
``pipe-gaps/tests/integration/mode_equivalence.py`` onto the ``dit.*``
library; runner / compare / dates / bq concerns now live there, and one
raw-gaps job is ``workflows/pipe_gaps/raw_gaps.py``.

Written for pipe-gaps v0.17.1 (``raw_gaps.DEFAULT_WORKER_IMAGE``), the
version production's AIS v5 DAG runs. Dates are half-open ``[--start,
--end)``, as pipe-gaps' own ``--start-date`` / ``--end-date``.

The daily modes rely on how a job finds the open gaps of the jobs before it:
it reads them from its own versioned table, which the earlier jobs of the
mode wrote (``raw_gaps`` module docstring, "Open gaps between jobs").
"""

from __future__ import annotations

import argparse
import importlib
import itertools
import logging
import os
import sys
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from typing import Any, Optional

from dit import bq as dit_bq
from dit import compare as dit_compare
from dit import workflow as dit_workflow
from dit.cache import CacheKey
from dit.dates import daterange_inclusive
from dit.workflow import (
    add_experiment_id_arg,
    add_infra_args,
    add_max_num_workers_arg,
    add_modes_arg,
    add_no_cache_arg,
    add_worker_sizing_args,
    log_production_sizing_hint,
    log_worker_caps,
    log_worker_sizings,
    parse_modes,
    resolve_run_context,
    resolve_worker_sizing,
    worker_cap,
    worker_sizing_from_args,
)

# One raw-gaps job (config, argv, in-process pipeline, runner dispatch) is
# shared with outage_recovery.py: workflows/pipe_gaps/raw_gaps.py, written for
# pipe-gaps v0.17.1.
from workflows.pipe_gaps import raw_gaps
from workflows.pipe_gaps.raw_gaps import (
    DEFAULT_IMAGE_TAG,
    DEFAULT_WORKER_IMAGE,
    DEFAULT_WORKER_SIZING,
    PIPELINE_NAME,
    PROJECT,
    STEP_WORKER_SIZING,
)
from workflows.pipe_gaps.raw_gaps import dit_run_labels as _dit_run_labels
from workflows.pipe_gaps.raw_gaps import job_name as _job_name
from workflows.pipe_gaps.raw_gaps import make_config as _make_config
from workflows.pipe_gaps.raw_gaps import resolve_suffix as _resolve_suffix
from workflows.pipe_gaps.raw_gaps import run_pipeline as _run_pipeline

# Compute once at import time; the dit-side cache buster. Covers raw_gaps.py
# too, where the job's config lives.
WORKFLOW_FILE_SHA1 = raw_gaps.workflow_sha1(__file__)

logger = logging.getLogger(__name__)


# Mode labels embedded in Dataflow job names + the bq_out_gaps view
# suffix. Single source of truth; execute_* helpers thread them through.
MODE_BF = "1_bf"
MODE_BFD = "2_bfd"
MODE_BFTRUNCATE = "3_bftruncate"
MODE_MUTATE_RECOVER = "4_mutate_recover"

#: Modes selectable via --modes. MODE_MUTATE_RECOVER is deliberately NOT
#: here: it keeps its own --enable-pipeline-4 gate (it needs the
#: restricted-ssvids machinery, which --modes knows nothing about). Two
#: overlapping gates for one mode would be worse than one explicit one;
#: parse_modes rejects it by name and the help text points at the flag.
SELECTABLE_MODES = (MODE_BF, MODE_BFD, MODE_BFTRUNCATE)

# Per-user infra knobs identical to both workflows (--dest-dataset,
# --service-account, --dataflow-*) live in dit.workflow; add_infra_args wires
# them onto the parser. --bq-temp-dataset is workflow-local and stays here.
DEFAULT_BQ_TEMP_DATASET = os.environ.get(
    "DIT_BQ_TEMP_DATASET", f"{PROJECT}.{dit_workflow.DEFAULT_DEST_DATASET}"
)

# Workflow-specific defaults (no env var; one-off overrides via CLI flag).
# Pipe-gaps reads two input tables. They happen to live in different halves of
# the AIS-staging cohort (messages in _internal, segs_activity in _published);
# rather than parameterise by dataset (which would need separate stems per
# half), each input is its own fully-qualified flag. Override one or both
# directly when running against a non-staging cohort.
DEFAULT_SOURCE_MESSAGES = (
    f"{PROJECT}.pipe_ais_test_202408290000_internal.messages_positions"
)
DEFAULT_SOURCE_SEGMENTS = (
    f"{PROJECT}.pipe_ais_test_202408290000_published.segs_activity"
)

# min_gap_length 1, where production passes 4: on the 142 vessels of the
# staging cohort a lower threshold gives more gaps and more boundary cases to
# compare (owner decision 2026-11-27; an accepted difference in the drift
# spec). --min-gap-length 4 runs production's threshold.
DEFAULT_MIN_GAP_LENGTH = 1.0
DEFAULT_N_HOURS_BEFORE = 12
# 2-day windows: production passes none, and pipe-gaps then takes the job's
# date range, which is 2 days on production's daily job. On a long DIT job
# the small windows give many window boundaries to process.
DEFAULT_WINDOW_PERIOD_D = 2
DEFAULT_FILTER_GOOD_SEG = True
DEFAULT_BACKFILL_DAYS_W = 4

DEFAULT_START = "2020-01-01"
DEFAULT_END = "2021-01-01"
DEFAULT_TAIL_DAYS = 4

RUNNERS = ("docker", "dataflow")

# Dataflow worker cap (--max-num-workers, PIPELINE-4701): production's daily
# raw-gaps task (core.ais.v5 gaps.detect_raw_gaps; composer b5c3496e1). Before
# the flag existed dit emitted no cap, so gfw-common's
# Pipeline.default_options() applied (max_num_workers=100, still so in
# gfw-common 0.24.2); the emitted cap overrides that default. Shared with
# outage_recovery.py.
DEFAULT_MAX_NUM_WORKERS = 50
MAX_NUM_WORKERS_HELP_DETAIL = (
    "The default matches production's daily raw-gaps task, and replaces "
    "gfw-common's own default of 100."
)

# Worker IPs. Every DIT Dataflow job runs with private worker IPs
# (dit.workflow (a3)). pipe-gaps already does without any option from dit:
# gfw-common's Pipeline.default_options() sets use_public_ips=False (0.24.2,
# which pipe-gaps v0.17.1 pins, as every earlier version), and Beam's
# PipelineOptions.from_dictionary renders that as --no_use_public_ips. dit
# passes nothing that overrides it, so it emits nothing here; a test pins
# that. Worker sizing mirrors production: raw_gaps.DEFAULT_WORKER_SIZING.

COMPARE_KEYS = ("gap_id", "start_timestamp")


def mode_windows(
    mode: str, *, start: date, end: date, tail_days: int, backfill_days_w: int,
) -> list[tuple[date, date, bool]]:
    """Every job of ``mode`` in order, as ``(start, end, restricted)``.

    Half-open windows, as pipe-gaps' ``--start-date`` / ``--end-date``. The
    daily jobs end on each of the last ``tail_days`` days' next day and reach
    back ``backfill_days_w`` days. ``restricted`` marks the jobs of
    ``4_mutate_recover`` that run on the restricted ssvids only. Pure: the
    executors and ``--dry-run`` both read it.
    """
    mid = end - timedelta(days=tail_days)
    daily = [
        (day_end - timedelta(days=backfill_days_w), day_end)
        for day_end in daterange_inclusive(mid + timedelta(days=1), end + timedelta(days=1))
    ]
    every_day = [(s, e, False) for s, e in daily]
    if mode == MODE_BF:
        return [(start, end, False)]
    if mode == MODE_BFD:
        return [(start, mid, False), *every_day]
    if mode == MODE_BFTRUNCATE:
        return [(start, end, False), *every_day]
    if mode == MODE_MUTATE_RECOVER:
        return [(start, mid, False), *((s, e, True) for s, e in daily), *every_day]
    raise ValueError(f"unknown mode: {mode}")


def _execute_mode(
    mode: str,
    runner: str,
    *,
    base_cfg: dict,
    start: date,
    end: date,
    output: str,
    experiment_id: str,
    image_tag: str,
    tail_days: int = 0,
    backfill_days_w: int = 0,
    restricted_ssvids: tuple[str, ...] = (),
) -> None:
    """Run every job of ``mode`` in order, each into ``output``."""
    windows = mode_windows(
        mode, start=start, end=end, tail_days=tail_days, backfill_days_w=backfill_days_w,
    )
    restricted_cfg = {**base_cfg, "ssvids": restricted_ssvids}
    for i, (job_start, job_end, restricted) in enumerate(windows, start=1):
        cfg = _make_config(
            start=job_start, end=job_end, bq_out_gaps=output,
            job_name=_job_name(experiment_id, mode, i, len(windows)),
            **(restricted_cfg if restricted else base_cfg),
        )
        _run_pipeline(runner, cfg, image_tag)


def execute_bf(
    runner: str, *, base_cfg: dict, start: date, end: date, output: str,
    experiment_id: str, image_tag: str,
) -> None:
    _execute_mode(
        MODE_BF, runner, base_cfg=base_cfg, start=start, end=end, output=output,
        experiment_id=experiment_id, image_tag=image_tag,
    )


def execute_bfd(
    runner: str,
    *,
    base_cfg: dict,
    start: date,
    end: date,
    tail_days: int,
    backfill_days_w: int,
    output: str,
    experiment_id: str,
    image_tag: str,
) -> None:
    _execute_mode(
        MODE_BFD, runner, base_cfg=base_cfg, start=start, end=end, output=output,
        experiment_id=experiment_id, image_tag=image_tag,
        tail_days=tail_days, backfill_days_w=backfill_days_w,
    )


def execute_bftruncate(
    runner: str,
    *,
    base_cfg: dict,
    start: date,
    end: date,
    tail_days: int,
    backfill_days_w: int,
    output: str,
    experiment_id: str,
    image_tag: str,
) -> None:
    _execute_mode(
        MODE_BFTRUNCATE, runner, base_cfg=base_cfg, start=start, end=end, output=output,
        experiment_id=experiment_id, image_tag=image_tag,
        tail_days=tail_days, backfill_days_w=backfill_days_w,
    )


def execute_mutate_recover(
    runner: str,
    *,
    base_cfg: dict,
    start: date,
    end: date,
    tail_days: int,
    backfill_days_w: int,
    output: str,
    restricted_ssvids: tuple[str, ...],
    experiment_id: str,
    image_tag: str,
) -> None:
    if not restricted_ssvids:
        raise ValueError(
            "execute_mutate_recover requires a non-empty restricted_ssvids tuple."
        )
    _execute_mode(
        MODE_MUTATE_RECOVER, runner, base_cfg=base_cfg, start=start, end=end, output=output,
        experiment_id=experiment_id, image_tag=image_tag,
        tail_days=tail_days, backfill_days_w=backfill_days_w,
        restricted_ssvids=restricted_ssvids,
    )


# --------------------------------------------------------------------------
# Run cache integration (M4 of dit.cache rollout; see docs/run-cache-impl.md)
# --------------------------------------------------------------------------

#: The workflow name recorded on every CachedRun row.
WORKFLOW_NAME = "workflows/pipe_gaps/mode_equivalence.py"


#: Modes that consume ``tail_days`` + ``backfill_days``. ``MODE_BF`` is the
#: single big-range run -- those fields are wired through ``execute_*`` for
#: it but never read, so they must NOT contribute to its cache key (otherwise
#: changing ``--tail-days`` would invalidate BF's cache for no behavioural
#: reason, dropping the hit rate).
_MODES_USING_TAIL = frozenset({MODE_BFD, MODE_BFTRUNCATE, MODE_MUTATE_RECOVER})


def canonical_params_dict(args: argparse.Namespace, mode: str) -> dict[str, Any]:
    """Output-affecting params for a pipe-gaps mode-equivalence run.

    Mode-aware: only the params each mode actually consumes contribute
    to its cache key. ``MODE_BF`` runs a single big range and ignores
    ``tail_days`` / ``backfill_days``; the other modes use them for
    their daily slices. Including irrelevant fields in BF's key would
    invalidate its cache on every ``--tail-days`` change, even though
    BF's output doesn't depend on it.

    Excludes plumbing (service accounts, regions, datasets), naming
    (experiment_id, suffix), and runner-only knobs (image_tag, parallel,
    skip_pipelines, max_num_workers, worker_machine_type, disk_size_gb) —
    none of which affect the output content.
    """
    params: dict[str, Any] = {
        "mode": mode,
        "start": args.start,
        "end": args.end,
        "min_gap_length": args.min_gap_length,
        "n_hours_before": args.n_hours_before,
        "window_period_d": args.window_period_d,
        "filter_good_seg": (args.filter_good_seg == "True"),
        "skip_open_gaps": bool(args.skip_open_gaps),
        "ssvids": sorted(
            s.strip() for s in args.ssvids.split(",") if s.strip()
        ),
        "source_messages": args.source_messages,
        "source_segments": args.source_segments,
    }
    if mode in _MODES_USING_TAIL:
        params["tail_days"] = args.tail_days
        params["backfill_days"] = args.backfill_days
    return params


def _build_cache_key(args: argparse.Namespace, mode: str, **extra_params: Any) -> CacheKey:
    """Compose a :class:`CacheKey` for the given mode.

    ``extra_params`` are merged into the params dict for modes whose
    output depends on additional inputs (e.g. ``mutate_recover``'s
    ``restricted_ssvids``).
    """
    params = canonical_params_dict(args, mode)
    params.update(extra_params)
    return CacheKey(
        pipeline_commit=args.pipeline_commit,
        worker_image_digest=args.worker_image_digest,
        workflow_file_sha1=WORKFLOW_FILE_SHA1,
        params=params,
    )


def _run_with_cache(
    execute_fn: Callable[..., None],
    *,
    args: argparse.Namespace,
    mode: str,
    output_fqn: str,
    execute_kwargs: dict[str, Any],
    cache_key_extras: Optional[dict[str, Any]] = None,
) -> str:
    """Workflow-local adapter over :func:`dit.workflow.run_with_cache`.

    Builds the mode-aware :class:`CacheKey` from the stamped ``args`` context
    (via :func:`_build_cache_key`) and threads the per-run :class:`RunContext`
    (stamped onto ``args`` in :func:`main`) and the pipe-gaps workflow/pipeline
    identity into the generic wrapper. Keeps the call-site signature unchanged
    so :func:`main`'s ``_wrap_*`` closures don't churn.

    ``output_fqn`` is the mode's latest-state view, the comparison target. Its
    versioned table is recorded beside it, so ``cancel_run`` drops both.
    """
    cache_key = _build_cache_key(args, mode, **(cache_key_extras or {}))
    return dit_workflow.run_with_cache(
        execute_fn,
        ctx=args.run_context,
        workflow=WORKFLOW_NAME,
        pipeline="pipe-gaps",
        experiment_id=args.experiment_id,
        cache_key=cache_key,
        output_fqn=output_fqn,
        # The versioned table behind the view: recorded so cleanup drops it.
        extra_output_tables=raw_gaps.output_tables(output_fqn)[1:],
        execute_kwargs=execute_kwargs,
        log_label=mode,
        use_cache=not args.no_cache,
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n", 1)[0])
    p.add_argument("--runner", choices=list(RUNNERS), default="dataflow")
    p.add_argument("--source-messages", default=DEFAULT_SOURCE_MESSAGES,
                   help=f"Fully-qualified BQ table of input AIS messages. "
                        f"Default: {DEFAULT_SOURCE_MESSAGES}")
    p.add_argument("--source-segments", default=DEFAULT_SOURCE_SEGMENTS,
                   help=f"Fully-qualified BQ table of segs_activity. "
                        f"Default: {DEFAULT_SOURCE_SEGMENTS}")
    p.add_argument("--start", default=DEFAULT_START)
    p.add_argument("--end", default=DEFAULT_END)
    p.add_argument("--tail-days", type=int, default=DEFAULT_TAIL_DAYS)
    p.add_argument("--backfill-days", type=int, default=DEFAULT_BACKFILL_DAYS_W)
    p.add_argument("--ssvids", default="")
    p.add_argument("--min-gap-length", type=float, default=DEFAULT_MIN_GAP_LENGTH)
    p.add_argument("--n-hours-before", type=int, default=DEFAULT_N_HOURS_BEFORE)
    p.add_argument("--window-period-d", type=int, default=DEFAULT_WINDOW_PERIOD_D)
    p.add_argument("--filter-good-seg", default=str(DEFAULT_FILTER_GOOD_SEG),
                   choices=["True", "False"])
    p.add_argument("--skip-open-gaps", action="store_true")
    p.add_argument("--suffix", default=None)
    add_experiment_id_arg(p)
    add_no_cache_arg(p)
    p.add_argument("--require-clean", action="store_true",
                   help="Error on a dirty tree instead of auto-snapshotting "
                        "(for CI / strict-provenance callers).")
    p.add_argument("--skip-pipelines", action="store_true")
    p.add_argument("--skip-comparisons", action="store_true")
    p.add_argument("--parallel", "--async", dest="parallel", action="store_true")
    add_modes_arg(
        p, choices=SELECTABLE_MODES,
        help_suffix=f"{MODE_MUTATE_RECOVER} is not selectable here -- it "
                    f"keeps its own --enable-pipeline-4 gate.",
    )
    add_infra_args(p)
    add_max_num_workers_arg(
        p, default=DEFAULT_MAX_NUM_WORKERS, help_detail=MAX_NUM_WORKERS_HELP_DETAIL,
    )
    add_worker_sizing_args(p, defaults=STEP_WORKER_SIZING)
    # --bq-temp-dataset is workflow-local; not part of add_infra_args.
    p.add_argument("--bq-temp-dataset", default=DEFAULT_BQ_TEMP_DATASET)
    p.add_argument("--image-tag", default=DEFAULT_IMAGE_TAG,
                   help=f"Local image tag for the docker runner (default: "
                        f"{DEFAULT_IMAGE_TAG}). Built from source; not used "
                        f"by the dataflow runner -- see --worker-image.")
    p.add_argument("--worker-image", default=DEFAULT_WORKER_IMAGE,
                   help=f"Dataflow worker container image (registry-published) "
                        f"forwarded as sdk_container_image. Default: "
                        f"{DEFAULT_WORKER_IMAGE}.")
    p.add_argument("--enable-pipeline-4", action="store_true")
    p.add_argument("--restricted-ssvids", default="")
    p.add_argument("--auto-restrict", action="store_true")
    p.add_argument("--auto-restrict-seed", type=int, default=42)
    p.add_argument("--dry-run", action="store_true",
                   help="Log every job the run would submit (mode, window, job name, "
                        "output view) and build each job's pipe-gaps config and DAG "
                        "offline where pipe-gaps v0.17.x is installed, then stop. Reads "
                        "no git state, makes no cloud call, creates nothing.")
    args = p.parse_args(argv)
    # Validate before any cloud call: a typo'd mode that silently ran
    # nothing would look exactly like a passing run.
    args.modes = parse_modes(args.modes, choices=SELECTABLE_MODES)
    if args.enable_pipeline_4 and MODE_BF not in args.modes:
        # mutate_recover is compared against bf, and --auto-restrict reads
        # bf's output table to pick the restricted ssvids.
        raise SystemExit(
            f'error: --enable-pipeline-4 compares {MODE_MUTATE_RECOVER} against '
            f'{MODE_BF}, so {MODE_BF} must be in --modes '
            f'(got {",".join(args.modes)}).'
        )
    # Flag > ceiling env var > default; see dit.workflow.resolve_max_num_workers.
    args.worker_cap = worker_cap(args.max_num_workers, default=DEFAULT_MAX_NUM_WORKERS)
    args.max_num_workers = args.worker_cap.workers
    # Field by field: flag > production's sizing (resolve_worker_sizing).
    args.cli_worker_sizing = worker_sizing_from_args(args)
    args.worker_sizing = resolve_worker_sizing(args.cli_worker_sizing, DEFAULT_WORKER_SIZING)
    return args


def _base_cfg(args: argparse.Namespace, *, labels: Sequence[str]) -> dict[str, Any]:
    """The ``raw_gaps.make_config`` arguments every job of the run shares."""
    return dict(
        bq_in_messages=args.source_messages,
        bq_in_segments=args.source_segments,
        ssvids=tuple(s.strip() for s in args.ssvids.split(",") if s.strip()),
        min_gap_length=args.min_gap_length,
        n_hours_before=args.n_hours_before,
        window_period_d=args.window_period_d,
        filter_good_seg=(args.filter_good_seg == "True"),
        skip_open_gaps=args.skip_open_gaps,
        service_account=args.service_account,
        bq_temp_dataset=args.bq_temp_dataset or None,
        dataflow_region=args.dataflow_region or None,
        dataflow_temp_bucket=args.dataflow_temp_bucket or None,
        dataflow_subnetwork=args.dataflow_subnetwork or None,
        worker_image=args.worker_image or None,
        labels=list(labels),
        max_num_workers=args.max_num_workers,
        worker_sizing=args.worker_sizing,
    )


#: The cleanup label of a dry run's job configs: nothing is submitted, so no
#: run id is minted.
DRY_RUN_LABELS = ("dit_run_id=dry-run",)


def _dry_run(args: argparse.Namespace) -> int:
    """Log every job of the selected modes and check each job's config.

    Returns before ``resolve_run_context``, which reads git state and can
    snapshot a dirty tree or build a worker image. With pipe-gaps v0.17.x
    importable, every job's ``RawGapsConfig`` and DAG are built offline
    (``raw_gaps.check_job``); otherwise only the plan is logged.
    """
    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    base_cfg = _base_cfg(args, labels=DRY_RUN_LABELS)
    restricted = (
        tuple(s.strip() for s in args.restricted_ssvids.split(",") if s.strip())
        or ("<chosen from 1_bf at run time>",)
    )
    modes = [*args.modes, *([MODE_MUTATE_RECOVER] if args.enable_pipeline_4 else [])]
    try:
        importlib.import_module("pipe_gaps.pipelines.raw_gaps.factory")
        check = raw_gaps.check_job
    except ImportError as exc:
        logger.warning("--dry-run: pipe-gaps %s not importable (%s); job configs "
                       "are not checked", raw_gaps.PIPELINE_VERSION, exc)
        check = None

    n_jobs = 0
    for mode in modes:
        view = f"{PROJECT}.{args.dest_dataset}.three_way_<suffix>_{mode}"
        windows = mode_windows(mode, start=start, end=end, tail_days=args.tail_days,
                               backfill_days_w=args.backfill_days)
        logger.info("mode %s: %d job(s) into %s (rows in <view>_%s)",
                    mode, len(windows), view, raw_gaps.VERSIONED_SUFFIX)
        for i, (job_start, job_end, is_restricted) in enumerate(windows, start=1):
            cfg = _make_config(
                start=job_start, end=job_end, bq_out_gaps=view.replace("<suffix>", "dry_run"),
                job_name=_job_name(args.experiment_id, mode, i, len(windows)),
                **({**base_cfg, "ssvids": restricted} if is_restricted else base_cfg),
            )
            logger.info("  %d/%d [%s, %s)%s  %s", i, len(windows), job_start, job_end,
                        " restricted ssvids" if is_restricted else "", cfg.job_name)
            if check is not None and not (is_restricted and not args.restricted_ssvids):
                check(cfg)
            n_jobs += 1
    logger.info("--dry-run: %d job(s)%s; nothing was submitted or created", n_jobs,
                ", each config and DAG built offline" if check else "")
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(list(argv) if argv is not None else None)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.dry_run:
        return _dry_run(args)

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    repo_dir = os.getcwd()

    # Resolve the committed ref, worker image, and per-run lineage context in
    # one shot via the shared workflow harness (no-dirty-tree policy + the
    # --suffix manual/cross-version escape hatch live in dit.workflow). Stamp
    # the fields onto args so the rest of this module (suffix builder, cache
    # wrapper, base_cfg) reads them from one place.
    ctx = resolve_run_context(
        repo_dir=repo_dir,
        pipeline_name=PIPELINE_NAME,
        runner=args.runner,
        require_clean=args.require_clean,
        suffix=args.suffix,
        worker_image=args.worker_image,
        default_worker_image=DEFAULT_WORKER_IMAGE,
    )
    args.run_context = ctx
    args.pipeline_commit = ctx.pipeline_commit
    args.unreviewed = ctx.unreviewed
    args.pipeline_commit_parent = ctx.pipeline_commit_parent
    args.worker_image = ctx.worker_image
    args.worker_image_digest = ctx.worker_image_digest
    args.run_id = ctx.run_id
    args.dit_commit = ctx.dit_commit

    suffix = _resolve_suffix(args)
    logger.info("experiment_id: %s", args.experiment_id)
    logger.info("Run suffix: %s", suffix)
    log_worker_caps({"every job": args.worker_cap})
    log_worker_sizings({"every job": args.worker_sizing})
    if args.runner == "dataflow":
        log_production_sizing_hint(
            args.cli_worker_sizing, {"every job": args.worker_sizing},
            [args.source_messages, args.source_segments],
        )

    # Per-run static labels (incl. the dit_run_id cleanup key), shared by
    # every mode's job: RawGapsConfig.labels, which gfw-common applies to the
    # Dataflow job and pipe-gaps to the BigQuery jobs of its reads.
    dit_labels = _dit_run_labels(args)
    logger.info("dit labels: %s", dit_labels)

    base_cfg = _base_cfg(args, labels=dit_labels)

    # Each mode's bq_out_gaps: the latest-state view pipe-gaps creates, over
    # its versioned table <view>_versioned (raw_gaps.output_tables).
    base = f"{PROJECT}.{args.dest_dataset}.three_way_{suffix}"
    bf_table = f"{base}_{MODE_BF}"
    bfd_table = f"{base}_{MODE_BFD}"
    bft_table = f"{base}_{MODE_BFTRUNCATE}"
    mr_table = f"{base}_{MODE_MUTATE_RECOVER}"

    logger.info("output views (each over <view>_%s):", raw_gaps.VERSIONED_SUFFIX)
    logger.info("  %s", bf_table)
    logger.info("  %s", bfd_table)
    logger.info("  %s", bft_table)
    if args.enable_pipeline_4:
        logger.info("  %s", mr_table)

    explicit_restricted = tuple(s.strip() for s in args.restricted_ssvids.split(",") if s.strip())
    if args.enable_pipeline_4:
        if explicit_restricted and args.auto_restrict:
            raise SystemExit(
                "--restricted-ssvids and --auto-restrict are mutually exclusive."
            )
        if not explicit_restricted and not args.auto_restrict:
            raise SystemExit(
                "--enable-pipeline-4 requires either --restricted-ssvids or --auto-restrict."
            )

    if not args.skip_pipelines:
        common = dict(
            runner=args.runner, base_cfg=base_cfg, start=start, end=end,
            experiment_id=args.experiment_id, image_tag=args.image_tag,
        )
        bf_kwargs = dict(common, output=bf_table)
        bfd_kwargs = dict(
            common, output=bfd_table,
            tail_days=args.tail_days, backfill_days_w=args.backfill_days,
        )
        bft_kwargs = dict(
            common, output=bft_table,
            tail_days=args.tail_days, backfill_days_w=args.backfill_days,
        )

        mr_restricted: Optional[tuple[str, ...]] = (
            explicit_restricted if (args.enable_pipeline_4 and explicit_restricted) else None
        )

        # Wrap each mode's execute_* through the run cache so identical
        # (commit, image, params, workflow_file) tuples reuse prior
        # output tables instead of re-running Dataflow. Returns the
        # FQN to use for downstream comparisons (cached or fresh).
        def _wrap_bf() -> str:
            return _run_with_cache(
                execute_bf, args=args, mode=MODE_BF,
                output_fqn=bf_table, execute_kwargs=bf_kwargs,
            )

        def _wrap_bfd() -> str:
            return _run_with_cache(
                execute_bfd, args=args, mode=MODE_BFD,
                output_fqn=bfd_table, execute_kwargs=bfd_kwargs,
            )

        def _wrap_bft() -> str:
            return _run_with_cache(
                execute_bftruncate, args=args, mode=MODE_BFTRUNCATE,
                output_fqn=bft_table, execute_kwargs=bft_kwargs,
            )

        # --modes gates which of the three primary modes actually run. Each
        # mode is cached independently, so a run that adds a mode later reuses
        # the earlier modes' output tables rather than recomputing them.
        _wrappers = {
            MODE_BF: _wrap_bf,
            MODE_BFD: _wrap_bfd,
            MODE_BFTRUNCATE: _wrap_bft,
        }
        skipped = [m for m in SELECTABLE_MODES if m not in args.modes]
        if skipped:
            logger.info(
                "--modes: running %s; skipping %s",
                ",".join(args.modes), ",".join(skipped),
            )
        results: dict[str, str] = {}

        if args.parallel:
            can_parallel_p4 = args.enable_pipeline_4 and mr_restricted is not None
            max_workers = len(args.modes) + (1 if can_parallel_p4 else 0)

            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futures = {m: ex.submit(_wrappers[m]) for m in args.modes}
                f_mr = None
                if can_parallel_p4:
                    assert mr_restricted is not None  # narrowed by can_parallel_p4
                    mr_kwargs = dict(
                        runner=args.runner, base_cfg=base_cfg,
                        start=start, end=end,
                        tail_days=args.tail_days, backfill_days_w=args.backfill_days,
                        output=mr_table,
                        restricted_ssvids=mr_restricted,
                        experiment_id=args.experiment_id,
                        image_tag=args.image_tag,
                    )
                    f_mr = ex.submit(
                        _run_with_cache,
                        execute_mutate_recover,
                        args=args, mode=MODE_MUTATE_RECOVER,
                        output_fqn=mr_table, execute_kwargs=mr_kwargs,
                        cache_key_extras={"restricted_ssvids": sorted(mr_restricted)},
                    )
                results = {m: f.result() for m, f in futures.items()}
                if f_mr is not None:
                    mr_table = f_mr.result()
        else:
            results = {m: _wrappers[m]() for m in args.modes}

        # Unselected modes keep their derived FQN (unused -- no pair
        # references them); selected modes take the cached-or-fresh value.
        bf_table = results.get(MODE_BF, bf_table)
        bfd_table = results.get(MODE_BFD, bfd_table)
        bft_table = results.get(MODE_BFTRUNCATE, bft_table)

        if args.enable_pipeline_4 and args.auto_restrict:
            mid = end - timedelta(days=args.tail_days)
            restricted_list = dit_bq.query_for_restricted_ssvids(
                bf_table,
                mid=mid,
                backfill_days_w=args.backfill_days,
                seed=args.auto_restrict_seed,
            )
            mr_restricted = tuple(restricted_list)
            mr_kwargs = dict(
                runner=args.runner, base_cfg=base_cfg,
                start=start, end=end,
                tail_days=args.tail_days, backfill_days_w=args.backfill_days,
                output=mr_table,
                restricted_ssvids=mr_restricted,
                experiment_id=args.experiment_id,
                image_tag=args.image_tag,
            )
            mr_table = _run_with_cache(
                execute_mutate_recover,
                args=args, mode=MODE_MUTATE_RECOVER,
                output_fqn=mr_table, execute_kwargs=mr_kwargs,
                cache_key_extras={"restricted_ssvids": sorted(mr_restricted)},
            )
        elif args.enable_pipeline_4 and not args.parallel:
            assert mr_restricted is not None  # validated above when enable_pipeline_4
            mr_kwargs = dict(
                runner=args.runner, base_cfg=base_cfg,
                start=start, end=end,
                tail_days=args.tail_days, backfill_days_w=args.backfill_days,
                output=mr_table,
                restricted_ssvids=mr_restricted,
                experiment_id=args.experiment_id,
                image_tag=args.image_tag,
            )
            mr_table = _run_with_cache(
                execute_mutate_recover,
                args=args, mode=MODE_MUTATE_RECOVER,
                output_fqn=mr_table, execute_kwargs=mr_kwargs,
                cache_key_extras={"restricted_ssvids": sorted(mr_restricted)},
            )

    if args.skip_comparisons:
        return 0

    # Pairwise over the SELECTED modes only -- comparing against a mode that
    # never ran would diff an empty/absent table and report a spurious
    # difference. args.modes is in canonical order (parse_modes), so pair
    # order is stable regardless of the order given on the command line.
    tables_by_mode = {
        MODE_BF: bf_table,
        MODE_BFD: bfd_table,
        MODE_BFTRUNCATE: bft_table,
    }
    pairs = [
        (f"{a} vs {b}", tables_by_mode[a], tables_by_mode[b])
        for a, b in itertools.combinations(args.modes, 2)
    ]
    if args.enable_pipeline_4:
        pairs.append((f"{MODE_BF} vs {MODE_MUTATE_RECOVER}", bf_table, mr_table))

    if not pairs:
        # Single-mode run (the cheap smoke shape). Say so explicitly rather
        # than falling through to "all 0 comparisons passed", which reads
        # like an equivalence assertion that actually never ran.
        only = args.modes[0]
        logger.info(
            "only one mode selected (%s) -- no pair to compare. Its output "
            "table is %s. Add a second mode to --modes to get a comparison.",
            only, tables_by_mode[only],
        )
        return 0

    rcs = []
    for label, a, b in pairs:
        logger.info("=" * 80)
        logger.info("comparison: %s", label)
        logger.info("=" * 80)
        # a and b are the modes' latest-state views (bq_out_gaps).
        rcs.append(dit_compare.compare_tables(a, b, keys=COMPARE_KEYS))

    n_failed = sum(1 for rc in rcs if rc != 0)
    if n_failed:
        logger.error("%d/%d comparisons reported differences", n_failed, len(rcs))
        return 1
    logger.info("all %d comparisons passed", len(rcs))
    return 0


if __name__ == "__main__":
    sys.exit(main())
