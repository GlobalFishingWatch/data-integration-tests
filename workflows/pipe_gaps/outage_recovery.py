"""Outage-recovery integration test for the pipe-gaps detect pipeline.

Reproduces the divergence between (a) a long history of daily incremental
runs interrupted by an outage and then "fixed" by a recovery backfill, and
(b) a clean single-shot backfill over the same range. On current pipe-gaps
the two states differ on ``raw_gaps_last_versions``; that difference is
the bug this workflow locks in (RED today, GREEN once fixed).

Scenario
--------
The production shape is:

1. The pipe-gaps daily DAG runs normally as incrementals for a long time.
2. At some point an outage interrupts daily runs for one or more days
   (here simulated by simply SKIPPING the daily runs for those days --
   the source is NOT mutated, which keeps the workflow runnable against
   the frozen 2020 staging cohort).
3. The daily DAG resumes after the outage and keeps running through to
   the most recent day. The SCD-2 ``raw_gaps`` table now reflects "all
   days processed, except the outage days were never seen at their
   normal cadence".
4. A recovery backfill is launched. It runs the pipeline **daily-by-daily
   from ``outage_start - recovery_buffer_days`` to the most recent day**
   against the same (now-healthy) source. The intent is that the final
   SCD-2 state matches what a clean single-shot backfill over
   ``[start, most_recent_day]`` would produce.

Stages
------
1. **Initial backfill** ``[start, backfill_end]`` (one big run).
   Represents all prior daily DAG history condensed into one chunk.
2. **Post-outage continuation** ``[outage_end + 1, end]`` (one big run).
   Represents the daily DAG resuming after the outage. The outage
   period ``[outage_start, outage_end]`` is implicitly skipped --
   no stage writes for those dates until Stage 3.
3. **Recovery backfill** ``[outage_start - recovery_buffer_days, end]``
   (one big run). The on-call's reprocess-from-before-outage-to-current.
   Per the pipe-gaps reprocess-to-end contract
   (see ``workflows/pipe_gaps/CLAUDE.md``), this MUST extend to ``end``.

All three stages write ``WRITE_APPEND`` to the same SCD-2 ``raw_gaps``
table. The oracle is a single-shot backfill ``[start, end]`` against the
same source. Comparison is on the ``_last_versions`` view via
``dit.compare.compare_tables``.

Total: 3 staged Dataflow jobs + 1 oracle = 4 jobs per run.

Mechanism
---------
A single source-state pin (``--pin-at``) is realised by
``dit.bq.snapshot_into_experiment``: the workflow clones
``research_messages`` and ``segs_activity`` into ``<project>.tech_great_expectations``
(the canonical dit BQ artifact dataset, defaulting to
``world-fishing-827.tech_great_expectations``; ``--snapshot-dest-project``
overrides for the cross-org dodge path) under per-experiment table names
``dit_exp_<sanitised(experiment_id)>_outage_<source_table_name>``. Each
stage's pipeline reads from this same snapshot; pipe-gaps doesn't know
it's reading a snapshot. Per-table ``expiration_timestamp`` is set at
creation (``--snapshot-expiration-days``); BQ self-cleans.

Only ONE snapshot is needed because the outage is simulated by skipping
runs, not by mutating source data. (A future variant could add a synthetic
source-mutation step; out of scope here.)

Expected results
----------------
* Current pipe-gaps main: ``outage_recovery`` vs ``oracle`` diverges on
  ``raw_gaps_last_versions`` because the recovery's daily DELETE-then-LOAD
  doesn't reproduce what a single-shot backfill would write. The
  divergence is the bug.
* Once the pipe-gaps fix lands: zero diff.

Defaults
--------
Following dit's staging-by-default convention (see
``workflows/pipe_gaps/mode_equivalence.py``, ``README.md`` "Staging data
sources"), the workflow defaults to the ``pipe_ais_test_202408290000``
staging cohort -- same project as the snapshot dest
(``world-fishing-827``), so no cross-org snapshot block.

The staging cohort is frozen 2020 AIS data; an outage in 2020 means a
gap in the daily DAG schedule, not new source-side data evolution. To
exercise prod-VMS where late-arrivals do create real evolution, override
``--source-messages``, ``--source-segments``, and ``--snapshot-dest-project``
(see ``--snapshot-dest-project`` help).

Source-table requirements
-------------------------
The ``--source-messages`` / ``--source-segments`` tables must be
**physical** tables (not views) within BQ's time-travel window at the
moment ``--pin-at`` resolves to, because
``CREATE SNAPSHOT TABLE ... CLONE ... FOR SYSTEM_TIME AS OF`` requires
both. Both the staging defaults and the prod-VMS opt-in
(``gfw-int-vms-v3.pipe_vms_v3_internal.{research_messages,segs_activity}``)
satisfy this. The published view
``pipe_vms_v4_published.segs_activity`` is NOT a valid choice.

The default ``--pin-at`` is today UTC midnight minus 1 day (always inside
BQ's 7-day time-travel window); override for any specific reproduction.
Re-runs of the same ``--experiment-id`` reuse the existing snapshot tables
until they expire (default ``--snapshot-expiration-days = 7``).
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Optional

from dit import bq as dit_bq
from dit import compare as dit_compare
from dit import workflow as dit_workflow
from dit.cache import CacheKey, sha1_of_workflow_file
from dit.job_names import make_job_name
from dit.runners import dataflow as dit_dataflow
from dit.runners import docker as dit_docker
from dit.workflow import (
    add_experiment_id_arg,
    add_infra_args,
    resolve_run_context,
)

# Reused workflow-wide constants. See mode_equivalence.py for full doc.
from workflows.pipe_gaps.mode_equivalence import (
    DEFAULT_BQ_TEMP_DATASET,
    DEFAULT_FILTER_GOOD_SEG,
    DEFAULT_IMAGE_TAG,
    DEFAULT_MIN_GAP_LENGTH,
    DEFAULT_N_HOURS_BEFORE,
    DEFAULT_WINDOW_PERIOD_D,
    DEFAULT_WORKER_IMAGE,
    PIPELINE_NAME,
    PROJECT,
    REPO_NAME,
    RUNNERS,
    STEP_NAME,
)

# Defaults point at the dit staging cohort (same project as the snapshot
# destination -- world-fishing-827 -- so no cross-org block on ``CREATE
# SNAPSHOT TABLE``). Matches the staging-by-default precedent set by
# ``workflows/pipe_gaps/mode_equivalence.py``; see also the README's
# "Staging data sources" section for the table inventory. To exercise
# the actual prod-VMS bug shape (e.g. the 9cc... case), override
# explicitly with ``--source-messages gfw-int-vms-v3.pipe_vms_v3_internal.research_messages``
# and ``--source-segments gfw-int-vms-v3.pipe_vms_v3_internal.segs_activity``
# plus ``--snapshot-dest-project gfw-int-vms-v3`` (to dodge the cross-
# org block, since the dest must live in the source's project).
DEFAULT_SOURCE_MESSAGES = (
    f"{PROJECT}.pipe_ais_test_202408290000_internal.messages_positions"
)
DEFAULT_SOURCE_SEGMENTS = (
    f"{PROJECT}.pipe_ais_test_202408290000_published.segs_activity"
)

# Mode labels. Single workflow, two output tables (the staged run and the
# oracle); modes follow the ``5_*`` namespace so they sort after the
# mode_equivalence modes 1-4.
MODE_OUTAGE_RECOVERY = "5_outage_recovery"
MODE_OUTAGE_ORACLE = "5_outage_oracle"


# Default pin timestamp. Computed today-relative at parse_args time so a
# default run always lands inside BQ's 7-day time-travel window (a
# hardcoded default would go stale immediately).
def _default_pin_at() -> str:
    # ``isoformat(sep=" ")`` matches the space-separated form the CLI help /
    # error messages document (``YYYY-MM-DD HH:MM:SS UTC``).
    return _utc_floor_days_ago(1).isoformat(sep=" ").replace("+00:00", " UTC")


# Default snapshot-table expiration. Matches cross_version_ais.py
# (``DEFAULT_SNAPSHOT_EXPIRATION_DAYS = 7``): long enough for a typical
# multi-day reproduction session, short enough to bound storage cost on
# stale experiment-ids.
DEFAULT_SNAPSHOT_EXPIRATION_DAYS = 7

# Stage boundaries are tight (Dec 28-31) so a default run finishes in
# <10 min on a single Dataflow worker while still exercising the 3-stage
# shape end-to-end (one-day outage on Dec 29, post-outage continuation on
# Dec 30-31, recovery on Dec 28-31). The 2020-01-01 start matches
# ``mode_equivalence.py``
# (``DEFAULT_START = "2020-01-01"``) so both workflows hit the same cohort
# data; the cohort name ``pipe_ais_test_202408290000`` is the snapshot
# date, NOT the data date.
DEFAULT_START = "2020-01-01"
DEFAULT_BACKFILL_END = "2020-12-28"
DEFAULT_OUTAGE_START = "2020-12-29"  # one-day outage by default; same as outage_end
DEFAULT_OUTAGE_END = "2020-12-29"
DEFAULT_END = "2020-12-31"
DEFAULT_RECOVERY_BUFFER_DAYS = 1

# Role passed to ``dit.bq.snapshot_into_experiment`` so the dest tables
# land at ``<project>.tech_great_expectations.dit_exp_<experiment>_outage_<source_table_name>``.
# Single role -- the outage is simulated by skipping daily runs, not by
# pinning source data at two different times, so one snapshot is enough.
SNAPSHOT_ROLE = "outage"

COMPARE_KEYS = ("gap_id", "start_timestamp")
COMPARE_VIEW_SUFFIX = "_last_versions"

# Cache buster for the dit-side run cache.
WORKFLOW_FILE_SHA1 = sha1_of_workflow_file(__file__)
WORKFLOW_NAME = "workflows/pipe_gaps/outage_recovery.py"

logger = logging.getLogger(__name__)


def _utc_floor_days_ago(days: int) -> datetime:
    """Today UTC at 00:00:00, minus ``days`` days. Used for time-travel-
    window-friendly default pin-at values.
    """
    return (datetime.now(tz=timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0,
    ) - timedelta(days=days))


_UNSAFE_LABEL_CHAR_RE = re.compile(r"[^a-z0-9_-]")
_DIGEST_RE = re.compile(r"@sha\d{3}:[0-9a-f]+$", re.IGNORECASE)


def _safe_label_value(value: str) -> str:
    return _UNSAFE_LABEL_CHAR_RE.sub("-", value.lower())[:63]


def _worker_image_tag(image: str) -> str:
    image = _DIGEST_RE.sub("", image)
    return image.rsplit(":", 1)[-1] if ":" in image else "latest"


def _dit_run_labels(args: argparse.Namespace) -> list[str]:
    return [
        f"dit_run_id={args.run_id}",
        f"dit_commit_sha={_safe_label_value(args.pipeline_commit)}",
        f"dit_worker_image_tag={_safe_label_value(_worker_image_tag(args.worker_image))}",
        f"dit_launched_by={_safe_label_value(os.environ.get('USER', 'unknown'))}",
    ]


def _resolve_suffix(args: argparse.Namespace) -> str:
    if args.suffix is not None:
        return args.suffix
    return f"{args.experiment_id}_{args.pipeline_commit}_{uuid.uuid4().hex[:6]}"


def _job_name(experiment_id: str, mode: str, iteration: int, total: int) -> str:
    return make_job_name(
        repo=REPO_NAME,
        step=STEP_NAME,
        experiment_id=experiment_id,
        mode=mode,
        iteration=iteration,
        total_iterations=total,
    )


# --------------------------------------------------------------------------
# BQ snapshot helpers (dit-pattern, see workflows/port_visits/cross_version_ais.py)
# --------------------------------------------------------------------------

def _validate_distinct_source_basenames(args: argparse.Namespace) -> None:
    """Raise ``ValueError`` if ``--source-messages`` and ``--source-segments``
    have the same basename. Under the canonical-dataset shape they would
    produce identical snapshot dest table names and collide.

    Called once in :func:`main` so BOTH the snapshot-create path
    (:func:`_snapshot_source_at`) AND the ``--skip-snapshots`` reconstruction
    path inherit the protection -- otherwise the skip-snapshots path
    silently computes colliding FQNs that point at non-existent / wrong
    tables. Production sources have distinct basenames
    (``research_messages`` vs ``segs_activity``); this only fires on
    misconfigured CLI input.
    """
    msgs_basename = args.source_messages.rsplit(".", 1)[-1]
    segs_basename = args.source_segments.rsplit(".", 1)[-1]
    if msgs_basename == segs_basename:
        raise ValueError(
            "--source-messages and --source-segments have identical basenames "
            f"({msgs_basename!r}); under the canonical-dataset snapshot shape "
            "they would produce a single dest table name and collide. "
            "Distinct basenames are required."
        )


def _outage_snapshot_dest_fqn(
    *, experiment_id: str, source_table: str, project: str,
) -> str:
    """The dest FQN that :func:`dit.bq.snapshot_into_experiment` would
    produce for an outage-snapshot of ``source_table``.

    Pure function; no BQ call. Used by both :func:`_snapshot_source_at`
    (which creates the snapshot) and the ``--skip-snapshots`` path
    (which reconstructs the FQN without re-creating it).

    Mirrors ``dit.bq.snapshot_into_experiment``'s canonical naming
    convention by construction: same ``-`` → ``_`` sanitisation rule on
    ``experiment_id``, same ``dit_exp_<sanitised(experiment_id)>_<role>_<source_table_name>``
    shape under ``<project>.tech_great_expectations``. Role here is
    :data:`SNAPSHOT_ROLE` (already underscore-safe; no sanitisation needed).
    """
    sanitised_experiment_id = experiment_id.replace("-", "_")
    source_table_name = source_table.rsplit(".", 1)[-1]
    return (
        f"{project}.{dit_bq.CANONICAL_DATASET}."
        f"dit_exp_{sanitised_experiment_id}_{SNAPSHOT_ROLE}_{source_table_name}"
    )


def _snapshot_source_at(
    args: argparse.Namespace, *, pin_at: datetime,
) -> tuple[str, str]:
    """Create snapshots of ``--source-messages`` and ``--source-segments``
    at ``pin_at``.

    Returns ``(messages_snapshot_fqn, segments_snapshot_fqn)`` which the
    stages then pass as their ``bq_input_messages`` / ``bq_input_segments``.

    Snapshots land in ``<project>.tech_great_expectations`` per the
    canonical-dataset policy (see CLAUDE.md § Working agreements).
    ``args.snapshot_dest_project`` controls the dest project; a prod-VMS
    opt-in run (sources in ``gfw-int-vms-v3``, a different GCP org from
    ``world-fishing-827``) must pass it so both sides live in the same
    org and dodge BQ's cross-org snapshot block.

    Precondition: callers must invoke
    :func:`_validate_distinct_source_basenames` first (done at the top of
    :func:`main`). Without distinct basenames the two snapshots collide
    on a single dest table name.
    """
    msgs_fqn = dit_bq.snapshot_into_experiment(
        args.source_messages,
        experiment_id=args.experiment_id,
        role=SNAPSHOT_ROLE,
        expiration_days=args.snapshot_expiration_days,
        as_of=pin_at,
        project=args.snapshot_dest_project,
    )
    segs_fqn = dit_bq.snapshot_into_experiment(
        args.source_segments,
        experiment_id=args.experiment_id,
        role=SNAPSHOT_ROLE,
        expiration_days=args.snapshot_expiration_days,
        as_of=pin_at,
        project=args.snapshot_dest_project,
    )
    logger.info(
        "snapshotted %s -> %s (as_of=%s)",
        args.source_messages, msgs_fqn, pin_at.isoformat(),
    )
    logger.info(
        "snapshotted %s -> %s (as_of=%s)",
        args.source_segments, segs_fqn, pin_at.isoformat(),
    )
    return msgs_fqn, segs_fqn


def _make_config(
    *,
    start: date,
    end: date,
    bq_input_messages: str,
    bq_input_segments: str,
    bq_output_gaps: str,
    ssvids: tuple[str, ...],
    min_gap_length: float,
    n_hours_before: int,
    window_period_d: int,
    filter_good_seg: bool,
    skip_open_gaps: bool,
    service_account: Optional[str] = None,
    bq_temp_dataset: Optional[str] = None,
    dataflow_region: Optional[str] = None,
    dataflow_temp_bucket: Optional[str] = None,
    dataflow_subnetwork: Optional[str] = None,
    worker_image: Optional[str] = None,
    job_name: Optional[str] = None,
    labels: Optional[Sequence[str]] = None,
) -> SimpleNamespace:
    """Build a DetectGapsConfig-shaped namespace.

    Mirrors ``mode_equivalence._make_config``; the workflow-local copy
    exists so we can iterate the source-table FQNs per stage without
    drifting against mode_equivalence's signature. Source-state pinning
    is handled OUTSIDE pipe-gaps via ``dit.bq.snapshot_into_experiment``
    (see :func:`_snapshot_source_at` above), so the pipe-gaps process
    receives ordinary table FQNs and is none the wiser about the
    snapshot layer.
    """
    unknown_parsed_args: dict[str, Any] = {"project": PROJECT}
    if labels:
        unknown_parsed_args["labels"] = list(labels)
    cfg = SimpleNamespace(
        date_range=(start.isoformat(), end.isoformat()),
        bq_input_messages=bq_input_messages,
        bq_input_segments=bq_input_segments,
        bq_output_gaps=bq_output_gaps,
        ssvids=ssvids,
        min_gap_length=min_gap_length,
        n_hours_before=n_hours_before,
        window_period_d=window_period_d,
        filter_good_seg=filter_good_seg,
        skip_open_gaps=skip_open_gaps,
        unknown_parsed_args=unknown_parsed_args,
        unknown_unparsed_args=[],
    )
    cfg.service_account = service_account
    cfg.bq_temp_dataset = bq_temp_dataset
    cfg.dataflow_region = dataflow_region
    cfg.dataflow_temp_bucket = dataflow_temp_bucket
    cfg.dataflow_subnetwork = dataflow_subnetwork
    cfg.worker_image = worker_image
    cfg.job_name = job_name
    return cfg


def _cfg_to_cli_flags(cfg: SimpleNamespace) -> list[str]:
    flags: list[str] = []

    def _add(name: str, value: object) -> None:
        if value is None or value == "" or value == ():
            return
        flags.append(f"--{name.replace('_', '-')}")
        flags.append(str(value))

    _add("date-range", ",".join(cfg.date_range))
    _add("bq-input-messages", cfg.bq_input_messages)
    _add("bq-input-segments", cfg.bq_input_segments)
    _add("bq-output-gaps", cfg.bq_output_gaps)
    _add("min-gap-length", cfg.min_gap_length)
    _add("n-hours-before", cfg.n_hours_before)
    _add("window-period-d", cfg.window_period_d)
    if cfg.filter_good_seg:
        _add("filter-good-seg", "true")
    if cfg.skip_open_gaps:
        _add("skip-open-gaps", "true")
    if cfg.ssvids:
        _add("ssvids", ",".join(cfg.ssvids))
    return flags


def _build_pipeline_for(cfg: SimpleNamespace):
    """Return a ``pipeline_builder`` closure for the dataflow runner.

    Mirrors ``mode_equivalence._build_pipeline_for``.
    """
    def _build(options: Mapping[str, Any]):
        from gfw.common.beam.pipeline.factory import PipelineFactory

        from pipe_gaps.pipelines.detect.config import DetectGapsConfig
        from pipe_gaps.pipelines.detect.factory import DetectGapsLinearDagFactory
        from pipe_gaps.version import __version__ as pipe_gaps_version

        opts = dict(options)
        factory_cls = opts.pop("dag_factory_cls", DetectGapsLinearDagFactory)
        opts.pop("bq_temp_dataset", None)

        parsed = dict(cfg.unknown_parsed_args)
        for key, value in opts.items():
            parsed.setdefault(key, value)

        if cfg.job_name:
            parsed.setdefault("job_name", cfg.job_name)

        runner_only_attrs = {
            "service_account",
            "bq_temp_dataset",
            "dataflow_region",
            "dataflow_temp_bucket",
            "dataflow_subnetwork",
            "worker_image",
            "job_name",
        }
        cfg_attrs = {k: v for k, v in vars(cfg).items() if k not in runner_only_attrs}
        df_cfg = SimpleNamespace(**cfg_attrs)
        df_cfg.unknown_parsed_args = parsed

        config = DetectGapsConfig.from_namespace(df_cfg, version=pipe_gaps_version)
        return PipelineFactory(config, dag_factory=factory_cls(config)).build_pipeline()

    return _build


def _run_pipeline(runner: str, cfg: SimpleNamespace, image_tag: str) -> None:
    """Submit one detect run via the chosen runner.

    Identical control flow to ``mode_equivalence._run_pipeline``; forked
    here for two reasons: (1) so we can keep the workflow self-contained
    and not import private helpers from a sibling workflow, and (2) so
    the log line names the source table that's actually being read,
    which differs per stage in this workflow.
    """
    logger.info(
        "[%s] start=%s end=%s msgs=%s out=%s",
        runner, cfg.date_range[0], cfg.date_range[1],
        cfg.bq_input_messages, cfg.bq_output_gaps,
    )
    if runner == "docker":
        flags = ["detect", *_cfg_to_cli_flags(cfg), "--project", PROJECT]
        rc = dit_docker.run(
            image_tag=image_tag,
            args=flags,
            build_from_source=True,
            entrypoint="pipe-gaps",
        )
        if rc != 0:
            raise RuntimeError(f"docker runner exited with {rc} for {cfg.bq_output_gaps}")
        return

    if runner == "dataflow":
        from pipe_gaps.pipelines.detect.factory import DetectGapsLinearDagFactory

        if not cfg.worker_image:
            raise RuntimeError(
                "dataflow runner requires --worker-image (or a non-empty "
                "DEFAULT_WORKER_IMAGE)."
            )
        rc = dit_dataflow.run(
            args=[],
            image_tag=cfg.worker_image,
            service_account=cfg.service_account,
            region=cfg.dataflow_region,
            temp_bucket=cfg.dataflow_temp_bucket,
            subnetwork=cfg.dataflow_subnetwork,
            bq_temp_dataset=cfg.bq_temp_dataset or "",
            pipeline_builder=_build_pipeline_for(cfg),
            dag_factory_cls=DetectGapsLinearDagFactory,
        )
        if rc != 0:
            raise RuntimeError(f"dataflow runner exited with {rc} for {cfg.bq_output_gaps}")
        return

    raise ValueError(f"unknown runner: {runner}")


# --------------------------------------------------------------------------
# Stage execution
# --------------------------------------------------------------------------


def execute_outage_recovery(
    runner: str,
    *,
    base_cfg: dict,
    start: date,
    backfill_end: date,
    outage_start: date,
    outage_end: date,
    end: date,
    recovery_buffer_days: int,
    output: str,
    experiment_id: str,
    image_tag: str,
) -> None:
    """Three-stage outage simulation writing to a single SCD-2 ``output`` table.

    Stage 1 (initial backfill): one big slice ``[start, backfill_end]``.
        Represents all prior daily DAG history condensed into one
        chunk -- writes the initial set of gaps that the recovery will
        clobber and reload.
    Stage 2 (post-outage continuation): one big slice
        ``[outage_end + 1, end]``. Represents the daily DAG resuming
        after the outage. The outage period ``[outage_start, outage_end]``
        is implicitly skipped -- no stage writes for those dates until
        the recovery in Stage 3.
    Stage 3 (recovery backfill): one big slice
        ``[outage_start - recovery_buffer_days, end]``. The on-call's
        reprocess-from-before-outage-to-current. Per the pipe-gaps
        reprocess-to-end contract (see ``workflows/pipe_gaps/CLAUDE.md``),
        this MUST extend to ``end`` -- the pre-write delete in
        ``gaps_delete.sql.j2`` is unbounded on the right, so a recovery
        with a narrower end would leak rows for ``[end+1, ...)``.

    All three stages read the same source snapshot (the outage is
    simulated by skipping date ranges, not by mutating source data),
    so ``base_cfg`` is a single kwargs dict.
    """
    # Stage 1: initial backfill [start, backfill_end].
    cfg = _make_config(
        start=start, end=backfill_end, bq_output_gaps=output,
        job_name=_job_name(experiment_id, MODE_OUTAGE_RECOVERY, 1, 3),
        **base_cfg,
    )
    _run_pipeline(runner, cfg, image_tag)

    # Stage 2: post-outage continuation [outage_end + 1, end].
    cfg = _make_config(
        start=outage_end + timedelta(days=1), end=end, bq_output_gaps=output,
        job_name=_job_name(experiment_id, MODE_OUTAGE_RECOVERY, 2, 3),
        **base_cfg,
    )
    _run_pipeline(runner, cfg, image_tag)

    # Stage 3: recovery backfill [outage_start - buffer, end].
    recovery_start_day = outage_start - timedelta(days=recovery_buffer_days)
    cfg = _make_config(
        start=recovery_start_day, end=end, bq_output_gaps=output,
        job_name=_job_name(experiment_id, MODE_OUTAGE_RECOVERY, 3, 3),
        **base_cfg,
    )
    _run_pipeline(runner, cfg, image_tag)


def execute_outage_oracle(
    runner: str,
    *,
    base_cfg: dict,
    start: date,
    end: date,
    output: str,
    experiment_id: str,
    image_tag: str,
) -> None:
    """Single-shot backfill ``[start, end]`` against the source snapshot.

    The ground-truth answer the staged ``execute_outage_recovery`` should
    match: what a clean one-shot run over the full range would produce
    on a healthy source.
    """
    cfg = _make_config(
        start=start, end=end, bq_output_gaps=output,
        job_name=_job_name(experiment_id, MODE_OUTAGE_ORACLE, 1, 1),
        **base_cfg,
    )
    _run_pipeline(runner, cfg, image_tag)


# --------------------------------------------------------------------------
# Cache integration (parallels mode_equivalence.py)
# --------------------------------------------------------------------------


# Keys that only affect the staged 3-stage run, not the single-shot oracle.
# Stripped from the oracle's cache key so iterating on outage geometry
# (or the recovery buffer) doesn't needlessly invalidate the oracle.
_RECOVERY_ONLY_KEYS = frozenset({
    "backfill_end",
    "outage_start",
    "outage_end",
    "recovery_buffer_days",
})


def canonical_params_dict(args: argparse.Namespace, mode: str) -> dict[str, Any]:
    """Output-affecting params for a pipe-gaps outage-recovery run.

    The pin timestamp is stored as its ISO-8601 string (normalised by
    ``_parse_pin_at``); the snapshot dataset / table names are NOT included
    in the cache key, because two runs with the same ``--source-*`` +
    pin-at timestamp produce the same snapshot CONTENT regardless of
    where the snapshot tables live.
    """
    params: dict[str, Any] = {
        "mode": mode,
        "start": args.start,
        "backfill_end": args.backfill_end,
        "outage_start": args.outage_start,
        "outage_end": args.outage_end,
        "end": args.end,
        "recovery_buffer_days": args.recovery_buffer_days,
        "min_gap_length": args.min_gap_length,
        "n_hours_before": args.n_hours_before,
        "window_period_d": args.window_period_d,
        "filter_good_seg": (args.filter_good_seg == "True"),
        "skip_open_gaps": bool(args.skip_open_gaps),
        # Normalised so order-of-CLI-input doesn't dent the hit rate.
        # An empty string -> empty list (no ssvid filter, the default).
        "ssvids": sorted(
            s.strip() for s in (args.ssvids or "").split(",") if s.strip()
        ),
        "source_messages": args.source_messages,
        "source_segments": args.source_segments,
        "pin_at": _parse_pin_at(args.pin_at).isoformat(),
        # When set, the workflow reads live source tables instead of
        # snapshots; pin_at is ignored but kept in the key for shape
        # stability. The boolean differentiates live-source runs from
        # snapshot runs that happen to share a pin_at.
        "no_snapshot": bool(args.no_snapshot),
    }
    if mode == MODE_OUTAGE_ORACLE:
        for k in _RECOVERY_ONLY_KEYS:
            params.pop(k, None)
    return params


def _build_cache_key(args: argparse.Namespace, mode: str) -> CacheKey:
    return CacheKey(
        pipeline_commit=args.pipeline_commit,
        worker_image_digest=args.worker_image_digest,
        workflow_file_sha1=WORKFLOW_FILE_SHA1,
        params=canonical_params_dict(args, mode),
    )


def _run_with_cache(
    execute_fn: Callable[..., None],
    *,
    args: argparse.Namespace,
    mode: str,
    output_fqn: str,
    execute_kwargs: dict[str, Any],
) -> str:
    cache_key = _build_cache_key(args, mode)
    return dit_workflow.run_with_cache(
        execute_fn,
        ctx=args.run_context,
        workflow=WORKFLOW_NAME,
        pipeline="pipe-gaps",
        experiment_id=args.experiment_id,
        cache_key=cache_key,
        output_fqn=output_fqn,
        execute_kwargs=execute_kwargs,
        log_label=mode,
    )


# --------------------------------------------------------------------------
# CLI / main
# --------------------------------------------------------------------------


def _parse_pin_at(value: str) -> datetime:
    """Parse a pin-at string into a tz-aware datetime.

    Accepts ISO-8601 with explicit zone offset, trailing ``Z``, or trailing
    ``UTC``. Rejects naive timestamps -- BQ ``FOR SYSTEM_TIME AS OF`` (used
    by ``snapshot_table`` under the hood) interprets naive against the
    session zone, which is not the user's intent for an outage-recovery
    reproduction (the test would silently drift when run from a non-UTC
    session).
    """
    s = value.strip()
    if s.endswith("UTC"):
        candidate = s[:-3].rstrip() + "+00:00"
    else:
        candidate = s.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"pin-at timestamp {value!r} is not parseable as ISO-8601 "
            f"(expected e.g. '2026-05-27 18:00:00 UTC')"
        ) from exc
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError(
            f"pin-at timestamp {value!r} is missing an explicit time zone; "
            f"add ' UTC' (e.g. '2026-05-27 18:00:00 UTC') or an offset "
            f"(e.g. '+00:00')"
        )
    return parsed


def _validate_pin_at(value: str) -> str:
    """argparse type: parse + reject naive; return the (stripped) string.

    The string form (not the parsed datetime) is what lives on ``args``
    so a later ``_parse_pin_at`` re-parse hits the same validation path
    (no need to special-case "already a datetime"). The cache key itself
    normalises via ``_parse_pin_at(...).isoformat()``, so two equivalent
    user-supplied forms (``'... UTC'`` vs ``'...+00:00'``) collapse to
    one canonical key (see :func:`canonical_params_dict`).
    """
    _parse_pin_at(value)
    return value.strip()


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n", 1)[0])
    p.add_argument("--runner", choices=list(RUNNERS), default="dataflow")
    p.add_argument(
        "--source-messages", default=DEFAULT_SOURCE_MESSAGES,
        help=f"Physical messages table (must support time-travel). "
             f"Default: {DEFAULT_SOURCE_MESSAGES}",
    )
    p.add_argument(
        "--source-segments", default=DEFAULT_SOURCE_SEGMENTS,
        help=f"Segments source. Default: {DEFAULT_SOURCE_SEGMENTS}",
    )
    p.add_argument(
        "--start", default=DEFAULT_START,
        help=(f"Inclusive start of the initial backfill (Stage 1). "
              f"Default: {DEFAULT_START}."),
    )
    p.add_argument(
        "--backfill-end", default=DEFAULT_BACKFILL_END,
        help=(f"Inclusive end of the initial backfill (Stage 1). The "
              f"outage period starts the day after this; Stage 2 "
              f"(post-outage continuation) resumes the day after the "
              f"outage ends. Default: {DEFAULT_BACKFILL_END}."),
    )
    p.add_argument(
        "--outage-start", default=DEFAULT_OUTAGE_START,
        help=(f"First day of the simulated outage (no daily run for this "
              f"or any subsequent day until --outage-end). "
              f"Default: {DEFAULT_OUTAGE_START}."),
    )
    p.add_argument(
        "--outage-end", default=DEFAULT_OUTAGE_END,
        help=(f"Last day of the simulated outage (inclusive). Set equal "
              f"to --outage-start for a one-day outage. "
              f"Default: {DEFAULT_OUTAGE_END}."),
    )
    p.add_argument(
        "--end", default=DEFAULT_END,
        help=(f"Inclusive endpoint of the post-outage continuation "
              f"(Stage 2), the recovery backfill (Stage 3), and the "
              f"oracle. Default: {DEFAULT_END}."),
    )
    p.add_argument(
        "--recovery-buffer-days", type=int,
        default=DEFAULT_RECOVERY_BUFFER_DAYS,
        help=(f"Stage 3 (recovery) starts at "
              f"``outage_start - recovery_buffer_days`` so it overlaps the "
              f"last pre-outage day(s). Default: {DEFAULT_RECOVERY_BUFFER_DAYS}."),
    )
    p.add_argument(
        "--pin-at",
        type=_validate_pin_at,
        default=_default_pin_at(),
        help=("UTC timestamp for the source-table snapshot used by all "
              "stages and the oracle. Default: today UTC midnight minus "
              "1 day (always inside BQ's time-travel window)."),
    )
    p.add_argument(
        "--snapshot-expiration-days", type=int,
        default=DEFAULT_SNAPSHOT_EXPIRATION_DAYS,
        help=("default_table_expiration on the per-experiment snapshot "
              f"datasets, in days. Default: {DEFAULT_SNAPSHOT_EXPIRATION_DAYS}."),
    )
    p.add_argument(
        "--snapshot-dest-project", default=PROJECT,
        help=(f"Project that hosts the snapshot datasets. Default: {PROJECT}. "
              "Override to the source's project when running against a source "
              "that lives in a different GCP org (e.g. "
              "``--snapshot-dest-project gfw-int-vms-v3`` when --source-* "
              "points at the prod VMS tables) -- "
              "``CREATE SNAPSHOT TABLE`` refuses cross-org sources."),
    )
    p.add_argument("--ssvids", default="",
                   help="Comma-separated ssvids to restrict to; empty = all.")
    p.add_argument("--min-gap-length", type=float, default=DEFAULT_MIN_GAP_LENGTH)
    p.add_argument("--n-hours-before", type=int, default=DEFAULT_N_HOURS_BEFORE)
    p.add_argument("--window-period-d", type=int, default=DEFAULT_WINDOW_PERIOD_D)
    p.add_argument("--filter-good-seg", default=str(DEFAULT_FILTER_GOOD_SEG),
                   choices=["True", "False"])
    p.add_argument("--skip-open-gaps", action="store_true")
    p.add_argument("--suffix", default=None)
    add_experiment_id_arg(p)
    p.add_argument("--require-clean", action="store_true")
    p.add_argument("--skip-pipelines", action="store_true")
    p.add_argument("--skip-comparisons", action="store_true")
    p.add_argument("--skip-snapshots", action="store_true",
                   help=("Don't create snapshot tables; assume an earlier run "
                         "of the same --experiment-id already did. Useful when "
                         "iterating on the pipeline logic without re-snapshotting. "
                         "NOTE: --pin-at is still parsed, validated and folded "
                         "into the cache key, but the actual source-state "
                         "pinning comes from the existing snapshot tables -- "
                         "which were created with the earlier run's pin-at, NOT "
                         "the current run's. If you change pin-at while "
                         "--skip-snapshots is set, drop the snapshot tables "
                         "(or use a fresh --experiment-id) to force re-creation."))
    p.add_argument("--no-snapshot", action="store_true",
                   help=("Bypass snapshotting entirely: every stage reads the "
                         "live --source-messages / --source-segments tables "
                         "directly, no FOR SYSTEM_TIME AS OF pin. Use ONLY when "
                         "the source is frozen for the duration of the run -- "
                         "the staging cohort (`pipe_ais_test_*`) is the canonical "
                         "case (2020 AIS data, never changes). For prod-VMS or "
                         "any live-ingest source this would silently sample "
                         "different states across the 3+1 staged jobs -- DO NOT USE. "
                         "When set, --pin-at and --snapshot-* flags are ignored "
                         "(but still parsed for argparse coherence); the cache "
                         "key includes a `no_snapshot=true` marker so runs with "
                         "and without snapshotting don't conflate."))
    add_infra_args(p)
    p.add_argument("--bq-temp-dataset", default=DEFAULT_BQ_TEMP_DATASET)
    p.add_argument("--image-tag", default=DEFAULT_IMAGE_TAG)
    p.add_argument("--worker-image", default=DEFAULT_WORKER_IMAGE)

    args = p.parse_args(argv)

    # Cross-validate stage boundaries:
    #     start <= backfill_end < outage_start <= outage_end < end
    # ``outage_start == outage_end`` is allowed (one-day outage).
    try:
        start_d = date.fromisoformat(args.start)
        backfill_end_d = date.fromisoformat(args.backfill_end)
        outage_start_d = date.fromisoformat(args.outage_start)
        outage_end_d = date.fromisoformat(args.outage_end)
        end_d = date.fromisoformat(args.end)
    except ValueError as exc:
        p.error(f"invalid date in stage-boundary args: {exc}")

    if not (start_d <= backfill_end_d
            < outage_start_d <= outage_end_d
            < end_d):
        p.error(
            "date boundaries must satisfy "
            "start <= backfill_end < outage_start <= outage_end < end; "
            f"got start={args.start}, backfill_end={args.backfill_end}, "
            f"outage_start={args.outage_start}, outage_end={args.outage_end}, "
            f"end={args.end}."
        )

    if args.recovery_buffer_days < 0:
        p.error(
            f"--recovery-buffer-days must be >= 0; got "
            f"{args.recovery_buffer_days}."
        )

    # --snapshot-expiration-days = 0 / negative would compute an invalid
    # expiration_timestamp inside snapshot_into_experiment; fail early
    # with an actionable message.
    if args.snapshot_expiration_days < 1:
        p.error(
            f"--snapshot-expiration-days must be >= 1; got "
            f"{args.snapshot_expiration_days}."
        )

    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(list(argv) if argv is not None else None)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    start = date.fromisoformat(args.start)
    backfill_end = date.fromisoformat(args.backfill_end)
    outage_start = date.fromisoformat(args.outage_start)
    outage_end = date.fromisoformat(args.outage_end)
    end = date.fromisoformat(args.end)
    repo_dir = os.getcwd()

    # The docker runner always builds from the working tree (see
    # ``_run_pipeline``'s ``build_from_source=True`` arg to ``dit_docker.run``),
    # so it never pulls the registry worker image. Skip the kaniko auto-build
    # in that case -- it'd be wasted work, especially noticeable on unreviewed
    # (dirty-tree) docker runs. Beam/dataflow runs still need the image.
    ctx = resolve_run_context(
        repo_dir=repo_dir,
        pipeline_name=PIPELINE_NAME,
        runner=args.runner,
        require_clean=args.require_clean,
        suffix=args.suffix,
        worker_image=args.worker_image,
        default_worker_image=DEFAULT_WORKER_IMAGE,
        build_from_source=(args.runner == "docker"),
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
    logger.info("Pin-at:        %s", args.pin_at)
    logger.info("Stage 1 (initial backfill):           [%s, %s]", start, backfill_end)
    logger.info("       outage (skipped):              [%s, %s]",
                outage_start, outage_end)
    logger.info("Stage 2 (post-outage continuation):   [%s, %s]",
                outage_end + timedelta(days=1), end)
    recovery_start_day = outage_start - timedelta(days=args.recovery_buffer_days)
    logger.info("Stage 3 (recovery backfill):          [%s, %s]",
                recovery_start_day, end)
    if recovery_start_day < backfill_end:
        logger.info(
            "recovery start day %s is inside the initial backfill range "
            "[%s, %s]; this is allowed and exercises within-backfill "
            "SCD-2 re-layering",
            recovery_start_day, start, backfill_end,
        )

    dit_labels = _dit_run_labels(args)
    logger.info("dit labels: %s", dit_labels)

    # Source-state pinning. Three paths:
    #   1. --no-snapshot: read live source tables directly, no snapshot.
    #      ONLY safe for frozen-source cases (staging cohort); for live
    #      sources every stage would sample a different point in time.
    #   2. --skip-snapshots: reuse existing snapshot tables from an earlier
    #      same-experiment-id run (no creation).
    #   3. Default: create one snapshot via snapshot_into_experiment.
    if args.no_snapshot:
        logger.warning(
            "--no-snapshot: every stage reads live %s / %s directly. "
            "Safe only when the source is frozen; for prod-VMS or live "
            "sources this would silently sample different states across "
            "stages. --pin-at (%s) is ignored.",
            args.source_messages, args.source_segments, args.pin_at,
        )
        src_msgs = args.source_messages
        src_segs = args.source_segments
    elif args.skip_snapshots:
        # The basename precondition only matters on snapshot paths --
        # snapshot dest FQNs collide if both sources share a basename.
        _validate_distinct_source_basenames(args)
        # CAREFUL: this re-uses snapshot tables created by an earlier run
        # of the same --experiment-id. The pin-at value you pass now is
        # only used for cache-key composition (and validation) -- it is
        # NOT compared against the snapshot's actual FOR SYSTEM_TIME AS OF
        # creation timestamp. If those drift apart, the workflow reads a
        # stale source state while logging the new pin, which would
        # produce misleading results and cache pollution. Drop the
        # snapshot tables (or use a fresh --experiment-id) to refresh.
        src_msgs = _outage_snapshot_dest_fqn(
            experiment_id=args.experiment_id,
            source_table=args.source_messages, project=args.snapshot_dest_project,
        )
        src_segs = _outage_snapshot_dest_fqn(
            experiment_id=args.experiment_id,
            source_table=args.source_segments, project=args.snapshot_dest_project,
        )
        logger.warning(
            "--skip-snapshots: re-using existing snapshot tables for "
            "experiment-id=%r. pin-at value (%s) is NOT verified against "
            "the snapshot's actual creation timestamp; if you've changed "
            "pin-at since the snapshot was created, drop tables %s, %s "
            "and re-run without --skip-snapshots.",
            args.experiment_id, args.pin_at, src_msgs, src_segs,
        )
    else:
        # Same basename-collision precondition as the skip-snapshots branch.
        _validate_distinct_source_basenames(args)
        src_msgs, src_segs = _snapshot_source_at(
            args, pin_at=_parse_pin_at(args.pin_at),
        )
    logger.info("source tables: %s, %s", src_msgs, src_segs)

    base_cfg = dict(
        bq_input_messages=src_msgs,
        bq_input_segments=src_segs,
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
        labels=dit_labels,
    )

    base = f"{PROJECT}.{args.dest_dataset}.outage_{suffix}"
    recovery_table = f"{base}_{MODE_OUTAGE_RECOVERY}"
    oracle_table = f"{base}_{MODE_OUTAGE_ORACLE}"
    logger.info("output tables:")
    logger.info("  %s  (staged outage_recovery)", recovery_table)
    logger.info("  %s  (single-shot oracle)",       oracle_table)

    if not args.skip_pipelines:
        recovery_kwargs = dict(
            runner=args.runner,
            base_cfg=base_cfg,
            start=start,
            backfill_end=backfill_end,
            outage_start=outage_start,
            outage_end=outage_end,
            end=end,
            recovery_buffer_days=args.recovery_buffer_days,
            output=recovery_table,
            experiment_id=args.experiment_id,
            image_tag=args.image_tag,
        )
        oracle_kwargs = dict(
            runner=args.runner,
            base_cfg=base_cfg,
            start=start, end=end,
            output=oracle_table,
            experiment_id=args.experiment_id,
            image_tag=args.image_tag,
        )

        recovery_table = _run_with_cache(
            execute_outage_recovery,
            args=args, mode=MODE_OUTAGE_RECOVERY,
            output_fqn=recovery_table, execute_kwargs=recovery_kwargs,
        )
        oracle_table = _run_with_cache(
            execute_outage_oracle,
            args=args, mode=MODE_OUTAGE_ORACLE,
            output_fqn=oracle_table, execute_kwargs=oracle_kwargs,
        )

    if args.skip_comparisons:
        return 0

    logger.info("=" * 80)
    logger.info("comparison: %s vs %s", MODE_OUTAGE_RECOVERY, MODE_OUTAGE_ORACLE)
    logger.info("=" * 80)
    rc = dit_compare.compare_tables(
        recovery_table, oracle_table,
        keys=COMPARE_KEYS, view_suffix=COMPARE_VIEW_SUFFIX,
    )
    if rc != 0:
        logger.error(
            "outage_recovery vs oracle reported differences -- "
            "these are the sub-threshold close artefacts (the bug). "
            "Expected RED on current pipe-gaps main; expected GREEN after "
            "the redesign lands."
        )
        return 1
    logger.info("outage_recovery vs oracle matched -- no bug-shape divergence.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
