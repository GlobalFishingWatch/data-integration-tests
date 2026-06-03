"""Outage-recovery integration test for the pipe-gaps detect pipeline.

Covers a class of bug invisible to the four ``mode_equivalence`` modes:
**source-data evolution between runs**, the failure mode that produced the
production sub-threshold-close artefacts (e.g. ssvid 9cc... at
2026-05-26 23:30:31 / 2026-05-27 00:28:33, 58 min < 1h VMS threshold).

The four existing modes (bf, bfd, bftruncate, mutate_recover) all run
against a **static** source: every stage reads the same data. The
``mutate_recover`` mode mutates *which ssvids are processed*, not what
each ssvid's data looks like. So the situation where an early run sees a
partial source (because of ingestion lag / outage), emits an open-v1,
and a later run with a fuller source closes that open with a sub-threshold
ON, never fires inside the mode-equivalence harness.

This workflow simulates exactly that.

Stages
------
1. **Backfill** ``[start, end]`` with ``@pre_outage_snapshot``.
   Standard cold-start. Source as it was at the start of the outage.

2. **Late incremental at end + offset** (default offset = 3 days).
   One run, date range ``[end + offset - backfill_days, end + offset]``,
   ``--as-of-timestamp = pre_outage_snapshot``. The source still doesn't
   have the messages that landed during the outage window. For ssvids
   whose last visible message is < threshold before a daily midnight that
   the date-range crosses, ``eval_open_gap`` fires and emits an open-v1.

3. **Catch-up incrementals** for days ``[end + 1, ..., end + offset]``.
   Each daily run uses ``--as-of-timestamp = post_outage_snapshot``;
   source now contains all the late-arrived messages. The
   ``ProcessBoundaries`` close-recovery path fires on the open-v1 rows
   written in stage 2; if ``first_message_inside_range`` returns a
   late-arrived message within ``threshold`` of OFF, a sub-threshold
   closed-v2 is emitted (the bug).

The output of stage 3 is compared against a single-shot **oracle**:
``execute_outage_oracle`` runs one backfill ``[start, end + offset]``
with ``@post_outage_snapshot`` (= the answer a redesigned pipeline
should produce, since detect() over the full source has no need for
side-input opens).

Expected results
----------------
* Current (buggy) pipeline: ``outage_recovery`` vs ``oracle`` diverges on
  any ssvid whose data shape hits the (OFF near-midnight + late-arrival
  within-threshold) trigger. The divergence is the bug.
* Redesigned pipeline (no side-input close path, threshold re-checked at
  every emission): ``outage_recovery`` vs ``oracle`` matches. Zero diff.

So this test is RED on current main and locks in the fix once the
redesign lands.

Requirements
------------
Requires the ``--as-of-timestamp`` plumbing in pipe-gaps' detect CLI
(branch ``PIPELINE-3974/source_table_time_travel``). The source table
must support BQ time-travel (must be a physical table, not a view, and
within the 7-day retention window). For the production VMS source
``gfw-int-vms-v3.pipe_vms_v3_internal.research_messages`` this is
satisfied.

Choosing snapshots
------------------
* ``--pre-outage-snapshot``: a UTC timestamp at which the source was
  missing the messages whose late arrival triggers the bug. For
  reproducing the 9cc... case, use ``2026-05-27 18:00 UTC`` (around the
  daily-run time when only ``22:32`` / ``23:30`` were visible).
* ``--post-outage-snapshot``: a UTC timestamp at which the source has
  the late-arrived messages. Use ``2026-06-01 18:00 UTC`` or later.

The defaults below are calibrated for the 9cc... reproduction; override
for other datasets.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from typing import Any, Optional

from dit import compare as dit_compare
from dit import workflow as dit_workflow
from dit.cache import CacheKey, sha1_of_workflow_file
from dit.dates import daterange_inclusive
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
    DEFAULT_BACKFILL_DAYS_W,
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

PIPE_VMS_RESEARCH_MESSAGES = (
    "gfw-int-vms-v3.pipe_vms_v3_internal.research_messages"
)
PIPE_VMS_SEGS_ACTIVITY = (
    "global-fishing-watch.pipe_vms_v4_published.segs_activity"
)

# Default source for this workflow is VMS production (the dataset where the
# bug actually manifests). AIS lacks the ~58-min cadence and 1h threshold
# combination that makes the trigger fire reliably.
DEFAULT_SOURCE_MESSAGES = PIPE_VMS_RESEARCH_MESSAGES
DEFAULT_SOURCE_SEGMENTS = PIPE_VMS_SEGS_ACTIVITY

# Mode labels. Single workflow, two output tables (the staged run and the
# oracle); modes follow the ``5_*`` namespace so they sort after the
# mode_equivalence modes 1-4.
MODE_OUTAGE_RECOVERY = "5_outage_recovery"
MODE_OUTAGE_ORACLE = "5_outage_oracle"

# Default outage duration (days between the backfill end and the late
# incremental that runs "during the outage" with the pre-outage snapshot).
DEFAULT_OFFSET_DAYS = 3

# Default snapshots calibrated for the 9cc... reproduction. Override via CLI
# for any other production-data exercise. The pre-outage snapshot must be
# old enough that the late-arrived messages were not yet in source; the
# post-outage snapshot must be recent enough that they are.
DEFAULT_PRE_OUTAGE_SNAPSHOT = "2026-05-27 18:00:00 UTC"
DEFAULT_POST_OUTAGE_SNAPSHOT = "2026-06-01 18:00:00 UTC"

# For the 9cc... case, the OFF lives on 2026-05-26 and the ON on 2026-05-27.
# Default start/end target this two-week window so the test runs in <10 min
# on a single Dataflow worker.
DEFAULT_OUTAGE_START = "2026-05-12"
DEFAULT_OUTAGE_END = "2026-05-26"

COMPARE_KEYS = ("gap_id", "start_timestamp")
COMPARE_VIEW_SUFFIX = "_last_versions"

# Cache buster for the dit-side run cache.
WORKFLOW_FILE_SHA1 = sha1_of_workflow_file(__file__)
WORKFLOW_NAME = "workflows/pipe_gaps/outage_recovery.py"

logger = logging.getLogger(__name__)

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
    as_of_timestamp: Optional[str] = None,
    service_account: Optional[str] = None,
    bq_temp_dataset: Optional[str] = None,
    dataflow_region: Optional[str] = None,
    dataflow_temp_bucket: Optional[str] = None,
    dataflow_subnetwork: Optional[str] = None,
    worker_image: Optional[str] = None,
    job_name: Optional[str] = None,
    labels: Optional[Sequence[str]] = None,
) -> SimpleNamespace:
    """Variant of ``mode_equivalence._make_config`` that additionally
    threads ``as_of_timestamp`` into the DetectGapsConfig namespace.

    ``as_of_timestamp`` becomes ``--as-of-timestamp`` on the CLI side and
    ``FOR SYSTEM_TIME AS OF TIMESTAMP(...)`` in the rendered source query
    (see ``pipe_gaps/assets/queries/messages.sql.j2``).
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
        as_of_timestamp=as_of_timestamp,
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
    if cfg.as_of_timestamp:
        _add("as-of-timestamp", cfg.as_of_timestamp)
    return flags


def _build_pipeline_for(cfg: SimpleNamespace):
    """Return a ``pipeline_builder`` closure for the dataflow runner.

    Mirrors ``mode_equivalence._build_pipeline_for`` so that
    DetectGapsConfig.from_namespace reads ``as_of_timestamp`` off the
    cfg namespace and passes it through to MessagesQuery.
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
    here only to call our local ``_cfg_to_cli_flags`` (which knows about
    ``--as-of-timestamp``) and ``_build_pipeline_for`` (which preserves
    the cfg attribute).
    """
    logger.info(
        "[%s] start=%s end=%s as_of=%s out=%s",
        runner, cfg.date_range[0], cfg.date_range[1],
        cfg.as_of_timestamp or "(unset)", cfg.bq_output_gaps,
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
    end: date,
    offset_days: int,
    backfill_days_w: int,
    pre_outage_snapshot: str,
    post_outage_snapshot: str,
    output: str,
    experiment_id: str,
    image_tag: str,
) -> None:
    """Three-stage outage simulation writing to a single SCD-2 ``output`` table.

    Stage 1 (backfill, pre-outage source): one big slice [start, end].
    Stage 2 (late incremental, pre-outage source): one slice
        [end + offset_days - backfill_days_w, end + offset_days].
    Stage 3 (catch-up incrementals, post-outage source): one slice per day
        in [end + 1, end + offset_days], each
        [d - backfill_days_w, d].
    """
    if offset_days <= 0:
        raise ValueError(f"offset_days must be > 0; got {offset_days}")

    late_d = end + timedelta(days=offset_days)
    catchup_ends = list(daterange_inclusive(end + timedelta(days=1), late_d))
    # 1 backfill + 1 late incremental + N catch-ups
    total = 2 + len(catchup_ends)

    # Stage 1: backfill, pre-outage snapshot.
    cfg = _make_config(
        start=start, end=end, bq_output_gaps=output,
        as_of_timestamp=pre_outage_snapshot,
        job_name=_job_name(experiment_id, MODE_OUTAGE_RECOVERY, 1, total),
        **base_cfg,
    )
    _run_pipeline(runner, cfg, image_tag)

    # Stage 2: late incremental, pre-outage snapshot. ONE run.
    late_start = late_d - timedelta(days=backfill_days_w)
    cfg = _make_config(
        start=late_start, end=late_d, bq_output_gaps=output,
        as_of_timestamp=pre_outage_snapshot,
        job_name=_job_name(experiment_id, MODE_OUTAGE_RECOVERY, 2, total),
        **base_cfg,
    )
    _run_pipeline(runner, cfg, image_tag)

    # Stage 3: catch-up incrementals, post-outage snapshot.
    for i, day_end in enumerate(catchup_ends, start=3):
        day_start = day_end - timedelta(days=backfill_days_w)
        cfg = _make_config(
            start=day_start, end=day_end, bq_output_gaps=output,
            as_of_timestamp=post_outage_snapshot,
            job_name=_job_name(experiment_id, MODE_OUTAGE_RECOVERY, i, total),
            **base_cfg,
        )
        _run_pipeline(runner, cfg, image_tag)


def execute_outage_oracle(
    runner: str,
    *,
    base_cfg: dict,
    start: date,
    end: date,
    offset_days: int,
    post_outage_snapshot: str,
    output: str,
    experiment_id: str,
    image_tag: str,
) -> None:
    """Single-shot backfill [start, end + offset_days] with post-outage
    source snapshot. This is the ground-truth answer the staged
    ``execute_outage_recovery`` should match.
    """
    full_end = end + timedelta(days=offset_days)
    cfg = _make_config(
        start=start, end=full_end, bq_output_gaps=output,
        as_of_timestamp=post_outage_snapshot,
        job_name=_job_name(experiment_id, MODE_OUTAGE_ORACLE, 1, 1),
        **base_cfg,
    )
    _run_pipeline(runner, cfg, image_tag)


# --------------------------------------------------------------------------
# Cache integration (parallels mode_equivalence.py)
# --------------------------------------------------------------------------


def canonical_params_dict(args: argparse.Namespace, mode: str) -> dict[str, Any]:
    """Output-affecting params for a pipe-gaps outage-recovery run."""
    params: dict[str, Any] = {
        "mode": mode,
        "start": args.start,
        "end": args.end,
        "offset_days": args.offset_days,
        "backfill_days": args.backfill_days,
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
        "post_outage_snapshot": args.post_outage_snapshot,
    }
    # The staged mode's output depends on BOTH snapshots; the oracle's
    # only on the post-outage one. Including the pre-outage snapshot in
    # the oracle's cache key would needlessly invalidate it.
    if mode == MODE_OUTAGE_RECOVERY:
        params["pre_outage_snapshot"] = args.pre_outage_snapshot
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


def _parse_snapshot(value: str) -> datetime:
    """Parse a snapshot string into a tz-aware datetime.

    Accepts ISO-8601 with explicit zone offset, trailing ``Z``, or trailing
    ``UTC``. Rejects naive timestamps -- BQ ``FOR SYSTEM_TIME AS OF`` would
    interpret these against the session's time zone, which is not the
    user's intent for an outage-recovery reproduction (the test would
    silently drift when run from a non-UTC session).
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
            f"snapshot timestamp {value!r} is not parseable as ISO-8601 "
            f"(expected e.g. '2026-05-27 18:00:00 UTC')"
        ) from exc
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError(
            f"snapshot timestamp {value!r} is missing an explicit time zone; "
            f"add ' UTC' (e.g. '2026-05-27 18:00:00 UTC') or an offset "
            f"(e.g. '+00:00')"
        )
    return parsed


def _validate_snapshot(value: str) -> str:
    """argparse type: parse + reject naive; return the original string.

    The original string (not the parsed datetime) is preserved so the
    pipeline receives a value BQ ``FOR SYSTEM_TIME AS OF`` accepts
    verbatim.
    """
    _parse_snapshot(value)
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
    p.add_argument("--start", default=DEFAULT_OUTAGE_START)
    p.add_argument("--end", default=DEFAULT_OUTAGE_END)
    p.add_argument(
        "--offset-days", type=int, default=DEFAULT_OFFSET_DAYS,
        help=("Days between backfill end and the late incremental run "
              "(simulates the outage duration). Default: "
              f"{DEFAULT_OFFSET_DAYS}."),
    )
    p.add_argument("--backfill-days", type=int, default=DEFAULT_BACKFILL_DAYS_W)
    p.add_argument(
        "--pre-outage-snapshot",
        type=_validate_snapshot,
        default=DEFAULT_PRE_OUTAGE_SNAPSHOT,
        help=("UTC timestamp pinned via FOR SYSTEM_TIME AS OF for stages "
              "1 and 2. Pick a moment when the late-arrived messages were "
              f"NOT yet in source. Default: {DEFAULT_PRE_OUTAGE_SNAPSHOT}."),
    )
    p.add_argument(
        "--post-outage-snapshot",
        type=_validate_snapshot,
        default=DEFAULT_POST_OUTAGE_SNAPSHOT,
        help=("UTC timestamp pinned via FOR SYSTEM_TIME AS OF for stage 3 "
              "and the oracle. Pick a moment when the late-arrived messages "
              f"ARE in source. Default: {DEFAULT_POST_OUTAGE_SNAPSHOT}."),
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
    add_infra_args(p)
    p.add_argument("--bq-temp-dataset", default=DEFAULT_BQ_TEMP_DATASET)
    p.add_argument("--image-tag", default=DEFAULT_IMAGE_TAG)
    p.add_argument("--worker-image", default=DEFAULT_WORKER_IMAGE)

    args = p.parse_args(argv)

    # Cross-validate: post-outage snapshot must be strictly after pre-outage,
    # otherwise stage 3 won't see "more data" than stages 1-2 and the test
    # collapses to a vanilla bfd run. _parse_snapshot already guarantees
    # tz-aware datetimes (it rejects naive at arg-parse time).
    pre = _parse_snapshot(args.pre_outage_snapshot)
    post = _parse_snapshot(args.post_outage_snapshot)
    if post <= pre:
        p.error(
            "--post-outage-snapshot must be strictly later than "
            "--pre-outage-snapshot; otherwise stage 3 sees no new data and "
            "the test reduces to a static-source bfd run."
        )

    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(list(argv) if argv is not None else None)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    start = date.fromisoformat(args.start)
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
    logger.info("Pre-outage snapshot:  %s", args.pre_outage_snapshot)
    logger.info("Post-outage snapshot: %s", args.post_outage_snapshot)
    logger.info("Outage window: %s + %d day(s) = simulated incomplete-source span",
                args.end, args.offset_days)

    dit_labels = _dit_run_labels(args)
    logger.info("dit labels: %s", dit_labels)

    base_cfg = dict(
        bq_input_messages=args.source_messages,
        bq_input_segments=args.source_segments,
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
        common = dict(
            runner=args.runner, base_cfg=base_cfg,
            start=start, end=end, offset_days=args.offset_days,
            post_outage_snapshot=args.post_outage_snapshot,
            experiment_id=args.experiment_id,
            image_tag=args.image_tag,
        )
        recovery_kwargs = dict(
            common,
            backfill_days_w=args.backfill_days,
            pre_outage_snapshot=args.pre_outage_snapshot,
            output=recovery_table,
        )
        oracle_kwargs = dict(common, output=oracle_table)

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
