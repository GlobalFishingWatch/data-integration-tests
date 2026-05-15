"""Mode equivalence integration test for the pipe-gaps detect pipeline.

Drives the same pipeline several different ways ("modes") and asserts all
modes produce identical output on the ``..._last_versions`` views. Ported
from ``pipe-gaps/tests/integration/mode_equivalence.py`` onto the
``dit.*`` library; runner / compare / dates / bq concerns now live there.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import subprocess
import sys
import uuid
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from types import SimpleNamespace
from typing import Any, Optional

from dit import bq as dit_bq
from dit import compare as dit_compare
from dit.dates import daterange_inclusive
from dit.runners import dataflow as dit_dataflow
from dit.runners import docker as dit_docker

logger = logging.getLogger(__name__)


PROJECT = "world-fishing-827"

# Per-user infra knobs: defaults below, override via DIT_* env vars or CLI flags.
DEFAULT_DEST_DATASET = os.environ.get("DIT_DEST_DATASET", "tech_great_expectations")
DEFAULT_DATAFLOW_SA = os.environ.get(
    "DIT_DATAFLOW_SA", "automated-testing@world-fishing-827.iam.gserviceaccount.com"
)
DEFAULT_BQ_TEMP_DATASET = os.environ.get(
    "DIT_BQ_TEMP_DATASET", f"{PROJECT}.{DEFAULT_DEST_DATASET}"
)
DEFAULT_DATAFLOW_REGION = os.environ.get("DIT_DATAFLOW_REGION", "us-central1")
DEFAULT_DATAFLOW_TEMP_BUCKET = os.environ.get("DIT_DATAFLOW_TEMP_BUCKET", "pipe-temp-us-central-ttl7")
DEFAULT_DATAFLOW_SUBNETWORK = os.environ.get(
    "DIT_DATAFLOW_SUBNETWORK", "regions/us-central1/subnetworks/gfw-internal-us-central1"
)

# Workflow-specific defaults (no env var; one-off overrides via CLI flag).
DEFAULT_SOURCE_DATASET = "pipe_ais_test_202408290000_published"

DEFAULT_MIN_GAP_LENGTH = 1.0
DEFAULT_N_HOURS_BEFORE = 12
DEFAULT_WINDOW_PERIOD_D = 2
DEFAULT_FILTER_GOOD_SEG = True
DEFAULT_BACKFILL_DAYS_W = 4

DEFAULT_START = "2020-01-01"
DEFAULT_END = "2021-01-01"
DEFAULT_TAIL_DAYS = 4

DEFAULT_IMAGE_TAG = "gfw/pipe-gaps:dev"

RUNNERS = ("docker", "dataflow")

COMPARE_KEYS = ("gap_id", "start_timestamp")
COMPARE_VIEW_SUFFIX = "_last_versions"

# BQ-table-name-safe slug; max 32 chars. Compiled once.
_EXPERIMENT_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")


def _default_experiment_id() -> str:
    """Auto-generate a per-invocation experiment id when none is provided.

    The literal ``solo_`` prefix marks "not part of a cross-version
    experiment" so BQ filtering can ignore them.
    """
    return f"solo_{uuid.uuid4().hex[:6]}"


def _validate_experiment_id(value: str) -> str:
    if not _EXPERIMENT_ID_RE.match(value):
        raise SystemExit(
            f"error: invalid --experiment-id {value!r}: must match "
            f"{_EXPERIMENT_ID_RE.pattern} (BQ-table-name safe; max 32 chars)."
        )
    return value


def _git_info(repo_dir: str) -> tuple[str, bool]:
    short = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        cwd=repo_dir, check=True, capture_output=True, text=True,
    ).stdout.strip()
    porcelain = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=no"],
        cwd=repo_dir, check=True, capture_output=True, text=True,
    ).stdout.strip()
    return short, bool(porcelain)


def _resolve_suffix(args: argparse.Namespace, repo_dir: str) -> str:
    if args.suffix is not None:
        return args.suffix
    commit, dirty = _git_info(repo_dir)
    if dirty and not args.allow_dirty_tree:
        raise SystemExit(
            f"Refusing to run with uncommitted changes (commit={commit}, dirty=True). "
            "Commit your changes so the run is traceable, or pass --allow-dirty-tree to "
            "override (the suffix will include 'dirty' to flag this)."
        )
    body = (
        f"{commit}_dirty_{uuid.uuid4().hex[:6]}"
        if dirty
        else f"{commit}_{uuid.uuid4().hex[:6]}"
    )
    return f"{args.experiment_id}_{body}"


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
) -> SimpleNamespace:
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
        unknown_parsed_args={"project": PROJECT},
        unknown_unparsed_args=[],
    )
    cfg.service_account = service_account
    cfg.bq_temp_dataset = bq_temp_dataset
    cfg.dataflow_region = dataflow_region
    cfg.dataflow_temp_bucket = dataflow_temp_bucket
    cfg.dataflow_subnetwork = dataflow_subnetwork
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
    """Return a ``pipeline_builder`` closure for the given workflow cfg.

    The runner contract is: ``pipeline_builder(options) -> Pipeline`` where
    ``options`` is the merged Beam-pipeline-options mapping the runner has
    assembled (CLI args + Dataflow knobs). When ``bq_temp_dataset`` is set,
    the runner has already wrapped the workflow's ``dag_factory_cls`` to
    inject ``temp_dataset`` and forwarded the wrapped class via
    ``options["dag_factory_cls"]``; we pull it back out here.

    The returned closure captures ``cfg`` (the workflow's per-call
    SimpleNamespace), strips the runner-only attributes (service_account,
    bq_temp_dataset, dataflow_*), threads ``options`` into
    ``unknown_parsed_args``, builds the ``DetectGapsConfig`` from the
    namespace, and constructs the pipeline via ``PipelineFactory``.
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

        output_basename = cfg.bq_output_gaps.rsplit(".", 1)[-1]
        start, end = cfg.date_range
        parsed.setdefault(
            "job_name",
            f"three-way-eq-{output_basename}-{start}-{end}".replace("_", "-"),
        )

        runner_only_attrs = {
            "service_account",
            "bq_temp_dataset",
            "dataflow_region",
            "dataflow_temp_bucket",
            "dataflow_subnetwork",
        }
        cfg_attrs = {k: v for k, v in vars(cfg).items() if k not in runner_only_attrs}
        df_cfg = SimpleNamespace(**cfg_attrs)
        df_cfg.unknown_parsed_args = parsed

        config = DetectGapsConfig.from_namespace(df_cfg, version=pipe_gaps_version)
        return PipelineFactory(config, dag_factory=factory_cls(config)).build_pipeline()

    return _build


def _run_pipeline(runner: str, cfg: SimpleNamespace, image_tag: str) -> None:
    logger.info(
        "[%s] start=%s end=%s out=%s",
        runner, cfg.date_range[0], cfg.date_range[1], cfg.bq_output_gaps,
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

        rc = dit_dataflow.run(
            args=[],
            image_tag=image_tag,
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


def execute_bf(
    runner: str, *, base_cfg: dict, start: date, end: date, output: str, image_tag: str,
) -> None:
    cfg = _make_config(start=start, end=end, bq_output_gaps=output, **base_cfg)
    _run_pipeline(runner, cfg, image_tag)


def execute_bfd(
    runner: str,
    *,
    base_cfg: dict,
    start: date,
    end: date,
    tail_days: int,
    backfill_days_w: int,
    output: str,
    image_tag: str,
) -> None:
    mid = end - timedelta(days=tail_days)
    cfg = _make_config(start=start, end=mid, bq_output_gaps=output, **base_cfg)
    _run_pipeline(runner, cfg, image_tag)

    for day_end in daterange_inclusive(mid + timedelta(days=1), end + timedelta(days=1)):
        day_start = day_end - timedelta(days=backfill_days_w)
        cfg = _make_config(start=day_start, end=day_end, bq_output_gaps=output, **base_cfg)
        _run_pipeline(runner, cfg, image_tag)


def execute_bftruncate(
    runner: str,
    *,
    base_cfg: dict,
    start: date,
    end: date,
    tail_days: int,
    backfill_days_w: int,
    output: str,
    image_tag: str,
) -> None:
    cfg = _make_config(start=start, end=end, bq_output_gaps=output, **base_cfg)
    _run_pipeline(runner, cfg, image_tag)

    tail_start = end - timedelta(days=tail_days)
    for day_end in daterange_inclusive(tail_start + timedelta(days=1), end + timedelta(days=1)):
        day_start = day_end - timedelta(days=backfill_days_w)
        cfg = _make_config(start=day_start, end=day_end, bq_output_gaps=output, **base_cfg)
        _run_pipeline(runner, cfg, image_tag)


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
    image_tag: str,
) -> None:
    if not restricted_ssvids:
        raise ValueError(
            "execute_mutate_recover requires a non-empty restricted_ssvids tuple."
        )

    mid = end - timedelta(days=tail_days)

    cfg = _make_config(start=start, end=mid, bq_output_gaps=output, **base_cfg)
    _run_pipeline(runner, cfg, image_tag)

    restricted_cfg = {**base_cfg, "ssvids": restricted_ssvids}
    for day_end in daterange_inclusive(mid + timedelta(days=1), end + timedelta(days=1)):
        day_start = day_end - timedelta(days=backfill_days_w)
        cfg = _make_config(
            start=day_start, end=day_end, bq_output_gaps=output, **restricted_cfg,
        )
        _run_pipeline(runner, cfg, image_tag)

    for day_end in daterange_inclusive(mid + timedelta(days=1), end + timedelta(days=1)):
        day_start = day_end - timedelta(days=backfill_days_w)
        cfg = _make_config(
            start=day_start, end=day_end, bq_output_gaps=output, **base_cfg,
        )
        _run_pipeline(runner, cfg, image_tag)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n", 1)[0])
    p.add_argument("--runner", choices=list(RUNNERS), default="dataflow")
    p.add_argument("--source-dataset", default=DEFAULT_SOURCE_DATASET)
    p.add_argument("--source-messages", default=None)
    p.add_argument("--source-segments", default=None)
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
    env_experiment_id = os.environ.get("DIT_EXPERIMENT_ID") or None
    p.add_argument(
        "--experiment-id",
        type=_validate_experiment_id,
        default=(
            _validate_experiment_id(env_experiment_id)
            if env_experiment_id
            else _default_experiment_id()
        ),
        help="Slug prepended to the output-table suffix (<experiment_id>_<commit>_<uuid>) "
             "for cross-version run linkage. Env-var fallback DIT_EXPERIMENT_ID. "
             "Auto-default solo_<6-hex> when unset. Regex ^[a-z0-9][a-z0-9_-]{0,31}$. "
             "Bypassed entirely when --suffix is set.",
    )
    p.add_argument("--allow-dirty-tree", action="store_true")
    p.add_argument("--skip-pipelines", action="store_true")
    p.add_argument("--skip-comparisons", action="store_true")
    p.add_argument("--parallel", "--async", dest="parallel", action="store_true")
    p.add_argument("--dest-dataset", default=DEFAULT_DEST_DATASET,
                   help="BQ dataset for output tables; env-var fallback DIT_DEST_DATASET.")
    p.add_argument("--service-account", default=DEFAULT_DATAFLOW_SA)
    p.add_argument("--bq-temp-dataset", default=DEFAULT_BQ_TEMP_DATASET)
    p.add_argument("--dataflow-region", default=DEFAULT_DATAFLOW_REGION)
    p.add_argument("--dataflow-temp-bucket", default=DEFAULT_DATAFLOW_TEMP_BUCKET)
    p.add_argument("--dataflow-subnetwork", default=DEFAULT_DATAFLOW_SUBNETWORK)
    p.add_argument("--image-tag", default=DEFAULT_IMAGE_TAG,
                   help=f"Pipeline image tag (default: {DEFAULT_IMAGE_TAG}). "
                        "Docker uses build-from-source so this is informational; "
                        "Dataflow forwards it as sdk_container_image.")
    p.add_argument("--enable-pipeline-4", action="store_true")
    p.add_argument("--restricted-ssvids", default="")
    p.add_argument("--auto-restrict", action="store_true")
    p.add_argument("--auto-restrict-seed", type=int, default=42)
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(list(argv) if argv is not None else None)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    repo_dir = os.getcwd()
    suffix = _resolve_suffix(args, repo_dir)
    logger.info("experiment_id: %s", args.experiment_id)
    logger.info("Run suffix: %s", suffix)

    source_messages = args.source_messages or f"{args.source_dataset}.messages"
    source_segments = args.source_segments or f"{args.source_dataset}.segs_activity"

    base_cfg = dict(
        bq_input_messages=source_messages,
        bq_input_segments=source_segments,
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
    )

    base = f"{PROJECT}.{args.dest_dataset}.three_way_{suffix}"
    bf_table = f"{base}_1_bf"
    bfd_table = f"{base}_2_bfd"
    bft_table = f"{base}_3_bftruncate"
    mr_table = f"{base}_4_mutate_recover"

    logger.info("output tables:")
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
        bf_kwargs = dict(
            runner=args.runner, base_cfg=base_cfg, start=start, end=end, output=bf_table,
            image_tag=args.image_tag,
        )
        bfd_kwargs = dict(
            runner=args.runner, base_cfg=base_cfg, start=start, end=end,
            tail_days=args.tail_days, backfill_days_w=args.backfill_days,
            output=bfd_table, image_tag=args.image_tag,
        )
        bft_kwargs = dict(
            runner=args.runner, base_cfg=base_cfg, start=start, end=end,
            tail_days=args.tail_days, backfill_days_w=args.backfill_days,
            output=bft_table, image_tag=args.image_tag,
        )

        mr_restricted: Optional[tuple[str, ...]] = (
            explicit_restricted if (args.enable_pipeline_4 and explicit_restricted) else None
        )

        if args.parallel:
            can_parallel_p4 = args.enable_pipeline_4 and mr_restricted is not None
            max_workers = 4 if can_parallel_p4 else 3

            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futures = [
                    ex.submit(execute_bf, **bf_kwargs),
                    ex.submit(execute_bfd, **bfd_kwargs),
                    ex.submit(execute_bftruncate, **bft_kwargs),
                ]
                if can_parallel_p4:
                    futures.append(ex.submit(
                        execute_mutate_recover,
                        runner=args.runner, base_cfg=base_cfg,
                        start=start, end=end,
                        tail_days=args.tail_days, backfill_days_w=args.backfill_days,
                        output=mr_table,
                        restricted_ssvids=mr_restricted,
                        image_tag=args.image_tag,
                    ))
                for f in futures:
                    f.result()
        else:
            execute_bf(**bf_kwargs)
            execute_bfd(**bfd_kwargs)
            execute_bftruncate(**bft_kwargs)

        if args.enable_pipeline_4 and args.auto_restrict:
            mid = end - timedelta(days=args.tail_days)
            restricted_list = dit_bq.query_for_restricted_ssvids(
                bf_table,
                mid=mid,
                backfill_days_w=args.backfill_days,
                seed=args.auto_restrict_seed,
            )
            mr_restricted = tuple(restricted_list)
            execute_mutate_recover(
                runner=args.runner, base_cfg=base_cfg,
                start=start, end=end,
                tail_days=args.tail_days, backfill_days_w=args.backfill_days,
                output=mr_table,
                restricted_ssvids=mr_restricted,
                image_tag=args.image_tag,
            )
        elif args.enable_pipeline_4 and not args.parallel:
            execute_mutate_recover(
                runner=args.runner, base_cfg=base_cfg,
                start=start, end=end,
                tail_days=args.tail_days, backfill_days_w=args.backfill_days,
                output=mr_table,
                restricted_ssvids=mr_restricted,
                image_tag=args.image_tag,
            )

    if args.skip_comparisons:
        return 0

    pairs = [
        ("1_bf vs 2_bfd",         bf_table, bfd_table),
        ("1_bf vs 3_bftruncate",  bf_table, bft_table),
        ("2_bfd vs 3_bftruncate", bfd_table, bft_table),
    ]
    if args.enable_pipeline_4:
        pairs.append(("1_bf vs 4_mutate_recover", bf_table, mr_table))

    rcs = []
    for label, a, b in pairs:
        logger.info("=" * 80)
        logger.info("comparison: %s", label)
        logger.info("=" * 80)
        rcs.append(dit_compare.compare_tables(
            a, b, keys=COMPARE_KEYS, view_suffix=COMPARE_VIEW_SUFFIX,
        ))

    n_failed = sum(1 for rc in rcs if rc != 0)
    if n_failed:
        logger.error("%d/%d comparisons reported differences", n_failed, len(rcs))
        return 1
    logger.info("all %d comparisons passed", len(rcs))
    return 0


if __name__ == "__main__":
    sys.exit(main())
