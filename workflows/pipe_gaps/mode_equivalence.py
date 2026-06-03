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
import sys
import uuid
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from types import SimpleNamespace
from typing import Any, Optional

from dit import bq as dit_bq
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

# Pipeline repo name; used to namespace auto-snapshot refs
# (refs/dit-snapshots/<PIPELINE_NAME>/<sha>).
PIPELINE_NAME = "pipe-gaps"

# Compute once at import time; the dit-side cache buster.
WORKFLOW_FILE_SHA1 = sha1_of_workflow_file(__file__)

logger = logging.getLogger(__name__)


PROJECT = "world-fishing-827"
REPO_NAME = "pipe-gaps"

# Step label for Dataflow job names. Pipe-gaps' detect pipeline is a single
# step, but the dit-shaped naming keeps the slot for symmetry with multi-step
# workflows like port_visits/ais.py (which has thin + visits).
STEP_NAME = "detect"

# Mode labels embedded in Dataflow job names + the bq_output_gaps table
# suffix. Single source of truth; execute_* helpers thread them through.
MODE_BF = "1_bf"
MODE_BFD = "2_bfd"
MODE_BFTRUNCATE = "3_bftruncate"
MODE_MUTATE_RECOVER = "4_mutate_recover"

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

DEFAULT_MIN_GAP_LENGTH = 1.0
DEFAULT_N_HOURS_BEFORE = 12
DEFAULT_WINDOW_PERIOD_D = 2
DEFAULT_FILTER_GOOD_SEG = True
DEFAULT_BACKFILL_DAYS_W = 4

DEFAULT_START = "2020-01-01"
DEFAULT_END = "2021-01-01"
DEFAULT_TAIL_DAYS = 4

DEFAULT_IMAGE_TAG = "gfw/pipe-gaps:dev"

# Dataflow worker container image -- needs pipe_gaps installed (workers
# unpickle DoFns from pipe_gaps.*). Published path in GFW's Artifact Registry.
# Distinct from DEFAULT_IMAGE_TAG (which names the local image the docker
# runner builds + runs for the submission process). Override via --worker-image
# when you've published a custom build (e.g. cross-version testing).
DEFAULT_WORKER_IMAGE = (
    "us-central1-docker.pkg.dev/gfw-int-infrastructure/core/pipe-gaps:v0.10.0"
)

RUNNERS = ("docker", "dataflow")

COMPARE_KEYS = ("gap_id", "start_timestamp")
COMPARE_VIEW_SUFFIX = "_last_versions"


# --------------------------------------------------------------------------
# Dataflow labels
# --------------------------------------------------------------------------
# Mirrors port_visits/ais.py's per-run static labels. The dit_run_id label is
# the load-bearing one: `make dit-cancel` / dit.cache.cancel_run discover this
# run's Dataflow jobs by `labels.dit_run_id=<run_id>` (the runner never
# captures submitted job ids back into the cache row -- dataflow_job_ids is
# always []). The others are provenance niceties for ad-hoc filtering.
#
# Unlike port-visits (which forwards labels as --labels=k=v CLI flags to the
# in-container pipe-anchorages process), pipe-gaps submits Dataflow in-process
# via dit.runners.dataflow, so labels are threaded as a `labels` list in
# unknown_parsed_args -> Beam's GoogleCloudOptions.labels (a list of "k=v").

_UNSAFE_LABEL_CHAR_RE = re.compile(r"[^a-z0-9_-]")


def _safe_label_value(value: str) -> str:
    """Coerce arbitrary strings into BQ-label-safe form (``[a-z0-9_-]``,
    max 63 chars). Matches port_visits/ais.py's sanitiser."""
    return _UNSAFE_LABEL_CHAR_RE.sub("-", value.lower())[:63]


def _dit_run_labels(args: argparse.Namespace) -> list[str]:
    """Per-run static Dataflow labels (``"key=value"`` strings).

    Computed once from the resolved RunContext stamped on ``args``; the same
    set is applied to every mode's Dataflow job so cleanup-by-label sweeps the
    whole run. Free-form values pass through ``_safe_label_value`` for BQ's
    label constraints; the hex ``run_id`` is already label-safe.
    """
    return [
        f"dit_run_id={args.run_id}",
        f"dit_commit_sha={_safe_label_value(args.pipeline_commit)}",
        f"dit_worker_image_tag={_safe_label_value(_worker_image_tag(args.worker_image))}",
        f"dit_launched_by={_safe_label_value(os.environ.get('USER', 'unknown'))}",
    ]


_DIGEST_RE = re.compile(r"@sha\d{3}:[0-9a-f]+$", re.IGNORECASE)


def _worker_image_tag(image: str) -> str:
    """Extract the tag portion of a docker image ref (digest stripped first).

    Mirrors port_visits/ais.py._worker_image_tag: ``foo/bar:tag`` -> ``tag``;
    ``foo/bar@sha256:..`` -> ``latest``.
    """
    image = _DIGEST_RE.sub("", image)
    return image.rsplit(":", 1)[-1] if ":" in image else "latest"


def _resolve_suffix(args: argparse.Namespace) -> str:
    """Build the output-table suffix from the already-resolved pipeline_commit.

    ``main()`` resolves ``args.pipeline_commit`` once (via
    :func:`dit.snapshot.resolve_pipeline_commit`, which auto-snapshots a dirty
    dataflow run) before calling this, so the suffix references the same
    committed ref the cache records. ``--suffix`` still bypasses everything.
    """
    if args.suffix is not None:
        return args.suffix
    # Every run now executes a committed ref (real or snapshot), so the suffix
    # is always <experiment_id>_<commit>_<uuid> -- the legacy `_dirty` marker
    # was dropped in M-pivot-4 (provenance lives in pipeline_commit /
    # unreviewed_code, not the table name).
    return f"{args.experiment_id}_{args.pipeline_commit}_{uuid.uuid4().hex[:6]}"


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
    # `labels` (a list of "key=value" strings) is seeded into unknown_parsed_args
    # so it flows into Beam's GoogleCloudOptions.labels via the PipelineFactory
    # (which spreads **unknown_parsed_args into the Pipeline constructor). The
    # dit_run_id label is what make dit-cancel discovers the run's jobs by.
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

        # Workers pull from a registry; the local docker tag (image_tag) is
        # for the in-process submission image only. Require an explicit
        # worker image -- silently falling back to image_tag reproduces the
        # original ImagePullBackOff this split was meant to fix.
        if not cfg.worker_image:
            raise RuntimeError(
                "dataflow runner requires --worker-image (or a non-empty "
                "DEFAULT_WORKER_IMAGE). The docker runner's --image-tag "
                "is local-only and cannot be pulled by Dataflow workers."
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


def _job_name(experiment_id: str, mode: str, iteration: int, total: int) -> str:
    """Workflow-local shorthand for the dit-shaped Dataflow job name."""
    return make_job_name(
        repo=REPO_NAME,
        step=STEP_NAME,
        experiment_id=experiment_id,
        mode=mode,
        iteration=iteration,
        total_iterations=total,
    )


def execute_bf(
    runner: str, *, base_cfg: dict, start: date, end: date, output: str,
    experiment_id: str, image_tag: str,
) -> None:
    cfg = _make_config(
        start=start, end=end, bq_output_gaps=output,
        job_name=_job_name(experiment_id, MODE_BF, 1, 1),
        **base_cfg,
    )
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
    experiment_id: str,
    image_tag: str,
) -> None:
    mid = end - timedelta(days=tail_days)
    daily_ends = list(daterange_inclusive(mid + timedelta(days=1), end + timedelta(days=1)))
    total = 1 + len(daily_ends)  # initial big slice + N dailies

    cfg = _make_config(
        start=start, end=mid, bq_output_gaps=output,
        job_name=_job_name(experiment_id, MODE_BFD, 1, total),
        **base_cfg,
    )
    _run_pipeline(runner, cfg, image_tag)

    for i, day_end in enumerate(daily_ends, start=2):
        day_start = day_end - timedelta(days=backfill_days_w)
        cfg = _make_config(
            start=day_start, end=day_end, bq_output_gaps=output,
            job_name=_job_name(experiment_id, MODE_BFD, i, total),
            **base_cfg,
        )
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
    experiment_id: str,
    image_tag: str,
) -> None:
    tail_start = end - timedelta(days=tail_days)
    daily_ends = list(daterange_inclusive(tail_start + timedelta(days=1), end + timedelta(days=1)))
    total = 1 + len(daily_ends)  # initial full slice + N tail-day re-runs

    cfg = _make_config(
        start=start, end=end, bq_output_gaps=output,
        job_name=_job_name(experiment_id, MODE_BFTRUNCATE, 1, total),
        **base_cfg,
    )
    _run_pipeline(runner, cfg, image_tag)

    for i, day_end in enumerate(daily_ends, start=2):
        day_start = day_end - timedelta(days=backfill_days_w)
        cfg = _make_config(
            start=day_start, end=day_end, bq_output_gaps=output,
            job_name=_job_name(experiment_id, MODE_BFTRUNCATE, i, total),
            **base_cfg,
        )
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
    experiment_id: str,
    image_tag: str,
) -> None:
    if not restricted_ssvids:
        raise ValueError(
            "execute_mutate_recover requires a non-empty restricted_ssvids tuple."
        )

    mid = end - timedelta(days=tail_days)
    daily_ends = list(daterange_inclusive(mid + timedelta(days=1), end + timedelta(days=1)))
    # 1 initial slice + N restricted dailies + N recovery dailies.
    total = 1 + 2 * len(daily_ends)

    cfg = _make_config(
        start=start, end=mid, bq_output_gaps=output,
        job_name=_job_name(experiment_id, MODE_MUTATE_RECOVER, 1, total),
        **base_cfg,
    )
    _run_pipeline(runner, cfg, image_tag)

    restricted_cfg = {**base_cfg, "ssvids": restricted_ssvids}
    iteration = 2
    for day_end in daily_ends:
        day_start = day_end - timedelta(days=backfill_days_w)
        cfg = _make_config(
            start=day_start, end=day_end, bq_output_gaps=output,
            job_name=_job_name(experiment_id, MODE_MUTATE_RECOVER, iteration, total),
            **restricted_cfg,
        )
        _run_pipeline(runner, cfg, image_tag)
        iteration += 1

    for day_end in daily_ends:
        day_start = day_end - timedelta(days=backfill_days_w)
        cfg = _make_config(
            start=day_start, end=day_end, bq_output_gaps=output,
            job_name=_job_name(experiment_id, MODE_MUTATE_RECOVER, iteration, total),
            **base_cfg,
        )
        _run_pipeline(runner, cfg, image_tag)
        iteration += 1


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
    skip_pipelines) — none of which affect the output content.
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
        execute_kwargs=execute_kwargs,
        log_label=mode,
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
    p.add_argument("--require-clean", action="store_true",
                   help="Error on a dirty tree instead of auto-snapshotting "
                        "(for CI / strict-provenance callers).")
    p.add_argument("--skip-pipelines", action="store_true")
    p.add_argument("--skip-comparisons", action="store_true")
    p.add_argument("--parallel", "--async", dest="parallel", action="store_true")
    add_infra_args(p)
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
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(list(argv) if argv is not None else None)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

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

    source_messages = args.source_messages
    source_segments = args.source_segments

    # Per-run static Dataflow labels (incl. the dit_run_id cleanup key),
    # shared by every mode's job. Threaded into the dataflow runner via cfg ->
    # unknown_parsed_args -> GoogleCloudOptions.labels.
    dit_labels = _dit_run_labels(args)
    logger.info("dit labels: %s", dit_labels)

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
        worker_image=args.worker_image or None,
        labels=dit_labels,
    )

    base = f"{PROJECT}.{args.dest_dataset}.three_way_{suffix}"
    bf_table = f"{base}_{MODE_BF}"
    bfd_table = f"{base}_{MODE_BFD}"
    bft_table = f"{base}_{MODE_BFTRUNCATE}"
    mr_table = f"{base}_{MODE_MUTATE_RECOVER}"

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

        if args.parallel:
            can_parallel_p4 = args.enable_pipeline_4 and mr_restricted is not None
            max_workers = 4 if can_parallel_p4 else 3

            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                f_bf = ex.submit(_wrap_bf)
                f_bfd = ex.submit(_wrap_bfd)
                f_bft = ex.submit(_wrap_bft)
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
                bf_table = f_bf.result()
                bfd_table = f_bfd.result()
                bft_table = f_bft.result()
                if f_mr is not None:
                    mr_table = f_mr.result()
        else:
            bf_table = _wrap_bf()
            bfd_table = _wrap_bfd()
            bft_table = _wrap_bft()

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

    pairs = [
        (f"{MODE_BF} vs {MODE_BFD}",         bf_table, bfd_table),
        (f"{MODE_BF} vs {MODE_BFTRUNCATE}",  bf_table, bft_table),
        (f"{MODE_BFD} vs {MODE_BFTRUNCATE}", bfd_table, bft_table),
    ]
    if args.enable_pipeline_4:
        pairs.append((f"{MODE_BF} vs {MODE_MUTATE_RECOVER}", bf_table, mr_table))

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
