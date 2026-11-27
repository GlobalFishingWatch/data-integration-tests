"""One pipe-gaps ``raw-gaps`` job, as both pipe-gaps workflows run it.

``mode_equivalence.py`` and ``outage_recovery.py`` run the same job with
different dates and inputs. Before 2026-11-27 each had its own copy of the
config builder, the CLI argv, the in-process pipeline builder and the runner
dispatch; the copies had not diverged (only a log line and an error message
differed), and porting both to pipe-gaps v0.17.1 would have edited the same
code twice. They live here now.

Written for **pipe-gaps v0.17.1** (:data:`DEFAULT_WORKER_IMAGE`), the version
production's AIS v5 DAG pins (composer-dags-production origin/main
b5c3496e1, task ``gaps.detect_raw_gaps``, read 2026-11-27). Read with
``git show v0.17.1:<path>`` in a pipe-gaps checkout:

* **Command and classes.** ``pipe-gaps raw-gaps``;
  ``pipe_gaps.pipelines.raw_gaps.config.RawGapsConfig`` (a gfw-common
  ``DatePipelineConfig``) and ``...raw_gaps.factory.RawGapsLinearDagFactory``.
  v0.10.0's ``detect`` package is gone, and so is DIT's support for it (owner
  decision 2026-11-27, as port visits dropped the v4 CLI on 2026-11-19): a
  pipe-gaps checkout older than v0.11.0 cannot be driven by these workflows.
* **Dates.** ``start_date`` inclusive, ``end_date`` exclusive
  (``DatePipelineConfig``; ``--start-date`` / ``--end-date`` on the CLI). The
  same half-open convention as v0.10.0's ``--date-range``, so no job's window
  moves.
* **Labels.** Required: ``PipelineConfig.labels`` is a dict and an empty one
  raises. gfw-common passes it to the Dataflow job (``Pipeline(labels=...)``)
  and the factory passes it to every BigQuery read as
  ``bigquery_job_labels``. It must not also sit in ``unknown_parsed_args``:
  ``PipelineFactory`` spreads both into one call.
* **Output.** ``bq_out_gaps`` names the latest-state VIEW. Rows are appended
  to the versioned table ``<bq_out_gaps>_<versioned_suffix>``
  (:func:`versioned_table`). The view is created by a post-hook, so only a
  job that finished; it keeps each gap's last version and drops closed gaps
  of ``duration_h <= min_gap_length``. Comparisons read the view; cleanup
  must drop both.
* **Open gaps between jobs.** The side input reads the still-open gaps from
  ``bq_in_open_gaps or bq_out_gaps_versioned`` (factory ``side_inputs``),
  from ``open_gaps_start_date`` on, whenever ``start_date >
  open_gaps_start_date`` and ``skip_open_gaps`` is off. DIT passes no
  ``bq_in_open_gaps``, so each job reads the open gaps the earlier jobs wrote
  to the same versioned table, after its own pre-write delete. That is the
  hand-over the daily modes and the outage stages rely on, and it is the same
  self-feed as v0.10.0 (which read ``bq_input_open_gaps or bq_output_gaps``).
* **Pre-write delete.** Unchanged from v0.10.0 (``gaps_delete.sql.j2``,
  ``GapsTableConfig.delete_query`` ignores the end date): see the reprocess
  contract in ``workflows/pipe_gaps/CLAUDE.md``.
* **Temp dataset.** Every BigQuery read (the messages source and the
  open-gaps side input) is built by ``ReadFromBigQuery.from_query(...,
  read_from_bigquery_kwargs=self.read_from_bigquery_kwargs)``;
  ``dit.runners.dataflow`` overrides that property to add ``temp_dataset``.

Fixed here, not flags (owner decisions 2026-11-27):

* ``eval_last`` true, passed explicitly: production's DAG makes it a required
  field;
* ``good_seg_stabilization_days`` 0: production passes 1, but the staging
  cohort does not change, so there is nothing to wait for;
* ``open_gaps_start_date`` 2012-01-01, production's value. Every DIT window
  starts later, so every job reads the open gaps.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import re
import uuid
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Optional

from dit.job_names import make_job_name
from dit.runners import dataflow as dit_dataflow
from dit.runners import docker as dit_docker
from dit.workflow import (
    WorkerSizing,
    max_num_workers_options,
    worker_sizing_options,
)

logger = logging.getLogger(__name__)

PROJECT = "world-fishing-827"
REPO_NAME = "pipe-gaps"

#: Pipeline repo name; used to namespace auto-snapshot refs
#: (refs/dit-snapshots/<PIPELINE_NAME>/<sha>).
PIPELINE_NAME = "pipe-gaps"

#: The pipe-gaps version these workflows are written for.
PIPELINE_VERSION = "v0.17.1"

#: DIT's id of the gap-detection step, in Dataflow job names, labels, the
#: drift spec and the orchestration declaration. Kept from v0.10.0, whose
#: command was ``detect``, as port visits kept its step ids across its CLI
#: change: existing job-name and label filters keep working.
STEP_NAME = "detect"

#: Entrypoint and command of the docker runner, as production's task runs it.
CLI_ENTRYPOINT = "pipe-gaps"
COMMAND = "raw-gaps"

#: Dataflow worker container image -- needs pipe_gaps installed (workers
#: unpickle DoFns from pipe_gaps.*). Production's AIS v5 DAG pins this tag;
#: it exists in the registry (``gcloud artifacts docker tags list``,
#: 2026-11-27: digest sha256:f29c4d01f870...). Override with --worker-image
#: for a custom build.
DEFAULT_WORKER_IMAGE = (
    "us-central1-docker.pkg.dev/gfw-int-infrastructure/core/pipe-gaps:"
    + PIPELINE_VERSION
)

#: Local image tag for the docker runner (built from the checkout's source).
DEFAULT_IMAGE_TAG = "gfw/pipe-gaps:dev"

#: ``RawGapsConfig.versioned_suffix``: the versioned table is
#: ``<bq_out_gaps>_<suffix>``. Production passes the same value.
VERSIONED_SUFFIX = "versioned"

#: ``RawGapsConfig.open_gaps_start_date``: production's value (its
#: ``data_available_from_date``).
OPEN_GAPS_START_DATE = "2012-01-01"

#: ``RawGapsConfig.eval_last``: evaluate each vessel's last message for an
#: open gap. pipe-gaps' default too, passed explicitly because production's
#: DAG made it a required field.
EVAL_LAST = True

#: ``RawGapsConfig.good_seg_stabilization_days``: 0 turns the check off.
#: Production passes 1 (segments must be a day ahead of the run). The staging
#: cohort does not change, so DIT does not wait (owner decision 2026-11-27);
#: an accepted difference in the drift spec.
GOOD_SEG_STABILIZATION_DAYS = 0

# Worker sizing, mirroring production (owner rule 2026-10-23; the worker cap
# does not follow it): the daily raw-gaps task, core_ais_v5.py
# gaps.detect_raw_gaps, --worker_machine_type=e2-custom-2-3072
# --disk_size_gb=25 (composer origin/main b5c3496e1, dags/core/ais/v5.py
# gaps_config.dataflow). Replaces gfw-common's own e2-standard-2 / 25 GB.
# Not mirrored: core.ais.v5.backfill's year-dependent choice (read at composer
# 0a644da on 2026-10-23: e2-highmem-2 from 2025 on, else e2-standard-2,
# 50 GB); one default cannot follow a per-year choice, so pass those values
# for a production-scale backfill. Hand-written until PIPELINE-4700 reads
# them from composer.
#
# gfw-common merges options by option name, so the machine type goes in as
# Beam's alias --machine_type: that parses to the same key as gfw-common's
# default and replaces it, rather than sitting beside it.
DEFAULT_WORKER_SIZING = WorkerSizing(machine_type="e2-custom-2-3072", disk_size_gb=25)
STEP_WORKER_SIZING = {STEP_NAME: DEFAULT_WORKER_SIZING}
GFW_COMMON_MACHINE_TYPE_OPTION = "--machine_type"

#: The attributes of a job's namespace that only the runner reads. They are
#: stripped before ``RawGapsConfig.from_namespace``, a frozen dataclass built
#: from ``vars(ns)``, which a stray attribute crashes.
RUNNER_ONLY_ATTRS = frozenset({
    "service_account",
    "bq_temp_dataset",
    "dataflow_region",
    "dataflow_temp_bucket",
    "dataflow_subnetwork",
    "worker_image",
    "job_name",
    "max_num_workers",
    "worker_sizing",
})


def versioned_table(view_fqn: str) -> str:
    """The versioned table behind the latest-state view ``view_fqn``
    (``RawGapsConfig.bq_out_gaps_versioned``)."""
    return f"{view_fqn}_{VERSIONED_SUFFIX}"


def output_tables(view_fqn: str) -> tuple[str, str]:
    """What one output writes: the view (compared, first) and its versioned
    table. Both go into the run-cache row, so ``cancel_run`` drops both."""
    return view_fqn, versioned_table(view_fqn)


def workflow_sha1(workflow_file: str | Path) -> str:
    """The run cache's ``workflow_file_sha1`` for a pipe-gaps workflow.

    Hashes the workflow file and this module: the job's config lives here, so
    an edit here must invalidate cached rows as an edit of the workflow does.
    """
    return hashlib.sha1(
        Path(workflow_file).read_bytes() + Path(__file__).read_bytes()
    ).hexdigest()


# --------------------------------------------------------------------------
# Labels, suffixes, job names
# --------------------------------------------------------------------------
# The dit_run_id label is the load-bearing one: `make dit-cancel` /
# dit.cache.cancel_run discover this run's Dataflow jobs by
# `labels.dit_run_id=<run_id>` (the runner never captures submitted job ids
# back into the cache row -- dataflow_job_ids is always []). The others are
# provenance niceties for ad-hoc filtering. They also label the BigQuery jobs
# of every read (pipe-gaps' read_from_bigquery_kwargs).

_UNSAFE_LABEL_CHAR_RE = re.compile(r"[^a-z0-9_-]")
_DIGEST_RE = re.compile(r"@sha\d{3}:[0-9a-f]+$", re.IGNORECASE)


def safe_label_value(value: str) -> str:
    """Coerce arbitrary strings into BQ-label-safe form (``[a-z0-9_-]``,
    max 63 chars). Matches port_visits/ais.py's sanitiser."""
    return _UNSAFE_LABEL_CHAR_RE.sub("-", value.lower())[:63]


def worker_image_tag(image: str) -> str:
    """Extract the tag portion of a docker image ref (digest stripped first):
    ``foo/bar:tag`` -> ``tag``; ``foo/bar@sha256:..`` -> ``latest``."""
    image = _DIGEST_RE.sub("", image)
    return image.rsplit(":", 1)[-1] if ":" in image else "latest"


def dit_run_labels(args: argparse.Namespace) -> list[str]:
    """Per-run static labels (``"key=value"`` strings), from the RunContext
    stamped on ``args``; every job of the run gets the same set."""
    return [
        f"dit_run_id={args.run_id}",
        f"dit_commit_sha={safe_label_value(args.pipeline_commit)}",
        f"dit_worker_image_tag={safe_label_value(worker_image_tag(args.worker_image))}",
        f"dit_launched_by={safe_label_value(os.environ.get('USER', 'unknown'))}",
    ]


def labels_dict(labels: Sequence[str]) -> dict[str, str]:
    """``["k=v", ...]`` -> ``{"k": "v", ...}``, the shape of
    ``PipelineConfig.labels``. Raises on an entry without ``=``."""
    def pair(entry: str) -> tuple[str, str]:
        key, sep, value = entry.partition("=")
        if not sep or not key:
            raise ValueError(f"label {entry!r} is not key=value")
        return key, value

    return dict(pair(entry) for entry in labels)


def resolve_suffix(args: argparse.Namespace) -> str:
    """The output-table suffix, from the already-resolved pipeline_commit:
    ``<experiment_id>_<commit>_<uuid>``. ``--suffix`` bypasses it."""
    if args.suffix is not None:
        return args.suffix
    return f"{args.experiment_id}_{args.pipeline_commit}_{uuid.uuid4().hex[:6]}"


def job_name(experiment_id: str, mode: str, iteration: int, total: int) -> str:
    """The dit-shaped Dataflow job name of one job of a mode."""
    return make_job_name(
        repo=REPO_NAME,
        step=STEP_NAME,
        experiment_id=experiment_id,
        mode=mode,
        iteration=iteration,
        total_iterations=total,
    )


# --------------------------------------------------------------------------
# One job: config, argv, pipeline
# --------------------------------------------------------------------------

def make_config(
    *,
    start: date,
    end: date,
    bq_in_messages: str,
    bq_in_segments: str,
    bq_out_gaps: str,
    ssvids: tuple[str, ...],
    min_gap_length: float,
    n_hours_before: int,
    window_period_d: int,
    filter_good_seg: bool,
    skip_open_gaps: bool,
    labels: Sequence[str],
    service_account: Optional[str] = None,
    bq_temp_dataset: Optional[str] = None,
    dataflow_region: Optional[str] = None,
    dataflow_temp_bucket: Optional[str] = None,
    dataflow_subnetwork: Optional[str] = None,
    worker_image: Optional[str] = None,
    job_name: Optional[str] = None,
    max_num_workers: Optional[int] = None,
    worker_sizing: WorkerSizing | None = None,
) -> SimpleNamespace:
    """The namespace of one raw-gaps job over ``[start, end)``.

    Its pipeline attributes are ``RawGapsConfig`` fields by name, so the
    Dataflow path builds the config from it directly; the docker path writes
    the same values as CLI flags (:func:`cfg_to_cli_flags`). ``bq_out_gaps``
    is the latest-state view. ``labels`` (``"key=value"`` strings) must not be
    empty: v0.17.1 requires them.
    """
    if not labels:
        raise ValueError("pipe-gaps v0.17.1 requires labels; pass at least dit_run_id.")
    cfg = SimpleNamespace(
        start_date=start,
        end_date=end,
        bq_in_messages=bq_in_messages,
        bq_in_segments=bq_in_segments,
        bq_out_gaps=bq_out_gaps,
        versioned_suffix=VERSIONED_SUFFIX,
        open_gaps_start_date=OPEN_GAPS_START_DATE,
        ssvids=tuple(ssvids),
        min_gap_length=min_gap_length,
        n_hours_before=n_hours_before,
        window_period_d=window_period_d,
        filter_good_seg=filter_good_seg,
        skip_open_gaps=skip_open_gaps,
        eval_last=EVAL_LAST,
        good_seg_stabilization_days=GOOD_SEG_STABILIZATION_DAYS,
        labels=labels_dict(labels),
        # Beam options; the runner's own (Dataflow placement, cap, sizing)
        # are merged in by build_pipeline_for.
        unknown_parsed_args={"project": PROJECT},
        unknown_unparsed_args=(),
    )
    cfg.service_account = service_account
    cfg.bq_temp_dataset = bq_temp_dataset
    cfg.dataflow_region = dataflow_region
    cfg.dataflow_temp_bucket = dataflow_temp_bucket
    cfg.dataflow_subnetwork = dataflow_subnetwork
    cfg.worker_image = worker_image
    cfg.job_name = job_name
    # Runner-only: emitted by run_pipeline (Dataflow only), stripped before
    # RawGapsConfig.from_namespace in build_pipeline_for.
    cfg.max_num_workers = max_num_workers
    cfg.worker_sizing = DEFAULT_WORKER_SIZING if worker_sizing is None else worker_sizing
    return cfg


def cfg_to_cli_flags(cfg: SimpleNamespace) -> list[str]:
    """The ``pipe-gaps raw-gaps`` flags of ``cfg`` (after the command word).

    Booleans are gfw-common ``--x`` / ``--no-x`` pairs: ``--filter-good-seg``
    and ``--skip-open-gaps`` only when on (both default off), ``--eval-last``
    always, as production passes it. ``--labels`` is repeated per pair, as
    production passes it.
    """
    def opt(name: str, value: object) -> list[str]:
        if value is None or value == "" or value == ():
            return []
        return [f"--{name}", str(value)]

    return [
        *opt("start-date", cfg.start_date.isoformat()),
        *opt("end-date", cfg.end_date.isoformat()),
        *opt("bq-in-messages", cfg.bq_in_messages),
        *opt("bq-in-segments", cfg.bq_in_segments),
        *opt("bq-out-gaps", cfg.bq_out_gaps),
        *opt("versioned-suffix", cfg.versioned_suffix),
        *opt("open-gaps-start-date", cfg.open_gaps_start_date),
        *opt("min-gap-length", cfg.min_gap_length),
        *opt("n-hours-before", cfg.n_hours_before),
        *opt("window-period-d", cfg.window_period_d),
        *(["--filter-good-seg"] if cfg.filter_good_seg else []),
        *(["--skip-open-gaps"] if cfg.skip_open_gaps else []),
        "--eval-last" if cfg.eval_last else "--no-eval-last",
        *opt("good-seg-stabilization-days", cfg.good_seg_stabilization_days),
        *opt("ssvids", ",".join(cfg.ssvids)),
        *(f"--labels={key}={value}" for key, value in cfg.labels.items()),
    ]


def docker_argv(cfg: SimpleNamespace) -> list[str]:
    """What the docker runner passes after the entrypoint."""
    return [COMMAND, *cfg_to_cli_flags(cfg), "--project", PROJECT]


def pipeline_config(cfg: SimpleNamespace, *, unknown_parsed_args: Mapping[str, Any] | None = None):
    """``RawGapsConfig`` of ``cfg``: its pipeline attributes, without the
    runner-only ones. ``unknown_parsed_args`` replaces the job's own Beam
    options when given. Needs pipe-gaps v0.17.x installed."""
    from pipe_gaps.pipelines.raw_gaps.config import RawGapsConfig

    attrs = {k: v for k, v in vars(cfg).items() if k not in RUNNER_ONLY_ATTRS}
    if unknown_parsed_args is not None:
        attrs["unknown_parsed_args"] = dict(unknown_parsed_args)
    return RawGapsConfig.from_namespace(SimpleNamespace(**attrs))


def check_job(cfg: SimpleNamespace) -> None:
    """Build the job's config and DAG offline, as a Dataflow submission would:
    validates the config and renders the queries. Touches no BigQuery and
    builds no Beam pipeline. Raises what pipe-gaps raises."""
    from pipe_gaps.pipelines.raw_gaps.factory import RawGapsLinearDagFactory

    RawGapsLinearDagFactory(pipeline_config(cfg)).build_dag()


def build_pipeline_for(cfg: SimpleNamespace):
    """Return a ``pipeline_builder`` closure for ``dit.runners.dataflow``.

    The runner contract is ``pipeline_builder(options) -> Pipeline``, where
    ``options`` is the Beam-options mapping the runner assembled (Dataflow
    placement, cap, sizing). When ``bq_temp_dataset`` is set, the runner has
    wrapped the DAG factory class to add ``temp_dataset`` to every read and
    passes the wrapped class as ``options["dag_factory_cls"]``.

    The closure merges ``options`` under the job's own Beam options, strips
    the runner-only attributes, builds ``RawGapsConfig`` from the namespace
    and the pipeline through gfw-common's ``PipelineFactory``.
    """
    def _build(options: Mapping[str, Any]):
        from gfw.common.beam.pipeline.factory import PipelineFactory

        from pipe_gaps.pipelines.raw_gaps.factory import RawGapsLinearDagFactory

        opts = dict(options)
        factory_cls = opts.pop("dag_factory_cls", RawGapsLinearDagFactory)
        opts.pop("bq_temp_dataset", None)

        parsed = dict(cfg.unknown_parsed_args)
        for key, value in opts.items():
            parsed.setdefault(key, value)
        if cfg.job_name:
            parsed.setdefault("job_name", cfg.job_name)

        config = pipeline_config(cfg, unknown_parsed_args=parsed)
        return PipelineFactory(config, dag_factory=factory_cls(config)).build_pipeline()

    return _build


#: Not a runner a user selects: ``--dry-run`` passes it to build each job's
#: config and DAG offline (:func:`check_job`) instead of running the job.
CHECK_RUNNER = "check"


def run_pipeline(runner: str, cfg: SimpleNamespace, image_tag: str) -> None:
    """Run one raw-gaps job with the chosen runner; raise if it fails.

    ``docker`` runs ``pipe-gaps raw-gaps`` with Beam's DirectRunner in the
    checkout's compose ``dev`` service, built from source. ``dataflow``
    builds the pipeline in-process and submits it (``dit.runners.dataflow``).
    :data:`CHECK_RUNNER` only builds the job's config and DAG offline.
    """
    logger.info(
        "[%s] start=%s end=%s msgs=%s out=%s",
        runner, cfg.start_date, cfg.end_date, cfg.bq_in_messages, cfg.bq_out_gaps,
    )
    if runner == CHECK_RUNNER:
        check_job(cfg)
        return

    if runner == "docker":
        rc = dit_docker.run(
            image_tag=image_tag,
            args=docker_argv(cfg),
            build_from_source=True,
            entrypoint=CLI_ENTRYPOINT,
        )
        if rc != 0:
            raise RuntimeError(f"docker runner exited with {rc} for {cfg.bq_out_gaps}")
        return

    if runner == "dataflow":
        # Workers pull from a registry; the local docker tag (image_tag) is
        # for the docker runner only. Require an explicit worker image --
        # silently falling back to image_tag reproduces the ImagePullBackOff
        # the split was meant to fix.
        if not cfg.worker_image:
            raise RuntimeError(
                "dataflow runner requires --worker-image (or a non-empty "
                "DEFAULT_WORKER_IMAGE). The docker runner's --image-tag "
                "is local-only and cannot be pulled by Dataflow workers."
            )
        from pipe_gaps.pipelines.raw_gaps.factory import RawGapsLinearDagFactory

        rc = dit_dataflow.run(
            # The worker cap and sizing, if any. They reach gfw-common's
            # Pipeline through the options mapping and override its own
            # defaults. Nothing for worker IPs: gfw-common's default is
            # already private (Pipeline.default_options use_public_ips=False).
            args=[
                *max_num_workers_options(cfg.max_num_workers),
                *worker_sizing_options(
                    cfg.worker_sizing, machine_type_option=GFW_COMMON_MACHINE_TYPE_OPTION,
                ),
            ],
            image_tag=cfg.worker_image,
            service_account=cfg.service_account,
            region=cfg.dataflow_region,
            temp_bucket=cfg.dataflow_temp_bucket,
            subnetwork=cfg.dataflow_subnetwork,
            bq_temp_dataset=cfg.bq_temp_dataset or "",
            pipeline_builder=build_pipeline_for(cfg),
            dag_factory_cls=RawGapsLinearDagFactory,
        )
        if rc != 0:
            raise RuntimeError(f"dataflow runner exited with {rc} for {cfg.bq_out_gaps}")
        return

    raise ValueError(f"unknown runner: {runner}")
