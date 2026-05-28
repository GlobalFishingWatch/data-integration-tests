"""Mode equivalence integration test for pipe-anchorages port-visits (AIS staging).

Drives the two-step port-visits pipeline (``thin_port_messages`` -> ``port_visits``)
three different ways ("modes") and asserts all three produce identical output on
the per-mode ``port_visits_..._<mode>`` BQ tables. Any divergence between modes
signals non-determinism in the pipeline or a bug in the partitioned-write /
re-run logic.

Modes (each gets its own UUID-suffixed output table; compared pairwise at the
end):

* ``_1_bf``: a single thin+visits over ``[start, end]``. Range-mode oracle.
* ``_2_bfd``: thin+visits ending ``tail_days`` short, then per-day thin+visits
  for each tail day. Mirrors steady-state daily reprocessing.
* ``_3_bftruncate``: a full thin+visits over the whole range, then the same
  per-day re-runs as ``_2_bfd``. Tests that re-running already-processed days
  truncates partitions cleanly.

There is no ``_4_mutate_recover`` mode here -- that was pipe-gaps' Bug A
trigger and doesn't apply to port-visits.

Date semantics: ``--start`` and ``--end`` are **inclusive on both ends**,
matching the pipe-anchorages CLI. (Pipe-gaps' workflow uses half-open dates,
matching detect-gaps' CLI; the wart is unavoidable given the downstream
tools' contracts.)

The default ``--runner=dataflow`` submits to Dataflow from inside the
container (the same pattern composer uses). ``--runner=docker`` runs
DirectRunner inside the container for fast local sanity checks. Both go
through ``dit.runners.docker`` -- the Dataflow submission is opaque to the
runner, which only owns the docker process and waits for it via
``--wait_for_job`` inside the pipeline.

This is the **abstraction-validation step** for Phase 2: first real exercise
of ``dit.compare.compare_tables(view_suffix="", keys=["visit_id"])``
(truncate shape, not SCD-2) and the docker runner's ``entrypoint`` extension
(pipe-anchorages' dev image needs ``--entrypoint pipe-anchorages``).
"""
from __future__ import annotations

import argparse
import concurrent.futures
import itertools
import logging
import os
import re
import sys
import uuid
from datetime import date, datetime, timedelta
from typing import Optional, Sequence

from dit import compare as dit_compare
from dit import dates as dit_dates
from dit.git_info import git_info
from dit.job_names import make_job_name
from dit.runners import docker as dit_docker
from dit.snapshot import resolve_pipeline_commit

logger = logging.getLogger(__name__)

# Pipeline repo name; namespaces auto-snapshot refs
# (refs/dit-snapshots/<PIPELINE_NAME>/<sha>).
PIPELINE_NAME = "anchorages_pipeline"


# --------------------------------------------------------------------------
# Constants / defaults
# --------------------------------------------------------------------------

PROJECT = "world-fishing-827"
REPO_NAME = "anchorages-pipeline"

# Short step labels for Dataflow job names (the cap at 63 chars constrains
# composition). The full step name is used in the dit_step label, which has
# more room.
_JOB_STEP_NAMES = {
    "thin_port_messages": "thin",
    "port_visits": "visits",
}

# Per-user infra knobs: defaults below, override via DIT_* env vars or CLI flags.
DEFAULT_DEST_DATASET = os.environ.get("DIT_DEST_DATASET", "tech_great_expectations")
DEFAULT_DATAFLOW_SA = os.environ.get(
    "DIT_DATAFLOW_SA", "automated-testing@world-fishing-827.iam.gserviceaccount.com"
)
DEFAULT_DATAFLOW_REGION = os.environ.get("DIT_DATAFLOW_REGION", "us-central1")
DEFAULT_DATAFLOW_TEMP_BUCKET = os.environ.get("DIT_DATAFLOW_TEMP_BUCKET", "pipe-temp-us-central-ttl7")
DEFAULT_DATAFLOW_SUBNETWORK = os.environ.get(
    "DIT_DATAFLOW_SUBNETWORK", "regions/us-central1/subnetworks/gfw-internal-us-central1"
)
# Pre-existing BQ dataset Beam uses as its temp dataset for ReadFromBigQuery
# EXPORT staging; lets the SA skip bigquery.datasets.create. Inherits
# ${PROJECT}.${DIT_DEST_DATASET} unless overridden.
DEFAULT_BQ_TEMP_DATASET = os.environ.get(
    "DIT_BQ_TEMP_DATASET", f"{PROJECT}.{DEFAULT_DEST_DATASET}"
)

# Workflow-specific defaults (no env var; one-off overrides via CLI flag).
# Staging cohort: 2020-01-01 -> 2020-12-31, reduced AIS data.
DEFAULT_SOURCE_DATASET_STEM = "pipe_ais_test_202408290000"

# Global anchorages reference (not staging-specific).
DEFAULT_NAMED_ANCHORAGES = f"{PROJECT}.anchorages.named_anchorages_v20240117"

DEFAULT_START = "2020-01-01"   # inclusive
DEFAULT_END = "2020-12-31"     # inclusive
DEFAULT_TAIL_DAYS = 3

DEFAULT_IMAGE_TAG = "gfw/pipe-anchorages:v4.6.4"

# Dataflow worker container image -- needs pipe_anchorages installed (workers
# unpickle DoFns from pipe_anchorages.*). Published path in GFW's Artifact
# Registry. Distinct from DEFAULT_IMAGE_TAG (which names the local image
# docker run uses for the submission process).
DEFAULT_WORKER_IMAGE = (
    "us-central1-docker.pkg.dev/gfw-int-infrastructure/core/pipe-anchorages:v4.6.4"
)

# Comparison contract for port-visits (truncate shape, no SCD-2).
COMPARE_KEYS = ("visit_id",)
COMPARE_VIEW_SUFFIX = ""

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


# --------------------------------------------------------------------------
# Suffix
# --------------------------------------------------------------------------

def _resolve_suffix(args: argparse.Namespace) -> str:
    """Build the output-table suffix from the already-resolved commit.

    ``main()`` resolves ``args.commit_sha`` once (auto-snapshotting a dirty
    dataflow run via dit.snapshot) before calling this. Every run executes a
    committed ref (real or snapshot), so the suffix is always
    ``<experiment_id>_<commit>_<uuid>`` -- the legacy ``_dirty`` marker was
    dropped in M-pivot-4. ``--suffix`` bypasses everything.
    """
    if args.suffix:
        return args.suffix
    return f"{args.experiment_id}_{args.commit_sha}_{uuid.uuid4().hex[:6]}"


# --------------------------------------------------------------------------
# Beam pipeline-options assembly
# --------------------------------------------------------------------------

_DIGEST_RE = re.compile(r"@sha\d{3}:[0-9a-f]+$", re.IGNORECASE)
_UNSAFE_LABEL_CHAR_RE = re.compile(r"[^a-z0-9_-]")


def _worker_image_tag(image: str) -> str:
    """Extract the tag portion of a docker image ref.

    Handles both shapes:
      foo/bar:tag                      -> "tag"
      foo/bar:tag@sha256:abc...        -> "tag"  (digest stripped first)
      foo/bar@sha256:abc...            -> "latest"  (digest-only, no tag)
      foo/bar                          -> "latest"  (no tag, no digest)
    """
    # Strip any trailing @<digest> before extracting the tag, otherwise the
    # final ':' in the digest gets mistaken for a tag separator.
    image = _DIGEST_RE.sub("", image)
    return image.rsplit(":", 1)[-1] if ":" in image else "latest"


def _safe_label_value(value: str) -> str:
    """Coerce arbitrary strings into BQ-label-safe form.

    BQ label values are restricted to ASCII ``[a-z0-9_-]`` (max 63 chars).
    Non-ASCII letters that ``str.isalnum()`` accepts are NOT allowed by BQ,
    so we use an explicit ASCII-only character-class via regex.
    """
    return _UNSAFE_LABEL_CHAR_RE.sub("-", value.lower())[:63]


def _make_job_name(
    args: argparse.Namespace,
    *,
    step: str,
    mode: str,
    iteration: int,
    total_iterations: int,
) -> str:
    """Thin adapter over ``dit.job_names.make_job_name`` that pulls the
    workflow-specific repo + step abbreviation from local constants and
    threads the ``args`` namespace's experiment_id/binding_name through."""
    return make_job_name(
        repo=REPO_NAME,
        step=_JOB_STEP_NAMES.get(step, step),
        experiment_id=args.experiment_id,
        mode=mode,
        binding=args.binding_name or None,
        iteration=iteration,
        total_iterations=total_iterations,
    )


def _dynamic_labels(
    args: argparse.Namespace,
    *,
    step: str,
    mode: str,
    iteration: int,
    total_iterations: int,
    slice_start: date,
    slice_end: date,
) -> list[str]:
    """Per-invocation labels. Propagated by Beam to the Dataflow job and (via
    pipe-anchorages' BigQueryHelper) to every BQ table written in this run.

    Mix of per-run statics (set once in main(): run_id, commit_sha, worker
    image tag, launched_by) and per-iteration values (slice dates, iteration
    counter).

    Free-form-text values (experiment_id, commit_sha, worker_image_tag,
    launched_by, binding_name) pass through ``_safe_label_value()`` to enforce
    BQ's ``[a-z0-9_-]{1,63}`` constraint. Values that are statically guaranteed
    label-safe (``REPO_NAME``, ``step``, ``mode``, integer counters,
    ISO-date strings, hex ``run_id``) skip the sanitiser for clarity."""
    labels = [
        f"--labels=dit_repo={REPO_NAME}",
        f"--labels=dit_step={step}",
        f"--labels=dit_experiment_id={_safe_label_value(args.experiment_id)}",
        f"--labels=dit_mode={mode}",
        f"--labels=dit_iteration={iteration}",
        f"--labels=dit_total_iterations={total_iterations}",
        f"--labels=dit_slice_start={slice_start.isoformat()}",
        f"--labels=dit_slice_end={slice_end.isoformat()}",
        f"--labels=dit_run_id={args.run_id}",
        f"--labels=dit_commit_sha={_safe_label_value(args.commit_sha)}",
        f"--labels=dit_worker_image_tag={_safe_label_value(args.worker_image_tag)}",
        f"--labels=dit_launched_by={_safe_label_value(args.launched_by)}",
    ]
    if args.binding_name:
        labels.append(f"--labels=dit_binding={_safe_label_value(args.binding_name)}")
    return labels


def _dataflow_pipeline_options(
    args: argparse.Namespace,
    *,
    step: str,
    mode: str,
    iteration: int,
    total_iterations: int,
    slice_start: date,
    slice_end: date,
) -> list[str]:
    return [
        "--runner=DataflowRunner",
        f"--project={PROJECT}",
        f"--region={args.dataflow_region}",
        f"--service_account_email={args.service_account}",
        f"--temp_location=gs://{args.dataflow_temp_bucket}/dataflow_temp",
        f"--staging_location=gs://{args.dataflow_temp_bucket}/dataflow_staging",
        f"--subnetwork={args.dataflow_subnetwork}",
        f"--sdk_container_image={args.worker_image}",
        f"--job_name={_make_job_name(args, step=step, mode=mode, iteration=iteration, total_iterations=total_iterations)}",
        "--wait_for_job",
        # pipe-anchorages requires --labels to be non-None
        # (cloud_to_labels in transforms/sink.py iterates without a None guard).
        # Mirrors composer's LabelsConfig.as_dataflow_cli_arguments shape.
        "--labels=environment=integration_test",
        "--labels=resource_creator=dit",
        "--labels=project=core_pipeline",
        "--labels=workflow=port_visits_ais",
        "--labels=stage=testing",
        *_dynamic_labels(
            args, step=step, mode=mode,
            iteration=iteration, total_iterations=total_iterations,
            slice_start=slice_start, slice_end=slice_end,
        ),
    ]


def _directrunner_pipeline_options() -> list[str]:
    return ["--runner=DirectRunner", "--wait_for_job"]


def _pipeline_options(
    args: argparse.Namespace,
    *,
    step: str,
    mode: str,
    iteration: int,
    total_iterations: int,
    slice_start: date,
    slice_end: date,
) -> list[str]:
    return (
        _dataflow_pipeline_options(
            args, step=step, mode=mode,
            iteration=iteration, total_iterations=total_iterations,
            slice_start=slice_start, slice_end=slice_end,
        ) if args.runner == "dataflow"
        else _directrunner_pipeline_options()
    )


# --------------------------------------------------------------------------
# Table names
# --------------------------------------------------------------------------

def _internal_dataset(stem: str) -> str:
    return f"{stem}_internal"


def _published_dataset(stem: str) -> str:
    return f"{stem}_published"


def _messages_table(args: argparse.Namespace) -> str:
    return f"{PROJECT}.{_internal_dataset(args.source_dataset_stem)}.messages_positions"


def _segment_info_table(args: argparse.Namespace) -> str:
    return f"{PROJECT}.{_published_dataset(args.source_dataset_stem)}.segment_info"


def _bad_segs_sql(args: argparse.Namespace) -> str:
    return (
        f"(SELECT DISTINCT seg_id "
        f"FROM `{PROJECT}.{_published_dataset(args.source_dataset_stem)}.segs_activity` "
        f"WHERE overlapping_and_short)"
    )


def _thinned_table(args: argparse.Namespace, suffix: str, mode: str) -> str:
    return f"{PROJECT}.{args.dest_dataset}.port_events_{suffix}_{mode}"


def _visits_table(args: argparse.Namespace, suffix: str, mode: str) -> str:
    return f"{PROJECT}.{args.dest_dataset}.port_visits_{suffix}_{mode}"


# --------------------------------------------------------------------------
# Slice runner: thin -> visits
# --------------------------------------------------------------------------

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
    """One slice = one thin_port_messages call + one port_visits call.

    port_visits' --start_date is the workflow's --start (pipeline-level
    data_available_from); it does a full recompute over [start, slice_end]
    on every call, matching production semantics.

    ``iteration`` is 1-indexed within the mode; ``total_iterations`` is the
    number of slices the mode will run. Both flow into the Dataflow job name
    (as ``-N-M`` suffix) and BQ labels for per-iteration provenance.

    When ``args.thinned_message_table`` is set, step 1 is SKIPPED and step 2
    reads from that table instead of the per-mode ``port_events_<suffix>_<mode>``
    table the workflow would normally produce. This is the supported way to
    run dit against a change that lives only in step 2 -- avoids re-thinning
    AIS data that step 1 has already produced upstream (e.g. in prod).
    """
    if args.thinned_message_table:
        logger.info(
            "thin_port_messages SKIPPED for mode=%s slice=[%s, %s] iter=%d/%d -- using external table %s",
            mode, slice_start, slice_end, iteration, total_iterations,
            args.thinned_message_table,
        )
        thinned_input = args.thinned_message_table
    else:
        thin_args = [
            "thin_port_messages",
            f"--start_date={slice_start.isoformat()}",
            f"--end_date={slice_end.isoformat()}",
            f"--anchorage_table={args.named_anchorages}",
            f"--input_table={_messages_table(args)}",
            f"--output_table={_thinned_table(args, suffix, mode)}",
            f"--temp_dataset={args.bq_temp_dataset}",
            *_pipeline_options(
                args, step="thin_port_messages", mode=mode,
                iteration=iteration, total_iterations=total_iterations,
                slice_start=slice_start, slice_end=slice_end,
            ),
        ]
        logger.info("thin_port_messages %s [%s, %s] iter=%d/%d",
                    mode, slice_start, slice_end, iteration, total_iterations)
        rc = dit_docker.run(
            args.image_tag, thin_args,
            entrypoint="pipe-anchorages",
            build_from_source=args.build_from_source,
        )
        if rc != 0:
            raise SystemExit(
                f"thin_port_messages failed (rc={rc}, mode={mode}, slice=[{slice_start}, {slice_end}])"
            )
        thinned_input = _thinned_table(args, suffix, mode)

    visits_args = [
        "port_visits",
        f"--start_date={args.start}",
        f"--end_date={slice_end.isoformat()}",
        f"--thinned_message_table={thinned_input}",
        f"--vessel_id_table={_segment_info_table(args)}",
        f"--output_table={_visits_table(args, suffix, mode)}",
        f"--bad_segs={_bad_segs_sql(args)}",
        f"--temp_dataset={args.bq_temp_dataset}",
        *_pipeline_options(
            args, step="port_visits", mode=mode,
            iteration=iteration, total_iterations=total_iterations,
            slice_start=_parse_date(args.start), slice_end=slice_end,
        ),
    ]
    logger.info("port_visits %s [%s, %s] iter=%d/%d",
                mode, args.start, slice_end, iteration, total_iterations)
    rc = dit_docker.run(
        args.image_tag, visits_args,
        entrypoint="pipe-anchorages",
        build_from_source=args.build_from_source,
    )
    if rc != 0:
        raise SystemExit(
            f"port_visits failed (rc={rc}, mode={mode}, slice_end={slice_end})"
        )


# --------------------------------------------------------------------------
# Modes
# --------------------------------------------------------------------------

def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def execute_bf(args: argparse.Namespace, suffix: str) -> None:
    start = _parse_date(args.start)
    end = _parse_date(args.end)
    _run_slice(args, mode="1_bf", slice_start=start, slice_end=end, suffix=suffix,
               iteration=1, total_iterations=1)


def execute_bfd(args: argparse.Namespace, suffix: str) -> None:
    start = _parse_date(args.start)
    end = _parse_date(args.end)
    initial_end = end - timedelta(days=args.tail_days)
    total = 1 + args.tail_days
    _run_slice(args, mode="2_bfd", slice_start=start, slice_end=initial_end, suffix=suffix,
               iteration=1, total_iterations=total)
    # daterange_inclusive is half-open per dit.dates contract; +1 day on end
    # to include the final `end` date.
    for i, d in enumerate(
        dit_dates.daterange_inclusive(initial_end + timedelta(days=1), end + timedelta(days=1)),
        start=2,
    ):
        _run_slice(args, mode="2_bfd", slice_start=d, slice_end=d, suffix=suffix,
                   iteration=i, total_iterations=total)


def execute_bftruncate(args: argparse.Namespace, suffix: str) -> None:
    start = _parse_date(args.start)
    end = _parse_date(args.end)
    total = 1 + args.tail_days
    _run_slice(args, mode="3_bftruncate", slice_start=start, slice_end=end, suffix=suffix,
               iteration=1, total_iterations=total)
    for i, d in enumerate(
        dit_dates.daterange_inclusive(end - timedelta(days=args.tail_days - 1), end + timedelta(days=1)),
        start=2,
    ):
        _run_slice(args, mode="3_bftruncate", slice_start=d, slice_end=d, suffix=suffix,
                   iteration=i, total_iterations=total)


# --------------------------------------------------------------------------
# Comparisons
# --------------------------------------------------------------------------

def compare_all(args: argparse.Namespace, suffix: str) -> int:
    modes = ["1_bf", "2_bfd", "3_bftruncate"]
    overall = 0
    for a, b in itertools.combinations(modes, 2):
        rc = dit_compare.compare_tables(
            _visits_table(args, suffix, a),
            _visits_table(args, suffix, b),
            keys=COMPARE_KEYS,
            view_suffix=COMPARE_VIEW_SUFFIX,
        )
        logger.info("compare %s vs %s -> rc=%s", a, b, rc)
        overall = overall or rc
    return overall


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Mode equivalence integration test for pipe-anchorages port-visits (AIS staging).",
    )
    p.add_argument("--runner", choices=["dataflow", "docker"], default="dataflow",
                   help="dataflow: submit DataflowRunner from inside the container (default). "
                        "docker: run DirectRunner inside the container (for local sanity checks).")
    p.add_argument("--dest-dataset", default=DEFAULT_DEST_DATASET,
                   help="BQ dataset for output tables; env-var fallback DIT_DEST_DATASET.")
    p.add_argument("--bq-temp-dataset", default=DEFAULT_BQ_TEMP_DATASET,
                   help="Pre-existing BQ dataset for Beam EXPORT staging; "
                        "env-var fallback DIT_BQ_TEMP_DATASET (defaults to "
                        "${PROJECT}.${DIT_DEST_DATASET}).")
    p.add_argument("--source-dataset-stem", default=DEFAULT_SOURCE_DATASET_STEM,
                   help=f"Staging dataset stem (default {DEFAULT_SOURCE_DATASET_STEM}); "
                        "_internal and _published are appended.")
    p.add_argument("--named-anchorages", default=DEFAULT_NAMED_ANCHORAGES)
    p.add_argument("--start", default=DEFAULT_START,
                   help="Inclusive start date (also pipeline data_available_from).")
    p.add_argument("--end", default=DEFAULT_END, help="Inclusive end date.")
    p.add_argument("--tail-days", type=int, default=DEFAULT_TAIL_DAYS,
                   help="Number of tail days for bfd / bftruncate iteration.")
    p.add_argument("--image-tag", default=DEFAULT_IMAGE_TAG)
    p.add_argument("--worker-image", default=DEFAULT_WORKER_IMAGE,
                   help="Dataflow worker container image with pipe_anchorages installed; "
                        "passed to Beam as --sdk_container_image.")
    p.add_argument("--build-from-source", action="store_true",
                   help="Fall back to `docker compose run dev` when no published image is available.")
    p.add_argument("--suffix", default=None,
                   help="Output-table suffix; auto-generated from git HEAD when omitted.")
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
    p.add_argument("--binding-name", default="",
                   help="Optional binding label (e.g. 'before', 'after') used by the "
                        "cross-version wrapper. Surfaces in Dataflow job names and BQ labels "
                        "(dit_binding=<name>). Empty when running standalone.")
    p.add_argument("--thinned-message-table", default=None,
                   help="Fully-qualified BQ table holding pre-thinned port messages. When set, "
                        "step 1 (thin_port_messages) is SKIPPED and step 2 (port_visits) reads "
                        "directly from this table instead of the per-mode port_events_<suffix>_<mode> "
                        "the workflow would otherwise produce. Useful when the change under test "
                        "is in step 2 only (e.g. PORT_GAP_BEGIN anchorage fixes) -- saves the "
                        "dominant cost of running the thin step on full AIS data. Cross-version "
                        "runs should provide a snapshotted FQN (cross_version_ais.py pins this "
                        "automatically when given a prod-side FQN).")
    p.add_argument("--require-clean", action="store_true",
                   help="Error on a dirty tree instead of auto-snapshotting "
                        "(for CI / strict-provenance callers).")
    p.add_argument("--skip-pipelines", action="store_true",
                   help="Skip the pipeline phase; only run comparisons.")
    p.add_argument("--skip-comparisons", action="store_true",
                   help="Skip the comparison phase; only run pipelines.")
    p.add_argument("--parallel", action="store_true",
                   help="Run the three modes' pipelines in parallel threads (each submits Dataflow concurrently).")
    # Dataflow knobs
    p.add_argument("--service-account", default=DEFAULT_DATAFLOW_SA)
    p.add_argument("--dataflow-region", default=DEFAULT_DATAFLOW_REGION)
    p.add_argument("--dataflow-temp-bucket", default=DEFAULT_DATAFLOW_TEMP_BUCKET)
    p.add_argument("--dataflow-subnetwork", default=DEFAULT_DATAFLOW_SUBNETWORK)
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)

    if args.tail_days < 0:
        raise SystemExit(
            f"--tail-days must be >= 0; got {args.tail_days}. "
            "Negative values produce nonsensical iteration counts and invert "
            "the daily-tail date range."
        )

    repo_dir = os.getcwd()

    # Resolve the committed ref this run records (no-dirty-tree policy).
    # --suffix is the manual / cross-version escape hatch: when set, record git
    # state as-is (cross_version_ais.py runs committed worktree refs and owns
    # provenance). Otherwise dirty + --runner=dataflow auto-snapshots + pushes;
    # the cloud path supplies DIT_PIPELINE_COMMIT instead.
    if args.suffix:
        commit_sha, dirty = git_info(repo_dir)
    else:
        commit_sha, dirty = resolve_pipeline_commit(
            repo_dir, PIPELINE_NAME, runner=args.runner, require_clean=args.require_clean,
        )
    args.commit_sha = commit_sha
    args.dirty = dirty

    suffix = _resolve_suffix(args)

    # Per-invocation lineage attributes, computed once and stashed on args so
    # every Dataflow job + BQ output table from this run shares them. See
    # the dit_run_id / dit_commit_sha / dit_worker_image_tag / dit_launched_by
    # labels in _dynamic_labels.
    args.run_id = uuid.uuid4().hex[:12]
    args.worker_image_tag = _worker_image_tag(args.worker_image)
    args.launched_by = os.environ.get("USER", "unknown")

    logger.info("experiment_id: %s", args.experiment_id)
    logger.info("suffix: %s", suffix)
    logger.info("run_id: %s  commit_sha: %s  worker_image_tag: %s  launched_by: %s",
                args.run_id, args.commit_sha, args.worker_image_tag, args.launched_by)
    logger.info("source dataset: %s_{internal,published}", args.source_dataset_stem)
    logger.info("date range (inclusive): %s -> %s, tail_days=%d", args.start, args.end, args.tail_days)
    logger.info("runner: %s", args.runner)

    if not args.skip_pipelines:
        mode_fns = [execute_bf, execute_bfd, execute_bftruncate]
        if args.parallel:
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(mode_fns)) as pool:
                futures = [pool.submit(fn, args, suffix) for fn in mode_fns]
                for fut in concurrent.futures.as_completed(futures):
                    fut.result()
        else:
            for fn in mode_fns:
                fn(args, suffix)

    if args.skip_comparisons:
        return 0
    return compare_all(args, suffix)


if __name__ == "__main__":
    sys.exit(main())
