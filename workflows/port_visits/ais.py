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
import subprocess
import sys
import uuid
from datetime import date, datetime, timedelta
from typing import Optional, Sequence

from dit import compare as dit_compare
from dit import dates as dit_dates
from dit.runners import docker as dit_docker

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Constants / defaults
# --------------------------------------------------------------------------

PROJECT = "world-fishing-827"

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


# --------------------------------------------------------------------------
# Suffix / git info  (lifted from workflows/pipe_gaps/mode_equivalence.py;
# promote to dit.git_info when a third consumer appears.)
# --------------------------------------------------------------------------

def _git_info(repo_dir: str) -> tuple[str, bool]:
    """Return (short commit, dirty?) for the given repo dir. Falls back to
    ('unknown', False) outside a git repo."""
    try:
        commit = subprocess.check_output(
            ["git", "-C", repo_dir, "rev-parse", "--short", "HEAD"],
            text=True, stderr=subprocess.DEVNULL,
        ).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return ("unknown", False)
    try:
        dirty = bool(subprocess.check_output(
            ["git", "-C", repo_dir, "status", "--porcelain"],
            text=True, stderr=subprocess.DEVNULL,
        ).strip())
    except subprocess.CalledProcessError:
        dirty = False
    return commit, dirty


def _resolve_suffix(args: argparse.Namespace, repo_dir: str) -> str:
    if args.suffix:
        return args.suffix
    commit, dirty = _git_info(repo_dir)
    if dirty and not args.allow_dirty_tree:
        raise SystemExit(
            "error: working tree is dirty; pass --allow-dirty-tree, or commit/stash first."
        )
    uid = uuid.uuid4().hex[:6]
    return f"{commit}_dirty_{uid}" if dirty else f"{commit}_{uid}"


# --------------------------------------------------------------------------
# Beam pipeline-options assembly
# --------------------------------------------------------------------------

def _dataflow_pipeline_options(args: argparse.Namespace) -> list[str]:
    return [
        "--runner=DataflowRunner",
        f"--project={PROJECT}",
        f"--region={args.dataflow_region}",
        f"--service_account_email={args.service_account}",
        f"--temp_location=gs://{args.dataflow_temp_bucket}/dataflow_temp",
        f"--staging_location=gs://{args.dataflow_temp_bucket}/dataflow_staging",
        f"--subnetwork={args.dataflow_subnetwork}",
        f"--sdk_container_image={args.worker_image}",
        "--wait_for_job",
        # pipe-anchorages requires --labels to be non-None
        # (cloud_to_labels in transforms/sink.py iterates without a None guard).
        # Mirrors composer's LabelsConfig.as_dataflow_cli_arguments shape.
        "--labels=environment=integration_test",
        "--labels=resource_creator=dit",
        "--labels=project=core_pipeline",
        "--labels=workflow=port_visits_ais",
        "--labels=stage=testing",
    ]


def _directrunner_pipeline_options() -> list[str]:
    return ["--runner=DirectRunner", "--wait_for_job"]


def _pipeline_options(args: argparse.Namespace) -> list[str]:
    return (
        _dataflow_pipeline_options(args) if args.runner == "dataflow"
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
) -> None:
    """One slice = one thin_port_messages call + one port_visits call.

    port_visits' --start_date is the workflow's --start (pipeline-level
    data_available_from); it does a full recompute over [start, slice_end]
    on every call, matching production semantics.
    """
    thin_args = [
        "thin_port_messages",
        f"--start_date={slice_start.isoformat()}",
        f"--end_date={slice_end.isoformat()}",
        f"--anchorage_table={args.named_anchorages}",
        f"--input_table={_messages_table(args)}",
        f"--output_table={_thinned_table(args, suffix, mode)}",
        f"--temp_dataset={args.bq_temp_dataset}",
        *_pipeline_options(args),
    ]
    logger.info("thin_port_messages %s [%s, %s]", mode, slice_start, slice_end)
    rc = dit_docker.run(
        args.image_tag, thin_args,
        entrypoint="pipe-anchorages",
        build_from_source=args.build_from_source,
    )
    if rc != 0:
        raise SystemExit(
            f"thin_port_messages failed (rc={rc}, mode={mode}, slice=[{slice_start}, {slice_end}])"
        )

    visits_args = [
        "port_visits",
        f"--start_date={args.start}",
        f"--end_date={slice_end.isoformat()}",
        f"--thinned_message_table={_thinned_table(args, suffix, mode)}",
        f"--vessel_id_table={_segment_info_table(args)}",
        f"--output_table={_visits_table(args, suffix, mode)}",
        f"--bad_segs={_bad_segs_sql(args)}",
        f"--temp_dataset={args.bq_temp_dataset}",
        *_pipeline_options(args),
    ]
    logger.info("port_visits %s [%s, %s]", mode, args.start, slice_end)
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
    _run_slice(args, mode="1_bf", slice_start=start, slice_end=end, suffix=suffix)


def execute_bfd(args: argparse.Namespace, suffix: str) -> None:
    start = _parse_date(args.start)
    end = _parse_date(args.end)
    initial_end = end - timedelta(days=args.tail_days)
    _run_slice(args, mode="2_bfd", slice_start=start, slice_end=initial_end, suffix=suffix)
    # daterange_inclusive is half-open per dit.dates contract; +1 day on end
    # to include the final `end` date.
    for d in dit_dates.daterange_inclusive(
        initial_end + timedelta(days=1), end + timedelta(days=1)
    ):
        _run_slice(args, mode="2_bfd", slice_start=d, slice_end=d, suffix=suffix)


def execute_bftruncate(args: argparse.Namespace, suffix: str) -> None:
    start = _parse_date(args.start)
    end = _parse_date(args.end)
    _run_slice(args, mode="3_bftruncate", slice_start=start, slice_end=end, suffix=suffix)
    for d in dit_dates.daterange_inclusive(
        end - timedelta(days=args.tail_days - 1), end + timedelta(days=1)
    ):
        _run_slice(args, mode="3_bftruncate", slice_start=d, slice_end=d, suffix=suffix)


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
    p.add_argument("--allow-dirty-tree", action="store_true",
                   help="Permit auto-suffix on a dirty working tree.")
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

    suffix = _resolve_suffix(args, repo_dir=os.getcwd())
    logger.info("suffix: %s", suffix)
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
