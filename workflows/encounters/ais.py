"""Mode equivalence integration test for encounters detection (AIS staging).

Drives the two-step encounters **generation** pipeline
(``create_raw_encounters`` -> ``merge_encounters``) three different ways
("modes") and asserts they produce identical output. Divergence between modes
signals non-determinism in the pipeline or a bug in the re-run logic.

**Scope: generation only.** Every GFW event type is produced in two halves --
a *generation* step in a pipeline-specific repo, then a *product* step
(``product_events_*``) in pipe-events. dit covers generation. Encounters
generates in ``encounters_pipeline``; the ``generate_encounter_events``
publication step is pipe-events' and out of scope here. (``pipe_events/
fishing.py`` spans both halves only because fishing generates in pipe-events
too.) The sibling template for this file is therefore
``workflows/port_visits/ais.py``, not ``fishing.py`` -- see
``docs/encounters-onboarding-2026-07.md``.

Modes (each gets its own suffixed output tables; compared pairwise at the end):

* ``_1_bf``: a single create+merge over ``[start, end]``. Range-mode oracle.
* ``_2_bfd``: create+merge ending ``tail_days`` short, then per-day create+merge
  for each tail day. Mirrors steady-state daily reprocessing.
* ``_3_bftruncate``: a full create+merge over the whole range, then the same
  per-day re-runs as ``_2_bfd``. Tests that re-running already-processed days
  is clean.

Structurally identical to port-visits: **step 1 is per-slice, step 2 is a full
recompute** from the pipeline's ``--start`` to the slice end, matching what the
composer DAG does (``merge_encounters --start_date=data_available_from_date``).

WHAT EACH COMPARISON DETECTS
---------------------------
Both output tables are compared, and they are sensitive to **different** bug
classes -- neither is redundant:

* **raw_encounters** -- built incrementally (bounded pre-write DELETE + append
  per slice), so it is the one that can catch *incrementality* bugs: a
  re-run-over-processed-days path that duplicates, drops, or fails to replace
  rows. The modes genuinely diverge here if that logic is wrong.
* **encounters (merged)** -- ``merge_encounters`` rebuilds its whole range and
  writes ``WRITE_TRUNCATE`` every call, so incrementality bugs are invisible
  here by construction. That makes it a *weak* detector of those -- but a
  **strong** detector of NON-DETERMINISM: if the same inputs recomputed from
  scratch produce different output across modes, the pipeline is
  non-deterministic. There is a live candidate for exactly that (see the
  ``vessel_N_seg_ids[0]`` hypothesis below), and non-determinism can also be
  introduced by a future change, so this comparison stays.

So: a diff in raw points at re-run logic; a diff in merged points at
non-determinism. Read a green *merged* result as "recompute is reproducible",
not as "the modes agree" -- the raw table is what proves the latter. (Contrast
pipe-gaps, where the SCD-2 tail is what diverges.)

INTERPRETING DIFFS -- ``encounter_id`` IS A CONTENT HASH
--------------------------------------------------------
``encounter_id`` is ``md5("encounter|<seg_1>|<seg_2>|<start_time>")``
(``pipeline/transforms/add_id.py``). Two consequences:

1. It is a legitimately unique key (verified, not assumed) -- good for TIC.
2. Because the id derives from content, a content difference yields a
   DIFFERENT id, so TIC reports "row only in A / only in B" rather than
   field-level diffs. Don't read only-in-X rows as missing data; they are
   usually the same encounter with a changed identity input.

**Pre-registered hypothesis if diffs appear**: the *merged* id keys on
``vessel_N_seg_ids[0]`` -- the FIRST element of a repeated field. If the merge
groups seg_ids in a non-deterministic order, semantically identical encounters
get different ids. That is the same bug class as pipe-gaps' message-sort
tie-break and pipe-events' ``ARRAY_AGG``-without-``ORDER BY``, both of which
dit caught. Check it first.

Date semantics: ``--start`` / ``--end`` are **inclusive on both ends**,
matching the encounters CLI (``--end_date`` help says "Last date (inclusive)"
on both steps). Verified against the flag help; re-verify against the SQL
before trusting a first live run -- the pipe-gaps off-by-one (PR #69) came
from trusting a flag name over the query.

``--temp_dataset`` and the overlay image
----------------------------------------
Both steps read via ``ReadFromBigQuery``'s EXPORT method, which creates a
``beam_temp_dataset_*`` per read and so needs ``bigquery.datasets.create``.
``automated-testing@`` deliberately lacks that, so **any** run whose Dataflow
workers are that SA dies on a 403 ``POST /datasets`` raised from inside the
job -- this is not a Cloud-Build-only problem. (Laptop DirectRunner escapes it
because the dataset is then created under the user's own ADC.)

Published ``pipe-encounters`` does not expose ``--temp_dataset``. Until that
lands upstream, point ``--worker-image`` at the dit overlay image, which is
the published v4.4.0 with the patch layered over site-packages -- same Beam
version, same entrypoint, so it serves as both submitter and
``sdk_container_image``::

    --worker-image gcr.io/world-fishing-827/dit/encounters:v4.4.0-temp-dataset-<sha>

``--bq-temp-dataset`` is emitted unconditionally (mirroring
``workflows/port_visits/ais.py``); against a stock image the flag is simply
rejected as unknown, which fails loudly rather than silently reverting to
dataset creation.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import itertools
import json
import logging
import os
import subprocess
import sys
import uuid
from datetime import date, datetime, timedelta
from typing import Optional, Sequence

from dit import compare as dit_compare
from dit import dates as dit_dates
from dit import workflow as dit_workflow
from dit.job_names import make_job_name
from dit.runners import docker as dit_docker
from dit.workflow import (
    add_experiment_id_arg,
    add_infra_args,
    add_modes_arg,
    parse_modes,
    resolve_run_context,
)

logger = logging.getLogger(__name__)

# Pipeline repo name; namespaces auto-snapshot refs
# (refs/dit-snapshots/<PIPELINE_NAME>/<sha>) + the auto-built image path.
PIPELINE_NAME = "encounters_pipeline"

#: Short repo slug for Dataflow job names (63-char cap).
REPO_NAME = "encounters"

# Mode labels. Single source of truth shared by execute_* and compare_all.
MODE_BF = "1_bf"
MODE_BFD = "2_bfd"
MODE_BFTRUNCATE = "3_bftruncate"

#: Modes selectable via --modes, in canonical order.
SELECTABLE_MODES = (MODE_BF, MODE_BFD, MODE_BFTRUNCATE)


# --------------------------------------------------------------------------
# Constants / defaults
# --------------------------------------------------------------------------

PROJECT = "world-fishing-827"

# Short step labels for Dataflow job names (63-char cap constrains composition).
_JOB_STEP_NAMES = {
    "create_raw_encounters": "createraw",
    "merge_encounters": "merge",
}

# Staging cohort. NOTE the cohort name carries the upstream SNAPSHOT date
# (2024-08-29), NOT the data date -- the messages inside are 2020 AIS. Date
# defaults below therefore sit in 2020, mirroring workflows/port_visits/ais.py.
# See CLAUDE.md § Working agreements (staging-by-default) and README §
# "Staging data sources".
DEFAULT_SOURCE_DATASET_STEM = "pipe_ais_test_202408290000"

DEFAULT_SOURCE_MESSAGES_FQN = (
    f"{PROJECT}.{DEFAULT_SOURCE_DATASET_STEM}_internal.messages_positions"
)
DEFAULT_SOURCE_SEGMENT_INFO_FQN = (
    f"{PROJECT}.{DEFAULT_SOURCE_DATASET_STEM}_published.segment_info"
)
DEFAULT_SOURCE_SEGS_ACTIVITY_FQN = (
    f"{PROJECT}.{DEFAULT_SOURCE_DATASET_STEM}_published.segs_activity"
)

# spatial_measures is NOT in the staging cohort -- it is a static prod
# reference table, read-only. Same precedent as pipe_events/fishing.py's
# --pipe-static flag.
DEFAULT_SPATIAL_MEASURES_FQN = f"{PROJECT}.pipe_static.spatial_measures_20201105"

# Inclusive on both ends, matching the encounters CLI.
DEFAULT_START = "2020-01-01"
DEFAULT_END = "2020-12-31"
DEFAULT_TAIL_DAYS = 3

# Dataflow autoscaling ceiling. Mirrors prod's
# detect_encounters_config.max_num_workers (composer-dags dags/core/vms/config.py).
# Without a cap, a wide run (all vessels over a year) scales unbounded.
DEFAULT_MAX_NUM_WORKERS = 50

# Detection tuning -- prod values from composer-dags-production
# gfw/pipes/v3/detect_encounters.py.
DEFAULT_MAX_ENCOUNTER_DIST_KM = 0.5
DEFAULT_MIN_ENCOUNTER_TIME_MINUTES = 120.0
DEFAULT_MIN_HOURS_BETWEEN_ENCOUNTERS = 4.0

# Local compose image tag (docker-compose.yaml `image: gfw/pipe-encounters`).
DEFAULT_IMAGE_TAG = "gfw/pipe-encounters"

# Canonical published image, pinned at the version composer-dags-production
# runs (`Versions.DETECT_ENCOUNTERS` in dags/core/ais/v3.py). Read-only to dit
# per the absolute prod-infra boundary.
DEFAULT_WORKER_IMAGE = (
    "us-central1-docker.pkg.dev/gfw-int-infrastructure/core/pipe-encounters:v4.4.0"
)

# Pre-existing BQ dataset Beam uses for ReadFromBigQuery EXPORT staging, so the
# SA never needs bigquery.datasets.create. Same shape as
# workflows/port_visits/ais.py. Requires an image exposing --temp_dataset; see
# the module docstring for the overlay tag until that lands upstream.
DEFAULT_BQ_TEMP_DATASET = os.environ.get(
    "DIT_BQ_TEMP_DATASET", f"{PROJECT}.{dit_workflow.DEFAULT_DEST_DATASET}"
)

# Entrypoint for the published image. The composer DAG passes
# cmds=["pipe-encounters"], mirroring pipe-anchorages' convention, but
# encounters' setup.py declares no console_scripts and Dockerfile-scheduler's
# ENTRYPOINT is ./main.py -- so this is UNCONFIRMED against a real published
# image (see docs/encounters-onboarding-2026-07.md). --build-from-source
# sidesteps it by using the compose services, which carry explicit entrypoints.
CLI_ENTRYPOINT = "pipe-encounters"

# GCP auth: shared named volume mounted at /root/.config, exactly as
# docker-compose.yaml declares (and as pipe-events / pipe-segment use). In
# Cloud Build, DIT_CLOUD_MODE swaps this for --network=cloudbuild automatically
# (dit.runners.docker._apply_cloud_mode).
GCP_VOLUME = "gcp:/root/.config"

# Beam's WriteToBigQuery builds its OWN google-cloud-bigquery client inside the
# SDK worker, and that client resolves its project from the GOOGLE_CLOUD_PROJECT
# env var / ADC metadata -- NOT from the `--project` pipeline option, which Beam
# consumes earlier and does not forward. Without this the write fails with
# `OSError: Project was not passed and could not be determined from the
# environment` at BigQueryBatchFileLoads/TriggerLoadJobs.
#
# Exactly the failure pipe-segment hit on 2026-06-03, which is why
# dit.runners.docker.run has `container_env` at all (see CLAUDE.md Plan
# changelog). Second consumer of that parameter -- a third would justify making
# it a runner default rather than per-workflow opt-in.
#
# NOTE it only bites once the write path has ROWS to load: a run that produces
# zero encounters never triggers a load job and appears to pass.
CONTAINER_ENV = {"GOOGLE_CLOUD_PROJECT": PROJECT}

# Comparison contract: truncate shape, no SCD-2. encounter_id is a verified
# unique content hash on BOTH tables (see module docstring).
COMPARE_KEYS = ("encounter_id",)
COMPARE_VIEW_SUFFIX = ""


# --------------------------------------------------------------------------
# Dates
# --------------------------------------------------------------------------

def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _resolve_suffix(args: argparse.Namespace) -> str:
    """Output-table suffix. ``--suffix`` bypasses the derived form entirely."""
    if args.suffix:
        return args.suffix
    return f"{args.experiment_id}_{args.commit_sha}_{uuid.uuid4().hex[:6]}"


# --------------------------------------------------------------------------
# Table names
# --------------------------------------------------------------------------

def _raw_table(args: argparse.Namespace, suffix: str, mode: str) -> str:
    return f"{PROJECT}.{args.dest_dataset}.raw_encounters_{suffix}_{mode}"


def _encounters_table(args: argparse.Namespace, suffix: str, mode: str) -> str:
    return f"{PROJECT}.{args.dest_dataset}.encounters_{suffix}_{mode}"


def _bad_segs_sql(args: argparse.Namespace) -> str:
    """Subquery of bad segment ids, mirroring the composer DAG's
    ``bad_segs_query``. Passed to ``merge_encounters --bad_segs_table``, which
    accepts a subquery in place of a table name."""
    return (
        f"(SELECT DISTINCT seg_id "
        f"FROM `{args.source_segs_activity_fqn}` "
        f"WHERE overlapping_and_short)"
    )


# --------------------------------------------------------------------------
# Destination-table bootstrap
# --------------------------------------------------------------------------
# BOTH steps call `writer.update_table_metadata()`, which does a
# `bqclient.get_table(...)` to stamp description + labels -- BEFORE Beam's
# CREATE_IF_NEEDED sink has made anything. On a table that does not exist yet
# that is a hard 404. Production never notices: its `raw_encounters` /
# `encounters` tables are long-lived, and the DAG additionally carries an
# explicit `ensure_daily_encounters_table_exists` task. dit mints a FRESH
# table per (suffix, mode) on every run, so without this bootstrap every run
# fails on the first step. Found by the third laptop smoke; the audit had
# wrongly assumed CREATE_IF_NEEDED was sufficient.
#
# The schemas are read OUT OF THE IMAGE rather than copied into dit, so they
# cannot drift from the pipeline that writes them. (A hardcoded copy would be
# a silent-divergence hazard exactly like the compare-key class of bug.)

_SCHEMA_SNIPPET = (
    "import json;"
    "from apache_beam.io.gcp.bigquery_tools import get_dict_table_schema as g;"
    # v4.4.0 renamed the package `pipeline` -> `pipe_encounters`; support both
    # so --build-from-source against an older checkout still works.
    "\ntry:\n    from pipe_encounters.schemas import output\n"
    "except ImportError:\n    from pipeline.schemas import output\n"
    "print('DIT_SCHEMAS=' + json.dumps({"
    "'raw': g(output.build_raw_encounter())['fields'],"
    "'merged': g(output.build())['fields']}))"
)


def _fetch_schemas(image_tag: str) -> dict[str, list[dict]]:
    """Read the raw + merged output schemas out of the pipeline image."""
    proc = subprocess.run(
        ["docker", "run", "--rm", "--entrypoint", "python", image_tag,
         "-c", _SCHEMA_SNIPPET],
        capture_output=True, text=True,
    )
    for line in proc.stdout.splitlines():
        if line.startswith("DIT_SCHEMAS="):
            return json.loads(line[len("DIT_SCHEMAS="):])
    raise SystemExit(
        f"could not read output schemas from {image_tag} (rc={proc.returncode}).\n"
        f"stdout: {proc.stdout[-500:]}\nstderr: {proc.stderr[-500:]}"
    )


def _ensure_table(fqn: str, fields: list[dict]) -> None:
    """Create ``fqn`` with ``fields`` if absent. Idempotent (``exists_ok``).

    Partitioning MUST match what the pipeline's sink declares in
    ``additional_bq_parameters`` (transforms/writers.py): MONTH on
    ``start_time``, clustered on ``start_time``. BigQuery rejects a load whose
    partitioning spec differs from the destination's with
    "Incompatible table partitioning specification", so creating a plain
    unpartitioned table here fails every write.

    Note the DAG's own ``ensure_daily_encounters_table_exists`` task creates
    the table WITHOUT partitioning -- copying it is not sufficient. Prod does
    not notice because its tables were created partitioned long ago and that
    task is then a no-op. Found by the sixth laptop smoke.
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT)
    schema = [
        bigquery.SchemaField(
            f["name"], f["type"], mode=f.get("mode", "NULLABLE"),
            description=f.get("description"),
        )
        for f in fields
    ]
    table = bigquery.Table(fqn, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.MONTH,
        field="start_time",
        require_partition_filter=False,
    )
    table.clustering_fields = ["start_time"]
    client.create_table(table, exists_ok=True)
    logger.info(
        "ensured output table %s (%d fields, MONTH-partitioned on start_time)",
        fqn, len(schema),
    )


def _ensure_output_tables(args: argparse.Namespace, suffix: str) -> None:
    """Pre-create every selected mode's raw + merged table. See the comment
    block above for why this is required rather than merely tidy."""
    schemas = _fetch_schemas(args.image_tag)
    for mode in args.modes:
        _ensure_table(_raw_table(args, suffix, mode), schemas["raw"])
        _ensure_table(_encounters_table(args, suffix, mode), schemas["merged"])


# --------------------------------------------------------------------------
# Beam pipeline options
# --------------------------------------------------------------------------

def _make_job_name(
    args: argparse.Namespace, *, step: str, mode: str,
    iteration: int, total_iterations: int,
) -> str:
    return make_job_name(
        repo=REPO_NAME,
        step=_JOB_STEP_NAMES[step],
        experiment_id=args.experiment_id,
        mode=mode,
        binding=args.binding_name or None,
        iteration=iteration,
        total_iterations=total_iterations,
    )


def _pipeline_options(
    args: argparse.Namespace, *, step: str, mode: str,
    iteration: int, total_iterations: int,
) -> list[str]:
    # Labels are a PIPELINE requirement, not a Dataflow one: readers.py and
    # writers.py both do `list_to_dict(cloud_opts.labels)` -- a comprehension
    # over `labels` with no None guard -- and ReadSources is constructed on
    # EVERY runner. Omitting them raises
    # `TypeError: 'NoneType' object is not iterable` before the pipeline even
    # starts. Confirmed the hard way by the first laptop smoke, which failed
    # exactly here on DirectRunner. Contract item #6 (docs/pipeline-contract.md);
    # a 1-line upstream guard would remove the need.
    labels = [
        "--labels=environment=integration_test",
        "--labels=resource_creator=dit",
        "--labels=project=core_pipeline",
        "--labels=workflow=encounters_ais",
        "--labels=stage=testing",
        f"--labels=dit_run_id={args.run_id}",
        f"--labels=dit_mode={mode}",
    ]
    if args.binding_name:
        labels.append(f"--labels=dit_binding={args.binding_name}")
    if args.runner != "dataflow":
        # Two options are required even on DirectRunner, because they are
        # demanded by the PIPELINE's BQ access rather than by Dataflow:
        #   --project      : the pipeline builds its own client as
        #                    `bigquery.Client(project=cloud_opts.project)`
        #                    (transforms/writers.py); omitting it yields
        #                    project=None and leaves the destination to ADC's
        #                    default -- which may not be ours.
        #   --temp_location: ReadFromBigQuery uses the EXPORT read method,
        #                    which stages the table to GCS first and raises
        #                    "requires a GCS location" without it on ANY runner.
        # Both were found by the first laptop smokes, in that order.
        # NOTE --temp_location (a GCS path) is NOT --temp_dataset (the BQ
        # dataset the EXPORT temp table lands in). Both are emitted; see the
        # module docstring for why the latter needs the overlay image.
        return [
            f"--project={PROJECT}",
            f"--temp_location=gs://{args.dataflow_temp_bucket}/dataflow_temp",
            f"--temp_dataset={args.bq_temp_dataset}",
            "--runner=DirectRunner",
            "--wait_for_job",
            *labels,
        ]
    return [
        "--runner=DataflowRunner",
        f"--project={PROJECT}",
        f"--region={args.dataflow_region}",
        f"--service_account_email={args.service_account}",
        f"--temp_location=gs://{args.dataflow_temp_bucket}/dataflow_temp",
        f"--temp_dataset={args.bq_temp_dataset}",
        f"--staging_location=gs://{args.dataflow_temp_bucket}/dataflow_staging",
        f"--subnetwork={args.dataflow_subnetwork}",
        # Bound autoscaling. Without this a wide run (all vessels over a year)
        # scales freely; prod caps this same step at 50
        # (detect_encounters_config.max_num_workers in composer-dags).
        f"--max_num_workers={args.max_num_workers}",
        f"--sdk_container_image={args.worker_image}",
        f"--job_name={_make_job_name(args, step=step, mode=mode, iteration=iteration, total_iterations=total_iterations)}",
        "--wait_for_job",
        *labels,
    ]


# --------------------------------------------------------------------------
# Slice runner: create_raw -> merge
# --------------------------------------------------------------------------

def _run_slice(
    args: argparse.Namespace, *, mode: str, slice_start: date, slice_end: date,
    suffix: str, iteration: int, total_iterations: int,
) -> None:
    """One slice = one ``create_raw_encounters`` + one ``merge_encounters``.

    ``create_raw_encounters`` runs over ``[slice_start, slice_end]`` and is
    idempotent for that window: the pipeline issues a pre-write
    ``DELETE ... WHERE DATE(start_time) BETWEEN start AND end`` -- bounded on
    BOTH sides, so unlike pipe-gaps it does NOT take ownership of the tail.

    ``merge_encounters`` runs over ``[args.start, slice_end]`` -- the full
    history every call, matching the composer DAG
    (``--start_date=data_available_from_date``) -- and writes WRITE_TRUNCATE.
    """
    create_args = [
        "create_raw_encounters",
        f"--source_table={args.source_messages_fqn}",
        f"--raw_table={_raw_table(args, suffix, mode)}",
        f"--start_date={slice_start.isoformat()}",
        f"--end_date={slice_end.isoformat()}",
        f"--max_encounter_dist_km={args.max_encounter_dist_km}",
        f"--min_encounter_time_minutes={args.min_encounter_time_minutes}",
        *( [f"--ssvid_filter={args.ssvid_filter}"] if args.ssvid_filter else [] ),
        *_pipeline_options(
            args, step="create_raw_encounters", mode=mode,
            iteration=iteration, total_iterations=total_iterations,
        ),
    ]
    logger.info("create_raw_encounters %s [%s, %s] iter=%d/%d",
                mode, slice_start, slice_end, iteration, total_iterations)
    rc = dit_docker.run(
        args.image_tag, create_args,
        entrypoint=CLI_ENTRYPOINT,
        volumes=[GCP_VOLUME],
        service="pipe_encounters",
        container_env=CONTAINER_ENV,
        build_from_source=args.build_from_source,
    )
    if rc != 0:
        raise SystemExit(
            f"create_raw_encounters failed (rc={rc}, mode={mode}, "
            f"slice=[{slice_start}, {slice_end}])"
        )

    merge_args = [
        "merge_encounters",
        # Full-history rebuild, as prod does -- NOT the slice start.
        f"--start_date={args.start}",
        f"--end_date={slice_end.isoformat()}",
        f"--raw_table={_raw_table(args, suffix, mode)}",
        f"--sink_table={_encounters_table(args, suffix, mode)}",
        f"--vessel_id_table={args.source_segment_info_fqn}",
        f"--spatial_measures_table={args.spatial_measures_fqn}",
        f"--bad_segs_table={_bad_segs_sql(args)}",
        f"--min_hours_between_encounters={args.min_hours_between_encounters}",
        *( [f"--ssvid_filter={args.ssvid_filter}"] if args.ssvid_filter else [] ),
        *_pipeline_options(
            args, step="merge_encounters", mode=mode,
            iteration=iteration, total_iterations=total_iterations,
        ),
    ]
    logger.info("merge_encounters %s [%s, %s] iter=%d/%d",
                mode, args.start, slice_end, iteration, total_iterations)
    rc = dit_docker.run(
        args.image_tag, merge_args,
        entrypoint=CLI_ENTRYPOINT,
        volumes=[GCP_VOLUME],
        service="pipe_encounters",
        container_env=CONTAINER_ENV,
        build_from_source=args.build_from_source,
    )
    if rc != 0:
        raise SystemExit(
            f"merge_encounters failed (rc={rc}, mode={mode}, "
            f"slice=[{args.start}, {slice_end}])"
        )


# --------------------------------------------------------------------------
# Modes
# --------------------------------------------------------------------------

def execute_bf(args: argparse.Namespace, suffix: str) -> None:
    _run_slice(
        args, mode=MODE_BF,
        slice_start=_parse_date(args.start), slice_end=_parse_date(args.end),
        suffix=suffix, iteration=1, total_iterations=1,
    )


def execute_bfd(args: argparse.Namespace, suffix: str) -> None:
    start = _parse_date(args.start)
    end = _parse_date(args.end)
    initial_end = end - timedelta(days=args.tail_days)
    total = 1 + args.tail_days
    _run_slice(args, mode=MODE_BFD, slice_start=start, slice_end=initial_end,
               suffix=suffix, iteration=1, total_iterations=total)
    # dit.dates.daterange_inclusive is half-open despite the name (pinned by
    # tests/test_dates.py), so +1 day on the end to include `end` itself.
    for i, d in enumerate(
        dit_dates.daterange_inclusive(initial_end + timedelta(days=1), end + timedelta(days=1)),
        start=2,
    ):
        _run_slice(args, mode=MODE_BFD, slice_start=d, slice_end=d,
                   suffix=suffix, iteration=i, total_iterations=total)


def execute_bftruncate(args: argparse.Namespace, suffix: str) -> None:
    start = _parse_date(args.start)
    end = _parse_date(args.end)
    total = 1 + args.tail_days
    _run_slice(args, mode=MODE_BFTRUNCATE, slice_start=start, slice_end=end,
               suffix=suffix, iteration=1, total_iterations=total)
    for i, d in enumerate(
        dit_dates.daterange_inclusive(
            end - timedelta(days=args.tail_days - 1), end + timedelta(days=1)),
        start=2,
    ):
        _run_slice(args, mode=MODE_BFTRUNCATE, slice_start=d, slice_end=d,
                   suffix=suffix, iteration=i, total_iterations=total)


# --------------------------------------------------------------------------
# Comparison
# --------------------------------------------------------------------------

def compare_all(
    raw_fqns: dict[str, str], merged_fqns: dict[str, str], modes: Sequence[str],
) -> int:
    """Pairwise-compare the selected modes, on BOTH output tables.

    The two are sensitive to different bug classes and neither is redundant:
    **raw** catches incrementality bugs (it is built slice-by-slice), **merged**
    catches non-determinism (it is recomputed from scratch every call, so
    incrementality bugs cannot show there but irreproducible output can). See
    the module docstring.
    """
    pairs = list(itertools.combinations(modes, 2))
    if not pairs:
        only = modes[0]
        logger.info(
            "only one mode selected (%s) -- no pair to compare. Outputs: raw=%s "
            "merged=%s. Add a second mode to --modes to get a comparison.",
            only, raw_fqns[only], merged_fqns[only],
        )
        return 0

    overall = 0
    for label, fqns, note in (
        ("raw_encounters", raw_fqns, "detects incrementality bugs"),
        ("encounters (merged)", merged_fqns, "detects non-determinism"),
    ):
        for a, b in pairs:
            rc = dit_compare.compare_tables(
                fqns[a], fqns[b],
                keys=COMPARE_KEYS, view_suffix=COMPARE_VIEW_SUFFIX,
            )
            logger.info("compare [%s / %s] %s vs %s -> rc=%s", label, note, a, b, rc)
            overall = overall or rc
    return overall


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Mode-equivalence test for encounters detection (AIS staging).",
    )
    p.add_argument("--runner", default="dataflow", choices=["dataflow", "docker"],
                   help="dataflow: submit DataflowRunner from inside the container "
                        "(default). docker: DirectRunner inside the container.")
    p.add_argument("--source-messages-fqn", default=DEFAULT_SOURCE_MESSAGES_FQN,
                   help=f"Messages source (create_raw_encounters --source_table). "
                        f"Default: {DEFAULT_SOURCE_MESSAGES_FQN}")
    p.add_argument("--source-segment-info-fqn", default=DEFAULT_SOURCE_SEGMENT_INFO_FQN,
                   help=f"segment_info (merge_encounters --vessel_id_table). "
                        f"Default: {DEFAULT_SOURCE_SEGMENT_INFO_FQN}")
    p.add_argument("--source-segs-activity-fqn", default=DEFAULT_SOURCE_SEGS_ACTIVITY_FQN,
                   help=f"segs_activity, source of the bad-segs subquery. "
                        f"Default: {DEFAULT_SOURCE_SEGS_ACTIVITY_FQN}")
    p.add_argument("--spatial-measures-fqn", default=DEFAULT_SPATIAL_MEASURES_FQN,
                   help="spatial_measures table. NOT in the staging cohort -- this "
                        "default reads a static PROD reference table (read-only). "
                        f"Default: {DEFAULT_SPATIAL_MEASURES_FQN}")
    p.add_argument("--start", default=DEFAULT_START,
                   help="Inclusive start date (also the merge step's full-history start).")
    p.add_argument("--end", default=DEFAULT_END, help="Inclusive end date.")
    p.add_argument("--tail-days", type=int, default=DEFAULT_TAIL_DAYS,
                   help="Number of trailing days re-run day-by-day in the "
                        "daily-slice modes.")
    p.add_argument("--max-encounter-dist-km", type=float,
                   default=DEFAULT_MAX_ENCOUNTER_DIST_KM)
    p.add_argument("--min-encounter-time-minutes", type=float,
                   default=DEFAULT_MIN_ENCOUNTER_TIME_MINUTES)
    p.add_argument("--min-hours-between-encounters", type=float,
                   default=DEFAULT_MIN_HOURS_BETWEEN_ENCOUNTERS)
    p.add_argument("--ssvid-filter", default=None,
                   help="Passed to BOTH steps as --ssvid_filter: a subquery, a "
                        "comma-separated ssvid list, or @path. The cheapest way to "
                        "shrink a run -- encounters supports this on both steps.")
    p.add_argument("--image-tag", default=DEFAULT_IMAGE_TAG,
                   help=f"Submitter image. Default: {DEFAULT_IMAGE_TAG}")
    p.add_argument("--worker-image", default=DEFAULT_WORKER_IMAGE,
                   help=f"Dataflow worker image (sdk_container_image). "
                        f"Default: {DEFAULT_WORKER_IMAGE}")
    p.add_argument("--bq-temp-dataset", default=DEFAULT_BQ_TEMP_DATASET,
                   help="Pre-existing BQ dataset for Beam EXPORT staging; "
                        "env-var fallback DIT_BQ_TEMP_DATASET (defaults to "
                        "${PROJECT}.${DIT_DEST_DATASET}). Needs an image that "
                        "exposes --temp_dataset; see the module docstring.")
    p.add_argument("--max-num-workers", type=int, default=DEFAULT_MAX_NUM_WORKERS,
                   help=f"Dataflow autoscaling ceiling, mirroring prod's cap for this "
                        f"step. Default: {DEFAULT_MAX_NUM_WORKERS}")
    p.add_argument("--build-from-source", action="store_true",
                   help="Build the image from the local checkout via docker compose "
                        "instead of pulling the published one. Recommended on laptop: "
                        "also sidesteps the unconfirmed published-image entrypoint.")
    p.add_argument("--binding-name", default="",
                   help="Optional binding label (e.g. 'before', 'after') for "
                        "cross-version runs. Surfaces in the Dataflow job name and "
                        "as the dit_binding=<name> BQ label. WITHOUT it, two "
                        "concurrent runs sharing an --experiment-id collide with "
                        "DataflowJobAlreadyExistsError. Empty when standalone.")
    p.add_argument("--suffix", default=None)
    add_experiment_id_arg(p)
    p.add_argument("--require-clean", action="store_true",
                   help="Error on a dirty tree instead of auto-snapshotting.")
    p.add_argument("--skip-pipelines", action="store_true")
    p.add_argument("--skip-comparisons", action="store_true")
    p.add_argument("--parallel", "--async", dest="parallel", action="store_true",
                   help="Run the selected modes in parallel threads.")
    add_infra_args(p)
    add_modes_arg(
        p, choices=SELECTABLE_MODES, cached=False,
        help_suffix="This workflow has no run-cache integration yet.",
    )
    args = p.parse_args(argv)
    # Validate before any cloud call: a typo'd mode that silently ran nothing
    # would look exactly like a passing run.
    args.modes = parse_modes(args.modes, choices=SELECTABLE_MODES)
    return args


def run(
    args: argparse.Namespace,
    *,
    default_worker_image: str = DEFAULT_WORKER_IMAGE,
) -> int:
    """Execute a parsed encounters run: validate, resolve context, run the
    selected modes, compare.

    Everything here is source-agnostic -- it reads FQNs, dates and tuning off
    ``args`` and never looks at this module's defaults. That is what lets
    ``workflows/encounters/vms.py`` reuse it wholesale rather than fork 80
    lines: the two workflows differ ONLY in their defaults and help text, which
    is exactly the part each owns. Keep it that way -- anything genuinely
    source-specific belongs in the caller, not behind a flag here.
    """
    if args.tail_days < 0:
        raise SystemExit(f"--tail-days must be >= 0; got {args.tail_days}.")
    if _parse_date(args.start) > _parse_date(args.end):
        raise SystemExit(
            f"--start ({args.start}) must be <= --end ({args.end}); both are inclusive."
        )
    if args.tail_days > (_parse_date(args.end) - _parse_date(args.start)).days:
        raise SystemExit(
            f"--tail-days ({args.tail_days}) exceeds the [{args.start}, {args.end}] "
            "window, so the daily-slice modes would start before --start."
        )

    ctx = resolve_run_context(
        repo_dir=os.getcwd(),
        pipeline_name=PIPELINE_NAME,
        runner=args.runner,
        require_clean=args.require_clean,
        suffix=args.suffix or None,
        worker_image=args.worker_image,
        default_worker_image=default_worker_image,
        build_from_source=args.build_from_source,
        # No run-cache integration yet (mirrors pipe-events' deferral), so the
        # worker-image digest is unused -- skip the gcloud describe.
        resolve_digest=False,
    )
    args.run_context = ctx
    args.commit_sha = ctx.pipeline_commit
    args.unreviewed = ctx.unreviewed
    args.worker_image = ctx.worker_image
    args.run_id = ctx.run_id

    suffix = _resolve_suffix(args)

    logger.info("experiment_id: %s", args.experiment_id)
    logger.info("suffix: %s", suffix)
    logger.info("run_id: %s  commit: %s", args.run_id, args.commit_sha)
    logger.info("messages:        %s", args.source_messages_fqn)
    logger.info("segment_info:    %s", args.source_segment_info_fqn)
    logger.info("segs_activity:   %s", args.source_segs_activity_fqn)
    logger.info("spatial_measures: %s  (static PROD reference)", args.spatial_measures_fqn)
    logger.info("date range (inclusive): %s -> %s, tail_days=%d",
                args.start, args.end, args.tail_days)
    if args.ssvid_filter:
        logger.info("ssvid_filter: %s", args.ssvid_filter)

    raw_fqns = {m: _raw_table(args, suffix, m) for m in SELECTABLE_MODES}
    merged_fqns = {m: _encounters_table(args, suffix, m) for m in SELECTABLE_MODES}

    if not args.skip_pipelines:
        # Must happen BEFORE any step runs -- see the bootstrap comment block.
        _ensure_output_tables(args, suffix)
        _execs_by_mode = {
            MODE_BF: execute_bf,
            MODE_BFD: execute_bfd,
            MODE_BFTRUNCATE: execute_bftruncate,
        }
        skipped = [m for m in SELECTABLE_MODES if m not in args.modes]
        if skipped:
            logger.info("--modes: running %s; skipping %s",
                        ",".join(args.modes), ",".join(skipped))
        if args.parallel:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(args.modes)
            ) as pool:
                futures = {
                    pool.submit(_execs_by_mode[m], args, suffix): m for m in args.modes
                }
                for fut in concurrent.futures.as_completed(futures):
                    fut.result()  # re-raise
        else:
            for m in args.modes:
                _execs_by_mode[m](args, suffix)

    if args.skip_comparisons:
        return 0
    return compare_all(raw_fqns, merged_fqns, args.modes)


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return run(parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
