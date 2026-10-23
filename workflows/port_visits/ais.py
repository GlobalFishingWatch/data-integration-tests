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

Steps (``--steps``, default both, in pipeline order):

* ``thin_port_messages``: messages -> thinned port messages (production's
  ``raw_port_events``), written to ``port_events_<suffix>_<mode>``.
* ``port_visits``: thinned port messages -> visits, written to
  ``port_visits_<suffix>_<mode>``.

``--steps port_visits`` skips step 1 and requires ``--thinned-message-table``
as step 2's input, e.g. production's ``raw_port_events`` for a change that
lives in step 2 only. ``--steps thin_port_messages`` runs step 1 alone. The
modes compare the output of the LAST selected step: visits on ``visit_id``,
or thinned messages on ``(identifier, timestamp)``. That key was unique in
every mode of the 2026-10-22 staging smoke; it is not guaranteed (a segment
could hold two messages in one second), but it serves for QA.

Date semantics: ``--start`` and ``--end`` are **inclusive on both ends**,
matching the pipe-anchorages v4 CLI. (Pipe-gaps' workflow uses half-open dates,
matching detect-gaps' CLI; the wart is unavoidable given the downstream
tools' contracts.)

Two pipe-anchorages CLI generations (``--pipeline-cli``, PIPELINE-4699):

* ``v4`` (default): the v4.6.4 CLI production still pins --
  ``thin_port_messages`` / ``port_visits``, underscore flags, inclusive
  ``--end_date``, ``--wait_for_job``, ``--temp_dataset`` (honoured only by
  DIT's patched branch, never upstream).
* ``v5``: the gfw-common CLI on pipe-anchorages main --
  ``port-state-transitions`` / ``port-visits``, ``--bq-in-*`` / ``--bq-out-*``
  flags, **exclusive** ``--end-date`` (the workflow keeps its inclusive
  ``--start/--end`` and passes ``end + 1 day``), required ``--labels`` on every
  runner, no ``--wait_for_job`` (gfw-common's Pipeline waits). Upstream has no
  way to name the BigQuery staging dataset, so v5 runs need a DIT overlay
  image (``docker/overlays/pipe-anchorages-temp-dataset/``) as both submitter
  (``--image-tag``) and worker (``--worker-image``); the workflow hands the
  dataset to it through the ``DIT_BQ_TEMP_DATASET`` container env var.

The default ``--runner=dataflow`` submits to Dataflow from inside the
container (the same pattern composer uses). ``--runner=docker`` runs
DirectRunner inside the container for fast local sanity checks. Both go
through ``dit.runners.docker`` -- the Dataflow submission is opaque to the
runner, which only owns the docker process and waits for it via
``--wait_for_job`` inside the pipeline.

Dataflow jobs always run with private worker IPs (``--no_use_public_ips``,
as production; without it the v4 CLI falls back to Dataflow's public IPs).
``--worker-machine-type`` / ``--disk-size-gb`` are emitted only when given;
unset, the CLI's own default applies (v4: Dataflow's n1-standard-1 and 30 GB;
v5: gfw-common's e2-standard-2 and 25 GB).

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
from dit import workflow as dit_workflow
from dit.cache import CacheKey, resolve_worker_image_to_digest, sha1_of_workflow_file
from dit.dataflow_paths import dataflow_staging_location, dataflow_temp_location
from dit.job_names import make_job_name
from dit.runners import docker as dit_docker
from dit.workflow import (
    NO_PUBLIC_IPS_OPTION,
    UNSET_WORKER_SIZING,
    WorkerSizing,
    add_experiment_id_arg,
    add_infra_args,
    add_max_num_workers_arg,
    add_modes_arg,
    add_no_cache_arg,
    add_worker_sizing_args,
    log_worker_caps,
    log_worker_sizings,
    max_num_workers_options,
    parse_choice_list,
    parse_modes,
    resolve_run_context,
    worker_cap,
    worker_sizing_from_args,
    worker_sizing_options,
)

logger = logging.getLogger(__name__)

# Pipeline repo name; namespaces auto-snapshot refs
# (refs/dit-snapshots/<PIPELINE_NAME>/<sha>).
PIPELINE_NAME = "anchorages_pipeline"

# Compute once at import time; the dit-side cache buster (workflow-file edits
# invalidate the cache, pure dit.* refactors don't).
WORKFLOW_FILE_SHA1 = sha1_of_workflow_file(__file__)

#: The workflow name recorded on every CachedRun row.
WORKFLOW_NAME = "workflows/port_visits/ais.py"

# Mode labels. Single source of truth shared by execute_* and the cache key.
MODE_BF = "1_bf"
MODE_BFD = "2_bfd"
MODE_BFTRUNCATE = "3_bftruncate"

#: Modes whose output depends on ``tail_days`` (the daily-slice modes).
#: ``MODE_BF`` runs a single big range and ignores it, so ``tail_days`` must
#: NOT contribute to BF's cache key (else a ``--tail-days`` change would
#: needlessly invalidate BF, matching the pipe-gaps mode-aware rule).
_MODES_USING_TAIL = frozenset({MODE_BFD, MODE_BFTRUNCATE})

#: Modes selectable via --modes, in canonical order.
SELECTABLE_MODES = (MODE_BF, MODE_BFD, MODE_BFTRUNCATE)

# Step names (DIT's standard, from the v4 CLI; also used in Dataflow job
# names and the dit_step label). Selectable via --steps, in pipeline order.
STEP_THIN = "thin_port_messages"
STEP_VISITS = "port_visits"
SELECTABLE_STEPS = (STEP_THIN, STEP_VISITS)


# --------------------------------------------------------------------------
# Constants / defaults
# --------------------------------------------------------------------------

PROJECT = "world-fishing-827"
REPO_NAME = "anchorages-pipeline"

# Short step labels for Dataflow job names (the cap at 63 chars constrains
# composition). The full step name is used in the dit_step label, which has
# more room.
_JOB_STEP_NAMES = {
    STEP_THIN: "thin",
    STEP_VISITS: "visits",
}

# Per-user infra knobs identical to both workflows (--dest-dataset,
# --service-account, --dataflow-*) live in dit.workflow; add_infra_args wires
# them onto the parser. --bq-temp-dataset is workflow-specific and stays here.
#
# Pre-existing BQ dataset Beam uses as its temp dataset for ReadFromBigQuery
# EXPORT staging; lets the SA skip bigquery.datasets.create. Inherits
# ${PROJECT}.${DIT_DEST_DATASET} unless overridden.
DEFAULT_BQ_TEMP_DATASET = os.environ.get(
    "DIT_BQ_TEMP_DATASET", f"{PROJECT}.{dit_workflow.DEFAULT_DEST_DATASET}"
)

# Workflow-specific defaults (no env var; one-off overrides via CLI flag).
# Staging cohort: 2020-01-01 -> 2020-12-31, reduced AIS data.
DEFAULT_SOURCE_DATASET_STEM = "pipe_ais_test_202408290000"

# Per-table FQN defaults, derived from the stem default for byte-identical
# back-compat. Callers can override each table individually via the
# `--source-*-fqn` flags below -- useful for cross_version_ais.py (M5),
# which snapshots into the canonical `tech_great_expectations` dataset
# (NOT the stem-with-halves convention) and needs to pass per-table FQNs
# that don't follow the `<stem>_internal.X` / `<stem>_published.Y` shape.
# When none of the override flags are set, the workflow resolves the same
# FQNs the stem-derivation produced before M4.
DEFAULT_SOURCE_MESSAGES_FQN = (
    f"{PROJECT}.{DEFAULT_SOURCE_DATASET_STEM}_internal.messages_positions"
)
DEFAULT_SOURCE_SEGMENT_INFO_FQN = (
    f"{PROJECT}.{DEFAULT_SOURCE_DATASET_STEM}_published.segment_info"
)
DEFAULT_SOURCE_SEGS_ACTIVITY_FQN = (
    f"{PROJECT}.{DEFAULT_SOURCE_DATASET_STEM}_published.segs_activity"
)

# Global anchorages reference (not staging-specific).
DEFAULT_NAMED_ANCHORAGES = f"{PROJECT}.anchorages.named_anchorages_v20240117"

DEFAULT_START = "2020-01-01"   # inclusive
DEFAULT_END = "2020-12-31"     # inclusive
DEFAULT_TAIL_DAYS = 3

# Dataflow autoscaling ceiling. Without one, Dataflow scales up to the project's
# quota, and world-fishing-827 is also the execution project of prod's
# core.ais.v5.backfill, so an uncapped dit run competes with prod for regional
# CPU and IP quota. The default fits the 2020 staging cohort (142 ssvid); pass a
# larger value for production slices (e.g. 16 for one month of AIS). Prod runs
# the full-history port-visits step with 100 (core.ais.v3) or 250 (v5 backfill).
# The flag, the DIT_MAX_NUM_WORKERS_CEILING env var and the emission are shared
# (dit.workflow, PIPELINE-4701); choosing the right scale per job is a
# follow-up under the DIT epic PIPELINE-4698.
DEFAULT_MAX_NUM_WORKERS = 4

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

# Comparison key of the thinned port messages (step 1's output). Not proven
# unique -- a segment could hold two messages in one second -- but unique in
# every mode of the 2026-10-22 staging smoke and good enough for QA.
THIN_COMPARE_KEYS = ("identifier", "timestamp")

#: Comparison key per step output; a run compares its last step's output.
COMPARE_KEYS_BY_STEP = {STEP_THIN: THIN_COMPARE_KEYS, STEP_VISITS: COMPARE_KEYS}

# pipe-anchorages CLI generations this workflow can drive (see module docstring).
PIPELINE_CLI_V4 = "v4"
PIPELINE_CLI_V5 = "v5"
PIPELINE_CLIS = (PIPELINE_CLI_V4, PIPELINE_CLI_V5)
DEFAULT_PIPELINE_CLI = PIPELINE_CLI_V4

#: Container env var the DIT overlay patch reads to pass ``temp_dataset`` to
#: Beam's ReadFromBigQuery (v5 only; see docker/overlays/pipe-anchorages-temp-dataset/).
TEMP_DATASET_ENV = "DIT_BQ_TEMP_DATASET"

#: Laptop-mode GCP credentials: the ``gcp`` docker named volume holding the
#: gcloud config / ADC, mounted at the image's ``$HOME/.config`` (root). Same
#: spec as encounters, pipe-events and pipe-segment. Without it a published-
#: image ``docker run`` has no credentials; only the compose path
#: (``--build-from-source``) mounted it, via pipe-anchorages' compose file.
#: Cloud mode (``DIT_CLOUD_MODE``) drops it in ``dit.runners.docker``.
GCP_VOLUME = "gcp:/root/.config"

# Labels on every pipe-anchorages job. pipe-anchorages requires --labels to be
# non-None (v4: cloud_to_labels in transforms/sink.py iterates without a None
# guard; v5: --labels is a required option). Mirrors composer's
# LabelsConfig.as_dataflow_cli_arguments shape.
_STATIC_LABELS = (
    "--labels=environment=integration_test",
    "--labels=resource_creator=dit",
    "--labels=project=core_pipeline",
    "--labels=workflow=port_visits_ais",
    "--labels=stage=testing",
)


def _pipeline_cli(args: argparse.Namespace) -> str:
    """The CLI generation to emit; defaults to v4 for Namespaces built without it."""
    return getattr(args, "pipeline_cli", DEFAULT_PIPELINE_CLI)


def _max_num_workers(args: argparse.Namespace) -> int:
    """The Dataflow worker cap; defaults for Namespaces built without it."""
    return getattr(args, "max_num_workers", DEFAULT_MAX_NUM_WORKERS)


def _worker_sizing(args: argparse.Namespace) -> WorkerSizing:
    """The worker sizing (``--worker-machine-type``, ``--disk-size-gb``);
    unset for Namespaces built without it."""
    return getattr(args, "worker_sizing", UNSET_WORKER_SIZING)


def _exclusive_end(inclusive_end: date) -> str:
    """The v5 CLI's exclusive ``--end-date`` for an inclusive last day."""
    return (inclusive_end + timedelta(days=1)).isoformat()


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
    options = [
        "--runner=DataflowRunner",
        f"--project={PROJECT}",
        f"--region={args.dataflow_region}",
        f"--service_account_email={args.service_account}",
        f"--temp_location={dataflow_temp_location(args.dataflow_temp_bucket)}",
        f"--staging_location={dataflow_staging_location(args.dataflow_temp_bucket)}",
        f"--subnetwork={args.dataflow_subnetwork}",
        # Private worker IPs, as production; see dit.workflow (a3).
        NO_PUBLIC_IPS_OPTION,
        # Bound autoscaling; see DEFAULT_MAX_NUM_WORKERS.
        *max_num_workers_options(_max_num_workers(args)),
        # Machine type and disk, only when set (both CLIs have their own default).
        *worker_sizing_options(_worker_sizing(args)),
        f"--sdk_container_image={args.worker_image}",
        f"--job_name={_make_job_name(args, step=step, mode=mode, iteration=iteration, total_iterations=total_iterations)}",
    ]
    # v5 removed --wait_for_job (gfw-common's Pipeline waits for batch jobs).
    if _pipeline_cli(args) == PIPELINE_CLI_V4:
        options.append("--wait_for_job")
    return [
        *options,
        *_STATIC_LABELS,
        *_dynamic_labels(
            args, step=step, mode=mode,
            iteration=iteration, total_iterations=total_iterations,
            slice_start=slice_start, slice_end=slice_end,
        ),
    ]


def _directrunner_pipeline_options(
    args: Optional[argparse.Namespace] = None, **label_kwargs
) -> list[str]:
    """DirectRunner options. v4 keeps its historical two-flag shape; v5 needs
    labels (required option) plus a project and temp_location, which Beam's
    BigQuery read needs on any runner."""
    if args is None or _pipeline_cli(args) == PIPELINE_CLI_V4:
        return ["--runner=DirectRunner", "--wait_for_job"]
    return [
        "--runner=DirectRunner",
        f"--project={PROJECT}",
        f"--temp_location={dataflow_temp_location(args.dataflow_temp_bucket)}",
        *_STATIC_LABELS,
        *_dynamic_labels(args, **label_kwargs),
    ]


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
    label_kwargs = dict(
        step=step, mode=mode,
        iteration=iteration, total_iterations=total_iterations,
        slice_start=slice_start, slice_end=slice_end,
    )
    return (
        _dataflow_pipeline_options(args, **label_kwargs) if args.runner == "dataflow"
        else _directrunner_pipeline_options(args, **label_kwargs)
    )


# --------------------------------------------------------------------------
# Table names
# --------------------------------------------------------------------------

def _internal_dataset(stem: str) -> str:
    return f"{stem}_internal"


def _published_dataset(stem: str) -> str:
    return f"{stem}_published"


def _messages_table(args: argparse.Namespace) -> str:
    """Resolved FQN of the source messages_positions table.

    Per-table override (``--source-messages-fqn``) wins; otherwise derives
    from ``--source-dataset-stem`` for byte-identical back-compat. M4 of
    the canonical-dataset migration.

    Truthiness check on the override -- treating ``""`` as unset matches
    other optional string args in this module (``thinned_message_table``)
    and avoids generating invalid SQL (``FROM `` ``) if a caller
    accidentally passes ``--source-messages-fqn ''``. (Copilot review on
    PR #62.)
    """
    if args.source_messages_fqn:
        return args.source_messages_fqn
    return f"{PROJECT}.{_internal_dataset(args.source_dataset_stem)}.messages_positions"


def _segment_info_table(args: argparse.Namespace) -> str:
    """Resolved FQN of the source segment_info table. See ``_messages_table``."""
    if args.source_segment_info_fqn:
        return args.source_segment_info_fqn
    return f"{PROJECT}.{_published_dataset(args.source_dataset_stem)}.segment_info"


def _segs_activity_table(args: argparse.Namespace) -> str:
    """Resolved FQN of the source segs_activity table. See ``_messages_table``."""
    if args.source_segs_activity_fqn:
        return args.source_segs_activity_fqn
    return f"{PROJECT}.{_published_dataset(args.source_dataset_stem)}.segs_activity"


def _bad_segs_sql(args: argparse.Namespace) -> str:
    return (
        f"(SELECT DISTINCT seg_id "
        f"FROM `{_segs_activity_table(args)}` "
        f"WHERE overlapping_and_short)"
    )


def _thinned_table(args: argparse.Namespace, suffix: str, mode: str) -> str:
    return f"{PROJECT}.{args.dest_dataset}.port_events_{suffix}_{mode}"


def _visits_table(args: argparse.Namespace, suffix: str, mode: str) -> str:
    return f"{PROJECT}.{args.dest_dataset}.port_visits_{suffix}_{mode}"


def validate_steps(steps: Sequence[str], thinned_message_table: Optional[str]) -> None:
    """Reject step selections whose step-2 input is missing or ambiguous.

    Skipping step 1 needs ``--thinned-message-table`` as step 2's input, and
    running step 1 makes that table meaningless (step 2 reads step 1's
    output), so it is an error rather than silently ignored. Shared with
    ``cross_version_ais.py``, which validates its own ``--steps`` the same way.
    """
    if STEP_THIN not in steps and not thinned_message_table:
        raise SystemExit(
            f"--steps {','.join(steps)} skips {STEP_THIN}: pass "
            f"--thinned-message-table as {STEP_VISITS}' input"
        )
    if STEP_THIN in steps and thinned_message_table:
        raise SystemExit(
            f"--thinned-message-table replaces {STEP_THIN}'s output; it needs "
            f"--steps {STEP_VISITS}"
        )


def _last_step(args: argparse.Namespace) -> str:
    """The step whose output a run produces and compares."""
    return args.steps[-1]


def _output_table(args: argparse.Namespace, suffix: str, mode: str) -> str:
    """This run's comparison target for ``mode``: the last step's output."""
    if _last_step(args) == STEP_VISITS:
        return _visits_table(args, suffix, mode)
    return _thinned_table(args, suffix, mode)


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

    Only the steps in ``args.steps`` run. Without step 1 (``--steps
    port_visits``), step 2 reads ``args.thinned_message_table`` instead of the
    per-mode ``port_events_<suffix>_<mode>`` table the workflow would normally
    produce. This is the supported way to run dit against a change that lives
    only in step 2 -- avoids re-thinning AIS data that step 1 has already
    produced upstream (e.g. in prod). Without step 2 (``--steps
    thin_port_messages``), the slice ends after step 1.
    """
    if STEP_THIN not in args.steps:
        logger.info(
            "thin_port_messages SKIPPED for mode=%s slice=[%s, %s] iter=%d/%d -- using external table %s",
            mode, slice_start, slice_end, iteration, total_iterations,
            args.thinned_message_table,
        )
        thinned_input = args.thinned_message_table
    else:
        thin_args = _thin_argv(
            args, suffix=suffix, mode=mode, slice_start=slice_start, slice_end=slice_end,
            iteration=iteration, total_iterations=total_iterations,
        )
        logger.info("thin_port_messages %s [%s, %s] iter=%d/%d",
                    mode, slice_start, slice_end, iteration, total_iterations)
        rc = dit_docker.run(args.image_tag, thin_args, **_docker_run_kwargs(args))
        if rc != 0:
            raise SystemExit(
                f"thin_port_messages failed (rc={rc}, mode={mode}, slice=[{slice_start}, {slice_end}])"
            )
        thinned_input = _thinned_table(args, suffix, mode)

    if STEP_VISITS not in args.steps:
        return

    visits_args = _visits_argv(
        args, suffix=suffix, mode=mode, thinned_input=thinned_input, slice_end=slice_end,
        iteration=iteration, total_iterations=total_iterations,
    )
    logger.info("port_visits %s [%s, %s] iter=%d/%d",
                mode, args.start, slice_end, iteration, total_iterations)
    rc = dit_docker.run(args.image_tag, visits_args, **_docker_run_kwargs(args))
    if rc != 0:
        raise SystemExit(
            f"port_visits failed (rc={rc}, mode={mode}, slice_end={slice_end})"
        )


def _docker_run_kwargs(args: argparse.Namespace) -> dict:
    """Keyword arguments for ``dit_docker.run``. Every call mounts the GCP
    credentials volume; v5 calls also hand the BigQuery staging dataset to the
    DIT overlay patch inside the container (upstream has no option for it)."""
    kwargs: dict = {
        "entrypoint": "pipe-anchorages",
        "build_from_source": args.build_from_source,
        "volumes": [GCP_VOLUME],
    }
    if _pipeline_cli(args) == PIPELINE_CLI_V5:
        kwargs["container_env"] = {TEMP_DATASET_ENV: args.bq_temp_dataset}
    return kwargs


def _thin_argv(
    args: argparse.Namespace,
    *,
    suffix: str,
    mode: str,
    slice_start: date,
    slice_end: date,
    iteration: int,
    total_iterations: int,
) -> list[str]:
    """argv for step 1 (``thin_port_messages`` / ``port-state-transitions``)
    over the inclusive slice ``[slice_start, slice_end]``."""
    options = _pipeline_options(
        args, step=STEP_THIN, mode=mode,
        iteration=iteration, total_iterations=total_iterations,
        slice_start=slice_start, slice_end=slice_end,
    )
    if _pipeline_cli(args) == PIPELINE_CLI_V5:
        return [
            "port-state-transitions",
            f"--start-date={slice_start.isoformat()}",
            f"--end-date={_exclusive_end(slice_end)}",
            f"--bq-in-named-anchorages={args.named_anchorages}",
            f"--bq-in-messages={_messages_table(args)}",
            f"--bq-out-port-state-transitions={_thinned_table(args, suffix, mode)}",
            *options,
        ]
    return [
        "thin_port_messages",
        f"--start_date={slice_start.isoformat()}",
        f"--end_date={slice_end.isoformat()}",
        f"--anchorage_table={args.named_anchorages}",
        f"--input_table={_messages_table(args)}",
        f"--output_table={_thinned_table(args, suffix, mode)}",
        f"--temp_dataset={args.bq_temp_dataset}",
        *options,
    ]


def _visits_argv(
    args: argparse.Namespace,
    *,
    suffix: str,
    mode: str,
    thinned_input: str,
    slice_end: date,
    iteration: int,
    total_iterations: int,
) -> list[str]:
    """argv for step 2 (``port_visits`` / ``port-visits``), a full recompute
    over ``[args.start, slice_end]`` inclusive, as in production."""
    options = _pipeline_options(
        args, step=STEP_VISITS, mode=mode,
        iteration=iteration, total_iterations=total_iterations,
        slice_start=_parse_date(args.start), slice_end=slice_end,
    )
    if _pipeline_cli(args) == PIPELINE_CLI_V5:
        return [
            "port-visits",
            f"--start-date={args.start}",
            f"--end-date={_exclusive_end(slice_end)}",
            f"--bq-in-port-state-transitions={thinned_input}",
            f"--bq-in-segment-info={_segment_info_table(args)}",
            f"--bq-out-port-visits={_visits_table(args, suffix, mode)}",
            f"--bad-segs={_bad_segs_sql(args)}",
            *options,
        ]
    return [
        "port_visits",
        f"--start_date={args.start}",
        f"--end_date={slice_end.isoformat()}",
        f"--thinned_message_table={thinned_input}",
        f"--vessel_id_table={_segment_info_table(args)}",
        f"--output_table={_visits_table(args, suffix, mode)}",
        f"--bad_segs={_bad_segs_sql(args)}",
        f"--temp_dataset={args.bq_temp_dataset}",
        *options,
    ]


# --------------------------------------------------------------------------
# Modes
# --------------------------------------------------------------------------

def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def execute_bf(args: argparse.Namespace, suffix: str) -> None:
    start = _parse_date(args.start)
    end = _parse_date(args.end)
    _run_slice(args, mode=MODE_BF, slice_start=start, slice_end=end, suffix=suffix,
               iteration=1, total_iterations=1)


def execute_bfd(args: argparse.Namespace, suffix: str) -> None:
    start = _parse_date(args.start)
    end = _parse_date(args.end)
    initial_end = end - timedelta(days=args.tail_days)
    total = 1 + args.tail_days
    _run_slice(args, mode=MODE_BFD, slice_start=start, slice_end=initial_end, suffix=suffix,
               iteration=1, total_iterations=total)
    # daterange_inclusive is half-open per dit.dates contract; +1 day on end
    # to include the final `end` date.
    for i, d in enumerate(
        dit_dates.daterange_inclusive(initial_end + timedelta(days=1), end + timedelta(days=1)),
        start=2,
    ):
        _run_slice(args, mode=MODE_BFD, slice_start=d, slice_end=d, suffix=suffix,
                   iteration=i, total_iterations=total)


def execute_bftruncate(args: argparse.Namespace, suffix: str) -> None:
    start = _parse_date(args.start)
    end = _parse_date(args.end)
    total = 1 + args.tail_days
    _run_slice(args, mode=MODE_BFTRUNCATE, slice_start=start, slice_end=end, suffix=suffix,
               iteration=1, total_iterations=total)
    for i, d in enumerate(
        dit_dates.daterange_inclusive(end - timedelta(days=args.tail_days - 1), end + timedelta(days=1)),
        start=2,
    ):
        _run_slice(args, mode=MODE_BFTRUNCATE, slice_start=d, slice_end=d, suffix=suffix,
                   iteration=i, total_iterations=total)


# --------------------------------------------------------------------------
# Run cache integration (M5b of dit.cache rollout; see docs/run-cache-impl.md)
# --------------------------------------------------------------------------

def _submitter_image_identity(args: argparse.Namespace) -> dict:
    """Cache-key contribution that content-addresses the **submitter** image.

    SOUNDNESS (M5 fix): a port-visits run loads pipe-anchorages code in TWO
    places, both of which determine output:

    * the **submitter** image (``args.image_tag``, run by ``dit_docker.run``)
      builds and submits the Beam graph -- the pipeline-construction code; and
    * the **worker** image (``--sdk_container_image={args.worker_image}``) runs
      the DoFns on Dataflow.

    The cache key already content-addresses the worker image (via
    ``worker_image_digest``) and the local source (via ``pipeline_commit``).
    Without the submitter image too, two runs with the SAME worker image + SAME
    local commit but DIFFERENT submitter images would collide on a wrong cache
    hit. So the submitter image's identity must be in the key. Two cases:

    * ``build_from_source=True`` -- the submitter image is built fresh from the
      local worktree, so its content is already captured by ``pipeline_commit``
      (a real or deterministic-snapshot SHA). No extra field needed; we record
      a marker so a build-from-source run and a published-image run at the same
      commit still differ in the key (they are different artefacts).
    * ``build_from_source=False`` -- the submitter is the published
      ``args.image_tag``. We add its resolved digest (``submitter_image_digest``,
      stamped on ``args`` in :func:`main` via the same
      :func:`resolve_worker_image_to_digest` machinery the worker image uses).
      If resolution failed, ``main`` stamps the raw tag string instead so
      different tags at least don't collide (logged there as tag-based).

    Mode-independent (the submitter image is identical across modes), so this
    sits outside the ``_MODES_USING_TAIL`` branch.
    """
    if args.build_from_source:
        return {"submitter_build_from_source": True}
    # Published submitter image: digest (preferred) or raw tag (fallback),
    # stamped on args in main(). getattr keeps the compare-only / test paths
    # that never call main() working (they don't hit a real submitter image).
    return {
        "submitter_image_digest": getattr(
            args, "submitter_image_digest", args.image_tag
        )
    }


def canonical_params_dict(args: argparse.Namespace, mode: str) -> dict:
    """Output-affecting params for a port-visits mode-equivalence run.

    Mode-aware (matches pipe-gaps): only the fields each mode actually
    consumes contribute to its cache key. The daily-slice modes
    (``2_bfd`` / ``3_bftruncate``) read ``tail_days``; ``1_bf`` runs a single
    big range and ignores it, so including ``tail_days`` in BF's key would
    invalidate its cache on every ``--tail-days`` change for no behavioural
    reason.

    Output content depends on: the mode, the inclusive date window, the
    three RESOLVED source-table FQNs (`messages_positions`,
    `segment_info`, `segs_activity` — derived from
    ``--source-dataset-stem`` by default, overridable per table via the
    ``--source-{messages,segment-info,segs-activity}-fqn`` flags; the
    cache key is on the resolved FQN, NOT the stem or which CLI shape
    produced it, so two runs pointing at the same three tables share a
    cache row regardless of which knob they used), the named-anchorages
    reference, the **submitter image identity** (see
    :func:`_submitter_image_identity` -- the pipeline-construction code that
    builds/submits the Beam graph; mode-independent), the selected steps
    (only when not both), and -- with ``--steps port_visits`` --
    ``thinned_message_table`` (step 2 reads pre-thinned messages from that
    table, which changes what step 2 sees). Each source is in the key only
    when a selected step reads it. Excludes plumbing (datasets,
    regions, SAs), naming (experiment_id, suffix, binding), and runner-only
    knobs (worker_image is in the key via ``worker_image_digest`` on the
    CacheKey itself; ``parallel``, the worker cap and the worker sizing don't
    affect output). NOTE: ``image_tag``
    is NOT a pure runner-only knob -- it IS the submitter image, so its
    identity is folded in via ``_submitter_image_identity`` above.
    """
    # Cache key includes the RESOLVED source FQNs, not the stem-with-halves
    # shorthand. Two runs that point at the same three tables -- one via
    # --source-dataset-stem, one via the three per-table --source-*-fqn
    # overrides -- must share a cache row. Two runs with DIFFERENT effective
    # source tables (e.g. cross_version_ais.py routing each binding at its
    # own snapshot FQNs) must NOT. Storing resolved FQNs answers both.
    # M4 of canonical-dataset migration: cache-key shape change. First run
    # after merge misses; subsequent runs converge.
    #
    # Step-aware like the modes: a field is in the key only when a selected
    # step reads it (step 1: messages + named anchorages; step 2: segment_info,
    # segs_activity, and the external thinned table when step 1 is skipped).
    # With both steps every field is present and the key is unchanged.
    params: dict = {
        "mode": mode,
        "start": args.start,
        "end": args.end,
        **_submitter_image_identity(args),
    }
    if STEP_THIN in args.steps:
        params["source_messages_fqn"] = _messages_table(args)
        params["named_anchorages"] = args.named_anchorages
    if STEP_VISITS in args.steps:
        params["source_segment_info_fqn"] = _segment_info_table(args)
        params["source_segs_activity_fqn"] = _segs_activity_table(args)
        params["thinned_message_table"] = args.thinned_message_table
    # The steps decide which table a row's output is, so a subset must not
    # share a row with a full run. Added only for subsets, so every
    # both-steps key stays byte-identical.
    if list(args.steps) != list(SELECTABLE_STEPS):
        params["steps"] = list(args.steps)
    if mode in _MODES_USING_TAIL:
        params["tail_days"] = args.tail_days
    # v5 drives a different pipeline CLI (and an overlay image), so its rows
    # must not satisfy v4 lookups. Added only for v5 so every existing v4 key
    # stays byte-identical.
    if _pipeline_cli(args) == PIPELINE_CLI_V5:
        params["pipeline_cli"] = PIPELINE_CLI_V5
    return params


def _build_cache_key(args: argparse.Namespace, mode: str) -> CacheKey:
    """Compose a :class:`CacheKey` for the given mode from the stamped
    ``args`` context (commit + worker-image digest resolved in :func:`main`)."""
    return CacheKey(
        pipeline_commit=args.commit_sha,
        worker_image_digest=args.worker_image_digest,
        workflow_file_sha1=WORKFLOW_FILE_SHA1,
        params=canonical_params_dict(args, mode),
    )


def _run_with_cache(args: argparse.Namespace, mode: str, suffix: str, execute_fn) -> str:
    """Workflow-local adapter over :func:`dit.workflow.run_with_cache`.

    Builds the mode-aware :class:`CacheKey` and threads the per-run
    :class:`RunContext` (stamped on ``args`` in :func:`main`) + the
    port-visits workflow/pipeline identity into the generic wrapper. Returns
    the FQN of the last step's output to compare (cached on hit, freshly
    computed on miss).

    With both steps, the chain (thin_port_messages -> port_visits) produces TWO
    tables per mode: the visits table (the comparison target, passed as
    ``output_fqn``) and the per-mode thinned ``port_events_<suffix>_<mode>``
    intermediate. Both are recorded on the cache row so ``cancel_run`` cleans
    up both -- otherwise the intermediate is orphaned. With one step there is
    one table of ours: the thinned table for ``--steps thin_port_messages``,
    the visits table for ``--steps port_visits`` (whose input is the caller's
    ``--thinned-message-table``, never a cleanup target).
    """
    cache_key = _build_cache_key(args, mode)
    output_fqn = _output_table(args, suffix, mode)
    extra_output_tables = (
        (_thinned_table(args, suffix, mode),) if list(args.steps) == list(SELECTABLE_STEPS)
        else ()
    )
    return dit_workflow.run_with_cache(
        execute_fn,
        ctx=args.run_context,
        workflow=WORKFLOW_NAME,
        pipeline=PIPELINE_NAME,
        experiment_id=args.experiment_id,
        cache_key=cache_key,
        output_fqn=output_fqn,
        execute_kwargs={"args": args, "suffix": suffix},
        extra_output_tables=extra_output_tables,
        log_label=mode,
        use_cache=not args.no_cache,
    )


# --------------------------------------------------------------------------
# Comparisons
# --------------------------------------------------------------------------

def compare_all(
    mode_fqns: dict[str, str],
    modes: Sequence[str],
    keys: Sequence[str] = COMPARE_KEYS,
) -> int:
    """Pairwise-compare the selected modes' output tables on ``keys``.

    ``mode_fqns`` maps each mode label to the FQN to compare -- the
    cached-or-fresh table returned by the cache wrapper (a cache hit reuses a
    prior run's UUID-suffixed table, so the FQN is NOT derivable from the
    current run's ``suffix`` alone). The tables are the last selected step's
    output, and ``keys`` its key (``COMPARE_KEYS_BY_STEP``); the default is
    the visits key.
    """
    # Pairs over the SELECTED modes only -- comparing against a mode that
    # never ran would diff an absent table and report a spurious difference.
    # ``modes`` arrives in canonical order (parse_modes), so pair order is
    # stable regardless of the order given on the command line.
    pairs = list(itertools.combinations(modes, 2))
    if not pairs:
        # Single-mode run (the cheap smoke shape). Say so explicitly rather
        # than returning a clean 0 that reads like a passed equivalence
        # assertion which actually never ran.
        only = modes[0]
        logger.info(
            "only one mode selected (%s) -- no pair to compare. Its output "
            "table is %s. Add a second mode to --modes to get a comparison.",
            only, mode_fqns[only],
        )
        return 0
    overall = 0
    for a, b in pairs:
        rc = dit_compare.compare_tables(
            mode_fqns[a],
            mode_fqns[b],
            keys=keys,
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
    # Infra knobs identical to both workflows (--dest-dataset, --service-account,
    # --dataflow-*) come from dit.workflow.add_infra_args (called below).
    p.add_argument("--bq-temp-dataset", default=DEFAULT_BQ_TEMP_DATASET,
                   help="Pre-existing BQ dataset for Beam EXPORT staging; "
                        "env-var fallback DIT_BQ_TEMP_DATASET (defaults to "
                        "${PROJECT}.${DIT_DEST_DATASET}). v4 passes it as --temp_dataset; "
                        "v5 passes it to the overlay image as the DIT_BQ_TEMP_DATASET "
                        "container env var.")
    p.add_argument("--pipeline-cli", choices=PIPELINE_CLIS, default=DEFAULT_PIPELINE_CLI,
                   help="pipe-anchorages CLI generation to drive. v4 (default): the v4.6.4 "
                        "CLI production pins (thin_port_messages / port_visits). v5: the CLI on "
                        "pipe-anchorages main (port-state-transitions / port-visits, exclusive "
                        "--end-date, required labels); needs a DIT overlay image as --image-tag "
                        "and --worker-image, see docker/overlays/pipe-anchorages-temp-dataset/.")
    p.add_argument("--source-dataset-stem", default=DEFAULT_SOURCE_DATASET_STEM,
                   help=f"Staging dataset stem (default {DEFAULT_SOURCE_DATASET_STEM}); "
                        "_internal and _published are appended to derive the three source "
                        "table FQNs. Per-table overrides via --source-{messages,segment-info,"
                        "segs-activity}-fqn take precedence when set.")
    # Per-table FQN overrides (M4 of canonical-dataset migration). Default None
    # so absence triggers stem-derivation in the helpers; explicit FQNs win.
    # cross_version_ais.py (M5) uses these to point at snapshots in
    # tech_great_expectations that don't follow the stem-with-halves shape.
    p.add_argument("--source-messages-fqn", default=None,
                   help="Fully-qualified `<project>.<dataset>.<table>` for the messages-positions "
                        "input; overrides --source-dataset-stem-based derivation for this table only. "
                        "The basename does not have to be `messages_positions` -- the workflow "
                        "treats whatever table you pass as the equivalent input. "
                        f"Default: stem-derived ({DEFAULT_SOURCE_MESSAGES_FQN}).")
    p.add_argument("--source-segment-info-fqn", default=None,
                   help="Fully-qualified `<project>.<dataset>.<table>` for the segment-info "
                        "input; overrides --source-dataset-stem-based derivation for this table only. "
                        f"Default: stem-derived ({DEFAULT_SOURCE_SEGMENT_INFO_FQN}).")
    p.add_argument("--source-segs-activity-fqn", default=None,
                   help="Fully-qualified `<project>.<dataset>.<table>` for the segs-activity "
                        "input (used by --bad-segs filtering); overrides --source-dataset-stem-based "
                        "derivation for this table only. "
                        f"Default: stem-derived ({DEFAULT_SOURCE_SEGS_ACTIVITY_FQN}).")
    p.add_argument("--named-anchorages", default=DEFAULT_NAMED_ANCHORAGES)
    p.add_argument("--start", default=DEFAULT_START,
                   help="Inclusive start date (also pipeline data_available_from).")
    p.add_argument("--end", default=DEFAULT_END, help="Inclusive end date.")
    p.add_argument("--tail-days", type=int, default=DEFAULT_TAIL_DAYS,
                   help="Number of tail days for bfd / bftruncate iteration.")
    add_max_num_workers_arg(
        p, default=DEFAULT_MAX_NUM_WORKERS,
        help_detail="The default is sized for the 2020 staging cohort; e.g. 16 for one "
                    "month of production AIS, 100 for production's full-history step.",
    )
    add_worker_sizing_args(
        p, help_detail="Production runs both steps on e2-highmem-4 with 50 GB; "
                       "cross_version_ais.py --baseline-production passes those.",
    )
    p.add_argument("--image-tag", default=DEFAULT_IMAGE_TAG)
    p.add_argument("--worker-image", default=DEFAULT_WORKER_IMAGE,
                   help="Dataflow worker container image with pipe_anchorages installed; "
                        "passed to Beam as --sdk_container_image.")
    p.add_argument("--build-from-source", action="store_true",
                   help="Fall back to `docker compose run dev` when no published image is available.")
    p.add_argument("--suffix", default=None,
                   help="Output-table suffix; auto-generated from git HEAD when omitted.")
    add_experiment_id_arg(p)
    add_no_cache_arg(p)
    p.add_argument("--binding-name", default="",
                   help="Optional binding label (e.g. 'before', 'after') used by the "
                        "cross-version wrapper. Surfaces in Dataflow job names and BQ labels "
                        "(dit_binding=<name>). Empty when running standalone.")
    p.add_argument("--steps", default=",".join(SELECTABLE_STEPS),
                   help=f"Comma-separated pipeline steps to run, in pipeline order "
                        f"(default {','.join(SELECTABLE_STEPS)}). {STEP_VISITS} alone needs "
                        f"--thinned-message-table as its input; {STEP_THIN} alone stops after "
                        f"step 1. The modes compare the last step's output: visits on "
                        f"{','.join(COMPARE_KEYS)}, thinned messages on "
                        f"{','.join(THIN_COMPARE_KEYS)}.")
    p.add_argument("--thinned-message-table", default=None,
                   help="Fully-qualified BQ table holding pre-thinned port messages (e.g. "
                        "production's raw_port_events), read by step 2 (port_visits) instead of "
                        "the per-mode port_events_<suffix>_<mode>. Only with --steps port_visits. "
                        "Useful when the change under test is in step 2 only (e.g. PORT_GAP_BEGIN "
                        "anchorage fixes) -- saves the dominant cost of running the thin step on "
                        "full AIS data. Cross-version runs pin it (cross_version_ais.py copies "
                        "it at run start).")
    p.add_argument("--require-clean", action="store_true",
                   help="Error on a dirty tree instead of auto-snapshotting "
                        "(for CI / strict-provenance callers).")
    p.add_argument("--skip-pipelines", action="store_true",
                   help="Skip the pipeline phase; only run comparisons.")
    p.add_argument("--skip-comparisons", action="store_true",
                   help="Skip the comparison phase; only run pipelines.")
    p.add_argument("--parallel", action="store_true",
                   help="Run the three modes' pipelines in parallel threads (each submits Dataflow concurrently).")
    # Infra knobs: --dest-dataset, --service-account, --dataflow-region,
    # --dataflow-temp-bucket, --dataflow-subnetwork (identical to both workflows).
    add_infra_args(p)
    add_modes_arg(p, choices=SELECTABLE_MODES)
    args = p.parse_args(argv)
    # Validate before any cloud call: a typo'd mode that silently ran
    # nothing would look exactly like a passing run.
    args.modes = parse_modes(args.modes, choices=SELECTABLE_MODES)
    args.steps = parse_choice_list(
        args.steps, choices=SELECTABLE_STEPS, flag="--steps", noun="step",
    )
    validate_steps(args.steps, args.thinned_message_table)
    # Flag > ceiling env var > default; see dit.workflow.resolve_max_num_workers.
    args.worker_cap = worker_cap(args.max_num_workers, default=DEFAULT_MAX_NUM_WORKERS)
    args.max_num_workers = args.worker_cap.workers
    # No default sizing of its own: unset flags emit nothing.
    args.worker_sizing = worker_sizing_from_args(args)
    return args


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

    # Resolve the committed ref, worker image, and per-run lineage context in
    # one shot via the shared workflow harness (no-dirty-tree policy + the
    # --suffix manual/cross-version escape hatch live in dit.workflow). Pass
    # `args.suffix or None` so a falsy suffix (the default None or an empty
    # string) routes through the auto-snapshot path -- matching this workflow's
    # historical truthy `if args.suffix:` check. resolve_digest=True (M5b):
    # the run cache keys on the worker-image digest, so we resolve it here.
    ctx = resolve_run_context(
        repo_dir=repo_dir,
        pipeline_name=PIPELINE_NAME,
        runner=args.runner,
        require_clean=args.require_clean,
        suffix=args.suffix or None,
        worker_image=args.worker_image,
        default_worker_image=DEFAULT_WORKER_IMAGE,
        # Run-cache wired in M5b: resolve the worker image to a content-
        # addressable digest so :tag retags invalidate the cache cleanly.
        # Falls back to the tag form on failure (cache misses, run still works).
        resolve_digest=True,
    )
    args.run_context = ctx
    args.commit_sha = ctx.pipeline_commit
    args.unreviewed = ctx.unreviewed
    args.worker_image = ctx.worker_image
    args.worker_image_digest = ctx.worker_image_digest
    args.run_id = ctx.run_id

    # SOUNDNESS (Fix 1): the submitter image (args.image_tag, run by
    # dit_docker.run to build/submit the Beam graph) is a second code source
    # that determines output, alongside the worker image. For a published
    # submitter image (no --build-from-source) resolve it to a content-
    # addressable digest the same way resolve_run_context resolves the worker
    # image, so two different published submitter images don't collide on a
    # cache hit. build_from_source folds into pipeline_commit instead (the
    # image is built from the worktree) -- see _submitter_image_identity.
    if not args.build_from_source:
        try:
            args.submitter_image_digest = resolve_worker_image_to_digest(args.image_tag)
        except RuntimeError as e:
            logger.warning(
                "could not resolve submitter image %s to a digest (%s); the "
                "cache key falls back to the raw tag string for this run "
                "(tag-based, NOT digest-based -- a :tag retag won't invalidate "
                "the cache). Different tags still don't collide.",
                args.image_tag, e,
            )
            args.submitter_image_digest = args.image_tag

    suffix = _resolve_suffix(args)

    # Per-invocation lineage attributes, computed once and stashed on args so
    # every Dataflow job + BQ output table from this run shares them. See
    # the dit_run_id / dit_commit_sha / dit_worker_image_tag / dit_launched_by
    # labels in _dynamic_labels. run_id comes from the resolved RunContext;
    # the worker-image tag + launched-by are port-visits-specific label fields.
    args.worker_image_tag = _worker_image_tag(args.worker_image)
    args.launched_by = os.environ.get("USER", "unknown")

    logger.info("experiment_id: %s", args.experiment_id)
    logger.info("suffix: %s", suffix)
    logger.info("run_id: %s  commit_sha: %s  worker_image_tag: %s  launched_by: %s",
                args.run_id, args.commit_sha, args.worker_image_tag, args.launched_by)
    logger.info("steps: %s (comparing %s output on %s)", ",".join(args.steps),
                _last_step(args), ",".join(COMPARE_KEYS_BY_STEP[_last_step(args)]))
    if STEP_THIN in args.steps:
        logger.info("source messages:     %s", _messages_table(args))
    else:
        logger.info("thinned input:       %s", args.thinned_message_table)
    if STEP_VISITS in args.steps:
        logger.info("source segment_info: %s", _segment_info_table(args))
        logger.info("source segs_activity: %s", _segs_activity_table(args))
    logger.info("date range (inclusive): %s -> %s, tail_days=%d", args.start, args.end, args.tail_days)
    logger.info("runner: %s  pipeline_cli: %s", args.runner, args.pipeline_cli)
    log_worker_caps({"every job": args.worker_cap})
    log_worker_sizings({"every job": args.worker_sizing})

    # Per-mode FQN to compare. Default to this run's freshly-suffixed output
    # tables (the last step's); the cache wrapper overwrites each with the
    # cached FQN on a hit.
    # This default is also what the --skip-pipelines (compare-only) path uses:
    # the user pre-populated those tables, so we compare them directly without
    # consulting the cache.
    mode_fqns: dict[str, str] = {
        mode: _output_table(args, suffix, mode)
        for mode in (MODE_BF, MODE_BFD, MODE_BFTRUNCATE)
    }

    if not args.skip_pipelines:
        # Each mode flows through the run cache: an identical
        # (commit, worker-image digest, workflow-file sha1, params) tuple
        # reuses a prior run's visits table instead of re-submitting Dataflow.
        # run_with_cache returns the FQN to compare (cached on hit, fresh
        # on miss). compare keys on the visits table only.
        # --modes gates which modes actually run. Each is cached
        # independently, so a later run adding a mode reuses the earlier
        # modes' output tables rather than recomputing them.
        _execs_by_mode = {
            MODE_BF: execute_bf,
            MODE_BFD: execute_bfd,
            MODE_BFTRUNCATE: execute_bftruncate,
        }
        mode_execs = [(m, _execs_by_mode[m]) for m in args.modes]
        skipped = [m for m in SELECTABLE_MODES if m not in args.modes]
        if skipped:
            logger.info(
                "--modes: running %s; skipping %s",
                ",".join(args.modes), ",".join(skipped),
            )
        if args.parallel:
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(mode_execs)) as pool:
                future_to_mode = {
                    pool.submit(_run_with_cache, args, mode, suffix, fn): mode
                    for mode, fn in mode_execs
                }
                for fut in concurrent.futures.as_completed(future_to_mode):
                    mode_fqns[future_to_mode[fut]] = fut.result()
        else:
            for mode, fn in mode_execs:
                mode_fqns[mode] = _run_with_cache(args, mode, suffix, fn)

    if args.skip_comparisons:
        return 0
    return compare_all(mode_fqns, args.modes, keys=COMPARE_KEYS_BY_STEP[_last_step(args)])


if __name__ == "__main__":
    sys.exit(main())
