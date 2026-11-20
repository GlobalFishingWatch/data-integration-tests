"""Cross-version experiment for pipe-segment: identity-match-key change in gpsdio-segment.

Tests the impact of reducing ``MsgProcessor.match_key`` from
``(transponder_type, receiver_type, source)`` to ``(transponder_type,)`` in
gpsdio-segment (the AIS identity-to-position match heuristic). The change
lives in the gpsdio-segment library, not pipe-segment itself; we expose it
via two short-lived pipe-segment branches that pin different gpsdio-segment
SHAs (in ``pyproject.toml`` from v5.0.3 on; ``requirements/prod.in`` before).

No satellite timing offsets in DIT
----------------------------------
**DIT's segmenter workflows do not run pipe-segment's satellite timing
offsets** (owner decision, 2026-11-20). Production's ``segment`` task works
out, per receiver and hour, how far a satellite's clock is off, and drops the
messages of the hours above the threshold. A DIT ``segment`` run does neither:
it passes none of the four satellite flags and writes no offsets table. The
``--include-satellite-offsets`` flag is removed and rejected.

Why (measured or read on 2026-11-20):

* **The offsets must be computed from the full message feed, not from a test
  cohort.** The staging cohort's own offsets table
  (``pipe_ais_test_202408290000_published.satellite_timing_offsets``) was
  computed from the full satellite feed when the cohort was built; its table
  description names that feed as the source. Computed from the cohort's 142
  vessels, the offsets come out different and noisy: for January 2020, 990
  receiver-hours above the segmenter's 30 s threshold against 183 in the
  cohort's offsets table, with 39 in common. A run on such offsets would drop
  the wrong messages.
* **The full feed is too large to read on every test run:** the 2020 messages
  are about 4.1 TB.
* **pipe-segment has no option to read precomputed offsets.** ``segment``
  computes them from three inputs and uses them in the same run
  (``pipeline.py`` lines 195 to 222 at v5.0.3).
* Also true, and secondary: the satellite positions are date-sharded tables in
  another organisation (366 tables and 139 GB for 2020), and the offsets query
  reads its message source through a table wildcard that does not match a
  partitioned table.

What follows from it:

* On the 2020 staging cohort, DIT's ``messages_segmented`` keeps the messages
  that the correction would drop. The cohort's offsets table lists 1,361 such
  receiver-hours in 2020, on 27 receivers and 173 days. For those hours DIT's
  staging output is not comparable with production's historical output.
* Comparisons between two DIT runs (modes, or base against head) are not
  affected: both sides run without the correction.
* The correction only ever applied to data that carries satellite receiver
  IDs. The current feed has none (since 2026-04-22), so production computes no
  offsets for current data either.

What would change this: an option in pipe-segment to read an offsets table
instead of computing one. DIT could then pass the cohort's existing offsets
table. That is the path, not planned work.

Pattern follows ``workflows/port_visits/cross_version_ais.py`` (git-worktree
each binding's ref) but the per-binding execution mirrors
``workflows/pipe_events/fishing.py`` (a docker-runner consumer that goes
through ``dit.runners.docker.run`` + ``dit.workflow.resolve_run_context``).

Steps:

1. Verify every binding's git ref exists in ``--pipeline-dir``.
2. Pin the source by copying it at run start
   (``dit.bq.pin_source_into_experiment``, PIPELINE-4705) into
   ``world-fishing-827.tech_great_expectations`` (default 7-day TTL). The one
   source is ``normalized_messages``, copied over the run's ``--date-range``:
   ``segment`` reads it by ``DATE(<partition column>) BETWEEN start AND end``
   and nothing else of it (``transform/read_messages.py`` at v5.0.3; no
   lookback), and no other step reads it. The copy keeps the source's
   partitioning, which pipe-segment reads the filter column from. Its name
   ends in the pin stamp (the run's start time in UTC, e.g.
   ``20261119t101500z``):
   ``dit_exp_<experiment_id>_pipe_segment_<source table>_<start>_<end>_<stamp>``.
   ``--reuse-pin <stamp>`` reuses an earlier run's copy, e.g. on a retry.
   ``--pin-max-gb`` caps the bytes the copy may process. Copies replaced
   BigQuery snapshots on 2026-11-19: BigQuery refuses a snapshot of a table
   in another organisation, and no DIT identity may create one with an
   expiration. ``--pin-source-at``, ``--snapshot-dest-project`` and
   ``--snapshot-expiration-days`` are gone; each is rejected with what
   replaces it.
3. For each binding (sequential -- chdir for ``dit_docker.run`` isn't
   thread-safe; with 2 bindings the cost is negligible):

   a. ``git worktree add`` a temp dir at the ref.
   b. ``resolve_run_context(repo_dir=worktree, pipeline_name="pipe-segment",
      runner="docker", suffix=<per-binding>, worker_image=args.image_tag,
      default_worker_image=DEFAULT_IMAGE_TAG, ...)``. The harness classifies
      the ref as reviewed/unreviewed and stamps ``ctx.worker_image`` --
      canonical published for reviewed code at the default tag; auto-built
      ``gcr.io/world-fishing-827/dit/pipe-segment:dit-<commit>`` (kaniko) for
      unreviewed code; an explicit ``--image-tag`` always wins.
   c. Sanity check: log the worktree's ``gpsdio-segment`` pin (catches a
      kaniko cache-hit silently reusing the wrong image -- a false
      "IDENTICAL" verdict is worse than a noisy diff).
   d. Run ``segment`` via ``dit_docker.run(image_tag=ctx.worker_image,
      args=[...], entrypoint="pipe-segment", volumes=["gcp:/root/.config"],
      service="dev", build_from_source=args.build_from_source)`` from the
      worktree CWD. Cloud-mode handling (``--network=cloudbuild``) is
      automatic via ``_apply_cloud_mode``. Pass
      ``--sdk_container_image=ctx.worker_image`` to the segment CLI so
      Dataflow workers run the same per-binding code.
   e. If ``--skip-downstream`` is not set, run the other five steps against
      the per-binding outputs (see "The six steps" below).

4. Diff: for each (output table, binding-pair) run ``dit.compare.compare_tables``.
   Output tables are date-partitioned single tables (since pipe-segment v5.0.0
   PIPELINE-2074), so no per-shard iteration. Diffs are informational (exit
   0 even when non-empty); the overall exit code is non-zero only if a
   binding execution itself failed.

The six steps
-------------
Each binding runs what production's segment group runs (the ``segment.*``
tasks of the AIS v3 DAG, pipe-segment v5.1.0), in this order:

1. ``segment`` (Beam): normalized messages -> ``messages_segmented``,
   ``segments``, ``fragments``.
2. ``segment_identity`` (Beam): ``segments``, ``fragments`` ->
   ``segment_identity_daily``.
3. ``segment_vessel_daily`` (BigQuery SQL): ``segment_identity_daily`` ->
   ``segment_vessel_daily``.
4. ``segment_info`` (BigQuery SQL): ``segment_identity_daily``,
   ``segment_vessel_daily`` -> ``segment_info``.
5. ``vessel_info`` (BigQuery SQL): the same two inputs -> ``vessel_info``.
6. ``segment_vessel`` (BigQuery SQL): ``segment_vessel_daily`` ->
   ``segment_vessel``.

Production runs the last three in parallel; here they run one after the other.

* The two Beam steps take the runner and, on Dataflow, the Dataflow options.
  ``--setup_file`` is passed only with ``--build-from-source``: the published
  image holds the installed package and no source tree, and production passes
  no setup file.
* The four SQL steps are BigQuery queries run by the container. They take
  their tables, ``--labels`` and ``--project`` and nothing else: no runner and
  no Dataflow option. They behave the same whatever ``--runner`` is.
* Every step gets ``--labels``. pipe-segment builds its BigQuery helper from
  them without a ``None`` guard, so a step without labels raises ``TypeError``
  before it does any work.

**What a run here does not reproduce.** A binding starts from empty output
tables, so ``segment`` has no fragment history to stitch onto and
``segment_vessel_daily`` sees only the run's own days in its 30-day window.
Both hold for every binding alike, so an A/B is fair; the outputs are not
comparable with production's own tables. Nor does a run apply the satellite
timing offsets (see "No satellite timing offsets in DIT" above).

**No ``--incremental`` here.** Since 2026-11-20 the AIS v3 DAG passes
``--incremental`` to ``segment`` (pipe-segment v5.1.0; composer-dags-production
PR #3145). This workflow does not, and its drift spec accepts that difference.
Why:

* **Bindings are arbitrary refs.** A ref older than v5.1.0 does not know the
  flag, and pipe-segment hands unknown flags to Beam instead of rejecting them
  (``cli/main.py`` line 47, ``parse_known_args``). Inferred, not run: such a
  binding would run full history while the other ran incremental, and the A/B
  would compare two segmenters instead of two refs.
* **Nothing here exercises what the flag changes.** It changes where a job
  takes the state of the day before its range from. A binding runs one
  ``segment`` job from empty tables, so that state is empty either way.
* **Whether the two segmenters agree is the subject of ``ais.py``**, the
  mode-equivalence workflow, and no run has confirmed it yet. Until one has,
  the cross-version A/B stays on the segmenter that needs no stored state.

Passing it later is one entry in ``_segment_args``'s ``extra_flags`` plus a
check that every binding's ref declares the flag.

``--dry-run`` skips the pipe-segment invocations and the diff phase, and only
dry-runs the copy (reporting its bytes against the ``--pin-max-gb`` cap). It
still sets up and tears down the worktrees.

Example (DirectRunner smoke, 1 day, single binding to validate plumbing)::

    dit run workflows/pipe_segment/identity_match_key.py \\
        --experiment-id pipe-segment-smoke \\
        --binding before=v5.1.0 \\
        --date-range 2020-01-01,2020-01-01 \\
        --runner DirectRunner \\
        --skip-downstream \\
        --build-from-source

Example (Dataflow A/B, 1 day, after the experiment branches are pushed)::

    dit run workflows/pipe_segment/identity_match_key.py \\
        --experiment-id idmatchkey \\
        --binding before=v5.1.0 \\
        --binding after=experiment/identity-match-key-transponder-only \\
        --date-range 2020-01-01,2020-01-01 \\
        --runner dataflow
"""
from __future__ import annotations

import argparse
import contextlib
import dataclasses
import functools
import itertools
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Optional, Sequence

from dit import bq as dit_bq
from dit import compare as dit_compare
from dit.dataflow_paths import dataflow_staging_location, dataflow_temp_location
from dit.job_names import make_job_name
from dit.runners import docker as dit_docker
from dit.workflow import (
    DEFAULT_DATAFLOW_TEMP_BUCKET,
    EXPERIMENT_ID_RE,
    NO_PUBLIC_IPS_OPTION,
    WorkerSizing,
    add_experiment_id_arg,
    add_max_num_workers_arg,
    add_worker_sizing_args,
    log_production_sizing_hint,
    log_worker_caps,
    log_worker_sizings,
    max_num_workers_options,
    resolve_run_context,
    resolve_worker_sizing,
    worker_cap,
    worker_sizing_from_args,
    worker_sizing_options,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Defaults
# --------------------------------------------------------------------------

# Pipeline repo name; namespaces auto-snapshot refs
# (refs/dit-snapshots/<PIPELINE_NAME>/<sha>) + the auto-built image registry
# path (gcr.io/world-fishing-827/dit/<PIPELINE_NAME>:dit-<commit>).
PIPELINE_NAME = "pipe-segment"

PROJECT = "world-fishing-827"
DEFAULT_DEST_DATASET = os.environ.get("DIT_DEST_DATASET", "tech_great_expectations")
DEFAULT_PROJECTS_DIR = os.environ.get("PROJECTS") or str(Path(__file__).resolve().parents[2].parent)
DEFAULT_PIPELINE_DIR = os.path.join(DEFAULT_PROJECTS_DIR, "pipe-segment")
DEFAULT_PIN_EXPIRATION_DAYS = 7
DEFAULT_PIN_MAX_GB = dit_bq.DEFAULT_PIN_MAX_BYTES / 1e9

#: The pipe-segment version the AIS v3 DAG's segment group is configured for:
#: ``SEGMENT = "v5.1.0"`` in composer-dags-production's ``dags/core/ais/v3.py``
#: since its PR #3145 (merged 2026-11-20; ``origin/main`` e4cb29361 read the
#: same day). Until then it was v5.0.3, which the VMS v5 DAG still pins. This
#: is what the DAG says; no production run was looked at. Shared with
#: ``ais.py``; ``composer_drift.py`` compares it with the tasks' image tags.
DEFAULT_PIPELINE_VERSION = "v5.1.0"

#: Where DIT's overlay images of pipe-segment are built.
OVERLAY_DIR = "docker/overlays/pipe-segment-temp-dataset"

#: The default image of both pipe-segment workflows: pipe-segment
#: DEFAULT_PIPELINE_VERSION (commit 4ddde03) with DIT's temp-dataset overlay
#: (``temp_dataset.patch``, sha256 04b40b54...), as ``build.sh`` in
#: OVERLAY_DIR names it. The published image cannot be the default: upstream
#: has no way to name the dataset Beam stages query results in, and DIT's
#: service account may not create one.
#: NOT BUILT YET (2026-11-20): the tag follows from the commit and the patch,
#: and the image exists once ``build.sh <repo> v5.1.0 v5.1.0 --push`` has run.
#: When the DAG's version moves: build the overlay image of the new tag, then
#: bump both constants.
DEFAULT_OVERLAY_IMAGE = (
    f"gcr.io/world-fishing-827/dit/pipe-segment:"
    f"{DEFAULT_PIPELINE_VERSION}-4ddde03-temp-dataset-04b40b54"
)

# This workflow's default image, for reviewed code. For unreviewed code
# (snapshot / unmerged experiment branch), ``resolve_run_context`` ->
# ``ensure_pipeline_image`` auto-builds a content-addressable
# ``gcr.io/world-fishing-827/dit/pipe-segment:dit-<commit>`` via the M-pivot-4
# kaniko machinery, and the docker runner pulls THAT. Such a build is plain
# source without the overlay: fine on DirectRunner, expected to fail on
# Dataflow as the test service account (``_run_binding`` warns).
# ``--build-from-source`` short-circuits both paths (compose builds the
# ``dev`` service from the worktree's working tree).
DEFAULT_IMAGE_TAG = DEFAULT_OVERLAY_IMAGE

#: Container variable the overlay patch reads to pass ``temp_dataset`` to
#: Beam's ReadFromBigQuery (see OVERLAY_DIR). An image without the overlay
#: ignores it.
TEMP_DATASET_ENV = "DIT_BQ_TEMP_DATASET"

#: Pre-existing dataset Beam stages query results in.
DEFAULT_BQ_TEMP_DATASET = os.environ.get(
    TEMP_DATASET_ENV, f"{PROJECT}.{DEFAULT_DEST_DATASET}"
)

# Compose service + entrypoint pipe-segment v5.0.x exposes:
#   docker compose run --rm --entrypoint pipe-segment dev segment <args>
# (pyproject.toml's [project.scripts] = "pipe-segment". This differs from
# pipe-events, which exposes "pipe" -- be careful not to mirror that one.
# The original draft set CLI_ENTRYPOINT="pipe" by analogy with pipe-events
# and surfaced as `executable file not found in $PATH` on the first smoke.)
COMPOSE_SERVICE = "dev"
CLI_ENTRYPOINT = "pipe-segment"

# GCP auth: shared named volume mounted into the container at /root/.config.
# On laptop, populate via:  docker volume create gcp +
# (from the pipeline checkout)  docker compose run gcloud auth application-default login.
# In Cloud Build, ``DIT_CLOUD_MODE`` triggers ``--network=cloudbuild`` instead
# (handled by ``dit.runners.docker._apply_cloud_mode``) -- the volume mount is
# dropped, the container reaches the cloud-build per-build fake metadata server.
GCP_VOLUME = "gcp:/root/.config"

# Default source: the staging-cohort normalized messages table, matching the
# dit convention used by pipe-gaps and port-visits (`pipe_ais_test_202408290000_*`).
# Single date-partitioned table (partitioned on `timestamp`, clustered on `ssvid`);
# see README.md § "Source data targets".
# Override via --source-normalized-table for ad-hoc runs against a different
# normalized source (e.g. prod cohort, a custom-filtered subset).
DEFAULT_SRC_NORMALIZED_TABLE = (
    f"{PROJECT}.pipe_ais_test_202408290000_internal.normalized_messages"
)

# Role of the pinned copy in its name, and the column its window is on.
# `segment` filters its source on DATE(<the source's partition column>), or on
# DATE(timestamp) when the source is not time-partitioned
# (utils/bq_source.py at v5.0.3). The default source is partitioned on
# `timestamp` (checked with `bq show`, 2026-11-19). A source partitioned on
# another column would be copied over one column and read over another; the
# workflow does not check this.
PIN_ROLE = "pipe_segment"
NORMALIZED_WINDOW_COLUMN = "timestamp"

# No satellite timing offsets in DIT (owner decision, 2026-11-20; the module
# docstring has the full statement and the figures).
#
# Production's `segment` task passes three satellite-offset inputs and an
# offsets output table. This workflow passes none of the four, has no code
# path that could, and compares no offsets table. That is an accepted
# difference in its drift spec (composer_drift.py).
#
# `--include-satellite-offsets` once asked for that branch. It is still
# parsed, hidden, so an old invocation fails with the message below instead
# of argparse's "unrecognized arguments".
SATELLITE_OFFSETS_REMOVED = (
    "--include-satellite-offsets was removed: DIT's segmenter workflows do not run "
    "satellite timing offsets (owner decision, 2026-11-20; PIPELINE-4705). The offsets "
    "must be computed from the full message feed, not from a test cohort: computed "
    "from the staging cohort's 142 vessels they come out different and noisy, and a "
    "run on them would drop the wrong messages. The full feed is too large to read on "
    "every test run (about 4.1 TB for 2020), and pipe-segment has no option to read "
    "precomputed offsets. Its inputs are also date-sharded tables in another "
    "organisation, which DIT does not support. Drop the flag. Every run leaves the "
    "correction off, so comparisons between two DIT runs are unaffected. See \"No "
    "satellite timing offsets in DIT\" in workflows/pipe_segment/RUN_INSTRUCTIONS.md."
)
_SATELLITE_OFFSETS_DEST = "removed_include_satellite_offsets"

# Output tables produced by `segment`. Single date-partitioned tables since
# pipe-segment v5.0.0 (PIPELINE-2074: sharded date tables to partitioned
# tables). Each is one FQN; pipe-segment writes within the date range via
# the partitioning_field="timestamp" column. No _YYYYMMDD suffix.
SEGMENT_OUTPUT_TABLES = (
    "messages_segmented",
    "segments",
    "fragments",
)

# The six pipe-segment commands one binding runs, in the order production's
# segment group runs them (the segment.* tasks of the AIS v3 DAG: segment,
# segment_identity_daily, segment_vessel_daily, then segment_info, vessel_info
# and segment_vessel). --skip-downstream runs the first only.
BEAM_STEPS = ("segment", "segment_identity")
# BigQuery SQL commands at v5.0.3, not Beam jobs: each builds a BigQuery
# client, creates or clears its destination and runs one query per day
# (segment_vessel_daily) or one query (the other three). They parse only their
# own flags and ignore the rest, so Dataflow options mean nothing to them.
SQL_STEPS = ("segment_vessel_daily", "segment_info", "vessel_info", "segment_vessel")
STEPS = (*BEAM_STEPS, *SQL_STEPS)

# Tables the five downstream steps write, one per step, in step order.
DOWNSTREAM_OUTPUT_TABLES = (
    "segment_identity_daily",
    "segment_vessel_daily",
    "segment_info",
    "vessel_info",
    "segment_vessel",
)

# Dataflow worker cap per Beam step, matching production (segment.segment and
# segment.segment_identity_daily run with 50 in core.ais.v3 and
# core.vms.v5.daily). The SQL steps are not Dataflow jobs and have no cap.
# These are the defaults of the shared resolution (PIPELINE-4701): an explicit
# --max-num-workers replaces both, and DIT_MAX_NUM_WORKERS_CEILING lowers each
# to min(step default, ceiling).
STEP_MAX_NUM_WORKERS = {
    "segment": 50,
    "segment_identity": 50,
}


def _step_max_num_workers(step: str, resolved: int | None) -> int:
    """The cap for ``step``: its resolved cap (``args.worker_caps``) when given,
    else the step's default."""
    return STEP_MAX_NUM_WORKERS[step] if resolved is None else resolved


# Worker sizing per step (--worker-machine-type / --disk-size-gb), mirroring
# production (owner rule 2026-10-23; the caps above do not follow it).
# composer-dags-production origin/main 0a644da, rendered snapshots
# core_ais_v3.py, core_vms_v3_last_4days_daily.py and core_vms_v5_daily.py:
# segment.segment and segment.segment_identity_daily both run with
# --worker_machine_type=e2-standard-4 --disk_size_gb=50 (segment's predate the
# flags in dit). The SQL steps have no workers to size.
# Hand-written until PIPELINE-4700 reads them from composer. A flag replaces
# that field in both Beam steps.
STEP_WORKER_SIZING = {
    "segment": WorkerSizing(machine_type="e2-standard-4", disk_size_gb=50),
    "segment_identity": WorkerSizing(machine_type="e2-standard-4", disk_size_gb=50),
}


def _step_worker_sizing(step: str, resolved: WorkerSizing | None) -> WorkerSizing:
    """The sizing for ``step``: its resolved sizing (``args.worker_sizings``)
    when given, else the step's default."""
    return STEP_WORKER_SIZING[step] if resolved is None else resolved


# Compare-key map per output table. Verified against pipe_segment/schemas/
# (segment_schema.py, message_schema.py) and segment_identity/pipeline.py.
# Each row in the date-partitioned tables is uniquely identified by these
# columns. NOTE: avoid using `summary_timestamp` for segment_identity_daily
# -- that's the row-creation timestamp and will differ between runs by
# wall-clock time even when the data is identical. `first_timestamp` is
# the first POSITION message timestamp in the segment for the day and is
# stable if the segment composition is.
#
# The keys of the three tables added on 2026-11-19 (segment_vessel_daily,
# vessel_info, segment_vessel) come from the final GROUP BY of each step's
# query at v5.0.3 (src/pipe_segment/assets/queries/*.sql.j2), so uniqueness
# follows from the SQL; no run has confirmed it:
#   * segment_vessel_daily: per processed day, one row per (ssvid, seg_id)
#     (segment_most_common GROUP BY ssvid, seg_id; the join that adds the
#     vessel id is one row per ssvid). `day` is the processed day, and a run
#     deletes its date range before it writes.
#   * vessel_info: vessel_most_common GROUP BY ssvid, vessel_id; the table is
#     truncated and rebuilt.
#   * segment_vessel: seg_vessel_ssvid GROUP BY seg_id, ssvid, vessel_id; the
#     table is truncated and rebuilt.
# `vessel_id` is a hash of the ssvid and the most common identity values
# (vesselID in util.sql.j2). So when a binding changes a vessel's identity,
# vessel_info and segment_vessel show the old id as only-in-A and the new id as
# only-in-B, not as a field difference on one row. In segment_vessel_daily and
# segment_info the same change is a field difference in `vessel_id`.
COMPARE_KEYS = {
    "messages_segmented":       ["msgid"],
    "segments":                 ["seg_id", "frag_id"],   # one row per (seg, frag, day)
    "fragments":                ["frag_id"],             # frag_id is globally unique
    "segment_identity_daily":   ["seg_id", "first_timestamp"],
    "segment_vessel_daily":     ["seg_id", "ssvid", "day"],
    "segment_info":             ["seg_id"],              # clustered_by seg_id
    "vessel_info":              ["vessel_id", "ssvid"],
    "segment_vessel":           ["seg_id", "ssvid", "vessel_id"],
}


# --------------------------------------------------------------------------
# Argparse
# --------------------------------------------------------------------------

#: Flags removed with snapshot pinning (PIPELINE-4705), and what to do instead.
REMOVED_FLAGS = {
    "--pin-source-at": "the source is pinned by copying it at run start, and a copy "
                       "cannot be taken at a past time. Drop the flag; to reuse an "
                       "earlier run's copy, pass --reuse-pin <stamp>.",
    "--snapshot-dest-project": "the pinned copy always lands in "
                               f"{PROJECT}.{dit_bq.CANONICAL_DATASET}; a copy only reads "
                               "its source, so a source in another organisation needs "
                               "no flag. Drop it.",
    "--snapshot-expiration-days": "use --pin-expiration-days.",
}


def _removed_flag_dest(flag: str) -> str:
    return "removed_" + flag.lstrip("-").replace("-", "_")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Cross-version A/B for pipe-segment, varying the gpsdio-segment dep.",
    )
    # Wires --experiment-id with the standard validator (regex
    # ^[a-z0-9][a-z0-9_-]{0,31}$, BQ-table-name safe, max 32 chars) and the
    # auto-default solo_<6-hex>. Workflows must keep this consistent so the
    # output-table suffix shape is portable across them. (Copilot PR #44
    # comment.)
    add_experiment_id_arg(p)
    for removed in REMOVED_FLAGS:
        # Still parsed, hidden, so an old invocation fails with a message that
        # names the replacement. nargs="?" so that holds with or without a value.
        p.add_argument(removed, nargs="?", const="", default=None,
                       dest=_removed_flag_dest(removed), help=argparse.SUPPRESS)
    p.add_argument("--binding", action="append", required=True, dest="bindings",
                   help="`name=ref` pair, repeatable. `name` must satisfy the experiment-id "
                        "regex (lowercase / digits / _ / -, max 32 chars) since it is embedded "
                        "into BQ table names and the docker compose project name. Each ref "
                        "must resolve in --pipeline-dir.")
    p.add_argument("--date-range", required=True,
                   help="YYYY-MM-DD,YYYY-MM-DD, inclusive on both ends. Forwarded to the "
                        "steps and used to window the pinned copy of the source.")
    p.add_argument("--runner", default="dataflow", choices=("DirectRunner", "dataflow"),
                   help="DirectRunner runs in-process inside the compose container; dataflow "
                        "submits to GCP. Default: dataflow. For Dataflow, --sdk_container_image "
                        "is auto-set to the per-binding image resolved by the harness.")
    p.add_argument("--pipeline-dir", default=DEFAULT_PIPELINE_DIR,
                   help="pipe-segment checkout used for git worktrees.")
    p.add_argument("--dest-dataset", default=DEFAULT_DEST_DATASET,
                   help="BQ dataset holding the output tables produced by each binding's run.")
    p.add_argument("--reuse-pin", default=None, metavar="STAMP",
                   help="Reuse the pinned copy of an earlier run with the same --experiment-id, "
                        "--date-range and source instead of copying again. STAMP is the pin "
                        "stamp that run logged (e.g. 20261119t101500z). Fails if the copy is "
                        "missing.")
    p.add_argument("--pin-expiration-days", type=int, default=DEFAULT_PIN_EXPIRATION_DAYS,
                   help=f"Days until the pinned copy expires. Default "
                        f"{DEFAULT_PIN_EXPIRATION_DAYS}.")
    p.add_argument("--pin-max-gb", type=float, default=DEFAULT_PIN_MAX_GB,
                   help=f"Refuse to pin a source whose copy would process more than this many GB "
                        f"(10^9 bytes), checked by a dry run. Default {DEFAULT_PIN_MAX_GB:g}.")
    p.add_argument("--image-tag", default=DEFAULT_IMAGE_TAG,
                   help=f"Pipeline image to run pipe-segment in. Default: {DEFAULT_IMAGE_TAG} "
                        f"(DIT's temp-dataset overlay of pipe-segment "
                        f"{DEFAULT_PIPELINE_VERSION}, the version the AIS DAG is configured "
                        f"for; see {OVERLAY_DIR}). For reviewed code at this default, the "
                        f"docker runner pulls it directly. For unreviewed code (snapshot / "
                        f"unmerged experiment branch), resolve_run_context auto-builds a "
                        f"content-addressable gcr.io/world-fishing-827/dit/pipe-segment:dit-"
                        f"<commit> via M-pivot-4 kaniko and uses THAT instead; that build has "
                        f"no overlay, so on Dataflow as the test service account it is "
                        f"expected to fail. An explicit "
                        f"override is respected per binding (applied to every binding's "
                        f"resolve_run_context call -- use for manual prebuilt images).")
    p.add_argument("--bq-temp-dataset", default=DEFAULT_BQ_TEMP_DATASET,
                   help="Existing BigQuery dataset Beam stages query results in; env-var "
                        "fallback DIT_BQ_TEMP_DATASET (default ${PROJECT}.${DIT_DEST_DATASET}). "
                        "Passed to the container as the DIT_BQ_TEMP_DATASET variable, which "
                        "an overlay image reads; pipe-segment has no option for it, and an "
                        "image without the overlay ignores it.")
    p.add_argument("--build-from-source", action="store_true",
                   help="Build pipe-segment from each worktree via `docker compose build dev` "
                        "instead of pulling/building an image. Compose mounts the worktree's "
                        "source tree, so worker code reflects each binding's ref. Bypasses both "
                        "the canonical-image pull and the kaniko auto-build -- the natural "
                        "laptop inner-loop pattern. Note: --runner=dataflow + --build-from-source "
                        "is rarely what you want: workers still pull --sdk_container_image, the "
                        "published image. Only this combination passes --setup_file=./setup.py, "
                        "which ships the worktree's package to the workers on top of that image.")
    p.add_argument("--skip-downstream", action="store_true",
                   help="Run and diff the segment step only; skip the other five "
                        f"({', '.join(STEPS[1:])}). Useful for fast smoke tests.")
    p.add_argument("--dry-run", action="store_true",
                   help="Skip pipe-segment invocations and the diff phase; dry-run the copy only "
                        "(its bytes are checked against --pin-max-gb and nothing is created); "
                        "still sets up worktrees. Lets you validate orchestration without "
                        "burning runs.")
    p.add_argument("--ssvid-filter-query", default=None,
                   help="Optional SSVID filter to thin the source data (forwarded to "
                        "segment's --ssvid_filter_query). Highly recommended for DirectRunner "
                        "smoke tests; format: comma-separated double-quoted strings.")
    p.add_argument("--source-normalized-table",
                   default=DEFAULT_SRC_NORMALIZED_TABLE,
                   help=f"Fully-qualified FQN of the normalized AIS messages source, which is "
                        f"pinned by copy over --date-range. "
                        f"Default: {DEFAULT_SRC_NORMALIZED_TABLE} (staging cohort, "
                        f"single date-partitioned table). Must be one table (not a view, not "
                        f"date-sharded tables) partitioned on `timestamp`, e.g. the prod cohort "
                        f"or a custom-filtered subset.")
    # Removed 2026-11-20: DIT does not run satellite timing offsets. Hidden, and
    # rejected below with or without a value (nargs="?").
    p.add_argument("--include-satellite-offsets", nargs="?", const="", default=None,
                   dest=_SATELLITE_OFFSETS_DEST, help=argparse.SUPPRESS)
    p.add_argument("--service-account",
                   default="automated-testing@world-fishing-827.iam.gserviceaccount.com",
                   help="Service account for Dataflow runs.")
    step_caps = ", ".join(f"{step} {cap}" for step, cap in STEP_MAX_NUM_WORKERS.items())
    add_max_num_workers_arg(
        p, default=None,
        help_detail=f"Unset here means per Beam step: {step_caps}, each lowered by the "
                    f"ceiling env var; an explicit value replaces both. The four SQL "
                    f"steps are not Dataflow jobs and take no cap.",
    )
    add_worker_sizing_args(
        p, defaults=STEP_WORKER_SIZING,
        help_detail="An explicit value replaces that field in both Beam steps. The four "
                    "SQL steps have no workers.",
    )
    args = p.parse_args(argv)

    if getattr(args, _SATELLITE_OFFSETS_DEST) is not None:
        raise SystemExit(SATELLITE_OFFSETS_REMOVED)
    delattr(args, _SATELLITE_OFFSETS_DEST)
    for removed, instead in REMOVED_FLAGS.items():
        if getattr(args, _removed_flag_dest(removed)) is not None:
            raise SystemExit(
                f"{removed} was removed with snapshot pinning (PIPELINE-4705): {instead}"
            )
        delattr(args, _removed_flag_dest(removed))

    args.bindings = [_parse_binding(b) for b in args.bindings]
    args.date_range = _parse_daterange(args.date_range)
    if args.reuse_pin is not None:
        try:
            dit_bq.parse_pin_stamp(args.reuse_pin)
        except ValueError as exc:
            raise SystemExit(f"--reuse-pin: {exc}") from exc
    if args.pin_max_gb <= 0:
        raise SystemExit(f"--pin-max-gb must be > 0; got {args.pin_max_gb}")
    if args.pin_expiration_days < 1:
        raise SystemExit(f"--pin-expiration-days must be >= 1; got {args.pin_expiration_days}")
    if args.runner == "dataflow":
        _verify_job_names(args.experiment_id, args.bindings)
    # Per step: flag > ceiling env var > step default (resolve_max_num_workers).
    # args.max_num_workers stays the raw flag value.
    args.worker_caps = {
        step: worker_cap(args.max_num_workers, default=default)
        for step, default in STEP_MAX_NUM_WORKERS.items()
    }
    # Per step and field: flag > production's step sizing (resolve_worker_sizing).
    args.cli_worker_sizing = worker_sizing_from_args(args)
    args.worker_sizings = {
        step: resolve_worker_sizing(args.cli_worker_sizing, default)
        for step, default in STEP_WORKER_SIZING.items()
    }
    return args


def _parse_binding(spec: str) -> tuple[str, str]:
    if "=" not in spec:
        raise SystemExit(f"--binding must be name=ref; got {spec!r}")
    name, value = spec.split("=", 1)
    if not name or not value:
        raise SystemExit(f"both parts of name=ref must be non-empty; got {spec!r}")
    # `name` is embedded into BQ table names (via the per-binding suffix
    # `<experiment_id>_<name>`) and the docker compose project name; reuse
    # the experiment-id regex so an invalid name fails fast here instead of
    # producing opaque BQ / docker errors later. (Copilot PR #44 comment.)
    if not EXPERIMENT_ID_RE.match(name):
        raise SystemExit(
            f"--binding name {name!r} must match {EXPERIMENT_ID_RE.pattern} "
            f"(lowercase letters / digits / _ / -, max 32 chars). "
            f"Got: {spec!r}."
        )
    return name, value


def _job_name(step: str, *, experiment_id: str, binding_name: str) -> str:
    """Dataflow job name of one Beam step of one binding:
    ``dit-pipe-segment-<step>-<experiment>-<binding>``.

    ``dit.job_names.make_job_name`` enforces Dataflow's ``[a-z0-9-]`` alphabet
    and the 63-character cap. It shortens the experiment id when the name is
    too long and keeps step and binding whole, since those tell concurrent
    jobs apart.
    """
    return make_job_name(
        repo=PIPELINE_NAME, step=step, experiment_id=experiment_id, binding=binding_name,
    )


def _verify_job_names(experiment_id: str, bindings: list[tuple[str, str]]) -> None:
    """Fail at parse time when a binding name leaves no room for a job name.

    A binding name may be 32 characters, but the longest step name
    (``segment_identity``) leaves 27 for it. Without this check the error
    would come from the second step's argv, after the first job has run.
    """
    for name, _ in bindings:
        for step in BEAM_STEPS:
            try:
                _job_name(step, experiment_id=experiment_id, binding_name=name)
            except ValueError as exc:
                raise SystemExit(
                    f"--binding name {name!r} is too long for the Dataflow job name of "
                    f"step {step!r}: {exc}"
                ) from exc


def _parse_daterange(s: str) -> tuple[date, date]:
    parts = s.split(",")
    if len(parts) != 2:
        raise SystemExit(f"--date-range must be 'YYYY-MM-DD,YYYY-MM-DD'; got {s!r}")
    try:
        start, end = date.fromisoformat(parts[0]), date.fromisoformat(parts[1])
    except ValueError as exc:
        raise SystemExit(f"--date-range: invalid date in {s!r}: {exc}") from exc
    # Checked here, before anything is pinned: the copy's window needs it.
    if end < start:
        raise SystemExit(f"--date-range end ({end}) is before start ({start})")
    return start, end


# --------------------------------------------------------------------------
# Pre-flight: verify refs
# --------------------------------------------------------------------------

def _verify_refs(pipeline_dir: str, bindings: list[tuple[str, str]]) -> None:
    if not Path(pipeline_dir, ".git").exists():
        raise SystemExit(f"--pipeline-dir {pipeline_dir} is not a git repo.")
    for name, ref in bindings:
        result = subprocess.run(
            ["git", "-C", pipeline_dir, "rev-parse", "--verify", "--quiet", ref + "^{commit}"],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise SystemExit(
                f"binding {name!r} ref {ref!r} not found in {pipeline_dir}. "
                f"Fetch it first: `git -C {pipeline_dir} fetch origin {ref}`."
            )
        logger.info("binding %s: ref %s -> %s", name, ref, result.stdout.strip())


def _read_gpsdio_pin_at_ref(pipeline_dir: str, ref: str) -> str:
    """Return the ``gpsdio-segment`` pin line from ``ref``'s deps file.

    Uses ``git show <ref>:<path>`` so we don't need to materialise a worktree
    just to read one file. Cheap enough to call per binding at preflight time
    before any expensive copy or run begins.

    Layout shifted between pipe-segment versions:
    * v5.0.0 -- v5.0.2: deps in ``requirements/prod.in``.
    * v5.0.3+: deps in ``pyproject.toml`` (PIPELINE-3363 "improve repository
      structure" refactor folded everything into PEP 621). ``requirements.txt``
      is the pip-compile output and also contains the pin but the line may be
      a resolved SHA, not the canonical declared URL.

    Probes paths in v5.0.3-first order so the common case (the workflow's
    default image, v5.1.0) is fast.
    """
    for path in ("pyproject.toml", "requirements/prod.in", "requirements.txt"):
        result = subprocess.run(
            ["git", "-C", pipeline_dir, "show", f"{ref}:{path}"],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            continue  # path doesn't exist at this ref; try the next
        for line in result.stdout.splitlines():
            # prod.in / requirements.txt: ``gpsdio-segment @ <url>``
            # pyproject.toml: ``    "gpsdio-segment @ <url>",`` (PEP 621 deps list).
            # Strip the union of decorating chars from BOTH ends in a single
            # call -- the multi-step ``.strip('"').strip("',")`` form left a
            # trailing ``"`` on PEP 621 lines because the inner ``.strip('"')``
            # ran while the comma was still trailing. (Copilot PR #49 comment.)
            stripped = line.strip().strip('"\',')
            if stripped.startswith("gpsdio-segment"):
                return stripped
    raise SystemExit(
        f"no gpsdio-segment pin found at {ref!r} in pyproject.toml, "
        f"requirements/prod.in, or requirements.txt."
    )


def _verify_distinct_gpsdio_pins(
    pipeline_dir: str,
    bindings: list[tuple[str, str]],
) -> dict[str, str]:
    """Fail fast if every binding pins the same gpsdio-segment line.

    A two-binding A/B with identical pins is a misconfiguration that would
    otherwise silently produce an IDENTICAL diff — looking like "the change
    has no effect" when in fact the change isn't under test at all. Reads
    each ref's ``requirements/prod.in`` via ``git show`` (no worktree needed)
    and aborts before pinning if all values collapse to one. Single-
    binding runs are exempt (nothing to compare). (Copilot PR #44 round 2.)
    """
    pins = {name: _read_gpsdio_pin_at_ref(pipeline_dir, ref) for name, ref in bindings}
    for name, pin in pins.items():
        logger.info("binding %s: gpsdio-segment pin -> %s", name, pin)
    if len(bindings) > 1 and len(set(pins.values())) == 1:
        single = next(iter(set(pins.values())))
        raise SystemExit(
            f"All {len(bindings)} bindings pin the same gpsdio-segment "
            f"({single!r}). The A/B is misconfigured -- there is no actual "
            f"code difference under test. Did you forget to push the "
            f"experiment branch with the repinned requirements/prod.in?"
        )
    return pins


# --------------------------------------------------------------------------
# Pin the source (a copy in tech_great_expectations, dit.bq.pin_source_into_experiment)
# --------------------------------------------------------------------------

def _sanitize_for_dataset(s: str) -> str:
    return s.replace("-", "_")


# BQ labels are limited to ``[a-z0-9_-]{1,63}`` per the BQ docs; arbitrary
# strings (experiment_id, binding name, commit sha) need coercing before they
# can flow through ``--labels=k=v`` flags. Mirrors port_visits/ais.py's
# ``_safe_label_value`` -- if a third workflow needs it, lift into dit.bq.
_UNSAFE_LABEL_CHAR_RE = re.compile(r"[^a-z0-9_-]")


def _safe_label_value(value: str) -> str:
    """Coerce ``value`` into ``[a-z0-9_-]{1,63}`` BQ-label form."""
    return _UNSAFE_LABEL_CHAR_RE.sub("-", value.lower())[:63]


@dataclasses.dataclass(frozen=True)
class _PinnedSources:
    """What every binding reads: the pinned copy of the source.

    ``normalized_messages`` is a table in
    ``world-fishing-827.tech_great_expectations`` named
    ``dit_exp_<experiment_id>_pipe_segment_<source table>_<start>_<end>_<stamp>``
    (``dit.bq.pinned_source_fqn``). Frozen so nothing changes the pin between
    pinning and the bindings' runs.

    It is the only source: DIT runs no satellite timing offsets, so there is
    no satellite input to pin.
    """
    normalized_messages: str


def _normalized_window(date_range: tuple[date, date]) -> dit_bq.SourceWindow:
    """The normalized messages a run reads: its inclusive ``--date-range``.

    ``segment`` reads the source by ``DATE(timestamp) BETWEEN start AND end``
    (``transform/read_messages.py`` at v5.0.3), with no lookback: what it
    needs of earlier days comes from the binding's own fragments table. No
    other step reads the source.
    """
    start, end = date_range
    return dit_bq.SourceWindow(
        column=NORMALIZED_WINDOW_COLUMN, start=start, end=end + timedelta(days=1),
    )


def _pin_sources(
    args: argparse.Namespace, pinned_at: datetime, *, reuse: bool,
) -> _PinnedSources:
    """Pin the normalized messages under the stamp of ``pinned_at``, or with
    ``reuse``, check that an earlier run's copy under that stamp exists.

    A reused copy must match this run's experiment id, date range and source
    table name, since all of them are in the name. With ``args.dry_run`` the
    copy is only dry-run: its bytes are checked against the cap and nothing
    is created.
    """
    window = _normalized_window(args.date_range)
    if reuse:
        normalized = dit_bq.require_pinned_source(
            dit_bq.pinned_source_fqn(
                args.source_normalized_table,
                experiment_id=args.experiment_id,
                role=PIN_ROLE,
                pinned_at=pinned_at,
                window=window,
                project=PROJECT,
            ),
            project=PROJECT,
        )
    else:
        normalized = dit_bq.pin_source_into_experiment(
            args.source_normalized_table,
            experiment_id=args.experiment_id,
            role=PIN_ROLE,
            pinned_at=pinned_at,
            window=window,
            expiration_days=args.pin_expiration_days,
            max_bytes=int(args.pin_max_gb * 1e9),
            dry_run=args.dry_run,
            project=PROJECT,
        )
    logger.info("pinned normalized messages: %s", normalized)
    return _PinnedSources(normalized_messages=normalized)


def _pin(args: argparse.Namespace) -> _PinnedSources:
    """Copy the source at run start, or with ``--reuse-pin`` reuse an earlier
    run's copy."""
    reuse = args.reuse_pin is not None
    pinned_at = (
        dit_bq.parse_pin_stamp(args.reuse_pin) if reuse
        else datetime.now(timezone.utc).replace(microsecond=0)
    )
    logger.info("pin stamp: %s (%s)", dit_bq.format_pin_stamp(pinned_at),
                "reusing an earlier run's copy" if reuse else "a new copy")
    logger.info("pinning: %s (copy in %s.%s)",
                args.source_normalized_table, PROJECT, dit_bq.CANONICAL_DATASET)
    pinned = _pin_sources(args, pinned_at, reuse=reuse)
    if not reuse and not args.dry_run:
        logger.info("to rerun on this copy, pass --reuse-pin %s",
                    dit_bq.format_pin_stamp(pinned_at))
    return pinned


# --------------------------------------------------------------------------
# Per-binding pipeline runs
# --------------------------------------------------------------------------

def _output_prefix(table: str, *, dest_dataset: str, suffix: str) -> str:
    """Per-binding output-table prefix (without the date-shard suffix)."""
    return f"{PROJECT}.{dest_dataset}.{table}_{_sanitize_for_dataset(suffix)}"


def _step_labels(step: str, *, experiment_id: str, binding_name: str) -> list[str]:
    """``--labels`` of one step. Every step needs them, on every runner.

    Each pipe-segment command builds ``BigQueryHelper(labels=...)``, which
    splits every entry on ``=`` with no ``None`` guard
    (``utils/bq_tools.py`` at v5.0.3). Without ``--labels`` the command raises
    ``TypeError`` before it does any work. The labels land on the step's
    tables and query jobs, and for the Beam steps on the Dataflow job.

    ``experiment_id`` and ``binding`` are separate labels, each sanitised by
    ``_safe_label_value``: BigQuery caps label values at 63 characters and the
    raw suffix can be 65 (``<exp32>_<binding32>``). (Copilot PR #44 round 2.)
    """
    return [
        "--labels=environment=develop",
        "--labels=resource_creator=dit",
        f"--labels=step={step}",
        f"--labels=experiment_id={_safe_label_value(experiment_id)}",
        f"--labels=binding={_safe_label_value(binding_name)}",
    ]


def _dataflow_options(
    step: str,
    *,
    experiment_id: str,
    binding_name: str,
    worker_image: str | None,
    service_account: str,
    max_num_workers: int | None,
    worker_sizing: WorkerSizing | None,
    build_from_source: bool,
    job_name: str | None = None,
) -> list[str]:
    """The Dataflow options of one Beam step, as production's task passes them.

    ``job_name`` replaces the per-binding name (:func:`_job_name`); the
    mode-equivalence workflow (``ais.py``) names its jobs by mode and
    iteration.

    ``--setup_file=./setup.py`` is passed only with ``build_from_source``.
    Production passes none: the published image has the package installed and
    no source tree (the Dockerfile's ``prod`` stage at v5.0.3), so
    ``./setup.py`` does not exist in it. With ``--build-from-source`` the
    compose ``dev`` service mounts the worktree at the working directory, so
    the file exists. Beam then stages the worktree's package for the workers,
    which otherwise run only what ``--sdk_container_image`` holds. Read from
    the Dockerfile and compose file; not confirmed by a run.

    No ``--experiments``: production passes none.
    """
    return [
        *(["--setup_file=./setup.py"] if build_from_source else []),
        "--wait_for_job",
        "--region=us-central1",
        *max_num_workers_options(_step_max_num_workers(step, max_num_workers)),
        *worker_sizing_options(_step_worker_sizing(step, worker_sizing)),
        NO_PUBLIC_IPS_OPTION,
        "--network=gfw-internal-network",
        "--subnetwork=regions/us-central1/subnetworks/gfw-internal-us-central1",
        f"--service_account_email={service_account}",
        "--job_name="
        + (job_name
           or _job_name(step, experiment_id=experiment_id, binding_name=binding_name)),
        *([f"--sdk_container_image={worker_image}"] if worker_image is not None else []),
    ]


def _segment_args(
    *,
    pinned: _PinnedSources,
    dest_dataset: str,
    suffix: str,
    experiment_id: str,
    binding_name: str,
    date_range: tuple[date, date],
    runner: str,
    worker_image: Optional[str],
    ssvid_filter_query: Optional[str],
    service_account: str,
    max_num_workers: Optional[int] = None,
    worker_sizing: WorkerSizing | None = None,
    build_from_source: bool = False,
    labels: Sequence[str] | None = None,
    job_name: str | None = None,
    extra_flags: Sequence[str] = (),
) -> list[str]:
    """CLI args for `pipe-segment segment`.

    The source table comes from ``pinned`` (produced by :func:`_pin`):
    ``pinned.normalized_messages`` is the pinned copy. pipe-segment's
    BigQuerySource finds it partitioned, as the source is, and filters by
    DATE(timestamp).

    The last three parameters are for the mode-equivalence workflow
    (``ais.py``), which shares this builder. ``labels`` replaces the
    per-binding ``--labels`` (:func:`_step_labels`), ``job_name`` the
    per-binding Dataflow job name, and ``extra_flags`` are appended after the
    table flags (``--incremental`` and its tables). Left at their defaults,
    the argv is what it was before they existed.

    No satellite flag is ever passed (``--in_normalized_sat_offset_messages_table``,
    ``--in_norad_to_receiver_table``, ``--in_sat_positions_table``,
    ``--out_sat_offsets_table``). ``segment`` runs its offsets branch only
    when it gets all three inputs, so a DIT run computes no satellite timing
    offsets and drops no message for them. See the module docstring.

    The Dataflow options, the job name among them, come from
    :func:`_dataflow_options`.
    """
    start, end = date_range
    args = [
        "segment",
        f"--date_range={start.isoformat()},{end.isoformat()}",
        f"--in_normalized_messages_table={pinned.normalized_messages}",
        f"--out_segmented_messages_table={_output_prefix('messages_segmented', dest_dataset=dest_dataset, suffix=suffix)}",
        f"--out_segments_table={_output_prefix('segments', dest_dataset=dest_dataset, suffix=suffix)}",
        f"--fragments_table={_output_prefix('fragments', dest_dataset=dest_dataset, suffix=suffix)}",
        *extra_flags,
        *(_step_labels("segment", experiment_id=experiment_id, binding_name=binding_name)
          if labels is None else labels),
        # Project + runner.
        f"--project={PROJECT}",
        f"--runner={runner}",
        # temp_location + staging_location are required even for DirectRunner
        # because Beam's ``ReadFromBigQuery`` path goes through a GCS export
        # (``MapFilesToRemove`` raises ``ValueError: ReadFromBigQuery requires
        # a GCS location to be provided`` if it can't find a temp_location to
        # stage to). example_segment.sh sets the same gs://pipe-temp-us-
        # central-ttl7 bucket for both runners. Moving this out of the
        # ``runner == "dataflow"`` branch matches that.
        f"--temp_location={dataflow_temp_location(DEFAULT_DATAFLOW_TEMP_BUCKET)}",
        f"--staging_location={dataflow_staging_location(DEFAULT_DATAFLOW_TEMP_BUCKET)}",
    ]
    if ssvid_filter_query is not None:
        args.append(f"--ssvid_filter_query={ssvid_filter_query}")
    if runner == "dataflow":
        args.extend(_dataflow_options(
            "segment",
            experiment_id=experiment_id,
            binding_name=binding_name,
            worker_image=worker_image,
            service_account=service_account,
            max_num_workers=max_num_workers,
            worker_sizing=worker_sizing,
            build_from_source=build_from_source,
            job_name=job_name,
        ))
    return args


def _segment_identity_args(
    *,
    dest_dataset: str,
    suffix: str,
    experiment_id: str,
    binding_name: str,
    date_range: tuple[date, date],
    runner: str,
    worker_image: Optional[str],
    service_account: str,
    max_num_workers: Optional[int] = None,
    worker_sizing: WorkerSizing | None = None,
    build_from_source: bool = False,
) -> list[str]:
    """CLI args for `pipe-segment segment_identity` (a Beam job, like `segment`)."""
    start, end = date_range
    args = [
        "segment_identity",
        f"--date_range={start.isoformat()},{end.isoformat()}",
        f"--source_segments={_output_prefix('segments', dest_dataset=dest_dataset, suffix=suffix)}",
        f"--source_fragments={_output_prefix('fragments', dest_dataset=dest_dataset, suffix=suffix)}",
        f"--dest_segment_identity={_output_prefix('segment_identity_daily', dest_dataset=dest_dataset, suffix=suffix)}",
        *_step_labels("segment_identity", experiment_id=experiment_id, binding_name=binding_name),
        f"--project={PROJECT}",
        f"--runner={runner}",
        # See _segment_args for the temp_location/staging_location rationale.
        f"--temp_location={dataflow_temp_location(DEFAULT_DATAFLOW_TEMP_BUCKET)}",
        f"--staging_location={dataflow_staging_location(DEFAULT_DATAFLOW_TEMP_BUCKET)}",
    ]
    if runner == "dataflow":
        args.extend(_dataflow_options(
            "segment_identity",
            experiment_id=experiment_id,
            binding_name=binding_name,
            worker_image=worker_image,
            service_account=service_account,
            max_num_workers=max_num_workers,
            worker_sizing=worker_sizing,
            build_from_source=build_from_source,
        ))
    return args


def _sql_step_args(
    step: str,
    table_flags: Sequence[str],
    *,
    experiment_id: str,
    binding_name: str,
) -> list[str]:
    """CLI args for one of pipe-segment's four BigQuery SQL commands.

    The command's own table (and date) flags, the labels, and the project the
    queries are billed to. Nothing else: these commands are not Beam jobs, so
    they get no runner and no Dataflow option, as in production. Production
    leaves ``--project`` to the CLI's default; dit names its project on every
    step so the billing project never depends on a pipeline's default.
    """
    return [
        step,
        *table_flags,
        *_step_labels(step, experiment_id=experiment_id, binding_name=binding_name),
        f"--project={PROJECT}",
    ]


def _segment_vessel_daily_args(
    *,
    dest_dataset: str,
    suffix: str,
    experiment_id: str,
    binding_name: str,
    date_range: tuple[date, date],
) -> list[str]:
    """CLI args for `pipe-segment segment_vessel_daily`.

    Assigns a ``vessel_id`` to each segment of each day in ``date_range``
    (inclusive on both ends, like the Beam steps), from the identity counts of
    the 30 days up to that day. A binding's identity table holds only the
    run's own days, so the window is shorter here than in production.
    """
    start, end = date_range
    table = functools.partial(_output_prefix, dest_dataset=dest_dataset, suffix=suffix)
    return _sql_step_args(
        "segment_vessel_daily",
        [
            f"--date_range={start.isoformat()},{end.isoformat()}",
            f"--source_segment_identity={table('segment_identity_daily')}",
            f"--destination={table('segment_vessel_daily')}",
        ],
        experiment_id=experiment_id, binding_name=binding_name,
    )


def _segment_info_args(
    *,
    dest_dataset: str,
    suffix: str,
    experiment_id: str,
    binding_name: str,
) -> list[str]:
    """CLI args for `pipe-segment segment_info`: one row per segment.

    ``--source_segment_vessel`` is the ``segment_vessel_daily`` table, as in
    production. The query selects ``vessel_id``, ``seg_id`` and ``day`` from
    it; the identity table has no ``vessel_id`` or ``day`` and cannot stand in.
    """
    table = functools.partial(_output_prefix, dest_dataset=dest_dataset, suffix=suffix)
    return _sql_step_args(
        "segment_info",
        [
            f"--source_segment_identity={table('segment_identity_daily')}",
            f"--source_segment_vessel={table('segment_vessel_daily')}",
            f"--destination={table('segment_info')}",
        ],
        experiment_id=experiment_id, binding_name=binding_name,
    )


def _vessel_info_args(
    *,
    dest_dataset: str,
    suffix: str,
    experiment_id: str,
    binding_name: str,
) -> list[str]:
    """CLI args for `pipe-segment vessel_info`: one row per vessel id and
    ssvid, from the same two inputs as `segment_info`."""
    table = functools.partial(_output_prefix, dest_dataset=dest_dataset, suffix=suffix)
    return _sql_step_args(
        "vessel_info",
        [
            f"--source_segment_identity={table('segment_identity_daily')}",
            f"--source_segment_vessel={table('segment_vessel_daily')}",
            f"--destination={table('vessel_info')}",
        ],
        experiment_id=experiment_id, binding_name=binding_name,
    )


def _segment_vessel_args(
    *,
    dest_dataset: str,
    suffix: str,
    experiment_id: str,
    binding_name: str,
) -> list[str]:
    """CLI args for `pipe-segment segment_vessel`: the mapping between
    segments, ssvids and vessel ids over the whole `segment_vessel_daily`
    table. Production's later stages read this table."""
    table = functools.partial(_output_prefix, dest_dataset=dest_dataset, suffix=suffix)
    return _sql_step_args(
        "segment_vessel",
        [
            f"--source_segment_vessel_daily={table('segment_vessel_daily')}",
            f"--destination={table('segment_vessel')}",
        ],
        experiment_id=experiment_id, binding_name=binding_name,
    )


def _step_argvs(
    args: argparse.Namespace,
    *,
    binding_name: str,
    suffix: str,
    pinned: _PinnedSources,
    worker_image: str | None,
) -> dict[str, list[str]]:
    """The argv of every step one binding runs, by step, in run order.

    Pure: no docker and no cloud call, so the drift spec builds the same
    tokens offline. ``--skip-downstream`` keeps ``segment`` only.
    """
    tables = dict(dest_dataset=args.dest_dataset, suffix=suffix)
    identity = dict(experiment_id=args.experiment_id, binding_name=binding_name)

    def beam(step: str) -> dict:
        return dict(
            runner=args.runner,
            worker_image=worker_image,
            service_account=args.service_account,
            max_num_workers=args.worker_caps[step].workers,
            worker_sizing=args.worker_sizings[step],
            build_from_source=args.build_from_source,
        )

    builders = {
        "segment": lambda: _segment_args(
            pinned=pinned,
            date_range=args.date_range,
            ssvid_filter_query=args.ssvid_filter_query,
            **tables, **identity, **beam("segment"),
        ),
        "segment_identity": lambda: _segment_identity_args(
            date_range=args.date_range, **tables, **identity, **beam("segment_identity"),
        ),
        "segment_vessel_daily": lambda: _segment_vessel_daily_args(
            date_range=args.date_range, **tables, **identity,
        ),
        "segment_info": lambda: _segment_info_args(**tables, **identity),
        "vessel_info": lambda: _vessel_info_args(**tables, **identity),
        "segment_vessel": lambda: _segment_vessel_args(**tables, **identity),
    }
    steps = STEPS[:1] if args.skip_downstream else STEPS
    return {step: builders[step]() for step in steps}


@contextlib.contextmanager
def _chdir(target: str) -> Iterator[None]:
    """Process-wide chdir, restored on exit. NOT thread-safe -- bindings run sequentially.

    ``dit.runners.docker.run`` invokes ``docker compose`` from the process CWD,
    so to run a binding's docker compose against its worktree we briefly chdir
    into the worktree, invoke, then restore. Two bindings sequential -> two
    chdirs sequential, no race.
    """
    prev = os.getcwd()
    os.chdir(target)
    try:
        yield
    finally:
        os.chdir(prev)


def _run_pipe_subcommand(
    *,
    worktree_dir: str,
    binding_name: str,
    image_tag: str,
    cli_args: list[str],
    build_from_source: bool,
    bq_temp_dataset: str = DEFAULT_BQ_TEMP_DATASET,
) -> int:
    """One ``pipe-segment`` subcommand (any of ``STEPS``) via dit_docker.run.

    Runs from ``worktree_dir`` (chdir context) so ``docker compose`` finds the
    worktree's compose file. ``dit_docker.run`` handles cloud-mode
    (``--network=cloudbuild``), per-call compose project name
    uniquification, and ``--entrypoint`` injection.

    ``container_env={"GOOGLE_CLOUD_PROJECT": PROJECT}`` is set because
    Beam's ``WriteToBigQuery`` (inside pipe-segment v5.0.x) constructs its
    own ``google-cloud-bigquery`` client whose default-project resolution
    walks ``GOOGLE_CLOUD_PROJECT`` env -> ADC metadata; the pipeline option
    ``--project=...`` is read by Beam earlier in pipeline construction and
    isn't forwarded to that internal client. Without the env, writes fail
    with ``OSError: Project was not passed and could not be determined from
    the environment``. ``examples/example_segment.sh`` already documents
    this escape hatch (inline ``-e GOOGLE_CLOUD_PROJECT=...`` on docker
    compose); see ``dit.runners.docker.run.__doc__`` for the env vs
    container_env distinction.

    ``DIT_BQ_TEMP_DATASET`` (``bq_temp_dataset``) is for DIT's overlay images:
    their patched BigQuery reads stage in that existing dataset, where Beam
    would otherwise create one, which DIT's service account may not. It is an
    environment variable, not an argument, so no step's argv changes; an image
    without the overlay ignores it.
    """
    project_name = f"dit-pipe-segment-{binding_name}"
    logger.info("[%s] invoking pipe-segment %s (image=%s)",
                binding_name, cli_args[0] if cli_args else "<no-subcmd>", image_tag)
    with _chdir(worktree_dir):
        return dit_docker.run(
            image_tag,
            cli_args,
            container_env={
                "GOOGLE_CLOUD_PROJECT": PROJECT,
                TEMP_DATASET_ENV: bq_temp_dataset,
            },
            entrypoint=CLI_ENTRYPOINT,
            volumes=[GCP_VOLUME],
            service=COMPOSE_SERVICE,
            build_from_source=build_from_source,
            project_name=project_name,
        )


def _verify_gpsdio_segment(worktree_dir: str, *, binding_name: str) -> str:
    """Sanity check: confirm the worktree's pinned gpsdio-segment URL.

    Layout-aware (see _read_gpsdio_pin_at_ref): probes pyproject.toml
    (v5.0.3+), then requirements/prod.in (v5.0.0--v5.0.2), then
    requirements.txt as a last resort.
    """
    for rel in ("pyproject.toml", "requirements/prod.in", "requirements.txt"):
        path = Path(worktree_dir, rel)
        if not path.exists():
            continue
        for line in path.read_text().splitlines():
            # Combined char-class strip (see _read_gpsdio_pin_at_ref for why
            # multi-step strip left a trailing ``"`` on PEP 621 lines).
            stripped = line.strip().strip('"\',')
            if stripped.startswith("gpsdio-segment"):
                return stripped
    raise SystemExit(
        f"binding {binding_name!r}: no gpsdio-segment pin found in {worktree_dir} "
        f"(checked pyproject.toml, requirements/prod.in, requirements.txt)."
    )


def _run_binding(
    *,
    name: str,
    ref: str,
    args: argparse.Namespace,
    pinned: _PinnedSources,
    suffix: str,
) -> int:
    worktree_dir = tempfile.mkdtemp(prefix=f"dit-pipe-segment-{name}-")
    try:
        subprocess.run(
            ["git", "-C", args.pipeline_dir, "worktree", "add", "--force", worktree_dir, ref],
            check=True, capture_output=True, text=True,
        )
        logger.info("[%s] worktree %s @ %s", name, worktree_dir, ref)

        gpsdio_pin = _verify_gpsdio_segment(worktree_dir, binding_name=name)
        logger.info("[%s] gpsdio-segment pin -> %s", name, gpsdio_pin)

        # Resolve the per-binding image: canonical for reviewed code at the
        # default tag, kaniko auto-build for unreviewed code, or the explicit
        # --image-tag passthrough. Passing suffix=<per-binding> takes the
        # "manual / cross-version escape hatch" path in resolve_run_context
        # (record git state as-is, no auto-snapshot -- our worktrees ARE
        # committed refs).
        ctx = resolve_run_context(
            repo_dir=worktree_dir,
            pipeline_name=PIPELINE_NAME,
            runner="docker",
            require_clean=False,
            suffix=suffix,
            worker_image=args.image_tag,
            default_worker_image=DEFAULT_IMAGE_TAG,
            resolve_digest=False,  # no run cache for pipe-segment yet
            build_from_source=args.build_from_source,
        )
        image_tag = ctx.worker_image
        logger.info("[%s] commit=%s%s image=%s",
                    name, ctx.pipeline_commit,
                    " (UNREVIEWED)" if ctx.unreviewed else "",
                    image_tag)

        if image_tag != args.image_tag and args.runner == "dataflow":
            # The harness replaced the image with a build of the binding's
            # commit. That build is plain source: no temp-dataset overlay.
            logger.warning(
                "[%s] %s was auto-built from plain source, without DIT's temp-dataset "
                "overlay. On Dataflow, Beam will try to create its own BigQuery staging "
                "dataset as %s, which DIT's default service account may not. Build an "
                "overlay image of the commit (%s/build.sh) and pass it as --image-tag.",
                name, image_tag, args.service_account, OVERLAY_DIR,
            )

        if args.dry_run:
            logger.info("[%s] --dry-run set; skipping pipe-segment invocations.", name)
            return 0

        # Each step reads what the one before wrote, so the first failure
        # ends the binding.
        argvs = _step_argvs(
            args, binding_name=name, suffix=suffix,
            pinned=pinned, worker_image=image_tag,
        )
        for step, cli_args in argvs.items():
            rc = _run_pipe_subcommand(
                worktree_dir=worktree_dir, binding_name=name,
                image_tag=image_tag, cli_args=cli_args,
                build_from_source=args.build_from_source,
                bq_temp_dataset=args.bq_temp_dataset,
            )
            if rc != 0:
                logger.error("[%s] %s step failed with rc=%d", name, step, rc)
                return rc

        return 0
    finally:
        subprocess.run(
            ["git", "-C", args.pipeline_dir, "worktree", "remove", "--force", worktree_dir],
            check=False, capture_output=True,
        )
        shutil.rmtree(worktree_dir, ignore_errors=True)
        logger.info("[%s] worktree torn down", name)


# --------------------------------------------------------------------------
# Pairwise diffs
# --------------------------------------------------------------------------

_SKIPPED = -1  # sentinel for diff pairs we couldn't run

# Tuple key: (table, binding_a, binding_b). Output tables are single
# date-partitioned tables since pipe-segment v5.0.0, so no shard axis.
DiffKey = tuple[str, str, str]


def _diff_table_pair(
    *,
    table: str,
    dest_dataset: str,
    suffix_a: str,
    suffix_b: str,
) -> int:
    """Diff a single table between two bindings. Returns 0/1."""
    a = _output_prefix(table, dest_dataset=dest_dataset, suffix=suffix_a)
    b = _output_prefix(table, dest_dataset=dest_dataset, suffix=suffix_b)
    return dit_compare.compare_tables(
        a, b,
        keys=COMPARE_KEYS[table],
        view_suffix="",
    )


def _run_diffs(
    *,
    args: argparse.Namespace,
    suffix_by_binding: dict[str, str],
    failed_bindings: set[str],
) -> dict[DiffKey, int]:
    """Run diffs over all (table, binding-pair) combinations.

    Output tables are single date-partitioned tables; pipe-segment's
    ``prepare_output_tables`` (pipeline.py:158) clears + writes the
    --date-range slice of each table per run. Because the per-binding
    suffix differs, each binding writes to its own table FQN, so the
    diff sees only the run's date range (no stale data to filter out).
    """
    results: dict[DiffKey, int] = {}
    tables: list[str] = list(SEGMENT_OUTPUT_TABLES)
    if not args.skip_downstream:
        tables.extend(DOWNSTREAM_OUTPUT_TABLES)

    for table in tables:
        for a, b in itertools.combinations(suffix_by_binding.keys(), 2):
            if a in failed_bindings or b in failed_bindings:
                results[(table, a, b)] = _SKIPPED
                bad = a if a in failed_bindings else b
                logger.info("diff %s  %s vs %s -> SKIPPED (binding %s failed)",
                            table, a, b, bad)
                continue
            rc = _diff_table_pair(
                table=table,
                dest_dataset=args.dest_dataset,
                suffix_a=suffix_by_binding[a],
                suffix_b=suffix_by_binding[b],
            )
            results[(table, a, b)] = rc
            verdict = "IDENTICAL" if rc == 0 else f"DIFFERENT (table-check rc={rc})"
            logger.info("diff %s  %s vs %s -> %s", table, a, b, verdict)
    return results


def _summarize(results: dict[DiffKey, int]) -> str:
    lines = ["", "pipe-segment cross-version diff summary:"]
    for (table, a, b), rc in results.items():
        if rc == _SKIPPED:
            verdict = "SKIPPED"
        elif rc == 0:
            verdict = "IDENTICAL"
        else:
            verdict = "DIFFERENT"
        lines.append(f"  {table}  {a} vs {b}  -> {verdict} (rc={rc})")
    return "\n".join(lines)


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)

    logger.info("experiment_id: %s", args.experiment_id)
    logger.info("bindings:      %s", args.bindings)
    logger.info("date_range:    %s..%s", args.date_range[0], args.date_range[1])
    logger.info("runner:        %s", args.runner)
    logger.info("pipeline_dir:  %s", args.pipeline_dir)
    logger.info("dest_dataset:  %s", args.dest_dataset)
    log_worker_caps(args.worker_caps)
    log_worker_sizings(args.worker_sizings)
    if args.runner == "dataflow":
        log_production_sizing_hint(
            args.cli_worker_sizing, args.worker_sizings, [args.source_normalized_table],
        )
    if args.skip_downstream:
        logger.info("downstream:    SKIPPED (%s)", ", ".join(STEPS[1:]))

    _verify_refs(args.pipeline_dir, args.bindings)
    # Cheap preflight: fail before pinning if the A/B is misconfigured
    # with identical gpsdio-segment pins (an IDENTICAL diff would otherwise
    # look like "the change has no effect" when in fact nothing was tested).
    _verify_distinct_gpsdio_pins(args.pipeline_dir, args.bindings)
    pinned = _pin(args)

    suffix_by_binding = {name: f"{args.experiment_id}_{name}" for name, _ in args.bindings}

    # Bindings run sequentially. dit_docker.run uses os.getcwd() to locate the
    # worktree's compose.yaml; process-wide chdir isn't thread-safe. With
    # typically 2 bindings (before / after) the wall-clock cost is negligible.
    rc_by_binding: dict[str, int] = {}
    logger.info("running %d binding(s) sequentially", len(args.bindings))
    for name, ref in args.bindings:
        rc = _run_binding(
            name=name, ref=ref, args=args,
            pinned=pinned,
            suffix=suffix_by_binding[name],
        )
        rc_by_binding[name] = rc

    failed_bindings = {n for n, rc in rc_by_binding.items() if rc != 0}
    for name, rc in rc_by_binding.items():
        if rc != 0:
            logger.error("binding %s failed with rc=%d", name, rc)

    if args.dry_run:
        logger.info("--dry-run set; skipping pairwise diffs.")
        return 1 if failed_bindings else 0

    results = _run_diffs(
        args=args,
        suffix_by_binding=suffix_by_binding,
        failed_bindings=failed_bindings,
    )
    print(_summarize(results), file=sys.stderr)
    return 1 if failed_bindings else 0


if __name__ == "__main__":
    sys.exit(main())
