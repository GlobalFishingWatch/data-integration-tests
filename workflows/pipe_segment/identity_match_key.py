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

1. Verify every binding's git ref exists in ``--pipeline-dir``. Then work out
   each binding's interface (its command lines, inputs and outputs after its
   adjustments), log it, and check it against the ref's own CLI source; see
   "Bindings that differ in their interface" below. Nothing is pinned or run
   before this passes.
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
      runner="docker", suffix=<per-binding>, worker_image=<the binding's
      --binding-image, else --image-tag>,
      default_worker_image=DEFAULT_IMAGE_TAG, check_images=...)``. The
      harness classifies the ref as reviewed/unreviewed; ``check_images``
      replaces the auto-build and refuses unreviewed code on the default
      image (already checked for every binding before pinning, by
      ``_preflight_images``). An explicit ``--image-tag`` or
      ``--binding-image`` always wins.
   c. Sanity check: log the worktree's ``gpsdio-segment`` pin (catches a
      cached image silently reusing the wrong code -- a false
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
   Two bindings are compared by output name, each on its own table.
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

Bindings that differ in their interface
---------------------------------------
By default every binding gets the same command line, reads the same pinned
source and writes the same tables. That is the *base* interface
(:func:`_base_interface`), and it mirrors what production's segment group runs
today. Where a binding's pipe-segment differs from that, the binding gets
adjustments (``dit.adjust``), in layers, a later one winning:

1. the base;
2. the adjustments declared in ``workflows/pipe_segment/adjustments.py`` that
   apply to the binding, chosen by what the binding's own ref declares;
3. the run's ``--binding-adjust NAME=<step>:<op>:<flag>[=<value>]`` edits, for
   a ref whose change is not released yet. ``<op>`` is ``remove``, ``add``,
   ``rename`` or ``set``.

**``--incremental`` follows the binding's pipe-segment.** Since 2026-11-20 the
AIS v3 DAG passes ``--incremental`` to ``segment`` (pipe-segment v5.1.0;
composer-dags-production PR #3145), so the base passes it too, with
``--in_segments_table`` and ``--in_fragments_table`` naming the binding's own
tables. A binding whose ``segment`` command does not declare the flag (below
v5.1.0) has the three removed by the one declared adjustment, and runs the
full-history segmenter, the only one it has. What this means for an A/B:

* Two bindings at v5.1.0 or later both run incremental. A binding runs one
  ``segment`` job from empty tables, so the state of the day before its range
  is empty (read from ``--incremental``'s help text at v5.1.0; not run).
* A binding below v5.1.0 against one at or above it compares full history
  with incremental, as well as two refs. The run log says so: it prints the
  flag difference between the two. To hold the segmenter fixed, take the
  three flags from the newer binding with ``--binding-adjust``.
* Whether the two segmenters agree is what ``ais.py``, the mode-equivalence
  workflow, tests. No run has confirmed it yet.

**Checked before anything is pinned** (:func:`_preflight`), with git reads of
``--pipeline-dir`` only:

* Every flag of every step must be declared by that step's command module at
  the binding's ref. pipe-segment hands a flag it does not know to Beam, which
  ignores it (``cli/main.py``, ``parse_known_args``), so a wrong flag would
  not fail: the job would run without it. Beam's own options on the two Beam
  steps are exempt (``adjustments.BEAM_OPTIONS`` lists them and says why).
* An adjustment that finds nothing to change is an error, not a no-op.
* Two bindings must be compared on the same keys.

The run log prints, per binding, the edits applied with their reasons, then
per pair of bindings the flag differences of each step.

**Images.** ``--binding-image NAME=IMAGE`` gives one binding its own image,
as submitter and as Dataflow worker. Two bindings of different pipe-segment
versions need it: a ref merged into ``origin/main`` otherwise runs
``--image-tag``, whatever version the ref is. The flag check reads the ref,
and the image is what runs; a warning says when the image's tag names another
version than the ref states.

**Tables.** An adjustment may also change a binding's inputs and outputs
(``dit.adjust``: two inputs become one, an output is renamed). The union of
the bindings' inputs is pinned once; outputs are compared by name, and an
output only one binding writes is listed as not compared. No pipe-segment
version changes its tables today, so no such adjustment is declared and this
path has run on synthetic interfaces only.

``--dry-run`` skips the pipe-segment invocations and the diff phase, and only
dry-runs the copy (reporting its bytes against the ``--pin-max-gb`` cap). It
still runs the checks above, logs each binding's interface, and sets up and
tears down the worktrees.

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

Example (two pipe-segment versions, each on its own overlay image; the older
binding loses ``--incremental`` by the declared adjustment)::

    dit run workflows/pipe_segment/identity_match_key.py \\
        --experiment-id segment-503-510 \\
        --binding before=v5.0.3 --binding-image before=<v5.0.3 overlay> \\
        --binding after=v5.1.0 \\
        --allow-same-gpsdio-pin \\
        --date-range 2020-01-01,2020-01-01 \\
        --runner dataflow

Example (the same, with both bindings on the full-history segmenter)::

        ... \\
        --binding-adjust after=segment:remove:--incremental \\
        --binding-adjust after=segment:remove:--in_segments_table \\
        --binding-adjust after=segment:remove:--in_fragments_table
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
from collections import Counter
from collections.abc import Mapping
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Optional, Sequence

from dit import adjust
from dit import bq as dit_bq
from dit import compare as dit_compare
from dit.dataflow_paths import dataflow_staging_location, dataflow_temp_location
from dit.job_names import make_job_name
from dit.runners import docker as dit_docker
from dit.snapshot import is_unreviewed
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
from workflows.pipe_segment import adjustments

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
#: Built and pushed on 2026-11-24 (digest sha256:f785658b...; see the table in
#: OVERLAY_DIR's README). When the DAG's version moves: build the overlay image of the new tag, then
#: bump both constants.
DEFAULT_OVERLAY_IMAGE = (
    f"gcr.io/world-fishing-827/dit/pipe-segment:"
    f"{DEFAULT_PIPELINE_VERSION}-4ddde03-temp-dataset-04b40b54"
)

# This workflow's default image, for reviewed code. An unreviewed binding
# (a commit not merged into ``origin/main``) that would run on it is refused
# before anything is pinned (``check_images``): nothing is auto-built, because
# the shared auto-build is plain source without the overlay. The way out is
# an overlay image of the commit as ``--image-tag``, or ``--runner
# DirectRunner --build-from-source`` (compose builds the ``dev`` service from
# the worktree).
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

#: The role of the one input of the base interface (``{in.normalized_messages}``
#: in a step's tokens). Its pinned copy keeps PIN_ROLE in its name; an input a
#: declared adjustment adds is pinned as ``<PIN_ROLE>_<role>``.
INPUT_ROLE = "normalized_messages"

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

#: What a row of each output table is, as its keys above say it.
OUTPUT_MEANINGS = {
    "messages_segmented":       "one row per message",
    "segments":                 "one row per segment, fragment and day",
    "fragments":                "one row per fragment",
    "segment_identity_daily":   "one row per segment and day",
    "segment_vessel_daily":     "one row per segment, ssvid and processed day",
    "segment_info":             "one row per segment",
    "vessel_info":              "one row per vessel id and ssvid",
    "segment_vessel":           "one row per segment, ssvid and vessel id",
}

#: Flags whose values are a binding's own by construction: the tables it
#: writes and hands from step to step, its labels, its job name and its image.
#: The flag diff between two bindings compares these by presence only;
#: otherwise every one of them would show as a difference on every run.
BINDING_OWN_FLAGS = frozenset({
    "--out_segmented_messages_table",
    "--out_segments_table",
    "--fragments_table",
    "--in_segments_table",
    "--in_fragments_table",
    "--source_segments",
    "--source_fragments",
    "--dest_segment_identity",
    "--source_segment_identity",
    "--source_segment_vessel",
    "--source_segment_vessel_daily",
    "--destination",
    "--labels",
    "--job_name",
    "--sdk_container_image",
})


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
    p.add_argument("--binding-image", action="append", default=[], dest="binding_images",
                   metavar="NAME=IMAGE",
                   help="Image for one binding, repeatable: that binding's submitter image "
                        "and, on Dataflow, its worker image, in place of --image-tag. Needed "
                        "when the bindings are different pipe-segment versions: without it a "
                        "binding whose ref is merged into origin/main runs --image-tag, "
                        f"whatever version the ref is. Pass a DIT overlay image of the "
                        f"binding's commit ({OVERLAY_DIR}/build.sh).")
    p.add_argument("--binding-adjust", action="append", default=[], dest="binding_adjustments",
                   metavar="NAME=EDIT",
                   help="Change one binding's command line for this run, as "
                        f"{adjust.AD_HOC_FORM}. Repeatable and "
                        "applied in the order given, after the adjustments declared in "
                        "workflows/pipe_segment/adjustments.py. <op> is one of "
                        f"{', '.join(adjust.AD_HOC_OPS)}; <step> is one of {', '.join(STEPS)}. "
                        "Examples: after=segment:remove:--incremental, "
                        "after=segment:add:--bins_per_day=8, "
                        "after=segment:set:--bins_per_day=8, "
                        "after=segment:rename:--fragments_table=--out_fragments_table. A "
                        "value may name the binding's own tables: {out.<table>} (e.g. "
                        "{out.segments}) and {in.normalized_messages}, the pinned source. "
                        "An edit that finds nothing to change is an error. Every binding "
                        "runs the same command line unless an adjustment says otherwise.")
    p.add_argument("--allow-same-gpsdio-pin", action="store_true",
                   help="Let the run start when every binding pins the same gpsdio-segment. "
                        "By default that is refused, since this workflow was written to "
                        "compare two gpsdio-segment pins. Pass it when the bindings differ "
                        "in pipe-segment itself (e.g. v5.0.3 against v5.1.0, which pin the "
                        "same gpsdio-segment).")
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
                        f"for; see {OVERLAY_DIR}). Reviewed bindings run it. A binding whose "
                        f"ref is not merged into origin/main is refused on it before anything "
                        f"is pinned: nothing is auto-built, since a plain build has no overlay. "
                        f"Pass an overlay image of the commit (built with {OVERLAY_DIR}/"
                        f"build.sh), or use --runner DirectRunner --build-from-source. An "
                        f"explicit value applies to every binding.")
    p.add_argument("--bq-temp-dataset", default=DEFAULT_BQ_TEMP_DATASET,
                   help="Existing BigQuery dataset Beam stages query results in; env-var "
                        "fallback DIT_BQ_TEMP_DATASET (default ${PROJECT}.${DIT_DEST_DATASET}). "
                        "Passed to the container as the DIT_BQ_TEMP_DATASET variable, which "
                        "an overlay image reads; pipe-segment has no option for it, and an "
                        "image without the overlay ignores it.")
    p.add_argument("--build-from-source", action="store_true",
                   help="Build pipe-segment from each worktree via `docker compose build dev` "
                        "instead of pulling an image. Compose mounts the worktree's "
                        "source tree, so worker code reflects each binding's ref. Bypasses the "
                        "default-image pull -- the natural "
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
    args.binding_images = _parse_binding_images(args.binding_images, args.bindings)
    args.binding_adjustments = _parse_binding_adjustments(
        args.binding_adjustments, args.bindings,
    )
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


def _require_binding(flag: str, name: str, bindings: Sequence[tuple[str, str]]) -> None:
    names = [binding for binding, _ in bindings]
    if name not in names:
        raise SystemExit(
            f"{flag} names the binding {name!r}, and the bindings are {', '.join(names)}."
        )


def _parse_binding_images(
    specs: Sequence[str], bindings: Sequence[tuple[str, str]],
) -> dict[str, str]:
    """``--binding-image NAME=IMAGE`` as binding name to image. A name that is
    no binding, or given twice, is refused: the image would run nowhere, or
    one of the two would be dropped silently."""
    images: dict[str, str] = {}
    for spec in specs:
        name, separator, image = spec.partition("=")
        if not (name and separator and image):
            raise SystemExit(f"--binding-image must be NAME=IMAGE; got {spec!r}")
        _require_binding("--binding-image", name, bindings)
        if name in images:
            raise SystemExit(f"--binding-image is given twice for the binding {name!r}.")
        images[name] = image
    return images


def _parse_binding_adjustments(
    specs: Sequence[str], bindings: Sequence[tuple[str, str]],
) -> list[adjust.Adjustment]:
    """``--binding-adjust`` entries as adjustments, in the order given. The
    binding and the step must exist, so a mistyped name cannot read as an
    edit that was applied."""
    def parse(spec: str) -> adjust.Adjustment:
        try:
            name, adjustment = adjust.parse_ad_hoc(spec)
        except adjust.AdjustmentError as exc:
            raise SystemExit(f"--binding-adjust {exc}") from exc
        _require_binding("--binding-adjust", name, bindings)
        unknown = [edit.step for edit in adjustment.edits if edit.step not in STEPS]
        if unknown:
            raise SystemExit(
                f"--binding-adjust {spec!r}: no step {unknown[0]!r}; the steps are "
                f"{', '.join(STEPS)}."
            )
        return adjustment

    return [parse(spec) for spec in specs]


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


def _binding_image(args: argparse.Namespace, binding_name: str) -> str:
    """The image a binding asks for: its ``--binding-image``, else
    ``--image-tag``."""
    return args.binding_images.get(binding_name, args.image_tag)


def _runs_default_image(args: argparse.Namespace, binding_name: str) -> bool:
    """Whether the binding executes the default image: its image
    (:func:`_binding_image`) is the default and is executed, as the submitter
    unless ``--build-from-source``, or as the workers' image on Dataflow
    (passed as ``--sdk_container_image`` with or without
    ``--build-from-source``)."""
    return _binding_image(args, binding_name) == DEFAULT_IMAGE_TAG and (
        not args.build_from_source or args.runner == "dataflow"
    )


def check_images(
    args: argparse.Namespace, binding_name: str, pipeline_commit: str, unreviewed: bool,
) -> None:
    """Refuse a binding whose unreviewed code would run on the default image.

    The same guard as ``ais.py``'s and port visits' ``check_images``. The
    default image holds pipe-segment ``DEFAULT_PIPELINE_VERSION`` with DIT's
    temp-dataset overlay, not the binding's code. The shared harness would
    replace it with a kaniko build of plain source, which has no overlay, so
    Beam would try to create its own BigQuery staging dataset and the Dataflow
    job would fail. So nothing is auto-built for this workflow:
    ``resolve_run_context`` calls this in place of the auto-build, and
    ``_preflight_images`` calls it for every binding before anything is
    pinned. An explicit ``--image-tag`` or ``--binding-image`` is always
    respected.
    """
    if not (unreviewed and _runs_default_image(args, binding_name)):
        return
    raise SystemExit(
        f"error: binding {binding_name!r} is unreviewed (commit {pipeline_commit} is not "
        f"merged into pipe-segment's origin/main), and the run would execute DIT's default "
        f"image:\n"
        f"  {DEFAULT_OVERLAY_IMAGE}\n"
        f"That image holds pipe-segment {DEFAULT_PIPELINE_VERSION} with DIT's temp-dataset "
        f"overlay, not this code. DIT does not auto-build an image for this workflow: a "
        f"plain build has no overlay, and its Dataflow job would fail creating a BigQuery "
        f"staging dataset. Choose one:\n"
        f"  - build an overlay image of the commit and pass it as "
        f"--binding-image {binding_name}=<image>:\n"
        f"      {OVERLAY_DIR}/build.sh <pipe-segment dir> {pipeline_commit} <label> --push\n"
        f"  - check the code locally, without Dataflow: --runner DirectRunner "
        f"--build-from-source (the docker runner builds each binding's worktree; no "
        f"overlay there either: Beam creates its own staging dataset, with your "
        f"credentials)"
    )


def _preflight_images(args: argparse.Namespace) -> None:
    """Apply ``check_images`` to every binding before any cloud call. Reads each
    ref's commit and its ancestry of ``origin/main`` (``git fetch`` included),
    as ``resolve_run_context`` does later for the binding's worktree."""
    for name, ref in args.bindings:
        if not _runs_default_image(args, name):
            continue
        commit = subprocess.run(
            ["git", "-C", args.pipeline_dir, "rev-parse", "--verify", ref + "^{commit}"],
            check=True, capture_output=True, text=True,
        ).stdout.strip()
        check_images(args, name, commit, is_unreviewed(commit, args.pipeline_dir))


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
    *,
    allow_same: bool = False,
) -> dict[str, str]:
    """Fail fast if every binding pins the same gpsdio-segment line.

    A two-binding A/B with identical pins is a misconfiguration that would
    otherwise silently produce an IDENTICAL diff — looking like "the change
    has no effect" when in fact the change isn't under test at all. Reads
    each ref's ``requirements/prod.in`` via ``git show`` (no worktree needed)
    and aborts before pinning if all values collapse to one. Single-
    binding runs are exempt (nothing to compare). (Copilot PR #44 round 2.)

    ``allow_same`` (``--allow-same-gpsdio-pin``) turns the refusal into a log
    line, for a run whose bindings differ in pipe-segment itself: v5.0.3 and
    v5.1.0 pin the same gpsdio-segment.
    """
    pins = {name: _read_gpsdio_pin_at_ref(pipeline_dir, ref) for name, ref in bindings}
    for name, pin in pins.items():
        logger.info("binding %s: gpsdio-segment pin -> %s", name, pin)
    if len(bindings) > 1 and len(set(pins.values())) == 1:
        single = next(iter(set(pins.values())))
        if allow_same:
            logger.info(
                "all %d bindings pin the same gpsdio-segment (%s); allowed by "
                "--allow-same-gpsdio-pin: the bindings are taken to differ in pipe-segment "
                "itself.", len(bindings), single,
            )
            return pins
        raise SystemExit(
            f"All {len(bindings)} bindings pin the same gpsdio-segment "
            f"({single!r}). The A/B is misconfigured -- there is no actual "
            f"code difference under test. Did you forget to push the "
            f"experiment branch with the repinned requirements/prod.in? If the "
            f"bindings differ in pipe-segment itself, pass --allow-same-gpsdio-pin."
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

    This is the base interface's one input (``INPUT_ROLE``). A binding whose
    inputs a table adjustment changed gets the copies of all of them by role
    instead (``pinned_inputs`` of :func:`_step_argvs`); none is declared today.
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


def _base_input(args: argparse.Namespace) -> adjust.Input:
    """The one input of the base interface: the normalized messages over the
    run's date range."""
    return adjust.Input(
        role=INPUT_ROLE,
        source=args.source_normalized_table,
        window=_normalized_window(args.date_range),
    )


def _pin_role(role: str) -> str:
    """The role in a pinned copy's name. The base input keeps PIN_ROLE, so its
    copies are named as before and ``--reuse-pin`` finds earlier ones."""
    return PIN_ROLE if role == INPUT_ROLE else f"{PIN_ROLE}_{role}"


def _pin_inputs(
    args: argparse.Namespace,
    inputs: Sequence[adjust.Input],
    pinned_at: datetime,
    *,
    reuse: bool,
) -> dict[adjust.Input, str]:
    """Pin each of ``inputs`` under the stamp of ``pinned_at``, or with
    ``reuse``, check that an earlier run's copy under that stamp exists.
    Returns each input's pinned copy.

    A reused copy must match this run's experiment id, window and source
    table name, since all of them are in the name. With ``args.dry_run`` the
    copy is only dry-run: its bytes are checked against the cap and nothing
    is created.

    Two inputs whose copies would get the same name are refused: the name
    holds the role and the source's table name, not its dataset.
    """
    names = {
        source: dit_bq.pinned_source_fqn(
            source.source,
            experiment_id=args.experiment_id,
            role=_pin_role(source.role),
            pinned_at=pinned_at,
            window=source.window,
            project=PROJECT,
        )
        for source in inputs
    }
    clashing = [name for name, count in Counter(names.values()).items() if count > 1]
    if clashing:
        raise SystemExit(
            f"error: two inputs would be pinned under one name ({', '.join(clashing)}): "
            + "; ".join(f"{s.role}={s.source}" for s in inputs if names[s] in clashing)
            + ". Give the inputs different roles."
        )

    def pin(source: adjust.Input) -> str:
        if reuse:
            return dit_bq.require_pinned_source(names[source], project=PROJECT)
        return dit_bq.pin_source_into_experiment(
            source.source,
            experiment_id=args.experiment_id,
            role=_pin_role(source.role),
            pinned_at=pinned_at,
            window=source.window,
            expiration_days=args.pin_expiration_days,
            max_bytes=int(args.pin_max_gb * 1e9),
            dry_run=args.dry_run,
            project=PROJECT,
        )

    pinned = {source: pin(source) for source in inputs}
    for source, copy in pinned.items():
        logger.info("pinned %s: %s", source.role, copy)
    return pinned


def _pin_sources(
    args: argparse.Namespace, pinned_at: datetime, *, reuse: bool,
) -> _PinnedSources:
    """Pin the base interface's one input, the normalized messages
    (:func:`_pin_inputs`). What a run without table adjustments reads."""
    source = _base_input(args)
    return _PinnedSources(
        normalized_messages=_pin_inputs(args, (source,), pinned_at, reuse=reuse)[source],
    )


def _pin_stamp(args: argparse.Namespace) -> tuple[datetime, bool]:
    """``(pin stamp, reuse)``: the stamp of ``--reuse-pin``, else now in whole
    seconds."""
    reuse = args.reuse_pin is not None
    pinned_at = (
        dit_bq.parse_pin_stamp(args.reuse_pin) if reuse
        else datetime.now(timezone.utc).replace(microsecond=0)
    )
    logger.info("pin stamp: %s (%s)", dit_bq.format_pin_stamp(pinned_at),
                "reusing an earlier run's copy" if reuse else "a new copy")
    return pinned_at, reuse


def _log_reuse_hint(args: argparse.Namespace, pinned_at: datetime, *, reuse: bool) -> None:
    if not reuse and not args.dry_run:
        logger.info("to rerun on this copy, pass --reuse-pin %s",
                    dit_bq.format_pin_stamp(pinned_at))


def _pin(args: argparse.Namespace) -> _PinnedSources:
    """Copy the source at run start, or with ``--reuse-pin`` reuse an earlier
    run's copy."""
    pinned_at, reuse = _pin_stamp(args)
    logger.info("pinning: %s (copy in %s.%s)",
                args.source_normalized_table, PROJECT, dit_bq.CANONICAL_DATASET)
    pinned = _pin_sources(args, pinned_at, reuse=reuse)
    _log_reuse_hint(args, pinned_at, reuse=reuse)
    return pinned


def _pin_union(
    args: argparse.Namespace, inputs: Sequence[adjust.Input],
) -> dict[adjust.Input, str]:
    """Pin every distinct input of every binding once, at one pin stamp
    (``dit.adjust.union_inputs``). With no table adjustment that is the one
    base input, and this does what :func:`_pin` does."""
    pinned_at, reuse = _pin_stamp(args)
    for source in inputs:
        logger.info("pinning: %s (copy in %s.%s)",
                    source.source, PROJECT, dit_bq.CANONICAL_DATASET)
    pinned = _pin_inputs(args, inputs, pinned_at, reuse=reuse)
    _log_reuse_hint(args, pinned_at, reuse=reuse)
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


def _incremental_flags(*, dest_dataset: str, suffix: str) -> list[str]:
    """The flags that select pipe-segment's incremental segmenter, as
    production's ``segment`` task does since 2026-11-20 (pipe-segment v5.1.0).

    ``--in_segments_table`` and ``--in_fragments_table`` name the run's own
    output tables. Those are pipe-segment's defaults too (``pipeline.py``,
    ``prior_segments_table`` and ``prior_fragments_table`` at v5.1.0); they
    are passed so the argv says where the state of the day before comes from,
    and so a change of default upstream cannot move it.
    """
    table = functools.partial(_output_prefix, dest_dataset=dest_dataset, suffix=suffix)
    return [
        "--incremental",
        f"--in_segments_table={table('segments')}",
        f"--in_fragments_table={table('fragments')}",
    ]


#: The flags without a value that the steps' argvs carry: ``--incremental``
#: (``segment``), ``--wait_for_job`` and ``--no_use_public_ips`` (the two Beam
#: steps). Handed to ``dit.adjust`` so that an edit or the diff never reads
#: the token after one of them as its value. No step takes a flag with several
#: values, so ``multi`` stays empty.
BARE_FLAGS = frozenset({"--incremental", "--wait_for_job", NO_PUBLIC_IPS_OPTION})


def _base_interface(
    args: argparse.Namespace,
    *,
    binding_name: str,
    suffix: str,
    worker_image: str | None,
) -> adjust.Interface:
    """What a binding hands to pipe-segment and takes back when no adjustment
    applies to it: the layer every binding starts from.

    It mirrors production's segment group today (the AIS v3 DAG, pipe-segment
    v5.1.0), so ``segment`` passes ``--incremental``. A binding whose
    pipe-segment differs gets the adjustments of
    ``workflows/pipe_segment/adjustments.py`` and of ``--binding-adjust``
    (:func:`_adjusted`).

    * ``steps``: each step's argv, from the builders above, in run order.
      ``--skip-downstream`` keeps ``segment`` only. The pinned source is the
      placeholder ``{in.normalized_messages}``, filled once it is pinned.
    * ``inputs``: the normalized messages over the run's date range.
    * ``outputs``: the binding's own table for every table the steps write,
      with its compare keys.

    Pure: no docker and no cloud call.
    """
    tables = dict(dest_dataset=args.dest_dataset, suffix=suffix)
    identity = dict(experiment_id=args.experiment_id, binding_name=binding_name)
    source = _base_input(args)

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
            pinned=_PinnedSources(normalized_messages=f"{{in.{source.role}}}"),
            date_range=args.date_range,
            ssvid_filter_query=args.ssvid_filter_query,
            extra_flags=_incremental_flags(**tables),
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
    return adjust.Interface(
        steps={step: builders[step]() for step in steps},
        bare=BARE_FLAGS,
        inputs=(source,),
        outputs=tuple(
            adjust.Output(
                name=table,
                table=_output_prefix(table, **tables),
                keys=tuple(COMPARE_KEYS[table]),
                meaning=OUTPUT_MEANINGS[table],
            )
            for table in _output_tables(args)
        ),
    )


def _output_tables(args: argparse.Namespace) -> tuple[str, ...]:
    """The tables a base binding writes: ``segment``'s three, and the five
    downstream ones unless ``--skip-downstream``."""
    return (
        SEGMENT_OUTPUT_TABLES if args.skip_downstream
        else (*SEGMENT_OUTPUT_TABLES, *DOWNSTREAM_OUTPUT_TABLES)
    )


def _binding_info(args: argparse.Namespace, name: str) -> adjust.BindingInfo:
    """The binding ``name`` as far as it is known without a git call: its
    version when its ref is written as a version tag (``v5.0.3``). ``main``
    reads each ref's own source instead (:func:`_binding_infos`) and hands
    the result on."""
    return adjustments.offline_binding_info(name, dict(args.bindings).get(name))


def _adjusted(
    args: argparse.Namespace,
    *,
    binding_name: str,
    suffix: str,
    worker_image: str | None,
    binding: adjust.BindingInfo | None = None,
) -> adjust.Adjusted:
    """The interface of one binding after its layers: the base, the declared
    adjustments that apply to the binding, then its ``--binding-adjust``
    edits. ``binding`` is what the rules know about it; left out, it is what
    :func:`_binding_info` knows offline.

    ``dit.adjust.AdjustmentError`` when an adjustment does not fit the base:
    a stale declared rule, or an ad hoc edit that finds nothing to change.
    """
    try:
        return adjust.apply(
            _base_interface(
                args, binding_name=binding_name, suffix=suffix, worker_image=worker_image,
            ),
            binding or _binding_info(args, binding_name),
            declared=adjustments.DECLARED,
            ad_hoc=args.binding_adjustments,
        )
    except adjust.AdjustmentError as exc:
        raise adjust.AdjustmentError(f"binding {binding_name}: {exc}") from exc


def _step_argvs(
    args: argparse.Namespace,
    *,
    binding_name: str,
    suffix: str,
    pinned: _PinnedSources,
    worker_image: str | None,
    binding: adjust.BindingInfo | None = None,
    pinned_inputs: Mapping[str, str] | None = None,
) -> dict[str, list[str]]:
    """The argv of every step one binding runs, by step, in run order.

    Derived from the binding's adjusted interface (:func:`_adjusted`), with
    its placeholders filled: ``{in.<role>}`` from the pinned copies and
    ``{out.<table>}`` from the binding's own tables. With no adjustment
    applying, each argv is the base builder's, token for token.

    ``pinned`` is the copy of the base input. ``pinned_inputs`` maps every
    input role of the binding to its copy; it is needed only when a table
    adjustment gave the binding other inputs, and defaults to the base input.

    Pure: no docker and no cloud call, so the drift spec builds the same
    tokens offline.
    """
    interface = _adjusted(
        args, binding_name=binding_name, suffix=suffix,
        worker_image=worker_image, binding=binding,
    ).interface
    roles = (
        {INPUT_ROLE: pinned.normalized_messages} if pinned_inputs is None else pinned_inputs
    )
    filled = adjust.fill(interface, adjust.placeholder_values(interface, roles))
    return {step: list(tokens) for step, tokens in filled.steps.items()}


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


def _next_releases(version: tuple[int, ...]) -> frozenset[tuple[int, ...]]:
    """The versions one release after ``version``: the next patch, minor or
    major. ``(5, 0, 3)`` gives ``(5, 0, 4)``, ``(5, 1, 0)`` and ``(6, 0, 0)``."""
    major, minor, patch_ = version[:3]
    return frozenset({(major, minor, patch_ + 1), (major, minor + 1, 0), (major + 1, 0, 0)})


def _version_problem(
    name: str, binding: adjust.BindingInfo, image: str,
) -> tuple[str, str] | None:
    """Whether the image a binding runs is tagged another pipe-segment version
    than its ref states: ``None`` when not, else ``("warning" | "refuse",
    message)``.

    The binding's flags are chosen and checked for its ref; the image is what
    runs. Without its own ``--binding-image`` a binding runs ``--image-tag``,
    the v5.1.0 overlay by default, whatever version its ref is. So
    ``--binding before=v5.0.3`` would get its ref's flags (no ``--incremental``)
    and run v5.1.0 code, and the run would report v5.1.0 as v5.0.3.

    Nothing to say when no version can be read from the image's tag or from
    the ref's code, or when they agree.

    Only a warning for what looks like an unreleased head: the ref is not
    written as a version tag (a branch or a commit), and the image's tag
    names the release right after the version the ref's code states. An
    unreleased head still states the release before it, and its image may
    carry the version it will be released as (``v5.1.0-pr227`` for the head
    of the PR that became v5.1.0). The rule reads names and versions only, so
    a branch that really is at the older release also gets only the warning.
    """
    in_image = adjustments.image_pipeline_version(image)
    if in_image is None or binding.version is None or in_image == binding.version:
        return None
    stated = adjustments.version_text(binding.version)
    tagged = adjustments.version_text(in_image)
    head_like = (
        adjustments.ref_version(binding.ref or "") is None
        and in_image in _next_releases(binding.version)
    )
    if head_like:
        return "warning", (
            f"binding {name}: the image {image} is tagged pipe-segment {tagged}, the release "
            f"after the {stated} that the binding's ref {binding.ref} states. Taken as an "
            f"unreleased head, which still states the release before it. The binding's "
            f"flags were chosen and checked for the ref; if the image holds other code than "
            f"the ref, give the binding its own: --binding-image {name}=<image of the ref>."
        )
    return "refuse", (
        f"binding {name}: the image {image} is tagged pipe-segment {tagged}, and the "
        f"binding's ref {binding.ref} states {stated}. The binding's flags were chosen and "
        f"checked for the ref, but the image is what runs, so the run would report "
        f"{tagged} code as {stated}. Give the binding an image of its ref: "
        f"--binding-image {name}=<overlay image of {binding.ref}> "
        f"({OVERLAY_DIR}/build.sh)."
    )


def _check_image_versions(
    args: argparse.Namespace, sides: Mapping[str, adjust.Adjusted],
) -> None:
    """Refuse a binding whose image is tagged another pipe-segment version
    than its ref states (:func:`_version_problem`), before anything is
    pinned; log the unreleased-head case as a warning. On the docker runner
    with ``--build-from-source`` no image runs, so there is nothing to
    compare.

    The image is the one the binding asks for (:func:`_binding_image`). The
    harness runs exactly that image: ``check_images`` replaces its auto-build,
    so it never substitutes another one.
    """
    if args.build_from_source and args.runner != "dataflow":
        return
    problems = [
        problem for name, side in sides.items()
        if (problem := _version_problem(name, side.binding, _binding_image(args, name)))
    ]
    for _, message in (p for p in problems if p[0] == "warning"):
        logger.warning("%s", message)
    refused = [message for kind, message in problems if kind == "refuse"]
    if refused:
        raise SystemExit(
            "error: a binding would run an image of another pipe-segment version than its "
            "ref:\n" + "\n".join(f"  {m}" for m in refused) + "\nNothing was pinned or run."
        )


def _run_binding(
    *,
    name: str,
    ref: str,
    args: argparse.Namespace,
    pinned: _PinnedSources,
    suffix: str,
    binding: adjust.BindingInfo | None = None,
    pinned_inputs: Mapping[str, str] | None = None,
) -> int:
    """Run one binding's steps in a worktree of its ref.

    ``binding`` is what the adjustment rules know about the binding (``main``
    reads it from the ref's own source); ``pinned_inputs`` the pinned copy of
    each of its inputs by role. Both default to what a run without
    adjustments needs: see :func:`_step_argvs`.
    """
    binding = binding or _binding_info(args, name)
    requested_image = _binding_image(args, name)
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
        # default tag (unreviewed code refused there by check_images), or the
        # explicit --binding-image / --image-tag passthrough. Passing
        # suffix=<per-binding> takes the "manual / cross-version escape hatch"
        # path in resolve_run_context (record git state as-is, no
        # auto-snapshot -- our worktrees ARE committed refs).
        ctx = resolve_run_context(
            repo_dir=worktree_dir,
            pipeline_name=PIPELINE_NAME,
            runner="docker",
            require_clean=False,
            suffix=suffix,
            worker_image=requested_image,
            default_worker_image=DEFAULT_IMAGE_TAG,
            resolve_digest=False,  # no run cache for pipe-segment yet
            build_from_source=args.build_from_source,
            check_images=functools.partial(check_images, args, name),
        )
        # The image asked for (``check_images`` replaces the auto-build, so the
        # harness substitutes none): the one the preflight checked.
        image_tag = ctx.worker_image
        logger.info("[%s] commit=%s%s image=%s",
                    name, ctx.pipeline_commit,
                    " (UNREVIEWED)" if ctx.unreviewed else "",
                    image_tag)

        if args.dry_run:
            logger.info("[%s] --dry-run set; skipping pipe-segment invocations.", name)
            return 0

        # Each step reads what the one before wrote, so the first failure
        # ends the binding.
        argvs = _step_argvs(
            args, binding_name=name, suffix=suffix,
            pinned=pinned, worker_image=image_tag,
            binding=binding, pinned_inputs=pinned_inputs,
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


def _sides(
    args: argparse.Namespace,
    suffix_by_binding: Mapping[str, str],
    infos: Mapping[str, adjust.BindingInfo] | None = None,
) -> dict[str, adjust.Adjusted]:
    """Every binding's adjusted interface, by binding name. ``infos`` is what
    the rules know about each binding (:func:`_binding_infos`); a binding
    missing from it is known offline only (:func:`_binding_info`).

    The image in a step's ``--sdk_container_image`` is the one asked for
    (:func:`_binding_image`), the same that ``_run_binding`` runs:
    ``resolve_run_context`` hands it back unchanged, because ``check_images``
    replaces its auto-build. No rule and no flag check reads its value;
    :func:`_check_image_versions` reads its tag.
    """
    return {
        name: _adjusted(
            args, binding_name=name, suffix=suffix,
            worker_image=_binding_image(args, name),
            binding=(infos or {}).get(name),
        )
        for name, suffix in suffix_by_binding.items()
    }


#: An output only one binding of a pair writes: (output, the binding that
#: writes it, the binding it is not compared with).
NotCompared = tuple[str, str, str]


def _not_compared(interfaces: Mapping[str, adjust.Interface]) -> list[NotCompared]:
    """The outputs that one binding of a pair writes and the other does not.
    They are listed in the verdict, never dropped silently."""
    def one_sided(a: str, b: str) -> list[NotCompared]:
        pairing = adjust.pair_outputs(interfaces[a], interfaces[b])
        return [
            *((o.name, a, b) for o in pairing.only_left),
            *((o.name, b, a) for o in pairing.only_right),
        ]

    return [
        entry for a, b in itertools.combinations(interfaces, 2) for entry in one_sided(a, b)
    ]


def _run_diffs(
    *,
    args: argparse.Namespace,
    suffix_by_binding: dict[str, str],
    failed_bindings: set[str],
    interfaces: Mapping[str, adjust.Interface] | None = None,
) -> dict[DiffKey, int]:
    """Run diffs over all (output, binding-pair) combinations.

    Two bindings are compared by output name (``dit.adjust.pair_outputs``),
    each on its own table, with the keys of the pair. ``interfaces`` holds
    each binding's adjusted interface; left out, they are rebuilt from
    ``args``. An output only one binding of a pair writes is not compared
    (:func:`_not_compared` lists those for the verdict).

    Output tables are single date-partitioned tables; pipe-segment's
    ``prepare_output_tables`` (pipeline.py:158) clears + writes the
    --date-range slice of each table per run. Because the per-binding
    suffix differs, each binding writes to its own table FQN, so the
    diff sees only the run's date range (no stale data to filter out).
    """
    if interfaces is None:
        interfaces = {
            name: side.interface for name, side in _sides(args, suffix_by_binding).items()
        }
    pairs = {
        (a, b): {
            left.name: (left, right)
            for left, right in adjust.pair_outputs(interfaces[a], interfaces[b]).compared
        }
        for a, b in itertools.combinations(suffix_by_binding.keys(), 2)
    }
    results: dict[DiffKey, int] = {}
    tables = dict.fromkeys(o.name for interface in interfaces.values() for o in interface.outputs)

    for table in tables:
        for (a, b), compared in pairs.items():
            if table not in compared:
                continue
            if a in failed_bindings or b in failed_bindings:
                results[(table, a, b)] = _SKIPPED
                bad = a if a in failed_bindings else b
                logger.info("diff %s  %s vs %s -> SKIPPED (binding %s failed)",
                            table, a, b, bad)
                continue
            left, right = compared[table]
            rc = dit_compare.compare_tables(
                left.table, right.table, keys=list(left.keys), view_suffix="",
            )
            results[(table, a, b)] = rc
            verdict = "IDENTICAL" if rc == 0 else f"DIFFERENT (table-check rc={rc})"
            logger.info("diff %s  %s vs %s -> %s", table, a, b, verdict)
    return results


def _summarize(
    results: dict[DiffKey, int], not_compared: Sequence[NotCompared] = (),
) -> str:
    lines = ["", "pipe-segment cross-version diff summary:"]
    for (table, a, b), rc in results.items():
        if rc == _SKIPPED:
            verdict = "SKIPPED"
        elif rc == 0:
            verdict = "IDENTICAL"
        else:
            verdict = "DIFFERENT"
        lines.append(f"  {table}  {a} vs {b}  -> {verdict} (rc={rc})")
    lines.extend(
        f"  {table}  {a} vs {b}  -> NOT COMPARED (only {a} writes it)"
        for table, a, b in not_compared
    )
    return "\n".join(lines)


# --------------------------------------------------------------------------
# Before anything is pinned: each binding's interface, checked
# --------------------------------------------------------------------------

def _binding_infos(args: argparse.Namespace) -> dict[str, adjust.BindingInfo]:
    """What the adjustment rules may know about each binding, read from its
    ref's own source in ``--pipeline-dir`` with ``git show``: the flags each
    step's command declares, and the version the code states."""
    return {
        name: adjustments.binding_info(args.pipeline_dir, name, ref, STEPS)
        for name, ref in args.bindings
    }


def _check_flags(sides: Mapping[str, adjust.Adjusted]) -> None:
    """Refuse a run in which a binding would pass a flag its pipe-segment does
    not declare.

    pipe-segment hands flags it does not know to Beam, which ignores them
    (``cli/main.py``, ``parse_known_args``; read at v5.0.3 and v5.1.0, not
    run). So a wrong flag does not fail: the job runs without it, and a
    comparison built on it tests something else than it says. Every flag of
    every step is therefore looked up in the ref's own command module, except
    Beam's options on the two Beam steps (``adjustments.BEAM_OPTIONS``).

    The check reads the ref. It assumes the image a binding runs holds that
    ref's code; :func:`_check_image_versions` refuses when the image's tag
    says otherwise.
    """
    problems = [
        f"  binding {name} (ref {side.binding.ref}): {problem}"
        for name, side in sides.items()
        for problem in adjustments.flag_problems(
            side.interface, side.binding, beam_steps=BEAM_STEPS,
        )
    ]
    if problems:
        raise SystemExit(
            "error: the command line of a binding does not fit its pipe-segment:\n"
            + "\n".join(problems)
            + "\nNothing was pinned or run. pipe-segment hands a flag it does not know to "
              "Beam, which ignores it, so such a job would run without the flag and "
              "nothing would fail. For a flag the ref does not declare: take it out for "
              "that binding (--binding-adjust <binding>=<step>:remove:<flag>), or declare "
              "the difference in workflows/pipe_segment/adjustments.py."
        )


def _log_sides(sides: Mapping[str, adjust.Adjusted]) -> None:
    """Put on record what each binding runs and where two bindings differ:
    the edits applied to each with their reasons, the flag differences per
    step, the outputs only one of two writes, and the lineage note when they
    do not read the same inputs."""
    for name, side in sides.items():
        version = side.binding.version
        edits = adjust.describe(side)
        logger.info(
            "[%s] ref %s, pipe-segment %s: %s", name, side.binding.ref,
            adjustments.version_text(version) if version else "of unknown version",
            f"{len(edits)} edit(s) to the base interface" if edits
            else "the base interface, no adjustment",
        )
        for edit in edits:
            logger.info("[%s]   %s", name, edit)

    interfaces = {name: side.interface for name, side in sides.items()}
    for a, b in itertools.combinations(interfaces, 2):
        for diff in adjust.diff_steps(
            interfaces[a], interfaces[b], presence_only=BINDING_OWN_FLAGS,
        ):
            for line in adjust.format_step_diff(diff, a, b):
                logger.info("flags  %s", line)
    for table, writer, other in _not_compared(interfaces):
        logger.warning("output %s: only %s writes it, so it is not compared with %s.",
                       table, writer, other)
    note = adjust.lineage_note(interfaces)
    if note:
        logger.warning("lineage: %s", note)


def _preflight(
    args: argparse.Namespace, suffix_by_binding: Mapping[str, str],
) -> dict[str, adjust.Adjusted]:
    """Each binding's adjusted interface, logged and checked. Runs before
    anything is pinned and makes no cloud call: git reads of ``--pipeline-dir``
    only.

    Refused here:

    * an adjustment that does not fit the base (a stale declared rule, an ad
      hoc edit that finds nothing to change);
    * a placeholder that names no output and no input of its binding;
    * a flag a binding's pipe-segment does not declare (:func:`_check_flags`);
    * an image tagged another version than its binding's ref states, unless it
      looks like an unreleased head (:func:`_check_image_versions`);
    * two bindings that would be compared on different keys.
    """
    try:
        sides = _sides(args, suffix_by_binding, _binding_infos(args))
    except adjust.AdjustmentError as exc:
        raise SystemExit(f"error: {exc}. Nothing was pinned or run.") from exc
    _log_sides(sides)

    for name, side in sides.items():
        unknown = adjust.unknown_placeholders(side.interface)
        if unknown:
            known = adjust.placeholder_values(
                side.interface, {i.role: "" for i in side.interface.inputs},
            )
            raise SystemExit(
                f"error: binding {name}: {', '.join('{' + p + '}' for p in unknown)} names "
                f"no output and no input of this binding. Known: "
                f"{', '.join('{' + p + '}' for p in known)}. Nothing was pinned or run."
            )
    _check_flags(sides)
    _check_image_versions(args, sides)
    for a, b in itertools.combinations(sides, 2):
        try:
            adjust.check_keys(adjust.pair_outputs(sides[a].interface, sides[b].interface))
        except adjust.AdjustmentError as exc:
            raise SystemExit(
                f"error: bindings {a} and {b}: {exc} Nothing was pinned or run."
            ) from exc
    return sides


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
    _verify_distinct_gpsdio_pins(
        args.pipeline_dir, args.bindings, allow_same=args.allow_same_gpsdio_pin,
    )
    # Refuse an unreviewed binding on the default image before anything is
    # pinned (check_images).
    _preflight_images(args)

    suffix_by_binding = {name: f"{args.experiment_id}_{name}" for name, _ in args.bindings}

    # Each binding's interface after its adjustments, logged and checked
    # against its ref's own CLI source, before anything is pinned.
    sides = _preflight(args, suffix_by_binding)
    interfaces = {name: side.interface for name, side in sides.items()}

    # The union of the bindings' inputs, each pinned once at one pin stamp.
    # With no table adjustment that is the one base input.
    copies = _pin_union(args, adjust.union_inputs(interfaces.values()))

    # Bindings run sequentially. dit_docker.run uses os.getcwd() to locate the
    # worktree's compose.yaml; process-wide chdir isn't thread-safe. With
    # typically 2 bindings (before / after) the wall-clock cost is negligible.
    rc_by_binding: dict[str, int] = {}
    logger.info("running %d binding(s) sequentially", len(args.bindings))
    for name, ref in args.bindings:
        pinned_inputs = {i.role: copies[i] for i in interfaces[name].inputs}
        rc = _run_binding(
            name=name, ref=ref, args=args,
            # The copy of the base input; empty for a binding a table
            # adjustment took that input from. The argvs are filled from
            # pinned_inputs.
            pinned=_PinnedSources(normalized_messages=pinned_inputs.get(INPUT_ROLE, "")),
            suffix=suffix_by_binding[name],
            binding=sides[name].binding,
            pinned_inputs=pinned_inputs,
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
        interfaces=interfaces,
    )
    print(_summarize(results, _not_compared(interfaces)), file=sys.stderr)
    return 1 if failed_bindings else 0


if __name__ == "__main__":
    sys.exit(main())
