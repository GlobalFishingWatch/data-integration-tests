"""Cross-version experiment for pipe-segment: identity-match-key change in gpsdio-segment.

Tests the impact of reducing ``MsgProcessor.match_key`` from
``(transponder_type, receiver_type, source)`` to ``(transponder_type,)`` in
gpsdio-segment (the AIS identity-to-position match heuristic). The change
lives in the gpsdio-segment library, not pipe-segment itself; we expose it
via two short-lived pipe-segment branches that pin different gpsdio-segment
SHAs in ``requirements/prod.in`` and ``setup.py``.

Pattern follows ``workflows/port_visits/cross_version_ais.py`` (git-worktree
each binding's ref) but the per-binding execution mirrors
``workflows/pipe_events/fishing.py`` (single-step docker-runner consumer that
goes through ``dit.runners.docker.run`` + ``dit.workflow.resolve_run_context``).

Steps:

1. Verify every binding's git ref exists in ``--pipeline-dir``.
2. Create snapshot dataset ``dit_exp_<sanitized_exp_id>_pipe_segment`` (default
   7-day expiration) and snapshot the source data (``normalized_messages``
   single partitioned table; if ``--include-satellite-offsets`` also
   ``satellite_positions_*`` shards + ``norad_to_receiver_*``).
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
      args=[...], entrypoint="pipe", volumes=["gcp:/root/.config"],
      service="dev", build_from_source=args.build_from_source)`` from the
      worktree CWD. Cloud-mode handling (``--network=cloudbuild``,
      ``GOOGLE_CLOUD_QUOTA_PROJECT``) is automatic via ``_apply_cloud_mode``.
      Pass ``--sdk_container_image=ctx.worker_image`` to the segment CLI so
      Dataflow workers run the same per-binding code.
   e. If ``--skip-downstream`` is not set, chain ``segment_identity`` +
      ``segment_info`` against the per-binding outputs.

4. Diff: for each (output table, binding-pair) run ``dit.compare.compare_tables``.
   Output tables are date-partitioned single tables (since pipe-segment v5.0.0
   PIPELINE-2074), so no per-shard iteration. Diffs are informational (exit
   0 even when non-empty); the overall exit code is non-zero only if a
   binding execution itself failed.

Example (DirectRunner smoke, 1 day, single binding to validate plumbing)::

    dit run workflows/pipe_segment/identity_match_key.py \\
        --experiment-id pipe-segment-smoke \\
        --pin-source-at 2026-06-03T10:00:00Z \\
        --binding before=v5.0.3 \\
        --date-range 2020-01-01,2020-01-01 \\
        --runner DirectRunner \\
        --include-satellite-offsets \\
        --skip-downstream \\
        --build-from-source

Example (Dataflow A/B, 1 day, after the experiment branches are pushed)::

    dit run workflows/pipe_segment/identity_match_key.py \\
        --experiment-id idmatchkey \\
        --pin-source-at 2026-06-03T10:00:00Z \\
        --binding before=v5.0.3 \\
        --binding after=experiment/identity-match-key-transponder-only \\
        --date-range 2020-01-01,2020-01-01 \\
        --runner dataflow \\
        --include-satellite-offsets
"""
from __future__ import annotations

import argparse
import contextlib
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
from dit.job_names import make_job_name
from dit.runners import docker as dit_docker
from dit.workflow import (
    EXPERIMENT_ID_RE,
    add_experiment_id_arg,
    resolve_run_context,
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
DEFAULT_SNAPSHOT_EXPIRATION_DAYS = 7

# Canonical published pipe-segment image, pinned at the prod version that
# composer-dags-production currently runs (``Versions.SEGMENT = "v5.0.3"`` in
# ``dags/core/ais/v3.py``). Read-only to dit by IAM per the absolute prod-infra
# boundary; the docker runner pulls from here for reviewed code at the default
# version. For unreviewed code (snapshot / unmerged experiment branch),
# ``resolve_run_context`` -> ``ensure_pipeline_image`` auto-builds a content-
# addressable ``gcr.io/world-fishing-827/dit/pipe-segment:dit-<commit>`` via
# the M-pivot-4 kaniko machinery, and the docker runner pulls THAT.
# ``--build-from-source`` short-circuits both paths (compose builds the
# ``dev`` service from the worktree's working tree).
DEFAULT_IMAGE_TAG = (
    "us-central1-docker.pkg.dev/gfw-int-infrastructure/core/pipe-segment:v5.0.3"
)

# Compose service + entrypoint pipe-segment's compose.yaml exposes:
#   docker compose run --rm --entrypoint pipe dev segment <args>
COMPOSE_SERVICE = "dev"
CLI_ENTRYPOINT = "pipe"

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
# see CLAUDE.md § "Staging data conventions" / README.md § "Staging data sources".
# Override via --source-normalized-table for ad-hoc runs against a different
# normalized source (e.g. prod cohort, a custom-filtered subset).
DEFAULT_SRC_NORMALIZED_TABLE = (
    f"{PROJECT}.pipe_ais_test_202408290000_internal.normalized_messages"
)

# Satellite-offsets inputs are NOT in the staging cohort; they're date-sharded
# in prod. Opt in via --include-satellite-offsets to also run the offsets
# branch of `segment` (and snapshot the relevant prod tables). For the
# identity-match-key change under test, satellite offsets are not relevant,
# so the default is OFF.
#
# Note the cross-project source for satellite positions: the v3-era prod
# table lives in `gfw-int-pipe-v3` (the prod-v3 AIS project), not in
# `world-fishing-827`. dit reads cross-project from the snapshot job.
PROD_NORAD_TABLE = f"{PROJECT}.pipe_static.norad_to_receiver_v20230510"
PROD_SAT_POS_DATASET = "gfw-int-pipe-v3.satellite_positions"
PROD_SAT_POS_STEM = "satellite_positions_one_second_resolution_"  # date-sharded

# Output tables produced by `segment`. Single date-partitioned tables since
# pipe-segment v5.0.0 (PIPELINE-2074: sharded date tables to partitioned
# tables). Each is one FQN; pipe-segment writes within the date range via
# the partitioning_field="timestamp" column. No _YYYYMMDD suffix.
SEGMENT_OUTPUT_TABLES = (
    "messages_segmented",
    "segments",
    "fragments",
)

# Only produced + diffed when --include-satellite-offsets is set.
SAT_OFFSET_OUTPUT_TABLES = (
    "satellite_timing_offsets",
)

# Optional downstream tables produced by segment_identity / segment_info.
DOWNSTREAM_OUTPUT_TABLES = (
    "segment_identity_daily",
    "segment_info",
)

# Compare-key map per output table. Verified against pipe_segment/schemas/
# (segment_schema.py, message_schema.py) and segment_identity/pipeline.py.
# Each row in the date-partitioned tables is uniquely identified by these
# columns. NOTE: avoid using `summary_timestamp` for segment_identity_daily
# -- that's the row-creation timestamp and will differ between runs by
# wall-clock time even when the data is identical. `first_timestamp` is
# the first POSITION message timestamp in the segment for the day and is
# stable if the segment composition is.
COMPARE_KEYS = {
    "messages_segmented":       ["msgid"],
    "segments":                 ["seg_id", "frag_id"],   # one row per (seg, frag, day)
    "fragments":                ["frag_id"],             # frag_id is globally unique
    "satellite_timing_offsets": ["timestamp", "receiver"],
    "segment_identity_daily":   ["seg_id", "first_timestamp"],
    "segment_info":             ["seg_id"],              # clustered_by seg_id
}


# --------------------------------------------------------------------------
# Argparse
# --------------------------------------------------------------------------

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
    p.add_argument("--pin-source-at", required=True,
                   help="ISO 8601 timestamp for the source-data snapshot (e.g. 2026-06-03T10:00:00Z).")
    p.add_argument("--binding", action="append", required=True, dest="bindings",
                   help="`name=ref` pair, repeatable. `name` must satisfy the experiment-id "
                        "regex (lowercase / digits / _ / -, max 32 chars) since it is embedded "
                        "into BQ table names and the docker compose project name. Each ref "
                        "must resolve in --pipeline-dir.")
    p.add_argument("--date-range", required=True,
                   help="YYYY-MM-DD,YYYY-MM-DD. Forwarded to `segment` and used to derive "
                        "the date-shard list snapshotted from the source dataset.")
    p.add_argument("--runner", default="dataflow", choices=("DirectRunner", "dataflow"),
                   help="DirectRunner runs in-process inside the compose container; dataflow "
                        "submits to GCP. Default: dataflow. For Dataflow, --sdk_container_image "
                        "is auto-set to the per-binding image resolved by the harness.")
    p.add_argument("--pipeline-dir", default=DEFAULT_PIPELINE_DIR,
                   help="pipe-segment checkout used for git worktrees.")
    p.add_argument("--dest-dataset", default=DEFAULT_DEST_DATASET,
                   help="BQ dataset holding the output tables produced by each binding's run.")
    p.add_argument("--snapshot-expiration-days", type=int, default=DEFAULT_SNAPSHOT_EXPIRATION_DAYS,
                   help="default_table_expiration for the snapshot dataset, in days.")
    p.add_argument("--image-tag", default=DEFAULT_IMAGE_TAG,
                   help=f"Pipeline image to run pipe-segment in. Default: {DEFAULT_IMAGE_TAG} "
                        f"(canonical published v5.0.3). For reviewed code at this default, the "
                        f"docker runner pulls it directly. For unreviewed code (snapshot / "
                        f"unmerged experiment branch), resolve_run_context auto-builds a "
                        f"content-addressable gcr.io/world-fishing-827/dit/pipe-segment:dit-"
                        f"<commit> via M-pivot-4 kaniko and uses THAT instead. An explicit "
                        f"override is respected per binding (applied to every binding's "
                        f"resolve_run_context call -- use for manual prebuilt images).")
    p.add_argument("--build-from-source", action="store_true",
                   help="Build pipe-segment from each worktree via `docker compose build dev` "
                        "instead of pulling/building an image. Compose mounts the worktree's "
                        "source tree, so worker code reflects each binding's ref. Bypasses both "
                        "the canonical-image pull and the kaniko auto-build -- the natural "
                        "laptop inner-loop pattern. Note: --runner=dataflow + --build-from-source "
                        "is rarely what you want (workers still pull --sdk_container_image which "
                        "wouldn't reflect the worktree).")
    p.add_argument("--skip-downstream", action="store_true",
                   help="Skip segment_identity and segment_info; only run + diff the segment step. "
                        "Useful for fast smoke tests.")
    p.add_argument("--dry-run", action="store_true",
                   help="Skip pipe-segment invocations and the diff phase; still snapshots source "
                        "and sets up worktrees. Lets you validate orchestration without burning runs.")
    p.add_argument("--ssvid-filter-query", default=None,
                   help="Optional SSVID filter to thin the source data (forwarded to "
                        "segment's --ssvid_filter_query). Highly recommended for DirectRunner "
                        "smoke tests; format: comma-separated double-quoted strings.")
    p.add_argument("--source-normalized-table",
                   default=DEFAULT_SRC_NORMALIZED_TABLE,
                   help=f"Fully-qualified FQN of the normalized AIS messages source. "
                        f"Default: {DEFAULT_SRC_NORMALIZED_TABLE} (staging cohort, "
                        f"single date-partitioned table). pipe-segment's BigQuerySource "
                        f"auto-detects partitioning vs sharding, so any normalized AIS "
                        f"source table works (e.g. prod cohort, custom-filtered subset).")
    p.add_argument("--include-satellite-offsets", action="store_true",
                   help="Also run the satellite-offsets branch of `segment` (requires "
                        "satellite_positions and norad_to_receiver tables). Default OFF: "
                        "the identity-match-key change under test doesn't touch sat offsets, "
                        "and those tables are NOT mirrored to the staging cohort, so "
                        "enabling this snapshots from prod instead.")
    p.add_argument("--service-account",
                   default="automated-testing@world-fishing-827.iam.gserviceaccount.com",
                   help="Service account for Dataflow runs.")
    args = p.parse_args(argv)

    args.bindings = [_parse_binding(b) for b in args.bindings]
    args.pin_source_at = _parse_iso8601(args.pin_source_at)
    args.date_range = _parse_daterange(args.date_range)
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


def _parse_iso8601(s: str) -> datetime:
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s[:-1]).replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(s)
    except ValueError as exc:
        raise SystemExit(f"--pin-source-at: invalid ISO 8601 timestamp {s!r}: {exc}") from exc


def _parse_daterange(s: str) -> tuple[date, date]:
    parts = s.split(",")
    if len(parts) != 2:
        raise SystemExit(f"--date-range must be 'YYYY-MM-DD,YYYY-MM-DD'; got {s!r}")
    try:
        return date.fromisoformat(parts[0]), date.fromisoformat(parts[1])
    except ValueError as exc:
        raise SystemExit(f"--date-range: invalid date in {s!r}: {exc}") from exc


def _shard_dates(start: date, end: date) -> list[date]:
    """Inclusive list of dates between ``start`` and ``end``."""
    if end < start:
        raise SystemExit(f"--date-range end ({end}) is before start ({start})")
    out = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def _shard_suffix(d: date) -> str:
    return d.strftime("%Y%m%d")


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
    """Return the ``gpsdio-segment`` pin line from ``requirements/prod.in`` at ``ref``.

    Uses ``git show <ref>:requirements/prod.in`` so we don't need to materialise
    a worktree just to read one file. Cheap enough to call per binding at
    preflight time before any expensive snapshot/run work begins.
    """
    result = subprocess.run(
        ["git", "-C", pipeline_dir, "show", f"{ref}:requirements/prod.in"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise SystemExit(
            f"could not read requirements/prod.in at {ref!r} in {pipeline_dir}: "
            f"{result.stderr.strip()}"
        )
    for line in result.stdout.splitlines():
        if line.strip().startswith("gpsdio-segment"):
            return line.strip()
    raise SystemExit(
        f"no gpsdio-segment pin found in requirements/prod.in at {ref!r}."
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
    and aborts before snapshotting if all values collapse to one. Single-
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
# Snapshot dataset
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


def _snapshot_dataset_name(experiment_id: str) -> str:
    """Single snapshot dataset; pipe-segment doesn't use the _internal/_published split."""
    return f"{PROJECT}.dit_exp_{_sanitize_for_dataset(experiment_id)}_pipe_segment"


def _ensure_dataset(fq_name: str, *, expiration_days: int) -> None:
    from google.cloud import bigquery
    from google.cloud.exceptions import Conflict

    client = bigquery.Client(project=PROJECT)
    dataset = bigquery.Dataset(fq_name)
    dataset.default_table_expiration_ms = expiration_days * 24 * 60 * 60 * 1000
    try:
        client.create_dataset(dataset, exists_ok=True)
    except Conflict:
        pass
    logger.info("ensured dataset %s (expiration %dd)", fq_name, expiration_days)


def _snapshot_source(args: argparse.Namespace) -> str:
    """Snapshot the input tables and return the destination dataset FQN.

    Always snapshots ``args.source_normalized_table`` (single date-partitioned
    table). If ``--include-satellite-offsets`` is set, also snapshots the
    date-sharded prod satellite-positions table and the static norad table
    that the segment offsets branch needs.
    """
    snap_dataset = _snapshot_dataset_name(args.experiment_id)
    _ensure_dataset(snap_dataset, expiration_days=args.snapshot_expiration_days)

    # Normalized messages: single date-partitioned table -> single snapshot.
    # The unqualified table name (last dot-segment) becomes the snapshot's
    # name in the snapshot dataset; both bindings will read it via the
    # FQN computed in _segment_args.
    src_normalized = args.source_normalized_table
    snap_normalized_name = src_normalized.rsplit(".", 1)[-1]
    logger.info("snapshotting normalized messages: %s -> %s.%s",
                src_normalized, snap_dataset, snap_normalized_name)
    dit_bq.snapshot_table(
        src_normalized,
        f"{snap_dataset}.{snap_normalized_name}",
        as_of=args.pin_source_at,
        project=PROJECT,
        if_not_exists=True,
    )

    if args.include_satellite_offsets:
        start, end = args.date_range
        shard_dates = _shard_dates(start, end)
        logger.info("snapshotting %d date shard(s) of %s (satellite offsets opt-in)",
                    len(shard_dates), PROD_SAT_POS_STEM)
        for d in shard_dates:
            sfx = _shard_suffix(d)
            dit_bq.snapshot_table(
                f"{PROD_SAT_POS_DATASET}.{PROD_SAT_POS_STEM}{sfx}",
                f"{snap_dataset}.{PROD_SAT_POS_STEM}{sfx}",
                as_of=args.pin_source_at,
                project=PROJECT,
                if_not_exists=True,
            )
        # Single static aux table: norad_to_receiver.
        dit_bq.snapshot_table(
            PROD_NORAD_TABLE,
            f"{snap_dataset}.norad_to_receiver_v20230510",
            as_of=args.pin_source_at,
            project=PROJECT,
            if_not_exists=True,
        )

    logger.info("snapshot dataset: %s", snap_dataset)
    return snap_dataset


# --------------------------------------------------------------------------
# Per-binding pipeline runs
# --------------------------------------------------------------------------

def _output_prefix(table: str, *, dest_dataset: str, suffix: str) -> str:
    """Per-binding output-table prefix (without the date-shard suffix)."""
    return f"{PROJECT}.{dest_dataset}.{table}_{_sanitize_for_dataset(suffix)}"


def _segment_args(
    *,
    snap_dataset: str,
    snap_normalized_name: str,
    dest_dataset: str,
    suffix: str,
    experiment_id: str,
    binding_name: str,
    date_range: tuple[date, date],
    runner: str,
    worker_image: Optional[str],
    ssvid_filter_query: Optional[str],
    service_account: str,
    include_satellite_offsets: bool,
) -> list[str]:
    """CLI args for `pipe-segment segment`.

    Normalized messages table is the snapshot of ``--source-normalized-table``
    (a single date-partitioned table); pipe-segment's BigQuerySource auto-
    detects partitioning vs sharding and filters by DATE(timestamp).

    Satellite-offset args only appear when ``include_satellite_offsets`` is
    set; the satellite-offset branch isn't needed to exercise the identity-
    match-key change under test, and its inputs are date-sharded in prod
    (not mirrored to the staging cohort).

    Dataflow ``--job_name`` is built via ``dit.job_names.make_job_name``
    (Copilot PR #44 comment): the shared helper enforces Dataflow's
    ``[a-z0-9-]`` alphabet (underscores are rejected) and the 63-char cap,
    truncating ``experiment_id`` from the right when overlong while
    preserving ``binding`` / ``step`` (load-bearing for triage).
    """
    start, end = date_range
    args = [
        "segment",
        f"--date_range={start.isoformat()},{end.isoformat()}",
        f"--in_normalized_messages_table={snap_dataset}.{snap_normalized_name}",
        f"--out_segmented_messages_table={_output_prefix('messages_segmented', dest_dataset=dest_dataset, suffix=suffix)}",
        f"--out_segments_table={_output_prefix('segments', dest_dataset=dest_dataset, suffix=suffix)}",
        f"--fragments_table={_output_prefix('fragments', dest_dataset=dest_dataset, suffix=suffix)}",
        # Standard labels — make A/B-runs visibly distinct in Dataflow UI.
        # experiment_id + binding are split + sanitised via _safe_label_value
        # because BQ caps label values at 63 chars and the raw suffix can be
        # up to 65 chars (`<exp32>_<binding32>`). (Copilot PR #44 round 2.)
        "--labels=environment=develop",
        "--labels=resource_creator=dit",
        "--labels=step=segment",
        f"--labels=experiment_id={_safe_label_value(experiment_id)}",
        f"--labels=binding={_safe_label_value(binding_name)}",
        # Project + runner.
        f"--project={PROJECT}",
        f"--runner={runner}",
    ]
    if include_satellite_offsets:
        args.extend([
            f"--in_normalized_sat_offset_messages_table={snap_dataset}.{snap_normalized_name}",
            f"--out_sat_offsets_table={_output_prefix('satellite_timing_offsets', dest_dataset=dest_dataset, suffix=suffix)}",
            f"--in_norad_to_receiver_table={snap_dataset}.norad_to_receiver_v20230510",
            f"--in_sat_positions_table={snap_dataset}.{PROD_SAT_POS_STEM}",
        ])
    if ssvid_filter_query is not None:
        args.append(f"--ssvid_filter_query={ssvid_filter_query}")
    if runner == "dataflow":
        args.extend([
            "--setup_file=./setup.py",
            "--wait_for_job",
            "--temp_location=gs://pipe-temp-us-central-ttl7/dataflow_temp",
            "--staging_location=gs://pipe-temp-us-central-ttl7/dataflow_staging",
            "--region=us-central1",
            "--max_num_workers=50",
            "--worker_machine_type=e2-standard-4",
            "--disk_size_gb=50",
            "--experiments=use_runner_v2",
            "--no_use_public_ips",
            "--network=gfw-internal-network",
            "--subnetwork=regions/us-central1/subnetworks/gfw-internal-us-central1",
            f"--service_account_email={service_account}",
            f"--job_name={make_job_name(repo=PIPELINE_NAME, step='segment', experiment_id=experiment_id, binding=binding_name)}",
        ])
        if worker_image is not None:
            args.append(f"--sdk_container_image={worker_image}")
    return args


def _segment_identity_args(
    *,
    dest_dataset: str,
    suffix: str,
    date_range: tuple[date, date],
    runner: str,
    worker_image: Optional[str],
    service_account: str,
) -> list[str]:
    """CLI args for `pipe-segment segment_identity`."""
    start, end = date_range
    args = [
        "segment_identity",
        f"--date_range={start.isoformat()},{end.isoformat()}",
        f"--source_segments={_output_prefix('segments', dest_dataset=dest_dataset, suffix=suffix)}",
        f"--source_fragments={_output_prefix('fragments', dest_dataset=dest_dataset, suffix=suffix)}",
        f"--dest_segment_identity={_output_prefix('segment_identity_daily', dest_dataset=dest_dataset, suffix=suffix)}",
        f"--project={PROJECT}",
        f"--runner={runner}",
    ]
    if runner == "dataflow":
        args.extend([
            "--setup_file=./setup.py",
            "--wait_for_job",
            "--temp_location=gs://pipe-temp-us-central-ttl7/dataflow_temp",
            "--staging_location=gs://pipe-temp-us-central-ttl7/dataflow_staging",
            "--region=us-central1",
            "--max_num_workers=50",
            "--no_use_public_ips",
            "--network=gfw-internal-network",
            "--subnetwork=regions/us-central1/subnetworks/gfw-internal-us-central1",
            f"--service_account_email={service_account}",
        ])
        if worker_image is not None:
            args.append(f"--sdk_container_image={worker_image}")
    return args


def _segment_info_args(
    *,
    dest_dataset: str,
    suffix: str,
    runner: str,
    worker_image: Optional[str],
    service_account: str,
) -> list[str]:
    """CLI args for `pipe-segment segment_info`.

    Note: segment_info reads `segment_vessel`, but our A/B only runs `segment`
    + `segment_identity`. For the smoke version we pass the identity table as
    a stand-in for segment_vessel — segment_info will still produce a per-seg
    aggregation that reflects the identity differences. If it errors, you may
    need to also run the `segment_vessel` step (out of scope for v1).
    """
    args = [
        "segment_info",
        f"--source_segment_identity={_output_prefix('segment_identity_daily', dest_dataset=dest_dataset, suffix=suffix)}",
        f"--source_segment_vessel={_output_prefix('segment_identity_daily', dest_dataset=dest_dataset, suffix=suffix)}",
        f"--destination={_output_prefix('segment_info', dest_dataset=dest_dataset, suffix=suffix)}",
        f"--project={PROJECT}",
        f"--runner={runner}",
    ]
    if runner == "dataflow":
        args.extend([
            "--setup_file=./setup.py",
            "--wait_for_job",
            "--temp_location=gs://pipe-temp-us-central-ttl7/dataflow_temp",
            "--staging_location=gs://pipe-temp-us-central-ttl7/dataflow_staging",
            "--region=us-central1",
            "--max_num_workers=20",
            "--no_use_public_ips",
            "--network=gfw-internal-network",
            "--subnetwork=regions/us-central1/subnetworks/gfw-internal-us-central1",
            f"--service_account_email={service_account}",
        ])
        if worker_image is not None:
            args.append(f"--sdk_container_image={worker_image}")
    return args


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
) -> int:
    """One ``pipe`` subcommand (segment / segment_identity / segment_info) via dit_docker.run.

    Runs from ``worktree_dir`` (chdir context) so ``docker compose`` finds the
    worktree's ``compose.yaml``. ``dit_docker.run`` handles cloud-mode
    (``--network=cloudbuild`` + ``GOOGLE_CLOUD_QUOTA_PROJECT``), per-call
    compose project name uniquification, and ``--entrypoint`` injection.
    """
    project_name = f"dit-pipe-segment-{binding_name}"
    logger.info("[%s] invoking pipe %s (image=%s)",
                binding_name, cli_args[0] if cli_args else "<no-subcmd>", image_tag)
    with _chdir(worktree_dir):
        return dit_docker.run(
            image_tag,
            cli_args,
            entrypoint=CLI_ENTRYPOINT,
            volumes=[GCP_VOLUME],
            service=COMPOSE_SERVICE,
            build_from_source=build_from_source,
            project_name=project_name,
        )


def _verify_gpsdio_segment(worktree_dir: str, *, binding_name: str) -> str:
    """Sanity check: confirm the worktree's pinned gpsdio-segment URL.

    Reads the URL from requirements/prod.in (not requirements.txt — that's
    pip-compiled and includes transitive deps). Returns the URL for logging
    and pair-wise inequality assertion.
    """
    prod_in = Path(worktree_dir, "requirements", "prod.in")
    if not prod_in.exists():
        raise SystemExit(
            f"binding {binding_name!r}: {prod_in} not found. "
            f"Is the worktree pointing at a non-pipe-segment ref?"
        )
    for line in prod_in.read_text().splitlines():
        if line.strip().startswith("gpsdio-segment"):
            return line.strip()
    raise SystemExit(
        f"binding {binding_name!r}: no gpsdio-segment pin found in {prod_in}."
    )


def _run_binding(
    *,
    name: str,
    ref: str,
    args: argparse.Namespace,
    snap_dataset: str,
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

        if args.dry_run:
            logger.info("[%s] --dry-run set; skipping pipe-segment invocations.", name)
            return 0

        snap_normalized_name = args.source_normalized_table.rsplit(".", 1)[-1]
        seg_args = _segment_args(
            snap_dataset=snap_dataset,
            snap_normalized_name=snap_normalized_name,
            dest_dataset=args.dest_dataset,
            suffix=suffix,
            experiment_id=args.experiment_id,
            binding_name=name,
            date_range=args.date_range,
            runner=args.runner,
            worker_image=image_tag,
            ssvid_filter_query=args.ssvid_filter_query,
            service_account=args.service_account,
            include_satellite_offsets=args.include_satellite_offsets,
        )
        rc = _run_pipe_subcommand(
            worktree_dir=worktree_dir, binding_name=name,
            image_tag=image_tag, cli_args=seg_args,
            build_from_source=args.build_from_source,
        )
        if rc != 0:
            logger.error("[%s] segment step failed with rc=%d", name, rc)
            return rc

        if args.skip_downstream:
            return 0

        ident_args = _segment_identity_args(
            dest_dataset=args.dest_dataset,
            suffix=suffix,
            date_range=args.date_range,
            runner=args.runner,
            worker_image=image_tag,
            service_account=args.service_account,
        )
        rc = _run_pipe_subcommand(
            worktree_dir=worktree_dir, binding_name=name,
            image_tag=image_tag, cli_args=ident_args,
            build_from_source=args.build_from_source,
        )
        if rc != 0:
            logger.error("[%s] segment_identity step failed with rc=%d", name, rc)
            return rc

        info_args = _segment_info_args(
            dest_dataset=args.dest_dataset,
            suffix=suffix,
            runner=args.runner,
            worker_image=image_tag,
            service_account=args.service_account,
        )
        rc = _run_pipe_subcommand(
            worktree_dir=worktree_dir, binding_name=name,
            image_tag=image_tag, cli_args=info_args,
            build_from_source=args.build_from_source,
        )
        if rc != 0:
            logger.error("[%s] segment_info step failed with rc=%d", name, rc)
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
    if args.include_satellite_offsets:
        tables.extend(SAT_OFFSET_OUTPUT_TABLES)
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
    logger.info("pin_source_at: %s", args.pin_source_at.isoformat())
    logger.info("bindings:      %s", args.bindings)
    logger.info("date_range:    %s..%s", args.date_range[0], args.date_range[1])
    logger.info("runner:        %s", args.runner)
    logger.info("pipeline_dir:  %s", args.pipeline_dir)
    logger.info("dest_dataset:  %s", args.dest_dataset)
    if args.skip_downstream:
        logger.info("downstream:    SKIPPED (segment_identity, segment_info)")

    _verify_refs(args.pipeline_dir, args.bindings)
    # Cheap preflight: fail before snapshotting if the A/B is misconfigured
    # with identical gpsdio-segment pins (an IDENTICAL diff would otherwise
    # look like "the change has no effect" when in fact nothing was tested).
    _verify_distinct_gpsdio_pins(args.pipeline_dir, args.bindings)
    snap_dataset = _snapshot_source(args)

    suffix_by_binding = {name: f"{args.experiment_id}_{name}" for name, _ in args.bindings}

    # Bindings run sequentially. dit_docker.run uses os.getcwd() to locate the
    # worktree's compose.yaml; process-wide chdir isn't thread-safe. With
    # typically 2 bindings (before / after) the wall-clock cost is negligible.
    rc_by_binding: dict[str, int] = {}
    logger.info("running %d binding(s) sequentially", len(args.bindings))
    for name, ref in args.bindings:
        rc = _run_binding(
            name=name, ref=ref, args=args,
            snap_dataset=snap_dataset,
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
