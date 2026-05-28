"""Shared workflow-harness plumbing for dit integration-test workflows.

Both ``workflows/pipe_gaps/mode_equivalence.py`` and
``workflows/port_visits/ais.py`` need the same scaffolding around their
pipeline-specific logic:

* per-user infra knobs (DIT_* env-backed defaults + the argparse flags that
  override them);
* the ``--experiment-id`` slug (validation, auto-default, argparse flag);
* the ``main()`` preamble that resolves the committed pipeline ref, classifies
  it as reviewed/unreviewed, ensures a worker image exists, and stamps the
  per-run lineage context (run id, dit commit, worker-image digest);
* the run-cache wrapper that turns an ``execute_*`` call into a
  cache-lookup-or-recompute (adopted by pipe-gaps only; port-visits has no
  cache integration).

Everything pipeline-specific (the phase/mode logic, output-table FQN builders,
compare keys, ``DEFAULT_WORKER_IMAGE``, ``--bq-temp-dataset``,
``canonical_params_dict``) stays in the individual workflow files.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import subprocess
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from dit.cache import (
    STATUS_SUCCEEDED,
    CachedRun,
    CacheKey,
    compute_cache_key,
    expires_at_for,
    read_cache,
    resolve_worker_image_to_digest,
    verify_tables_exist,
    write_cache,
)
from dit.git_info import git_info
from dit.snapshot import is_unreviewed, resolve_pipeline_commit, snapshot_parent
from dit.worker_image import ensure_worker_image

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# (a) Infra-knob defaults + args
# --------------------------------------------------------------------------
# Per-user infra knobs: defaults below, override via DIT_* env vars or CLI flags.
# These five are IDENTICAL in both workflows; pipe-gaps keeps the workflow-
# specific --bq-temp-dataset (DEFAULT_BQ_TEMP_DATASET) locally.
DEFAULT_DEST_DATASET = os.environ.get("DIT_DEST_DATASET", "tech_great_expectations")
DEFAULT_DATAFLOW_SA = os.environ.get(
    "DIT_DATAFLOW_SA", "automated-testing@world-fishing-827.iam.gserviceaccount.com"
)
DEFAULT_DATAFLOW_REGION = os.environ.get("DIT_DATAFLOW_REGION", "us-central1")
DEFAULT_DATAFLOW_TEMP_BUCKET = os.environ.get("DIT_DATAFLOW_TEMP_BUCKET", "pipe-temp-us-central-ttl7")
DEFAULT_DATAFLOW_SUBNETWORK = os.environ.get(
    "DIT_DATAFLOW_SUBNETWORK", "regions/us-central1/subnetworks/gfw-internal-us-central1"
)


def add_infra_args(parser: argparse.ArgumentParser) -> None:
    """Add the infra knobs identical to both workflows to ``parser``.

    Adds exactly ``--dest-dataset``, ``--service-account``,
    ``--dataflow-region``, ``--dataflow-temp-bucket``, ``--dataflow-subnetwork``
    with the defaults above. Does NOT add ``--bq-temp-dataset`` -- that is
    pipe-gaps-specific (DEFAULT_BQ_TEMP_DATASET) and stays in the workflow.
    """
    parser.add_argument("--dest-dataset", default=DEFAULT_DEST_DATASET,
                        help="BQ dataset for output tables; env-var fallback DIT_DEST_DATASET.")
    parser.add_argument("--service-account", default=DEFAULT_DATAFLOW_SA)
    parser.add_argument("--dataflow-region", default=DEFAULT_DATAFLOW_REGION)
    parser.add_argument("--dataflow-temp-bucket", default=DEFAULT_DATAFLOW_TEMP_BUCKET)
    parser.add_argument("--dataflow-subnetwork", default=DEFAULT_DATAFLOW_SUBNETWORK)


# --------------------------------------------------------------------------
# (b) Experiment-id helpers
# --------------------------------------------------------------------------
# BQ-table-name-safe slug; max 32 chars. Compiled once.
EXPERIMENT_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")


def default_experiment_id() -> str:
    """Auto-generate a per-invocation experiment id when none is provided.

    The literal ``solo_`` prefix marks "not part of a cross-version
    experiment" so BQ filtering can ignore them.
    """
    return f"solo_{uuid.uuid4().hex[:6]}"


def validate_experiment_id(value: str) -> str:
    if not EXPERIMENT_ID_RE.match(value):
        raise SystemExit(
            f"error: invalid --experiment-id {value!r}: must match "
            f"{EXPERIMENT_ID_RE.pattern} (BQ-table-name safe; max 32 chars)."
        )
    return value


def add_experiment_id_arg(parser: argparse.ArgumentParser) -> None:
    """Add ``--experiment-id`` (with env-var fallback + auto-default)."""
    env_experiment_id = os.environ.get("DIT_EXPERIMENT_ID") or None
    parser.add_argument(
        "--experiment-id",
        type=validate_experiment_id,
        default=(
            validate_experiment_id(env_experiment_id)
            if env_experiment_id
            else default_experiment_id()
        ),
        help="Slug prepended to the output-table suffix (<experiment_id>_<commit>_<uuid>) "
             "for cross-version run linkage. Env-var fallback DIT_EXPERIMENT_ID. "
             "Auto-default solo_<6-hex> when unset. Regex ^[a-z0-9][a-z0-9_-]{0,31}$. "
             "Bypassed entirely when --suffix is set.",
    )


# --------------------------------------------------------------------------
# (c) RunContext + resolve_run_context — the shared main() preamble
# --------------------------------------------------------------------------

def dit_commit() -> str:
    """Short git SHA of the dit checkout the workflow was loaded from.

    Best-effort — returns ``"unknown"`` outside a git repo (e.g. when
    dit is pip-installed from a tarball). The value is recorded for
    provenance only; it does NOT feed into the cache key (dit
    refactors shouldn't invalidate cache entries).
    """
    try:
        import dit
        # dit/__init__.py -> dit/ -> src/ -> repo root
        dit_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(dit.__file__))))
        return subprocess.check_output(
            ["git", "-C", dit_root, "rev-parse", "--short", "HEAD"],
            text=True, stderr=subprocess.DEVNULL,
        ).strip()
    except (subprocess.CalledProcessError, FileNotFoundError, ImportError):
        return "unknown"


@dataclass
class RunContext:
    """Per-``main()`` lineage context resolved once at the start of a run.

    Stamped onto the workflow's argparse namespace so downstream code (suffix
    building, cache wrapper, BQ labels) reads it from one place.
    """

    pipeline_commit: str
    unreviewed: bool
    pipeline_commit_parent: str | None
    worker_image: str
    worker_image_digest: str
    run_id: str
    dit_commit: str


def resolve_run_context(
    *,
    repo_dir: str,
    pipeline_name: str,
    runner: str,
    require_clean: bool,
    suffix: Optional[str],
    worker_image: str,
    default_worker_image: str,
    resolve_digest: bool = True,
) -> RunContext:
    """Resolve the committed ref, worker image, and per-run lineage context.

    Replicates the shared ``main()`` preamble of both workflows:

    * ``suffix is not None`` -> manual / cross-version escape hatch: record
      git state as-is (no auto-snapshot), classifying unreviewed when the tree
      is dirty OR the commit isn't merged into origin/main. Using only the
      dirty bit here would let a clean-but-unmerged worktree skip the
      worker-image auto-build.
    * otherwise -> ``resolve_pipeline_commit`` (auto-snapshots a dirty dataflow
      run + pushes; the cloud path supplies DIT_PIPELINE_COMMIT instead).

    Then resolves the snapshot parent, ensures a worker image exists (closing
    the submitter-vs-worker gap for unreviewed code), and generates the run id +
    dit commit.

    ``resolve_digest`` (default True): resolve the worker image to a
    content-addressable digest (falling back to the tag form on failure, with a
    warning). Set False for callers with no run-cache integration (e.g.
    port-visits) — the digest is unused there, so skipping the ~1-2s gcloud
    describe keeps the run-context resolution side-effect-free for them.
    """
    if suffix is not None:
        pipeline_commit, dirty = git_info(repo_dir)
        unreviewed = dirty or is_unreviewed(pipeline_commit, repo_dir)
    else:
        pipeline_commit, unreviewed = resolve_pipeline_commit(
            repo_dir, pipeline_name, runner=runner, require_clean=require_clean,
        )

    # For snapshot runs, record the HEAD the dirty tree was based on (parsed
    # from the snapshot commit message); None for real/main commits. M-pivot-3.
    pipeline_commit_parent = snapshot_parent(pipeline_commit, repo_dir)

    # Close the submitter-vs-worker gap (M-pivot-4): if this run executes
    # unreviewed code against the default worker image, the workers would run
    # the stale published code. Auto-build a content-addressable worker image
    # from the source so they actually run this code. No-op for reviewed code,
    # an explicit --worker-image, or the docker runner. Done before the digest
    # resolution so the cache key reflects the image actually used.
    worker_image = ensure_worker_image(
        pipeline=pipeline_name,
        repo_dir=repo_dir,
        commit=pipeline_commit,
        runner=runner,
        unreviewed=unreviewed,
        worker_image=worker_image,
        default_worker_image=default_worker_image,
    )

    run_id = uuid.uuid4().hex[:12]
    dc = dit_commit()
    if resolve_digest:
        # resolve_worker_image_to_digest is a one-off ~1-2s gcloud call;
        # fall back to the tag form if it fails (no cache hits then, but
        # the run still produces output).
        try:
            worker_image_digest = resolve_worker_image_to_digest(worker_image)
        except RuntimeError as e:
            logger.warning(
                "could not resolve %s to a digest (%s); falling back to tag form. "
                "Cache lookups will likely miss.",
                worker_image, e,
            )
            worker_image_digest = worker_image
    else:
        # No run-cache integration on this path -> the digest is unused; skip
        # the gcloud describe and record the tag form.
        worker_image_digest = worker_image

    logger.info(
        "run_id=%s pipeline_commit=%s%s dit_commit=%s",
        run_id, pipeline_commit,
        " (UNREVIEWED)" if unreviewed else "",
        dc,
    )

    return RunContext(
        pipeline_commit=pipeline_commit,
        unreviewed=unreviewed,
        pipeline_commit_parent=pipeline_commit_parent,
        worker_image=worker_image,
        worker_image_digest=worker_image_digest,
        run_id=run_id,
        dit_commit=dc,
    )


# --------------------------------------------------------------------------
# (d) run_with_cache — the generic cache wrapper
# --------------------------------------------------------------------------

def run_with_cache(
    execute_fn: Callable[..., None],
    *,
    ctx: RunContext,
    workflow: str,
    pipeline: str,
    experiment_id: str,
    cache_key: CacheKey,
    output_fqn: str,
    execute_kwargs: dict[str, Any],
    log_label: str = "",
) -> str:
    """Wrap an ``execute_*`` call with cache lookup + record-on-miss.

    Returns the FQN of the output table to use for downstream comparisons:

    * On cache **hit** (matching key + output tables verified to still exist):
      skip ``execute_fn`` entirely; return the cached row's
      ``output_tables[0]`` (the cached FQN, which differs from the current
      run's ``output_fqn`` because of the per-run UUID suffix).
    * On cache **miss** or **stale** (row exists but tables expired): call
      ``execute_fn(**execute_kwargs)``; write a :class:`CachedRun` row with the
      current run's metadata; return ``output_fqn``.

    The cache row is written for **every** completed run, including unreviewed
    (snapshot / dirty-tree) ones. Post M-pivot-3 ``read_cache`` no longer
    filters on ``unreviewed_code``: the cache key is content-addressable on
    ``pipeline_commit`` (a real or deterministic-snapshot SHA), so a snapshot
    row is a legitimate hit for a repeat run of the same uncommitted code.
    ``unreviewed_code`` is informational only.

    The caller builds the :class:`CacheKey` (using its own
    ``canonical_params_dict`` + workflow-file sha1) and passes it in, so this
    wrapper has no dependency on a workflow-specific argparse namespace.
    """
    computed_key = compute_cache_key(cache_key)
    key_short = computed_key[:12]
    # Optional caller-supplied label (e.g. the mode) for log readability;
    # padded to match the historical aligned "mode=<...> key=" log shape.
    label = f"mode={log_label:<16} " if log_label else ""

    cached = read_cache(computed_key)
    if cached is not None:
        # Empty output_tables on a "succeeded" row is a degenerate state
        # (shouldn't happen given our write path, but guard against it
        # in case future runs / manual seeds write malformed rows -- the
        # vacuous `all([]) == True` would otherwise step into an
        # IndexError on `cached.output_tables[0]`).
        if cached.output_tables and all(verify_tables_exist(cached.output_tables)):
            logger.info(
                "cache HIT  %skey=%s -> %s",
                label, key_short, cached.output_tables[0],
            )
            return cached.output_tables[0]
        reason = "empty output_tables" if not cached.output_tables else "tables expired"
        logger.info(
            "cache STALE %skey=%s (%s); recomputing",
            label, key_short, reason,
        )

    started_at = datetime.now(timezone.utc)
    execute_fn(**execute_kwargs)
    finished_at = datetime.now(timezone.utc)

    output_tables = [output_fqn]
    row = CachedRun(
        run_id=ctx.run_id,
        cache_key=computed_key,
        workflow=workflow,
        pipeline=pipeline,
        experiment_id=experiment_id,
        pipeline_commit=ctx.pipeline_commit,
        # ctx.unreviewed: True when the code isn't merged into origin/main
        # (snapshot / dirty / unmerged commit via is_unreviewed). See M-pivot-4.
        unreviewed_code=ctx.unreviewed,
        pipeline_commit_parent=ctx.pipeline_commit_parent,
        dit_commit=ctx.dit_commit,
        workflow_file_sha1=cache_key.workflow_file_sha1,
        worker_image=ctx.worker_image,
        params=cache_key.params,
        output_tables=output_tables,
        # TODO(M5): runner doesn't return Dataflow job IDs yet; cancel_run
        # will need to find them via the dit_run_id label until then.
        dataflow_job_ids=[],
        cloud_build_id=os.environ.get("BUILD_ID"),
        started_at=started_at,
        finished_at=finished_at,
        status=STATUS_SUCCEEDED,
        expires_at=expires_at_for(output_tables),
    )
    write_cache(row)
    logger.info(
        "cache MISS %skey=%s -> wrote run %s",
        label, key_short, ctx.run_id,
    )
    return output_fqn
