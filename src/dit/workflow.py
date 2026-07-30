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
from collections.abc import Callable, Sequence
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
from dit.worker_image import ensure_pipeline_image

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# (a) Infra-knob defaults + args
# --------------------------------------------------------------------------
# Per-user infra knobs: defaults below, override via DIT_* env vars or CLI flags.
# These five are IDENTICAL in both workflows. --bq-temp-dataset is NOT here:
# it's workflow-local (each consumer defines its own DEFAULT_BQ_TEMP_DATASET).
DEFAULT_DEST_DATASET = os.environ.get("DIT_DEST_DATASET", "tech_great_expectations")
DEFAULT_DATAFLOW_SA = os.environ.get(
    "DIT_DATAFLOW_SA", "automated-testing@world-fishing-827.iam.gserviceaccount.com"
)
DEFAULT_DATAFLOW_REGION = os.environ.get("DIT_DATAFLOW_REGION", "us-central1")
DEFAULT_DATAFLOW_TEMP_BUCKET = os.environ.get("DIT_DATAFLOW_TEMP_BUCKET", "pipe-temp-us-central-ttl7")
DEFAULT_DATAFLOW_SUBNETWORK = os.environ.get(
    "DIT_DATAFLOW_SUBNETWORK", "regions/us-central1/subnetworks/gfw-internal-us-central1"
)


def add_dataset_args(parser: argparse.ArgumentParser) -> None:
    """Add the dataset-shaped infra knob that EVERY consumer uses.

    Adds ``--dest-dataset`` (output dataset) only -- runner-agnostic. A BQ-SQL
    pipeline (pipe-events) needs it but none of the Dataflow knobs, incl.
    ``--service-account`` (that is the Dataflow worker SA; pipe-events
    authenticates via the mounted ``gcp`` ADC volume, not an SA flag), so it
    calls this and skips :func:`add_dataflow_args`.

    Does NOT add ``--bq-temp-dataset`` -- that is workflow-local (each Beam
    consumer defines its own DEFAULT_BQ_TEMP_DATASET).
    """
    parser.add_argument("--dest-dataset", default=DEFAULT_DEST_DATASET,
                        help="BQ dataset for output tables; env-var fallback DIT_DEST_DATASET.")


def add_dataflow_args(parser: argparse.ArgumentParser) -> None:
    """Add the Dataflow knobs the Beam consumers use.

    Adds ``--service-account`` (the SA Dataflow workers run as),
    ``--dataflow-region``, ``--dataflow-temp-bucket``, ``--dataflow-subnetwork``.
    A BQ-SQL pipeline (pipe-events) runs no Dataflow and authenticates via the
    mounted ``gcp`` ADC volume, so it must NOT add these.
    """
    parser.add_argument("--service-account", default=DEFAULT_DATAFLOW_SA)
    parser.add_argument("--dataflow-region", default=DEFAULT_DATAFLOW_REGION)
    parser.add_argument("--dataflow-temp-bucket", default=DEFAULT_DATAFLOW_TEMP_BUCKET)
    parser.add_argument("--dataflow-subnetwork", default=DEFAULT_DATAFLOW_SUBNETWORK)


def add_infra_args(parser: argparse.ArgumentParser) -> None:
    """Add the full infra-knob set used by the two Beam workflows.

    Composition of :func:`add_dataset_args` + :func:`add_dataflow_args`, so
    the namespace is identical to the pre-split helper: ``--dest-dataset``,
    ``--service-account``, ``--dataflow-region``, ``--dataflow-temp-bucket``,
    ``--dataflow-subnetwork``. pipe-gaps and port-visits keep calling this;
    pipe-events calls only :func:`add_dataset_args` (no Dataflow knobs).
    """
    add_dataset_args(parser)
    add_dataflow_args(parser)


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
# (b2) Mode-subset selection
# --------------------------------------------------------------------------
# Shared by every mode-family workflow (pipe-gaps mode_equivalence,
# port-visits ais, pipe-events fishing) plus port-visits cross_version_ais.
# Four consumers, identical parse-and-validate needs -> shared, per the
# duplicate-until-3 rule.

def parse_modes(raw: str, *, choices: Sequence[str]) -> list[str]:
    """Parse a comma-separated ``--modes`` value into a validated list.

    Returns the selection in **``choices`` order**, not CLI order, so
    ``--modes 3_bftruncate,1_bf`` and ``--modes 1_bf,3_bftruncate`` produce
    identical run order, comparison-pair order, and log output. Duplicates
    collapse. Whitespace around names is tolerated.

    Raises ``SystemExit`` (argparse-style) on an unknown or empty selection,
    naming the valid choices -- a typo'd mode silently running nothing would
    look exactly like a passing run.
    """
    requested = [m.strip() for m in raw.split(",") if m.strip()]
    if not requested:
        raise SystemExit(
            f"error: --modes must name at least one mode; valid: {','.join(choices)}"
        )
    choices_set = set(choices)
    requested_set = set(requested)
    # Report unknowns in the order given, so the message echoes what was typed.
    unknown = [m for m in requested if m not in choices_set]
    if unknown:
        raise SystemExit(
            f"error: unknown mode(s) {','.join(unknown)}; "
            f"valid: {','.join(choices)}"
        )
    return [m for m in choices if m in requested_set]


def add_modes_arg(
    parser: argparse.ArgumentParser,
    *,
    choices: Sequence[str],
    cached: bool = True,
    help_suffix: str = "",
) -> None:
    """Add ``--modes`` (comma-separated subset; defaults to all of ``choices``).

    The value stays a raw string on the namespace; call :func:`parse_modes`
    to validate it (workflows do this in their ``parse_args`` so a bad value
    fails before any cloud call).

    ``cached`` describes whether THIS workflow runs its modes through the run
    cache, because that changes what a subset actually buys the user: with a
    cache, a subset now is reused by a larger selection later; without one
    (pipe-events has no cache integration) a subset only saves time and cost
    on this invocation. Claiming the cache behaviour unconditionally would make
    the CLI help wrong for that consumer -- Copilot review on PR #71.
    """
    default = ",".join(choices)
    reuse_note = (
        "Modes are cached independently, so running a subset now and more "
        "later reuses the earlier modes' output."
        if cached else
        "This workflow has no run cache, so a subset saves time and cost on "
        "this invocation only -- a later run re-executes the modes it needs."
    )
    parser.add_argument(
        "--modes", default=default,
        help=(
            f"Comma-separated subset of modes to run. Default: {default} (all). "
            f"Use a subset for a cheap smoke (e.g. --modes {choices[0]}); "
            f"comparisons run only for pairs where both modes ran, and a "
            f"single-mode run performs no comparison at all. {reuse_note}"
            + (f" {help_suffix}" if help_suffix else "")
        ),
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
    build_from_source: bool = False,
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

    ``build_from_source`` (default False): the docker-runner caller is opting
    out of registry-image consumption (the runner will build the container
    from the working tree via ``docker compose``, ignoring ``image_tag``).
    When True, :func:`ensure_pipeline_image` is bypassed entirely — no
    unnecessary kaniko build is incurred for an image the runner won't pull.
    Beam consumers don't pass this; default False keeps their behaviour
    unchanged.
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

    # Ensure a registry-pullable pipeline image exists for whatever consumer
    # will execute it. M-pivot-4 closes the submitter-vs-worker gap: if the
    # run executes unreviewed code against the default canonical image, the
    # consumer would otherwise run the stale published code, so we auto-build
    # a content-addressable image from the source. Same trigger for both
    # consumers (Beam workers + dit's docker runner). No-op for reviewed code
    # or an explicit override. Done before the digest resolution so the cache
    # key reflects the image actually used.
    #
    # ``build_from_source=True`` short-circuits the auto-build entirely: the
    # docker runner will build the container from the working tree via
    # compose and ignore ``image_tag``, so the kaniko submit would be wasted.
    if not build_from_source:
        worker_image = ensure_pipeline_image(
            pipeline=pipeline_name,
            repo_dir=repo_dir,
            commit=pipeline_commit,
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
    extra_output_tables: Sequence[str] = (),
    log_label: str = "",
) -> str:
    """Wrap an ``execute_*`` call with cache lookup + record-on-miss.

    Returns the FQN of the output table to use for downstream comparisons:

    * On cache **hit** (matching key + all recorded output tables verified to
      still exist): skip ``execute_fn`` entirely; return the cached row's
      ``output_tables[0]`` (the cached comparison FQN, which differs from the
      current run's ``output_fqn`` because of the per-run UUID suffix).
    * On cache **miss** or **stale** (row exists but some table expired): call
      ``execute_fn(**execute_kwargs)``; write a :class:`CachedRun` row with the
      current run's metadata; return ``output_fqn``.

    ``output_fqn`` is the **comparison** FQN -- always recorded FIRST in the
    row's ``output_tables`` list, so a cache hit's ``output_tables[0]`` is the
    table to compare. ``extra_output_tables`` are additional artefacts the run
    produced (e.g. port-visits' per-mode thinned ``port_events_*`` intermediate)
    that must be tracked for cleanup (``cancel_run`` deletes every recorded
    output table) but are NOT comparison targets. They participate in
    existence-verification on a hit (a missing intermediate is a stale entry)
    and in the cache row's TTL (:func:`expires_at_for` over all outputs).
    pipe-gaps passes none -> byte-identical single-element ``output_tables``.

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

    # Comparison FQN first (output_tables[0] is what a future hit returns),
    # then any extra artefacts (deduped, order-preserving) so cleanup can drop
    # every table this run produced -- not just the comparison target.
    output_tables = [output_fqn]
    for fqn in extra_output_tables:
        if fqn not in output_tables:
            output_tables.append(fqn)
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
