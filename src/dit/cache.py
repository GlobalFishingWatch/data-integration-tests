"""``tech_great_expectations.dit_runs`` — content-addressable run cache + cleanup registry.

Single BQ table serves three jobs (see `docs/run-cache.md` for the full
design):

1. **Cache** — workflows query for a prior successful run of the same
   ``(pipeline_commit, worker_image, workflow_file, params)`` tuple and
   reuse its output tables instead of recomputing.
2. **Registry** — every run records the Dataflow job IDs + BQ tables it
   produced, so ``make dit-cancel`` can clean them up after a Cloud Build
   cancellation.
3. **Provenance** — "which commit produced this table?" is one BQ query.

This module is the dit-side library; workflows call it from their
``execute_*`` helpers. The actual BQ table is created via
``migrations/001_dit_meta_runs.sql``.

Scaffolding status (2026-05-22): the pure functions are implemented;
BQ-touching functions raise ``NotImplementedError`` with TODO markers.
See `docs/run-cache-impl.md` for the implementation plan.
"""

from __future__ import annotations

import hashlib
import json
import logging
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

#: Fully-qualified name of the cache table. Lives in the same dataset dit
#: already writes workflow outputs to, so no new dataset / IAM grant is
#: required. The ``dit_`` prefix scopes it within the shared dataset.
TABLE_FQN = "world-fishing-827.tech_great_expectations.dit_runs"

#: ``status`` column values.
STATUS_RUNNING = "running"
STATUS_SUCCEEDED = "succeeded"
STATUS_FAILED = "failed"
STATUS_CANCELLED = "cancelled"


# --------------------------------------------------------------------------
# Cache-key types
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class CacheKey:
    """Inputs that hash into the cache key.

    Kept as a structured value so workflows can build it step-by-step (each
    field has its own provenance) and so the hash itself is testable
    independent of how the fields were sourced.

    * ``pipeline_commit`` — short or full SHA of the pipeline tree at submit
      time. Comes from ``git rev-parse HEAD`` in the workflow's repo.
    * ``worker_image_digest`` — ``<repo>@sha256:...`` form, NOT ``:tag``
      form. Tags are mutable; digests are not. Resolve via
      :func:`resolve_worker_image_to_digest`.
    * ``workflow_file_sha1`` — sha1 of the workflow file's bytes
      (:func:`sha1_of_workflow_file`). The dit-side cache buster: pure
      ``dit.*`` library refactors don't change this; workflow-file edits do.
    * ``params`` — output-affecting parameters as a JSON-serialisable dict.
      Each workflow exposes a ``canonical_params_dict(args)`` function
      filtering its argparse namespace.
    """

    pipeline_commit: str
    worker_image_digest: str
    workflow_file_sha1: str
    params: Mapping[str, Any]


def compute_cache_key(key: CacheKey) -> str:
    """Return the sha256-hex of the canonical-JSON encoding of ``key``.

    Determinism is load-bearing: ``sort_keys=True`` + tight separators
    + UTF-8 encoding pin the byte sequence so two callers with the same
    inputs always produce the same hash. Adding a field to ``CacheKey``
    will silently shift every existing hash; if that happens
    intentionally, all in-flight cache entries are invalidated (which is
    usually what you want).

    ``key.params`` is passed through :func:`canonicalise_params` defensively
    so callers can't accidentally end up with different cache keys for
    semantically identical params (e.g. ``ssvids`` in different orders).
    Calling canonicalise on already-canonical params is a cheap no-op.
    """
    payload = json.dumps(
        {
            "pipeline_commit": key.pipeline_commit,
            "worker_image_digest": key.worker_image_digest,
            "workflow_file_sha1": key.workflow_file_sha1,
            "params": canonicalise_params(key.params),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------
# Component computations (pure helpers)
# --------------------------------------------------------------------------

def sha1_of_workflow_file(workflow_path: str | Path) -> str:
    """Return the sha1-hex of the workflow file's raw bytes."""
    return hashlib.sha1(Path(workflow_path).read_bytes()).hexdigest()


def canonicalise_params(params: Mapping[str, Any]) -> dict[str, Any]:
    """Sort + normalise a params mapping for inclusion in the cache key.

    **Contract**: ``list`` values are treated as unordered and always
    sorted (e.g. ``ssvids``). ``tuple`` values are treated as ordered and
    preserved (e.g. ``modes``). Callers choose the container that matches
    the semantics of each field. Everything else (str / int / bool / None
    / dict) passes through unchanged.

    Callers (workflow-side ``canonical_params_dict``) are responsible for
    excluding plumbing-only fields (``service_account``, ``region``, etc.)
    BEFORE handing the dict to this function. This function just
    normalises what it's given; it does not know which fields affect
    pipeline output.
    """
    out: dict[str, Any] = {}
    for k in sorted(params):
        v = params[k]
        if isinstance(v, list):
            # Unordered: sort for cache-key stability.
            out[k] = sorted(v)
        elif isinstance(v, tuple):
            # Ordered: preserve, but render as list for JSON.
            out[k] = list(v)
        else:
            out[k] = v
    return out


def resolve_worker_image_to_digest(image_ref: str) -> str:
    """Resolve a tag-form image ref to ``<repo>@sha256:<digest>`` form.

    Tags are mutable (e.g. ``:main`` retags happen); digests are not.
    Pinning the cache key to a digest means a retag invalidates existing
    cache entries automatically.

    Implementation calls ``gcloud container images describe`` under the
    hood (~1–2s; could be cached locally per-image-ref per-run if the
    workflow resolves more than one image, but each workflow has one
    worker image at a time so we don't bother).

    If the input already looks digest-shaped (``@sha256:...``), return as-is.

    Raises ``RuntimeError`` if the resolution fails (image doesn't exist,
    no network, etc.) — callers that can tolerate this should catch and
    fall back to the tag form, accepting cache-invalidation risk.
    """
    if "@sha256:" in image_ref:
        return image_ref
    result = subprocess.run(
        ["gcloud", "container", "images", "describe", image_ref,
         "--format=value(image_summary.digest)"],
        capture_output=True, text=True, check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"could not resolve {image_ref!r} to a digest: "
            f"gcloud exited {result.returncode}: {result.stderr.strip()!r}"
        )
    digest = result.stdout.strip()
    if not digest.startswith("sha256:"):
        raise RuntimeError(
            f"gcloud returned unexpected digest format for {image_ref!r}: {digest!r}"
        )
    repo = image_ref.rsplit(":", 1)[0] if ":" in image_ref else image_ref
    return f"{repo}@{digest}"


# --------------------------------------------------------------------------
# Cached row
# --------------------------------------------------------------------------

@dataclass
class CachedRun:
    """A row of ``tech_great_expectations.dit_runs``.

    Mirrors the BQ schema in ``docs/run-cache.md`` § Schema. The
    ``params`` field maps to the BQ ``JSON`` column (NULLABLE) and round-
    trips as a Python dict via the bigquery client — no manual
    ``json.loads`` / ``json.dumps`` on the application side.
    """

    run_id: str
    cache_key: str
    workflow: str
    pipeline: str
    experiment_id: str
    pipeline_commit: str
    pipeline_dirty: bool
    dit_commit: str
    workflow_file_sha1: str
    worker_image: str
    params: Mapping[str, Any] | None
    output_tables: list[str]
    dataflow_job_ids: list[str]
    cloud_build_id: str | None
    started_at: datetime
    finished_at: datetime | None
    status: str
    expires_at: datetime

    @classmethod
    def from_bq_row(cls, row: Mapping[str, Any]) -> "CachedRun":
        """Construct from a BQ row mapping. Out-of-band coercion for the
        types BQ surfaces differently (TIMESTAMP -> datetime, ARRAY -> list).
        """
        # TODO: implement when read path lands.
        raise NotImplementedError("CachedRun.from_bq_row — implement with read_cache()")

    def to_bq_row(self) -> dict[str, Any]:
        """Render as a dict suitable for `bigquery.Client.insert_rows_json`."""
        # TODO: implement when write path lands.
        raise NotImplementedError("CachedRun.to_bq_row — implement with write_cache()")


# --------------------------------------------------------------------------
# BQ-touching operations (stubs; see docs/run-cache-impl.md)
# --------------------------------------------------------------------------

def read_cache(cache_key: str) -> CachedRun | None:
    """Return the most-recent successful, non-dirty, non-expired
    :class:`CachedRun` with the given ``cache_key``, or None.

    Query shape::

        SELECT *
        FROM `world-fishing-827.tech_great_expectations.dit_runs`
        WHERE cache_key = @key
          AND status = 'succeeded'
          AND pipeline_dirty = FALSE
          AND expires_at > CURRENT_TIMESTAMP()
        ORDER BY started_at DESC
        LIMIT 1
    """
    raise NotImplementedError("read_cache — see docs/run-cache-impl.md § Milestone 2")


def verify_tables_exist(table_fqns: list[str]) -> list[bool]:
    """For each FQN, return True iff the table currently exists in BQ.

    Cache hits must verify physical existence before returning — TTL may
    have removed the tables since the row was written. Single
    INFORMATION_SCHEMA.TABLES query per dataset is more efficient than
    one ``Client.get_table`` per FQN.
    """
    raise NotImplementedError("verify_tables_exist — see docs/run-cache-impl.md § Milestone 2")


def expires_at_for(table_fqns: list[str]) -> datetime:
    """``min(expiration_time)`` across the supplied tables.

    Computed from INFORMATION_SCHEMA.TABLES. Any earlier-expiring output
    invalidates the whole cache entry — the row is unusable the moment
    any of its outputs expire. Returns ``now + 1 day`` if none of the
    tables have an explicit expiration (effectively a short caching
    window for caller awareness).
    """
    raise NotImplementedError("expires_at_for — see docs/run-cache-impl.md § Milestone 2")


def write_cache(row: CachedRun) -> None:
    """Insert a row into ``tech_great_expectations.dit_runs``.

    The caller decides whether to write (dirty trees should not, per
    the design's reproducibility rule). This function records anything
    given to it.
    """
    raise NotImplementedError("write_cache — see docs/run-cache-impl.md § Milestone 3")


# --------------------------------------------------------------------------
# Cancellation helpers (used by `make dit-cancel`; see Milestone 5)
# --------------------------------------------------------------------------

def cancel_run(run_id: str) -> None:
    """Cancel an in-flight or stale run: cancel Dataflow jobs, drop
    output tables, mark the row ``cancelled``.

    Idempotent — re-running on an already-cancelled run is a no-op.
    """
    raise NotImplementedError("cancel_run — see docs/run-cache-impl.md § Milestone 5")
