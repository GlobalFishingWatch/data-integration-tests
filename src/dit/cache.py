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
from datetime import datetime, timedelta, timezone
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
    # unreviewed_code: approximates "this code isn't a reviewed/main commit".
    # As shipped (M-pivot-3) it's the M-pivot-2 resolved flag: TRUE for
    # snapshot refs, dirty trees, and DIT_PIPELINE_COMMIT-provided commits;
    # FALSE for a clean checkout (of ANY branch -- the precise
    # `git merge-base --is-ancestor origin/main` refinement is deferred).
    # read_cache no longer filters on it; it's informational. Strict-provenance
    # queries should read FALSE as "clean checkout", not literally "on main".
    unreviewed_code: bool
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
    # For snapshot runs: the HEAD the dirty tree was based on (parsed from the
    # snapshot commit message). NULL for non-snapshot runs. Trails the required
    # fields with a default so existing positional construction keeps working.
    pipeline_commit_parent: str | None = None

    @classmethod
    def from_bq_row(cls, row: Any) -> "CachedRun":
        """Construct from a ``google.cloud.bigquery.Row`` (or anything with
        the same attribute-access shape).

        The BQ Python client coerces TIMESTAMP → ``datetime``, ARRAY → list,
        and JSON → ``dict`` natively; we just pull each column off the row.
        The dataclass field ``params`` reads from the BQ column
        ``params_json`` (the rename keeps the dataclass field pythonic
        while the BQ column name stays self-documenting).
        """
        return cls(
            run_id=row.run_id,
            cache_key=row.cache_key,
            workflow=row.workflow,
            pipeline=row.pipeline,
            experiment_id=row.experiment_id,
            pipeline_commit=row.pipeline_commit,
            # Prefer unreviewed_code; fall back to the legacy pipeline_dirty
            # for the brief window before migration 002 backfills (and for any
            # reader pointed at a not-yet-migrated table).
            unreviewed_code=bool(
                getattr(row, "unreviewed_code", None)
                if getattr(row, "unreviewed_code", None) is not None
                else row.pipeline_dirty
            ),
            dit_commit=row.dit_commit,
            workflow_file_sha1=row.workflow_file_sha1,
            worker_image=row.worker_image,
            params=row.params_json,
            output_tables=list(row.output_tables or []),
            dataflow_job_ids=list(row.dataflow_job_ids or []),
            cloud_build_id=row.cloud_build_id,
            started_at=row.started_at,
            finished_at=row.finished_at,
            status=row.status,
            expires_at=row.expires_at,
            pipeline_commit_parent=getattr(row, "pipeline_commit_parent", None),
        )

    def to_bq_row(self) -> dict[str, Any]:
        """Render as a dict suitable for ``bigquery.Client.insert_rows_json``.

        Conversions vs the in-memory dataclass shape:

        * ``datetime`` -> ISO-8601 string (BQ TIMESTAMP).
        * ``params`` (dict) -> JSON-encoded string (BQ JSON column). The
          ``insert_rows_json`` streaming API accepts a string for JSON
          columns and parses it server-side.
        * ``None`` for the nullable fields (``params``, ``cloud_build_id``,
          ``finished_at``) passes through.
        """
        def _iso(dt: datetime | None) -> str | None:
            return dt.isoformat() if dt is not None else None

        return {
            "run_id": self.run_id,
            "cache_key": self.cache_key,
            "workflow": self.workflow,
            "pipeline": self.pipeline,
            "experiment_id": self.experiment_id,
            "pipeline_commit": self.pipeline_commit,
            # Dual-write the legacy column (= unreviewed_code) for one release
            # so the NOT NULL constraint stays satisfied and older readers
            # keep working until pipeline_dirty is dropped.
            "pipeline_dirty": self.unreviewed_code,
            "unreviewed_code": self.unreviewed_code,
            "pipeline_commit_parent": self.pipeline_commit_parent,
            "dit_commit": self.dit_commit,
            "workflow_file_sha1": self.workflow_file_sha1,
            "worker_image": self.worker_image,
            "params_json": json.dumps(self.params) if self.params is not None else None,
            "output_tables": list(self.output_tables),
            "dataflow_job_ids": list(self.dataflow_job_ids),
            "cloud_build_id": self.cloud_build_id,
            "started_at": _iso(self.started_at),
            "finished_at": _iso(self.finished_at),
            "status": self.status,
            "expires_at": _iso(self.expires_at),
        }


# --------------------------------------------------------------------------
# BQ client factory (lazy; tests can pass their own ``client``)
# --------------------------------------------------------------------------

def _make_client() -> Any:
    """Construct a default ``google.cloud.bigquery.Client``.

    Lazy-import so ``import dit.cache`` doesn't require ``google-cloud-bigquery``
    to be installed for the pure-function surface (e.g. for downstream
    callers that only use :func:`compute_cache_key`).
    """
    from google.cloud import bigquery
    return bigquery.Client(project="world-fishing-827")


def _group_by_dataset(table_fqns: list[str]) -> dict[tuple[str, str], list[str]]:
    """Partition fully-qualified table refs by ``(project, dataset)``.

    Both INFORMATION_SCHEMA.TABLES and INFORMATION_SCHEMA.TABLE_OPTIONS
    are dataset-scoped views, so one query per distinct ``(project, dataset)``
    serves any number of tables within it.
    """
    by_dataset: dict[tuple[str, str], list[str]] = {}
    for fqn in table_fqns:
        parts = fqn.split(".")
        if len(parts) != 3:
            raise ValueError(
                f"expected fully-qualified `project.dataset.table` form, got {fqn!r}"
            )
        project, dataset, table = parts
        by_dataset.setdefault((project, dataset), []).append(table)
    return by_dataset


# --------------------------------------------------------------------------
# BQ-touching operations
# --------------------------------------------------------------------------

def read_cache(cache_key: str, *, client: Any = None) -> CachedRun | None:
    """Return the most-recent successful, non-expired :class:`CachedRun` with
    the given ``cache_key``, or None.

    No ``unreviewed_code`` filter (M-pivot-3): the cache key is content-
    addressable on ``pipeline_commit`` (a real or snapshot commit SHA), so an
    unreviewed snapshot row is a legitimate cache hit for a repeat run of the
    same uncommitted code — which is exactly the waste the no-dirty-tree pivot
    set out to eliminate. Strict-provenance callers (cross-pipeline / PR
    validation) filter ``unreviewed_code = FALSE`` themselves at query time.
    """
    client = client or _make_client()
    from google.cloud import bigquery

    query = f"""
        SELECT *
        FROM `{TABLE_FQN}`
        WHERE cache_key = @cache_key
          AND status = '{STATUS_SUCCEEDED}'
          AND expires_at > CURRENT_TIMESTAMP()
        ORDER BY started_at DESC
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("cache_key", "STRING", cache_key),
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        return None
    return CachedRun.from_bq_row(rows[0])


def verify_tables_exist(table_fqns: list[str], *, client: Any = None) -> list[bool]:
    """For each FQN, return True iff the table currently exists in BQ.

    Cache hits must verify physical existence before returning — TTL may
    have removed the tables since the cache row was written. One
    ``INFORMATION_SCHEMA.TABLES`` query per distinct dataset; output
    preserves input order.
    """
    if not table_fqns:
        return []
    client = client or _make_client()
    from google.cloud import bigquery

    by_dataset = _group_by_dataset(table_fqns)
    found: set[str] = set()
    for (project, dataset), tables in by_dataset.items():
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("tables", "STRING", tables),
            ]
        )
        # `INFORMATION_SCHEMA.TABLES` lives at the dataset level in BQ;
        # the project/dataset are part of the *view* reference, not query
        # parameters (BQ doesn't accept parameterised identifiers).
        query = f"""
            SELECT table_name
            FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES`
            WHERE table_name IN UNNEST(@tables)
        """
        for row in client.query(query, job_config=job_config).result():
            found.add(f"{project}.{dataset}.{row.table_name}")
    return [fqn in found for fqn in table_fqns]


def expires_at_for(table_fqns: list[str], *, client: Any = None) -> datetime:
    """``min(table.expires)`` across the supplied tables.

    Any earlier-expiring output invalidates the whole cache entry — the
    row is unusable the moment any of its outputs expire. Returns
    ``now + 1 day`` (UTC) if no input tables have an explicit expiration
    (rare; means we'd cache for at most a day without a TTL handshake).

    Uses ``Client.get_table`` per table rather than INFORMATION_SCHEMA
    because table-level expiration is exposed cleanly on the SDK side
    (combines explicit table TTL + dataset default_table_expiration_ms
    into a single resolved ``expires`` field). One get-table per table
    is fine at our scale (a cache row typically has 1-3 outputs).
    """
    if not table_fqns:
        return datetime.now(timezone.utc) + timedelta(days=1)
    client = client or _make_client()
    from google.api_core import exceptions as gax_exceptions

    expirations: list[datetime] = []
    for fqn in table_fqns:
        try:
            table = client.get_table(fqn)
        except gax_exceptions.NotFound:
            # Missing tables are fine: verify_tables_exist handles
            # existence separately, and a missing table doesn't gate
            # the cache entry's TTL. Other errors (permission, rate
            # limit, transient network) propagate -- silently swallowing
            # them would mask real ops problems.
            logger.debug("expires_at_for: %s not found; skipping", fqn)
            continue
        if table.expires is not None:
            expirations.append(table.expires)

    if not expirations:
        return datetime.now(timezone.utc) + timedelta(days=1)
    return min(expirations)


def write_cache(row: CachedRun, *, client: Any = None) -> None:
    """Insert a row into ``tech_great_expectations.dit_runs`` via a
    parameterised DML INSERT.

    **Why DML INSERT, not streaming inserts (``insert_rows_json``)**:
    streaming-inserted rows sit in a 90-minute buffer during which
    UPDATE/DELETE against them is rejected. Our cancel path
    (``cancel_run`` / ``make dit-cancel`` in M5) needs to UPDATE
    ``status='cancelled'`` mid-flight, which is by definition within
    the buffer window. Streaming is also at-least-once: retries can
    create duplicate rows unless an explicit ``row_ids=`` is passed.
    DML INSERT sidesteps both: rows land in permanent storage
    immediately + every submission is exactly-once. Cost is the same
    (INSERT VALUES scans zero bytes); latency is a few seconds vs
    streaming's sub-second — invisible inside multi-minute Dataflow
    workflows.

    **unreviewed_code** (M-pivot-3) replaces the legacy ``pipeline_dirty``
    flag. Every row is recorded regardless of its value. :func:`read_cache`
    no longer filters on it — a snapshot row is a valid cache hit for a
    repeat run of the same content-addressable commit. ``pipeline_dirty`` is
    dual-written (= ``unreviewed_code``) for one release so the NOT NULL
    column and older readers keep working until it's dropped.

    Exceptions from the BQ query job propagate. ``.result()`` blocks
    until the INSERT commits.
    """
    client = client or _make_client()
    from google.cloud import bigquery

    params_json_str = (
        json.dumps(row.params) if row.params is not None else None
    )

    query = f"""
        INSERT INTO `{TABLE_FQN}` (
            run_id, cache_key, workflow, pipeline, experiment_id,
            pipeline_commit, pipeline_dirty, unreviewed_code,
            pipeline_commit_parent, dit_commit, workflow_file_sha1,
            worker_image, params_json, output_tables, dataflow_job_ids,
            cloud_build_id, started_at, finished_at, status, expires_at
        ) VALUES (
            @run_id, @cache_key, @workflow, @pipeline, @experiment_id,
            @pipeline_commit, @pipeline_dirty, @unreviewed_code,
            @pipeline_commit_parent, @dit_commit, @workflow_file_sha1,
            @worker_image, PARSE_JSON(@params_json), @output_tables, @dataflow_job_ids,
            @cloud_build_id, @started_at, @finished_at, @status, @expires_at
        )
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("run_id", "STRING", row.run_id),
        bigquery.ScalarQueryParameter("cache_key", "STRING", row.cache_key),
        bigquery.ScalarQueryParameter("workflow", "STRING", row.workflow),
        bigquery.ScalarQueryParameter("pipeline", "STRING", row.pipeline),
        bigquery.ScalarQueryParameter("experiment_id", "STRING", row.experiment_id),
        bigquery.ScalarQueryParameter("pipeline_commit", "STRING", row.pipeline_commit),
        # Dual-write the legacy NOT NULL column (= unreviewed_code) for one
        # release; drop once nothing reads pipeline_dirty.
        bigquery.ScalarQueryParameter("pipeline_dirty", "BOOL", row.unreviewed_code),
        bigquery.ScalarQueryParameter("unreviewed_code", "BOOL", row.unreviewed_code),
        bigquery.ScalarQueryParameter("pipeline_commit_parent", "STRING", row.pipeline_commit_parent),
        bigquery.ScalarQueryParameter("dit_commit", "STRING", row.dit_commit),
        bigquery.ScalarQueryParameter("workflow_file_sha1", "STRING", row.workflow_file_sha1),
        bigquery.ScalarQueryParameter("worker_image", "STRING", row.worker_image),
        # JSON column gets a STRING-typed parameter + server-side PARSE_JSON;
        # documented BQ pattern for parameterised JSON inserts. NULL passes
        # through because PARSE_JSON(NULL) returns NULL.
        bigquery.ScalarQueryParameter("params_json", "STRING", params_json_str),
        bigquery.ArrayQueryParameter("output_tables", "STRING", list(row.output_tables)),
        bigquery.ArrayQueryParameter("dataflow_job_ids", "STRING", list(row.dataflow_job_ids)),
        bigquery.ScalarQueryParameter("cloud_build_id", "STRING", row.cloud_build_id),
        bigquery.ScalarQueryParameter("started_at", "TIMESTAMP", row.started_at),
        bigquery.ScalarQueryParameter("finished_at", "TIMESTAMP", row.finished_at),
        bigquery.ScalarQueryParameter("status", "STRING", row.status),
        bigquery.ScalarQueryParameter("expires_at", "TIMESTAMP", row.expires_at),
    ])

    client.query(query, job_config=job_config).result()


# --------------------------------------------------------------------------
# Cancellation helpers (used by `make dit-cancel`; see Milestone 5)
# --------------------------------------------------------------------------

def cancel_run(run_id: str) -> None:
    """Cancel an in-flight or stale run: cancel Dataflow jobs, drop
    output tables, mark the row ``cancelled``.

    Idempotent — re-running on an already-cancelled run is a no-op.
    """
    raise NotImplementedError("cancel_run — see docs/run-cache-impl.md § Milestone 5")
