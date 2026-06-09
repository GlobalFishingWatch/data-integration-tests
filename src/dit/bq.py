"""BigQuery helpers for integration-test orchestration."""

from __future__ import annotations

import logging
import random
from datetime import date, datetime, timedelta, timezone
from typing import Literal, Sequence

from google.cloud import bigquery

logger = logging.getLogger(__name__)

DEFAULT_PROJECT = "world-fishing-827"
CANONICAL_DATASET = "tech_great_expectations"


def _utc_now() -> datetime:
    """Indirection over ``datetime.now(timezone.utc)`` for testability.

    ``snapshot_into_experiment`` reads this to compute the snapshot's
    ``expiration_timestamp``; tests patch ``dit.bq._utc_now`` to make the
    emitted DDL deterministic.
    """
    return datetime.now(timezone.utc)


def drop_tables(prefix: str, *, project: str = DEFAULT_PROJECT) -> None:
    """Drop every table whose fully-qualified id starts with ``prefix``.

    ``prefix`` is matched against ``<project>.<dataset>.<table>`` after the
    dataset has been resolved -- callers must pass at least
    ``<project>.<dataset>.<stem>`` so the dataset can be inferred. Views
    backing the matching tables (e.g. ``<table>_last_versions``) are dropped
    too.
    """
    if prefix.count(".") < 2:
        raise ValueError(
            f"prefix must include project and dataset: '<proj>.<dataset>.<stem>', got {prefix!r}"
        )

    proj, dataset_id, stem = prefix.split(".", 2)
    client = bigquery.Client(project=project)
    dataset_ref = bigquery.DatasetReference(proj, dataset_id)

    # client.list_tables yields refs for both tables and views; one pass covers both.
    for ref in client.list_tables(dataset_ref):
        if ref.table_id.startswith(stem):
            fq = f"{proj}.{dataset_id}.{ref.table_id}"
            logger.info("dropping %s", fq)
            client.delete_table(fq, not_found_ok=True)


def query_for_restricted_ssvids(
    reference_table: str,
    *,
    mid: date,
    backfill_days_w: int,
    seed: int = 42,
    project: str = DEFAULT_PROJECT,
) -> list[str]:
    """Pick a ~half-size sample of ssvids whose data was 'visible' during step 2.

    Reads ``{reference_table}_last_versions`` and partitions ssvids into

    * **triggering** -- have a closed gap that meets all three conditions:
      ``DATE(start_timestamp) < mid - W - 1d`` (OFF predates step 3's
      messages window so the rewrite path can't reconstruct from raw),
      ``mid - W <= DATE(end_timestamp) < mid`` (ON falls in step 2's daily
      DELETE scope under the bugged predicate, so closed v2 gets wiped),
      and ``duration_h > 24`` (so the new fix's open v1 seed actually gets
      emitted -- the seed condition requires at least one full day between
      OFF and ON);
    * **non-triggering** -- everything else.

    Returns ``|G| / 2`` ssvids drawn at random from non-triggering, with the
    complement guaranteed to contain *every* triggering ssvid -- that
    complement is the test signal. If more than half the ssvids are
    triggering, returns the entire non-triggering set (smaller than half)
    rather than diluting the signal. Reproducible via ``seed``.

    Pre-condition: ``{reference_table}_last_versions`` already exists.
    """
    client = bigquery.Client(project=project)

    off_cutoff = mid - timedelta(days=backfill_days_w + 1)
    end_lower = mid - timedelta(days=backfill_days_w)

    sql = f"""
        WITH all_ssvids AS (
            SELECT DISTINCT ssvid FROM `{reference_table}_last_versions`
        ),
        triggering AS (
            SELECT DISTINCT ssvid
            FROM `{reference_table}_last_versions`
            WHERE is_closed = TRUE
              AND DATE(start_timestamp) < DATE('{off_cutoff.isoformat()}')
              AND DATE(end_timestamp) >= DATE('{end_lower.isoformat()}')
              AND DATE(end_timestamp) < DATE('{mid.isoformat()}')
              AND duration_h > 24
        )
        SELECT
          a.ssvid,
          t.ssvid IS NOT NULL AS is_triggering
        FROM all_ssvids a
        LEFT JOIN triggering t USING (ssvid)
    """

    logger.info("Querying %s for triggering ssvids", reference_table)
    rows = list(client.query(sql).result())
    triggering = [r["ssvid"] for r in rows if r["is_triggering"]]
    non_triggering = [r["ssvid"] for r in rows if not r["is_triggering"]]
    target_size = len(rows) // 2

    rng = random.Random(seed)
    rng.shuffle(non_triggering)

    if len(non_triggering) >= target_size:
        restricted = non_triggering[:target_size]
    else:
        restricted = non_triggering

    logger.info(
        "Restricted ssvids: %d / %d total (%d triggering); "
        "complement size %d, contains %d triggering ssvids",
        len(restricted),
        len(rows),
        len(triggering),
        len(rows) - len(restricted),
        len(triggering),
    )

    return list(restricted)


def snapshot_table(
    source_table: str,
    dest_table: str,
    *,
    as_of: datetime | None = None,
    expiration: datetime | None = None,
    project: str = DEFAULT_PROJECT,
    if_not_exists: bool = False,
) -> None:
    """Snapshot ``source_table`` to ``dest_table`` via ``CREATE SNAPSHOT TABLE``.

    BQ snapshots are preferred over time-travel-in-queries for source-data
    pinning: they're pipeline-agnostic (no changes to pipe-gaps /
    pipe-anchorages / pipe-events queries), they persist beyond the source's
    time-travel window (default 7 days), and they're cheap storage-wise --
    only the delta from the source is billed.

    Both table ids are fully-qualified ``project.dataset.table`` strings.
    ``as_of`` pins the snapshot to a past timestamp (must be inside BQ's
    time-travel window). ``expiration`` auto-deletes the snapshot at that
    timestamp; ``None`` persists indefinitely. ``if_not_exists`` emits
    ``CREATE SNAPSHOT TABLE IF NOT EXISTS`` for idempotent re-runs.

    KNOWN FOOTGUN -- ``if_not_exists=True`` ignores ``as_of`` mismatches.
        If ``dest_table`` already exists from a prior call that pinned at a
        DIFFERENT ``as_of``, ``IF NOT EXISTS`` makes the second call a silent
        no-op rather than raising a conflict. Callers re-running with the
        same destination name but a different pin time (e.g. a workflow with
        a re-used ``--experiment-id`` but a fresh ``--pin-source-at``) get
        an A/B run reading from the WRONG baseline with no warning.
        Mitigated in practice by callers using one-shot identifiers in the
        destination name (e.g. ``solo_<6-hex>`` auto-generated
        ``--experiment-id`` defaults + 7-day TTL on snapshot datasets), but
        the trap is real for any caller that reuses an explicit name.

        Affected callers: ``workflows/pipe_segment/identity_match_key.py``
        ``_snapshot_source`` (passes ``if_not_exists=True`` per the matching
        block-comment there). ``workflows/port_visits/cross_version_ais.py``
        ``_snapshot_source`` goes through ``snapshot_dataset`` below which
        applies the same skip-existing rule at the table level. The explicit
        fail-fast path is ``snapshot_table(..., if_not_exists=False)`` (the
        function default), used by ``port_visits/cross_version_ais.py``
        ``_snapshot_thinned_table`` for user-supplied tables.

        RECOMMENDED RESOLUTION (deferred): replace ``if_not_exists: bool``
        with ``if_existing: Literal["skip", "fail", "verify_as_of"]`` (or
        the equivalent overload). ``"skip"`` keeps current
        ``if_not_exists=True`` behaviour for back-compat; ``"fail"`` is the
        current default; ``"verify_as_of"`` reads the existing snapshot's
        ``snapshot_definition.snapshot_time`` and (a) skips when it matches
        ``as_of`` (true idempotence on legitimate retry); (b) raises naming
        both timestamps when it differs (closes the silent-reuse bug).
        ``snapshot_dataset`` would thread the same parameter and apply per
        table. Then cross-version workflows flip to ``"verify_as_of"`` for
        safety while retaining clean retries.
    """
    from google.cloud import bigquery

    parts = ["CREATE SNAPSHOT TABLE"]
    if if_not_exists:
        parts.append("IF NOT EXISTS")
    parts.append(f"`{dest_table}` CLONE `{source_table}`")
    if as_of is not None:
        parts.append(f'FOR SYSTEM_TIME AS OF TIMESTAMP("{as_of.isoformat()}")')
    if expiration is not None:
        parts.append(
            f'OPTIONS(expiration_timestamp=TIMESTAMP("{expiration.isoformat()}"))'
        )
    sql = " ".join(parts)

    logger.info("snapshotting %s -> %s", source_table, dest_table)
    client = bigquery.Client(project=project)
    client.query(sql).result()


def snapshot_dataset(
    source_dataset: str,
    dest_dataset: str,
    *,
    tables: Sequence[str] | None = None,
    as_of: datetime | None = None,
    expiration: datetime | None = None,
    project: str = DEFAULT_PROJECT,
) -> list[str]:
    """Snapshot every table in ``source_dataset`` into ``dest_dataset``.

    Both dataset ids are ``project.dataset`` strings. If ``tables`` is given,
    only those table names (bare, not fully-qualified) are snapshotted.
    Tables already present in ``dest_dataset`` are skipped, so re-runs are
    idempotent. Raises if ``dest_dataset`` does not exist. Returns the list
    of created snapshot table-ids (fully-qualified).

    KNOWN FOOTGUN -- the table-level skip-existing rule (``if table_id in
    existing: continue``) is the same shape as ``snapshot_table``'s
    ``if_not_exists=True`` and shares its trap: a re-run with the same
    ``dest_dataset`` but a different ``as_of`` silently keeps the prior
    snapshots, so the caller's "pinned at the new timestamp" claim is wrong
    with no error surfaced. See ``snapshot_table.__doc__`` for the full
    write-up and the recommended ``if_existing="verify_as_of"`` resolution.
    """
    from google.cloud import bigquery
    from google.cloud.exceptions import NotFound

    client = bigquery.Client(project=project)

    try:
        client.get_dataset(dest_dataset)
    except NotFound as exc:
        raise ValueError(f"destination dataset does not exist: {dest_dataset}") from exc

    existing = {ref.table_id for ref in client.list_tables(dest_dataset)}
    source_tables = [ref.table_id for ref in client.list_tables(source_dataset)]
    if tables is not None:
        wanted = set(tables)
        source_tables = [t for t in source_tables if t in wanted]

    created: list[str] = []
    for table_id in source_tables:
        if table_id in existing:
            logger.info("skipping %s.%s (already exists)", dest_dataset, table_id)
            continue
        src = f"{source_dataset}.{table_id}"
        dst = f"{dest_dataset}.{table_id}"
        snapshot_table(
            src,
            dst,
            as_of=as_of,
            expiration=expiration,
            project=project,
        )
        created.append(dst)

    return created


def snapshot_into_experiment(
    source_table: str,
    *,
    experiment_id: str,
    role: str,
    expiration_days: int = 7,
    as_of: datetime | None = None,
    if_existing: Literal["fail", "skip"] = "skip",
    project: str = DEFAULT_PROJECT,
) -> str:
    """Snapshot ``source_table`` into ``<project>.tech_great_expectations``
    (the canonical dit BQ artifact dataset, defaulting to
    ``world-fishing-827.tech_great_expectations``) with a per-table TTL.
    Returns the destination FQN.

    Dest FQN shape::

        <project>.tech_great_expectations.dit_exp_<sanitised(experiment_id)>_<sanitised(role)>_<source_table_name>

    where:

    * ``sanitised(experiment_id)`` and ``sanitised(role)`` both replace
      ``-`` with ``_`` (matches the legacy ``_sanitize_for_dataset`` shape
      that the per-workflow helpers used; lets old and new artifact names
      share a prefix during migration, and prevents a freeform ``role``
      from producing a BQ table id that needs special quoting).
    * ``source_table_name`` is the last ``.``-separated component of
      ``source_table`` (so ``proj.ds.tbl`` → ``tbl``).
    * ``role`` is a caller-supplied label (e.g. ``cross_version``,
      ``outage_pre``, ``outage_post``, ``pipe_segment``). Caller is
      responsible for keeping roles disjoint per workflow so concurrent
      experiments don't collide on a table name.

    Implements the canonical-dataset policy from ``CLAUDE.md`` § Working
    agreements: dit BQ artifacts belong in ``tech_great_expectations``, no
    per-experiment dataset creation. ``project`` defaults to
    ``world-fishing-827`` but is overridable for the cross-org dodge path
    (e.g. when both source and dest must live in ``gfw-int-vms-v3`` to
    satisfy BQ's same-org snapshot constraint). The expiration is set
    per-table via ``OPTIONS(expiration_timestamp=...)``, computed as
    ``_utc_now() + timedelta(days=expiration_days)``; BQ deletes the
    snapshot at that timestamp automatically.

    ``if_existing="skip"`` (the default) translates to ``CREATE SNAPSHOT
    TABLE IF NOT EXISTS`` for idempotent re-runs. ``if_existing="fail"``
    drops the ``IF NOT EXISTS`` and lets a name collision raise. The
    ``"verify_as_of"`` mode documented in ``snapshot_table.__doc__`` is
    deferred — see ``docs/snapshot-dataset-migration-2026-06.md`` (it's
    a follow-up PR after the migration completes).
    """
    sanitised_experiment_id = experiment_id.replace("-", "_")
    sanitised_role = role.replace("-", "_")
    source_table_name = source_table.rsplit(".", 1)[-1]
    dest_table = (
        f"{project}.{CANONICAL_DATASET}."
        f"dit_exp_{sanitised_experiment_id}_{sanitised_role}_{source_table_name}"
    )
    expiration = _utc_now() + timedelta(days=expiration_days)

    snapshot_table(
        source_table,
        dest_table,
        as_of=as_of,
        expiration=expiration,
        project=project,
        if_not_exists=(if_existing == "skip"),
    )
    return dest_table


def derived_source_into_experiment(
    source_table: str,
    *,
    experiment_id: str,
    role: str,
    where_clause: str,
    expiration_days: int = 7,
    materialise: bool = False,
    if_existing: Literal["fail", "skip"] = "skip",
    project: str = DEFAULT_PROJECT,
) -> str:
    """Create a derived view (or materialised table) over ``source_table``
    with ``WHERE <where_clause>`` applied, at the canonical
    ``<project>.tech_great_expectations`` dataset with a per-table TTL.
    Returns the destination FQN.

    Dest FQN shape (identical to ``snapshot_into_experiment``)::

        <project>.tech_great_expectations.dit_exp_<sanitised(experiment_id)>_<sanitised(role)>_<source_table_name>

    Sanitisation (``-`` -> ``_`` on both ``experiment_id`` and ``role``)
    and ``source_table_name`` (last ``.``-separated component of
    ``source_table``) follow the same rules as ``snapshot_into_experiment``.
    Callers using the two helpers in the same experiment keep the ``role``
    values disjoint per layer (e.g. ``"outage_pre"`` for the snapshot,
    ``"outage_pre_filtered"`` for the view on top) so the two artifacts
    don't collide on a table name.

    The new primitive layers ON TOP of ``snapshot_into_experiment``: the
    two concerns (pin source at a moment in time vs. mutate source via a
    SQL transform) are orthogonal and compose by passing one helper's
    output FQN as the other's ``source_table`` input. Either step is
    optional; the workflow picks what it needs. See
    ``docs/source-mutation-primitive-2026-06.md`` for the full design.

    ``materialise=False`` (the default) emits ``CREATE VIEW``; the source
    is queried at read time with predicate push-down to the underlying
    partitioned table. ``materialise=True`` switches to ``CREATE TABLE``
    (single CTAS); the result is stored once and read fast on multi-pass
    paths. Both honour ``expiration_timestamp`` and TTL-delete identically.

    ``where_clause`` is interpolated VERBATIM into the SQL. This is the
    same convention as the other ``dit.bq`` helpers; the caller controls
    the SQL fragment and is trusted not to inject. The string itself is
    a stable identifier the caller folds into ``canonical_params_dict`` so
    two runs with different mutations don't share a cache row.

    ``if_existing="skip"`` (the default) emits ``CREATE ... IF NOT EXISTS``
    for idempotent re-runs; ``if_existing="fail"`` drops it and lets a
    name collision raise. The same KNOWN FOOTGUN documented on
    ``snapshot_table`` applies: a re-run with the same dest name but a
    DIFFERENT ``where_clause`` silently keeps the prior artifact. Mitigated
    in practice by keeping ``role`` values disjoint per shape. The
    ``"verify_as_of"`` mode is the eventual parity follow-up shared with
    ``snapshot_into_experiment``.

    ``project`` defaults to ``world-fishing-827`` but is overridable for
    the cross-org dodge path (e.g. when both source and dest must live in
    the same org for downstream consumers).
    """
    sanitised_experiment_id = experiment_id.replace("-", "_")
    sanitised_role = role.replace("-", "_")
    source_table_name = source_table.rsplit(".", 1)[-1]
    dest_table = (
        f"{project}.{CANONICAL_DATASET}."
        f"dit_exp_{sanitised_experiment_id}_{sanitised_role}_{source_table_name}"
    )
    expiration = _utc_now() + timedelta(days=expiration_days)

    kind = "TABLE" if materialise else "VIEW"
    parts = [f"CREATE {kind}"]
    if if_existing == "skip":
        parts.append("IF NOT EXISTS")
    parts.append(
        f'`{dest_table}` OPTIONS(expiration_timestamp=TIMESTAMP("{expiration.isoformat()}"))'
    )
    parts.append(
        f"AS SELECT * FROM `{source_table}` WHERE {where_clause}"
    )
    sql = " ".join(parts)

    logger.info(
        "deriving %s `%s` <- `%s` WHERE %s",
        kind.lower(), dest_table, source_table, where_clause,
    )
    client = bigquery.Client(project=project)
    client.query(sql).result()
    return dest_table
