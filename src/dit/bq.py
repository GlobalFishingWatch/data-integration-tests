"""BigQuery helpers for integration-test orchestration."""

from __future__ import annotations

import logging
import random
from datetime import date, datetime, timedelta
from typing import Sequence

from google.cloud import bigquery

logger = logging.getLogger(__name__)

DEFAULT_PROJECT = "world-fishing-827"


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
