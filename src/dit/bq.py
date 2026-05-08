"""BigQuery helpers for integration-test orchestration."""

from __future__ import annotations

import logging
import random
from datetime import date, timedelta

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
