"""BigQuery helpers for integration-test orchestration."""

from __future__ import annotations

import dataclasses
import logging
import random
import re
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Literal, Sequence

from google.cloud import bigquery

logger = logging.getLogger(__name__)

DEFAULT_PROJECT = "world-fishing-827"
CANONICAL_DATASET = "tech_great_expectations"

#: Default ceiling on the bytes one pinned copy may process: 1 TB (10**12
#: bytes). ``pin_source_into_experiment`` dry-runs the copy and refuses
#: above it, and gives the job the same value as ``maximum_bytes_billed``.
DEFAULT_PIN_MAX_BYTES = 10**12

#: ``strftime`` format of a pin stamp, as it appears in pinned table names
#: and in the ``dit_pin`` label (labels must be lowercase).
PIN_STAMP_FORMAT = "%Y%m%dt%H%M%Sz"

#: Value of the ``dit_artifact`` label on every pinned copy.
PIN_ARTIFACT_LABEL = "source_pin"

#: Value of the ``dit_artifact`` label on every pinned time-travel view.
PIN_VIEW_ARTIFACT_LABEL = "source_pin_view"

#: BigQuery's time-travel window when a dataset does not set its own.
DEFAULT_TIME_TRAVEL_HOURS = 168


def _utc_now() -> datetime:
    """Indirection over ``datetime.now(timezone.utc)`` for testability.

    ``snapshot_into_experiment`` reads this to compute the snapshot's
    ``expiration_timestamp``; tests patch ``dit.bq._utc_now`` to make the
    emitted DDL deterministic.
    """
    return datetime.now(timezone.utc)


def _experiment_table_fqn(
    source_table: str,
    *,
    experiment_id: str,
    role: str,
    project: str,
    name_suffix: Sequence[str] = (),
) -> str:
    """Canonical dest FQN shared by the ``*_into_experiment`` helpers::

        <project>.tech_great_expectations.dit_exp_<sanitised(experiment_id)>_<sanitised(role)>_<source_table_name>[_<suffix>...]

    Sanitisation replaces ``-`` with ``_`` in ``experiment_id`` and ``role``;
    ``source_table_name`` is the last ``.``-separated part of ``source_table``.
    """
    sanitised_experiment_id = experiment_id.replace("-", "_")
    sanitised_role = role.replace("-", "_")
    source_table_name = source_table.rsplit(".", 1)[-1]
    name = "_".join([
        "dit_exp", sanitised_experiment_id, sanitised_role, source_table_name,
        *name_suffix,
    ])
    return f"{project}.{CANONICAL_DATASET}.{name}"


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
    dest_table = _experiment_table_fqn(
        source_table, experiment_id=experiment_id, role=role, project=project,
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
    dest_table = _experiment_table_fqn(
        source_table, experiment_id=experiment_id, role=role, project=project,
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


# --------------------------------------------------------------------------
# Source pinning by copy
# --------------------------------------------------------------------------
#
# Pins a source by copying it into tech_great_expectations at run start,
# replacing BigQuery snapshots, which fail twice over: they cannot cross
# organisations (production sources sit in a different org than
# world-fishing-827), and a snapshot with an expiration needs
# bigquery.tables.deleteSnapshot, which no DIT identity has. A copy only
# reads the source (cross-org reads work) and needs bigquery.tables.create.
# It uses no time travel, which loses partition pruning on tables modified
# inside their time-travel window. `CREATE TABLE ... LIKE` keeps the
# source's partitioning and clustering, so pipelines read the copy as
# cheaply as the source.


@dataclasses.dataclass(frozen=True)
class SourceWindow:
    """Half-open date window ``[start, end)`` on ``column`` of a source table.

    The predicate is ``DATE(column) >= start AND DATE(column) < end``, the
    form the pipelines use themselves. It prunes partitions of TIMESTAMP and
    DATE partition columns alike.
    """

    column: str
    start: date
    end: date

    def __post_init__(self) -> None:
        if self.end <= self.start:
            raise ValueError(
                f"SourceWindow end {self.end} must be after start {self.start}"
            )

    def predicate(self) -> str:
        return (
            f"DATE({self.column}) >= '{self.start.isoformat()}' "
            f"AND DATE({self.column}) < '{self.end.isoformat()}'"
        )

    def name_part(self) -> str:
        return f"{self.start:%Y%m%d}_{self.end:%Y%m%d}"


def format_pin_stamp(pinned_at: datetime) -> str:
    """``pinned_at`` (timezone-aware) in UTC, formatted as ``PIN_STAMP_FORMAT``."""
    if pinned_at.tzinfo is None:
        raise ValueError(f"pinned_at must be timezone-aware, got {pinned_at!r}")
    return pinned_at.astimezone(timezone.utc).strftime(PIN_STAMP_FORMAT)


def parse_pin_stamp(stamp: str) -> datetime:
    """Inverse of ``format_pin_stamp``; accepts either case."""
    try:
        parsed = datetime.strptime(stamp.lower(), PIN_STAMP_FORMAT)
    except ValueError as exc:
        raise ValueError(
            f"invalid pin stamp {stamp!r}; expected the form 20261022t171500z"
        ) from exc
    return parsed.replace(tzinfo=timezone.utc)


def pinned_source_fqn(
    source_table: str,
    *,
    experiment_id: str,
    role: str,
    pinned_at: datetime,
    window: SourceWindow | None = None,
    project: str = DEFAULT_PROJECT,
) -> str:
    """Dest FQN of a pinned copy of ``source_table``::

        <project>.tech_great_expectations.dit_exp_<experiment_id>_<role>_<source_table_name>
            _<window>_<pin stamp>

    (one name, wrapped here).

    ``<window>`` is ``<start>_<end>`` as ``YYYYMMDD`` (end exclusive), or
    ``all`` without a window. The pin stamp distinguishes copies of a table
    that has no window but still changes (e.g. ``segs_activity``), and the
    window distinguishes copies of one table over different ranges. Pure:
    callers reusing an earlier run's copies recompute the names from the
    same inputs.
    """
    return _experiment_table_fqn(
        source_table,
        experiment_id=experiment_id,
        role=role,
        project=project,
        name_suffix=(
            window.name_part() if window is not None else "all",
            format_pin_stamp(pinned_at),
        ),
    )


def _label_value(value: str) -> str:
    """``value`` as a valid BigQuery label value: lowercase letters, digits,
    ``_`` and ``-``, at most 63 characters."""
    return re.sub(r"[^a-z0-9_-]", "_", value.lower())[:63]


def _sql_string_literal(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n")
    return f"'{escaped}'"


def _pin_ddl(
    *,
    source_table: str,
    dest_table: str,
    predicate: str,
    labels: Sequence[tuple[str, str]],
    description: str,
    expiration: datetime,
) -> str:
    """``CREATE TABLE ... LIKE ... AS SELECT`` for a pinned copy.

    ``LIKE`` copies the source's partitioning, clustering and options. The
    options that would mislead on a copy are overridden: ``labels`` and
    ``description`` are replaced, not merged, and ``partition_expiration_days``
    is cleared, since a source keeping e.g. 365 days of partitions would
    otherwise drop an older window's partitions as they land.
    """
    label_list = ", ".join(
        f"({_sql_string_literal(k)}, {_sql_string_literal(v)})" for k, v in labels
    )
    return (
        f"CREATE TABLE `{dest_table}` LIKE `{source_table}` "
        f"OPTIONS("
        f"expiration_timestamp=TIMESTAMP('{expiration.isoformat()}'), "
        f"partition_expiration_days=NULL, "
        f"labels=[{label_list}], "
        f"description={_sql_string_literal(description)}) "
        f"AS SELECT * FROM `{source_table}` WHERE {predicate}"
    )


def pin_source_into_experiment(
    source_table: str,
    *,
    experiment_id: str,
    role: str,
    pinned_at: datetime,
    window: SourceWindow | None = None,
    expiration_days: int = 7,
    max_bytes: int = DEFAULT_PIN_MAX_BYTES,
    dry_run: bool = False,
    project: str = DEFAULT_PROJECT,
) -> str:
    """Copy ``source_table`` (or its ``window``) into
    ``<project>.tech_great_expectations`` and return the copy's FQN
    (``pinned_source_fqn``).

    The copy is what a run reads instead of the moving source, so runs that
    share it see identical inputs. Its data is the source as of the start
    of the copy job; ``pinned_at`` only names the copy and groups the copies
    of one run, so callers pass the same ``pinned_at`` to every source they
    pin. Copies of several sources start seconds apart.

    The copy keeps the source's partitioning and clustering, and carries:

    * ``expiration_timestamp`` = now + ``expiration_days``;
    * labels ``dit_artifact=source_pin``, ``dit_experiment_id``,
      ``dit_role``, ``dit_pin`` (the pin stamp) and ``dit_source_table``;
    * a description naming the source FQN, the filter, the pin and the copy
      job id (chosen before submission so the description can name it).

    Without a window the whole table is copied (``WHERE TRUE``). BigQuery
    rejects that for sources that require a partition filter, which forces a
    window exactly where a full copy would be expensive.

    Cost guard: the copy statement is dry-run first and refused with
    ``ValueError`` if it would process more than ``max_bytes``; the job
    also carries ``max_bytes`` as ``maximum_bytes_billed``. ``dry_run=True``
    stops after the check and creates nothing.

    There is no ``IF NOT EXISTS``: the pin stamp makes every run's names
    new, so an existing table means a reused stamp and the create fails.
    To reuse an earlier run's copies, recompute their names with
    ``pinned_source_fqn`` and check them with ``require_pinned_source``.
    """
    dest_table = pinned_source_fqn(
        source_table,
        experiment_id=experiment_id,
        role=role,
        pinned_at=pinned_at,
        window=window,
        project=project,
    )
    expiration = _utc_now() + timedelta(days=expiration_days)
    job_id = f"dit_pin_{uuid.uuid4().hex}"
    predicate = window.predicate() if window is not None else "TRUE"
    labels = (
        ("dit_artifact", PIN_ARTIFACT_LABEL),
        ("dit_experiment_id", _label_value(experiment_id)),
        ("dit_role", _label_value(role)),
        ("dit_pin", format_pin_stamp(pinned_at)),
        ("dit_source_table", _label_value(source_table.rsplit(".", 1)[-1])),
    )
    description = "\n".join([
        "DIT pinned copy of a source table (data integration tests).",
        f"Source: {source_table}",
        f"Rows: WHERE {predicate}" if window is not None else "Rows: all (no filter)",
        f"Pinned at: {pinned_at.isoformat()}; the data is as of the start of "
        f"copy job {project}:{job_id}.",
        f"Experiment: {experiment_id}; role: {role}",
        f"Expires: {expiration.isoformat()}",
    ])
    sql = _pin_ddl(
        source_table=source_table,
        dest_table=dest_table,
        predicate=predicate,
        labels=labels,
        description=description,
        expiration=expiration,
    )

    client = bigquery.Client(project=project)
    estimate = int(
        client.query(
            sql,
            job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False),
        ).total_bytes_processed
        or 0
    )
    logger.info(
        "pinning %s -> %s (%s): dry run estimates %.1f GB (cap %.1f GB)",
        source_table, dest_table, predicate, estimate / 1e9, max_bytes / 1e9,
    )
    if estimate > max_bytes:
        raise ValueError(
            f"pinning {source_table} would process {estimate / 1e9:.1f} GB, above "
            f"the {max_bytes / 1e9:.1f} GB cap. Narrow the window, or raise the cap "
            f"deliberately."
        )
    if dry_run:
        logger.info("dry run: %s not created", dest_table)
        return dest_table

    # job_retry=None: the description names job_id, and BigQuery cannot
    # retry a job under the same id.
    client.query(
        sql,
        job_id=job_id,
        job_retry=None,
        job_config=bigquery.QueryJobConfig(maximum_bytes_billed=max_bytes),
    ).result()
    return dest_table


def require_pinned_source(dest_table: str, *, project: str = DEFAULT_PROJECT) -> str:
    """Return ``dest_table`` if it exists and carries the
    ``dit_artifact=source_pin`` (copy) or ``source_pin_view`` (time-travel
    view) label; raise ``ValueError`` otherwise.

    For reusing an earlier run's copies: the caller recomputes their names
    with ``pinned_source_fqn`` and checks each one here before any run reads
    it, so a missing or foreign table fails before any Dataflow job starts.
    """
    from google.cloud.exceptions import NotFound

    client = bigquery.Client(project=project)
    try:
        table = client.get_table(dest_table)
    except NotFound as exc:
        raise ValueError(
            f"pinned copy {dest_table} does not exist: it expired, or the pin "
            f"stamp, window, role or experiment id differ from the run that "
            f"created it"
        ) from exc
    artifact = (table.labels or {}).get("dit_artifact")
    if artifact not in (PIN_ARTIFACT_LABEL, PIN_VIEW_ARTIFACT_LABEL):
        raise ValueError(
            f"{dest_table} exists but is not a DIT source pin "
            f"(label dit_artifact={artifact!r})"
        )
    return dest_table


# --------------------------------------------------------------------------
# Source pinning by time-travel view
# --------------------------------------------------------------------------
#
# A copy pins a source as of "now". A production-baseline test (PIPELINE-4703)
# must pin a source as of a moment in the PAST: the moment production read its
# inputs, and the moment it finished writing its output. A view over
# `SELECT * FROM src FOR SYSTEM_TIME AS OF <t>` does that without copying
# anything and works across organisations. It stays readable only while <t>
# is inside the source dataset's time-travel window (48 h to 168 h at GFW),
# after which queries through it fail. Readers need access to the source
# tables, since these are not authorized views.


def table_last_modified(table_fqn: str, *, project: str = DEFAULT_PROJECT) -> datetime:
    """``lastModifiedTime`` of ``table_fqn``, timezone-aware."""
    return bigquery.Client(project=project).get_table(table_fqn).modified


def check_time_travel(
    source_table: str,
    *,
    as_of: datetime,
    margin_hours: float,
    project: str = DEFAULT_PROJECT,
) -> datetime:
    """Return when ``as_of`` leaves ``source_table``'s time-travel window,
    and raise ``ValueError`` if that is less than ``margin_hours`` (the
    expected run duration) away, so a run cannot outlive the pins it reads.

    The window is the source dataset's ``max_time_travel_hours``
    (``DEFAULT_TIME_TRAVEL_HOURS`` when unset).
    """
    if as_of.tzinfo is None:
        raise ValueError(f"as_of must be timezone-aware, got {as_of!r}")
    now = _utc_now()
    if as_of > now:
        raise ValueError(f"as_of {as_of.isoformat()} is in the future")
    dataset = ".".join(source_table.split(".")[:2])
    hours = bigquery.Client(project=project).get_dataset(dataset).max_time_travel_hours
    window = timedelta(hours=int(hours) if hours else DEFAULT_TIME_TRAVEL_HOURS)
    deadline = as_of + window
    if now + timedelta(hours=margin_hours) >= deadline:
        raise ValueError(
            f"{source_table} as of {as_of.isoformat()} leaves time travel at "
            f"{deadline.isoformat()} ({dataset} keeps {window}), less than the "
            f"{margin_hours} h this run needs. Pin a newer production run, or copy "
            f"the source manually while it is still readable."
        )
    return deadline


def pin_view_into_experiment(
    source_table: str,
    *,
    experiment_id: str,
    role: str,
    as_of: datetime,
    window: SourceWindow | None = None,
    expiration_days: int = 7,
    max_bytes: int = DEFAULT_PIN_MAX_BYTES,
    dry_run: bool = False,
    project: str = DEFAULT_PROJECT,
) -> str:
    """Create a view of ``source_table`` (or its ``window``) as of ``as_of``
    in ``<project>.tech_great_expectations`` and return its FQN
    (``pinned_source_fqn``, stamped with ``as_of``).

    The DDL is ``CREATE VIEW IF NOT EXISTS <fqn> OPTIONS(...) AS SELECT *
    FROM <src> FOR SYSTEM_TIME AS OF TIMESTAMP(<as_of>) WHERE <window or
    TRUE>``. ``IF NOT EXISTS`` is safe here, unlike for copies: source,
    ``as_of`` and window are all in the name, so a view of that name always
    has the same definition and reruns are idempotent.

    Labels match the copies' (``dit_artifact=source_pin_view``); the
    description names the source, the filter and ``as_of``.

    Cost guard: the full read through the view is dry-run and refused with
    ``ValueError`` above ``max_bytes``, so reading a large source in full is
    a deliberate choice of cap. ``dry_run=True`` stops after the check and
    creates nothing. Callers check ``check_time_travel`` first.

    ``as_of`` must be whole seconds, the precision of the stamp in the name.
    """
    if as_of.microsecond:
        raise ValueError(
            f"as_of must be whole seconds (the pin stamp's precision), got {as_of.isoformat()}"
        )
    dest_table = pinned_source_fqn(
        source_table,
        experiment_id=experiment_id,
        role=role,
        pinned_at=as_of,
        window=window,
        project=project,
    )
    expiration = _utc_now() + timedelta(days=expiration_days)
    predicate = window.predicate() if window is not None else "TRUE"
    as_of_utc = as_of.astimezone(timezone.utc).isoformat()
    select = (
        f"SELECT * FROM `{source_table}` FOR SYSTEM_TIME AS OF TIMESTAMP('{as_of_utc}') "
        f"WHERE {predicate}"
    )
    labels = (
        ("dit_artifact", PIN_VIEW_ARTIFACT_LABEL),
        ("dit_experiment_id", _label_value(experiment_id)),
        ("dit_role", _label_value(role)),
        ("dit_pin", format_pin_stamp(as_of)),
        ("dit_source_table", _label_value(source_table.rsplit(".", 1)[-1])),
    )
    label_list = ", ".join(
        f"({_sql_string_literal(k)}, {_sql_string_literal(v)})" for k, v in labels
    )
    description = "\n".join([
        "DIT pinned time-travel view of a source table (data integration tests).",
        f"Source: {source_table}",
        f"Rows: WHERE {predicate}" if window is not None else "Rows: all (no filter)",
        f"As of: {as_of_utc}",
        f"Experiment: {experiment_id}; role: {role}",
        f"Expires: {expiration.isoformat()} (unreadable earlier once {as_of_utc} "
        f"leaves the source dataset's time-travel window)",
    ])
    sql = (
        f"CREATE VIEW IF NOT EXISTS `{dest_table}` "
        f"OPTIONS("
        f"expiration_timestamp=TIMESTAMP('{expiration.isoformat()}'), "
        f"labels=[{label_list}], "
        f"description={_sql_string_literal(description)}) "
        f"AS {select}"
    )

    client = bigquery.Client(project=project)
    estimate = int(
        client.query(
            select,
            job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False),
        ).total_bytes_processed
        or 0
    )
    logger.info(
        "pinning view %s -> %s (as of %s, %s): a full read is estimated at "
        "%.1f GB (cap %.1f GB)",
        source_table, dest_table, as_of_utc, predicate, estimate / 1e9, max_bytes / 1e9,
    )
    if estimate > max_bytes:
        raise ValueError(
            f"reading {source_table} as of {as_of_utc} in full would process "
            f"{estimate / 1e9:.1f} GB, above the {max_bytes / 1e9:.1f} GB cap. "
            f"Raise the cap deliberately if the run needs the full read."
        )
    if dry_run:
        logger.info("dry run: view %s not created", dest_table)
        return dest_table
    client.query(sql).result()
    return dest_table
