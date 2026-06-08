"""Tests for ``dit.bq`` snapshot helpers.

Mock-based; no real BQ traffic. Both helpers shell out to
``bigquery.Client(...).query(sql).result()`` (with ``snapshot_dataset``
adding ``get_dataset`` + ``list_tables`` on top), so the tests assert on
the DDL string passed to ``client.query`` and on which tables the loop
visited.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from dit.bq import snapshot_dataset, snapshot_into_experiment, snapshot_table


def _make_client_mock() -> MagicMock:
    client = MagicMock()
    client.query.return_value.result.return_value = None
    return client


def _captured_sql(client: MagicMock) -> str:
    assert client.query.call_count == 1, (
        f"expected exactly one query call, got {client.query.call_count}"
    )
    return client.query.call_args.args[0]


# -- snapshot_table ---------------------------------------------------------


def test_snapshot_table_plain_emits_clone_only() -> None:
    client = _make_client_mock()
    with patch("google.cloud.bigquery.Client", return_value=client) as ctor:
        snapshot_table(
            "world-fishing-827.src.tbl",
            "world-fishing-827.dst.tbl",
        )
    ctor.assert_called_once_with(project="world-fishing-827")
    sql = _captured_sql(client)
    assert "CREATE SNAPSHOT TABLE" in sql
    assert "IF NOT EXISTS" not in sql
    assert "`world-fishing-827.dst.tbl`" in sql
    assert "CLONE `world-fishing-827.src.tbl`" in sql
    assert "FOR SYSTEM_TIME AS OF" not in sql
    assert "OPTIONS(" not in sql


def test_snapshot_table_with_as_of() -> None:
    client = _make_client_mock()
    ts = datetime(2026, 5, 14, 12, 0, 0)
    with patch("google.cloud.bigquery.Client", return_value=client):
        snapshot_table(
            "p.src.t",
            "p.dst.t",
            as_of=ts,
        )
    sql = _captured_sql(client)
    assert f'FOR SYSTEM_TIME AS OF TIMESTAMP("{ts.isoformat()}")' in sql
    assert "OPTIONS(" not in sql


def test_snapshot_table_with_expiration() -> None:
    client = _make_client_mock()
    exp = datetime(2026, 6, 1, 0, 0, 0)
    with patch("google.cloud.bigquery.Client", return_value=client):
        snapshot_table(
            "p.src.t",
            "p.dst.t",
            expiration=exp,
        )
    sql = _captured_sql(client)
    assert f'OPTIONS(expiration_timestamp=TIMESTAMP("{exp.isoformat()}"))' in sql
    assert "FOR SYSTEM_TIME AS OF" not in sql


def test_snapshot_table_if_not_exists() -> None:
    client = _make_client_mock()
    with patch("google.cloud.bigquery.Client", return_value=client):
        snapshot_table(
            "p.src.t",
            "p.dst.t",
            if_not_exists=True,
        )
    sql = _captured_sql(client)
    assert "CREATE SNAPSHOT TABLE IF NOT EXISTS `p.dst.t`" in sql


def test_snapshot_table_as_of_and_expiration_both_present() -> None:
    client = _make_client_mock()
    as_of = datetime(2026, 5, 10)
    exp = datetime(2026, 7, 1)
    with patch("google.cloud.bigquery.Client", return_value=client):
        snapshot_table(
            "p.src.t",
            "p.dst.t",
            as_of=as_of,
            expiration=exp,
        )
    sql = _captured_sql(client)
    assert f'FOR SYSTEM_TIME AS OF TIMESTAMP("{as_of.isoformat()}")' in sql
    assert f'OPTIONS(expiration_timestamp=TIMESTAMP("{exp.isoformat()}"))' in sql
    # AS OF must precede OPTIONS in DDL grammar.
    assert sql.index("FOR SYSTEM_TIME") < sql.index("OPTIONS(")


def test_snapshot_table_custom_project() -> None:
    client = _make_client_mock()
    with patch("google.cloud.bigquery.Client", return_value=client) as ctor:
        snapshot_table("p.src.t", "p.dst.t", project="other-project")
    ctor.assert_called_once_with(project="other-project")


# -- snapshot_dataset -------------------------------------------------------


def _table_ref(table_id: str) -> SimpleNamespace:
    return SimpleNamespace(table_id=table_id)


def test_snapshot_dataset_lists_and_snapshots_each() -> None:
    client = MagicMock()
    client.get_dataset.return_value = MagicMock()

    def list_tables(dataset: str):
        if dataset == "p.src":
            return iter([_table_ref("a"), _table_ref("b"), _table_ref("c")])
        if dataset == "p.dst":
            return iter([])
        raise AssertionError(f"unexpected dataset: {dataset}")

    client.list_tables.side_effect = list_tables
    client.query.return_value.result.return_value = None

    with patch("google.cloud.bigquery.Client", return_value=client):
        created = snapshot_dataset("p.src", "p.dst")

    assert created == ["p.dst.a", "p.dst.b", "p.dst.c"]
    assert client.query.call_count == 3
    sqls = [call.args[0] for call in client.query.call_args_list]
    assert any("CLONE `p.src.a`" in s and "`p.dst.a`" in s for s in sqls)
    assert any("CLONE `p.src.b`" in s and "`p.dst.b`" in s for s in sqls)
    assert any("CLONE `p.src.c`" in s and "`p.dst.c`" in s for s in sqls)


def test_snapshot_dataset_filters_to_requested_tables() -> None:
    client = MagicMock()
    client.get_dataset.return_value = MagicMock()

    def list_tables(dataset: str):
        if dataset == "p.src":
            return iter([_table_ref("a"), _table_ref("b"), _table_ref("c")])
        if dataset == "p.dst":
            return iter([])
        raise AssertionError(f"unexpected dataset: {dataset}")

    client.list_tables.side_effect = list_tables
    client.query.return_value.result.return_value = None

    with patch("google.cloud.bigquery.Client", return_value=client):
        created = snapshot_dataset("p.src", "p.dst", tables=["a", "c"])

    assert created == ["p.dst.a", "p.dst.c"]
    assert client.query.call_count == 2


def test_snapshot_dataset_skips_existing_tables() -> None:
    client = MagicMock()
    client.get_dataset.return_value = MagicMock()

    def list_tables(dataset: str):
        if dataset == "p.src":
            return iter([_table_ref("a"), _table_ref("b")])
        if dataset == "p.dst":
            return iter([_table_ref("a")])
        raise AssertionError(f"unexpected dataset: {dataset}")

    client.list_tables.side_effect = list_tables
    client.query.return_value.result.return_value = None

    with patch("google.cloud.bigquery.Client", return_value=client):
        created = snapshot_dataset("p.src", "p.dst")

    assert created == ["p.dst.b"]
    assert client.query.call_count == 1
    sql = client.query.call_args.args[0]
    assert "CLONE `p.src.b`" in sql


def test_snapshot_dataset_raises_when_dest_missing() -> None:
    from google.cloud.exceptions import NotFound

    client = MagicMock()
    client.get_dataset.side_effect = NotFound("nope")

    with patch("google.cloud.bigquery.Client", return_value=client):
        with pytest.raises(ValueError, match="destination dataset does not exist"):
            snapshot_dataset("p.src", "p.dst")

    client.list_tables.assert_not_called()
    client.query.assert_not_called()


def test_snapshot_dataset_forwards_as_of_and_expiration() -> None:
    client = MagicMock()
    client.get_dataset.return_value = MagicMock()

    def list_tables(dataset: str):
        if dataset == "p.src":
            return iter([_table_ref("a")])
        if dataset == "p.dst":
            return iter([])
        raise AssertionError(f"unexpected dataset: {dataset}")

    client.list_tables.side_effect = list_tables
    client.query.return_value.result.return_value = None

    as_of = datetime(2026, 5, 10)
    exp = datetime(2026, 7, 1)

    with patch("google.cloud.bigquery.Client", return_value=client):
        snapshot_dataset("p.src", "p.dst", as_of=as_of, expiration=exp)

    sql = client.query.call_args.args[0]
    assert f'FOR SYSTEM_TIME AS OF TIMESTAMP("{as_of.isoformat()}")' in sql
    assert f'OPTIONS(expiration_timestamp=TIMESTAMP("{exp.isoformat()}"))' in sql


# -- snapshot_into_experiment ----------------------------------------------
#
# The helper composes:
#   * a dest FQN under <project>.tech_great_expectations using a deterministic
#     naming convention (sanitised experiment_id + role + source table name);
#   * an expiration_timestamp computed from _utc_now() + expiration_days;
#   * a delegation to snapshot_table with if_not_exists=(if_existing=="skip").
# Tests pin all four mechanical pieces; the underlying snapshot_table DDL
# emission is exercised by the tests above.


_FIXED_NOW = datetime(2026, 6, 8, 12, 0, 0, tzinfo=timezone.utc)


def test_snapshot_into_experiment_default_dest_and_expiration() -> None:
    client = _make_client_mock()
    with (
        patch("google.cloud.bigquery.Client", return_value=client),
        patch("dit.bq._utc_now", return_value=_FIXED_NOW),
    ):
        dest = snapshot_into_experiment(
            "world-fishing-827.src_ds.tbl",
            experiment_id="exp1",
            role="cross_version",
        )
    expected_dest = (
        "world-fishing-827.tech_great_expectations."
        "dit_exp_exp1_cross_version_tbl"
    )
    assert dest == expected_dest

    sql = _captured_sql(client)
    assert "CREATE SNAPSHOT TABLE IF NOT EXISTS" in sql, sql  # default if_existing="skip"
    assert f"`{expected_dest}` CLONE `world-fishing-827.src_ds.tbl`" in sql

    # Default 7-day expiration -> 2026-06-15T12:00:00+00:00
    expected_exp_ts = '2026-06-15T12:00:00+00:00'
    assert f'OPTIONS(expiration_timestamp=TIMESTAMP("{expected_exp_ts}"))' in sql


def test_snapshot_into_experiment_sanitises_hyphenated_experiment_id() -> None:
    client = _make_client_mock()
    with (
        patch("google.cloud.bigquery.Client", return_value=client),
        patch("dit.bq._utc_now", return_value=_FIXED_NOW),
    ):
        dest = snapshot_into_experiment(
            "world-fishing-827.src_ds.tbl",
            experiment_id="pipeline-1465",
            role="cross_version",
        )
    assert "dit_exp_pipeline_1465_cross_version_tbl" in dest
    assert "pipeline-1465" not in dest  # hyphen must be sanitised


def test_snapshot_into_experiment_sanitises_hyphenated_role() -> None:
    """``role`` is sanitised the same way as ``experiment_id`` so a
    caller-supplied label containing hyphens doesn't produce a dest
    table id that needs special quoting."""
    client = _make_client_mock()
    with (
        patch("google.cloud.bigquery.Client", return_value=client),
        patch("dit.bq._utc_now", return_value=_FIXED_NOW),
    ):
        dest = snapshot_into_experiment(
            "world-fishing-827.src_ds.tbl",
            experiment_id="exp1",
            role="cross-version",  # hyphenated
        )
    assert "dit_exp_exp1_cross_version_tbl" in dest
    assert "cross-version" not in dest  # hyphen must be sanitised


def test_snapshot_into_experiment_strips_source_fqn_to_table_name() -> None:
    """source_table_name is the last `.`-separated component."""
    client = _make_client_mock()
    with (
        patch("google.cloud.bigquery.Client", return_value=client),
        patch("dit.bq._utc_now", return_value=_FIXED_NOW),
    ):
        dest = snapshot_into_experiment(
            "some-project.some_ds.research_messages",
            experiment_id="exp1",
            role="outage_pre",
        )
    assert dest.endswith(".dit_exp_exp1_outage_pre_research_messages")


def test_snapshot_into_experiment_threads_as_of() -> None:
    client = _make_client_mock()
    as_of = datetime(2026, 5, 14, 12, 0, 0, tzinfo=timezone.utc)
    with (
        patch("google.cloud.bigquery.Client", return_value=client),
        patch("dit.bq._utc_now", return_value=_FIXED_NOW),
    ):
        snapshot_into_experiment(
            "world-fishing-827.src_ds.tbl",
            experiment_id="exp1",
            role="cross_version",
            as_of=as_of,
        )
    sql = _captured_sql(client)
    assert f'FOR SYSTEM_TIME AS OF TIMESTAMP("{as_of.isoformat()}")' in sql


def test_snapshot_into_experiment_if_existing_fail_drops_if_not_exists() -> None:
    client = _make_client_mock()
    with (
        patch("google.cloud.bigquery.Client", return_value=client),
        patch("dit.bq._utc_now", return_value=_FIXED_NOW),
    ):
        snapshot_into_experiment(
            "world-fishing-827.src_ds.tbl",
            experiment_id="exp1",
            role="cross_version",
            if_existing="fail",
        )
    sql = _captured_sql(client)
    assert sql.startswith("CREATE SNAPSHOT TABLE `"), sql  # no IF NOT EXISTS
    assert "IF NOT EXISTS" not in sql


def test_snapshot_into_experiment_if_existing_skip_includes_if_not_exists() -> None:
    """Explicit ``if_existing="skip"`` matches the default."""
    client = _make_client_mock()
    with (
        patch("google.cloud.bigquery.Client", return_value=client),
        patch("dit.bq._utc_now", return_value=_FIXED_NOW),
    ):
        snapshot_into_experiment(
            "world-fishing-827.src_ds.tbl",
            experiment_id="exp1",
            role="cross_version",
            if_existing="skip",
        )
    sql = _captured_sql(client)
    assert "CREATE SNAPSHOT TABLE IF NOT EXISTS" in sql


def test_snapshot_into_experiment_custom_expiration_days() -> None:
    client = _make_client_mock()
    with (
        patch("google.cloud.bigquery.Client", return_value=client),
        patch("dit.bq._utc_now", return_value=_FIXED_NOW),
    ):
        snapshot_into_experiment(
            "world-fishing-827.src_ds.tbl",
            experiment_id="exp1",
            role="cross_version",
            expiration_days=30,
        )
    sql = _captured_sql(client)
    # 2026-06-08T12:00:00Z + 30d = 2026-07-08T12:00:00+00:00
    assert 'OPTIONS(expiration_timestamp=TIMESTAMP("2026-07-08T12:00:00+00:00"))' in sql


def test_snapshot_into_experiment_custom_project_threads_through() -> None:
    """``project`` controls both the BQ client target AND the dest FQN."""
    client = _make_client_mock()
    with (
        patch("google.cloud.bigquery.Client", return_value=client) as ctor,
        patch("dit.bq._utc_now", return_value=_FIXED_NOW),
    ):
        dest = snapshot_into_experiment(
            "gfw-int-vms-v3.pipe_vms_v3_internal.research_messages",
            experiment_id="exp1",
            role="outage_pre",
            project="gfw-int-vms-v3",  # cross-org dodge path
        )
    ctor.assert_called_once_with(project="gfw-int-vms-v3")
    assert dest.startswith("gfw-int-vms-v3.tech_great_expectations.")
    sql = _captured_sql(client)
    assert "`gfw-int-vms-v3.tech_great_expectations.dit_exp_exp1_outage_pre_research_messages`" in sql
