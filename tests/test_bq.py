"""Tests for ``dit.bq`` snapshot helpers.

Mock-based; no real BQ traffic. Both helpers shell out to
``bigquery.Client(...).query(sql).result()`` (with ``snapshot_dataset``
adding ``get_dataset`` + ``list_tables`` on top), so the tests assert on
the DDL string passed to ``client.query`` and on which tables the loop
visited.
"""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from dit.bq import snapshot_dataset, snapshot_table


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
