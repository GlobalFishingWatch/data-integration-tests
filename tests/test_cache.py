"""Tests for the pure (non-BQ-touching) parts of `dit.cache`.

BQ-touching tests (read_cache / verify_tables_exist / write_cache /
expires_at_for / cancel_run) live in a separate file once those land —
they need either a BQ emulator or mocked client and aren't worth that
plumbing for the scaffold milestone.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from dit.cache import (
    CacheKey,
    CachedRun,
    canonicalise_params,
    compute_cache_key,
    expires_at_for,
    read_cache,
    resolve_worker_image_to_digest,
    sha1_of_workflow_file,
    verify_tables_exist,
    write_cache,
)


# --------------------------------------------------------------------------
# compute_cache_key
# --------------------------------------------------------------------------

def _key(
    pipeline_commit: str = "abc123",
    worker_image_digest: str = "gcr.io/foo/bar@sha256:0011",
    workflow_file_sha1: str = "def456",
    params: dict[str, Any] | None = None,
) -> CacheKey:
    return CacheKey(
        pipeline_commit=pipeline_commit,
        worker_image_digest=worker_image_digest,
        workflow_file_sha1=workflow_file_sha1,
        params=params if params is not None else {"start": "2020-01-01", "end": "2020-12-31"},
    )


def test_compute_cache_key_is_deterministic():
    assert compute_cache_key(_key()) == compute_cache_key(_key())


def test_compute_cache_key_independent_of_params_dict_ordering():
    a = _key(params={"start": "2020-01-01", "end": "2020-12-31"})
    b = _key(params={"end": "2020-12-31", "start": "2020-01-01"})
    assert compute_cache_key(a) == compute_cache_key(b)


def test_compute_cache_key_changes_when_pipeline_commit_changes():
    assert compute_cache_key(_key(pipeline_commit="abc123")) != \
           compute_cache_key(_key(pipeline_commit="abc124"))


def test_compute_cache_key_changes_when_worker_image_digest_changes():
    assert compute_cache_key(_key(worker_image_digest="gcr.io/foo/bar@sha256:0011")) != \
           compute_cache_key(_key(worker_image_digest="gcr.io/foo/bar@sha256:0022"))


def test_compute_cache_key_changes_when_workflow_file_sha1_changes():
    # The dit-side cache buster: editing the workflow file must invalidate.
    assert compute_cache_key(_key(workflow_file_sha1="def456")) != \
           compute_cache_key(_key(workflow_file_sha1="def457"))


def test_compute_cache_key_changes_when_any_param_changes():
    assert compute_cache_key(_key(params={"start": "2020-01-01"})) != \
           compute_cache_key(_key(params={"start": "2020-01-02"}))


def test_compute_cache_key_is_sha256_hex():
    h = compute_cache_key(_key())
    assert len(h) == 64
    assert all(c in "0123456789abcdef" for c in h)


# --------------------------------------------------------------------------
# canonicalise_params
# --------------------------------------------------------------------------

def test_canonicalise_params_sorts_keys():
    assert list(canonicalise_params({"b": 1, "a": 2}).keys()) == ["a", "b"]


def test_canonicalise_params_sorts_list_values():
    # ssvids-shaped: order doesn't affect output, so we sort to stabilise
    # the cache key.
    assert canonicalise_params({"ssvids": ["999", "111", "222"]})["ssvids"] == \
           ["111", "222", "999"]


def test_canonicalise_params_preserves_tuple_order():
    # Tuples signal "order matters" -- preserve it.
    assert canonicalise_params({"modes": ("1_bf", "2_bfd")})["modes"] == ["1_bf", "2_bfd"]


def test_canonicalise_params_passes_scalars_through():
    p = canonicalise_params({"start": "2020-01-01", "tail_days": 4, "good": True, "x": None})
    assert p == {"good": True, "start": "2020-01-01", "tail_days": 4, "x": None}


# --------------------------------------------------------------------------
# sha1_of_workflow_file
# --------------------------------------------------------------------------

def test_sha1_of_workflow_file(tmp_path: Path):
    f = tmp_path / "wf.py"
    f.write_text("print('hello')\n")
    h = sha1_of_workflow_file(f)
    assert len(h) == 40
    # Same content -> same hash.
    assert sha1_of_workflow_file(f) == h


def test_sha1_of_workflow_file_changes_on_edit(tmp_path: Path):
    f = tmp_path / "wf.py"
    f.write_text("a = 1\n")
    h1 = sha1_of_workflow_file(f)
    f.write_text("a = 2\n")
    assert sha1_of_workflow_file(f) != h1


def test_sha1_changes_for_docstring_edit(tmp_path: Path):
    # Documented behaviour: a docstring edit DOES bust the cache (we treat
    # all workflow-file bytes as significant). Tolerable; refinement via a
    # BEHAVIOUR_VERSION constant is deferred.
    f = tmp_path / "wf.py"
    f.write_text('"""docstring v1."""\n\ndef main(): pass\n')
    h1 = sha1_of_workflow_file(f)
    f.write_text('"""docstring v2."""\n\ndef main(): pass\n')
    assert sha1_of_workflow_file(f) != h1


# --------------------------------------------------------------------------
# resolve_worker_image_to_digest
# --------------------------------------------------------------------------

def test_resolve_worker_image_to_digest_passes_digest_form_through():
    digest_form = "gcr.io/foo/bar@sha256:0011aabbccddeeff"
    # No network call -- direct return.
    assert resolve_worker_image_to_digest(digest_form) == digest_form


# Tag-form resolution would require a real gcloud call; no test for it
# here. Manual verification at M2 / first real workflow integration:
#   resolve_worker_image_to_digest("us-central1-docker.pkg.dev/.../pipe-gaps:v0.9.6")
# should return the same image ref with @sha256:... appended.


# --------------------------------------------------------------------------
# BQ-touching paths (M2): read_cache / verify_tables_exist /
# expires_at_for / CachedRun.from_bq_row. Tests use a mocked client.
# --------------------------------------------------------------------------

def _bq_row(**overrides: Any) -> Any:
    """SimpleNamespace stand-in for a `google.cloud.bigquery.Row`."""
    base = dict(
        run_id="rid01",
        cache_key="key01",
        workflow="workflows/pipe_gaps/mode_equivalence.py",
        pipeline="pipe-gaps",
        experiment_id="exp01",
        pipeline_commit="abc1234",
        pipeline_dirty=False,
        unreviewed_code=False,
        pipeline_commit_parent=None,
        dit_commit="def5678",
        workflow_file_sha1="aa" * 20,
        worker_image="gcr.io/foo/pipe-gaps@sha256:0011",
        params_json={"start": "2020-01-01"},
        output_tables=["world-fishing-827.tech_great_expectations.bf_table"],
        dataflow_job_ids=["2026-05-22_00_00_00-1"],
        cloud_build_id="build-01",
        started_at=datetime(2026, 5, 22, tzinfo=timezone.utc),
        finished_at=datetime(2026, 5, 22, 0, 30, tzinfo=timezone.utc),
        status="succeeded",
        expires_at=datetime(2026, 9, 22, tzinfo=timezone.utc),
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _query_returns(rows: list[Any]) -> MagicMock:
    """Build a mock client whose `.query(...).result()` returns `rows`."""
    client = MagicMock()
    job = MagicMock()
    job.result.return_value = iter(rows)
    client.query.return_value = job
    return client


def test_from_bq_row_round_trip():
    row = _bq_row()
    cached = CachedRun.from_bq_row(row)
    assert cached.run_id == row.run_id
    assert cached.params == {"start": "2020-01-01"}  # JSON column -> dict
    assert cached.output_tables == row.output_tables
    assert cached.dataflow_job_ids == row.dataflow_job_ids
    assert cached.started_at == row.started_at
    assert cached.expires_at == row.expires_at


def test_from_bq_row_coerces_none_arrays():
    # ARRAY columns can come back as None in some BQ client paths; we
    # normalise to empty list so callers don't have to None-guard.
    row = _bq_row(output_tables=None, dataflow_job_ids=None)
    cached = CachedRun.from_bq_row(row)
    assert cached.output_tables == []
    assert cached.dataflow_job_ids == []


def test_read_cache_returns_cached_run_on_match():
    row = _bq_row(cache_key="match")
    client = _query_returns([row])
    result = read_cache("match", client=client)
    assert result is not None
    assert result.cache_key == "match"


def test_read_cache_returns_none_on_empty():
    client = _query_returns([])
    assert read_cache("nonexistent", client=client) is None


def test_read_cache_sql_filters_match_design():
    client = _query_returns([])
    read_cache("k", client=client)
    sql = client.query.call_args[0][0]
    assert "cache_key = @cache_key" in sql
    assert "status = 'succeeded'" in sql
    assert "expires_at > CURRENT_TIMESTAMP()" in sql
    assert "ORDER BY started_at DESC" in sql
    assert "LIMIT 1" in sql
    # M-pivot-3: the unreviewed/dirty filter is DROPPED so content-addressable
    # snapshot rows are cacheable. Neither the legacy nor the new column gates
    # lookups.
    assert "pipeline_dirty" not in sql
    assert "unreviewed_code" not in sql


def test_verify_tables_exist_empty_input():
    # No BQ call should happen; passing client=None is fine because we
    # short-circuit on empty input.
    assert verify_tables_exist([]) == []


def test_verify_tables_exist_preserves_order_and_marks_missing():
    # Two tables in the same dataset; only one exists per the mock.
    fqns = [
        "world-fishing-827.tech_great_expectations.dit_runs",
        "world-fishing-827.tech_great_expectations.missing_table",
    ]
    found_rows = [SimpleNamespace(table_name="dit_runs")]
    client = _query_returns(found_rows)
    result = verify_tables_exist(fqns, client=client)
    assert result == [True, False]


def test_verify_tables_exist_groups_by_dataset():
    # Tables across two datasets -> exactly two queries.
    fqns = [
        "world-fishing-827.ds_a.t1",
        "world-fishing-827.ds_b.t2",
    ]
    # Each query call returns its own iterator; chain side_effects.
    client = MagicMock()
    client.query.side_effect = [
        MagicMock(result=lambda: iter([SimpleNamespace(table_name="t1")])),
        MagicMock(result=lambda: iter([SimpleNamespace(table_name="t2")])),
    ]
    result = verify_tables_exist(fqns, client=client)
    assert result == [True, True]
    assert client.query.call_count == 2


def test_verify_tables_exist_rejects_non_fqn():
    with pytest.raises(ValueError, match="fully-qualified"):
        verify_tables_exist(["just_a_table_name"], client=MagicMock())


def test_expires_at_for_returns_min():
    a = datetime(2026, 6, 1, tzinfo=timezone.utc)
    b = datetime(2026, 7, 1, tzinfo=timezone.utc)
    client = MagicMock()
    client.get_table.side_effect = [
        SimpleNamespace(expires=a),
        SimpleNamespace(expires=b),
    ]
    result = expires_at_for(
        ["proj.ds.a", "proj.ds.b"], client=client,
    )
    assert result == a  # min


def test_expires_at_for_falls_back_when_no_expirations():
    client = MagicMock()
    client.get_table.return_value = SimpleNamespace(expires=None)
    before = datetime.now(timezone.utc)
    result = expires_at_for(["proj.ds.t"], client=client)
    # Fallback: now + 1 day. Allow a small wall-clock margin either side.
    assert timedelta(hours=23) < result - before < timedelta(hours=25)


def test_expires_at_for_skips_missing_tables():
    # A missing table shouldn't crash the call; verify_tables_exist
    # handles the existence check separately. Only NotFound is swallowed
    # -- other exceptions (permission, rate limit, etc.) propagate.
    from google.api_core import exceptions as gax_exceptions
    a = datetime(2026, 6, 1, tzinfo=timezone.utc)
    client = MagicMock()
    client.get_table.side_effect = [
        SimpleNamespace(expires=a),
        gax_exceptions.NotFound("proj.ds.gone"),
    ]
    result = expires_at_for(["proj.ds.a", "proj.ds.gone"], client=client)
    assert result == a


def test_expires_at_for_propagates_non_notfound_errors():
    from google.api_core import exceptions as gax_exceptions
    client = MagicMock()
    client.get_table.side_effect = gax_exceptions.Forbidden("nope")
    with pytest.raises(gax_exceptions.Forbidden):
        expires_at_for(["proj.ds.a"], client=client)


def test_expires_at_for_empty_input():
    before = datetime.now(timezone.utc)
    result = expires_at_for([])
    # No client used; pure fallback.
    assert timedelta(hours=23) < result - before < timedelta(hours=25)


# --------------------------------------------------------------------------
# Write path (M3): to_bq_row + write_cache.
# --------------------------------------------------------------------------

def _cached_run(**overrides: Any) -> CachedRun:
    base = dict(
        run_id="rid01",
        cache_key="key01",
        workflow="workflows/pipe_gaps/mode_equivalence.py",
        pipeline="pipe-gaps",
        experiment_id="exp01",
        pipeline_commit="abc1234",
        unreviewed_code=False,
        pipeline_commit_parent=None,
        dit_commit="def5678",
        workflow_file_sha1="aa" * 20,
        worker_image="gcr.io/foo/pipe-gaps@sha256:0011",
        params={"start": "2020-01-01", "end": "2020-12-31"},
        output_tables=["world-fishing-827.tech_great_expectations.bf_table"],
        dataflow_job_ids=["2026-05-22_00_00_00-1"],
        cloud_build_id="build-01",
        started_at=datetime(2026, 5, 22, tzinfo=timezone.utc),
        finished_at=datetime(2026, 5, 22, 0, 30, tzinfo=timezone.utc),
        status="succeeded",
        expires_at=datetime(2026, 9, 22, tzinfo=timezone.utc),
    )
    base.update(overrides)
    return CachedRun(**base)


def test_to_bq_row_shape():
    row = _cached_run().to_bq_row()
    assert set(row.keys()) == {
        "run_id", "cache_key", "workflow", "pipeline", "experiment_id",
        "pipeline_commit", "pipeline_dirty", "unreviewed_code",
        "pipeline_commit_parent", "dit_commit", "workflow_file_sha1",
        "worker_image", "params_json", "output_tables", "dataflow_job_ids",
        "cloud_build_id", "started_at", "finished_at", "status", "expires_at",
    }


def test_to_bq_row_dual_writes_pipeline_dirty_from_unreviewed():
    # The legacy pipeline_dirty column is dual-written (= unreviewed_code)
    # for the transition release.
    row = _cached_run(unreviewed_code=True).to_bq_row()
    assert row["unreviewed_code"] is True
    assert row["pipeline_dirty"] is True


def test_to_bq_row_timestamps_are_iso_strings():
    row = _cached_run().to_bq_row()
    assert row["started_at"] == "2026-05-22T00:00:00+00:00"
    assert row["finished_at"] == "2026-05-22T00:30:00+00:00"
    assert row["expires_at"] == "2026-09-22T00:00:00+00:00"


def test_to_bq_row_params_json_is_serialised():
    row = _cached_run().to_bq_row()
    # Streaming inserts accept JSON columns as JSON-string literals; the
    # server parses them back. Round-trip should yield the original dict.
    import json as _json
    assert _json.loads(row["params_json"]) == {"start": "2020-01-01", "end": "2020-12-31"}


def test_to_bq_row_nullable_fields_pass_through():
    row = _cached_run(
        params=None,
        cloud_build_id=None,
        finished_at=None,
    ).to_bq_row()
    assert row["params_json"] is None
    assert row["cloud_build_id"] is None
    assert row["finished_at"] is None


def test_from_bq_row_falls_back_to_pipeline_dirty_pre_migration():
    """Transition safety: a row from a not-yet-migrated table has no
    unreviewed_code attribute (or it's NULL). from_bq_row must fall back to
    the legacy pipeline_dirty so reads keep working before migration 002
    backfills."""
    # No unreviewed_code attribute at all (pre-ADD COLUMN).
    row = _bq_row(pipeline_dirty=True)
    del row.unreviewed_code
    del row.pipeline_commit_parent
    restored = CachedRun.from_bq_row(row)
    assert restored.unreviewed_code is True
    assert restored.pipeline_commit_parent is None

    # Column present but NULL (ADDed, not yet backfilled).
    row2 = _bq_row(pipeline_dirty=True, unreviewed_code=None)
    assert CachedRun.from_bq_row(row2).unreviewed_code is True


def test_to_bq_row_from_bq_row_round_trip():
    # Symmetric: to_bq_row -> simulate BQ deserialisation -> from_bq_row
    # should reproduce the same fields the round-trip preserves.
    original = _cached_run()
    bq_dict = original.to_bq_row()
    # Simulate the BQ read coming back:
    #   * TIMESTAMP -> datetime (parsing the ISO string)
    #   * JSON -> dict (parsed by the BQ client)
    import json as _json
    row_obj = SimpleNamespace(
        **{k: v for k, v in bq_dict.items() if k != "params_json"},
        params_json=_json.loads(bq_dict["params_json"]) if bq_dict["params_json"] else None,
    )
    # Re-parse the TIMESTAMP fields back into datetime (BQ client does this).
    for field in ("started_at", "finished_at", "expires_at"):
        v = getattr(row_obj, field)
        if v is not None:
            setattr(row_obj, field, datetime.fromisoformat(v))

    restored = CachedRun.from_bq_row(row_obj)
    assert restored == original


def test_write_cache_calls_dml_insert():
    client = MagicMock()
    run = _cached_run()
    write_cache(run, client=client)
    client.query.assert_called_once()
    args, kwargs = client.query.call_args
    sql = args[0]
    assert "INSERT INTO" in sql
    assert "world-fishing-827.tech_great_expectations.dit_runs" in sql
    # JSON column uses the documented PARSE_JSON(STRING_param) pattern.
    assert "PARSE_JSON(@params_json)" in sql
    # All columns appear as @parameters in the VALUES clause.
    expected = {
        "run_id", "cache_key", "workflow", "pipeline", "experiment_id",
        "pipeline_commit", "pipeline_dirty", "unreviewed_code",
        "pipeline_commit_parent", "dit_commit", "workflow_file_sha1",
        "worker_image", "params_json", "output_tables", "dataflow_job_ids",
        "cloud_build_id", "started_at", "finished_at", "status", "expires_at",
    }
    job_config = kwargs["job_config"]
    actual = {p.name for p in job_config.query_parameters}
    assert actual == expected
    # .result() must be called so we block until the INSERT commits.
    client.query.return_value.result.assert_called_once()


def test_write_cache_binds_params_with_correct_values():
    client = MagicMock()
    run = _cached_run(run_id="rid-x", cache_key="ck-x", unreviewed_code=True)
    write_cache(run, client=client)
    job_config = client.query.call_args.kwargs["job_config"]
    by_name = {p.name: p for p in job_config.query_parameters}
    assert by_name["run_id"].value == "rid-x"
    assert by_name["cache_key"].value == "ck-x"
    assert by_name["unreviewed_code"].value is True
    # pipeline_dirty is dual-written from unreviewed_code during the transition.
    assert by_name["pipeline_dirty"].value is True
    # params_json is the JSON-STRING form, server-side PARSE_JSON converts.
    import json as _json
    assert _json.loads(by_name["params_json"].value) == run.params
    # Arrays bind via ArrayQueryParameter (`.values`, not `.value`).
    assert by_name["output_tables"].values == list(run.output_tables)


def test_write_cache_propagates_query_errors():
    # DML errors come back via client.query(...).result(), not as a
    # returned list (which is what insert_rows_json does).
    client = MagicMock()
    client.query.return_value.result.side_effect = RuntimeError("bad SQL")
    with pytest.raises(RuntimeError, match="bad SQL"):
        write_cache(_cached_run(), client=client)


def test_write_cache_writes_unreviewed_rows_too():
    # Design invariant: unreviewed (snapshot) rows ARE inserted. Post-M-pivot-3
    # they're also cacheable (read_cache no longer filters them) — but the
    # write path must still record them either way.
    client = MagicMock()
    write_cache(_cached_run(unreviewed_code=True), client=client)
    job_config = client.query.call_args.kwargs["job_config"]
    unrev = next(p for p in job_config.query_parameters if p.name == "unreviewed_code")
    assert unrev.value is True


def test_write_cache_handles_null_params():
    # PARSE_JSON(NULL) returns NULL; the params_json binding must pass None.
    client = MagicMock()
    write_cache(_cached_run(params=None), client=client)
    job_config = client.query.call_args.kwargs["job_config"]
    params_json_param = next(
        p for p in job_config.query_parameters if p.name == "params_json"
    )
    assert params_json_param.value is None
