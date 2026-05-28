"""Tests for the cache-integration layer in `workflows/pipe_gaps/mode_equivalence.py`.

These tests don't need real pipe_gaps installed — the workflow's pipe_gaps
imports are function-local (inside `_build_pipeline_for`), so importing the
module at the top of this test file works as long as dit itself is installed.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch

# This import works even without pipe_gaps; the workflow's pipe_gaps imports
# are inside function bodies, not at module level.
from workflows.pipe_gaps import mode_equivalence as mod


def _args(**overrides: Any) -> argparse.Namespace:
    """Build an argparse-like namespace with the fields the cache wrapper reads."""
    base = dict(
        start="2020-01-01",
        end="2020-12-31",
        tail_days=4,
        backfill_days=4,
        min_gap_length=1.0,
        n_hours_before=12,
        window_period_d=2,
        filter_good_seg="True",
        skip_open_gaps=False,
        ssvids="",
        source_messages="proj.ds.messages",
        source_segments="proj.ds.segments",
        # Per-`main()` context normally set inside main():
        run_id="rid01",
        experiment_id="exp01",
        pipeline_commit="abc1234",
        unreviewed=False,
        pipeline_commit_parent=None,
        dit_commit="def5678",
        worker_image="gcr.io/foo/pipe-gaps:v0.9.6",
        worker_image_digest="gcr.io/foo/pipe-gaps@sha256:0011",
    )
    base.update(overrides)
    return argparse.Namespace(**base)


# --------------------------------------------------------------------------
# canonical_params_dict
# --------------------------------------------------------------------------

def test_canonical_params_dict_includes_mode():
    p = mod.canonical_params_dict(_args(), mod.MODE_BF)
    assert p["mode"] == mod.MODE_BF


def test_canonical_params_dict_changes_with_mode():
    a = mod.canonical_params_dict(_args(), mod.MODE_BF)
    b = mod.canonical_params_dict(_args(), mod.MODE_BFD)
    assert a != b


def test_canonical_params_dict_bf_excludes_tail_and_backfill():
    # MODE_BF runs a single big-range slice; tail_days / backfill_days
    # are wired through execute_* for symmetry but BF doesn't read
    # them. They must not contribute to BF's cache key or changing
    # --tail-days would invalidate BF for no behavioural reason.
    p = mod.canonical_params_dict(_args(), mod.MODE_BF)
    assert "tail_days" not in p
    assert "backfill_days" not in p


def test_canonical_params_dict_bfd_includes_tail_and_backfill():
    # MODE_BFD does multiple daily slices controlled by tail_days +
    # backfill_days, so these DO affect its output.
    p = mod.canonical_params_dict(_args(), mod.MODE_BFD)
    assert p["tail_days"] == 4
    assert p["backfill_days"] == 4


def test_canonical_params_dict_bf_invariant_to_tail_change():
    # Concrete consequence: changing --tail-days alone should produce
    # the same BF cache key.
    a = mod.canonical_params_dict(_args(tail_days=4), mod.MODE_BF)
    b = mod.canonical_params_dict(_args(tail_days=10), mod.MODE_BF)
    assert a == b


def test_canonical_params_dict_excludes_plumbing():
    # Set various plumbing-only fields; they must not appear in the result.
    args = _args(
        experiment_id="ignored",
        run_id="ignored",
    )
    args.service_account = "ignored"  # set extra fields via attribute
    args.dataflow_region = "ignored"
    p = mod.canonical_params_dict(args, mod.MODE_BF)
    for plumbing_key in (
        "service_account", "dataflow_region", "bq_temp_dataset",
        "experiment_id", "run_id", "suffix", "image_tag", "worker_image",
        "dest_dataset",
    ):
        assert plumbing_key not in p, f"plumbing key {plumbing_key!r} leaked into params"


def test_canonical_params_dict_ssvids_sorted():
    # ssvids order doesn't affect output -> always sort.
    p = mod.canonical_params_dict(_args(ssvids="999,111,222"), mod.MODE_BF)
    assert p["ssvids"] == ["111", "222", "999"]


def test_canonical_params_dict_filter_good_seg_coerced_to_bool():
    p = mod.canonical_params_dict(_args(filter_good_seg="False"), mod.MODE_BF)
    assert p["filter_good_seg"] is False


# --------------------------------------------------------------------------
# _build_cache_key
# --------------------------------------------------------------------------

def test_build_cache_key_uses_args_context():
    key = mod._build_cache_key(_args(), mod.MODE_BF)
    assert key.pipeline_commit == "abc1234"
    assert key.worker_image_digest == "gcr.io/foo/pipe-gaps@sha256:0011"
    assert key.workflow_file_sha1 == mod.WORKFLOW_FILE_SHA1
    assert key.params["mode"] == mod.MODE_BF


def test_build_cache_key_extras_merge_into_params():
    key = mod._build_cache_key(
        _args(), mod.MODE_MUTATE_RECOVER,
        restricted_ssvids=sorted(["111", "999"]),
    )
    assert key.params["restricted_ssvids"] == ["111", "999"]
    assert key.params["mode"] == mod.MODE_MUTATE_RECOVER


# --------------------------------------------------------------------------
# _run_with_cache
# --------------------------------------------------------------------------

def _cached_row(**overrides: Any) -> mod.CachedRun:
    base = dict(
        run_id="prev-run",
        cache_key="prev-key",
        workflow=mod.WORKFLOW_NAME,
        pipeline="pipe-gaps",
        experiment_id="prev-exp",
        pipeline_commit="abc1234",
        unreviewed_code=False,
        dit_commit="def5678",
        workflow_file_sha1=mod.WORKFLOW_FILE_SHA1,
        worker_image="gcr.io/foo/pipe-gaps:v0.9.6",
        params={"mode": mod.MODE_BF},
        output_tables=["proj.ds.cached_bf_table"],
        dataflow_job_ids=[],
        cloud_build_id=None,
        started_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        finished_at=datetime(2026, 5, 1, 0, 30, tzinfo=timezone.utc),
        status=mod.STATUS_SUCCEEDED,
        expires_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
    )
    base.update(overrides)
    return mod.CachedRun(**base)


def test_run_with_cache_hit_skips_execute_fn():
    execute_fn = MagicMock()
    with (
        patch.object(mod, "read_cache", return_value=_cached_row()),
        patch.object(mod, "verify_tables_exist", return_value=[True]),
        patch.object(mod, "write_cache") as mock_write,
    ):
        result = mod._run_with_cache(
            execute_fn,
            args=_args(),
            mode=mod.MODE_BF,
            output_fqn="proj.ds.fresh_bf_table",
            execute_kwargs={},
        )
    execute_fn.assert_not_called()
    mock_write.assert_not_called()
    # Returns the CACHED FQN, not the fresh local one.
    assert result == "proj.ds.cached_bf_table"


def test_run_with_cache_miss_runs_and_writes():
    execute_fn = MagicMock()
    with (
        patch.object(mod, "read_cache", return_value=None),
        patch.object(mod, "verify_tables_exist", return_value=[True]),
        patch.object(mod, "write_cache") as mock_write,
        patch.object(mod, "expires_at_for",
                     return_value=datetime(2026, 9, 1, tzinfo=timezone.utc)),
    ):
        result = mod._run_with_cache(
            execute_fn,
            args=_args(),
            mode=mod.MODE_BF,
            output_fqn="proj.ds.fresh_bf_table",
            execute_kwargs={"foo": "bar"},
        )
    execute_fn.assert_called_once_with(foo="bar")
    mock_write.assert_called_once()
    written_row = mock_write.call_args.args[0]
    assert written_row.output_tables == ["proj.ds.fresh_bf_table"]
    assert written_row.status == mod.STATUS_SUCCEEDED
    assert written_row.pipeline == "pipe-gaps"
    # Returns the FRESH FQN (we just computed it).
    assert result == "proj.ds.fresh_bf_table"


def test_run_with_cache_empty_output_tables_treats_as_miss():
    # Degenerate state: a "succeeded" row with no output_tables. Without
    # the guard, all([]) -> True and indexing [0] would IndexError.
    execute_fn = MagicMock()
    with (
        patch.object(mod, "read_cache", return_value=_cached_row(output_tables=[])),
        patch.object(mod, "verify_tables_exist", return_value=[]),
        patch.object(mod, "write_cache") as mock_write,
        patch.object(
            mod, "expires_at_for",
            return_value=datetime(2026, 9, 1, tzinfo=timezone.utc),
        ),
    ):
        result = mod._run_with_cache(
            execute_fn,
            args=_args(),
            mode=mod.MODE_BF,
            output_fqn="proj.ds.fresh_bf_table",
            execute_kwargs={},
        )
    execute_fn.assert_called_once()
    mock_write.assert_called_once()
    assert result == "proj.ds.fresh_bf_table"


def test_run_with_cache_stale_row_treats_as_miss():
    # Row exists but the referenced tables don't (TTL'd out).
    execute_fn = MagicMock()
    with (
        patch.object(mod, "read_cache", return_value=_cached_row()),
        patch.object(mod, "verify_tables_exist", return_value=[False]),
        patch.object(mod, "write_cache") as mock_write,
        patch.object(mod, "expires_at_for",
                     return_value=datetime(2026, 9, 1, tzinfo=timezone.utc)),
    ):
        result = mod._run_with_cache(
            execute_fn,
            args=_args(),
            mode=mod.MODE_BF,
            output_fqn="proj.ds.fresh_bf_table",
            execute_kwargs={},
        )
    execute_fn.assert_called_once()
    mock_write.assert_called_once()
    assert result == "proj.ds.fresh_bf_table"


def test_run_with_cache_writes_unreviewed_rows():
    # Design invariant: unreviewed (snapshot / unmerged) runs ARE recorded.
    # args.unreviewed holds the resolved flag; it maps to the row's unreviewed_code.
    execute_fn = MagicMock()
    args = _args(unreviewed=True)
    with (
        patch.object(mod, "read_cache", return_value=None),
        patch.object(mod, "verify_tables_exist", return_value=[True]),
        patch.object(mod, "write_cache") as mock_write,
        patch.object(mod, "expires_at_for",
                     return_value=datetime(2026, 9, 1, tzinfo=timezone.utc)),
    ):
        mod._run_with_cache(
            execute_fn, args=args, mode=mod.MODE_BF,
            output_fqn="x", execute_kwargs={},
        )
    written_row = mock_write.call_args.args[0]
    assert written_row.unreviewed_code is True


def test_run_with_cache_includes_extras_in_key():
    # When extras change, the cache key changes (different lookup).
    args = _args()
    calls = []
    def fake_read(key):
        calls.append(key)
        return None
    with (
        patch.object(mod, "read_cache", side_effect=fake_read),
        patch.object(mod, "verify_tables_exist", return_value=[True]),
        patch.object(mod, "write_cache"),
        patch.object(mod, "expires_at_for",
                     return_value=datetime(2026, 9, 1, tzinfo=timezone.utc)),
    ):
        mod._run_with_cache(
            MagicMock(), args=args, mode=mod.MODE_MUTATE_RECOVER,
            output_fqn="x", execute_kwargs={},
            cache_key_extras={"restricted_ssvids": ["a"]},
        )
        mod._run_with_cache(
            MagicMock(), args=args, mode=mod.MODE_MUTATE_RECOVER,
            output_fqn="x", execute_kwargs={},
            cache_key_extras={"restricted_ssvids": ["b"]},
        )
    assert calls[0] != calls[1], "different extras -> different cache keys"
