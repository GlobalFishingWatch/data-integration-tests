"""Tests for the shared workflow harness in ``src/dit/workflow.py``.

Covers the four concerns extracted from the two workflows:

* experiment-id helpers (validate / default / arg wiring),
* infra-knob arg wiring,
* ``resolve_run_context`` (suffix escape-hatch vs normal path, digest fallback),
* ``run_with_cache`` (hit / miss / stale).

The cache + git/worker-image collaborators are monkeypatched on the
``dit.workflow`` module so no BQ / git / gcloud calls happen.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from dit import workflow as dit_workflow
from dit.cache import CacheKey
from dit.workflow import (
    EXPERIMENT_ID_RE,
    RunContext,
    add_dataflow_args,
    add_dataset_args,
    add_experiment_id_arg,
    add_infra_args,
    default_experiment_id,
    validate_experiment_id,
)

# --------------------------------------------------------------------------
# (b) experiment-id helpers
# --------------------------------------------------------------------------

@pytest.mark.parametrize("value", ["exp01", "a", "0", "solo_abc123", "a-b_c", "x" * 32])
def test_validate_experiment_id_accepts_valid(value: str) -> None:
    assert validate_experiment_id(value) == value


@pytest.mark.parametrize(
    "value",
    [
        "",            # empty
        "-leading",    # must start [a-z0-9]
        "_leading",    # must start [a-z0-9]
        "UPPER",       # no uppercase
        "has space",   # no space
        "a/b",         # no slash
        "x" * 33,      # too long (max 32)
    ],
)
def test_validate_experiment_id_rejects_invalid(value: str) -> None:
    with pytest.raises(SystemExit):
        validate_experiment_id(value)


def test_default_experiment_id_shape() -> None:
    val = default_experiment_id()
    assert val.startswith("solo_")
    # solo_ + 6 hex chars
    assert len(val) == len("solo_") + 6
    # the auto-default must itself be a valid experiment id
    assert EXPERIMENT_ID_RE.match(val)


def test_default_experiment_id_is_unique_per_call() -> None:
    assert default_experiment_id() != default_experiment_id()


# --------------------------------------------------------------------------
# (a) + (b) arg wiring
# --------------------------------------------------------------------------

def test_add_infra_args_wires_defaults() -> None:
    p = argparse.ArgumentParser()
    add_infra_args(p)
    args = p.parse_args([])
    assert args.dest_dataset == dit_workflow.DEFAULT_DEST_DATASET
    assert args.service_account == dit_workflow.DEFAULT_DATAFLOW_SA
    assert args.dataflow_region == dit_workflow.DEFAULT_DATAFLOW_REGION
    assert args.dataflow_temp_bucket == dit_workflow.DEFAULT_DATAFLOW_TEMP_BUCKET
    assert args.dataflow_subnetwork == dit_workflow.DEFAULT_DATAFLOW_SUBNETWORK


def test_add_infra_args_does_not_add_bq_temp_dataset() -> None:
    # --bq-temp-dataset is workflow-specific; it must NOT be added by the
    # shared helper (pipe-gaps + port-visits keep it locally).
    p = argparse.ArgumentParser()
    add_infra_args(p)
    args = p.parse_args([])
    assert not hasattr(args, "bq_temp_dataset")


def test_add_infra_args_flags_override() -> None:
    p = argparse.ArgumentParser()
    add_infra_args(p)
    args = p.parse_args(["--dest-dataset", "my_ds", "--dataflow-region", "europe-west1"])
    assert args.dest_dataset == "my_ds"
    assert args.dataflow_region == "europe-west1"


# --------------------------------------------------------------------------
# (a) add_infra_args split: add_dataset_args + add_dataflow_args (Phase 3)
# --------------------------------------------------------------------------

def test_add_dataset_args_wires_only_dataset_knobs() -> None:
    # The runner-agnostic dataset knob EVERY consumer uses; NO Dataflow knobs
    # (incl. --service-account, which is the Dataflow worker SA).
    p = argparse.ArgumentParser()
    add_dataset_args(p)
    args = p.parse_args([])
    assert args.dest_dataset == dit_workflow.DEFAULT_DEST_DATASET
    assert not hasattr(args, "service_account")
    assert not hasattr(args, "dataflow_region")
    assert not hasattr(args, "dataflow_temp_bucket")
    assert not hasattr(args, "dataflow_subnetwork")


def test_add_dataflow_args_wires_only_dataflow_knobs() -> None:
    p = argparse.ArgumentParser()
    add_dataflow_args(p)
    args = p.parse_args([])
    assert args.service_account == dit_workflow.DEFAULT_DATAFLOW_SA
    assert args.dataflow_region == dit_workflow.DEFAULT_DATAFLOW_REGION
    assert args.dataflow_temp_bucket == dit_workflow.DEFAULT_DATAFLOW_TEMP_BUCKET
    assert args.dataflow_subnetwork == dit_workflow.DEFAULT_DATAFLOW_SUBNETWORK
    assert not hasattr(args, "dest_dataset")


def test_add_infra_args_namespace_identical_to_split_composition() -> None:
    # The Beam consumers keep calling add_infra_args; its parsed namespace must
    # be byte-identical to dataset+dataflow composed (no behaviour change).
    p_combined = argparse.ArgumentParser()
    add_infra_args(p_combined)
    ns_combined = vars(p_combined.parse_args([]))

    p_split = argparse.ArgumentParser()
    add_dataset_args(p_split)
    add_dataflow_args(p_split)
    ns_split = vars(p_split.parse_args([]))

    assert ns_combined == ns_split
    # and the exact key set the two Beam workflows depend on:
    assert set(ns_combined) == {
        "dest_dataset", "service_account",
        "dataflow_region", "dataflow_temp_bucket", "dataflow_subnetwork",
    }


def test_add_experiment_id_arg_auto_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DIT_EXPERIMENT_ID", raising=False)
    p = argparse.ArgumentParser()
    add_experiment_id_arg(p)
    args = p.parse_args([])
    assert args.experiment_id.startswith("solo_")


def test_add_experiment_id_arg_env_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DIT_EXPERIMENT_ID", "from-env")
    p = argparse.ArgumentParser()
    add_experiment_id_arg(p)
    args = p.parse_args([])
    assert args.experiment_id == "from-env"


def test_add_experiment_id_arg_explicit_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DIT_EXPERIMENT_ID", raising=False)
    p = argparse.ArgumentParser()
    add_experiment_id_arg(p)
    args = p.parse_args(["--experiment-id", "explicit-exp"])
    assert args.experiment_id == "explicit-exp"


def test_add_experiment_id_arg_rejects_invalid_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DIT_EXPERIMENT_ID", raising=False)
    p = argparse.ArgumentParser()
    add_experiment_id_arg(p)
    with pytest.raises(SystemExit):
        p.parse_args(["--experiment-id", "BAD VALUE"])


# --------------------------------------------------------------------------
# (c) resolve_run_context
# --------------------------------------------------------------------------

def test_resolve_run_context_normal_path() -> None:
    """No --suffix: resolve_pipeline_commit is consulted, snapshot escape
    hatch (git_info / is_unreviewed) is NOT."""
    with (
        patch.object(dit_workflow, "resolve_pipeline_commit",
                     return_value=("abc1234", False)) as mock_resolve,
        patch.object(dit_workflow, "git_info") as mock_git_info,
        patch.object(dit_workflow, "is_unreviewed") as mock_is_unreviewed,
        patch.object(dit_workflow, "snapshot_parent", return_value=None),
        patch.object(dit_workflow, "ensure_pipeline_image",
                     return_value="img:tag") as mock_ensure,
        patch.object(dit_workflow, "resolve_worker_image_to_digest",
                     return_value="img@sha256:dead"),
        patch.object(dit_workflow, "dit_commit", return_value="ditshaa"),
    ):
        ctx = dit_workflow.resolve_run_context(
            repo_dir="/repo",
            pipeline_name="pipe-gaps",
            runner="dataflow",
            require_clean=False,
            suffix=None,
            worker_image="default:img",
            default_worker_image="default:img",
        )

    mock_resolve.assert_called_once()
    mock_git_info.assert_not_called()
    mock_is_unreviewed.assert_not_called()
    mock_ensure.assert_called_once()
    assert isinstance(ctx, RunContext)
    assert ctx.pipeline_commit == "abc1234"
    assert ctx.unreviewed is False
    assert ctx.pipeline_commit_parent is None
    assert ctx.worker_image == "img:tag"
    assert ctx.worker_image_digest == "img@sha256:dead"
    assert ctx.dit_commit == "ditshaa"
    # run_id is a fresh 12-hex string
    assert len(ctx.run_id) == 12


def test_resolve_run_context_suffix_escape_hatch() -> None:
    """--suffix set: git state recorded as-is (git_info + is_unreviewed),
    resolve_pipeline_commit is NOT called (no auto-snapshot)."""
    with (
        patch.object(dit_workflow, "resolve_pipeline_commit") as mock_resolve,
        patch.object(dit_workflow, "git_info",
                     return_value=("clean567", False)) as mock_git_info,
        patch.object(dit_workflow, "is_unreviewed",
                     return_value=True) as mock_is_unreviewed,
        patch.object(dit_workflow, "snapshot_parent", return_value=None),
        patch.object(dit_workflow, "ensure_pipeline_image", return_value="img:tag"),
        patch.object(dit_workflow, "resolve_worker_image_to_digest",
                     return_value="img@sha256:dead"),
        patch.object(dit_workflow, "dit_commit", return_value="ditshaa"),
    ):
        ctx = dit_workflow.resolve_run_context(
            repo_dir="/repo",
            pipeline_name="anchorages_pipeline",
            runner="dataflow",
            require_clean=False,
            suffix="my-manual-suffix",
            worker_image="default:img",
            default_worker_image="default:img",
        )

    mock_resolve.assert_not_called()
    mock_git_info.assert_called_once()
    mock_is_unreviewed.assert_called_once()
    assert ctx.pipeline_commit == "clean567"
    # clean tree (dirty=False) but is_unreviewed=True -> unreviewed True.
    assert ctx.unreviewed is True


def test_resolve_run_context_suffix_dirty_short_circuits_is_unreviewed() -> None:
    """When the tree is dirty, unreviewed is True regardless of is_unreviewed
    (short-circuit ``dirty or is_unreviewed(...)``)."""
    with (
        patch.object(dit_workflow, "git_info", return_value=("sha", True)),
        patch.object(dit_workflow, "is_unreviewed") as mock_is_unreviewed,
        patch.object(dit_workflow, "snapshot_parent", return_value=None),
        patch.object(dit_workflow, "ensure_pipeline_image", return_value="img:tag"),
        patch.object(dit_workflow, "resolve_worker_image_to_digest", return_value="d"),
        patch.object(dit_workflow, "dit_commit", return_value="x"),
    ):
        ctx = dit_workflow.resolve_run_context(
            repo_dir="/repo",
            pipeline_name="pipe-gaps",
            runner="dataflow",
            require_clean=False,
            suffix="s",
            worker_image="default:img",
            default_worker_image="default:img",
        )
    assert ctx.unreviewed is True
    # short-circuit: dirty=True means is_unreviewed never gets called.
    mock_is_unreviewed.assert_not_called()


def test_resolve_run_context_digest_fallback_on_runtimeerror() -> None:
    """If resolve_worker_image_to_digest raises RuntimeError, fall back to the
    tag form (the worker image itself)."""
    with (
        patch.object(dit_workflow, "resolve_pipeline_commit",
                     return_value=("abc1234", False)),
        patch.object(dit_workflow, "snapshot_parent", return_value=None),
        patch.object(dit_workflow, "ensure_pipeline_image", return_value="img:tag"),
        patch.object(dit_workflow, "resolve_worker_image_to_digest",
                     side_effect=RuntimeError("gcloud blew up")),
        patch.object(dit_workflow, "dit_commit", return_value="x"),
    ):
        ctx = dit_workflow.resolve_run_context(
            repo_dir="/repo",
            pipeline_name="pipe-gaps",
            runner="dataflow",
            require_clean=False,
            suffix=None,
            worker_image="default:img",
            default_worker_image="default:img",
        )
    # falls back to the (resolved) worker image tag form
    assert ctx.worker_image_digest == "img:tag"
    assert ctx.worker_image == "img:tag"


def test_resolve_run_context_threads_worker_image_into_ensure() -> None:
    """ensure_pipeline_image gets the resolved commit + the passed images, and
    its return value becomes ctx.worker_image (feeding the digest resolve)."""
    with (
        patch.object(dit_workflow, "resolve_pipeline_commit",
                     return_value=("commit99", True)),
        patch.object(dit_workflow, "snapshot_parent", return_value="parent00"),
        patch.object(dit_workflow, "ensure_pipeline_image",
                     return_value="built:custom") as mock_ensure,
        patch.object(dit_workflow, "resolve_worker_image_to_digest",
                     return_value="built@sha256:beef") as mock_digest,
        patch.object(dit_workflow, "dit_commit", return_value="x"),
    ):
        ctx = dit_workflow.resolve_run_context(
            repo_dir="/repo",
            pipeline_name="pipe-gaps",
            runner="dataflow",
            require_clean=False,
            suffix=None,
            worker_image="default:img",
            default_worker_image="default:img",
        )
    mock_ensure.assert_called_once_with(
        pipeline="pipe-gaps",
        repo_dir="/repo",
        commit="commit99",
        unreviewed=True,
        worker_image="default:img",
        default_worker_image="default:img",
    )
    # the digest is resolved against the image ensure_pipeline_image returned.
    mock_digest.assert_called_once_with("built:custom")
    assert ctx.worker_image == "built:custom"
    assert ctx.worker_image_digest == "built@sha256:beef"
    assert ctx.pipeline_commit_parent == "parent00"


def test_resolve_run_context_resolve_digest_false_skips_gcloud() -> None:
    """resolve_digest=False (callers with no run-cache, e.g. port-visits) must
    NOT call resolve_worker_image_to_digest; the digest is the tag form."""
    with (
        patch.object(dit_workflow, "resolve_pipeline_commit",
                     return_value=("abc1234", False)),
        patch.object(dit_workflow, "snapshot_parent", return_value=None),
        patch.object(dit_workflow, "ensure_pipeline_image", return_value="img:tag"),
        patch.object(dit_workflow, "resolve_worker_image_to_digest") as mock_digest,
        patch.object(dit_workflow, "dit_commit", return_value="x"),
    ):
        ctx = dit_workflow.resolve_run_context(
            repo_dir="/repo",
            pipeline_name="anchorages_pipeline",
            runner="dataflow",
            require_clean=False,
            suffix=None,
            worker_image="default:img",
            default_worker_image="default:img",
            resolve_digest=False,
        )
    mock_digest.assert_not_called()
    assert ctx.worker_image_digest == "img:tag"  # tag form, no gcloud call


def test_resolve_run_context_build_from_source_bypasses_ensure_pipeline_image() -> None:
    """build_from_source=True signals the docker runner will build the
    container locally via compose, so the harness must NOT call
    ensure_pipeline_image — kaniko would build an image that's never pulled."""
    with (
        patch.object(dit_workflow, "resolve_pipeline_commit",
                     return_value=("abc1234", True)),  # unreviewed, would normally build
        patch.object(dit_workflow, "snapshot_parent", return_value=None),
        patch.object(dit_workflow, "ensure_pipeline_image") as mock_ensure,
        patch.object(dit_workflow, "dit_commit", return_value="x"),
    ):
        ctx = dit_workflow.resolve_run_context(
            repo_dir="/repo",
            pipeline_name="pipe-events",
            runner="docker",
            require_clean=False,
            suffix=None,
            worker_image="canonical:v1",
            default_worker_image="canonical:v1",
            resolve_digest=False,
            build_from_source=True,
        )
    mock_ensure.assert_not_called()
    # worker_image flows through unchanged from input.
    assert ctx.worker_image == "canonical:v1"


def test_resolve_run_context_build_from_source_false_still_calls_ensure() -> None:
    """The default (build_from_source=False) path keeps the auto-build trigger
    active — no regression for Beam consumers."""
    with (
        patch.object(dit_workflow, "resolve_pipeline_commit",
                     return_value=("abc1234", True)),
        patch.object(dit_workflow, "snapshot_parent", return_value=None),
        patch.object(dit_workflow, "ensure_pipeline_image",
                     return_value="built:img") as mock_ensure,
        patch.object(dit_workflow, "dit_commit", return_value="x"),
    ):
        dit_workflow.resolve_run_context(
            repo_dir="/repo",
            pipeline_name="pipe-gaps",
            runner="dataflow",
            require_clean=False,
            suffix=None,
            worker_image="default:img",
            default_worker_image="default:img",
            resolve_digest=False,
        )
    mock_ensure.assert_called_once()


# --------------------------------------------------------------------------
# (d) run_with_cache
# --------------------------------------------------------------------------

def _ctx(**overrides: Any) -> RunContext:
    base = dict(
        pipeline_commit="abc1234",
        unreviewed=False,
        pipeline_commit_parent=None,
        worker_image="gcr.io/foo/pipe-gaps:v0.9.6",
        worker_image_digest="gcr.io/foo/pipe-gaps@sha256:0011",
        run_id="rid01",
        dit_commit="def5678",
    )
    base.update(overrides)
    return RunContext(**base)


def _cache_key(params: dict[str, Any] | None = None) -> CacheKey:
    return CacheKey(
        pipeline_commit="abc1234",
        worker_image_digest="gcr.io/foo/pipe-gaps@sha256:0011",
        workflow_file_sha1="wfsha1",
        params=params if params is not None else {"mode": "1_bf"},
    )


def _cached_row(**overrides: Any) -> dit_workflow.CachedRun:
    base = dict(
        run_id="prev-run",
        cache_key="prev-key",
        workflow="workflows/pipe_gaps/mode_equivalence.py",
        pipeline="pipe-gaps",
        experiment_id="prev-exp",
        pipeline_commit="abc1234",
        unreviewed_code=False,
        dit_commit="def5678",
        workflow_file_sha1="wfsha1",
        worker_image="gcr.io/foo/pipe-gaps:v0.9.6",
        params={"mode": "1_bf"},
        output_tables=["proj.ds.cached_table"],
        dataflow_job_ids=[],
        cloud_build_id=None,
        started_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        finished_at=datetime(2026, 5, 1, 0, 30, tzinfo=timezone.utc),
        status=dit_workflow.STATUS_SUCCEEDED,
        expires_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
    )
    base.update(overrides)
    return dit_workflow.CachedRun(**base)


def test_run_with_cache_hit_returns_cached_and_skips_execute() -> None:
    execute_fn = MagicMock()
    with (
        patch.object(dit_workflow, "read_cache", return_value=_cached_row()),
        patch.object(dit_workflow, "verify_tables_exist", return_value=[True]),
        patch.object(dit_workflow, "write_cache") as mock_write,
    ):
        result = dit_workflow.run_with_cache(
            execute_fn,
            ctx=_ctx(),
            workflow="workflows/pipe_gaps/mode_equivalence.py",
            pipeline="pipe-gaps",
            experiment_id="exp01",
            cache_key=_cache_key(),
            output_fqn="proj.ds.fresh_table",
            execute_kwargs={},
        )
    execute_fn.assert_not_called()
    mock_write.assert_not_called()
    assert result == "proj.ds.cached_table"


def test_run_with_cache_miss_runs_executes_and_writes_row() -> None:
    execute_fn = MagicMock()
    with (
        patch.object(dit_workflow, "read_cache", return_value=None),
        patch.object(dit_workflow, "verify_tables_exist", return_value=[True]),
        patch.object(dit_workflow, "write_cache") as mock_write,
        patch.object(dit_workflow, "expires_at_for",
                     return_value=datetime(2026, 9, 1, tzinfo=timezone.utc)),
    ):
        result = dit_workflow.run_with_cache(
            execute_fn,
            ctx=_ctx(),
            workflow="workflows/pipe_gaps/mode_equivalence.py",
            pipeline="pipe-gaps",
            experiment_id="exp01",
            cache_key=_cache_key(),
            output_fqn="proj.ds.fresh_table",
            execute_kwargs={"foo": "bar"},
        )
    execute_fn.assert_called_once_with(foo="bar")
    mock_write.assert_called_once()
    written = mock_write.call_args.args[0]
    assert written.output_tables == ["proj.ds.fresh_table"]
    assert written.status == dit_workflow.STATUS_SUCCEEDED
    assert written.pipeline == "pipe-gaps"
    assert written.workflow == "workflows/pipe_gaps/mode_equivalence.py"
    assert written.run_id == "rid01"
    assert written.dataflow_job_ids == []
    assert result == "proj.ds.fresh_table"


def test_run_with_cache_row_built_from_ctx_and_key() -> None:
    """The written row's provenance fields come from the RunContext + CacheKey,
    not from any workflow namespace."""
    with (
        patch.object(dit_workflow, "read_cache", return_value=None),
        patch.object(dit_workflow, "verify_tables_exist", return_value=[True]),
        patch.object(dit_workflow, "write_cache") as mock_write,
        patch.object(dit_workflow, "expires_at_for",
                     return_value=datetime(2026, 9, 1, tzinfo=timezone.utc)),
    ):
        dit_workflow.run_with_cache(
            MagicMock(),
            ctx=_ctx(unreviewed=True, pipeline_commit_parent="parent99"),
            workflow="wf",
            pipeline="pipe-gaps",
            experiment_id="exp42",
            cache_key=_cache_key(params={"mode": "2_bfd", "x": 1}),
            output_fqn="proj.ds.t",
            execute_kwargs={},
        )
    written = mock_write.call_args.args[0]
    assert written.unreviewed_code is True
    assert written.pipeline_commit_parent == "parent99"
    assert written.pipeline_commit == "abc1234"
    assert written.dit_commit == "def5678"
    assert written.worker_image == "gcr.io/foo/pipe-gaps:v0.9.6"
    assert written.workflow_file_sha1 == "wfsha1"
    assert written.experiment_id == "exp42"
    assert written.params == {"mode": "2_bfd", "x": 1}


def test_run_with_cache_stale_recomputes() -> None:
    """Row exists but its tables are gone -> treat as miss, recompute."""
    execute_fn = MagicMock()
    with (
        patch.object(dit_workflow, "read_cache", return_value=_cached_row()),
        patch.object(dit_workflow, "verify_tables_exist", return_value=[False]),
        patch.object(dit_workflow, "write_cache") as mock_write,
        patch.object(dit_workflow, "expires_at_for",
                     return_value=datetime(2026, 9, 1, tzinfo=timezone.utc)),
    ):
        result = dit_workflow.run_with_cache(
            execute_fn,
            ctx=_ctx(),
            workflow="wf",
            pipeline="pipe-gaps",
            experiment_id="exp01",
            cache_key=_cache_key(),
            output_fqn="proj.ds.fresh_table",
            execute_kwargs={},
        )
    execute_fn.assert_called_once()
    mock_write.assert_called_once()
    assert result == "proj.ds.fresh_table"


def test_run_with_cache_records_extra_output_tables() -> None:
    """Fix 4: extra_output_tables are recorded AFTER output_fqn (which stays
    first / the comparison target) so cleanup drops every produced table."""
    with (
        patch.object(dit_workflow, "read_cache", return_value=None),
        patch.object(dit_workflow, "verify_tables_exist", return_value=[True, True]),
        patch.object(dit_workflow, "write_cache") as mock_write,
        patch.object(dit_workflow, "expires_at_for",
                     return_value=datetime(2026, 9, 1, tzinfo=timezone.utc)),
    ):
        result = dit_workflow.run_with_cache(
            MagicMock(),
            ctx=_ctx(),
            workflow="wf",
            pipeline="anchorages_pipeline",
            experiment_id="exp01",
            cache_key=_cache_key(),
            output_fqn="proj.ds.visits",
            execute_kwargs={},
            extra_output_tables=("proj.ds.port_events",),
        )
    written = mock_write.call_args.args[0]
    # comparison FQN returned + first in the list; extras follow.
    assert result == "proj.ds.visits"
    assert written.output_tables == ["proj.ds.visits", "proj.ds.port_events"]


def test_run_with_cache_default_extras_keep_single_output_table() -> None:
    """pipe-gaps passes no extras -> byte-identical single-element list."""
    with (
        patch.object(dit_workflow, "read_cache", return_value=None),
        patch.object(dit_workflow, "verify_tables_exist", return_value=[True]),
        patch.object(dit_workflow, "write_cache") as mock_write,
        patch.object(dit_workflow, "expires_at_for",
                     return_value=datetime(2026, 9, 1, tzinfo=timezone.utc)),
    ):
        dit_workflow.run_with_cache(
            MagicMock(),
            ctx=_ctx(),
            workflow="wf",
            pipeline="pipe-gaps",
            experiment_id="exp01",
            cache_key=_cache_key(),
            output_fqn="proj.ds.only",
            execute_kwargs={},
        )
    written = mock_write.call_args.args[0]
    assert written.output_tables == ["proj.ds.only"]


def test_run_with_cache_dedupes_extra_equal_to_output_fqn() -> None:
    """A defensive guard: an extra equal to output_fqn isn't duplicated."""
    with (
        patch.object(dit_workflow, "read_cache", return_value=None),
        patch.object(dit_workflow, "verify_tables_exist", return_value=[True]),
        patch.object(dit_workflow, "write_cache") as mock_write,
        patch.object(dit_workflow, "expires_at_for",
                     return_value=datetime(2026, 9, 1, tzinfo=timezone.utc)),
    ):
        dit_workflow.run_with_cache(
            MagicMock(),
            ctx=_ctx(),
            workflow="wf",
            pipeline="pipe-gaps",
            experiment_id="exp01",
            cache_key=_cache_key(),
            output_fqn="proj.ds.t",
            execute_kwargs={},
            extra_output_tables=("proj.ds.t",),
        )
    written = mock_write.call_args.args[0]
    assert written.output_tables == ["proj.ds.t"]


def test_run_with_cache_empty_output_tables_treated_as_miss() -> None:
    """Degenerate 'succeeded' row with no output_tables must not IndexError;
    treat as a miss and recompute."""
    execute_fn = MagicMock()
    with (
        patch.object(dit_workflow, "read_cache",
                     return_value=_cached_row(output_tables=[])),
        patch.object(dit_workflow, "verify_tables_exist", return_value=[]),
        patch.object(dit_workflow, "write_cache") as mock_write,
        patch.object(dit_workflow, "expires_at_for",
                     return_value=datetime(2026, 9, 1, tzinfo=timezone.utc)),
    ):
        result = dit_workflow.run_with_cache(
            execute_fn,
            ctx=_ctx(),
            workflow="wf",
            pipeline="pipe-gaps",
            experiment_id="exp01",
            cache_key=_cache_key(),
            output_fqn="proj.ds.fresh_table",
            execute_kwargs={},
        )
    execute_fn.assert_called_once()
    mock_write.assert_called_once()
    assert result == "proj.ds.fresh_table"
