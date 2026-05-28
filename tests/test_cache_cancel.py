"""Tests for the M5a cleanup control plane in ``dit.cache``.

Covers ``read_rows_for_run`` and ``cancel_run`` with mocked ``gcloud``
(via ``subprocess.run``) and a mocked ``bigquery.Client``. No live BQ /
Dataflow: cancel_run never touches real infra here.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from dit import cache as dit_cache
from dit.cache import (
    STATUS_CANCELLED,
    STATUS_RUNNING,
    CachedRun,
    cancel_run,
    read_rows_for_run,
)

# --------------------------------------------------------------------------
# Fixtures / builders
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
        params={"mode": "1_bf"},
        output_tables=["world-fishing-827.tech_great_expectations.bf_table"],
        dataflow_job_ids=[],
        cloud_build_id=None,
        started_at=datetime(2026, 5, 22, tzinfo=timezone.utc),
        finished_at=None,
        status=STATUS_RUNNING,
        expires_at=datetime(2026, 9, 22, tzinfo=timezone.utc),
    )
    base.update(overrides)
    return CachedRun(**base)


def _bq_row(**overrides: Any) -> Any:
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
        params_json={"mode": "1_bf"},
        output_tables=["world-fishing-827.tech_great_expectations.bf_table"],
        dataflow_job_ids=[],
        cloud_build_id=None,
        started_at=datetime(2026, 5, 22, tzinfo=timezone.utc),
        finished_at=None,
        status=STATUS_RUNNING,
        expires_at=datetime(2026, 9, 22, tzinfo=timezone.utc),
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _gcloud_jobs(*jobs: dict[str, str]) -> SimpleNamespace:
    """A fake completed subprocess for `gcloud dataflow jobs list`."""
    return SimpleNamespace(returncode=0, stdout=json.dumps(list(jobs)), stderr="")


# --------------------------------------------------------------------------
# read_rows_for_run
# --------------------------------------------------------------------------

def test_read_rows_for_run_returns_all_rows() -> None:
    rows = [_bq_row(cache_key="k1"), _bq_row(cache_key="k2")]
    client = MagicMock()
    client.query.return_value.result.return_value = iter(rows)
    out = read_rows_for_run("rid01", client=client)
    assert len(out) == 2
    assert {r.cache_key for r in out} == {"k1", "k2"}


def test_read_rows_for_run_does_not_filter_status_or_expiry() -> None:
    # The cleanup path must see running + cancelled + expired rows too.
    client = MagicMock()
    client.query.return_value.result.return_value = iter([])
    read_rows_for_run("rid01", client=client)
    sql = client.query.call_args[0][0]
    assert "run_id = @run_id" in sql
    assert "status" not in sql  # no status filter
    assert "expires_at" not in sql  # no expiry filter


# --------------------------------------------------------------------------
# cancel_run — happy path
# --------------------------------------------------------------------------

def _patch_rows(rows: list[CachedRun]):
    return patch.object(dit_cache, "read_rows_for_run", return_value=rows)


def test_cancel_run_no_rows_and_no_jobs_raises() -> None:
    # A genuinely-unknown run_id: no cache rows AND no labelled Dataflow jobs.
    # That (and only that) surfaces loudly.
    client = MagicMock()
    run = MagicMock(return_value=_gcloud_jobs())  # empty jobs list
    with _patch_rows([]), patch.object(dit_cache.subprocess, "run", run):
        with pytest.raises(ValueError, match="no rows and no labelled"):
            cancel_run("missing", client=client, region="us-central1")


def test_cancel_run_no_rows_but_labelled_job_cancels_without_raising() -> None:
    # FUNCTIONAL GAP (Fix 2): an in-flight run has a live dit_run_id-labelled
    # Dataflow job but NO cache row yet (rows are written only on mode
    # completion). cancel_run must cancel the job and NOT raise.
    client = MagicMock()
    run = MagicMock()
    run.side_effect = [
        _gcloud_jobs({"id": "job-inflight", "name": "n", "state": "Running"}),
        SimpleNamespace(returncode=0, stdout="", stderr=""),  # cancel
    ]
    with _patch_rows([]), patch.object(dit_cache.subprocess, "run", run):
        cancel_run("rid01", client=client, region="us-central1")  # no raise
    # The in-flight job was cancelled.
    cancel_argv = run.call_args_list[1].args[0]
    assert "cancel" in cancel_argv and "job-inflight" in cancel_argv
    # No row -> nothing to delete, no UPDATE issued.
    client.delete_table.assert_not_called()
    assert not any("UPDATE" in c.args[0] for c in client.query.call_args_list)


def test_cancel_run_discovers_jobs_by_label_and_cancels_running() -> None:
    client = MagicMock()
    rows = [_cached_run()]
    run = MagicMock()
    # First call: jobs list; subsequent: cancel calls.
    run.side_effect = [
        _gcloud_jobs(
            {"id": "job-running", "name": "n1", "state": "Running"},
            {"id": "job-done", "name": "n2", "state": "Done"},
        ),
        SimpleNamespace(returncode=0, stdout="", stderr=""),  # cancel job-running
    ]
    with _patch_rows(rows), patch.object(dit_cache.subprocess, "run", run):
        cancel_run("rid01", client=client, region="europe-west1")

    # The list call filters by the dit_run_id label, in the right region.
    list_call = run.call_args_list[0]
    list_argv = list_call.args[0]
    assert "jobs" in list_argv and "list" in list_argv
    assert "--filter=labels.dit_run_id=rid01" in list_argv
    assert "--region=europe-west1" in list_argv
    # Only the Running job is cancelled; the Done job is skipped.
    cancel_call = run.call_args_list[1]
    cancel_argv = cancel_call.args[0]
    assert "cancel" in cancel_argv
    assert "job-running" in cancel_argv
    assert "job-done" not in cancel_argv
    # Exactly two subprocess calls: one list, one cancel.
    assert run.call_count == 2


def test_cancel_run_deletes_output_tables() -> None:
    client = MagicMock()
    rows = [_cached_run(output_tables=["proj.ds.t_bf", "proj.ds.t_bfd"])]
    run = MagicMock(return_value=_gcloud_jobs())  # no jobs found
    with _patch_rows(rows), patch.object(dit_cache.subprocess, "run", run):
        cancel_run("rid01", client=client, region="us-central1")
    deleted = {c.args[0] for c in client.delete_table.call_args_list}
    assert deleted == {"proj.ds.t_bf", "proj.ds.t_bfd"}
    # not_found_ok keeps deletion idempotent.
    for c in client.delete_table.call_args_list:
        assert c.kwargs.get("not_found_ok") is True


def test_cancel_run_deletes_both_visits_and_port_events() -> None:
    # Fix 4: a port-visits cache row records BOTH the visits table and the
    # per-mode thinned port_events intermediate; cancel_run must delete both
    # so the intermediate isn't orphaned.
    client = MagicMock()
    rows = [
        _cached_run(
            workflow="workflows/port_visits/ais.py",
            pipeline="anchorages_pipeline",
            output_tables=[
                "world-fishing-827.tech_great_expectations.port_visits_exp_c_u_1_bf",
                "world-fishing-827.tech_great_expectations.port_events_exp_c_u_1_bf",
            ],
        )
    ]
    run = MagicMock(return_value=_gcloud_jobs())  # no jobs found
    with _patch_rows(rows), patch.object(dit_cache.subprocess, "run", run):
        cancel_run("rid01", client=client, region="us-central1")
    deleted = {c.args[0] for c in client.delete_table.call_args_list}
    assert deleted == {
        "world-fishing-827.tech_great_expectations.port_visits_exp_c_u_1_bf",
        "world-fishing-827.tech_great_expectations.port_events_exp_c_u_1_bf",
    }


def test_cancel_run_dedupes_tables_across_rows() -> None:
    # Two modes whose rows reference the same table -> deleted once.
    client = MagicMock()
    rows = [
        _cached_run(cache_key="k1", output_tables=["proj.ds.shared"]),
        _cached_run(cache_key="k2", output_tables=["proj.ds.shared"]),
    ]
    run = MagicMock(return_value=_gcloud_jobs())
    with _patch_rows(rows), patch.object(dit_cache.subprocess, "run", run):
        cancel_run("rid01", client=client, region="us-central1")
    assert client.delete_table.call_count == 1


def test_cancel_run_skips_non_table_values() -> None:
    # SAFETY: a dataset-shaped value (project.dataset) or anything that is not
    # a fully-qualified project.dataset.table must be skipped, never deleted.
    client = MagicMock()
    rows = [
        _cached_run(
            output_tables=[
                "world-fishing-827.dit_exp_pipeline_1465_internal",  # dataset!
                "proj.ds.real_table",  # valid table
                "bare_name",  # malformed
            ]
        )
    ]
    run = MagicMock(return_value=_gcloud_jobs())
    with _patch_rows(rows), patch.object(dit_cache.subprocess, "run", run):
        cancel_run("rid01", client=client, region="us-central1")
    deleted = {c.args[0] for c in client.delete_table.call_args_list}
    assert deleted == {"proj.ds.real_table"}
    # The dataset-shaped + malformed values were NOT passed to delete_table.
    assert "world-fishing-827.dit_exp_pipeline_1465_internal" not in deleted
    assert "bare_name" not in deleted


def test_cancel_run_marks_rows_cancelled() -> None:
    client = MagicMock()
    rows = [_cached_run()]
    run = MagicMock(return_value=_gcloud_jobs())
    with _patch_rows(rows), patch.object(dit_cache.subprocess, "run", run):
        cancel_run("rid01", client=client, region="us-central1")
    # The DML UPDATE query is the one issued on the client.
    update_calls = [
        c for c in client.query.call_args_list
        if "UPDATE" in c.args[0]
    ]
    assert len(update_calls) == 1
    sql = update_calls[0].args[0]
    assert "SET status = @cancelled" in sql
    assert "WHERE run_id = @run_id" in sql
    params = {p.name: p.value for p in update_calls[0].kwargs["job_config"].query_parameters}
    assert params["cancelled"] == STATUS_CANCELLED
    assert params["run_id"] == "rid01"
    client.query.return_value.result.assert_called()


def test_cancel_run_region_defaults_to_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DIT_DATAFLOW_REGION", "asia-east1")
    client = MagicMock()
    rows = [_cached_run(output_tables=[])]
    run = MagicMock(return_value=_gcloud_jobs())
    with _patch_rows(rows), patch.object(dit_cache.subprocess, "run", run):
        cancel_run("rid01", client=client)  # no explicit region
    list_argv = run.call_args_list[0].args[0]
    assert "--region=asia-east1" in list_argv


def test_cancel_run_tolerates_gcloud_list_failure() -> None:
    # A failed jobs-list shouldn't abort cleanup: tables still get dropped and
    # the rows still get marked cancelled.
    client = MagicMock()
    rows = [_cached_run(output_tables=["proj.ds.t"])]
    run = MagicMock(return_value=SimpleNamespace(returncode=1, stdout="", stderr="boom"))
    with _patch_rows(rows), patch.object(dit_cache.subprocess, "run", run):
        cancel_run("rid01", client=client, region="us-central1")
    client.delete_table.assert_called_once()
    assert any("UPDATE" in c.args[0] for c in client.query.call_args_list)


def test_cancel_run_no_rows_and_discovery_failed_raises_runtime() -> None:
    # A gcloud jobs-list FAILURE with no cache rows must NOT be reported as an
    # unknown run_id: the run may exist with live jobs we just couldn't list.
    # It surfaces as a RuntimeError pointing at auth/region -- distinct from the
    # ValueError raised when discovery SUCCEEDS but finds nothing (genuine typo).
    client = MagicMock()
    run = MagicMock(return_value=SimpleNamespace(returncode=1, stdout="", stderr="boom"))
    with _patch_rows([]), patch.object(dit_cache.subprocess, "run", run):
        with pytest.raises(RuntimeError, match="could not list Dataflow jobs"):
            cancel_run("rid01", client=client, region="us-central1")
    # Nothing destructive happened: no table deletes, no status UPDATE.
    client.delete_table.assert_not_called()
    assert not any("UPDATE" in c.args[0] for c in client.query.call_args_list)


def test_cancel_run_cancels_queued_job() -> None:
    # ROBUSTNESS (Fix 3): a Queued job is non-terminal -- it would start later
    # if left alone -- so it must be cancelled, not skipped.
    client = MagicMock()
    rows = [_cached_run()]
    run = MagicMock()
    run.side_effect = [
        _gcloud_jobs({"id": "job-queued", "name": "n", "state": "Queued"}),
        SimpleNamespace(returncode=0, stdout="", stderr=""),  # cancel
    ]
    with _patch_rows(rows), patch.object(dit_cache.subprocess, "run", run):
        cancel_run("rid01", client=client, region="us-central1")
    cancel_argv = run.call_args_list[1].args[0]
    assert "cancel" in cancel_argv and "job-queued" in cancel_argv
    assert run.call_count == 2  # list + cancel


def test_cancel_run_skips_terminal_done_job() -> None:
    # A terminal Done job is a no-op to cancel -> skipped (no cancel subprocess).
    client = MagicMock()
    rows = [_cached_run()]
    run = MagicMock(
        return_value=_gcloud_jobs({"id": "job-done", "name": "n", "state": "Done"})
    )
    with _patch_rows(rows), patch.object(dit_cache.subprocess, "run", run):
        cancel_run("rid01", client=client, region="us-central1")
    assert run.call_count == 1  # list only; no cancel for a terminal job


def test_cancel_run_idempotent_when_jobs_terminal() -> None:
    # Re-running on an already-cancelled run: jobs are terminal, no cancel
    # subprocess call is made, but tables + status update still run idempotently.
    client = MagicMock()
    rows = [_cached_run(status=STATUS_CANCELLED, output_tables=["proj.ds.t"])]
    run = MagicMock(
        return_value=_gcloud_jobs({"id": "j1", "name": "n", "state": "Cancelled"})
    )
    with _patch_rows(rows), patch.object(dit_cache.subprocess, "run", run):
        cancel_run("rid01", client=client, region="us-central1")
    # Only the list call; no cancel call for a terminal job.
    assert run.call_count == 1
