"""Tests for ``dit.production``: finding the production Dataflow job whose
start and end pin a production-baseline run (PIPELINE-4703).

Mock-based: ``subprocess.run`` stands in for ``gcloud dataflow jobs list``,
whose JSON keys (``creationTime``, ``stateTime``, ``state``) these tests pin.
"""

from __future__ import annotations

import json
import subprocess
from datetime import date, datetime, timezone
from unittest.mock import patch

import pytest

from dit import production

_NAME = "core-ais-v5--port-visits-detect-port-visits--20261020"


def _job(job_id: str, state: str, created: str, state_time: str, name: str = _NAME) -> dict:
    return {
        "id": job_id, "name": name, "state": state,
        "creationTime": created, "stateTime": state_time,
    }


def _find(jobs: list[dict], *, returncode: int = 0, stderr: str = ""):
    completed = subprocess.CompletedProcess(
        args=[], returncode=returncode, stdout=json.dumps(jobs), stderr=stderr,
    )
    with patch("dit.production.subprocess.run", return_value=completed) as run:
        found = production.find_dataflow_job(
            project="world-fishing-827", region="us-central1",
            name=_NAME, created_after=date(2026, 10, 19),
        )
    return found, run


def test_find_dataflow_job_returns_the_done_job_with_utc_times() -> None:
    found, run = _find([
        _job("2026-10-23_00_46_26-1", "Done", "2026-10-23 07:46:26", "2026-10-23 09:04:17"),
    ])

    assert found == production.ProductionRun(
        job_id="2026-10-23_00_46_26-1",
        name=_NAME,
        create_time=datetime(2026, 10, 23, 7, 46, 26, tzinfo=timezone.utc),
        end_time=datetime(2026, 10, 23, 9, 4, 17, tzinfo=timezone.utc),
    )
    cmd = run.call_args.args[0]
    assert cmd[:4] == ["gcloud", "dataflow", "jobs", "list"]
    assert "--project=world-fishing-827" in cmd
    assert "--region=us-central1" in cmd
    assert "--created-after=2026-10-19" in cmd
    assert f"--filter=name={_NAME}" in cmd


def test_find_dataflow_job_takes_the_latest_done_retry() -> None:
    """A cleared task or retry reuses the job name; the latest finished one
    wrote the table. A later failed attempt does not count."""
    found, _ = _find([
        _job("first", "Done", "2026-10-22 07:53:20", "2026-10-22 09:19:13"),
        _job("second", "Done", "2026-10-23 07:46:26", "2026-10-23 09:04:17"),
        _job("third", "Failed", "2026-10-23 10:00:00", "2026-10-23 10:30:00"),
    ])

    assert found.job_id == "second"


def test_find_dataflow_job_ignores_names_that_only_contain_the_filter() -> None:
    """gcloud's name filter matches substrings; only the exact name counts."""
    with pytest.raises(RuntimeError, match="found: none"):
        _find([_job("x", "Done", "2026-10-23 07:46:26", "2026-10-23 09:04:17",
                    name=_NAME + "-rerun")])


def test_find_dataflow_job_rejects_a_run_that_has_not_finished() -> None:
    with pytest.raises(RuntimeError, match=r"no finished \(Done\).*found: j1=Running"):
        _find([_job("j1", "Running", "2026-10-23 07:46:26", "2026-10-23 07:50:00")])


def test_find_dataflow_job_surfaces_gcloud_failures() -> None:
    with pytest.raises(RuntimeError, match="rc=1.*PERMISSION_DENIED"):
        _find([], returncode=1, stderr="PERMISSION_DENIED")


def test_default_created_after_is_the_day_before_the_production_date() -> None:
    assert production.default_created_after(date(2026, 10, 20)) == date(2026, 10, 19)
