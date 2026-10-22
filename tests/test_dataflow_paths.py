"""Every dit Beam/Dataflow job writes its GCS temp and staging objects under ``dit/``.

The default bucket is also production's Dataflow temp bucket, where production
writes directly under ``dataflow_temp/`` and ``dataflow_staging/``; the prefix keeps
dit's objects separable. These tests pin the prefix in the shared helper and in
every place that builds Beam options: the in-process runner and each workflow.
"""

from __future__ import annotations

import sys
import types
from datetime import date
from unittest.mock import patch

import pytest

from dit import dataflow_paths
from dit.runners import dataflow as dit_dataflow
from workflows.encounters import ais as encounters_ais
from workflows.pipe_segment import identity_match_key as pipe_segment
from workflows.port_visits import ais as port_visits_ais

from .test_encounters_ais import _args as encounters_args
from .test_encounters_ais import _capture_slice as encounters_capture_slice


def _gcs_options(argv):
    return {a.split("=", 1)[0]: a.split("=", 1)[1]
            for a in argv if a.startswith(("--temp_location=", "--staging_location="))}


def test_helpers_put_every_location_under_the_dit_prefix():
    assert dataflow_paths.GCS_PREFIX == "dit"
    assert dataflow_paths.dataflow_temp_location("b") == "gs://b/dit/dataflow_temp"
    assert dataflow_paths.dataflow_staging_location("b") == "gs://b/dit/dataflow_staging"


@pytest.mark.parametrize("cli", ["v4", "v5"])
def test_port_visits_dataflow_jobs_use_the_prefix(cli):
    args = port_visits_ais.parse_args(["--pipeline-cli", cli, "--dataflow-temp-bucket", "b"])
    args.run_id, args.commit_sha, args.worker_image_tag, args.launched_by = "r", "c", "t", "u"
    calls = []
    with patch.object(port_visits_ais.dit_docker, "run",
                      side_effect=lambda *a, **k: calls.append(a[1]) or 0):
        port_visits_ais._run_slice(args, mode=port_visits_ais.MODE_BF,
                                   slice_start=date(2020, 1, 1), slice_end=date(2020, 1, 2),
                                   suffix="s", iteration=1, total_iterations=1)

    for argv in calls:
        assert _gcs_options(argv) == {
            "--temp_location": "gs://b/dit/dataflow_temp",
            "--staging_location": "gs://b/dit/dataflow_staging",
        }


@pytest.mark.parametrize("runner", ["dataflow", "docker"])
def test_encounters_jobs_use_the_prefix(runner):
    calls = encounters_capture_slice(
        encounters_args(runner=runner), mode=encounters_ais.MODE_BF,
        slice_start=date(2020, 1, 1), slice_end=date(2020, 1, 2),
        suffix="sfx", iteration=1, total_iterations=1,
    )

    for argv in calls:
        for value in _gcs_options(argv).values():
            assert value.startswith("gs://bucket/dit/"), argv


@pytest.mark.parametrize("runner", ["DataflowRunner", "DirectRunner"])
def test_pipe_segment_jobs_use_the_prefix_in_the_configured_bucket(runner):
    argv = pipe_segment._segment_info_args(
        dest_dataset="d", suffix="s", runner=runner, worker_image=None, service_account="sa",
    )
    bucket = pipe_segment.DEFAULT_DATAFLOW_TEMP_BUCKET

    assert _gcs_options(argv) == {
        "--temp_location": f"gs://{bucket}/dit/dataflow_temp",
        "--staging_location": f"gs://{bucket}/dit/dataflow_staging",
    }


def test_the_in_process_dataflow_runner_defaults_to_the_prefix(monkeypatch):
    # dit's venv has no apache_beam; the runner only needs PipelineState from it.
    beam_runner = types.ModuleType("apache_beam.runners.runner")
    beam_runner.PipelineState = types.SimpleNamespace(DONE="DONE")
    for name in ("apache_beam", "apache_beam.runners"):
        monkeypatch.setitem(sys.modules, name, types.ModuleType(name))
    monkeypatch.setitem(sys.modules, "apache_beam.runners.runner", beam_runner)

    captured = {}

    class _Stop(Exception):
        pass

    def builder(options):
        captured.update(options)
        raise _Stop

    with pytest.raises(_Stop):
        dit_dataflow.run(
            [], image_tag=None, service_account="sa", region="us-central1", temp_bucket="b",
            subnetwork="net", bq_temp_dataset="", pipeline_builder=builder,
        )

    assert captured["temp_location"] == "gs://b/dit/dataflow_temp"
    assert captured["staging_location"] == "gs://b/dit/dataflow_staging"
