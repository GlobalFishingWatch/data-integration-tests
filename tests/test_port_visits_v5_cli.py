"""Tests for ``--pipeline-cli`` in ``workflows/port_visits/ais.py`` (PIPELINE-4699).

v4 must keep emitting exactly what it did before the switch existed; v5 must
emit the CLI on pipe-anchorages main (port-state-transitions / port-visits,
hyphenated --bq-in-* / --bq-out-* flags, exclusive --end-date, labels on
every runner, no --wait_for_job) and hand the BigQuery staging dataset to the
overlay image through the container environment. No docker, BQ or Dataflow
calls: dit_docker.run is patched.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

import pytest

from workflows.port_visits import ais as mod
from workflows.port_visits import cross_version_ais as xv


def _args(*cli: str, runner: str = "dataflow", thinned: str | None = None):
    argv = ["--runner", runner, *cli]
    if thinned:
        argv += ["--steps", mod.STEP_VISITS, "--thinned-message-table", thinned]
    args = mod.parse_args(argv)
    # Normally stamped by main() from the resolved run context.
    args.run_id = "rid01"
    args.commit_sha = "abc1234"
    args.worker_image_tag = "tag"
    args.launched_by = "tester"
    return args


def _calls(args, *, slice_start=date(2020, 12, 29), slice_end=date(2020, 12, 29)):
    calls = []
    with patch.object(mod.dit_docker, "run",
                      side_effect=lambda *a, **k: calls.append((a, k)) or 0):
        mod._run_slice(args, mode=mod.MODE_BFD, slice_start=slice_start,
                       slice_end=slice_end, suffix="sfx", iteration=2, total_iterations=4)
    return calls


def _flag(argv, name):
    """Value of ``--name=value`` in argv (first occurrence), or None."""
    prefix = f"{name}="
    return next((a[len(prefix):] for a in argv if a.startswith(prefix)), None)


# --------------------------------------------------------------------------
# parsing
# --------------------------------------------------------------------------

def test_pipeline_cli_defaults_to_v4():
    assert mod.parse_args([]).pipeline_cli == mod.PIPELINE_CLI_V4


def test_pipeline_cli_rejects_unknown_values():
    with pytest.raises(SystemExit):
        mod.parse_args(["--pipeline-cli", "v6"])


# --------------------------------------------------------------------------
# v4: unchanged
# --------------------------------------------------------------------------

def test_v4_emits_the_v4_cli_with_inclusive_end_and_temp_dataset_flag():
    (thin_call, visits_call) = _calls(_args())
    (_, thin_argv), thin_kwargs = thin_call
    (_, visits_argv), visits_kwargs = visits_call

    assert thin_argv[0] == "thin_port_messages"
    assert _flag(thin_argv, "--end_date") == "2020-12-29"
    assert _flag(thin_argv, "--temp_dataset") == mod.DEFAULT_BQ_TEMP_DATASET
    assert visits_argv[0] == "port_visits"
    assert _flag(visits_argv, "--end_date") == "2020-12-29"
    assert "--wait_for_job" in thin_argv and "--wait_for_job" in visits_argv
    assert "container_env" not in thin_kwargs and "container_env" not in visits_kwargs


def test_v4_directrunner_options_are_unchanged():
    (thin_call, _) = _calls(_args(runner="docker"))
    (_, thin_argv), _ = thin_call

    assert thin_argv[-2:] == ["--runner=DirectRunner", "--wait_for_job"]


# --------------------------------------------------------------------------
# v5
# --------------------------------------------------------------------------

def test_v5_step_1_emits_port_state_transitions_with_exclusive_end():
    (thin_call, _) = _calls(_args("--pipeline-cli", "v5"),
                            slice_start=date(2020, 12, 1), slice_end=date(2020, 12, 29))
    (_, thin_argv), _ = thin_call

    assert thin_argv[:6] == [
        "port-state-transitions",
        "--start-date=2020-12-01",
        "--end-date=2020-12-30",
        f"--bq-in-named-anchorages={mod.DEFAULT_NAMED_ANCHORAGES}",
        f"--bq-in-messages={mod.DEFAULT_SOURCE_MESSAGES_FQN}",
        f"--bq-out-port-state-transitions={mod.PROJECT}.tech_great_expectations"
        f".port_events_sfx_{mod.MODE_BFD}",
    ]


def test_v5_one_day_slice_never_passes_end_equal_to_start():
    # The v5 CLI rejects end_date <= start_date; a one-day slice is [d, d+1).
    (thin_call, _) = _calls(_args("--pipeline-cli", "v5"))
    (_, thin_argv), _ = thin_call

    assert _flag(thin_argv, "--start-date") == "2020-12-29"
    assert _flag(thin_argv, "--end-date") == "2020-12-30"


def test_v5_step_2_emits_port_visits_over_the_full_history():
    (_, visits_call) = _calls(_args("--pipeline-cli", "v5"))
    (_, visits_argv), _ = visits_call

    assert visits_argv[:7] == [
        "port-visits",
        f"--start-date={mod.DEFAULT_START}",
        "--end-date=2020-12-30",
        f"--bq-in-port-state-transitions={mod.PROJECT}.tech_great_expectations"
        f".port_events_sfx_{mod.MODE_BFD}",
        f"--bq-in-segment-info={mod.DEFAULT_SOURCE_SEGMENT_INFO_FQN}",
        f"--bq-out-port-visits={mod.PROJECT}.tech_great_expectations"
        f".port_visits_sfx_{mod.MODE_BFD}",
        f"--bad-segs=(SELECT DISTINCT seg_id FROM `{mod.DEFAULT_SOURCE_SEGS_ACTIVITY_FQN}` "
        "WHERE overlapping_and_short)",
    ]


def test_v5_reads_an_external_thinned_table_and_skips_step_1():
    calls = _calls(_args("--pipeline-cli", "v5", thinned="p.d.slice"))

    assert len(calls) == 1
    (_, visits_argv), _ = calls[0]
    assert _flag(visits_argv, "--bq-in-port-state-transitions") == "p.d.slice"


@pytest.mark.parametrize("runner", ["dataflow", "docker"])
def test_v5_never_passes_v4_only_flags(runner):
    for (_, argv), _ in _calls(_args("--pipeline-cli", "v5", runner=runner)):
        assert "--wait_for_job" not in argv
        assert _flag(argv, "--temp_dataset") is None
        assert not any(a.startswith(("--start_date", "--end_date", "--output_table")) for a in argv)


@pytest.mark.parametrize("runner", ["dataflow", "docker"])
def test_v5_passes_labels_on_every_runner(runner):
    for (_, argv), _ in _calls(_args("--pipeline-cli", "v5", runner=runner)):
        labels = [a for a in argv if a.startswith("--labels=")]
        assert "--labels=resource_creator=dit" in labels
        assert "--labels=dit_run_id=rid01" in labels


def test_v5_directrunner_gets_project_and_temp_location():
    args = _args("--pipeline-cli", "v5", runner="docker")
    for (_, argv), _ in _calls(args):
        assert f"--project={mod.PROJECT}" in argv
        assert _flag(argv, "--temp_location") == f"gs://{args.dataflow_temp_bucket}/dit/dataflow_temp"


def test_v5_hands_the_temp_dataset_to_the_overlay_through_the_container_env():
    args = _args("--pipeline-cli", "v5", "--bq-temp-dataset", "p.staging")
    for _, kwargs in _calls(args):
        assert kwargs["container_env"] == {mod.TEMP_DATASET_ENV: "p.staging"}
        assert kwargs["entrypoint"] == "pipe-anchorages"


@pytest.mark.parametrize("cli", ["v4", "v5"])
@pytest.mark.parametrize("build_from_source", [False, True])
def test_every_docker_call_mounts_the_gcp_credentials_volume(cli, build_from_source):
    # A published-image `docker run` (the only way to run the v5 overlay) has
    # no credentials otherwise; encounters / pipe-events use the same spec.
    cli_args = ["--pipeline-cli", cli, *(["--build-from-source"] if build_from_source else [])]
    calls = _calls(_args(*cli_args))
    assert calls
    for _, kwargs in calls:
        assert kwargs["volumes"] == [mod.GCP_VOLUME] == ["gcp:/root/.config"]


# --------------------------------------------------------------------------
# worker cap
# --------------------------------------------------------------------------

@pytest.mark.parametrize("cli", ["v4", "v5"])
def test_dataflow_jobs_are_capped_at_the_staging_default(cli):
    for (_, argv), _ in _calls(_args("--pipeline-cli", cli)):
        assert _flag(argv, "--max_num_workers") == str(mod.DEFAULT_MAX_NUM_WORKERS)
    assert mod.DEFAULT_MAX_NUM_WORKERS == 4


def test_the_worker_cap_can_be_raised_for_production_slices():
    for (_, argv), _ in _calls(_args("--pipeline-cli", "v5", "--max-num-workers", "16")):
        assert _flag(argv, "--max_num_workers") == "16"


def test_the_docker_runner_gets_no_worker_cap():
    for (_, argv), _ in _calls(_args(runner="docker")):
        assert _flag(argv, "--max_num_workers") is None


def test_the_worker_cap_is_not_part_of_the_cache_key():
    four = mod.canonical_params_dict(_args(), mod.MODE_BF)
    sixteen = mod.canonical_params_dict(_args("--max-num-workers", "16"), mod.MODE_BF)

    assert four == sixteen


def test_a_worker_cap_below_one_is_rejected():
    with pytest.raises(SystemExit, match="--max-num-workers must be >= 1"):
        mod.main(["--max-num-workers", "0", "--skip-pipelines", "--skip-comparisons"])


def test_cross_version_forwards_the_worker_cap_to_every_binding():
    argv = xv._ais_args_for_binding(
        ["--max-num-workers", "16"],
        pinned=xv._PinnedSources(
            start=date(2020, 1, 1), end=date(2020, 12, 31),
            messages_positions="p.d.m", segment_info="p.d.si", segs_activity="p.d.sa",
        ),
        suffix="s", experiment_id="e", binding_name="after", modes=[mod.MODE_BF],
    )

    assert argv[argv.index("--max-num-workers") + 1] == "16"


# --------------------------------------------------------------------------
# cache key
# --------------------------------------------------------------------------

def test_v4_cache_key_params_have_no_pipeline_cli_field():
    assert "pipeline_cli" not in mod.canonical_params_dict(_args(), mod.MODE_BF)


def test_v5_cache_key_params_differ_from_v4():
    v4 = mod.canonical_params_dict(_args(), mod.MODE_BF)
    v5 = mod.canonical_params_dict(_args("--pipeline-cli", "v5"), mod.MODE_BF)

    assert v5 == {**v4, "pipeline_cli": mod.PIPELINE_CLI_V5}


# --------------------------------------------------------------------------
# cross_version_ais: per-binding submitter image
# --------------------------------------------------------------------------

_PINNED = xv._PinnedSources(
    start=date(2020, 1, 1), end=date(2020, 12, 31),
    messages_positions="p.d.m", segment_info="p.d.si", segs_activity="p.d.sa",
)


def test_binding_image_tag_replaces_a_user_supplied_image_tag():
    argv = xv._ais_args_for_binding(
        ["--image-tag", "user/image:1", "--pipeline-cli", "v5"],
        pinned=_PINNED, suffix="s", experiment_id="e", binding_name="after",
        modes=[mod.MODE_BF], image_tag="dit/overlay:after",
    )

    assert "user/image:1" not in argv
    assert argv[argv.index("--image-tag") + 1] == "dit/overlay:after"
    assert argv[argv.index("--pipeline-cli") + 1] == "v5"


def test_without_binding_image_tag_the_user_image_tag_passes_through():
    argv = xv._ais_args_for_binding(
        ["--image-tag", "user/image:1"],
        pinned=_PINNED, suffix="s", experiment_id="e", binding_name="before",
        modes=[mod.MODE_BF],
    )

    assert argv[argv.index("--image-tag") + 1] == "user/image:1"


def test_binding_image_tag_for_an_unknown_binding_is_rejected():
    with pytest.raises(SystemExit, match="--binding-image-tag references unknown"):
        xv.parse_args([
            "--experiment-id", "e",
            "--binding", "before=main", "--binding-image-tag", "after=dit/overlay:x",
        ])
