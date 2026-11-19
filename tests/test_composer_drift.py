"""Tests for the workflow drift specs and ``workflows/composer_drift.py`` (PIPELINE-4700).

Two things are covered here; the comparison itself is tested in
``tests/test_drift.py``.

* Each spec's token builder runs offline (no subprocess, no docker, no
  pipeline package) and emits exactly what its workflow hands to the runner.
* The runner's selection, summary and exit codes.

The production DAGs are synthetic. For most specs they are built from DIT's own
tokens, so production "runs what DIT runs" and every level matches; the port
visits tasks come from that spec's own test fixture. Images and infrastructure
values are invented (``example.invalid``). Nothing is copied from composer.
"""

from __future__ import annotations

import subprocess
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from dit import composer, drift
from dit.workflow import RunContext
from workflows import composer_drift as runner
from workflows.encounters import ais as encounters_ais
from workflows.encounters import composer_drift as encounters_drift
from workflows.encounters import vms as encounters_vms
from workflows.pipe_events import composer_drift as events_drift
from workflows.pipe_events import fishing
from workflows.pipe_gaps import composer_drift as gaps_drift
from workflows.pipe_gaps import mode_equivalence as gaps_modes
from workflows.pipe_segment import composer_drift as segment_drift
from workflows.pipe_segment import identity_match_key as segment

from .test_port_visits_composer_drift import _raw_dag as _port_visits_raw_dag

#: A ten-day run, so inclusive and exclusive end dates differ.
PLACEHOLDERS = {
    "start_date": date(2026, 3, 1),
    "end_date": date(2026, 3, 10),
    "end_date_exclusive": date(2026, 3, 11),
}

SPECS = {spec.name: spec for spec in runner.SPECS}

#: Every step of every spec and the entrypoint and command its tokens name.
COMMANDS = {
    ("port_visits/ais", "thin_port_messages"): ("pipe-anchorages", "port-state-transitions"),
    ("port_visits/ais", "port_visits"): ("pipe-anchorages", "port-visits"),
    ("encounters/ais", "create_raw_encounters"): ("pipe-encounters", "create_raw_encounters"),
    ("encounters/ais", "merge_encounters"): ("pipe-encounters", "merge_encounters"),
    ("encounters/vms", "create_raw_encounters"): ("pipe-encounters", "create_raw_encounters"),
    ("encounters/vms", "merge_encounters"): ("pipe-encounters", "merge_encounters"),
    ("pipe_events/fishing", "incremental_events[nnet_score]"): ("pipe", "incremental_events"),
    ("pipe_events/fishing", "incremental_events[night_loitering]"):
        ("pipe", "incremental_events"),
    ("pipe_events/fishing", "incremental_filter_events[nnet_score]"):
        ("pipe", "incremental_filter_events"),
    ("pipe_events/fishing", "incremental_filter_events[night_loitering]"):
        ("pipe", "incremental_filter_events"),
    ("pipe_events/fishing", "auth_and_regions_fishing_events"):
        ("pipe", "auth_and_regions_fishing_events"),
    ("pipe_events/fishing", "fishing_restrictive"): ("pipe", "fishing_restrictive"),
    ("pipe_gaps/mode_equivalence", "detect"): ("pipe-gaps", "detect"),
    ("pipe_segment/identity_match_key", "segment"): ("pipe-segment", "segment"),
    ("pipe_segment/identity_match_key", "segment_identity"):
        ("pipe-segment", "segment_identity"),
    ("pipe_segment/identity_match_key", "segment_vessel_daily"):
        ("pipe-segment", "segment_vessel_daily"),
    ("pipe_segment/identity_match_key", "segment_info"): ("pipe-segment", "segment_info"),
    ("pipe_segment/identity_match_key", "vessel_info"): ("pipe-segment", "vessel_info"),
    ("pipe_segment/identity_match_key", "segment_vessel"): ("pipe-segment", "segment_vessel"),
}

#: The workflows marked in sync with production: drift in one of them exits 1.
IN_SYNC = ["port_visits/ais", "pipe_segment/identity_match_key"]


def _steps() -> list[tuple[str, str]]:
    return [(spec.name, step.step) for spec in runner.SPECS for step in spec.steps]


def _step(workflow: str, step: str) -> drift.StepSpec:
    (found,) = [s for s in SPECS[workflow].steps if s.step == step]
    return found


def _parsed(workflow: str, step: str, placeholders=PLACEHOLDERS) -> composer.ParsedArgv:
    spec = SPECS[workflow]
    return composer.parse_argv(
        _step(workflow, step).tokens(placeholders), multi=spec.multi_value, bare=spec.bare,
    )


@pytest.fixture
def offline(monkeypatch):
    """Fail on any subprocess: no git, docker, gcloud or bq."""
    def refuse(*args, **kwargs):
        raise AssertionError(f"a drift spec must build its tokens offline; ran {args}")

    monkeypatch.setattr(subprocess, "run", refuse)
    monkeypatch.setattr(subprocess, "Popen", refuse)


# --------------------------------------------------------------------------
# the registry
# --------------------------------------------------------------------------

def test_every_workflow_has_a_spec_and_two_are_marked_in_sync() -> None:
    assert [spec.name for spec in runner.SPECS] == [
        "port_visits/ais", "encounters/ais", "encounters/vms", "pipe_events/fishing",
        "pipe_gaps/mode_equivalence", "pipe_segment/identity_match_key",
    ]
    assert [spec.name for spec in runner.SPECS if spec.in_sync] == IN_SYNC
    assert all((Path(__file__).parents[1] / spec.workflow).is_file() for spec in runner.SPECS)
    # A workflow not yet in sync says why.
    assert all(spec.note for spec in runner.SPECS if not spec.in_sync)


def test_every_spec_names_its_snapshot_and_explains_what_it_accepts() -> None:
    assert {spec.name: spec.snapshot for spec in runner.SPECS} == {
        "port_visits/ais": "core_ais_v5",
        "encounters/ais": "core_ais_v5",
        "encounters/vms": "core_vms_v5_daily",
        "pipe_events/fishing": "core_ais_v5",
        "pipe_gaps/mode_equivalence": "core_ais_v5",
        "pipe_segment/identity_match_key": "core_ais_v3",
    }
    assert set(_steps()) == set(COMMANDS)
    for spec in runner.SPECS:
        assert len({step.step for step in spec.steps}) == len(spec.steps)
        assert all(reason for step in spec.steps for reason in spec.reasons(step).values())


# --------------------------------------------------------------------------
# token builders: offline, and naming the right command
# --------------------------------------------------------------------------

@pytest.mark.parametrize("workflow, step", _steps())
def test_tokens_are_built_offline_and_name_the_command(workflow, step, offline) -> None:
    assert _parsed(workflow, step).positionals == COMMANDS[workflow, step]


@pytest.mark.parametrize("workflow, step", _steps())
def test_default_image_is_a_tagged_image_of_the_pipeline(workflow, step) -> None:
    spec = _step(workflow, step)

    assert spec.pipeline_version and spec.pipeline_version.startswith("v")
    assert spec.image_name in {
        "pipe-anchorages", "pipe-encounters", "github-globalfishingwatch-pipe-events",
        "pipe-gaps", "pipe-segment",
    }


@pytest.mark.parametrize("workflow", ["encounters/ais", "encounters/vms"])
def test_encounters_tokens_carry_the_runs_inclusive_dates(workflow) -> None:
    create = _parsed(workflow, "create_raw_encounters")
    merge = _parsed(workflow, "merge_encounters")

    assert (create.value("--start_date"), create.value("--end_date")) == (
        "2026-03-01", "2026-03-10",
    )
    assert (merge.value("--start_date"), merge.value("--end_date")) == (
        "2026-03-01", "2026-03-10",
    )
    assert create.value("--runner") == "DataflowRunner"


def test_the_two_encounters_specs_differ_in_their_workflows_defaults() -> None:
    ais = _parsed("encounters/ais", "create_raw_encounters")
    vms = _parsed("encounters/vms", "create_raw_encounters")

    assert ais.value("--source_table") == encounters_ais.DEFAULT_SOURCE_MESSAGES_FQN
    assert vms.value("--source_table") == encounters_vms.DEFAULT_SOURCE_MESSAGES_FQN
    assert "workflow=encounters_ais" in ais.values("--labels")
    assert "workflow=encounters_vms" in vms.values("--labels")


@pytest.mark.parametrize("module, workflow", [
    (encounters_ais, "encounters/ais"), (encounters_vms, "encounters/vms"),
])
def test_encounters_tokens_are_what_the_workflow_hands_the_runner(module, workflow) -> None:
    args = encounters_drift._dit_args(module, PLACEHOLDERS)
    calls = []
    with patch.object(encounters_ais.dit_docker, "run",
                      side_effect=lambda image, argv, **kw: calls.append((argv, kw)) or 0):
        encounters_ais._run_slice(
            args, mode=encounters_ais.MODE_BF, suffix="drift", iteration=1, total_iterations=1,
            slice_start=PLACEHOLDERS["start_date"], slice_end=PLACEHOLDERS["end_date"],
        )

    assert [[kw["entrypoint"], *argv] for argv, kw in calls] == [
        _step(workflow, "create_raw_encounters").tokens(PLACEHOLDERS),
        _step(workflow, "merge_encounters").tokens(PLACEHOLDERS),
    ]


def test_pipe_events_tokens_are_what_the_workflow_hands_the_runner() -> None:
    args = events_drift._dit_args(PLACEHOLDERS)
    calls = []
    with patch.object(fishing.dit_docker, "run",
                      side_effect=lambda image, argv, **kw: calls.append((argv, kw)) or 0):
        fishing._run_slice(
            args, mode=fishing.MODE_BF, suffix="drift", iteration=1, total_iterations=1,
            slice_start=PLACEHOLDERS["start_date"],
            slice_end=PLACEHOLDERS["end_date_exclusive"],
        )

    # The chain's six calls, in the order of the spec's steps.
    assert [[kw["entrypoint"], *argv] for argv, kw in calls] == [
        step.tokens(PLACEHOLDERS) for step in events_drift.SPEC.steps
    ]


def test_pipe_events_tokens_carry_the_runs_half_open_window() -> None:
    incremental = _parsed("pipe_events/fishing", "incremental_events[night_loitering]")
    auth = _parsed("pipe_events/fishing", "auth_and_regions_fishing_events")

    assert (incremental.value("-start"), incremental.value("-end")) == (
        "2026-03-01", "2026-03-11",
    )
    assert incremental.value("-sfield") == "night_loitering"
    assert auth.value("-rdate") == "2026-03-11"


def test_pipe_gaps_tokens_are_the_docker_runners_argv() -> None:
    args = gaps_modes.parse_args(["--runner", "docker", "--experiment-id", "composer-drift"])
    cfg = gaps_modes._make_config(
        start=PLACEHOLDERS["start_date"], end=PLACEHOLDERS["end_date_exclusive"],
        bq_output_gaps=f"{gaps_modes.PROJECT}.{args.dest_dataset}.three_way_drift_1_bf",
        **gaps_drift.detect_params(args),
    )
    calls = []
    with patch.object(gaps_modes.dit_docker, "run",
                      side_effect=lambda **kw: calls.append(kw) or 0):
        gaps_modes._run_pipeline("docker", cfg, "local-tag")

    (call,) = calls
    tokens = gaps_drift.dit_detect_tokens(PLACEHOLDERS)
    assert [call["entrypoint"], *call["args"]] == tokens
    parsed = _parsed("pipe_gaps/mode_equivalence", "detect")
    assert parsed.value("--date-range") == "2026-03-01,2026-03-11"
    assert parsed.value("--bq-input-messages") == gaps_modes.DEFAULT_SOURCE_MESSAGES


def test_pipe_gaps_detect_params_are_the_ones_main_runs_with() -> None:
    ctx = RunContext(pipeline_commit="c", unreviewed=False, pipeline_commit_parent=None,
                     worker_image="img", worker_image_digest="img", run_id="r", dit_commit="d")
    argv = ["--experiment-id", "exp", "--skip-comparisons", "--modes", "1_bf",
            "--ssvids", "111, 222", "--min-gap-length", "6", "--filter-good-seg", "False",
            "--skip-open-gaps"]
    seen = []
    with (
        patch.object(gaps_modes, "resolve_run_context", return_value=ctx),
        patch.object(gaps_modes, "_run_with_cache",
                     side_effect=lambda fn, **k: seen.append(k["execute_kwargs"])
                     or k["output_fqn"]),
    ):
        assert gaps_modes.main(argv) == 0

    params = gaps_drift.detect_params(gaps_modes.parse_args(argv))
    (kwargs,) = seen
    assert params == {key: kwargs["base_cfg"][key] for key in params}
    assert params["ssvids"] == ("111", "222") and params["filter_good_seg"] is False


def test_pipe_gaps_sizing_is_checked_as_fields() -> None:
    sizing = gaps_modes.DEFAULT_WORKER_SIZING

    def checks(*flags: str) -> list[tuple[str, bool]]:
        raw = {"dag_id": "example", "execution_date": "2026-03-05T00:00:00+00:00",
               "data_interval_start": "2026-03-05T00:00:00+00:00",
               "data_interval_end": "2026-03-06T00:00:00+00:00",
               "tasks": {gaps_drift.TASK_ID: {
                   "image": "registry.example.invalid/core/pipe-gaps:v9.9.9",
                   "cmds": ["pipe-gaps", "raw-gaps"], "arguments": list(flags)}}}
        dag = composer.dag_from_snapshot(raw, snapshot="synthetic")
        return [(c.field, c.matches) for c in gaps_drift.sizing_checks(dag)]

    assert checks(f"--worker_machine_type={sizing.machine_type}",
                  f"--disk_size_gb={sizing.disk_size_gb}") == [
        ("worker_machine_type", True), ("disk_size_gb", True),
    ]
    assert checks("--worker_machine_type=example-machine") == [
        ("worker_machine_type", False), ("disk_size_gb", False),
    ]


def test_pipe_segment_spec_covers_productions_six_tasks_in_order() -> None:
    assert [(step.step, step.task_id) for step in segment_drift.SPEC.steps] == [
        ("segment", "segment.segment"),
        ("segment_identity", "segment.segment_identity_daily"),
        ("segment_vessel_daily", "segment.segment_vessel_daily"),
        ("segment_info", "segment.segment_info"),
        ("vessel_info", "segment.vessel_info"),
        ("segment_vessel", "segment.segment_vessel"),
    ]
    assert [step.step for step in segment_drift.SPEC.steps] == list(segment.STEPS)


def test_pipe_segment_tokens_are_one_default_bindings_argv() -> None:
    seg, identity, daily, info, vessel_info, seg_vessel = (
        _parsed("pipe_segment/identity_match_key", step) for step in segment.STEPS
    )

    # --date-range is inclusive on both ends and forwarded as given.
    assert (seg.value("--date_range") == identity.value("--date_range")
            == daily.value("--date_range") == "2026-03-01,2026-03-10")
    assert seg.value("--in_normalized_messages_table") == segment.DEFAULT_SRC_NORMALIZED_TABLE
    assert seg.value("--runner") == identity.value("--runner") == "dataflow"
    assert seg.value("--sdk_container_image") == segment.DEFAULT_IMAGE_TAG
    assert seg.value("--max_num_workers") == str(segment.STEP_MAX_NUM_WORKERS["segment"])
    # Satellite offsets are opt-in, so a default run passes none of their flags.
    assert "--in_sat_positions_table" not in seg.names()
    # A default run is a published-image run: no setup file, as in production.
    assert "--setup_file" not in seg.names() + identity.names()
    assert "--experiments" not in seg.names() + identity.names()
    # Each step reads what the step before it wrote, as production's chain does.
    assert identity.value("--source_segments") == seg.value("--out_segments_table")
    assert identity.value("--source_fragments") == seg.value("--fragments_table")
    assert daily.value("--source_segment_identity") == identity.value("--dest_segment_identity")
    for step in (info, vessel_info):
        assert step.value("--source_segment_identity") == identity.value(
            "--dest_segment_identity")
        assert step.value("--source_segment_vessel") == daily.value("--destination")
    assert seg_vessel.value("--source_segment_vessel_daily") == daily.value("--destination")


def test_pipe_segment_tokens_are_what_the_workflow_hands_the_runner() -> None:
    args = segment_drift._dit_args(PLACEHOLDERS)
    ctx = SimpleNamespace(worker_image=args.image_tag, pipeline_commit="c", unreviewed=False)
    calls = []
    with (
        patch.object(segment.subprocess, "run"),
        patch.object(segment, "_verify_gpsdio_segment", return_value="pin"),
        patch.object(segment, "resolve_run_context", return_value=ctx),
        patch.object(segment.dit_docker, "run",
                     side_effect=lambda image, argv, **kw: calls.append((argv, kw)) or 0),
    ):
        assert segment._run_binding(
            name=segment_drift._BINDING, ref="HEAD", args=args,
            suffix=f"{args.experiment_id}_{segment_drift._BINDING}",
            snapshot_fqns=segment._PipeSegmentSnapshotFQNs(
                normalized_messages=args.source_normalized_table),
        ) == 0

    # The chain's six calls, in the order of the spec's steps.
    assert [[kw["entrypoint"], *argv] for argv, kw in calls] == [
        step.tokens(PLACEHOLDERS) for step in segment_drift.SPEC.steps
    ]


def test_pipe_segment_accepts_the_satellite_inputs_and_owns_the_handed_over_tables() -> None:
    spec = segment_drift.SPEC
    by_step = {step.step: step for step in spec.steps}

    # Production's satellite-offset inputs are accepted for `segment` only.
    satellite = {"--in_normalized_sat_offset_messages_table", "--in_norad_to_receiver_table",
                 "--in_sat_positions_table"}
    assert satellite <= set(by_step["segment"].accepted)
    assert all("date-sharded" in by_step["segment"].accepted[flag] for flag in satellite)
    assert not any(satellite & set(by_step[step].accepted) for step in segment.STEPS[1:])
    # The tables one step hands to the next carry DIT's own names.
    assert {"--source_segment_identity", "--source_segment_vessel",
            "--source_segment_vessel_daily", "--destination"} <= spec.dit_owned
    # No Dataflow flag is waved through for the SQL steps: each accepts
    # --project only, so a Dataflow option passed to one would be drift.
    for step in segment.SQL_STEPS:
        assert set(by_step[step].accepted) == {"--project"}


# --------------------------------------------------------------------------
# the runner, against a synthetic composer checkout
# --------------------------------------------------------------------------

_RUN = composer.date_placeholders(
    composer.DataInterval(*(composer._parse_time(f"2026-03-0{d}T00:00:00+00:00") for d in (5, 6))),
    composer._parse_time("2026-03-05T00:00:00+00:00"),
)


def _mirror_task(step: drift.StepSpec, *, version: str | None = None) -> dict:
    """A production task that runs exactly DIT's tokens for the synthetic run,
    in production's own registry at DIT's version (or ``version``)."""
    tokens = list(step.tokens(_RUN))
    sizing = gaps_modes.DEFAULT_WORKER_SIZING
    extra = (
        [f"--worker_machine_type={sizing.machine_type}", f"--disk_size_gb={sizing.disk_size_gb}"]
        if step.task_id == gaps_drift.TASK_ID else []
    )
    return {
        "image": f"registry.example.invalid/core/{step.image_name}:"
                 f"{version or step.pipeline_version}",
        "cmds": tokens[:1], "arguments": [*tokens[1:], *extra],
        "upstream_task_ids": [], "downstream_task_ids": [],
    }


def _snapshot(name: str, *, version: str | None = None, in_sync_version: str | None = None,
              port_visits: dict | None = None) -> dict:
    """Snapshot ``name``: a mirror task for every step of every spec that reads
    it, plus the port-visits tasks where given. ``version`` is production's
    version for the workflows not yet in sync, ``in_sync_version`` for the
    mirrored ones marked in sync."""
    base = _port_visits_raw_dag(production_only=())
    tasks = {
        step.task_id: _mirror_task(step, version=in_sync_version if spec.in_sync else version)
        for spec in runner.SPECS if spec.snapshot == name and spec.name != "port_visits/ais"
        for step in spec.steps
    }
    return {**base, "tasks": {**tasks, **(port_visits or {})}}


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


def _commit(repo: Path, snapshots: dict[str, dict]) -> Path:
    directory = repo / composer.SNAPSHOT_DIR
    directory.mkdir(parents=True)
    for name, raw in snapshots.items():
        (directory / f"{name}.py").write_text(f"rendered_dag = {raw!r}\n")
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "add", ".")
    _git(repo, "-c", "user.name=t", "-c", "user.email=t@example.invalid",
         "commit", "-q", "-m", "fixture")
    return repo


def _repo(tmp_path: Path, *, version: str | None = None, in_sync_version: str | None = None,
          without: tuple[str, ...] = (), port_visits_kwargs: dict | None = None) -> Path:
    """A composer checkout in which production runs what DIT runs.
    ``port_visits_kwargs`` are passed to the port-visits fixture's DAG."""
    port_visits = _port_visits_raw_dag(
        **({"production_only": ()} if port_visits_kwargs is None else port_visits_kwargs),
    )
    versions = {"version": version, "in_sync_version": in_sync_version}
    snapshots = {
        "core_ais_v5": _snapshot("core_ais_v5", port_visits=port_visits["tasks"], **versions),
        "core_vms_v5_daily": _snapshot("core_vms_v5_daily", **versions),
        "core_ais_v3": _snapshot("core_ais_v3", **versions),
    }
    return _commit(tmp_path / "composer",
                   {k: v for k, v in snapshots.items() if k not in without})


def _main(repo: Path, *argv: str) -> int:
    return runner.main(["--composer-dir", str(repo), "--ref", "main", *argv])


def test_no_drift_anywhere_when_production_runs_what_dit_runs(tmp_path, capsys) -> None:
    assert _main(_repo(tmp_path)) == 0

    out = capsys.readouterr().out
    assert "port_visits/ais: in sync; 12 field checks, 0 differ" in out
    assert "pipe_segment/identity_match_key: in sync" in out
    for name in SPECS:
        if name not in IN_SYNC:
            assert f"{name}: no drift (can be marked in sync)" in out
    assert "DIFF" not in out and "indicative" not in out
    assert sum(line.startswith("  ") for line in out.splitlines()) == 1 + len(COMMANDS)


def test_drift_in_a_workflow_not_yet_in_sync_is_reported_but_exits_0(tmp_path, capsys) -> None:
    repo = _repo(tmp_path, version="v99.0.0")

    assert _main(repo) == 0
    out = capsys.readouterr().out
    assert "encounters/ais: drift (known: not expected in sync yet)" in out
    assert "DIFF v4.4.0 -> v99.0.0" in out
    assert "port_visits/ais: in sync" in out
    assert "pipe_segment/identity_match_key: in sync" in out

    assert _main(repo, "--strict") == 1


def test_drift_in_pipe_segment_exits_1(tmp_path, capsys) -> None:
    repo = _repo(tmp_path, in_sync_version="v99.0.0")

    assert _main(repo) == 1
    out = capsys.readouterr().out
    assert "pipe_segment/identity_match_key: DRIFT (expected in sync)" in out
    assert "DIFF v5.0.3 -> v99.0.0" in out
    assert _main(repo, "--workflow", "encounters/ais") == 0


def test_a_dataflow_option_on_a_pipe_segment_sql_step_is_drift() -> None:
    """Production passes its SQL tasks no Dataflow option. If DIT passed one
    again, the check must report it and not wave it through as DIT-owned."""
    step = _step("pipe_segment/identity_match_key", "segment_info")
    mirror = _mirror_task(step)
    dag = composer.dag_from_snapshot(
        {**_port_visits_raw_dag(production_only=()), "tasks": {step.task_id: mirror}},
        snapshot="synthetic",
    )
    with_runner = drift.StepSpec(
        step=step.step, task_id=step.task_id, image=step.image, accepted=step.accepted,
        tokens=lambda placeholders: [*step.tokens(placeholders), "--runner=dataflow",
                                     "--region=us-central1"],
    )

    assert not drift.check_step(segment_drift.SPEC, step, dag).drift
    report = drift.check_step(segment_drift.SPEC, with_runner, dag)
    assert report.drift
    assert report.argv.diff.only_left == ("--runner", "--region")


def test_drift_in_port_visits_exits_1(tmp_path, capsys) -> None:
    # The fixture's default DAG passes two invented production-only flags.
    repo = _repo(tmp_path, port_visits_kwargs={})

    assert _main(repo) == 1
    assert "port_visits/ais: DRIFT (expected in sync)" in capsys.readouterr().out
    assert _main(repo, "--workflow", "encounters/ais", "--workflow", "encounters/vms") == 0


def test_an_unreadable_snapshot_exits_2_and_the_other_workflows_are_still_reported(
    tmp_path, capsys,
) -> None:
    repo = _repo(tmp_path, without=("core_ais_v3",))

    assert _main(repo) == 2

    captured = capsys.readouterr()
    assert "pipe_segment/identity_match_key: could not be read" in captured.out
    assert "composer drift check could not read pipe_segment/identity_match_key" in captured.err
    assert "port_visits/ais: in sync" in captured.out
    assert _main(repo, "--workflow", "port_visits/ais") == 0


def test_a_missing_task_exits_2(tmp_path) -> None:
    repo = _commit(tmp_path / "composer", {"core_ais_v5": _port_visits_raw_dag()})

    assert _main(repo, "--workflow", "pipe_gaps/mode_equivalence") == 2


def test_workflow_selects_by_name_and_rejects_unknown_names(tmp_path, capsys) -> None:
    repo = _repo(tmp_path)

    assert _main(repo, "--workflow", "pipe_gaps/mode_equivalence") == 0
    out = capsys.readouterr().out
    assert "pipe_gaps/mode_equivalence:" in out and "port_visits/ais" not in out

    with pytest.raises(SystemExit, match="unknown --workflow nope; choose from port_visits/ais"):
        _main(repo, "--workflow", "nope")


def test_detail_adds_each_workflows_full_report(tmp_path, capsys) -> None:
    repo = _repo(tmp_path)

    _main(repo, "--workflow", "encounters/vms")
    summary = capsys.readouterr().out
    _main(repo, "--workflow", "encounters/vms", "--detail")
    detail = capsys.readouterr().out

    assert "expected to differ" not in summary and "--source_table" not in summary
    assert detail.startswith(summary.rstrip("\n"))
    assert "encounters/vms (workflows/encounters/vms.py)" in detail
    assert "command: same (pipe-encounters create_raw_encounters)" in detail
    assert "expected in sync: no\ndrift: no" in detail


def test_the_help_says_the_output_is_for_local_reading(capsys) -> None:
    with pytest.raises(SystemExit):
        runner.parse_args(["--help"])

    text = " ".join(capsys.readouterr().out.split())
    assert "LOCAL READING ONLY" in text
    assert "--strict" in text and "--detail" in text and "pipe_segment/identity_match_key" in text
