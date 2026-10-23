"""The Dataflow worker cap is one shared, configurable knob (PIPELINE-4701).

world-fishing-827 is also the execution project of production Dataflow jobs,
so dit must never raise a cap by accident. These tests pin:

* the shared helpers in ``dit.workflow`` (flag, validation, emission);
* every Dataflow workflow's default, unchanged by the move to shared code
  (port visits 4, encounters 50, pipe-gaps and pipe-segment unset), and the
  absence of an environment-variable fallback;
* that the default emitted options are byte-identical where it matters: the
  cap's value and position in each option list;
* that a set cap actually reaches the job, including pipe-gaps' in-process
  path, where it must override gfw-common's own default of 100;
* that the cap never enters a cache key.

No docker, BQ or Dataflow calls.
"""

from __future__ import annotations

import sys
import types
from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from dit import workflow as dit_workflow
from dit.workflow import RunContext
from workflows.encounters import ais as encounters_ais
from workflows.encounters import vms as encounters_vms
from workflows.pipe_gaps import mode_equivalence as gaps_modes
from workflows.pipe_gaps import outage_recovery as gaps_outage
from workflows.pipe_segment import identity_match_key as pipe_segment
from workflows.port_visits import ais as port_visits

# Explicit infra flags, so the pinned option lists don't depend on DIT_* env vars.
INFRA_ARGV = [
    "--experiment-id", "exp",
    "--service-account", "sa@p.iam.gserviceaccount.com",
    "--dataflow-region", "us-central1",
    "--dataflow-temp-bucket", "b",
    "--dataflow-subnetwork", "net",
    "--bq-temp-dataset", "p.tmp",
]
SEGMENT_ARGV = [
    "--experiment-id", "exp",
    "--pin-source-at", "2026-06-03T10:00:00Z",
    "--binding", "before=v5.0.3",
    "--date-range", "2020-01-01,2020-01-01",
]

#: (parse_args, argv that satisfies its required flags, default cap).
WORKFLOWS = {
    "port_visits": (port_visits.parse_args, [], 4),
    "encounters_ais": (encounters_ais.parse_args, [], 50),
    "encounters_vms": (encounters_vms.parse_args, [], 50),
    "pipe_gaps_modes": (gaps_modes.parse_args, [], None),
    "pipe_gaps_outage": (gaps_outage.parse_args, ["--experiment-id", "exp"], None),
    "pipe_segment": (pipe_segment.parse_args, SEGMENT_ARGV, None),
}


def _cap_tokens(argv):
    return [a for a in argv if a.startswith("--max_num_workers")]


# --------------------------------------------------------------------------
# shared helpers
# --------------------------------------------------------------------------

def test_options_emit_one_token_or_none():
    assert dit_workflow.max_num_workers_options(None) == []
    assert dit_workflow.max_num_workers_options(7) == ["--max_num_workers=7"]


@pytest.mark.parametrize("value", ["1", "16", "250"])
def test_type_accepts_positive_integers(value):
    assert dit_workflow.max_num_workers_type(value) == int(value)


@pytest.mark.parametrize("value, reason", [
    ("0", ">= 1"), ("-3", ">= 1"), ("abc", "an integer"), ("1.5", "an integer"),
])
def test_type_rejects_everything_else_before_any_cloud_call(value, reason):
    with pytest.raises(SystemExit, match=f"--max-num-workers must be {reason}"):
        dit_workflow.max_num_workers_type(value)


def test_help_names_the_default_and_the_cache_rule():
    import argparse

    def help_text(default, **kwargs):
        parser = argparse.ArgumentParser()
        dit_workflow.add_max_num_workers_arg(parser, default=default, **kwargs)
        return " ".join(parser.format_help().split())  # undo argparse's wrapping

    capped = help_text(4, help_detail="Sized for X.")
    assert "Default: 4." in capped and "Sized for X." in capped
    assert "Default: unset." in help_text(None)
    assert "not part of the cache key" in help_text(None)


# --------------------------------------------------------------------------
# every Dataflow workflow: default unchanged, validated, no env var
# --------------------------------------------------------------------------

@pytest.mark.parametrize("name", sorted(WORKFLOWS))
def test_every_workflow_keeps_its_default(name, monkeypatch):
    # A silent env var could raise caps unnoticed, so none is read.
    monkeypatch.setenv("DIT_MAX_NUM_WORKERS", "999")
    parse, argv, default = WORKFLOWS[name]

    assert parse(argv).max_num_workers == default


@pytest.mark.parametrize("name", sorted(WORKFLOWS))
def test_every_workflow_parses_and_validates_through_the_shared_type(name):
    parse, argv, _ = WORKFLOWS[name]

    assert parse([*argv, "--max-num-workers", "12"]).max_num_workers == 12
    with pytest.raises(SystemExit, match="--max-num-workers must be >= 1"):
        parse([*argv, "--max-num-workers", "0"])


# --------------------------------------------------------------------------
# docker-submitted workflows: byte-identical defaults, set caps arrive
# --------------------------------------------------------------------------

def _port_visits_options(*cli):
    args = port_visits.parse_args([*INFRA_ARGV, *cli])
    args.run_id, args.commit_sha, args.worker_image_tag, args.launched_by = "r", "c", "t", "u"
    return port_visits._dataflow_pipeline_options(
        args, step=port_visits.STEP_THIN, mode=port_visits.MODE_BF, iteration=1,
        total_iterations=1, slice_start=date(2020, 1, 1), slice_end=date(2020, 12, 31),
    )


@pytest.mark.parametrize("cli", ["v4", "v5"])
def test_port_visits_default_options_are_unchanged(cli):
    assert _port_visits_options("--pipeline-cli", cli)[:10] == [
        "--runner=DataflowRunner",
        "--project=world-fishing-827",
        "--region=us-central1",
        "--service_account_email=sa@p.iam.gserviceaccount.com",
        "--temp_location=gs://b/dit/dataflow_temp",
        "--staging_location=gs://b/dit/dataflow_staging",
        "--subnetwork=net",
        "--max_num_workers=4",
        f"--sdk_container_image={port_visits.DEFAULT_WORKER_IMAGE}",
        "--job_name=dit-anchorages-pipeline-thin-exp-1-bf-1-1",
    ]


def _encounters_options(module, *cli, runner="dataflow"):
    args = module.parse_args([*INFRA_ARGV, "--runner", runner, *cli])
    args.run_id, args.commit_sha = "r", "c"
    return encounters_ais._pipeline_options(
        args, step="create_raw_encounters", mode=encounters_ais.MODE_BF,
        iteration=1, total_iterations=1,
    )


@pytest.mark.parametrize("module", [encounters_ais, encounters_vms])
def test_encounters_default_options_are_unchanged(module):
    assert _encounters_options(module)[:11] == [
        "--runner=DataflowRunner",
        "--project=world-fishing-827",
        "--region=us-central1",
        "--service_account_email=sa@p.iam.gserviceaccount.com",
        "--temp_location=gs://b/dit/dataflow_temp",
        "--temp_dataset=p.tmp",
        "--staging_location=gs://b/dit/dataflow_staging",
        "--subnetwork=net",
        "--max_num_workers=50",
        f"--sdk_container_image={encounters_ais.DEFAULT_WORKER_IMAGE}",
        "--job_name=dit-encounters-createraw-exp-1-bf-1-1",
    ]


@pytest.mark.parametrize("module", [encounters_ais, encounters_vms])
def test_encounters_cap_is_dataflow_only(module):
    assert _cap_tokens(_encounters_options(module, "--max-num-workers", "7")) == [
        "--max_num_workers=7"
    ]
    assert _cap_tokens(_encounters_options(module, runner="docker")) == []


def _segment_step_argvs(runner="dataflow", max_num_workers=None):
    """The three pipe-segment steps' argv, in pipeline order."""
    common = dict(dest_dataset="d", suffix="exp_before", runner=runner, worker_image="img",
                  service_account="sa", max_num_workers=max_num_workers)
    one_day = (date(2020, 1, 1), date(2020, 1, 1))
    return {
        "segment": pipe_segment._segment_args(
            snapshot_fqns=pipe_segment._PipeSegmentSnapshotFQNs(normalized_messages="p.d.n"),
            experiment_id="exp", binding_name="before", date_range=one_day,
            ssvid_filter_query=None, include_satellite_offsets=False, **common,
        ),
        "segment_identity": pipe_segment._segment_identity_args(date_range=one_day, **common),
        "segment_info": pipe_segment._segment_info_args(**common),
    }


def test_pipe_segment_steps_keep_their_caps_in_place():
    expected = {"segment": 50, "segment_identity": 50, "segment_info": 20}
    assert pipe_segment.STEP_MAX_NUM_WORKERS == expected
    for step, argv in _segment_step_argvs().items():
        assert _cap_tokens(argv) == [f"--max_num_workers={expected[step]}"], step
        assert argv[argv.index("--region=us-central1") + 1] == f"--max_num_workers={expected[step]}"


def test_pipe_segment_flag_replaces_every_step_cap_on_dataflow_only():
    for step, argv in _segment_step_argvs(max_num_workers=7).items():
        assert _cap_tokens(argv) == ["--max_num_workers=7"], step
    for step, argv in _segment_step_argvs(runner="DirectRunner", max_num_workers=7).items():
        assert _cap_tokens(argv) == [], step


def test_pipe_segment_run_binding_passes_the_flag_to_every_step():
    args = pipe_segment.parse_args([*SEGMENT_ARGV, "--runner", "dataflow",
                                    "--max-num-workers", "7"])
    cli_args = []
    ctx = SimpleNamespace(worker_image="img", pipeline_commit="c", unreviewed=False)
    with (
        patch.object(pipe_segment.subprocess, "run"),
        patch.object(pipe_segment, "_verify_gpsdio_segment", return_value="pin"),
        patch.object(pipe_segment, "resolve_run_context", return_value=ctx),
        patch.object(pipe_segment, "_run_pipe_subcommand",
                     side_effect=lambda **k: cli_args.append(k["cli_args"]) or 0),
    ):
        rc = pipe_segment._run_binding(
            name="before", ref="v5.0.3", args=args, suffix="exp_before",
            snapshot_fqns=pipe_segment._PipeSegmentSnapshotFQNs(normalized_messages="p.d.n"),
        )

    assert rc == 0
    assert [argv[0] for argv in cli_args] == ["segment", "segment_identity", "segment_info"]
    assert all(_cap_tokens(argv) == ["--max_num_workers=7"] for argv in cli_args)


# --------------------------------------------------------------------------
# pipe-gaps: in-process submission through dit.runners.dataflow
# --------------------------------------------------------------------------

GAPS_MODULES = [gaps_modes, gaps_outage]


def _install_modules(monkeypatch, **modules):
    """Stub ``modules`` and every missing parent package in sys.modules."""
    for name, module in modules.items():
        parts = name.split(".")
        for i in range(1, len(parts)):
            parent = ".".join(parts[:i])
            if parent not in sys.modules:
                monkeypatch.setitem(sys.modules, parent, types.ModuleType(parent))
        monkeypatch.setitem(sys.modules, name, module)


def _module(name, **attrs):
    module = types.ModuleType(name)
    module.__dict__.update(attrs)
    return module


class _Stop(Exception):
    """Raised by the stub factory, so nothing is submitted."""


@pytest.fixture
def stubbed_gaps_stack(monkeypatch):
    """Stub Beam, gfw-common and pipe_gaps (none are in dit's venv) and record
    the namespace ``DetectGapsConfig.from_namespace`` receives."""
    seen = {}

    class DetectGapsConfig:
        @classmethod
        def from_namespace(cls, ns, **_):
            seen["ns"] = ns
            return cls()

    class PipelineFactory:
        def __init__(self, config, dag_factory):
            pass

        def build_pipeline(self):
            raise _Stop

    _install_modules(
        monkeypatch,
        **{
            "apache_beam.runners.runner": _module(
                "apache_beam.runners.runner", PipelineState=SimpleNamespace(DONE="DONE")),
            "gfw.common.beam.pipeline.factory": _module(
                "gfw.common.beam.pipeline.factory", PipelineFactory=PipelineFactory),
            "pipe_gaps.pipelines.detect.config": _module(
                "pipe_gaps.pipelines.detect.config", DetectGapsConfig=DetectGapsConfig),
            "pipe_gaps.pipelines.detect.factory": _module(
                "pipe_gaps.pipelines.detect.factory",
                DetectGapsLinearDagFactory=type("F", (), {"__init__": lambda self, c: None})),
            "pipe_gaps.version": _module("pipe_gaps.version", __version__="0"),
        },
    )
    return seen


def _gaps_cfg(module, max_num_workers):
    return module._make_config(
        start=date(2020, 1, 1), end=date(2020, 1, 2), bq_input_messages="p.d.m",
        bq_input_segments="p.d.s", bq_output_gaps="p.d.out", ssvids=(), min_gap_length=1.0,
        n_hours_before=12, window_period_d=2, filter_good_seg=True, skip_open_gaps=False,
        service_account="sa", bq_temp_dataset=None, dataflow_region="us-central1",
        dataflow_temp_bucket="b", dataflow_subnetwork="net", worker_image="img",
        job_name="job", labels=["dit_run_id=r"], max_num_workers=max_num_workers,
    )


@pytest.mark.parametrize("module", GAPS_MODULES)
@pytest.mark.parametrize("cap, expected_args", [(None, []), (7, ["--max_num_workers=7"])])
def test_gaps_runner_gets_the_cap_as_beam_args(module, cap, expected_args, monkeypatch):
    # Unset, args stays [] -- the runner call is byte-identical to before.
    _install_modules(monkeypatch, **{"pipe_gaps.pipelines.detect.factory": _module(
        "pipe_gaps.pipelines.detect.factory", DetectGapsLinearDagFactory=object)})
    calls = []
    with patch.object(module.dit_dataflow, "run", side_effect=lambda **k: calls.append(k) or 0):
        module._run_pipeline("dataflow", _gaps_cfg(module, cap), "local-tag")

    assert calls[0]["args"] == expected_args


@pytest.mark.parametrize("module", GAPS_MODULES)
@pytest.mark.parametrize("cap", [None, 7])
def test_gaps_cap_reaches_the_pipeline_options_and_not_the_config(module, cap, stubbed_gaps_stack):
    with pytest.raises(_Stop):
        module._run_pipeline("dataflow", _gaps_cfg(module, cap), "local-tag")

    ns = stubbed_gaps_stack["ns"]
    # DetectGapsConfig is a dataclass built from vars(ns): a stray attribute crashes it.
    assert not hasattr(ns, "max_num_workers")
    # Unset, gfw-common's Pipeline default (100) applies, as before PIPELINE-4701.
    assert ns.unknown_parsed_args.get("max_num_workers") == (None if cap is None else str(cap))


@pytest.mark.parametrize("module", GAPS_MODULES)
def test_gaps_docker_runner_gets_no_cap(module):
    calls = []
    with patch.object(module.dit_docker, "run", side_effect=lambda **k: calls.append(k) or 0):
        module._run_pipeline("docker", _gaps_cfg(module, 7), "local-tag")

    assert not any("max" in str(a) for a in calls[0]["args"])


def _ctx():
    return RunContext(pipeline_commit="c", unreviewed=False, pipeline_commit_parent=None,
                      worker_image="img", worker_image_digest="img", run_id="r",
                      dit_commit="d")


@pytest.mark.parametrize("module, argv", [
    (gaps_modes, ["--modes", "1_bf"]),
    (gaps_outage, ["--no-snapshot"]),
])
@pytest.mark.parametrize("cli, expected", [([], None), (["--max-num-workers", "7"], 7)])
def test_gaps_main_threads_the_flag_into_every_stage_config(module, argv, cli, expected):
    seen = []
    with (
        patch.object(module, "resolve_run_context", return_value=_ctx()),
        patch.object(module, "_run_with_cache",
                     side_effect=lambda fn, **k: seen.append(k["execute_kwargs"])
                     or k["output_fqn"]),
    ):
        assert module.main(["--experiment-id", "exp", "--skip-comparisons", *argv, *cli]) == 0

    assert seen
    assert all(kwargs["base_cfg"]["max_num_workers"] == expected for kwargs in seen)


# --------------------------------------------------------------------------
# the cap is runner-only: never part of a cache key
# --------------------------------------------------------------------------

CACHED = [
    (port_visits, [], port_visits.SELECTABLE_MODES),
    (gaps_modes, [], (*gaps_modes.SELECTABLE_MODES, gaps_modes.MODE_MUTATE_RECOVER)),
    (gaps_outage, ["--experiment-id", "exp"],
     (gaps_outage.MODE_OUTAGE_RECOVERY, gaps_outage.MODE_OUTAGE_ORACLE)),
]


@pytest.mark.parametrize("module, argv, modes", CACHED)
def test_cache_keys_ignore_the_cap(module, argv, modes):
    unset = module.parse_args(argv)
    capped = module.parse_args([*argv, "--max-num-workers", "7"])

    for mode in modes:
        params = module.canonical_params_dict(capped, mode)
        assert params == module.canonical_params_dict(unset, mode), mode
        assert "max_num_workers" not in params, mode
