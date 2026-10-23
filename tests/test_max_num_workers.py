"""The Dataflow worker cap is one shared, configurable knob (PIPELINE-4701).

world-fishing-827 is also the execution project of production Dataflow jobs,
so dit must never raise a cap by accident. These tests pin:

* the precedence, the more specific setting winning:
  ``--max-num-workers`` > ``min(default, DIT_MAX_NUM_WORKERS_CEILING)`` >
  default, in the pure resolver and in every workflow and pipe-segment step;
* the shared helpers in ``dit.workflow`` (flag, validation, emission, logging);
* each workflow's default: port visits 4, encounters 50, pipe-gaps 50,
  pipe-segment 50 / 50 / 20 per step;
* the default emitted options where they did not change: the cap's value
  and position in each option list;
* that a set cap actually reaches the job, including pipe-gaps' in-process
  path, where it must override gfw-common's own default of 100, and through
  ``cross_version_ais.py`` to every binding;
* that the cap never enters a cache key.

``tests/conftest.py`` clears the ceiling env var for every test. No docker,
BQ or Dataflow calls.
"""

from __future__ import annotations

import io
import logging
import sys
import types
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from dit import workflow as dit_workflow
from dit.workflow import (
    CAP_FROM_CEILING,
    CAP_FROM_CLI,
    CAP_FROM_DEFAULT,
    MAX_NUM_WORKERS_CEILING_ENV,
    RunContext,
    WorkerCap,
)
from workflows.encounters import ais as encounters_ais
from workflows.encounters import vms as encounters_vms
from workflows.pipe_gaps import mode_equivalence as gaps_modes
from workflows.pipe_gaps import outage_recovery as gaps_outage
from workflows.pipe_segment import identity_match_key as pipe_segment
from workflows.port_visits import ais as port_visits
from workflows.port_visits import cross_version_ais as cross_version

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
CROSS_VERSION_ARGV = ["--experiment-id", "exp", "--binding", "before=main"]

#: One cap per run: (parse_args, argv that satisfies its required flags, default cap).
SINGLE_CAP = {
    "port_visits": (port_visits.parse_args, [], 4),
    "encounters_ais": (encounters_ais.parse_args, [], 50),
    "encounters_vms": (encounters_vms.parse_args, [], 50),
    "pipe_gaps_modes": (gaps_modes.parse_args, [], 50),
    "pipe_gaps_outage": (gaps_outage.parse_args, ["--experiment-id", "exp"], 50),
}
#: Every parser that takes --max-num-workers: (parse_args, argv).
PARSERS = {
    **{name: (parse, argv) for name, (parse, argv, _) in SINGLE_CAP.items()},
    "pipe_segment": (pipe_segment.parse_args, SEGMENT_ARGV),
}
SEGMENT_DEFAULTS = {"segment": 50, "segment_identity": 50, "segment_info": 20}

DEFAULT = object()  # in a case table: "the workflow's or step's default"


def _cap_tokens(argv):
    return [a for a in argv if a.startswith("--max_num_workers")]


# --------------------------------------------------------------------------
# precedence: the pure resolver
# --------------------------------------------------------------------------

@pytest.mark.parametrize("cli, ceiling, default, workers, source", [
    (None, None, 4, 4, CAP_FROM_DEFAULT),
    (None, 2, 4, 2, CAP_FROM_CEILING),
    (None, 4, 4, 4, CAP_FROM_DEFAULT),
    (None, 100, 4, 4, CAP_FROM_DEFAULT),     # the ceiling never raises a default
    (16, None, 4, 16, CAP_FROM_CLI),
    (16, 2, 4, 16, CAP_FROM_CLI),            # the flag wins, even above the ceiling
    (1, 100, 4, 1, CAP_FROM_CLI),
    (None, None, None, None, CAP_FROM_DEFAULT),
    (None, 7, None, 7, CAP_FROM_CEILING),    # no default of its own: the ceiling is the cap
])
def test_resolver_precedence(cli, ceiling, default, workers, source):
    assert dit_workflow.resolve_max_num_workers(cli, ceiling, default) == WorkerCap(workers, source)


@pytest.mark.parametrize("raw, expected", [(None, None), ("", None), ("  ", None),
                                           ("8", 8), (" 8 ", 8)])
def test_ceiling_reads_the_env_var(raw, expected):
    environ = {} if raw is None else {MAX_NUM_WORKERS_CEILING_ENV: raw}
    assert dit_workflow.max_num_workers_ceiling(environ) == expected


@pytest.mark.parametrize("raw, reason", [("0", ">= 1"), ("-3", ">= 1"),
                                         ("abc", "an integer"), ("1.5", "an integer")])
def test_ceiling_is_validated_like_the_flag(raw, reason):
    with pytest.raises(SystemExit, match=f"{MAX_NUM_WORKERS_CEILING_ENV} must be {reason}"):
        dit_workflow.max_num_workers_ceiling({MAX_NUM_WORKERS_CEILING_ENV: raw})


# --------------------------------------------------------------------------
# shared helpers: flag, emission, logging
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


def test_help_names_the_default_the_ceiling_and_the_cache_rule():
    import argparse

    def help_text(default, **kwargs):
        parser = argparse.ArgumentParser()
        dit_workflow.add_max_num_workers_arg(parser, default=default, **kwargs)
        return " ".join(parser.format_help().split())  # undo argparse's wrapping

    capped = help_text(4, help_detail="Sized for X.")
    assert "Default: 4, lowered (never raised)" in capped and "Sized for X." in capped
    assert MAX_NUM_WORKERS_CEILING_ENV in capped and "An explicit value wins" in capped
    assert "Default: unset," in help_text(None)
    assert "not part of the cache key" in help_text(None)


def test_the_flag_parses_to_none_when_absent():
    """The resolver must tell an explicit value from the default."""
    import argparse

    parser = argparse.ArgumentParser()
    dit_workflow.add_max_num_workers_arg(parser, default=4)
    assert parser.parse_args([]).max_num_workers is None
    assert parser.parse_args(["--max-num-workers", "4"]).max_num_workers == 4


def test_log_line_names_each_cap_and_its_source(caplog):
    caplog.set_level(logging.INFO, logger="dit.workflow")
    dit_workflow.log_worker_caps({
        "segment": WorkerCap(10, CAP_FROM_CEILING),
        "segment_info": WorkerCap(None, CAP_FROM_DEFAULT),
        "every job": WorkerCap(30, CAP_FROM_CLI),
    })

    assert caplog.messages == [
        "Dataflow worker cap: segment 10 (ceiling), segment_info unset (default), "
        "every job 30 (cli)"
    ]


# --------------------------------------------------------------------------
# precedence in every workflow
# --------------------------------------------------------------------------

#: (ceiling env, extra argv, expected cap or DEFAULT, source).
WORKFLOW_CASES = [
    ("", [], DEFAULT, CAP_FROM_DEFAULT),
    ("2", [], 2, CAP_FROM_CEILING),
    ("1000", [], DEFAULT, CAP_FROM_DEFAULT),
    ("", ["--max-num-workers", "30"], 30, CAP_FROM_CLI),
    ("2", ["--max-num-workers", "30"], 30, CAP_FROM_CLI),
]


@pytest.mark.parametrize("ceiling, extra, expected, source", WORKFLOW_CASES)
@pytest.mark.parametrize("name", sorted(SINGLE_CAP))
def test_every_workflow_resolves_the_same_way(name, ceiling, extra, expected, source,
                                              monkeypatch):
    monkeypatch.setenv(MAX_NUM_WORKERS_CEILING_ENV, ceiling)
    parse, argv, default = SINGLE_CAP[name]
    workers = default if expected is DEFAULT else expected

    args = parse([*argv, *extra])

    assert args.worker_cap == WorkerCap(workers, source)
    assert args.max_num_workers == workers


#: (ceiling env, extra argv, {step: (cap or DEFAULT, source)}).
SEGMENT_CASES = [
    ("", [], {s: (DEFAULT, CAP_FROM_DEFAULT) for s in SEGMENT_DEFAULTS}),
    ("30", [], {"segment": (30, CAP_FROM_CEILING), "segment_identity": (30, CAP_FROM_CEILING),
                "segment_info": (DEFAULT, CAP_FROM_DEFAULT)}),
    ("10", [], {s: (10, CAP_FROM_CEILING) for s in SEGMENT_DEFAULTS}),
    ("", ["--max-num-workers", "7"], {s: (7, CAP_FROM_CLI) for s in SEGMENT_DEFAULTS}),
    ("10", ["--max-num-workers", "80"], {s: (80, CAP_FROM_CLI) for s in SEGMENT_DEFAULTS}),
]


@pytest.mark.parametrize("ceiling, extra, expected", SEGMENT_CASES)
def test_pipe_segment_resolves_each_step(ceiling, extra, expected, monkeypatch):
    monkeypatch.setenv(MAX_NUM_WORKERS_CEILING_ENV, ceiling)

    caps = pipe_segment.parse_args([*SEGMENT_ARGV, *extra]).worker_caps

    assert pipe_segment.STEP_MAX_NUM_WORKERS == SEGMENT_DEFAULTS
    assert caps == {
        step: WorkerCap(SEGMENT_DEFAULTS[step] if workers is DEFAULT else workers, source)
        for step, (workers, source) in expected.items()
    }


@pytest.mark.parametrize("name", sorted(PARSERS))
def test_every_parser_validates_the_flag(name):
    parse, argv = PARSERS[name]

    assert parse([*argv, "--max-num-workers", "12"]).max_num_workers == 12
    with pytest.raises(SystemExit, match="--max-num-workers must be >= 1"):
        parse([*argv, "--max-num-workers", "0"])


@pytest.mark.parametrize("name", [*sorted(PARSERS), "cross_version_ais"])
def test_every_parser_fails_fast_on_a_bad_ceiling(name, monkeypatch):
    monkeypatch.setenv(MAX_NUM_WORKERS_CEILING_ENV, "0")
    parse, argv = PARSERS.get(name, (cross_version.parse_args, CROSS_VERSION_ARGV))

    with pytest.raises(SystemExit, match=f"{MAX_NUM_WORKERS_CEILING_ENV} must be >= 1"):
        parse(argv)


# --------------------------------------------------------------------------
# docker-submitted workflows: unchanged defaults, resolved caps arrive
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


def test_port_visits_emits_the_ceiling(monkeypatch):
    monkeypatch.setenv(MAX_NUM_WORKERS_CEILING_ENV, "2")
    assert _cap_tokens(_port_visits_options()) == ["--max_num_workers=2"]


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


def _segment_step_argvs(runner="dataflow", caps=None):
    """The three pipe-segment steps' argv, in pipeline order; ``caps`` maps a
    step to its resolved cap."""
    caps = caps or {}
    common = dict(dest_dataset="d", suffix="exp_before", runner=runner, worker_image="img",
                  service_account="sa")
    one_day = (date(2020, 1, 1), date(2020, 1, 1))
    return {
        "segment": pipe_segment._segment_args(
            snapshot_fqns=pipe_segment._PipeSegmentSnapshotFQNs(normalized_messages="p.d.n"),
            experiment_id="exp", binding_name="before", date_range=one_day,
            ssvid_filter_query=None, include_satellite_offsets=False,
            max_num_workers=caps.get("segment"), **common,
        ),
        "segment_identity": pipe_segment._segment_identity_args(
            date_range=one_day, max_num_workers=caps.get("segment_identity"), **common),
        "segment_info": pipe_segment._segment_info_args(
            max_num_workers=caps.get("segment_info"), **common),
    }


def test_pipe_segment_steps_keep_their_caps_in_place():
    for step, argv in _segment_step_argvs().items():
        cap = f"--max_num_workers={SEGMENT_DEFAULTS[step]}"
        assert _cap_tokens(argv) == [cap], step
        assert argv[argv.index("--region=us-central1") + 1] == cap


def test_pipe_segment_caps_are_dataflow_only():
    caps = dict.fromkeys(SEGMENT_DEFAULTS, 7)
    for step, argv in _segment_step_argvs(caps=caps).items():
        assert _cap_tokens(argv) == ["--max_num_workers=7"], step
    for step, argv in _segment_step_argvs(runner="DirectRunner", caps=caps).items():
        assert _cap_tokens(argv) == [], step


@pytest.mark.parametrize("ceiling, extra, expected", [
    ("", ["--max-num-workers", "7"], [7, 7, 7]),
    ("30", [], [30, 30, 20]),
])
def test_pipe_segment_run_binding_emits_each_resolved_cap(ceiling, extra, expected,
                                                          monkeypatch):
    monkeypatch.setenv(MAX_NUM_WORKERS_CEILING_ENV, ceiling)
    args = pipe_segment.parse_args([*SEGMENT_ARGV, "--runner", "dataflow", *extra])
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
    assert [_cap_tokens(argv) for argv in cli_args] == [
        [f"--max_num_workers={n}"] for n in expected
    ]


# --------------------------------------------------------------------------
# cross_version_ais.py: every binding's ais.py resolves the same way
# --------------------------------------------------------------------------

@pytest.mark.parametrize("extra, workers, source", [
    ([], 2, CAP_FROM_CEILING),
    (["--max-num-workers", "30"], 30, CAP_FROM_CLI),
])
def test_cross_version_bindings_inherit_the_ceiling(extra, workers, source, monkeypatch):
    monkeypatch.setenv(MAX_NUM_WORKERS_CEILING_ENV, "2")
    proc = MagicMock(stdout=io.StringIO(""), **{"wait.return_value": 0})
    with (
        patch.object(cross_version.subprocess, "run"),
        patch.object(cross_version.subprocess, "Popen", return_value=proc) as popen,
    ):
        rc = cross_version._run_binding(
            name="after", ref="main", experiment_id="exp",
            pinned=cross_version._PinnedSources(
                start=date(2020, 1, 1), end=date(2020, 12, 31),
                messages_positions="p.d.m", segment_info="p.d.si", segs_activity="p.d.sa",
            ),
            suffix="s", modes=[port_visits.MODE_BF], steps=list(port_visits.SELECTABLE_STEPS),
            pipeline_dir="/nowhere", ais_extra_args=extra, dry_run=False,
        )

    assert rc == 0
    cmd, env = popen.call_args.args[0], popen.call_args.kwargs["env"]
    assert env[MAX_NUM_WORKERS_CEILING_ENV] == "2"
    # cmd is [python, ais.py, *argv]; ais.py parses argv under that environment.
    assert port_visits.parse_args(cmd[2:]).worker_cap == WorkerCap(workers, source)


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
@pytest.mark.parametrize("cap, expected_args", [
    (None, []), (50, ["--max_num_workers=50"]), (7, ["--max_num_workers=7"]),
])
def test_gaps_runner_gets_the_cap_as_beam_args(module, cap, expected_args, monkeypatch):
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
    # gfw-common's Pipeline gives these precedence over its own default of 100;
    # Beam's from_dictionary turns "7" into --max_num_workers=7 (type=int).
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
@pytest.mark.parametrize("ceiling, cli, expected, logged", [
    ("", [], 50, "every job 50 (default)"),
    ("10", [], 10, "every job 10 (ceiling)"),
    ("10", ["--max-num-workers", "70"], 70, "every job 70 (cli)"),
])
def test_gaps_main_threads_the_cap_into_every_stage_and_logs_it_once(
    module, argv, ceiling, cli, expected, logged, monkeypatch, caplog,
):
    monkeypatch.setenv(MAX_NUM_WORKERS_CEILING_ENV, ceiling)
    caplog.set_level(logging.INFO, logger="dit.workflow")
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
    assert [m for m in caplog.messages if m.startswith("Dataflow worker cap")] == [
        f"Dataflow worker cap: {logged}"
    ]


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
def test_cache_keys_ignore_the_cap(module, argv, modes, monkeypatch):
    unset = module.parse_args(argv)
    capped = module.parse_args([*argv, "--max-num-workers", "7"])
    monkeypatch.setenv(MAX_NUM_WORKERS_CEILING_ENV, "2")
    ceilinged = module.parse_args(argv)

    for mode in modes:
        params = module.canonical_params_dict(unset, mode)
        assert module.canonical_params_dict(capped, mode) == params, mode
        assert module.canonical_params_dict(ceilinged, mode) == params, mode
        assert "max_num_workers" not in params and "worker_cap" not in params, mode
