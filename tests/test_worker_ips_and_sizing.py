"""Private worker IPs and production-mirrored worker sizing on every Dataflow workflow.

Owner decisions of 2026-10-23 on draft PR #86 (items 2 and 4) and on PR #87:

* every DIT Dataflow job runs with private worker IPs. The docker-submitted
  workflows emit ``--no_use_public_ips``; pipe-segment already did; pipe-gaps
  goes through gfw-common, whose default is already private, so dit adds
  nothing there and these tests pin that it overrides nothing either;
* the worker sizing defaults to production's for each step (hand-written
  ``STEP_WORKER_SIZING`` constants citing composer's rendered snapshots);
  ``--worker-machine-type`` / ``--disk-size-gb`` override it field by field,
  are validated before any cloud call, and are never part of a cache key;
* a Dataflow run that keeps the production sizing on non-production data (the
  AIS staging cohort) logs a cost hint; nothing prompts or blocks;
* with ``--baseline-production`` every binding gets the production target's
  sizing, an explicit flag winning field by field.

No docker, BQ or Dataflow calls.
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
import types
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from dit import workflow as dit_workflow
from dit.workflow import NO_PUBLIC_IPS_OPTION, RunContext, WorkerSizing
from workflows.encounters import ais as encounters_ais
from workflows.encounters import vms as encounters_vms
from workflows.pipe_gaps import mode_equivalence as gaps_modes
from workflows.pipe_gaps import outage_recovery as gaps_outage
from workflows.pipe_segment import identity_match_key as pipe_segment
from workflows.port_visits import ais as port_visits
from workflows.port_visits import cross_version_ais as cross_version

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
CROSS_VERSION_ARGV = ["--experiment-id", "exp", "--binding", "after=main"]
PRODUCTION_ARGV = [
    *CROSS_VERSION_ARGV,
    "--baseline-production", "ais-v5-datalake", "--production-date", "2026-10-20",
]
SIZING_ARGV = ["--worker-machine-type", "n2-standard-8", "--disk-size-gb", "100"]
SIZING_TOKENS = ["--worker_machine_type=n2-standard-8", "--disk_size_gb=100"]

#: Production's sizing per workflow step, from composer-dags-production
#: origin/main 0a644da's rendered snapshots (see each STEP_WORKER_SIZING).
E2_HIGHMEM_4 = WorkerSizing("e2-highmem-4", 50)
E2_STANDARD_4 = WorkerSizing("e2-standard-4", 50)
GAPS_DAILY = WorkerSizing("e2-custom-2-3072", 25)
E2_HIGHMEM_4_TOKENS = ["--worker_machine_type=e2-highmem-4", "--disk_size_gb=50"]

#: Every parser that takes the sizing flags: (parse_args, argv for its required flags).
PARSERS = {
    "port_visits": (port_visits.parse_args, []),
    "cross_version_ais": (lambda argv: cross_version.parse_args(argv)[0], CROSS_VERSION_ARGV),
    "encounters_ais": (encounters_ais.parse_args, []),
    "encounters_vms": (encounters_vms.parse_args, []),
    "pipe_gaps_modes": (gaps_modes.parse_args, []),
    "pipe_gaps_outage": (gaps_outage.parse_args, ["--experiment-id", "exp"]),
    "pipe_segment": (pipe_segment.parse_args, SEGMENT_ARGV),
}

STAGING_TABLE = "world-fishing-827.pipe_ais_test_202408290000_internal.messages_positions"
PROD_TABLE = "gfw-int-ais-datalake.port_visits_v1.raw_port_events"


def _ip_tokens(argv):
    return [a for a in argv if "public_ips" in a]


def _sizing_tokens(argv):
    return [a for a in argv if a.startswith(("--worker_machine_type", "--machine_type",
                                             "--disk_size_gb"))]


def _hints(caplog):
    return [r.getMessage() for r in caplog.records
            if r.levelno == logging.WARNING and r.getMessage().startswith("cost hint")]


# --------------------------------------------------------------------------
# shared helpers
# --------------------------------------------------------------------------

def test_the_private_ip_option_is_beams_flag():
    assert NO_PUBLIC_IPS_OPTION == "--no_use_public_ips"


@pytest.mark.parametrize("value, expected", [
    ("e2-highmem-4", "e2-highmem-4"), (" n2-standard-8 ", "n2-standard-8"),
])
def test_machine_type_accepts_a_non_empty_string(value, expected):
    assert dit_workflow.machine_type_type(value) == expected


@pytest.mark.parametrize("value", ["", "   "])
def test_machine_type_rejects_an_empty_string(value):
    with pytest.raises(SystemExit, match="--worker-machine-type must be a non-empty"):
        dit_workflow.machine_type_type(value)


@pytest.mark.parametrize("value", ["10", "50", "500"])
def test_disk_size_accepts_integers_of_at_least_10(value):
    assert dit_workflow.disk_size_gb_type(value) == int(value)


@pytest.mark.parametrize("value, reason", [
    ("9", ">= 10"), ("0", ">= 10"), ("-50", ">= 10"), ("abc", "an integer"), ("50.5", "an integer"),
])
def test_disk_size_rejects_everything_else(value, reason):
    with pytest.raises(SystemExit, match=f"--disk-size-gb must be {reason}"):
        dit_workflow.disk_size_gb_type(value)


@pytest.mark.parametrize("cli, default, expected", [
    (WorkerSizing(), WorkerSizing(), WorkerSizing()),
    (WorkerSizing(), E2_HIGHMEM_4, E2_HIGHMEM_4),
    (WorkerSizing("n2-standard-8", None), E2_HIGHMEM_4, WorkerSizing("n2-standard-8", 50)),
    (WorkerSizing(None, 100), E2_HIGHMEM_4, WorkerSizing("e2-highmem-4", 100)),
    (WorkerSizing("n2-standard-8", 100), E2_HIGHMEM_4, WorkerSizing("n2-standard-8", 100)),
    (WorkerSizing(None, 100), WorkerSizing(), WorkerSizing(None, 100)),
])
def test_resolver_lets_the_flag_win_field_by_field(cli, default, expected):
    assert dit_workflow.resolve_worker_sizing(cli, default) == expected


@pytest.mark.parametrize("sizing, options, argv", [
    (WorkerSizing(), [], []),
    (WorkerSizing("e2-highmem-4", None), ["--worker_machine_type=e2-highmem-4"],
     ["--worker-machine-type", "e2-highmem-4"]),
    (WorkerSizing(None, 50), ["--disk_size_gb=50"], ["--disk-size-gb", "50"]),
    (E2_HIGHMEM_4, E2_HIGHMEM_4_TOKENS,
     ["--worker-machine-type", "e2-highmem-4", "--disk-size-gb", "50"]),
])
def test_sizing_emits_only_the_set_fields(sizing, options, argv):
    assert dit_workflow.worker_sizing_options(sizing) == options
    assert dit_workflow.worker_sizing_argv(sizing) == argv


def test_sizing_options_take_another_machine_type_flag():
    options = dit_workflow.worker_sizing_options(E2_HIGHMEM_4, machine_type_option="--machine_type")
    assert options == ["--machine_type=e2-highmem-4", "--disk_size_gb=50"]


def _help(**kwargs):
    """The parser and both flags' help texts, joined (unwrapped, so hyphenated
    words stay whole)."""
    parser = argparse.ArgumentParser()
    dit_workflow.add_worker_sizing_args(parser, **kwargs)
    helps = [a.help for a in parser._actions if a.dest in ("worker_machine_type", "disk_size_gb")]
    return parser, " ".join(helps)


def test_sizing_flags_parse_to_none_when_absent():
    parser, _ = _help(defaults={"a": E2_HIGHMEM_4})

    assert dit_workflow.worker_sizing_from_args(parser.parse_args([])) == WorkerSizing()
    assert dit_workflow.worker_sizing_from_args(parser.parse_args(SIZING_ARGV)) == (
        WorkerSizing("n2-standard-8", 100)
    )


def test_help_shows_the_production_defaults_and_nudges_overrides():
    _, same = _help(defaults={"a": E2_HIGHMEM_4, "b": E2_HIGHMEM_4}, help_detail="Sized for X.")
    _, per_step = _help(defaults={"segment": E2_STANDARD_4, "segment_info": WorkerSizing()})
    _, none = _help(defaults=None, help_detail="See the binding.")

    assert "Default, mirroring production: e2-highmem-4." in same
    assert "Default, mirroring production: 50 GB." in same
    assert "Default, mirroring production: segment e2-standard-4, segment_info unset." in per_step
    assert "segment 50 GB, segment_info unset." in per_step
    assert "Default, mirroring" not in none and "See the binding." in none
    for text in (same, per_step, none):
        assert text.count("on non-production data (e.g. the AIS staging cohort) pass a "
                          "smaller one to save cost") == 2
        assert text.count("never output, so it is not part of the cache key") == 2


def test_log_line_names_each_scope_and_its_sizing(caplog):
    caplog.set_level(logging.INFO, logger="dit.workflow")
    dit_workflow.log_worker_sizings({"segment": E2_HIGHMEM_4, "segment_info": WorkerSizing()})

    assert caplog.messages == [
        "Dataflow worker sizing: segment machine type e2-highmem-4, disk 50 GB; "
        "segment_info machine type unset, disk unset"
    ]


@pytest.mark.parametrize("name", sorted(PARSERS))
def test_every_parser_validates_the_sizing_flags(name):
    parse, argv = PARSERS[name]

    with pytest.raises(SystemExit, match="--disk-size-gb must be >= 10"):
        parse([*argv, "--disk-size-gb", "9"])
    with pytest.raises(SystemExit, match="--worker-machine-type must be a non-empty"):
        parse([*argv, "--worker-machine-type", " "])


# --------------------------------------------------------------------------
# the staging-cost hint
# --------------------------------------------------------------------------

@pytest.mark.parametrize("table, staging", [
    (STAGING_TABLE, True),
    ("world-fishing-827.pipe_ais_test_202408290000_published.segment_info", True),
    (PROD_TABLE, False),
    ("world-fishing-827.tech_great_expectations.dit_exp_x_cross_version_messages_positions", False),
    ("pipe_ais_test_x.table", False),  # not project.dataset.table
])
def test_staging_sources_are_the_ais_test_cohort(table, staging):
    assert dit_workflow.is_staging_source(table) is staging


def test_hint_when_production_sizing_meets_staging_data():
    hint = dit_workflow.production_sizing_hint(
        WorkerSizing(), {"thin": E2_HIGHMEM_4, "info": WorkerSizing()},
        [None, STAGING_TABLE, PROD_TABLE, STAGING_TABLE.replace("messages", "other")],
    )

    assert hint is not None
    assert "thin machine type e2-highmem-4, disk 50 GB" in hint and "info" not in hint
    assert f"({STAGING_TABLE} and 1 more)" in hint
    assert "--worker-machine-type / --disk-size-gb" in hint


@pytest.mark.parametrize("cli, sizings, sources", [
    (WorkerSizing("n2", 20), {"thin": WorkerSizing("n2", 20)}, [STAGING_TABLE]),  # all overridden
    (WorkerSizing(), {"thin": E2_HIGHMEM_4}, [PROD_TABLE, None]),                 # production data
    (WorkerSizing(), {"info": WorkerSizing()}, [STAGING_TABLE]),                  # nothing mirrored
])
def test_no_hint_otherwise(cli, sizings, sources):
    assert dit_workflow.production_sizing_hint(cli, sizings, sources) is None


def test_a_partial_override_still_hints_about_the_mirrored_field():
    hint = dit_workflow.production_sizing_hint(
        WorkerSizing(None, 20), {"thin": WorkerSizing("e2-highmem-4", 20)}, [STAGING_TABLE],
    )
    assert hint is not None and "e2-highmem-4" in hint


def test_the_hint_is_a_warning_and_nothing_more(caplog):
    caplog.set_level(logging.INFO, logger="dit.workflow")
    assert dit_workflow.log_production_sizing_hint(
        WorkerSizing(), {"thin": E2_HIGHMEM_4}, [STAGING_TABLE],
    ) is None
    dit_workflow.log_production_sizing_hint(WorkerSizing(), {"thin": E2_HIGHMEM_4}, [PROD_TABLE])

    assert len(_hints(caplog)) == 1


# --------------------------------------------------------------------------
# port visits: both CLIs, both steps
# --------------------------------------------------------------------------

def _port_visits_argvs(*cli):
    """The argv of each selected step of one slice (both by default), docker mocked."""
    args = port_visits.parse_args([*INFRA_ARGV, *cli])
    args.run_id, args.commit_sha, args.worker_image_tag, args.launched_by = "r", "c", "t", "u"
    calls = []
    with patch.object(port_visits.dit_docker, "run",
                      side_effect=lambda image, argv, **k: calls.append(argv) or 0):
        port_visits._run_slice(args, mode=port_visits.MODE_BF, slice_start=date(2020, 1, 1),
                               slice_end=date(2020, 12, 31), suffix="s", iteration=1,
                               total_iterations=1)
    assert [argv[0] for argv in calls][-1] in ("port_visits", "port-visits")
    return calls


def test_port_visits_steps_mirror_production():
    assert port_visits.STEP_WORKER_SIZING == {
        port_visits.STEP_THIN: E2_HIGHMEM_4, port_visits.STEP_VISITS: E2_HIGHMEM_4,
    }
    # The production-baseline target records the same sizing.
    assert cross_version.PRODUCTION_TARGETS["ais-v5-datalake"].worker_sizing == E2_HIGHMEM_4


@pytest.mark.parametrize("cli", ["v4", "v5"])
def test_port_visits_dataflow_jobs_get_private_ips_and_production_sizing(cli):
    argvs = _port_visits_argvs("--pipeline-cli", cli)
    assert len(argvs) == 2
    for argv in argvs:
        assert _ip_tokens(argv) == [NO_PUBLIC_IPS_OPTION]
        assert argv[argv.index("--subnetwork=net") + 1] == NO_PUBLIC_IPS_OPTION
        cap = argv.index("--max_num_workers=4")
        assert argv[cap + 1:cap + 3] == E2_HIGHMEM_4_TOKENS


@pytest.mark.parametrize("cli", ["v4", "v5"])
def test_port_visits_direct_runner_gets_no_dataflow_options(cli):
    for argv in _port_visits_argvs("--pipeline-cli", cli, "--runner", "docker", *SIZING_ARGV):
        assert _ip_tokens(argv) == [] and _sizing_tokens(argv) == []


@pytest.mark.parametrize("cli", ["v4", "v5"])
def test_port_visits_flags_override_production_field_by_field(cli):
    for argv in _port_visits_argvs("--pipeline-cli", cli, *SIZING_ARGV):
        cap = argv.index("--max_num_workers=4")
        assert argv[cap + 1:cap + 3] == SIZING_TOKENS
    only_disk = _port_visits_argvs("--pipeline-cli", cli, "--disk-size-gb", "60")
    assert [_sizing_tokens(argv) for argv in only_disk] == [
        ["--worker_machine_type=e2-highmem-4", "--disk_size_gb=60"]
    ] * 2


def _port_visits_main(*cli):
    ctx = RunContext(pipeline_commit="c", unreviewed=False, pipeline_commit_parent=None,
                     worker_image="img", worker_image_digest="img", run_id="r",
                     dit_commit="d")
    with (
        patch.object(port_visits, "resolve_run_context", return_value=ctx),
        patch.object(port_visits, "resolve_worker_image_to_digest", return_value="sha"),
    ):
        assert port_visits.main([*INFRA_ARGV, "--skip-pipelines", "--skip-comparisons",
                                 *cli]) == 0


@pytest.mark.parametrize("cli, hinted", [
    ([], True),                                          # staging cohort, production sizing
    (["--disk-size-gb", "20"], True),                    # machine type still production's
    (SIZING_ARGV, False),                                # both overridden
    (["--runner", "docker"], False),                     # no Dataflow
    (["--source-messages-fqn", PROD_TABLE, "--steps", "thin_port_messages"], False),
])
def test_port_visits_hints_on_staging_data(cli, hinted, caplog):
    caplog.set_level(logging.INFO)
    _port_visits_main(*cli)

    assert bool(_hints(caplog)) is hinted


# --------------------------------------------------------------------------
# encounters: AIS and VMS, both steps
# --------------------------------------------------------------------------

def _encounters_argvs(module, *cli):
    args = module.parse_args([*INFRA_ARGV, "--suffix", "s", *cli])
    args.run_id, args.commit_sha = "r", "c"
    calls = []
    with patch.object(encounters_ais.dit_docker, "run",
                      side_effect=lambda image, argv, **k: calls.append(argv) or 0):
        encounters_ais._run_slice(args, mode=encounters_ais.MODE_BF,
                                  slice_start=date(2020, 1, 1), slice_end=date(2020, 1, 10),
                                  suffix="s", iteration=1, total_iterations=1)
    assert [argv[0] for argv in calls] == ["create_raw_encounters", "merge_encounters"]
    return calls


def test_encounters_steps_mirror_production():
    assert encounters_ais.STEP_WORKER_SIZING == {
        "create_raw_encounters": E2_HIGHMEM_4, "merge_encounters": E2_HIGHMEM_4,
    }


@pytest.mark.parametrize("module", [encounters_ais, encounters_vms])
def test_encounters_dataflow_jobs_get_private_ips_and_production_sizing(module):
    for argv in _encounters_argvs(module):
        assert _ip_tokens(argv) == [NO_PUBLIC_IPS_OPTION]
        assert argv[argv.index("--subnetwork=net") + 1] == NO_PUBLIC_IPS_OPTION
        cap = argv.index("--max_num_workers=50")
        assert argv[cap + 1:cap + 3] == E2_HIGHMEM_4_TOKENS
    for argv in _encounters_argvs(module, *SIZING_ARGV):
        cap = argv.index("--max_num_workers=50")
        assert argv[cap + 1:cap + 3] == SIZING_TOKENS


@pytest.mark.parametrize("module", [encounters_ais, encounters_vms])
def test_encounters_direct_runner_gets_no_dataflow_options(module):
    for argv in _encounters_argvs(module, "--runner", "docker", *SIZING_ARGV):
        assert _ip_tokens(argv) == [] and _sizing_tokens(argv) == []


@pytest.mark.parametrize("module, cli, hinted", [
    (encounters_ais, [], True),           # AIS staging cohort
    (encounters_ais, SIZING_ARGV, False),
    (encounters_vms, [], False),          # VMS sources are production data
])
def test_encounters_hint_on_staging_data(module, cli, hinted, caplog):
    caplog.set_level(logging.INFO)
    ctx = SimpleNamespace(pipeline_commit="c", unreviewed=False, worker_image="img",
                          run_id="r")
    args = module.parse_args([*INFRA_ARGV, "--modes", "1_bf", "--skip-pipelines",
                              "--skip-comparisons", *cli])
    with patch.object(encounters_ais, "resolve_run_context", return_value=ctx):
        encounters_ais.run(args)

    assert bool(_hints(caplog)) is hinted


# --------------------------------------------------------------------------
# pipe-segment: per step
# --------------------------------------------------------------------------

def _segment_argvs(*cli, runner="dataflow"):
    """The three steps' argv through _run_binding, in pipeline order."""
    args = pipe_segment.parse_args([*SEGMENT_ARGV, "--runner", runner, *cli])
    calls = []
    ctx = SimpleNamespace(worker_image="img", pipeline_commit="c", unreviewed=False)
    with (
        patch.object(pipe_segment.subprocess, "run"),
        patch.object(pipe_segment, "_verify_gpsdio_segment", return_value="pin"),
        patch.object(pipe_segment, "resolve_run_context", return_value=ctx),
        patch.object(pipe_segment, "_run_pipe_subcommand",
                     side_effect=lambda **k: calls.append(k["cli_args"]) or 0),
    ):
        assert pipe_segment._run_binding(
            name="before", ref="v5.0.3", args=args, suffix="exp_before",
            snapshot_fqns=pipe_segment._PipeSegmentSnapshotFQNs(normalized_messages="p.d.n"),
        ) == 0
    assert [argv[0] for argv in calls] == ["segment", "segment_identity", "segment_info"]
    return calls


def test_pipe_segment_steps_keep_private_ips():
    for argv in _segment_argvs():
        assert _ip_tokens(argv) == [NO_PUBLIC_IPS_OPTION], argv[0]
    for argv in _segment_argvs(*SIZING_ARGV, runner="DirectRunner"):
        assert _ip_tokens(argv) == [] and _sizing_tokens(argv) == [], argv[0]


STANDARD_4_TOKENS = ["--worker_machine_type=e2-standard-4", "--disk_size_gb=50"]


@pytest.mark.parametrize("cli, expected", [
    # Production: segment and segment_identity e2-standard-4 / 50 GB; segment_info none.
    ([], [STANDARD_4_TOKENS, STANDARD_4_TOKENS, []]),
    (["--disk-size-gb", "100"],
     [["--worker_machine_type=e2-standard-4", "--disk_size_gb=100"]] * 2
     + [["--disk_size_gb=100"]]),
    (["--worker-machine-type", "n2-standard-8"],
     [["--worker_machine_type=n2-standard-8", "--disk_size_gb=50"]] * 2
     + [["--worker_machine_type=n2-standard-8"]]),
    (SIZING_ARGV, [SIZING_TOKENS] * 3),
])
def test_pipe_segment_resolves_the_sizing_per_step(cli, expected):
    argvs = _segment_argvs(*cli)

    assert [_sizing_tokens(argv) for argv in argvs] == expected
    for argv in argvs:
        cap = next(i for i, a in enumerate(argv) if a.startswith("--max_num_workers="))
        assert argv[cap + 1:cap + 1 + len(_sizing_tokens(argv))] == _sizing_tokens(argv)


def test_pipe_segment_steps_mirror_production():
    assert pipe_segment.STEP_WORKER_SIZING == {
        "segment": E2_STANDARD_4,
        "segment_identity": E2_STANDARD_4,
        "segment_info": WorkerSizing(),
    }


class _Stop(Exception):
    """Raised by a stub, to stop a workflow after the part under test."""


@pytest.mark.parametrize("cli, hinted", [
    ([], True),
    (SIZING_ARGV, False),
    (["--runner", "DirectRunner"], False),
    (["--source-normalized-table", PROD_TABLE], False),
])
def test_pipe_segment_hints_on_staging_data(cli, hinted, caplog):
    caplog.set_level(logging.INFO)
    with (
        patch.object(pipe_segment, "_verify_refs", side_effect=_Stop),
        pytest.raises(_Stop),
    ):
        pipe_segment.main([*SEGMENT_ARGV, *cli])

    assert bool(_hints(caplog)) is hinted


# --------------------------------------------------------------------------
# pipe-gaps: in-process submission through gfw-common
# --------------------------------------------------------------------------

GAPS_MODULES = [gaps_modes, gaps_outage]
GAPS_DAILY_ARGS = ["--machine_type=e2-custom-2-3072", "--disk_size_gb=25"]


def test_gaps_mirror_productions_daily_detect():
    assert gaps_modes.DEFAULT_WORKER_SIZING == GAPS_DAILY
    assert gaps_outage.DEFAULT_WORKER_SIZING is gaps_modes.DEFAULT_WORKER_SIZING


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


@pytest.fixture
def gaps_stack(monkeypatch):
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


def _gaps_cfg(module, sizing=None):
    return module._make_config(
        start=date(2020, 1, 1), end=date(2020, 1, 2), bq_input_messages="p.d.m",
        bq_input_segments="p.d.s", bq_output_gaps="p.d.out", ssvids=(), min_gap_length=1.0,
        n_hours_before=12, window_period_d=2, filter_good_seg=True, skip_open_gaps=False,
        service_account="sa", bq_temp_dataset=None, dataflow_region="us-central1",
        dataflow_temp_bucket="b", dataflow_subnetwork="net", worker_image="img",
        job_name="job", labels=["dit_run_id=r"], max_num_workers=50, worker_sizing=sizing,
    )


@pytest.mark.parametrize("module", GAPS_MODULES)
@pytest.mark.parametrize("sizing, expected", [
    (None, ["--max_num_workers=50", *GAPS_DAILY_ARGS]),
    (WorkerSizing("n2-standard-8", 100),
     ["--max_num_workers=50", "--machine_type=n2-standard-8", "--disk_size_gb=100"]),
    (WorkerSizing("e2-custom-2-3072", 60),
     ["--max_num_workers=50", "--machine_type=e2-custom-2-3072", "--disk_size_gb=60"]),
])
def test_gaps_runner_gets_the_sizing_and_nothing_about_ips(module, sizing, expected,
                                                           monkeypatch):
    _install_modules(monkeypatch, **{"pipe_gaps.pipelines.detect.factory": _module(
        "pipe_gaps.pipelines.detect.factory", DetectGapsLinearDagFactory=object)})
    calls = []
    with patch.object(module.dit_dataflow, "run", side_effect=lambda **k: calls.append(k) or 0):
        module._run_pipeline("dataflow", _gaps_cfg(module, sizing), "local-tag")

    assert calls[0]["args"] == expected


@pytest.mark.parametrize("module", GAPS_MODULES)
@pytest.mark.parametrize("sizing, machine_type, disk", [
    (None, "e2-custom-2-3072", "25"),
    (WorkerSizing("n2-standard-8", 100), "n2-standard-8", "100"),
])
def test_gaps_options_leave_gfw_commons_private_ip_default_alone(
    module, sizing, machine_type, disk, gaps_stack,
):
    """gfw-common's Pipeline.default_options() has use_public_ips=False, which
    Beam renders as --no_use_public_ips. It applies only if nothing dit hands
    over carries a public-IP key; the sizing replaces gfw-common's
    machine_type / disk_size_gb keys by name."""
    with pytest.raises(_Stop):
        module._run_pipeline("dataflow", _gaps_cfg(module, sizing), "local-tag")

    ns = gaps_stack["ns"]
    options = ns.unknown_parsed_args
    assert not any("public_ips" in key for key in options)
    # DetectGapsConfig is a dataclass built from vars(ns): a stray attribute crashes it.
    assert not hasattr(ns, "worker_sizing")
    assert options["machine_type"] == machine_type
    assert options["disk_size_gb"] == disk
    assert "worker_machine_type" not in options


def test_gfw_commons_default_is_private_ips_when_installed():
    """Runs where pipe-gaps (and so gfw-common) is installed, e.g. after
    `make install-pipe-gaps`; skipped in the framework-only venv."""
    base = pytest.importorskip("gfw.common.beam.pipeline.base")
    assert base.Pipeline.default_options()["use_public_ips"] is False


@pytest.mark.parametrize("module", GAPS_MODULES)
def test_gaps_docker_runner_gets_no_sizing(module):
    calls = []
    with patch.object(module.dit_docker, "run", side_effect=lambda **k: calls.append(k) or 0):
        module._run_pipeline("docker", _gaps_cfg(module, WorkerSizing("n2", 100)), "local-tag")

    assert not any("machine" in str(a) or "disk" in str(a) for a in calls[0]["args"])


def _ctx():
    return RunContext(pipeline_commit="c", unreviewed=False, pipeline_commit_parent=None,
                      worker_image="img", worker_image_digest="img", run_id="r",
                      dit_commit="d")


@pytest.mark.parametrize("module, argv", [
    (gaps_modes, ["--modes", "1_bf"]),
    (gaps_outage, ["--no-snapshot"]),
])
@pytest.mark.parametrize("cli, expected, hinted", [
    ([], GAPS_DAILY, True),
    (["--disk-size-gb", "40"], WorkerSizing("e2-custom-2-3072", 40), True),
    (SIZING_ARGV, WorkerSizing("n2-standard-8", 100), False),
])
def test_gaps_main_threads_the_sizing_into_every_stage(module, argv, cli, expected, hinted,
                                                       caplog):
    caplog.set_level(logging.INFO)
    seen = []
    with (
        patch.object(module, "resolve_run_context", return_value=_ctx()),
        patch.object(module, "_run_with_cache",
                     side_effect=lambda fn, **k: seen.append(k["execute_kwargs"])
                     or k["output_fqn"]),
    ):
        assert module.main(["--experiment-id", "exp", "--skip-comparisons", *argv, *cli]) == 0

    assert seen
    assert all(kwargs["base_cfg"]["worker_sizing"] == expected for kwargs in seen)
    assert bool(_hints(caplog)) is hinted


# --------------------------------------------------------------------------
# cross_version_ais.py: forwarding, and production's sizing in baseline mode
# --------------------------------------------------------------------------

@pytest.mark.parametrize("argv, expected", [
    (CROSS_VERSION_ARGV, WorkerSizing()),  # each binding's ais.py default applies
    ([*CROSS_VERSION_ARGV, *SIZING_ARGV], WorkerSizing("n2-standard-8", 100)),
    (PRODUCTION_ARGV, E2_HIGHMEM_4),
    ([*PRODUCTION_ARGV, "--disk-size-gb", "100"], WorkerSizing("e2-highmem-4", 100)),
    ([*PRODUCTION_ARGV, "--worker-machine-type", "n2-standard-8"],
     WorkerSizing("n2-standard-8", 50)),
    ([*PRODUCTION_ARGV, *SIZING_ARGV], WorkerSizing("n2-standard-8", 100)),
])
def test_cross_version_resolves_the_sizing_for_every_binding(argv, expected):
    args, extras = cross_version.parse_args(argv)

    assert args.worker_sizing == expected
    assert not any("machine" in a or "disk" in a for a in extras)


def test_cross_version_drops_sizing_flags_from_extras():
    argv = cross_version._ais_args_for_binding(
        ["--worker-machine-type", "x", "--disk-size-gb=70", "--max-num-workers", "30"],
        pinned=cross_version._PinnedSources(
            start=date(2020, 1, 1), end=date(2020, 12, 31),
            messages_positions="p.d.m", segment_info="p.d.si", segs_activity="p.d.sa",
        ),
        suffix="s", experiment_id="exp", binding_name="after", modes=["1_bf"],
        worker_sizing=E2_HIGHMEM_4,
    )

    assert "x" not in argv and "--disk-size-gb=70" not in argv
    assert argv[:2] == ["--max-num-workers", "30"]
    i = argv.index("--worker-machine-type")
    assert argv[i:i + 4] == ["--worker-machine-type", "e2-highmem-4", "--disk-size-gb", "50"]


@pytest.mark.parametrize("extras, runner", [
    ([], "dataflow"), (["--runner", "docker"], "docker"), (["--runner=docker"], "docker"),
    (["--runner", "docker", "--runner=dataflow"], "dataflow"), (["--runner"], "dataflow"),
])
def test_cross_version_reads_the_bindings_runner_from_the_extras(extras, runner):
    assert cross_version._extra_runner(extras) == runner


def _cross_version_main(argv):
    """main() with everything external patched; returns each binding's ais.py argv."""
    production = "--baseline-production" in argv
    pinned = cross_version._PinnedSources(
        start=date(2012, 1, 1), end=date(2026, 10, 20), segment_info="p.d.si",
        segs_activity="p.d.sa", thinned="p.d.t", production_output="p.d.out",
    ) if production else cross_version._PinnedSources(
        start=date(2020, 1, 1), end=date(2020, 12, 31),
        messages_positions="p.d.m", segment_info="p.d.si", segs_activity="p.d.sa",
    )
    proc = MagicMock(stdout=io.StringIO(""), **{"wait.return_value": 0})
    with (
        patch.object(cross_version, "_verify_refs"),
        patch.object(cross_version, "_pin", return_value=pinned),
        patch.object(cross_version.subprocess, "run"),
        patch.object(cross_version.subprocess, "Popen", return_value=proc) as popen,
        patch.object(cross_version.dit_compare, "compare_tables", return_value=0),
    ):
        assert cross_version.main(argv) == 0
    return popen.call_args.args[0][2:]  # [python, ais.py, *argv]


@pytest.mark.parametrize("argv, expected_tokens", [
    (CROSS_VERSION_ARGV, E2_HIGHMEM_4_TOKENS),  # ais.py's own production default
    ([*CROSS_VERSION_ARGV, "--disk-size-gb", "20"],
     ["--worker_machine_type=e2-highmem-4", "--disk_size_gb=20"]),
    (PRODUCTION_ARGV, E2_HIGHMEM_4_TOKENS),
    ([*PRODUCTION_ARGV, "--disk-size-gb", "100"],
     ["--worker_machine_type=e2-highmem-4", "--disk_size_gb=100"]),
])
def test_every_binding_runs_on_the_resolved_sizing(argv, expected_tokens, caplog):
    """Through main(): the binding's ais.py argv, parsed by ais.py, emits the
    sizing and private IPs in every Dataflow job."""
    caplog.set_level(logging.INFO, logger="dit.workflow")
    argvs = _port_visits_argvs(*_cross_version_main(argv))

    assert len(argvs) == (1 if "--baseline-production" in argv else 2)
    for step_argv in argvs:
        assert _ip_tokens(step_argv) == [NO_PUBLIC_IPS_OPTION]
        assert _sizing_tokens(step_argv) == expected_tokens
    assert any(m.startswith("Dataflow worker sizing: every binding's") for m in caplog.messages)


@pytest.mark.parametrize("argv, hinted", [
    (CROSS_VERSION_ARGV, True),                          # pinned from the staging cohort
    ([*CROSS_VERSION_ARGV, *SIZING_ARGV], False),
    ([*CROSS_VERSION_ARGV, "--runner", "docker"], False),
    (PRODUCTION_ARGV, False),                            # production's own inputs
])
def test_cross_version_hints_on_staging_sources(argv, hinted, caplog):
    caplog.set_level(logging.INFO)
    _cross_version_main(argv)

    assert bool(_hints(caplog)) is hinted


# --------------------------------------------------------------------------
# runner-only: never part of a cache key
# --------------------------------------------------------------------------

CACHED = [
    (port_visits, [], port_visits.SELECTABLE_MODES),
    (gaps_modes, [], (*gaps_modes.SELECTABLE_MODES, gaps_modes.MODE_MUTATE_RECOVER)),
    (gaps_outage, ["--experiment-id", "exp"],
     (gaps_outage.MODE_OUTAGE_RECOVERY, gaps_outage.MODE_OUTAGE_ORACLE)),
]


@pytest.mark.parametrize("module, argv, modes", CACHED)
def test_cache_keys_ignore_the_sizing(module, argv, modes):
    unset = module.parse_args(argv)
    sized = module.parse_args([*argv, *SIZING_ARGV])

    for mode in modes:
        params = module.canonical_params_dict(unset, mode)
        assert module.canonical_params_dict(sized, mode) == params, mode
        assert not any(k in params for k in ("worker_machine_type", "disk_size_gb",
                                              "worker_sizing", "machine_type")), mode
