"""One pipe-gaps raw-gaps job (``workflows/pipe_gaps/raw_gaps.py``), v0.17.1.

Two groups:

* offline tests of what DIT builds (the namespace, the CLI argv, the windows,
  the cache rows, the comparison targets, the temp-dataset hook on a stub);
* tests against the real pipe-gaps v0.17.1, skipped unless pipe-gaps 0.17.1
  and gfw-common 0.24.x are installed (a throwaway venv with
  ``pip install "pipe-gaps @ git+file://<checkout>@v0.17.1"`` and this repo).
  They build the config and the pipeline offline: no BigQuery, no Dataflow.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from dit import workflow as dit_workflow
from dit.runners import dataflow as dit_dataflow
from dit.workflow import RunContext
from workflows.pipe_gaps import mode_equivalence as modes
from workflows.pipe_gaps import outage_recovery as outage
from workflows.pipe_gaps import raw_gaps

VIEW = "world-fishing-827.ds.three_way_x_1_bf"
LABELS = ["dit_run_id=rid01", "dit_commit_sha=abc"]


def _cfg(**overrides: Any) -> SimpleNamespace:
    kwargs = dict(
        start=date(2020, 1, 5), end=date(2020, 1, 9),
        bq_in_messages="world-fishing-827.ds.messages",
        bq_in_segments="world-fishing-827.ds.segments",
        bq_out_gaps=VIEW, ssvids=("111", "222"), min_gap_length=1.0,
        n_hours_before=12, window_period_d=2, filter_good_seg=True, skip_open_gaps=False,
        labels=LABELS, job_name="dit-pipe-gaps-detect-x",
    )
    kwargs.update(overrides)
    return raw_gaps.make_config(**kwargs)


# --------------------------------------------------------------------------
# the namespace and the argv
# --------------------------------------------------------------------------

def test_make_config_names_raw_gaps_config_fields_and_holds_the_fixed_values() -> None:
    cfg = _cfg()

    assert (cfg.start_date, cfg.end_date) == (date(2020, 1, 5), date(2020, 1, 9))
    assert cfg.bq_out_gaps == VIEW
    assert cfg.versioned_suffix == "versioned"
    assert cfg.open_gaps_start_date == "2012-01-01"
    assert cfg.eval_last is True
    assert cfg.good_seg_stabilization_days == 0
    assert cfg.unknown_unparsed_args == ()


def test_labels_are_a_config_field_and_not_a_beam_option() -> None:
    """gfw-common's PipelineFactory passes ``labels=config.labels`` and spreads
    ``unknown_parsed_args`` into the same call: labels in both would collide."""
    cfg = _cfg()

    assert cfg.labels == {"dit_run_id": "rid01", "dit_commit_sha": "abc"}
    assert cfg.unknown_parsed_args == {"project": raw_gaps.PROJECT}


@pytest.mark.parametrize("labels", [[], ()])
def test_a_job_without_labels_is_refused(labels) -> None:
    with pytest.raises(ValueError, match="requires labels"):
        _cfg(labels=labels)


def test_a_label_without_a_value_separator_is_refused() -> None:
    with pytest.raises(ValueError, match="key=value"):
        raw_gaps.labels_dict(["dit_run_id"])


def test_the_docker_argv_is_productions_command_and_flag_names() -> None:
    assert raw_gaps.docker_argv(_cfg()) == [
        "raw-gaps",
        "--start-date", "2020-01-05",
        "--end-date", "2020-01-09",
        "--bq-in-messages", "world-fishing-827.ds.messages",
        "--bq-in-segments", "world-fishing-827.ds.segments",
        "--bq-out-gaps", VIEW,
        "--versioned-suffix", "versioned",
        "--open-gaps-start-date", "2012-01-01",
        "--min-gap-length", "1.0",
        "--n-hours-before", "12",
        "--window-period-d", "2",
        "--filter-good-seg",
        "--eval-last",
        "--good-seg-stabilization-days", "0",
        "--ssvids", "111,222",
        "--labels=dit_run_id=rid01",
        "--labels=dit_commit_sha=abc",
        "--project", raw_gaps.PROJECT,
    ]


def test_boolean_flags_are_gfw_commons_on_off_pairs() -> None:
    argv = raw_gaps.cfg_to_cli_flags(_cfg(filter_good_seg=False, skip_open_gaps=True, ssvids=()))

    assert "--filter-good-seg" not in argv and "--no-filter-good-seg" not in argv
    assert "--skip-open-gaps" in argv
    assert "--ssvids" not in argv
    assert not any(token in {"true", "True", "false", "False"} for token in argv)


def test_the_docker_runner_runs_the_argv_from_the_checkout() -> None:
    calls = []
    with patch.object(raw_gaps.dit_docker, "run", side_effect=lambda **kw: calls.append(kw) or 0):
        raw_gaps.run_pipeline("docker", _cfg(), "gfw/pipe-gaps:dev")

    (call,) = calls
    assert call["entrypoint"] == "pipe-gaps"
    assert call["build_from_source"] is True
    assert call["args"] == raw_gaps.docker_argv(_cfg())


def test_the_dataflow_runner_needs_a_worker_image() -> None:
    with pytest.raises(RuntimeError, match="worker-image"):
        raw_gaps.run_pipeline("dataflow", _cfg(worker_image=None), "gfw/pipe-gaps:dev")


# --------------------------------------------------------------------------
# outputs, cache key, defaults
# --------------------------------------------------------------------------

def test_the_view_is_compared_and_both_view_and_table_are_cleaned_up() -> None:
    assert raw_gaps.versioned_table(VIEW) == f"{VIEW}_versioned"
    assert raw_gaps.output_tables(VIEW) == (VIEW, f"{VIEW}_versioned")


def test_the_workflow_sha_covers_the_shared_module() -> None:
    for module in (modes, outage):
        workflow = Path(module.__file__).read_bytes()
        shared = Path(raw_gaps.__file__).read_bytes()
        assert module.WORKFLOW_FILE_SHA1 == hashlib.sha1(workflow + shared).hexdigest()
        assert module.WORKFLOW_FILE_SHA1 != hashlib.sha1(workflow).hexdigest()


def test_the_default_worker_image_is_productions_v0_17_1() -> None:
    assert raw_gaps.DEFAULT_WORKER_IMAGE == (
        "us-central1-docker.pkg.dev/gfw-int-infrastructure/core/pipe-gaps:v0.17.1"
    )
    assert modes.DEFAULT_WORKER_IMAGE == outage.DEFAULT_WORKER_IMAGE == raw_gaps.DEFAULT_WORKER_IMAGE
    assert modes.parse_args([]).worker_image == raw_gaps.DEFAULT_WORKER_IMAGE


def test_the_staging_defaults_keep_dits_min_gap_length() -> None:
    """Owner decision 2026-11-27: 1 hour on the staging cohort, production's 4
    is an accepted difference."""
    assert modes.parse_args([]).min_gap_length == 1.0
    assert outage.parse_args([]).min_gap_length == 1.0


# --------------------------------------------------------------------------
# the windows of every job: half-open, unchanged by the port
# --------------------------------------------------------------------------

def _windows(execute, **kwargs) -> list[tuple[date, date]]:
    captured: list[dict] = []
    module = kwargs.pop("module")
    with (
        patch.object(module, "_make_config", side_effect=lambda **kw: captured.append(kw)),
        patch.object(module, "_run_pipeline"),
    ):
        execute(**kwargs)
    return [(job["start"], job["end"]) for job in captured]


def test_mode_equivalence_windows_are_half_open_and_reach_the_end() -> None:
    common = dict(module=modes, runner="dataflow", base_cfg={}, start=date(2020, 1, 1),
                  end=date(2021, 1, 1), output=VIEW, experiment_id="e", image_tag="t")
    daily = dict(tail_days=4, backfill_days_w=4)

    assert _windows(modes.execute_bf, **common) == [(date(2020, 1, 1), date(2021, 1, 1))]
    tail = [
        (date(2020, 12, 25), date(2020, 12, 29)), (date(2020, 12, 26), date(2020, 12, 30)),
        (date(2020, 12, 27), date(2020, 12, 31)), (date(2020, 12, 28), date(2021, 1, 1)),
    ]
    assert _windows(modes.execute_bfd, **common, **daily) == [
        (date(2020, 1, 1), date(2020, 12, 28)), *tail,
    ]
    assert _windows(modes.execute_bftruncate, **common, **daily) == [
        (date(2020, 1, 1), date(2021, 1, 1)), *tail,
    ]


def test_outage_windows_convert_the_inclusive_end() -> None:
    windows = _windows(
        outage.execute_outage_recovery, module=outage, runner="dataflow", base_cfg={},
        start=date(2020, 1, 1), backfill_end=date(2020, 12, 28),
        outage_start=date(2020, 12, 29), outage_end=date(2020, 12, 29),
        end=date(2020, 12, 31), recovery_buffer_days=0, output=VIEW,
        experiment_id="e", image_tag="t",
    )

    assert windows == [
        (date(2020, 1, 1), date(2020, 12, 29)),
        (date(2020, 12, 30), date(2021, 1, 1)),
        (date(2020, 12, 29), date(2021, 1, 1)),
    ]


# --------------------------------------------------------------------------
# both workflows: what they record and what they compare
# --------------------------------------------------------------------------

def _ctx() -> RunContext:
    return RunContext(pipeline_commit="c", unreviewed=False, pipeline_commit_parent=None,
                      worker_image=raw_gaps.DEFAULT_WORKER_IMAGE, worker_image_digest="d",
                      run_id="rid01", dit_commit="dc")


@pytest.mark.parametrize("module, argv", [
    (modes, ["--modes", "1_bf,2_bfd"]),
    (outage, ["--no-pin"]),
])
def test_workflows_record_view_and_table_and_compare_the_views(module, argv) -> None:
    rows: list[Any] = []
    with (
        patch.object(module, "resolve_run_context", return_value=_ctx()),
        patch.object(module, "_run_pipeline") as run,
        patch.object(dit_workflow, "read_cache", return_value=None),
        patch.object(dit_workflow, "write_cache", side_effect=rows.append),
        patch.object(dit_workflow, "expires_at_for"),
        patch.object(module.dit_compare, "compare_tables", return_value=0) as compare,
    ):
        assert module.main(["--experiment-id", "exp", *argv]) == 0

    assert run.called
    assert rows and all(
        row.output_tables == list(raw_gaps.output_tables(row.output_tables[0])) for row in rows
    )
    views = {row.output_tables[0] for row in rows}
    (call,) = compare.call_args_list
    assert set(call.args) == views
    assert call.kwargs == {"keys": ("gap_id", "start_timestamp")}
    # Every job of the run carries the cleanup label, in the config's labels.
    for job_call in run.call_args_list:
        cfg = job_call.args[1]
        assert cfg.labels["dit_run_id"] == "rid01"


# --------------------------------------------------------------------------
# the temp-dataset hook, on a stub factory
# --------------------------------------------------------------------------

class _FactoryWithKwargs:
    def __init__(self, config) -> None:
        self.config = config

    @property
    def read_from_bigquery_kwargs(self) -> dict:
        return {"bigquery_job_labels": {"dit_run_id": "rid01"}}


class _FactoryOfV0_10:
    """pipe-gaps v0.10.0's factory had no read_from_bigquery_kwargs."""

    def __init__(self, config) -> None:
        self.config = config


def test_the_hook_refuses_a_factory_whose_reads_it_cannot_reach() -> None:
    with pytest.raises(TypeError, match="read_from_bigquery_kwargs"):
        dit_dataflow._wrap_factory_with_temp_dataset(_FactoryOfV0_10, "world-fishing-827.ds")


def test_the_hook_adds_the_temp_dataset_and_keeps_the_labels() -> None:
    pytest.importorskip("apache_beam.io.gcp.internal.clients")
    wrapped = dit_dataflow._wrap_factory_with_temp_dataset(
        _FactoryWithKwargs, "world-fishing-827.tech_great_expectations",
    )
    kwargs = wrapped(config=None).read_from_bigquery_kwargs

    assert kwargs["bigquery_job_labels"] == {"dit_run_id": "rid01"}
    assert (kwargs["temp_dataset"].projectId, kwargs["temp_dataset"].datasetId) == (
        "world-fishing-827", "tech_great_expectations",
    )


def test_the_hook_does_not_replace_a_temp_dataset_the_factory_sets() -> None:
    pytest.importorskip("apache_beam.io.gcp.internal.clients")

    class Own(_FactoryWithKwargs):
        @property
        def read_from_bigquery_kwargs(self) -> dict:
            return {"temp_dataset": "own"}

    wrapped = dit_dataflow._wrap_factory_with_temp_dataset(Own, "world-fishing-827.ds")
    assert wrapped(config=None).read_from_bigquery_kwargs == {"temp_dataset": "own"}


# --------------------------------------------------------------------------
# the real pipe-gaps v0.17.1 (skipped elsewhere)
# --------------------------------------------------------------------------

def _installed(dist: str) -> str | None:
    try:
        return importlib.metadata.version(dist)
    except importlib.metadata.PackageNotFoundError:
        return None


real_pipe_gaps = pytest.mark.skipif(
    _installed("pipe-gaps") != "0.17.1"
    or not (_installed("gfw-common") or "").startswith("0.24."),
    reason="needs pipe-gaps 0.17.1 and gfw-common 0.24.x installed",
)


def _real_config(cfg: SimpleNamespace):
    from pipe_gaps.pipelines.raw_gaps.config import RawGapsConfig

    attrs = {k: v for k, v in vars(cfg).items() if k not in raw_gaps.RUNNER_ONLY_ATTRS}
    return RawGapsConfig.from_namespace(SimpleNamespace(**attrs))


@real_pipe_gaps
def test_real_config_accepts_dits_namespace() -> None:
    config = _real_config(_cfg())

    assert (config.start_date, config.end_date) == (date(2020, 1, 5), date(2020, 1, 9))
    assert config.bq_out_gaps == VIEW
    assert config.bq_out_gaps_versioned == raw_gaps.versioned_table(VIEW)
    assert config.labels == {"dit_run_id": "rid01", "dit_commit_sha": "abc"}
    assert config.eval_last is True and config.good_seg_stabilization_days == 0
    # No stabilization hook: only create-table and the pre-write delete.
    assert len(config.pre_hooks) == 2


@real_pipe_gaps
def test_real_cli_parses_the_docker_argv_to_the_same_config() -> None:
    """What the docker runner passes and what the Dataflow path builds
    in-process are the same job."""
    import pipe_gaps.cli.commands.raw_gaps as command
    from pipe_gaps.cli import main as cli

    seen = []
    with patch.object(command, "run", side_effect=lambda config, **_: seen.append(config)):
        cli.run(raw_gaps.docker_argv(_cfg()))

    (from_cli,) = seen
    from_dit = _real_config(_cfg()).to_dict()
    # The CLI leaves unset booleans None (falsy) and hands --project to Beam
    # unparsed; the in-process path passes it as a parsed option.
    ignored = {"unknown_parsed_args", "unknown_unparsed_args"}
    for key, value in from_cli.to_dict().items():
        if key not in ignored:
            assert (value or False) == (from_dit[key] or False), key
    assert from_cli.unknown_unparsed_args == ["--project", raw_gaps.PROJECT]


@real_pipe_gaps
def test_real_pre_write_delete_has_no_upper_bound() -> None:
    """The reprocess contract (workflows/pipe_gaps/CLAUDE.md) at this version."""
    config = _real_config(_cfg())
    sql = " ".join(config.table_config.delete_query(config.start_date, config.end_date).split())

    assert f"DELETE FROM `{raw_gaps.versioned_table(VIEW)}`" in sql
    assert "(is_closed = TRUE AND DATE(end_timestamp) >= '2020-01-05')" in sql
    assert "(is_closed = FALSE AND DATE(start_timestamp) >= '2020-01-05')" in sql
    assert "2020-01-09" not in sql


@real_pipe_gaps
def test_real_open_gaps_come_from_the_jobs_own_versioned_table() -> None:
    """How a job finds the open gaps of the jobs before it: DIT passes no
    bq_in_open_gaps, so the side input reads the job's own versioned table."""
    from pipe_gaps.pipelines.raw_gaps.factory import RawGapsLinearDagFactory

    side_input = RawGapsLinearDagFactory(_real_config(_cfg())).side_inputs
    sql = " ".join(side_input._query.split())

    assert f"FROM `{raw_gaps.versioned_table(VIEW)}` WHERE is_closed = FALSE" in sql
    assert "DATE(start_timestamp) >= '2012-01-01'" in sql
    # skip_open_gaps turns it off.
    assert RawGapsLinearDagFactory(_real_config(_cfg(skip_open_gaps=True))).side_inputs is None


@real_pipe_gaps
def test_real_pipeline_gives_every_bigquery_read_the_temp_dataset_and_labels() -> None:
    """Builds the pipeline the Dataflow runner would submit, on Beam's
    DirectRunner and without running it, and inspects the BigQuery sources
    the graph holds: the messages and the open-gaps side input."""
    import apache_beam as beam
    from apache_beam.pipeline import PipelineVisitor
    from pipe_gaps.pipelines.raw_gaps.factory import RawGapsLinearDagFactory

    temp = "world-fishing-827.tech_great_expectations"
    options = {
        "runner": "DirectRunner", "temp_location": "gs://example-bucket/tmp",
        "dag_factory_cls": dit_dataflow._wrap_factory_with_temp_dataset(
            RawGapsLinearDagFactory, temp),
        "bq_temp_dataset": temp,
    }
    pipeline = raw_gaps.build_pipeline_for(_cfg())(options)
    pipeline.apply_dag()

    sources: dict[str, Any] = {}

    class Reads(PipelineVisitor):
        def enter_composite_transform(self, node) -> None:
            self.visit_transform(node)

        def visit_transform(self, node) -> None:
            if isinstance(node.transform, beam.io.Read):
                sources[node.full_label] = node.transform.source

    pipeline.pipeline.visit(Reads())

    assert sorted(sources) == [
        "ReadMessages/ReadFromBigQuery/Read", "ReadOpenGaps/ReadFromBigQuery/Read",
    ]
    for source in sources.values():
        assert (source.temp_dataset.projectId, source.temp_dataset.datasetId) == (
            "world-fishing-827", "tech_great_expectations",
        )
        assert source.bigquery_job_labels == {"dit_run_id": "rid01", "dit_commit_sha": "abc"}
    assert pipeline.pipeline_options.get_all_options()["job_name"] == "dit-pipe-gaps-detect-x"
