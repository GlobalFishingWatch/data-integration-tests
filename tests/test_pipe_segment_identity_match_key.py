"""Tests for ``workflows/pipe_segment/identity_match_key.py``.

Two parts:

* The canonical-dataset migration (M3): the ``--snapshot-dest-project`` CLI
  flag, the ``_PipeSegmentSnapshotFQNs`` dataclass shape, and the
  ``_snapshot_source`` invocation pattern over
  ``dit.bq.snapshot_into_experiment``.
* The six-step chain a binding runs, shaped like production's segment group
  (2026-11-19): every step's argv on both runners, the labels on every step,
  no Dataflow option on the four BigQuery SQL steps, which table each step
  reads and writes, the compare keys, and the order ``_run_binding`` runs the
  steps in.

No docker, BigQuery or Dataflow call: the runner and the comparison are
patched. What the pipeline does with these argvs is not tested here.
"""

from __future__ import annotations

import argparse
import dataclasses
from datetime import date, datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from workflows.pipe_segment import identity_match_key as mod


# -- CLI flag: --snapshot-dest-project ------------------------------------

def _base_args(extra: list[str] | None = None) -> list[str]:
    """Minimal argv passing parse_args' required-arg validation."""
    return [
        "--experiment-id", "test01",
        "--pin-source-at", "2026-06-03T10:00:00Z",
        "--binding", "before=v5.0.3",
        "--date-range", "2020-01-01,2020-01-01",
    ] + (extra or [])


def test_snapshot_dest_project_defaults_to_PROJECT() -> None:
    args = mod.parse_args(_base_args())
    assert args.snapshot_dest_project == mod.PROJECT == "world-fishing-827"


def test_snapshot_dest_project_accepts_cross_org_override() -> None:
    args = mod.parse_args(_base_args(["--snapshot-dest-project", "gfw-int-pipe-v3"]))
    assert args.snapshot_dest_project == "gfw-int-pipe-v3"


# -- _PipeSegmentSnapshotFQNs dataclass ------------------------------------

def test_snapshot_fqns_normalized_only_when_satellite_offsets_off() -> None:
    """When --include-satellite-offsets is not set, the dataclass exposes
    only ``normalized_messages``; the satellite-offsets fields default to None."""
    fqns = mod._PipeSegmentSnapshotFQNs(
        normalized_messages="world-fishing-827.tech_great_expectations.dit_exp_test01_pipe_segment_normalized_messages",
    )
    assert fqns.normalized_messages.endswith("_normalized_messages")
    assert fqns.sat_positions_stem is None
    assert fqns.norad is None


def test_snapshot_fqns_is_frozen() -> None:
    """Dataclass is frozen: downstream code can't mutate snapshot pointers
    out from under callers reading them."""
    fqns = mod._PipeSegmentSnapshotFQNs(
        normalized_messages="proj.tech_great_expectations.x",
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        fqns.normalized_messages = "other"  # type: ignore[misc]


# -- _snapshot_source ------------------------------------------------------

def _snapshot_args(**overrides: Any) -> argparse.Namespace:
    base = dict(
        experiment_id="test01",
        source_normalized_table="world-fishing-827.pipe_ais_test_202408290000_internal.normalized_messages",
        pin_source_at=datetime(2026, 6, 3, 10, 0, 0, tzinfo=timezone.utc),
        snapshot_expiration_days=7,
        snapshot_dest_project="world-fishing-827",
        include_satellite_offsets=False,
        date_range=(date(2020, 1, 1), date(2020, 1, 1)),
    )
    base.update(overrides)
    return argparse.Namespace(**base)


def test_snapshot_source_no_satellite_offsets_calls_helper_once() -> None:
    """The base case (default --include-satellite-offsets off) makes exactly
    one ``snapshot_into_experiment`` call -- for normalized_messages."""
    args = _snapshot_args()
    expected_normalized = (
        "world-fishing-827.tech_great_expectations."
        "dit_exp_test01_pipe_segment_normalized_messages"
    )
    with patch.object(
        mod.dit_bq, "snapshot_into_experiment", return_value=expected_normalized
    ) as mock_helper:
        fqns = mod._snapshot_source(args)

    assert mock_helper.call_count == 1
    call = mock_helper.call_args
    assert call.args == (args.source_normalized_table,)
    assert call.kwargs["experiment_id"] == "test01"
    assert call.kwargs["role"] == "pipe_segment"
    assert call.kwargs["expiration_days"] == 7
    assert call.kwargs["as_of"] == args.pin_source_at
    assert call.kwargs["project"] == "world-fishing-827"

    assert fqns.normalized_messages == expected_normalized
    assert fqns.sat_positions_stem is None
    assert fqns.norad is None


def test_snapshot_source_with_satellite_offsets_calls_helper_for_each_source() -> None:
    """With ``--include-satellite-offsets``: one call for normalized_messages,
    N calls for satellite_positions shards (one per date in date_range),
    and one call for norad_to_receiver."""
    args = _snapshot_args(
        include_satellite_offsets=True,
        date_range=(date(2020, 1, 1), date(2020, 1, 3)),  # 3 days
    )
    # The helper's return value is the dest FQN; mock returns reflect the
    # canonical naming so the stem computation has something predictable
    # to strip from.
    def fake_return(source_table: str, *, experiment_id: str, role: str, **_: Any) -> str:
        basename = source_table.rsplit(".", 1)[-1]
        return f"world-fishing-827.tech_great_expectations.dit_exp_{experiment_id}_{role}_{basename}"

    with patch.object(
        mod.dit_bq, "snapshot_into_experiment", side_effect=fake_return,
    ) as mock_helper:
        fqns = mod._snapshot_source(args)

    # 1 normalized + 3 satellite-position shards + 1 norad = 5 calls.
    assert mock_helper.call_count == 5

    assert fqns.normalized_messages.endswith("_normalized_messages")
    assert fqns.norad is not None and fqns.norad.endswith("_norad_to_receiver_v20230510")
    assert fqns.sat_positions_stem is not None


def test_snapshot_source_sat_positions_stem_strips_date_suffix() -> None:
    """The satellite-positions stem must equal a per-shard dest FQN with
    the trailing ``<YYYYMMDD>`` stripped; pipe-segment appends the date at
    read time so the stem must end exactly where the date begins. Verify
    by capturing every dest FQN ``_snapshot_source`` produced and asserting
    that re-appending each shard's date to the stem reproduces a
    captured dest FQN — that's the load-bearing guarantee."""
    args = _snapshot_args(
        include_satellite_offsets=True,
        date_range=(date(2020, 1, 1), date(2020, 1, 3)),  # 3 shards
    )

    produced: list[str] = []

    def fake_return(source_table: str, *, experiment_id: str, role: str, **_: Any) -> str:
        basename = source_table.rsplit(".", 1)[-1]
        dest = f"world-fishing-827.tech_great_expectations.dit_exp_{experiment_id}_{role}_{basename}"
        produced.append(dest)
        return dest

    with patch.object(
        mod.dit_bq, "snapshot_into_experiment", side_effect=fake_return,
    ):
        fqns = mod._snapshot_source(args)

    assert fqns.sat_positions_stem is not None
    # The stem must end with the PROD_SAT_POS_STEM (trailing underscore).
    assert fqns.sat_positions_stem.endswith(f"_{mod.PROD_SAT_POS_STEM}")
    # Sanity: the shard-suffix helper still produces what we expect.
    assert mod._shard_suffix(date(2020, 1, 1)) == "20200101"
    # Load-bearing: re-appending each shard's date to the stem reproduces a
    # dest FQN that was actually created. This is what pipe-segment will do
    # at read time, so the stem MUST round-trip through this construction.
    for d in (date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3)):
        reconstructed_shard = f"{fqns.sat_positions_stem}{mod._shard_suffix(d)}"
        assert reconstructed_shard in produced, (
            f"shard reconstruction failed for {d}: "
            f"{reconstructed_shard!r} not in produced dests {produced!r}"
        )


def test_snapshot_source_threads_snapshot_dest_project() -> None:
    """When --snapshot-dest-project is overridden (the cross-org dodge path),
    every snapshot_into_experiment call must receive that project."""
    args = _snapshot_args(snapshot_dest_project="gfw-int-pipe-v3")
    with patch.object(
        mod.dit_bq, "snapshot_into_experiment", return_value="stub",
    ) as mock_helper:
        mod._snapshot_source(args)

    for call in mock_helper.call_args_list:
        assert call.kwargs["project"] == "gfw-int-pipe-v3"


# -- The six steps: what one binding runs ----------------------------------

BEAM_STEPS = ["segment", "segment_identity"]
SQL_STEPS = ["segment_vessel_daily", "segment_info", "vessel_info", "segment_vessel"]
RUNNERS = ["dataflow", "DirectRunner"]

OUT = "world-fishing-827.ds"          # project + --dest-dataset
SFX = "exp_before"                    # <experiment id>_<binding name>
TEMP = f"gs://{mod.DEFAULT_DATAFLOW_TEMP_BUCKET}/dit/dataflow_temp"
STAGING = f"gs://{mod.DEFAULT_DATAFLOW_TEMP_BUCKET}/dit/dataflow_staging"

#: Flags only a Beam or Dataflow job understands. None may reach a SQL step.
BEAM_ONLY_FLAGS = (
    "--runner", "--temp_location", "--staging_location", "--setup_file", "--wait_for_job",
    "--region", "--max_num_workers", "--worker_machine_type", "--disk_size_gb",
    "--experiments", "--no_use_public_ips", "--network", "--subnetwork",
    "--service_account_email", "--job_name", "--sdk_container_image",
)


def _labels(step: str) -> list[str]:
    return [
        "--labels=environment=develop",
        "--labels=resource_creator=dit",
        f"--labels=step={step}",
        "--labels=experiment_id=exp",
        "--labels=binding=before",
    ]


def _dataflow(step_in_job_name: str, *, setup_file: bool = False) -> list[str]:
    return [
        *(["--setup_file=./setup.py"] if setup_file else []),
        "--wait_for_job",
        "--region=us-central1",
        "--max_num_workers=50",
        "--worker_machine_type=e2-standard-4",
        "--disk_size_gb=50",
        "--no_use_public_ips",
        "--network=gfw-internal-network",
        "--subnetwork=regions/us-central1/subnetworks/gfw-internal-us-central1",
        "--service_account_email=sa@p.iam.gserviceaccount.com",
        f"--job_name=dit-pipe-segment-{step_in_job_name}-exp-before",
        "--sdk_container_image=registry.example.invalid/pipe-segment:v0",
    ]


def _expected(runner: str, *, setup_file: bool = False) -> dict[str, list[str]]:
    """Every step's argv for a two-day run of binding ``before``."""
    beam = [
        "--project=world-fishing-827",
        f"--runner={runner}",
        f"--temp_location={TEMP}",
        f"--staging_location={STAGING}",
    ]

    def dataflow(step: str) -> list[str]:
        return _dataflow(step, setup_file=setup_file) if runner == "dataflow" else []

    return {
        "segment": [
            "segment",
            "--date_range=2020-01-01,2020-01-02",
            "--in_normalized_messages_table=p.d.normalized",
            f"--out_segmented_messages_table={OUT}.messages_segmented_{SFX}",
            f"--out_segments_table={OUT}.segments_{SFX}",
            f"--fragments_table={OUT}.fragments_{SFX}",
            *_labels("segment"),
            *beam,
            *dataflow("segment"),
        ],
        "segment_identity": [
            "segment_identity",
            "--date_range=2020-01-01,2020-01-02",
            f"--source_segments={OUT}.segments_{SFX}",
            f"--source_fragments={OUT}.fragments_{SFX}",
            f"--dest_segment_identity={OUT}.segment_identity_daily_{SFX}",
            *_labels("segment_identity"),
            *beam,
            *dataflow("segment-identity"),
        ],
        "segment_vessel_daily": [
            "segment_vessel_daily",
            "--date_range=2020-01-01,2020-01-02",
            f"--source_segment_identity={OUT}.segment_identity_daily_{SFX}",
            f"--destination={OUT}.segment_vessel_daily_{SFX}",
            *_labels("segment_vessel_daily"),
            "--project=world-fishing-827",
        ],
        "segment_info": [
            "segment_info",
            f"--source_segment_identity={OUT}.segment_identity_daily_{SFX}",
            f"--source_segment_vessel={OUT}.segment_vessel_daily_{SFX}",
            f"--destination={OUT}.segment_info_{SFX}",
            *_labels("segment_info"),
            "--project=world-fishing-827",
        ],
        "vessel_info": [
            "vessel_info",
            f"--source_segment_identity={OUT}.segment_identity_daily_{SFX}",
            f"--source_segment_vessel={OUT}.segment_vessel_daily_{SFX}",
            f"--destination={OUT}.vessel_info_{SFX}",
            *_labels("vessel_info"),
            "--project=world-fishing-827",
        ],
        "segment_vessel": [
            "segment_vessel",
            f"--source_segment_vessel_daily={OUT}.segment_vessel_daily_{SFX}",
            f"--destination={OUT}.segment_vessel_{SFX}",
            *_labels("segment_vessel"),
            "--project=world-fishing-827",
        ],
    }


def _run_args(runner: str = "dataflow", *extra: str) -> argparse.Namespace:
    return mod.parse_args([
        "--experiment-id", "exp",
        "--pin-source-at", "2026-06-03T10:00:00Z",
        "--binding", "before=v5.0.3",
        "--date-range", "2020-01-01,2020-01-02",
        "--dest-dataset", "ds",
        "--service-account", "sa@p.iam.gserviceaccount.com",
        "--runner", runner,
        *extra,
    ])


def _argvs(runner: str = "dataflow", *extra: str) -> dict[str, list[str]]:
    return mod._step_argvs(
        _run_args(runner, *extra),
        binding_name="before",
        suffix=SFX,
        snapshot_fqns=mod._PipeSegmentSnapshotFQNs(normalized_messages="p.d.normalized"),
        worker_image="registry.example.invalid/pipe-segment:v0",
    )


def _flag_names(argv: list[str]) -> set[str]:
    return {token.split("=", 1)[0] for token in argv if token.startswith("--")}


def _value(argv: list[str], flag: str) -> str:
    (value,) = [t.split("=", 1)[1] for t in argv if t.startswith(f"{flag}=")]
    return value


def test_the_steps_are_productions_six_tasks_in_order() -> None:
    assert list(mod.BEAM_STEPS) == BEAM_STEPS
    assert list(mod.SQL_STEPS) == SQL_STEPS
    assert list(mod.STEPS) == BEAM_STEPS + SQL_STEPS
    assert list(_argvs()) == BEAM_STEPS + SQL_STEPS
    # Only the Beam steps have workers to cap or size.
    assert list(mod.STEP_MAX_NUM_WORKERS) == list(mod.STEP_WORKER_SIZING) == BEAM_STEPS


@pytest.mark.parametrize("runner", RUNNERS)
def test_every_steps_argv_is_pinned(runner: str) -> None:
    assert _argvs(runner) == _expected(runner)


@pytest.mark.parametrize("step", SQL_STEPS)
def test_sql_steps_run_the_same_on_both_runners_and_take_no_beam_option(step: str) -> None:
    on_dataflow, on_direct = _argvs("dataflow")[step], _argvs("DirectRunner")[step]

    assert on_dataflow == on_direct
    assert not _flag_names(on_dataflow) & set(BEAM_ONLY_FLAGS)
    # A sizing or cap flag given on the command line still reaches no SQL step.
    sized = _argvs("dataflow", "--max-num-workers", "7", "--disk-size-gb", "100")[step]
    assert sized == on_dataflow


@pytest.mark.parametrize("runner", RUNNERS)
@pytest.mark.parametrize("step", BEAM_STEPS + SQL_STEPS)
def test_every_step_carries_labels_on_every_runner(runner: str, step: str) -> None:
    """pipe-segment raises ``TypeError`` on a step without ``--labels``."""
    labels = [t for t in _argvs(runner)[step] if t.startswith("--labels=")]

    assert labels == _labels(step)
    # Each entry is one ``key=value``: pipe-segment splits it on ``=``.
    assert all(len(label.removeprefix("--labels=").split("=")) == 2 for label in labels)


def test_label_values_are_sanitised_for_bigquery() -> None:
    labels = mod._step_labels("segment_info", experiment_id="Exp.1", binding_name="a" * 70)

    assert "--labels=experiment_id=exp-1" in labels
    assert f"--labels=binding={'a' * 63}" in labels


@pytest.mark.parametrize("step", BEAM_STEPS)
def test_setup_file_is_passed_only_when_building_from_source_on_dataflow(step: str) -> None:
    """The published image has no source tree, and production passes none."""
    assert "--setup_file" not in _flag_names(_argvs("dataflow")[step])
    assert "--setup_file" not in _flag_names(
        _argvs("DirectRunner", "--build-from-source")[step])

    from_source = _argvs("dataflow", "--build-from-source")
    assert from_source == _expected("dataflow", setup_file=True)
    assert from_source[step].count("--setup_file=./setup.py") == 1


@pytest.mark.parametrize("extra", [(), ("--build-from-source",)])
def test_no_step_passes_experiments(extra: tuple[str, ...]) -> None:
    for runner in RUNNERS:
        for argv in _argvs(runner, *extra).values():
            assert "--experiments" not in _flag_names(argv)


def test_each_step_reads_what_the_step_before_it_wrote() -> None:
    argvs = _argvs()
    identity_table = _value(argvs["segment_identity"], "--dest_segment_identity")
    daily_table = _value(argvs["segment_vessel_daily"], "--destination")

    assert _value(argvs["segment_identity"], "--source_segments") == _value(
        argvs["segment"], "--out_segments_table")
    assert _value(argvs["segment_identity"], "--source_fragments") == _value(
        argvs["segment"], "--fragments_table")
    assert _value(argvs["segment_vessel_daily"], "--source_segment_identity") == identity_table
    for step in ("segment_info", "vessel_info"):
        assert _value(argvs[step], "--source_segment_identity") == identity_table
        # The segment_vessel_daily table, not the identity table as a stand-in.
        assert _value(argvs[step], "--source_segment_vessel") == daily_table
    assert _value(argvs["segment_vessel"], "--source_segment_vessel_daily") == daily_table
    assert daily_table != identity_table


def test_every_output_table_is_written_once_and_has_compare_keys() -> None:
    argvs = _argvs()
    written = [
        _value(argvs["segment"], "--out_segmented_messages_table"),
        _value(argvs["segment"], "--out_segments_table"),
        _value(argvs["segment"], "--fragments_table"),
        _value(argvs["segment_identity"], "--dest_segment_identity"),
        *(_value(argvs[step], "--destination") for step in SQL_STEPS),
    ]
    tables = [*mod.SEGMENT_OUTPUT_TABLES, *mod.DOWNSTREAM_OUTPUT_TABLES]

    assert written == [f"{OUT}.{table}_{SFX}" for table in tables]
    assert len(set(written)) == len(written)
    assert set(tables) <= set(mod.COMPARE_KEYS)


def test_compare_keys_of_the_downstream_tables() -> None:
    assert mod.DOWNSTREAM_OUTPUT_TABLES == (
        "segment_identity_daily", "segment_vessel_daily", "segment_info", "vessel_info",
        "segment_vessel",
    )
    assert {table: mod.COMPARE_KEYS[table] for table in mod.DOWNSTREAM_OUTPUT_TABLES} == {
        "segment_identity_daily": ["seg_id", "first_timestamp"],
        "segment_vessel_daily": ["seg_id", "ssvid", "day"],
        "segment_info": ["seg_id"],
        "vessel_info": ["vessel_id", "ssvid"],
        "segment_vessel": ["seg_id", "ssvid", "vessel_id"],
    }


def test_skip_downstream_runs_segment_only() -> None:
    assert list(_argvs("dataflow", "--skip-downstream")) == ["segment"]
    assert _argvs("dataflow", "--skip-downstream")["segment"] == _expected("dataflow")["segment"]


def test_satellite_offsets_stay_off_by_default_and_keep_their_flags_when_asked() -> None:
    satellite = {"--in_normalized_sat_offset_messages_table", "--in_norad_to_receiver_table",
                 "--in_sat_positions_table", "--out_sat_offsets_table"}
    assert _run_args().include_satellite_offsets is False
    assert not _flag_names(_argvs()["segment"]) & satellite

    asked = mod._step_argvs(
        _run_args("dataflow", "--include-satellite-offsets"),
        binding_name="before", suffix=SFX, worker_image=None,
        snapshot_fqns=mod._PipeSegmentSnapshotFQNs(
            normalized_messages="p.d.normalized", sat_positions_stem="p.d.sat_", norad="p.d.norad",
        ),
    )
    assert satellite <= _flag_names(asked["segment"])
    # The option reaches no other step.
    assert not any(_flag_names(asked[step]) & satellite for step in mod.STEPS[1:])


# -- Dataflow job names ------------------------------------------------------

def test_both_beam_steps_get_a_dit_job_name() -> None:
    argvs = _argvs()

    assert _value(argvs["segment"], "--job_name") == "dit-pipe-segment-segment-exp-before"
    assert _value(argvs["segment_identity"], "--job_name") == (
        "dit-pipe-segment-segment-identity-exp-before"
    )


def test_a_binding_name_too_long_for_a_job_name_fails_at_parse_time() -> None:
    longest_ok, too_long = "b" * 27, "b" * 28

    assert _run_args("dataflow", "--binding", f"{longest_ok}=v5.0.3")
    with pytest.raises(SystemExit, match="too long for the Dataflow job name of step "
                                         "'segment_identity'"):
        _run_args("dataflow", "--binding", f"{too_long}=v5.0.3")
    # No Dataflow job, no job name: DirectRunner accepts the name.
    assert _run_args("DirectRunner", "--binding", f"{too_long}=v5.0.3")


# -- _run_binding: the order, and stopping at the first failure --------------

def _run_binding(args: argparse.Namespace, *, rc_by_step: dict[str, int] | None = None):
    """Run one binding with the docker runner patched; returns ``(rc, calls)``
    where each call is ``(image, argv, keyword arguments)``."""
    rc_by_step = rc_by_step or {}
    calls = []
    ctx = SimpleNamespace(worker_image="registry.example.invalid/pipe-segment:v0",
                          pipeline_commit="c", unreviewed=False)

    def fake_run(image, argv, **kwargs):
        calls.append((image, argv, kwargs))
        return rc_by_step.get(argv[0], 0)

    with (
        patch.object(mod.subprocess, "run"),
        patch.object(mod, "_verify_gpsdio_segment", return_value="pin"),
        patch.object(mod, "resolve_run_context", return_value=ctx),
        patch.object(mod.dit_docker, "run", side_effect=fake_run),
    ):
        rc = mod._run_binding(
            name="before", ref="v5.0.3", args=args, suffix=SFX,
            snapshot_fqns=mod._PipeSegmentSnapshotFQNs(normalized_messages="p.d.normalized"),
        )
    return rc, calls


@pytest.mark.parametrize("runner", RUNNERS)
def test_run_binding_runs_the_six_steps_in_order_with_the_pinned_argv(runner: str) -> None:
    rc, calls = _run_binding(_run_args(runner))

    assert rc == 0
    assert [argv for _, argv, _ in calls] == list(_expected(runner).values())
    for image, _, kwargs in calls:
        assert image == "registry.example.invalid/pipe-segment:v0"
        assert kwargs["entrypoint"] == "pipe-segment"
        # Credentials and the project reach every step, the SQL ones included.
        assert kwargs["volumes"] == [mod.GCP_VOLUME]
        assert kwargs["container_env"] == {"GOOGLE_CLOUD_PROJECT": "world-fishing-827"}
        assert kwargs["build_from_source"] is False


def test_run_binding_stops_at_the_first_failing_step() -> None:
    rc, calls = _run_binding(_run_args(), rc_by_step={"segment_vessel_daily": 3})

    assert rc == 3
    assert [argv[0] for _, argv, _ in calls] == [
        "segment", "segment_identity", "segment_vessel_daily",
    ]


def test_run_binding_with_skip_downstream_runs_segment_only() -> None:
    rc, calls = _run_binding(_run_args("dataflow", "--skip-downstream"))

    assert rc == 0
    assert [argv[0] for _, argv, _ in calls] == ["segment"]


def test_run_binding_dry_run_runs_nothing() -> None:
    rc, calls = _run_binding(_run_args("dataflow", "--dry-run"))

    assert (rc, calls) == (0, [])


# -- The diff phase ----------------------------------------------------------

def _diffed(*extra: str) -> list[tuple[str, str, str, list[str]]]:
    """``(table, a, b, keys)`` of every comparison a two-binding run makes."""
    args = _run_args("dataflow", "--binding", "after=v5.0.3", *extra)
    with patch.object(mod.dit_compare, "compare_tables", return_value=0) as compare:
        results = mod._run_diffs(
            args=args,
            suffix_by_binding={"before": "exp_before", "after": "exp_after"},
            failed_bindings=set(),
        )
    assert set(results.values()) == {0}
    return [
        (table, call.args[0], call.args[1], call.kwargs["keys"])
        for (table, _, _), call in zip(results, compare.call_args_list, strict=True)
    ]


def test_every_output_table_is_diffed_between_the_bindings_on_its_keys() -> None:
    tables = [*mod.SEGMENT_OUTPUT_TABLES, *mod.DOWNSTREAM_OUTPUT_TABLES]

    assert _diffed() == [
        (table, f"{OUT}.{table}_exp_before", f"{OUT}.{table}_exp_after", mod.COMPARE_KEYS[table])
        for table in tables
    ]
    assert len(tables) == 8


def test_skip_downstream_diffs_the_segment_tables_only() -> None:
    assert [table for table, *_ in _diffed("--skip-downstream")] == list(
        mod.SEGMENT_OUTPUT_TABLES)
