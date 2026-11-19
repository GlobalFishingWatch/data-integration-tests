"""Tests for ``workflows/pipe_segment/identity_match_key.py``.

Two parts:

* Source pinning by copy (PIPELINE-4705): the window of the pinned source,
  new and reused pins, the dry run, the removed snapshot flags, and the
  refusal of ``--include-satellite-offsets``.
* The six-step chain a binding runs, shaped like production's segment group
  (2026-11-19): every step's argv on both runners, the labels on every step,
  no Dataflow option on the four BigQuery SQL steps, which table each step
  reads and writes, the compare keys, and the order ``_run_binding`` runs the
  steps in.

No docker, BigQuery or Dataflow call: ``dit.bq``'s pin helpers, the runner
and the comparison are patched. What the pipeline does with these argvs is
not tested here.
"""

from __future__ import annotations

import argparse
import dataclasses
import logging
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from dit import bq as dit_bq
from workflows.pipe_segment import identity_match_key as mod

_PINNED_AT = datetime(2026, 11, 19, 10, 15, 0, tzinfo=timezone.utc)
_STAMP = "20261119t101500z"
_SOURCE = "world-fishing-827.pipe_ais_test_202408290000_internal.normalized_messages"
_GE = "world-fishing-827.tech_great_expectations"


# -- parse_args: pin flags, removed flags, the satellite-offsets refusal ----

def _base_args(extra: list[str] | None = None) -> list[str]:
    """Minimal argv passing parse_args' required-arg validation."""
    return [
        "--experiment-id", "test01",
        "--binding", "before=v5.0.3",
        "--date-range", "2020-01-01,2020-01-01",
    ] + (extra or [])


def test_pin_flags_default_to_a_new_seven_day_pin_under_the_library_cap() -> None:
    args = mod.parse_args(_base_args())

    assert args.reuse_pin is None
    assert args.pin_expiration_days == 7
    # dit.bq's 1 TB cap per copy.
    assert args.pin_max_gb == 1000.0
    assert args.dry_run is False


@pytest.mark.parametrize("argv, instead", [
    (["--pin-source-at", "2026-06-03T10:00:00Z"], "copying it at run start.*--reuse-pin"),
    (["--snapshot-dest-project", "gfw-int-pipe-v3"], "needs no flag"),
    (["--snapshot-expiration-days", "30"], "use --pin-expiration-days"),
    # A removed flag without its value gets the same message, not argparse's
    # "expected one argument".
    (["--pin-source-at"], "copying it at run start"),
])
def test_removed_snapshot_flags_are_rejected_with_what_replaces_them(
    argv: list[str], instead: str,
) -> None:
    with pytest.raises(
        SystemExit,
        match=rf"{argv[0]} was removed with snapshot pinning \(PIPELINE-4705\).*{instead}",
    ):
        mod.parse_args(_base_args(argv))


def test_removed_flags_leave_no_trace_on_the_namespace() -> None:
    args = mod.parse_args(_base_args())

    assert not [name for name in vars(args) if name.startswith("removed_")]
    assert not {"pin_source_at", "snapshot_dest_project", "snapshot_expiration_days"} & set(
        vars(args))


def test_include_satellite_offsets_is_refused_at_parse_time_with_the_reasons() -> None:
    """The option cannot run. It is refused before any cloud call, with a
    message that says why and names the ticket."""
    with (
        patch.object(mod.dit_bq, "pin_source_into_experiment") as pin,
        patch.object(mod.subprocess, "run") as run,
        pytest.raises(SystemExit) as refused,
    ):
        mod.main(_base_args(["--include-satellite-offsets"]))

    message = str(refused.value)
    assert message == mod.SATELLITE_OFFSETS_REFUSED
    assert "PIPELINE-4705" in message
    assert "date-sharded" in message and "another organisation" in message
    assert "table wildcard" in message and "partitioned" in message
    # Nothing ran: no ref check, no copy.
    pin.assert_not_called()
    run.assert_not_called()


def test_the_refusal_comes_before_every_other_argument_check() -> None:
    """Even with an otherwise invalid invocation, the user learns first that
    the option is gone."""
    with pytest.raises(SystemExit, match="--include-satellite-offsets cannot run"):
        mod.parse_args(_base_args(["--include-satellite-offsets", "--reuse-pin", "nonsense"]))


def test_invalid_reuse_pin_is_rejected() -> None:
    with pytest.raises(SystemExit, match="--reuse-pin: invalid pin stamp"):
        mod.parse_args(_base_args(["--reuse-pin", "2026-11-19T10:15:00Z"]))


@pytest.mark.parametrize("flag, value, message", [
    ("--pin-max-gb", "0", "--pin-max-gb must be > 0"),
    ("--pin-max-gb", "-5", "--pin-max-gb must be > 0"),
    ("--pin-expiration-days", "0", "--pin-expiration-days must be >= 1"),
])
def test_non_positive_pin_knobs_are_rejected(flag: str, value: str, message: str) -> None:
    with pytest.raises(SystemExit, match=message):
        mod.parse_args(_base_args([flag, value]))


def test_a_date_range_that_ends_before_it_starts_is_rejected() -> None:
    """Checked at parse time: the copy's window needs start <= end."""
    with pytest.raises(SystemExit, match="is before start"):
        mod.parse_args([
            "--experiment-id", "test01", "--binding", "before=v5.0.3",
            "--date-range", "2020-01-02,2020-01-01",
        ])


# -- _PinnedSources dataclass -----------------------------------------------

def test_pinned_sources_hold_the_copy_and_no_satellite_input() -> None:
    """Nothing sets the two satellite-offset fields: the option that used
    them is refused."""
    pinned = mod._PinnedSources(normalized_messages=f"{_GE}.copy")
    assert pinned.normalized_messages == f"{_GE}.copy"
    assert pinned.sat_positions_stem is None
    assert pinned.norad is None


def test_pinned_sources_is_frozen() -> None:
    """Frozen so nothing changes the pin between pinning and the bindings'
    runs."""
    pinned = mod._PinnedSources(normalized_messages="proj.tech_great_expectations.x")
    with pytest.raises(dataclasses.FrozenInstanceError):
        pinned.normalized_messages = "other"  # type: ignore[misc]


# -- What is pinned: the window ----------------------------------------------

def _pin_args(**overrides: Any) -> argparse.Namespace:
    base = dict(
        experiment_id="test01",
        source_normalized_table=_SOURCE,
        date_range=(date(2020, 1, 1), date(2020, 1, 1)),
        pin_expiration_days=7,
        pin_max_gb=1000.0,
        reuse_pin=None,
        dry_run=False,
    )
    base.update(overrides)
    return argparse.Namespace(**base)


@pytest.mark.parametrize("date_range, window_end", [
    ((date(2020, 1, 1), date(2020, 1, 1)), date(2020, 1, 2)),
    ((date(2020, 1, 1), date(2020, 1, 31)), date(2020, 2, 1)),
    ((date(2020, 12, 30), date(2021, 1, 2)), date(2021, 1, 3)),
])
def test_the_window_is_the_inclusive_date_range_with_no_lookback(
    date_range: tuple[date, date], window_end: date,
) -> None:
    """``segment`` reads ``DATE(timestamp) BETWEEN start AND end`` and nothing
    before it (pipe-segment v5.0.3): the half-open window ends a day later."""
    window = mod._normalized_window(date_range)

    assert window == dit_bq.SourceWindow(
        column="timestamp", start=date_range[0], end=window_end,
    )
    assert window.predicate() == (
        f"DATE(timestamp) >= '{date_range[0].isoformat()}' "
        f"AND DATE(timestamp) < '{window_end.isoformat()}'"
    )


def test_the_window_matches_the_date_range_segment_is_given() -> None:
    """The copy holds exactly the days ``segment`` is asked to read."""
    args = _run_args("dataflow", "--date-range", "2020-03-01,2020-03-05")
    window = mod._normalized_window(args.date_range)
    segment = mod._step_argvs(
        args, binding_name="before", suffix=SFX, worker_image=None,
        pinned=mod._PinnedSources(normalized_messages="p.d.normalized"),
    )["segment"]

    start, end = _value(segment, "--date_range").split(",")
    assert date.fromisoformat(start) == window.start
    assert date.fromisoformat(end) + timedelta(days=1) == window.end


# -- _pin_sources / _pin: a new copy, reuse, dry run -------------------------

def test_pin_sources_copies_the_normalized_messages_over_the_window() -> None:
    args = _pin_args(
        date_range=(date(2020, 1, 1), date(2020, 1, 3)), pin_max_gb=250.0,
        pin_expiration_days=3,
    )
    with patch.object(mod.dit_bq, "pin_source_into_experiment",
                      return_value=f"{_GE}.copy") as pin:
        pinned = mod._pin_sources(args, _PINNED_AT, reuse=False)

    pin.assert_called_once()
    call = pin.call_args
    assert call.args == (_SOURCE,)
    assert call.kwargs == dict(
        experiment_id="test01",
        role="pipe_segment",
        pinned_at=_PINNED_AT,
        window=dit_bq.SourceWindow("timestamp", date(2020, 1, 1), date(2020, 1, 4)),
        expiration_days=3,
        max_bytes=250 * 10**9,
        dry_run=False,
        project="world-fishing-827",
    )
    assert pinned == mod._PinnedSources(normalized_messages=f"{_GE}.copy")


def test_pin_sources_dry_run_reaches_the_helper() -> None:
    """--dry-run must not create the copy: it can process up to the cap."""
    with patch.object(mod.dit_bq, "pin_source_into_experiment") as pin:
        mod._pin_sources(_pin_args(dry_run=True), _PINNED_AT, reuse=False)

    assert pin.call_args.kwargs["dry_run"] is True


def test_pin_sources_reuse_only_checks_the_recomputed_name() -> None:
    with (
        patch.object(mod.dit_bq, "pin_source_into_experiment") as pin,
        patch.object(
            mod.dit_bq, "require_pinned_source", side_effect=lambda fqn, **kw: fqn,
        ) as require,
    ):
        pinned = mod._pin_sources(_pin_args(), _PINNED_AT, reuse=True)

    pin.assert_not_called()
    require.assert_called_once()
    assert pinned.normalized_messages == (
        f"{_GE}.dit_exp_test01_pipe_segment_normalized_messages_20200101_20200102_{_STAMP}"
    )


def test_reuse_fails_before_any_binding_when_the_copy_is_missing() -> None:
    with (
        patch.object(
            mod.dit_bq, "require_pinned_source",
            side_effect=ValueError("pinned copy x does not exist"),
        ),
        pytest.raises(ValueError, match="does not exist"),
    ):
        mod._pin_sources(_pin_args(), _PINNED_AT, reuse=True)


def test_pin_takes_the_stamp_from_reuse_pin_or_from_now(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    pinned = mod._PinnedSources(normalized_messages="p.d.copy")
    with patch.object(mod, "_pin_sources", return_value=pinned) as pin_sources:
        assert mod._pin(_pin_args(reuse_pin=_STAMP)) == pinned
        assert mod._pin(_pin_args()) == pinned

    reused, fresh = pin_sources.call_args_list
    assert reused.args[1] == _PINNED_AT
    assert reused.kwargs["reuse"] is True
    assert fresh.kwargs["reuse"] is False
    # A fresh stamp is now, in whole seconds: the stamp's precision.
    assert fresh.args[1].tzinfo is not None
    assert fresh.args[1].microsecond == 0
    assert abs(datetime.now(timezone.utc) - fresh.args[1]) < timedelta(minutes=1)
    # Only a run that made a new copy tells how to reuse it.
    assert [m for m in caplog.messages if "pass --reuse-pin" in m] == [
        f"to rerun on this copy, pass --reuse-pin {dit_bq.format_pin_stamp(fresh.args[1])}"
    ]


def _main(*extra: str) -> dict[str, Any]:
    """Run ``main`` for two bindings with every cloud, git and docker call
    patched. Returns the copy helper's mock, the pinned sources each binding
    got, and the exit code."""
    seen: list[mod._PinnedSources] = []
    with (
        patch.object(mod, "_verify_refs"),
        patch.object(mod, "_verify_distinct_gpsdio_pins"),
        patch.object(mod.dit_bq, "pin_source_into_experiment",
                     return_value=f"{_GE}.copy") as pin,
        patch.object(mod.dit_bq, "require_pinned_source",
                     side_effect=lambda fqn, **kw: fqn) as require,
        patch.object(mod, "_run_binding",
                     side_effect=lambda **kw: seen.append(kw["pinned"]) or 0),
        patch.object(mod, "_run_diffs", return_value={}) as diffs,
    ):
        rc = mod.main([
            "--experiment-id", "test01",
            "--binding", "before=v5.0.3", "--binding", "after=exp-branch",
            "--date-range", "2020-01-01,2020-01-01",
            *extra,
        ])
    return dict(rc=rc, pin=pin, require=require, seen=seen, diffs=diffs)


def test_main_pins_once_and_every_binding_reads_the_same_copy() -> None:
    result = _main()

    assert result["rc"] == 0
    result["pin"].assert_called_once()
    assert result["pin"].call_args.kwargs["dry_run"] is False
    assert result["seen"] == [mod._PinnedSources(normalized_messages=f"{_GE}.copy")] * 2


def test_main_reuse_pin_copies_nothing_and_reads_the_earlier_copy() -> None:
    result = _main("--reuse-pin", _STAMP)

    result["pin"].assert_not_called()
    result["require"].assert_called_once()
    earlier = f"{_GE}.dit_exp_test01_pipe_segment_normalized_messages_20200101_20200102_{_STAMP}"
    assert result["seen"] == [mod._PinnedSources(normalized_messages=earlier)] * 2


def test_main_dry_run_only_dry_runs_the_copy_and_skips_the_diffs() -> None:
    result = _main("--dry-run", "--pin-max-gb", "5")

    assert result["rc"] == 0
    assert result["pin"].call_args.kwargs["dry_run"] is True
    assert result["pin"].call_args.kwargs["max_bytes"] == 5 * 10**9
    result["diffs"].assert_not_called()


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
        pinned=mod._PinnedSources(normalized_messages="p.d.normalized"),
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


def test_satellite_offsets_stay_off_and_the_kept_branch_still_emits_their_flags() -> None:
    """Every run leaves the branch off. The code that emits its four flags is
    kept until the owner decides on the option, though no run reaches it: the
    command line refuses the flag, so the namespace is set by hand here."""
    satellite = {"--in_normalized_sat_offset_messages_table", "--in_norad_to_receiver_table",
                 "--in_sat_positions_table", "--out_sat_offsets_table"}
    assert _run_args().include_satellite_offsets is False
    assert not _flag_names(_argvs()["segment"]) & satellite
    with pytest.raises(SystemExit, match="cannot run and is refused"):
        _run_args("dataflow", "--include-satellite-offsets")

    asked_args = _run_args("dataflow")
    asked_args.include_satellite_offsets = True
    asked = mod._step_argvs(
        asked_args,
        binding_name="before", suffix=SFX, worker_image=None,
        pinned=mod._PinnedSources(
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
            pinned=mod._PinnedSources(normalized_messages="p.d.normalized"),
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
