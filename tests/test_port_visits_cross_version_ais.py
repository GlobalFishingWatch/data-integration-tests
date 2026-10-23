"""Tests for ``workflows/port_visits/cross_version_ais.py``.

Focused on source pinning and what reaches each binding:

- ``_PinnedSources`` dataclass shape + frozen-ness.
- ``parse_args``: the removed ``--pin-source-at`` is rejected; ``--start`` /
  ``--end`` / ``--reuse-pin`` are validated before anything runs.
- ``_pin_specs``: which sources are pinned, with which window and role
  (``messages_positions`` windowed, or replaced by the thinned table;
  ``segment_info`` / ``segs_activity`` whole).
- ``_pin_sources``: one ``dit.bq.pin_source_into_experiment`` call per spec
  under one pin stamp, or with reuse only existence checks.
- ``_ais_args_for_binding`` emits the M4 per-table FQN flags and the pinned
  dates, and drops user-supplied overrides of both from extras
  (load-bearing for the cross-version pin -- otherwise a user extra arg
  could leak an unpinned table, or dates outside the copies, into one
  binding).

The git-worktree path, the parallel-bindings ThreadPoolExecutor, the
diff phase, and the integration with the real ``ais.py`` subprocess are
exercised by live ``dit run`` invocations; not unit-tested here.
"""

from __future__ import annotations

import argparse
import dataclasses
from datetime import date, datetime, timezone
from typing import Any
from unittest.mock import patch

import pytest

from dit import bq as dit_bq
from workflows.port_visits import cross_version_ais as mod

_PINNED_AT = datetime(2026, 10, 22, 17, 15, 0, tzinfo=timezone.utc)
_STEM = "world-fishing-827.pipe_ais_test_202408290000"


def _args(**overrides: Any) -> argparse.Namespace:
    base = dict(
        experiment_id="exp01",
        start=date(2020, 1, 1),
        end=date(2020, 12, 31),
        source_dataset_stem="pipe_ais_test_202408290000",
        source_messages_fqn=None,
        source_segment_info_fqn=None,
        source_segs_activity_fqn=None,
        steps=list(mod.AIS_SELECTABLE_STEPS),
        thinned_message_table=None,
        pin_expiration_days=7,
        pin_max_gb=1000.0,
        dry_run=False,
    )
    base.update(overrides)
    return argparse.Namespace(**base)


def _pinned(**overrides: Any) -> mod._PinnedSources:
    base = dict(
        start=date(2020, 1, 1),
        end=date(2020, 12, 31),
        messages_positions="dst.m",
        segment_info="dst.si",
        segs_activity="dst.sa",
    )
    base.update(overrides)
    return mod._PinnedSources(**base)


def _binding_args(extras: list[str], pinned: mod._PinnedSources | None = None) -> list[str]:
    return mod._ais_args_for_binding(
        extras,
        pinned=pinned if pinned is not None else _pinned(),
        suffix="exp01-before",
        experiment_id="exp01",
        binding_name="before",
        modes=mod.AIS_SELECTABLE_MODES,
    )


# --------------------------------------------------------------------------
# _PinnedSources dataclass
# --------------------------------------------------------------------------

def test_pinned_sources_is_frozen() -> None:
    """Frozen so nothing changes the pins between pinning and the `ais.py`
    invocations."""
    pinned = _pinned()
    with pytest.raises(dataclasses.FrozenInstanceError):
        pinned.segment_info = "other"  # type: ignore[misc]


def test_pinned_sources_messages_and_thinned_default_to_none() -> None:
    pinned = mod._PinnedSources(
        start=date(2020, 1, 1), end=date(2020, 1, 2),
        segment_info="dst.si", segs_activity="dst.sa",
    )
    assert pinned.messages_positions is None
    assert pinned.thinned is None


# --------------------------------------------------------------------------
# parse_args
# --------------------------------------------------------------------------

def test_pin_source_at_is_rejected_with_a_pointer_to_reuse_pin() -> None:
    """Rejected explicitly: otherwise parse_known_args would forward it to
    ais.py's extras, which fails with a far less helpful error."""
    with pytest.raises(SystemExit, match="--pin-source-at was removed.*--reuse-pin"):
        mod.parse_args([
            "--experiment-id", "e", "--binding", "before=main",
            "--pin-source-at", "2026-10-22T00:00:00Z",
        ])


def test_dates_default_to_ais_defaults_and_are_not_forwarded_as_extras() -> None:
    args, extras = mod.parse_args(["--experiment-id", "e", "--binding", "before=main"])
    assert args.start == date.fromisoformat(mod.AIS_DEFAULT_START)
    assert args.end == date.fromisoformat(mod.AIS_DEFAULT_END)
    assert extras == []


def test_end_before_start_is_rejected() -> None:
    with pytest.raises(SystemExit, match="is before --start"):
        mod.parse_args([
            "--experiment-id", "e", "--binding", "before=main",
            "--start", "2020-02-01", "--end", "2020-01-31",
        ])


def test_invalid_reuse_pin_is_rejected() -> None:
    with pytest.raises(SystemExit, match="--reuse-pin"):
        mod.parse_args([
            "--experiment-id", "e", "--binding", "before=main",
            "--reuse-pin", "2026-10-22T17:15:00Z",
        ])


def test_steps_default_to_both_in_pipeline_order() -> None:
    args, _ = mod.parse_args([
        "--experiment-id", "e", "--binding", "before=main",
        "--steps", "port_visits,thin_port_messages",
    ])
    assert args.steps == [mod.STEP_THIN, mod.STEP_VISITS]


@pytest.mark.parametrize(("extra", "message"), [
    (["--steps", "port_visits"], "pass --thinned-message-table"),
    (["--thinned-message-table", "p.d.t"], "needs --steps port_visits"),
    (["--steps", "raw_port_events"], "unknown step"),
])
def test_step_selections_without_a_clear_input_are_rejected(extra, message) -> None:
    """The same rules as ais.py (shared validate_steps): skipping step 1 needs
    an input table, and the input table is meaningless when step 1 runs."""
    with pytest.raises(SystemExit, match=message):
        mod.parse_args(["--experiment-id", "e", "--binding", "before=main", *extra])


def test_a_non_positive_pin_cap_is_rejected() -> None:
    with pytest.raises(SystemExit, match="--pin-max-gb must be > 0"):
        mod.parse_args([
            "--experiment-id", "e", "--binding", "before=main", "--pin-max-gb", "0",
        ])


# --------------------------------------------------------------------------
# _pin_specs
# --------------------------------------------------------------------------

def test_pin_specs_window_messages_and_copy_the_rest_whole() -> None:
    specs = {s.field: s for s in mod._pin_specs(_args())}

    assert set(specs) == {"messages_positions", "segment_info", "segs_activity"}
    messages = specs["messages_positions"]
    assert messages.source_table == f"{_STEM}_internal.messages_positions"
    # Inclusive --end 2020-12-31 -> exclusive window end 2021-01-01.
    assert messages.window == dit_bq.SourceWindow(
        column="timestamp", start=date(2020, 1, 1), end=date(2021, 1, 1),
    )
    assert specs["segment_info"].source_table == f"{_STEM}_published.segment_info"
    assert specs["segs_activity"].source_table == f"{_STEM}_published.segs_activity"
    assert specs["segment_info"].window is None
    assert specs["segs_activity"].window is None
    assert {s.role for s in specs.values()} == {"cross_version"}


def test_pin_specs_for_port_visits_alone_pin_the_thinned_input_not_messages() -> None:
    """Without step 1, the only reader of messages_positions, copying it would
    only cost (a month of prod messages is ~3.6 TB)."""
    specs = {s.field: s for s in mod._pin_specs(_args(
        steps=[mod.STEP_VISITS], thinned_message_table="p.d.thinned",
    ))}

    assert set(specs) == {"thinned", "segment_info", "segs_activity"}
    thinned = specs["thinned"]
    assert thinned.source_table == "p.d.thinned"
    assert thinned.role == "cross_version_thinned"
    assert thinned.window == dit_bq.SourceWindow(
        column="timestamp", start=date(2020, 1, 1), end=date(2021, 1, 1),
    )


def test_pin_specs_for_thin_port_messages_alone_pin_only_messages() -> None:
    """Step 1 reads neither segment_info nor segs_activity."""
    specs = mod._pin_specs(_args(steps=[mod.STEP_THIN]))

    assert [s.field for s in specs] == ["messages_positions"]


def test_pin_specs_use_per_table_source_overrides() -> None:
    """Production sources sit in another organisation and outside any stem."""
    specs = {s.field: s for s in mod._pin_specs(_args(
        source_messages_fqn="gfw-int-pipe-v3.pipe_ais_v3_internal.messages_positions",
        source_segment_info_fqn="global-fishing-watch.pipe_ais_v3_published.segment_info",
    ))}

    assert specs["messages_positions"].source_table == (
        "gfw-int-pipe-v3.pipe_ais_v3_internal.messages_positions"
    )
    assert specs["segment_info"].source_table == (
        "global-fishing-watch.pipe_ais_v3_published.segment_info"
    )
    # No override -> still derived from the stem.
    assert specs["segs_activity"].source_table == f"{_STEM}_published.segs_activity"


# --------------------------------------------------------------------------
# _pin_sources
# --------------------------------------------------------------------------

def test_pin_sources_copies_every_spec_under_one_stamp() -> None:
    with patch.object(mod.dit_bq, "pin_source_into_experiment") as pin:
        pin.side_effect = lambda src, **kw: f"pin:{src}"
        pinned = mod._pin_sources(_args(pin_max_gb=250.0), _PINNED_AT, reuse=False)

    assert pin.call_count == 3
    for call in pin.call_args_list:
        assert call.kwargs["pinned_at"] == _PINNED_AT
        assert call.kwargs["experiment_id"] == "exp01"
        assert call.kwargs["expiration_days"] == 7
        assert call.kwargs["max_bytes"] == 250 * 10**9
        assert call.kwargs["dry_run"] is False
        assert call.kwargs["project"] == mod.PROJECT
    assert pinned == mod._PinnedSources(
        start=date(2020, 1, 1),
        end=date(2020, 12, 31),
        messages_positions=f"pin:{_STEM}_internal.messages_positions",
        segment_info=f"pin:{_STEM}_published.segment_info",
        segs_activity=f"pin:{_STEM}_published.segs_activity",
    )


def test_pin_sources_dry_run_reaches_the_helper() -> None:
    """--dry-run must not create copies: each can process up to the cap."""
    with patch.object(mod.dit_bq, "pin_source_into_experiment") as pin:
        mod._pin_sources(_args(dry_run=True), _PINNED_AT, reuse=False)

    assert all(call.kwargs["dry_run"] is True for call in pin.call_args_list)


def test_pin_sources_reuse_only_checks_the_recomputed_names() -> None:
    with (
        patch.object(mod.dit_bq, "pin_source_into_experiment") as pin,
        patch.object(
            mod.dit_bq, "require_pinned_source", side_effect=lambda fqn, **kw: fqn,
        ) as require,
    ):
        pinned = mod._pin_sources(_args(), _PINNED_AT, reuse=True)

    pin.assert_not_called()
    assert require.call_count == 3
    assert pinned.messages_positions == dit_bq.pinned_source_fqn(
        f"{_STEM}_internal.messages_positions",
        experiment_id="exp01",
        role="cross_version",
        pinned_at=_PINNED_AT,
        window=dit_bq.SourceWindow("timestamp", date(2020, 1, 1), date(2021, 1, 1)),
        project=mod.PROJECT,
    )
    assert pinned.segment_info.endswith("_segment_info_all_20261022t171500z")


# --------------------------------------------------------------------------
# _ais_args_for_binding
# --------------------------------------------------------------------------

def test_ais_args_for_binding_emits_per_table_fqn_flags_not_stem() -> None:
    """ais.py is routed at the pinned copies via M4's per-table FQN flags,
    NOT --source-dataset-stem (which can only address a stem-with-halves
    shape and can't reach tech_great_expectations)."""
    out = _binding_args([])
    assert out[out.index("--source-messages-fqn") + 1] == "dst.m"
    assert out[out.index("--source-segment-info-fqn") + 1] == "dst.si"
    assert out[out.index("--source-segs-activity-fqn") + 1] == "dst.sa"
    assert "--source-dataset-stem" not in out


def test_ais_args_for_binding_forwards_the_pinned_dates() -> None:
    out = _binding_args([], _pinned(start=date(2020, 3, 1), end=date(2020, 3, 31)))
    assert out[out.index("--start") + 1] == "2020-03-01"
    assert out[out.index("--end") + 1] == "2020-03-31"


def test_ais_args_for_binding_drops_user_supplied_source_and_date_overrides() -> None:
    """Load-bearing for the cross-version pin: a user-supplied
    --source-messages-fqn in extras could leak an UNPINNED table into one
    binding while the others read the copies, and user dates could reach
    beyond the windowed copies. Drop them from extras; the wrapper's values
    are the only ones that should reach ais.py."""
    user_extras = [
        "--source-messages-fqn", "proj.ds.user_provided_messages",
        "--source-segment-info-fqn", "proj.ds.user_provided_si",
        "--source-segs-activity-fqn", "proj.ds.user_provided_sa",
        "--start=2019-01-01",
        "--end", "2021-06-30",
        "--runner", "dataflow",  # unrelated flag should survive
    ]
    out = _binding_args(user_extras)
    for value in ("proj.ds.user_provided_messages", "proj.ds.user_provided_si",
                  "proj.ds.user_provided_sa", "--start=2019-01-01", "2021-06-30"):
        assert value not in out
    assert out.count("--start") == 1 and out.count("--end") == 1
    assert "--runner" in out and "dataflow" in out
    assert "dst.m" in out and "dst.si" in out and "dst.sa" in out


def test_ais_args_for_binding_threads_thinned_and_omits_unpinned_messages() -> None:
    out = _binding_args([], _pinned(messages_positions=None, thinned="dst.thinned_pin"))
    assert out[out.index("--thinned-message-table") + 1] == "dst.thinned_pin"
    assert "--source-messages-fqn" not in out


def test_ais_args_for_binding_omits_thinned_when_absent() -> None:
    assert "--thinned-message-table" not in _binding_args([])


def test_ais_args_for_binding_forwards_no_cache_to_every_binding() -> None:
    """A determinism re-run on reused pins needs every binding to recompute."""
    assert "--no-cache" in _binding_args(["--no-cache"])


def test_ais_args_for_binding_forwards_steps_and_drops_user_steps() -> None:
    """The wrapper owns --steps: it decides what was pinned and what is
    diffed, so a user extra must not desync the bindings from either."""
    out = mod._ais_args_for_binding(
        ["--steps", "port_visits"],
        pinned=_pinned(segment_info=None, segs_activity=None),
        suffix="exp01-before",
        experiment_id="exp01",
        binding_name="before",
        modes=mod.AIS_SELECTABLE_MODES,
        steps=[mod.STEP_THIN],
    )
    assert out.count("--steps") == 1
    assert out[out.index("--steps") + 1] == "thin_port_messages"
    # Step 1 alone: only its own input reaches ais.py.
    assert "--source-messages-fqn" in out
    assert "--source-segment-info-fqn" not in out
    assert "--source-segs-activity-fqn" not in out


def test_diff_for_thin_port_messages_compares_port_events_on_its_key() -> None:
    with patch.object(mod.dit_compare, "compare_tables", return_value=0) as compare:
        mod._diff_pair(
            dest_dataset="ds", a_suffix="e-before", b_suffix="e-after",
            mode="1_bf", step=mod.STEP_THIN,
        )
    (a, b), kwargs = compare.call_args
    assert a == f"{mod.PROJECT}.ds.port_events_e-before_1_bf"
    assert b == f"{mod.PROJECT}.ds.port_events_e-after_1_bf"
    assert kwargs["keys"] == ["identifier", "timestamp"]


def test_diff_for_port_visits_compares_visits_on_visit_id() -> None:
    with patch.object(mod.dit_compare, "compare_tables", return_value=0) as compare:
        mod._diff_pair(
            dest_dataset="ds", a_suffix="e-before", b_suffix="e-after",
            mode="1_bf", step=mod.STEP_VISITS,
        )
    (a, _), kwargs = compare.call_args
    assert a == f"{mod.PROJECT}.ds.port_visits_e-before_1_bf"
    assert kwargs["keys"] == ["visit_id"]


def test_ais_args_for_binding_drops_user_thinned_message_table() -> None:
    """Same load-bearing reason as the per-table FQN drops: the wrapper
    exclusively owns the --thinned-message-table flag (it pinned the table
    or knows there is none). User extras can never inject one."""
    out = _binding_args(["--thinned-message-table", "proj.ds.user_thinned"])
    assert "proj.ds.user_thinned" not in out
    assert "--thinned-message-table" not in out


# --------------------------------------------------------------------------
# --baseline-production (PIPELINE-4703)
# --------------------------------------------------------------------------

_TARGET = mod.PRODUCTION_TARGETS["ais-v5-datalake"]
_T0 = datetime(2026, 10, 23, 7, 46, 26, tzinfo=timezone.utc)
_T1 = datetime(2026, 10, 23, 9, 4, 17, tzinfo=timezone.utc)
_PRODUCTION_FLAGS = [
    "--experiment-id", "e", "--binding", "pr=14b4be8",
    "--baseline-production", "ais-v5-datalake", "--production-date", "2026-10-20",
]


def _production_args(*extra: str) -> argparse.Namespace:
    args, _ = mod.parse_args([*_PRODUCTION_FLAGS, *extra])
    return args


def test_production_mode_runs_what_production_runs() -> None:
    args = _production_args()

    assert args.steps == [mod.STEP_VISITS]
    assert args.modes == [mod.MODE_BF]
    assert args.start == date(2012, 1, 1)
    assert args.end == date(2026, 10, 20)
    assert args.production_date == date(2026, 10, 20)
    assert args.thinned_message_table == _TARGET.thinned_table
    assert args.source_segment_info_fqn == _TARGET.segment_info_table
    assert args.source_segs_activity_fqn == _TARGET.segs_activity_table


def test_production_mode_accepts_the_production_values_spelled_out() -> None:
    args = _production_args("--modes", "1_bf", "--steps", "port_visits", "--end", "2026-10-20")
    assert args.modes == [mod.MODE_BF] and args.end == date(2026, 10, 20)


@pytest.mark.parametrize(("extra", "message"), [
    (["--modes", "2_bfd"], "--modes must be 1_bf"),
    (["--end", "2026-10-19"], "--end must be 2026-10-20"),
    (["--start", "2020-01-01"], "--start must be 2012-01-01"),
    (["--steps", "thin_port_messages,port_visits"], "--steps must be port_visits"),
])
def test_production_mode_rejects_a_run_production_does_not_do(extra, message) -> None:
    """A different run cannot match production's output, so it is an error
    rather than silently replaced."""
    with pytest.raises(SystemExit, match=message):
        _production_args(*extra)


@pytest.mark.parametrize("extra", [
    ["--thinned-message-table", "p.d.t"],
    ["--source-messages-fqn", "p.d.m"],
    ["--source-segment-info-fqn", "p.d.si"],
    ["--source-dataset-stem", "other_stem"],
    ["--reuse-pin", "20261022t171500z"],
])
def test_production_mode_rejects_its_own_inputs_being_overridden(extra) -> None:
    with pytest.raises(SystemExit, match=f"defines its own inputs and pins; drop {extra[0]}"):
        _production_args(*extra)


def test_production_mode_needs_a_production_date_and_vice_versa() -> None:
    with pytest.raises(SystemExit, match="needs --production-date"):
        mod.parse_args([
            "--experiment-id", "e", "--binding", "pr=x",
            "--baseline-production", "ais-v5-datalake",
        ])
    with pytest.raises(SystemExit, match="--production-\\* flags need --baseline-production"):
        mod.parse_args([
            "--experiment-id", "e", "--binding", "pr=x", "--production-date", "2026-10-20",
        ])


def test_production_as_of_overrides_need_whole_seconds_and_a_zone() -> None:
    args = _production_args("--production-inputs-as-of", "2026-10-23T07:46:26Z")
    assert args.production_inputs_as_of == _T0
    with pytest.raises(SystemExit, match="needs a time zone"):
        _production_args("--production-output-as-of", "2026-10-23T09:04:17")
    with pytest.raises(SystemExit, match="whole seconds"):
        _production_args("--production-output-as-of", "2026-10-23T09:04:17.5Z")


def _pin_production(args: argparse.Namespace, *, last_modified: datetime | None = None):
    """Run ``_pin_production`` with the job lookup and BigQuery mocked; the
    inputs were last modified before production's job by default."""
    run = mod.dit_production.ProductionRun(
        job_id="2026-10-23_00_46_26-1", name=_TARGET.job_name(date(2026, 10, 20)),
        create_time=_T0, end_time=_T1,
    )
    with (
        patch.object(mod.dit_production, "find_dataflow_job", return_value=run) as find,
        patch.object(mod.dit_bq, "check_time_travel",
                     return_value=datetime(2026, 10, 25, 7, 46, 26, tzinfo=timezone.utc)) as check,
        patch.object(mod.dit_bq, "table_last_modified",
                     return_value=last_modified or datetime(2026, 10, 23, 1, tzinfo=timezone.utc)),
        patch.object(mod.dit_bq, "pin_view_into_experiment",
                     side_effect=lambda src, **kw: f"view:{src}") as view,
    ):
        pinned = mod._pin_production(args, _TARGET)
    return pinned, find, check, view


def test_pin_production_pins_inputs_at_job_start_and_output_at_job_end() -> None:
    pinned, find, check, view = _pin_production(_production_args("--pin-max-gb", "1300"))

    find.assert_called_once_with(
        project="world-fishing-827", region="us-central1",
        name="core-ais-v5--port-visits-detect-port-visits--20261020",
        created_after=date(2026, 10, 19),
    )
    as_of_by_source = {call.args[0]: call.kwargs["as_of"] for call in view.call_args_list}
    assert as_of_by_source == {
        _TARGET.thinned_table: _T0,
        _TARGET.segment_info_table: _T0,
        _TARGET.segs_activity_table: _T0,
        _TARGET.output_table: _T1,
    }
    calls = {call.args[0]: call.kwargs for call in view.call_args_list}
    # The thinned input covers what production reads: [history start, D].
    assert calls[_TARGET.thinned_table]["window"] == dit_bq.SourceWindow(
        column="timestamp", start=date(2012, 1, 1), end=date(2026, 10, 21),
    )
    assert calls[_TARGET.segment_info_table]["window"] is None
    assert calls[_TARGET.output_table]["window"] is None
    assert calls[_TARGET.output_table]["role"] == "production_output"
    assert calls[_TARGET.thinned_table]["role"] == "production_input"
    assert all(kw["max_bytes"] == 1300 * 10**9 for kw in calls.values())
    # Every pin's time-travel window was checked before any view was made.
    assert {call.args[0] for call in check.call_args_list} == set(calls)

    assert pinned == mod._PinnedSources(
        start=date(2012, 1, 1),
        end=date(2026, 10, 20),
        thinned=f"view:{_TARGET.thinned_table}",
        segment_info=f"view:{_TARGET.segment_info_table}",
        segs_activity=f"view:{_TARGET.segs_activity_table}",
        production_output=f"view:{_TARGET.output_table}",
    )


def test_pin_production_with_explicit_times_skips_the_job_lookup() -> None:
    args = _production_args(
        "--production-inputs-as-of", "2026-10-23T07:00:00Z",
        "--production-output-as-of", "2026-10-23T10:00:00Z",
    )
    _, find, _, view = _pin_production(args)

    find.assert_not_called()
    assert {call.kwargs["as_of"] for call in view.call_args_list} == {
        datetime(2026, 10, 23, 7, tzinfo=timezone.utc),
        datetime(2026, 10, 23, 10, tzinfo=timezone.utc),
    }


def test_pin_production_refuses_inputs_that_changed_while_production_ran() -> None:
    """Which state production read is then unknown, so pinning the job's
    start could pin the wrong one."""
    with pytest.raises(SystemExit, match="inputs changed while production ran"):
        _pin_production(
            _production_args(), last_modified=datetime(2026, 10, 23, 8, tzinfo=timezone.utc),
        )


def test_pin_production_accepts_inputs_changed_after_production_finished() -> None:
    """Time travel returns the job-start state regardless."""
    _, _, _, view = _pin_production(
        _production_args(), last_modified=datetime(2026, 10, 23, 12, tzinfo=timezone.utc),
    )
    assert view.call_count == 4


def test_pin_production_dry_run_creates_no_views() -> None:
    _, _, _, view = _pin_production(_production_args("--dry-run"))
    assert all(call.kwargs["dry_run"] is True for call in view.call_args_list)


def test_pin_dispatches_production_mode_without_copying() -> None:
    args = _production_args()
    with (
        patch.object(mod, "_pin_production", return_value="views") as pin_production,
        patch.object(mod, "_pin_sources") as pin_sources,
    ):
        assert mod._pin(args) == "views"
    pin_production.assert_called_once_with(args, _TARGET)
    pin_sources.assert_not_called()


def test_production_binding_reads_the_views_over_full_history() -> None:
    out = mod._ais_args_for_binding(
        [],
        pinned=mod._PinnedSources(
            start=date(2012, 1, 1), end=date(2026, 10, 20),
            thinned="v.thinned", segment_info="v.si", segs_activity="v.sa",
            production_output="v.out",
        ),
        suffix="e-pr", experiment_id="e", binding_name="pr",
        modes=[mod.MODE_BF], steps=[mod.STEP_VISITS],
    )
    assert out[out.index("--steps") + 1] == "port_visits"
    assert out[out.index("--modes") + 1] == "1_bf"
    assert out[out.index("--start") + 1] == "2012-01-01"
    assert out[out.index("--end") + 1] == "2026-10-20"
    assert out[out.index("--thinned-message-table") + 1] == "v.thinned"
    assert "--source-messages-fqn" not in out
    # Production's output is a diff target, never a binding input.
    assert "v.out" not in out


def test_production_diffs_compare_each_binding_with_production_on_visit_id() -> None:
    with patch.object(mod.dit_compare, "compare_tables", return_value=1) as compare:
        results = mod._run_production_diffs(
            modes=[mod.MODE_BF],
            step=mod.STEP_VISITS,
            suffix_by_binding={"pr": "e-pr", "broken": "e-broken"},
            dest_dataset="ds",
            failed_bindings={"broken"},
            production_output="v.out",
        )

    assert results == {("1_bf", "pr", "production"): 1, ("1_bf", "broken", "production"): -1}
    compare.assert_called_once()
    (a, b), kwargs = compare.call_args
    assert a == f"{mod.PROJECT}.ds.port_visits_e-pr_1_bf"
    assert b == "v.out"
    assert kwargs["keys"] == ["visit_id"]
