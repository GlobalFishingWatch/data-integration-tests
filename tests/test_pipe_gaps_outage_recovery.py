"""Tests for ``workflows/pipe_gaps/outage_recovery.py``.

Focused on workflow-local helpers: the stage-boundary ordering check in
``parse_args``, ``canonical_params_dict`` cache-key composition, and source
pinning by copy (PIPELINE-4705): the window of each source, new and reused
pins, the dry run, the removed snapshot flags, and how ``main`` routes the
copies and the ``--synthetic-outage`` view into the stages.

No test touches BigQuery, docker or Dataflow: ``dit.bq``'s pin helpers, the
run context and the cached execution are patched. What pipe-gaps does with
the copies is exercised by live ``dit run`` invocations only.
"""

from __future__ import annotations

import argparse
import logging
import math
from datetime import date, datetime, timedelta, timezone
from typing import Any
from unittest.mock import patch

import pytest

from dit import bq as dit_bq
from dit.workflow import RunContext
from workflows.pipe_gaps import outage_recovery as mod

_PINNED_AT = datetime(2026, 11, 19, 10, 15, 0, tzinfo=timezone.utc)
_STAMP = "20261119t101500z"
_STEM = "world-fishing-827.pipe_ais_test_202408290000"
_GE = "world-fishing-827.tech_great_expectations"
#: The copies' names for the default run of experiment ``exp01`` under _STAMP.
_PINNED_MESSAGES = f"{_GE}.dit_exp_exp01_outage_messages_positions_20191231_20210101_{_STAMP}"
_PINNED_SEGMENTS = f"{_GE}.dit_exp_exp01_outage_segs_activity_all_{_STAMP}"


def _args(**overrides: Any) -> argparse.Namespace:
    base = dict(
        experiment_id="exp01",
        no_cache=False,
        start="2020-01-01",
        backfill_end="2020-12-28",
        outage_start="2020-12-29",
        outage_end="2020-12-29",
        end="2020-12-31",
        recovery_buffer_days=1,
        min_gap_length=1.0,
        n_hours_before=12,
        window_period_d=2,
        filter_good_seg="True",
        skip_open_gaps=False,
        # CLI form: empty string = no ssvid restriction (the default).
        ssvids="",
        # Default sources point at the AIS staging cohort -- same
        # staging-by-default precedent as mode_equivalence.py uses.
        source_messages=(
            "world-fishing-827.pipe_ais_test_202408290000_internal."
            "messages_positions"
        ),
        source_segments=(
            "world-fishing-827.pipe_ais_test_202408290000_published."
            "segs_activity"
        ),
        # The copies the jobs read (set by main once the sources resolve).
        pinned_messages=_PINNED_MESSAGES,
        pinned_segments=_PINNED_SEGMENTS,
        pin_expiration_days=7,
        pin_max_gb=1000.0,
        reuse_pin=None,
        no_pin=False,
        dry_run=False,
        synthetic_outage=False,
    )
    base.update(overrides)
    return argparse.Namespace(**base)


# --------------------------------------------------------------------------
# The flags removed with snapshot pinning
# --------------------------------------------------------------------------

@pytest.mark.parametrize("argv, instead", [
    (["--pin-at", "2026-06-01 18:00:00 UTC"], "copying them at run start"),
    (["--snapshot-dest-project", "gfw-int-vms-v3"], "needs no flag"),
    (["--snapshot-expiration-days", "30"], "use --pin-expiration-days"),
    (["--skip-snapshots"], "use --reuse-pin <stamp>"),
    (["--no-snapshot"], "use --no-pin"),
    # A removed flag without its value gets the same message, not argparse's
    # "expected one argument".
    (["--pin-at"], "copying them at run start"),
])
def test_removed_snapshot_flags_are_rejected_with_what_replaces_them(
    argv: list[str], instead: str, capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit):
        mod.parse_args(["--experiment-id", "test", *argv])

    message = " ".join(capsys.readouterr().err.split())
    assert f"{argv[0]} was removed with snapshot pinning (PIPELINE-4705)" in message
    assert instead in message


def test_removed_flags_leave_no_trace_on_the_namespace() -> None:
    args = mod.parse_args(["--experiment-id", "test"])

    assert not [name for name in vars(args) if name.startswith("removed_")]
    assert not {"pin_at", "snapshot_dest_project", "snapshot_expiration_days",
                "skip_snapshots", "no_snapshot"} & set(vars(args))


# --------------------------------------------------------------------------
# parse_args stage-boundary ordering
# --------------------------------------------------------------------------

def test_parse_args_rejects_backfill_end_at_or_after_outage_start() -> None:
    with pytest.raises(SystemExit):
        mod.parse_args([
            "--backfill-end", "2020-12-25",
            "--outage-start", "2020-12-25",
            "--experiment-id", "test",
        ])
    with pytest.raises(SystemExit):
        mod.parse_args([
            "--backfill-end", "2020-12-30",
            "--outage-start", "2020-12-25",
            "--experiment-id", "test",
        ])


def test_parse_args_rejects_outage_end_before_outage_start() -> None:
    with pytest.raises(SystemExit):
        mod.parse_args([
            "--backfill-end", "2020-12-26",
            "--outage-start", "2020-12-27",
            "--outage-end", "2020-12-25",
            "--experiment-id", "test",
        ])


def test_parse_args_rejects_outage_end_at_or_after_end() -> None:
    with pytest.raises(SystemExit):
        mod.parse_args([
            "--outage-end", "2020-12-31",
            "--end", "2020-12-31",
            "--experiment-id", "test",
        ])


def test_parse_args_accepts_one_day_outage() -> None:
    # outage_start == outage_end is the minimum bug-reproduction shape:
    # one skipped day. This is the new default shape too -- exactly the
    # repro the workflow ships out of the box.
    args = mod.parse_args(["--experiment-id", "test"])
    assert args.outage_start == "2020-12-29"
    assert args.outage_end == "2020-12-29"


def test_parse_args_rejects_negative_recovery_buffer_days() -> None:
    with pytest.raises(SystemExit):
        mod.parse_args([
            "--recovery-buffer-days", "-1",
            "--experiment-id", "test",
        ])


def test_parse_args_rejects_recovery_buffer_exceeding_history() -> None:
    """If recovery_buffer_days > (outage_start - start).days, Stage 3
    starts BEFORE --start and reprocesses dates the oracle never covered,
    which would make comparisons fail for configuration reasons rather
    than for the bug surface. Reject at arg-parse time. (Copilot review
    on PR #63.)"""
    # start=2020-12-01, outage_start=2020-12-05 -> max buffer = 4.
    # Asking for buffer=5 should fail.
    with pytest.raises(SystemExit):
        mod.parse_args([
            "--start", "2020-12-01",
            "--backfill-end", "2020-12-03",
            "--outage-start", "2020-12-05",
            "--outage-end", "2020-12-05",
            "--end", "2020-12-10",
            "--recovery-buffer-days", "5",
            "--experiment-id", "test",
        ])


def test_parse_args_accepts_recovery_buffer_at_history_boundary() -> None:
    """Boundary case: recovery_buffer_days == (outage_start - start).days
    means Stage 3 starts EXACTLY at --start, which is fine -- the recovery
    is allowed to cover the full modelled history."""
    args = mod.parse_args([
        "--start", "2020-12-01",
        "--backfill-end", "2020-12-03",
        "--outage-start", "2020-12-05",
        "--outage-end", "2020-12-05",
        "--end", "2020-12-10",
        "--recovery-buffer-days", "4",  # exactly outage_start - start
        "--experiment-id", "test",
    ])
    assert args.recovery_buffer_days == 4


def test_parse_args_parallel_default_false() -> None:
    args = mod.parse_args(["--experiment-id", "test"])
    assert args.parallel is False


@pytest.mark.parametrize("flag", ["--parallel", "--async"])
def test_parse_args_parallel_set_true(flag: str) -> None:
    # --async is the alias mode_equivalence also accepts; both spellings
    # land on args.parallel.
    args = mod.parse_args(["--experiment-id", "test", flag])
    assert args.parallel is True


def test_canonical_params_independent_of_parallel() -> None:
    """``--parallel`` is a runner-only knob (same convention as
    ``image_tag``): a parallel run and a sequential run produce identical
    output tables and must share a cache row. Pin that
    ``canonical_params_dict`` ignores it, so a future refactor can't
    silently fold it into the cache key and tank the hit rate.
    (Copilot review on PR #66.)"""
    base = ["--experiment-id", "test"]
    sequential = mod.parse_args(base)
    parallel = mod.parse_args(base + ["--parallel"])
    for mode in (mod.MODE_OUTAGE_RECOVERY, mod.MODE_OUTAGE_ORACLE):
        assert (
            mod.canonical_params_dict(sequential, mode)
            == mod.canonical_params_dict(parallel, mode)
        )


def test_parse_args_pin_flags_default_to_a_new_pin() -> None:
    args = mod.parse_args(["--experiment-id", "test"])

    assert args.no_pin is False
    assert args.reuse_pin is None
    assert args.dry_run is False
    assert args.pin_expiration_days == 7
    # dit.bq's 1 TB cap per copy.
    assert args.pin_max_gb == 1000.0
    # Nothing is pinned at parse time; main sets these.
    assert args.pinned_messages is None
    assert args.pinned_segments is None


def test_parse_args_no_pin_set_true() -> None:
    args = mod.parse_args(["--experiment-id", "test", "--no-pin"])
    assert args.no_pin is True


def test_parse_args_accepts_a_pin_stamp_to_reuse() -> None:
    args = mod.parse_args(["--experiment-id", "test", "--reuse-pin", _STAMP])
    assert args.reuse_pin == _STAMP


def test_parse_args_rejects_an_invalid_reuse_pin(capsys: pytest.CaptureFixture[str]) -> None:
    """An ISO timestamp is not a pin stamp; fail before anything runs."""
    with pytest.raises(SystemExit):
        mod.parse_args(["--experiment-id", "test", "--reuse-pin", "2026-11-19T10:15:00Z"])

    assert "--reuse-pin: invalid pin stamp" in capsys.readouterr().err


def test_parse_args_rejects_no_pin_and_reuse_pin_together() -> None:
    """Both decide what the jobs read (the live sources vs an earlier run's
    copies) and contradict each other. Reject at arg-parse time so an
    ambiguous safety-sensitive combo can't reach the read path. (Copilot
    review on PR #63, for the flags' predecessors.)"""
    with pytest.raises(SystemExit):
        mod.parse_args([
            "--experiment-id", "test",
            "--no-pin",
            "--reuse-pin", _STAMP,
        ])


@pytest.mark.parametrize("value", ["0", "-1"])
def test_parse_args_rejects_a_non_positive_pin_cap(value: str) -> None:
    with pytest.raises(SystemExit):
        mod.parse_args(["--experiment-id", "test", "--pin-max-gb", value])


def test_parse_args_recovery_buffer_defaults_to_zero() -> None:
    """0 is the discriminating regression-test setting (issue #59 2x2
    matrix): the recovery's DELETE then orphans the open-v1 seeds of
    wrong gaps starting before the outage, exercising the close-recovery
    path where the MAP-1676 bug class lived. buffer >= 1 sweeps the
    seeds too and masks the class on both fixed and broken code."""
    args = mod.parse_args(["--experiment-id", "test"])
    assert args.recovery_buffer_days == 0


def test_parse_args_accepts_zero_recovery_buffer_days() -> None:
    # Buffer = 0 means recovery starts exactly at outage_start (no
    # overlap with the last pre-outage day). Allowed.
    args = mod.parse_args([
        "--recovery-buffer-days", "0",
        "--experiment-id", "test",
    ])
    assert args.recovery_buffer_days == 0


# --------------------------------------------------------------------------
# canonical_params_dict
# --------------------------------------------------------------------------

def test_canonical_params_includes_mode() -> None:
    p = mod.canonical_params_dict(_args(), mod.MODE_OUTAGE_RECOVERY)
    assert p["mode"] == mod.MODE_OUTAGE_RECOVERY


@pytest.mark.parametrize("mode", [mod.MODE_OUTAGE_RECOVERY, mod.MODE_OUTAGE_ORACLE])
def test_canonical_params_identify_the_pinned_inputs_by_the_copies_names(mode: str) -> None:
    """The key holds the tables the jobs read, in both modes, and nothing of
    the snapshot era."""
    p = mod.canonical_params_dict(_args(), mode)

    assert p["pinned_messages"] == _PINNED_MESSAGES
    assert p["pinned_segments"] == _PINNED_SEGMENTS
    assert not {"pin_at", "no_snapshot"} & set(p)


@pytest.mark.parametrize("mode", [mod.MODE_OUTAGE_RECOVERY, mod.MODE_OUTAGE_ORACLE])
def test_canonical_params_a_reused_pin_hits_and_a_fresh_pin_misses(mode: str) -> None:
    """A --reuse-pin rerun recomputes the same names, so the same key. A
    fresh pin has a new stamp in both names, so another key."""
    def pinned(stamp: datetime) -> dict[str, str]:
        messages, segments = (
            dit_bq.pinned_source_fqn(
                spec.source_table, experiment_id="exp01", role=mod.PIN_ROLE,
                pinned_at=stamp, window=spec.window,
            )
            for spec in mod._pin_specs(_args())
        )
        return dict(pinned_messages=messages, pinned_segments=segments)

    first = mod.canonical_params_dict(_args(**pinned(_PINNED_AT)), mode)
    reused = mod.canonical_params_dict(_args(**pinned(dit_bq.parse_pin_stamp(_STAMP))), mode)
    fresh = mod.canonical_params_dict(
        _args(**pinned(_PINNED_AT + timedelta(seconds=1))), mode,
    )

    assert first["pinned_messages"] == _PINNED_MESSAGES
    assert reused == first
    assert fresh != first
    assert fresh["pinned_messages"] != first["pinned_messages"]
    assert fresh["pinned_segments"] != first["pinned_segments"]


@pytest.mark.parametrize("mode", [mod.MODE_OUTAGE_RECOVERY, mod.MODE_OUTAGE_ORACLE])
def test_canonical_params_ignore_the_pin_knobs_that_change_no_output(mode: str) -> None:
    base = mod.canonical_params_dict(_args(), mode)

    assert mod.canonical_params_dict(
        _args(pin_max_gb=5.0, pin_expiration_days=30, reuse_pin=_STAMP, dry_run=True), mode,
    ) == base


def test_canonical_params_oracle_drops_recovery_only_keys() -> None:
    # The oracle is a single-shot backfill [start, end] against the
    # snapshot; including the outage geometry or recovery buffer in its
    # cache key would invalidate it every time the staged geometry
    # moves, dropping the hit rate for no behavioural reason.
    rec = mod.canonical_params_dict(_args(), mod.MODE_OUTAGE_RECOVERY)
    ora = mod.canonical_params_dict(_args(), mod.MODE_OUTAGE_ORACLE)
    for k in ("backfill_end", "outage_start", "outage_end",
              "recovery_buffer_days"):
        assert k in rec, f"recovery mode should include {k!r}"
        assert k not in ora, f"oracle mode should not include {k!r}"


def test_canonical_params_recovery_depends_on_outage_geometry() -> None:
    a = mod.canonical_params_dict(_args(outage_start="2020-12-25"),
                                  mod.MODE_OUTAGE_RECOVERY)
    b = mod.canonical_params_dict(_args(outage_start="2020-12-26"),
                                  mod.MODE_OUTAGE_RECOVERY)
    assert a["outage_start"] != b["outage_start"]


def test_canonical_params_recovery_depends_on_recovery_buffer_days() -> None:
    a = mod.canonical_params_dict(_args(recovery_buffer_days=1),
                                  mod.MODE_OUTAGE_RECOVERY)
    b = mod.canonical_params_dict(_args(recovery_buffer_days=2),
                                  mod.MODE_OUTAGE_RECOVERY)
    assert a["recovery_buffer_days"] != b["recovery_buffer_days"]


def test_canonical_params_oracle_stable_under_outage_geometry_changes() -> None:
    # Bumping --outage-start (or any recovery-only key) must NOT change
    # the oracle's cache key.
    a = mod.canonical_params_dict(_args(outage_start="2020-12-25",
                                        recovery_buffer_days=1),
                                  mod.MODE_OUTAGE_ORACLE)
    b = mod.canonical_params_dict(_args(outage_start="2020-12-26",
                                        recovery_buffer_days=5),
                                  mod.MODE_OUTAGE_ORACLE)
    assert a == b


def test_canonical_params_changes_with_source_messages() -> None:
    a = mod.canonical_params_dict(
        _args(source_messages="proj.ds.messages_a"), mod.MODE_OUTAGE_RECOVERY,
    )
    b = mod.canonical_params_dict(
        _args(source_messages="proj.ds.messages_b"), mod.MODE_OUTAGE_RECOVERY,
    )
    assert a["source_messages"] != b["source_messages"]


def test_canonical_params_ssvids_default_empty_list() -> None:
    p = mod.canonical_params_dict(_args(ssvids=""), mod.MODE_OUTAGE_RECOVERY)
    assert p["ssvids"] == []


def test_canonical_params_changes_with_ssvids() -> None:
    a = mod.canonical_params_dict(_args(ssvids=""), mod.MODE_OUTAGE_RECOVERY)
    b = mod.canonical_params_dict(
        _args(ssvids="ssvid_a,ssvid_b"), mod.MODE_OUTAGE_RECOVERY,
    )
    assert a["ssvids"] != b["ssvids"]
    assert b["ssvids"] == ["ssvid_a", "ssvid_b"]


def test_canonical_params_live_reads_differ_from_pinned_runs_in_both_modes() -> None:
    # A --no-pin run reads the live sources: no copy, so both names are
    # None. It must not share a cache row with a pinned run in EITHER mode.
    live = dict(pinned_messages=None, pinned_segments=None, no_pin=True)
    for mode in (mod.MODE_OUTAGE_RECOVERY, mod.MODE_OUTAGE_ORACLE):
        pinned = mod.canonical_params_dict(_args(), mode)
        liv = mod.canonical_params_dict(_args(**live), mode)
        assert liv["pinned_messages"] is None
        assert liv["pinned_segments"] is None
        assert pinned != liv
        # Two live reads of the same sources share a row: the flag is for
        # sources that do not change.
        assert mod.canonical_params_dict(_args(**live), mode) == liv


def test_canonical_params_synthetic_outage_in_recovery_key_only() -> None:
    """M6b: the flag changes what stages 1+2 READ (filtered view vs full
    source), so it must distinguish recovery-mode cache rows. The oracle
    reads the unfiltered source either way -- its output is invariant to
    the flag, so _RECOVERY_ONLY_KEYS drops it from the oracle key
    (otherwise toggling the flag would needlessly invalidate the oracle)."""
    rec_off = mod.canonical_params_dict(
        _args(synthetic_outage=False), mod.MODE_OUTAGE_RECOVERY)
    rec_on = mod.canonical_params_dict(
        _args(synthetic_outage=True), mod.MODE_OUTAGE_RECOVERY)
    assert rec_off["synthetic_outage"] is False
    assert rec_on["synthetic_outage"] is True
    assert rec_off != rec_on

    ora_off = mod.canonical_params_dict(
        _args(synthetic_outage=False), mod.MODE_OUTAGE_ORACLE)
    ora_on = mod.canonical_params_dict(
        _args(synthetic_outage=True), mod.MODE_OUTAGE_ORACLE)
    assert "synthetic_outage" not in ora_off
    assert ora_off == ora_on  # oracle invariant to the flag


def test_synthetic_outage_where_clause_excludes_inclusive_window() -> None:
    clause = mod._synthetic_outage_where_clause(
        date(2020, 12, 29), date(2020, 12, 29))
    assert clause == (
        "DATE(timestamp) NOT BETWEEN '2020-12-29' AND '2020-12-29'"
    )


def test_synthetic_outage_role_encodes_geometry() -> None:
    """The role (hence the derived view's name) encodes the outage dates,
    so a re-run with the same --experiment-id but different geometry
    derives a NEW view instead of silently reusing the stale one --
    the same staleness class as the documented skip-existing snapshot
    footgun, dodged structurally."""
    a = mod._synthetic_outage_role(date(2020, 12, 29), date(2020, 12, 29))
    b = mod._synthetic_outage_role(date(2020, 12, 25), date(2020, 12, 29))
    assert a == "outage_filtered_20201229_20201229"
    assert b == "outage_filtered_20201225_20201229"
    assert a != b


def test_derive_synthetic_outage_view_calls_helper() -> None:
    """_derive_synthetic_outage_view delegates to the M6a helper with the
    geometry-encoding role, the outage WHERE clause, the copies' expiration
    and dit's project; returns the helper's dest FQN. The view is derived
    over the pinned copy it is given."""
    with patch(
        "workflows.pipe_gaps.outage_recovery.dit_bq.derived_source_into_experiment"
    ) as helper:
        helper.return_value = "proj.tech_great_expectations.dit_exp_exp01_view"
        out = mod._derive_synthetic_outage_view(
            _args(pin_expiration_days=3),
            source_messages_fqn=_PINNED_MESSAGES,
            outage_start=date(2020, 12, 29),
            outage_end=date(2020, 12, 29),
        )

    assert out == "proj.tech_great_expectations.dit_exp_exp01_view"
    helper.assert_called_once()
    call = helper.call_args
    assert call.args[0] == _PINNED_MESSAGES
    assert call.kwargs["experiment_id"] == "exp01"
    assert call.kwargs["role"] == "outage_filtered_20201229_20201229"
    assert call.kwargs["where_clause"] == (
        "DATE(timestamp) NOT BETWEEN '2020-12-29' AND '2020-12-29'"
    )
    assert call.kwargs["expiration_days"] == 3
    assert call.kwargs["project"] == "world-fishing-827"


def test_the_synthetic_outage_view_of_a_new_pin_has_a_new_name() -> None:
    """The view's name ends in the name of the table it filters, and a copy's
    name ends in its pin stamp. So a fresh pin never reuses an older pin's
    view through ``IF NOT EXISTS``; a --reuse-pin rerun does."""
    def view_name(messages: str) -> str:
        # The real helper, with a stand-in BigQuery client.
        with patch.object(dit_bq.bigquery, "Client"):
            return mod._derive_synthetic_outage_view(
                _args(), source_messages_fqn=messages,
                outage_start=date(2020, 12, 29), outage_end=date(2020, 12, 29),
            )

    other_pin = _PINNED_MESSAGES.replace(_STAMP, "20261120t000000z")
    assert view_name(_PINNED_MESSAGES).endswith(_STAMP)
    assert view_name(_PINNED_MESSAGES) == view_name(_PINNED_MESSAGES)
    assert view_name(other_pin) != view_name(_PINNED_MESSAGES)


def test_execute_outage_recovery_routes_filtered_messages_to_stages_1_2_only() -> None:
    """The load-bearing M6b routing: stages 1+2 read the filtered view
    (the source as it looked DURING the outage); stage 3 (recovery) reads
    the unfiltered messages (the source after it healed). If stage 3 ever
    read the filtered view, the recovery couldn't reconcile and the test
    would flag a false positive; if stages 1+2 read unfiltered, the bug
    class never triggers and the comparison is trivially clean."""
    from unittest.mock import patch

    captured: list[dict] = []

    def fake_make_config(**kwargs):
        captured.append(kwargs)
        return object()

    with (
        patch.object(mod, "_make_config", side_effect=fake_make_config),
        patch.object(mod, "_run_pipeline"),
    ):
        mod.execute_outage_recovery(
            "dataflow",
            base_cfg=dict(
                bq_in_messages="proj.ds.messages_unfiltered",
                bq_in_segments="proj.ds.segments",
            ),
            start=date(2020, 1, 1),
            backfill_end=date(2020, 12, 28),
            outage_start=date(2020, 12, 29),
            outage_end=date(2020, 12, 29),
            end=date(2020, 12, 31),
            recovery_buffer_days=1,
            output="proj.ds.out",
            experiment_id="exp01",
            image_tag="img",
            filtered_messages="proj.ds.messages_FILTERED",
        )

    assert len(captured) == 3
    assert captured[0]["bq_in_messages"] == "proj.ds.messages_FILTERED"  # Stage 1
    assert captured[1]["bq_in_messages"] == "proj.ds.messages_FILTERED"  # Stage 2
    assert captured[2]["bq_in_messages"] == "proj.ds.messages_unfiltered"  # Stage 3
    # Segments are never filtered (known simplification).
    assert all(c["bq_in_segments"] == "proj.ds.segments" for c in captured)


def test_execute_outage_recovery_default_no_filter_all_stages_unfiltered() -> None:
    """Without filtered_messages (the default), all three stages read the
    same source -- byte-identical to the pre-M6b behaviour."""
    from unittest.mock import patch

    captured: list[dict] = []

    def fake_make_config(**kwargs):
        captured.append(kwargs)
        return object()

    with (
        patch.object(mod, "_make_config", side_effect=fake_make_config),
        patch.object(mod, "_run_pipeline"),
    ):
        mod.execute_outage_recovery(
            "dataflow",
            base_cfg=dict(
                bq_in_messages="proj.ds.messages",
                bq_in_segments="proj.ds.segments",
            ),
            start=date(2020, 1, 1),
            backfill_end=date(2020, 12, 28),
            outage_start=date(2020, 12, 29),
            outage_end=date(2020, 12, 29),
            end=date(2020, 12, 31),
            recovery_buffer_days=1,
            output="proj.ds.out",
            experiment_id="exp01",
            image_tag="img",
        )

    assert len(captured) == 3
    assert all(c["bq_in_messages"] == "proj.ds.messages" for c in captured)


def _captured_stage_configs(**execute_overrides: Any) -> list[dict]:
    """Run execute_outage_recovery with mocked pipeline calls and return
    the per-stage _make_config kwargs. Defaults mirror the workflow's
    default geometry (one-day outage on 2020-12-29,
    recovery_buffer_days=0 -- the discriminating regression-test
    default)."""
    from unittest.mock import patch

    captured: list[dict] = []

    def fake_make_config(**kwargs):
        captured.append(kwargs)
        return object()

    kwargs: dict[str, Any] = dict(
        base_cfg=dict(
            bq_in_messages="proj.ds.messages",
            bq_in_segments="proj.ds.segments",
        ),
        start=date(2020, 1, 1),
        backfill_end=date(2020, 12, 28),
        outage_start=date(2020, 12, 29),
        outage_end=date(2020, 12, 29),
        end=date(2020, 12, 31),
        recovery_buffer_days=0,
        output="proj.ds.out",
        experiment_id="exp01",
        image_tag="img",
    )
    kwargs.update(execute_overrides)

    with (
        patch.object(mod, "_make_config", side_effect=fake_make_config),
        patch.object(mod, "_run_pipeline"),
    ):
        mod.execute_outage_recovery("dataflow", **kwargs)

    return captured


def test_stage_ranges_inclusive_end_schedule_skip() -> None:
    """Workflow-level dates are inclusive; pipe-gaps' --date-range upper
    bound is exclusive (messages.sql.j2: DATE(timestamp) < end_date), so
    every pipeline call must pass <inclusive end> + 1 day. Pre-fix, the
    workflow passed the inclusive dates verbatim and silently dropped
    each stage's final day (verified live in issue #59 debugging: zero
    gaps on the cohort's last data day despite 62k source messages)."""
    captured = _captured_stage_configs()

    # Stage 1: [start, backfill_end] inclusive -> end passes 12-29.
    assert captured[0]["start"] == date(2020, 1, 1)
    assert captured[0]["end"] == date(2020, 12, 29)
    # Stage 2 (schedule-skip): [outage_end + 1, end] -> 12-30 .. 1-1.
    assert captured[1]["start"] == date(2020, 12, 30)
    assert captured[1]["end"] == date(2021, 1, 1)
    # Stage 3 (default buffer=0): [outage_start, end] -> 12-29 .. 1-1.
    assert captured[2]["start"] == date(2020, 12, 29)
    assert captured[2]["end"] == date(2021, 1, 1)


def test_stage3_start_shifts_with_recovery_buffer() -> None:
    """A non-zero --recovery-buffer-days shifts the recovery start
    earlier (outage_start - buffer); kept covered explicitly since the
    default flipped to 0."""
    captured = _captured_stage_configs(recovery_buffer_days=1)
    assert captured[2]["start"] == date(2020, 12, 28)
    assert captured[2]["end"] == date(2021, 1, 1)


def test_stage2_bridges_outage_when_synthetic() -> None:
    """The issue #59 geometry fix: with the synthetic outage active,
    Stage 2 must SPAN the outage window ([outage_start - 1, end]) so its
    range queries the filtered-out dates and the detector bridges from
    the last pre-outage message to the first post-outage one. Without
    the spanning range the filter is a no-op (it hides rows the stage's
    range never queries) -- proven live on run e2esynth1."""
    captured = _captured_stage_configs(
        filtered_messages="proj.ds.messages_FILTERED",
    )

    # Stage 2 starts on the bridge day (outage_start - 1), not after the
    # outage -- AND reads the filtered view. (The bridge day is
    # independent of --recovery-buffer-days.)
    assert captured[1]["start"] == date(2020, 12, 28)
    assert captured[1]["end"] == date(2021, 1, 1)
    assert captured[1]["bq_in_messages"] == "proj.ds.messages_FILTERED"
    # Stages 1+3 keep their schedule-skip ranges (Stage 3 with the
    # default buffer=0 starts at outage_start).
    assert captured[0]["start"] == date(2020, 1, 1)
    assert captured[0]["end"] == date(2020, 12, 29)
    assert captured[2]["start"] == date(2020, 12, 29)
    assert captured[2]["end"] == date(2021, 1, 1)


def test_oracle_end_inclusive() -> None:
    """The oracle converts the workflow's inclusive --end the same way
    the staged stages do (end + 1 day)."""
    from unittest.mock import patch

    captured: list[dict] = []

    def fake_make_config(**kwargs):
        captured.append(kwargs)
        return object()

    with (
        patch.object(mod, "_make_config", side_effect=fake_make_config),
        patch.object(mod, "_run_pipeline"),
    ):
        mod.execute_outage_oracle(
            "dataflow",
            base_cfg=dict(
                bq_in_messages="proj.ds.messages",
                bq_in_segments="proj.ds.segments",
            ),
            start=date(2020, 1, 1),
            end=date(2020, 12, 31),
            output="proj.ds.out",
            experiment_id="exp01",
            image_tag="img",
        )

    assert len(captured) == 1
    assert captured[0]["start"] == date(2020, 1, 1)
    assert captured[0]["end"] == date(2021, 1, 1)


def test_parse_args_synthetic_outage_default_false() -> None:
    args = mod.parse_args(["--experiment-id", "test"])
    assert args.synthetic_outage is False


def test_parse_args_synthetic_outage_set_true() -> None:
    args = mod.parse_args(["--experiment-id", "test", "--synthetic-outage"])
    assert args.synthetic_outage is True


def test_canonical_params_ssvids_normalised_by_sort() -> None:
    a = mod.canonical_params_dict(
        _args(ssvids="zeta,alpha,mike"), mod.MODE_OUTAGE_RECOVERY,
    )
    b = mod.canonical_params_dict(
        _args(ssvids="alpha,mike,zeta"), mod.MODE_OUTAGE_RECOVERY,
    )
    assert a["ssvids"] == b["ssvids"] == ["alpha", "mike", "zeta"]


# --------------------------------------------------------------------------
# What is pinned: the window of each source
# --------------------------------------------------------------------------

def test_pin_specs_window_the_messages_and_copy_the_segments_whole() -> None:
    messages, segments = mod._pin_specs(_args())

    assert messages.source_table == f"{_STEM}_internal.messages_positions"
    # --start 2020-01-01 minus one day of lookback (ceil(12 h / 24)); the
    # inclusive --end 2020-12-31 becomes the exclusive 2021-01-01.
    assert messages.window == dit_bq.SourceWindow(
        column="timestamp", start=date(2019, 12, 31), end=date(2021, 1, 1),
    )
    assert messages.window.predicate() == (
        "DATE(timestamp) >= '2019-12-31' AND DATE(timestamp) < '2021-01-01'"
    )
    # pipe-gaps reads the segments without a date filter.
    assert segments.source_table == f"{_STEM}_published.segs_activity"
    assert segments.window is None


@pytest.mark.parametrize("n_hours_before, lookback_days", [
    (0, 0), (1, 1), (12, 1), (24, 1), (25, 2), (48, 2), (49, 3),
])
def test_messages_window_starts_pipe_gaps_lookback_before_the_start(
    n_hours_before: int, lookback_days: int,
) -> None:
    """pipe-gaps reads from ``start_date - ceil(n_hours_before / 24)`` days
    (``RawGapsConfig.messages_query_start_date`` at v0.17.1, as at v0.10.0)."""
    window = mod._messages_window(date(2020, 3, 1), date(2020, 3, 10), n_hours_before)

    assert window.column == "timestamp"
    assert window.start == date(2020, 3, 1) - timedelta(days=lookback_days)
    assert window.end == date(2020, 3, 11)


def test_pin_specs_follow_the_run_dates_and_the_source_flags() -> None:
    """Production VMS sources sit in another organisation; a copy reads them
    like any other table."""
    messages, segments = mod._pin_specs(_args(
        start="2026-06-01", end="2026-06-30", n_hours_before=30,
        source_messages="gfw-int-vms-v3.pipe_vms_v3_internal.research_messages",
        source_segments="gfw-int-vms-v3.pipe_vms_v3_internal.segs_activity",
    ))

    assert messages.source_table == "gfw-int-vms-v3.pipe_vms_v3_internal.research_messages"
    assert messages.window == dit_bq.SourceWindow(
        column="timestamp", start=date(2026, 5, 30), end=date(2026, 7, 1),
    )
    assert segments == mod._PinSpec("gfw-int-vms-v3.pipe_vms_v3_internal.segs_activity", None)


@pytest.mark.parametrize("execute_overrides", [
    dict(),
    dict(filtered_messages="proj.ds.messages_FILTERED"),
    dict(recovery_buffer_days=3),
    dict(backfill_end=date(2020, 6, 30), outage_start=date(2020, 7, 10),
         outage_end=date(2020, 7, 20), recovery_buffer_days=10),
])
@pytest.mark.parametrize("n_hours_before", [0, 12, 36])
def test_the_messages_window_covers_what_every_stage_and_the_oracle_read(
    execute_overrides: dict, n_hours_before: int,
) -> None:
    """Every job over ``[s, e)`` reads the messages of ``[s - lookback, e)``.
    The copy must hold all of them, in schedule-skip and synthetic-outage
    geometry alike, or a stage would silently read fewer rows than it would
    from the source."""
    start, end = date(2020, 1, 1), date(2020, 12, 31)
    window = mod._messages_window(start, end, n_hours_before)
    lookback = timedelta(days=math.ceil(n_hours_before / 24))

    oracle: list[dict] = []
    with (
        patch.object(mod, "_make_config", side_effect=lambda **kw: oracle.append(kw)),
        patch.object(mod, "_run_pipeline"),
    ):
        mod.execute_outage_oracle(
            "dataflow", base_cfg={}, start=start, end=end, output="proj.ds.out",
            experiment_id="exp01", image_tag="img",
        )
    jobs = [*_captured_stage_configs(**execute_overrides), *oracle]

    assert len(jobs) == 4
    for job in jobs:
        assert window.start <= job["start"] - lookback
        assert job["end"] <= window.end
    # And no wider than needed: the oracle reads the whole window.
    assert oracle[0]["start"] - lookback == window.start
    assert oracle[0]["end"] == window.end


def test_sources_sharing_a_table_name_get_distinct_copies() -> None:
    """One role serves both sources: the messages' name holds a window and
    the segments' holds ``all``, so the two never collide."""
    messages, segments = mod._pin_specs(_args(
        source_messages="proj.a.same_name", source_segments="proj.b.same_name",
    ))

    names = [
        dit_bq.pinned_source_fqn(
            spec.source_table, experiment_id="exp01", role=mod.PIN_ROLE,
            pinned_at=_PINNED_AT, window=spec.window,
        )
        for spec in (messages, segments)
    ]
    assert names == [
        f"{_GE}.dit_exp_exp01_outage_same_name_20191231_20210101_{_STAMP}",
        f"{_GE}.dit_exp_exp01_outage_same_name_all_{_STAMP}",
    ]


# --------------------------------------------------------------------------
# _pin_sources / _pin / _resolve_sources: new copies, reuse, dry run, live
# --------------------------------------------------------------------------

def test_pin_sources_copies_both_sources_under_one_stamp() -> None:
    with patch.object(mod.dit_bq, "pin_source_into_experiment") as pin:
        pin.side_effect = lambda src, **kw: f"pin:{src}"
        pinned = mod._pin_sources(
            _args(pin_max_gb=250.0, pin_expiration_days=3), _PINNED_AT, reuse=False,
        )

    assert pinned == (
        f"pin:{_STEM}_internal.messages_positions",
        f"pin:{_STEM}_published.segs_activity",
    )
    assert [call.kwargs["window"] for call in pin.call_args_list] == [
        dit_bq.SourceWindow("timestamp", date(2019, 12, 31), date(2021, 1, 1)),
        None,
    ]
    for call in pin.call_args_list:
        assert call.kwargs["pinned_at"] == _PINNED_AT
        assert call.kwargs["experiment_id"] == "exp01"
        assert call.kwargs["role"] == "outage"
        assert call.kwargs["expiration_days"] == 3
        assert call.kwargs["max_bytes"] == 250 * 10**9
        assert call.kwargs["dry_run"] is False
        assert call.kwargs["project"] == "world-fishing-827"


def test_pin_sources_dry_run_reaches_the_helper() -> None:
    """--dry-run must not create copies: each can process up to the cap."""
    with patch.object(mod.dit_bq, "pin_source_into_experiment") as pin:
        mod._pin_sources(_args(dry_run=True), _PINNED_AT, reuse=False)

    assert [call.kwargs["dry_run"] for call in pin.call_args_list] == [True, True]


def test_pin_sources_reuse_only_checks_the_recomputed_names() -> None:
    with (
        patch.object(mod.dit_bq, "pin_source_into_experiment") as pin,
        patch.object(
            mod.dit_bq, "require_pinned_source", side_effect=lambda fqn, **kw: fqn,
        ) as require,
    ):
        pinned = mod._pin_sources(_args(), _PINNED_AT, reuse=True)

    pin.assert_not_called()
    assert require.call_count == 2
    assert pinned == (_PINNED_MESSAGES, _PINNED_SEGMENTS)


def test_reuse_fails_before_any_job_when_a_copy_is_missing() -> None:
    with (
        patch.object(
            mod.dit_bq, "require_pinned_source",
            side_effect=ValueError("pinned copy x does not exist"),
        ),
        pytest.raises(ValueError, match="does not exist"),
    ):
        mod._pin_sources(_args(), _PINNED_AT, reuse=True)


def test_pin_takes_the_stamp_from_reuse_pin_or_from_now(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    with patch.object(mod, "_pin_sources", return_value=("m", "s")) as pin_sources:
        assert mod._pin(_args(reuse_pin=_STAMP)) == ("m", "s")
        assert mod._pin(_args()) == ("m", "s")

    reused, fresh = pin_sources.call_args_list
    assert reused.args[1] == _PINNED_AT
    assert reused.kwargs["reuse"] is True
    assert fresh.kwargs["reuse"] is False
    # A fresh stamp is now, in whole seconds: the stamp's precision.
    assert fresh.args[1].tzinfo is not None
    assert fresh.args[1].microsecond == 0
    assert abs(datetime.now(timezone.utc) - fresh.args[1]) < timedelta(minutes=1)
    # Only a run that made new copies tells how to reuse them.
    hints = [m for m in caplog.messages if "pass --reuse-pin" in m]
    assert hints == [
        f"to rerun on these copies, pass --reuse-pin {dit_bq.format_pin_stamp(fresh.args[1])}"
    ]


def test_resolve_sources_reads_the_live_sources_with_no_pin(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with (
        patch.object(mod.dit_bq, "pin_source_into_experiment") as pin,
        patch.object(mod.dit_bq, "require_pinned_source") as require,
    ):
        sources = mod._resolve_sources(_args(no_pin=True))

    assert sources == (
        f"{_STEM}_internal.messages_positions", f"{_STEM}_published.segs_activity",
    )
    pin.assert_not_called()
    require.assert_not_called()
    assert any("--no-pin: every job reads the live" in m for m in caplog.messages)


# --------------------------------------------------------------------------
# main: what the stages read, the cache key, the dry run
# --------------------------------------------------------------------------

def _ctx() -> RunContext:
    return RunContext(pipeline_commit="c", unreviewed=False, pipeline_commit_parent=None,
                      worker_image="img", worker_image_digest="img", run_id="r",
                      dit_commit="d")


def _main(*argv: str) -> dict[str, Any]:
    """Run ``main`` with every cloud call patched. Returns the calls made:
    ``pin`` and ``require`` (the dit.bq helpers), ``derive`` (the view),
    ``run`` (per mode: the execute kwargs and the cache-key params at the
    time the mode ran), ``context`` and ``rc``."""
    runs: dict[str, dict[str, Any]] = {}

    def fake_run_with_cache(fn, *, args, mode, output_fqn, execute_kwargs):
        runs[mode] = dict(
            kwargs=execute_kwargs, params=mod.canonical_params_dict(args, mode),
        )
        return output_fqn

    def copy_name(source_table: str, *, dry_run: bool = False, **kwargs: Any) -> str:
        names = {"messages_positions": _PINNED_MESSAGES, "segs_activity": _PINNED_SEGMENTS}
        return names[source_table.rsplit(".", 1)[-1]]

    with (
        patch.object(mod, "resolve_run_context", return_value=_ctx()) as context,
        patch.object(mod, "_run_with_cache", side_effect=fake_run_with_cache),
        patch.object(mod.dit_bq, "pin_source_into_experiment", side_effect=copy_name) as pin,
        patch.object(mod.dit_bq, "require_pinned_source",
                     side_effect=lambda fqn, **kw: fqn) as require,
        patch.object(mod.dit_bq, "derived_source_into_experiment",
                     return_value="p.d.FILTERED_VIEW") as derive,
        patch.object(mod.dit_compare, "compare_tables", return_value=0) as compare,
        # The dry run's offline config check (tests/test_pipe_gaps_raw_gaps.py).
        patch.object(mod, "_check_jobs") as check_jobs,
    ):
        rc = mod.main(["--experiment-id", "exp01", *argv])
    return dict(rc=rc, pin=pin, require=require, derive=derive, run=runs,
                context=context, compare=compare, check_jobs=check_jobs)


def _stage_messages(recovery_kwargs: dict[str, Any]) -> list[str]:
    """The messages table each of the three stages reads, from the kwargs
    ``main`` hands to ``execute_outage_recovery``."""
    captured: list[dict] = []
    with (
        patch.object(mod, "_make_config", side_effect=lambda **kw: captured.append(kw)),
        patch.object(mod, "_run_pipeline"),
    ):
        mod.execute_outage_recovery(**recovery_kwargs)
    return [stage["bq_in_messages"] for stage in captured]


def test_main_runs_every_job_on_the_pinned_copies() -> None:
    result = _main("--skip-comparisons")

    assert result["rc"] == 0
    assert result["pin"].call_count == 2
    result["derive"].assert_not_called()
    recovery = result["run"][mod.MODE_OUTAGE_RECOVERY]["kwargs"]
    oracle = result["run"][mod.MODE_OUTAGE_ORACLE]["kwargs"]
    for kwargs in (recovery, oracle):
        assert kwargs["base_cfg"]["bq_in_messages"] == _PINNED_MESSAGES
        assert kwargs["base_cfg"]["bq_in_segments"] == _PINNED_SEGMENTS
    assert recovery["filtered_messages"] is None
    assert _stage_messages(recovery) == [_PINNED_MESSAGES] * 3


def test_main_synthetic_outage_filters_the_copy_for_stages_1_and_2_only() -> None:
    """The view is derived over the pinned copy. Stages 1 and 2 read the
    view; stage 3 and the oracle read the unfiltered copy."""
    result = _main("--skip-comparisons", "--synthetic-outage")

    derive = result["derive"].call_args
    assert derive.args[0] == _PINNED_MESSAGES
    assert derive.kwargs["role"] == "outage_filtered_20201229_20201229"
    recovery = result["run"][mod.MODE_OUTAGE_RECOVERY]["kwargs"]
    oracle = result["run"][mod.MODE_OUTAGE_ORACLE]["kwargs"]
    assert _stage_messages(recovery) == [
        "p.d.FILTERED_VIEW", "p.d.FILTERED_VIEW", _PINNED_MESSAGES,
    ]
    assert oracle["base_cfg"]["bq_in_messages"] == _PINNED_MESSAGES
    assert "filtered_messages" not in oracle
    # The segments are never filtered.
    assert recovery["base_cfg"]["bq_in_segments"] == _PINNED_SEGMENTS


def test_main_puts_the_copies_in_the_cache_key_before_any_mode_runs() -> None:
    result = _main("--skip-comparisons")

    for mode in (mod.MODE_OUTAGE_RECOVERY, mod.MODE_OUTAGE_ORACLE):
        params = result["run"][mode]["params"]
        assert params["pinned_messages"] == _PINNED_MESSAGES
        assert params["pinned_segments"] == _PINNED_SEGMENTS


def test_main_reuse_pin_copies_nothing_and_keeps_the_first_runs_cache_key() -> None:
    """The copy helper is patched to return the names of stamp _STAMP, so the
    first run stands for the run that made them. The --reuse-pin rerun
    recomputes those names itself."""
    first = _main("--skip-comparisons", "--synthetic-outage")
    rerun = _main("--skip-comparisons", "--synthetic-outage", "--reuse-pin", _STAMP)

    rerun["pin"].assert_not_called()
    assert [call.args[0] for call in rerun["require"].call_args_list] == [
        _PINNED_MESSAGES, _PINNED_SEGMENTS,
    ]
    # The view is derived again, over the same copy: IF NOT EXISTS keeps it.
    assert rerun["derive"].call_args.args[0] == _PINNED_MESSAGES
    for mode in (mod.MODE_OUTAGE_RECOVERY, mod.MODE_OUTAGE_ORACLE):
        assert rerun["run"][mode]["params"] == first["run"][mode]["params"]


def test_main_oracle_cache_key_is_invariant_to_the_synthetic_outage_flag() -> None:
    plain = _main("--skip-comparisons")
    synthetic = _main("--skip-comparisons", "--synthetic-outage")

    assert (synthetic["run"][mod.MODE_OUTAGE_ORACLE]["params"]
            == plain["run"][mod.MODE_OUTAGE_ORACLE]["params"])
    assert (synthetic["run"][mod.MODE_OUTAGE_RECOVERY]["params"]
            != plain["run"][mod.MODE_OUTAGE_RECOVERY]["params"])


def test_main_no_pin_reads_the_live_sources_and_filters_them_when_asked() -> None:
    result = _main("--skip-comparisons", "--no-pin", "--synthetic-outage")

    result["pin"].assert_not_called()
    result["require"].assert_not_called()
    live_messages = f"{_STEM}_internal.messages_positions"
    assert result["derive"].call_args.args[0] == live_messages
    recovery = result["run"][mod.MODE_OUTAGE_RECOVERY]
    assert _stage_messages(recovery["kwargs"]) == [
        "p.d.FILTERED_VIEW", "p.d.FILTERED_VIEW", live_messages,
    ]
    assert recovery["kwargs"]["base_cfg"]["bq_in_segments"] == (
        f"{_STEM}_published.segs_activity"
    )
    assert recovery["params"]["pinned_messages"] is None
    assert recovery["params"]["pinned_segments"] is None


def test_main_dry_run_estimates_the_copies_and_runs_nothing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A dry run reports the copies against the cap and stops: no run context
    (which can push an auto-snapshot or build an image), no view, no
    pipeline, no comparison."""
    caplog.set_level(logging.INFO)
    result = _main("--dry-run", "--synthetic-outage", "--pin-max-gb", "5")

    assert result["rc"] == 0
    assert [call.kwargs["dry_run"] for call in result["pin"].call_args_list] == [True, True]
    assert [call.kwargs["max_bytes"] for call in result["pin"].call_args_list] == [5 * 10**9] * 2
    result["context"].assert_not_called()
    result["derive"].assert_not_called()
    result["compare"].assert_not_called()
    assert result["run"] == {}
    assert not any("pass --reuse-pin" in m for m in caplog.messages)
    # The jobs are checked on the copies the run would read.
    assert result["check_jobs"].call_args.args[1:] == (_PINNED_MESSAGES, _PINNED_SEGMENTS)


def test_main_dry_run_fails_when_a_copy_is_over_the_cap() -> None:
    """The cap is checked by dit.bq's dry run of the copy statement; the
    workflow lets that refusal through."""
    with (
        patch.object(mod, "resolve_run_context") as context,
        patch.object(
            mod.dit_bq, "pin_source_into_experiment",
            side_effect=ValueError("pinning x would process 4.4 GB, above the 1.0 GB cap"),
        ),
        pytest.raises(ValueError, match="above the 1.0 GB cap"),
    ):
        mod.main(["--experiment-id", "exp01", "--dry-run", "--pin-max-gb", "1"])

    context.assert_not_called()


def test_main_dry_run_with_reuse_pin_checks_that_the_copies_exist() -> None:
    result = _main("--dry-run", "--reuse-pin", _STAMP)

    result["pin"].assert_not_called()
    assert result["require"].call_count == 2
    assert result["run"] == {}


def test_main_dry_run_of_a_compare_only_run_estimates_nothing() -> None:
    """--skip-pipelines pins nothing, so its dry run has no copy to report."""
    result = _main("--dry-run", "--skip-pipelines")

    assert result["rc"] == 0
    result["pin"].assert_not_called()
    result["compare"].assert_not_called()


def test_main_skip_pipelines_pins_nothing() -> None:
    """A compare-only run reads no source. A copy costs a query, unlike the
    snapshot it replaced, so none is made."""
    result = _main("--skip-pipelines", "--synthetic-outage")

    assert result["rc"] == 0
    result["pin"].assert_not_called()
    result["require"].assert_not_called()
    result["derive"].assert_not_called()
    assert result["run"] == {}
    result["compare"].assert_called_once()


# --------------------------------------------------------------------------
# Staging-by-default contract
# --------------------------------------------------------------------------

def test_default_source_messages_is_staging_cohort() -> None:
    # Matches mode_equivalence.py's staging-by-default precedent: no
    # source-data flag's default may resolve to a prod FQN. The default
    # must point at the pipe_ais_test_* staging cohort in world-fishing-827.
    assert "pipe_ais_test_202408290000" in mod.DEFAULT_SOURCE_MESSAGES
    assert mod.DEFAULT_SOURCE_MESSAGES.startswith("world-fishing-827.")
    # Defensive: make sure no prod-VMS leaks into the default.
    assert "gfw-int-vms-v3" not in mod.DEFAULT_SOURCE_MESSAGES
    assert "pipe_vms_v" not in mod.DEFAULT_SOURCE_MESSAGES


def test_default_source_segments_is_staging_cohort() -> None:
    assert "pipe_ais_test_202408290000" in mod.DEFAULT_SOURCE_SEGMENTS
    assert mod.DEFAULT_SOURCE_SEGMENTS.startswith("world-fishing-827.")
    assert "gfw-int-vms-v3" not in mod.DEFAULT_SOURCE_SEGMENTS
    assert "pipe_vms_v" not in mod.DEFAULT_SOURCE_SEGMENTS


# --------------------------------------------------------------------------
# --pin-expiration-days validation
# --------------------------------------------------------------------------

@pytest.mark.parametrize("value", ["0", "-1", "-100"])
def test_parse_args_rejects_nonpositive_pin_expiration(value: str) -> None:
    with pytest.raises(SystemExit):
        mod.parse_args([
            "--experiment-id", "test",
            "--pin-expiration-days", value,
        ])


def test_parse_args_accepts_positive_pin_expiration() -> None:
    args = mod.parse_args([
        "--experiment-id", "test",
        "--pin-expiration-days", "30",
    ])
    assert args.pin_expiration_days == 30
