"""Tests for ``workflows/pipe_gaps/outage_recovery.py``.

Focused on workflow-local helpers: pin-at validation, the stage-boundary
ordering check in ``parse_args``, ``canonical_params_dict`` cache-key
composition, and the snapshot-table naming. The 3-stage execute
function, the snapshot creation, and the dataflow runner are exercised
by live ``dit run`` invocations against real BQ; they're not unit-tested
here.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pytest

from workflows.pipe_gaps import outage_recovery as mod


def _args(**overrides: Any) -> argparse.Namespace:
    base = dict(
        experiment_id="exp01",
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
        pin_at="2026-06-01 18:00:00 UTC",
        snapshot_expiration_days=7,
        snapshot_dest_project="world-fishing-827",
        no_snapshot=False,
        synthetic_outage=False,
    )
    base.update(overrides)
    return argparse.Namespace(**base)


# --------------------------------------------------------------------------
# _parse_pin_at / _validate_pin_at
# --------------------------------------------------------------------------

@pytest.mark.parametrize("value", [
    "2026-05-27 18:00:00 UTC",
    "2026-05-27T18:00:00Z",
    "2026-05-27T18:00:00+00:00",
    "  2026-05-27 18:00:00 UTC  ",
])
def test_validate_pin_at_accepts_iso(value: str) -> None:
    assert mod._validate_pin_at(value).strip() == value.strip()


@pytest.mark.parametrize("value", ["", "not-a-date", "2026-13-99"])
def test_validate_pin_at_rejects_garbage(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        mod._validate_pin_at(value)


def test_validate_pin_at_rejects_naive() -> None:
    # The snapshot mechanism (CREATE SNAPSHOT TABLE ... FOR SYSTEM_TIME AS OF)
    # interprets naive timestamps against the session zone, which would silently
    # drift if run from non-UTC. Reject at arg-parse time.
    with pytest.raises(argparse.ArgumentTypeError):
        mod._validate_pin_at("2026-05-27 18:00:00")
    with pytest.raises(argparse.ArgumentTypeError):
        mod._validate_pin_at("2026-05-27T18:00:00")


def test_parse_pin_at_returns_tz_aware() -> None:
    parsed = mod._parse_pin_at("2026-05-27 18:00:00 UTC")
    assert parsed.tzinfo is not None
    assert parsed == datetime(2026, 5, 27, 18, 0, 0, tzinfo=timezone.utc)


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


def test_parse_args_no_snapshot_default_false() -> None:
    args = mod.parse_args(["--experiment-id", "test"])
    assert args.no_snapshot is False


def test_parse_args_no_snapshot_set_true() -> None:
    args = mod.parse_args(["--experiment-id", "test", "--no-snapshot"])
    assert args.no_snapshot is True


def test_parse_args_rejects_no_snapshot_and_skip_snapshots_together() -> None:
    """Both flags affect the read path of the staged stages (live source vs
    reuse-prior-snapshot) but they're mutually exclusive: --no-snapshot
    reads live; --skip-snapshots reads a prior snapshot. The if/elif in
    main() would silently let --no-snapshot win. Reject at arg-parse time
    so an ambiguous safety-sensitive combo can't reach the read path.
    (Copilot review on PR #63.)"""
    with pytest.raises(SystemExit):
        mod.parse_args([
            "--experiment-id", "test",
            "--no-snapshot",
            "--skip-snapshots",
        ])


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


def test_canonical_params_pin_at_normalised_to_iso() -> None:
    # The cache key stores the parsed-and-re-emitted ISO form so two
    # equivalent strings (one with " UTC", one with "+00:00") produce
    # identical cache keys.
    a = mod.canonical_params_dict(
        _args(pin_at="2026-06-01 18:00:00 UTC"),
        mod.MODE_OUTAGE_ORACLE,
    )
    b = mod.canonical_params_dict(
        _args(pin_at="2026-06-01T18:00:00+00:00"),
        mod.MODE_OUTAGE_ORACLE,
    )
    assert a["pin_at"] == b["pin_at"]


def test_canonical_params_includes_pin_at_for_both_modes() -> None:
    for mode in (mod.MODE_OUTAGE_RECOVERY, mod.MODE_OUTAGE_ORACLE):
        p = mod.canonical_params_dict(_args(), mode)
        assert p["pin_at"] == "2026-06-01T18:00:00+00:00"


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


def test_canonical_params_no_snapshot_in_key_for_both_modes() -> None:
    # The boolean must be in BOTH modes' keys so runs with --no-snapshot
    # and without don't share a cache row (they read different source data).
    for mode in (mod.MODE_OUTAGE_RECOVERY, mod.MODE_OUTAGE_ORACLE):
        rec = mod.canonical_params_dict(_args(no_snapshot=False), mode)
        liv = mod.canonical_params_dict(_args(no_snapshot=True), mode)
        assert rec["no_snapshot"] is False
        assert liv["no_snapshot"] is True
        assert rec != liv


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
    geometry-encoding role, the outage WHERE clause, and the workflow's
    expiration/project knobs; returns the helper's dest FQN."""
    from unittest.mock import patch

    with patch(
        "workflows.pipe_gaps.outage_recovery.dit_bq.derived_source_into_experiment"
    ) as helper:
        helper.return_value = "proj.tech_great_expectations.dit_exp_exp01_view"
        out = mod._derive_synthetic_outage_view(
            _args(),
            source_messages_fqn="proj.ds.messages_snap",
            outage_start=date(2020, 12, 29),
            outage_end=date(2020, 12, 29),
        )

    assert out == "proj.tech_great_expectations.dit_exp_exp01_view"
    helper.assert_called_once()
    call = helper.call_args
    assert call.args[0] == "proj.ds.messages_snap"
    assert call.kwargs["experiment_id"] == "exp01"
    assert call.kwargs["role"] == "outage_filtered_20201229_20201229"
    assert call.kwargs["where_clause"] == (
        "DATE(timestamp) NOT BETWEEN '2020-12-29' AND '2020-12-29'"
    )
    assert call.kwargs["expiration_days"] == 7
    assert call.kwargs["project"] == "world-fishing-827"


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
                bq_input_messages="proj.ds.messages_unfiltered",
                bq_input_segments="proj.ds.segments",
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
    assert captured[0]["bq_input_messages"] == "proj.ds.messages_FILTERED"  # Stage 1
    assert captured[1]["bq_input_messages"] == "proj.ds.messages_FILTERED"  # Stage 2
    assert captured[2]["bq_input_messages"] == "proj.ds.messages_unfiltered"  # Stage 3
    # Segments are never filtered (known simplification).
    assert all(c["bq_input_segments"] == "proj.ds.segments" for c in captured)


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
                bq_input_messages="proj.ds.messages",
                bq_input_segments="proj.ds.segments",
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
    assert all(c["bq_input_messages"] == "proj.ds.messages" for c in captured)


def _captured_stage_configs(**execute_overrides: Any) -> list[dict]:
    """Run execute_outage_recovery with mocked pipeline calls and return
    the per-stage _make_config kwargs. Defaults mirror the workflow's
    default geometry (one-day outage on 2020-12-29)."""
    from unittest.mock import patch

    captured: list[dict] = []

    def fake_make_config(**kwargs):
        captured.append(kwargs)
        return object()

    kwargs: dict[str, Any] = dict(
        base_cfg=dict(
            bq_input_messages="proj.ds.messages",
            bq_input_segments="proj.ds.segments",
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
    # Stage 3: [outage_start - buffer, end] -> 12-28 .. 1-1.
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
    # outage -- AND reads the filtered view.
    assert captured[1]["start"] == date(2020, 12, 28)
    assert captured[1]["end"] == date(2021, 1, 1)
    assert captured[1]["bq_input_messages"] == "proj.ds.messages_FILTERED"
    # Stages 1+3 keep their schedule-skip ranges.
    assert captured[0]["start"] == date(2020, 1, 1)
    assert captured[0]["end"] == date(2020, 12, 29)
    assert captured[2]["start"] == date(2020, 12, 28)
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
                bq_input_messages="proj.ds.messages",
                bq_input_segments="proj.ds.segments",
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
# Snapshot naming helpers
# --------------------------------------------------------------------------

def test_outage_snapshot_dest_fqn_basic() -> None:
    name = mod._outage_snapshot_dest_fqn(
        experiment_id="exp01",
        source_table="proj.ds.research_messages",
        project="world-fishing-827",
    )
    assert name == (
        "world-fishing-827.tech_great_expectations."
        "dit_exp_exp01_outage_research_messages"
    )


def test_outage_snapshot_dest_fqn_sanitises_hyphens() -> None:
    # BQ identifiers can't contain hyphens; experiment-ids commonly do.
    name = mod._outage_snapshot_dest_fqn(
        experiment_id="my-exp-2026",
        source_table="proj.ds.research_messages",
        project="world-fishing-827",
    )
    assert "my_exp_2026" in name
    assert "my-exp-2026" not in name.split(".", 2)[2]


def test_outage_snapshot_dest_fqn_honours_dest_project() -> None:
    # Cross-org opt-in: when running against prod-VMS sources, the dest
    # project must match the source's (gfw-int-vms-v3) to avoid the
    # cross-org snapshot block. The helper forwards project into the FQN.
    name = mod._outage_snapshot_dest_fqn(
        experiment_id="exp01",
        source_table="proj.ds.research_messages",
        project="gfw-int-vms-v3",
    )
    assert name.startswith("gfw-int-vms-v3.tech_great_expectations.")


def test_outage_snapshot_dest_fqn_matches_snapshot_into_experiment() -> None:
    """Synchronisation pin.

    ``_outage_snapshot_dest_fqn`` is a PURE reimplementation of
    ``dit.bq.snapshot_into_experiment``'s naming convention -- it lets
    the ``--skip-snapshots`` path reconstruct snapshot FQNs without a BQ
    round-trip. If the upstream helper's naming ever shifts, the
    skip-snapshots path silently reads the wrong table (or fails to find
    one). Pin the two implementations to agree.

    Restored in this commit (Copilot review on PR #63 noted the M2-era
    sync test had been removed during the 3-stage refactor).
    """
    from unittest.mock import MagicMock, patch
    from dit.bq import snapshot_into_experiment

    # Mock the BQ client so the helper doesn't actually hit BQ -- we only
    # care about the dest FQN it constructs and returns.
    bq_client = MagicMock()
    bq_client.query.return_value.result.return_value = None
    with patch("google.cloud.bigquery.Client", return_value=bq_client):
        helper_dest = snapshot_into_experiment(
            "world-fishing-827.src_ds.research_messages",
            experiment_id="exp01",
            role=mod.SNAPSHOT_ROLE,  # mirrors what _snapshot_source_at passes
        )

    workflow_dest = mod._outage_snapshot_dest_fqn(
        experiment_id="exp01",
        source_table="world-fishing-827.src_ds.research_messages",
        project="world-fishing-827",
    )

    assert workflow_dest == helper_dest, (
        "_outage_snapshot_dest_fqn drifted from snapshot_into_experiment's "
        "naming. --skip-snapshots would reconstruct the wrong FQN."
    )


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


def test_default_snapshot_dest_project_matches_dit() -> None:
    # Default dest project is dit's; same-project snapshot works against
    # the staging default. Cross-project (e.g. against prod-VMS) requires
    # the user to explicitly pass --snapshot-dest-project.
    args = mod.parse_args(["--experiment-id", "test"])
    assert args.snapshot_dest_project == "world-fishing-827"


# --------------------------------------------------------------------------
# Today-relative pin-at default
# --------------------------------------------------------------------------

def test_default_pin_at_inside_time_travel_window() -> None:
    # Today-relative default: today UTC midnight minus 1 day. Must be
    # inside BQ's 7-day time-travel window so a default run always
    # succeeds against staging.
    args = mod.parse_args(["--experiment-id", "test"])
    pin = mod._parse_pin_at(args.pin_at)
    now = datetime.now(timezone.utc)
    assert (now - pin) < timedelta(days=7)
    assert pin <= now


def test_utc_floor_days_ago_is_midnight_utc() -> None:
    d = mod._utc_floor_days_ago(3)
    assert d.tzinfo == timezone.utc
    assert d.hour == 0 and d.minute == 0 and d.second == 0
    # Sanity: 3 days ago is between 2-4 days ago (give wall-clock slack).
    delta = datetime.now(timezone.utc) - d
    assert timedelta(days=2) < delta < timedelta(days=4)


# --------------------------------------------------------------------------
# Basename collision validation
# --------------------------------------------------------------------------

def test_validate_distinct_source_basenames_accepts_distinct() -> None:
    # Production layout: research_messages vs segs_activity.
    mod._validate_distinct_source_basenames(_args(
        source_messages="proj.ds.research_messages",
        source_segments="proj.ds.segs_activity",
    ))


def test_validate_distinct_source_basenames_rejects_collision() -> None:
    with pytest.raises(ValueError, match="identical basenames"):
        mod._validate_distinct_source_basenames(_args(
            source_messages="proj.a.messages_positions",
            source_segments="proj.b.messages_positions",
        ))


# --------------------------------------------------------------------------
# --snapshot-expiration-days validation
# --------------------------------------------------------------------------

@pytest.mark.parametrize("value", ["0", "-1", "-100"])
def test_parse_args_rejects_nonpositive_snapshot_expiration(value: str) -> None:
    with pytest.raises(SystemExit):
        mod.parse_args([
            "--pin-at", "2026-06-01 18:00:00 UTC",
            "--experiment-id", "test",
            "--snapshot-expiration-days", value,
        ])


def test_parse_args_accepts_positive_snapshot_expiration() -> None:
    args = mod.parse_args([
        "--pin-at", "2026-06-01 18:00:00 UTC",
        "--experiment-id", "test",
        "--snapshot-expiration-days", "30",
    ])
    assert args.snapshot_expiration_days == 30
