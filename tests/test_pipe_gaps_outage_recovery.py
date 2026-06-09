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
from datetime import datetime, timedelta, timezone
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
