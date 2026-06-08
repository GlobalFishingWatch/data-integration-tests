"""Tests for ``workflows/pipe_gaps/outage_recovery.py``.

Focused on workflow-local helpers: pin-at validation, the post-vs-pre
cross-validation in ``parse_args``, ``canonical_params_dict`` cache-key
composition, and the snapshot-dataset naming. The 3-stage execute
functions, the snapshot creation, and the dataflow runner are exercised
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
        start="2024-08-22",
        end="2024-08-29",
        offset_days=3,
        backfill_days=4,
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
        pre_outage_pin_at="2026-05-27 18:00:00 UTC",
        post_outage_pin_at="2026-06-01 18:00:00 UTC",
        snapshot_expiration_days=7,
        snapshot_dest_project="world-fishing-827",
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
# parse_args cross-validation
# --------------------------------------------------------------------------

def test_parse_args_rejects_post_at_or_before_pre() -> None:
    with pytest.raises(SystemExit):
        mod.parse_args([
            "--pre-outage-pin-at", "2026-06-01 00:00:00 UTC",
            "--post-outage-pin-at", "2026-06-01 00:00:00 UTC",
            "--experiment-id", "test",
        ])


def test_parse_args_rejects_post_before_pre() -> None:
    with pytest.raises(SystemExit):
        mod.parse_args([
            "--pre-outage-pin-at", "2026-06-01 00:00:00 UTC",
            "--post-outage-pin-at", "2026-05-01 00:00:00 UTC",
            "--experiment-id", "test",
        ])


def test_parse_args_accepts_post_strictly_after_pre() -> None:
    args = mod.parse_args([
        "--pre-outage-pin-at", "2026-05-27 18:00:00 UTC",
        "--post-outage-pin-at", "2026-06-01 18:00:00 UTC",
        "--experiment-id", "test",
    ])
    assert args.pre_outage_pin_at.startswith("2026-05-27")
    assert args.post_outage_pin_at.startswith("2026-06-01")


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
        _args(post_outage_pin_at="2026-06-01 18:00:00 UTC"),
        mod.MODE_OUTAGE_ORACLE,
    )
    b = mod.canonical_params_dict(
        _args(post_outage_pin_at="2026-06-01T18:00:00+00:00"),
        mod.MODE_OUTAGE_ORACLE,
    )
    assert a["post_outage_pin_at"] == b["post_outage_pin_at"]


def test_canonical_params_includes_post_pin_for_both_modes() -> None:
    for mode in (mod.MODE_OUTAGE_RECOVERY, mod.MODE_OUTAGE_ORACLE):
        p = mod.canonical_params_dict(_args(), mode)
        assert p["post_outage_pin_at"] == "2026-06-01T18:00:00+00:00"


def test_canonical_params_includes_pre_pin_for_recovery_only() -> None:
    # The oracle is a single-shot backfill against post; including the
    # pre-outage pin in its cache key would invalidate it every time the
    # pre-outage pin moves, dropping the hit rate for no behavioural reason.
    rec = mod.canonical_params_dict(_args(), mod.MODE_OUTAGE_RECOVERY)
    ora = mod.canonical_params_dict(_args(), mod.MODE_OUTAGE_ORACLE)
    assert rec["pre_outage_pin_at"] == "2026-05-27T18:00:00+00:00"
    assert "pre_outage_pin_at" not in ora


def test_canonical_params_recovery_depends_on_offset_days() -> None:
    a = mod.canonical_params_dict(_args(offset_days=3), mod.MODE_OUTAGE_RECOVERY)
    b = mod.canonical_params_dict(_args(offset_days=7), mod.MODE_OUTAGE_RECOVERY)
    assert a["offset_days"] != b["offset_days"]


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

def test_outage_snapshot_dest_fqn_distinct_pre_post() -> None:
    pre = mod._outage_snapshot_dest_fqn(
        experiment_id="exp01", label=mod.SNAPSHOT_LABEL_PRE,
        source_table="world-fishing-827.ds.research_messages",
        project=mod.PROJECT,
    )
    post = mod._outage_snapshot_dest_fqn(
        experiment_id="exp01", label=mod.SNAPSHOT_LABEL_POST,
        source_table="world-fishing-827.ds.research_messages",
        project=mod.PROJECT,
    )
    assert pre != post
    # Canonical-dataset shape: <project>.tech_great_expectations.dit_exp_<exp>_outage_<label>_<source>
    assert pre.startswith(f"{mod.PROJECT}.tech_great_expectations.dit_exp_exp01_outage_pre_")
    assert post.startswith(f"{mod.PROJECT}.tech_great_expectations.dit_exp_exp01_outage_post_")
    # Source basename suffix preserved
    assert pre.endswith("_research_messages")
    assert post.endswith("_research_messages")


def test_outage_snapshot_dest_fqn_sanitises_hyphens_in_experiment_id() -> None:
    # BQ dataset names can't contain hyphens; experiment-ids commonly do.
    # The helper mirrors dit.bq.snapshot_into_experiment's - -> _ rule.
    fqn = mod._outage_snapshot_dest_fqn(
        experiment_id="my-exp-2026", label=mod.SNAPSHOT_LABEL_PRE,
        source_table="world-fishing-827.ds.research_messages",
        project=mod.PROJECT,
    )
    # No hyphen in the dit_exp_... portion (project itself can contain
    # hyphens, so check only the table-id portion after the second dot).
    table_id_part = fqn.rsplit(".", 1)[-1]
    assert "-" not in table_id_part
    assert "my_exp_2026" in fqn


def test_outage_snapshot_dest_fqn_honours_project() -> None:
    # Cross-org opt-in: when running against prod-VMS sources, the dest
    # project must match the source's (gfw-int-vms-v3) to avoid the
    # cross-org snapshot block.
    fqn = mod._outage_snapshot_dest_fqn(
        experiment_id="exp01", label=mod.SNAPSHOT_LABEL_PRE,
        source_table="gfw-int-vms-v3.pipe_vms_v3_internal.research_messages",
        project="gfw-int-vms-v3",
    )
    assert fqn.startswith("gfw-int-vms-v3.tech_great_expectations.dit_exp_exp01_outage_pre_")


def test_outage_snapshot_dest_fqn_matches_snapshot_into_experiment() -> None:
    """Synchronisation test: the local FQN reconstruction must agree with
    what ``dit.bq.snapshot_into_experiment`` would produce. Otherwise the
    ``--skip-snapshots`` path points at the wrong tables."""
    from dit import bq as dit_bq
    from unittest.mock import MagicMock, patch

    client = MagicMock()
    client.query.return_value.result.return_value = None
    with patch("google.cloud.bigquery.Client", return_value=client):
        canonical = dit_bq.snapshot_into_experiment(
            "world-fishing-827.ds.research_messages",
            experiment_id="exp01",
            role="outage_pre",
            project=mod.PROJECT,
        )
    local = mod._outage_snapshot_dest_fqn(
        experiment_id="exp01", label=mod.SNAPSHOT_LABEL_PRE,
        source_table="world-fishing-827.ds.research_messages",
        project=mod.PROJECT,
    )
    assert local == canonical, (
        f"reconstruction drift: local={local!r} canonical={canonical!r}"
    )


def test_validate_distinct_source_basenames_rejects_collision() -> None:
    """When --source-messages and --source-segments have the same basename,
    the canonical-dataset shape would produce a single dest table name and
    collide. ``_validate_distinct_source_basenames`` is called once in
    ``main()`` so both the create path AND the ``--skip-snapshots``
    reconstruction path inherit the protection.
    """
    args = argparse.Namespace(
        source_messages="world-fishing-827.a.messages_positions",
        source_segments="world-fishing-827.b.messages_positions",
    )
    with pytest.raises(ValueError, match="identical basenames"):
        mod._validate_distinct_source_basenames(args)


def test_validate_distinct_source_basenames_accepts_distinct() -> None:
    """The production layout (research_messages vs segs_activity) must
    pass — sanity check that the validator isn't over-eager."""
    args = argparse.Namespace(
        source_messages="world-fishing-827.ds.research_messages",
        source_segments="world-fishing-827.ds.segs_activity",
    )
    mod._validate_distinct_source_basenames(args)  # no raise


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
# Today-relative pin-at defaults
# --------------------------------------------------------------------------

def test_default_pin_at_inside_time_travel_window() -> None:
    # Today-relative defaults: pre = today UTC - 6d, post = today UTC - 1d.
    # Both must be inside BQ's 7-day time-travel window so a default run
    # always succeeds against staging.
    args = mod.parse_args(["--experiment-id", "test"])
    pre = mod._parse_pin_at(args.pre_outage_pin_at)
    post = mod._parse_pin_at(args.post_outage_pin_at)
    now = datetime.now(timezone.utc)
    assert (now - pre) < timedelta(days=7)
    assert (now - post) < timedelta(days=7)
    assert pre < post


def test_utc_floor_days_ago_is_midnight_utc() -> None:
    d = mod._utc_floor_days_ago(3)
    assert d.tzinfo == timezone.utc
    assert d.hour == 0 and d.minute == 0 and d.second == 0
    # Sanity: 3 days ago is between 2-4 days ago (give wall-clock slack).
    delta = datetime.now(timezone.utc) - d
    assert timedelta(days=2) < delta < timedelta(days=4)


# --------------------------------------------------------------------------
# --snapshot-expiration-days validation
# --------------------------------------------------------------------------

@pytest.mark.parametrize("value", ["0", "-1", "-100"])
def test_parse_args_rejects_nonpositive_snapshot_expiration(value: str) -> None:
    with pytest.raises(SystemExit):
        mod.parse_args([
            "--pre-outage-pin-at", "2026-05-27 18:00:00 UTC",
            "--post-outage-pin-at", "2026-06-01 18:00:00 UTC",
            "--experiment-id", "test",
            "--snapshot-expiration-days", value,
        ])


def test_parse_args_accepts_positive_snapshot_expiration() -> None:
    args = mod.parse_args([
        "--pre-outage-pin-at", "2026-05-27 18:00:00 UTC",
        "--post-outage-pin-at", "2026-06-01 18:00:00 UTC",
        "--experiment-id", "test",
        "--snapshot-expiration-days", "30",
    ])
    assert args.snapshot_expiration_days == 30
