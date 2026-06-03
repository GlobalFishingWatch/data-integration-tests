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
from datetime import datetime, timezone
from typing import Any

import pytest

from workflows.pipe_gaps import outage_recovery as mod


def _args(**overrides: Any) -> argparse.Namespace:
    base = dict(
        experiment_id="exp01",
        start="2026-05-12",
        end="2026-05-26",
        offset_days=3,
        backfill_days=4,
        min_gap_length=1.0,
        n_hours_before=12,
        window_period_d=2,
        filter_good_seg="True",
        skip_open_gaps=False,
        # CLI form: empty string = no ssvid restriction (the default).
        ssvids="",
        source_messages="proj.ds.research_messages",
        source_segments="proj.ds.segs_activity",
        pre_outage_pin_at="2026-05-27 18:00:00 UTC",
        post_outage_pin_at="2026-06-01 18:00:00 UTC",
        snapshot_expiration_days=7,
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

def test_snapshot_dataset_name_distinct_pre_post() -> None:
    pre = mod._snapshot_dataset_name("exp01", mod.SNAPSHOT_LABEL_PRE)
    post = mod._snapshot_dataset_name("exp01", mod.SNAPSHOT_LABEL_POST)
    assert pre != post
    assert pre.endswith("_outage_pre")
    assert post.endswith("_outage_post")
    # Both share the project + experiment-id stem.
    assert pre.startswith(f"{mod.PROJECT}.dit_exp_exp01_")
    assert post.startswith(f"{mod.PROJECT}.dit_exp_exp01_")


def test_snapshot_dataset_name_sanitises_hyphens() -> None:
    # BQ dataset names can't contain hyphens; experiment-ids commonly do.
    name = mod._snapshot_dataset_name("my-exp-2026", mod.SNAPSHOT_LABEL_PRE)
    assert "-" not in name.split(".", 1)[1]
    assert "my_exp_2026" in name


def test_snapshot_table_names_distinct_basenames() -> None:
    msgs, segs = mod._snapshot_table_names(
        "proj.ds.research_messages", "proj.ds.segs_activity",
    )
    assert msgs == "research_messages"
    assert segs == "segs_activity"


def test_snapshot_table_names_disambiguates_basename_collision() -> None:
    # If both sources have the same basename (rare in production but
    # possible across datasets), the helper must disambiguate or the
    # snapshot dataset has a collision. Both _snapshot_source_at (which
    # creates) and the --skip-snapshots path (which reconstructs) MUST
    # agree on this mapping -- that's the whole reason this helper exists.
    msgs, segs = mod._snapshot_table_names(
        "proj.a.messages_positions", "proj.b.messages_positions",
    )
    assert msgs == "messages_messages_positions"
    assert segs == "segments_messages_positions"
    assert msgs != segs


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
