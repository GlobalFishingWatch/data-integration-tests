"""Tests for ``workflows/pipe_gaps/outage_recovery.py``.

Mirrors the style of ``test_pipe_gaps_mode_equivalence.py``: focused on
the workflow-local helpers (``canonical_params_dict``, the snapshot
validator, the post-vs-pre cross-validation in ``parse_args``). The
3-stage execute functions and the dataflow runner are exercised by the
mode_equivalence tests' equivalents and by the live ``dit run`` command;
they're not unit-tested here.
"""

from __future__ import annotations

import argparse
from typing import Any

import pytest

from workflows.pipe_gaps import outage_recovery as mod


def _args(**overrides: Any) -> argparse.Namespace:
    base = dict(
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
        pre_outage_snapshot="2026-05-27 18:00:00 UTC",
        post_outage_snapshot="2026-06-01 18:00:00 UTC",
    )
    base.update(overrides)
    return argparse.Namespace(**base)


# --------------------------------------------------------------------------
# _validate_snapshot
# --------------------------------------------------------------------------

@pytest.mark.parametrize("value", [
    "2026-05-27 18:00:00 UTC",
    "2026-05-27T18:00:00Z",
    "2026-05-27T18:00:00+00:00",
    "  2026-05-27 18:00:00 UTC  ",  # whitespace tolerated
])
def test_validate_snapshot_accepts_iso(value: str) -> None:
    assert mod._validate_snapshot(value).strip() == value.strip()


@pytest.mark.parametrize("value", ["", "not-a-date", "2026-13-99"])
def test_validate_snapshot_rejects_garbage(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        mod._validate_snapshot(value)


# --------------------------------------------------------------------------
# parse_args cross-validation
# --------------------------------------------------------------------------

def test_parse_args_rejects_post_at_or_before_pre() -> None:
    # post equal to pre is rejected (test reduces to bfd with identical source)
    with pytest.raises(SystemExit):
        mod.parse_args([
            "--pre-outage-snapshot", "2026-06-01 00:00:00 UTC",
            "--post-outage-snapshot", "2026-06-01 00:00:00 UTC",
            "--experiment-id", "test",
        ])


def test_parse_args_rejects_post_before_pre() -> None:
    with pytest.raises(SystemExit):
        mod.parse_args([
            "--pre-outage-snapshot", "2026-06-01 00:00:00 UTC",
            "--post-outage-snapshot", "2026-05-01 00:00:00 UTC",
            "--experiment-id", "test",
        ])


def test_parse_args_accepts_post_strictly_after_pre() -> None:
    args = mod.parse_args([
        "--pre-outage-snapshot", "2026-05-27 18:00:00 UTC",
        "--post-outage-snapshot", "2026-06-01 18:00:00 UTC",
        "--experiment-id", "test",
    ])
    assert args.pre_outage_snapshot.startswith("2026-05-27")
    assert args.post_outage_snapshot.startswith("2026-06-01")


# --------------------------------------------------------------------------
# canonical_params_dict
# --------------------------------------------------------------------------

def test_canonical_params_includes_mode() -> None:
    p = mod.canonical_params_dict(_args(), mod.MODE_OUTAGE_RECOVERY)
    assert p["mode"] == mod.MODE_OUTAGE_RECOVERY


def test_canonical_params_includes_post_snapshot_for_both_modes() -> None:
    # Both modes depend on the post-outage snapshot.
    for mode in (mod.MODE_OUTAGE_RECOVERY, mod.MODE_OUTAGE_ORACLE):
        p = mod.canonical_params_dict(_args(), mode)
        assert p["post_outage_snapshot"] == "2026-06-01 18:00:00 UTC"


def test_canonical_params_includes_pre_snapshot_for_recovery_only() -> None:
    # The oracle is a single-shot backfill that only reads post; including
    # the pre-outage snapshot in its cache key would invalidate it every
    # time the pre-outage snapshot moves, dropping the hit rate for no
    # behavioural reason.
    rec = mod.canonical_params_dict(_args(), mod.MODE_OUTAGE_RECOVERY)
    ora = mod.canonical_params_dict(_args(), mod.MODE_OUTAGE_ORACLE)
    assert rec["pre_outage_snapshot"] == "2026-05-27 18:00:00 UTC"
    assert "pre_outage_snapshot" not in ora


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
    # An empty CLI ssvids string (the default) should normalise to [], not
    # something falsy-but-string like "" -- so the cache key shape is stable
    # whether the user passed --ssvids '' or omitted it.
    p = mod.canonical_params_dict(_args(ssvids=""), mod.MODE_OUTAGE_RECOVERY)
    assert p["ssvids"] == []


def test_canonical_params_changes_with_ssvids() -> None:
    # An unrestricted run and a restricted run must produce different cache
    # keys -- otherwise a restricted-ssvid run could erroneously hit an
    # unrestricted cached table (or vice versa).
    a = mod.canonical_params_dict(_args(ssvids=""), mod.MODE_OUTAGE_RECOVERY)
    b = mod.canonical_params_dict(
        _args(ssvids="ssvid_a,ssvid_b"), mod.MODE_OUTAGE_RECOVERY,
    )
    assert a["ssvids"] != b["ssvids"]
    assert b["ssvids"] == ["ssvid_a", "ssvid_b"]


def test_canonical_params_ssvids_normalised_by_sort() -> None:
    # CLI order shouldn't affect the cache key. Two equivalent ssvid sets
    # presented in different orders must produce identical params.
    a = mod.canonical_params_dict(
        _args(ssvids="zeta,alpha,mike"), mod.MODE_OUTAGE_RECOVERY,
    )
    b = mod.canonical_params_dict(
        _args(ssvids="alpha,mike,zeta"), mod.MODE_OUTAGE_RECOVERY,
    )
    assert a["ssvids"] == b["ssvids"] == ["alpha", "mike", "zeta"]


def test_validate_snapshot_rejects_naive() -> None:
    # BQ FOR SYSTEM_TIME AS OF interprets naive timestamps against the
    # session zone, which would silently drift if run from non-UTC. Reject
    # at arg-parse time.
    with pytest.raises(argparse.ArgumentTypeError):
        mod._validate_snapshot("2026-05-27 18:00:00")
    with pytest.raises(argparse.ArgumentTypeError):
        mod._validate_snapshot("2026-05-27T18:00:00")
