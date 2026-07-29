"""Tests for ``workflows/port_visits/cross_version_ais.py``.

Focused on M5 of the canonical-dataset migration:

- ``_CrossVersionSnapshotFQNs`` dataclass shape + frozen-ness.
- ``_snapshot_source`` invocation pattern against
  ``dit.bq.snapshot_into_experiment`` (correct role, expiration, project
  threaded; one call per source table; thinned table opt-in with the
  fail-fast ``if_existing="fail"`` semantic preserved).
- ``_ais_args_for_binding`` emits the M4 per-table FQN flags (not
  ``--source-dataset-stem``) and drops user-supplied per-table FQN
  overrides from extras (load-bearing for the cross-version pin --
  otherwise a user extra arg could leak an unpinned table into one
  binding).

The git-worktree path, the parallel-bindings ThreadPoolExecutor, the
diff phase, and the integration with the real ``ais.py`` subprocess are
exercised by live ``dit run`` invocations; not unit-tested here.
"""

from __future__ import annotations

import argparse
import dataclasses
from datetime import datetime, timezone
from typing import Any
from unittest.mock import patch

import pytest

from workflows.port_visits import cross_version_ais as mod


def _args(**overrides: Any) -> argparse.Namespace:
    base = dict(
        experiment_id="exp01",
        pin_source_at=datetime(2026, 5, 15, 10, 0, 0, tzinfo=timezone.utc),
        source_dataset_stem="pipe_ais_test_202408290000",
        snapshot_expiration_days=7,
        thinned_message_table=None,
    )
    base.update(overrides)
    return argparse.Namespace(**base)


# --------------------------------------------------------------------------
# _CrossVersionSnapshotFQNs dataclass
# --------------------------------------------------------------------------

def test_snapshot_fqns_is_frozen() -> None:
    """Frozen so callers can't accidentally mutate the FQNs between
    snapshot creation and `ais.py` invocation. If a future change tries
    to mutate one of the fields downstream, this fails fast."""
    fqns = mod._CrossVersionSnapshotFQNs(
        messages_positions="proj.ds.messages_positions_snap",
        segment_info="proj.ds.segment_info_snap",
        segs_activity="proj.ds.segs_activity_snap",
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        fqns.messages_positions = "other"  # type: ignore[misc]


def test_snapshot_fqns_thinned_defaults_to_none() -> None:
    fqns = mod._CrossVersionSnapshotFQNs(
        messages_positions="proj.ds.m",
        segment_info="proj.ds.si",
        segs_activity="proj.ds.sa",
    )
    assert fqns.thinned is None


# --------------------------------------------------------------------------
# _snapshot_source
# --------------------------------------------------------------------------

def test_snapshot_source_calls_helper_three_times_without_thinned() -> None:
    """Three source tables, three calls, role='cross_version' on each.
    No thinned table -> no fourth call. Returns dataclass with thinned=None."""
    with patch("workflows.port_visits.cross_version_ais.dit_bq.snapshot_into_experiment") as helper:
        # Return the source as a stand-in for the dest FQN; we only care
        # about the call pattern here, not the return value composition.
        helper.side_effect = lambda src, **kw: f"dest:{src}"
        fqns = mod._snapshot_source(_args())

    assert helper.call_count == 3
    sources_passed = [call.args[0] for call in helper.call_args_list]
    assert "pipe_ais_test_202408290000_internal.messages_positions" in sources_passed[0]
    assert "pipe_ais_test_202408290000_published.segment_info" in sources_passed[1]
    assert "pipe_ais_test_202408290000_published.segs_activity" in sources_passed[2]

    # All three calls use role="cross_version"; thinned not invoked.
    for call in helper.call_args_list:
        assert call.kwargs["role"] == "cross_version"
        assert call.kwargs["experiment_id"] == "exp01"
        assert call.kwargs["expiration_days"] == 7
        assert call.kwargs["project"] == mod.PROJECT
        # if_existing not passed -> helper default ("skip") applies.
        # The intentional idempotence trade-off (silent reuse on a new
        # --pin-source-at with the same --experiment-id) inherited from
        # M1's snapshot_into_experiment is documented on _snapshot_source.
        assert "if_existing" not in call.kwargs

    assert fqns.thinned is None


def test_snapshot_source_thinned_uses_distinct_role_and_fail_fast() -> None:
    """--thinned-message-table -> a FOURTH call with role='cross_version_thinned'
    AND `if_existing="fail"`. Distinct role prevents naming collision with
    the source-table snapshots; fail-fast preserves the prior contract
    (a re-run with the same --experiment-id but a different --pin-source-at
    raises rather than silently reading the prior snapshot)."""
    with patch("workflows.port_visits.cross_version_ais.dit_bq.snapshot_into_experiment") as helper:
        helper.side_effect = lambda src, **kw: f"dest:{src}"
        fqns = mod._snapshot_source(_args(
            thinned_message_table="proj.ds.user_supplied_thinned",
        ))

    assert helper.call_count == 4
    # First three are source-table snapshots (role="cross_version", default if_existing).
    for call in helper.call_args_list[:3]:
        assert call.kwargs["role"] == "cross_version"
        assert "if_existing" not in call.kwargs
    # Fourth is the thinned snapshot -- distinct role, explicit fail-fast.
    thinned_call = helper.call_args_list[3]
    assert thinned_call.args[0] == "proj.ds.user_supplied_thinned"
    assert thinned_call.kwargs["role"] == "cross_version_thinned"
    assert thinned_call.kwargs["if_existing"] == "fail"
    assert fqns.thinned == "dest:proj.ds.user_supplied_thinned"


def test_snapshot_source_returns_dataclass_with_helper_outputs() -> None:
    """Return value composes the helper's returned FQNs into the dataclass --
    each field is the helper's return value for the matching source."""
    expected = {
        "world-fishing-827.pipe_ais_test_202408290000_internal.messages_positions":
            "dest.messages_positions_snap",
        "world-fishing-827.pipe_ais_test_202408290000_published.segment_info":
            "dest.segment_info_snap",
        "world-fishing-827.pipe_ais_test_202408290000_published.segs_activity":
            "dest.segs_activity_snap",
    }
    with patch("workflows.port_visits.cross_version_ais.dit_bq.snapshot_into_experiment") as helper:
        helper.side_effect = lambda src, **kw: expected[src]
        fqns = mod._snapshot_source(_args())

    assert fqns.messages_positions == "dest.messages_positions_snap"
    assert fqns.segment_info == "dest.segment_info_snap"
    assert fqns.segs_activity == "dest.segs_activity_snap"


# --------------------------------------------------------------------------
# _ais_args_for_binding
# --------------------------------------------------------------------------

def _fqns() -> "mod._CrossVersionSnapshotFQNs":
    return mod._CrossVersionSnapshotFQNs(
        messages_positions="dst.m",
        segment_info="dst.si",
        segs_activity="dst.sa",
    )


def test_ais_args_for_binding_emits_per_table_fqn_flags_not_stem() -> None:
    """M5 routes ais.py at the canonical-dataset snapshots via M4's
    per-table FQN flags, NOT --source-dataset-stem (which can only address
    a stem-with-halves shape and can't reach tech_great_expectations)."""
    out = mod._ais_args_for_binding(
        [],
        snapshot_fqns=_fqns(),
        suffix="exp01-before",
        experiment_id="exp01",
        binding_name="before",
        modes=mod.AIS_SELECTABLE_MODES,
    )
    assert "--source-messages-fqn" in out
    assert "--source-segment-info-fqn" in out
    assert "--source-segs-activity-fqn" in out
    # Per-table FQN values land right after each flag.
    assert out[out.index("--source-messages-fqn") + 1] == "dst.m"
    assert out[out.index("--source-segment-info-fqn") + 1] == "dst.si"
    assert out[out.index("--source-segs-activity-fqn") + 1] == "dst.sa"
    # --source-dataset-stem is NOT emitted (stem-derivation doesn't reach
    # tech_great_expectations).
    assert "--source-dataset-stem" not in out


def test_ais_args_for_binding_drops_user_supplied_source_fqn_overrides() -> None:
    """Load-bearing for the cross-version pin: a user-supplied
    --source-messages-fqn in extras could leak an UNPINNED table into one
    binding while the others read the snapshot, defeating the point of the
    cross-version comparison. Drop them from extras; the wrapper-supplied
    snapshot FQNs are the only values that should reach ais.py."""
    user_extras = [
        "--source-messages-fqn", "proj.ds.user_provided_messages",
        "--source-segment-info-fqn", "proj.ds.user_provided_si",
        "--source-segs-activity-fqn", "proj.ds.user_provided_sa",
        "--runner", "dataflow",  # unrelated flag should survive
    ]
    out = mod._ais_args_for_binding(
        user_extras,
        snapshot_fqns=_fqns(),
        suffix="exp01-before",
        experiment_id="exp01",
        binding_name="before",
        modes=mod.AIS_SELECTABLE_MODES,
    )
    # No user-supplied value reaches ais.py.
    assert "proj.ds.user_provided_messages" not in out
    assert "proj.ds.user_provided_si" not in out
    assert "proj.ds.user_provided_sa" not in out
    # Unrelated flag survives.
    assert "--runner" in out and "dataflow" in out
    # Wrapper-supplied values land.
    assert "dst.m" in out and "dst.si" in out and "dst.sa" in out


def test_ais_args_for_binding_threads_thinned_when_present() -> None:
    fqns = mod._CrossVersionSnapshotFQNs(
        messages_positions="dst.m",
        segment_info="dst.si",
        segs_activity="dst.sa",
        thinned="dst.thinned_snap",
    )
    out = mod._ais_args_for_binding(
        [],
        snapshot_fqns=fqns,
        suffix="exp01-before",
        experiment_id="exp01",
        binding_name="before",
        modes=mod.AIS_SELECTABLE_MODES,
    )
    assert "--thinned-message-table" in out
    assert out[out.index("--thinned-message-table") + 1] == "dst.thinned_snap"


def test_ais_args_for_binding_omits_thinned_when_absent() -> None:
    out = mod._ais_args_for_binding(
        [],
        snapshot_fqns=_fqns(),  # thinned=None
        suffix="exp01-before",
        experiment_id="exp01",
        binding_name="before",
        modes=mod.AIS_SELECTABLE_MODES,
    )
    assert "--thinned-message-table" not in out


def test_ais_args_for_binding_drops_user_thinned_message_table() -> None:
    """Same load-bearing reason as the per-table FQN drops: the wrapper
    exclusively owns the --thinned-message-table flag (it snapshotted the
    table or knows there's no snapshot). User extras can never inject one."""
    out = mod._ais_args_for_binding(
        ["--thinned-message-table", "proj.ds.user_thinned"],
        snapshot_fqns=_fqns(),  # thinned=None
        suffix="exp01-before",
        experiment_id="exp01",
        binding_name="before",
        modes=mod.AIS_SELECTABLE_MODES,
    )
    assert "proj.ds.user_thinned" not in out
    assert "--thinned-message-table" not in out
