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


def test_pin_specs_with_thinned_table_pin_it_instead_of_messages() -> None:
    """A thinned run skips step 1, the only reader of messages_positions, so
    copying it would only cost (a month of prod messages is ~3.6 TB)."""
    specs = {s.field: s for s in mod._pin_specs(_args(thinned_message_table="p.d.thinned"))}

    assert set(specs) == {"thinned", "segment_info", "segs_activity"}
    thinned = specs["thinned"]
    assert thinned.source_table == "p.d.thinned"
    assert thinned.role == "cross_version_thinned"
    assert thinned.window == dit_bq.SourceWindow(
        column="timestamp", start=date(2020, 1, 1), end=date(2021, 1, 1),
    )


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


def test_ais_args_for_binding_drops_user_thinned_message_table() -> None:
    """Same load-bearing reason as the per-table FQN drops: the wrapper
    exclusively owns the --thinned-message-table flag (it pinned the table
    or knows there is none). User extras can never inject one."""
    out = _binding_args(["--thinned-message-table", "proj.ds.user_thinned"])
    assert "proj.ds.user_thinned" not in out
    assert "--thinned-message-table" not in out
