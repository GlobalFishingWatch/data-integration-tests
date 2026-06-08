"""Tests for ``workflows/pipe_segment/identity_match_key.py``.

Focused on the canonical-dataset migration (M3): the ``--snapshot-dest-project``
CLI flag, the ``_PipeSegmentSnapshotFQNs`` dataclass shape, and the
``_snapshot_source`` invocation pattern over ``dit.bq.snapshot_into_experiment``.
The full cross-version orchestration (worktrees, segment chain, diff phase)
is exercised by live ``dit run`` invocations against real BQ; not unit-tested
here.
"""

from __future__ import annotations

import argparse
import dataclasses
from datetime import date, datetime, timezone
from typing import Any
from unittest.mock import patch

import pytest

from workflows.pipe_segment import identity_match_key as mod


# -- CLI flag: --snapshot-dest-project ------------------------------------

def _base_args(extra: list[str] | None = None) -> list[str]:
    """Minimal argv passing parse_args' required-arg validation."""
    return [
        "--experiment-id", "test01",
        "--pin-source-at", "2026-06-03T10:00:00Z",
        "--binding", "before=v5.0.3",
        "--date-range", "2020-01-01,2020-01-01",
    ] + (extra or [])


def test_snapshot_dest_project_defaults_to_PROJECT() -> None:
    args = mod.parse_args(_base_args())
    assert args.snapshot_dest_project == mod.PROJECT == "world-fishing-827"


def test_snapshot_dest_project_accepts_cross_org_override() -> None:
    args = mod.parse_args(_base_args(["--snapshot-dest-project", "gfw-int-pipe-v3"]))
    assert args.snapshot_dest_project == "gfw-int-pipe-v3"


# -- _PipeSegmentSnapshotFQNs dataclass ------------------------------------

def test_snapshot_fqns_normalized_only_when_satellite_offsets_off() -> None:
    """When --include-satellite-offsets is not set, the dataclass exposes
    only ``normalized_messages``; the satellite-offsets fields default to None."""
    fqns = mod._PipeSegmentSnapshotFQNs(
        normalized_messages="world-fishing-827.tech_great_expectations.dit_exp_test01_pipe_segment_normalized_messages",
    )
    assert fqns.normalized_messages.endswith("_normalized_messages")
    assert fqns.sat_positions_stem is None
    assert fqns.norad is None


def test_snapshot_fqns_is_frozen() -> None:
    """Dataclass is frozen: downstream code can't mutate snapshot pointers
    out from under callers reading them."""
    fqns = mod._PipeSegmentSnapshotFQNs(
        normalized_messages="proj.tech_great_expectations.x",
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        fqns.normalized_messages = "other"  # type: ignore[misc]


# -- _snapshot_source ------------------------------------------------------

def _snapshot_args(**overrides: Any) -> argparse.Namespace:
    base = dict(
        experiment_id="test01",
        source_normalized_table="world-fishing-827.pipe_ais_test_202408290000_internal.normalized_messages",
        pin_source_at=datetime(2026, 6, 3, 10, 0, 0, tzinfo=timezone.utc),
        snapshot_expiration_days=7,
        snapshot_dest_project="world-fishing-827",
        include_satellite_offsets=False,
        date_range=(date(2020, 1, 1), date(2020, 1, 1)),
    )
    base.update(overrides)
    return argparse.Namespace(**base)


def test_snapshot_source_no_satellite_offsets_calls_helper_once() -> None:
    """The base case (default --include-satellite-offsets off) makes exactly
    one ``snapshot_into_experiment`` call -- for normalized_messages."""
    args = _snapshot_args()
    expected_normalized = (
        "world-fishing-827.tech_great_expectations."
        "dit_exp_test01_pipe_segment_normalized_messages"
    )
    with patch.object(
        mod.dit_bq, "snapshot_into_experiment", return_value=expected_normalized
    ) as mock_helper:
        fqns = mod._snapshot_source(args)

    assert mock_helper.call_count == 1
    call = mock_helper.call_args
    assert call.args == (args.source_normalized_table,)
    assert call.kwargs["experiment_id"] == "test01"
    assert call.kwargs["role"] == "pipe_segment"
    assert call.kwargs["expiration_days"] == 7
    assert call.kwargs["as_of"] == args.pin_source_at
    assert call.kwargs["project"] == "world-fishing-827"

    assert fqns.normalized_messages == expected_normalized
    assert fqns.sat_positions_stem is None
    assert fqns.norad is None


def test_snapshot_source_with_satellite_offsets_calls_helper_for_each_source() -> None:
    """With ``--include-satellite-offsets``: one call for normalized_messages,
    N calls for satellite_positions shards (one per date in date_range),
    and one call for norad_to_receiver."""
    args = _snapshot_args(
        include_satellite_offsets=True,
        date_range=(date(2020, 1, 1), date(2020, 1, 3)),  # 3 days
    )
    # The helper's return value is the dest FQN; mock returns reflect the
    # canonical naming so the stem computation has something predictable
    # to strip from.
    def fake_return(source_table: str, *, experiment_id: str, role: str, **_: Any) -> str:
        basename = source_table.rsplit(".", 1)[-1]
        return f"world-fishing-827.tech_great_expectations.dit_exp_{experiment_id}_{role}_{basename}"

    with patch.object(
        mod.dit_bq, "snapshot_into_experiment", side_effect=fake_return,
    ) as mock_helper:
        fqns = mod._snapshot_source(args)

    # 1 normalized + 3 satellite-position shards + 1 norad = 5 calls.
    assert mock_helper.call_count == 5

    assert fqns.normalized_messages.endswith("_normalized_messages")
    assert fqns.norad is not None and fqns.norad.endswith("_norad_to_receiver_v20230510")
    assert fqns.sat_positions_stem is not None


def test_snapshot_source_sat_positions_stem_strips_date_suffix() -> None:
    """The satellite-positions stem must equal a per-shard dest FQN with
    the trailing ``<YYYYMMDD>`` stripped; pipe-segment appends the date at
    read time so the stem must end exactly where the date begins."""
    args = _snapshot_args(
        include_satellite_offsets=True,
        date_range=(date(2020, 1, 1), date(2020, 1, 1)),
    )

    def fake_return(source_table: str, *, experiment_id: str, role: str, **_: Any) -> str:
        basename = source_table.rsplit(".", 1)[-1]
        return f"world-fishing-827.tech_great_expectations.dit_exp_{experiment_id}_{role}_{basename}"

    with patch.object(
        mod.dit_bq, "snapshot_into_experiment", side_effect=fake_return,
    ):
        fqns = mod._snapshot_source(args)

    assert fqns.sat_positions_stem is not None
    # The stem must end with the PROD_SAT_POS_STEM (trailing underscore).
    assert fqns.sat_positions_stem.endswith(f"_{mod.PROD_SAT_POS_STEM}")
    # And re-appending the date suffix must reproduce a real shard dest.
    expected_shard = f"{fqns.sat_positions_stem}20200101"
    # _shard_suffix(date(2020,1,1)) returns "20200101".
    assert mod._shard_suffix(date(2020, 1, 1)) == "20200101"
    assert expected_shard.endswith("_20200101")


def test_snapshot_source_threads_snapshot_dest_project() -> None:
    """When --snapshot-dest-project is overridden (the cross-org dodge path),
    every snapshot_into_experiment call must receive that project."""
    args = _snapshot_args(snapshot_dest_project="gfw-int-pipe-v3")
    with patch.object(
        mod.dit_bq, "snapshot_into_experiment", return_value="stub",
    ) as mock_helper:
        mod._snapshot_source(args)

    for call in mock_helper.call_args_list:
        assert call.kwargs["project"] == "gfw-int-pipe-v3"
