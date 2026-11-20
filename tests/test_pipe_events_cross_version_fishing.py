"""Tests for ``workflows/pipe_events/cross_version_fishing.py``.

Focused on source pinning and what reaches each binding:

- ``_PinnedSources`` dataclass shape + frozen-ness. Seven tables; the
  intentional exclusion of ``spatial_measures_20201105`` is pinned
  (content-addressable version literal -- see the module docstring).
- ``parse_args``: the removed ``--pin-source-at`` and
  ``--snapshot-expiration-days`` are rejected; ``--start`` / ``--end`` /
  ``--tail-days`` / ``--reuse-pin`` / ``--pin-max-gb`` are validated before
  anything runs.
- ``_messages_window`` / ``_pin_specs``: ``research_messages`` windowed over
  every date any slice of any mode reads, the other six sources whole.
- ``_pin_sources`` / ``_pin``: one ``dit.bq.pin_source_into_experiment`` call
  per source under one pin stamp; with reuse only existence checks; with
  ``--dry-run`` nothing is created.
- ``_fishing_args_for_binding`` emits E4's per-table FQN flags and the
  pinned run shape, and drops every category of user-extras the wrapper
  owns -- load-bearing for the cross-version pin (a user extra could
  otherwise leak an unpinned table, or dates outside the copy, into one
  binding).
- Binding-name validation (regex-safe subset for BQ table suffixes).

No test touches BigQuery. The git-worktree path, the parallel-bindings
ThreadPoolExecutor, the diff phase, and the integration with the real
``fishing.py`` subprocess are exercised by live ``dit run`` invocations;
not unit-tested here.
"""

from __future__ import annotations

import argparse
import dataclasses
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from dit import bq as dit_bq
from workflows.pipe_events import cross_version_fishing as mod
from workflows.pipe_events import fishing
from workflows.pipe_events.fishing import (
    DEFAULT_INTERNAL_DS,
    DEFAULT_PIPE_REGIONS_LAYERS,
    DEFAULT_PIPE_STATIC,
    DEFAULT_PUBLISHED_DS,
)
from workflows.pipe_events.fishing import (
    MODES as FISHING_MODES,
)

_PINNED_AT = datetime(2026, 11, 19, 17, 15, 0, tzinfo=timezone.utc)
_BASE_ARGV = ["--experiment-id", "e", "--binding", "main=main"]

#: The six sources pipe-events reads without a date filter.
_WHOLE_SOURCES = {
    "segs_activity": f"{DEFAULT_PUBLISHED_DS}.segs_activity",
    "segment_vessel": f"{DEFAULT_INTERNAL_DS}.segment_vessel",
    "product_vessel_info_summary": f"{DEFAULT_PUBLISHED_DS}.product_vessel_info_summary",
    "identity_core": f"{DEFAULT_PUBLISHED_DS}.identity_core",
    "identity_authorization": f"{DEFAULT_PUBLISHED_DS}.identity_authorization",
    "event_regions": f"{DEFAULT_PIPE_REGIONS_LAYERS}.event_regions",
}
_MESSAGES_SOURCE = f"{DEFAULT_INTERNAL_DS}.research_messages"


def _args(**overrides: Any) -> argparse.Namespace:
    base = dict(
        experiment_id="exp01",
        start=date(2020, 1, 1),
        end=date(2021, 1, 1),
        tail_days=3,
        internal_ds=DEFAULT_INTERNAL_DS,
        published_ds=DEFAULT_PUBLISHED_DS,
        pipe_static=DEFAULT_PIPE_STATIC,
        pipe_regions_layers=DEFAULT_PIPE_REGIONS_LAYERS,
        reuse_pin=None,
        pin_expiration_days=7,
        pin_max_gb=1000.0,
        dry_run=False,
    )
    base.update(overrides)
    return argparse.Namespace(**base)


def _pinned(**overrides: Any) -> mod._PinnedSources:
    base = dict(
        start=date(2020, 1, 1),
        end=date(2021, 1, 1),
        tail_days=3,
        research_messages="dst.rm",
        segs_activity="dst.sa",
        segment_vessel="dst.sv",
        product_vessel_info_summary="dst.pvis",
        identity_core="dst.ic",
        identity_authorization="dst.ia",
        event_regions="dst.er",
    )
    base.update(overrides)
    return mod._PinnedSources(**base)


def _binding_args(
    extras: list[str], pinned: mod._PinnedSources | None = None, **overrides: Any,
) -> list[str]:
    kwargs = dict(
        pinned=pinned if pinned is not None else _pinned(),
        suffix="exp01-before",
        experiment_id="exp01",
        dest_dataset="tech_great_expectations",
        modes=FISHING_MODES,
    )
    kwargs.update(overrides)
    return mod._fishing_args_for_binding(extras, **kwargs)


# --------------------------------------------------------------------------
# _PinnedSources dataclass
# --------------------------------------------------------------------------

def test_pinned_sources_is_frozen() -> None:
    """Frozen so nothing changes the pins between pinning and the
    fishing.py invocations."""
    pinned = _pinned()
    with pytest.raises(dataclasses.FrozenInstanceError):
        pinned.research_messages = "other"  # type: ignore[misc]


def test_pinned_sources_hold_seven_tables_and_the_run_shape() -> None:
    """Guard against silent field addition/removal. If someone later adds
    a `spatial_measures` field on the mistaken assumption that ALL fishing
    inputs should be pinned, this fails and the reviewer notices the
    intentional exclusion. Symmetric guard against dropping one."""
    field_names = {f.name for f in dataclasses.fields(mod._PinnedSources)}
    assert field_names == {
        "start", "end", "tail_days",
        "research_messages",
        "segs_activity",
        "segment_vessel",
        "product_vessel_info_summary",
        "identity_core",
        "identity_authorization",
        "event_regions",
    }
    # spatial_measures is INTENTIONALLY absent.
    assert "spatial_measures" not in field_names


# --------------------------------------------------------------------------
# parse_args
# --------------------------------------------------------------------------

def test_pin_source_at_is_rejected_with_a_pointer_to_reuse_pin() -> None:
    """Rejected explicitly: otherwise parse_known_args would forward it to
    fishing.py's extras, which fails in every binding, after the copies."""
    with pytest.raises(SystemExit, match="--pin-source-at was removed.*--reuse-pin"):
        mod.parse_args([*_BASE_ARGV, "--pin-source-at", "2026-08-12T09:00:00Z"])


def test_snapshot_expiration_days_is_rejected_with_a_pointer_to_its_successor() -> None:
    with pytest.raises(SystemExit, match="--snapshot-expiration-days.*--pin-expiration-days"):
        mod.parse_args([*_BASE_ARGV, "--snapshot-expiration-days", "14"])


def test_removed_flags_leave_no_attribute_behind() -> None:
    args, _ = mod.parse_args(_BASE_ARGV)
    assert not hasattr(args, "pin_source_at")
    assert not hasattr(args, "snapshot_expiration_days")


def test_run_shape_defaults_to_fishing_defaults_and_is_not_forwarded_as_extras() -> None:
    """The wrapper owns --start / --end / --tail-days: they decide the
    window of the research_messages copy."""
    args, extras = mod.parse_args(_BASE_ARGV)
    assert args.start == date.fromisoformat(fishing.DEFAULT_START)
    assert args.end == date.fromisoformat(fishing.DEFAULT_END)
    assert args.tail_days == fishing.DEFAULT_TAIL_DAYS
    assert extras == []

    args, extras = mod.parse_args([
        *_BASE_ARGV, "--start", "2020-03-01", "--end=2020-04-01", "--tail-days", "5",
        "--parallel",
    ])
    assert (args.start, args.end, args.tail_days) == (date(2020, 3, 1), date(2020, 4, 1), 5)
    assert extras == ["--parallel"]  # an unrelated fishing.py flag passes through


def test_pin_flags_default_like_the_port_visits_wrapper() -> None:
    args, _ = mod.parse_args(_BASE_ARGV)
    assert args.reuse_pin is None
    assert args.pin_expiration_days == 7
    assert args.pin_max_gb == dit_bq.DEFAULT_PIN_MAX_BYTES / 1e9


@pytest.mark.parametrize(("extra", "message"), [
    (["--start", "2020-02-01", "--end", "2020-02-01"], "must be after --start"),
    (["--start", "2020-02-01", "--end", "2020-01-31"], "must be after --start"),
    (["--start", "01/02/2020"], "--start: invalid date"),
    (["--end", "soon"], "--end: invalid date"),
    (["--tail-days", "0"], "--tail-days must be >= 1"),
    (["--reuse-pin", "2026-11-19T17:15:00Z"], "--reuse-pin: invalid pin stamp"),
    (["--pin-max-gb", "0"], "--pin-max-gb must be > 0"),
    (["--pin-expiration-days", "0"], "--pin-expiration-days must be >= 1"),
])
def test_bad_run_shape_and_pin_flags_fail_before_anything_is_pinned(extra, message) -> None:
    with pytest.raises(SystemExit, match=message):
        mod.parse_args([*_BASE_ARGV, *extra])


# --------------------------------------------------------------------------
# _messages_window: every date any slice of any mode reads
# --------------------------------------------------------------------------

def _dates_fishing_reads(start: date, end: date, tail_days: int) -> tuple[date, date]:
    """The earliest date and the latest exclusive end that fishing.py's
    modes pass to pipe-events' step 1, padded as its SQL pads: recorded from
    fishing.py's own execute_* functions, not recomputed here."""
    slices: list[tuple[date, date]] = []

    def record(_args, *, slice_start, slice_end, **_kw) -> None:
        slices.append((slice_start, slice_end))

    run_args = argparse.Namespace(
        start=start.isoformat(), end=end.isoformat(), tail_days=tail_days,
    )
    with patch.object(fishing, "_run_slice", side_effect=record):
        fishing.execute_bf(run_args, "s")
        fishing.execute_bfd(run_args, "s")
        fishing.execute_bftruncate(run_args, "s")
    earliest_read = min(s for s, _ in slices) - timedelta(days=1)
    return earliest_read, max(e for _, e in slices)


@pytest.mark.parametrize(("start", "end", "tail_days"), [
    (date(2020, 1, 1), date(2021, 1, 1), 3),    # the staging default
    (date(2012, 1, 1), date(2025, 5, 4), 15),   # pipe3 async
    (date(2020, 6, 1), date(2020, 6, 8), 1),
    (date(2020, 6, 1), date(2020, 6, 8), 7),    # the tail is the whole run
    (date(2020, 6, 1), date(2020, 6, 3), 10),   # a tail longer than the run
])
def test_messages_window_covers_every_slice_of_every_mode(start, end, tail_days) -> None:
    window = mod._messages_window(start, end, tail_days)
    earliest_read, latest_end = _dates_fishing_reads(start, end, tail_days)

    assert window.column == "timestamp"
    assert window.start == earliest_read
    assert window.end == latest_end == end


def test_messages_window_starts_one_day_before_start() -> None:
    """Step 1 reads DATE(timestamp) >= start - 1 day, for the merge with the
    previous day's open events; the end is the run's exclusive end."""
    window = mod._messages_window(date(2020, 1, 1), date(2021, 1, 1), 3)
    assert window == dit_bq.SourceWindow(
        column="timestamp", start=date(2019, 12, 31), end=date(2021, 1, 1),
    )
    assert window.predicate() == (
        "DATE(timestamp) >= '2019-12-31' AND DATE(timestamp) < '2021-01-01'"
    )


def test_messages_window_does_not_depend_on_tail_days_within_the_run() -> None:
    """So one set of copies serves reruns with another --tail-days or
    --modes: the window is in the copy's name."""
    windows = {
        mod._messages_window(date(2020, 1, 1), date(2021, 1, 1), tail)
        for tail in (1, 3, 15, 366)
    }
    assert len(windows) == 1


# --------------------------------------------------------------------------
# _pin_specs
# --------------------------------------------------------------------------

def test_pin_specs_window_messages_and_copy_the_other_six_whole() -> None:
    specs = {s.field: s for s in mod._pin_specs(_args())}

    assert set(specs) == {"research_messages", *_WHOLE_SOURCES}
    messages = specs["research_messages"]
    assert messages.source_table == _MESSAGES_SOURCE
    assert messages.window == dit_bq.SourceWindow(
        column="timestamp", start=date(2019, 12, 31), end=date(2021, 1, 1),
    )
    for field, source in _WHOLE_SOURCES.items():
        assert specs[field].source_table == source
        assert specs[field].window is None, f"{field} is read without a date filter"


def test_pin_specs_fill_exactly_the_tables_of_pinned_sources() -> None:
    """Every spec names a _PinnedSources field and every table field has a
    spec, so _pin_sources can build the dataclass from the specs."""
    table_fields = {
        f.name for f in dataclasses.fields(mod._PinnedSources)
    } - {"start", "end", "tail_days"}
    assert {s.field for s in mod._pin_specs(_args())} == table_fields


def test_pin_specs_do_not_pin_spatial_measures() -> None:
    """Pin the intentional exclusion. The `_20201105` filename literal is a
    content-addressable version pin; copying a version-pinned static table
    buys nothing."""
    sources = [s.source_table for s in mod._pin_specs(_args())]
    assert len(sources) == 7  # not 8
    assert not any("spatial_measures" in s for s in sources)


def test_pin_specs_preserve_cross_org_project_in_source_fqns() -> None:
    """Regression pin for the pre-fix behavior where user-supplied
    ``--internal-ds gfw-int-vms-v3.some_internal`` was split on ``.`` and
    re-prefixed with ``PROJECT``, silently reading from
    ``world-fishing-827.some_internal.research_messages`` instead of the
    user's actual FQN. The wrapper uses the knobs verbatim, matching
    fishing.py's source resolvers. A copy only reads its source, so a source
    in another organisation works."""
    sources = [s.source_table for s in mod._pin_specs(_args(
        internal_ds="gfw-int-vms-v3.some_internal",
        published_ds="gfw-int-vms-v3.some_published",
        pipe_regions_layers="other-project.pipe_regions_layers",
    ))]
    assert "gfw-int-vms-v3.some_internal.research_messages" in sources
    assert "gfw-int-vms-v3.some_internal.segment_vessel" in sources
    assert "gfw-int-vms-v3.some_published.segs_activity" in sources
    assert "gfw-int-vms-v3.some_published.product_vessel_info_summary" in sources
    assert "gfw-int-vms-v3.some_published.identity_core" in sources
    assert "gfw-int-vms-v3.some_published.identity_authorization" in sources
    assert "other-project.pipe_regions_layers.event_regions" in sources
    # The wrapper's own PROJECT (world-fishing-827) must NOT leak into any
    # source FQN when the user passed a cross-org project.
    assert not any(s.startswith("world-fishing-827.") for s in sources)


def test_pin_specs_name_the_tables_fishing_would_read_unpinned() -> None:
    """The wrapper pins exactly what fishing.py resolves from the same
    dataset knobs, by construction and not by coincidence of defaults."""
    run_args = argparse.Namespace(
        internal_ds="p.internal", published_ds="p.published",
        pipe_static="p.static", pipe_regions_layers="p.regions",
        **{f"source_{name}_fqn": None for name in (
            "research_messages", "segs_activity", "segment_vessel",
            "product_vessel_info_summary", "identity_core", "identity_authorization",
            "spatial_measures", "event_regions",
        )},
    )
    specs = {s.field: s.source_table for s in mod._pin_specs(_args(
        internal_ds="p.internal", published_ds="p.published",
        pipe_regions_layers="p.regions",
    ))}
    assert specs == {
        "research_messages": fishing._research_messages_table(run_args),
        "segs_activity": fishing._segs_activity_table(run_args),
        "segment_vessel": fishing._segment_vessel_table(run_args),
        "product_vessel_info_summary": fishing._product_vessel_info_summary_table(run_args),
        "identity_core": fishing._identity_core_table(run_args),
        "identity_authorization": fishing._identity_authorization_table(run_args),
        "event_regions": fishing._event_regions_table(run_args),
    }


# --------------------------------------------------------------------------
# _pin_sources
# --------------------------------------------------------------------------

def test_pin_sources_copies_every_source_under_one_stamp() -> None:
    with patch.object(mod.dit_bq, "pin_source_into_experiment") as pin:
        pin.side_effect = lambda src, **kw: f"pin:{src.rsplit('.', 1)[-1]}"
        pinned = mod._pin_sources(
            _args(pin_max_gb=250.0, pin_expiration_days=14), _PINNED_AT, reuse=False,
        )

    assert pin.call_count == 7
    for call in pin.call_args_list:
        # Workflow-specific role, NOT the bare "cross_version" (which
        # port_visits/cross_version_ais.py uses): a shared basename like
        # segs_activity stays apart across the two workflows.
        assert call.kwargs["role"] == "cross_version_fishing"
        assert call.kwargs["pinned_at"] == _PINNED_AT
        assert call.kwargs["experiment_id"] == "exp01"
        assert call.kwargs["expiration_days"] == 14
        assert call.kwargs["max_bytes"] == 250 * 10**9
        assert call.kwargs["dry_run"] is False
        assert call.kwargs["project"] == mod.PROJECT
    windows = {call.args[0]: call.kwargs["window"] for call in pin.call_args_list}
    assert windows.pop(_MESSAGES_SOURCE) == dit_bq.SourceWindow(
        column="timestamp", start=date(2019, 12, 31), end=date(2021, 1, 1),
    )
    assert windows == dict.fromkeys(_WHOLE_SOURCES.values())
    assert pinned == mod._PinnedSources(
        start=date(2020, 1, 1),
        end=date(2021, 1, 1),
        tail_days=3,
        research_messages="pin:research_messages",
        segs_activity="pin:segs_activity",
        segment_vessel="pin:segment_vessel",
        product_vessel_info_summary="pin:product_vessel_info_summary",
        identity_core="pin:identity_core",
        identity_authorization="pin:identity_authorization",
        event_regions="pin:event_regions",
    )


def test_pin_sources_dry_run_reaches_the_helper() -> None:
    """--dry-run must not create copies: each one is only dry-run, which
    logs its estimate and checks it against the cap."""
    with patch.object(mod.dit_bq, "pin_source_into_experiment") as pin:
        pin.side_effect = lambda src, **kw: f"pin:{src}"
        mod._pin_sources(_args(dry_run=True, pin_max_gb=5.0), _PINNED_AT, reuse=False)

    assert pin.call_count == 7
    assert all(call.kwargs["dry_run"] is True for call in pin.call_args_list)
    assert all(call.kwargs["max_bytes"] == 5 * 10**9 for call in pin.call_args_list)


def test_pin_sources_reuse_only_checks_the_recomputed_names() -> None:
    with (
        patch.object(mod.dit_bq, "pin_source_into_experiment") as pin,
        patch.object(
            mod.dit_bq, "require_pinned_source", side_effect=lambda fqn, **kw: fqn,
        ) as require,
    ):
        pinned = mod._pin_sources(_args(), _PINNED_AT, reuse=True)

    pin.assert_not_called()
    assert require.call_count == 7
    assert pinned.research_messages == dit_bq.pinned_source_fqn(
        _MESSAGES_SOURCE,
        experiment_id="exp01",
        role="cross_version_fishing",
        pinned_at=_PINNED_AT,
        window=dit_bq.SourceWindow("timestamp", date(2019, 12, 31), date(2021, 1, 1)),
        project=mod.PROJECT,
    )
    assert pinned.research_messages.endswith(
        "dit_exp_exp01_cross_version_fishing_research_messages_"
        "20191231_20210101_20261119t171500z"
    )
    assert pinned.identity_core.endswith(
        "dit_exp_exp01_cross_version_fishing_identity_core_all_20261119t171500z"
    )


def test_pin_sources_reuse_fails_when_a_copy_is_missing() -> None:
    with patch.object(
        mod.dit_bq, "require_pinned_source",
        side_effect=ValueError("pinned copy x does not exist"),
    ):
        with pytest.raises(ValueError, match="does not exist"):
            mod._pin_sources(_args(), _PINNED_AT, reuse=True)


# --------------------------------------------------------------------------
# _pin: a new stamp at run start, or an earlier run's
# --------------------------------------------------------------------------

def test_pin_stamps_new_copies_with_the_run_start_in_whole_seconds(caplog) -> None:
    with (
        patch.object(mod, "_pin_sources", return_value="pinned") as pin_sources,
        caplog.at_level(logging.INFO, logger=mod.logger.name),
    ):
        before = datetime.now(timezone.utc).replace(microsecond=0)
        assert mod._pin(_args()) == "pinned"
        after = datetime.now(timezone.utc)

    (_, pinned_at), kwargs = pin_sources.call_args
    assert kwargs == {"reuse": False}
    assert before <= pinned_at <= after
    assert pinned_at.microsecond == 0 and pinned_at.tzinfo is not None
    assert f"--reuse-pin {dit_bq.format_pin_stamp(pinned_at)}" in caplog.text


def test_pin_with_reuse_pin_uses_that_stamp_and_copies_nothing() -> None:
    with patch.object(mod, "_pin_sources", return_value="pinned") as pin_sources:
        mod._pin(_args(reuse_pin="20261119T171500Z"))

    pin_sources.assert_called_once()
    assert pin_sources.call_args.args[1] == _PINNED_AT
    assert pin_sources.call_args.kwargs == {"reuse": True}


def test_pin_dry_run_does_not_advertise_a_stamp_to_reuse(caplog) -> None:
    """A dry run creates no copies, so there is nothing to reuse."""
    with (
        patch.object(mod, "_pin_sources", return_value="pinned"),
        caplog.at_level(logging.INFO, logger=mod.logger.name),
    ):
        mod._pin(_args(dry_run=True))

    assert "to rerun on these copies" not in caplog.text


def test_dry_run_through_the_real_helper_creates_nothing_and_enforces_the_cap() -> None:
    """--dry-run end to end below the wrapper, with only the BigQuery client
    mocked: seven dry-run statements, no copy job; and a copy above
    --pin-max-gb is refused."""
    client = MagicMock()
    client.query.return_value.total_bytes_processed = 4 * 10**9
    with patch("google.cloud.bigquery.Client", return_value=client):
        pinned = mod._pin_sources(_args(dry_run=True), _PINNED_AT, reuse=False)

        assert client.query.call_count == 7
        assert all(
            call.kwargs["job_config"].dry_run is True for call in client.query.call_args_list
        )
        assert pinned.event_regions.endswith("_event_regions_all_20261119t171500z")

        with pytest.raises(ValueError, match="4.0 GB, above the 1.0 GB cap"):
            mod._pin_sources(_args(dry_run=True, pin_max_gb=1.0), _PINNED_AT, reuse=False)


# --------------------------------------------------------------------------
# _fishing_args_for_binding
# --------------------------------------------------------------------------

def test_fishing_args_for_binding_emits_seven_per_table_fqn_flags() -> None:
    """fishing.py is routed at the pinned copies via E4's per-table FQN
    flags. All 7 pinned sources appear; the 8th (spatial_measures) is NOT
    emitted; dataset knobs are NOT emitted (stem-derivation can't reach
    tech_great_expectations)."""
    out = _binding_args([])
    for flag, expected in [
        ("--source-research-messages-fqn", "dst.rm"),
        ("--source-segs-activity-fqn", "dst.sa"),
        ("--source-segment-vessel-fqn", "dst.sv"),
        ("--source-product-vessel-info-summary-fqn", "dst.pvis"),
        ("--source-identity-core-fqn", "dst.ic"),
        ("--source-identity-authorization-fqn", "dst.ia"),
        ("--source-event-regions-fqn", "dst.er"),
    ]:
        assert flag in out, f"{flag} missing from fishing.py argv"
        assert out[out.index(flag) + 1] == expected, (
            f"{flag} did not receive its pinned copy"
        )
    # Dataset knobs are NOT emitted (can only address <stem>_internal /
    # <stem>_published shapes; can't reach tech_great_expectations).
    for absent in ("--internal-ds", "--published-ds",
                   "--pipe-static", "--pipe-regions-layers"):
        assert absent not in out, f"{absent} should NOT be emitted"


def test_fishing_args_for_binding_does_not_emit_spatial_measures_fqn() -> None:
    """spatial_measures is intentionally not pinned, so the wrapper also
    does not emit --source-spatial-measures-fqn. Every binding falls back
    to fishing.py's own default for that table."""
    assert "--source-spatial-measures-fqn" not in _binding_args([])


def test_fishing_args_for_binding_forwards_the_pinned_run_shape() -> None:
    """The research_messages copy holds only the dates this run shape
    reads, so every binding must run exactly it."""
    out = _binding_args([], _pinned(
        start=date(2020, 3, 1), end=date(2020, 4, 1), tail_days=5,
    ))
    assert out[out.index("--start") + 1] == "2020-03-01"
    assert out[out.index("--end") + 1] == "2020-04-01"
    assert out[out.index("--tail-days") + 1] == "5"


def test_fishing_args_for_binding_is_accepted_by_fishing_parse_args() -> None:
    """The emitted argv is fishing.py's own CLI: every flag exists there and
    resolves to the pinned copies and run shape."""
    run_args = fishing.parse_args(_binding_args(["--parallel"], modes=("1_bf", "2_bfd")))

    assert fishing._research_messages_table(run_args) == "dst.rm"
    assert fishing._segs_activity_table(run_args) == "dst.sa"
    assert fishing._segment_vessel_table(run_args) == "dst.sv"
    assert fishing._product_vessel_info_summary_table(run_args) == "dst.pvis"
    assert fishing._identity_core_table(run_args) == "dst.ic"
    assert fishing._identity_authorization_table(run_args) == "dst.ia"
    assert fishing._event_regions_table(run_args) == "dst.er"
    assert fishing._spatial_measures_table(run_args) == (
        f"{DEFAULT_PIPE_STATIC}.spatial_measures_20201105"
    )
    assert (run_args.start, run_args.end, run_args.tail_days) == (
        "2020-01-01", "2021-01-01", 3,
    )
    assert run_args.modes == ["1_bf", "2_bfd"]
    assert run_args.parallel is True


def test_fishing_args_for_binding_drops_user_supplied_source_fqn_overrides() -> None:
    """Load-bearing for the cross-version pin: a user-supplied
    --source-*-fqn in extras could leak an UNPINNED table into one binding
    while the others read the copy, defeating the point of the
    cross-version comparison. Drop all 8 per-table FQN flags from extras
    (including --source-spatial-measures-fqn, which the wrapper otherwise
    leaves to fishing.py's default -- forcing bindings to agree on it too)."""
    user_extras = [
        "--source-research-messages-fqn", "proj.ds.user_rm",
        "--source-segs-activity-fqn", "proj.ds.user_sa",
        "--source-segment-vessel-fqn=proj.ds.user_sv",
        "--source-product-vessel-info-summary-fqn", "proj.ds.user_pvis",
        "--source-identity-core-fqn", "proj.ds.user_ic",
        "--source-identity-authorization-fqn", "proj.ds.user_ia",
        "--source-spatial-measures-fqn", "proj.ds.user_sm",
        "--source-event-regions-fqn", "proj.ds.user_er",
        "-v",  # unrelated flag should survive
    ]
    out = _binding_args(user_extras)
    # No user-supplied value reaches fishing.py.
    assert not [token for token in out if "proj.ds.user_" in token]
    # Unrelated flag survives.
    assert "-v" in out
    # Wrapper-supplied pinned copies land.
    for expected in ("dst.rm", "dst.sa", "dst.sv", "dst.pvis",
                     "dst.ic", "dst.ia", "dst.er"):
        assert expected in out


def test_fishing_args_for_binding_drops_user_supplied_run_shape() -> None:
    """User dates or tail days could make one binding read dates the
    research_messages copy does not hold. parse_args consumes these flags,
    so they can only arrive from a direct caller; dropped all the same."""
    out = _binding_args([
        "--start=2019-01-01", "--end", "2021-06-30", "--tail-days", "30", "--parallel",
    ])
    for tainted in ("--start=2019-01-01", "2021-06-30", "30"):
        assert tainted not in out
    assert out.count("--start") == out.count("--end") == out.count("--tail-days") == 1
    assert out[out.index("--end") + 1] == "2021-01-01"
    assert "--parallel" in out


def test_fishing_args_for_binding_drops_user_supplied_dataset_knob_overrides() -> None:
    """Dataset knobs would leave startup logs misleading even if they
    couldn't reach the pinned copies. Also protects against a future
    change that adds a helper fallback path from the dataset knob into
    something reachable."""
    out = _binding_args([
        "--internal-ds", "proj.internal",
        "--published-ds", "proj.published",
        "--pipe-static", "proj.pipestatic",
        "--pipe-regions-layers", "proj.regions",
    ])
    for absent in ("--internal-ds", "--published-ds",
                   "--pipe-static", "--pipe-regions-layers",
                   "proj.internal", "proj.published",
                   "proj.pipestatic", "proj.regions"):
        assert absent not in out


def test_fishing_args_for_binding_drops_image_tag() -> None:
    """--image-tag pins a single container image across all bindings.
    For pipe-events (BQ-SQL-via-container) the image IS the pipeline code:
    pin it and both bindings run the SAME code, producing a
    guaranteed-empty diff that the wrapper cheerfully reports as
    IDENTICAL -- a confident false pass on the exact question this tool
    exists to answer. Per-binding image identity has to come from the
    worktree HEAD via ensure_pipeline_image; --image-tag would short-
    circuit that path.

    Same hazard as --build-from-source, different mechanism."""
    out = _binding_args(["--image-tag", "gfw/pipe-events:pinned"])
    assert "--image-tag" not in out
    assert "gfw/pipe-events:pinned" not in out


def test_fishing_args_for_binding_drops_build_from_source() -> None:
    """--build-from-source makes dit.runners.docker.run IGNORE image_tag
    and build from the compose file's mounted working tree. That would
    make every binding's container run the same code (whichever tree
    docker compose resolves to), defeating per-binding identity. Force
    the ensure_pipeline_image auto-build path instead."""
    out = _binding_args(["--build-from-source", "-v"])  # -v should survive
    assert "--build-from-source" not in out
    assert "-v" in out


def test_fishing_args_for_binding_re_injects_wrapper_owned_flags() -> None:
    """Wrapper-owned run identity, mode selection, and destination
    dataset flow through as --suffix, --modes, --experiment-id, and
    --dest-dataset on the child argv. --suffix wins for table names via
    fishing.py's _resolve_suffix; --experiment-id is threaded so both
    bindings' startup logs report the same value instead of each auto-
    generating a distinct solo_<hex>; --dest-dataset keeps the write
    side (fishing.py) and the diff side (_view_fqn) in sync so passing
    an explicit --dest-dataset to the wrapper doesn't silently point
    them at different datasets."""
    out = _binding_args(
        [], suffix="exp01-refactor", dest_dataset="scratch_christian_ttl120d",
        modes=("1_bf",),
    )
    assert out[out.index("--suffix") + 1] == "exp01-refactor"
    assert out[out.index("--modes") + 1] == "1_bf"
    assert out[out.index("--experiment-id") + 1] == "exp01"
    assert out[out.index("--dest-dataset") + 1] == "scratch_christian_ttl120d"


def test_fishing_args_for_binding_drops_user_supplied_wrapper_owned_flags() -> None:
    """User extras for --suffix, --modes, --experiment-id, or
    --dest-dataset could desync output-table names, modes-run/modes-diff,
    or write/diff dataset. Wrapper always wins on all four."""
    out = _binding_args(
        ["--suffix", "user_suffix", "--modes", "3_bftruncate",
         "--experiment-id", "user_experiment",
         "--dest-dataset", "user_dataset"],
        modes=("1_bf", "2_bfd"),
    )
    for tainted in ("user_suffix", "3_bftruncate", "user_experiment", "user_dataset"):
        assert tainted not in out
    assert out[out.index("--suffix") + 1] == "exp01-before"
    assert out[out.index("--modes") + 1] == "1_bf,2_bfd"
    assert out[out.index("--experiment-id") + 1] == "exp01"
    assert out[out.index("--dest-dataset") + 1] == "tech_great_expectations"


# --------------------------------------------------------------------------
# main: nothing but the pin and the worktrees with --dry-run
# --------------------------------------------------------------------------

def test_main_dry_run_pins_then_skips_bindings_and_diffs() -> None:
    """--dry-run: refs verified, copies dry-run, each binding's worktree set
    up with dry_run=True (no fishing.py), and no diff."""
    with (
        patch.object(mod, "_verify_refs") as verify,
        patch.object(mod, "_pin", return_value=_pinned()) as pin,
        patch.object(mod, "_run_binding", return_value=0) as run_binding,
        patch.object(mod, "_run_diffs") as run_diffs,
    ):
        rc = mod.main([
            "--experiment-id", "exp01", "--binding", "main=main",
            "--binding", "refactor=some/branch", "--dry-run", "--sequential-bindings",
        ])

    assert rc == 0
    verify.assert_called_once()
    pin.assert_called_once()
    assert pin.call_args.args[0].dry_run is True
    assert run_binding.call_count == 2
    for call in run_binding.call_args_list:
        assert call.kwargs["dry_run"] is True
        assert call.kwargs["pinned"] == _pinned()
    run_diffs.assert_not_called()


# --------------------------------------------------------------------------
# Binding-name validation
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "bad_name",
    [
        "refactor/branch",  # slash: would be an invalid BQ table suffix
        "Refactor",          # uppercase: not in [a-z0-9_-]
        "",                  # empty: caught earlier by _parse_binding, but rechecked
        "1234567890" * 4,    # too long (>32 chars)
        "-leading-hyphen",   # doesn't match ^[a-z0-9]
        "trailing.dot",      # dot is not in the allowed set
    ],
)
def test_validate_binding_names_rejects_bq_unsafe(bad_name: str) -> None:
    """Fail at parse time rather than deep into the run where the first
    BQ write would error opaquely. `_parse_binding` gets called before
    `_validate_binding_names`, so empty strings still raise via that path
    -- but validation is defense-in-depth."""
    if bad_name == "":
        # Empty name is caught by _parse_binding's `if not name` guard.
        with pytest.raises(SystemExit):
            mod._parse_binding(f"={bad_name or 'anyref'}")
        return
    with pytest.raises(SystemExit, match=r"--binding NAME must match"):
        mod._validate_binding_names([(bad_name, "main")])


def test_validate_binding_names_accepts_bq_safe() -> None:
    """Sanity: valid names don't raise."""
    mod._validate_binding_names([
        ("main", "main"),
        ("refactor", "refactor/branch"),
        ("before_2", "sha1abc"),
        ("v4-2-17", "v4.2.17"),
    ])


def test_validate_binding_names_rejects_duplicate_names() -> None:
    """suffix_by_binding is a dict keyed on name -- a duplicate binding
    name silently collapses to one suffix while args.bindings still has
    both entries, so _invoke runs twice against the same suffix
    (concurrently under the default --parallel). Result: two fishing.py
    runs writing the SAME output tables at once, then zero diff pairs,
    then a clean exit 0. Plausible typo path is
    `--binding main=main --binding main=refactor/...` when someone means
    `--binding refactor=...` for the second."""
    with pytest.raises(SystemExit, match=r"must be unique"):
        mod._validate_binding_names([
            ("main", "main"),
            ("main", "refactor/branch"),
        ])


def test_validate_binding_names_reports_all_dupes_sorted() -> None:
    """Error message names every duplicated binding, sorted, so a fix
    doesn't need iterative bisection."""
    with pytest.raises(SystemExit, match=r"a.*b"):
        mod._validate_binding_names([
            ("b", "r1"), ("a", "r2"), ("b", "r3"), ("a", "r4"),
        ])
