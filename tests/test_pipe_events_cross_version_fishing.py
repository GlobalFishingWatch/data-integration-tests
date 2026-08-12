"""Tests for ``workflows/pipe_events/cross_version_fishing.py``.

Focused on E5 of the pipe-events cross-version arc:

- ``_CrossVersionSnapshotFQNs`` dataclass shape + frozen-ness. Exactly 7
  fields; the intentional exclusion of ``spatial_measures_20201105`` is
  pinned (content-addressable version literal -- see the module docstring).
- ``_snapshot_source`` invocation pattern against
  ``dit.bq.snapshot_into_experiment``: 7 calls, correct role/kwargs
  threaded; NO 8th call for spatial_measures.
- ``_fishing_args_for_binding`` emits E4's per-table FQN flags (not the
  dataset knobs) and drops every category of user-extras the wrapper owns
  -- load-bearing for the cross-version pin (a user extra could otherwise
  leak an unpinned table into one binding).
- Binding-name validation (regex-safe subset for BQ table suffixes).

The git-worktree path, the parallel-bindings ThreadPoolExecutor, the diff
phase, and the integration with the real ``fishing.py`` subprocess are
exercised by live ``dit run`` invocations; not unit-tested here.
"""

from __future__ import annotations

import argparse
import dataclasses
from datetime import datetime, timezone
from typing import Any
from unittest.mock import patch

import pytest

from workflows.pipe_events import cross_version_fishing as mod
from workflows.pipe_events.fishing import (
    DEFAULT_INTERNAL_DS,
    DEFAULT_PIPE_REGIONS_LAYERS,
    DEFAULT_PIPE_STATIC,
    DEFAULT_PUBLISHED_DS,
)
from workflows.pipe_events.fishing import (
    MODES as FISHING_MODES,
)


def _args(**overrides: Any) -> argparse.Namespace:
    base = dict(
        experiment_id="exp01",
        pin_source_at=datetime(2026, 8, 12, 10, 0, 0, tzinfo=timezone.utc),
        internal_ds=DEFAULT_INTERNAL_DS,
        published_ds=DEFAULT_PUBLISHED_DS,
        pipe_static=DEFAULT_PIPE_STATIC,
        pipe_regions_layers=DEFAULT_PIPE_REGIONS_LAYERS,
        snapshot_expiration_days=7,
    )
    base.update(overrides)
    return argparse.Namespace(**base)


# --------------------------------------------------------------------------
# _CrossVersionSnapshotFQNs dataclass
# --------------------------------------------------------------------------

def test_snapshot_fqns_is_frozen() -> None:
    """Frozen so callers can't accidentally mutate the FQNs between
    snapshot creation and fishing.py invocation."""
    fqns = _example_fqns()
    with pytest.raises(dataclasses.FrozenInstanceError):
        fqns.research_messages = "other"  # type: ignore[misc]


def test_snapshot_fqns_has_exactly_seven_fields() -> None:
    """Guard against silent field addition/removal. If someone later adds
    a `spatial_measures` field on the mistaken assumption that ALL fishing
    inputs should be snapshotted, this fails and the reviewer notices the
    intentional exclusion. Symmetric guard against dropping one."""
    field_names = {f.name for f in dataclasses.fields(mod._CrossVersionSnapshotFQNs)}
    assert field_names == {
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
# _snapshot_source
# --------------------------------------------------------------------------

def test_snapshot_source_calls_helper_seven_times() -> None:
    """Seven source tables, seven calls, role='cross_version' on each."""
    with patch(
        "workflows.pipe_events.cross_version_fishing.dit_bq.snapshot_into_experiment",
    ) as helper:
        helper.side_effect = lambda src, **kw: f"dest:{src}"
        mod._snapshot_source(_args())

    assert helper.call_count == 7

    sources = [call.args[0] for call in helper.call_args_list]
    assert any("research_messages" in s for s in sources)
    assert any(s.endswith(".segs_activity") for s in sources)
    assert any(s.endswith(".segment_vessel") for s in sources)
    assert any(s.endswith(".product_vessel_info_summary") for s in sources)
    assert any(s.endswith(".identity_core") for s in sources)
    assert any(s.endswith(".identity_authorization") for s in sources)
    assert any(s.endswith(".event_regions") for s in sources)

    for call in helper.call_args_list:
        # Workflow-specific role, NOT the bare "cross_version" (which
        # port_visits/cross_version_ais.py uses) -- pins that a shared
        # basename like segs_activity does not collide across the two
        # workflows for the same --experiment-id.
        assert call.kwargs["role"] == "cross_version_fishing"
        assert call.kwargs["experiment_id"] == "exp01"
        assert call.kwargs["expiration_days"] == 7
        assert call.kwargs["project"] == mod.PROJECT
        # if_existing not passed -> helper default ("skip") applies.
        # The intentional idempotence trade-off (silent reuse on a new
        # --pin-source-at with the same --experiment-id) inherited from
        # snapshot_into_experiment is documented on _snapshot_source.
        assert "if_existing" not in call.kwargs


def test_snapshot_source_does_not_snapshot_spatial_measures() -> None:
    """Pin the intentional exclusion. Only 7 calls; NONE reference
    `spatial_measures`. The `_20201105` filename literal is a content-
    addressable version pin; snapshotting a version-pinned static table
    buys nothing and clutters the canonical dataset."""
    with patch(
        "workflows.pipe_events.cross_version_fishing.dit_bq.snapshot_into_experiment",
    ) as helper:
        helper.side_effect = lambda src, **kw: f"dest:{src}"
        mod._snapshot_source(_args())

    sources = [call.args[0] for call in helper.call_args_list]
    assert not any("spatial_measures" in s for s in sources), (
        f"spatial_measures should NOT be snapshotted; got sources: {sources}"
    )
    assert helper.call_count == 7  # not 8


def test_snapshot_source_preserves_cross_org_project_in_source_fqns() -> None:
    """Regression pin for the pre-fix behavior where user-supplied
    ``--internal-ds gfw-int-vms-v3.some_internal`` was split on ``.`` and
    re-prefixed with ``PROJECT``, silently snapshotting from
    ``world-fishing-827.some_internal.research_messages`` instead of the
    user's actual FQN. Best case that produced a mid-run 404; worst case
    it snapshotted the wrong data from a same-named dataset in the
    default project. Now the wrapper uses the knobs verbatim, matching
    fishing.py's ``_run_slice``.

    Cross-org sources are already real (encounters/vms.py targets
    gfw-int-vms-v3) and snapshot_into_experiment carries a ``project``
    param for the write side."""
    with patch(
        "workflows.pipe_events.cross_version_fishing.dit_bq.snapshot_into_experiment",
    ) as helper:
        helper.side_effect = lambda src, **kw: f"dest:{src}"
        mod._snapshot_source(_args(
            internal_ds="gfw-int-vms-v3.some_internal",
            published_ds="gfw-int-vms-v3.some_published",
            pipe_regions_layers="other-project.pipe_regions_layers",
        ))
    sources = [call.args[0] for call in helper.call_args_list]
    # Every source snapshotted FROM the user-supplied cross-org project.
    assert "gfw-int-vms-v3.some_internal.research_messages" in sources
    assert "gfw-int-vms-v3.some_internal.segment_vessel" in sources
    assert "gfw-int-vms-v3.some_published.segs_activity" in sources
    assert "gfw-int-vms-v3.some_published.product_vessel_info_summary" in sources
    assert "gfw-int-vms-v3.some_published.identity_core" in sources
    assert "gfw-int-vms-v3.some_published.identity_authorization" in sources
    assert "other-project.pipe_regions_layers.event_regions" in sources
    # The wrapper's own PROJECT (world-fishing-827) must NOT leak into
    # any source FQN when the user passed a cross-org project.
    assert not any(s.startswith("world-fishing-827.") for s in sources), (
        f"world-fishing-827 leaked into cross-org sources: "
        f"{[s for s in sources if s.startswith('world-fishing-827.')]}"
    )


def test_snapshot_source_returns_dataclass_with_helper_outputs() -> None:
    """Return value composes the helper's returned FQNs into the dataclass --
    each field is the helper's return value for the matching source."""
    with patch(
        "workflows.pipe_events.cross_version_fishing.dit_bq.snapshot_into_experiment",
    ) as helper:
        # Return a marker per source so we can pin the composition.
        def _side(src, **_kw):
            return f"SNAP:{src.split('.')[-1]}"
        helper.side_effect = _side
        fqns = mod._snapshot_source(_args())

    assert fqns.research_messages == "SNAP:research_messages"
    assert fqns.segs_activity == "SNAP:segs_activity"
    assert fqns.segment_vessel == "SNAP:segment_vessel"
    assert fqns.product_vessel_info_summary == "SNAP:product_vessel_info_summary"
    assert fqns.identity_core == "SNAP:identity_core"
    assert fqns.identity_authorization == "SNAP:identity_authorization"
    assert fqns.event_regions == "SNAP:event_regions"


# --------------------------------------------------------------------------
# _fishing_args_for_binding
# --------------------------------------------------------------------------

def _example_fqns() -> mod._CrossVersionSnapshotFQNs:
    return mod._CrossVersionSnapshotFQNs(
        research_messages="dst.rm",
        segs_activity="dst.sa",
        segment_vessel="dst.sv",
        product_vessel_info_summary="dst.pvis",
        identity_core="dst.ic",
        identity_authorization="dst.ia",
        event_regions="dst.er",
    )


def test_fishing_args_for_binding_emits_seven_per_table_fqn_flags() -> None:
    """E5 routes fishing.py at the canonical-dataset snapshots via E4's
    per-table FQN flags. All 7 snapshotted sources appear; the 8th
    (spatial_measures) is NOT emitted; dataset knobs are NOT emitted
    (stem-derivation can't reach tech_great_expectations)."""
    out = mod._fishing_args_for_binding(
        [],
        snapshot_fqns=_example_fqns(),
        suffix="exp01-before",
        experiment_id="exp01",
        dest_dataset="tech_great_expectations",
        modes=FISHING_MODES,
    )
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
            f"{flag} did not receive its snapshot FQN"
        )
    # Dataset knobs are NOT emitted (can only address <stem>_internal /
    # <stem>_published shapes; can't reach tech_great_expectations).
    for absent in ("--internal-ds", "--published-ds",
                   "--pipe-static", "--pipe-regions-layers"):
        assert absent not in out, f"{absent} should NOT be emitted"


def test_fishing_args_for_binding_does_not_emit_spatial_measures_fqn() -> None:
    """spatial_measures is intentionally not snapshotted, so the wrapper
    also does not emit --source-spatial-measures-fqn. Every binding falls
    back to fishing.py's own default for that table."""
    out = mod._fishing_args_for_binding(
        [],
        snapshot_fqns=_example_fqns(),
        suffix="exp01-before",
        experiment_id="exp01",
        dest_dataset="tech_great_expectations",
        modes=FISHING_MODES,
    )
    assert "--source-spatial-measures-fqn" not in out


def test_fishing_args_for_binding_drops_user_supplied_source_fqn_overrides() -> None:
    """Load-bearing for the cross-version pin: a user-supplied
    --source-*-fqn in extras could leak an UNPINNED table into one binding
    while the others read the snapshot, defeating the point of the
    cross-version comparison. Drop all 8 per-table FQN flags from extras
    (including --source-spatial-measures-fqn, which the wrapper otherwise
    leaves to fishing.py's default -- forcing bindings to agree on it too)."""
    user_extras = [
        "--source-research-messages-fqn", "proj.ds.user_rm",
        "--source-segs-activity-fqn", "proj.ds.user_sa",
        "--source-segment-vessel-fqn", "proj.ds.user_sv",
        "--source-product-vessel-info-summary-fqn", "proj.ds.user_pvis",
        "--source-identity-core-fqn", "proj.ds.user_ic",
        "--source-identity-authorization-fqn", "proj.ds.user_ia",
        "--source-spatial-measures-fqn", "proj.ds.user_sm",
        "--source-event-regions-fqn", "proj.ds.user_er",
        "-v",  # unrelated flag should survive
    ]
    out = mod._fishing_args_for_binding(
        user_extras,
        snapshot_fqns=_example_fqns(),
        suffix="exp01-before",
        experiment_id="exp01",
        dest_dataset="tech_great_expectations",
        modes=FISHING_MODES,
    )
    # No user-supplied value reaches fishing.py.
    for tainted in ("proj.ds.user_rm", "proj.ds.user_sa", "proj.ds.user_sv",
                    "proj.ds.user_pvis", "proj.ds.user_ic", "proj.ds.user_ia",
                    "proj.ds.user_sm", "proj.ds.user_er"):
        assert tainted not in out, f"user extra {tainted!r} leaked into fishing.py argv"
    # Unrelated flag survives.
    assert "-v" in out
    # Wrapper-supplied snapshot values land.
    for expected in ("dst.rm", "dst.sa", "dst.sv", "dst.pvis",
                     "dst.ic", "dst.ia", "dst.er"):
        assert expected in out


def test_fishing_args_for_binding_drops_user_supplied_dataset_knob_overrides() -> None:
    """Dataset knobs would leave startup logs misleading even if they
    couldn't reach the canonical snapshots. Also protects against a future
    change that adds a helper fallback path from the dataset knob into
    something reachable."""
    user_extras = [
        "--internal-ds", "proj.internal",
        "--published-ds", "proj.published",
        "--pipe-static", "proj.pipestatic",
        "--pipe-regions-layers", "proj.regions",
    ]
    out = mod._fishing_args_for_binding(
        user_extras,
        snapshot_fqns=_example_fqns(),
        suffix="exp01-before",
        experiment_id="exp01",
        dest_dataset="tech_great_expectations",
        modes=FISHING_MODES,
    )
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
    out = mod._fishing_args_for_binding(
        ["--image-tag", "gfw/pipe-events:pinned"],
        snapshot_fqns=_example_fqns(),
        suffix="exp01-before",
        experiment_id="exp01",
        dest_dataset="tech_great_expectations",
        modes=FISHING_MODES,
    )
    assert "--image-tag" not in out
    assert "gfw/pipe-events:pinned" not in out


def test_fishing_args_for_binding_drops_build_from_source() -> None:
    """--build-from-source makes dit.runners.docker.run IGNORE image_tag
    and build from the compose file's mounted working tree. That would
    make every binding's container run the same code (whichever tree
    docker compose resolves to), defeating per-binding identity. Force
    the ensure_pipeline_image auto-build path instead."""
    out = mod._fishing_args_for_binding(
        ["--build-from-source", "-v"],  # -v is a fishing.py bare flag; should survive
        snapshot_fqns=_example_fqns(),
        suffix="exp01-before",
        experiment_id="exp01",
        dest_dataset="tech_great_expectations",
        modes=FISHING_MODES,
    )
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
    out = mod._fishing_args_for_binding(
        [],
        snapshot_fqns=_example_fqns(),
        suffix="exp01-refactor",
        experiment_id="exp01",
        dest_dataset="scratch_christian_ttl120d",
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
    out = mod._fishing_args_for_binding(
        ["--suffix", "user_suffix", "--modes", "3_bftruncate",
         "--experiment-id", "user_experiment",
         "--dest-dataset", "user_dataset"],
        snapshot_fqns=_example_fqns(),
        suffix="exp01-before",
        experiment_id="exp01",
        dest_dataset="tech_great_expectations",
        modes=("1_bf", "2_bfd"),
    )
    for tainted in ("user_suffix", "3_bftruncate", "user_experiment", "user_dataset"):
        assert tainted not in out
    assert out[out.index("--suffix") + 1] == "exp01-before"
    assert out[out.index("--modes") + 1] == "1_bf,2_bfd"
    assert out[out.index("--experiment-id") + 1] == "exp01"
    assert out[out.index("--dest-dataset") + 1] == "tech_great_expectations"


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
