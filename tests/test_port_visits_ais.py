"""Tests for the M5b cache-integration layer in ``workflows/port_visits/ais.py``.

Mirrors ``tests/test_pipe_gaps_mode_equivalence.py``. No real pipe_anchorages
needed -- the workflow's docker-runner calls happen inside the patched
``execute_*`` functions, and the cache collaborators are monkeypatched on the
``dit.workflow`` module so no BQ / gcloud calls happen.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch

from dit import workflow as dit_workflow
from dit.workflow import RunContext
from workflows.port_visits import ais as mod


def _args(**overrides: Any) -> argparse.Namespace:
    """argparse-like namespace with the fields the cache layer reads."""
    base = dict(
        start="2020-01-01",
        end="2020-12-31",
        tail_days=3,
        source_dataset_stem="pipe_ais_test_202408290000",
        named_anchorages="world-fishing-827.anchorages.named_anchorages_v1",
        thinned_message_table=None,
        dest_dataset="tech_great_expectations",
        # Submitter image identity (Fix 1): build_from_source decides whether
        # the key folds the published image's digest or a build marker.
        build_from_source=False,
        image_tag="gfw/pipe-anchorages:v4.6.4",
        submitter_image_digest="gcr.io/foo/pipe-anchorages@sha256:5ub",
        # Per-`main()` context normally set inside main():
        run_id="rid01",
        experiment_id="exp01",
        commit_sha="abc1234",
        unreviewed=False,
        worker_image="gcr.io/foo/pipe-anchorages:v4.6.4",
        worker_image_digest="gcr.io/foo/pipe-anchorages@sha256:0011",
    )
    base.update(overrides)
    ns = argparse.Namespace(**base)
    ns.run_context = RunContext(
        pipeline_commit=ns.commit_sha,
        unreviewed=ns.unreviewed,
        pipeline_commit_parent=None,
        worker_image=ns.worker_image,
        worker_image_digest=ns.worker_image_digest,
        run_id=ns.run_id,
        dit_commit="ditsha0",
    )
    return ns


# --------------------------------------------------------------------------
# canonical_params_dict
# --------------------------------------------------------------------------

def test_canonical_params_dict_includes_mode():
    p = mod.canonical_params_dict(_args(), mod.MODE_BF)
    assert p["mode"] == mod.MODE_BF


def test_canonical_params_dict_changes_with_mode():
    a = mod.canonical_params_dict(_args(), mod.MODE_BF)
    b = mod.canonical_params_dict(_args(), mod.MODE_BFD)
    assert a != b


def test_canonical_params_dict_bf_excludes_tail():
    # MODE_BF runs a single big range; tail_days doesn't affect its output,
    # so it must not contribute to BF's cache key.
    p = mod.canonical_params_dict(_args(), mod.MODE_BF)
    assert "tail_days" not in p


def test_canonical_params_dict_bfd_includes_tail():
    p = mod.canonical_params_dict(_args(), mod.MODE_BFD)
    assert p["tail_days"] == 3


def test_canonical_params_dict_bftruncate_includes_tail():
    p = mod.canonical_params_dict(_args(), mod.MODE_BFTRUNCATE)
    assert p["tail_days"] == 3


def test_canonical_params_dict_bf_invariant_to_tail_change():
    a = mod.canonical_params_dict(_args(tail_days=3), mod.MODE_BF)
    b = mod.canonical_params_dict(_args(tail_days=10), mod.MODE_BF)
    assert a == b


def test_canonical_params_dict_includes_output_affecting_inputs():
    p = mod.canonical_params_dict(_args(), mod.MODE_BF)
    assert p["start"] == "2020-01-01"
    assert p["end"] == "2020-12-31"
    assert p["source_dataset_stem"] == "pipe_ais_test_202408290000"
    assert p["named_anchorages"] == "world-fishing-827.anchorages.named_anchorages_v1"
    assert p["thinned_message_table"] is None


def test_canonical_params_dict_thinned_table_changes_key():
    # --thinned-message-table changes what step 2 reads -> must be in the key.
    a = mod.canonical_params_dict(_args(thinned_message_table=None), mod.MODE_BF)
    b = mod.canonical_params_dict(
        _args(thinned_message_table="proj.ds.pre_thinned"), mod.MODE_BF
    )
    assert a != b


def test_canonical_params_dict_excludes_plumbing():
    args = _args()
    args.service_account = "ignored"
    args.dataflow_region = "ignored"
    args.bq_temp_dataset = "ignored"
    args.image_tag = "ignored"
    args.binding_name = "ignored"
    p = mod.canonical_params_dict(args, mod.MODE_BF)
    for plumbing_key in (
        "service_account", "dataflow_region", "bq_temp_dataset",
        "experiment_id", "run_id", "suffix", "image_tag", "worker_image",
        "dest_dataset", "binding_name",
    ):
        assert plumbing_key not in p, f"plumbing key {plumbing_key!r} leaked into params"


# --------------------------------------------------------------------------
# _build_cache_key
# --------------------------------------------------------------------------

def test_build_cache_key_uses_args_context():
    key = mod._build_cache_key(_args(), mod.MODE_BF)
    assert key.pipeline_commit == "abc1234"
    assert key.worker_image_digest == "gcr.io/foo/pipe-anchorages@sha256:0011"
    assert key.workflow_file_sha1 == mod.WORKFLOW_FILE_SHA1
    assert key.params["mode"] == mod.MODE_BF


# --------------------------------------------------------------------------
# _run_with_cache
# --------------------------------------------------------------------------

def _cached_row(**overrides: Any) -> dit_workflow.CachedRun:
    base = dict(
        run_id="prev-run",
        cache_key="prev-key",
        workflow=mod.WORKFLOW_NAME,
        pipeline=mod.PIPELINE_NAME,
        experiment_id="prev-exp",
        pipeline_commit="abc1234",
        unreviewed_code=False,
        dit_commit="ditsha0",
        workflow_file_sha1=mod.WORKFLOW_FILE_SHA1,
        worker_image="gcr.io/foo/pipe-anchorages:v4.6.4",
        params={"mode": mod.MODE_BF},
        output_tables=["proj.ds.cached_visits"],
        dataflow_job_ids=[],
        cloud_build_id=None,
        started_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        finished_at=datetime(2026, 5, 1, 0, 30, tzinfo=timezone.utc),
        status=dit_workflow.STATUS_SUCCEEDED,
        expires_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
    )
    base.update(overrides)
    return dit_workflow.CachedRun(**base)


def test_run_with_cache_hit_skips_execute_fn():
    execute_fn = MagicMock()
    with (
        patch.object(dit_workflow, "read_cache", return_value=_cached_row()),
        patch.object(dit_workflow, "verify_tables_exist", return_value=[True]),
        patch.object(dit_workflow, "write_cache") as mock_write,
    ):
        result = mod._run_with_cache(_args(), mod.MODE_BF, "exp01_abc_123456", execute_fn)
    execute_fn.assert_not_called()
    mock_write.assert_not_called()
    # Returns the CACHED FQN, not the fresh local one.
    assert result == "proj.ds.cached_visits"


def test_run_with_cache_miss_runs_and_writes():
    args = _args()
    suffix = "exp01_abc1234_aabbcc"
    execute_fn = MagicMock()
    with (
        patch.object(dit_workflow, "read_cache", return_value=None),
        patch.object(dit_workflow, "verify_tables_exist", return_value=[True]),
        patch.object(dit_workflow, "write_cache") as mock_write,
        patch.object(dit_workflow, "expires_at_for",
                     return_value=datetime(2026, 9, 1, tzinfo=timezone.utc)),
    ):
        result = mod._run_with_cache(args, mod.MODE_BF, suffix, execute_fn)
    # execute_fn is called with the workflow's (args, suffix) signature.
    execute_fn.assert_called_once_with(args=args, suffix=suffix)
    mock_write.assert_called_once()
    written = mock_write.call_args.args[0]
    assert written.pipeline == mod.PIPELINE_NAME
    assert written.workflow == mod.WORKFLOW_NAME
    assert written.run_id == "rid01"
    # Returns the FRESH visits-table FQN we just computed.
    assert result == mod._visits_table(args, suffix, mod.MODE_BF)
    # Records BOTH the visits table (comparison target, first) AND the per-mode
    # thinned port_events intermediate (Fix 4: cleanup target).
    assert written.output_tables == [
        result,
        mod._thinned_table(args, suffix, mod.MODE_BF),
    ]


def test_run_with_cache_stale_recomputes():
    execute_fn = MagicMock()
    with (
        patch.object(dit_workflow, "read_cache", return_value=_cached_row()),
        patch.object(dit_workflow, "verify_tables_exist", return_value=[False]),
        patch.object(dit_workflow, "write_cache") as mock_write,
        patch.object(dit_workflow, "expires_at_for",
                     return_value=datetime(2026, 9, 1, tzinfo=timezone.utc)),
    ):
        result = mod._run_with_cache(_args(), mod.MODE_BF, "exp01_abc_x", execute_fn)
    execute_fn.assert_called_once()
    mock_write.assert_called_once()
    assert result == mod._visits_table(_args(), "exp01_abc_x", mod.MODE_BF)


def test_run_with_cache_writes_unreviewed_rows():
    execute_fn = MagicMock()
    args = _args(unreviewed=True)
    with (
        patch.object(dit_workflow, "read_cache", return_value=None),
        patch.object(dit_workflow, "verify_tables_exist", return_value=[True]),
        patch.object(dit_workflow, "write_cache") as mock_write,
        patch.object(dit_workflow, "expires_at_for",
                     return_value=datetime(2026, 9, 1, tzinfo=timezone.utc)),
    ):
        mod._run_with_cache(args, mod.MODE_BF, "exp01_abc_x", execute_fn)
    written = mock_write.call_args.args[0]
    assert written.unreviewed_code is True


# --------------------------------------------------------------------------
# Fix 1 — submitter image identity in the cache key
# --------------------------------------------------------------------------

def test_canonical_params_dict_published_image_folds_submitter_digest():
    # build_from_source=False: the published submitter image's digest is folded
    # into the key (stamped on args in main()); not the raw build marker.
    p = mod.canonical_params_dict(
        _args(build_from_source=False,
              submitter_image_digest="gcr.io/x/pa@sha256:beef"),
        mod.MODE_BF,
    )
    assert p["submitter_image_digest"] == "gcr.io/x/pa@sha256:beef"
    assert "submitter_build_from_source" not in p


def test_canonical_params_dict_build_from_source_uses_marker_not_digest():
    # build_from_source=True: the submitter image is built from the worktree, so
    # its content == pipeline_commit (already in the CacheKey). A marker keeps a
    # build-from-source run distinct from a published-image run at same commit.
    p = mod.canonical_params_dict(_args(build_from_source=True), mod.MODE_BF)
    assert p["submitter_build_from_source"] is True
    assert "submitter_image_digest" not in p


def test_canonical_params_dict_different_published_submitter_images_differ():
    # SOUNDNESS: two runs, same commit + same worker image, but DIFFERENT
    # published submitter images must NOT share a cache key.
    a = mod.canonical_params_dict(
        _args(build_from_source=False,
              submitter_image_digest="gcr.io/x/pa@sha256:aaaa"),
        mod.MODE_BF,
    )
    b = mod.canonical_params_dict(
        _args(build_from_source=False,
              submitter_image_digest="gcr.io/x/pa@sha256:bbbb"),
        mod.MODE_BF,
    )
    assert a != b


def test_canonical_params_dict_published_vs_build_from_source_differ():
    # A published-image run and a build-from-source run differ in the key even
    # at the same commit (different artefacts).
    pub = mod.canonical_params_dict(
        _args(build_from_source=False,
              submitter_image_digest="gcr.io/x/pa@sha256:aaaa"),
        mod.MODE_BF,
    )
    bfs = mod.canonical_params_dict(_args(build_from_source=True), mod.MODE_BF)
    assert pub != bfs


def test_build_cache_key_differs_on_submitter_image():
    # The end-to-end CacheKey (not just params) changes with the submitter image.
    from dit.cache import compute_cache_key
    k1 = mod._build_cache_key(
        _args(build_from_source=False, submitter_image_digest="gcr.io/x/pa@sha256:aaaa"),
        mod.MODE_BF,
    )
    k2 = mod._build_cache_key(
        _args(build_from_source=False, submitter_image_digest="gcr.io/x/pa@sha256:bbbb"),
        mod.MODE_BF,
    )
    assert compute_cache_key(k1) != compute_cache_key(k2)


def test_submitter_image_identity_falls_back_to_tag_when_digest_unstamped():
    # If main()'s digest resolution failed it stamps the raw tag; if it never
    # ran (compare-only / unit paths), getattr falls back to args.image_tag so
    # the key is still tag-distinct rather than blowing up.
    args = _args(build_from_source=False)
    del args.submitter_image_digest  # simulate "never stamped"
    p = mod.canonical_params_dict(args, mod.MODE_BF)
    assert p["submitter_image_digest"] == args.image_tag


# --------------------------------------------------------------------------
# Fix 4 — port_events intermediate recorded for cleanup
# --------------------------------------------------------------------------

def test_run_with_cache_miss_records_visits_and_port_events():
    # A cache miss records BOTH the visits table (comparison, output_tables[0])
    # and the per-mode thinned port_events intermediate, so cancel_run can clean
    # up the intermediate that would otherwise be orphaned.
    args = _args()
    suffix = "exp01_abc1234_aabbcc"
    execute_fn = MagicMock()
    with (
        patch.object(dit_workflow, "read_cache", return_value=None),
        patch.object(dit_workflow, "verify_tables_exist", return_value=[True, True]),
        patch.object(dit_workflow, "write_cache") as mock_write,
        patch.object(dit_workflow, "expires_at_for",
                     return_value=datetime(2026, 9, 1, tzinfo=timezone.utc)),
    ):
        result = mod._run_with_cache(args, mod.MODE_BF, suffix, execute_fn)
    written = mock_write.call_args.args[0]
    visits = mod._visits_table(args, suffix, mod.MODE_BF)
    port_events = mod._thinned_table(args, suffix, mod.MODE_BF)
    assert result == visits  # comparison target unchanged
    assert written.output_tables[0] == visits  # visits FIRST (the hit returns [0])
    assert port_events in written.output_tables
    assert len(written.output_tables) == 2


def test_run_with_cache_miss_external_thinned_table_records_only_visits():
    # When --thinned-message-table is set, step 1 is skipped and the workflow
    # creates no thinned table of its own; only the visits table is recorded
    # (the external thinned table belongs to the caller, not us).
    args = _args(thinned_message_table="proj.ds.external_thinned")
    suffix = "exp01_abc1234_aabbcc"
    execute_fn = MagicMock()
    with (
        patch.object(dit_workflow, "read_cache", return_value=None),
        patch.object(dit_workflow, "verify_tables_exist", return_value=[True]),
        patch.object(dit_workflow, "write_cache") as mock_write,
        patch.object(dit_workflow, "expires_at_for",
                     return_value=datetime(2026, 9, 1, tzinfo=timezone.utc)),
    ):
        mod._run_with_cache(args, mod.MODE_BF, suffix, execute_fn)
    written = mock_write.call_args.args[0]
    assert written.output_tables == [mod._visits_table(args, suffix, mod.MODE_BF)]
