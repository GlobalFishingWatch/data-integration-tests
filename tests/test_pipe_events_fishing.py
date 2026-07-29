"""Tests for ``workflows/pipe_events/fishing.py``.

pipe-events caches NOTHING (no Dataflow worker image to digest), so unlike
``test_port_visits_ais.py`` there is no cache layer to monkeypatch. The docker
runner is patched on the workflow module so no real ``docker`` calls happen;
assertions are on the constructed ``dit_docker.run`` call vectors, the per-mode
date-slice arithmetic, the suffix/table naming, and the comparison contract.
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Any
from unittest.mock import patch

from dit import compare as dit_compare
from workflows.pipe_events import fishing as mod


def _args(**overrides: Any) -> argparse.Namespace:
    base = dict(
        start="2012-01-01",
        end="2013-01-01",
        tail_days=3,
        pipeline_project="world-fishing-827",
        internal_ds="world-fishing-827.pipe_ais_test_202408290000_internal",
        published_ds="world-fishing-827.pipe_ais_test_202408290000_published",
        pipe_static="world-fishing-827.pipe_static",
        pipe_regions_layers="world-fishing-827.pipe_regions_layers",
        image_tag="gfw/pipe-events",
        labels=mod._LABELS_JSON,
        build_from_source=False,
        dest_dataset="tech_great_expectations",
        commit_sha="abc1234",
        experiment_id="exp01",
        run_id="rid01",
    )
    base.update(overrides)
    return argparse.Namespace(**base)


# --------------------------------------------------------------------------
# Date helpers
# --------------------------------------------------------------------------

def test_date_shard_strips_hyphens():
    # Mirrors pipe-events parse.py reference_date.replace('-', '').
    assert mod._date_shard(date(2012, 12, 29)) == "20121229"
    assert mod._date_shard(date(2013, 1, 1)) == "20130101"


def test_daily_slices_exclusive_ends_match_bash():
    # bash loops current_day over the last tail_days days; each slice is
    # [current_day, current_day + 1). The returned exclusive ends are
    # [end - tail_days + 1 .. end].
    ends = mod._daily_slices(date(2013, 1, 1), 3)
    assert ends == [date(2012, 12, 30), date(2012, 12, 31), date(2013, 1, 1)]


def test_daily_slices_single_tail_day():
    ends = mod._daily_slices(date(2013, 1, 1), 1)
    assert ends == [date(2013, 1, 1)]


# --------------------------------------------------------------------------
# Per-mode date-slice arithmetic (assert the (start, end) windows per slice)
# --------------------------------------------------------------------------

def _slice_windows(execute_fn, args, suffix="exp01_abc1234_aabbcc"):
    """Run an execute_* with _run_slice patched; return the list of
    (mode, slice_start, slice_end, iteration, total) it requested."""
    calls: list[tuple] = []

    def fake_run_slice(args, *, mode, slice_start, slice_end, suffix, iteration, total_iterations):
        calls.append((mode, slice_start, slice_end, iteration, total_iterations))

    with patch.object(mod, "_run_slice", side_effect=fake_run_slice):
        execute_fn(args, suffix)
    return calls


def test_execute_bf_single_full_window():
    calls = _slice_windows(mod.execute_bf, _args())
    assert calls == [(mod.MODE_BF, date(2012, 1, 1), date(2013, 1, 1), 1, 1)]


def test_execute_bfd_backfill_then_daily_loop():
    calls = _slice_windows(mod.execute_bfd, _args())
    # initial: [2012-01-01, 2012-12-29); then 3 daily 1-day slices.
    assert calls[0] == (mod.MODE_BFD, date(2012, 1, 1), date(2012, 12, 29), 1, 4)
    assert calls[1] == (mod.MODE_BFD, date(2012, 12, 29), date(2012, 12, 30), 2, 4)
    assert calls[2] == (mod.MODE_BFD, date(2012, 12, 30), date(2012, 12, 31), 3, 4)
    assert calls[3] == (mod.MODE_BFD, date(2012, 12, 31), date(2013, 1, 1), 4, 4)
    assert len(calls) == 4


def test_execute_bftruncate_full_then_daily_loop():
    calls = _slice_windows(mod.execute_bftruncate, _args())
    # initial: full [2012-01-01, 2013-01-01); then the SAME 3 daily slices.
    assert calls[0] == (mod.MODE_BFTRUNCATE, date(2012, 1, 1), date(2013, 1, 1), 1, 4)
    assert calls[1] == (mod.MODE_BFTRUNCATE, date(2012, 12, 29), date(2012, 12, 30), 2, 4)
    assert calls[2] == (mod.MODE_BFTRUNCATE, date(2012, 12, 30), date(2012, 12, 31), 3, 4)
    assert calls[3] == (mod.MODE_BFTRUNCATE, date(2012, 12, 31), date(2013, 1, 1), 4, 4)
    assert len(calls) == 4


def test_bfd_and_bftruncate_daily_slices_are_identical():
    bfd = _slice_windows(mod.execute_bfd, _args())[1:]
    bft = _slice_windows(mod.execute_bftruncate, _args())[1:]
    # The truncate path re-runs the exact same daily slices as bfd.
    assert [c[1:3] for c in bfd] == [c[1:3] for c in bft]


# --------------------------------------------------------------------------
# The 4-step chain: dit_docker.run call shape
# --------------------------------------------------------------------------

def _run_slice_steps(args, *, mode=mod.MODE_BF, slice_start=date(2012, 1, 1),
                     slice_end=date(2013, 1, 1), suffix="exp01_abc1234_aabbcc"):
    """Run _run_slice with dit_docker.run patched; return the list of
    (args_list, kwargs) docker invocations."""
    captured: list[tuple] = []

    def fake_run(image_tag, step_args, **kwargs):
        captured.append((image_tag, step_args, kwargs))
        return 0

    with patch.object(mod.dit_docker, "run", side_effect=fake_run):
        mod._run_slice(args, mode=mode, slice_start=slice_start, slice_end=slice_end,
                       suffix=suffix, iteration=1, total_iterations=1)
    return captured


def test_run_slice_emits_six_docker_calls():
    # 2 (incremental) + 2 (filter) + 1 (auth) + 1 (restrictive).
    captured = _run_slice_steps(_args())
    assert len(captured) == 6


_OPERATIONS = {
    "incremental_events", "incremental_filter_events",
    "auth_and_regions_fishing_events", "fishing_restrictive",
}


def test_run_slice_operations_in_order():
    captured = _run_slice_steps(_args())
    # The operation is the single token from the known operation set.
    ops = [next(a for a in step_args if a in _OPERATIONS) for _, step_args, _ in captured]
    assert ops == [
        "incremental_events", "incremental_events",
        "incremental_filter_events", "incremental_filter_events",
        "auth_and_regions_fishing_events",
        "fishing_restrictive",
    ]


def test_run_slice_score_fields():
    captured = _run_slice_steps(_args())
    # incremental + filter steps each run once per score field.
    inc = [s for _, s, _ in captured if "incremental_events" in s]
    sfields = [s[s.index("-sfield") + 1] for s in inc]
    assert sfields == list(mod.SCORE_FIELDS)


def test_run_slice_docker_kwargs():
    captured = _run_slice_steps(_args(build_from_source=True))
    for image_tag, _step_args, kwargs in captured:
        assert image_tag == "gfw/pipe-events"
        assert kwargs["entrypoint"] == "pipe"
        assert kwargs["volumes"] == ["gcp:/root/.config"]
        assert kwargs["service"] == "pipeline"
        assert kwargs["build_from_source"] is True


def test_run_slice_auth_dest_and_rdate():
    # The auth step writes _fishing_events_v with -rdate = the slice's
    # exclusive end (the version-table date suffix is appended by the CLI).
    captured = _run_slice_steps(_args(), slice_end=date(2012, 12, 30))
    auth = next(s for _, s, _ in captured if "auth_and_regions_fishing_events" in s)
    assert "-dest" in auth
    dest = auth[auth.index("-dest") + 1]
    assert dest.endswith("_1_bf_fishing_events_v")  # version suffix added by CLI
    assert auth[auth.index("-dest_view") + 1].endswith("_1_bf_fishing_events")
    assert auth[auth.index("-rdate") + 1] == "2012-12-30"


def test_run_slice_filter_reads_merged_tables():
    captured = _run_slice_steps(_args())
    filt = [s for _, s, _ in captured if "incremental_filter_events" in s]
    mtbls = [s[s.index("-mtbl") + 1] for s in filt]
    assert any(t.endswith("_1_bf_nnet_score_merged") for t in mtbls)
    assert any(t.endswith("_1_bf_night_loitering_merged") for t in mtbls)


def test_run_slice_raises_on_nonzero_rc():
    def fake_run(image_tag, step_args, **kwargs):
        return 1

    with patch.object(mod.dit_docker, "run", side_effect=fake_run):
        try:
            mod._run_slice(_args(), mode=mod.MODE_BF, slice_start=date(2012, 1, 1),
                           slice_end=date(2013, 1, 1), suffix="s", iteration=1,
                           total_iterations=1)
        except SystemExit as e:
            assert "rc=1" in str(e)
        else:
            raise AssertionError("expected SystemExit on non-zero docker rc")


# --------------------------------------------------------------------------
# Table / prefix naming
# --------------------------------------------------------------------------

def test_mode_prefix():
    assert mod._mode_prefix("exp01_abc_123", mod.MODE_BF) == "exp01_abc_123_1_bf"


def test_fishing_events_view_fqn():
    fqn = mod._fishing_events_view(_args(), "exp01_abc_123", mod.MODE_BFD)
    assert fqn == "world-fishing-827.tech_great_expectations.exp01_abc_123_2_bfd_fishing_events"


def test_product_events_view_fqn():
    fqn = mod._product_events_view(_args(), "exp01_abc_123", mod.MODE_BFTRUNCATE)
    assert fqn == (
        "world-fishing-827.tech_great_expectations."
        "exp01_abc_123_3_bftruncate_product_events_fishing"
    )


def test_resolve_suffix_auto():
    s = mod._resolve_suffix(_args(suffix=None))
    assert s.startswith("exp01_abc1234_")
    # experiment_id _ commit _ 6-hex
    assert len(s.split("_")[-1]) == 6


def test_resolve_suffix_explicit_override():
    assert mod._resolve_suffix(_args(suffix="manual-prefix")) == "manual-prefix"


# --------------------------------------------------------------------------
# compare_all
# --------------------------------------------------------------------------

def test_compare_all_keys_and_view_suffix():
    captured: list[dict] = []

    def fake_compare(a, b, *, keys, view_suffix):
        captured.append(dict(a=a, b=b, keys=keys, view_suffix=view_suffix))
        return 0

    fqns = {m: f"proj.ds.{m}_view" for m in mod.MODES}
    with patch.object(dit_compare, "compare_tables", side_effect=fake_compare):
        rc = mod.compare_all(fqns, mod.MODES, label="fishing_events")
    assert rc == 0
    # 3 pairwise comparisons over the 3 modes.
    assert len(captured) == 3
    for c in captured:
        assert c["keys"] == ("event_id",)
        assert c["view_suffix"] == ""


def test_compare_all_nonzero_on_divergence():
    def fake_compare(a, b, *, keys, view_suffix):
        return 1  # every pair diverges

    fqns = {m: f"proj.ds.{m}_view" for m in mod.MODES}
    with patch.object(dit_compare, "compare_tables", side_effect=fake_compare):
        rc = mod.compare_all(fqns, mod.MODES, label="fishing_events")
    assert rc != 0


def test_compare_all_propagates_partial_divergence():
    # rc is 0 only if ALL pairs are identical; one diverging pair -> non-zero.
    seq = iter([0, 1, 0])

    def fake_compare(a, b, *, keys, view_suffix):
        return next(seq)

    fqns = {m: f"proj.ds.{m}_view" for m in mod.MODES}
    with patch.object(dit_compare, "compare_tables", side_effect=fake_compare):
        rc = mod.compare_all(fqns, mod.MODES, label="fishing_events")
    assert rc != 0


# --------------------------------------------------------------------------
# main() wiring (no real docker / git / BQ)
# --------------------------------------------------------------------------

def _patch_main(**ctx_overrides):
    from dit.workflow import RunContext
    ctx = RunContext(
        pipeline_commit=ctx_overrides.get("pipeline_commit", "abc1234"),
        unreviewed=ctx_overrides.get("unreviewed", False),
        pipeline_commit_parent=None,
        worker_image="gfw/pipe-events",
        worker_image_digest="gfw/pipe-events",
        run_id="rid01",
        dit_commit="ditsha0",
    )
    return ctx


def test_main_skip_pipelines_runs_only_comparisons():
    ctx = _patch_main()
    with (
        patch.object(mod, "resolve_run_context", return_value=ctx),
        patch.object(mod, "_run_slice") as mock_slice,
        patch.object(mod, "compare_all", return_value=0) as mock_compare,
    ):
        rc = mod.main(["--skip-pipelines", "--experiment-id", "exp01"])
    assert rc == 0
    mock_slice.assert_not_called()
    # compare_all called twice: fishing_events view + product_events view.
    assert mock_compare.call_count == 2


def test_main_skip_comparisons_returns_zero_without_compare():
    ctx = _patch_main()
    with (
        patch.object(mod, "resolve_run_context", return_value=ctx),
        patch.object(mod, "execute_bf"),
        patch.object(mod, "execute_bfd"),
        patch.object(mod, "execute_bftruncate"),
        patch.object(mod, "compare_all") as mock_compare,
    ):
        rc = mod.main(["--skip-comparisons", "--experiment-id", "exp01"])
    assert rc == 0
    mock_compare.assert_not_called()


def test_main_rejects_bad_tail_days():
    ctx = _patch_main()
    with patch.object(mod, "resolve_run_context", return_value=ctx):
        try:
            mod.main(["--tail-days", "0", "--experiment-id", "exp01"])
        except SystemExit as e:
            assert "tail-days" in str(e)
        else:
            raise AssertionError("expected SystemExit on --tail-days 0")


def test_main_rejects_end_before_start():
    ctx = _patch_main()
    with patch.object(mod, "resolve_run_context", return_value=ctx):
        try:
            mod.main(["--start", "2013-01-01", "--end", "2012-01-01",
                      "--experiment-id", "exp01"])
        except SystemExit as e:
            assert "end" in str(e).lower()
        else:
            raise AssertionError("expected SystemExit on end <= start")


def test_main_resolve_run_context_uses_docker_runner_no_digest(monkeypatch):
    monkeypatch.delenv("DIT_CLOUD_MODE", raising=False)
    ctx = _patch_main()
    with (
        patch.object(mod, "resolve_run_context", return_value=ctx) as mock_ctx,
        patch.object(mod, "execute_bf"),
        patch.object(mod, "execute_bfd"),
        patch.object(mod, "execute_bftruncate"),
        patch.object(mod, "compare_all", return_value=0),
    ):
        mod.main(["--experiment-id", "exp01"])
    kwargs = mock_ctx.call_args.kwargs
    assert kwargs["runner"] == "docker"
    assert kwargs["resolve_digest"] is False
    assert kwargs["pipeline_name"] == "pipe-events"


# --------------------------------------------------------------------------
# Image resolution wiring (symmetric with Beam consumers)
#
# After the symmetrisation: the workflow does nothing fancy with image_tag.
# It calls resolve_run_context, which calls ensure_pipeline_image (canonical
# default for reviewed, auto-built dit/pipe-events:dit-<commit> for unreviewed,
# explicit override unchanged), then unconditionally stamps args.image_tag
# from ctx.worker_image. These tests pin that stamping + verify the harness
# is called with the symmetric kwargs (no need_registry_image).
# --------------------------------------------------------------------------

def _captured_image_tag(monkeypatch, *, args=None, build_from_source=False,
                        ctx_worker_image="gcr.io/world-fishing-827/dit/pipe-events:dit-abc1234"):
    """Run main() with a captured args.image_tag from inside the
    pipeline-execution path. Returns (image_tag_seen, run_context_kwargs)."""
    ctx = _patch_main()
    # Override the worker_image so we can assert the harness's return value
    # is what gets stamped onto args.image_tag.
    ctx.worker_image = ctx_worker_image

    captured: dict = {}

    def fake_execute(args, suffix):
        # Snapshot args.image_tag the first time any execute_* sees it.
        captured.setdefault("image_tag", args.image_tag)

    base_args = ["--experiment-id", "exp01"]
    if build_from_source:
        base_args.append("--build-from-source")
    base_args.extend(args or [])

    with (
        patch.object(mod, "resolve_run_context", return_value=ctx) as mock_ctx,
        patch.object(mod, "execute_bf", side_effect=fake_execute),
        patch.object(mod, "execute_bfd", side_effect=fake_execute),
        patch.object(mod, "execute_bftruncate", side_effect=fake_execute),
        patch.object(mod, "compare_all", return_value=0),
    ):
        mod.main(base_args)
    return captured.get("image_tag"), mock_ctx.call_args.kwargs


def test_main_stamps_image_tag_from_ctx_worker_image(monkeypatch):
    """args.image_tag = ctx.worker_image, always. The harness owns image
    resolution (canonical default for reviewed; auto-built for unreviewed;
    explicit override unchanged). The workflow just stamps."""
    auto_built = "gcr.io/world-fishing-827/dit/pipe-events:dit-abc1234"
    image_tag, _ = _captured_image_tag(monkeypatch, ctx_worker_image=auto_built)
    assert image_tag == auto_built


def test_main_explicit_image_tag_override_flows_through(monkeypatch):
    """An explicit --image-tag is what the harness sees AND what gets stamped
    (harness passes through; workflow stamps unconditionally)."""
    custom = "gcr.io/world-fishing-827/dit/pipe-events:explicit-override"
    image_tag, ctx_kwargs = _captured_image_tag(
        monkeypatch, args=["--image-tag", custom], ctx_worker_image=custom,
    )
    assert ctx_kwargs["worker_image"] == custom
    assert image_tag == custom


def test_main_build_from_source_stamps_and_signals_harness_to_skip_auto_build(monkeypatch):
    """--build-from-source pins TWO behaviours that must travel together:
    (a) the workflow still stamps ``args.image_tag = ctx.worker_image`` (uniform
    across modes), and (b) ``build_from_source=True`` is threaded into
    ``resolve_run_context`` so the harness bypasses the kaniko auto-build path
    (the runner ignores image_tag in build-from-source mode, so the build
    would be wasted). Combined into one test so a future refactor can't drop
    the kwargs check while keeping the stamping assertion green."""
    canonical = "ctx-worker-img"
    image_tag, ctx_kwargs = _captured_image_tag(
        monkeypatch, build_from_source=True, ctx_worker_image=canonical,
    )
    assert image_tag == canonical
    assert ctx_kwargs["build_from_source"] is True


def test_main_no_build_from_source_passes_false_to_harness(monkeypatch):
    """Default path: build_from_source=False reaches the harness, so the
    auto-build path is governed by the normal unreviewed + default trigger."""
    _, ctx_kwargs = _captured_image_tag(monkeypatch, build_from_source=False)
    assert ctx_kwargs["build_from_source"] is False


def test_main_resolve_run_context_called_without_need_registry_image(monkeypatch):
    """need_registry_image was removed from the harness — the symmetric trigger
    (unreviewed + default) carries the auto-build for both consumers. Pin that
    the workflow doesn't try to pass the removed param."""
    _, ctx_kwargs = _captured_image_tag(monkeypatch)
    assert "need_registry_image" not in ctx_kwargs
