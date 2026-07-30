"""Tests for ``workflows/encounters/ais.py``.

Mock-based; no BQ, no Dataflow, no docker. Focused on the things that would
silently produce a wrong-but-green result:

* the prod-parity of each step's argv (wrong flag name / wrong date = a run
  that either fails late or tests the wrong thing);
* ``merge_encounters`` receiving the FULL-history start, not the slice start
  (getting this wrong would make the modes trivially agree);
* both output tables being compared, with the raw one present -- the merged
  sink alone is the weak signal;
* date/mode validation firing before any cloud call.
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Any
from unittest.mock import patch

import pytest

from workflows.encounters import ais as mod


def _args(**overrides: Any) -> argparse.Namespace:
    base = dict(
        runner="dataflow",
        source_messages_fqn="proj.ds_internal.messages_positions",
        source_segment_info_fqn="proj.ds_published.segment_info",
        source_segs_activity_fqn="proj.ds_published.segs_activity",
        spatial_measures_fqn="proj.pipe_static.spatial_measures_20201105",
        start="2020-01-01",
        end="2020-01-10",
        tail_days=3,
        max_encounter_dist_km=0.5,
        min_encounter_time_minutes=120.0,
        min_hours_between_encounters=4.0,
        ssvid_filter=None,
        image_tag="gfw/pipe-encounters",
        worker_image="reg/pipe-encounters:v4.4.0",
        build_from_source=False,
        suffix="sfx",
        experiment_id="exp01",
        dest_dataset="tech_great_expectations",
        service_account="sa@proj.iam.gserviceaccount.com",
        dataflow_region="us-central1",
        dataflow_temp_bucket="bucket",
        dataflow_subnetwork="regions/us-central1/subnetworks/net",
        parallel=False,
        run_id="abc123abc123",
        commit_sha="c0ffee1",
        modes=list(mod.SELECTABLE_MODES),
    )
    base.update(overrides)
    return argparse.Namespace(**base)


def _capture_slice(args, **kw) -> list[list[str]]:
    """Run one slice with the docker runner mocked; return the argv of each call."""
    calls: list[list[str]] = []

    def fake_run(image_tag, argv, **kwargs):
        calls.append(argv)
        return 0

    with patch.object(mod.dit_docker, "run", side_effect=fake_run):
        mod._run_slice(args, **kw)
    return calls


# --------------------------------------------------------------------------
# Step argv — prod parity
# --------------------------------------------------------------------------

def test_slice_runs_create_then_merge() -> None:
    calls = _capture_slice(
        _args(), mode=mod.MODE_BF, slice_start=date(2020, 1, 1),
        slice_end=date(2020, 1, 10), suffix="sfx", iteration=1, total_iterations=1,
    )
    assert len(calls) == 2
    assert calls[0][0] == "create_raw_encounters"
    assert calls[1][0] == "merge_encounters"


def test_create_step_argv_matches_prod_flags() -> None:
    """Flag names + tuning values mirror composer-dags-production
    gfw/pipes/v3/detect_encounters.py. A renamed flag fails late, in the
    container, after the image has been pulled."""
    argv = _capture_slice(
        _args(), mode=mod.MODE_BF, slice_start=date(2020, 1, 1),
        slice_end=date(2020, 1, 10), suffix="sfx", iteration=1, total_iterations=1,
    )[0]
    assert "--source_table=proj.ds_internal.messages_positions" in argv
    assert "--start_date=2020-01-01" in argv
    assert "--end_date=2020-01-10" in argv
    assert "--max_encounter_dist_km=0.5" in argv
    assert "--min_encounter_time_minutes=120.0" in argv
    assert any(a.startswith("--raw_table=") for a in argv)


def test_merge_step_argv_matches_prod_flags() -> None:
    argv = _capture_slice(
        _args(), mode=mod.MODE_BF, slice_start=date(2020, 1, 1),
        slice_end=date(2020, 1, 10), suffix="sfx", iteration=1, total_iterations=1,
    )[1]
    assert "--vessel_id_table=proj.ds_published.segment_info" in argv
    assert "--spatial_measures_table=proj.pipe_static.spatial_measures_20201105" in argv
    assert "--min_hours_between_encounters=4.0" in argv
    assert any(a.startswith("--sink_table=") for a in argv)
    # bad_segs is a SUBQUERY, not a table name (the pipeline accepts either).
    bad_segs = [a for a in argv if a.startswith("--bad_segs_table=")]
    assert len(bad_segs) == 1
    assert "SELECT DISTINCT seg_id" in bad_segs[0]
    assert "overlapping_and_short" in bad_segs[0]


def test_merge_gets_full_history_start_not_the_slice_start() -> None:
    """LOAD-BEARING. Prod runs merge from data_available_from_date every day;
    the workflow mirrors that with --start. If merge were given the slice
    start instead, each mode would merge a different window and the modes
    would stop being comparable at all."""
    calls = _capture_slice(
        _args(start="2020-01-01"), mode=mod.MODE_BFD,
        slice_start=date(2020, 1, 8), slice_end=date(2020, 1, 8),
        suffix="sfx", iteration=2, total_iterations=4,
    )
    create, merge = calls
    # create is scoped to the slice ...
    assert "--start_date=2020-01-08" in create
    # ... merge always starts at the pipeline start.
    assert "--start_date=2020-01-01" in merge
    assert "--end_date=2020-01-08" in merge


def test_both_steps_share_one_raw_table_per_mode() -> None:
    """create writes it, merge reads it -- a mismatch would silently merge an
    empty/foreign table."""
    create, merge = _capture_slice(
        _args(), mode=mod.MODE_BF, slice_start=date(2020, 1, 1),
        slice_end=date(2020, 1, 10), suffix="sfx", iteration=1, total_iterations=1,
    )
    raw_out = [a for a in create if a.startswith("--raw_table=")][0]
    raw_in = [a for a in merge if a.startswith("--raw_table=")][0]
    assert raw_out == raw_in
    assert "1_bf" in raw_out


def test_ssvid_filter_reaches_both_steps_when_set() -> None:
    """encounters supports --ssvid_filter on BOTH steps -- the cheapest way to
    shrink a run. Filtering only one step would make the two disagree."""
    calls = _capture_slice(
        _args(ssvid_filter="123,456"), mode=mod.MODE_BF,
        slice_start=date(2020, 1, 1), slice_end=date(2020, 1, 2),
        suffix="sfx", iteration=1, total_iterations=1,
    )
    for argv in calls:
        assert "--ssvid_filter=123,456" in argv


def test_ssvid_filter_omitted_when_unset() -> None:
    calls = _capture_slice(
        _args(ssvid_filter=None), mode=mod.MODE_BF,
        slice_start=date(2020, 1, 1), slice_end=date(2020, 1, 2),
        suffix="sfx", iteration=1, total_iterations=1,
    )
    for argv in calls:
        assert not any(a.startswith("--ssvid_filter") for a in argv)


def test_runner_config_passes_gcp_volume_and_entrypoint() -> None:
    """Auth is the gcp named volume, same as pipe-events / pipe-segment; cloud
    mode swaps it for --network=cloudbuild inside the runner."""
    seen: list[dict] = []

    def fake_run(image_tag, argv, **kwargs):
        seen.append(kwargs)
        return 0

    with patch.object(mod.dit_docker, "run", side_effect=fake_run):
        mod._run_slice(
            _args(), mode=mod.MODE_BF, slice_start=date(2020, 1, 1),
            slice_end=date(2020, 1, 2), suffix="sfx",
            iteration=1, total_iterations=1,
        )
    for kw in seen:
        assert kw["volumes"] == [mod.GCP_VOLUME]
        assert kw["entrypoint"] == mod.CLI_ENTRYPOINT
        # GOOGLE_CLOUD_PROJECT must reach INSIDE the container: Beam's
        # WriteToBigQuery builds its own BQ client in the SDK worker, which
        # ignores --project and reads the env. Without it the write dies at
        # TriggerLoadJobs -- but ONLY once there are rows to load, so a
        # zero-row run hides the bug. Found by the fifth laptop smoke.
        assert kw["container_env"] == {"GOOGLE_CLOUD_PROJECT": mod.PROJECT}


@pytest.mark.parametrize("runner", ["dataflow", "docker"])
def test_labels_emitted_on_every_runner(runner: str) -> None:
    """encounters' readers.py/writers.py do list_to_dict(cloud_opts.labels)
    with no None guard, and ReadSources is built on EVERY runner -- so omitting
    --labels raises TypeError before the pipeline starts. The first laptop
    smoke failed exactly this way on DirectRunner, because labels were
    initially emitted only on the Dataflow path. Parametrised so that
    regression can't come back on either runner. Contract item #6."""
    calls = _capture_slice(
        _args(runner=runner), mode=mod.MODE_BF,
        slice_start=date(2020, 1, 1), slice_end=date(2020, 1, 2),
        suffix="sfx", iteration=1, total_iterations=1,
    )
    for argv in calls:
        assert any(a.startswith("--labels=") for a in argv), argv
        assert "--labels=resource_creator=dit" in argv


def test_directrunner_mode_skips_dataflow_placement_options() -> None:
    calls = _capture_slice(
        _args(runner="docker"), mode=mod.MODE_BF,
        slice_start=date(2020, 1, 1), slice_end=date(2020, 1, 2),
        suffix="sfx", iteration=1, total_iterations=1,
    )
    for argv in calls:
        assert "--runner=DirectRunner" in argv
        # Placement knobs are Dataflow-only ...
        assert not any(a.startswith("--sdk_container_image") for a in argv)
        assert not any(a.startswith("--subnetwork") for a in argv)


@pytest.mark.parametrize("runner", ["dataflow", "docker"])
def test_project_and_temp_location_present_on_every_runner(runner: str) -> None:
    """... but --project and --temp_location are NOT Dataflow-only: the
    pipeline builds its own BQ client from cloud_opts.project, and
    ReadFromBigQuery's EXPORT read stages through GCS and raises
    "requires a GCS location" without --temp_location on ANY runner. Both were
    found by the first laptop smokes. --temp_location (settable) is distinct
    from --temp_dataset (not exposed by encounters -- the cloud blocker)."""
    calls = _capture_slice(
        _args(runner=runner), mode=mod.MODE_BF,
        slice_start=date(2020, 1, 1), slice_end=date(2020, 1, 2),
        suffix="sfx", iteration=1, total_iterations=1,
    )
    for argv in calls:
        assert f"--project={mod.PROJECT}" in argv
        assert any(a.startswith("--temp_location=gs://") for a in argv), argv


def test_nonzero_rc_aborts_the_slice() -> None:
    with patch.object(mod.dit_docker, "run", return_value=3):
        with pytest.raises(SystemExit) as exc:
            mod._run_slice(
                _args(), mode=mod.MODE_BF, slice_start=date(2020, 1, 1),
                slice_end=date(2020, 1, 2), suffix="sfx",
                iteration=1, total_iterations=1,
            )
    assert "create_raw_encounters failed" in str(exc.value)


# --------------------------------------------------------------------------
# Mode date arithmetic
# --------------------------------------------------------------------------

def _slices_for(execute_fn, args) -> list[tuple[str, str]]:
    seen: list[tuple[str, str]] = []

    def fake_slice(a, *, mode, slice_start, slice_end, suffix, iteration, total_iterations):
        seen.append((slice_start.isoformat(), slice_end.isoformat()))

    with patch.object(mod, "_run_slice", side_effect=fake_slice):
        execute_fn(args, "sfx")
    return seen


def test_bf_is_one_slice_over_the_whole_inclusive_range() -> None:
    assert _slices_for(mod.execute_bf, _args(start="2020-01-01", end="2020-01-10")) == [
        ("2020-01-01", "2020-01-10")
    ]


def test_bfd_backfills_short_then_walks_the_tail_to_end_inclusive() -> None:
    got = _slices_for(mod.execute_bfd, _args(start="2020-01-01", end="2020-01-10", tail_days=3))
    assert got[0] == ("2020-01-01", "2020-01-07")
    assert got[1:] == [("2020-01-08", "2020-01-08"),
                       ("2020-01-09", "2020-01-09"),
                       ("2020-01-10", "2020-01-10")]
    # --end is INCLUSIVE, so the final day must be covered.
    assert got[-1][1] == "2020-01-10"


def test_bftruncate_covers_full_range_then_rewalks_the_tail() -> None:
    got = _slices_for(
        mod.execute_bftruncate, _args(start="2020-01-01", end="2020-01-10", tail_days=3))
    assert got[0] == ("2020-01-01", "2020-01-10")
    assert got[1:] == [("2020-01-08", "2020-01-08"),
                       ("2020-01-09", "2020-01-09"),
                       ("2020-01-10", "2020-01-10")]


# --------------------------------------------------------------------------
# Comparison
# --------------------------------------------------------------------------

def _fqns(*modes: str) -> dict[str, str]:
    return {m: f"proj.ds.t_{m}" for m in modes}


def test_compare_all_compares_both_tables() -> None:
    """The raw table is the discriminating signal; the merged sink alone would
    be near-tautological because merge truncates and rebuilds every call."""
    raw = _fqns("1_bf", "2_bfd")
    merged = {m: v + "_merged" for m, v in raw.items()}
    with patch.object(mod.dit_compare, "compare_tables", return_value=0) as cmp:
        rc = mod.compare_all(raw, merged, ["1_bf", "2_bfd"])
    assert rc == 0
    # one pair x two tables
    assert cmp.call_count == 2
    compared = {c.args[0] for c in cmp.call_args_list}
    assert "proj.ds.t_1_bf" in compared          # raw
    assert "proj.ds.t_1_bf_merged" in compared   # merged


def test_compare_all_uses_encounter_id_and_no_view_suffix() -> None:
    raw = _fqns("1_bf", "2_bfd")
    with patch.object(mod.dit_compare, "compare_tables", return_value=0) as cmp:
        mod.compare_all(raw, dict(raw), ["1_bf", "2_bfd"])
    for c in cmp.call_args_list:
        assert c.kwargs["keys"] == ("encounter_id",)
        assert c.kwargs["view_suffix"] == ""


def test_compare_all_single_mode_compares_nothing() -> None:
    with patch.object(mod.dit_compare, "compare_tables") as cmp:
        rc = mod.compare_all(_fqns("1_bf"), _fqns("1_bf"), ["1_bf"])
    assert rc == 0
    cmp.assert_not_called()


def test_compare_all_propagates_divergence() -> None:
    raw = _fqns("1_bf", "2_bfd")
    with patch.object(mod.dit_compare, "compare_tables", return_value=5):
        assert mod.compare_all(raw, dict(raw), ["1_bf", "2_bfd"]) != 0


def test_compare_all_three_modes_gives_three_pairs_per_table() -> None:
    raw = _fqns("1_bf", "2_bfd", "3_bftruncate")
    with patch.object(mod.dit_compare, "compare_tables", return_value=0) as cmp:
        mod.compare_all(raw, dict(raw), list(mod.SELECTABLE_MODES))
    assert cmp.call_count == 6  # 3 pairs x 2 tables


# --------------------------------------------------------------------------
# CLI / validation
# --------------------------------------------------------------------------

def test_defaults_are_staging_not_prod() -> None:
    """Staging-by-default working agreement: no source default may resolve to a
    prod cohort. spatial_measures is the documented exception -- a static
    read-only prod reference table, as pipe-events also uses."""
    args = mod.parse_args([])
    assert "pipe_ais_test_202408290000" in args.source_messages_fqn
    assert "pipe_ais_test_202408290000" in args.source_segment_info_fqn
    assert "pipe_ais_test_202408290000" in args.source_segs_activity_fqn
    assert "pipe_ais_v3" not in args.source_messages_fqn
    assert "pipe_static" in args.spatial_measures_fqn


def test_default_dates_sit_in_the_cohorts_actual_data_window() -> None:
    """The cohort NAME carries the 2024 snapshot date; the DATA inside is 2020.
    Reading the name as a year is how a workflow ends up running cleanly
    against zero rows (see CLAUDE.md)."""
    args = mod.parse_args([])
    assert args.start.startswith("2020-")
    assert args.end.startswith("2020-")


def test_default_tuning_matches_prod() -> None:
    args = mod.parse_args([])
    assert args.max_encounter_dist_km == 0.5
    assert args.min_encounter_time_minutes == 120.0


def test_modes_default_and_subset() -> None:
    assert mod.parse_args([]).modes == list(mod.SELECTABLE_MODES)
    assert mod.parse_args(["--modes", "1_bf"]).modes == ["1_bf"]


def test_modes_rejects_unknown() -> None:
    with pytest.raises(SystemExit):
        mod.parse_args(["--modes", "nope"])


def test_main_rejects_inverted_date_range() -> None:
    with pytest.raises(SystemExit):
        mod.main(["--start", "2020-02-01", "--end", "2020-01-01"])


def test_main_rejects_tail_days_exceeding_the_window() -> None:
    """tail_days larger than the window would walk the daily tail back before
    --start, producing slices outside the range the oracle covers."""
    with pytest.raises(SystemExit):
        mod.main(["--start", "2020-01-01", "--end", "2020-01-03", "--tail-days", "10"])


def test_main_rejects_negative_tail_days() -> None:
    with pytest.raises(SystemExit):
        mod.main(["--tail-days", "-1"])
