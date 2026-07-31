"""Tests for workflows/encounters/vms.py.

vms.py owns almost nothing except its defaults -- the execution path is
ais.run(). So these tests concentrate on the two things that can actually go
wrong: the defaults drifting from what prod reads, and the two parsers drifting
apart such that ais.run() sees an attribute vms.py never set.
"""
from __future__ import annotations

from datetime import date
from unittest.mock import patch

import pytest

from workflows.encounters import ais
from workflows.encounters import vms


# --------------------------------------------------------------------------
# Defaults: pinned against composer-dags-production dags/core/vms/config.py
# --------------------------------------------------------------------------
def test_source_defaults_match_prod_config() -> None:
    """detect_encounters_config in dags/core/vms/config.py resolves
    source_messages -> gfw-int-vms-v3.pipe_vms_v3_internal.messages_positions,
    source_segment_info -> global-fishing-watch.pipe_vms_v4_published.segment_info.
    Pointing dit somewhere else silently tests a different pipeline."""
    a = vms.parse_args([])
    assert a.source_messages_fqn == (
        "gfw-int-vms-v3.pipe_vms_v3_internal.messages_positions"
    )
    assert a.source_segment_info_fqn == (
        "global-fishing-watch.pipe_vms_v4_published.segment_info"
    )
    assert a.source_segs_activity_fqn == (
        "gfw-int-vms-v3.pipe_vms_v3_internal.segs_activity"
    )
    assert a.spatial_measures_fqn.startswith(
        "gfw-int-vms-v3.pipe_vms_v3_internal.spatial_measures_clustered_vms_v"
    )


def test_sources_span_two_orgs_so_snapshotting_is_not_viable() -> None:
    """Messages sit in gfw-int-vms-v3 (org 115316357079) while dit's output
    project is world-fishing-827 (org 433637338589). Cross-org CREATE SNAPSHOT
    is refused by BQ, so nobody should bolt source pinning onto this workflow
    without first moving the destination. Pinned so the constraint is visible
    in the test suite, not just in a docstring."""
    a = vms.parse_args([])
    assert a.source_messages_fqn.split(".")[0] == "gfw-int-vms-v3"
    assert a.source_segment_info_fqn.split(".")[0] == "global-fishing-watch"


# --------------------------------------------------------------------------
# The prod-volume guard
# --------------------------------------------------------------------------
def test_default_window_is_narrow_because_sources_are_prod() -> None:
    """messages_positions is ~503 GB / 1.13 B rows. ais.py can afford a
    year-wide default against a staging cohort; this workflow cannot. If
    someone widens these defaults, that is a cost decision that should have to
    break a test first."""
    a = vms.parse_args([])
    span = (date.fromisoformat(a.end) - date.fromisoformat(a.start)).days
    assert 0 < span <= 31, f"default window is {span} days; prod sources demand narrow"


def test_default_window_lies_inside_the_available_data() -> None:
    """VMS data runs 2012-01 through 2026-07. A default window outside that
    would run cleanly against zero rows -- the failure mode that made an
    earlier encounters smoke look green while proving nothing."""
    a = vms.parse_args([])
    assert date(2012, 1, 1) <= date.fromisoformat(a.start)
    assert date.fromisoformat(a.end) <= date(2026, 7, 31)


def test_tail_days_fits_inside_the_default_window() -> None:
    """ais.run rejects tail_days > window. With a 7-day default window the
    default tail must fit, or the out-of-the-box daily-slice modes cannot run."""
    a = vms.parse_args([])
    span = (date.fromisoformat(a.end) - date.fromisoformat(a.start)).days
    assert a.tail_days <= span


# --------------------------------------------------------------------------
# Parser parity -- the drift guard for the shared ais.run()
# --------------------------------------------------------------------------
def test_parser_exposes_every_attribute_ais_run_consumes() -> None:
    """vms.py duplicates the CLI (to keep help text honest) but delegates
    execution to ais.run(). If the two parsers drift, ais.run() blows up on a
    missing attribute at runtime -- after image builds and possibly after
    Dataflow submission. Compare the parsed namespaces instead."""
    ais_attrs = set(vars(ais.parse_args([])))
    vms_attrs = set(vars(vms.parse_args([])))
    missing = ais_attrs - vms_attrs
    assert not missing, f"vms.py parser is missing: {sorted(missing)}"


def test_shared_tuning_defaults_are_not_forked() -> None:
    """Detection tuning is a property of the pipeline, not of the source, so
    vms.py reads it off ais.py rather than restating it."""
    a, v = ais.parse_args([]), vms.parse_args([])
    assert v.max_encounter_dist_km == a.max_encounter_dist_km
    assert v.min_encounter_time_minutes == a.min_encounter_time_minutes
    assert v.min_hours_between_encounters == a.min_hours_between_encounters


def test_modes_default_to_all_and_reject_typos() -> None:
    assert vms.parse_args([]).modes == list(ais.SELECTABLE_MODES)
    assert vms.parse_args(["--modes", "1_bf"]).modes == ["1_bf"]
    with pytest.raises(SystemExit):
        vms.parse_args(["--modes", "1_bff"])


def test_main_delegates_to_shared_run() -> None:
    """The whole point of the module: no forked execution path."""
    with patch.object(ais, "run", return_value=0) as run:
        assert vms.main(["--modes", "1_bf"]) == 0
    assert run.call_count == 1
    passed = run.call_args.args[0]
    assert passed.source_messages_fqn.startswith("gfw-int-vms-v3.")


def test_worker_cap_defaults_to_prod_ceiling() -> None:
    """An all-vessel, year-wide VMS run would otherwise autoscale unbounded.
    Prod caps this same step at 50 (detect_encounters_config.max_num_workers
    in composer-dags dags/core/vms/config.py)."""
    assert vms.parse_args([]).max_num_workers == 50
    assert ais.parse_args([]).max_num_workers == 50


def test_worker_cap_is_emitted_on_dataflow_only() -> None:
    """A cap that never reaches the pipeline options is worthless."""
    import workflows.encounters.ais as mod
    args = vms.parse_args(["--max-num-workers", "7"])
    # run() stamps these at runtime; _pipeline_options reads them for labels.
    args.run_id, args.commit_sha = "abc123abc123", "deadbee"
    args.runner = "dataflow"
    df = mod._pipeline_options(args, mode="1_bf", step="create_raw_encounters",
                               iteration=1, total_iterations=1)
    assert "--max_num_workers=7" in df
    args.runner = "docker"
    local = mod._pipeline_options(args, mode="1_bf", step="create_raw_encounters",
                                  iteration=1, total_iterations=1)
    assert not any(a.startswith("--max_num_workers") for a in local)


def test_binding_name_disambiguates_concurrent_runs() -> None:
    """Without a binding component two concurrent runs sharing an
    --experiment-id collide with DataflowJobAlreadyExistsError. Mirrors
    workflows/port_visits/ais.py, which already threads binding_name into
    make_job_name and stamps a dit_binding= label."""
    import workflows.encounters.ais as mod
    base = vms.parse_args([])
    base.run_id, base.commit_sha, base.runner = "abc123abc123", "deadbee", "dataflow"

    plain = mod._make_job_name(base, step="create_raw_encounters", mode="1_bf",
                               iteration=1, total_iterations=1)
    bound = vms.parse_args(["--binding-name", "after"])
    bound.run_id, bound.commit_sha, bound.runner = "abc123abc123", "deadbee", "dataflow"
    named = mod._make_job_name(bound, step="create_raw_encounters", mode="1_bf",
                               iteration=1, total_iterations=1)

    assert plain != named, "binding must change the job name"
    assert "after" in named
    # ...and the label follows, so cancel-run / the Dataflow UI can filter on it.
    opts = mod._pipeline_options(bound, mode="1_bf", step="create_raw_encounters",
                                 iteration=1, total_iterations=1)
    assert "--labels=dit_binding=after" in opts
    plain_opts = mod._pipeline_options(base, mode="1_bf", step="create_raw_encounters",
                                       iteration=1, total_iterations=1)
    assert not any(a.startswith("--labels=dit_binding") for a in plain_opts)


def test_submitter_image_default_is_pullable_not_a_local_tag() -> None:
    """An unversioned local compose tag is neither pullable nor reproducible,
    so a default (no --build-from-source) run failed on any machine that had
    not built it by hand. Copilot review, PR #72."""
    assert ais.DEFAULT_IMAGE_TAG == ais.DEFAULT_WORKER_IMAGE
    assert "/" in ais.DEFAULT_IMAGE_TAG and ":" in ais.DEFAULT_IMAGE_TAG
    assert ais.DEFAULT_IMAGE_TAG != ais.LOCAL_IMAGE_TAG
    assert vms.parse_args([]).image_tag == ais.DEFAULT_WORKER_IMAGE


def test_build_from_source_builds_compose_image_before_fetching_schemas() -> None:
    """_ensure_output_tables shells out to `docker run <image>` BEFORE any
    runner call, and the runner is what lazily builds the compose image. So on
    --build-from-source the schema fetch would hit a non-existent image and
    fail a run that would otherwise have succeeded. Copilot review, PR #72."""
    import workflows.encounters.ais as mod
    args = vms.parse_args(["--build-from-source", "--modes", "1_bf"])
    args.dest_dataset = "ds"

    with patch.object(mod.subprocess, "run") as sub, \
         patch.object(mod, "_fetch_schemas", return_value={"raw": [], "merged": []}) as fetch, \
         patch.object(mod, "_ensure_table"):
        mod._ensure_output_tables(args, "sfx")

    built = [c for c in sub.call_args_list if "build" in c.args[0]]
    assert built, "compose image was not built before the schema fetch"
    assert mod.COMPOSE_SERVICE in built[0].args[0]
    # ...and the schema fetch must then target the LOCAL tag, not the published one.
    assert fetch.call_args.args[0] == mod.LOCAL_IMAGE_TAG


def test_no_compose_build_when_not_building_from_source() -> None:
    import workflows.encounters.ais as mod
    args = vms.parse_args(["--modes", "1_bf"])
    args.dest_dataset = "ds"
    with patch.object(mod.subprocess, "run") as sub, \
         patch.object(mod, "_fetch_schemas", return_value={"raw": [], "merged": []}) as fetch, \
         patch.object(mod, "_ensure_table"):
        mod._ensure_output_tables(args, "sfx")
    assert not any("build" in c.args[0] for c in sub.call_args_list)
    assert fetch.call_args.args[0] == mod.DEFAULT_WORKER_IMAGE
