"""Tests for ``workflows/port_visits/composer_drift.py`` (PIPELINE-4700 spike).

The production DAGs here are synthetic: built in code from DIT's own
``ProductionTarget`` and defaults, plus invented infrastructure values
(``example.invalid`` accounts and hosts). Nothing is copied from composer.

They follow the shape of production's argv for the pipe-anchorages CLI it runs
(v5.0.1): ``--bq-in-*`` / ``--bq-out-*`` flags, an exclusive ``--end-date``,
production's own image at the same version as DIT's overlay, and ONE
``--labels`` flag followed by space-separated pairs.
"""

from __future__ import annotations

import subprocess
from datetime import date, datetime
from pathlib import Path

import pytest

from dit import composer
from workflows.port_visits import ais
from workflows.port_visits import composer_drift as drift
from workflows.port_visits.cross_version_ais import PRODUCTION_TARGETS

TARGET = PRODUCTION_TARGETS["ais-v5-datalake"]
SOURCE = drift.TARGET_SOURCES["ais-v5-datalake"]

#: Invented production-only flags, which DIT does not emit. (DIT emits
#: production's sizing and --no_use_public_ips itself, so those are not here.)
_PRODUCTION_ONLY = (
    "--example_production_value=11",
    "--example_production_switch",
)

#: Invented production-only flags the owner accepted as differences.
_PRODUCTION_ACCEPTED = (
    "--dataflow_service_options=example_option=true",
    "--network=example-network",
)

#: Production's own image: another registry than DIT's overlay, same version.
_PRODUCTION_IMAGE = (
    f"registry.example.invalid/core/pipe-anchorages:{ais.DEFAULT_PIPELINE_VERSION}"
)

#: Invented values for the flags DIT owns.
_PRODUCTION_OWNED = {
    "--service_account_email": "runner@example.invalid",
    "--temp_location": "gs://example-bucket/temp",
    "--staging_location": "gs://example-bucket/staging",
    "--subnetwork": "regions/us-central1/subnetworks/example-subnet",
    "--max_num_workers": "7",
    "--sdk_container_image": _PRODUCTION_IMAGE,
}

#: Invented labels, in production's form: one flag, space-separated pairs.
_PRODUCTION_LABELS = ("--labels", "example_project=demo", "example_mode=recurrent",
                      "run_date=2026-03-05")


def _replace(tokens: list[str], values: dict[str, str]) -> list[str]:
    """Set ``--name=value`` flags in ``tokens`` and swap DIT's labels for production's."""
    kept = [t for t in tokens
            if t.split("=", 1)[0] not in values and not t.startswith("--labels=")]
    return [*_PRODUCTION_LABELS, *kept, *(f"{k}={v}" for k, v in values.items())]


def _task(cmds: list[str], arguments: list[str], image: str = _PRODUCTION_IMAGE) -> dict:
    return {"image": image, "cmds": cmds, "arguments": arguments,
            "upstream_task_ids": [], "downstream_task_ids": []}


def _raw_dag(
    *, start: str = "2026-03-05", end: str = "2026-03-06",
    job_prefix: str = TARGET.job_name_prefix, visits_overrides: dict | None = None,
    production_only: tuple[str, ...] = _PRODUCTION_ONLY, visits_cmds: list[str] | None = None,
    image: str = _PRODUCTION_IMAGE,
) -> dict:
    """A production DAG that runs what DIT runs, except for DIT-owned flags,
    the accepted production-only flags, ``production_only`` and the overrides."""
    production_only = (*production_only, *_PRODUCTION_ACCEPTED)
    interval = composer.DataInterval(
        datetime.fromisoformat(f"{start}T00:00:00+00:00"),
        datetime.fromisoformat(f"{end}T00:00:00+00:00"),
    )
    placeholders = composer.date_placeholders(interval, interval.start)
    run_date = placeholders["start_date"].strftime("%Y%m%d")

    thin = drift.dit_thin_tokens(placeholders)
    visits = drift.dit_visits_tokens(TARGET, placeholders)
    thin_args = _replace(thin[2:], {
        **_PRODUCTION_OWNED, "--bq-out-port-state-transitions": TARGET.thinned_table,
        "--job_name": f"{job_prefix.replace('visits--', 'events--')}{run_date}",
    })
    visits_args = _replace(visits[2:], {
        **_PRODUCTION_OWNED, "--bq-out-port-visits": TARGET.output_table,
        "--job_name": f"{job_prefix}{run_date}", **(visits_overrides or {}),
    })
    return {
        "dag_id": "example.ais.daily",
        "execution_date": f"{start}T00:00:00+00:00",
        "data_interval_start": f"{start}T00:00:00+00:00",
        "data_interval_end": f"{end}T00:00:00+00:00",
        "tasks": {
            SOURCE.thin_task: _task(thin[:2], [*thin_args, *production_only], image),
            SOURCE.visits_task: _task(visits_cmds or visits[:2],
                                      [*visits_args, *production_only], image),
        },
    }


def _dag(**kwargs) -> composer.RenderedDag:
    return composer.dag_from_snapshot(_raw_dag(**kwargs), snapshot="synthetic")


def _checks(**kwargs) -> dict[tuple[str, str], drift.FieldCheck]:
    return {(c.field, c.note): c
            for c in drift.production_target_checks(TARGET, _dag(**kwargs), SOURCE)}


def _check(field: str, **kwargs) -> drift.FieldCheck:
    (found,) = [c for (f, _), c in _checks(**kwargs).items() if f == field]
    return found


# --------------------------------------------------------------------------
# ProductionTarget fields
# --------------------------------------------------------------------------

def test_every_target_field_matches_a_consistent_dag() -> None:
    checks = drift.production_target_checks(TARGET, _dag(), SOURCE)

    assert [c.field for c in checks if not c.matches] == []
    assert {c.field for c in checks} == {
        f.name for f in TARGET.__dataclass_fields__.values() if f.name != "name"
    }


def test_both_thinned_table_uses_are_checked() -> None:
    notes = {note for (field, note) in _checks() if field == "thinned_table"}

    assert notes == {"step 1 output", "step 2 input"}


def test_a_job_name_prefix_from_another_dag_is_a_mismatch() -> None:
    check = _check("job_name_prefix", job_prefix="example-backfill--port-visits--")

    assert not check.matches
    assert check.composer == "example-backfill--port-visits--"


def test_run_date_suffix_is_ambiguous_in_a_one_day_run_and_named_in_a_longer_one() -> None:
    assert _check("job_name_prefix").note == "run date suffix: start_date or end_date"
    assert _check("job_name_prefix", end="2026-03-20").note == "run date suffix: start_date"


def test_a_job_name_without_a_run_date_keeps_its_name() -> None:
    assert drift.split_run_date("fixed-name", {"start_date": date(2026, 3, 5)}) == (
        "fixed-name", "no run date at the end of the job name",
    )


def test_a_history_start_tied_to_the_interval_is_reported() -> None:
    check = _check("history_start", visits_overrides={"--start-date": "2026-03-05"},
                   end="2026-03-20")

    assert not check.matches
    assert check.composer == "{start_date}"
    assert check.note == "derived from the run's interval"


def test_the_constant_history_start_is_described_as_constant() -> None:
    assert _check("history_start").note == "constant, not derived from the run's interval"


def test_deployed_version_follows_the_port_visits_image_tag() -> None:
    assert _check("deployed_version").matches
    assert TARGET.deployed_version == f"pipe-anchorages {ais.DEFAULT_PIPELINE_VERSION}"

    check = _check("deployed_version", image="registry.example.invalid/core/pipe-anchorages:v9.9.9")

    assert check.composer == "pipe-anchorages v9.9.9"
    assert not check.matches


def test_the_default_image_version_is_checked_against_both_tasks() -> None:
    # The image flag is DIT-owned (an overlay build under DIT's namespace), so
    # the argv diff cannot see a production version bump; this check does.
    same = drift.default_version_checks(_dag(), SOURCE)
    bumped = drift.default_version_checks(
        _dag(image="registry.example.invalid/core/pipe-anchorages:v9.9.9"), SOURCE,
    )

    assert [(c.dit, c.composer) for c in same] == [(ais.DEFAULT_PIPELINE_VERSION,) * 2] * 2
    assert {c.note for c in same} == {
        f"image tag of {SOURCE.thin_task}", f"image tag of {SOURCE.visits_task}",
    }
    assert all(c.matches for c in same)
    assert [c.composer for c in bumped] == ["v9.9.9", "v9.9.9"]
    assert not any(c.matches for c in bumped)


def test_a_production_version_bump_is_drift_even_with_identical_flags() -> None:
    image = "registry.example.invalid/core/pipe-anchorages:v9.9.9"
    report = drift.build_report(
        "ais-v5-datalake", _dag(production_only=(), image=image), SOURCE,
    )

    assert not any(c.drift for c in report.argv)
    assert report.drift
    assert "DIFF  default_pipeline_version" in drift.format_report(report)


def test_a_different_production_sizing_is_reported() -> None:
    overrides = {"--worker_machine_type": "example-machine", "--disk_size_gb": "11"}

    machine = _check("worker_machine_type", visits_overrides=overrides)
    disk = _check("disk_size_gb", visits_overrides=overrides)
    visits = _comparisons(visits_overrides=overrides, production_only=())[ais.STEP_VISITS]

    assert (machine.dit, machine.composer) == (TARGET.worker_machine_type, "example-machine")
    assert (disk.dit, disk.composer) == (str(TARGET.disk_size_gb), "11")
    assert not machine.matches and not disk.matches
    assert {"--worker_machine_type", "--disk_size_gb"} <= set(visits.diff.differing)
    assert visits.drift


def test_segs_activity_needs_exactly_one_table_in_bad_segs() -> None:
    check = _check("segs_activity_table", visits_overrides={
        "--bad-segs": "(SELECT seg_id FROM `p.d.a` JOIN `p.d.b` USING (seg_id))",
    })

    assert check.composer is None
    assert check.note == "2 tables in --bad-segs"


# --------------------------------------------------------------------------
# argv
# --------------------------------------------------------------------------

def _comparisons(**kwargs) -> dict[str, drift.ArgvComparison]:
    report = drift.build_report("ais-v5-datalake", _dag(**kwargs), SOURCE)
    return {c.step: c for c in report.argv}


def test_argv_differences_are_split_into_expected_and_drift() -> None:
    visits = _comparisons()[ais.STEP_VISITS]

    assert visits.command_matches
    assert set(visits.diff.only_right) == {t.split("=", 1)[0] for t in _PRODUCTION_ONLY}
    assert visits.diff.only_left == ()
    assert visits.diff.differing == ()
    assert set(visits.expected) == {
        *_PRODUCTION_OWNED, "--bq-out-port-visits", "--job_name", "--labels",
        "--dataflow_service_options", "--network",
    }
    assert {"--start-date", "--end-date", "--bq-in-port-state-transitions",
            "--bq-in-segment-info", "--bad-segs", "--project", "--region", "--runner",
            "--worker_machine_type", "--disk_size_gb", "--no_use_public_ips",
            } == set(visits.diff.same)
    assert visits.drift


def test_step_1_flags_compare_under_their_v5_names() -> None:
    thin = _comparisons(production_only=())[ais.STEP_THIN]

    assert thin.command_matches
    assert thin.dit.positionals == ("pipe-anchorages", "port-state-transitions")
    assert {"--start-date", "--end-date", "--bq-in-named-anchorages", "--bq-in-messages",
            "--project", "--region", "--runner", "--worker_machine_type", "--disk_size_gb",
            "--no_use_public_ips"} == set(thin.diff.same)
    assert "--bq-out-port-state-transitions" in thin.expected
    assert not thin.drift


def test_productions_space_separated_labels_stay_out_of_the_command() -> None:
    # Production passes ONE --labels flag followed by its pairs. Parsed without
    # the multi-value hint, every pair after the first reads as a positional
    # and the commands no longer match.
    task = _dag().task(SOURCE.visits_task)
    assert task.arguments[:len(_PRODUCTION_LABELS)] == _PRODUCTION_LABELS

    parsed = drift.parse_argv(task.argv)
    assert parsed.positionals == ("pipe-anchorages", "port-visits")
    assert parsed.values("--labels") == _PRODUCTION_LABELS[1:]
    assert composer.parse_argv(task.argv).positionals[2:] == _PRODUCTION_LABELS[2:]

    for comparison in _comparisons().values():
        assert comparison.command_matches
        assert "--labels" in comparison.expected


def test_expected_differences_are_dit_owned_plus_accepted_with_reasons() -> None:
    assert drift.EXPECTED_DIFFERENCES == drift.DIT_OWNED_FLAGS | {
        "--dataflow_service_options", "--network",
    }
    assert all(drift.ACCEPTED_DIFFERENCES.values())
    # The names are the v5 CLI's; nothing of the removed v4 CLI is left.
    assert drift.DIT_OWNED_FLAGS == {
        "--bq-out-port-state-transitions", "--bq-out-port-visits", "--job_name", "--labels",
        "--service_account_email", "--temp_location", "--staging_location", "--subnetwork",
        "--max_num_workers", "--sdk_container_image",
    }
    # DIT emits production's sizing and private IPs itself, so these are
    # compared like any other flag: a difference is drift, not expected.
    assert not {"--worker_machine_type", "--disk_size_gb", "--no_use_public_ips"} & (
        drift.EXPECTED_DIFFERENCES
    )


def test_no_drift_when_production_runs_what_dit_runs() -> None:
    comparisons = _comparisons(production_only=())

    assert not any(c.drift for c in comparisons.values())


def test_runner_spellings_compare_equal() -> None:
    visits = _comparisons(production_only=(), visits_overrides={"--runner": "dataflow"})[
        ais.STEP_VISITS
    ]

    assert "--runner" in visits.diff.same
    assert not visits.drift


def test_canonical_values_rewrites_only_listed_spellings() -> None:
    parsed = drift.parse_argv(["--runner=DataflowRunner", "--runner=DirectRunner", "--x"])

    assert drift.canonical_values(parsed).flags == (
        composer.Flag("--runner", "dataflow"),
        composer.Flag("--runner", "DirectRunner"),
        composer.Flag("--x", None),
    )


def test_a_changed_value_or_command_is_drift() -> None:
    changed = _comparisons(production_only=(), visits_overrides={"--project": "example-project"})
    visits = changed[ais.STEP_VISITS]
    assert visits.diff.differing == ("--project",)
    assert visits.drift

    # e.g. production still on the removed v4 command.
    renamed = _comparisons(production_only=(), visits_cmds=["pipe-anchorages", "port_visits"])
    assert not renamed[ais.STEP_VISITS].command_matches
    assert renamed[ais.STEP_VISITS].drift


def test_dit_tokens_use_the_snapshot_runs_dates() -> None:
    placeholders = {"start_date": date(2026, 3, 1), "end_date": date(2026, 3, 10),
                    "end_date_exclusive": date(2026, 3, 11)}

    thin = drift.parse_argv(drift.dit_thin_tokens(placeholders))
    visits = drift.parse_argv(drift.dit_visits_tokens(TARGET, placeholders))

    # DIT's inclusive [start_date, end_date] reaches the pipeline as the
    # half-open [start_date, end_date + 1 day), which is what production passes.
    assert thin.positionals == ("pipe-anchorages", "port-state-transitions")
    assert (thin.value("--start-date"), thin.value("--end-date")) == ("2026-03-01", "2026-03-11")
    assert visits.positionals == ("pipe-anchorages", "port-visits")
    assert visits.value("--start-date") == TARGET.history_start.isoformat()
    assert visits.value("--end-date") == "2026-03-11"
    assert visits.value("--bq-in-port-state-transitions") == TARGET.thinned_table
    assert visits.value("--bq-in-segment-info") == TARGET.segment_info_table
    assert f"`{TARGET.segs_activity_table}`" in visits.value("--bad-segs")


# --------------------------------------------------------------------------
# report + main
# --------------------------------------------------------------------------

def test_format_report_lists_mismatches_and_the_verdict() -> None:
    report = drift.build_report("ais-v5-datalake", _dag(job_prefix="other--"), SOURCE)

    text = drift.format_report(report)

    assert report.drift
    assert "DIFF  job_name_prefix" in text
    assert "ok    default_pipeline_version" in text
    assert "command: same (pipe-anchorages port-visits)" in text
    assert "only in production: --example_production_value=11" in text
    assert "--network (determined by DIT's --subnetwork)" in text
    assert text.endswith("drift: yes")


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


@pytest.fixture
def composer_repo(tmp_path: Path):
    """Commit synthetic snapshots into a throwaway repo."""
    repo = tmp_path / "composer"
    snapshots = repo / composer.SNAPSHOT_DIR
    snapshots.mkdir(parents=True)
    (snapshots / "consistent.py").write_text(f"rendered_dag = {_raw_dag(production_only=())!r}\n")
    (snapshots / "drifted.py").write_text(f"rendered_dag = {_raw_dag()!r}\n")
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "add", ".")
    _git(repo, "-c", "user.name=t", "-c", "user.email=t@example.invalid",
         "commit", "-q", "-m", "fixture")
    return repo


@pytest.mark.parametrize("snapshot, code", [("consistent", 0), ("drifted", 1), ("missing", 2)])
def test_main_exit_code_says_whether_anything_drifted(
    composer_repo: Path, snapshot: str, code: int, capsys,
) -> None:
    rc = drift.main(["--composer-dir", str(composer_repo), "--ref", "main",
                     "--snapshot", snapshot])

    assert rc == code
    if code == 2:
        assert "could not run" in capsys.readouterr().err
