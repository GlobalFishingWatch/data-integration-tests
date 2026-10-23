"""Tests for ``workflows/port_visits/composer_drift.py`` (PIPELINE-4700 spike).

The production DAGs here are synthetic: built in code from DIT's own
``ProductionTarget`` and defaults, plus invented infrastructure values
(``example.invalid`` accounts and hosts). Nothing is copied from composer.
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

#: Invented production-only flags: sizing, networking, a service option.
_PRODUCTION_ONLY = (
    "--disk_size_gb=11",
    "--worker_machine_type=example-machine",
    "--no_use_public_ips",
)

#: Invented production-only flags the owner accepted as differences.
_PRODUCTION_ACCEPTED = (
    "--dataflow_service_options=example_option=true",
    "--network=example-network",
)

#: Invented values for the flags DIT owns.
_PRODUCTION_OWNED = {
    "--service_account_email": "runner@example.invalid",
    "--temp_location": "gs://example-bucket/temp",
    "--staging_location": "gs://example-bucket/staging",
    "--subnetwork": "regions/us-central1/subnetworks/example-subnet",
    "--max_num_workers": "7",
}


def _replace(tokens: list[str], values: dict[str, str]) -> list[str]:
    """Set ``--name=value`` flags in ``tokens``; drop labels and DIT-only flags."""
    dropped = ("--labels=", "--temp_dataset=")
    kept = [t for t in tokens if t.split("=", 1)[0] not in values and not t.startswith(dropped)]
    return [*kept, *(f"{k}={v}" for k, v in values.items())]


def _task(cmds: list[str], arguments: list[str], image: str = ais.DEFAULT_WORKER_IMAGE) -> dict:
    return {"image": image, "cmds": cmds, "arguments": arguments,
            "upstream_task_ids": [], "downstream_task_ids": []}


def _raw_dag(
    *, start: str = "2026-03-05", end: str = "2026-03-06",
    job_prefix: str = TARGET.job_name_prefix, visits_overrides: dict | None = None,
    production_only: tuple[str, ...] = _PRODUCTION_ONLY, visits_cmds: list[str] | None = None,
    image: str = ais.DEFAULT_WORKER_IMAGE,
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
        **_PRODUCTION_OWNED, "--output_table": TARGET.thinned_table,
        "--job_name": f"{job_prefix.replace('visits--', 'events--')}{run_date}",
    })
    visits_args = _replace(visits[2:], {
        **_PRODUCTION_OWNED, "--output_table": TARGET.output_table,
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
    check = _check("history_start", visits_overrides={"--start_date": "2026-03-05"},
                   end="2026-03-20")

    assert not check.matches
    assert check.composer == "{start_date}"
    assert check.note == "derived from the run's interval"


def test_the_constant_history_start_is_described_as_constant() -> None:
    assert _check("history_start").note == "constant, not derived from the run's interval"


def test_deployed_version_follows_the_port_visits_image_tag() -> None:
    check = _check("deployed_version", image="registry.example.invalid/core/pipe-anchorages:v9.9.9")

    assert check.composer == "pipe-anchorages v9.9.9"
    assert not check.matches


def test_segs_activity_needs_exactly_one_table_in_bad_segs() -> None:
    check = _check("segs_activity_table", visits_overrides={
        "--bad_segs": "(SELECT seg_id FROM `p.d.a` JOIN `p.d.b` USING (seg_id))",
    })

    assert check.composer is None
    assert check.note == "2 tables in --bad_segs"


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
        *_PRODUCTION_OWNED, "--output_table", "--job_name", "--labels", "--temp_dataset",
        "--dataflow_service_options", "--network",
    }
    assert {"--start_date", "--end_date", "--thinned_message_table", "--vessel_id_table",
            "--bad_segs", "--project", "--region", "--sdk_container_image",
            "--wait_for_job", "--runner"} <= set(visits.diff.same)
    assert visits.drift


def test_expected_differences_are_dit_owned_plus_accepted_with_reasons() -> None:
    assert drift.EXPECTED_DIFFERENCES == drift.DIT_OWNED_FLAGS | {
        "--dataflow_service_options", "--network",
    }
    assert all(drift.ACCEPTED_DIFFERENCES.values())
    # Still reported until the sizing / private-IP change lands (separate PR).
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
    parsed = composer.parse_argv(["--runner=DataflowRunner", "--runner=DirectRunner", "--x"])

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

    renamed = _comparisons(production_only=(), visits_cmds=["pipe-anchorages", "port-visits"])
    assert not renamed[ais.STEP_VISITS].command_matches
    assert renamed[ais.STEP_VISITS].drift


def test_dit_tokens_use_the_snapshot_runs_dates() -> None:
    placeholders = {"start_date": date(2026, 3, 1), "end_date": date(2026, 3, 10),
                    "end_date_exclusive": date(2026, 3, 11)}

    thin = composer.parse_argv(drift.dit_thin_tokens(placeholders))
    visits = composer.parse_argv(drift.dit_visits_tokens(TARGET, placeholders))

    assert thin.positionals == ("pipe-anchorages", ais.STEP_THIN)
    assert (thin.value("--start_date"), thin.value("--end_date")) == ("2026-03-01", "2026-03-10")
    assert visits.positionals == ("pipe-anchorages", ais.STEP_VISITS)
    assert visits.value("--start_date") == TARGET.history_start.isoformat()
    assert visits.value("--end_date") == "2026-03-10"
    assert visits.value("--thinned_message_table") == TARGET.thinned_table


# --------------------------------------------------------------------------
# report + main
# --------------------------------------------------------------------------

def test_format_report_lists_mismatches_and_the_verdict() -> None:
    report = drift.build_report("ais-v5-datalake", _dag(job_prefix="other--"), SOURCE)

    text = drift.format_report(report)

    assert report.drift
    assert "DIFF  job_name_prefix" in text
    assert "only in production: --disk_size_gb=11" in text
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
