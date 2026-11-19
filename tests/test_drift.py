"""Tests for ``dit.drift``, the per-workflow drift check (PIPELINE-4700).

Everything here is synthetic: an invented pipeline (``example-pipe``), invented
images, tables and infrastructure values (``example.invalid``). Nothing is
copied from composer-dags-production.
"""

from __future__ import annotations

import subprocess
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path

import pytest

from dit import composer, drift

IMAGE = "registry.example.invalid/core/example-pipe:v2.0.0"
TASK = "group.detect"


def _dit_tokens(placeholders: Mapping[str, date]) -> list[str]:
    """What the invented workflow runs: one step, over the run's dates."""
    return [
        "example-pipe", "detect",
        f"--start-date={placeholders['start_date'].isoformat()}",
        f"--end-date={placeholders['end_date_exclusive'].isoformat()}",
        "--bq-in-messages=example.staging.messages",
        "--bq-out-events=example.dit.events",
        "--runner=DataflowRunner",
        "--job_name=dit-example",
        "--labels=owner=dit",
    ]


#: Production's argv for the same run: its own output, job name and labels,
#: the runner under its other spelling, and one flag DIT leaves out.
PRODUCTION = (
    "example-pipe", "detect",
    "--labels", "owner=production", "mode=recurrent",
    "--start-date=2026-03-05",
    "--end-date=2026-03-06",
    "--bq-in-messages=example.staging.messages",
    "--bq-out-events=example.production.events",
    "--runner=dataflow",
    "--job_name=production-example",
    "--lineage=on",
)

OWNED = frozenset({"--bq-out-events", "--job_name", "--labels"})
ACCEPTED = {"--lineage": "lineage stays off in DIT"}


def _raw_task(argv: Sequence[str], image: str = IMAGE) -> dict:
    return {"image": image, "cmds": [argv[0]], "arguments": list(argv[1:]),
            "upstream_task_ids": [], "downstream_task_ids": []}


def _raw_dag(tasks: Mapping[str, dict], *, start: str = "2026-03-05",
             end: str = "2026-03-06") -> dict:
    return {
        "dag_id": "example.daily",
        "execution_date": f"{start}T00:00:00+00:00",
        "data_interval_start": f"{start}T00:00:00+00:00",
        "data_interval_end": f"{end}T00:00:00+00:00",
        "tasks": dict(tasks),
    }


def _dag(argv: Sequence[str] = PRODUCTION, image: str = IMAGE, **kwargs) -> composer.RenderedDag:
    return composer.dag_from_snapshot(
        _raw_dag({TASK: _raw_task(argv, image)}, **kwargs), snapshot="synthetic",
    )


def _step(**kwargs) -> drift.StepSpec:
    return drift.StepSpec(**{"step": "detect", "task_id": TASK, "image": IMAGE,
                             "tokens": _dit_tokens, **kwargs})


def _spec(step: drift.StepSpec | None = None, **kwargs) -> drift.WorkflowSpec:
    return drift.WorkflowSpec(**{
        "name": "example/detect", "workflow": "workflows/example/detect.py",
        "snapshot": "example_daily", "steps": (step or _step(),),
        "dit_owned": OWNED, "accepted": ACCEPTED,
        "equivalents": drift.RUNNER_EQUIVALENTS, "multi_value": drift.LABELS_MULTI_VALUE,
        **kwargs,
    })


def _report(spec: drift.WorkflowSpec | None = None, **dag_kwargs) -> drift.WorkflowReport:
    return drift.check_workflow(spec or _spec(), _dag(**dag_kwargs))


def _swap(argv: Sequence[str], old: str, new: str) -> tuple[str, ...]:
    assert old in argv
    return tuple(new if token == old else token for token in argv)


# --------------------------------------------------------------------------
# specs
# --------------------------------------------------------------------------

@pytest.mark.parametrize("kwargs", [
    {"tokens": None},
    {"command": ("example-pipe", "detect")},
])
def test_a_step_gives_exactly_one_of_tokens_and_command(kwargs) -> None:
    with pytest.raises(ValueError, match="exactly one of tokens and command"):
        _step(**kwargs)


def test_the_pipeline_version_is_the_image_tag_unless_the_step_names_it() -> None:
    overlay = "registry.example.invalid/dit/example-pipe:v2.0.0-abc123-overlay"

    assert (_step().image_name, _step().pipeline_version) == ("example-pipe", "v2.0.0")
    assert _step(image=overlay).pipeline_version == "v2.0.0-abc123-overlay"
    assert _step(image=overlay, version="v2.0.0").pipeline_version == "v2.0.0"


def test_expected_flags_are_owned_plus_accepted_plus_the_steps_own() -> None:
    step = _step(accepted={"--bq-in-messages": "staging by design"})
    spec = _spec(step)

    assert spec.expected(step) == OWNED | {"--lineage", "--bq-in-messages"}
    assert spec.reasons(step) == {**ACCEPTED, "--bq-in-messages": "staging by design"}


def test_single_value_wants_exactly_one_mention() -> None:
    parsed = composer.parse_argv(["--a=1", "--b=1", "--b=2"])

    assert drift.single_value(parsed, "--a") == "1"
    assert drift.single_value(parsed, "--b") is None
    assert drift.single_value(parsed, "--missing") is None


# --------------------------------------------------------------------------
# the three levels of one step
# --------------------------------------------------------------------------

def test_a_step_that_runs_what_production_runs_has_no_drift() -> None:
    (step,) = _report().steps

    assert step.version.matches
    assert step.argv.command_matches
    assert step.argv.diff.identical
    assert not step.argv.indicative
    assert not step.drift


def test_expected_differences_are_left_out_of_the_diff_and_listed() -> None:
    (step,) = _report().steps

    assert step.argv.expected == ("--bq-out-events", "--job_name", "--labels", "--lineage")
    assert set(step.argv.diff.same) == {
        "--start-date", "--end-date", "--bq-in-messages", "--runner",
    }
    assert step.reasons == ACCEPTED


def test_equivalent_spellings_and_multi_value_labels_compare_as_one() -> None:
    (step,) = _report().steps

    # DataflowRunner and dataflow are one value.
    assert "--runner" in step.argv.diff.same
    # Production's space-separated label pairs are values, not command words.
    assert step.argv.production.positionals == ("example-pipe", "detect")
    assert step.argv.production.values("--labels") == ("owner=production", "mode=recurrent")

    unhinted = drift.check_workflow(_spec(multi_value=frozenset()), _dag()).steps[0]
    assert not unhinted.argv.command_matches


def test_bare_flags_do_not_swallow_the_command_word() -> None:
    production = ("example-pipe", "-v", "detect", *PRODUCTION[2:])

    hinted = _report(_spec(bare=frozenset({"-v"}), accepted={**ACCEPTED, "-v": "verbosity"}),
                     argv=production).steps[0]
    unhinted = _report(argv=production).steps[0]

    assert hinted.argv.command_matches and not hinted.drift
    assert unhinted.argv.production.positionals == ("example-pipe",)
    assert not unhinted.argv.command_matches


def test_a_version_bump_is_drift_with_identical_command_and_flags() -> None:
    (step,) = _report(image="registry.example.invalid/core/example-pipe:v3.0.0").steps

    assert not step.version.matches
    assert (step.version.dit_version, step.version.production_version) == ("v2.0.0", "v3.0.0")
    assert not step.argv.drift
    assert step.drift


def test_another_image_name_is_a_version_level_difference() -> None:
    (step,) = _report(image="registry.example.invalid/core/other-pipe:v2.0.0").steps

    assert (step.version.dit_image, step.version.production_image) == (
        "example-pipe", "other-pipe",
    )
    assert not step.version.matches


def test_a_flag_difference_is_drift_at_the_flags_level_only() -> None:
    production = (*_swap(PRODUCTION, "--bq-in-messages=example.staging.messages",
                         "--bq-in-messages=example.production.messages"), "--new-option=1")

    (step,) = _report(argv=production).steps

    assert step.version.matches and step.argv.command_matches
    assert step.argv.diff.differing == ("--bq-in-messages",)
    assert step.argv.diff.only_right == ("--new-option",)
    assert not step.argv.indicative
    assert step.drift


def test_when_the_command_differs_the_flag_differences_are_indicative_only() -> None:
    production = ("example-pipe", "detect-events", "--from=2026-03-05", "--runner=dataflow")

    (step,) = _report(argv=production).steps

    assert not step.argv.command_matches
    assert step.argv.indicative
    # Still listed, so the size of the gap shows.
    assert step.argv.diff.only_right == ("--from",)
    assert set(step.argv.diff.only_left) == {"--start-date", "--end-date", "--bq-in-messages"}
    assert step.drift


def test_a_step_accepted_difference_applies_to_that_step_only() -> None:
    production = _swap(PRODUCTION, "--bq-in-messages=example.staging.messages",
                       "--bq-in-messages=example.production.messages")
    accepting = _step(accepted={"--bq-in-messages": "staging by design"})
    other = _step(step="detect_again")

    first, second = _report(_spec(steps=(accepting, other)), argv=production).steps

    assert "--bq-in-messages" in first.argv.expected and not first.drift
    assert second.argv.diff.differing == ("--bq-in-messages",) and second.drift


def test_dit_tokens_are_built_for_the_snapshot_runs_dates() -> None:
    seen = []

    def tokens(placeholders):
        seen.append(dict(placeholders))
        return _dit_tokens(placeholders)

    production = _swap(_swap(PRODUCTION, "--start-date=2026-03-05", "--start-date=2026-04-01"),
                       "--end-date=2026-03-06", "--end-date=2026-04-11")
    (step,) = _report(_spec(_step(tokens=tokens)), argv=production,
                      start="2026-04-01", end="2026-04-11").steps

    assert seen == [{"start_date": date(2026, 4, 1), "end_date": date(2026, 4, 10),
                     "end_date_exclusive": date(2026, 4, 11)}]
    assert not step.drift


def test_a_step_without_offline_tokens_compares_version_and_command_only() -> None:
    spec = _spec(_step(tokens=None, command=("example-pipe", "detect")))

    (same,) = _report(spec).steps
    (renamed,) = _report(spec, argv=("example-pipe", "detect-events", "--x=1")).steps

    assert same.argv.diff is None and same.argv.command_matches
    assert not same.argv.indicative and not same.drift
    assert not renamed.argv.command_matches and renamed.drift


def test_a_missing_task_is_a_key_error_naming_it() -> None:
    with pytest.raises(KeyError, match="group.absent"):
        drift.check_workflow(_spec(_step(task_id="group.absent")), _dag())


# --------------------------------------------------------------------------
# workflow reports and exit codes
# --------------------------------------------------------------------------

def _field_checks(dag: composer.RenderedDag) -> tuple[drift.FieldCheck, ...]:
    parsed = composer.parse_argv(dag.task(TASK).argv, multi=drift.LABELS_MULTI_VALUE)
    return (
        drift.FieldCheck("job_name", "production-example",
                         drift.single_value(parsed, "--job_name")),
    )


def test_field_checks_count_towards_drift() -> None:
    spec = _spec(field_checks=_field_checks, fields_title="Target")

    same = _report(spec)
    renamed = _report(spec, argv=_swap(PRODUCTION, "--job_name=production-example",
                                       "--job_name=another-name"))

    assert [f.matches for f in same.fields] == [True] and not same.drift
    assert [f.composer for f in renamed.fields] == ["another-name"]
    assert not any(s.drift for s in renamed.steps)
    assert renamed.drift


def test_drift_is_unexpected_only_in_a_workflow_marked_in_sync() -> None:
    bumped = "registry.example.invalid/core/example-pipe:v3.0.0"

    assert not _report(_spec(in_sync=True)).unexpected_drift
    assert _report(_spec(in_sync=True), image=bumped).unexpected_drift
    assert _report(_spec(in_sync=False), image=bumped).drift
    assert not _report(_spec(in_sync=False), image=bumped).unexpected_drift


@pytest.mark.parametrize("in_sync, drifted, strict, unreadable, code", [
    (True, False, False, False, 0),
    (True, True, False, False, 1),
    (False, True, False, False, 0),   # known work: reported, not failed on
    (False, True, True, False, 1),    # --strict: any drift fails
    (False, False, True, False, 0),
    (True, False, False, True, 2),    # an unreadable snapshot wins over everything
    (True, True, True, True, 2),
])
def test_exit_code(in_sync, drifted, strict, unreadable, code) -> None:
    image = "registry.example.invalid/core/example-pipe:v3.0.0" if drifted else IMAGE
    spec = _spec(in_sync=in_sync)
    errors = [drift.WorkflowError(spec, "no such snapshot")] if unreadable else []

    assert drift.exit_code([_report(spec, image=image)], errors, strict=strict) == code


# --------------------------------------------------------------------------
# run_specs: reading a composer checkout
# --------------------------------------------------------------------------

def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


@pytest.fixture
def composer_repo(tmp_path: Path) -> Path:
    """A throwaway repo with two synthetic snapshots."""
    repo = tmp_path / "composer"
    snapshots = repo / composer.SNAPSHOT_DIR
    snapshots.mkdir(parents=True)
    consistent = _raw_dag({TASK: _raw_task(PRODUCTION)})
    bumped = _raw_dag({TASK: _raw_task(
        PRODUCTION, "registry.example.invalid/core/example-pipe:v3.0.0")})
    (snapshots / "example_daily.py").write_text(f"rendered_dag = {consistent!r}\n")
    (snapshots / "example_bumped.py").write_text(f"rendered_dag = {bumped!r}\n")
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "add", ".")
    _git(repo, "-c", "user.name=t", "-c", "user.email=t@example.invalid",
         "commit", "-q", "-m", "fixture")
    return repo


def test_run_specs_reads_every_spec_at_one_commit(composer_repo: Path) -> None:
    specs = [_spec(name="a"), _spec(name="b", snapshot="example_bumped")]

    reports, errors = drift.run_specs(specs, composer_repo, "main")

    assert errors == ()
    assert [(r.spec.name, r.drift) for r in reports] == [("a", False), ("b", True)]
    assert len({r.dag.commit for r in reports}) == 1
    assert reports[0].dag.commit == composer.resolve_ref(composer_repo, "main")


def test_run_specs_loads_a_shared_snapshot_once(composer_repo: Path, monkeypatch) -> None:
    loaded = []
    real = composer.load_dag
    monkeypatch.setattr(composer, "load_dag",
                        lambda repo, name, ref: loaded.append(name) or real(repo, name, ref))

    reports, _ = drift.run_specs([_spec(name="a"), _spec(name="b")], composer_repo, "main")

    assert loaded == ["example_daily"]
    assert [r.spec.name for r in reports] == ["a", "b"]


def test_an_unreadable_snapshot_or_task_is_an_error_and_the_rest_still_runs(
    composer_repo: Path,
) -> None:
    specs = [
        _spec(name="ok"),
        _spec(name="no-snapshot", snapshot="absent"),
        _spec(_step(task_id="group.absent"), name="no-task"),
    ]

    reports, errors = drift.run_specs(specs, composer_repo, "main")

    assert [r.spec.name for r in reports] == ["ok"]
    assert [e.spec.name for e in errors] == ["no-snapshot", "no-task"]
    assert "group.absent" in errors[1].message
    assert drift.exit_code(reports, errors) == 2


def test_an_unknown_ref_is_an_error_for_every_spec(composer_repo: Path) -> None:
    reports, errors = drift.run_specs([_spec(name="a"), _spec(name="b")],
                                      composer_repo, "no-such-ref")

    assert reports == ()
    assert [e.spec.name for e in errors] == ["a", "b"]


# --------------------------------------------------------------------------
# formatting
# --------------------------------------------------------------------------

def test_the_report_names_each_level_and_explains_expected_differences() -> None:
    spec = _spec(_step(note="a step note"), field_checks=_field_checks,
                 fields_title="Target", in_sync=True, note="a workflow note")

    text = drift.format_report(_report(spec))

    assert text.splitlines()[0] == "example/detect (workflows/example/detect.py)"
    assert "snapshot synthetic (example.daily)" in text
    assert "note: a workflow note" in text
    assert "Target:\n  ok    job_name" in text
    assert "detect (composer task group.detect):\n  note: a step note" in text
    assert "  version: same (example-pipe v2.0.0)" in text
    assert "  command: same (example-pipe detect)" in text
    assert "  flags:\n    same: --start-date, --end-date, --bq-in-messages, --runner" in text
    # One-sided expected flags say which side passes them; accepted ones say why.
    assert "--lineage [production only] (lineage stays off in DIT)" in text
    assert "--job_name," in text
    assert text.endswith("expected in sync: yes\ndrift: no")


def test_the_report_labels_flags_as_indicative_when_the_command_differs() -> None:
    text = drift.format_report(
        _report(argv=("example-pipe", "detect-events", "--from=2026-03-05")),
    )

    assert "command: DIFF  dit=example-pipe detect  production=example-pipe detect-events" in text
    assert "flags (indicative only: the command differs" in text
    assert "only in production: --from=2026-03-05" in text
    assert text.endswith("expected in sync: no\ndrift: yes")


def test_the_report_says_when_flags_were_not_compared() -> None:
    spec = _spec(_step(tokens=None, command=("example-pipe", "detect")))

    text = drift.format_report(_report(spec))

    assert "flags: not compared (DIT's argv cannot be built offline)" in text
    assert "only in production" not in text


@pytest.mark.parametrize("in_sync, drifted, phrase", [
    (True, False, "in sync"),
    (True, True, "DRIFT (expected in sync)"),
    (False, True, "drift (known: not expected in sync yet)"),
    (False, False, "no drift (can be marked in sync)"),
])
def test_verdict(in_sync, drifted, phrase) -> None:
    image = "registry.example.invalid/core/example-pipe:v3.0.0" if drifted else IMAGE

    assert drift.verdict(_report(_spec(in_sync=in_sync), image=image)) == phrase


def test_the_summary_has_one_row_per_step_and_one_verdict_per_workflow() -> None:
    renamed = ("example-pipe", "detect-events", "--from=2026-03-05", "--runner=dataflow")
    reports = [
        _report(_spec(name="example/same", in_sync=True, field_checks=_field_checks)),
        _report(_spec(name="example/bumped", note="not ported yet"),
                image="registry.example.invalid/core/example-pipe:v3.0.0"),
        _report(_spec(name="example/renamed"), argv=renamed),
    ]
    errors = [drift.WorkflowError(_spec(name="example/unread"), "no such snapshot")]

    lines = drift.format_summary(reports, errors).splitlines()

    def row(workflow: str) -> list[str]:
        cells = lines[lines.index(next(x for x in lines if x.startswith(workflow))) + 1]
        return [cell.strip() for cell in cells.split("  ") if cell.strip()]

    assert lines[0] == "composer-dags-production @ unknown"
    assert lines[2].split() == [
        "step", "composer", "task", "version", "(DIT", "->", "production)", "command",
        "only", "prod", "only", "DIT", "differ",
    ]
    assert "example/same: in sync; 1 field checks, 0 differ" in lines
    assert row("example/same") == ["detect", "group.detect", "ok v2.0.0", "ok", "0", "0", "0"]
    assert ("example/bumped: drift (known: not expected in sync yet). "
            "Note: not ported yet") in lines
    assert row("example/bumped")[2:4] == ["DIFF v2.0.0 -> v3.0.0", "ok"]
    assert row("example/renamed")[3:] == [
        "DIFF, production: example-pipe detect-events", "1*", "3*", "0*",
    ]
    assert any(line.startswith("* indicative only") for line in lines)
    assert lines[-1] == "example/unread: could not be read: no such snapshot"


def test_the_summary_prints_no_argv_values() -> None:
    renamed = ("example-pipe", "detect-events", "--from=2026-03-05", "--secret=example-value")

    text = drift.format_summary([_report(), _report(argv=renamed)])

    for value in ("example.production.events", "production-example", "example-value",
                  "example.staging.messages", "--secret", "--from"):
        assert value not in text


def test_the_summary_names_both_images_when_the_name_differs() -> None:
    text = drift.format_summary(
        [_report(image="registry.example.invalid/core/other-pipe:v2.0.0")],
    )

    assert "DIFF example-pipe v2.0.0 -> other-pipe v2.0.0" in text
