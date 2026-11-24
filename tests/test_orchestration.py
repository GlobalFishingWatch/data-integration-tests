"""Tests for ``dit.orchestration`` and the per-pipeline declaration files."""
from __future__ import annotations

import dataclasses

import pytest

from dit import orchestration as o
from workflows.encounters import orchestration as encounters_o
from workflows.pipe_events import fishing
from workflows.pipe_events import orchestration as events_o
from workflows.pipe_gaps import mode_equivalence as gaps
from workflows.pipe_gaps import orchestration as gaps_o
from workflows.pipe_segment import identity_match_key as imk
from workflows.pipe_segment import orchestration as segment_o
from workflows.port_visits import ais as port_visits
from workflows.port_visits import orchestration as port_visits_o

ALL = (port_visits_o, encounters_o, events_o, gaps_o, segment_o)


def _decl(parallel: str, status: str = o.STATUS_CLAIMED, source: str = "someone") -> o.StepOrchestration:
    return o.StepOrchestration(
        step="s", command="c", parallel_slices=parallel, depends_on_previous_run=None,
        status=status, source=source if status != o.STATUS_UNTESTED else "", reason="r",
    )


# --- the judgement table -------------------------------------------------------

@pytest.mark.parametrize("parallel,outcome,verdict,exit_code,warning", [
    (o.PARALLEL_ALLOWED, o.OUTCOME_IDENTICAL, o.VERDICT_PASS, 0, False),
    (o.PARALLEL_ALLOWED, o.OUTCOME_DIFFERENT, o.VERDICT_FAIL, 1, False),
    (o.PARALLEL_ALLOWED, o.OUTCOME_FAILED, o.VERDICT_FAIL, 1, False),
    (o.PARALLEL_FORBIDDEN, o.OUTCOME_DIFFERENT, o.VERDICT_AS_DECLARED, 0, False),
    (o.PARALLEL_FORBIDDEN, o.OUTCOME_FAILED, o.VERDICT_AS_DECLARED, 0, False),
    (o.PARALLEL_FORBIDDEN, o.OUTCOME_IDENTICAL, o.VERDICT_TOO_STRICT, 0, True),
    (o.PARALLEL_UNKNOWN, o.OUTCOME_IDENTICAL, o.VERDICT_UNDECLARED, 0, False),
    (o.PARALLEL_UNKNOWN, o.OUTCOME_DIFFERENT, o.VERDICT_UNDECLARED, 1, False),
    (o.PARALLEL_UNKNOWN, o.OUTCOME_FAILED, o.VERDICT_UNDECLARED, 1, False),
])
def test_judge_table(parallel, outcome, verdict, exit_code, warning) -> None:
    judgement = o.judge(_decl(parallel), outcome)
    assert (judgement.verdict, judgement.exit_code, judgement.warning) == (verdict, exit_code, warning)
    assert "s declares parallel slices" in judgement.message


def test_too_strict_says_warning_and_does_not_change_anything() -> None:
    message = o.judge(_decl(o.PARALLEL_FORBIDDEN), o.OUTCOME_IDENTICAL).message
    assert "WARNING" in message and "Not changed automatically" in message


def test_forbidden_failure_names_the_other_reasons_a_job_can_fail() -> None:
    assert "quota" in o.judge(_decl(o.PARALLEL_FORBIDDEN), o.OUTCOME_FAILED).message


def test_a_pass_on_a_claim_says_how_to_record_it_and_a_verified_one_does_not() -> None:
    claimed = o.judge(_decl(o.PARALLEL_ALLOWED), o.OUTCOME_IDENTICAL).message
    verified = o.judge(_decl(o.PARALLEL_ALLOWED, o.STATUS_VERIFIED, "build-id"),
                       o.OUTCOME_IDENTICAL).message
    assert "Cloud Build id" in claimed and "Cloud Build id" not in verified


def test_outcome_of() -> None:
    assert o.outcome_of(slices_failed=True, compare_rc=None) == o.OUTCOME_FAILED
    assert o.outcome_of(slices_failed=False, compare_rc=0) == o.OUTCOME_IDENTICAL
    assert o.outcome_of(slices_failed=False, compare_rc=2) == o.OUTCOME_DIFFERENT
    with pytest.raises(ValueError):
        o.outcome_of(slices_failed=False, compare_rc=None)


def test_judge_rejects_unknown_outcome() -> None:
    with pytest.raises(ValueError):
        o.judge(_decl(o.PARALLEL_ALLOWED), "maybe")


# --- validation ------------------------------------------------------------------

def test_declaration_validation() -> None:
    with pytest.raises(ValueError, match="parallel_slices"):
        _decl("sometimes")
    with pytest.raises(ValueError, match="status"):
        _decl(o.PARALLEL_ALLOWED, status="hoped")
    with pytest.raises(ValueError, match="source"):
        _decl(o.PARALLEL_ALLOWED, status=o.STATUS_VERIFIED, source="")
    with pytest.raises(ValueError, match="untested"):
        dataclasses.replace(_decl(o.PARALLEL_UNKNOWN, o.STATUS_UNTESTED), source="x")
    with pytest.raises(ValueError, match="reason"):
        dataclasses.replace(_decl(o.PARALLEL_ALLOWED), reason="")


def test_declarations_are_frozen() -> None:
    with pytest.raises(dataclasses.FrozenInstanceError):
        port_visits_o.ORCHESTRATION.steps[0].status = o.STATUS_VERIFIED  # type: ignore[misc]


def test_duplicate_steps_refused_and_unknown_step_named() -> None:
    step = _decl(o.PARALLEL_ALLOWED)
    with pytest.raises(ValueError, match="twice"):
        o.PipelineOrchestration(pipeline="p", steps=(step, step))
    with pytest.raises(KeyError, match="declares no step"):
        o.PipelineOrchestration(pipeline="p", steps=(step,)).step("other")


def test_airflow_guidance() -> None:
    allowed = port_visits_o.ORCHESTRATION.step(port_visits.STEP_THIN)
    assert o.airflow_guidance(allowed).startswith("parallel")
    assert o.airflow_guidance(segment_o.ORCHESTRATION.step("segment")).startswith("sequential")
    assert o.airflow_guidance(segment_o.ORCHESTRATION.step("segment_info")).startswith("unknown")


# --- the seeded declarations --------------------------------------------------------

def test_declared_steps_are_the_workflows_steps() -> None:
    assert [s.step for s in port_visits_o.ORCHESTRATION.steps] == list(port_visits.SELECTABLE_STEPS)
    assert [s.step for s in segment_o.ORCHESTRATION.steps] == list(imk.STEPS)
    assert [s.step for s in gaps_o.ORCHESTRATION.steps] == [gaps.STEP_NAME]
    assert [s.step for s in encounters_o.ORCHESTRATION.steps] == [
        "create_raw_encounters", "merge_encounters"]
    assert [s.step for s in events_o.ORCHESTRATION.steps] == [
        "incremental_events", "incremental_filter_events",
        "auth_and_regions_fishing_events", "fishing_restrictive"]
    # The pipe-events step names are the workflow's operations.
    assert fishing.CLI_ENTRYPOINT == "pipe"


def test_owner_claims_seeded_as_claimed_never_verified() -> None:
    expected = {
        ("anchorages_pipeline", "thin_port_messages"): o.PARALLEL_ALLOWED,
        ("encounters_pipeline", "create_raw_encounters"): o.PARALLEL_ALLOWED,
        ("pipe-events", "incremental_events"): o.PARALLEL_FORBIDDEN,
        ("pipe-gaps", "detect"): o.PARALLEL_FORBIDDEN,
        ("pipe-segment", "segment"): o.PARALLEL_FORBIDDEN,
    }
    for module in ALL:
        for step in module.ORCHESTRATION.steps:
            key = (module.ORCHESTRATION.pipeline, step.step)
            assert step.status != o.STATUS_VERIFIED, key
            if key in expected:
                assert step.parallel_slices == expected[key], key
                assert (step.status, step.source) == (o.STATUS_CLAIMED, o.OWNER_CLAIM_2026_11_24)
                assert step.depends_on_previous_run is (expected[key] == o.PARALLEL_FORBIDDEN)
            else:
                assert (step.parallel_slices, step.status) == (o.PARALLEL_UNKNOWN, o.STATUS_UNTESTED)


def test_gaps_claim_keeps_the_owners_reservation() -> None:
    assert "probably" in gaps_o.ORCHESTRATION.step("detect").reason


def test_summarize_lists_every_step() -> None:
    lines = o.summarize(segment_o.ORCHESTRATION)
    assert len(lines) == len(imk.STEPS)
    assert lines[0].startswith("pipe-segment/segment (segment): parallel slices forbidden")
