"""Per-step orchestration declarations, and judging a test run against them.

Some pipeline steps can load several date slices at the same time; others
carry state from one run to the next and must run one slice after the other.
That decides how a step may be orchestrated: whether an Airflow backfill may
run several intervals concurrently, or needs ``depends_on_past`` and
``max_active_runs=1``. DIT records it per step, one file per pipeline
(``workflows/<pkg>/orchestration.py``), so it is written down and can be
tested.

A declaration says, for one step:

* ``parallel_slices``: ``allowed``, ``forbidden`` or ``unknown``.
* ``depends_on_previous_run``: ``True``, ``False`` or ``None`` (unknown).
* ``status``: ``verified`` (``source`` is the Cloud Build id of the run that
  showed it), ``claimed`` (``source`` says who claimed it, and when), or
  ``untested``.
* ``reason``: why, in words.

Nothing writes ``verified`` automatically. A run's result reaches a
declaration only through a human edit of the file.

:func:`judge` compares a parallel-slices test outcome with the declaration.
Forbidden steps are run too, as negative tests: a difference (or failed jobs)
is then the declared behaviour, and no difference is reported as a sign that
the declaration may be too strict. The rules:

=================  ==========  ====================================  ====
declared           outcome     verdict                               exit
=================  ==========  ====================================  ====
allowed            identical   pass                                  0
allowed            different   fail                                  1
allowed            failed      fail                                  1
forbidden          different   as declared                           0
forbidden          failed      as declared                           0
forbidden          identical   declaration may be too strict         0
unknown            identical   no difference (declaration unknown)   0
unknown            different   difference (declaration unknown)      1
unknown            failed      jobs failed (declaration unknown)     1
=================  ==========  ====================================  ====

With ``unknown`` a workflow's normal rule applies: a difference fails.
"""
from __future__ import annotations

import dataclasses
from collections.abc import Sequence

PARALLEL_ALLOWED = "allowed"
PARALLEL_FORBIDDEN = "forbidden"
PARALLEL_UNKNOWN = "unknown"
PARALLEL_CHOICES = (PARALLEL_ALLOWED, PARALLEL_FORBIDDEN, PARALLEL_UNKNOWN)

STATUS_VERIFIED = "verified"
STATUS_CLAIMED = "claimed"
STATUS_UNTESTED = "untested"
STATUS_CHOICES = (STATUS_VERIFIED, STATUS_CLAIMED, STATUS_UNTESTED)

#: The source of the claims seeded on 2026-11-24.
OWNER_CLAIM_2026_11_24 = "owner, 2026-11-24, not verified"

OUTCOME_IDENTICAL = "identical"
OUTCOME_DIFFERENT = "different"
OUTCOME_FAILED = "failed"
OUTCOME_CHOICES = (OUTCOME_IDENTICAL, OUTCOME_DIFFERENT, OUTCOME_FAILED)

VERDICT_PASS = "pass"
VERDICT_FAIL = "fail"
VERDICT_AS_DECLARED = "as declared"
VERDICT_TOO_STRICT = "declaration may be too strict"
VERDICT_UNDECLARED = "declaration unknown"


@dataclasses.dataclass(frozen=True)
class StepOrchestration:
    """What one pipeline step allows. See the module docstring."""

    step: str
    command: str
    parallel_slices: str
    depends_on_previous_run: bool | None
    status: str
    source: str
    reason: str

    def __post_init__(self) -> None:
        if self.parallel_slices not in PARALLEL_CHOICES:
            raise ValueError(
                f"{self.step}: parallel_slices {self.parallel_slices!r}; "
                f"valid: {', '.join(PARALLEL_CHOICES)}"
            )
        if self.status not in STATUS_CHOICES:
            raise ValueError(
                f"{self.step}: status {self.status!r}; valid: {', '.join(STATUS_CHOICES)}"
            )
        if self.status in (STATUS_VERIFIED, STATUS_CLAIMED) and not self.source:
            raise ValueError(
                f"{self.step}: a {self.status} declaration names its source "
                f"(the Cloud Build id for verified, who claimed it for claimed)"
            )
        if self.status == STATUS_UNTESTED and self.source:
            raise ValueError(f"{self.step}: an untested declaration has no source")
        if not self.reason:
            raise ValueError(f"{self.step}: a declaration gives its reason")


@dataclasses.dataclass(frozen=True)
class PipelineOrchestration:
    """Every declared step of one pipeline, in pipeline order."""

    pipeline: str
    steps: tuple[StepOrchestration, ...]

    def __post_init__(self) -> None:
        names = [step.step for step in self.steps]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError(f"{self.pipeline}: steps declared twice: {', '.join(duplicates)}")

    def step(self, name: str) -> StepOrchestration:
        """The declaration of ``name``; ``KeyError`` naming the declared steps."""
        for step in self.steps:
            if step.step == name:
                return step
        raise KeyError(
            f"{self.pipeline} declares no step {name!r}; declared: "
            f"{', '.join(step.step for step in self.steps)}"
        )


@dataclasses.dataclass(frozen=True)
class Judgement:
    """The verdict on one parallel-slices test, and the exit code it implies."""

    verdict: str
    exit_code: int
    warning: bool
    message: str


def _declared(declaration: StepOrchestration) -> str:
    source = f", source: {declaration.source}" if declaration.source else ""
    return (
        f"{declaration.step} declares parallel slices {declaration.parallel_slices} "
        f"({declaration.status}{source})"
    )


_OUTCOME_TEXT = {
    OUTCOME_IDENTICAL: "the parallel slices gave the same output as one job over the window",
    OUTCOME_DIFFERENT: "the parallel slices gave a different output than one job over the window",
    OUTCOME_FAILED: "at least one parallel slice failed",
}


def judge(declaration: StepOrchestration, outcome: str) -> Judgement:
    """The verdict on ``outcome`` (``identical``, ``different`` or
    ``failed``) for a step declared as ``declaration``. Pure; the rules are in
    the module docstring."""
    if outcome not in OUTCOME_CHOICES:
        raise ValueError(f"outcome {outcome!r}; valid: {', '.join(OUTCOME_CHOICES)}")
    head = f"{_OUTCOME_TEXT[outcome]}; {_declared(declaration)}"
    record_hint = (
        f" A run shows it only once someone records it: edit the declaration "
        f"(status {STATUS_VERIFIED}, source: the Cloud Build id of this run)."
    )
    allowed = declaration.parallel_slices == PARALLEL_ALLOWED
    forbidden = declaration.parallel_slices == PARALLEL_FORBIDDEN
    if allowed and outcome == OUTCOME_IDENTICAL:
        return Judgement(VERDICT_PASS, 0, False, f"{head}. As declared." + (
            record_hint if declaration.status != STATUS_VERIFIED else ""))
    if allowed:
        return Judgement(VERDICT_FAIL, 1, False, (
            f"{head}. The declaration says slices may run in parallel: either the step "
            f"has a defect under concurrent loads, or the declaration is wrong."
        ))
    if forbidden and outcome == OUTCOME_IDENTICAL:
        return Judgement(VERDICT_TOO_STRICT, 0, True, (
            f"{head}. WARNING: no difference where one was expected. The declaration may "
            f"be too strict, or the window and slices did not reach the state the step "
            f"carries between runs. Not changed automatically."
        ))
    if forbidden:
        failed_note = (
            " Check the jobs' errors: a failure for another reason (quota, permissions) "
            "also reads as declared here."
            if outcome == OUTCOME_FAILED else ""
        )
        return Judgement(VERDICT_AS_DECLARED, 0, False, (
            f"{head}. As declared: this step must not load slices in parallel.{failed_note}"
        ))
    exit_code = 0 if outcome == OUTCOME_IDENTICAL else 1
    return Judgement(VERDICT_UNDECLARED, exit_code, False, (
        f"{head}. No declaration to judge against, so the workflow's normal rule applies "
        f"(a difference fails).{record_hint if outcome == OUTCOME_IDENTICAL else ''}"
    ))


def outcome_of(*, slices_failed: bool, compare_rc: int | None) -> str:
    """The outcome of a parallel-slices test: ``failed`` if a slice failed,
    else ``identical`` for comparison exit code 0 and ``different`` otherwise.
    A comparison that errored (a missing table, say) also reads as
    ``different``; the comparison's own log says which it was."""
    if slices_failed:
        return OUTCOME_FAILED
    if compare_rc is None:
        raise ValueError("no slice failed, so a comparison exit code is needed")
    return OUTCOME_IDENTICAL if compare_rc == 0 else OUTCOME_DIFFERENT


def airflow_guidance(declaration: StepOrchestration) -> str:
    """What the declaration means for the step's Airflow task, in one line."""
    if declaration.parallel_slices == PARALLEL_FORBIDDEN or declaration.depends_on_previous_run:
        return (
            "sequential: depends_on_past=True and max_active_runs=1 on the DAG; a backfill "
            "runs one interval after the other, in date order"
        )
    if declaration.parallel_slices == PARALLEL_ALLOWED and declaration.depends_on_previous_run is False:
        return (
            "parallel: a backfill may run several intervals at once (max_active_runs > 1, "
            "no depends_on_past); each interval rewrites only its own dates"
        )
    return "unknown: treat as sequential until a test shows otherwise"


def summarize(pipeline: PipelineOrchestration) -> Sequence[str]:
    """One line per declared step, for logs and docs."""
    def depends(value: bool | None) -> str:
        return {True: "yes", False: "no", None: "unknown"}[value]

    return tuple(
        f"{pipeline.pipeline}/{step.step} ({step.command}): parallel slices "
        f"{step.parallel_slices}, depends on previous run {depends(step.depends_on_previous_run)}, "
        f"{step.status}{f' ({step.source})' if step.source else ''}"
        for step in pipeline.steps
    )
