"""Per-workflow drift check against production's rendered DAGs (PIPELINE-4700).

DIT hand-copies what production runs into each workflow: the pipeline's
command line, its image version, its input tables. This module compares those
copies with composer-dags-production's render snapshots (read as data through
:mod:`dit.composer`) and reports where they differ. It changes nothing.

A workflow declares a :class:`WorkflowSpec` next to itself
(``workflows/<pkg>/composer_drift.py``): the snapshot it mirrors and one
:class:`StepSpec` per step, naming the composer task and how to build the
tokens DIT would run for that snapshot's run dates. ``workflows/composer_drift.py``
runs every spec.

**Three levels per step**, reported separately:

* **version**: the image name and pipeline version of DIT's default image
  against the production task's image.
* **command**: the entrypoint and command words (the argv's positionals).
* **flags**: by name and value. Flags a spec lists as DIT-owned or accepted
  are expected to differ and are left out of the diff. When the command
  differs, the flag differences are still listed but are *indicative only*:
  the two argvs belong to different CLI generations, so names and values do
  not line up.

A step whose tokens cannot be built offline (no pipeline package, no cloud
call) gives its command words instead (:attr:`StepSpec.command`) and gets the
version and command levels only.

**Expected differences** come in two kinds. *DIT-owned* flags carry DIT's own
values on purpose: outputs, job names, labels, service account, GCS paths.
*Accepted* differences are production flags DIT leaves out, or values DIT sets
differently, each with a written reason. Anything else that differs is drift.

**Frozen specs.** A spec may name its own composer commit
(:attr:`WorkflowSpec.composer_ref`). It is then read there, whatever ref the
run asks for. A workflow that can run an older pipeline version (its base plus
a declared adjustment, :mod:`dit.adjust`) uses this to check that older layer
against the last commit at which production ran that version. Checking only
the newest interface against production would leave the older layers
unchecked. A frozen spec cannot drift because production moved, only because
DIT changed, so it is a regression test of DIT.

Stdlib and :mod:`dit.composer` only. Nothing here runs a pipeline, docker or a
cloud call. The reports print production's argv values, so they are for local
reading; this repository is public.
"""
from __future__ import annotations

import dataclasses
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from datetime import date
from pathlib import Path

from dit import composer

#: Flags every Dataflow workflow of DIT sets to its own values on purpose: job
#: names and labels, its service account, GCS paths and subnetwork, its worker
#: cap, and its image path (the version inside the image is its own level).
#: Approved by the owner for port visits on 2026-10-23 (PR #86) and, for the
#: image flag, on 2026-11-19. Each spec adds its own output-table flags.
DATAFLOW_OWNED_FLAGS = frozenset({
    "--job_name",
    "--labels",
    "--service_account_email",
    "--temp_location",
    "--staging_location",
    "--subnetwork",
    "--max_num_workers",
    "--sdk_container_image",
})

#: Production Dataflow flags DIT leaves out on purpose, with the reason
#: (owner, 2026-10-23, for port visits). For workflows that pass no
#: ``--network`` of their own.
DATAFLOW_ACCEPTED_DIFFERENCES: Mapping[str, str] = {
    "--dataflow_service_options": "lineage stays off in DIT, so DIT's tables never "
                                  "enter production's lineage graph",
    "--network": "determined by DIT's --subnetwork",
}

#: Spellings of a value that mean the same thing, mapped to one form before
#: comparing. Beam resolves both runner names to the Dataflow runner (owner,
#: 2026-10-23: DIT keeps ``DataflowRunner``, production passes ``dataflow``).
RUNNER_EQUIVALENTS: Mapping[str, Mapping[str, str]] = {
    "--runner": {"DataflowRunner": "dataflow"},
}

#: gfw-common's ``--labels`` takes several values after one mention:
#: production passes ``--labels k=v k=v ...`` where DIT passes ``--labels=k=v``
#: repeated. Without the hint production's pairs read as positionals, i.e. as
#: part of the command.
LABELS_MULTI_VALUE = frozenset({"--labels"})

#: Builds the tokens DIT would run for a snapshot's run dates: entrypoint,
#: command words, flags. Takes the run's placeholder dates
#: (:func:`dit.composer.task_placeholders`).
TokenBuilder = Callable[[Mapping[str, date]], Sequence[str]]


# --------------------------------------------------------------------------
# Specs
# --------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class StepSpec:
    """One workflow step and the composer task it mirrors.

    Give ``tokens`` when DIT's argv can be built offline, else ``command``
    (entrypoint and command words, from the workflow's constants) for a
    version-and-command check. ``image`` is DIT's default image for the step;
    ``version`` names the pipeline version inside it when the image tag does
    not (a DIT overlay build). ``accepted`` holds differences accepted for this
    step only, flag name to reason. ``note`` says what the comparison leaves
    out, and is printed with the step.
    """

    step: str
    task_id: str
    image: str
    tokens: TokenBuilder | None = None
    command: tuple[str, ...] | None = None
    version: str | None = None
    accepted: Mapping[str, str] = dataclasses.field(default_factory=dict)
    note: str = ""

    def __post_init__(self) -> None:
        if (self.tokens is None) == (self.command is None):
            raise ValueError(f"step {self.step}: give exactly one of tokens and command")

    @property
    def image_name(self) -> str:
        return composer.image_name(self.image)

    @property
    def pipeline_version(self) -> str | None:
        return self.version or composer.image_version(self.image)


@dataclasses.dataclass(frozen=True)
class WorkflowSpec:
    """What one workflow mirrors in production, and what may differ.

    ``in_sync`` says whether the workflow is expected to match production
    today. Drift in such a workflow is a regression (exit 1); drift in any
    other is known work, reported but not failed on. ``note`` says why a
    workflow is not expected in sync, or what its check leaves out.

    ``field_checks`` adds checks beyond the argv (port visits'
    ``ProductionTarget`` fields), printed under ``fields_title``. ``bare`` and
    ``multi_value`` are :func:`dit.composer.parse_argv` hints, applied to both
    sides.

    ``composer_ref`` freezes the spec at one composer commit: it is read
    there, whatever ref the run asks for. That is for a workflow's older
    layer (its base plus the adjustment for an earlier pipeline version,
    :mod:`dit.adjust`), checked against the last commit at which production
    ran that version. Production cannot move such a spec; only a change in
    DIT can, so its drift is a regression of DIT. Left ``None``, the spec is
    read at the run's ref and says whether DIT mirrors production today.
    """

    name: str
    workflow: str
    snapshot: str
    steps: tuple[StepSpec, ...]
    dit_owned: frozenset[str] = frozenset()
    accepted: Mapping[str, str] = dataclasses.field(default_factory=dict)
    equivalents: Mapping[str, Mapping[str, str]] = dataclasses.field(default_factory=dict)
    multi_value: frozenset[str] = frozenset()
    bare: frozenset[str] = frozenset()
    field_checks: Callable[[composer.RenderedDag], Sequence[FieldCheck]] | None = None
    fields_title: str = "Fields"
    in_sync: bool = False
    note: str = ""
    composer_ref: str | None = None

    @property
    def frozen(self) -> bool:
        """Read at its own composer commit, not at the run's ref."""
        return self.composer_ref is not None

    def expected(self, step: StepSpec) -> frozenset[str]:
        """Every flag expected to differ in ``step``; a difference elsewhere is drift."""
        return self.dit_owned | frozenset(self.accepted) | frozenset(step.accepted)

    def reasons(self, step: StepSpec) -> dict[str, str]:
        """The accepted differences of ``step``, flag name to reason."""
        return {**self.accepted, **step.accepted}


# --------------------------------------------------------------------------
# Field checks
# --------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class FieldCheck:
    """One hand-copied value: DIT's and what composer passes."""

    field: str
    dit: str
    composer: str | None
    note: str = ""

    @property
    def matches(self) -> bool:
        return self.dit == self.composer


def single_value(parsed: composer.ParsedArgv, name: str) -> str | None:
    """The value of a flag given exactly once, else ``None``."""
    values = parsed.values(name)
    return values[0] if len(values) == 1 else None


# --------------------------------------------------------------------------
# Version, command and flags of one step
# --------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class VersionCheck:
    """DIT's default image against the production task's: name and version."""

    dit_image: str
    dit_version: str | None
    production_image: str | None
    production_version: str | None

    @property
    def matches(self) -> bool:
        return (self.dit_image, self.dit_version) == (
            self.production_image, self.production_version,
        )


@dataclasses.dataclass(frozen=True)
class ArgvComparison:
    """DIT's argv for one step against production's.

    ``diff`` is ``None`` when DIT's flags could not be built offline; then only
    the command (the positionals) is compared. ``expected`` lists the expected
    differences either side passes.
    """

    step: str
    dit: composer.ParsedArgv
    production: composer.ParsedArgv
    diff: composer.FlagDiff | None
    expected: tuple[str, ...] = ()

    @property
    def command_matches(self) -> bool:
        return self.dit.positionals == self.production.positionals

    @property
    def flags_match(self) -> bool:
        return self.diff is None or self.diff.identical

    @property
    def indicative(self) -> bool:
        """The command differs, so the flag differences only indicate the gap."""
        return self.diff is not None and not self.command_matches

    @property
    def drift(self) -> bool:
        """The command differs, or a flag does beyond the expected differences."""
        return not (self.command_matches and self.flags_match)


def canonical_values(
    parsed: composer.ParsedArgv, equivalents: Mapping[str, Mapping[str, str]],
) -> composer.ParsedArgv:
    """``parsed`` with each value that has an equivalent spelling written in one form."""
    return dataclasses.replace(parsed, flags=tuple(
        composer.Flag(f.name, equivalents.get(f.name, {}).get(f.value, f.value))
        for f in parsed.flags
    ))


def compare_argv(
    step: str, dit_tokens: Sequence[str], production_task: composer.RenderedTask,
    *, expected: Collection[str] = (),
    equivalents: Mapping[str, Mapping[str, str]] | None = None,
    multi: Collection[str] = (), bare: Collection[str] = (),
) -> ArgvComparison:
    """Compare DIT's argv (entrypoint first) with production's for ``step``.

    Both sides are parsed with the same ``multi`` and ``bare`` hints and their
    values go through :func:`canonical_values`. Flags in ``expected`` are taken
    out of the diff and listed in ``expected`` when either side passes them.
    """
    def parse(tokens: Sequence[str]) -> composer.ParsedArgv:
        return canonical_values(
            composer.parse_argv(tokens, multi=multi, bare=bare), equivalents or {},
        )

    dit = parse(dit_tokens)
    production = parse(production_task.argv)
    passed = set(dit.names()) | set(production.names())
    return ArgvComparison(
        step=step,
        dit=dit,
        production=production,
        diff=composer.diff_flags(dit, production, ignore=expected),
        expected=tuple(sorted(passed & set(expected))),
    )


def compare_command(
    step: str, command: Sequence[str], production_task: composer.RenderedTask,
    *, multi: Collection[str] = (), bare: Collection[str] = (),
) -> ArgvComparison:
    """Compare DIT's entrypoint and command words with production's; no flags."""
    return ArgvComparison(
        step=step,
        dit=composer.ParsedArgv(positionals=tuple(command), flags=()),
        production=composer.parse_argv(production_task.argv, multi=multi, bare=bare),
        diff=None,
    )


# --------------------------------------------------------------------------
# Reports
# --------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class StepReport:
    """The three levels of one step. ``reasons`` explains accepted differences."""

    spec: StepSpec
    task: composer.RenderedTask
    version: VersionCheck
    argv: ArgvComparison
    reasons: Mapping[str, str] = dataclasses.field(default_factory=dict)

    @property
    def drift(self) -> bool:
        return not self.version.matches or self.argv.drift


@dataclasses.dataclass(frozen=True)
class WorkflowReport:
    spec: WorkflowSpec
    dag: composer.RenderedDag
    fields: tuple[FieldCheck, ...]
    steps: tuple[StepReport, ...]

    @property
    def drift(self) -> bool:
        return any(not f.matches for f in self.fields) or any(s.drift for s in self.steps)

    @property
    def unexpected_drift(self) -> bool:
        """Drift in a workflow that is expected to be in sync: a regression."""
        return self.spec.in_sync and self.drift


@dataclasses.dataclass(frozen=True)
class WorkflowError:
    """A workflow whose snapshot or task could not be read."""

    spec: WorkflowSpec
    message: str


def check_step(spec: WorkflowSpec, step: StepSpec, dag: composer.RenderedDag) -> StepReport:
    """``step`` against its composer task in ``dag``. ``KeyError`` when the
    task, or a date the step's token builder needs, is missing."""
    task = dag.task(step.task_id)
    hints = {"multi": spec.multi_value, "bare": spec.bare}
    argv = (
        compare_command(step.step, step.command, task, **hints)
        if step.tokens is None
        else compare_argv(
            step.step, step.tokens(composer.task_placeholders(task)), task,
            expected=spec.expected(step), equivalents=spec.equivalents, **hints,
        )
    )
    return StepReport(
        spec=step,
        task=task,
        version=VersionCheck(step.image_name, step.pipeline_version,
                             task.image_name, task.version),
        argv=argv,
        reasons=spec.reasons(step),
    )


def check_workflow(spec: WorkflowSpec, dag: composer.RenderedDag) -> WorkflowReport:
    """Every step and field check of ``spec`` against ``dag``."""
    return WorkflowReport(
        spec=spec,
        dag=dag,
        fields=tuple(spec.field_checks(dag)) if spec.field_checks else (),
        steps=tuple(check_step(spec, step, dag) for step in spec.steps),
    )


#: What a snapshot that cannot be read, or lacks a task or a date, raises.
READ_ERRORS = (RuntimeError, KeyError, ValueError)


def _describe(exc: Exception) -> str:
    """A read error as one line. A bare ``KeyError`` is a missing key: a field
    the snapshot lacks, or a run date its interval does not have (an empty
    interval has no end date)."""
    if isinstance(exc, KeyError) and exc.args:
        detail = str(exc.args[0])
        return detail if " " in detail else f"missing key {detail!r}"
    return str(exc)


def run_specs(
    specs: Iterable[WorkflowSpec], composer_dir: Path, ref: str = composer.DEFAULT_REF,
) -> tuple[tuple[WorkflowReport, ...], tuple[WorkflowError, ...]]:
    """Check each spec against its snapshot: ``(reports, errors)``.

    A spec is read at ``ref``, or at its own ``composer_ref`` when it has one
    (a frozen spec). Each ref is resolved once, so every snapshot read at it
    comes from one commit, and each snapshot is loaded once per commit however
    many specs mirror it. A spec whose ref, snapshot or task cannot be read
    becomes an error and the others still run.
    """
    specs = tuple(specs)

    def wanted(spec: WorkflowSpec) -> str:
        return spec.composer_ref or ref

    def resolve(name: str) -> tuple[str | None, str]:
        """``(commit, "")``, or ``(None, why not)``."""
        try:
            return composer.resolve_ref(composer_dir, name), ""
        except RuntimeError as exc:
            return None, str(exc)

    commits = {name: resolve(name) for name in dict.fromkeys(map(wanted, specs))}

    def load(commit: str, snapshot: str) -> composer.RenderedDag | str:
        try:
            return composer.load_dag(composer_dir, snapshot, commit)
        except READ_ERRORS as exc:
            return _describe(exc)

    keys = dict.fromkeys((commits[wanted(spec)][0], spec.snapshot) for spec in specs)
    dags = {
        (commit, snapshot): load(commit, snapshot)
        for commit, snapshot in keys if commit is not None
    }

    def check(spec: WorkflowSpec) -> WorkflowReport | WorkflowError:
        commit, unresolved = commits[wanted(spec)]
        if commit is None:
            return WorkflowError(spec, unresolved)
        dag = dags[commit, spec.snapshot]
        if isinstance(dag, str):
            return WorkflowError(spec, dag)
        try:
            return check_workflow(spec, dag)
        except READ_ERRORS as exc:
            return WorkflowError(spec, _describe(exc))

    results = tuple(map(check, specs))
    return (
        tuple(r for r in results if isinstance(r, WorkflowReport)),
        tuple(r for r in results if isinstance(r, WorkflowError)),
    )


def exit_code(
    reports: Sequence[WorkflowReport], errors: Sequence[WorkflowError], *, strict: bool = False,
) -> int:
    """2 when something could not be read; else 1 when a workflow expected in
    sync drifts (with ``strict``: when any workflow drifts); else 0."""
    if errors:
        return 2
    return int(any(r.drift if strict else r.unexpected_drift for r in reports))


# --------------------------------------------------------------------------
# Formatting
# --------------------------------------------------------------------------

def _show(parsed: composer.ParsedArgv, name: str) -> str:
    return " ".join(name if v is None else f"{name}={v}" for v in parsed.values(name))


def _format_check(check: FieldCheck) -> str:
    return (
        f"  {'ok  ' if check.matches else 'DIFF'}  {check.field:<24} dit={check.dit}"
        + ("" if check.matches else f"  composer={check.composer}")
        + (f"  ({check.note})" if check.note else "")
    )


def _version_text(image: str | None, version: str | None) -> str:
    return f"{image or 'no image'} {version or 'untagged'}"


def _format_step(step: StepReport) -> list[str]:
    version, argv = step.version, step.argv
    dit_version = _version_text(version.dit_image, version.dit_version)
    lines = [
        "",
        f"{step.spec.step} (composer task {step.spec.task_id}):",
        *([f"  note: {step.spec.note}"] if step.spec.note else []),
        "  version: " + (
            f"same ({dit_version})" if version.matches
            else f"DIFF  dit={dit_version}  production="
                 f"{_version_text(version.production_image, version.production_version)}"
        ),
        "  command: " + (
            "same (" + " ".join(argv.dit.positionals) + ")" if argv.command_matches
            else f"DIFF  dit={' '.join(argv.dit.positionals)}  "
                 f"production={' '.join(argv.production.positionals)}"
        ),
    ]
    diff = argv.diff
    if diff is None:
        return [*lines, "  flags: not compared (DIT's argv cannot be built offline)"]

    in_dit, in_production = set(argv.dit.names()), set(argv.production.names())

    def expected(name: str) -> str:
        """The flag, which side passes it when only one does, and its reason."""
        side = (
            "" if name in in_dit and name in in_production
            else " [production only]" if name in in_production else " [DIT only]"
        )
        reason = step.reasons.get(name)
        return f"{name}{side}" + (f" ({reason})" if reason else "")

    return [
        *lines,
        "  flags" + (
            " (indicative only: the command differs, so the two sides are "
            "different CLI generations):" if argv.indicative else ":"
        ),
        "    same: " + (", ".join(diff.same) or "-"),
        "    expected to differ: " + (", ".join(map(expected, argv.expected)) or "-"),
        "    only in production: " + (
            "; ".join(_show(argv.production, n) for n in diff.only_right) or "-"),
        "    only in DIT: " + ("; ".join(_show(argv.dit, n) for n in diff.only_left) or "-"),
        "    values differ: " + ("; ".join(
            f"{n}: dit={_show(argv.dit, n)} production={_show(argv.production, n)}"
            for n in diff.differing
        ) or "-"),
    ]


def verdict(report: WorkflowReport) -> str:
    """One phrase: whether the workflow drifts, and whether that is known."""
    if report.spec.in_sync:
        return "DRIFT (expected in sync)" if report.drift else "in sync"
    return (
        "drift (known: not expected in sync yet)" if report.drift
        else "no drift (can be marked in sync)"
    )


def format_report(report: WorkflowReport) -> str:
    """The full detail of one workflow. Prints production's argv values: for
    local reading only."""
    dag, spec = report.dag, report.spec
    interval = dag.data_interval
    lines = [
        f"{spec.name} ({spec.workflow})",
        f"composer-dags-production @ {(dag.commit or 'unknown')[:12]}, snapshot {dag.snapshot} "
        f"({dag.dag_id}), run [{interval.start.isoformat()}, {interval.end.isoformat()})",
        *(["frozen: read at this commit whatever ref the run asks for"] if spec.frozen else []),
        *([f"note: {spec.note}"] if spec.note else []),
    ]
    if report.fields:
        lines += ["", f"{spec.fields_title}:", *map(_format_check, report.fields)]
    for step in report.steps:
        lines += _format_step(step)
    lines += [
        "",
        f"expected in sync: {'yes' if spec.in_sync else 'no'}",
        "drift: " + ("yes" if report.drift else "no"),
    ]
    return "\n".join(lines)


def _flag_counts(argv: ArgvComparison) -> tuple[str, str, str]:
    diff = argv.diff
    if diff is None:
        return ("-", "-", "-")
    mark = "*" if argv.indicative else ""
    return (
        f"{len(diff.only_right)}{mark}",
        f"{len(diff.only_left)}{mark}",
        f"{len(diff.differing)}{mark}",
    )


def _version_cell(version: VersionCheck) -> str:
    """``ok v1`` or ``DIFF v1 -> v2``; image names only where they differ."""
    if version.matches:
        return f"ok {version.dit_version}"
    if version.dit_image == version.production_image:
        return f"DIFF {version.dit_version} -> {version.production_version}"
    return (
        f"DIFF {_version_text(version.dit_image, version.dit_version)} -> "
        f"{_version_text(version.production_image, version.production_version)}"
    )


def _summary_row(step: StepReport) -> tuple[str, ...]:
    argv = step.argv
    return (
        step.spec.step,
        step.spec.task_id,
        _version_cell(step.version),
        "ok" if argv.command_matches
        else f"DIFF, production: {' '.join(argv.production.positionals)}",
        *_flag_counts(argv),
    )


_SUMMARY_HEADER = (
    "step", "composer task", "version (DIT -> production)", "command",
    "only prod", "only DIT", "differ",
)


def _workflow_line(report: WorkflowReport) -> str:
    bad_fields = sum(not f.matches for f in report.fields)
    fields = (
        f"; {len(report.fields)} field checks, {bad_fields} differ" if report.fields else ""
    )
    note = f". Note: {report.spec.note}" if report.spec.note and report.drift else ""
    frozen = (
        f"; frozen at composer {(report.dag.commit or 'unknown')[:12]}"
        if report.spec.frozen else ""
    )
    return f"{report.spec.name}: {verdict(report)}{fields}{frozen}{note}"


def format_summary(
    reports: Sequence[WorkflowReport], errors: Sequence[WorkflowError] = (),
) -> str:
    """One verdict line per workflow, each followed by one row per step.

    The three flag columns count unexpected differences: flags only production
    passes, flags only DIT passes, and flags whose values differ. A ``*`` marks
    counts that are indicative only, because the command differs. The summary
    holds versions, command names and counts, no argv values.
    """
    commits = sorted({(r.dag.commit or "unknown")[:12] for r in reports})
    rows = [_SUMMARY_HEADER, *(_summary_row(s) for r in reports for s in r.steps)]
    widths = [max(len(row[i]) for row in rows) for i in range(len(_SUMMARY_HEADER))]

    def line(row: Sequence[str]) -> str:
        cells = (cell.ljust(width) for cell, width in zip(row, widths, strict=True))
        return ("  " + "  ".join(cells)).rstrip()

    return "\n".join([
        f"composer-dags-production @ {', '.join(commits) or 'unread'}",
        *(["", line(_SUMMARY_HEADER)] if reports else []),
        *(
            text
            for report in reports
            for text in ("", _workflow_line(report), *map(line, map(_summary_row, report.steps)))
        ),
        *(["", "* indicative only: the command differs, so flag names and values "
               "belong to different CLI generations."]
          if any(s.argv.indicative for r in reports for s in r.steps) else []),
        *(text for e in errors for text in ("", f"{e.spec.name}: could not be read: {e.message}")),
    ])
