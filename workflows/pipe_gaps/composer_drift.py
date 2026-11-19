"""Drift spec of the pipe-gaps mode-equivalence workflow (PIPELINE-4700).

Read-only: the spec ``workflows/composer_drift.py`` runs for
``pipe_gaps/mode_equivalence.py`` against the raw-gaps task of the AIS v5 DAG.
See :mod:`dit.drift` for the check itself. ``outage_recovery.py`` drives the
same pipe-gaps generation with its own copy of the config builder and has no
spec of its own.

**Which tokens.** With its default Dataflow runner the workflow has no command
line: it builds a pipe-gaps config object in-process and hands Dataflow
options to gfw-common as a mapping. Building that needs the pipe-gaps package
at the version the workflow was written for. What can be built offline is the
argv of the workflow's docker runner (``--runner docker``): the same config,
written as ``pipe-gaps detect`` flags by ``_cfg_to_cli_flags``. The spec
compares that argv. So the flags level covers the pipeline's own flags; the
Dataflow options production passes on its command line are accepted
differences here, and the worker sizing DIT mirrors is checked as two fields
instead (:func:`sizing_checks`).

DIT's tokens are those of one detect run at the workflow's defaults over the
snapshot run's half-open ``[start, end)``.

**Not expected in sync today.** The workflow builds a v0.10.0 config and runs
``detect``. Production's AIS v5 DAG runs a later pipe-gaps generation with
another command and renamed flags, so the command level differs and the flag
differences are indicative only. Porting the workflow is the fix; this check
is its acceptance test.
"""
from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from typing import Any

from dit import composer, drift
from workflows.pipe_gaps import mode_equivalence as mode

#: The composer task the detect step mirrors (snapshot ``core_ais_v5``).
TASK_ID = "gaps.detect_raw_gaps"

#: Entrypoint and command of the docker runner, as ``mode._run_pipeline``
#: passes them. A test pins that these tokens equal that call.
CLI_ENTRYPOINT = "pipe-gaps"
COMMAND = "detect"

#: Flags DIT sets to its own values on purpose: its output table, plus the
#: Dataflow flags (which DIT never writes on a command line here).
DIT_OWNED_FLAGS = drift.DATAFLOW_OWNED_FLAGS | frozenset({"--bq-output-gaps"})

_IN_PROCESS = (
    "a Dataflow option: DIT submits in-process and passes it as an option, "
    "not on a command line"
)

#: Production flags the docker-runner argv cannot show, with the reason.
ACCEPTED_DIFFERENCES = {
    **drift.DATAFLOW_ACCEPTED_DIFFERENCES,
    "--runner": _IN_PROCESS,
    "--region": _IN_PROCESS,
    "--no_use_public_ips": _IN_PROCESS + "; gfw-common's default is private IPs",
    "--worker_machine_type": _IN_PROCESS + "; checked as a field",
    "--disk_size_gb": _IN_PROCESS + "; checked as a field",
}

_NOTE = (
    "builds a v0.10.0 config and runs detect; production runs a later pipe-gaps "
    "generation with another command and renamed flags"
)

_STEP_NOTE = (
    "DIT's tokens are the docker runner's argv; the Dataflow runner builds the "
    "same config in-process, so Dataflow options are not on a command line"
)


def detect_params(args: Any) -> dict[str, Any]:
    """The detect parameters every mode shares, from the parsed args: the
    pipeline-level entries of ``mode.main()``'s ``base_cfg``. A test pins that
    the two agree."""
    return dict(
        bq_input_messages=args.source_messages,
        bq_input_segments=args.source_segments,
        ssvids=tuple(s.strip() for s in args.ssvids.split(",") if s.strip()),
        min_gap_length=args.min_gap_length,
        n_hours_before=args.n_hours_before,
        window_period_d=args.window_period_d,
        filter_good_seg=(args.filter_good_seg == "True"),
        skip_open_gaps=args.skip_open_gaps,
    )


def dit_detect_tokens(placeholders: Mapping[str, date]) -> list[str]:
    """What the workflow's docker runner runs for one detect over the snapshot
    run's half-open ``[start_date, end_date_exclusive)``."""
    start, end = placeholders["start_date"], placeholders["end_date_exclusive"]
    args = mode.parse_args([
        "--runner", "docker", "--experiment-id", "composer-drift",
        "--start", start.isoformat(), "--end", end.isoformat(),
    ])
    cfg = mode._make_config(
        start=start, end=end,
        bq_output_gaps=f"{mode.PROJECT}.{args.dest_dataset}.three_way_drift_{mode.MODE_BF}",
        **detect_params(args),
    )
    return [CLI_ENTRYPOINT, COMMAND, *mode._cfg_to_cli_flags(cfg), "--project", mode.PROJECT]


def sizing_checks(dag: composer.RenderedDag) -> tuple[drift.FieldCheck, ...]:
    """The worker sizing DIT mirrors (``mode.DEFAULT_WORKER_SIZING``) against
    the production task's flags."""
    parsed = composer.parse_argv(dag.task(TASK_ID).argv, multi=drift.LABELS_MULTI_VALUE)
    sizing = mode.DEFAULT_WORKER_SIZING
    return (
        drift.FieldCheck("worker_machine_type", str(sizing.machine_type),
                         drift.single_value(parsed, "--worker_machine_type")),
        drift.FieldCheck("disk_size_gb", str(sizing.disk_size_gb),
                         drift.single_value(parsed, "--disk_size_gb")),
    )


SPEC = drift.WorkflowSpec(
    name="pipe_gaps/mode_equivalence",
    workflow="workflows/pipe_gaps/mode_equivalence.py",
    snapshot="core_ais_v5",
    steps=(
        drift.StepSpec(
            step=mode.STEP_NAME,
            task_id=TASK_ID,
            image=mode.DEFAULT_WORKER_IMAGE,
            tokens=dit_detect_tokens,
            note=_STEP_NOTE,
        ),
    ),
    dit_owned=DIT_OWNED_FLAGS,
    accepted=ACCEPTED_DIFFERENCES,
    multi_value=drift.LABELS_MULTI_VALUE,
    field_checks=sizing_checks,
    fields_title="Worker sizing (mode_equivalence.DEFAULT_WORKER_SIZING)",
    in_sync=False,
    note=_NOTE,
)

#: The specs ``workflows/composer_drift.py`` runs for this package.
SPECS = (SPEC,)
