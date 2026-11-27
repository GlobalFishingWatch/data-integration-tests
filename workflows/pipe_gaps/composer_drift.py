"""Drift spec of the pipe-gaps mode-equivalence workflow (PIPELINE-4700).

Read-only: the spec ``workflows/composer_drift.py`` runs for
``pipe_gaps/mode_equivalence.py`` against the raw-gaps task of the AIS v5 DAG.
See :mod:`dit.drift` for the check itself. ``outage_recovery.py`` runs the
same job (``workflows/pipe_gaps/raw_gaps.py``) and has no spec of its own.

**Which tokens.** With its default Dataflow runner the workflow has no command
line: it builds a pipe-gaps config object in-process and hands Dataflow
options to gfw-common as a mapping. What can be built offline is the argv of
the workflow's docker runner (``--runner docker``): the same config, written
as ``pipe-gaps raw-gaps`` flags by ``raw_gaps.cfg_to_cli_flags``. The spec
compares that argv. So the flags level covers the pipeline's own flags; the
Dataflow options production passes on its command line are accepted
differences here, and the worker sizing DIT mirrors is checked as two fields
instead (:func:`sizing_checks`).

DIT's tokens are those of one daily job of the workflow's daily modes at its
defaults, for the snapshot run's day: ``[end - --backfill-days, end)``, with
``end`` the run's exclusive end.

**In sync since 2026-11-27**, when the workflows moved to pipe-gaps v0.17.1.
What still differs is accepted, each with its reason
(:data:`ACCEPTED_DIFFERENCES`): DIT's staging inputs, its smaller
``min_gap_length``, segment stabilization off, the daily job's window, the
window period, and flags of production's operator that change no output.
"""
from __future__ import annotations

from collections.abc import Mapping
from datetime import date, timedelta
from typing import Any

from dit import composer, drift
from workflows.pipe_gaps import mode_equivalence as mode
from workflows.pipe_gaps import raw_gaps

#: The composer task the detect step mirrors (snapshot ``core_ais_v5``).
TASK_ID = "gaps.detect_raw_gaps"

#: Entrypoint and command of the docker runner, as ``raw_gaps.run_pipeline``
#: passes them. A test pins that these tokens equal that call.
CLI_ENTRYPOINT = raw_gaps.CLI_ENTRYPOINT
COMMAND = raw_gaps.COMMAND

#: Flags DIT sets to its own values on purpose: its output view, plus the
#: Dataflow flags (which DIT never writes on a command line here).
DIT_OWNED_FLAGS = drift.DATAFLOW_OWNED_FLAGS | frozenset({"--bq-out-gaps"})

_IN_PROCESS = (
    "a Dataflow option: DIT submits in-process and passes it as an option, "
    "not on a command line"
)

_STAGING_INPUT = "DIT reads the AIS staging cohort by design (README, Source data targets)"

#: Production flags the docker-runner argv cannot show or DIT sets otherwise
#: on purpose, with the reason. The owner decided the min-gap-length and
#: stabilization entries (2026-11-27). The --no-rich-logging entry extends the
#: owner's acceptance for port visits (2026-11-19, PR #91) to pipe-gaps.
ACCEPTED_DIFFERENCES = {
    **drift.DATAFLOW_ACCEPTED_DIFFERENCES,
    "--runner": _IN_PROCESS,
    "--region": _IN_PROCESS,
    "--no_use_public_ips": _IN_PROCESS + "; gfw-common's default is private IPs",
    "--worker_machine_type": _IN_PROCESS + "; checked as a field",
    "--disk_size_gb": _IN_PROCESS + "; checked as a field",
    "--wait_for_job": "DIT's in-process runner waits for every job itself "
                      "(dit.runners.dataflow)",
    "--no-rich-logging": "log format only: plain instead of coloured output "
                         "(accepted for port visits by the owner, 2026-11-19)",
    "--bq-in-messages": _STAGING_INPUT,
    "--bq-in-segments": _STAGING_INPUT,
    "--min-gap-length": "DIT's default is 1 hour, production's 4: on the 142 vessels "
                        "of the staging cohort a lower threshold gives more gaps and "
                        "boundary cases (owner, 2026-11-27); --min-gap-length 4 runs "
                        "production's",
    "--good-seg-stabilization-days": "0 in DIT, 1 in production: the staging cohort "
                                     "does not change, so there is nothing to wait for "
                                     "(owner, 2026-11-27)",
    "--start-date": "a DIT daily job reads back --backfill-days days (4 by default); "
                    "production's daily task covers 2 days. The end date is compared",
    "--window-period-d": "DIT passes 2 days; production passes none, and pipe-gaps then "
                         "takes the job's range, which is 2 days on production's daily "
                         "task. Fixed in DIT so a long job has many window boundaries",
}

_STEP_NOTE = (
    "DIT's tokens are the docker runner's argv of one daily job; the Dataflow runner "
    "builds the same config in-process, so Dataflow options are not on a command line"
)


def detect_params(args: Any) -> dict[str, Any]:
    """The detect parameters every mode shares, from the parsed args: the
    pipeline-level entries of ``mode.main()``'s ``base_cfg``. A test pins that
    the two agree."""
    return dict(
        bq_in_messages=args.source_messages,
        bq_in_segments=args.source_segments,
        ssvids=tuple(s.strip() for s in args.ssvids.split(",") if s.strip()),
        min_gap_length=args.min_gap_length,
        n_hours_before=args.n_hours_before,
        window_period_d=args.window_period_d,
        filter_good_seg=(args.filter_good_seg == "True"),
        skip_open_gaps=args.skip_open_gaps,
    )


#: Labels of the drift tokens. ``--labels`` is DIT-owned, so their values are
#: not compared; v0.17.1 requires at least one.
DRIFT_LABELS = ("dit_run_id=composer-drift",)


def daily_window(placeholders: Mapping[str, date], backfill_days: int) -> tuple[date, date]:
    """``[end - backfill_days, end)``: the daily job ``mode_equivalence`` runs
    for the snapshot run's last day (``end`` its exclusive end)."""
    end = placeholders["end_date_exclusive"]
    return end - timedelta(days=backfill_days), end


def dit_detect_tokens(placeholders: Mapping[str, date]) -> list[str]:
    """What the workflow's docker runner runs for the daily job of the
    snapshot run's day."""
    args = mode.parse_args(["--runner", "docker", "--experiment-id", "composer-drift"])
    start, end = daily_window(placeholders, args.backfill_days)
    cfg = raw_gaps.make_config(
        start=start, end=end,
        bq_out_gaps=f"{raw_gaps.PROJECT}.{args.dest_dataset}.three_way_drift_{mode.MODE_BFD}",
        labels=DRIFT_LABELS,
        **detect_params(args),
    )
    return [CLI_ENTRYPOINT, *raw_gaps.docker_argv(cfg)]


def sizing_checks(dag: composer.RenderedDag) -> tuple[drift.FieldCheck, ...]:
    """The worker sizing DIT mirrors (``raw_gaps.DEFAULT_WORKER_SIZING``)
    against the production task's flags."""
    parsed = composer.parse_argv(dag.task(TASK_ID).argv, multi=drift.LABELS_MULTI_VALUE)
    sizing = raw_gaps.DEFAULT_WORKER_SIZING
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
            step=raw_gaps.STEP_NAME,
            task_id=TASK_ID,
            image=raw_gaps.DEFAULT_WORKER_IMAGE,
            tokens=dit_detect_tokens,
            note=_STEP_NOTE,
        ),
    ),
    dit_owned=DIT_OWNED_FLAGS,
    accepted=ACCEPTED_DIFFERENCES,
    multi_value=drift.LABELS_MULTI_VALUE,
    field_checks=sizing_checks,
    fields_title="Worker sizing (raw_gaps.DEFAULT_WORKER_SIZING)",
    in_sync=True,
)

#: The specs ``workflows/composer_drift.py`` runs for this package.
SPECS = (SPEC,)
