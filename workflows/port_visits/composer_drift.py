"""Port-visits drift check against production's rendered DAG (PIPELINE-4700).

Read-only. The spec of ``workflows/port_visits/ais.py`` for the general drift
check (:mod:`dit.drift`, run for every workflow by
``workflows/composer_drift.py``), plus this workflow's own command line. It
loads the rendered DAG behind a ``ProductionTarget`` from
composer-dags-production (through :mod:`dit.composer`: data, not code) and
reports, changing nothing:

1. each ``ProductionTarget`` field (``cross_version_ais.PRODUCTION_TARGETS``)
   against what the DAG's port-visits tasks pass;
2. per step, the pipe-anchorages version DIT's default image holds
   (``ais.DEFAULT_PIPELINE_VERSION``) against the image tag of the task;
3. per step, the command and the flags DIT's argv builders
   (``ais._thin_argv`` / ``ais._visits_argv``) emit against production's argv
   for the same run.

The flag names are those of the pipe-anchorages CLI production runs (v5.0.1:
``port-state-transitions`` / ``port-visits``, ``--bq-in-*`` / ``--bq-out-*``).
Production passes ``--labels`` once, with space-separated pairs; both sides are
parsed with that in mind (:data:`MULTI_VALUE_FLAGS`).

DIT is configured the way ``--baseline-production`` configures it: the target's
tables, its history start, and the snapshot run's dates. A value that differs is
therefore a real difference, not a different run. Flags expected to differ are
listed apart (:data:`EXPECTED_DIFFERENCES`): those DIT sets to its own values on
purpose (:data:`DIT_OWNED_FLAGS`: outputs, identity, placement, labels, job
names, worker cap, image) and the accepted ones (:data:`ACCEPTED_DIFFERENCES`,
and :data:`THIN_ACCEPTED_DIFFERENCES` for step 1 only). Spellings that mean the
same value are compared as one (:data:`EQUIVALENT_VALUES`).

Exit code: 0 when nothing differs beyond the expected differences, 1 otherwise,
2 when the snapshot or a task cannot be read. Mismatches are for the workflow
owner to judge; this check never edits ``ProductionTarget`` or the argv builders.

    dit run workflows/port_visits/composer_drift.py \\
        --composer-dir ../composer-dags-production --ref origin/main

The output includes production argv values (service accounts, buckets, network)
for the flags DIT does not emit. Read it locally; do not paste it into this
repository, which is public.
"""
from __future__ import annotations

import argparse
import dataclasses
import functools
import re
import sys
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path

from dit import composer, drift
from dit.drift import FieldCheck
from workflows.port_visits import ais
from workflows.port_visits.cross_version_ais import PRODUCTION_TARGETS, ProductionTarget


@dataclasses.dataclass(frozen=True)
class TargetSource:
    """Where in composer a production target's port-visits tasks live."""

    snapshot: str
    thin_task: str
    visits_task: str


#: The snapshot each target was written from. ``ais-v5-datalake`` finds the job
#: of the daily DAG ``core.ais.v5`` (``core-ais-v5--...--<D>``);
#: ``core.ais.v5.backfill`` (snapshot ``core_ais_v5_backfill``, same task ids)
#: is a separate DAG with its own job names.
TARGET_SOURCES = {
    "ais-v5-datalake": TargetSource(
        snapshot="core_ais_v5",
        thin_task="port_visits.detect_port_events",
        visits_task="port_visits.detect_port_visits",
    ),
}

#: Flags DIT sets to its own values on purpose: its output tables, Dataflow job
#: names and labels, its service account, GCS paths and subnetwork, and its
#: worker cap. Approved by the owner on 2026-10-23 (PR #86), under the v4 CLI's
#: names: the two ``--bq-out-*`` flags were one ``--output_table``, and
#: ``--temp_dataset`` (DIT-only) left with the v4 CLI.
#:
#: ``--sdk_container_image`` joined on 2026-11-19, when the owner made a
#: DIT-built overlay image the default: the registry path differs from
#: production's by design. The version inside it is still checked, as each
#: step's version level.
DIT_OWNED_FLAGS = drift.DATAFLOW_OWNED_FLAGS | frozenset({
    "--bq-out-port-state-transitions",
    "--bq-out-port-visits",
})

#: Production flags DIT leaves out on purpose, with the reason.
#:
#: The first two are the owner's decisions of 2026-10-23.
#:
#: ``--no-rich-logging`` and ``--wait_for_job`` are accepted by the owner
#: (2026-11-19, on PR #91).
#: Neither changes output. To have the check report them again, delete the two
#: entries; to emit them instead, add them to ``ais._pipeline_options`` and
#: delete the entries.
ACCEPTED_DIFFERENCES = {
    **drift.DATAFLOW_ACCEPTED_DIFFERENCES,
    "--no-rich-logging": "log format only: plain instead of coloured output "
                         "(owner, 2026-11-19)",
    "--wait_for_job": "a v4 flag the v5 CLI does not define; gfw-common waits for "
                      "batch jobs regardless (owner, 2026-11-19)",
}

#: Accepted for step 1 only. The check runs step 1 on DIT's default messages
#: table, the AIS staging cohort, which is DIT's default by design
#: (``ProductionTarget`` has no step-1 tables: the production baseline runs
#: step 2 only). The named-anchorages table is still compared.
THIN_ACCEPTED_DIFFERENCES = {
    "--bq-in-messages": "step 1 reads DIT's AIS staging cohort by design; "
                        "the production target has no step-1 tables",
}

#: Every flag expected to differ in both steps; a difference anywhere else is drift.
EXPECTED_DIFFERENCES = DIT_OWNED_FLAGS | frozenset(ACCEPTED_DIFFERENCES)

#: Spellings of a value that mean the same thing, mapped to one form before
#: comparing. Beam resolves both runner names to the Dataflow runner (owner,
#: 2026-10-23: DIT keeps ``DataflowRunner``, production passes ``dataflow``).
EQUIVALENT_VALUES: Mapping[str, Mapping[str, str]] = drift.RUNNER_EQUIVALENTS

#: Flags that take several values after one mention. The pipe-anchorages CLI's
#: ``--labels`` does (gfw-common's labels option): production passes
#: ``--labels k=v k=v ...``, DIT ``--labels=k=v`` repeated, and both parse to
#: the same pairs. Without the hint production's pairs read as positionals.
MULTI_VALUE_FLAGS = drift.LABELS_MULTI_VALUE

_BACKTICKED = re.compile(r"`([^`]+)`")
_TRAILING_COMPACT_DATE = re.compile(r"(\d{8})$")


# --------------------------------------------------------------------------
# ProductionTarget vs the rendered DAG
# --------------------------------------------------------------------------

def parse_argv(tokens: Sequence[str]) -> composer.ParsedArgv:
    """A pipe-anchorages argv, DIT's or production's, split into flags."""
    return composer.parse_argv(tokens, multi=MULTI_VALUE_FLAGS)


def canonical_values(
    parsed: composer.ParsedArgv,
    equivalents: Mapping[str, Mapping[str, str]] = EQUIVALENT_VALUES,
) -> composer.ParsedArgv:
    """``parsed`` with each value that has an equivalent spelling written in one form."""
    return drift.canonical_values(parsed, equivalents)


def split_run_date(job_name: str, placeholders: Mapping[str, date]) -> tuple[str, str]:
    """Split the run date off the end of a job name: ``(prefix, note)``.

    The note names the placeholder(s) the date stands for; in a one-day interval
    the first and last day coincide, so it can name more than one.
    """
    match = _TRAILING_COMPACT_DATE.search(job_name)
    candidates = tuple(
        name for name, d in placeholders.items()
        if match and d.strftime("%Y%m%d") == match.group(1)
    )
    if not candidates:
        return job_name, "no run date at the end of the job name"
    return job_name[: match.start()], "run date suffix: " + " or ".join(candidates)


def _describe_date(value: composer.Templated) -> str:
    """How a templated date value relates to the snapshot's run."""
    if value.ambiguous:
        return "ambiguous: " + "; ".join(" or ".join(a.candidates) for a in value.ambiguous)
    if value.constants:
        return "constant, not derived from the run's interval"
    return "derived from the run's interval" if "{" in value.text else ""


def production_target_checks(
    target: ProductionTarget, dag: composer.RenderedDag, source: TargetSource,
) -> tuple[FieldCheck, ...]:
    """Each ``ProductionTarget`` field against the DAG's port-visits tasks."""
    visits = dag.task(source.visits_task)
    thin = dag.task(source.thin_task)
    v = parse_argv(visits.argv)
    t = parse_argv(thin.argv)
    single = drift.single_value
    placeholders = composer.task_placeholders(visits)

    job_name = single(v, "--job_name")
    prefix, job_note = (
        split_run_date(job_name, placeholders) if job_name else (None, "no single --job_name")
    )
    segs = tuple(dict.fromkeys(_BACKTICKED.findall(single(v, "--bad-segs") or "")))
    start = composer.templatize(single(v, "--start-date") or "", placeholders)
    return (
        FieldCheck("dataflow_project", target.dataflow_project, single(v, "--project")),
        FieldCheck("dataflow_region", target.dataflow_region, single(v, "--region")),
        FieldCheck("job_name_prefix", target.job_name_prefix, prefix, job_note),
        FieldCheck("output_table", target.output_table, single(v, "--bq-out-port-visits")),
        FieldCheck("thinned_table", target.thinned_table,
                   single(v, "--bq-in-port-state-transitions"), "step 2 input"),
        FieldCheck("thinned_table", target.thinned_table,
                   single(t, "--bq-out-port-state-transitions"), "step 1 output"),
        FieldCheck("segment_info_table", target.segment_info_table,
                   single(v, "--bq-in-segment-info")),
        FieldCheck("segs_activity_table", target.segs_activity_table,
                   segs[0] if len(segs) == 1 else None,
                   "from --bad-segs" if len(segs) == 1 else f"{len(segs)} tables in --bad-segs"),
        FieldCheck("history_start", target.history_start.isoformat(),
                   start.text or None, _describe_date(start)),
        FieldCheck("deployed_version", target.deployed_version,
                   f"{visits.image_name} {visits.version}" if visits.image else None,
                   "image of the port-visits task"),
        FieldCheck("worker_machine_type", target.worker_machine_type,
                   single(v, "--worker_machine_type")),
        FieldCheck("disk_size_gb", str(target.disk_size_gb), single(v, "--disk_size_gb")),
    )


# --------------------------------------------------------------------------
# DIT's argv, built offline
# --------------------------------------------------------------------------

def _dit_args(*cli: str) -> argparse.Namespace:
    """``ais.py``'s namespace for ``cli``, with the run context ``main()`` would stamp."""
    args = ais.parse_args(["--runner", "dataflow", "--experiment-id", "composer-drift", *cli])
    args.run_id = "composerdrift"
    args.commit_sha = "0000000"
    args.worker_image_tag = ais._worker_image_tag(args.worker_image)
    args.launched_by = "composer-drift"
    return args


def _entrypoint(args: argparse.Namespace) -> str:
    return ais._docker_run_kwargs(args)["entrypoint"]


def dit_thin_tokens(placeholders: Mapping[str, date]) -> list[str]:
    """What DIT runs for step 1 over the snapshot run's ``[start_date, end_date]``
    (inclusive, as DIT's ``--start`` / ``--end``; the builder passes the
    pipeline its exclusive end).

    ``ProductionTarget`` has no messages or named-anchorages table (the
    production baseline runs step 2 only), so those stay at DIT's defaults.
    """
    start, end = placeholders["start_date"], placeholders["end_date"]
    args = _dit_args("--start", start.isoformat(), "--end", end.isoformat(),
                     "--steps", ais.STEP_THIN)
    return [_entrypoint(args), *ais._thin_argv(
        args, suffix="drift", mode=ais.MODE_BF, slice_start=start, slice_end=end,
        iteration=1, total_iterations=1,
    )]


def dit_visits_tokens(target: ProductionTarget, placeholders: Mapping[str, date]) -> list[str]:
    """What DIT runs for step 2 with ``--baseline-production``'s configuration:
    the target's tables and worker sizing, ``[history_start, end_date]``."""
    end = placeholders["end_date"]
    args = _dit_args(
        "--start", target.history_start.isoformat(), "--end", end.isoformat(),
        "--steps", ais.STEP_VISITS,
        "--thinned-message-table", target.thinned_table,
        "--source-segment-info-fqn", target.segment_info_table,
        "--source-segs-activity-fqn", target.segs_activity_table,
        "--worker-machine-type", target.worker_machine_type,
        "--disk-size-gb", str(target.disk_size_gb),
    )
    return [_entrypoint(args), *ais._visits_argv(
        args, suffix="drift", mode=ais.MODE_BF, thinned_input=target.thinned_table,
        slice_end=end, iteration=1, total_iterations=1,
    )]


# --------------------------------------------------------------------------
# Spec and report
# --------------------------------------------------------------------------

def spec(
    target_name: str = "ais-v5-datalake", source: TargetSource | None = None,
) -> drift.WorkflowSpec:
    """The drift spec of ``ais.py`` against ``target_name``'s DAG.

    The image flag is DIT-owned (an overlay build under DIT's namespace), so
    each step names the pipeline version inside the image; that is what tells
    DIT that production moved to a new version and the overlay must be rebuilt.
    """
    target = PRODUCTION_TARGETS[target_name]
    source = source or TARGET_SOURCES[target_name]
    image = {"image": ais.DEFAULT_WORKER_IMAGE, "version": ais.DEFAULT_PIPELINE_VERSION}
    return drift.WorkflowSpec(
        name="port_visits/ais",
        workflow="workflows/port_visits/ais.py",
        snapshot=source.snapshot,
        steps=(
            drift.StepSpec(
                step=ais.STEP_THIN, task_id=source.thin_task, tokens=dit_thin_tokens,
                accepted=THIN_ACCEPTED_DIFFERENCES, **image,
            ),
            drift.StepSpec(
                step=ais.STEP_VISITS, task_id=source.visits_task,
                tokens=functools.partial(dit_visits_tokens, target), **image,
            ),
        ),
        dit_owned=DIT_OWNED_FLAGS,
        accepted=ACCEPTED_DIFFERENCES,
        equivalents=EQUIVALENT_VALUES,
        multi_value=MULTI_VALUE_FLAGS,
        field_checks=lambda dag: production_target_checks(target, dag, source),
        fields_title=f"ProductionTarget {target_name}",
        in_sync=True,
    )


#: The specs ``workflows/composer_drift.py`` runs for this package.
SPECS = (spec(),)


def build_report(
    target_name: str, dag: composer.RenderedDag, source: TargetSource,
) -> drift.WorkflowReport:
    return drift.check_workflow(spec(target_name, source), dag)


format_report = drift.format_report


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    p.add_argument("--composer-dir", type=Path, default=composer.default_composer_dir(),
                   help=f"composer-dags-production checkout (default: "
                        f"${composer.COMPOSER_DIR_ENV}, else $PROJECTS/composer-dags-production, "
                        "else a sibling of this repo). Read with git show only; fetch it first.")
    p.add_argument("--ref", default=composer.DEFAULT_REF,
                   help=f"composer ref to read (default {composer.DEFAULT_REF}).")
    p.add_argument("--target", choices=sorted(TARGET_SOURCES), default="ais-v5-datalake")
    p.add_argument("--snapshot", default=None,
                   help="Snapshot to compare with instead of the target's own, e.g. "
                        "core_ais_v5_backfill (same task ids).")
    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    source = TARGET_SOURCES[args.target]
    if args.snapshot:
        source = dataclasses.replace(source, snapshot=args.snapshot)
    try:
        dag = composer.load_dag(args.composer_dir, source.snapshot, args.ref)
        report = build_report(args.target, dag, source)
    except drift.READ_ERRORS as exc:
        print(f"composer drift check could not run: {exc}", file=sys.stderr)
        return 2
    print(format_report(report))
    return 1 if report.drift else 0


if __name__ == "__main__":
    sys.exit(main())
