"""Port-visits drift check against production's rendered DAG (PIPELINE-4700 spike).

Read-only. Loads the rendered DAG behind a ``ProductionTarget`` from
composer-dags-production (through :mod:`dit.composer`: data, not code) and
reports two things, changing nothing:

1. each ``ProductionTarget`` field (``cross_version_ais.PRODUCTION_TARGETS``)
   against what the DAG's port-visits tasks pass;
2. the flags DIT's v4 argv builders (``ais._thin_argv`` / ``ais._visits_argv``)
   emit against production's argv for the same run.

DIT is configured the way ``--baseline-production`` configures it: the target's
tables, its history start, and the snapshot run's dates. A value that differs is
therefore a real difference, not a different run. Flags DIT sets to its own
values on purpose (outputs, identity, placement, labels, job names, worker cap)
are listed apart as expected differences (:data:`DIT_OWNED_FLAGS`).

Exit code: 0 when nothing differs beyond DIT-owned flags, 1 otherwise, 2 when
the snapshot or a task cannot be read. Mismatches are for the workflow owner to
judge; this check never edits ``ProductionTarget`` or the argv builders.

    dit run workflows/port_visits/composer_drift.py \\
        --composer-dir ../composer-dags-production --ref origin/main

The output includes production argv values (service accounts, buckets, network)
for the flags DIT does not emit. Read it locally; do not paste it into this
repository, which is public.
"""
from __future__ import annotations

import argparse
import dataclasses
import re
import sys
from collections.abc import Collection, Mapping, Sequence
from datetime import date
from pathlib import Path

from dit import composer
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
#: names and labels, its service account, GCS paths and subnetwork, its worker
#: cap, and ``--temp_dataset`` (DIT's staging dataset, a flag production does
#: not pass). A difference here is expected; anything else is reported as drift.
#: This set is a proposal for the owner to confirm (PIPELINE-4700).
DIT_OWNED_FLAGS = frozenset({
    "--output_table",
    "--job_name",
    "--labels",
    "--service_account_email",
    "--temp_location",
    "--staging_location",
    "--subnetwork",
    "--max_num_workers",
    "--temp_dataset",
})

_BACKTICKED = re.compile(r"`([^`]+)`")
_TRAILING_COMPACT_DATE = re.compile(r"(\d{8})$")


# --------------------------------------------------------------------------
# ProductionTarget vs the rendered DAG
# --------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class FieldCheck:
    """One ``ProductionTarget`` field: DIT's value and what composer passes."""

    field: str
    dit: str
    composer: str | None
    note: str = ""

    @property
    def matches(self) -> bool:
        return self.dit == self.composer


def _single(parsed: composer.ParsedArgv, name: str) -> str | None:
    """The value of a flag given exactly once, else ``None``."""
    values = parsed.values(name)
    return values[0] if len(values) == 1 else None


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
    v = composer.parse_argv(visits.argv)
    t = composer.parse_argv(thin.argv)
    placeholders = composer.task_placeholders(visits)

    job_name = _single(v, "--job_name")
    prefix, job_note = (
        split_run_date(job_name, placeholders) if job_name else (None, "no single --job_name")
    )
    segs = tuple(dict.fromkeys(_BACKTICKED.findall(_single(v, "--bad_segs") or "")))
    start = composer.templatize(_single(v, "--start_date") or "", placeholders)
    return (
        FieldCheck("dataflow_project", target.dataflow_project, _single(v, "--project")),
        FieldCheck("dataflow_region", target.dataflow_region, _single(v, "--region")),
        FieldCheck("job_name_prefix", target.job_name_prefix, prefix, job_note),
        FieldCheck("output_table", target.output_table, _single(v, "--output_table")),
        FieldCheck("thinned_table", target.thinned_table,
                   _single(v, "--thinned_message_table"), "step 2 input"),
        FieldCheck("thinned_table", target.thinned_table,
                   _single(t, "--output_table"), "step 1 output"),
        FieldCheck("segment_info_table", target.segment_info_table,
                   _single(v, "--vessel_id_table")),
        FieldCheck("segs_activity_table", target.segs_activity_table,
                   segs[0] if len(segs) == 1 else None,
                   "from --bad_segs" if len(segs) == 1 else f"{len(segs)} tables in --bad_segs"),
        FieldCheck("history_start", target.history_start.isoformat(),
                   start.text or None, _describe_date(start)),
        FieldCheck("deployed_version", target.deployed_version,
                   f"{visits.image_name} {visits.version}" if visits.image else None,
                   "image of the port-visits task"),
    )


# --------------------------------------------------------------------------
# DIT's v4 argv vs production's
# --------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class ArgvComparison:
    """DIT's argv for one step against production's, by flag."""

    step: str
    dit: composer.ParsedArgv
    production: composer.ParsedArgv
    diff: composer.FlagDiff
    dit_owned: tuple[str, ...]

    @property
    def command_matches(self) -> bool:
        return self.dit.positionals == self.production.positionals

    @property
    def drift(self) -> bool:
        """Anything beyond the DIT-owned flags differs, or the command does."""
        return not (self.command_matches and self.diff.identical)


def compare_argv(
    step: str, dit_tokens: Sequence[str], production_task: composer.RenderedTask,
    *, dit_owned: Collection[str] = DIT_OWNED_FLAGS,
) -> ArgvComparison:
    """Compare DIT's argv (entrypoint first) with production's for ``step``.

    DIT-owned flags are taken out of the diff and listed in ``dit_owned`` when
    either side passes them.
    """
    dit = composer.parse_argv(dit_tokens)
    production = composer.parse_argv(production_task.argv)
    passed = set(dit.names()) | set(production.names())
    return ArgvComparison(
        step=step,
        dit=dit,
        production=production,
        diff=composer.diff_flags(dit, production, ignore=dit_owned),
        dit_owned=tuple(sorted(passed & set(dit_owned))),
    )


def _dit_args(*cli: str) -> argparse.Namespace:
    """``ais.py``'s namespace for ``cli``, with the run context ``main()`` would stamp."""
    args = ais.parse_args([
        "--runner", "dataflow", "--pipeline-cli", ais.PIPELINE_CLI_V4,
        "--experiment-id", "composer-drift", *cli,
    ])
    args.run_id = "composerdrift"
    args.commit_sha = "0000000"
    args.worker_image_tag = ais._worker_image_tag(args.worker_image)
    args.launched_by = "composer-drift"
    return args


def _entrypoint(args: argparse.Namespace) -> str:
    return ais._docker_run_kwargs(args)["entrypoint"]


def dit_thin_tokens(placeholders: Mapping[str, date]) -> list[str]:
    """What DIT runs for step 1 over the snapshot run's ``[start_date, end_date]``.

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
    the target's tables, ``[history_start, end_date]``."""
    end = placeholders["end_date"]
    args = _dit_args(
        "--start", target.history_start.isoformat(), "--end", end.isoformat(),
        "--steps", ais.STEP_VISITS,
        "--thinned-message-table", target.thinned_table,
        "--source-segment-info-fqn", target.segment_info_table,
        "--source-segs-activity-fqn", target.segs_activity_table,
    )
    return [_entrypoint(args), *ais._visits_argv(
        args, suffix="drift", mode=ais.MODE_BF, thinned_input=target.thinned_table,
        slice_end=end, iteration=1, total_iterations=1,
    )]


# --------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class DriftReport:
    target: str
    dag: composer.RenderedDag
    fields: tuple[FieldCheck, ...]
    argv: tuple[ArgvComparison, ...]

    @property
    def drift(self) -> bool:
        return any(not f.matches for f in self.fields) or any(a.drift for a in self.argv)


def build_report(target_name: str, dag: composer.RenderedDag, source: TargetSource) -> DriftReport:
    target = PRODUCTION_TARGETS[target_name]
    thin = dag.task(source.thin_task)
    visits = dag.task(source.visits_task)
    return DriftReport(
        target=target_name,
        dag=dag,
        fields=production_target_checks(target, dag, source),
        argv=(
            compare_argv(ais.STEP_THIN, dit_thin_tokens(composer.task_placeholders(thin)), thin),
            compare_argv(ais.STEP_VISITS,
                         dit_visits_tokens(target, composer.task_placeholders(visits)), visits),
        ),
    )


def _show(parsed: composer.ParsedArgv, name: str) -> str:
    return " ".join(name if v is None else f"{name}={v}" for v in parsed.values(name))


def format_report(report: DriftReport) -> str:
    dag = report.dag
    interval = dag.data_interval
    lines = [
        f"composer-dags-production @ {(dag.commit or 'unknown')[:12]}, snapshot {dag.snapshot} "
        f"({dag.dag_id}), run [{interval.start.isoformat()}, {interval.end.isoformat()})",
        "",
        f"ProductionTarget {report.target}:",
        *(
            f"  {'ok  ' if f.matches else 'DIFF'}  {f.field:<20} dit={f.dit}"
            + ("" if f.matches else f"  composer={f.composer}")
            + (f"  ({f.note})" if f.note else "")
            for f in report.fields
        ),
    ]
    for cmp in report.argv:
        lines += [
            "",
            f"{cmp.step} argv, DIT v4 vs production:",
            "  command: " + (
                "same (" + " ".join(cmp.dit.positionals) + ")" if cmp.command_matches
                else f"dit={' '.join(cmp.dit.positionals)}  "
                     f"production={' '.join(cmp.production.positionals)}"
            ),
            "  same: " + (", ".join(cmp.diff.same) or "-"),
            "  DIT-owned (expected to differ): " + (", ".join(cmp.dit_owned) or "-"),
            "  only in production: " + (
                "; ".join(_show(cmp.production, n) for n in cmp.diff.only_right) or "-"),
            "  only in DIT: " + ("; ".join(_show(cmp.dit, n) for n in cmp.diff.only_left) or "-"),
            "  values differ: " + ("; ".join(
                f"{n}: dit={_show(cmp.dit, n)} production={_show(cmp.production, n)}"
                for n in cmp.diff.differing
            ) or "-"),
        ]
    lines += ["", "drift: " + ("yes" if report.drift else "no")]
    return "\n".join(lines)


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
    except (RuntimeError, KeyError, ValueError) as exc:
        print(f"composer drift check could not run: {exc}", file=sys.stderr)
        return 2
    print(format_report(report))
    return 1 if report.drift else 0


if __name__ == "__main__":
    sys.exit(main())
