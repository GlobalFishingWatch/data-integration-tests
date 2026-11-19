"""Drift check of every workflow against production's rendered DAGs (PIPELINE-4700).

Read-only. For each workflow that has a spec (``workflows/<pkg>/composer_drift.py``),
compares what DIT would run with what production's DAG runs, per step and on
three levels: the pipeline version, the command, and the flags. The DAGs are
read from a composer-dags-production checkout as data (:mod:`dit.composer`);
nothing runs on GCP, in docker or in a pipeline package. See :mod:`dit.drift`.

    dit run workflows/composer_drift.py \\
        --composer-dir ../composer-dags-production --ref origin/main
    python -m workflows.composer_drift --workflow port_visits/ais --detail

Fetch the composer checkout first; this never touches the network or the
checkout's working tree.

The default output is a summary: one row per workflow step, then one verdict
line per workflow. ``--detail`` adds each workflow's full report.

Exit code: 0 when every workflow marked as expected in sync has no drift
beyond its expected differences; 1 when one of those drifts (with ``--strict``:
when any workflow drifts); 2 when a snapshot or a task cannot be read.

LOCAL READING ONLY. The detail prints production's argv values (tables,
service accounts, buckets, network). Do not paste it into this repository, a
commit message or a pull request: the repository is public.
"""
from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from dit import composer, drift
from workflows.encounters import composer_drift as encounters
from workflows.pipe_events import composer_drift as pipe_events
from workflows.pipe_gaps import composer_drift as pipe_gaps
from workflows.pipe_segment import composer_drift as pipe_segment
from workflows.port_visits import composer_drift as port_visits

#: Every workflow's spec, in the order they are reported. A new workflow adds
#: its module here.
SPECS: tuple[drift.WorkflowSpec, ...] = (
    *port_visits.SPECS,
    *encounters.SPECS,
    *pipe_events.SPECS,
    *pipe_gaps.SPECS,
    *pipe_segment.SPECS,
)


def select(
    names: Sequence[str] | None, specs: Sequence[drift.WorkflowSpec],
) -> tuple[drift.WorkflowSpec, ...]:
    """The specs named in ``names`` (all when empty), in ``specs``' order.
    ``SystemExit`` naming the valid set for an unknown name, so a typo cannot
    read as "nothing drifted"."""
    known = [spec.name for spec in specs]
    unknown = [name for name in names or () if name not in known]
    if unknown:
        raise SystemExit(
            f"error: unknown --workflow {', '.join(unknown)}; choose from {', '.join(known)}"
        )
    return tuple(spec for spec in specs if not names or spec.name in names)


def parse_args(
    argv: Sequence[str] | None = None, specs: Sequence[drift.WorkflowSpec] = SPECS,
) -> argparse.Namespace:
    doc = __doc__ or ""
    p = argparse.ArgumentParser(
        description=doc.split("\n", 1)[0],
        epilog="LOCAL READING ONLY: the output prints production's argv values. "
               "Do not paste it into this public repository, a commit or a pull request.",
    )
    p.add_argument("--composer-dir", type=Path, default=composer.default_composer_dir(),
                   help=f"composer-dags-production checkout (default: "
                        f"${composer.COMPOSER_DIR_ENV}, else $PROJECTS/composer-dags-production, "
                        "else a sibling of this repo). Read with git show only; fetch it first.")
    p.add_argument("--ref", default=composer.DEFAULT_REF,
                   help=f"composer ref to read (default {composer.DEFAULT_REF}).")
    p.add_argument("--workflow", action="append", metavar="NAME", dest="workflows",
                   help="Check only this workflow; repeatable. One of: "
                        + ", ".join(spec.name for spec in specs) + ". Default: all.")
    p.add_argument("--detail", action="store_true",
                   help="After the summary, print each workflow's full report: every "
                        "field check and, per step, the flags on both sides with their "
                        "values. Prints production values.")
    p.add_argument("--strict", action="store_true",
                   help="Exit 1 on any drift, also in workflows not yet expected in sync.")
    return p.parse_args(argv)


def main(
    argv: Sequence[str] | None = None, specs: Sequence[drift.WorkflowSpec] = SPECS,
) -> int:
    args = parse_args(argv, specs)
    reports, errors = drift.run_specs(
        select(args.workflows, specs), args.composer_dir, args.ref,
    )
    print(drift.format_summary(reports, errors))
    if args.detail:
        for report in reports:
            print("\n" + "=" * 78 + "\n" + drift.format_report(report))
    # One line per distinct cause: an unreadable checkout fails every spec alike.
    for message in dict.fromkeys(error.message for error in errors):
        names = ", ".join(e.spec.name for e in errors if e.message == message)
        print(f"composer drift check could not read {names}: {message}", file=sys.stderr)
    return drift.exit_code(reports, errors, strict=args.strict)


if __name__ == "__main__":
    sys.exit(main())
