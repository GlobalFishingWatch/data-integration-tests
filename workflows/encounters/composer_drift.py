"""Drift specs of the two encounters workflows (PIPELINE-4700).

Read-only: the specs ``workflows/composer_drift.py`` runs for
``encounters/ais.py`` (against the AIS v5 DAG) and ``encounters/vms.py``
(against the VMS v5 daily DAG). See :mod:`dit.drift` for the check itself.

Both workflows share one execution path and differ only in their defaults, so
the two specs are built by one function from the workflow module.

DIT's tokens are those of a plain single-slice run of the workflow at its
defaults: ``--start`` / ``--end`` set to the snapshot run's first and last day
(both inclusive, as the workflow's flags), mode ``1_bf``. Input tables stay at
the workflow's defaults.

**Not expected in sync today.** Both workflows still drive the encounters CLI
of v4.4.0 (``create_raw_encounters`` / ``merge_encounters``, underscore flags).
Production's v5 DAGs run a later CLI generation with renamed commands and
flags, so the command level differs and the flag differences are indicative
only. Porting the workflows is the fix; this check is its acceptance test. The
lists below name the flags of the CLI DIT drives today, plus the Dataflow
flags, whose names do not change with the CLI; the port rewrites the rest.
"""
from __future__ import annotations

import argparse
import functools
from collections.abc import Mapping
from datetime import date
from types import ModuleType

from dit import drift
from workflows.encounters import ais, vms

#: Flags DIT sets to its own values on purpose: its output tables (the raw
#: table is step 1's output and step 2's input) and the BigQuery staging
#: dataset its service account needs named, plus the Dataflow flags.
DIT_OWNED_FLAGS = drift.DATAFLOW_OWNED_FLAGS | frozenset({
    "--raw_table",
    "--sink_table",
    "--temp_dataset",
})

#: Production flags DIT leaves out on purpose: the Dataflow ones the owner
#: accepted for port visits (2026-10-23), which hold for every Dataflow workflow.
ACCEPTED_DIFFERENCES = drift.DATAFLOW_ACCEPTED_DIFFERENCES

_NOTE = (
    "drives the v4.4.0 CLI; production runs a later CLI generation with renamed "
    "commands and flags"
)


def _dit_args(module: ModuleType, placeholders: Mapping[str, date]) -> argparse.Namespace:
    """The workflow's namespace for the snapshot run's dates, with the run
    context ``run()`` would stamp."""
    args = module.parse_args([
        "--runner", "dataflow", "--experiment-id", "composer-drift",
        "--start", placeholders["start_date"].isoformat(),
        "--end", placeholders["end_date"].isoformat(),
    ])
    args.run_id = "composerdrift"
    args.workflow_label = module.WORKFLOW_LABEL
    return args


def dit_create_tokens(module: ModuleType, placeholders: Mapping[str, date]) -> list[str]:
    """What the workflow runs for step 1 over the snapshot run's dates."""
    args = _dit_args(module, placeholders)
    return [ais.CLI_ENTRYPOINT, *ais._create_argv(
        args, mode=ais.MODE_BF, suffix="drift", iteration=1, total_iterations=1,
        slice_start=placeholders["start_date"], slice_end=placeholders["end_date"],
    )]


def dit_merge_tokens(module: ModuleType, placeholders: Mapping[str, date]) -> list[str]:
    """What the workflow runs for step 2 over the snapshot run's dates."""
    args = _dit_args(module, placeholders)
    return [ais.CLI_ENTRYPOINT, *ais._merge_argv(
        args, mode=ais.MODE_BF, suffix="drift", iteration=1, total_iterations=1,
        slice_end=placeholders["end_date"],
    )]


def spec(module: ModuleType, *, name: str, snapshot: str) -> drift.WorkflowSpec:
    """The drift spec of the encounters workflow ``module`` against ``snapshot``."""
    return drift.WorkflowSpec(
        name=name,
        workflow=f"workflows/{name}.py",
        snapshot=snapshot,
        steps=(
            drift.StepSpec(
                step="create_raw_encounters",
                task_id="encounters.detect_daily_encounters",
                image=ais.DEFAULT_WORKER_IMAGE,
                tokens=functools.partial(dit_create_tokens, module),
            ),
            drift.StepSpec(
                step="merge_encounters",
                task_id="encounters.merge_encounters",
                image=ais.DEFAULT_WORKER_IMAGE,
                tokens=functools.partial(dit_merge_tokens, module),
            ),
        ),
        dit_owned=DIT_OWNED_FLAGS,
        accepted=ACCEPTED_DIFFERENCES,
        equivalents=drift.RUNNER_EQUIVALENTS,
        multi_value=drift.LABELS_MULTI_VALUE,
        in_sync=False,
        note=_NOTE,
    )


#: The specs ``workflows/composer_drift.py`` runs for this package.
SPECS = (
    spec(ais, name="encounters/ais", snapshot="core_ais_v5"),
    spec(vms, name="encounters/vms", snapshot="core_vms_v5_daily"),
)
