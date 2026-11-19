"""Drift spec of the fishing-events workflow (PIPELINE-4700).

Read-only: the spec ``workflows/composer_drift.py`` runs for
``pipe_events/fishing.py`` against the fishing-events tasks of the AIS v5 DAG.
See :mod:`dit.drift` for the check itself.

The workflow runs a four-step chain per slice, the first two steps once per
score field, so the spec has six steps, each mirroring one task of the DAG's
``fishing_intervals`` group. The DAG has more tasks in that group (longline
sets) which the workflow does not drive; they are not part of the spec.

DIT's tokens are those of a plain single-slice run of the workflow at its
defaults over the snapshot run's half-open ``[start, end)``, mode ``1_bf``.

**Not expected in sync today.** The workflow drives the pipe-events CLI of
v4.2.17: entrypoint ``pipe``, underscore command names, single-dash flags.
Production's AIS v5 DAG runs a later generation with another entrypoint and
renamed commands and flags, so the command level differs everywhere and the
flag differences are indicative only. Porting the workflow is the fix; this
check is its acceptance test. The DIT-owned list names the flags of the CLI
DIT drives today; the port rewrites it.
"""
from __future__ import annotations

import argparse
import functools
from collections.abc import Mapping
from datetime import date

from dit import drift
from workflows.pipe_events import fishing

#: Flags DIT sets to its own values on purpose: its output tables and views,
#: the intermediate tables one step hands to the next, and the labels and
#: descriptions it stamps on them. pipe-events runs BigQuery SQL, not Dataflow,
#: so there are no Dataflow flags.
DIT_OWNED_FLAGS = frozenset({
    "--table_description",
    "-labels",
    "-dest",
    "-dest_tbl_prefix",
    "-dest_view",
    "-destrest",
    "-destrestview",
    "-mtbl",
    "-source_fishing",
    "-source_nl",
    "-source_events",
})

#: Flags that never take a value. Production passes ``-v`` right before the
#: command name; without the hint a command following it would read as its value.
BARE_FLAGS = frozenset({"-v"})

_NOTE = (
    "drives the v4.2.17 CLI (entrypoint pipe); production runs a later CLI "
    "generation with another entrypoint and renamed commands and flags"
)


def _dit_args(placeholders: Mapping[str, date]) -> argparse.Namespace:
    """``fishing.py``'s namespace for the snapshot run's half-open window."""
    return fishing.parse_args([
        "--experiment-id", "composer-drift",
        "--start", placeholders["start_date"].isoformat(),
        "--end", placeholders["end_date_exclusive"].isoformat(),
    ])


#: Keyword arguments every argv builder takes here: one slice, mode ``1_bf``.
_SLICE = {"mode": fishing.MODE_BF, "suffix": "drift"}


def dit_incremental_tokens(sfield: str, placeholders: Mapping[str, date]) -> list[str]:
    """What the workflow runs for step 1 and one score field."""
    return [fishing.CLI_ENTRYPOINT, *fishing._incremental_argv(
        _dit_args(placeholders), sfield=sfield, **_SLICE,
        slice_start=placeholders["start_date"], slice_end=placeholders["end_date_exclusive"],
    )]


def dit_filter_tokens(sfield: str, placeholders: Mapping[str, date]) -> list[str]:
    """What the workflow runs for step 2 and one score field."""
    return [fishing.CLI_ENTRYPOINT, *fishing._filter_argv(
        _dit_args(placeholders), sfield=sfield, **_SLICE,
    )]


def dit_auth_tokens(placeholders: Mapping[str, date]) -> list[str]:
    """What the workflow runs for step 3."""
    return [fishing.CLI_ENTRYPOINT, *fishing._auth_argv(
        _dit_args(placeholders), **_SLICE, slice_end=placeholders["end_date_exclusive"],
    )]


def dit_restrictive_tokens(placeholders: Mapping[str, date]) -> list[str]:
    """What the workflow runs for step 4."""
    return [fishing.CLI_ENTRYPOINT, *fishing._restrictive_argv(
        _dit_args(placeholders), **_SLICE, slice_end=placeholders["end_date_exclusive"],
    )]


def _step(step: str, task: str, tokens: drift.TokenBuilder) -> drift.StepSpec:
    return drift.StepSpec(
        step=step,
        task_id=f"fishing_intervals.{task}",
        image=fishing.DEFAULT_IMAGE_TAG,
        tokens=tokens,
    )


#: Each (step, score field) of the chain and the DAG task it mirrors.
STEPS = (
    _step("incremental_events[nnet_score]", "merge_daily_fishing_events",
          functools.partial(dit_incremental_tokens, "nnet_score")),
    _step("incremental_events[night_loitering]", "merge_daily_night_loitering_events",
          functools.partial(dit_incremental_tokens, "night_loitering")),
    _step("incremental_filter_events[nnet_score]", "filter_fishing_events",
          functools.partial(dit_filter_tokens, "nnet_score")),
    _step("incremental_filter_events[night_loitering]", "filter_night_loitering_events",
          functools.partial(dit_filter_tokens, "night_loitering")),
    _step("auth_and_regions_fishing_events", "annotate_auth_and_regions", dit_auth_tokens),
    _step("fishing_restrictive", "filter_restrictive_fishing_events", dit_restrictive_tokens),
)

SPEC = drift.WorkflowSpec(
    name="pipe_events/fishing",
    workflow="workflows/pipe_events/fishing.py",
    snapshot="core_ais_v5",
    steps=STEPS,
    dit_owned=DIT_OWNED_FLAGS,
    multi_value=drift.LABELS_MULTI_VALUE,
    bare=BARE_FLAGS,
    in_sync=False,
    note=_NOTE,
)

#: The specs ``workflows/composer_drift.py`` runs for this package.
SPECS = (SPEC,)
