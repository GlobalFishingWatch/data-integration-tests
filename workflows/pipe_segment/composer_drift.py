"""Drift spec of the pipe-segment workflow (PIPELINE-4700).

Read-only: the spec ``workflows/composer_drift.py`` runs for
``pipe_segment/identity_match_key.py`` against the segment tasks of the AIS v3
DAG. See :mod:`dit.drift` for the check itself.

**Why the v3 DAG.** Production's AIS v5 DAG has no segmenter: it waits for the
v3 DAG's segment group and reads that group's tables. So the AIS segmenter
production runs is the v3 DAG's (snapshot ``core_ais_v3``). The VMS v5 DAG
runs its own segment stage, which this workflow does not mirror.

The workflow drives three of the group's six tasks: ``segment``,
``segment_identity`` and ``segment_info``. The other three are not part of the
spec.

DIT's tokens are those of one binding of a default run (Dataflow runner,
satellite offsets off) over the snapshot run's inclusive
``[start_date, end_date]``, as ``--date-range`` takes it. The workflow reads a
pinned copy of its source; offline, the spec names the source table itself.

**Not expected in sync today.** Version and command match production. The
flags do not: the differences are listed in ``docs/composer-snapshots-2026-10.md``
and are for the workflow's owner to judge, so they are reported rather than
accepted here.
"""
from __future__ import annotations

import argparse
from collections.abc import Mapping
from datetime import date

from dit import drift
from workflows.pipe_segment import identity_match_key as imk

#: Binding name used for the offline tokens (it only reaches DIT-owned flags).
_BINDING = "drift"

#: Flags DIT sets to its own values on purpose: its output tables, the tables
#: one step hands to the next, and the Dataflow flags. ``--fragments_table``
#: is both read and written by ``segment``.
DIT_OWNED_FLAGS = drift.DATAFLOW_OWNED_FLAGS | frozenset({
    "--out_segmented_messages_table",
    "--out_segments_table",
    "--fragments_table",
    "--out_sat_offsets_table",
    "--source_segments",
    "--source_fragments",
    "--dest_segment_identity",
    "--source_segment_identity",
    "--destination",
})

#: Differences accepted for every step, with the reason.
ACCEPTED_DIFFERENCES = {
    "--project": "DIT's jobs run in its own project, never in production's",
}

#: Accepted for the ``segment`` step only.
SEGMENT_ACCEPTED_DIFFERENCES = {
    "--in_normalized_messages_table": "DIT reads a pinned copy of its AIS staging "
                                      "cohort by design",
}

_NOTE = (
    "version and command match; the flag differences are known and need the "
    "owner's review"
)


def _dit_args(placeholders: Mapping[str, date]) -> argparse.Namespace:
    """The workflow's namespace for the snapshot run's inclusive dates. The
    pin time and the binding ref are required flags that no token reads."""
    start, end = placeholders["start_date"], placeholders["end_date"]
    return imk.parse_args([
        "--experiment-id", "composer-drift",
        "--pin-source-at", f"{start.isoformat()}T00:00:00Z",
        "--binding", f"{_BINDING}=HEAD",
        "--date-range", f"{start.isoformat()},{end.isoformat()}",
    ])


def _common(args: argparse.Namespace, step: str) -> dict:
    """Keyword arguments ``_run_binding`` passes to every argv builder. For
    reviewed code at the default tag, the worker image is the default image."""
    return dict(
        dest_dataset=args.dest_dataset,
        suffix=f"{args.experiment_id}_{_BINDING}",
        runner=args.runner,
        worker_image=args.image_tag,
        service_account=args.service_account,
        max_num_workers=args.worker_caps[step].workers,
        worker_sizing=args.worker_sizings[step],
    )


def dit_segment_tokens(placeholders: Mapping[str, date]) -> list[str]:
    """What one binding runs for ``segment``."""
    args = _dit_args(placeholders)
    return [imk.CLI_ENTRYPOINT, *imk._segment_args(
        snapshot_fqns=imk._PipeSegmentSnapshotFQNs(
            normalized_messages=args.source_normalized_table,
        ),
        experiment_id=args.experiment_id,
        binding_name=_BINDING,
        date_range=args.date_range,
        ssvid_filter_query=args.ssvid_filter_query,
        include_satellite_offsets=args.include_satellite_offsets,
        **_common(args, "segment"),
    )]


def dit_segment_identity_tokens(placeholders: Mapping[str, date]) -> list[str]:
    """What one binding runs for ``segment_identity``."""
    args = _dit_args(placeholders)
    return [imk.CLI_ENTRYPOINT, *imk._segment_identity_args(
        date_range=args.date_range, **_common(args, "segment_identity"),
    )]


def dit_segment_info_tokens(placeholders: Mapping[str, date]) -> list[str]:
    """What one binding runs for ``segment_info``."""
    args = _dit_args(placeholders)
    return [imk.CLI_ENTRYPOINT, *imk._segment_info_args(**_common(args, "segment_info"))]


SPEC = drift.WorkflowSpec(
    name="pipe_segment/identity_match_key",
    workflow="workflows/pipe_segment/identity_match_key.py",
    snapshot="core_ais_v3",
    steps=(
        drift.StepSpec(
            step="segment", task_id="segment.segment", image=imk.DEFAULT_IMAGE_TAG,
            tokens=dit_segment_tokens, accepted=SEGMENT_ACCEPTED_DIFFERENCES,
        ),
        drift.StepSpec(
            step="segment_identity", task_id="segment.segment_identity_daily",
            image=imk.DEFAULT_IMAGE_TAG, tokens=dit_segment_identity_tokens,
        ),
        drift.StepSpec(
            step="segment_info", task_id="segment.segment_info",
            image=imk.DEFAULT_IMAGE_TAG, tokens=dit_segment_info_tokens,
        ),
    ),
    dit_owned=DIT_OWNED_FLAGS,
    accepted=ACCEPTED_DIFFERENCES,
    multi_value=drift.LABELS_MULTI_VALUE,
    in_sync=False,
    note=_NOTE,
)

#: The specs ``workflows/composer_drift.py`` runs for this package.
SPECS = (SPEC,)
