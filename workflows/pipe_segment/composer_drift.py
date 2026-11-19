"""Drift spec of the pipe-segment workflow (PIPELINE-4700).

Read-only: the spec ``workflows/composer_drift.py`` runs for
``pipe_segment/identity_match_key.py`` against the segment tasks of the AIS v3
DAG. See :mod:`dit.drift` for the check itself.

**Why the v3 DAG.** Production's AIS v5 DAG has no segmenter: it waits for the
v3 DAG's segment group and reads that group's tables. So the AIS segmenter
production runs is the v3 DAG's (snapshot ``core_ais_v3``). The VMS v5 DAG
runs its own segment stage, which this workflow does not mirror.

The workflow drives all six tasks of the group: ``segment`` and
``segment_identity`` (Beam jobs), then ``segment_vessel_daily``,
``segment_info``, ``vessel_info`` and ``segment_vessel`` (BigQuery SQL).

DIT's tokens are those of one binding of a default run (Dataflow runner,
published image, satellite offsets off) over the snapshot run's inclusive
``[start_date, end_date]``, as ``--date-range`` takes it. They come from
``identity_match_key._step_argvs``, the function a binding's run loops over.
The workflow reads a pinned copy of its source; offline, the spec names the
source table itself.

**Expected in sync** since 2026-11-19. What still differs is DIT-owned or
accepted below, each with its reason.

**What the check cannot see.** The tables one step hands to the next are
DIT-owned flags, so their values are left out of the comparison. That the
chain is wired like production's (which step reads which table) is pinned by
the workflow's own tests instead.
"""
from __future__ import annotations

import argparse
from collections.abc import Mapping
from datetime import date

from dit import drift
from workflows.pipe_segment import identity_match_key as imk

#: Binding name used for the offline tokens (it only reaches DIT-owned flags).
_BINDING = "drift"

#: The composer task each step mirrors, in the group's order.
TASK_IDS = {
    "segment": "segment.segment",
    "segment_identity": "segment.segment_identity_daily",
    "segment_vessel_daily": "segment.segment_vessel_daily",
    "segment_info": "segment.segment_info",
    "vessel_info": "segment.vessel_info",
    "segment_vessel": "segment.segment_vessel",
}

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
    "--source_segment_vessel",
    "--source_segment_vessel_daily",
    "--destination",
})

#: Accepted for the two Beam steps.
BEAM_ACCEPTED_DIFFERENCES = {
    "--project": "DIT's jobs run in its own project, never in production's",
}

#: Why production's satellite-offset inputs are left out. The satellite
#: positions are date-sharded, which DIT does not support by policy. The
#: offsets query also reads its message source as ``<table>*`` filtered on
#: ``_TABLE_SUFFIX`` (``assets/satellite_offsets.sql.j2`` at v5.0.3), so it
#: cannot read DIT's source, one partitioned table. The workflow refuses
#: ``--include-satellite-offsets``, which asked for the branch
#: (PIPELINE-4705); whether to delete that option is the owner's decision.
_SATELLITE_OFFSETS_REASON = (
    "the satellite-offset branch is off in DIT: its inputs are "
    "date-sharded, which DIT does not support"
)

#: Accepted for the ``segment`` step only. ``--out_sat_offsets_table``, the
#: fourth satellite-offset flag, is DIT-owned.
SEGMENT_ACCEPTED_DIFFERENCES = {
    **BEAM_ACCEPTED_DIFFERENCES,
    "--in_normalized_messages_table": "DIT reads a pinned copy of its AIS staging "
                                      "cohort by design",
    "--in_normalized_sat_offset_messages_table": _SATELLITE_OFFSETS_REASON,
    "--in_norad_to_receiver_table": _SATELLITE_OFFSETS_REASON,
    "--in_sat_positions_table": _SATELLITE_OFFSETS_REASON,
}

#: Accepted for the four SQL steps.
SQL_ACCEPTED_DIFFERENCES = {
    "--project": "DIT names the project its queries are billed to on every step; "
                 "production leaves it to the CLI's default",
}

_NOTE = (
    "production's three satellite-offset inputs are an accepted difference: "
    "DIT leaves that branch off"
)


def _dit_args(placeholders: Mapping[str, date]) -> argparse.Namespace:
    """The workflow's namespace for the snapshot run's inclusive dates. The
    binding ref is a required flag that no token reads."""
    start, end = placeholders["start_date"], placeholders["end_date"]
    return imk.parse_args([
        "--experiment-id", "composer-drift",
        "--binding", f"{_BINDING}=HEAD",
        "--date-range", f"{start.isoformat()},{end.isoformat()}",
    ])


def dit_step_tokens(step: str, placeholders: Mapping[str, date]) -> list[str]:
    """What one binding runs for ``step``. For reviewed code at the default
    tag, the worker image is the default image."""
    args = _dit_args(placeholders)
    argvs = imk._step_argvs(
        args,
        binding_name=_BINDING,
        suffix=f"{args.experiment_id}_{_BINDING}",
        pinned=imk._PinnedSources(
            normalized_messages=args.source_normalized_table,
        ),
        worker_image=args.image_tag,
    )
    return [imk.CLI_ENTRYPOINT, *argvs[step]]


def _tokens(step: str) -> drift.TokenBuilder:
    """The token builder of ``step``."""
    return lambda placeholders: dit_step_tokens(step, placeholders)


def _accepted(step: str) -> Mapping[str, str]:
    if step == "segment":
        return SEGMENT_ACCEPTED_DIFFERENCES
    return BEAM_ACCEPTED_DIFFERENCES if step in imk.BEAM_STEPS else SQL_ACCEPTED_DIFFERENCES


SPEC = drift.WorkflowSpec(
    name="pipe_segment/identity_match_key",
    workflow="workflows/pipe_segment/identity_match_key.py",
    snapshot="core_ais_v3",
    steps=tuple(
        drift.StepSpec(
            step=step, task_id=TASK_IDS[step], image=imk.DEFAULT_IMAGE_TAG,
            tokens=_tokens(step), accepted=_accepted(step),
        )
        for step in imk.STEPS
    ),
    dit_owned=DIT_OWNED_FLAGS,
    multi_value=drift.LABELS_MULTI_VALUE,
    in_sync=True,
    note=_NOTE,
)

#: The specs ``workflows/composer_drift.py`` runs for this package.
SPECS = (SPEC,)
