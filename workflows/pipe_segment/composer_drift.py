"""Drift specs of the pipe-segment workflows (PIPELINE-4700).

Read-only: the specs ``workflows/composer_drift.py`` runs for
``pipe_segment/identity_match_key.py`` and ``pipe_segment/ais.py`` against the
segment tasks of the AIS v3 DAG. See :mod:`dit.drift` for the check itself.
Most of this docstring is about the first; the second, the mode-equivalence
workflow of the ``segment`` step, has its own section at the end of the file.

**Why the v3 DAG.** Production's AIS v5 DAG has no segmenter: it waits for the
v3 DAG's segment group and reads that group's tables. So the AIS segmenter
production runs is the v3 DAG's (snapshot ``core_ais_v3``). The VMS v5 DAG
runs its own segment stage, which this workflow does not mirror.

The workflow drives all six tasks of the group: ``segment`` and
``segment_identity`` (Beam jobs), then ``segment_vessel_daily``,
``segment_info``, ``vessel_info`` and ``segment_vessel`` (BigQuery SQL).

DIT's tokens are those of one binding of a default run (Dataflow runner,
published image) over the snapshot run's inclusive
``[start_date, end_date]``, as ``--date-range`` takes it. They come from
``identity_match_key._step_argvs``, the function a binding's run loops over.
The workflow reads a pinned copy of its source; offline, the spec names the
source table itself.

**Expected in sync** since 2026-11-19. What still differs is DIT-owned or
accepted below, each with its reason.

**No satellite timing offsets in DIT.** Production's ``segment`` task passes
four satellite flags; DIT passes none and never will without a pipe-segment
change (owner decision, 2026-11-20). All four are accepted differences of the
``segment`` step. The reasons are in the workflow's module docstring.

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
from workflows.pipe_segment import ais
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

#: Why production's satellite flags are left out: DIT runs no satellite timing
#: offsets (owner decision, 2026-11-20). The offsets must come from the full
#: message feed, which is too large to read on every test run; computed from
#: a test cohort they are different and noisy. pipe-segment has no option to
#: read precomputed offsets. Secondary: the satellite positions are
#: date-sharded tables in another organisation, and the offsets query reads
#: its message source through a table wildcard that a partitioned table does
#: not match. The figures are in the workflow's module docstring.
_SATELLITE_OFFSETS_REASON = (
    "DIT runs no satellite timing offsets: they must be computed from the full "
    "message feed, not a test cohort, the full feed is too large to read on "
    "every test run, and pipe-segment cannot read precomputed offsets"
)

#: The four satellite flags of production's ``segment`` task: three inputs and
#: the offsets output table. DIT passes none of them.
SATELLITE_FLAGS = (
    "--in_normalized_sat_offset_messages_table",
    "--in_norad_to_receiver_table",
    "--in_sat_positions_table",
    "--out_sat_offsets_table",
)

#: Accepted for the ``segment`` step only.
SEGMENT_ACCEPTED_DIFFERENCES = {
    **BEAM_ACCEPTED_DIFFERENCES,
    "--in_normalized_messages_table": "DIT reads a pinned copy of its AIS staging "
                                      "cohort by design",
    **dict.fromkeys(SATELLITE_FLAGS, _SATELLITE_OFFSETS_REASON),
}

#: Accepted for the four SQL steps.
SQL_ACCEPTED_DIFFERENCES = {
    "--project": "DIT names the project its queries are billed to on every step; "
                 "production leaves it to the CLI's default",
}

_NOTE = (
    "production's four satellite flags are an accepted difference: DIT runs no "
    "satellite timing offsets"
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


# --------------------------------------------------------------------------
# pipe_segment/ais.py: mode equivalence of the segment step
# --------------------------------------------------------------------------
#
# The workflow runs one production task, ``segment.segment``, in several
# shapes. Its argv comes from the same builder as identity_match_key's
# ``segment`` step, so the two specs drift together. Two steps mirror the one
# task: a full-history job, which is what production runs, and an incremental
# job, whose three extra flags are DIT's alone today.

#: Why the incremental segmenter's flags are in DIT's argv and not in
#: production's. Production's task passes none of them (rendered snapshot
#: core_ais_v3, read 2026-11-20).
_INCREMENTAL_REASON = (
    "DIT only: production does not run pipe-segment's incremental segmenter "
    "(opt-in since v5.1.0); DIT runs it to compare it with full history"
)
_INCREMENTAL_TABLE_REASON = (
    "DIT only, with --incremental: the variant's own table, where the state of "
    "the day before the date range is read"
)

#: Accepted for the incremental job only, on top of the ``segment`` step's.
INCREMENTAL_ACCEPTED_DIFFERENCES = {
    **SEGMENT_ACCEPTED_DIFFERENCES,
    "--incremental": _INCREMENTAL_REASON,
    "--in_segments_table": _INCREMENTAL_TABLE_REASON,
    "--in_fragments_table": _INCREMENTAL_TABLE_REASON,
}

#: The mode-equivalence steps: name, the workflow's ``--modes`` entry that
#: yields one job over the snapshot run's dates, and what may differ.
_MODE_EQUIVALENCE_STEPS = (
    ("segment", f"{ais.MODE_BF}:{ais.SEGMENTER_FULL_HISTORY}", SEGMENT_ACCEPTED_DIFFERENCES, ""),
    (
        f"segment[{ais.SEGMENTER_INCREMENTAL}]",
        f"{ais.MODE_BF}:{ais.SEGMENTER_INCREMENTAL}",
        INCREMENTAL_ACCEPTED_DIFFERENCES,
        "needs an image of pipe-segment v5.1.0 or later, passed by flag; the "
        "version compared here is the default image's",
    ),
)


def mode_equivalence_tokens(variant: str, placeholders: Mapping[str, date]) -> list[str]:
    """What ``ais.py`` runs for one job of ``variant`` over the snapshot run's
    inclusive dates, on a default Dataflow run. The pin, the run id and the
    commit only reach DIT-owned flags."""
    start, end = placeholders["start_date"], placeholders["end_date"]
    args = ais.parse_args([
        "--experiment-id", "composer-drift",
        "--start", start.isoformat(), "--end", end.isoformat(),
        "--tail-days", "0", "--modes", variant,
    ])
    args.run_id = args.commit_sha = _BINDING
    (job,) = ais.plan_jobs(args.variants[0], start, end, args.tail_days)
    argv = ais.segment_argv(
        args, job, suffix=f"{args.experiment_id}_{_BINDING}",
        pinned=imk._PinnedSources(normalized_messages=args.source_normalized_table),
    )
    return [imk.CLI_ENTRYPOINT, *argv]


def _mode_equivalence_tokens(variant: str) -> drift.TokenBuilder:
    return lambda placeholders: mode_equivalence_tokens(variant, placeholders)


MODE_EQUIVALENCE_SPEC = drift.WorkflowSpec(
    name="pipe_segment/ais",
    workflow="workflows/pipe_segment/ais.py",
    snapshot="core_ais_v3",
    steps=tuple(
        drift.StepSpec(
            step=step, task_id=TASK_IDS["segment"],
            # The default image is a DIT overlay build; its tag names the
            # version first, but the spec states it rather than parse it.
            image=ais.DEFAULT_IMAGE_TAG, version=ais.DEFAULT_PIPELINE_VERSION,
            tokens=_mode_equivalence_tokens(variant), accepted=accepted, note=note,
        )
        for step, variant, accepted, note in _MODE_EQUIVALENCE_STEPS
    ),
    dit_owned=DIT_OWNED_FLAGS,
    multi_value=drift.LABELS_MULTI_VALUE,
    in_sync=True,
    note=_NOTE + "; --incremental and its two table flags are DIT's alone",
)

#: The specs ``workflows/composer_drift.py`` runs for this package.
SPECS = (SPEC, MODE_EQUIVALENCE_SPEC)
