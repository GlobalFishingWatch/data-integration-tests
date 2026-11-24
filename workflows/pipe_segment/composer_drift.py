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
default image) over the snapshot run's inclusive
``[start_date, end_date]``, as ``--date-range`` takes it. They come from
``identity_match_key._step_argvs``, the function a binding's run loops over.
The workflow reads a pinned copy of its source; offline, the spec names the
source table itself.

**Expected in sync** since 2026-11-19. What still differs is DIT-owned or
accepted below, each with its reason.

**The DAG moved on 2026-11-20, and this check reported it.** composer-dags-
production PR #3145 (merged 17:26 UTC) set the segment group to pipe-segment
v5.1.0 and added ``--incremental`` to the ``segment`` task. Both specs then
reported the version on every step and the flag on ``segment``, and the runner
exited 1. DIT followed the same day: both workflows default to the overlay
image of v5.1.0 and ``ais.py`` defaults to the incremental segmenter. The
default image is a DIT overlay build, so each step states its version instead
of leaving it to the image tag.

**``--incremental`` is compared in the cross-version workflow too** (since
2026-11-24). Its base interface passes the flag, as the DAG's task does, so
dropping it on either side is drift. Until then the flag was an accepted
difference: every binding ran full history, because a binding may be older
than the flag. That is now a declared adjustment
(``workflows/pipe_segment/adjustments.py``), applied to such a binding only.

**The older layer is checked too** (``FROZEN_SPECS``). The base plus the
adjustment for v5.0.3 is what DIT runs for a v5.0.3 binding. It is compared
with the DAG at the last composer commit at which the segment group ran
v5.0.3 (``adjustments.FROZEN_AT``), and must be in sync there. That spec is
frozen: production cannot move it, so it is a regression test of DIT. It runs
in ``tests/test_composer_drift_live.py`` and with the runner's ``--frozen``,
not in the runner's default output.

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

from dit import adjust, drift
from workflows.pipe_segment import adjustments, ais
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

#: Accepted for the ``segment`` step only, in both workflows.
SEGMENT_ACCEPTED_DIFFERENCES = {
    **BEAM_ACCEPTED_DIFFERENCES,
    "--in_normalized_messages_table": "DIT reads a pinned copy of its AIS staging "
                                      "cohort by design",
    **dict.fromkeys(SATELLITE_FLAGS, _SATELLITE_OFFSETS_REASON),
}

#: The two table flags an incremental job passes and the DAG's task does not.
_INCREMENTAL_TABLE_REASON = (
    "DIT names where an incremental job reads the state of the day before its "
    "range: the run's own tables. The DAG's task leaves both to pipe-segment's "
    "defaults, which are its own output tables too"
)

#: Accepted for a ``segment`` job that passes ``--incremental``, on top of the
#: step's: the cross-version workflow's base, and the default job of
#: ``ais.py``. ``--incremental`` itself is not here: it is compared.
INCREMENTAL_ACCEPTED_DIFFERENCES = {
    **SEGMENT_ACCEPTED_DIFFERENCES,
    "--in_segments_table": _INCREMENTAL_TABLE_REASON,
    "--in_fragments_table": _INCREMENTAL_TABLE_REASON,
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


def dit_step_tokens(
    step: str,
    placeholders: Mapping[str, date],
    *,
    version: tuple[int, int, int] | None = None,
) -> list[str]:
    """What one binding runs for ``step``. For reviewed code at the default
    tag, the worker image is the default image.

    Without ``version`` the binding gets the base interface, which is what a
    binding at the default pipe-segment version runs. With it, the binding
    gets the declared adjustments for that version on top
    (``workflows/pipe_segment/adjustments.py``), chosen by the version alone:
    offline, no ref's source is read.
    """
    args = _dit_args(placeholders)
    argvs = imk._step_argvs(
        args,
        binding_name=_BINDING,
        suffix=f"{args.experiment_id}_{_BINDING}",
        pinned=imk._PinnedSources(
            normalized_messages=args.source_normalized_table,
        ),
        worker_image=args.image_tag,
        binding=adjust.BindingInfo(name=_BINDING, version=version),
    )
    return [imk.CLI_ENTRYPOINT, *argvs[step]]


def _tokens(step: str, version: tuple[int, int, int] | None = None) -> drift.TokenBuilder:
    """The token builder of ``step``, for a binding at ``version`` (the
    default version when ``None``)."""
    return lambda placeholders: dit_step_tokens(step, placeholders, version=version)


def _accepted(step: str, *, incremental: bool = True) -> Mapping[str, str]:
    """The accepted differences of ``step``. ``incremental`` says whether the
    ``segment`` job passes ``--incremental`` and its two table flags."""
    if step == "segment":
        return INCREMENTAL_ACCEPTED_DIFFERENCES if incremental else SEGMENT_ACCEPTED_DIFFERENCES
    return BEAM_ACCEPTED_DIFFERENCES if step in imk.BEAM_STEPS else SQL_ACCEPTED_DIFFERENCES


SPEC = drift.WorkflowSpec(
    name="pipe_segment/identity_match_key",
    workflow="workflows/pipe_segment/identity_match_key.py",
    snapshot="core_ais_v3",
    steps=tuple(
        drift.StepSpec(
            step=step, task_id=TASK_IDS[step],
            # A DIT overlay build: the tag starts with the version, but the
            # spec states it rather than parse it.
            image=imk.DEFAULT_IMAGE_TAG, version=imk.DEFAULT_PIPELINE_VERSION,
            tokens=_tokens(step), accepted=_accepted(step),
        )
        for step in imk.STEPS
    ),
    dit_owned=DIT_OWNED_FLAGS,
    multi_value=drift.LABELS_MULTI_VALUE,
    in_sync=True,
    note=_NOTE + "; the base interface passes --incremental as the DAG's task does",
)


# --------------------------------------------------------------------------
# Older layers of identity_match_key.py, frozen at their composer commit
# --------------------------------------------------------------------------
#
# For each pipe-segment version an adjustment is declared for
# (``adjustments.FROZEN_AT``): the base plus that version's adjustments,
# against the DAG at the last commit at which production ran the version.
#
# The image of a frozen spec is only read for its name and tag. DIT has no
# default image of an older version (a binding at one brings its own with
# ``--binding-image``), so the reference below need not exist.

def _frozen_image(version: tuple[int, int, int]) -> str:
    repository = imk.DEFAULT_IMAGE_TAG.rsplit(":", 1)[0]
    return f"{repository}:{adjustments.version_text(version)}"


def _frozen_spec(version: tuple[int, int, int], commit: str) -> drift.WorkflowSpec:
    name = adjustments.version_text(version)
    incremental = version >= adjustments.INCREMENTAL_SINCE
    return drift.WorkflowSpec(
        name=f"pipe_segment/identity_match_key@{name}",
        workflow="workflows/pipe_segment/identity_match_key.py",
        snapshot="core_ais_v3",
        steps=tuple(
            drift.StepSpec(
                step=step, task_id=TASK_IDS[step],
                image=_frozen_image(version), version=name,
                tokens=_tokens(step, version),
                accepted=_accepted(step, incremental=incremental),
            )
            for step in imk.STEPS
        ),
        dit_owned=DIT_OWNED_FLAGS,
        multi_value=drift.LABELS_MULTI_VALUE,
        in_sync=True,
        composer_ref=commit,
        note=_NOTE + f"; what DIT runs for a binding at pipe-segment {name}: the base "
                     f"interface with the adjustments declared for that version",
    )


#: The frozen specs: not in the runner's default output (``--frozen`` adds
#: them); checked in ``tests/test_composer_drift_live.py``.
FROZEN_SPECS = tuple(
    _frozen_spec(version, commit) for version, commit in adjustments.FROZEN_AT.items()
)


# --------------------------------------------------------------------------
# pipe_segment/ais.py: mode equivalence of the segment step
# --------------------------------------------------------------------------
#
# The workflow runs one task of the DAG, ``segment.segment``, in several
# shapes. Its argv comes from the same builder as identity_match_key's
# ``segment`` step, so the two specs drift together. Two steps mirror the one
# task:
#
# * ``segment``: a job of the default segmenter, incremental, which is what
#   the DAG's task passes. ``--incremental`` is compared, not accepted: if the
#   DAG drops it, this step drifts.
# * ``segment[full-history]``: a job of the reference variant, which leaves
#   the flag out on purpose.

#: Accepted for the full-history job, on top of the ``segment`` step's. (The
#: default, incremental job accepts INCREMENTAL_ACCEPTED_DIFFERENCES, above.)
FULL_HISTORY_ACCEPTED_DIFFERENCES = {
    **SEGMENT_ACCEPTED_DIFFERENCES,
    "--incremental": "DIT's reference variant runs full history on purpose: it is "
                     "what the incremental shapes are compared with",
}

#: The mode-equivalence steps: name, the workflow's ``--modes`` entry that
#: yields one job over the snapshot run's dates, and what may differ.
_MODE_EQUIVALENCE_STEPS = (
    ("segment", f"{ais.MODE_BF}:{ais.DEFAULT_SEGMENTER}", INCREMENTAL_ACCEPTED_DIFFERENCES, ""),
    (
        f"segment[{ais.SEGMENTER_FULL_HISTORY}]",
        f"{ais.MODE_BF}:{ais.SEGMENTER_FULL_HISTORY}",
        FULL_HISTORY_ACCEPTED_DIFFERENCES,
        "the reference variant; the DAG's task passes --incremental and this job "
        "does not",
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
            image=ais.DEFAULT_IMAGE_TAG, version=ais.DEFAULT_PIPELINE_VERSION,
            tokens=_mode_equivalence_tokens(variant), accepted=accepted, note=note,
        )
        for step, variant, accepted, note in _MODE_EQUIVALENCE_STEPS
    ),
    dit_owned=DIT_OWNED_FLAGS,
    multi_value=drift.LABELS_MULTI_VALUE,
    in_sync=True,
    note=_NOTE + "; the default job passes --incremental as the DAG's task does",
)

#: The specs ``workflows/composer_drift.py`` runs for this package.
SPECS = (SPEC, MODE_EQUIVALENCE_SPEC)
