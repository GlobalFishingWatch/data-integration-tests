"""Orchestration declarations of the segment group's steps (pipe-segment).

See ``dit.orchestration`` for the fields and ``docs/plan.md`` § "Orchestration
capabilities" for how they map to Airflow. Nothing here is set by a run, and
neither segmenter workflow has a parallel-slices mode yet (``ais.py``'s
``4_daily`` runs its daily jobs one after the other, through ``dit.slices``).
"""
from __future__ import annotations

from dit.orchestration import (
    OWNER_CLAIM_2026_11_24,
    PARALLEL_FORBIDDEN,
    PARALLEL_UNKNOWN,
    STATUS_CLAIMED,
    STATUS_UNTESTED,
    PipelineOrchestration,
    StepOrchestration,
)


def _unknown(step: str) -> StepOrchestration:
    return StepOrchestration(
        step=step,
        command=step,
        parallel_slices=PARALLEL_UNKNOWN,
        depends_on_previous_run=None,
        status=STATUS_UNTESTED,
        source="",
        reason="No statement by the owner; not checked.",
    )


ORCHESTRATION = PipelineOrchestration(
    pipeline="pipe-segment",
    steps=(
        StepOrchestration(
            step="segment",
            command="segment",
            parallel_slices=PARALLEL_FORBIDDEN,
            depends_on_previous_run=True,
            status=STATUS_CLAIMED,
            source=OWNER_CLAIM_2026_11_24,
            reason=(
                "Owner: the segmenter must run sequentially; no parallel yearly batches. "
                "A job continues the segments of the day before its range: the full-history "
                "segmenter reads the stored fragments, the incremental one the stored "
                "segments (workflows/pipe_segment/ais.py, check_chain)."
            ),
        ),
        _unknown("segment_identity"),
        _unknown("segment_vessel_daily"),
        _unknown("segment_info"),
        _unknown("vessel_info"),
        _unknown("segment_vessel"),
    ),
)
