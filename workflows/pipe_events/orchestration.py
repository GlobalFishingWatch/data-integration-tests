"""Orchestration declarations of the fishing-events steps (pipe-events).

See ``dit.orchestration`` for the fields and ``docs/plan.md`` § "Orchestration
capabilities" for how they map to Airflow. Nothing here is set by a run, and
``fishing.py`` has no parallel-slices mode yet.
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
    pipeline="pipe-events",
    steps=(
        StepOrchestration(
            step="incremental_events",
            command="incremental_events",
            parallel_slices=PARALLEL_FORBIDDEN,
            depends_on_previous_run=True,
            status=STATUS_CLAIMED,
            source=OWNER_CLAIM_2026_11_24,
            reason=(
                "Owner: incremental fishing events must run sequentially; no parallel "
                "batches. The step merges each slice into the events built so far."
            ),
        ),
        _unknown("incremental_filter_events"),
        _unknown("auth_and_regions_fishing_events"),
        _unknown("fishing_restrictive"),
    ),
)
