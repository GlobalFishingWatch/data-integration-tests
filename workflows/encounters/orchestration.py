"""Orchestration declarations of the encounters steps (encounters_pipeline).

See ``dit.orchestration`` for the fields and ``docs/plan.md`` § "Orchestration
capabilities" for how they map to Airflow. Nothing here is set by a run, and
no encounters workflow has a parallel-slices mode yet.
"""
from __future__ import annotations

from dit.orchestration import (
    OWNER_CLAIM_2026_11_24,
    PARALLEL_ALLOWED,
    PARALLEL_UNKNOWN,
    STATUS_CLAIMED,
    STATUS_UNTESTED,
    PipelineOrchestration,
    StepOrchestration,
)

ORCHESTRATION = PipelineOrchestration(
    pipeline="encounters_pipeline",
    steps=(
        StepOrchestration(
            step="create_raw_encounters",
            command="create_raw_encounters",
            parallel_slices=PARALLEL_ALLOWED,
            depends_on_previous_run=False,
            status=STATUS_CLAIMED,
            source=OWNER_CLAIM_2026_11_24,
            reason="Owner: raw encounters can load slices in parallel.",
        ),
        StepOrchestration(
            step="merge_encounters",
            command="merge_encounters",
            parallel_slices=PARALLEL_UNKNOWN,
            depends_on_previous_run=None,
            status=STATUS_UNTESTED,
            source="",
            reason=(
                "No statement by the owner. The workflow runs it as a full rebuild of its "
                "range from the start date (docs/encounters-onboarding-2026-07.md); not "
                "checked for this declaration."
            ),
        ),
    ),
)
