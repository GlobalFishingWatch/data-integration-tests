"""Orchestration declarations of the gap-detection step (pipe-gaps).

See ``dit.orchestration`` for the fields and ``docs/plan.md`` § "Orchestration
capabilities" for how they map to Airflow. Nothing here is set by a run, and
``mode_equivalence.py`` has no parallel-slices mode yet.
"""
from __future__ import annotations

from dit.orchestration import (
    OWNER_CLAIM_2026_11_24,
    PARALLEL_FORBIDDEN,
    STATUS_CLAIMED,
    PipelineOrchestration,
    StepOrchestration,
)

ORCHESTRATION = PipelineOrchestration(
    pipeline="pipe-gaps",
    steps=(
        StepOrchestration(
            step="detect",
            command="raw-gaps",
            parallel_slices=PARALLEL_FORBIDDEN,
            depends_on_previous_run=True,
            status=STATUS_CLAIMED,
            source=OWNER_CLAIM_2026_11_24,
            reason=(
                "Owner, with reservation (\"probably\"): gaps must run sequentially, no "
                "parallel batches. Consistent with the reprocess contract in CLAUDE.md: "
                "every run overwrites the raw_gaps tail from its start date on."
            ),
        ),
    ),
)
