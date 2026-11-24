"""Orchestration declarations of the port-visits steps (pipe-anchorages).

See ``dit.orchestration`` for the fields and ``docs/plan.md`` § "Orchestration
capabilities" for how they map to Airflow. Nothing here is set by a run: a
result reaches this file only by a human edit.

The test of step 1 is ``ais.py``'s ``6_parallel`` mode (``--steps
thin_port_messages --modes 1_bf,6_parallel --slices month``).
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
    pipeline="anchorages_pipeline",
    steps=(
        StepOrchestration(
            step="thin_port_messages",
            command="port-state-transitions",
            parallel_slices=PARALLEL_ALLOWED,
            depends_on_previous_run=False,
            status=STATUS_CLAIMED,
            source=OWNER_CLAIM_2026_11_24,
            reason=(
                "Owner: raw_port_events can load slices in parallel. Read, not run, at "
                "pipe-anchorages v5.0.1: the step reads messages over exactly [start, end) "
                "(assets/queries/messages.sql.j2), processes each segment-day on its own "
                "(port_state_transitions/transforms/smart_thin_records.py), deletes its own "
                "range before writing (table_config.py delete_query) and appends "
                "(port_state_transitions/main.py, WRITE_APPEND). Concurrent slices therefore "
                "write disjoint rows. What a parallel run may still show: concurrent DML on "
                "one monthly partition, or the create-table pre-hook racing."
            ),
        ),
        StepOrchestration(
            step="port_visits",
            command="port-visits",
            parallel_slices=PARALLEL_UNKNOWN,
            depends_on_previous_run=None,
            status=STATUS_UNTESTED,
            source="",
            reason=(
                "No statement by the owner. Read, not run, at pipe-anchorages v5.0.1: the "
                "step reads the thinned messages from the start date on and writes its "
                "whole output table with WRITE_TRUNCATE (port_visits/main.py), so slices "
                "would each replace the table. DIT does not run it in slices "
                "(ais.py refuses 6_parallel with this step)."
            ),
        ),
    ),
)
