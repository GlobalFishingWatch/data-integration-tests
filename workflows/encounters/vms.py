"""Mode-equivalence test for encounters detection against **VMS** sources.

Sibling of :mod:`workflows.encounters.ais`. Same pipeline, same two-step
generation chain (``create_raw_encounters`` -> ``merge_encounters``), same
three modes, same comparison contract -- the entire execution path is reused
via :func:`workflows.encounters.ais.run`. What this module owns is its
**defaults and their consequences**, and those differ enough from the AIS
workflow to be worth stating plainly.

Why a VMS workflow exists
-------------------------
Interpolation gap handling is materially more exercised on VMS. Measured over
one week of ``pipe_vms_v3_internal.messages_positions`` (2026-06-01..07):

===========================  ==========================  ================
gap between consecutive      AIS staging cohort          VMS prod
positions (per ``seg_id``)   (2020-01-19..22)            (2026-06-01..07)
===========================  ==========================  ================
exactly 3600.000s            1 of 268,913  (0.0004%)     200,825 of
                                                         3,662,711 (5.48%)
longer than 3600s            1,878                       125,064
===========================  ==========================  ================

Exact-hour gaps are not merely present on VMS, they are **more common than
long gaps** -- reporting cadences are hourly. Anything whose behaviour turns
on the ``DT == max_gap_s`` boundary (``Resample._interpolate``, with
``MAX_GAP_HOURS = 1.0``) is effectively untestable on the AIS cohort and
richly testable here.

THREE DIFFERENCES FROM ais.py THAT CHANGE HOW YOU RUN THIS
----------------------------------------------------------
1. **The sources are PROD, not a staging cohort.** There is no VMS equivalent
   of ``pipe_ais_test_202408290000``. ``messages_positions`` alone is ~503 GB /
   1.13 B rows across 176 monthly partitions. The repo's standing rule is that
   a default, no-flag run must never hit prod-volume data, so the date defaults
   here are a **7-day window**, not the year-wide window ais.py can afford.
   Widen deliberately, and prefer ``--ssvid-filter`` (encounters accepts it on
   both steps) over a longer window.

2. **The sources span three projects across two GCP organisations.** Messages,
   segs_activity and spatial_measures live in ``gfw-int-vms-v3`` (org
   115316357079); segment_info lives in ``global-fishing-watch``; dit's output
   and EXPORT temp dataset live in ``world-fishing-827`` (org 433637338589).
   All three datasets are US-located, so Beam's EXPORT staging is co-located
   and ``--bq-temp-dataset`` works unchanged.

   **Unverified precondition:** ``automated-testing@`` (the Dataflow worker SA)
   has no *direct* IAM binding on either ``gfw-int-vms-v3`` or
   ``global-fishing-watch``. Access may still be granted via a group binding --
   cross-project access at GFW usually is -- but that could not be confirmed
   from outside those projects. If the first Dataflow run fails reading a
   source, this is why, and the fix is an IAM grant, not a workflow change.
   Laptop ``--runner docker`` runs use the operator's own ADC and are
   unaffected.

   Note this is a plain cross-org **read**, which is fine. Cross-org
   *snapshots* are structurally impossible (``CREATE SNAPSHOT ... CLONE``
   refuses across orgs) -- so do not add source pinning here without moving the
   destination into the source's org.

3. **``--temp_dataset`` support is still required**, exactly as for ais.py:
   point ``--worker-image`` at a dit overlay image until the upstream patch
   lands. See :mod:`workflows.encounters.ais` for the standing tag.

Everything else -- modes, ``encounter_id`` comparison over both the raw and
merged tables, destination-table bootstrap with MONTH partitioning, the
always-emitted labels, ``container_env`` -- is inherited unchanged.
"""
from __future__ import annotations

import argparse
import logging
import sys
from typing import Optional, Sequence

from dit.workflow import (
    add_experiment_id_arg,
    add_infra_args,
    add_modes_arg,
    parse_modes,
)

from workflows.encounters import ais

logger = logging.getLogger("encounters_vms")

# --------------------------------------------------------------------------
# VMS source defaults -- read out of composer-dags-production
# dags/core/vms/config.py (detect_encounters_config), which resolves:
#   source_messages     -> datasets.internal.table("messages_positions")
#   source_segment_info -> datasets.public.table("segment_info")
#   spatial_measures    -> InputTables.SPATIAL_MEASURES
# with projects.internal = gfw-int-vms-v3 and projects.public =
# global-fishing-watch. These are the tables PROD encounters actually reads,
# so the workflow mirrors prod rather than inventing a source shape.
# --------------------------------------------------------------------------
#: Distinct from ais.py's, so Dataflow/BQ label filters, cancel_run and cost
#: attribution can tell VMS runs apart despite the shared execution path.
WORKFLOW_LABEL = "encounters_vms"

VMS_INTERNAL = "gfw-int-vms-v3.pipe_vms_v3_internal"

DEFAULT_SOURCE_MESSAGES_FQN = f"{VMS_INTERNAL}.messages_positions"
DEFAULT_SOURCE_SEGMENT_INFO_FQN = "global-fishing-watch.pipe_vms_v4_published.segment_info"
DEFAULT_SOURCE_SEGS_ACTIVITY_FQN = f"{VMS_INTERNAL}.segs_activity"
# Pinned to the version prod uses; it is a dated snapshot table, so it moves
# only when composer-dags does. Bump alongside dags/core/vms/config.py.
DEFAULT_SPATIAL_MEASURES_FQN = f"{VMS_INTERNAL}.spatial_measures_clustered_vms_v20251209"

# Deliberately a 7-day window, NOT a year. See difference (1) above: these are
# prod tables. 2026-06 is a complete, stable month (data runs through 2026-07),
# and was the window the gap statistics above were measured on -- so a
# default-no-flag run is both cheap and known to exercise the boundary case.
# Inclusive on both ends, matching the encounters CLI.
DEFAULT_START = "2026-06-01"
DEFAULT_END = "2026-06-07"
DEFAULT_TAIL_DAYS = 3


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Mode-equivalence test for encounters detection (VMS, prod sources).",
    )
    p.add_argument("--runner", default="dataflow", choices=["dataflow", "docker"],
                   help="dataflow: submit DataflowRunner from inside the container "
                        "(default). docker: DirectRunner inside the container.")
    p.add_argument("--source-messages-fqn", default=DEFAULT_SOURCE_MESSAGES_FQN,
                   help=f"Messages source (create_raw_encounters --source_table). "
                        f"PROD table (~503 GB total); the date window is what keeps "
                        f"a run cheap. Default: {DEFAULT_SOURCE_MESSAGES_FQN}")
    p.add_argument("--source-segment-info-fqn", default=DEFAULT_SOURCE_SEGMENT_INFO_FQN,
                   help=f"segment_info (merge_encounters --vessel_id_table). Lives in "
                        f"a THIRD project. Default: {DEFAULT_SOURCE_SEGMENT_INFO_FQN}")
    p.add_argument("--source-segs-activity-fqn", default=DEFAULT_SOURCE_SEGS_ACTIVITY_FQN,
                   help=f"segs_activity, source of the bad-segs subquery. "
                        f"Default: {DEFAULT_SOURCE_SEGS_ACTIVITY_FQN}")
    p.add_argument("--spatial-measures-fqn", default=DEFAULT_SPATIAL_MEASURES_FQN,
                   help=f"spatial_measures reference table, pinned to the version "
                        f"prod uses. Default: {DEFAULT_SPATIAL_MEASURES_FQN}")
    p.add_argument("--start", default=DEFAULT_START,
                   help=f"Inclusive start date (also the merge step's full-history "
                        f"start). Default: {DEFAULT_START}")
    p.add_argument("--end", default=DEFAULT_END,
                   help=f"Inclusive end date. Default {DEFAULT_END} -- a 7-day window, "
                        f"kept narrow because these are prod-volume sources.")
    p.add_argument("--tail-days", type=int, default=DEFAULT_TAIL_DAYS,
                   help="Number of trailing days re-run day-by-day in the "
                        "daily-slice modes.")
    p.add_argument("--max-encounter-dist-km", type=float,
                   default=ais.DEFAULT_MAX_ENCOUNTER_DIST_KM)
    p.add_argument("--min-encounter-time-minutes", type=float,
                   default=ais.DEFAULT_MIN_ENCOUNTER_TIME_MINUTES)
    p.add_argument("--min-hours-between-encounters", type=float,
                   default=ais.DEFAULT_MIN_HOURS_BETWEEN_ENCOUNTERS)
    p.add_argument("--ssvid-filter", default=None,
                   help="Passed to BOTH steps as --ssvid_filter: a subquery, a "
                        "comma-separated ssvid list, or @path. On VMS this is the "
                        "preferred way to shrink a run -- cheaper and less "
                        "distorting than narrowing the date window.")
    p.add_argument("--image-tag", default=ais.DEFAULT_IMAGE_TAG,
                   help=f"Submitter image. Default: {ais.DEFAULT_IMAGE_TAG}")
    p.add_argument("--worker-image", default=ais.DEFAULT_WORKER_IMAGE,
                   help=f"Dataflow worker image (sdk_container_image). Must expose "
                        f"--temp_dataset; use a dit overlay image until the upstream "
                        f"patch lands. Default: {ais.DEFAULT_WORKER_IMAGE}")
    p.add_argument("--bq-temp-dataset", default=ais.DEFAULT_BQ_TEMP_DATASET,
                   help="Pre-existing BQ dataset for Beam EXPORT staging; env-var "
                        "fallback DIT_BQ_TEMP_DATASET. Must be US-located to match "
                        "the VMS sources (it is, by default).")
    p.add_argument("--max-num-workers", type=int, default=ais.DEFAULT_MAX_NUM_WORKERS,
                   help=f"Dataflow autoscaling ceiling, mirroring prod's cap for this "
                        f"step. Load-bearing here: an all-vessel, year-wide VMS run "
                        f"would otherwise scale unbounded. "
                        f"Default: {ais.DEFAULT_MAX_NUM_WORKERS}")
    p.add_argument("--build-from-source", action="store_true",
                   help="Build the image from the local checkout via docker compose "
                        "instead of pulling the published one.")
    p.add_argument("--binding-name", default="",
                   help="Optional binding label (e.g. 'before', 'after') for "
                        "cross-version runs. Surfaces in the Dataflow job name and "
                        "as the dit_binding=<name> BQ label. WITHOUT it, two "
                        "concurrent runs sharing an --experiment-id collide with "
                        "DataflowJobAlreadyExistsError. Empty when standalone.")
    p.add_argument("--suffix", default=None)
    add_experiment_id_arg(p)
    p.add_argument("--require-clean", action="store_true",
                   help="Error on a dirty tree instead of auto-snapshotting.")
    p.add_argument("--skip-pipelines", action="store_true")
    p.add_argument("--skip-comparisons", action="store_true")
    p.add_argument("--parallel", "--async", dest="parallel", action="store_true",
                   help="Run the selected modes in parallel threads.")
    add_infra_args(p)
    add_modes_arg(
        p, choices=ais.SELECTABLE_MODES, cached=False,
        help_suffix="This workflow has no run-cache integration yet.",
    )
    args = p.parse_args(argv)
    # Validate before any cloud call, same reasoning as ais.py: a typo'd mode
    # that silently ran nothing is indistinguishable from a passing run.
    args.modes = parse_modes(args.modes, choices=ais.SELECTABLE_MODES)
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    logger.info(
        "VMS sources are PROD (%s + global-fishing-watch); window %s..%s inclusive",
        VMS_INTERNAL, args.start, args.end,
    )
    return ais.run(args, workflow_label=WORKFLOW_LABEL)


if __name__ == "__main__":
    sys.exit(main())
