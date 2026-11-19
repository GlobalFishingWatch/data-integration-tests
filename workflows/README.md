# `workflows/` — per-pipeline integration-test workflows

Each subdirectory holds one pipeline's dit workflow(s). A workflow is plain
importable Python with a `main(argv=None) -> int` entry point (the CLI / Cloud
Build runtime is one consumer); it composes the `dit.*` library (runners,
compare, dates, the shared `dit.workflow` harness) into a pipeline-specific
mode-equivalence test.

| Workflow | Pipeline | Runner | Compare shape | Cache |
|---|---|---|---|---|
| `pipe_gaps/mode_equivalence.py` | pipe-gaps (detect-gaps) | Beam **in-process** (`dit.runners.dataflow`) | SCD-2 (`_last_versions`, keyed by gap key) | yes (`dit_runs`) |
| `pipe_gaps/outage_recovery.py` | pipe-gaps (detect-gaps) | Beam **in-process** (`dit.runners.dataflow`) | SCD-2 (`_last_versions`, `gap_id` + `start_timestamp`): staged outage-and-recovery run vs a single-shot oracle | yes (`dit_runs`) |
| `port_visits/ais.py` | anchorages_pipeline (port-visits) | Beam **via container** (`dit.runners.docker`) | truncate (`view_suffix=""`, `visit_id`; `(identifier, timestamp)` with `--steps thin_port_messages`) | yes (`dit_runs`) |
| `port_visits/cross_version_ais.py` | anchorages_pipeline (port-visits) | N per-binding subprocess invocations of `ais.py`, each from a `git worktree` at that binding's ref; with `--baseline-production`, the candidate binding only, diffed against production's own output | truncate, same keys as `ais.py`, pairwise across bindings | via each binding's `ais.py` |
| `pipe_events/fishing.py` | pipe-events (fishing events) | **BQ-SQL via container** (`dit.runners.docker`) | truncate (`view_suffix=""`, `event_id`) | no (deferred) |
| `pipe_events/cross_version_fishing.py` | pipe-events (fishing events) | N per-binding subprocess invocations of `fishing.py` (each with its own **BQ-SQL via container** runtime, from a `git worktree` at that binding's ref) | truncate (`view_suffix=""`, `event_id`) across BOTH `_fishing_events` and `_product_events_fishing` views | no (deferred with `fishing.py`) |
| `pipe_segment/identity_match_key.py` | pipe-segment (cross-version, gpsdio-segment match key) | **via container** (`dit.runners.docker`), one binding at a time, each from a `git worktree` at that binding's ref. Six steps, as in production's segment group: two Beam jobs (`segment`, `segment_identity`), then four **BQ-SQL** steps (`segment_vessel_daily`, `segment_info`, `vessel_info`, `segment_vessel`) | truncate (`view_suffix=""`), per-table keys (`COMPARE_KEYS`) over eight output tables, pairwise across bindings | no |
| `encounters/ais.py` | encounters_pipeline (encounters generation) | Beam **via container** (`dit.runners.docker`) | truncate (`view_suffix=""`, `encounter_id`) on both the raw and merged tables | no |
| `encounters/vms.py` | encounters_pipeline (encounters generation, VMS) | same execution as `encounters/ais.py` (`ais.run(args)`); only defaults and help differ | same as `encounters/ais.py` | no |

## Drift check against production (`composer_drift.py`)

The `composer_drift.py` files are not test workflows. They are a read-only check that compares what each workflow would run with production's rendered DAG in composer-dags-production. Nothing runs on GCP or in docker ([PIPELINE-4700](https://globalfishingwatch.atlassian.net/browse/PIPELINE-4700), [`docs/composer-snapshots-2026-10.md`](../docs/composer-snapshots-2026-10.md) § 8).

- `composer_drift.py` (this directory) is the runner: `dit run workflows/composer_drift.py --composer-dir <checkout>`. It prints one row per workflow step (version, command, flag counts); `--detail` adds the full reports, which contain production values and are for local reading only.
- `<pkg>/composer_drift.py` holds the spec of each workflow in that package: the composer snapshot and tasks it mirrors, and what may differ.

| Spec | Workflow | Composer snapshot | Expected in sync today |
|---|---|---|---|
| `port_visits/ais` | `port_visits/ais.py` | `core_ais_v5` | yes |
| `encounters/ais` | `encounters/ais.py` | `core_ais_v5` | no: the workflow drives an older CLI generation |
| `encounters/vms` | `encounters/vms.py` | `core_vms_v5_daily` | no: same |
| `pipe_events/fishing` | `pipe_events/fishing.py` | `core_ais_v5` | no: older CLI generation |
| `pipe_gaps/mode_equivalence` | `pipe_gaps/mode_equivalence.py` | `core_ais_v5` | no: older pipe-gaps generation |
| `pipe_segment/identity_match_key` | `pipe_segment/identity_match_key.py` | `core_ais_v3` | yes (since 2026-11-19; all six tasks of the segment group) |

The two cross-version wrappers and `pipe_gaps/outage_recovery.py` have no spec of their own. A workflow that is ported to what production runs gets its spec updated in the same change, and `in_sync=True` once the check passes. A new workflow gets a spec when it lands.

`port_visits/composer_drift.py` also keeps its own command line, which additionally checks the port-visits `ProductionTarget` against another snapshot (`--snapshot core_ais_v5_backfill`).

## Framework-extraction decision (2026-05-29) — `dit.phases` DEFERRED

Phase 3 (the third consumer) was the designated point to decide whether to
extract the shared `Phase`/`Mode`/`Oracle` dataclasses (or the lighter
`dit.phases.backfill(...)` / `dit.phases.daily_tail(...)` slice helpers) from
`docs/framework-vision.md`. **Decision: do not extract. Keep the three
`execute_bf/bfd/bftruncate` explicit.**

### What we compared

Putting the three consumers' `execute_*` side by side:

| Consumer | Per-slice execution | bf | bfd / bftruncate daily window |
|---|---|---|---|
| pipe-gaps | one `_make_config` + `_run_pipeline` (in-process Beam) | single `[start, end]` | `[d - backfill_days_w, d)` (W-day overlap) |
| port-visits | two-step `thin_port_messages` → `port_visits` per slice (Beam-via-container) | single `[start, end]` | single-day-end (`thin [d,d]`, `visits [start,d]`) |
| pipe-events | four-step chain (`incremental` ×2 → `filter` ×2 → `auth` → `restrictive`), BQ-SQL-via-container | single `[start, end)` | `[d, d+1)` (1-day slices) |

### Why we deferred

1. **The per-slice execution bodies share <20%.** Different runners (in-process
   Beam vs container Beam vs container BQ-SQL), different step counts (1 / 2 / 4),
   different table-naming, different arg shapes. There is no common
   `execute_slice` to extract — only three pipeline-specific ones.

2. **Even the "obviously shared" date-slice arithmetic diverges.** The high-level
   shape is identical (bf = one full window; bfd = backfill-to-tail + daily loop;
   bftruncate = full + daily loop), but the *daily window definition* differs:
   pipe-gaps subtracts `backfill_days_w` to build each daily slice (`[d - W, d)`);
   port-visits and pipe-events use a single-day window. A `dit.phases` helper that
   yields `(start, end)` slices would have to model all three daily-window
   conventions (a `window_days` / `single_day` / `cumulative_end` parameter set)
   to be reusable — a leaky abstraction that saves ~6 lines per workflow.

3. **The cost/benefit is negative at three consumers.** The explicit `execute_*`
   functions are short, locally readable, and each maps 1:1 onto its pipeline's
   bash/CLI semantics (which is exactly what makes them trustworthy ports). An
   extracted helper would add indirection between the reader and the per-pipeline
   date math without removing meaningful duplication.

### When to revisit

Extract `dit.phases` only when a **fourth** consumer arrives that shares one of
the existing daily-window shapes *exactly* (e.g. another `backfill_days_w`-windowed
Beam pipeline, or another 1-day-slice pipeline). At that point the shared shape is
proven by two real consumers and the abstraction stops being speculative. Until
then the duplication is deliberate (decision 7, "duplicate until 3" — here, three
is the count that told us *not* to extract).

### Update (2026-10-23): revisit conditions met

Both conditions now hold:

- **Two consumers share a daily-window shape exactly.** `execute_bf` /
  `execute_bfd` / `execute_bftruncate` in `encounters/ais.py` slice dates the same
  way as in `port_visits/ais.py`: bf is one `[start, end]` slice; bfd backfills
  `[start, end - tail_days]`, then runs one single-day slice per day through
  `end`; bftruncate backfills `[start, end]`, then re-runs the last `tail_days`
  days one at a time. Iteration counts (`1 + tail_days`) match too. Only the
  per-slice `_run_slice` differs.
- **A third cross-version wrapper exists.** `port_visits/cross_version_ais.py`,
  `pipe_events/cross_version_fishing.py` and `pipe_segment/identity_match_key.py`
  all verify binding refs, pin sources, run each binding from a `git worktree`
  and diff outputs pairwise.

Planned extraction order, one step at a time: `dit.cross_version` first, then the
shared run scaffolding, then `dit.phases` (date slicing only; per-slice execution
stays in each workflow), then `dit.cohorts`. Nothing is extracted yet.
