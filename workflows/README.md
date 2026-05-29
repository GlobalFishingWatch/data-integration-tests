# `workflows/` — per-pipeline integration-test workflows

Each subdirectory holds one pipeline's dit workflow(s). A workflow is plain
importable Python with a `main(argv=None) -> int` entry point (the CLI / Cloud
Build runtime is one consumer); it composes the `dit.*` library (runners,
compare, dates, the shared `dit.workflow` harness) into a pipeline-specific
mode-equivalence test.

| Workflow | Pipeline | Runner | Compare shape | Cache |
|---|---|---|---|---|
| `pipe_gaps/mode_equivalence.py` | pipe-gaps (detect-gaps) | Beam **in-process** (`dit.runners.dataflow`) | SCD-2 (`_last_versions`, keyed by gap key) | yes (`dit_runs`) |
| `port_visits/ais.py` | anchorages_pipeline (port-visits) | Beam **via container** (`dit.runners.docker`) | truncate (`view_suffix=""`, `visit_id`) | yes (`dit_runs`) |
| `pipe_events/fishing.py` | pipe-events (fishing events) | **BQ-SQL via container** (`dit.runners.docker`) | truncate (`view_suffix=""`, `event_id`) | no (deferred) |

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
