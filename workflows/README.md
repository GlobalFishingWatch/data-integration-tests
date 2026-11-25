# `workflows/` — the index of DIT's integration-test workflows

A workflow is an importable Python file with a `main(argv=None) -> int` entry point. `dit run <file>` and `make dit-cloud WORKFLOW=<file>` are two ways to call it. How runners, the run cache, pinning and Cloud Build work is explained in the top-level [`README.md`](../README.md) and [`docs/architecture.md`](../docs/architecture.md).

Each pipeline has an entry page with versions, modes, commands and run history:

| Pipeline | Entry page | DIT's default version | Production's version |
|---|---|---|---|
| pipe-gaps (gap detection) | [`pipe_gaps/README.md`](pipe_gaps/README.md) | v0.10.0, `detect` | v0.17.1, `raw-gaps` (AIS v5 DAG). **DIT is behind** |
| pipe-events (fishing events) | [`pipe_events/README.md`](pipe_events/README.md) | v4.2.17, entrypoint `pipe` | v9.0.0, entrypoint `pipe-events` (AIS v5 DAG). **DIT is behind** |
| pipe-anchorages (port visits) | [`port_visits/README.md`](port_visits/README.md) | v5.0.1, DIT overlay image | v5.0.1 (AIS v5 DAG) |
| encounters_pipeline | [`encounters/README.md`](encounters/README.md) | v4.4.0, old CLI | v5.0.0 (AIS and VMS v5 DAGs). **DIT is behind** |
| pipe-segment | [`pipe_segment/README.md`](pipe_segment/README.md) | v5.1.0, DIT overlay image | v5.1.0 (AIS v3 DAG) |

This table is the source of truth for DIT's default version against production's; other pages (the per-pipeline pages, [`docs/pipeline-contract.md`](../docs/pipeline-contract.md)) defer to it, so update it first when either side moves. Production versions are those of composer-dags-production's rendered DAGs as read by the drift check (2026-11-19 and 2026-11-20). [`docs/composer-snapshots-2026-10.md`](../docs/composer-snapshots-2026-10.md) § 8d has the detail.

## Pick a workflow

| You want to know | Run |
|---|---|
| Does a backfill give the same output as the daily schedule? | the pipeline's mode-equivalence workflow: `pipe_gaps/mode_equivalence.py`, `pipe_events/fishing.py`, `port_visits/ais.py`, `encounters/ais.py` (VMS: `encounters/vms.py`), `pipe_segment/ais.py` |
| Does a change alter the output? (two refs, same inputs) | a cross-version wrapper: `port_visits/cross_version_ais.py`, `pipe_events/cross_version_fishing.py`, `pipe_segment/identity_match_key.py` |
| Does my change match what production wrote? | `port_visits/cross_version_ais.py --baseline-production` (port visits only) |
| Does a recovery backfill after an outage end in the clean state? | `pipe_gaps/outage_recovery.py --synthetic-outage` |
| Can a step load slices of a window in parallel? | `port_visits/ais.py --modes 1_bf,6_parallel --steps thin_port_messages` (the only test of an orchestration declaration so far) |
| Does DIT still run what production runs? | `workflows/composer_drift.py` (reads composer, runs nothing) |

For pipe-gaps, encounters and pipe-events, a mode-equivalence run tests the old version DIT drives, not production's. Read the entry page first.

## All workflows

| Workflow | Compares | Modes or bindings | Pinning | Run cache | Drift spec | Orchestration declared |
|---|---|---|---|---|---|---|
| [`pipe_gaps/mode_equivalence.py`](pipe_gaps/mode_equivalence.py) | modes pairwise, on the `_last_versions` view, keys `gap_id`, `start_timestamp` | `1_bf`, `2_bfd`, `3_bftruncate`; opt-in `4_mutate_recover` (`--enable-pipeline-4`) | none (reads the live staging tables) | yes | `pipe_gaps/mode_equivalence`, not in sync | `detect`: parallel forbidden (claimed) |
| [`pipe_gaps/outage_recovery.py`](pipe_gaps/outage_recovery.py) | three-stage outage and recovery against a single-shot oracle, same view and keys | one fixed shape (3 stages + oracle) | by copy: messages over the run's window, segments whole | yes | none | same step as above |
| [`pipe_events/fishing.py`](pipe_events/fishing.py) | modes pairwise, on the `_fishing_events` and `_product_events_fishing` views, key `event_id` | `1_bf`, `2_bfd`, `3_bftruncate` | none | no | `pipe_events/fishing`, not in sync | `incremental_events`: forbidden (claimed); three steps unknown |
| [`pipe_events/cross_version_fishing.py`](pipe_events/cross_version_fishing.py) | bindings pairwise, same views and key | `--binding name=ref`, repeatable; `--modes` per binding | by copy: seven of its eight inputs (`spatial_measures_20201105` is not pinned) | no | none | as above |
| [`port_visits/ais.py`](port_visits/ais.py) | modes pairwise, key `visit_id` (or `identifier`, `timestamp` for step 1 alone) | `1_bf`, `2_bfd`, `3_bftruncate`; opt-in `6_parallel` (step 1 only, compared with `1_bf` and judged against the declaration) | none | yes; `6_parallel` never reads it | `port_visits/ais`, in sync | `thin_port_messages`: allowed (claimed); `port_visits`: unknown |
| [`port_visits/cross_version_ais.py`](port_visits/cross_version_ais.py) | bindings pairwise, same keys; with `--baseline-production`, one candidate against production's own output | `--binding name=ref` | by copy; time-travel views with `--baseline-production` | through each binding's `ais.py` | none | as above |
| [`encounters/ais.py`](encounters/ais.py) | modes pairwise on the raw and the merged table, key `encounter_id` | `1_bf`, `2_bfd`, `3_bftruncate` | none | no | `encounters/ais`, not in sync | `create_raw_encounters`: allowed (claimed); `merge_encounters`: unknown |
| [`encounters/vms.py`](encounters/vms.py) | as `encounters/ais.py` (same code, VMS defaults) | as above | none | no | `encounters/vms`, not in sync | as above |
| [`pipe_segment/identity_match_key.py`](pipe_segment/identity_match_key.py) | bindings pairwise over eight output tables of the six-step segment group, per-table keys | `--binding name=ref` | by copy: `normalized_messages` over the run's dates | no | `pipe_segment/identity_match_key`, in sync; plus a frozen spec for v5.0.3 | `segment`: forbidden (claimed); five steps unknown |
| [`pipe_segment/ais.py`](pipe_segment/ais.py) | (mode, segmenter) variants pairwise on `segments`, `messages_segmented`, `fragments` | `1_bf`, `2_bfd`, `3_bftruncate`, opt-in `4_daily`; segmenter `incremental` or `full-history` | by copy, as above | no | `pipe_segment/ais`, in sync | as above |

Notes on the columns:

- **Exit codes.** Mode-equivalence workflows and `outage_recovery.py` fail (exit 1) on any difference: the outputs must be equal. Cross-version wrappers report differences and fail only when a binding fails: a difference there is information.
- **Run cache** means a mode with unchanged code, image and parameters reuses its earlier output table (`dit_runs`); `--no-cache` recomputes.
- **Cancelling.** `make dit-cancel RUN_ID=<id>` finds Dataflow jobs by their `dit_run_id` label. The pipe-gaps, port-visits and encounters workflows and `pipe_segment/ais.py` stamp it; `pipe_segment/identity_match_key.py` does not; the pipe-events workflows run no Dataflow jobs.
- **Runtime.** pipe-gaps submits Beam in-process (`dit.runners.dataflow`). Port visits, encounters and pipe-segment run Beam inside the pipeline's container (`dit.runners.docker`). pipe-events runs BigQuery SQL inside its container; pipe-segment's last four steps do too.

## Orchestration declarations (`orchestration.py`)

Each package has an `orchestration.py` declaring, per pipeline step, whether slices of a window may load in parallel, whether a run depends on the previous one, and on what evidence (`verified` with a Cloud Build id, `claimed` with its source, `untested`). The type and the judgement of a test outcome live in `dit.orchestration`; how the declarations map to Airflow (`depends_on_past`, `max_active_runs`) is in [`docs/plan.md`](../docs/plan.md) § "Orchestration capabilities". Seeded 2026-11-24 from the owner's statements; no declaration is `verified` yet. A run's result is recorded by editing the file, never automatically.

| File | Steps declared | Tested by |
|---|---|---|
| `port_visits/orchestration.py` | `thin_port_messages` (allowed, claimed), `port_visits` (unknown) | `port_visits/ais.py --modes 1_bf,6_parallel --steps thin_port_messages` |
| `encounters/orchestration.py` | `create_raw_encounters` (allowed, claimed), `merge_encounters` (unknown) | nothing yet |
| `pipe_events/orchestration.py` | `incremental_events` (forbidden, claimed), three more (unknown) | nothing yet |
| `pipe_gaps/orchestration.py` | `detect` (forbidden, claimed with "probably") | nothing yet |
| `pipe_segment/orchestration.py` | `segment` (forbidden, claimed), five more (unknown) | nothing yet |

## Drift check against production (`composer_drift.py`)

The `composer_drift.py` files are not test workflows. They are a read-only check that compares what each workflow would run with production's rendered DAG in composer-dags-production. Nothing runs on GCP or in docker ([PIPELINE-4700](https://globalfishingwatch.atlassian.net/browse/PIPELINE-4700), [`docs/composer-snapshots-2026-10.md`](../docs/composer-snapshots-2026-10.md) § 8).

- `composer_drift.py` (this directory) is the runner: `dit run workflows/composer_drift.py --composer-dir <checkout>`. It prints one row per workflow step (version, command, flag counts); `--detail` adds the full reports, which contain production values and are for local reading only. `--frozen` adds the frozen specs.
- `<pkg>/composer_drift.py` holds the spec of each workflow in that package: the composer snapshot and tasks it mirrors, and what may differ.

| Spec | Workflow | Composer snapshot | Expected in sync today |
|---|---|---|---|
| `port_visits/ais` | `port_visits/ais.py` | `core_ais_v5` | yes |
| `encounters/ais` | `encounters/ais.py` | `core_ais_v5` | no: older CLI generation |
| `encounters/vms` | `encounters/vms.py` | `core_vms_v5_daily` | no: same |
| `pipe_events/fishing` | `pipe_events/fishing.py` | `core_ais_v5` | no: older CLI generation |
| `pipe_gaps/mode_equivalence` | `pipe_gaps/mode_equivalence.py` | `core_ais_v5` | no: older pipe-gaps generation |
| `pipe_segment/identity_match_key` | `pipe_segment/identity_match_key.py` | `core_ais_v3` | yes, all six tasks of the segment group, at v5.1.0 with `--incremental` |
| `pipe_segment/identity_match_key@v5.0.3` (frozen, `--frozen`) | the same workflow for a binding at v5.0.3 (its declared adjustment removes `--incremental`) | `core_ais_v3` at composer `cb0c9a3b7930`, the last commit at which production ran v5.0.3 | yes; a regression test of DIT, not of production |
| `pipe_segment/ais` | `pipe_segment/ais.py` | `core_ais_v3` | yes. Two steps mirror the one `segment` task: the default incremental job, and the full-history reference job, which leaves `--incremental` out on purpose |

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

### Note (2026-11-20): a third consumer of the single-day slices, and a second image guard

`pipe_segment/ais.py` slices dates like `port_visits/ais.py` and
`encounters/ais.py` (bf, backfill-then-daily, backfill-then-rerun), and adds a
fourth shape, one job per day. It holds the slicing as a pure function
(`plan_slices`, with `check_chain` asserting that the jobs are consecutive), which
is the form a shared `dit.phases` would take. It also copies port visits' image
guard (`check_images`, `_default_images_executed`): two workflows now default to a
DIT overlay image and refuse unreviewed code on it. Neither is extracted here;
both are candidates for the extraction above.

### Update (2026-11-24): date slicing extracted as `dit.slices`

The slice generators are now one library module, `dit.slices` (days, calendar
weeks, calendar months, N-day chunks, an explicit list; a spec string selects
one; `check_partition` checks that slices cover a window exactly once). Its
consumers: `pipe_segment/ais.py` (the daily tails and `4_daily`, through
`by_day`; `plan_slices` and `check_chain` stay in the workflow, unchanged in
behaviour), and `port_visits/ais.py` (the daily tails, through a pure
`mode_slices`, and the slices of `6_parallel`, from `--slices`). `encounters/ais.py`
still slices on its own. What was extracted is the date arithmetic only; how a
slice is executed stays in each workflow, as planned above. `dit.phases` as a
mode framework is still not extracted.
