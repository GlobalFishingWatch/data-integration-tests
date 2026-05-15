# Context for `data_integration_tests`

This doc captures the background behind this repo, written for whoever picks up
the work — including future Claude sessions started in this directory.

## What this repo is

A standalone home for cross-pipeline integration tests at GFW. Three pipelines
are in scope, in order of migration:

1. **pipe-gaps** (`/mnt/encrypted_data/git/pipe-gaps`) — already has a working
   Python-first integration test. **Phase 1 ports it here.**
2. **anchorages_pipeline / port-visits** (`/mnt/encrypted_data/git/anchorages_pipeline`)
   — needs first-time integration tests. **Phase 2 builds them on top of the
   helpers extracted in Phase 1.**
3. **pipe-events / fishing** (`/mnt/encrypted_data/git/pipe-events`) — has a
   bash-only ancestor of the same idea, no automated comparisons. **Phase 3
   ports + extends it, then decides whether to extract framework primitives.**

The `dit` console script (placeholder name) drives a workflow file (Python),
which orchestrates phases (pipeline invocations) across modes (trigger
patterns: bf / bfd / bftruncate / mutate-recover) and asserts equivalence on
the resulting tables via `table-check summary`.

See [`plan.md`](plan.md) for the migration plan and [`framework-vision.md`](framework-vision.md)
for the longer-term shape this is evolving toward.

## Why this matters — bugs the integration test caught

While building this in pipe-gaps' repo, the four-mode equivalence test caught
**two real bugs** in a candidate fix that pipe-gaps' unit tests had not. This
is the strongest argument for the framework: real-BQ multi-mode equivalence is
a class of bug detection that unit tests don't replicate.

**Bug 1 — `ceil(duration_h / 24) - 1`**: the new "open-v1 seed" code in
`process_group.py` and `process_boundaries.py` skipped seed emission for any
gap with `duration_h < 24h`. For cross-midnight gaps in the dead zone of
sliding-window detection (window's 12h offset zone), this caused gaps to
permanently disappear from `raw_gaps` after the (W+1)th daily reprocessing
run. **Fixed in the pipe-gaps working tree on branch
`PIPELINE-3900-pipe-gaps-fix-missing-gaps-in-vms-when-range-load-processed-gaps-are-reprocessed`.**

**Bug 2 — `Boundaries.get_first_message_inside_range`**: pre-existing in
pipe-gaps, latent until Bug 1 was fixed. The function iterates boundaries by
their `first_message()` (== first message ≥ window_start + offset_h) and
returns the first one ≥ `date_range[0]`. When `date_range[0]` lands inside a
window (every daily-tail iter ≥ 2), the correct ON message often lives in
that window's `end` list rather than its `first_message` — so the function
skips past it and returns the next boundary's first message, ~24h too late.
Result: 8 gaps had their `end_timestamp` shifted forward by 12-26h in the
incremental modes vs the range-mode oracle. **Fix proposed:** iterate
`start ∪ end` of all boundaries and return the earliest in-range candidate.

Both bugs were invisible to pipe-gaps' unit tests because the unit tests don't
exercise the v1-seed surviving multiple daily-reprocessing rounds. The
integration test caught them by comparing `_2_bfd` / `_3_bftruncate` /
`_4_mutate_recover` against `_1_bf` (the range-mode oracle) on real BQ data.

## Source material to port

The pipe-gaps integration test is on branch
`testing/orchestration_equivalence_integration_tests` at
`/mnt/encrypted_data/git/pipe-gaps/tests/integration/mode_equivalence.py`
(~870 lines). Key sections to lift in Phase 1:

- **Runner dispatch** (`_RUNNERS` dict at ~line 380, plus `_run_local`, `_run_docker`,
  `_run_dataflow`).
- **`_run_docker`** (~lines 257–281): builds dev image once, uses unique
  `docker compose -p` project name per invocation to avoid network races.
- **`_run_dataflow`** (~lines 286–399): bypasses `detect_main.run` to split
  submission from waiting via `_DATAFLOW_SUBMIT_LOCK`. Includes the
  `_DagFactoryWithTempDataset` override that injects a pre-existing temp
  dataset to avoid `bigquery.datasets.create`.
- **Mode functions** `execute_bf`, `execute_bfd`, `execute_bftruncate`,
  `execute_mutate_recover` (~lines 430–540).
- **`compute_restricted_ssvids`** (~lines 540–640): queries `_1_bf_last_versions`
  for triggering closed gaps, picks `~|G|/2` non-triggering ssvids so the
  complement is guaranteed to contain every triggering ssvid.
- **`compare_tables`** (~lines 632–650): shells out to `table-check summary`.

`_run_local` is **dropped** in Phase 1 — it imports `pipe_gaps.pipelines.detect.main`,
which we don't want as a cross-repo dependency. Docker (DirectRunner-in-container)
replaces it for fast iteration.

## Decisions already made (don't relitigate)

- **Workflows are Python files**, not YAML or DSL. Pipeline configs are dataclasses
  already; jump-to-definition wins.
- **SCD-2 vs `WRITE_TRUNCATE` mismatch** (port visits doesn't use SCD-2) is
  absorbed by `compare_tables` kwargs, not by separate primitives.
- **`Phase` / `Mode` / `Mutation` / `Oracle` dataclasses are NOT extracted in
  Phase 1.** Workflows stay imperative until three consumers exist; that's the
  right sample size to decide whether to abstract.
- **Phase 2 ships AIS and VMS port-visits workflows** (production runs both).
- **Pipeline images are published on a registry**, referenced by tag. No
  `docker compose build` in the default Phase 1 path. (Pipe-gaps' workflow
  may need `--build-from-source` as a fallback if its image isn't published
  yet — see the plan's open items.)

## Production parameter sources

- `/mnt/encrypted_data/git/composer-dags-production/dags/core/ais/v3.py` — AIS daily DAG (1-day window, max_active_runs=5, no depends_on_past).
- `/mnt/encrypted_data/git/composer-dags-production/dags/core/vms/v3.py` — VMS daily DAG (4-day rolling, depends_on_past=True, max_active_runs=1).
- `/mnt/encrypted_data/git/composer-dags-production/dags/core/vms/config.py` — VMS-specific param overrides (`min_gap_length=1` instead of default 4).
- `/mnt/encrypted_data/git/composer-dags-production/gfw/pipes/v3/detect_gaps.py` — `RawGapsConfig` defaults.

These will feed `workflows/port_visits/params.yaml` in Phase 2.

## Branch state at handoff

- `pipe-gaps` branch `testing/orchestration_equivalence_integration_tests` (pushed)
  contains the integration test as of the migration starting point.
- `pipe-gaps` branch
  `PIPELINE-3900-pipe-gaps-fix-missing-gaps-in-vms-when-range-load-processed-gaps-are-reprocessed`
  has Bug 1's fix in the working tree (uncommitted as of last cutover) plus
  base_gap fix. Bug 2's fix is proposed; the integration test surfaces it as
  8 diff rows on `_1_bf` vs `_2_bfd`.

## What "next-level" looks like beyond Phase 3

Per [`framework-vision.md`](framework-vision.md):

- **Phase 4**: sync `params.yaml` from `composer-dags-production` automatically.
- **Phase 5**: promote ssvid-restriction into a `dit.mutations` library
  (`drop_messages`, `shift_timestamps`, `set_segment_flag`, ...). Cap at ~5
  mutations.
- **Phase 6**: hash-based phase sharing — second invocation of an identical
  phase becomes a `BQ COPY` instead of a re-run. Cuts wall-clock for CI.
- **Phase 7**: golden-table mode for cheap PR-validation regression checks.
- Cross-version testing via docker tags: a workflow can declare
  `mode "1_bf'" using image:gfw/pipe-gaps:pr-NNN` to compare PR builds against
  `:main` builds. Tier-1 of cross-version testing per the vision doc; cheap to
  build because the docker runner already exists.
