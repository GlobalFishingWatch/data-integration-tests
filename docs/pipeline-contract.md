# Pipeline contract for `dit` integration tests

This doc describes the **interface a GFW processing pipeline should expose** to be cleanly integration-testable by [`dit`](../README.md). Audience: pipeline maintainers (`pipe-gaps`, `anchorages_pipeline`, `pipe-events`, and future pipelines).

The contract is shaped by what's gone wrong (and right) running integration tests against the three current pipelines. Each section is grounded in concrete examples from those pipelines.

The contract is **non-binding**: a pipeline that doesn't satisfy it can still be tested with bespoke workarounds (we've patched two of them so far). But satisfying it means the integration-test workflow stays small and the test runs reliably under the project's least-privilege test SA.

## Universal — required for every pipeline

### 1. Parameterized date range

The pipeline accepts `--start_date` and `--end_date` (or equivalent) as `YYYY-MM-DD`. Settle inclusive vs half-open per pipeline and document it.

*Why:* bf/bfd/bftruncate mode-equivalence requires the workflow to call the pipeline with varying date ranges. A pipeline with a hardcoded range or one that always processes "since data_available_from" is not testable in the mode-equivalence frame.

### 2. Overridable source and output tables

Every BQ table reference (sources and outputs) must be configurable via CLI args. No hardcoded references to production datasets.

*Why:* the workflow points the pipeline at staging input datasets and per-suffix output tables (`<base>_<commit>_<uuid>_<mode>`). Hardcoded references force the test to either run against prod or fork the source.

### 3. Synchronous on exit

The CLI returns only when the pipeline has truly finished — not on submission. Exit 0 on success, non-zero on failure.

*Why:* `dit`'s runners block on the pipeline subprocess and tear down on `rc != 0`. A CLI that returns immediately after submitting an async Dataflow job would silently break mode-equivalence: the workflow advances thinking the slice is done when it isn't.

For Beam pipelines this typically means `--wait_for_job` (or local equivalent) is passed and respected. For BQ-SQL pipelines, blocking BQ queries suffice.

### 4. Idempotent re-runs over date ranges

Re-running on the same or overlapping date ranges produces identical output.

*Why:* bfd and bftruncate re-run the pipeline over tail days. If re-runs append instead of overwriting, or the result depends non-deterministically on which run executed last, mode-equivalence will fail.

Acceptable shapes: SCD-2 with a `_last_versions` view dedup, date-partitioned tables with pre-emptive partition DELETE, BQ session temp tables with explicit truncate-then-merge.

## Required if the pipeline uses Apache Beam

### 5. Temp-dataset override

The pipeline must offer a way to point Beam's `ReadFromBigQuery` (EXPORT method) at a pre-existing dataset, instead of letting Beam auto-create `beam_temp_dataset_<uuid>`.

*Why:* Beam's default behaviour requires `bigquery.datasets.create` on the worker SA. We don't grant that to the test SA, so without an override the pipeline fails on the first ReadFromBigQuery with a 403.

Two satisfactory shapes:

- **CLI flag** (recommended for new pipelines): `--temp_dataset <project>.<dataset>`. Pipeline plumbs it through to all `ReadFromBigQuery(..., temp_dataset=DatasetReference(...))` calls. Example: `pipe-anchorages.QuerySource` after the `dit-temp-dataset-support` patch.
- **Importable factory hook**: a subclassable factory class with an overridable `read_from_bigquery_factory` property. The workflow imports the class, subclasses it to inject `temp_dataset`, passes the subclass through the runner. Example: pipe-gaps' `DetectGapsLinearDagFactory`, consumed by `dit.runners.dataflow._wrap_factory_with_temp_dataset`.

The CLI flag is more universal (works for any submission shape — in-process or containerized). The factory hook is faster (no docker round-trip) but only works when the test runs Beam in the same Python process as the workflow. **Pipelines targeting cross-pipeline integration tests should default to the CLI flag.**

### 6. None-safe labels handling

If the pipeline reads `cloud_options.labels`, it must guard against None — or document that `--labels=k=v` must always be passed.

*Why:* Beam's `GoogleCloudOptions.labels` is None by default. Composer always passes `--labels=...` so production never hits the None path. Test or manual invocations may forget. `pipe-anchorages.cloud_to_labels` iterates the list with no None guard and raises `TypeError: 'NoneType' object is not iterable`. Treat missing labels as `[]`.

### 7. Standard Beam pipeline-options pass-through

The pipeline accepts the standard Beam pipeline-options surface unmodified: `--runner`, `--project`, `--region`, `--service_account_email`, `--temp_location`, `--staging_location`, `--subnetwork`, `--sdk_container_image`, `--labels`, etc.

*Why:* `dit`'s workflows construct Dataflow invocations from these flags. Pipelines that override or filter them break the contract. Standard if you use `PipelineOptions` without intercepting argv.

## Required if Beam runs inside the pipeline's container

These apply on top of §§5–7 for pipelines whose `pipeline.run()` is invoked by a CLI binary running inside the pipeline's container.

### 8. Workers' SDK image carries the pipeline package

Either:

- The pipeline image is published to a registry the worker SA can pull from, and the workflow passes `--sdk_container_image=<that image>` (composer pattern), OR
- The pipeline accepts a `--setup_file=setup.py` (or equivalent) so Beam stages the package as an sdist for workers.

*Why:* pickled DoFns reference the pipeline's modules. Workers need to `import` them. The stock Beam SDK image has only `apache_beam`, not the pipeline package.

## Required if the pipeline is non-Beam (BQ-SQL or similar)

### 9. Session-isolated parallel runs

Multiple invocations of the pipeline running concurrently must not collide on intermediate state. Pipe-events achieves this via BQ sessions (`_SESSION.foo`) — each invocation gets its own session, temp tables are session-scoped.

*Why:* `dit`'s workflows run modes in parallel under `--parallel`. Without isolation, concurrent invocations would race on shared intermediates.

### 10. All intermediate tables overridable

Multi-step BQ-SQL pipelines often have intermediate tables (pipe-events has `incremental_fishing_events_merged`, `incremental_fishing_events_filtered`, `fishing_events_v`, etc.). Each step's source and destination tables must be overridable.

*Why:* the workflow assigns each mode its own per-suffix output tables. Any hardcoded intermediate causes modes to collide on it. **Also enables step-skipping**: when the change under test lives only in step N, dit can point step N's input at an externally-supplied table (typically a snapshot of prod's step N-1 output) and skip running step N-1 entirely. Pipe-anchorages port-visits exercises this today via `--thinned-message-table` on `workflows/port_visits/ais.py`; pipelines that want the same optimisation must expose each step's input as a CLI flag.

## Strongly recommended for every pipeline

### 11. Vessel-cohort filter (INCLUDE semantics)

The pipeline accepts a `--ssvid_filter` (or equivalent) argument that restricts processing to a specified set of vessels. The filter takes either a comma-separated ssvid list, a SQL subquery returning an `ssvid` column, or an `@`-prefixed path to a file containing one of the above (pipe-anchorages' shape).

*Why,* two reasons compound:

- **Test-scale reduction**: integration tests can run on 10–100 vessels instead of the full cohort, cutting Dataflow worker-hours and BQ scan from hours to minutes. This is the primary driver — applies to every integration-test invocation, not just mode-equivalence.
- **Mutate-recover simulation**: the workflow can simulate "data for vessels X arrived after the pipeline already ran" by initially restricting processing to the complement of X, then re-running on the full set. Currently pipe-gaps-specific (Bug A trigger) but generally useful.

Recommended semantics: **INCLUDE** (the filter specifies vessels to *process*, not to *exclude*). Simpler to reason about. Mutate-recover's "process everyone except X" reduces to "INCLUDE the complement of X" computed at the workflow level — no need for the pipeline to expose EXCLUDE semantics directly.

This is the canonical example of a **test-scale knob** — a feature whose primary purpose is to make integration tests cheaper. Future vessel-events pipelines should follow the same shape.

## Nice-to-have

### 12. `--test` / `--dry-run` mode

A flag that prints the generated queries or pipeline graph without executing. Useful for catching template-rendering bugs without burning Dataflow worker time.

## Adoption matrix

Snapshot as of 2026-07-30 (encounters column added from the onboarding audit — see [`encounters-onboarding-2026-07.md`](encounters-onboarding-2026-07.md); pipe-events column re-verified during the Phase 3 port; see below). **pipe-segment is still missing a column** — `workflows/pipe_segment/identity_match_key.py` shipped 2026-06-08 without one; audit outstanding. **U** = universal, **B** = Beam-only, **C** = Beam-in-container, **S** = BQ-SQL-only, **R** = strongly recommended.

| # | Requirement | pipe-gaps | pipe-anchorages | pipe-events | encounters |
|---|---|---|---|---|---|
| 1 | Date range (U) | ✓ (half-open) | ✓ (inclusive) | ✓ (half-open) | ✓ (**inclusive**, documented in `--end_date` help on both steps) |
| 2 | Overridable tables (U) | ✓ | ✓ | ✓ | ✓ (every table a flag; `--source_table` / `--vessel_id_table` are appendable with `{ID}::` prefixes for multi-source) |
| 3 | Synchronous exit (U) | ✓ | ✓ (`--wait_for_job`) | ✓ | ✓ (`--wait_for_job`, both steps) |
| 4 | Idempotent re-runs (U) | ✓ (SCD-2) | ✓ (partitioned) | ✓ (truncate/merge) | ✓ (create: bounded pre-write `DELETE … BETWEEN start AND end` + append; merge: `WRITE_TRUNCATE`) |
| 5 | Temp-dataset override (B) | ✓ (factory hook) | ✓ (CLI flag, local patch — PR pending) | — | ✗ **no `--temp_dataset`** — same gap pipe-anchorages had; blocks the cloud path |
| 6 | None-safe labels (B) | ✓ | ✗ (workflow always passes `--labels`) | — | ✗ `list_to_dict(cloud_opts.labels)` raises on `None` (workflow must always pass `--labels`) |
| 7 | Beam options pass-through (B) | ✓ | ✓ | — | ✓ (`PipelineOptions` subclass) |
| 8 | SDK image w/ package (C) | — (in-process) | ✓ (`gfw/pipe-anchorages:<tag>`) | — | ✓ (DAG sets `sdk_container_image` = the scheduler image, which `pip install`s the package; `setup_file=None`) |
| 9 | Session-isolated parallel (S) | — | — | ✓ | — |
| 10 | Intermediate tables overridable (S) | — | — | ✓ | — |
| 11 | Vessel-cohort filter (R) | partial (`--restricted-ssvids`, EXCLUDE) | ✓ (`--ssvid_filter`, INCLUDE) | ✗ | ✓✓ `--ssvid_filter` on **both** steps (subquery, list, or `@path`) — the most capable of any onboarded pipeline |
| 12 | `--test` mode (nice) | — | — | ✓ | ✗ |

Legend: ✓ = satisfies, ✗ = missing, partial = present with caveats, — = N/A for this pipeline's architecture.

**pipe-events Phase 3 audit (2026-05-29).** Re-verified the pipe-events column against the source during the `workflows/pipe_events/fishing.py` port. All entries confirmed:
- **§1 (half-open):** the generate script passes `-end $end_d` as an exclusive incremental-query bound and `end_d = current_day + 1` for daily slices; the dit workflow uses `[start, end)`.
- **§4 (truncate/merge):** `incremental_events` accumulates into a persistent `_merged` table via `_SESSION` temp tables + truncate-then-merge; `auth_and_regions` / `fishing_restrictive` `WRITE_TRUNCATE` into date-versioned `_v{date}` tables with a view to the latest. **NOT SCD-2** (no `valid_from`/`valid_to`/`is_current`) — the comparison is the truncate shape on `event_id` via the latest-version view, not `_last_versions`.
- **§9 / §10:** `_SESSION`-isolated; every step's source/dest is a CLI flag (`-dest`, `-dest_tbl_prefix`, `-source_*`, `-mtbl`), so per-mode prefixes never collide.
- **§11 (✗):** no ssvid filter; test-scale reduction comes from the `pipe_ais_test_*` staging cohort instead. Filed as a future upstream issue (a `--ssvid_filter` with INCLUDE semantics would let pipe-events tests run on a vessel subset like pipe-anchorages does).
- **No workflow-side workarounds** were needed for pipe-events (contrast pipe-anchorages' `--temp_dataset` + None-labels patches) — the one infra addition was the docker runner's `volumes`/`service` params for the `gcp` auth volume, which is dit-side plumbing, not a pipeline workaround.

## Process: adding a new pipeline to `dit`'s scope

1. **Audit** the pipeline against this contract; fill in a new column in the adoption matrix above.
2. **Blockers** (Universal or applicable Required items missing) get fixed either:
   - In the pipeline (preferred; upstream PR), or
   - As a workflow-side workaround, **with a Plan-changelog entry in `CLAUDE.md` recording the trade-off**.
3. **Strongly Recommended** gaps don't block but should be filed as upstream issues and tracked.
4. **Update this doc** in the same commit as the workflow is added — both the matrix and any new pipeline-class-specific sections if the new pipeline's architecture isn't already covered.

The integration-test workflow must never carry pipeline-specific workarounds without an entry in the Plan changelog explaining why.
