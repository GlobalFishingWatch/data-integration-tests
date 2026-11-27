# Pipeline contract for `dit` integration tests

This doc describes the **interface a GFW processing pipeline should expose** to be cleanly integration-testable by [`dit`](../README.md). Audience: pipeline maintainers (`pipe-gaps`, `anchorages_pipeline`, `pipe-events`, `encounters_pipeline`, `pipe-segment`, and future pipelines).

The contract is shaped by what's gone wrong (and right) running integration tests against these five pipelines. Each section is grounded in concrete examples from them. Each pipeline's workflows and versions are listed in [`workflows/README.md`](../workflows/README.md).

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
- **Importable factory hook**: a subclassable factory class whose BigQuery reads take their keyword arguments from an overridable property. The workflow imports the class, subclasses it to inject `temp_dataset`, passes the subclass through the runner. Example: pipe-gaps' `RawGapsLinearDagFactory` (v0.17.1), whose sources and side input pass `read_from_bigquery_kwargs` to every read; consumed by `dit.runners.dataflow._wrap_factory_with_temp_dataset`. (Until 2026-11-27 DIT overrode `read_from_bigquery_factory` of v0.10.0's `DetectGapsLinearDagFactory` instead.)

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

*Why:* the workflow assigns each mode its own per-suffix output tables. Any hardcoded intermediate causes modes to collide on it. **Also enables step-skipping**: when the change under test lives only in step N, dit can point step N's input at an externally-supplied table (typically a snapshot of prod's step N-1 output) and skip running step N-1 entirely. Pipe-anchorages port-visits exercises this today via `--steps port_visits --thinned-message-table` on `workflows/port_visits/ais.py` (and `--steps thin_port_messages` runs step 1 alone); pipelines that want the same optimisation must expose each step's input as a CLI flag.

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

Each column says which version it was read at. That is the version DIT drives, which for two pipelines (pipe-events, encounters) is older than production's. The "Read at" column records the audit; for DIT's current default version against production's, the table in [`workflows/README.md`](../workflows/README.md) is the source of truth:

| Column | Read at | Production (composer, 2026-11-19/20) | Notes |
|---|---|---|---|
| pipe-gaps | v0.17.1 (2026-11-27, with the port; rows 3 and 7 carried over from v0.10.0) | v0.17.1 | v0.17.1 note below |
| pipe-anchorages | v5.0.1 (2026-11-19) | v5.0.1 | v5 notes below |
| pipe-events | v4.2.17 (Phase 3 port, 2026-05-29) | v9.0.0 | v9.0.0 note below |
| encounters | local 4.3.2 and published v4.4.0 (onboarding audit, 2026-07-30, [`encounters-onboarding-2026-07.md`](encounters-onboarding-2026-07.md)) | v5.0.0 | not re-audited at v5.0.0 |
| pipe-segment | v5.0.3 (2026-11-19); rows 5 and the mode-equivalence note at v5.1.0 (2026-11-20) | v5.1.0 (AIS v3 DAG) | pipe-segment note below |

**U** = universal, **B** = Beam-only, **C** = Beam-in-container, **S** = BQ-SQL-only, **R** = strongly recommended.

**Cells state UPSTREAM support, not whether dit can run.** A ✗ means the pipeline does not expose the thing; it does *not* mean the workflow is blocked, because dit may carry a workflow-side mitigation (always emitting `--labels`, an overlay image, a local patch). Where a mitigation exists the cell says so — read "✗ upstream, mitigated" as "works today, but on dit's crutch, and the crutch is retired when upstream lands". Keeping the two separate matters: the ✗ is what tracks the outstanding upstream ask, and per the working agreements every mitigation also needs a Plan-changelog entry recording the trade-off.

| # | Requirement | pipe-gaps | pipe-anchorages | pipe-events | encounters | pipe-segment |
|---|---|---|---|---|---|---|
| 1 | Date range (U) | ✓ (half-open: gfw-common's `--start-date` / `--end-date`, end exclusive, and the messages query reads `DATE(timestamp) < end_date`) | ✓ (**exclusive** `--end-date`, `end <= start` rejected; dit keeps inclusive `--start` / `--end` and passes `end + 1 day`) | ✓ (half-open) | ✓ (**inclusive**, documented in `--end_date` help on both steps) | ✓ (one flag, `--date_range START,END`, **inclusive on both ends**; on `segment`, `segment_identity` and `segment_vessel_daily`. `segment_info`, `vessel_info` and `segment_vessel` take no dates and rebuild from their whole input) |
| 2 | Overridable tables (U) | ✓ | ✓ | ✓ | ✓ (every table a flag; `--source_table` / `--vessel_id_table` are appendable with `{ID}::` prefixes for multi-source) | ✓ with one caveat: every source and output is a flag, and the messages source may be partitioned or date-sharded. The satellite-offset query reads date shards only (`<table>*` with `_TABLE_SUFFIX`), for the messages and for the satellite positions. **Gap:** the satellite timing offsets themselves are not an overridable input. `segment` computes them from three inputs and uses them in the same run; it cannot read an offsets table. **dit runs no satellite timing offsets** (note below) |
| 3 | Synchronous exit (U) | ✓ | ✓ (gfw-common's `Pipeline` waits for batch jobs; no flag. Production still passes the old `--wait_for_job`, which the CLI does not define) | ✓ | ✓ (`--wait_for_job`, both steps) | ✓ in practice: `--wait_for_job` on the two Beam commands, blocking queries in the four SQL commands. Weakness: `main()` drops the commands' return value, so a failure reaches the exit code only as an exception |
| 4 | Idempotent re-runs (U) | partial: rows appended to `<--bq-out-gaps>_versioned`, `--bq-out-gaps` a latest-state view, and the same range re-run gives the same rows. But the pre-write delete has no upper bound, so a run over `[D, E]` deletes every gap from `D` on and re-emits only up to `E`: a reprocess must run to the end of the data ([`workflows/pipe_gaps/CLAUDE.md`](../workflows/pipe_gaps/CLAUDE.md)). By design | ✓ (partitioned) | ✓ (truncate/merge) | ✓ (create: bounded pre-write `DELETE … BETWEEN start AND end` + append; merge: `WRITE_TRUNCATE`) | ✓ for the same range: each dated command deletes its date range, then appends; the other three truncate and rebuild. Caveat: `segment` reads the **whole** fragments table as history, so re-running an earlier range once later days exist is a different computation. Read, not tested |
| 5 | Temp-dataset override (B) | ✓ (factory hook: the `read_from_bigquery_kwargs` property, read by the messages source and the open-gaps side input) | ✗ upstream, **mitigated** — no option on any tag, including v5.0.1 (re-checked 2026-11-19). dit overlay image (`docker/overlays/pipe-anchorages-temp-dataset/`; decided 2026-10-22 and again 2026-11-19 not to upstream), since 2026-11-19 the workflows' **default image**. No Beam/Dataflow setting can substitute (verified 2026-10-22) | — | ✗ upstream, **mitigated** — no `--temp_dataset` in any published tag (same gap pipe-anchorages had); patch written (`dit-temp-dataset-support`, PR pending) and shipped meanwhile via a dit **overlay image**, so Dataflow/cloud runs work today | ✗ upstream, **mitigated** — four `ReadFromBigQuery` sites at v5.0.3 and five at v5.1.0, none with `temp_dataset`, and no option. dit overlay patches for both (`docker/overlays/pipe-segment-temp-dataset/`, 2026-11-20), and both pipe-segment workflows default to the v5.1.0 overlay image and hand it the dataset. Both images built and pushed 2026-11-24; `ais.py`'s Dataflow jobs ran on them in Cloud Build (2026-11-24/25) |
| 6 | None-safe labels (B) | ✓ explicit: `--labels` is required and an empty `PipelineConfig.labels` raises. dit passes them on every runner | partial: `--labels` is a required option — explicit rather than a crash on `None`; the workflow passes it on every runner | — | ✗ `list_to_dict(cloud_opts.labels)` raises on `None` (workflow must always pass `--labels`) | ✗ `BigQueryHelper` splits the labels with no `None` guard, in all six commands (`TypeError` without `--labels`). **Mitigated:** the workflow passes `--labels` on every step and every runner |
| 7 | Beam options pass-through (B) | ✓ | ✓ | — | ✓ (`PipelineOptions` subclass) | ✓ (unknown arguments go to `PipelineOptions`; Beam commands only) |
| 8 | SDK image w/ package (C) | — (in-process) | ✓ upstream (images are published for `main` and tags), but dit cannot use them as they are: row 5. dit runs its overlay build as both submitter and worker, by default the one of v5.0.1; any other commit needs its own overlay build, and dit refuses to auto-build one | — | ✓ (DAG sets `sdk_container_image` = the scheduler image, which `pip install`s the package; `setup_file=None`) | ✓ (the published image installs the package and carries the Beam 2.71.0 boot entrypoint; production passes it as `--sdk_container_image` with no setup file. It holds no source tree, so `--setup_file` cannot be used with it. Images are built for tags and `main` only) |
| 9 | Session-isolated parallel (S) | — | — | ✓ | — | ✓ for the four SQL commands: no sessions and no shared intermediates; each writes straight to its own destination |
| 10 | Intermediate tables overridable (S) | — | — | ✓ | — | ✓ (every step's inputs and outputs are flags; `--out_fragments_table` separates the fragments read from the fragments write) |
| 11 | Vessel-cohort filter (R) | partial: `--ssvids`, INCLUDE, a comma-separated list only (no subquery or file). dit's `4_mutate_recover` mode builds its restricted list on top of it | ✓ (`--ssvid-filter`, INCLUDE, on `port-state-transitions` only) | ✗ | ✓✓ `--ssvid_filter` on **both** steps (subquery, list, or `@path`) — the most capable of any onboarded pipeline | partial (`--ssvid_filter_query` on `segment` only, INCLUDE; later steps inherit it through the tables. Held in memory, meant for testing) |
| 12 | `--test` mode (nice) | partial: `--mock-bq-clients` runs without BigQuery, but prints no queries | — | ✓ (`--test`; a global `--dry-run` at v9.0.0) | ✗ | ✗ |

Legend: ✓ = satisfies, ✗ = missing, partial = present with caveats, — = N/A for this pipeline's architecture.

**pipe-events Phase 3 audit (2026-05-29).** Re-verified the pipe-events column against the source during the `workflows/pipe_events/fishing.py` port. All entries confirmed:
- **§1 (half-open):** the generate script passes `-end $end_d` as an exclusive incremental-query bound and `end_d = current_day + 1` for daily slices; the dit workflow uses `[start, end)`.
- **§4 (truncate/merge):** `incremental_events` accumulates into a persistent `_merged` table via `_SESSION` temp tables + truncate-then-merge; `auth_and_regions` / `fishing_restrictive` `WRITE_TRUNCATE` into date-versioned `_v{date}` tables with a view to the latest. **NOT SCD-2** (no `valid_from`/`valid_to`/`is_current`) — the comparison is the truncate shape on `event_id` via the latest-version view, not `_last_versions`.
- **§9 / §10:** `_SESSION`-isolated; every step's source/dest is a CLI flag (`-dest`, `-dest_tbl_prefix`, `-source_*`, `-mtbl`), so per-mode prefixes never collide.
- **§11 (✗):** no ssvid filter; test-scale reduction comes from the `pipe_ais_test_*` staging cohort instead. Filed as a future upstream issue (a `--ssvid_filter` with INCLUDE semantics would let pipe-events tests run on a vessel subset like pipe-anchorages does).
- **No workflow-side workarounds** were needed for pipe-events (contrast pipe-anchorages' `--temp_dataset` + None-labels patches) — the one infra addition was the docker runner's `volumes`/`service` params for the `gcp` auth volume, which is dit-side plumbing, not a pipeline workaround.

**pipe-anchorages v5 CLI re-audit (2026-10-22, PIPELINE-4699).** pipe-anchorages main (b36aed6, 191 commits past the v4.6.4 production pin) replaced the CLI dit's port-visits workflows were written against: `thin_port_messages` / `port_visits` became `port-state-transitions` / `port-visits`, underscore flags became `--bq-in-*` / `--bq-out-*`, `--end-date` became exclusive, `--labels` became required and `--wait_for_job` was removed. `workflows/port_visits/ais.py` then took `--pipeline-cli {v4,v5}` (default v4, byte-identical). Both emitted v5 commands were checked against the real CLI with mocked BigQuery clients. The drift went unnoticed because production then still pinned v4.6.4 and nothing tells dit when a pipeline's interface changes; how dit should stay in sync is PIPELINE-4700.

**pipe-anchorages v5.0.1, v4 path dropped (2026-11-19).** Production's `core.ais.v5` DAG runs pipe-anchorages v5.0.1 with the v5 CLI since 2026-11-04 (v5.0.0 from 2026-10-26). The audit of 2026-11-19 found dit's v5 argv still correct for v5.0.1: the CLI, the SQL and the schemas did not change between b36aed6 and the tag. The owner then decided:
- dit drops the v4 CLI and the `--pipeline-cli` switch entirely;
- the default image is dit's overlay build of v5.0.1 (no upstream request for a staging-dataset option);
- the 2020 staging cohort stays the default source for now.

The pipe-anchorages column above describes v5.0.1 only. Workflow-side mitigations still carried, each recorded in the Plan changelog:
- **Staging dataset (row 5).** The overlay image, now the default. Trade-off: the default image lives in dit's namespace and must be rebuilt at every production version bump. `composer_drift.py` reports the bump.
- **No auto-build.** The shared auto-build makes a plain image, without the overlay. The port-visits workflow opts out of it and refuses unreviewed code on a default image, before any cloud call.
- **Labels (row 6).** Passed on every runner.

Two other things changed up to v5.0.1. The port-visits output table now requires a partition filter (`require_partition_filter`); the production-baseline output pin is fixed for that in #89. gfw-common moved from 0.22.0 to 0.25.0, with no effect on the argv.

**pipe-segment audit (2026-11-19).** The column was filled in by reading pipe-segment at tag v5.0.3 (`f4d39fe`, also `origin/main`), the version the AIS DAG pinned and dit's default image at the time (since 2026-11-20 both are v5.1.0, see the notes below; the VMS v5 DAG still pins v5.0.3). Nothing was run, and the published image was not pulled: its contents are taken from the tag's Dockerfile.
- **Six commands, two kinds.** `segment` and `segment_identity` are Beam jobs. `segment_vessel_daily`, `segment_info`, `vessel_info` and `segment_vessel` are BigQuery queries run by the container. Rows marked B and C apply to the first two, rows marked S to the other four.
- **Dates (row 1).** `--date_range` is inclusive on both ends everywhere it exists: the pre-write delete is `>= start AND <= end`, the reads use `BETWEEN`, and `segment_vessel_daily` runs one query per day up to and including the end.
- **Cold start (row 4).** `segment` stitches new fragments onto every fragment already in `--fragments_table`. A dit binding starts from an empty table, so its segments are not production's for vessels active before the range. `segment_vessel_daily` reads a 30-day window of the identity table, which in a dit run holds only the run's days. Both are the same for every binding, so an A/B is fair.
- **Workflow-side mitigations**, each in the Plan changelog: `--labels` on every step and runner (row 6).
- **The temp dataset (row 5): overlay images built and run.** Without an overlay, a Dataflow run as `automated-testing@` is expected to fail when Beam tries to create its temp dataset, as pipe-anchorages and encounters did before their overlays. This is inferred. Since 2026-11-20 dit holds overlay patches for v5.0.3 and v5.1.0 ([`docker/overlays/pipe-segment-temp-dataset/`](../docker/overlays/pipe-segment-temp-dataset/README.md)); both images were built and pushed on 2026-11-24, and `ais.py`'s Dataflow jobs ran on them in Cloud Build. Both pipe-segment workflows default to the v5.1.0 overlay image and hand it the dataset through a container variable. Neither auto-builds a plain image: an unreviewed binding or checkout on the default image is refused before anything is pinned (`identity_match_key.py` since 2026-11-25).
- **Mode equivalence and the incremental segmenter (2026-11-20, read at v5.1.0, `4ddde03`).** v5.1.0 adds `--incremental`, `--in_segments_table` and `--in_fragments_table` to `segment` and keeps the rest of the v5.0.3 command line; the AIS v3 DAG is configured for v5.1.0 and passes `--incremental`, without the two table flags, since composer-dags-production PR #3145 of 2026-11-20 (rendered snapshot `core_ais_v3` at e4cb29361; earlier that day it still pinned v5.0.3, as the VMS v5 DAG does). So `ais.py` defaults to the incremental segmenter. `identity_match_key.py` first kept full history as an accepted difference; since 2026-11-24 its base passes `--incremental` too, and a binding at an older version loses the flag by a declared adjustment (`workflows/pipe_segment/adjustments.py`), checked by a frozen drift spec at v5.0.3. What `workflows/pipe_segment/ais.py` relies on, all read and none run:
  - **Row 4, the caveat made concrete.** A full-history job reads its fragments table with no date bound (`transform/read_fragments.py` lines 25 to 41), so the table must hold nothing but the run's own earlier days. The workflow gives each variant its own tables and refuses tables that hold rows.
  - **Incremental start-of-run checks** (`hooks.py`): every day of the range must have messages, and the segments of the day before the range must exist unless there are no earlier fragments. So jobs must be consecutive, and the workflow asserts that of every plan.
  - **Row 7 has a sharp edge.** Unknown arguments go to `PipelineOptions` (`cli/main.py` line 47, `parse_known_args`), so `--incremental` on an image older than v5.1.0 would not be rejected. Inferred: it would be ignored and the job would run full history. The workflow refuses `incremental` unless the image tag names v5.1.0 or later.
  - **Row 11:** `--ssvid_filter_query` also filters the prior-segments read of the incremental segmenter (`assets/queries/prior_segments.sql.j2`).
- **Recurring invocation gaps** (the list below): 1 needed, now done for all six steps; 2, 3 and 4 needed and done; 5 not needed, pipe-segment creates its own tables with the right partitioning before it writes; 6 not done, no run against the published image is recorded.
- **No satellite timing offsets in DIT (owner decision, 2026-11-20).** Production's `segment` passes three satellite inputs and an offsets output table. dit passes none of the four, and its segmenter workflows do not run the offsets at all. The option `--include-satellite-offsets` and the code behind it are removed.
  - **Why (row 2's gap).** The offsets must be computed from the full message feed, not from a test cohort: from the staging cohort's 142 vessels they come out different and noisy (January 2020: 990 receiver-hours above the 30 s threshold, against 183 in the cohort's own offsets table, 39 in common). The full feed is about 4.1 TB for 2020, too large to read on every test run. And `segment` cannot read precomputed offsets (`pipeline.py` lines 195 to 222 at v5.0.3), so the cohort's table cannot be passed in.
  - **Secondary:** the satellite positions are date-sharded tables in another organisation (366 tables and 139 GB for 2020), and the offsets query reads its message source through a table wildcard that a partitioned table does not match.
  - **Consequence.** On the 2020 staging cohort, dit's `messages_segmented` keeps the messages the correction would drop: 1,361 receiver-hours in 2020, on 27 receivers and 173 days, by the cohort's offsets table. For those hours dit's staging output is not comparable with production's historical output. Comparisons between two dit runs are unaffected.
  - **Scope in time.** The correction only ever applied to data that carries satellite receiver IDs. The current feed has none (since 2026-04-22), so production computes no offsets for current data either.
  - **What would close the gap:** an option in pipe-segment to read an offsets table instead of computing one. Then dit could pass the cohort's table. Not planned work.
  - The drift spec accepts production's four flags for the `segment` step, with this reason.
- **Source pinning.** Since 2026-11-19 the workflow copies `normalized_messages` over the run's `--date-range` at run start. `segment` reads the source by date between the two ends, with no lookback, and no other step reads it.

**pipe-events cross-version wrapper (2026-08-12).** `workflows/pipe_events/cross_version_fishing.py` landed as dit's second cross-version consumer (sibling of `port_visits/cross_version_ais.py`). It pins seven of the eight `fishing.py` source tables into `<project>.tech_great_expectations`: by snapshot at first, by copy since 2026-11-19 (`dit.bq.pin_source_into_experiment`, PIPELINE-4705; view sources are copied as plain tables). The eighth (`spatial_measures_20201105`) is content-addressed by its `_YYYYMMDD` filename suffix and skipped by design. Per-binding docker image identity comes free from `ensure_pipeline_image` reading each `git worktree`'s HEAD — no `--binding-image` flag surface (contrast port_visits' Dataflow-shaped `--binding-worker-image`). Diffs both `_fishing_events` and `_product_events_fishing` views pairwise on `event_id`. Enabled by E4 (2026-08-12), which added eight `--source-*-fqn` flags to `fishing.py` — the same pattern port_visits' M4 introduced there.

**pipe-gaps v0.17.1 (read 2026-11-27 with `git show`, and built offline in a venv at v0.17.1; nothing run on Dataflow).** dit's workflows run v0.17.1 since 2026-11-27, the version the AIS v5 DAG is configured for (composer, read 2026-11-27; not a production job). What changed against v0.10.0:
- **Row 1.** `detect` became `raw-gaps`, and `--date-range` became gfw-common's `--start-date` / `--end-date`. Same half-open window: the workflows' job windows did not move.
- **Row 2.** Still ✓, but the output changed shape: rows go to `<--bq-out-gaps>_versioned`, and `--bq-out-gaps` names a latest-state view, created by a post-hook, which drops closed gaps of `duration_h <= min_gap_length`. dit compares the views and records both view and table for cleanup.
- **Row 4.** The pre-write delete is the same query as at v0.10.0 (`gaps_delete.sql.j2` unchanged, `git diff v0.10.0 v0.17.1`): the reprocess contract still holds. A test pins it against the installed pipe-gaps.
- **Row 5.** The factory still has gfw-common's `read_from_bigquery_factory`, but dit now adds `temp_dataset` through `read_from_bigquery_kwargs`, which also carries pipe-gaps' BigQuery job labels. Offline, in a venv at v0.17.1, both BigQuery sources of the built graph carry the dataset and the labels; not run on Dataflow.
- **Open gaps between runs.** Without `--bq-in-open-gaps`, a job reads the open gaps from its own versioned table, from `--open-gaps-start-date` on. The wiring is v0.10.0's (which read them from its output table); the query is not: `gaps.sql.j2`, the query behind the side input and the view, was rewritten between v0.10.0 and v0.17.1: the start-date filter now comes before the dedup to each gap's last version; a gap counts as open only if it has no closed row (v0.10.0: if its last version was open); and with `min_gap_length` set (the view, not the side input) closed gaps of `duration_h <= min_gap_length` are dropped. Verified on real data only by the first live run.
- **Rows 11 and 12.** `--ssvids` and `--mock-bq-clients` are still there.

**pipe-events v9.0.0, what production runs (read 2026-11-25 with `git show`; nothing run).** The column describes v4.2.17. Against the contract:
- **Rows 1, 2, 10.** The entrypoint is `pipe-events` and the commands are renamed (`fishing_events_incremental`, `fishing_events_incremental_filter`, `fishing_events_auth_and_regions`, `fishing_events_restrictive`). Flags moved to `--bq-in-*` / `--bq-out-*` names; every source and destination is still a flag. Production passes inputs v4.2.17 had no counterpart for (composer-snapshots § 8d). Not checked: whether the outputs are still `_v<date>` tables with a view to the latest.
- **Row 9.** Still ✓: `fishing_events_incremental` begins and ends a BigQuery session.
- **Row 11.** Still ✗: no vessel filter.
- **Row 12.** ✓: a global `--dry-run` runs BigQuery dry runs only.
- **Comparison noise, unchanged.** The authorization query still builds `public_authorizations` with `ARRAY_AGG` and no `ORDER BY`, and serialises it to a JSON string. Mode comparisons therefore report differences on equal events ([`workflows/pipe_events/README.md`](../workflows/pipe_events/README.md)).

## Recurring invocation gaps (check all of these on every new pipeline)

Found the hard way onboarding encounters (2026-07-30, six failed smokes). None are visible in unit tests; all are re-encounterable.

1. **Emit `--labels` on EVERY runner, not just Dataflow.** Several GFW pipelines do `list_to_dict(cloud_opts.labels)` with no `None` guard, and the transform is often constructed regardless of runner (pipe-anchorages and encounters both).
2. **`--project` is needed on DirectRunner too** — pipelines commonly build their own `bigquery.Client(project=cloud_opts.project)`.
3. **`--temp_location` is needed on DirectRunner too** — `ReadFromBigQuery`'s EXPORT method stages via GCS on any runner. NOT the same knob as `--temp_dataset` (the BQ dataset for the temp table), which is what the Cloud Build SA cannot create and is the usual *cloud* blocker.
4. **`container_env={"GOOGLE_CLOUD_PROJECT": …}`** — Beam's `WriteToBigQuery` builds its own BQ client inside the SDK worker, which reads the env var and never sees `--project`. Hit by pipe-segment (2026-06-03) and encounters; a third consumer should make it a `dit.runners.docker` default.
5. **Destination tables may need pre-creating, WITH the sink's partitioning.** If a pipeline stamps table metadata (`get_table()`) before its `CREATE_IF_NEEDED` sink runs, a fresh table 404s. dit mints fresh tables per run while prod reuses long-lived ones, so **dit is often the first thing to exercise a bootstrap path**. Take schema/partitioning/clustering from the **sink's `additional_bq_parameters`**, not from the DAG's convenience `ensure_table` task — the latter can be an unpartitioned no-op that would itself fail. Read schemas out of the image rather than hardcoding a copy that can drift.
6. **Validate flags against the PUBLISHED IMAGE, not the repo source.** Encounters' published `v4.4.0` was Python 3.12 with a renamed package; local master was `4.3.2` on Python 3.8. `docker run --entrypoint <cli> <image> <subcommand> --help` turns assumptions into facts.

## Process: adding a new pipeline to `dit`'s scope

1. **Audit** the pipeline against this contract; fill in a new column in the adoption matrix above.
2. **Blockers** (Universal or applicable Required items missing) get fixed either:
   - In the pipeline (preferred; upstream PR), or
   - As a workflow-side workaround, **with a Plan-changelog entry in `CLAUDE.md` recording the trade-off**.
3. **Strongly Recommended** gaps don't block but should be filed as upstream issues and tracked.
4. **Update this doc** in the same commit as the workflow is added — both the matrix and any new pipeline-class-specific sections if the new pipeline's architecture isn't already covered.
5. **Add a drift spec** (`workflows/<pkg>/composer_drift.py`) in the same change, and run `dit run workflows/composer_drift.py --workflow <name> --detail`. The spec names the production task each step mirrors, and the check compares the workflow's version, command and flags with it. A workflow is "in sync" when nothing differs beyond the differences its spec accepts with a reason. The same applies when an existing workflow is ported to a new version of its pipeline. See [`composer-snapshots-2026-10.md`](composer-snapshots-2026-10.md) § 8c.

The integration-test workflow must never carry pipeline-specific workarounds without an entry in the Plan changelog explaining why.
