# Workflow notes and run records

Notes about single workflows that used to sit in the README's feature list and roadmap. They were moved here on 2026-11-25 so that the README can stay an index. The text of each note is unchanged, apart from link paths and three cross-references that pointed at a neighbouring README entry. Headings were added.

How to read this page:

- **It is a holding place.** Per-pipeline docs live next to the workflows ([`workflows/README.md`](../workflows/README.md), `workflows/<pkg>/`). Where one of them covers the same point, that doc is the current one.
- **It is not the status source.** Status and priority are in [`README.md`](../README.md) § Roadmap.
- **Dates matter.** Each note says when it was true. "Not run" means not run at that date.
- For the concepts the notes use (bindings, pins, overlays, the run cache), see [`framework.md`](framework.md).

Contents:

- [Workflow inventory](#workflow-inventory)
- [Run cleanup coverage](#run-cleanup-coverage)
- [pipe-segment](#pipe-segment)
- [Port visits](#port-visits)
- [Fishing events](#fishing-events)
- [pipe-gaps outage recovery](#pipe-gaps-outage-recovery)
- [Drift check: state per workflow](#drift-check-state-per-workflow)

---

## Workflow inventory

- **Ten workflows across five pipelines today**: `pipe_gaps/mode_equivalence.py` (detect-gaps; in-process Beam; SCD-2 compare) and `pipe_gaps/outage_recovery.py`; `port_visits/ais.py` (Beam-via-container; truncate compare on `visit_id`) and `port_visits/cross_version_ais.py`; `pipe_events/fishing.py` (fishing events; **BQ-SQL-via-container**; truncate compare on `event_id`) and `pipe_events/cross_version_fishing.py` (the fishing-events sibling of port-visits' cross-version wrapper — docker-native, no `--binding-image` needed since each binding's image comes from its worktree's HEAD); `pipe_segment/identity_match_key.py` (cross-version A/B over all six commands of production's segment group: two Beam jobs, then four BigQuery SQL steps; per-table compare keys; **without satellite timing offsets**, see "No satellite timing offsets" below) and `pipe_segment/ais.py` (mode equivalence of the `segment` step, see "pipe-segment" below); and `encounters/ais.py` + `encounters/vms.py` (encounters **generation**; truncate compare on `encounter_id` over both the raw and merged tables). Most are faithful ports of that pipeline's bash/CLI mode-equivalence test plus the automated cross-mode comparison. `encounters/vms.py` is the first workflow to target VMS rather than the AIS staging cohort, and shares its entire execution path with `encounters/ais.py` via `ais.run(args)` — the two differ only in defaults and help text. See [`workflows/README.md`](../workflows/README.md).

---

## Run cleanup coverage

`make dit-cancel` finds a run's Dataflow jobs by the `dit_run_id` label and its tables through its `dit_runs` rows ([`framework.md`](framework.md) § 6). Coverage differs per workflow (state of 2026-11-25, from the README's cleanup note):

| Workflow | Stamps `dit_run_id` on its jobs | Records its tables in `dit_runs` |
|---|---|---|
| `pipe_gaps/mode_equivalence.py` | yes | yes |
| `pipe_gaps/outage_recovery.py` | yes | yes |
| `port_visits/ais.py` (and `cross_version_ais.py` through it) | yes | yes |
| `encounters/ais.py` (and `encounters/vms.py` through it) | yes | no (no run cache) |
| `pipe_segment/ais.py` | yes | no (no run cache). A cancel stops its jobs and leaves its tables. |
| `pipe_segment/identity_match_key.py` | no | no |
| `pipe_events/fishing.py`, `cross_version_fishing.py` | no Dataflow jobs | no |

---

## pipe-segment

Commands and results: [`workflows/pipe_segment/RUN_INSTRUCTIONS.md`](../workflows/pipe_segment/RUN_INSTRUCTIONS.md).

### Mode equivalence of the segment step (`ais.py`)

- **Mode equivalence for the segmenter** (`workflows/pipe_segment/ais.py`, 2026-11-20). Runs pipe-segment's `segment` step over one window in several shapes and compares `segments`, `messages_segmented` and `fragments` across them. **Run in Cloud Build** (2026-11-24/25) on both overlay images, built 2026-11-24: the default run, the PR #227 test against the v5.0.3 base (45 jobs, 15 comparisons) and ten days of production input (v5.0.3 against v5.1.0) all IDENTICAL; results in [`RUN_INSTRUCTIONS.md`](../workflows/pipe_segment/RUN_INSTRUCTIONS.md#mode-equivalence-of-the-segment-step-aispy).
  - **The unit is a (mode, segmenter) variant.** Modes: `1_bf` (one job), `2_bfd` (backfill, then one job per tail day), `3_bftruncate` (full backfill, then the tail days rerun in order) and `4_daily` (every day its own job; opt-in). Segmenters: `incremental`, pipe-segment's `--incremental` (v5.1.0), and `full-history`, pipe-segment's own default. A `--modes` entry is a mode, run with every segmenter of `--segmenter`, or `mode:segmenter` for exactly that pair.
  - **The default follows the AIS DAG.** Since 2026-11-20 the AIS v3 DAG's `segment` task is configured for v5.1.0 and passes `--incremental` (composer-dags-production PR #3145; the VMS v5 DAG still pins v5.0.3 and passes no such flag). So the default segmenter is `incremental`, and a run without `--modes` is `1_bf:full-history,1_bf,2_bfd,3_bftruncate`: full history in one job as the reference, against three incremental shapes. Four variants, ten jobs on the default window, 363 MB pinned.
  - **A base version next to the head** (2026-11-24). A `--modes` entry ending in `@base` runs on `--base-image`, an overlay image of an earlier pipe-segment version; the others run on the run's own images. So one run holds the base, the head in the base's mode and the head in its new mode (`--modes 1_bf:full-history@base,1_bf:full-history,1_bf:incremental`). Both sides read the same pinned copy and get the same flags; a base whose command line differs in more than the `incremental` flags would need per-side adjustments, which so far only the cross-version workflow `identity_match_key.py` has (see "Per-side adjustments" below). No default base image; a base variant is the reference when selected.
  - **Short jobs on DirectRunner** (2026-11-24): `--direct-runner-max-days N` runs every job of N days or fewer on DirectRunner in the container, longer ones on Dataflow. For the staging cohort's daily jobs, where Dataflow's start-up and shutdown take about four of six minutes. Set by hand. Ran inside Cloud Build on 2026-11-24 (build `967b1030`): three one-day jobs on DirectRunner, about 1.5 min each, IDENTICAL to a Dataflow reference on all three tables. The shared helper is `dit.workflow.runner_for_days`.
  - **Each variant has its own three tables, which must start empty.** A job reads its own fragments and segments tables as its history, so a run refuses tables that already hold rows. Jobs of a variant are consecutive days with no gap.
  - **`segments` is the discriminating comparison:** it holds the segment id carried from day to day and the cumulative values. `fragments` are built per vessel and day, so they detect chaining errors only weakly.
  - **`incremental` needs an image of v5.1.0 or later** and is refused before any cloud call otherwise: on older code pipe-segment would ignore the flag silently and the comparison would pass without testing anything. The default image is one; a run on the v5.0.3 overlay must select `--segmenter full-history`.
  - **Images:** both pipe-segment workflows default to a DIT overlay of v5.1.0 ([`docker/overlays/pipe-segment-temp-dataset/`](../docker/overlays/pipe-segment-temp-dataset/README.md)), because pipe-segment cannot be told where Beam stages query results. The directory also holds the patch for v5.0.3. For `ais.py`: no auto-build, and unreviewed code on a default image is refused, as for port visits. `identity_match_key.py` hands the overlay its dataset too. Since 2026-11-24 it passes the DAG's `--incremental` to every binding whose pipe-segment declares it, and removes it for older ones ("Per-side adjustments in `identity_match_key.py`" below).
- No run cache. `--dry-run` reports the pin estimate, the planned jobs and the comparisons, and creates nothing. Commands: [`workflows/pipe_segment/RUN_INSTRUCTIONS.md`](../workflows/pipe_segment/RUN_INSTRUCTIONS.md) § "Mode equivalence of the segment step".

### Run records: segmenter (from Roadmap 7e)

Run on 2026-11-24/25, all measured in Cloud Build. Both overlay images were built and pushed on 2026-11-24.

- Default run on January 2020 (`2e57a93b`): 10 Dataflow jobs, 9 comparisons IDENTICAL.
- PR #227 test with the v5.0.3 base (`4bb84ec0`): 45 jobs, 15 comparisons against `1_bf:full-history@base` IDENTICAL.
- Short jobs on DirectRunner (`967b1030`): IDENTICAL to a Dataflow reference.
- Production input, 2012-12-01..10, v5.0.3 against v5.1.0 (`56af531e`): `segments` and `fragments` IDENTICAL; `messages_segmented` showed 2 differences on one msgid shared by two vessels, a table-check pairing bug fixed in v0.3.1; the compare-only rerun (`de4e5831`) is IDENTICAL on all 9.

### Per-side adjustments in `identity_match_key.py`

- **Per-side adjustments in cross-version runs** (`dit.adjust`, 2026-11-24; first consumer `pipe_segment/identity_match_key.py`). By default every binding gets the same command line, inputs and outputs: a base that mirrors production. A binding whose pipeline differs gets edits on top, in layers, a later one winning: declared adjustments for a range of versions (`workflows/pipe_segment/adjustments.py`, chosen by what the binding's ref declares, then by its version), then ad hoc `--binding-adjust NAME=<step>:<op>:<flag>[=<value>]` for one run (`remove`, `add`, `rename`, `set`; placeholders `{out.<name>}`, `{in.<role>}`). No adjustment means byte-identical argv; an edit that finds nothing to change is an error.
  - **Today's one declared adjustment:** below v5.1.0, `segment` loses `--incremental`, `--in_segments_table` and `--in_fragments_table`.
  - **Preflight before pinning:** every pipeline flag of each binding's argv must be declared by that ref's CLI source (`git show`), Beam's options exempt on the Beam steps. pipe-segment hands unknown flags to Beam, which ignores them, so a wrong flag would otherwise do nothing silently.
  - **Logged per run:** each binding's edits with their reason, the flag difference per step between bindings, and a note naming the tables when bindings read different inputs (lineage is then the operator's claim).
  - **Also new on `identity_match_key.py`:** `--binding-image NAME=IMAGE` (that binding's submitter and worker image) and `--allow-same-gpsdio-pin` (for two pipe-segment versions with the same gpsdio-segment). A binding whose image is tagged another pipe-segment version than its ref states is refused before pinning (2026-11-25), except an unreleased head on the next release, which is only warned about.
  - **Older layers are drift-checked too:** a spec can be frozen at the composer commit where production last ran that version (`WorkflowSpec.composer_ref`). The pipe-segment v5.0.3 layer is in sync at composer `cb0c9a3b7930`; checked in the opt-in live test and with `workflows/composer_drift.py --frozen`, not in the default output.
  - **Table edits** (inputs and outputs per side, pinning the union, comparing by output name) are built but have **no real case yet**: no pipeline changes its tables between versions today, so they are tested with synthetic interfaces only.
  - **Not run:** no binding has run with adjustments; the checks are reads of code and composer snapshots.

### No satellite timing offsets

- **No satellite timing offsets in DIT** (owner decision, 2026-11-20). DIT's segmenter workflows do not run pipe-segment's satellite timing offsets. Production's `segment` task works out how far each satellite's clock is off and drops the messages of the affected hours; a DIT `segment` run does neither. `--include-satellite-offsets` is removed and rejected.
  - **Why:** the offsets must be computed from the full message feed, not from a test cohort (computed from the staging cohort's 142 vessels they come out different and noisy); the full feed is too large to read on every test run (about 4.1 TB for 2020); and pipe-segment has no option to read precomputed offsets.
  - **What it means:** on the 2020 staging cohort, DIT's `messages_segmented` keeps the messages the correction would drop (1,361 receiver-hours in 2020, by the cohort's own offsets table). For those hours DIT's output is not comparable with production's historical output. Comparisons between two DIT runs are not affected: both sides run without the correction.
  - The current feed carries no satellite receiver IDs (since 2026-04-22), so production computes no offsets for current data either.
  - Full statement and figures: [`workflows/pipe_segment/RUN_INSTRUCTIONS.md`](../workflows/pipe_segment/RUN_INSTRUCTIONS.md) § "No satellite timing offsets in DIT".

### Pinned copies (with pipe-gaps outage recovery)

- **Pinned copies for pipe-gaps outage recovery and pipe-segment** (2026-11-19, [PIPELINE-4705](https://globalfishingwatch.atlassian.net/browse/PIPELINE-4705)). `pipe_gaps/outage_recovery.py` and `pipe_segment/identity_match_key.py` pin by copy at run start too, with the same flags: `--reuse-pin <stamp>`, `--pin-max-gb`, `--pin-expiration-days`, and a `--dry-run` that reports each copy's bytes against the cap.
  - **What is copied** is what the pipeline reads. Outage recovery: the messages over `[--start minus pipe-gaps' lookback, --end]` and the segments whole. pipe-segment: `normalized_messages` over the run's `--date-range`.
  - **Outage recovery extras:** `--no-pin` reads the live sources (for sources that do not change). `--synthetic-outage` filters the pinned copy for stages 1 and 2. The run cache is keyed on the copies' names, so a `--reuse-pin` rerun hits and a new pin misses.
  - **Gone:** `--pin-at`, `--pin-source-at`, `--snapshot-dest-project`, `--snapshot-expiration-days`, `--skip-snapshots`, `--no-snapshot`. Each is rejected with what replaces it.
  - **pipe-segment's `--include-satellite-offsets` is removed:** DIT runs no satellite timing offsets (see "No satellite timing offsets" above).
  - Checked with BigQuery dry runs on the staging sources. No copy was created and no pipeline ran.

---

## Port visits

### On the pipeline version production runs

- **Port visits on the pipe-anchorages version production runs** (`workflows/port_visits/ais.py`, `cross_version_ais.py`; 2026-11-19, following [PIPELINE-4699](https://globalfishingwatch.atlassian.net/browse/PIPELINE-4699)). The workflows drive one CLI, that of pipe-anchorages v5.0.1 (`port-state-transitions` / `port-visits`, exclusive `--end-date`, required labels), which production's `core.ais.v5` runs since 2026-11-04. The v4 CLI and `--pipeline-cli` are removed. DIT's step names (`thin_port_messages`, `port_visits`) and the inclusive `--start` / `--end` are unchanged.
  - **Default image.** `--image-tag` and `--worker-image` default to DIT's **temp-dataset overlay** of v5.0.1 ([`docker/overlays/pipe-anchorages-temp-dataset/`](../docker/overlays/pipe-anchorages-temp-dataset/README.md)). Upstream has no way to name Beam's BigQuery staging dataset, so production's own image cannot run as DIT's service account. The image must be rebuilt at every production version bump.
  - **No auto-build, and a guard.** A reviewed checkout (clean, merged into pipe-anchorages `origin/main`) runs the default image. For unreviewed code, a run that would execute a default image refuses to start, before any cloud call, and names the overlay build command. Pass an overlay image of the commit as both `--image-tag` and `--worker-image` (cross-version: `--binding-image-tag` and `--binding-worker-image`).

### Step selection

- **Port-visits step selection** (`workflows/port_visits/ais.py` and `cross_version_ais.py --steps`, 2026-10-22). Default runs both steps (`thin_port_messages,port_visits`) and compares visits on `visit_id`, unchanged. `--steps port_visits --thinned-message-table <FQN>` runs step 2 alone on a given input, e.g. production's `raw_port_events`. `--steps thin_port_messages` runs step 1 alone and compares the thinned tables on `(identifier, timestamp)`; that key was unique on the staging cohort but is not guaranteed. The cross-version wrapper pins only what the selected steps read. `--thinned-message-table` no longer implies skipping step 1 by itself.

### Production baseline

- **Production baseline for port visits** (`workflows/port_visits/cross_version_ais.py --baseline-production ais-v5-datalake --production-date D`, 2026-10-23, [PIPELINE-4703](https://globalfishingwatch.atlassian.net/browse/PIPELINE-4703)). Runs only the candidate binding and diffs it, on `visit_id`, against the table production wrote; production is not re-run. The candidate reads exactly what production read. Inputs are pinned as time-travel views as of production's Dataflow job start, and the output as of its end (`dit.production.find_dataflow_job`, `dit.bq.pin_view_into_experiment`). The run refuses if a pin would leave its source's time-travel window during the run, or if an input changed while production ran. See [`docs/production-baseline-2026-10.md`](production-baseline-2026-10.md).

First live run (from Roadmap 7c):

- **Verified live the same day** (Cloud Build `7781dd48`): PIPELINE-4690's PR (#176) against production's 2026-10-20 output, full history. Dataflow took 99 minutes at 100 × e2-standard-2.
- **Result:** DIFFERENT, consistent with #176.
  - production has 412,190,532 rows (2,801 with a duplicate `visit_id`); the PR has 412,181,996, all unique;
  - 9,789 rows exist only in production (6,849 of them zero-duration), 1,361 only in the PR;
  - 2,715 shared visits changed, 2,342 of them longer.
  - Breakdown: [`docs/production-baseline-2026-10.md`](production-baseline-2026-10.md).

### Parallel slices and orchestration declarations

- **Parallel slices, and per-step orchestration declarations** (2026-11-24). Some steps may load several date slices at once; others carry state from one run to the next and must run in order. DIT now declares which is which per step and tests it.
  - **Declarations:** `workflows/<pkg>/orchestration.py` for every onboarded pipeline (`dit.orchestration`): parallel slices allowed / forbidden / unknown, whether a run depends on the previous one, and the evidence (`verified` with a Cloud Build id, `claimed` with its source, `untested`). Seeded from the owner's statements, all `claimed` and none verified: `raw_port_events` (port visits step 1) and raw encounters allowed; the segmenter, incremental fishing events and (probably) gaps forbidden. How they map to Airflow (`depends_on_past`, `max_active_runs`): [`docs/plan.md`](plan.md) § "Orchestration capabilities".
  - **The test:** `workflows/port_visits/ais.py --steps thin_port_messages --modes 1_bf,6_parallel --slices month` submits the 12 months of 2020 as 12 concurrent Dataflow jobs into one table and compares it with one job over the year. The outcome is judged against the declaration: allowed and identical passes; a difference or a failed slice fails. A forbidden step would be run too, as a negative test (a difference is "as declared"; none is reported as "the declaration may be too strict"). `6_parallel` is refused for step 2, which rewrites its whole table on every run. `--max-concurrent-slices` caps the overlap (default: none). Slices in flight x `--max-num-workers` is logged and refused above 100 workers unless `--allow-many-workers` (world-fishing-827 shares its regional Dataflow quota with production; 2026-11-25). A failed run still writes its `dit_runs` row (status `failed`), so `make dit-cancel` drops its partly filled table. `--dry-run` prints every job and the worker bound, and creates nothing.
  - **Slices are an expression** (`dit.slices`): `--slices day|week|month|<N>d`, or a JSON list of `[first, last]` pairs, inclusive. The segmenter's `4_daily` and daily tails, and port visits' daily tails, use the same generators, with unchanged plans.
  - **Not run.** Nothing has been submitted; the proposed first run is in Roadmap 7f.

### Run records: PIPELINE-4690 (from Roadmap 7a)

First consumer: the PIPELINE-4690 A/B (pipe-anchorages PR #176, one record per vessel and instant). Dataflow runs, all from October 2026, when production still ran v4.6.4:
- **Staging smoke**, three modes, v5 CLI, candidate overlay `…-905dc761`: passed 2026-10-22, run `99cc678ee30c`, 10,291 visits per mode, 0 differing rows.
- **Staging cross-version**, b36aed6 vs 14b4be8 (Cloud Build `1f7447b4`): step 1 identical; visits differ in 2 of 10,292 rows, both explained by same-second ties, which #176 intends.
- **September 2026**, main b36aed6 vs PR 14b4be8 (Cloud Build `2a984881`): DIFFERENT, consistent with #176. Duplicate `visit_id`s are gone and burst splits are merged.
- **Q3 2026**, production v4.6.4 vs PR (Cloud Builds `0c6a46a6` and `77c967ef`): DIFFERENT, consistent with #176.
- **September 2026 three-way**, v4.6.4 on the same pins (Cloud Build `ec8b6dae`, 2026-10-23): this separates #176 from main's other changes.
  - Rows with a duplicate `visit_id`: 2,302 for v4.6.4, 2,658 for main, 0 for the PR.
  - v4.6.4 vs main changes about 1,100 visits (mostly zero-duration ones); v4.6.4 vs the PR about 4,000.
- **Determinism**, September (Cloud Build `8a1348d1`, `--no-cache`, 2026-10-23): the PR twice, plus the 2026-10-22 run. All three are IDENTICAL (3,869,516 rows).
- **Production baseline** (7c), PR vs production's own 2026-10-20 output: see 7c.

Open: TIC has no multiset comparison, so baseline duplicate `visit_id`s show up as duplicate-key warnings rather than counted diffs.

---

## Fishing events

Cross-version experiments and what the fishing wrapper pins:

- **Cross-version experiments** — pin source data, run a workflow at N pipeline-version bindings (git refs in the pipeline checkout), then diff corresponding output tables pairwise. Two consumers today: `workflows/port_visits/cross_version_ais.py` (port-visits, keyed on `visit_id`, Dataflow-shaped with `--binding-image-tag` and `--binding-worker-image` for per-binding code; a binding without them runs the default v5.0.1 overlay image) and `workflows/pipe_events/cross_version_fishing.py` (fishing events, keyed on `event_id` over both the `_fishing_events` and `_product_events_fishing` views, docker-native — per-binding container image comes free from the worktree's HEAD via `ensure_pipeline_image`, no `--binding-image` flag needed). Foundation for PR-validation comparisons (`pipe-anchorages@main` vs `@pr-NNN`; `pipe-events@main` vs `@refactor/*`). **Pinning:** both wrappers copy their sources at run start (`dit.bq.pin_source_into_experiment`; `cross_version_ais.py` since 2026-10-22, `cross_version_fishing.py` since 2026-11-19, [PIPELINE-4705](https://globalfishingwatch.atlassian.net/browse/PIPELINE-4705)). The copies keep the sources' partitioning and clustering, read across organisations, and are dry-run against a 1 TB cap per copy. `--reuse-pin <stamp>` reruns on them, and `--dry-run` reports each copy's estimated bytes without creating anything. Each wrapper owns the run's dates, so no binding runs outside its copies. What `cross_version_fishing.py` copies:
  - `research_messages` over `[--start - 1 day, --end)`, the dates pipe-events reads (one day before each slice, for the merge with the previous day's open events);
  - `segs_activity`, `segment_vessel`, `product_vessel_info_summary`, `identity_core`, `identity_authorization` and `event_regions` whole, since pipe-events reads them without a date filter;
  - not `spatial_measures_20201105`, whose dated name fixes its content.

  A source that is a view is copied too, as a plain table of the rows it returned (BigQuery refuses `LIKE` and snapshots on views). On the staging cohort that is `identity_core`, `identity_authorization` and `event_regions`. Not yet run live: the fishing wrapper's copies are checked with BigQuery dry runs only.

---

## pipe-gaps outage recovery

Its pinning note is shared with pipe-segment: see [Pinned copies](#pinned-copies-with-pipe-gaps-outage-recovery) above. Workflow-author rules: [`workflows/pipe_gaps/CLAUDE.md`](../workflows/pipe_gaps/CLAUDE.md).

---

## Drift check: state per workflow

How the check works: [`composer-drift.md`](composer-drift.md). The state below is the README's note of 2026-11-20.

- **Today** (2026-11-20, composer `origin/main` e4cb29361): port visits and both pipe-segment workflows are in sync.
  - **The check caught a change the same day.** The AIS v3 DAG moved its segment group from pipe-segment v5.0.3 to v5.1.0 and added `--incremental` to `segment` (composer-dags-production PR #3145, merged 17:26 UTC). Hours later the runner exited 1: both pipe-segment specs reported the version on every step and the flag on `segment`. DIT followed: the default image is the v5.1.0 overlay, and `ais.py` defaults to the incremental segmenter.
  - **What the specs accept now.** `pipe_segment/ais`: its default job passes `--incremental` as the DAG's task does, so that flag is compared; its two table flags are DIT's own; its full-history reference job leaves the flag out on purpose. `pipe_segment/identity_match_key`: `--incremental` is an accepted difference, because every binding runs full history. Each has its reason in the spec.
  - This is what the DAG is configured for. Whether a production run has executed with it was not checked.
- Encounters, pipe-events and pipe-gaps differ from production in version, command and flags. Those specs are the acceptance tests for porting the workflows. Details and how to add a spec: [`docs/composer-snapshots-2026-10.md`](composer-snapshots-2026-10.md) § 8.
- The port-visits spec also checks `ProductionTarget` field by field, and keeps its own command line (`workflows/port_visits/composer_drift.py`, with `--snapshot` for the backfill DAG). It accepts two production-only flags, `--no-rich-logging` and `--wait_for_job` (owner decision, 2026-11-19).
