# data_integration_tests

`dit` is a framework for integration tests of GFW data pipelines. It runs a real pipeline on real BigQuery data several times, in ways that should agree, and compares the output tables.

Five pipelines have workflows today: pipe-gaps, port visits (pipe-anchorages), fishing events (pipe-events), pipe-segment and encounters.

**Status: proof of concept.** There have been successful runs, some of which found real defects. DIT is not yet a routine gate on pull requests. Development is tracked in Jira epic [PIPELINE-4698](https://globalfishingwatch.atlassian.net/browse/PIPELINE-4698); new work items go under it.

- [DIT in one page](#dit-in-one-page)
- [Documentation](#documentation)
- [Quick start](#quick-start)
- [Features](#features)
- [Source data targets](#source-data-targets)
- [Roadmap](#roadmap)

## DIT in one page

### The idea

A unit test checks a function. DIT checks a claim about a whole pipeline:

- a backfill and a backfill followed by daily runs write the same table;
- a pull request changes these rows and no others;
- a candidate version reproduces what production wrote.

DIT is a small library (`src/dit/`) plus one or two workflow files per pipeline (`workflows/<pipeline>/`). A workflow is a plain Python file with a `main(argv) -> int`. `dit run <file>` calls it; so can a test.

### A run, step by step

1. Parse and check the arguments. Everything that can fail without a cloud call fails here.
2. Resolve the run context: which commit of the pipeline runs, whether it is reviewed, which image runs it.
3. Pin the sources, where the test needs fixed inputs.
4. Run the pipeline, once or many times, in a container or on Dataflow.
5. Compare the output tables with `table-check`.
6. State the verdict and return an exit code.

### What a workflow can ask

| Shape | Question | A difference means |
|---|---|---|
| Mode equivalence | Do different ways of loading one window (`1_bf` backfill, `2_bfd` backfill + daily, `3_bftruncate` backfill + truncate, daily) give the same table? | Failure: non-zero exit |
| Cross-version | What does this change do to the output? Two or more git refs ("bindings") on the same pinned input. | Information: the exit code is non-zero only if a binding failed |
| Production baseline | Does the candidate reproduce what production wrote? Production's inputs and output are read through time-travel views. | Information, as above |
| Parallel slices | May this step load several date slices at once? | Judged against the step's declaration |

### Where the edges are

| Rule | Meaning |
|---|---|
| Three repos, three owners | Pipeline repos own pipeline code. `composer-dags-production` owns what production runs; DIT reads it as data and never imports it. This repo owns the tests. |
| Library first | Everything is importable Python. The CLI is one consumer. |
| The comparison is not ours | `dit.compare` is a thin shim over [`table-check`](https://github.com/GlobalFishingWatch/table_identical_checks). Comparison features go there. |
| Writes stay in DIT's namespaces | Images under `gcr.io/world-fishing-827/dit/`. Tables, cache rows and pinned sources in `world-fishing-827.tech_great_expectations`. Reads are unrestricted. DIT never writes to production infrastructure. |
| A default run is cheap | Three source targets (AIS staging, VMS, AIS production). A run without flags uses a small cohort or a narrow window. |

### The building blocks

| Block | What it does | Read |
|---|---|---|
| Source pinning | Copies what a pipeline reads at run start, so all jobs of a run see the same data. Pin stamps, `--reuse-pin`, a size cap. | [`docs/framework.md`](docs/framework.md) § 5 |
| Run cache | One table, `dit_runs`: skips work already done, records what a run wrote, answers "which commit produced this table". `make dit-cancel` cleans a run up. | § 6 |
| Run context | Every run executes a committed ref. A dirty checkout is snapshotted to `refs/dit-snapshots/*`, scanned for secrets, and pushed. | § 7 |
| Images | Default images, images built automatically for unreviewed code, and overlay images that carry a test-only patch. | § 7 |
| Dataflow knobs | A worker cap with a global ceiling, sizing that mirrors production, private IPs, job names and labels. | § 8 |
| Cloud runtime | `make dit-cloud` runs a workflow in Cloud Build, in the `ditbox` image. | § 9 |
| Drift check | Compares what each workflow would run with what production's DAG runs: version, command, flags. | [`docs/composer-drift.md`](docs/composer-drift.md) |
| Per-side adjustments | Lets the bindings of a cross-version run differ in their command line or tables, by declared rules. | [`docs/framework.md`](docs/framework.md) § 10 |
| Orchestration declarations | Records per step whether date slices may load in parallel, and tests it. | § 11 |

## Documentation

Start with the first two.

| Doc | What it is |
|---|---|
| [`docs/framework.md`](docs/framework.md) | The framework guide: concepts, building blocks, the cloud runtime, how to add a pipeline. |
| [`docs/architecture.md`](docs/architecture.md) | Diagrams: repos, modules, run flows, Cloud Build, image namespace. |
| [`docs/usage.md`](docs/usage.md) | Install modes, running a workflow, scenarios, snapshot safety, cleanup. |
| [`docs/composer-drift.md`](docs/composer-drift.md) | The drift check against production's DAGs, in full. |
| [`docs/conventions.md`](docs/conventions.md) | Rules: the prod-infra boundary, image naming, Dataflow knobs, credentials in Cloud Build. |
| [`docs/pipeline-contract.md`](docs/pipeline-contract.md) | What a pipeline must expose to be testable, and where each pipeline stands. For pipeline maintainers. |
| [`workflows/README.md`](workflows/README.md) | The workflows, per pipeline. Per-pipeline docs sit next to the workflows (`workflows/<pkg>/`). |
| [`CHANGELOG.md`](CHANGELOG.md) | What changed for users. |
| [`CLAUDE.md`](CLAUDE.md) | Working agreements, and the Plan changelog: every design decision, newest first. |

Design records and dated analyses, for when you need the reasoning:

| Doc | Topic |
|---|---|
| [`docs/plan.md`](docs/plan.md) | The design record: decisions, API contracts, scope of each phase. Does not track status. |
| [`docs/framework-vision.md`](docs/framework-vision.md) | The long-term shape, and how today's code compares with it. |
| [`docs/context.md`](docs/context.md) | Historical: why the repo was started, and the two bugs the first test found. |
| [`docs/run-cache.md`](docs/run-cache.md), [`docs/run-cache-impl.md`](docs/run-cache-impl.md) | The run cache: design and milestones. |
| [`docs/no-dirty-tree-pivot.md`](docs/no-dirty-tree-pivot.md) | Why every run executes a committed ref. |
| [`docs/production-baseline-2026-10.md`](docs/production-baseline-2026-10.md) | Comparing a candidate with production's own output. |
| [`docs/composer-snapshots-2026-10.md`](docs/composer-snapshots-2026-10.md) | The composer spike and the drift check's result per workflow. |
| [`docs/pr-triggers-2026-06.md`](docs/pr-triggers-2026-06.md), [`docs/llm-pr-gating.md`](docs/llm-pr-gating.md) | Pull-request triggers: plan and pre-filter. |
| [`docs/encounters-onboarding-2026-07.md`](docs/encounters-onboarding-2026-07.md) | A worked onboarding audit. |
| `docs/*-2026-06.md` | Point-in-time reviews: workflow structure, snapshots, source mutation. |

## Quick start

```bash
python3 -m venv venv && source venv/bin/activate
make install-pipe-gaps            # or install-port-visits / install-pipe-events / install-pipe-segment

dit run workflows/<pipeline>/<workflow>.py --help
dit run workflows/port_visits/ais.py --runner dataflow --parallel

# The same in Cloud Build; returns a build id in seconds:
make dit-cloud PIPELINE=anchorages_pipeline WORKFLOW=workflows/port_visits/ais.py \
    ARGS="--runner dataflow --parallel"

# Does DIT still run what production runs?
dit run workflows/composer_drift.py
```

Install targets exist for four pipelines: `install-pipe-gaps`, `install-port-visits` (pipe-anchorages), `install-pipe-events` and `install-pipe-segment`, each with a `-ref` variant, plus `install-all`. Encounters has no install target; its workflows run the pipeline in a container ([`workflows/encounters/README.md`](workflows/encounters/README.md)). The Makefile expects the pipeline checkouts next to this repo; set `PROJECTS=/path` otherwise. Install modes, scenarios (uncommitted code, a named ref, two refs compared, cancelling a run) and the safety rules for snapshots are in [`docs/usage.md`](docs/usage.md).

## Features

What exists today. One line each; the linked doc has the detail. This section and the Roadmap are kept current with every change.

### Framework

| Feature | Where | Since | Doc |
|---|---|---|---|
| `dit run <workflow>`; workflows as importable Python | `dit.cli` | 2026-05 | [`framework.md`](docs/framework.md) § 3 |
| Two runners: a container (published image or built from source), and in-process Dataflow submission | `dit.runners.docker`, `dit.runners.dataflow` | 2026-05 | § 3 |
| Table comparison through `table-check`, for SCD-2 and rewritten tables | `dit.compare` | 2026-05 | § 3 |
| Mode selection (`--modes`), validated before any cloud call | `dit.workflow.parse_modes` | 2026-06-11 | § 4 |
| Cross-version runs: bindings, worktrees, pairwise diffs | the `cross_version_*` and `identity_match_key` workflows | 2026-05-15 | § 4 |
| Production baseline through time-travel views | `dit.production`, `dit.bq.pin_view_into_experiment` | 2026-10-23 | § 4 |
| Source pinning by copy, including view sources; `--reuse-pin`, a 1 TB cap per copy, dry runs | `dit.bq.pin_source_into_experiment` | 2026-10-22; views 2026-11-19 | § 5 |
| Whole-table pins of tables that require a partition filter | `dit.bq.all_rows_predicate` | 2026-11-19 | § 5 |
| Derived source views (synthetic outages) | `dit.bq.derived_source_into_experiment` | 2026-06 | § 5 |
| Run cache, `--no-cache`; wired into `pipe_gaps/mode_equivalence.py` (2026-05-22), `port_visits/ais.py` (2026-05-29) and `pipe_gaps/outage_recovery.py` (2026-06-03) | `dit.cache`, `dit.workflow.run_with_cache` | 2026-05-22 | § 6 |
| Run cleanup: `make dit-cancel RUN_ID=...`, and a SIGTERM trap in `dit run` | `dit.cache.cancel_run`, `dit.cli` | 2026-05-29; trap 2026-06-11 | § 6 |
| Every run executes a committed ref; automatic snapshots of dirty trees, with a secret scan | `dit.snapshot`, `scripts/snapshot.sh` | 2026-05 | § 7 |
| Images built automatically for unreviewed code | `dit.worker_image` | 2026-05; docker runner 2026-06-02 | § 7 |
| Overlay images, and the refusal of unreviewed code on an overlay default | `docker/overlays/`, `check_images` | 2026-07-30 (encounters, image built outside this repo); `docker/overlays/` 2026-10-22; overlay default and refusal 2026-11-19 | § 7 |
| Experiment ids, structured Dataflow job names, labels | `dit.workflow`, `dit.job_names` | 2026-05 | § 8 |
| Worker cap on every Dataflow workflow, with a global ceiling ([PIPELINE-4701](https://globalfishingwatch.atlassian.net/browse/PIPELINE-4701)) | `dit.workflow` | 2026-10-23 | § 8 |
| Private worker IPs; worker sizing that mirrors production; a cost hint on staging data | `dit.workflow` | 2026-10-23 | § 8 |
| Short jobs on DirectRunner (`--direct-runner-max-days`) | `dit.workflow.runner_for_days` | 2026-11-24 | § 8 |
| Dataflow temp and staging paths under `gs://<bucket>/dit/` | `dit.dataflow_paths` | 2026-10-22 | § 8 |
| Per-user settings through `DIT_*` environment variables | `dit.workflow`, `.envrc.example` | 2026-05-14 | § 8 |
| Cloud Build runtime: `make dit-cloud`, `ditbox`, `make publish-ditbox` | `cloudbuild-dit.yaml`, `docker/ditbox/` | 2026-05-15 | § 9 |
| Credentials in Cloud Build without a key file (`--network=cloudbuild`) | `dit.runners.docker` | 2026-06-03 | § 9 |
| Production DAGs read as data; `dit composer-dag <snapshot>` ([PIPELINE-4700](https://globalfishingwatch.atlassian.net/browse/PIPELINE-4700)) | `dit.composer` | 2026-10-23 | [`composer-drift.md`](docs/composer-drift.md) |
| Drift check for every workflow, with frozen specs for older versions | `dit.drift`, `workflows/composer_drift.py` | 2026-11-19; frozen 2026-11-24 | [`composer-drift.md`](docs/composer-drift.md) |
| Per-side adjustments in cross-version runs; preflight against the ref's source. Not yet used in a run. | `dit.adjust` | 2026-11-24 | [`framework.md`](docs/framework.md) § 10 |
| Orchestration declarations per step; date slices; a parallel-slices mode. Not yet run. | `dit.orchestration`, `dit.slices` | 2026-11-24 | § 11 |
| Pipeline integration contract and adoption matrix | [`docs/pipeline-contract.md`](docs/pipeline-contract.md) | | [`framework.md`](docs/framework.md) § 12 |

### Workflows

Ten workflows across five pipelines. What each one does, how to run it and what its results were: [`workflows/README.md`](workflows/README.md) and the page next to each workflow: [`pipe_gaps`](workflows/pipe_gaps/README.md), [`port_visits`](workflows/port_visits/README.md), [`pipe_events`](workflows/pipe_events/README.md), [`pipe_segment`](workflows/pipe_segment/README.md), [`encounters`](workflows/encounters/README.md).

| Pipeline | Workflow | Shape | Mirrors production today (drift check, 2026-11-20; pipe-gaps 2026-11-27) |
|---|---|---|---|
| pipe-gaps | `pipe_gaps/mode_equivalence.py` | Mode equivalence | Yes, at v0.17.1 (ported 2026-11-27; not yet run live) |
| pipe-gaps | `pipe_gaps/outage_recovery.py` | A staged outage and recovery against one full run | Runs the same job as `mode_equivalence.py`; no spec of its own |
| Port visits | `port_visits/ais.py` | Mode equivalence; parallel slices for step 1 | Yes |
| Port visits | `port_visits/cross_version_ais.py` | Cross-version; production baseline | Runs `ais.py` |
| Fishing events | `pipe_events/fishing.py` | Mode equivalence | No: older version and command line |
| Fishing events | `pipe_events/cross_version_fishing.py` | Cross-version | Runs `fishing.py` |
| pipe-segment | `pipe_segment/ais.py` | Mode equivalence of the `segment` step | Yes |
| pipe-segment | `pipe_segment/identity_match_key.py` | Cross-version over the six segment steps, with per-side adjustments | Yes |
| Encounters | `encounters/ais.py` | Mode equivalence, AIS staging | No: older version and command line |
| Encounters | `encounters/vms.py` | Mode equivalence, VMS | No: older version and command line |

Two things hold for every workflow and are easy to miss:

- **DIT's segmenter workflows run no satellite timing offsets** (owner decision, 2026-11-20). Their output is not comparable with production's historical output for the affected hours. Two DIT runs compare fairly. See "No satellite timing offsets in DIT" under Source data targets.
- **An IDENTICAL result is only as good as the data.** Check that the target could have shown a difference at all. See "Where target 1 runs out of road" under Source data targets.

### Not built, on purpose

- **No shared `Phase` / `Mode` / `Mutation` / `Oracle` types, and no shared mode loops.** The workflows' backfill and daily loops were compared side by side and are written out per workflow. Reasoning and the planned order of extraction: [`workflows/README.md`](workflows/README.md); the long-term picture: [`docs/framework-vision.md`](docs/framework-vision.md).
- **No support for date-sharded input tables.**
- **No pull-request triggers yet.** Runs are started by hand (Roadmap).

## Source data targets

*(Formerly "Staging data sources" — renamed once VMS and AIS prod became first-class targets alongside the AIS staging cohort.)*

dit runs against **three** source families. They differ by roughly four orders of magnitude in size, so picking one is mostly a coverage-vs-cost decision — not a "real vs. not-real" one. All figures below are `messages_positions` measured from `INFORMATION_SCHEMA.PARTITIONS`.

| Target | Where | Data span | Size | Use it for | Sane window |
|---|---|---|---|---|---|
| **1. AIS staging** | `world-fishing-827.pipe_ais_test_202408290000_{internal,published}` | **2020 only** | 4.1 GB / 15.7 M rows / **142 distinct ssvid** | The default for most workflows. Fast iteration, safe to hammer. | The whole year is affordable |
| **2. VMS** | `gfw-int-vms-v3.pipe_vms_v3_internal` + `global-fishing-watch.pipe_vms_v4_published` | **2012-01 onward** (earliest varies by country) | 502.7 GB / 1.13 B rows | Realistic, multi-year, many-vessel data at manageable cost. Most pipelines can run a **full backfill** here for a reasonable spend. | 7–30 days is almost always extremely cheap |
| **3. AIS prod** | `gfw-int-pipe-v3.pipe_ais_v3_internal` | **2012-01 onward** | **22.9 TB** (`messages_positions`) / 41.1 TB (`research_messages`) / 80.4 TB (`messages_segmented`) — 90.6 B rows | **Final integration tests on prod data only.** Not for iteration. | 7–30 days for recent years (~2022+); 3–6 months is viable for 2012–2020, which is far sparser |

**Two traps in target 3.** (a) Volume is wildly non-uniform across time — recent years dominate, which is why the sane window shrinks as you move forward. (b) `pipe_ais_v3_internal.messages_positions` retains only **~14 months** (partitions start `202507`); the **full history lives in `research_messages` and `messages_segmented`** (both 176 monthly partitions from `201201`). A workflow pointed at `messages_positions` for a 2015 window will run cleanly against zero rows — the same silent-null failure mode documented under "Cohort name vs. data window" below.

Targets 2 and 3 are **cross-organisation**: `gfw-int-vms-v3` and `gfw-int-pipe-v3` sit in org `115316357079`, while `world-fishing-827` is in org `433637338589`. Plain reads are fine; **`CREATE SNAPSHOT ... CLONE` is refused across orgs**. Pin these sources with copies instead (`dit.bq.pin_source_into_experiment`, used by `cross_version_ais.py`, `cross_version_fishing.py`, `outage_recovery.py` and `identity_match_key.py`), which only read them. The older `--snapshot-dest-project` workaround wrote the snapshot into the source's org, i.e. into production infrastructure; `outage_recovery.py` and `identity_match_key.py` dropped the flag on 2026-11-19. All three targets' datasets are US-located, so Beam EXPORT staging via `--bq-temp-dataset` is co-located by default.

**Production outputs as a baseline.** A production-baseline run (`cross_version_ais.py --baseline-production`) reads production's own inputs and output through time-travel views, which are readable only inside each source dataset's time-travel window:
- 48 h for `global-fishing-watch.pipe_ais_v3_published`;
- 168 h for `gfw-int-ais-datalake.port_visits_v1` and `gfw-int-ais-datalake.summarized_positions_v1`.

The 48 h window of `segment_info` makes such a run time-boxed: it must finish within about two days of production's job starting. Targets, tables and sizes: [`docs/production-baseline-2026-10.md`](docs/production-baseline-2026-10.md).

**Port-visits tables require a partition filter** since pipe-anchorages v5.0.1: production's output, and the output of any DIT run on that version or later. BigQuery rejects a query on them that has no filter on `end_timestamp`. DIT's pins and `table-check` handle this themselves. Your own SQL needs the filter; to read every row use ``WHERE (end_timestamp IS NULL OR end_timestamp IS NOT NULL)``, which also keeps rows with a NULL `end_timestamp`. See the same doc, "Tables that require a partition filter".

**Reference tables, whatever the target.** Some inputs are small, versioned, production reference tables that no cohort mirrors; workflows read them directly, and their defaults should track the version production reads:
- `workflows/port_visits/ais.py --named-anchorages` (step 1) defaults to `global-fishing-watch.anchorages.named_anchorages_v20260302`, the version production v5 reads (`dags/core/ais/v5.py`): 172,123 anchorages, US, unpartitioned. Until 2026-10-23 the default was `world-fishing-827.anchorages.named_anchorages_v20240117` (166,497 rows; same fields). Read access for `automated-testing@` to `global-fishing-watch.anchorages` is **unverified**; its access to `global-fishing-watch.pipe_ais_v3_published` is verified. If the first step-1 run under that identity (Cloud Build, or the Dataflow workers) fails reading anchorages, suspect an IAM grant before the workflow; meanwhile, pass the old table with `--named-anchorages`.
- `workflows/encounters/ais.py --spatial-measures-fqn` reads a static production reference table (see its help).

Keeping these defaults in sync is manual today; detecting such drift automatically is [PIPELINE-4700](https://globalfishingwatch.atlassian.net/browse/PIPELINE-4700).

**Port visits: v5 code on a v3-lineage cohort.** Since 2026-11-19 the port-visits workflows run pipe-anchorages v5.0.1 by default, but their default sources are still target 1, a 2020 snapshot of tables the v3 pipeline wrote. Production v5 reads v5 datalake tables for messages and bad segments (and still the v3 `segment_info`). The owner kept the cohort as the default for now: it serves mode equivalence, and a comparison against production pins production's own inputs (`--baseline-production`). The last run of this cohort on the v5 CLI was the 2026-10-22 smoke, at pipe-anchorages b36aed6; the SQL has not changed since, but v5.0.1 itself has not been run on it yet.

### Target 1 inventory — the AIS staging cohort

`pipe_ais_test_202408290000` is a synthetic AIS-staging dataset in `world-fishing-827` mirroring a representative slice of the prod pipeline. It is deliberately tiny: **142 distinct ssvid over 2020**, which is what makes it cheap — and also what limits it (see the sparsity note below).

| Table FQN | Consumed by | Shape |
|---|---|---|
| `pipe_ais_test_202408290000_internal.normalized_messages` | `workflows/pipe_segment/identity_match_key.py` and `workflows/pipe_segment/ais.py` (`--source-normalized-table`) | Date-partitioned (`timestamp`, by day), clustered on `ssvid`. The normalized AIS source — input to pipe-segment. Pinned by copy over the run's dates: 10.7 MB for one day, 3.78 GB for all of 2020 (dry runs, 2026-11-19). `ais.py` defaults to January 2020: 363 MB to pin (dry run, 2026-11-20), and each of its 31 days has 66,000 to 90,000 rows (partition metadata, same day), which the incremental segmenter needs: it fails on a day without messages. |
| `pipe_ais_test_202408290000_internal.messages_positions` | `workflows/pipe_gaps/mode_equivalence.py`, `workflows/pipe_gaps/outage_recovery.py` (`--source-messages`) | Output of pipe-segment, input to pipe-gaps. Partitioned by month on `timestamp`; requires a partition filter. `outage_recovery.py` pins it by copy: 4.41 GB for its default run, the whole table. |
| `pipe_ais_test_202408290000_published.segs_activity` | `workflows/pipe_gaps/mode_equivalence.py`, `workflows/pipe_gaps/outage_recovery.py` (`--source-segments`) | Segment activity stream, input to pipe-gaps. Unpartitioned, 4.1 MB. `outage_recovery.py` pins it whole. |
| `pipe_ais_test_202408290000_internal.messages_positions` / `pipe_ais_test_202408290000_published.segment_info` / `segs_activity` | `workflows/port_visits/ais.py` (`--source-dataset-stem`) | port-visits' three input tables. |
| `pipe_ais_test_202408290000_published.satellite_timing_offsets` | none | The satellite timing offsets of the cohort's period, computed from the full satellite feed when the cohort was built. No DIT workflow reads it, and none computes its own: see "No satellite timing offsets in DIT" below. |
| `pipe_ais_test_202408290000_internal.raw_port_events_YYYYMMDD` | none | Step-1 (`thin_port_messages`) output, **date-sharded (legacy)**: 365 shards, 2020-01-01..2020-12-30 (2020-12-31 missing), 332,515 rows, same schema as DIT's v5 step-1 output. Not usable as `--steps port_visits --thinned-message-table` input (one table expected; DIT does not support sharded inputs), so staging runs run step 1 themselves. |
| `pipe_ais_test_202408290000_internal` / `_published` (full datasets) | `workflows/pipe_events/fishing.py` | Fishing-events BQ-SQL inputs. |
| `pipe_ais_test_202408290000_internal.research_messages` | `workflows/pipe_events/fishing.py`, `cross_version_fishing.py` | 1.96 GB, 8.0 M rows, 2020 only. Partitioned by month on `timestamp`, clustered on `timestamp`, `ssvid`; **requires a partition filter**. The only fishing input read by date. |
| `pipe_ais_test_202408290000_internal.segment_vessel` / `_published.segs_activity` / `_published.product_vessel_info_summary` | same | Plain tables, 2 MB / 4 MB / 0.1 MB. Read whole. |
| `pipe_ais_test_202408290000_published.identity_core` / `identity_authorization` | same | **Views**, not tables: `SELECT * FROM global-fishing-watch.pipe_ais_identity_v4_published.<name>`, production's identity tables (29 MB / 6 MB per full read). They change when production does, which is why a cross-version run copies them. |
| `world-fishing-827.pipe_regions_layers.event_regions` | same | Outside the cohort, read by every fishing run. A **view** on `event_regions_v20250804` (3.96 GB per full read). The view can be repointed to a newer version, which is why a cross-version run copies it. |
| `world-fishing-827.pipe_static.spatial_measures_20201105` | same | Outside the cohort. 58 GB, static, version in the name. Never pinned. |

**Why target 1 is the default for most workflows.** It is small (~3.8 GB for `normalized_messages`), pinned to a known shape, safe to query freely, and avoids accidental cost when a smoke test goes wide. The cohort is regenerated upstream when prod's schema or data shape changes — workflows shouldn't pin to a specific snapshot of it.

**Where target 1 runs out of road.** 142 vessels over one year is thin, and some behaviour is simply unreachable there. Two measured cases: encounters need two vessels within 0.5 km for 120+ minutes, and the cohort yields only 2–3 co-located pairs per day; and gaps of *exactly* one hour — the boundary that `Resample`'s interpolation guard turns on — occur **once per 268,913 gaps (0.0004%)** in AIS staging versus **5.48% of all gaps** in VMS, where hourly reporting cadences make them *more common than gaps longer than an hour*. When a comparison comes back IDENTICAL, check that the target could have shown a difference at all before reading it as a pass. That is the reason `workflows/encounters/vms.py` exists.

**Cohort name vs. data window.** The cohort's dataset suffix is its **upstream snapshot date**, not the date of the data it contains. `pipe_ais_test_202408290000` was snapshotted on 2024-08-29, but the messages inside are **2020 AIS** (12 monthly partitions, `2020-01` through `2020-12` — confirmed by `SELECT MIN(timestamp), MAX(timestamp) FROM ...` and by the partition listing in `INFORMATION_SCHEMA.PARTITIONS`). New workflows must therefore default `--start` / `--end` to a window covering the cohort's 2020 AIS data — the canonical shape is `workflows/pipe_gaps/mode_equivalence.py`'s `DEFAULT_START = "2020-01-01"` / `DEFAULT_END = "2021-01-01"` (half-open, so 2020-01-01 inclusive through 2020-12-31 inclusive). Don't infer a date range from the cohort name; mirror an existing workflow's defaults, and verify against `INFORMATION_SCHEMA.PARTITIONS` if introducing the first workflow against a new staging dataset. A workflow that defaults to a date range *outside* the cohort's data will run cleanly against zero rows — silent, expensive, hard to spot.

**Adding a new table to the cohort.** If you generate a new staging table for a workflow under development, document it in this section in the same commit that lands the workflow. The README is the discoverability surface for "where do I point my workflow's source flag?" — if it's not here, the next person re-derives it from scratch (or worse, points at prod by default).

### Target 2 — VMS

`workflows/encounters/vms.py` is the first workflow to default to VMS. Sources mirror what prod encounters actually reads (`composer-dags-production` `dags/core/vms/config.py`): `gfw-int-vms-v3.pipe_vms_v3_internal.{messages_positions,segs_activity,spatial_measures_clustered_vms_v20251209}` plus `global-fishing-watch.pipe_vms_v4_published.segment_info`.

**VMS is cheap in practice, despite the 502 GB headline.** Measured on the query shape the pipeline actually issues, the workflow's default 7-day window scans **538 MB** — *less than half* the **1,265 MB** `encounters/ais.py` scans over its own year-wide default against the staging cohort. MONTH partition pruning on a big table beats a small unpruned one. Sizing a run by the table's total size is the wrong instinct here; size it by the window. Prefer `--ssvid-filter` (encounters accepts it on **both** steps) over widening dates.

**Unverified precondition.** `automated-testing@` (the Dataflow worker SA) has no *direct* IAM binding on `gfw-int-vms-v3` or `global-fishing-watch`. Access may come via a group binding — GFW cross-project access usually does — but that can't be confirmed from outside those projects. A first-run source-read failure here is an IAM grant, not a workflow bug. Laptop `--runner docker` uses the operator's own ADC and is unaffected.

**Prod-only inputs.** Tables not mirrored to the staging cohort should be opt-in via a workflow flag, default off, with the flag's help text noting that enabling it pins from prod instead. Keeps the default-staging contract clean while still allowing prod-only paths to be exercised when relevant. No workflow has such a flag today.

**No satellite timing offsets in DIT.** The satellite positions and the norad table that pipe-segment's offsets need are not such an opt-in input: DIT's segmenter workflows do not run satellite timing offsets at all (owner decision, 2026-11-20).
- The offsets must be computed from the full message feed, about 4.1 TB for 2020. The cohort's 142 vessels give a different, noisy result: for January 2020, 990 receiver-hours above the 30 s threshold against 183 in the cohort's offsets table, with 39 in common.
- pipe-segment cannot read precomputed offsets, so the cohort's own table (`pipe_ais_test_202408290000_published.satellite_timing_offsets`, computed from the full feed when the cohort was built) cannot be passed in.
- So DIT's `messages_segmented` on the cohort keeps the messages of the 1,361 receiver-hours that table lists for 2020 (27 receivers, 173 days). For those hours it is not comparable with production's historical output. Two DIT runs compare fairly.
- Details: [`workflows/pipe_segment/RUN_INSTRUCTIONS.md`](workflows/pipe_segment/RUN_INSTRUCTIONS.md) § "No satellite timing offsets in DIT".

## Roadmap

Each phase below is a short summary of what's planned and where we are. This section is the single status source; [`docs/plan.md`](docs/plan.md) holds the design (architecture, decisions, API contracts, phase scope) and does not track status.

| Phase / capability | Scope | Status (dated per row) |
|---|---|---|
| **1 — pipe-gaps port** | Stand up the repo. Lift the four-mode mode-equivalence test from `pipe-gaps/tests/integration/mode_equivalence.py` onto `dit.*` helpers. Drop `--runner=local`. Replace the source file with a shim. | **Code complete + verified on real BQ 2026-05-22** (caught a real `pipe-gaps:v0.9.6` non-determinism bug; custom worker image with fix collapsed all 3 pairwise diffs to zero). Pending only: Track 5 shim swap in the `pipe-gaps` repo. |
| **2 — port-visits** | Ship AIS-staging, VMS, and AIS-full workflows for `anchorages_pipeline`'s two-step port-visits (`thin_port_messages` → `port_visits`). First real test of the `dit.compare` abstraction on the truncate-shape (`view_suffix=""`, `keys=["visit_id"]`). | **AIS-staging verified 2026-05-15** (3/3 pairwise green). On the v5 CLI only, with a v5.0.1 overlay default image, since 2026-11-19; that and the PIPELINE-4690 runs: item 7a below. **VMS and AIS-full deferred 2026-10-23** (item 6). |
| **Cross-version experiments** | BQ snapshot helpers (`dit.bq.snapshot_table`/`snapshot_dataset`), experiment-id linkage (`--experiment-id`/`DIT_EXPERIMENT_ID`), structured Dataflow job names + dynamic labels, and an end-to-end orchestrator (`workflows/port_visits/cross_version_ais.py`) for diffing pipeline outputs across versions against pinned input. | **Landed 2026-05-15.** Validated end-to-end via the PIPELINE-1465 cross-version test + the pipe-gaps `--worker-image` flow on 2026-05-22. **Re-pinned 2026-10-22:** snapshot pinning had never worked after the June canonical-dataset migration (no `deleteSnapshot`) and cannot cross orgs; `cross_version_ais.py` now pins with copies (`dit.bq.pin_source_into_experiment`). **2026-11-19 ([PIPELINE-4705](https://globalfishingwatch.atlassian.net/browse/PIPELINE-4705)):** `cross_version_fishing.py` pins with copies too, and the helper copies view sources; checked with dry runs, not yet run live. `identity_match_key.py` and `outage_recovery.py` pin with copies as well, so no workflow pins with snapshots any more. |
| **Runtime & CI (Cloud Build)** | `ditbox` image + `cloudbuild-dit.yaml` + `make dit-cloud` / `make publish-ditbox` targets. Moves the orchestrator off the laptop; serves both `gcloud builds submit` ad-hoc and GitHub-webhook PR triggers. Tiered triggers (cheap AIS-staging on every PR, heavy AIS-full on label) come on top. | **Ad-hoc path battle-tested 2026-05-22** (kaniko cache, uv per-run installs, `_BEAM_VERSION` pin, `dit.job_names` centralisation, `dit.git_info` warn). Per-pipeline PR triggers pending. 30-concurrent-build slot cap is the next architectural ceiling — Cloud Run jobs flagged as the migration target if dit graduates to per-PR matrix testing. |
| **3 — pipe-events port** | Port the bash mode-equivalence test (`pipe-events/integration_tests/pipe3-bf_bfd_bftruncate.sh`, no comparisons) to `workflows/pipe_events/fishing.py`. Add the automated comparison it lacks. Then decide whether to extract phase helpers (three-consumer evidence). | **Code complete 2026-05-29 + verified live 2026-06-03** (build `e6f06a00`, ~47 min, all 12 nested-docker invocations clean against BQ; surfaced a real pipe-events SQL non-determinism finding — `ARRAY_AGG`-without-`ORDER BY` + float-aggregation drift in JSON-encoded STRING columns — recommended upstream fix is low-priority and out of dit's scope). `workflows/pipe_events/fishing.py` lands the 4-step BQ-SQL chain + truncate-shape comparison on `event_id` (NOT SCD-2 — plan corrected). Docker runner gained `volumes`/`service`; `add_infra_args` split. Phase-helper extraction **deliberately deferred** (three-consumer review: shapes too divergent — [`workflows/README.md`](workflows/README.md)). Pending: the pipe-events-repo bash shim (a separate processing-repo change). |
| **4 — composer-dags param sync** | `dit sync-params --from <composer-dags-checkout>` reads production DAGs and regenerates `params.yaml`. Triggered when a real prod-vs-test param drift bug shows up. | Spike 2026-10-23 ([PIPELINE-4700](https://globalfishingwatch.atlassian.net/browse/PIPELINE-4700)): `dit.composer` reads composer's render snapshots as data; no params file yet. See [`docs/composer-snapshots-2026-10.md`](docs/composer-snapshots-2026-10.md). 2026-11-19: a drift check for every workflow detects differences but syncs nothing ([`docs/composer-drift.md`](docs/composer-drift.md); item 7b below). |
| **5 — Mutation library** | Promote pipe-gaps' `compute_restricted_ssvids` into `dit.mutations` along with `drop_messages`, `shift_timestamps`, `set_segment_flag`. Cap at ~5 mutations. | Not started; waits for a second consumer. |
| **6 — Phase sharing** | Hash `(image-tag, phase-config, mutation-set)`; second invocation of an identical phase becomes a `BQ COPY` instead of a re-run. Cuts wall-clock for CI. | Not started; build only when duplicate-run cost matters operationally. |
| **7 — Golden-table mode** | Per-workflow reference `_1_bf` table keyed by `(image-tag, params-hash, date-range)`; future runs assert byte-equivalence vs. the golden table. Cheap PR-validation regression check. Implementable on top of the cross-version snapshot machinery. | Not started. |

**Operational next steps** (rolling, in priority order):

0. **No-dirty-tree pivot — landed** ([`docs/no-dirty-tree-pivot.md`](docs/no-dirty-tree-pivot.md)). M-pivot-1..5 shipped (#22–#25 + docs): deterministic orphan snapshots + gitleaks; auto-snapshot in `make dit-cloud` (`--async`) / `dit run`; `pipeline_dirty` → `unreviewed_code` + `pipeline_commit_parent` (snapshots cacheable); auto-built worker images closing the submitter-vs-worker gap; `--allow-dirty-tree` / the warn-helper / the `_dirty` suffix removed. **Remaining follow-ups** (tracked in the pivot doc § Loose ends): pipe-gaps Dockerfile reorder PR ([GlobalFishingWatch/pipe-gaps#128](https://github.com/GlobalFishingWatch/pipe-gaps/pull/128)); drop the dual-written `pipeline_dirty` column after a release. The first real end-to-end `make dit-cloud` auto-build run is **done (2026-05-29)** — it also surfaced + fixed two `REF=` cloud-path bugs (#27) and reconfirmed the `v0.9.6` non-determinism (main 350/317/347 vs fix 0/0/0).
1. **Run cache M5 — landed 2026-05-29** (see [`docs/run-cache-impl.md`](docs/run-cache-impl.md) § Milestone 5). **M5a**, done: `cancel_run` + `dit cache-cancel` + `make dit-cancel` — discovers Dataflow jobs by `dit_run_id` label (stored `dataflow_job_ids` are empty), deletes the output tables (table-level only), UPDATEs status; pipe-gaps now stamps the `dit_run_id` label too. **M5b**, done: port-visits caching wired (its own `canonical_params_dict` + `CacheKey` over the shared `dit.workflow.run_with_cache`; `ais.py` flipped to `resolve_digest=True`). **User-gated follow-up**: verify `dataflow.jobs.cancel` + BQ table-delete for the laptop user (`make dit-cancel`) and `automated-testing@` (the M6 path) against live infra — the M5 tests mock gcloud/BQ.
2. **Run cache M6 — SIGTERM trap inside `dit run` — landed 2026-06-11.** Cloud Build cancellations (and, once PR triggers land, force-pushes to triggered builds) now tear down their own Dataflow jobs via `cancel_run` instead of orphaning them. New `dit.runstate` carries the `run_id` from `resolve_run_context` to the CLI handler; exits `143`; escape hatch `DIT_NO_CANCEL_ON_SIGTERM=1`. **This completes the run-cache arc (M1–M6).** Known limitation: non-default `--dataflow-region` runs need `DIT_DATAFLOW_REGION` exported too for the trap to find their jobs (the message names the manual fallback). See [`docs/run-cache-impl.md`](docs/run-cache-impl.md) § Milestone 6.
3. **Per-pipeline PR triggers.** Wire `pipe-gaps` (most-verified) → `anchorages_pipeline` → `pipe-events` to fire dit on PRs. Trigger config lives in each pipeline repo; references `cloudbuild-dit.yaml` from the dit checkout. Path-filter on pipeline-relevant files; `dit:run` label as the escape-hatch override.
4. **PR comment integration via Check Runs.** `dit.report` module + GitHub Check Run API call from inside `dit run`. Typed contract end-to-end (no log parsing). Design doc TBD; separately, [`docs/llm-pr-gating.md`](docs/llm-pr-gating.md) covers the negative-signal-only LLM pre-filter that gates *whether* a PR triggers dit (orthogonal concern from how the verdict is posted).
5. **Track 5** — pipe-gaps repo shim, opportunistically (replace `pipe-gaps/tests/integration/mode_equivalence.py` with a thin re-export from `dit/workflows/pipe_gaps/...`).
6. **Port-visits follow-ups: deferred** (owner decision 2026-10-23, "wrapping up port visits on DIT for now"):
   - the VMS port-visits workflow, and AIS-full (the latter is what motivates Cloud Run jobs hardest once concurrent runs scale up);
   - comparing step 1 and step 2 outputs in one run (today a run compares only its last selected step's output);
   - the core AIS v3 target for the production baseline (a 7c follow-up).
7. PIPELINE-1465 cross-version full validation against newly-built pipe-anchorages images (parallel to the pipe-gaps validation just completed).

7a. **pipe-anchorages on its current CLI ([PIPELINE-4699](https://globalfishingwatch.atlassian.net/browse/PIPELINE-4699)).** `--pipeline-cli v5` + the temp-dataset overlay landed 2026-10-22, as an opt-in next to the v4 default.
   - **v5 only since 2026-11-19.** Production's `core.ais.v5` moved to pipe-anchorages v5.0.0 on 2026-10-26 and v5.0.1 on 2026-11-04. The owner then dropped the v4 path: the workflows drive the v5 CLI only, the default image is DIT's overlay of v5.0.1, and `--pipeline-cli` is gone. PR #176 is part of v5.0.0, so the first consumer's runs are history.
   - **Open after that change:** the staging smoke on the v5.0.1 overlay image has run (2026-11-19, Cloud Build `5f87695c`, three modes IDENTICAL; [`workflows/port_visits/README.md`](workflows/port_visits/README.md) § "Run history"); no production-baseline run since production moved to v5.0.1 (its output pin is fixed for the table's new partition-filter requirement in #89); VMS port visits, voyages and `anchorages-visited-info` are not covered.

   The first consumer's run records (the PIPELINE-4690 A/B, pipe-anchorages PR #176: staging smoke, staging cross-version, September and Q3 2026, the three-way with v4.6.4, determinism) are in [`workflows/port_visits/README.md`](workflows/port_visits/README.md) § "Run history". All are from October 2026, when production still ran v4.6.4.

7b. **Keeping dit in sync with pipeline interface changes ([PIPELINE-4700](https://globalfishingwatch.atlassian.net/browse/PIPELINE-4700)).** The v5 CLI drift went unnoticed for months because production then still pinned v4.6.4. Then the opposite happened: production moved to v5.0.1 on 2026-11-04, DIT stayed on v4.6.4, and only a manual audit on 2026-11-19 found it. Define how drift is detected, who updates dit, and how cross-version runs handle bindings on different interfaces.
   - Spike landed 2026-10-23: `dit.composer` reads composer's render snapshots as data, and `workflows/port_visits/composer_drift.py` checks the port-visits target and argv against them (owner decisions applied 2026-10-23).
   - 2026-11-19: the check is ported to the v5 CLI and now also compares the pipe-anchorages version in DIT's default image with production's image tag.
   - 2026-11-19: the check is general. `dit.drift` plus one spec per workflow, run by `workflows/composer_drift.py`, compare version, command and flags per step (how it works: [`docs/composer-drift.md`](docs/composer-drift.md); result per workflow: [`docs/composer-snapshots-2026-10.md`](docs/composer-snapshots-2026-10.md) § 8). Port visits is in sync. The specs of encounters (AIS and VMS), pipe-events and pipe-gaps reported drift; each is the acceptance test for porting that workflow (pipe-gaps ported 2026-11-27).
   - 2026-11-19: pipe-segment is in sync. Its version and commands already matched and its flags did not; the flags were fixed, the three missing steps of production's segment group were added, and the spec covers all six tasks. Read from code, not confirmed by a run: a one-day DirectRunner smoke is proposed and waits for the owner's go-ahead. Dataflow runs as the test service account still need a temp-dataset overlay image.
   - 2026-11-20: the check caught its first live change. The AIS v3 DAG moved its segment group to pipe-segment v5.1.0 with `--incremental` (composer-dags-production PR #3145, merged 17:26 UTC); a run of the check that evening exited 1 and named the version on six steps and the flag on one. DIT's pipe-segment defaults followed the same day (7e), and both specs are in sync again at composer e4cb29361. The check was run by hand; nothing would have run it otherwise.
   - 2026-11-24: bindings on different interfaces. `dit.adjust` gives each binding of a cross-version run the base interface (mirroring production) plus declared and ad hoc adjustments, checked against the ref's CLI source before pinning; first consumer `pipe_segment/identity_match_key.py`. Older layers get frozen drift specs at the composer commit where production last ran them (pipe-segment v5.0.3: in sync). Not run; the table edits have no real case yet ([`docs/framework.md`](docs/framework.md) § 10; [`workflows/pipe_segment/RUN_INSTRUCTIONS.md`](workflows/pipe_segment/RUN_INSTRUCTIONS.md) § B4 and B5).
   - 2026-11-27: pipe-gaps is in sync. Both workflows run pipe-gaps v0.17.1 (`raw-gaps`), the version the AIS v5 DAG is configured for (composer); v0.10.0 is dropped. The accepted differences are named in the spec (staging inputs, `--min-gap-length` 1 and stabilization off by owner decision, the daily job's start date, `--window-period-d`, and three operator flags). Read and built offline, not run on Dataflow ([`workflows/pipe_gaps/README.md`](workflows/pipe_gaps/README.md)).
   - Next: port the drifted workflows (encounters, pipe-events), each against its spec, and set `in_sync=True` as each one passes. Open: nothing runs the check on a schedule yet (owner, 2026-11-19: on demand is enough for now).

7c. **Candidate against production's own output ([PIPELINE-4703](https://globalfishingwatch.atlassian.net/browse/PIPELINE-4703)).** `--baseline-production ais-v5-datalake` landed 2026-10-23 (#83).
   - **Verified live the same day** (Cloud Build `7781dd48`): the PIPELINE-4690 PR against production's 2026-10-20 output, full history. Result: DIFFERENT, consistent with the PR. Figures: [`workflows/port_visits/README.md`](workflows/port_visits/README.md) § "Run history" and [`docs/production-baseline-2026-10.md`](docs/production-baseline-2026-10.md).
   - **Follow-ups:**
     - the core AIS v3 target;
     - a trigger after the production DAG;
     - a copy fallback when the time-travel window is too short;
     - deriving inputs from what production actually read (job metadata; PIPELINE-4700 comment of 2026-10-23).

7d. **Source pinning by copy on every workflow that pins ([PIPELINE-4705](https://globalfishingwatch.atlassian.net/browse/PIPELINE-4705)).** Snapshot pinning fails for every DIT identity.
   - 2026-10-22: `port_visits/cross_version_ais.py` moved to copies.
   - 2026-11-19: `pipe_gaps/outage_recovery.py`, `pipe_segment/identity_match_key.py` and `pipe_events/cross_version_fishing.py` moved, and `dit.bq` pins view sources with a plain copy. Checked with BigQuery dry runs, plus a live copy of the two small identity views. So no workflow pins with snapshots any more.
   - **Open:**
     - retiring `snapshot_into_experiment`, `snapshot_table` and `snapshot_dataset`, now that no workflow uses them;
     - a first live run of each moved workflow. Smallest ones: a one-week `1_bf` run of the fishing wrapper, a one-day DirectRunner smoke for pipe-segment, and a short-window `--synthetic-outage` run in Cloud Build for outage recovery. All wait for the owner's go-ahead.
   - 2026-11-20, owner decision: DIT runs no satellite timing offsets. pipe-segment's `--include-satellite-offsets` and the code behind it are removed (Source data targets, "No satellite timing offsets in DIT"). The path back is an option in pipe-segment to read an offsets table instead of computing one; not planned work.

7e. **Mode equivalence for the segmenter.** `workflows/pipe_segment/ais.py` and the pipe-segment temp-dataset overlay landed 2026-11-20 ([`workflows/pipe_segment/README.md`](workflows/pipe_segment/README.md)). First use: pipe-segment PR #227 (`--incremental`, released as v5.1.0), whose claim is that incremental day-by-day runs equal full history.
   - **The AIS DAG adopted the flag the same day** (composer-dags-production PR #3145). The defaults follow it: the incremental segmenter, on the overlay of v5.1.0, for a run that compares three incremental shapes with full history in one job (ten jobs). `identity_match_key.py` defaults to the same image, hands it the temp dataset, and since 2026-11-24 passes `--incremental` to every binding whose pipe-segment declares it ([`docs/framework.md`](docs/framework.md) § 10).
   - **Run (2026-11-24/25), all measured in Cloud Build:** the default run on January 2020 (10 jobs, 9 comparisons), the PR #227 test with the v5.0.3 base (45 jobs, 15 comparisons), short jobs on DirectRunner, and ten days of production input (v5.0.3 against v5.1.0) are all IDENTICAL. Build ids and the one table-check pairing bug found on the way: [`workflows/pipe_segment/README.md`](workflows/pipe_segment/README.md) § "Run history".
   - **Open:** `identity_match_key.py` has not run on the overlay images (it takes one per binding with `--binding-image` since 2026-11-24, and refuses an unreviewed binding on the default image since 2026-11-25); the mode-equivalence workflow has no run cache and its output tables have no expiration; a cross-version wrapper for the segmenter (base against head, per mode) does not exist; the VMS segment stage (v5.0.3, full history) has no workflow of its own.

7f. **Which steps may load slices in parallel.** Declarations (`workflows/<pkg>/orchestration.py`), `dit.slices` and port visits' `6_parallel` landed 2026-11-24 ([`docs/framework.md`](docs/framework.md) § 11). What the mode does, what it refuses and how its result is judged: [`workflows/port_visits/README.md`](workflows/port_visits/README.md) § "`6_parallel`". Every declaration is a claim; none is verified.
   - **Next, waiting for the owner's go-ahead** (each needs a clean pipe-anchorages checkout of a commit merged into `origin/main`, so the default v5.0.1 overlay image runs):
     1. January 2020 in calendar weeks: `--steps thin_port_messages --modes 1_bf,6_parallel --start 2020-01-01 --end 2020-01-31 --slices week`. 6 Dataflow jobs (1 + 5 concurrent), all five slices in one monthly partition, so concurrent deletes and appends on one partition and the concurrent create-table hook are exercised;
     2. the 12 months of 2020: `--slices month` on the default window. 13 jobs (1 + 12 concurrent).
   - **Then:** record the result in `port_visits/orchestration.py` by hand (status `verified`, the Cloud Build id), and give the next workflow the mode (candidates: encounters' raw step; the forbidden steps as negative tests).

8. **Docs reorganisation.** This README has outgrown its shape — it now carries source-target inventories, per-workflow interpretation notes, and operational findings that belong in `docs/` or alongside the workflow they describe (`workflows/<pipeline>/README.md`, as `workflows/encounters/README.md` now does). Near-term: move content out and leave the README as an index. Longer-term this needs a **concept for storing dit outputs and analyses in a structured way** — run results, comparison summaries and their interpretation currently have no home at all, which is why they keep landing in prose. Recorded 2026-07-31.
   - **2026-11-25, first step:** the top of this README now explains the framework in a page. New: [`docs/framework.md`](docs/framework.md), [`docs/composer-drift.md`](docs/composer-drift.md) and [`docs/usage.md`](docs/usage.md) (the install modes and scenarios that were here). The long per-workflow feature notes and the run records of items 7a, 7c and 7e moved to the per-pipeline pages next to the workflows (`workflows/<pkg>/README.md`, indexed by [`workflows/README.md`](workflows/README.md)); facts that belong to no pipeline, such as which workflows `make dit-cancel` covers, went to [`docs/framework.md`](docs/framework.md). This spans PRs #102 to #105.
   - **Open:** moving the Source data targets inventory out; the concept for storing run results.
9. The first dit release tag (`v0.1.0`) once per-pipeline PR triggers + caching land — that's the natural "framework is usable by outsiders" milestone.

This list is the canonical status and priority list. `docs/plan.md` § Next steps keeps only the design intent of the items it numbers (it stopped tracking status on 2026-10-23), and the PR-trigger arc is sequenced in [`docs/pr-triggers-2026-06.md`](docs/pr-triggers-2026-06.md). That plan proposes tagging `v0.1.0` *before* the PR triggers, the opposite of item 9 above; the order is an open owner decision.
