# `workflows/port_visits/`: port visits (pipe-anchorages)

DIT's tests of the port-visits pipeline in `anchorages_pipeline` (pipe-anchorages). This page covers what is specific to port visits. The framework concepts it relies on (source pinning, run cache, drift check, parallel slices, orchestration declarations, the production-baseline mechanics) are explained in the top-level [`README.md`](../../README.md) and [`docs/architecture.md`](../../docs/architecture.md).

## The pipeline

Port visits has two steps. DIT drives both.

| DIT step name | pipe-anchorages command | reads | writes | how it writes |
|---|---|---|---|---|
| `thin_port_messages` | `port-state-transitions` | messages over `[start, end)`, named anchorages | thinned port messages (production's `raw_port_events`) | deletes its own date range, then appends |
| `port_visits` | `port-visits` | thinned messages from the start date on, `segment_info`, `segs_activity` (bad segments) | port visits | replaces the whole table (`WRITE_TRUNCATE`) |

DIT kept its step names from the old v4 CLI. They appear in `--steps`, Dataflow job names and the `dit_step` label. The commands DIT runs are the current ones.

Port visits is the generation half of the port-visit events. The product step (`product_events_port_visit`) belongs to pipe-events and is out of scope.

## Versions

| | version | CLI | source |
|---|---|---|---|
| Production, AIS v5 DAG (`core.ais.v5`) | pipe-anchorages **v5.0.1** since 2026-11-04 (v5.0.0 from 2026-10-26) | `port-state-transitions` / `port-visits` | drift check at composer `origin/main` 85700ebc3, 2026-11-25 |
| Production, VMS v5 DAGs | v5.0.1 | same | [`docs/composer-snapshots-2026-10.md`](../../docs/composer-snapshots-2026-10.md) § 6, read 2026-11-19 |
| Production, AIS v3 and VMS v3 DAGs | v4.6.4 | old v4 CLI (`thin_port_messages` / `port_visits`) | same, read 2026-11-19 |
| **DIT default** | **v5.0.1**, as DIT's temp-dataset overlay image | v5 CLI only | `ais.DEFAULT_PIPELINE_VERSION`, `ais.DEFAULT_OVERLAY_IMAGE` |

DIT dropped the v4 CLI and the `--pipeline-cli` switch on 2026-11-19. Passing `--pipeline-cli` now fails with the reason.

The drift check (`composer_drift.py`, run by `workflows/composer_drift.py`) reports port visits **in sync** with the AIS v5 DAG: version, commands and flags of both steps, and all twelve fields of the production-baseline target. Three differences are accepted with reasons in the spec: step 1's messages table (DIT reads its staging cohort by design), `--no-rich-logging` and `--wait_for_job`.

### The default image

pipe-anchorages has no option to name Beam's BigQuery staging dataset, and DIT's service account may not create one. So the default `--image-tag` (submitter) and `--worker-image` (Dataflow workers) are DIT's overlay of v5.0.1: [`docker/overlays/pipe-anchorages-temp-dataset/`](../../docker/overlays/pipe-anchorages-temp-dataset/README.md). The workflow hands the dataset to the image through the `DIT_BQ_TEMP_DATASET` container variable, from `--bq-temp-dataset`.

What follows:

- A clean checkout of a commit merged into pipe-anchorages `origin/main` runs the v5.0.1 image, whatever that commit is.
- Unreviewed code (a dirty tree, a DIT snapshot, or a commit not merged into `origin/main`) on a default image is refused before any cloud call. Build an overlay image of the commit with `build.sh` and pass it as both `--image-tag` and `--worker-image`.
- DIT never auto-builds an image for port visits: a plain build has no overlay.
- `--build-from-source` runs the working tree through `docker compose`, without the overlay. Fine with `--runner docker`; on Dataflow Beam would try to create its own staging dataset, and the workflow logs a warning.

## Workflows

| file | what it tests | use it when |
|---|---|---|
| `ais.py` | mode equivalence: the same code run several ways must give the same output | checking re-run and incremental behaviour, or a step's parallel loading |
| `cross_version_ais.py` | two or more pipeline versions on the same pinned inputs | checking what a code change does to the output |
| `cross_version_ais.py --baseline-production` | one candidate against the table production wrote | checking a candidate against what production published |
| `composer_drift.py` | DIT's argv and production target against production's rendered DAG | after production changes its DAG or bumps pipe-anchorages |
| `orchestration.py` | per step: may slices load in parallel, and the evidence | read by `ais.py`'s `6_parallel` verdict; edited by hand |

All of them are run with `dit run <file> [args]`. `ais.py` reads the pipeline commit from the current directory, so run it from an `anchorages_pipeline` checkout. `cross_version_ais.py` takes `--pipeline-dir` instead.

## `ais.py`: mode equivalence

### Modes

| mode | jobs | what it tests |
|---|---|---|
| `1_bf` | one job per step over `[start, end]` | the reference |
| `2_bfd` | one job over `[start, end - tail_days]`, then one per tail day | steady-state daily processing |
| `3_bftruncate` | one job over `[start, end]`, then the same daily tail again | that re-running processed days replaces them cleanly |
| `6_parallel` | the window cut into slices (`--slices`, default `month`), all submitted at once into one table | that step 1 may load slices in parallel |

The default is the first three, compared pairwise. `6_parallel` is opt-in and compared with `1_bf` only. There is no mode 4 or 5 here; those numbers belong to other workflows.

In every slice, step 1 runs over the slice and step 2 recomputes everything from `--start` to the slice's end, as production does.

### Steps and what is compared

`--steps` selects the steps, in pipeline order. A run compares the output of its **last** selected step.

| `--steps` | compared table | key |
|---|---|---|
| `thin_port_messages,port_visits` (default) | `port_visits_<suffix>_<mode>` | `visit_id` |
| `thin_port_messages` | `port_events_<suffix>_<mode>` | `(identifier, timestamp)` |
| `port_visits` (needs `--thinned-message-table`) | `port_visits_<suffix>_<mode>` | `visit_id` |

Which table catches what:

- **Step 1's thinned table** is built slice by slice (delete the slice's range, append). It is the table where an incrementality bug shows directly.
- **Step 2's visits table** is rebuilt from `--start` on every call. It shows non-determinism directly, and a step-1 difference only through its effect on visits.

A default run compares visits only. To compare step 1's output directly, run `--steps thin_port_messages`. Comparing both steps' outputs in one run is not built (README Roadmap item 6).

`(identifier, timestamp)` is not guaranteed unique: a segment could hold two messages in one second. It was unique in every mode of the 2026-10-22 staging smoke (333,367 rows each).

Output tables go to `--dest-dataset` (default `tech_great_expectations`, env `DIT_DEST_DATASET`).

### `6_parallel`

Each slice is its own Dataflow job with its own job name and `dit_slice_start` / `dit_slice_end` labels. All are submitted before any is awaited. A failed slice does not stop the others.

The outcome is judged against `thin_port_messages`' declaration in `orchestration.py`, which today says parallel loading is **allowed**, as claimed by the owner on 2026-11-24 (status `claimed`, not `verified`). So:

- slices succeed and the table equals `1_bf`'s: pass (exit 0);
- the tables differ, or a slice failed: fail.

Refused before any cloud call:

- step 2 selected (`port_visits` replaces its whole table, so slices would overwrite each other);
- no `1_bf` in `--modes`;
- slices that do not cover the window exactly once, or only one slice;
- `--slices` or `--max-concurrent-slices` without the mode;
- slices in flight times `--max-num-workers` above 100, unless `--allow-many-workers` (`world-fishing-827` shares its regional Dataflow quota with production).

The mode never reads the run cache. Its `dit_runs` row is still written, with `status='failed'` when a slice failed, so `make dit-cancel RUN_ID=<id>` can drop a partly filled table.

Slices that share a month partition run their deletes and inserts on it at the same time. BigQuery may fail some of them when its DML queue fills; that would show as a failed slice.

`cross_version_ais.py` refuses `6_parallel`.

### Sources and window

| input | default | flag |
|---|---|---|
| messages | AIS staging cohort `pipe_ais_test_202408290000_internal.messages_positions` | `--source-messages-fqn` |
| `segment_info` | cohort `_published.segment_info` | `--source-segment-info-fqn` |
| `segs_activity` | cohort `_published.segs_activity` | `--source-segs-activity-fqn` |
| named anchorages | the production reference table production v5 reads (`named_anchorages_v20260302`) | `--named-anchorages` |
| thinned messages (step 2 alone) | none | `--thinned-message-table` |

`--source-dataset-stem` sets all three cohort tables at once; each `--source-*-fqn` overrides one.

The default window is `2020-01-01` to `2020-12-31`. `--start` and `--end` are **inclusive**; the workflow passes pipe-anchorages `end + 1 day`, since its `--end-date` is exclusive. The cohort holds 142 vessels of 2020 data; its name carries the snapshot date, not the data date. Other targets: README § "Source data targets".

### Workers

- `--max-num-workers` defaults to **4**, sized for the staging cohort. Production runs step 2 with 100 (250 in the v5 backfill). Use e.g. 16 for one month of production data.
- Machine type and disk mirror production on both steps: e2-highmem-4, 50 GB. A Dataflow run on the staging cohort logs a hint to pass a smaller `--worker-machine-type`.
- Private worker IPs always.

### Run cache

Every mode except `6_parallel` goes through the run cache. The key holds the pipeline commit, the worker-image digest, the submitter-image digest, this file's sha1 and the mode's output-affecting parameters: dates, the resolved source tables of the selected steps, named anchorages, `tail_days` for the daily modes, the steps when not both. `--no-cache` recomputes (for a determinism rerun) and still records the run.

### Commands

```bash
DIT=<path to this repository>
cd "$PROJECTS/anchorages_pipeline"   # clean, merged into origin/main: runs the v5.0.1 image

# See the plan; creates nothing, reads no commit
dit run "$DIT/workflows/port_visits/ais.py" --dry-run

# Cheapest real run: one mode, one month, no comparison
dit run "$DIT/workflows/port_visits/ais.py" --modes 1_bf --start 2020-01-01 --end 2020-01-31

# Mode equivalence, three modes in parallel, staging year
dit run "$DIT/workflows/port_visits/ais.py" --parallel

# Step 1 alone: incrementality, compared on (identifier, timestamp)
dit run "$DIT/workflows/port_visits/ais.py" --steps thin_port_messages --parallel

# Step 1 loaded in 12 monthly slices at once, against one job
dit run "$DIT/workflows/port_visits/ais.py" --steps thin_port_messages --modes 1_bf,6_parallel

# Unmerged code: build its overlay image first
"$DIT/docker/overlays/pipe-anchorages-temp-dataset/build.sh" "$PROJECTS/anchorages_pipeline" <commit> <label> --push
dit run "$DIT/workflows/port_visits/ais.py" --image-tag <image> --worker-image <image>

# Local check of the working tree, no Dataflow
dit run "$DIT/workflows/port_visits/ais.py" --runner docker --build-from-source --modes 1_bf --start 2020-01-01 --end 2020-01-07
```

In Cloud Build: `make dit-cloud PIPELINE=anchorages_pipeline WORKFLOW=workflows/port_visits/ais.py ARGS="..."`. The Makefile sets the Beam pin (2.76.0) for this pipeline.

### Reading the result

- Each pair logs `compare <a> vs <b> -> rc=<n>`. `rc=0` is identical; anything else is a difference, and the workflow exits non-zero.
- A single mode logs "no pair to compare" and exits 0. That is not a pass.
- `6_parallel` logs its verdict, the declaration it was judged against, and how Airflow may schedule the step as declared.
- Before reading IDENTICAL as a pass, check the window had rows: an empty table compares identical to another empty table.

## `cross_version_ais.py`: versions against each other

Runs `ais.py` once per binding (`--binding name=ref`, a git ref in `--pipeline-dir`) on the same pinned inputs, then diffs the bindings' outputs pairwise per mode. A difference is reported, not a failure. The exit code is non-zero only if a binding failed.

What is pinned depends on `--steps` (copies in `tech_great_expectations`, default 7-day expiry, refused above `--pin-max-gb`, default 1000):

| step | pinned |
|---|---|
| `thin_port_messages` | messages over `[--start, --end]` |
| `port_visits` | `segment_info` and `segs_activity` whole; with step 2 alone also `--thinned-message-table` over `[--start, --end]` |

Every copy's name ends in the pin stamp. The run logs it; `--reuse-pin <stamp>` reruns on the same copies.

The wrapper owns the inputs, dates, modes, steps, suffix and experiment id of every binding. The same flags passed as extras are dropped, so no binding can read an unpinned table.

**Each binding needs its own image to test its own code.** A binding without `--binding-image-tag` / `--binding-worker-image` runs `ais.py`'s default image, v5.0.1, whatever its ref. Two such bindings run the same code, and their diff is empty by construction. A binding whose ref is not merged and that would run the default image fails at its start (sources already pinned; rerun with `--reuse-pin`).

```bash
dit run workflows/port_visits/cross_version_ais.py \
    --experiment-id my-change \
    --start 2020-01-01 --end 2020-12-31 \
    --binding before=v5.0.1 \
    --binding after=<branch or sha> \
    --binding-image-tag after=<overlay image> --binding-worker-image after=<overlay image> \
    --modes 1_bf --runner dataflow
```

Bindings run in parallel (`--sequential-bindings` opts out); their log lines are prefixed `[<name>]`. `--dry-run` sets up and removes the worktrees and dry-runs the copies, and runs nothing else. The summary at the end lists `IDENTICAL`, `DIFFERENT` or `SKIPPED` (a binding failed) per mode and pair.

## Production baseline (`--baseline-production`)

`cross_version_ais.py --baseline-production ais-v5-datalake --production-date D` runs only the candidate binding(s), on the inputs production read for day D, and diffs each against production's own output on `visit_id`. Production is not re-run. Mechanics, guards, costs and the first live run: [`docs/production-baseline-2026-10.md`](../../docs/production-baseline-2026-10.md).

What is specific to port visits:

- The run shape is fixed to production's: `--steps port_visits`, `--modes 1_bf`, `--start 2012-01-01`, `--end D`. Other values are refused, because open visits take their end and confidence from the end date.
- The target (`ProductionTarget` in `cross_version_ais.py`) is hand-written from composer and checked field by field by the drift check (12 fields, all matching on 2026-11-25).
- Since v5.0.1, production's port-visits table requires a partition filter on `end_timestamp`. The pins and table-check handle it. Hand-written SQL on a v5.0.1 output, DIT's included, needs `WHERE (end_timestamp IS NULL OR end_timestamp IS NOT NULL)` to read every row.
- Production's job for D runs about three days after D. `segment_info`'s 48-hour time travel sets the deadline. The run refuses to start if the pins would expire within `--production-margin-hours` (default 6).
- A full-history candidate run reads about 1.3 TB through the views, so it needs `--pin-max-gb 1300` and, to match production's speed, `--max-num-workers 100`. Both need the owner's go-ahead.

## Run history

Evidence recorded in this repository. Builds are Cloud Build ids; "run" is DIT's run id.

| date | what | id | result |
|---|---|---|---|
| 2026-05-15 | staging, three modes, v4 CLI, local temp-dataset patch | suffix `cb916bf_94dde7` | 3 of 3 pairs identical on `visit_id` |
| 2026-10-22 | first `make dit-cloud` cross-version attempt | build `d147efc5` | failed loading the workflow; fixed in `dit run` |
| 2026-10-22 | first v5 staging smoke | run `89744c79850f` | failed: overlay patch `21aed7a5` missed a read; fixed by `905dc761` |
| 2026-10-22 | staging smoke, three modes, v5 CLI, PR #176 overlay | run `99cc678ee30c` | 10,291 visits per mode, 0 differing rows |
| 2026-10 | staging cross-version, main b36aed6 vs PR 14b4be8 | build `1f7447b4` | step 1 identical; visits differ in 2 of 10,292 rows (same-second ties, intended by #176) |
| 2026-10 | September 2026, main vs PR | build `2a984881` | DIFFERENT, consistent with #176 |
| 2026-10 | Q3 2026, v4.6.4 vs PR | builds `0c6a46a6`, `77c967ef` | DIFFERENT, consistent with #176 |
| 2026-10-23 | September 2026 three-way, v4.6.4 on the same pins | build `ec8b6dae` | main alone changes about 1,100 visits; the PR about 4,000 |
| 2026-10-23 | determinism, September, PR twice with `--no-cache` | build `8a1348d1` | three runs IDENTICAL (3,869,516 rows) |
| 2026-10-23 | production baseline dry run as `automated-testing@` | build `947240a8` | job found, all four time-travel reads dry-run |
| 2026-10-23 | production baseline, PR #176 vs production's 2026-10-20 output, full history | build `7781dd48` | DIFFERENT, consistent with #176 (detail in the baseline doc) |
| 2026-11-19 | staging smoke on today's default image (`v5.0.1-767770f-temp-dataset-905dc761`), three modes, both steps, experiment `pv-v501-smoke`, e2-standard-2 / 25 GB | build `5f87695c` | 18 Dataflow jobs done; 10,596 visits per mode; all three pairs IDENTICAL. Also showed private worker IPs and `automated-testing@` reading the v20260302 named-anchorages table |

The October runs predate v5.0.1 and used the October overlay images; the 2026-11-19 smoke is the one run on today's default image.

**Not run yet:**

- no production-baseline run since production moved to v5.0.1;
- `6_parallel` has never run (code and tests only, 2026-11-24).

## Known limits and open items

- A default run compares step 2's output only; step 1's incremental table is compared only with `--steps thin_port_messages`.
- `(identifier, timestamp)` as step 1's key is unique in practice, not by construction.
- `thin_port_messages` is declared parallel-safe on the owner's claim; `port_visits` is `unknown` and not testable in slices.
- The drift check runs only when someone runs it.
- Production targets are hand-written; the core AIS v3 target is not built.
- Not covered: VMS port visits, AIS full-volume runs, voyages, `anchorages-visited-info`.
- When production bumps pipe-anchorages: rebuild the overlay image and bump the constants. Steps: overlay README § "Default image".
