# DIT as a framework

This is the guide for a developer who is new to DIT. It explains what the framework is, where its edges are, and what each building block does. It describes the library (`src/dit/`) and the shared runtime. It does not describe pipelines: for those, follow the links to the per-pipeline docs.

Every statement about behaviour names the file it was checked against. What has not been checked by a run is listed in [What is not verified](#13-what-is-not-verified).

Contents:

1. [What DIT is for](#1-what-dit-is-for)
2. [Boundaries](#2-boundaries)
3. [Anatomy of a run](#3-anatomy-of-a-run)
4. [Comparison shapes](#4-comparison-shapes)
5. [Source pinning](#5-source-pinning)
6. [The run cache](#6-the-run-cache)
7. [Run context: commits, snapshots, images](#7-run-context-commits-snapshots-images)
8. [Dataflow knobs, job names and labels](#8-dataflow-knobs-job-names-and-labels)
9. [The cloud runtime](#9-the-cloud-runtime)
10. [Staying in step with production](#10-staying-in-step-with-production)
11. [Orchestration declarations and slices](#11-orchestration-declarations-and-slices)
12. [Adding a pipeline or a workflow](#12-adding-a-pipeline-or-a-workflow)
13. [What is not verified](#13-what-is-not-verified)
14. [Module map](#14-module-map)

---

## 1. What DIT is for

DIT runs a real pipeline on real BigQuery data, several times, in ways that should give the same result. Then it compares the output tables.

A unit test checks one function. DIT checks a claim about the whole pipeline, for example:

- "A full backfill and a backfill followed by daily runs write the same table."
- "This pull request changes these rows and no others."
- "The candidate version reproduces what production wrote yesterday."

The first run of this idea, in the pipe-gaps repo, found two real bugs that unit tests had missed. That history is in [`context.md`](context.md).

DIT is a proof of concept. It has had successful runs on five pipelines. It is not yet a routine gate on pull requests. Status: [`README.md`](../README.md) § Roadmap.

---

## 2. Boundaries

### Who owns what

| Repo | Owns | DIT's relation to it |
|---|---|---|
| Processing repos (one per pipeline) | Pipeline code, its command line, its published images | Runs them as a black box, at any git ref. Never changes them. |
| `composer-dags-production` | Production DAGs and their parameters | Reads it **as data**: `git show` on rendered DAG snapshots. Never imports it. See [`composer-drift.md`](composer-drift.md). |
| `data-integration-tests` (this repo) | The `dit` library, the workflows, the cloud runtime | |
| [`table_identical_checks`](https://github.com/GlobalFishingWatch/table_identical_checks) | The table comparison (`table-check`) | Calls it as a subprocess. |

Diagram: [`architecture.md`](architecture.md) § 1.

### Rules that follow

| Rule | What it means in practice | Where |
|---|---|---|
| Library first, CLI second | Everything is importable Python. `dit run <file>` only loads a workflow file and calls its `main(argv)`. | `src/dit/cli.py` (`run`) |
| The comparison is a thin shim | `dit.compare.compare_tables` runs `table-check summary` and reads its JSON. New comparison features go upstream, not here. `ignore_columns` raises `NotImplementedError` for that reason. | `src/dit/compare.py` |
| Composer is data, not code | No `import gfw...`. No airflow. | `src/dit/composer.py` |
| A workflow lives in one place | The home is `workflows/<pipeline>/`. A workflow file is plain Python with a `main(argv) -> int`. | [`workflows/README.md`](../workflows/README.md) |
| Writes stay in DIT's namespaces | Images go to `gcr.io/world-fishing-827/dit/*`. Tables, cache rows and pinned sources go to `world-fishing-827.tech_great_expectations`. Dataflow temp files go under `gs://<bucket>/dit/`. Reads are unrestricted. | [`conventions.md`](conventions.md) § Prod-infra boundary |
| No datasets are created | The service account may create tables in the one dataset, not datasets. Cleanup deletes tables only. | `src/dit/cache.py` (`_looks_like_table_fqn`) |

### Source data targets

A workflow reads one of three source families. All three are first-class choices.

| Target | Size | Use |
|---|---|---|
| AIS staging cohort | about 4 GB, 2020 only, 142 vessels | The default. Cheap and safe. Too thin for some behaviour. |
| VMS | about 500 GB, 2012 onward | Realistic data at a low cost per window. |
| AIS production | tens of TB | Final integration tests only, on a narrow window. |

The rule is that **a run without flags stays cheap**: a small cohort for the first target, a narrow date window for the other two. The full inventory, with tables and traps, is in [`README.md`](../README.md) § Source data targets.

---

## 3. Anatomy of a run

Most workflows follow the same steps. The pipeline-specific parts are the command line a step gets, the output tables, and the compare keys. The rest is shared (`src/dit/workflow.py`).

| Step | What happens | Shared helper |
|---|---|---|
| 1. Parse arguments | Validate everything that can be validated without a cloud call: modes, steps, dates, worker cap, experiment id. | `add_dataset_args`, `add_dataflow_args`, `add_experiment_id_arg`, `add_modes_arg`, `add_max_num_workers_arg`, `add_worker_sizing_args` |
| 2. Resolve the run context | Which commit of the pipeline runs, whether it is reviewed, which image runs it, and a run id. | `resolve_run_context` (section 7) |
| 3. Pin the sources | Copy what the pipeline will read, so every job of the run reads the same data. Cross-version and outage workflows do this; plain mode-equivalence workflows on the frozen staging cohort do not. | `dit.bq.pin_source_into_experiment` (section 5) |
| 4. Run the jobs | One or more pipeline invocations per mode or binding, through a runner. | `dit.runners.docker.run`, `dit.runners.dataflow.run` |
| 5. Compare | Pairwise table comparison on the workflow's keys. | `dit.compare.compare_tables` |
| 6. State the verdict | Log IDENTICAL or DIFFERENT per pair and return the exit code the shape defines (section 4). | per workflow |

### The comparison call

```python
compare_tables(table_a, table_b, keys=[...], view_suffix="", tolerance={...})
```

- `keys` is the join key of the comparison. Each workflow fixes it in code.
- `view_suffix` is appended to both table names. Outputs that keep versions of a row are compared through a view of the latest version: pipe-gaps v0.17.1 names that view itself (`--bq-out-gaps`; the rows are in `<view>_versioned`), so the workflows pass the view and `""` (v0.10.0, until 2026-11-27, had a `_last_versions` suffix). Outputs that are rewritten (port visits, fishing events) are compared directly, with `""`.
- `tolerance` maps a column to an absolute tolerance.
- The return value is 0 for identical and 1 for any difference. The number of differing rows is logged. It is clamped to 1 on purpose: workflows pass the value to `sys.exit`, and a raw count that is a multiple of 256 would read as success.

### Worked example

```bash
dit run workflows/port_visits/ais.py --runner dataflow --parallel
```

This is mode equivalence for port visits on the staging cohort.

1. `parse_args` resolves the three default modes and the worker cap (default 4).
2. `resolve_run_context` reads the pipe-anchorages checkout. A clean checkout of a commit merged into `origin/main` is "reviewed" and runs the workflow's default image.
3. Nothing is pinned: the staging cohort does not change.
4. Each mode runs the pipeline's two steps into its own output tables. Each mode goes through the run cache, so a mode that already ran with the same code and parameters is skipped.
5. The three output tables are compared pairwise on `visit_id`.
6. The exit code is 0 only if all three pairs are identical.

What the steps and tables mean for port visits is in the per-pipeline docs ([`workflows/README.md`](../workflows/README.md)).

### Two runners

| Runner | How the pipeline runs | Used by |
|---|---|---|
| `dit.runners.docker.run` | `docker run` of a published image, or `docker compose run` of an image built from the working tree (`build_from_source`). The pipeline inside may itself submit to Dataflow. | Port visits, encounters, pipe-events, pipe-segment |
| `dit.runners.dataflow.run` | Builds the Beam pipeline in the DIT process and submits it to Dataflow. Submission is serialised by a lock; waiting is not. | pipe-gaps |

The split follows the pipelines, not taste. pipe-gaps exposes a pipeline factory that can be driven in-process. The others are driven through their container's command line, as production's Airflow does.

One consequence for pipe-gaps: the Beam version in the submitting environment must equal the one in the worker image. The pins live in one place, the `*_BEAM_VERSION` constants at the top of the [`Makefile`](../Makefile).

---

## 4. Comparison shapes

A shape is the question a workflow asks. Each has its own rule for the verdict and the exit code.

| Shape | Question | What is compared | Exit code |
|---|---|---|---|
| Mode equivalence | Do different ways of loading the same window give the same table? | Each pair of modes, one pipeline version | Non-zero if any pair differs |
| Cross-version | What does this code change do to the output? | The same mode at two or more git refs (bindings) | Non-zero only if a binding failed. A difference is the result, not an error. |
| Production baseline | Does the candidate reproduce what production wrote? | One binding against production's own table | As cross-version |
| Parallel slices | May this step load several date slices at once? | Slices loaded concurrently into one table, against one job over the window | Judged against the step's declaration (section 11) |

The split between the first two is deliberate. An equivalence test asserts sameness, so a difference fails. A cross-version test surfaces change, so a difference is information. Read a workflow's module docstring before wiring its exit code into anything.

### Mode equivalence

A mode is a trigger pattern: one way of orchestrating the pipeline over a date window.

| Mode | Pattern |
|---|---|
| `1_bf` | One backfill job over the whole window. The reference. |
| `2_bfd` | A backfill up to the tail, then one daily job per tail day. |
| `3_bftruncate` | A full backfill, then the tail days run again on top of it. |
| `4_daily` | Every day its own job (pipe-segment; opt-in). |
| `4_mutate_recover` | Daily runs on a restricted input, then a recovery (pipe-gaps; opt-in with its own flag). |
| `6_parallel` | Slices loaded concurrently (port visits step 1; see "Parallel slices"). |

- `--modes` selects a subset. The selection is returned in the workflow's declared order, whatever order was typed, so two equivalent invocations log the same thing (`dit.workflow.parse_modes`).
- An unknown mode is an error at argument parsing. A typo that ran nothing would look like a pass.
- A single-mode run compares nothing and says so ("no pair to compare"). It returns 0.
- The three mode loops are written out per workflow. They were compared side by side twice and not merged; the reasoning is in [`workflows/README.md`](../workflows/README.md).

### Cross-version

A binding is a name and a git ref of the pipeline: `--binding before=v5.0.1 --binding after=my-branch`.

1. The wrapper pins the sources once (section 5).
2. For each binding it adds a git worktree at the ref and runs the single-version workflow there, pointed at the pinned copies. Bindings run in parallel by default.
3. It compares the bindings' outputs pairwise. Pairs that touch a failed binding are skipped.

Three things make the comparison honest:

- **Same inputs.** All bindings read the same pinned copies. The wrapper owns the dates, so no binding reads outside them.
- **The right code on both sides of Dataflow.** Code runs in the submitter and in the workers. Each binding needs an image that holds its code (section 7). Without that, every binding runs the same published worker code and the test passes without testing anything. This happened once, on 2026-05-15.
- **The right interface per side.** When the refs differ in their command line, each binding gets adjustments (section 10).

Example: `workflows/port_visits/cross_version_ais.py`; its docstring lists the steps.

### Production baseline

Production is not re-run. The candidate reads exactly what production read, and its output is compared with the table production wrote.

- The run finds production's Dataflow job for a date and takes two moments from it: the job's creation time and the time it reached `Done` (`dit.production.find_dataflow_job`). Table metadata cannot give these: a production job touches its output table at job start.
- Inputs are pinned as views `FOR SYSTEM_TIME AS OF` the first moment, the output as of the second (`dit.bq.pin_view_into_experiment`).
- A time-travel view is readable only inside the source dataset's time-travel window. `dit.bq.check_time_travel` refuses a run that would outlive a pin. So such a run is time-boxed.
- The run shape is forced to match production's: steps, modes and dates.

Only port visits has this today (`--baseline-production` on `cross_version_ais.py`). Design and first live run: [`production-baseline-2026-10.md`](production-baseline-2026-10.md).

### Parallel slices

The window is cut into slices. Every slice is its own job, all are submitted before any is awaited, and all write into one table. That table is compared with one job over the window. Section 11 has the judgement table.

---

## 5. Source pinning

Sources move. A comparison of two runs means little if they read different data. Pinning gives every job of a run one fixed input.

| Method | Pins | Helper | Used for |
|---|---|---|---|
| Copy | The source as of run start | `dit.bq.pin_source_into_experiment` | Cross-version runs, outage recovery |
| Copy of a view | The rows the view returned at run start | same helper; it reads the source's type | Sources that are views |
| Time-travel view | The source as of a past moment | `dit.bq.pin_view_into_experiment` | Production baseline |
| Derived view | A filtered view over a source or a pin | `dit.bq.derived_source_into_experiment` | Synthetic outages (pipe-gaps) |

BigQuery snapshots are not used. They cannot cross organisations, and a snapshot with an expiration needs a permission no DIT identity has. The snapshot helpers are still in `dit.bq`; no workflow calls them.

### Copies

- **Statement.** `CREATE TABLE <dest> LIKE <source> ... AS SELECT * FROM <source> WHERE <predicate>`. `LIKE` keeps partitioning and clustering, so the pipeline reads the copy as cheaply as the source.
- **Window.** A `SourceWindow(column, start, end)` is half-open and filters with `DATE(column)`, the form the pipelines use. Without a window the whole table is copied.
- **Whole table with a required partition filter.** The predicate is ``(`col` IS NULL OR `col` IS NOT NULL)`` on the partition column (`all_rows_predicate`). BigQuery accepts it as a filter, and it drops no row.
- **Views.** BigQuery refuses `LIKE` on a view, so a view is copied with a plain `CREATE TABLE ... AS SELECT`. A windowed copy of a view is partitioned by month on the window column.
- **Name.** `dit_exp_<experiment id>_<role>_<source table>_<window or "all">_<pin stamp>` (`pinned_source_fqn`).
- **Pin stamp.** The run's start time in UTC, like `20261022t171500z`. One stamp per run, shared by all its copies. The copy's data is the source as of the copy job's start; copies of several sources start seconds apart.
- **Reuse.** `--reuse-pin <stamp>` makes a rerun read an earlier run's copies. The names are recomputed and each is checked for existence and for the `dit_artifact=source_pin` label before any job starts (`require_pinned_source`).
- **Cap.** The copy statement is dry-run first and refused above `max_bytes` (default 1 TB, `DEFAULT_PIN_MAX_BYTES`). The job also carries the cap as `maximum_bytes_billed`. Workflows expose it as `--pin-max-gb`. The cap is per copy, not per run.
- **Expiry.** Seven days by default (`--pin-expiration-days`).
- **Dry run.** A workflow's `--dry-run` reports each copy's estimated bytes and creates nothing.

There is no `IF NOT EXISTS` for copies. The stamp makes every run's names new, so an existing table means a reused stamp, and the create fails.

### Time-travel views

- The name holds the source, the window and the `as_of` moment, so the name fixes the definition. That is why these views are created with `IF NOT EXISTS`.
- `as_of` must be whole seconds, the precision of the stamp.
- A full read through the view is dry-run against the same cap.
- Time travel was rejected as the general pin. On a table modified inside its time-travel window, a time-travel read loses partition pruning. [`plan.md`](plan.md) § `dit.bq` has the measured example.

### A known gap

A reused copy's source is not checked. `--reuse-pin` with another source that has the same table name reads the earlier copy. The copy's description names its source.

---

## 6. The run cache

One BigQuery table, `world-fishing-827.tech_great_expectations.dit_runs`, does three jobs (`src/dit/cache.py`):

| Job | How |
|---|---|
| Cache | Before a mode runs, the workflow looks for an earlier successful run with the same key. On a hit it skips the pipeline and compares the cached table. |
| Registry | Every run records the tables it wrote, so they can be cleaned up. |
| Provenance | "Which commit produced this table" is one query. |

### The key

```
sha256( pipeline_commit, worker_image_digest, workflow_file_sha1, params )
```

| Part | Why it is in the key |
|---|---|
| `pipeline_commit` | The code under test. A real commit, or a snapshot commit (section 7). |
| `worker_image_digest` | The image as a digest, not a tag. Tags move. |
| `workflow_file_sha1` | The workflow file's bytes. Editing a workflow invalidates its rows; a refactor inside `dit.*` does not. |
| `params` | Only what affects the output. Each workflow filters its arguments in `canonical_params_dict`. |

Runner-only settings (worker cap, machine type, disk, bucket) are never in the key.

### Behaviour

- **Hit.** The key matches a succeeded, unexpired row and every recorded table still exists. The wrapper returns the cached table name (`dit.workflow.run_with_cache`).
- **Stale.** A row matches but a table expired. The mode runs again.
- **Miss.** The mode runs and a row is written.
- **`--no-cache`.** Skips the lookup and recomputes. The run is still recorded. Use it for a determinism check: without it, a repeat run would compare a table with itself.
- **Failed runs.** For executions that can fail after writing part of their tables, a row with status `failed` is written, so cleanup can find the tables. A failed row is never a hit.
- **Rows are written with DML `INSERT`**, not streaming inserts. Streamed rows cannot be updated for about 90 minutes, which would block cancelling a fresh run.

The cache assumes the pipeline is deterministic. A pipeline that is not makes the cache unsound.

Not every workflow uses the cache. Three do today: `pipe_gaps/mode_equivalence.py`, `pipe_gaps/outage_recovery.py` and `port_visits/ais.py` (and the port-visits cross-version wrapper through it).

### Cleanup

```bash
make dit-cancel RUN_ID=<12-hex id>      # or: dit cache-cancel <id> [--region <r>]
```

`dit.cache.cancel_run` does three things, in this order:

1. Finds the run's Dataflow jobs by the label `dit_run_id=<id>` and cancels those not yet finished. This comes first because a run that is still going has jobs but no row yet.
2. Deletes the tables recorded on the run's rows. Only values of the form `project.dataset.table` are deleted. Anything else is skipped with a warning.
3. Marks the rows `cancelled`.

It is idempotent. An id that matches neither a job nor a row is an error.

`dit run` also traps SIGTERM and calls `cancel_run` for the active run, then exits 143 (`src/dit/cli.py`, `_on_sigterm`). Cloud Build sends SIGTERM when a build is cancelled, so a cancelled build stops its own Dataflow jobs. Limits: jobs are searched in `DIT_DATAFLOW_REGION` (default `us-central1`), and `DIT_NO_CANCEL_ON_SIGTERM` switches the trap off.

Design: [`run-cache.md`](run-cache.md). Milestones: [`run-cache-impl.md`](run-cache-impl.md).

---

## 7. Run context: commits, snapshots, images

`dit.workflow.resolve_run_context` answers three questions at the start of every run and returns a `RunContext`.

### Which commit runs

Every run records a committed git ref of the pipeline (`dit.snapshot.resolve_pipeline_commit`).

| State of the pipeline checkout | What is recorded |
|---|---|
| Clean | `HEAD` |
| Dirty, runner is Dataflow | A snapshot commit, created and pushed automatically |
| Dirty, runner is docker | `HEAD`, marked unreviewed. No snapshot is made. See the note below. |
| `DIT_PIPELINE_COMMIT` is set (cloud path) | That value. The laptop resolved it before submitting. |

"Dirty" means changes to tracked files only (`dit.git_info.git_info`).

**A dirty tree with the docker runner.** The uncommitted changes run only with `--build-from-source`, where `docker compose` builds the container from the working tree. Without that flag the container comes from an image, and the image rules below apply: an automatic build exports the recorded commit (`git archive`), which is `HEAD` here, so the uncommitted changes are not in it. Commit first, or pass `--build-from-source`.

### Snapshots of a dirty tree

A snapshot is an orphan commit of the tracked files, pushed to `refs/dit-snapshots/<pipeline>/<sha>` on the pipeline repo's origin (`scripts/snapshot.sh`).

- **Deterministic.** Author, committer and dates are frozen. The same tree gives the same SHA, so a repeat run of unchanged uncommitted code hits the cache.
- **Orphan.** No parent, so pushing it pushes none of your unpushed commits. The parent SHA is written into the commit message (`dit snapshot of <sha>`) and stored in `dit_runs.pipeline_commit_parent`.
- **Hidden namespace.** Not a branch. Fetchable by anyone with read access.

The push goes to a repository that may be public. Three safeguards apply, in this order:

1. **Tracked files only.** The temporary index is seeded from `HEAD` and updated with `git add -u`. Untracked files, including files staged but never committed, do not enter the snapshot.
2. **A banner** lists the changed paths before the push.
3. **A secret scan** (gitleaks, pinned in ditbox at v8.30.1) runs on the snapshot tree. A finding aborts before the commit and the push. If gitleaks is missing the snapshot refuses to proceed. `DIT_SKIP_SECRET_SCAN=1` bypasses it with a loud warning.

If a secret was pushed: rotate it first. Then `make clean-snapshot PIPELINE=<name> REF=<sha>`. Background: [`no-dirty-tree-pivot.md`](no-dirty-tree-pivot.md).

### Reviewed or unreviewed

A commit is **reviewed** when it is an ancestor of the pipeline's `origin/main` (`dit.snapshot.is_unreviewed`). A snapshot, a dirty tree and an unmerged branch are unreviewed. When the check cannot run, the answer is "unreviewed".

The flag is stored on the cache row (`unreviewed_code`) and decides what happens with images.

### Which image runs

All DIT images live under `gcr.io/world-fishing-827/dit/`. Naming table: [`conventions.md`](conventions.md) § Image namespace.

| Situation | What runs | Mechanism |
|---|---|---|
| Reviewed code, default image | The workflow's default image | Nothing is built |
| An explicit `--worker-image` / `--image-tag` | That image | Always respected |
| Unreviewed code, default image, plain default | An image built from the commit, tagged `dit/<pipeline>:dit-<commit>` | Auto-build: `dit.worker_image.ensure_pipeline_image`, a kaniko Cloud Build. An existing tag is reused. |
| Unreviewed code, default image, **overlay** default | Nothing. The run is refused before any cloud call. | The workflow passes `check_images` to `resolve_run_context` |
| `--build-from-source` (docker runner) | The working tree, built by `docker compose` | The auto-build is skipped |

**Why the auto-build exists.** A Dataflow job runs code in two places: the submitter and the workers. The workers take their code from the image. Unreviewed code on a published image would run the old code on the workers.

**Why some workflows refuse instead.** An overlay image is a pipeline image plus a small patch that only DIT needs. The patch in use today lets the pipeline stage BigQuery reads in an existing dataset, because DIT's service account may not create datasets. An auto-built image is plain source, without the patch, so it cannot stand in for an overlay default. The operator builds the overlay image of the commit with the `build.sh` in [`docker/overlays/`](../docker/overlays/) and passes it explicitly.

An overlay default must be rebuilt whenever production moves to a new pipeline version. The drift check reports when that is due (section 10).

---

## 8. Dataflow knobs, job names and labels

### Worker cap

`world-fishing-827` also runs production's Dataflow jobs. An uncapped DIT run would compete for regional quota. So every Dataflow workflow takes `--max-num-workers` (`dit.workflow`, section a2).

Resolution, the more specific setting winning (`resolve_max_num_workers`):

1. the flag, even above the ceiling;
2. else `min(default, DIT_MAX_NUM_WORKERS_CEILING)` when that environment variable is set;
3. else the workflow's default.

The ceiling only lowers a default. Each run logs its cap and where it came from.

### Worker sizing and IPs

- **Private IPs.** Every DIT Dataflow job runs with `--no_use_public_ips`, as production does.
- **Machine type and disk default to production's**, per step. They are hand-written constants (`STEP_WORKER_SIZING`) that cite the composer snapshot they were read from.
- `--worker-machine-type` and `--disk-size-gb` override them, field by field (`resolve_worker_sizing`).
- A Dataflow run that keeps production's sizing while reading the staging cohort logs a `cost hint:` warning. It never blocks.
- In a production-baseline run, the production target's sizing is the default for every binding.
- Steps that are not Dataflow jobs take no Dataflow option at all (pipe-segment's four SQL steps).

| Workflow | Default worker cap | Default sizing |
|---|---|---|
| Port visits | 4 (`DEFAULT_MAX_NUM_WORKERS`, `workflows/port_visits/ais.py`) | e2-highmem-4, 50 GB (`STEP_WORKER_SIZING`, same file) |
| Encounters (AIS, VMS) | 50 (`DEFAULT_MAX_NUM_WORKERS`, `workflows/encounters/ais.py`; `vms.py` reuses it) | e2-highmem-4, 50 GB (`STEP_WORKER_SIZING`, same file) |
| pipe-gaps | 50 (`DEFAULT_MAX_NUM_WORKERS`, `workflows/pipe_gaps/mode_equivalence.py`; `outage_recovery.py` imports it) | e2-custom-2-3072, 25 GB (`DEFAULT_WORKER_SIZING`, wrapped as `STEP_WORKER_SIZING`, in `workflows/pipe_gaps/raw_gaps.py`) |
| pipe-segment, cross-version | 50 per Beam step (`STEP_MAX_NUM_WORKERS`, `workflows/pipe_segment/identity_match_key.py`) | e2-standard-4, 50 GB (`STEP_WORKER_SIZING`, same file) |
| pipe-segment, mode equivalence | 4 (`DEFAULT_MAX_NUM_WORKERS`, `workflows/pipe_segment/ais.py`) | e2-standard-4, 50 GB (`STEP_WORKER_SIZING`, same file, taken from `identity_match_key.py`'s `segment` entry) |

Output must not depend on the cap or the sizing. They are never in a cache key. Rules for a new workflow: [`conventions.md`](conventions.md) § Runner-only Dataflow knobs.

### Short jobs on DirectRunner

A Dataflow job spends about four minutes starting and stopping workers. On a small cohort that dwarfs the work. `--direct-runner-max-days N` runs every job of N days or fewer on Beam's DirectRunner inside the container and leaves longer jobs on Dataflow (`dit.workflow.runner_for_days`). It is set by hand. DIT does not check that the two runners agree; the run's own comparison does, whenever a short job is compared with a long one. Used by `workflows/pipe_segment/ais.py`.

### Per-user settings

| Variable | Sets |
|---|---|
| `DIT_DEST_DATASET` | Output dataset (default `tech_great_expectations`) |
| `DIT_DATAFLOW_SA`, `DIT_DATAFLOW_REGION`, `DIT_DATAFLOW_TEMP_BUCKET`, `DIT_DATAFLOW_SUBNETWORK` | Dataflow placement |
| `DIT_BQ_TEMP_DATASET` | Where Beam stages BigQuery reads |
| `DIT_EXPERIMENT_ID` | Default for `--experiment-id` |
| `DIT_MAX_NUM_WORKERS_CEILING` | The worker-cap ceiling |
| `DIT_COMPOSER_DAGS_DIR` | The composer checkout the drift check reads |

Most have a matching flag. `.envrc.example` documents the infrastructure ones. Whatever the bucket, temp and staging paths go under `gs://<bucket>/dit/` and are built only by `dit.dataflow_paths`.

### Names

- **Experiment id.** `--experiment-id` is a slug that groups runs. It leads the output-table suffix: `<experiment_id>_<commit>_<uuid>`. Without one, a run gets `solo_<6 hex>`.
- **Run id.** Twelve hex characters, new for every run. Logged as `run_id=...`.
- **Dataflow job name.** `dit-<repo>-<step>-<experiment>-<binding>-<mode>-<n>-<of>` (`dit.job_names.make_job_name`). At most 63 characters. When the name is too long the experiment id is shortened, never the tail. If nothing can be shortened it raises.
- **Labels.** `dit_run_id` on Dataflow jobs is what cleanup searches for. Workflows add their own (`dit_repo`, `dit_step`, `dit_experiment_id`, `dit_binding`, `dit_mode`). Pinned sources carry `dit_artifact`, `dit_experiment_id`, `dit_role`, `dit_pin` and `dit_source_table`.

Not every workflow stamps `dit_run_id`, and only the workflows with a run cache record their tables in `dit_runs`, so `make dit-cancel` (§ 6, Cleanup) covers them unevenly. State of 2026-11-25:

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

## 9. The cloud runtime

Long runs do not belong on a laptop: a sleeping laptop stalls the chain of jobs. `make dit-cloud` moves the orchestrator into Cloud Build.

```bash
make dit-cloud PIPELINE=<checkout dir> WORKFLOW=workflows/<pkg>/<file>.py ARGS="..."
```

### What happens on the laptop

Read from the `dit-cloud` target in the [`Makefile`](../Makefile).

1. **The commit is resolved here**, because only the laptop can push. With `REF=` it is that ref. With a dirty checkout a snapshot is created and pushed (section 7). Otherwise it is `HEAD`. `REQUIRE_CLEAN=1` turns the dirty case into an error.
2. **Reviewed or not is resolved here too**, with a fetch of `origin/main` and an ancestry check. The builder cannot fetch the pipeline's origin.
3. **The build is submitted** with `gcloud builds submit --async`. The source upload is the pipeline checkout at `$PROJECTS/<PIPELINE>`. The command returns a build id in seconds. `WAIT=1` streams the logs and blocks instead.

| Variable | Meaning |
|---|---|
| `PIPELINE` | Directory name of the pipeline checkout under `$PROJECTS` |
| `WORKFLOW` | Path of the workflow file inside the DIT repo |
| `ARGS` | Appended to `dit run <workflow>` |
| `REF` | Pipeline ref to install, non-editable. Empty: the uploaded working tree, editable. |
| `DIT_REF` | DIT ref to clone (default `main`). A branch or tag: it is passed to `git clone --branch`. |
| `BEAM_VERSION` | Pins apache-beam in the build. Defaults per pipeline from the Makefile's constants. |
| `WAIT`, `REQUIRE_CLEAN` | See above |

### What happens in the build

Read from [`cloudbuild-dit.yaml`](../cloudbuild-dit.yaml). One step, in the `ditbox` image.

1. Clone DIT from GitHub at `DIT_REF` into `/workspace/.dit`.
2. Install DIT with `uv pip install --no-deps -e`. Its dependencies are already in ditbox.
3. Install the pipeline with `uv`: from `git+file:///workspace@<REF>` when `REF` is set, else editable from `/workspace`. When a Beam version is given it is applied as a constraint.
4. Run `dit run /workspace/.dit/<WORKFLOW> <ARGS>` from `/workspace`.

The build runs as `automated-testing@world-fishing-827`, for at most 24 hours. Its environment carries `DIT_PIPELINE_COMMIT`, `DIT_UNREVIEWED` and `DIT_CLOUD_MODE=1`.

### ditbox

`gcr.io/world-fishing-827/dit/ditbox` is a tooling image: Python 3.12, git, gcloud with `bq`, the docker CLI, gitleaks, `uv`, and DIT's own requirements ([`docker/ditbox/Dockerfile`](../docker/ditbox/Dockerfile)). It holds neither DIT nor any pipeline. Both are installed per run, so a change to either needs no image rebuild.

`make publish-ditbox` rebuilds it with kaniko and pushes `:latest` and `:<short sha>`.

Two consequences:

- **A new dependency in `requirements.txt` needs a new ditbox.** DIT is installed with `--no-deps` in the build.
- **`:latest` is shared.** Every later run pulls it. Verify a new ditbox on a real run before publishing it.

### Credentials

There is no credential file anywhere in the cloud path. In Cloud Build, the build step gets its identity from a metadata server on a docker network named `cloudbuild`. A container that DIT starts with `docker run` is not on that network by default.

So when `DIT_CLOUD_MODE` is set, the docker runner adds `--network=cloudbuild` and drops any mount onto `/root/.config` (`dit.runners.docker._apply_cloud_mode`). On a laptop the workflow's own mount of the `gcp` docker volume is used. Workflow code is the same in both places.

`--network=host` does not work: it reaches a different metadata server, with a Google-managed identity. The three contexts, and the two designs that failed first, are in [`conventions.md`](conventions.md) § "Auth in the cloud path (ditbox)".

### After submitting, check

| Check | How | Why |
|---|---|---|
| Exactly one build was created | `gcloud builds list --ongoing` | A hung upload can retry and create a second build (recorded in `production-baseline-2026-10.md`). |
| The run id | `run_id=` in the build log | You need it to cancel. |
| The commit and whether it is unreviewed | Same log line, and the `>>> pipeline_commit=... unreviewed=...` line `make` prints | Confirms what code runs. |
| The worker cap | `Dataflow worker cap:` in the log | A local `DIT_MAX_NUM_WORKERS_CEILING` does **not** reach the build. Pass `--max-num-workers` in `ARGS`. |
| The pin stamp | Logged when sources are pinned | You need it for `--reuse-pin`. |
| Job durations | `gcloud dataflow jobs describe` | The submitter's log timestamps are not compute time. |

To stop a run: cancel the build (the SIGTERM trap cleans up), or `make dit-cancel RUN_ID=<id>`.

Diagram: [`architecture.md`](architecture.md) § 6. Day-to-day commands: [`usage.md`](usage.md).

---

## 10. Staying in step with production

DIT copies by hand what production runs: the command line of each step, the image version, the input tables. Copies go stale. Two mechanisms deal with that.

### The drift check

`dit.drift` compares what each workflow would run with what production's DAG runs, per step, on three levels: version, command and flags. It reads composer's rendered DAG snapshots as data and runs nothing.

```bash
git -C ../composer-dags-production fetch origin
dit run workflows/composer_drift.py
```

It is explained in full in [`composer-drift.md`](composer-drift.md).

### Per-side adjustments

A cross-version run gives every binding the same command line. That only works while the refs share an interface. When they do not, each binding gets edits (`src/dit/adjust.py`).

**The interface** of a binding (`adjust.Interface`) is what faces the pipeline:

| Part | Content |
|---|---|
| `steps` | Per step, the command tokens |
| `inputs` | The source tables it reads: role, source, window |
| `outputs` | The tables it writes: name, table, compare keys |

**Layers**, a later one winning (`adjust.apply`):

| Layer | What | Where | Chosen by |
|---|---|---|---|
| Base | The workflow's own builders. Mirrors production today. | `workflows/<pkg>/*.py` | Always |
| Declared | Edits that hold for a range of pipeline versions | `workflows/<pkg>/adjustments.py` | The binding's ref: what its command-line source declares first, its version second |
| Ad hoc | Edits for one run | `--binding-adjust NAME=<step>:<op>:<flag>[=<value>]` | The operator |

**Edits.**

- Flag edits: `remove`, `add`, `rename`, `set`, `command`. The ad hoc form allows the first four.
- Table edits: `add_input`, `remove_input`, `replace_inputs`, `rename_output`, `add_output`, `remove_output`, `set_keys`. Declared in code only.
- Placeholders in a value: `{out.<name>}` is the binding's own output table, `{in.<role>}` the pinned copy of an input.

**Rules.**

- No adjustment means the same tokens, in the same order and spelling.
- An edit that finds nothing to change raises `AdjustmentError`. A stale rule must be loud.
- Each declared adjustment has a `reason`, printed with every edit in the run log.
- A rule about a flag asks the ref's source first. An unreleased branch still carries the previous version number.

**Tables.** The union of all sides' inputs is pinned once. Sides are compared by output name. An output only one side writes is listed as not compared. A compared pair must have equal keys on both sides, or the run refuses before pinning. When the sides read different source tables, DIT cannot show they hold the same data; the run log says so.

**Preflight.** Before anything is pinned, every pipeline flag of each binding's adjusted command line is checked against the flags that ref's source declares (`declared_flags`, `undeclared_flags`). The source is read with `git show <ref>:<path>`. This is a static read: it finds flags written as string literals. Beam's own options are exempt on Beam steps, from a hand-written list next to the workflow. The check exists because some pipelines hand unknown flags to Beam, which ignores them, so a wrong flag would run without effect.

**The version check.** A binding whose image tag names another pipeline version than its ref states is refused before pinning. An unreleased head on the next release is only warned about. Implemented in the consumer, `workflows/pipe_segment/identity_match_key.py`.

**Older layers are checked too.** Each declared adjustment for an older version names the composer commit at which production last ran that version. A frozen drift spec checks base plus adjustment there ([`composer-drift.md`](composer-drift.md) § Frozen specs). An adjustment and its frozen spec are kept only while their version is production's current or previous one.

**Two stages.**

1. **Now.** The rules live in DIT, one file per pipeline, keyed by version range.
2. **Later ([PIPELINE-4819](https://globalfishingwatch.atlassian.net/browse/PIPELINE-4819)).** Each pipeline repo carries a file that describes its own interface for DIT. DIT reads it at each binding's ref, as it reads the command-line source today. DIT's table then remains only as the fallback for refs older than that file.

One consumer today: `workflows/pipe_segment/identity_match_key.py`, with one declared adjustment ([`workflows/pipe_segment/adjustments.py`](../workflows/pipe_segment/adjustments.py)). The table edits have no real case yet. Design record: [`plan.md`](plan.md) § "Per-side adjustments".

---

## 11. Orchestration declarations and slices

Some steps may load several date slices at once. Others carry state from one run to the next and must run in order. That decides how a step may be scheduled. DIT writes it down per step and can test it.

### Declarations

One file per pipeline, `workflows/<pkg>/orchestration.py`, holds a `dit.orchestration.PipelineOrchestration`.

| Field | Values |
|---|---|
| `parallel_slices` | `allowed`, `forbidden`, `unknown` |
| `depends_on_previous_run` | `True`, `False`, `None` |
| `status` | `verified` (the source is a Cloud Build id), `claimed` (the source says who and when), `untested` |
| `reason` | Why, in words |

Nothing writes `verified` automatically. A result reaches a declaration only through a human edit. Today every declaration is a claim or untested. The seeded table is in [`plan.md`](plan.md) § "Orchestration capabilities".

### The judgement

A parallel-slices test has one of three outcomes. `dit.orchestration.judge` turns the outcome and the declaration into a verdict and an exit code.

| Declared | Outcome | Verdict | Exit |
|---|---|---|---|
| allowed | identical | pass | 0 |
| allowed | different | fail | 1 |
| allowed | a slice failed | fail | 1 |
| forbidden | different | as declared | 0 |
| forbidden | a slice failed | as declared, with a warning | 0 |
| forbidden | identical | the declaration may be too strict, with a warning | 0 |
| unknown | identical | no difference | 0 |
| unknown | different or failed | difference | 1 |

A forbidden step is still run, as a negative test. A failed job is only weak evidence there: quota or permissions can fail a job too.

### The Airflow mapping

`dit.orchestration.airflow_guidance` gives one line per declaration.

| Declaration | Airflow |
|---|---|
| Forbidden, or depends on the previous run | `depends_on_past=True` and `max_active_runs=1`. A backfill runs one interval after the other. |
| Allowed, and no dependency | A backfill may run several intervals at once. |
| Anything else | Treat as sequential until a test shows otherwise. |

### Slices

`dit.slices` is pure date arithmetic over an inclusive window.

| Spec (`--slices`) | Slices |
|---|---|
| `day` | One per day |
| `week` | Calendar weeks, Monday to Sunday, cut at the window's ends |
| `month` | Calendar months, cut at the window's ends |
| `<N>d` | Chunks of N days |
| A JSON list of `[first, last]` pairs | Exactly those, both days included |

`check_partition` checks that the slices cover the window exactly once. That is what slices loaded in parallel must satisfy to be comparable with one job.

One consumer today: mode `6_parallel` of `workflows/port_visits/ais.py`, for its first step. It has not been run.

---

## 12. Adding a pipeline or a workflow

The process and the checklist for a pipeline are in [`pipeline-contract.md`](pipeline-contract.md). Its adoption matrix shows where each pipeline stands. At framework level the steps are:

| Step | What to do |
|---|---|
| 1. Audit | Check the pipeline against the contract. Record gaps in the matrix. A workaround in the workflow for a missing contract item needs an entry in the Plan changelog. |
| 2. Read production | Find the DAG's tasks with `dit composer-dag <snapshot> --task-prefix <group>`. Mirror their order, commands and flags. |
| 3. Write the workflow | `workflows/<pkg>/<name>.py` with `main(argv) -> int`. Build each step's command line in a pure function. Use the `dit.workflow` helpers for arguments, the worker cap, sizing and the run context. |
| 4. Choose defaults | A source in the staging cohort, or a narrow window on a larger target. Dates inside the data that is really there: check partition metadata, not the cohort's name. Document new tables in the README. |
| 5. Decide the image policy | A published default with auto-build, or an overlay default with `check_images`. |
| 6. Choose the compare keys | From the output's schema, not from a flag. Keys are fixed in code: a wrong key on the command line could report IDENTICAL falsely. |
| 7. Add a drift spec | `workflows/<pkg>/composer_drift.py`, added to `workflows/composer_drift.py` ([`composer-drift.md`](composer-drift.md) § Adding a spec). |
| 8. Declare orchestration | `workflows/<pkg>/orchestration.py`. `unknown` and `untested` are valid entries. |
| 9. Test without the cloud | Pin the command lines, the wiring between steps and the argument checks. Load the workflow the way `dit run` does. |
| 10. Run small first | One mode, a short window, `--dry-run` where there is one. Never read exit code 0 as proof: a run on zero rows also exits 0. |
| 11. Update the docs | README Features and Roadmap, the per-pipeline doc, `CHANGELOG.md`, the Plan changelog. |

What a workflow should not do:

- import the pipeline's package or composer's code, unless the pipeline is driven in-process;
- build GCS paths, job names or worker options by hand;
- create a dataset;
- push an image outside `gcr.io/world-fishing-827/dit/`.

Shared code is extracted only when a third consumer needs the same thing. Until then workflows duplicate. What is planned for extraction: [`workflows/README.md`](../workflows/README.md).

---

## 13. What is not verified

This guide was written from the code. It was not checked by running anything. In particular:

- No Dataflow job, Cloud Build, docker call or BigQuery query was run for it.
- The per-workflow defaults in section 8 were read from constants and from the README they replace, not from a job.
- Run results quoted in other docs are those docs' statements.

Things the code itself says are unverified, and that matter when you rely on them:

- Per-side adjustments have not been used in a run. The table edits are tested with synthetic interfaces only.
- The parallel-slices mode has not been submitted. Every orchestration declaration is a claim.
- `dataflow.jobs.cancel` and table deletion by `cancel_run` under the Cloud Build identity are tested with mocks.
- The SIGTERM trap looks for jobs in one region only.

Per-workflow gaps are in each pipeline's page (§ "Known limits and open items" of [`pipe_segment`](../workflows/pipe_segment/README.md), [`port_visits`](../workflows/port_visits/README.md), [`encounters`](../workflows/encounters/README.md), [`pipe_gaps`](../workflows/pipe_gaps/README.md), [`pipe_events`](../workflows/pipe_events/README.md); index: [`workflows/README.md`](../workflows/README.md)) and in [`README.md`](../README.md) § Roadmap.

---

## 14. Module map

| Module | Role |
|---|---|
| `dit.cli` | `dit run`, `dit cache-cancel`, `dit composer-dag`; the SIGTERM trap |
| `dit.workflow` | Shared arguments, worker cap and sizing, experiment id, mode parsing, `resolve_run_context`, `run_with_cache` |
| `dit.runners.docker`, `dit.runners.dataflow` | The two runners |
| `dit.compare` | The shim over `table-check summary` |
| `dit.bq` | Pinning (copies, views), dropping tables by prefix, the legacy snapshot helpers |
| `dit.cache` | The `dit_runs` table: key, read, write, `cancel_run` |
| `dit.runstate` | Carries the active run id to the SIGTERM handler |
| `dit.snapshot`, `dit.git_info` | Commit resolution, snapshots, reading a file at a ref |
| `dit.worker_image` | The auto-built pipeline image |
| `dit.dataflow_paths`, `dit.job_names` | GCS paths and Dataflow job names |
| `dit.production` | Finds a production Dataflow job and its times |
| `dit.composer` | Reads composer's rendered DAG snapshots |
| `dit.drift` | The drift check: specs, comparison, reports |
| `dit.adjust` | Per-side adjustments |
| `dit.orchestration`, `dit.slices` | Orchestration declarations and date slices |
| `dit.dates` | A half-open day iterator |
