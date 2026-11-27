# `workflows/pipe_gaps/`

DIT's workflows for [pipe-gaps](https://github.com/GlobalFishingWatch/pipe-gaps), the AIS gap-detection pipeline. For the framework itself (runners, run cache, pinning, Cloud Build) see the top-level [`README.md`](../../README.md) and [`docs/architecture.md`](../../docs/architecture.md). The index of all workflows is [`workflows/README.md`](../README.md).

Workflow-author rules (the reprocess contract, what the pinned copies must hold) are in [`CLAUDE.md`](CLAUDE.md) in this directory. Read it before changing either workflow.

## What pipe-gaps does

pipe-gaps reads AIS position messages and the segments table, and writes one row per gap: a time between two consecutive messages of a vessel (`ssvid`) longer than `min_gap_length` hours. A gap with no closing message yet is an *open* gap. When later data closes it, a new row is appended. So the output table holds several versions of a gap, and a view keeps the latest state of each.

DIT drives one step: gap detection (`raw-gaps`). The later gap-events step (`gap-events`) is not driven.

## Versions

| | pipe-gaps version | Command | Output |
|---|---|---|---|
| DIT default worker image (since 2026-11-27) | v0.17.1 | `raw-gaps` | table `<out>_versioned` plus view `<out>` |
| Production, AIS v5 DAG (composer b5c3496e1, read 2026-11-27) | v0.17.1 | `raw-gaps` | as DIT |
| Production, AIS v3 DAG (composer, 2026-10-23) | v0.10.3 | `detect` | table `<out>` plus view `<out>_last_versions` |

DIT builds a v0.17.1 config **in-process** with the laptop's (or ditbox's) installed pipe-gaps package: `RawGapsConfig` and `RawGapsLinearDagFactory` from `pipe_gaps.pipelines.raw_gaps`. So the checkout must be v0.17.x (pipe-gaps' `pipelines/detect` package was renamed in v0.11.0, commit `38e6c14`). v0.10.0 support was dropped on 2026-11-27 (owner decision), with no switch: the two workflows no longer run a checkout older than v0.11.0.

One job is [`raw_gaps.py`](raw_gaps.py), shared by both workflows. What it fixes, each read at v0.17.1 with `git show`:

- **Dates.** `--start-date` inclusive, `--end-date` exclusive (gfw-common's `DatePipelineConfig`), the same half-open window as v0.10.0's `--date-range`. No job's window moved; a test pins the windows of every mode and stage.
- **Labels.** Required. DIT's run labels (`dit_run_id` and three more) go into `RawGapsConfig.labels`, which gfw-common puts on the Dataflow job and pipe-gaps on every BigQuery read job.
- **Output.** `--bq-out-gaps` names the latest-state view; rows are appended to `<view>_versioned`. The view is created after a job finishes. It keeps each gap's last version and drops closed gaps of `duration_h <= min_gap_length` (pipe-gaps' fix of the MAP-1676 class, fdc7336, first in v0.10.1). Both workflows compare the views; the run cache records the view and its table, so `make dit-cancel` drops both.
- **Open gaps between jobs.** DIT passes no `--bq-in-open-gaps`, so each job reads the still-open gaps from its own versioned table (`bq_in_open_gaps or bq_out_gaps_versioned`), from `--open-gaps-start-date` on, after its own pre-write delete. That is how the daily modes and the outage stages hand open gaps from one job to the next. The wiring is unchanged since v0.10.0, and so are the pre-write delete and the messages query. `gaps.sql.j2`, the query behind the side input and the view, was rewritten between v0.10.0 and v0.17.1: the start-date filter now comes before the dedup to each gap's last version; a gap counts as open only if it has no closed row (v0.10.0: if its last version was open); and with `min_gap_length` set (the view, not the side input) closed gaps of `duration_h <= min_gap_length` are dropped. A test pins the side input's query against the installed pipe-gaps; the hand-over on real data is verified only by the first live run.
- **Fixed values.** `--eval-last` (production's DAG makes it a required field), `--open-gaps-start-date 2012-01-01` (production's), `--versioned-suffix versioned` (production's), and `--good-seg-stabilization-days 0` (production passes 1; owner decision: the staging cohort does not change).
- **Temp dataset.** `dit.runners.dataflow` adds `temp_dataset` to the keyword arguments of every BigQuery read, through the factory's `read_from_bigquery_kwargs` property. Built offline at v0.17.1, both reads of the graph (messages, open gaps) carry it. Not yet run on Dataflow.
- **Beam.** v0.17.1 pins `apache-beam==2.76.0`, gfw-common 0.24.2 and Python 3.12; `PIPE_GAPS_BEAM_VERSION` in the `Makefile` is 2.76.0.

## The reprocess contract

Every pipe-gaps run first deletes the rows it is about to rewrite (`gaps_delete.sql.j2`):

```sql
DELETE FROM `<gaps table>`
WHERE (is_closed = TRUE  AND DATE(end_timestamp)   >= '<start_date>')
   OR (is_closed = FALSE AND DATE(start_timestamp) >= '<start_date>');
```

The delete has no upper bound. It is the same query at v0.10.0 and v0.17.1, and a test pins it against the installed pipe-gaps. A run over `[D, D+15]` deletes every gap from `D` on and re-emits only `[D, D+15]`. So a reprocess must always run to the most recent data day. This is by design. Both workflows obey it: every reprocess stage ends at `--end`. Rules and a worked example: [`CLAUDE.md`](CLAUDE.md).

## Workflows

| Workflow | Use it to | Jobs per run, at defaults |
|---|---|---|
| [`mode_equivalence.py`](mode_equivalence.py) | check that a backfill and the daily-incremental schedule produce the same gaps | 11 Dataflow jobs at defaults (3 modes, `--tail-days 4`) |
| [`outage_recovery.py`](outage_recovery.py) | check that an outage followed by a recovery backfill ends in the same state as a clean backfill | 4 Dataflow jobs at defaults |

Both run on Dataflow by default, submitted **in-process** from the Python process that runs the workflow (`dit.runners.dataflow`). `--runner docker` instead runs `pipe-gaps raw-gaps` locally with Beam's DirectRunner, in the `dev` service of the pipe-gaps checkout's docker compose file, built from that checkout.

Both have a run cache (`dit_runs`): a mode whose inputs, code and parameters are unchanged reuses the earlier output table. `--no-cache` recomputes. Both stamp the `dit_run_id` label on their Dataflow jobs, so `make dit-cancel RUN_ID=<id>` stops a run.

Neither has a cross-version wrapper. To compare two pipe-gaps refs, run a workflow once per ref with different `--worker-image` values (see [`docs/architecture.md`](../../docs/architecture.md) for the submitter and worker split).

### `mode_equivalence.py`

Runs the same detection several ways and compares the outputs pairwise. Dates are **half-open** `[--start, --end)`: the default `2020-01-01` to `2021-01-01` covers 2020.

| Mode | Jobs | What it models |
|---|---|---|
| `1_bf` | one job over `[start, end)` | a single backfill; the reference |
| `2_bfd` | one job over `[start, end - tail_days)`, then `tail_days` daily jobs, each over `[d - backfill_days, d)` for `d` = `end - tail_days + 1` ... `end` | a backfill followed by the daily DAG |
| `3_bftruncate` | one job over `[start, end)`, then the same daily jobs | the daily DAG re-running days already processed |
| `4_mutate_recover` | one job over `[start, mid)`, then the daily jobs restricted to some ssvids, then the daily jobs for all | data for some vessels arriving late |

- Defaults: `--tail-days 4`, `--backfill-days 4`, `--min-gap-length 1.0`, `--n-hours-before 12`, `--window-period-d 2`, good segments only.
- `--modes` selects among the first three (default: all three). A single mode runs and reports "no pair to compare".
- `4_mutate_recover` is off by default. Turn it on with `--enable-pipeline-4` plus either `--restricted-ssvids a,b,c` or `--auto-restrict`. It is compared with `1_bf` only, so `1_bf` must be selected. `--auto-restrict` reads `1_bf`'s output and picks about half of the vessels whose gaps cannot trigger the bug it targets (`dit.bq.query_for_restricted_ssvids`).
- `--parallel` runs the modes at the same time; each mode's own jobs stay in order.
- `--dry-run` logs every job of the selected modes (window, job name, output view) and, where pipe-gaps v0.17.x is installed, builds each job's config and DAG offline. It reads no git state, makes no cloud call and creates nothing.
- Each mode writes `three_way_<suffix>_<mode>` (the view) over `three_way_<suffix>_<mode>_versioned` (the rows).
- Exit code: 0 when every pair is identical, 1 when any pair differs.

### `outage_recovery.py`

Simulates production's daily DAG, an outage, and the recovery backfill an on-call engineer would launch, then compares the result with a clean single backfill (the oracle). All dates are **inclusive**.

| Stage | Range (defaults) | Reads |
|---|---|---|
| 1. Initial backfill | `[--start, --backfill-end]` = 2020-01-01 to 2020-12-28 | messages (filtered view with `--synthetic-outage`) |
| 2. Daily DAG resumes | `[--outage-end + 1, --end]` = 2020-12-30 to 2020-12-31; with `--synthetic-outage`, `[--outage-start - 1, --end]` | the same as stage 1 |
| 3. Recovery | `[--outage-start - --recovery-buffer-days, --end]` = 2020-12-29 to 2020-12-31 | unfiltered messages |
| Oracle | `[--start, --end]` | unfiltered messages |

- The three stages write to one versioned table; the oracle writes to its own. The two latest-state views are compared. The view hides sub-threshold closed gaps (pipe-gaps' MAP-1676 fix, above), so a comparison sees what consumers read: a regression of that filter shows, leftovers in the versioned table alone do not.
- Without `--synthetic-outage` the outage is only a gap in the schedule. On the frozen 2020 cohort the stages then read the full source, and the staged run matches the oracle by construction. **Use `--synthetic-outage` for a meaningful test.** It derives a view of the messages with the outage days removed; stages 1 and 2 read the view, stage 3 and the oracle read the unfiltered table.
- `--recovery-buffer-days 0` (the default) is the setting that discriminates. A buffer of 1 or more widens the recovery's delete enough to hide the bug class (MAP-1676) this workflow was built for.
- **Pinning.** The sources are copied at run start into `world-fishing-827.tech_great_expectations` (since 2026-11-19, PIPELINE-4705): the messages over `[--start - lookback, --end]` (4.41 GB on the default run, the whole staging table) and the segments whole (4.1 MB). `--reuse-pin <stamp>` reuses an earlier run's copies, `--no-pin` reads the live sources, `--dry-run` reports each copy's size, builds the four jobs' configs and DAGs offline where pipe-gaps v0.17.x is installed, and stops. Sources must be tables, not views. With `--synthetic-outage` the filtered view is derived over the pinned copy. The run cache is keyed on the copies' names, so a `--reuse-pin` rerun hits and a new pin misses. The flags of snapshot pinning (`--pin-at`, `--pin-source-at`, `--snapshot-dest-project`, `--snapshot-expiration-days`, `--skip-snapshots`, `--no-snapshot`) are rejected, each with what replaces it. The copies were checked with BigQuery dry runs; no copy has been created by this workflow and no pipeline has run on one.
- `--parallel` runs the oracle beside the three stages; the stages stay in order.
- Exit code: 0 when the staged view equals the oracle's, 1 otherwise.

## What a difference means

Compare keys: `(gap_id, start_timestamp)` on the latest-state view, for both workflows. With the staging default `--min-gap-length 1`, the view hides closed gaps of 1 hour or less, and the comparisons say nothing about gaps between 1 and 4 hours at production's threshold of 4 (the AIS v5 DAG's value). `gap_id` is a hash of `ssvid`, start timestamp, start latitude and start longitude (table description at v0.17.1).

| Pattern in the table-check summary | Likely cause |
|---|---|
| Symmetric: about as many keys only in A as only in B, plus `end_msgid` / `end_lat` / `end_lon` value differences | the known close-path non-determinism of pipe-gaps (which message closes a gap); not an outage or mode bug |
| Asymmetric: extra keys only in the staged run, and more rows than the oracle | sub-threshold gaps that survived a reprocess into the view (the MAP-1676 class; since v0.10.1 the view filters them, so this means the filter regressed) |
| Same `end_timestamp`, different `end_msgid` / coordinates | message-sort ties at slice boundaries (the v0.9.6 bug DIT caught in May 2026) |

Attribute differences through the table-check summary before calling a red run a regression. The bugs the mode-equivalence test caught first are described in [`docs/context.md`](../../docs/context.md).

## Source targets

Both default to the AIS staging cohort (2020 data, 142 vessels):

- messages: `world-fishing-827.pipe_ais_test_202408290000_internal.messages_positions`
- segments: `world-fishing-827.pipe_ais_test_202408290000_published.segs_activity`

`--source-messages` and `--source-segments` point either workflow elsewhere. For VMS, `outage_recovery.py` documents `gfw-int-vms-v3.pipe_vms_v3_internal.research_messages` and `.segs_activity`. A copy only reads its source, so a source in another organisation needs no extra flag. Targets and their sizes: [`README.md`](../../README.md) § "Source data targets".

## Running

From a pipe-gaps checkout at **v0.17.x** (see above), with `$DIT` the path of this repository.

Cloud Build (recommended; it pins Beam to 2.76.0 to match the worker image, and uploads the checkout at `$PROJECTS/pipe-gaps`):

```bash
make -C $DIT dit-cloud PIPELINE=pipe-gaps \
    WORKFLOW=workflows/pipe_gaps/mode_equivalence.py \
    ARGS="--runner dataflow --parallel"

make -C $DIT dit-cloud PIPELINE=pipe-gaps \
    WORKFLOW=workflows/pipe_gaps/outage_recovery.py \
    ARGS="--runner dataflow --synthetic-outage --parallel"
```

Laptop (the venv must hold pipe-gaps v0.17.x and Beam 2.76.0, Python 3.12; `make install-pipe-gaps` pins Beam):

```bash
make -C $DIT install-pipe-gaps
cd $PROJECTS/pipe-gaps
dit run $DIT/workflows/pipe_gaps/mode_equivalence.py --dry-run
dit run $DIT/workflows/pipe_gaps/mode_equivalence.py --runner dataflow --modes 1_bf
dit run $DIT/workflows/pipe_gaps/outage_recovery.py --synthetic-outage --dry-run
```

Useful flags: `--experiment-id` (groups runs), `--worker-image` (a worker image built from your change; workers cannot see the laptop's tree), `--max-num-workers` (default 50), `--worker-machine-type` / `--disk-size-gb` (default e2-custom-2-3072 / 25 GB, production's daily raw-gaps task; on the staging cohort a smaller machine is cheaper), `--skip-pipelines` / `--skip-comparisons`.

## Orchestration and drift

- **Orchestration** ([`orchestration.py`](orchestration.py)): step `detect` (DIT's step id, kept from v0.10.0; command `raw-gaps`), parallel slices forbidden, depends on the previous run. Status `claimed` (owner, 2026-11-24, "probably"). Nothing tests it.
- **Drift** ([`composer_drift.py`](composer_drift.py)): `pipe_gaps/mode_equivalence` against the `gaps.detect_raw_gaps` task of `core_ais_v5`. **In sync** since 2026-11-27 (composer b5c3496e1): version, command and every compared flag match. It compares the docker runner's argv of one daily job, since the Dataflow runner has no command line. Accepted differences, each with its reason in the spec: the staging inputs (`--bq-in-messages`, `--bq-in-segments`); `--min-gap-length` 1 against 4 and `--good-seg-stabilization-days` 0 against 1 (owner decisions, 2026-11-27); `--start-date` (a DIT daily job reads back `--backfill-days`, 4, where production's covers 2 days); `--window-period-d` (DIT passes 2; production passes none, which on its 2-day daily job is 2 as well); `--no-rich-logging` and `--wait_for_job` (no effect on output); the Dataflow options. `outage_recovery.py` runs the same job and has no spec.

## Run history

| Date | Run | Result | Build |
|---|---|---|---|
| 2026-05-22 | `mode_equivalence.py`, staging, worker image v0.9.6 | 320 / 307 / 331 differing rows in the three pairs: message-sort ties | not recorded |
| 2026-05-22 | same, worker image built from the tie-break fix | 0 / 0 / 0, about 41 minutes | `df159e3f-042d-474c-9bb7-b1fef6069f67` |
| 2026-05-29 | first auto-built worker images: `main` against the fix | 350 / 317 / 347 against 0 / 0 / 0 | not recorded |
| 2026-06-04 | `outage_recovery.py` smoke | ran on zero rows: the default dates were outside the 2020 data; fixed | not recorded |
| 2026-06-10 | `outage_recovery.py --synthetic-outage`, four runs (issue #59) | outage class RED on pipe-gaps `6cd6706` (58 sub-threshold gaps), GREEN on `81118be` and later; the close-path non-determinism remains in all runs | not recorded |
| 2026-11-19 | pin by copy for `outage_recovery.py` | BigQuery dry runs only | none |
| 2026-11-27 | port to v0.17.1: both `--dry-run`s, and the offline tests against pipe-gaps v0.17.1 in a throwaway venv | every job's config and DAG built; both BigQuery reads carry the temp dataset and labels; the copies' dry runs estimate 4.41 GB and 4 MB. Nothing run on Dataflow | none |

No run on the current default worker image (v0.17.1) is recorded in this repository, and no live run since pinning moved to copies.

## Known limits and open items

- **Not yet run on v0.17.1.** The port is read and built offline. The temp-dataset hook, the open-gap hand-over between jobs and the view comparison need one live run (the smallest is in the PR).
- **A reviewed checkout runs the default worker image.** When the checkout's commit is merged into pipe-gaps `origin/main` and `--worker-image` is not given, the Dataflow workers run v0.17.1, whatever the commit (read from `dit.worker_image.ensure_pipeline_image`). Only unreviewed code gets an auto-built worker image. Pass `--worker-image` to run another reviewed version on the workers.
- **No guard for the reprocess contract.** Nothing rejects a stage that would stop before `--end`; review enforces it ([`CLAUDE.md`](CLAUDE.md)).
- **One job, two workflows.** Since 2026-11-27 both run one job through [`raw_gaps.py`](raw_gaps.py). The two copies it replaced had not drifted (only a log line and an error message differed).
- **Different date conventions.** `mode_equivalence.py` is half-open, `outage_recovery.py` inclusive.
- **Synthetic outage filters messages only.** The segments input is not filtered.
- **The close-path non-determinism** makes `outage_recovery.py` red even when the outage class is fixed; the comparison does not tolerate it.
- **No parallel-slices mode** and no test of the orchestration declaration.
- **Editing either workflow file or `raw_gaps.py`** changes the cache key, so every cached row misses once. The port did that.
