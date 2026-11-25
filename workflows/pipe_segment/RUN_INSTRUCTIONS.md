# identity_match_key workflow — setup and run instructions

This file covers both pipe-segment workflows. Most of it is about
`identity_match_key.py`, the cross-version A/B. The mode-equivalence workflow
of the `segment` step, `ais.py`, has its own part at the end:
["Mode equivalence of the segment step"](#mode-equivalence-of-the-segment-step-aispy).
"No satellite timing offsets in DIT" below holds for both.

End-to-end steps to A/B-test the gpsdio-segment identity-match-key change
(reducing `match_key = (transponder_type, receiver_type, source)` to
`(transponder_type,)`) via this workflow.

**Status, 2026-11-20.** The workflow's command lines match what the AIS v3
DAG's segment group is configured for (pipe-segment v5.1.0, six tasks; see
[`composer_drift.py`](composer_drift.py)), with one accepted difference: the
DAG's `segment` task passes `--incremental` and this workflow does not (below).
That was established by reading code and the DAG's rendered snapshot. The only
recorded run is one laptop smoke of the `segment` step on 2026-06-03, on
v5.0.3. Two things still stand between this file and a full run:

- **Dataflow as the test service account.** pipe-segment has no way to name
  the dataset Beam stages query results in, and `automated-testing@` may not
  create one. Inferred from the code; not tried. Since 2026-11-20 the default
  image is DIT's overlay of v5.1.0
  ([`docker/overlays/pipe-segment-temp-dataset/`](../../docker/overlays/pipe-segment-temp-dataset/README.md)),
  and the workflow hands it the dataset (`--bq-temp-dataset`). The image is
  built and pushed (2026-11-24), and `ais.py`'s Dataflow jobs have run on it.
  An unreviewed binding on the default image is refused before anything is
  pinned (section 4).
- **Nothing here has run end to end.** Treat the expected results below as
  expectations.

**The version moved on 2026-11-20.** Until that day the AIS DAG pinned v5.0.3
and passed no `--incremental`. composer-dags-production PR #3145 changed both,
and DIT's drift check reported it the same evening. The VMS v5 DAG still pins
v5.0.3. Examples below use `v5.1.0`; an experiment branch should fork from it.

**No `--incremental` in this workflow.** Every binding runs the full-history
segmenter:

- A binding may be a ref older than v5.1.0. Such code hands the unknown flag
  to Beam instead of rejecting it (read, not tried), so the two sides of the
  A/B would run different segmenters.
- A binding is one `segment` job from empty tables, so the state the flag
  reads is empty either way.
- Whether the two segmenters agree is what `ais.py` tests, and no run has
  confirmed it yet.

The drift spec accepts the DAG's flag for the `segment` step, with this
reason.

Source pinning no longer stands in the way. The workflow pinned its source
with a BigQuery snapshot, which no DIT identity can create in
`tech_great_expectations`. Since 2026-11-19 it pins by copy
([PIPELINE-4705](https://globalfishingwatch.atlassian.net/browse/PIPELINE-4705));
see section 5. The copy statement was accepted by BigQuery dry runs. No copy
has been created yet.

## No satellite timing offsets in DIT

**DIT's segmenter workflows do not run pipe-segment's satellite timing
offsets** (owner decision, 2026-11-20). Production's `segment` task works
out, per receiver and hour, how far a satellite's clock is off, and drops the
messages of the hours above the threshold. A DIT run does neither. It passes
none of the four satellite flags and writes no offsets table. The
`--include-satellite-offsets` flag is removed; passing it stops the run at
argument parsing.

**Why** (measured or read on 2026-11-20):

- **The offsets must be computed from the full message feed, not from a test
  cohort.** The staging cohort's own offsets table
  (`pipe_ais_test_202408290000_published.satellite_timing_offsets`) was
  computed from the full satellite feed when the cohort was built; its table
  description names that feed as the source. Computed from the cohort's 142
  vessels, the offsets come out different and noisy. For January 2020: 990
  receiver-hours above the segmenter's 30 s threshold, against 183 in the
  cohort's offsets table, with 39 in common. A run on such offsets would drop
  the wrong messages.
- **The full feed is too large to read on every test run.** The 2020 messages
  are about 4.1 TB.
- **pipe-segment has no option to read precomputed offsets.** `segment`
  computes them from three inputs and uses them in the same run (`pipeline.py`
  lines 195 to 222 at v5.0.3).
- Also true, and secondary: the satellite positions are date-sharded tables in
  another organisation (366 tables and 139 GB for 2020), and the offsets query
  reads its message source through a table wildcard that does not match a
  partitioned table.

**What follows from it:**

- On the 2020 staging cohort, DIT's `messages_segmented` keeps the messages
  that the correction would drop. The cohort's offsets table lists 1,361 such
  receiver-hours in 2020, on 27 receivers and 173 days.
- For those hours, DIT's staging output is **not comparable with production's
  historical output**.
- Comparisons **between two DIT runs** (modes, or base against head) are not
  affected. Both sides run without the correction.
- The correction only ever applied to data that carries satellite receiver
  IDs. The current feed has none (since 2026-04-22), so production computes no
  offsets for current data either.

**What would change this:** an option in pipe-segment to read an offsets table
instead of computing one. DIT could then pass the cohort's existing offsets
table. That is the path, not planned work.

Production's task still passes the four flags. The drift check accepts that
difference for the `segment` step, with this reason
([`composer_drift.py`](composer_drift.py)).

## 1. Install dit

```bash
cd ~/git/data_integration_tests
make install
```

That is enough to run the workflow: it runs pipe-segment in its container and
never imports it. `make install-pipe-segment` adds an editable install of
`$(PROJECTS)/pipe-segment` (a sibling checkout by default; override via
`PROJECTS=…` or `.envrc`) for stepping through its source. pipe-segment v5.1.0
needs Python 3.12 or newer; the target pins apache-beam 2.71.0, the version
v5.1.0 pins.

For the laptop docker path, the auth named volume needs to be populated **once** before any docker run:

```bash
docker volume create gcp
```

Then log in through a `gcloud` compose service that mounts that volume.
pipe-segment v5.1.0's `docker-compose.yaml` has none (its services are `dev`,
`test` and `bigquery`), so use another pipeline checkout that has one, for
example:

```bash
docker compose -f ~/git/anchorages_pipeline/docker-compose.yaml run --rm gcloud auth application-default login
```

In Cloud Build, `DIT_CLOUD_MODE=1` is set and the docker runner adds
`--network=cloudbuild` instead of mounting `gcp:/root/.config` — no laptop
ADC volume needed.

## 2. Fork gpsdio-segment

```bash
cd ~/git/gpsdio-segment
git fetch --tags origin
git checkout -b experiment/identity-match-key-transponder-only v3.0.0
```

Edit `gpsdio_segment/msg_processor.py`, three sites:

- **Line 196** (in `_store_info`):
  ```python
  # before
  match_key = (transponder_type, receiver_type, source)
  # after
  match_key = (transponder_type,)
  ```

- **Line 233** (in `add_info_to_msg`, identities branch): same change.
- **Line 241** (in `add_info_to_msg`, destinations branch): same change.

Commit and push:

```bash
git add gpsdio_segment/msg_processor.py
git commit -m "experiment: reduce identity match_key to (transponder_type,) only"
git push -u origin experiment/identity-match-key-transponder-only
```

Record the resulting SHA — call it `<GPSDIO_FORK_SHA>`. You'll need it for step 3.

## 3. Fork pipe-segment

```bash
cd ~/git/pipe-segment
git fetch --tags origin
git checkout -b experiment/identity-match-key-transponder-only v5.1.0
```

At v5.1.0 (as since v5.0.3) the dependencies live in `pyproject.toml`; `requirements.txt` is
compiled from it and is what the image installs first. Repin gpsdio-segment in
both.

### `pyproject.toml` (`[project] dependencies`)

```diff
-    "gpsdio-segment @ git+https://github.com/GlobalFishingWatch/gpsdio-segment.git@v3.0.0",
+    "gpsdio-segment @ git+https://github.com/GlobalFishingWatch/gpsdio-segment.git@<GPSDIO_FORK_SHA>",
```

### Regenerate `requirements.txt`

```bash
make reqs    # pip-compile in the dev container
grep gpsdio-segment requirements.txt
# should show: gpsdio-segment @ git+https://github.com/GlobalFishingWatch/gpsdio-segment.git@<GPSDIO_FORK_SHA>
```

Commit and push:

```bash
git add pyproject.toml requirements.txt
git commit -m "experiment: repin gpsdio-segment to identity-match-key fork"
git push -u origin experiment/identity-match-key-transponder-only
```

The workflow reads each binding's pin from `pyproject.toml` (older tags:
`requirements/prod.in`) and refuses a two-binding run whose bindings pin the
same gpsdio-segment.

## 4. Image resolution

The workflow resolves a per-binding image via `dit.workflow.resolve_run_context`:

- **"before=v5.1.0"** binding: reviewed code at the default tag → the docker
  runner pulls the default image, DIT's overlay of v5.1.0
  (`gcr.io/world-fishing-827/dit/pipe-segment:v5.1.0-4ddde03-temp-dataset-04b40b54`;
  built 2026-11-24). It holds v5.1.0 whatever the reviewed ref is.
- **"after=experiment/…"** binding: unreviewed code (not merged into
  `origin/main`) on the default image → **refused** before anything is
  pinned (since 2026-11-25). The workflow used to auto-build a plain image of
  the commit, which has no overlay, and only warned. Nothing is auto-built
  now. Ways out, named in the message:
  - an overlay image of the commit
    (`docker/overlays/pipe-segment-temp-dataset/build.sh`) as `--image-tag`;
  - `--runner DirectRunner --build-from-source`: the docker runner builds
    each binding's worktree, no image and no Dataflow.
- **Explicit override**: `--image-tag <FQN>` is respected for all bindings.
  There is no per-binding image flag, so an A/B of two overlay images on
  Dataflow is not possible yet.
- **Laptop fast-iter**: `--build-from-source` bypasses both the default
  image and the kaniko build — compose builds the `dev` service from the
  worktree's working tree.

Every step's container gets `DIT_BQ_TEMP_DATASET` (from `--bq-temp-dataset`,
default `world-fishing-827.tech_great_expectations`). An overlay image stages
its BigQuery reads there; any other image ignores the variable.

For Dataflow, the same image is passed to `--sdk_container_image` so workers
run the same per-binding code. No `--setup_file` is passed, as in production:
these images hold the installed package and no source tree. The one exception
is `--build-from-source` on Dataflow, where the worktree is mounted and
`--setup_file=./setup.py` ships its package to the workers.

## 5. What a binding runs

Six pipe-segment commands, in production's order. Each reads what the one
before it wrote; the first failure ends the binding.

| Step | Kind | Reads | Writes |
|---|---|---|---|
| `segment` | Beam | normalized messages | `messages_segmented`, `segments`, `fragments` |
| `segment_identity` | Beam | `segments`, `fragments` | `segment_identity_daily` |
| `segment_vessel_daily` | BigQuery SQL | `segment_identity_daily` | `segment_vessel_daily` |
| `segment_info` | BigQuery SQL | `segment_identity_daily`, `segment_vessel_daily` | `segment_info` |
| `vessel_info` | BigQuery SQL | `segment_identity_daily`, `segment_vessel_daily` | `vessel_info` |
| `segment_vessel` | BigQuery SQL | `segment_vessel_daily` | `segment_vessel` |

- `--runner` (and `--max-num-workers`, `--worker-machine-type`,
  `--disk-size-gb`) apply to the two Beam steps only. The four SQL steps are
  queries run by the container and behave the same on every runner.
- `--skip-downstream` runs and diffs `segment` only.
- Output tables are named `<table>_<experiment id>_<binding name>` in
  `--dest-dataset` (default `tech_great_expectations`).
- `--date-range` is inclusive on both ends.

**The pinned source.** Before any binding runs, the workflow copies the
normalized messages of the run's `--date-range` into
`world-fishing-827.tech_great_expectations`. Every binding's `segment` reads
that copy, so a difference between bindings comes from their code.

- The copy holds exactly the run's days. `segment` reads its source by date
  between the two ends of `--date-range`, with no lookback, and no other step
  reads it (pipe-segment v5.0.3).
- Size on the staging table: 10.7 MB for one day, 363 MB for January 2020,
  3.78 GB for all of 2020 (dry runs, 2026-11-19).
- The run logs a pin stamp, e.g. `20261119t101500z`. It ends the copy's name:
  `dit_exp_<experiment id>_pipe_segment_normalized_messages_<start>_<end>_<stamp>`.
- `--reuse-pin <stamp>` reads an earlier run's copy again, e.g. to retry after
  a failed binding. It needs the same `--experiment-id`, `--date-range` and
  source table, and fails if the copy is gone.
- `--pin-max-gb` (default 1000) refuses a copy that would process more.
  `--pin-expiration-days` (default 7) sets when the copy expires.
- `--dry-run` only dry-runs the copy, reports its bytes against the cap and
  skips the pipe-segment steps and the diff. It still creates the worktrees.
- `--source-normalized-table` must be one table, partitioned on `timestamp`:
  not a view and not date-sharded tables.
- Removed with the snapshots: `--pin-source-at`, `--snapshot-dest-project`,
  `--snapshot-expiration-days`. Each is rejected with what replaces it.

**Not production's state.** A binding starts from empty tables. `segment` has
no fragment history to stitch onto, and `segment_vessel_daily` sees only the
run's own days in its 30-day window. Both are the same for every binding, so
the A/B is fair; the tables are not comparable with production's.

**No satellite timing offsets.** `segment` here passes none of production's
four satellite flags, so it computes no offsets and drops no message for
them. See "No satellite timing offsets in DIT" at the top.

## 6. DirectRunner smoke (laptop, fast)

Single-binding orchestration check against `v5.1.0`, 1 day, a handful of
vessels, source = staging cohort. Start with the `segment` step:

```bash
cd ~/git/data_integration_tests
dit run workflows/pipe_segment/identity_match_key.py \
    --experiment-id pipe-segment-smoke \
    --binding before=v5.1.0 \
    --date-range 2020-01-01,2020-01-01 \
    --runner DirectRunner \
    --ssvid-filter-query '"<ssvid>","<ssvid>"' \
    --skip-downstream \
    --build-from-source
```

Then drop `--skip-downstream` to run all six steps, and drop
`--build-from-source` to run the default image, once it is built.

Expected:

- `worktree … @ v5.1.0` log line per binding.
- `gpsdio-segment pin -> …v3.0.0` log line per binding.
- `commit=… image=…` log line per binding.
- One `invoking pipe-segment <step>` log line per step, six without
  `--skip-downstream`.
- Diff phase no-ops on a single binding (`itertools.combinations` is empty).
- Output tables in `tech_great_expectations`:
  `messages_segmented_pipe_segment_smoke_before`,
  `segments_pipe_segment_smoke_before`, etc.

## 7. Cloud-build smoke (one-binding, Dataflow)

Runs on the default overlay image (built 2026-11-24). This workflow has not
run on it yet.

```bash
make dit-cloud PIPELINE=pipe-segment \
    WORKFLOW=workflows/pipe_segment/identity_match_key.py \
    ARGS="--experiment-id pipe-segment-cloud-smoke \
          --binding before=v5.1.0 \
          --date-range 2020-01-01,2020-01-01 \
          --runner dataflow \
          --skip-downstream"
```

Cloud-mode auth is automatic (`--network=cloudbuild`).
Reviewed v5.1.0 → the docker runner pulls the default overlay image.

## 8. Full Dataflow A/B (after experiment branches are pushed)

```bash
make dit-cloud PIPELINE=pipe-segment \
    WORKFLOW=workflows/pipe_segment/identity_match_key.py \
    ARGS="--experiment-id idmatchkey \
          --binding before=v5.1.0 \
          --binding after=experiment/identity-match-key-transponder-only \
          --date-range 2020-01-01,2020-01-03 \
          --runner dataflow"
```

**On Dataflow this command is refused** before anything is pinned: the
"after" binding is unreviewed and would run on the default image (section 4).
One `--image-tag` for both bindings would run the same code on both sides, so
an A/B on Dataflow needs an image per binding, which the workflow cannot take
yet. Run it with `--runner DirectRunner --build-from-source` instead, which
builds each binding's worktree. Expected results:

- `messages_segmented`: **DIFFERENT** (per-row identity/destinations dict
  contents differ — that's the metric).
- `segments` / `fragments`: identical or very nearly so (segmentation is
  driven by position kinematics, not identity matching).
- `segment_identity_daily`, `segment_vessel_daily`, `segment_info`: most
  informative — they aggregate per-segment identity assignments, and that's
  where the fix most matters. A changed identity shows as a field difference,
  in `segment_vessel_daily` and `segment_info` also in `vessel_id`.
- `vessel_info`, `segment_vessel`: `vessel_id` is part of their key and is a
  hash of the ssvid and the vessel's most common identity values. A changed
  identity therefore shows as one row only in "before" and one only in
  "after", not as a field difference. Read those counts as changed vessels,
  not as missing data.

Compare keys per table (`COMPARE_KEYS` in the workflow):

| Table | Key |
|---|---|
| `messages_segmented` | `msgid` |
| `segments` | `seg_id`, `frag_id` |
| `fragments` | `frag_id` |
| `segment_identity_daily` | `seg_id`, `first_timestamp` |
| `segment_vessel_daily` | `seg_id`, `ssvid`, `day` |
| `segment_info` | `seg_id` |
| `vessel_info` | `vessel_id`, `ssvid` |
| `segment_vessel` | `seg_id`, `ssvid`, `vessel_id` |

## 9. Cleanup

This workflow does not use the run cache and does not stamp the `dit_run_id`
label, so `make dit-cancel` finds nothing for it.

- Output tables: every table name ends in `_<experiment id>_<binding name>`,
  and each carries the labels `resource_creator=dit` and
  `experiment_id=<experiment id>`. They have no expiration; drop them by hand.
- Dataflow jobs: named `dit-pipe-segment-<step>-<experiment id>-<binding name>`.
  Cancel a running one in the Dataflow console or with
  `gcloud dataflow jobs cancel`.
- The pinned copy of the source expires after 7 days (configurable via
  `--pin-expiration-days`). It carries the labels `dit_artifact=source_pin`
  and `dit_experiment_id=<experiment id>`.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `gpsdio-segment pin -> …` identical across bindings | The "after" branch wasn't pushed, or the worktree didn't pick it up. Re-fetch the pipe-segment refs (`git -C ~/git/pipe-segment fetch origin`). |
| All output-table diffs IDENTICAL | `--image-tag` was set to a single image, so both bindings ran the same code. |
| `binding '…' is unreviewed …` | The binding's ref is not merged into `origin/main` and would run on the default image (section 4). Pass an overlay image of the commit, or use `--runner DirectRunner --build-from-source`. |
| `rows_only_in_a` / `rows_only_in_b` non-zero on `messages_segmented` | The bindings did not read the same copy, e.g. they ran in two invocations without `--reuse-pin`. Re-run them in one invocation, or the second with the first's `--reuse-pin <stamp>`. |
| `rows_only_in_a` / `rows_only_in_b` non-zero on `vessel_info` or `segment_vessel` | Expected when identities change: `vessel_id` is in the key (section 8). |
| A step fails with `TypeError: 'NoneType' object is not iterable` in `BigQueryHelper` | The step got no `--labels`. The workflow passes them on every step; check for a hand-built command line. |
| `--binding name … is too long for the Dataflow job name` | On Dataflow a binding name may be at most 27 characters. Shorten it. |
| `pinning … would process … GB, above the … GB cap` | The copy of the run's dates is larger than `--pin-max-gb`. Narrow `--date-range`, or raise the cap on purpose. |
| `pinned copy … does not exist` with `--reuse-pin` | The copy expired, or the stamp, `--experiment-id`, `--date-range` or source table differ from the run that made it. |
| The copy is denied `bigquery.tables.create` | The identity may not create tables in `tech_great_expectations`. With gcloud impersonating `dq-monitoring@`, that is the case. |
| A Dataflow job fails creating `beam_temp_dataset_…` | The image has no overlay: an explicit `--image-tag` of a plain build, or `--build-from-source` (section 4). The test service account may not create datasets. |
| `--include-satellite-offsets was removed` | DIT runs no satellite timing offsets ("No satellite timing offsets in DIT" at the top). Drop the flag. |
| DIT's `messages_segmented` has more rows than production's for some hours of 2020 | Expected: production dropped those hours' messages for satellite clock offsets, and DIT does not. Same section. |
| `… was removed with snapshot pinning` | `--pin-source-at`, `--snapshot-dest-project` or `--snapshot-expiration-days` was passed. The message names what to use instead. |

# Mode equivalence of the segment step (`ais.py`)

`workflows/pipe_segment/ais.py` runs pipe-segment's `segment` step over one
window in several shapes and checks that every shape writes the same
`segments`, `messages_segmented` and `fragments` tables.

**Status, 2026-11-25: run in Cloud Build on both overlay images** (built
2026-11-24). Measured results:

| Run | Cloud Build | What ran | Result |
|---|---|---|---|
| Smoke, default run | `2e57a93b` | 4 variants, 10 Dataflow jobs, 9 comparisons | All IDENTICAL. Rows per variant: `segments` 5,612, `messages_segmented` 2,068,881, `fragments` 5,612 |
| PR #227 with the v5.0.3 base (M6) | `4bb84ec0` | 6 variants, 45 jobs, 15 comparisons with `1_bf:full-history@base` | All IDENTICAL; the same row counts in all 6 variants |
| Short jobs on DirectRunner | `967b1030` | 3 one-day jobs on DirectRunner (about 1.5 min each, against about 6 on Dataflow) | IDENTICAL to a Dataflow reference |
| Production input, 2012-12-01..10, v5.0.3 against v5.1.0 | `56af531e` | 10 jobs, production machine type, worker cap 4, about 1 h 50 min | `segments` and `fragments` (1,031,761 rows) IDENTICAL. `messages_segmented` (81,357,979 rows) reported 2 differences, all on one msgid that two vessels share, the same two rows in both versions: a pairing bug in table-check, fixed in v0.3.1 |
| Compare-only rerun of `56af531e` with table-check v0.3.1 | `de4e5831` | 9 comparisons | All IDENTICAL |

The argv, plan and refusals are also covered by tests with every cloud call
mocked.

## M1. The unit: a (mode, segmenter) variant

A **mode** is how the window is cut into jobs (`--start`, `--end`, both
inclusive; `--tail-days`, default 3):

| Mode | Jobs | On the default window (January 2020) |
|---|---|---|
| `1_bf` | one job over the window | 1 |
| `2_bfd` | one job up to `--tail-days` before the end, then one job per day | 4 |
| `3_bftruncate` | one job over the window, then the last `--tail-days` days rerun one by one, in order | 4 |
| `4_daily` | every day as its own job, in order. Not in the default `--modes` | 31 |

A **segmenter** is which of pipe-segment's two segmenters each job runs
(`--segmenter`, default `incremental`):

- `incremental`: pipe-segment's `--incremental` (v5.1.0, pipe-segment PR #227).
  Each job starts from the segments of the day before its date range. The
  default, because the AIS v3 DAG's `segment` task passes it since 2026-11-20
  (composer-dags-production PR #3145). That is the DAG's configuration; whether
  a production run has executed with it was not checked.
- `full-history`: pipe-segment's own default. Each job reads every fragment in
  its fragments table and matches them all again. What the AIS DAG passed
  until 2026-11-20, and what the VMS v5 DAG, on v5.0.3, still does.

A `--modes` entry is a variant:

- a bare mode (`2_bfd`) runs with every segmenter of `--segmenter`;
- `mode:segmenter` (`1_bf:full-history`) runs exactly that pair.

So `--segmenter full-history,incremental` runs the cross product, and a list of
qualified entries runs only the pairs named. The first variant in canonical
order (by mode, then segmenter) is the reference; `1_bf:full-history` when it
is selected.

**The default run** (no `--modes`, no `--segmenter`) is
`1_bf:full-history,1_bf,2_bfd,3_bftruncate`:

| Variant | Jobs on January 2020 | Role |
|---|---|---|
| `1_bf:full-history` | 1 | the reference: every segment matched from the window's fragments in one pass, no state handed from job to job |
| `1_bf:incremental` | 1 | the incremental matching without any hand-over between jobs |
| `2_bfd:incremental` | 4 | a backfill, then three daily runs: the shape the DAG now runs every day |
| `3_bftruncate:incremental` | 4 | three days rerun in order on top of a full backfill |

Ten jobs, six pairs, eighteen comparisons, 363 MB pinned. A run of
incremental shapes alone would only show that incremental agrees with itself,
so the reference stays full history. `1_bf:incremental` tells a difference in
the matching from a difference in the hand-over.

`4_daily` is a mode and not `--tail-days` set to the window's length, because
`--tail-days` applies to every mode of a run, and one run must be able to hold
"backfill, then three days" next to "every day".

## M2. What the workflow guarantees

- **Each variant has its own three tables**, named
  `<table>_<suffix>_<variant>` in `--dest-dataset`, e.g.
  `segments_<suffix>_2_bfd_inc`. The suffix is
  `<experiment id>_<commit>_<random>` unless `--suffix` is given.
- **They start empty.** A run refuses to start if one of them already holds
  rows. A job reads its own fragments and segments tables as its history; a
  table that already holds the window is read twice. That gave doubled message
  counts in the PR author's own test.
- **Jobs of a variant are consecutive.** Each starts no later than the day
  after the last day written, and the last one ends on the window's last day.
  An incremental job needs the segments of the day before its range.
- **Reruns go in order, through the end.** A job rewrites only its own date
  range.
- **Every day of the window must have messages.** An incremental job fails on
  a day without them. The default window's 31 days each have 66,000 to 90,000
  rows in the staging table (partition metadata, 2026-11-20).
- **The source is pinned by copy** over the window before any job runs, as in
  `identity_match_key.py` (section 5 above): `--reuse-pin`, `--pin-max-gb`,
  `--pin-expiration-days`. January 2020 is 363 MB (dry run, 2026-11-20).
- **No satellite timing offsets.** No job passes a satellite flag, and there
  is no option for it.

## M3. Images

- The default `--image-tag` and `--worker-image` are DIT's overlay of
  pipe-segment v5.1.0, the version the AIS DAG is configured for
  ([overlay README](../../docker/overlays/pipe-segment-temp-dataset/README.md)).
  Built and pushed on 2026-11-24, like the v5.0.3 overlay.
- **`incremental` needs an image of v5.1.0 or later**, which the default image
  is. On older code pipe-segment hands `--incremental` to Beam, which ignores
  it (read from `cli/main.py` and `pipeline.py`; not tried), so the job would
  run full history and the comparison would pass without testing anything. The
  workflow refuses before any cloud call. It reads the version from the start
  of the image tag (`v5.1.0-…`); an image whose tag names no version needs
  `--assume-incremental-support`.
- **To run an earlier version next to the head**, end a `--modes` entry in
  `@base` and pass that version's overlay image as `--base-image`. The entry
  runs on that image as submitter and worker; the others run on the images
  above. See M6 for the PR #227 command.
  - Both sides read the same pinned copy and get the same flags. Nothing is
    added or removed for the older version, so a base whose command line
    differs in more than the three `incremental` flags cannot run this way
    yet.
  - There is no default base image. `@base` without `--base-image`,
    `--base-image` without an `@base` entry, and `@base` with
    `--build-from-source` are refused at parse time.
  - `incremental` is checked per side: a base older than v5.1.0 must name full
    history in every `@base` entry.
  - A base variant sorts first within its mode, so `1_bf:full-history@base`
    is the reference when selected.
- **To run the version the VMS DAG pins on its own**, pass the v5.0.3 overlay image as
  both flags and select full history only:
  `--segmenter full-history --modes 1_bf,2_bfd,3_bftruncate`. With an
  incremental variant left in the selection the run is refused.
- Unreviewed pipe-segment code (a dirty tree, a DIT snapshot, a commit not
  merged into `origin/main`) is refused on a default image. Build an overlay
  image of the commit and pass it as both flags. DIT never auto-builds one.
- `--runner docker --build-from-source` runs the working tree locally with
  DirectRunner and needs no image.

### Short jobs on DirectRunner

A one-day job of the staging cohort spends about four of its six minutes on
Dataflow starting and stopping workers (measured on build `4bb84ec0`,
2026-11-24: about 2 min start-up, 1 min work, 2 min 10 s shutdown).
`--direct-runner-max-days 1` runs every one-day job on DirectRunner inside
the container and keeps longer jobs on Dataflow:

```bash
ARGS="--experiment-id pr227 --modes ...,4_daily:incremental --direct-runner-max-days 1 ..."
```

- Set by hand. Use it for small inputs (the staging cohort) where the
  overhead dwarfs the work, not for production-sized days.
- The comparison checks that DirectRunner gives what Dataflow gives: a
  variant whose daily jobs ran locally is compared with a reference on
  Dataflow.
- `--dry-run` says which runner each job uses and how many run where.
- Ran inside Cloud Build on 2026-11-24 (build `967b1030`): three one-day jobs on DirectRunner, about 1.5 min each, IDENTICAL to a Dataflow reference on all three tables. Not tested: memory on the build machine (E2_HIGHCPU_8)
  when `--parallel` runs many such jobs at once.

## M4. Dry run

```bash
cd "$PROJECTS/pipe-segment"
dit run "$DIT/workflows/pipe_segment/ais.py" --experiment-id seg-plan --dry-run
```

That plans the default run. Add `--modes` to plan another, e.g. the one of M6.

(`$DIT` is this repository's checkout.) It logs the pin's size from a BigQuery
dry run, every variant's tables, every job with its dates and what it reads,
and the comparisons. It creates nothing: no copy, no table, no image, no job.
It still applies the refusals of M3 and reads the pipe-segment commit.

## M5. Smoke (the default run, step 3, ran on Cloud Build `2e57a93b`, 2026-11-24)

**Step 1, laptop, no image and no Dataflow.** From a clean pipe-segment
checkout at v5.1.0:

```bash
cd "$PROJECTS/pipe-segment" && git checkout v5.1.0
dit run "$DIT/workflows/pipe_segment/ais.py" --experiment-id seg-smoke \
    --runner docker --build-from-source \
    --start 2020-01-01 --end 2020-01-03 \
    --modes 1_bf:full-history,4_daily
```

Four local DirectRunner runs (full history over three days, then three
incremental single days),
three comparisons. It reads about 33 MB for the pin (dry run, 2026-11-20) and
about 220,000 messages. Cost: BigQuery only, a few cents at most (estimated).
It is the first run of the incremental start-of-run checks against real tables
from DIT, and the first look at whether `table-check` treats the array columns
as expected. `--build-from-source` has no overlay, so Beam creates its own
staging dataset with your credentials.

**Step 2, Dataflow, on the default overlay image.** The same window
in Cloud Build, on the default image:

```bash
make dit-cloud PIPELINE=pipe-segment REF=v5.1.0 \
    WORKFLOW=workflows/pipe_segment/ais.py \
    ARGS="--experiment-id seg-smoke-df \
          --start 2020-01-01 --end 2020-01-03 \
          --modes 1_bf:full-history,4_daily"
```

Four Dataflow jobs of at most 4 workers each. Rough cost: under USD 1,
estimated from a few worker-minutes per job; not measured. It is the first
test of the overlay on Dataflow as the test service account.

**Step 3, the default run.** The same command without `--start`, `--end` and
`--modes`: January 2020, ten jobs (the table in M1). Estimated at about USD 1;
not measured. Ran on 2026-11-24 (Cloud Build `2e57a93b`): 10 Dataflow jobs, 9 comparisons, all IDENTICAL (status table at the top of this part).

## M6. The pipe-segment PR #227 test

PR #227 (merged 2026-11-20, v5.1.0) claims that incremental day-by-day runs
equal full history. On the staging cohort, January 2020, pipe-segment v5.1.0:

| | Variant | Shape | Jobs |
|---|---|---|---|
| A | `1_bf:full-history` | full history, one job over the window (the reference) | 1 |
| B | `2_bfd:full-history` | full history, backfill then daily | 4 |
| C | `2_bfd:incremental` | incremental, backfill then daily | 4 |
| D | `4_daily:incremental` | incremental, every day as its own job | 31 |
| E | `3_bftruncate:incremental` | incremental, full backfill then the last days rerun in order | 4 |

```bash
make dit-cloud PIPELINE=pipe-segment REF=v5.1.0 \
    WORKFLOW=workflows/pipe_segment/ais.py \
    ARGS="--experiment-id pr227 \
          --modes 1_bf:full-history,2_bfd:full-history,2_bfd:incremental,3_bftruncate:incremental,4_daily:incremental \
          --parallel"
```

It runs on the default image, the overlay of v5.1.0, so it needs no image
flag. Every entry names its segmenter, so the command does not depend on the
default of `--segmenter`. Locally, from a clean pipe-segment checkout at
v5.1.0, the same flags follow `dit run "$DIT/workflows/pipe_segment/ais.py"`.

Since 2026-11-20 this is no longer a test of an opt-in: the AIS DAG passes
`--incremental`. The default run (M1) holds A, C and E plus `1_bf:incremental`;
this one adds B, full history in the daily shape, and D, every day as its own
job.

- **44 Dataflow jobs**, each capped at 4 workers. With `--parallel` the five
  variants run side by side; D's 31 jobs run one after the other and set the
  duration.
- **12 comparisons**: each of the four other variants with A, on three
  tables (`--compare reference`, the default). `--compare all-pairs` gives
  30: every pair of the five.
- Rough cost: a few USD, estimated from startup-dominated jobs of a few
  worker-minutes each on e2-standard-4. Not measured: the runs of
  2026-11-24/25 recorded no cost. `--worker-machine-type` lowers it.

### With the base version (v5.0.3) as a third side

The command above runs v5.1.0 only. Its reference A is the head's code in the
old mode, so it shows that the two segmenters of v5.1.0 agree, not that
v5.1.0 agrees with the version before the PR. The PR also changed code both
segmenters share (`pipeline.py`, `transform/read_messages.py`,
`transform/create_segments.py`; `git diff --stat v5.0.3 v5.1.0`). Adding the
base as a sixth variant closes that:

| | Variant | Shape | Jobs |
|---|---|---|---|
| Z | `1_bf:full-history@base` | pipe-segment v5.0.3, one job over the window (the reference) | 1 |

```bash
make dit-cloud PIPELINE=pipe-segment REF=v5.1.0 \
    WORKFLOW=workflows/pipe_segment/ais.py \
    ARGS="--experiment-id pr227 \
          --modes 1_bf:full-history@base,1_bf:full-history,2_bfd:full-history,2_bfd:incremental,3_bftruncate:incremental,4_daily:incremental \
          --base-image gcr.io/world-fishing-827/dit/pipe-segment:v5.0.3-f4d39fe-temp-dataset-82b7f0a8 \
          --parallel"
```

- **45 Dataflow jobs** and **15 comparisons**: each of the five other
  variants with Z, on three tables (45 with `--compare all-pairs`).
- Z against A is the pair that tests the shared code: same mode, same flags,
  two versions.
- It needs both overlay images: v5.1.0 (the default) and v5.0.3. Both were
  built on 2026-11-24.
- Optional: `2_bfd:full-history@base` (4 more jobs) shows whether v5.0.3's own
  day-by-day runs equal its one-job run. Without it, a difference in B cannot
  be told apart as new or as there before the PR.
- The schema definitions of the three tables are unchanged between the two
  tags (`schemas/message_schema.py`, `schemas/segment_schema.py`,
  `transform/fragment.py`; read from `git diff v5.0.3 v5.1.0`, no table
  written).

**Expected result:** all three tables IDENTICAL in every pair. **Measured**
on Cloud Build `4bb84ec0` (2026-11-24): all 15 comparisons against
`1_bf:full-history@base` IDENTICAL, the same row counts in all 6 variants
(status table above).

- The PR's own validation found differences between incremental and
  production only in the order of the elements inside the array columns of
  `fragments` and `segments`.
- Those columns are `ARRAY<STRUCT<...>>` whose fields are all scalars:
  `identities` and `destinations` in `fragments` (`count` INTEGER, `length`
  and `width` FLOAT, the rest STRING; `transform/fragment.py` lines 50 to 70
  at v5.1.0), and `daily_identities`, `daily_destinations`,
  `cumulative_identities` and `cumulative_destinations` in `segments` (`count`
  INTEGER, the rest STRING; `schemas/segment_schema.py` lines 24 to 41).
  `messages_segmented` has no array column.
- `table-check` compares `ARRAY<STRUCT<scalars>>` columns as multisets (its
  README, "Supported Column Types"), so element order alone is not a difference.
- Not checked: that the installed `table-check` does so on these tables, and
  that nothing else differs on a cohort of 142 vessels. A difference in
  `segments` between A and an incremental variant would be the finding the
  test exists for.

## M7. Reading the result

The run ends with one line per (table, pair) and exits 1 if any pair differs.

| Table | Key | What a difference means |
|---|---|---|
| `segments` | `seg_id`, `frag_id` | **The discriminating one.** A row holds the segment a fragment was matched to and the segment's cumulative count, identities and destinations. Both depend on earlier days. |
| `messages_segmented` | `msgid` | Discriminating for `seg_id` only: each message carries its fragment's segment id. |
| `fragments` | `frag_id` | Weak. A fragment is built from one vessel's messages of one day. It differs only if fragmenting is not deterministic or a clear-and-append went wrong. |

A changed `seg_id` shows in `segments` as one row only in A and one only in B,
because `seg_id` is in the key.

## M8. Cleanup

- Output tables: named `<table>_<suffix>_<variant>`; each carries the labels
  `resource_creator=dit`, `experiment_id=<experiment id>` and
  `dit_run_id=<run id>`. They have no expiration; drop them by hand.
- Dataflow jobs: named
  `dit-pipe-segment-segment-<experiment id>-<variant>-<N>-<M>` and labelled
  `dit_run_id`. `make dit-cancel RUN_ID=<id>` cancels a run's running jobs; it
  does not drop the tables, because this workflow records nothing in
  `dit_runs`. A cancelled `dit run` (SIGTERM) cancels its jobs the same way.
- The pinned copy expires after `--pin-expiration-days` (default 7).

## M9. Troubleshooting

| Symptom | Likely cause |
|---|---|
| `the incremental segmenter … needs pipe-segment v5.1.0 or later` | An executed image is older than the flag, or its tag names no version (M3). |
| `this pipe-segment checkout is unreviewed` | The run would execute the default image on code it does not hold (M3). |
| `these output tables already hold rows` | `--suffix` names an earlier run's tables. Use another suffix, or `--skip-pipelines` to compare them as they are. |
| An incremental job fails with `Only N of M days … have messages` | A day of the window has no messages in the source. pipe-segment's own check; choose another window. |
| An incremental job fails with `has no segments on <day>, but … has fragments up to that day` | The job before it did not write that day. pipe-segment's own check; the plan rules it out, so look for a failed or cancelled job. |
| A Dataflow job fails creating `beam_temp_dataset_…` | The image has no overlay (a plain build, or `--build-from-source` on Dataflow). |
| docker cannot pull the default image | The identity has no read access to `gcr.io/world-fishing-827/dit/`, or the image tag was changed by hand (M3). |
