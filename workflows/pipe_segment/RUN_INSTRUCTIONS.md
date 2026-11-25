# pipe-segment workflows: run instructions

How to run the two pipe-segment workflows, read their results and clean up.
What pipe-segment is, which versions production is configured for, the run
history and the open items are on the entry page,
[`README.md`](README.md).

- [Setup](#setup)
- [No satellite timing offsets in DIT](#no-satellite-timing-offsets-in-dit)
- [Mode equivalence of the segment step (`ais.py`)](#mode-equivalence-of-the-segment-step-aispy)
- [Cross-version A/B (`identity_match_key.py`)](#cross-version-ab-identity_match_keypy)

In the commands, `$DIT` is this repository's checkout and `$PROJECTS` the
directory holding the pipeline checkouts (default: the parent of `$DIT`).

## Setup

```bash
cd "$DIT"
make install
```

That is enough: both workflows run pipe-segment in its container and never
import it. `make install-pipe-segment` adds an editable install of
`$PROJECTS/pipe-segment` for stepping through its source (Python 3.12 or
newer; the target pins apache-beam 2.71.0, the version v5.0.3 and v5.1.0 pin).

**Laptop docker runs** need the `gcp` auth volume, once:

```bash
docker volume create gcp
docker compose -f "$PROJECTS/anchorages_pipeline/docker-compose.yaml" \
    run --rm gcloud auth application-default login
```

At v5.0.3 and v5.1.0 (and on pipe-segment `origin/main` as of 2026-11-25),
pipe-segment's `docker-compose.yaml` has no `gcloud` service, so use another
pipeline checkout that has one. Releases up to v5.0.2 shipped a `compose.yaml`
with a `gcloud` service, and some newer branches carry it too, so an older or
feature-branch checkout of pipe-segment may offer one.

**Cloud Build runs** (`make dit-cloud`) need no volume: the docker runner
attaches the container to the build's network instead. Run `make dit-cloud`
as your own account (`CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT=`), and check
`gcloud builds list` afterwards: a hung submission has been retried into a
duplicate build before. See the repository's [`README.md`](../../README.md)
for `make dit-cloud` in general.

## No satellite timing offsets in DIT

DIT's segmenter workflows do not run pipe-segment's satellite timing offsets.
No job passes any of the four satellite flags, and `--include-satellite-offsets`
is rejected. On the 2020 staging cohort this makes DIT's `messages_segmented`
not comparable with production's historical output for 1,361 receiver-hours;
comparisons between two DIT runs are not affected. The statement, the reasons
and the figures are in [`README.md`](README.md#no-satellite-timing-offsets-in-dit).

# Mode equivalence of the segment step (`ais.py`)

`ais.py` runs pipe-segment's `segment` step over one window in several shapes
and checks that every shape writes the same `segments`, `messages_segmented`
and `fragments`. It has run five times in Cloud Build, all IDENTICAL
([run history](README.md#run-history)).

## A1. Variants: mode, segmenter, side

A **mode** is how the window is cut into jobs. `--start` and `--end` are both
inclusive; `--tail-days` defaults to 3.

| Mode | Jobs | On the default window (January 2020) |
|---|---|---|
| `1_bf` | one job over the window | 1 |
| `2_bfd` | one job up to `--tail-days` before the end, then one job per day | 4 |
| `3_bftruncate` | one job over the window, then the last `--tail-days` days rerun one by one, in order | 4 |
| `4_daily` | every day its own job, in order (not in the default) | 31 |

A **segmenter** is `incremental` (pipe-segment's `--incremental`, v5.1.0 and
later; the default, as the AIS v3 DAG passes it) or `full-history`
(pipe-segment's default; what the VMS v5 DAG runs).

A **side** is `head` (the run's own images) or `base` (`--base-image`, an
earlier pipe-segment version).

A `--modes` entry is `mode[:segmenter][@base]`:

- a bare mode (`2_bfd`) runs with every segmenter in `--segmenter`;
- `1_bf:full-history` runs exactly that pair;
- `1_bf:full-history@base` runs it on `--base-image`.

Variants sort by mode, then side (base first), then segmenter (full history
first). **The first selected variant is the reference**: `1_bf:full-history@base`
when selected, else `1_bf:full-history`.

**The default run** (no `--modes`, no `--segmenter`):

| Variant | Jobs | Role |
|---|---|---|
| `1_bf:full-history` | 1 | the reference: no state handed from job to job |
| `1_bf:incremental` | 1 | incremental matching without a hand-over between jobs |
| `2_bfd:incremental` | 4 | backfill, then daily: the shape the DAG runs every day |
| `3_bftruncate:incremental` | 4 | three days rerun on top of a full backfill |

Ten jobs, 363 MB pinned. `--compare reference` (the default) compares each
variant with the reference on the three tables: 9 comparisons.
`--compare all-pairs` compares every pair: 18.

## A2. What the workflow guarantees and refuses

All checks run before any cloud call unless noted.

- **Each variant has its own three tables**, `<table>_<suffix>_<variant tag>`
  in `--dest-dataset`, for example `segments_<suffix>_2_bfd_inc`. The suffix is
  `<experiment id>_<commit>_<random>` unless `--suffix` is given.
- **They must start empty.** A job reads its own fragments and segments
  tables as its history, so a table that already holds the window would be
  read twice. A run refuses tables that hold rows (a metadata read, before the
  pin).
- **Every plan chains.** Each job starts no later than the day after the last
  day written, and the last job ends on the window's last day.
- **Every day of the window must have messages.** pipe-segment's incremental
  segmenter fails otherwise (its own check, at run time).
- **The source is pinned by copy** over the window before any job runs, and
  every job reads the copy. `--reuse-pin <stamp>` reads an earlier run's copy;
  `--pin-max-gb` (default 1000) caps it; `--pin-expiration-days` (default 7).
- **`incremental` needs an image of v5.1.0 or later** on the side that runs
  it. On older code pipe-segment would hand the flag to Beam, which ignores
  it, so the job would run full history and the comparison would pass without
  testing anything. The version is read from the start of the image tag
  (`v5.1.0-…`). A tag that names no version needs `--assume-incremental-support`.
  With `--build-from-source` the working tree's `segment` command is read for
  the flag instead.
- **`@base` rules:** `@base` without `--base-image`, `--base-image` without an
  `@base` entry, and `@base` with `--build-from-source` are refused. Both
  sides get the same flags; nothing is adjusted for the older version.
- **Unreviewed code on a default image is refused** (a dirty tree, a DIT
  snapshot, a commit not merged into `origin/main`).
- **`--tail-days`** must be between 0 and the window's length minus one.

## A3. Images

| Flag | Default | Runs |
|---|---|---|
| `--image-tag` | overlay of v5.1.0 | the submitter of every head job |
| `--worker-image` | the same | Dataflow workers of every head job; must hold the same code |
| `--base-image` | none | submitter and worker of every `@base` job |

The two overlay images: [overlay README](../../docker/overlays/pipe-segment-temp-dataset/README.md).
For any other commit, build an overlay image of it (`build.sh`, label starting
with the version) and pass it as both `--image-tag` and `--worker-image`.

`--runner docker --build-from-source` runs the working tree with DirectRunner
in a local container and needs no image. That build has no overlay: Beam
creates its own staging dataset with your credentials, so do not use it on
Dataflow.

## A4. Short jobs on DirectRunner

A one-day staging job spends about 4 of its 6 minutes on Dataflow starting and
stopping workers (build `4bb84ec0`). `--direct-runner-max-days N` runs every
job of N days or fewer on DirectRunner inside the container and keeps longer
jobs on Dataflow.

- Set by hand. Use it for small inputs such as the staging cohort, not for
  production-sized days.
- The comparison checks it: a variant whose daily jobs ran locally is compared
  with a reference on Dataflow. Build `967b1030`: IDENTICAL.
- `--dry-run` says which runner each job uses.
- Refused with `--runner docker`, where it would change nothing.
- Not tested: memory on the build machine when `--parallel` runs many local
  jobs at once.
- The choice per job is made by the shared helper
  `dit.workflow.runner_for_days`.

## A5. Commands

`ais.py` reads the pipe-segment commit from the current directory, so run it
from a pipe-segment checkout. In Cloud Build that is the uploaded checkout of
`REF`.

**Dry run.** Plans the run and creates nothing: no copy, no table, no image,
no job. Logs the pin's size (a BigQuery dry run), every variant's tables and
image, every job with its dates and runner, and the comparisons.

```bash
cd "$PROJECTS/pipe-segment" && git checkout v5.1.0
dit run "$DIT/workflows/pipe_segment/ais.py" --experiment-id seg-plan --dry-run
```

**Laptop smoke, no image and no Dataflow.** Three days, full history against
three incremental single days: 4 local jobs, 3 comparisons, about 33 MB pinned.

```bash
cd "$PROJECTS/pipe-segment" && git checkout v5.1.0
dit run "$DIT/workflows/pipe_segment/ais.py" --experiment-id seg-smoke \
    --runner docker --build-from-source \
    --start 2020-01-01 --end 2020-01-03 \
    --modes 1_bf:full-history,4_daily
```

**The default run** (ran as `2e57a93b`): January 2020, 10 Dataflow jobs,
9 comparisons.

```bash
make dit-cloud PIPELINE=pipe-segment REF=v5.1.0 \
    WORKFLOW=workflows/pipe_segment/ais.py \
    ARGS="--experiment-id seg-default"
```

**The pipe-segment PR #227 test, with v5.0.3 as the base** (ran as
`4bb84ec0`): does v5.1.0's incremental segmenter equal full history, and does
v5.1.0 equal v5.0.3? 45 Dataflow jobs, 15 comparisons.

| Variant | Shape | Jobs |
|---|---|---|
| `1_bf:full-history@base` | v5.0.3, one job (the reference) | 1 |
| `1_bf:full-history` | v5.1.0, one job: against the base, it tests the code both segmenters share | 1 |
| `2_bfd:full-history` | full history, backfill then daily | 4 |
| `2_bfd:incremental` | incremental, backfill then daily | 4 |
| `3_bftruncate:incremental` | incremental, last days rerun in order | 4 |
| `4_daily:incremental` | incremental, every day its own job (sets the duration) | 31 |

```bash
make dit-cloud PIPELINE=pipe-segment REF=v5.1.0 \
    WORKFLOW=workflows/pipe_segment/ais.py \
    ARGS="--experiment-id pr227 \
          --modes 1_bf:full-history@base,1_bf:full-history,2_bfd:full-history,2_bfd:incremental,3_bftruncate:incremental,4_daily:incremental \
          --base-image gcr.io/world-fishing-827/dit/pipe-segment:v5.0.3-f4d39fe-temp-dataset-82b7f0a8 \
          --parallel"
```

Without the `@base` entry and `--base-image` it is the v5.1.0-only test: 44
jobs, 12 comparisons. Add `--direct-runner-max-days 1` to run the one-day
jobs (40 of the 45) locally. Optional: `2_bfd:full-history@base` (4 more jobs) shows
whether v5.0.3's own daily runs equal its one-job run.

**Production input** (ran as `56af531e`): ten days of December 2012, v5.0.3
against v5.1.0, production machine type (the default sizing), worker cap 4
(the default). About 1 h 50 min.

```bash
make dit-cloud PIPELINE=pipe-segment REF=v5.1.0 \
    WORKFLOW=workflows/pipe_segment/ais.py \
    ARGS="--experiment-id seg-prod2012 \
          --source-normalized-table <production normalized messages table> \
          --start 2012-12-01 --end 2012-12-10 \
          --modes 1_bf:full-history@base,1_bf:full-history,2_bfd:incremental,3_bftruncate:incremental \
          --base-image gcr.io/world-fishing-827/dit/pipe-segment:v5.0.3-f4d39fe-temp-dataset-82b7f0a8 \
          --parallel"
```

Dry-run first to see the pin size against `--pin-max-gb`.

**Compare an earlier run's tables again** (as `de4e5831` did for `56af531e`
after the `table-check` fix): the same `ARGS` plus `--skip-pipelines --suffix
<that run's suffix>`. Nothing is pinned or run.

**The version the VMS DAG pins, on its own.** v5.0.3 has no incremental
segmenter, so select full history only:

```bash
make dit-cloud PIPELINE=pipe-segment REF=v5.0.3 \
    WORKFLOW=workflows/pipe_segment/ais.py \
    ARGS="--experiment-id seg-v503 \
          --segmenter full-history --modes 1_bf,2_bfd,3_bftruncate \
          --image-tag gcr.io/world-fishing-827/dit/pipe-segment:v5.0.3-f4d39fe-temp-dataset-82b7f0a8 \
          --worker-image gcr.io/world-fishing-827/dit/pipe-segment:v5.0.3-f4d39fe-temp-dataset-82b7f0a8"
```

Not run yet.

**Cost.** None measured. Rough estimates, not billed figures: the default run about USD 1, the
PR #227 test a few USD (startup-dominated jobs of a few worker-minutes each).
The production machine type (e2-standard-4) is more than the staging cohort
needs; the run logs a `cost hint:` and `--worker-machine-type` lowers it.

## A6. Reading the result

The run ends with one line per (table, pair) and **exits 1 if any pair
differs**.

| Table | Key | What a difference means |
|---|---|---|
| `segments` | `seg_id`, `frag_id` | **The discriminating one.** A row holds the segment a fragment was matched to and the segment's cumulative count, identities and destinations, all carried from earlier days. |
| `messages_segmented` | `msgid` | Discriminating for `seg_id` only: each message carries its fragment's segment. |
| `fragments` | `frag_id` | Weak. A fragment is built from one vessel's messages of one day; it differs only if fragmenting is not deterministic or a clear-and-append went wrong. |

- A changed `seg_id` shows in `segments` as one row only in A and one only in
  B, because `seg_id` is in the key.
- The array columns (`identities`, `destinations` in `fragments`; the four
  `daily_*` and `cumulative_*` in `segments`) are `ARRAY<STRUCT<scalars>>`.
  `table-check` compares them as multisets, so element order alone is not a
  difference. The PR author's own validation saw only order differences
  there.
- A `msgid` shared by two vessels is a duplicated key in `messages_segmented`.
  `table-check` before v0.3.1 reported such rows as differences even when both
  sides held the same rows (run `56af531e`). Check the `table-check` version
  before reading a small count of differences on that table.

## A7. Cleanup

- **Output tables:** `<table>_<suffix>_<variant tag>`, labels
  `resource_creator=dit`, `experiment_id=<experiment id>`,
  `dit_run_id=<run id>`. No expiration: drop them by hand.
- **Dataflow jobs:** `dit-pipe-segment-segment-<experiment id>-<variant tag>-<N>-<M>`,
  labelled `dit_run_id`. `make dit-cancel RUN_ID=<id>` cancels a run's running
  jobs, and so does a cancelled `dit run` (SIGTERM). Neither drops the tables:
  this workflow records nothing in `dit_runs`.
- **The pinned copy** expires after `--pin-expiration-days` (default 7).

## A8. Troubleshooting

| Symptom | Likely cause |
|---|---|
| `the incremental segmenter … needs pipe-segment v5.1.0 or later` | An image that runs an incremental variant is older, or its tag names no version (A2). |
| `this pipe-segment checkout is unreviewed` | The run would execute the default image on code it does not hold (A3). Build an overlay image of the commit. |
| `these output tables already hold rows` | `--suffix` names an earlier run's tables. Use another suffix, or `--skip-pipelines` to compare them as they are. |
| `Only N of M days … have messages` | A day of the window has no messages. pipe-segment's own check; choose another window. |
| `has no segments on <day>, but … has fragments up to that day` | The job before did not write that day: look for a failed or cancelled job. pipe-segment's own check. |
| A Dataflow job fails creating `beam_temp_dataset_…` | The image has no overlay (a plain build, or `--build-from-source` on Dataflow). |
| docker cannot pull the default image | The identity cannot read `gcr.io/world-fishing-827/dit/`, or the tag was changed by hand. |
| 2 differences in `messages_segmented`, nowhere else | Possibly a duplicated `msgid` and a `table-check` older than v0.3.1 (A6). |

# Cross-version A/B (`identity_match_key.py`)

`identity_match_key.py` runs the six steps of the segment group once per
**binding** (`--binding name=ref`, a ref of pipe-segment) and compares the
bindings' tables. It was written for one experiment: reducing gpsdio-segment's
identity `match_key` from `(transponder_type, receiver_type, source)` to
`(transponder_type,)`, exposed through a pipe-segment branch that pins a
gpsdio-segment fork. It also compares two pipe-segment versions.

**Status.** Not run since 2026-06-03 (one laptop smoke of `segment`, v5.0.3),
never on the overlay images and never in Cloud Build. What it emits is
covered by tests with every cloud call mocked, and checked against the AIS v3
DAG by the drift check. Treat the expected results below as expectations.

## B1. What a binding runs

The six steps in production's order (table on the [entry page](README.md#what-pipe-segment-does)).
Each reads what the one before wrote; the first failure ends the binding.
Bindings run one after the other.

- `--runner` (`dataflow` or `DirectRunner`), `--max-num-workers` (default 50
  per Beam step, as production), `--worker-machine-type` and `--disk-size-gb`
  (default e2-standard-4, 50 GB, as production) apply to the two Beam steps.
  The four SQL steps are queries run by the container; they take their
  tables, `--labels` and `--project` only.
- Production runs the last three SQL steps in parallel; here they run in turn.
- `--skip-downstream` runs and compares `segment` only.
- `--date-range START,END` is inclusive on both ends. A binding runs one
  `segment` job over it.
- Output tables: `<table>_<experiment id>_<binding name>` in `--dest-dataset`.
- **Not production's state.** A binding starts from empty tables: `segment`
  has no history to continue, and `segment_vessel_daily` sees only the run's
  days in its 30-day window. The same for every binding, so the A/B is fair;
  the tables are not comparable with production's.

## B2. The pinned source

Before any binding runs, the normalized messages of `--date-range` are copied
into `world-fishing-827.tech_great_expectations`, and every binding's
`segment` reads the copy.

- The copy holds exactly the run's days: `segment` reads its source by date,
  with no lookback, and no other step reads it.
- Size on the staging table: 10.7 MB for one day, 363 MB for January 2020,
  3.78 GB for 2020 (dry runs).
- The run logs a pin stamp, e.g. `20261119t101500z`; the copy's name ends in
  it. `--reuse-pin <stamp>` reads that copy again (same `--experiment-id`,
  `--date-range` and source).
- `--pin-max-gb` (default 1000) and `--pin-expiration-days` (default 7) as in
  `ais.py`.
- `--dry-run` dry-runs the copy only, still runs the checks of B5 and creates
  the worktrees; it runs no step and compares nothing.
- `--pin-source-at`, `--snapshot-dest-project` and `--snapshot-expiration-days`
  were removed with snapshot pinning; each is rejected with its replacement.

## B3. Images per binding

| Binding | Image it runs |
|---|---|
| reviewed ref, no flag | `--image-tag`, default the v5.1.0 overlay, whatever version the ref is |
| `--binding-image NAME=IMAGE` | that image, as submitter and Dataflow worker |
| unreviewed ref on the default image | **refused** before anything is pinned |
| `--runner DirectRunner --build-from-source` | the binding's worktree, built locally; no image |

- **Two pipe-segment versions need `--binding-image`.** Otherwise both run
  the default image.
- **Image version against ref version:** a binding whose image tag names
  another pipe-segment version than its ref's code states is refused before
  anything is pinned. Only a warning when it looks like an unreleased head: the
  ref is a branch or a commit, and the image is tagged the release right after
  the version its code states. Not checked when either version cannot be read.
- Every step's container gets `DIT_BQ_TEMP_DATASET` from `--bq-temp-dataset`.
- No `--setup_file` is passed, as in production, except with
  `--build-from-source` on Dataflow.

## B4. Flags per binding: adjustments and `--incremental`

Every binding starts from the **base** command line, which mirrors the AIS v3
DAG's segment group. On top of it, in order (framework:
[`README.md`](../../README.md), "Per-side adjustments in cross-version runs"):

1. **Declared adjustments** ([`adjustments.py`](adjustments.py)). There is one:
   a binding whose `segment` command does not declare `--incremental` loses
   `--incremental`, `--in_segments_table` and `--in_fragments_table`, and runs
   full history. The ref's source decides (read with `git show`); only when it
   cannot be read does the version (below v5.1.0) decide. So an unreleased
   head that has the flag but still states 5.0.3 keeps it.
2. **Ad hoc edits** for one run:
   `--binding-adjust NAME=<step>:<op>:<flag>[=<value>]`, `<op>` one of
   `remove`, `add`, `rename`, `set`, repeatable, applied in the order given.
   Values may name the binding's own tables (`{out.<table>}`) or the pinned
   source (`{in.normalized_messages}`).

What this means for an A/B:

- Two bindings at v5.1.0 or later both run incremental, from empty tables.
- **v5.0.3 against v5.1.0** compares full history with incremental as well as
  two versions. That is the owner's chosen default: the release production ran
  before, against the one it runs now. The run log prints the flag difference
  per step. To hold the segmenter fixed, remove the three flags from the newer
  binding (command in B7). The code-only comparison of the two versions on
  `segment` is `ais.py`'s `@base` pair.

## B5. Checks before anything is pinned

With git reads of `--pipeline-dir` only:

- every binding's ref exists;
- every flag of every step is declared by that step's command module at the
  binding's ref. pipe-segment hands unknown flags to Beam, which ignores them,
  so a wrong flag would otherwise run without effect. Beam's own options are
  exempt on the two Beam steps (`adjustments.BEAM_OPTIONS`). Best effort: a
  quoted flag literal anywhere in the module counts as declared;
- an adjustment that finds nothing to change is an error;
- two bindings are compared on the same keys;
- each binding's image (B3);
- the bindings pin **different gpsdio-segment versions**, unless
  `--allow-same-gpsdio-pin` (v5.0.3 and v5.1.0 both pin v3.0.0);
- on Dataflow, each binding name fits the job name (at most 27 characters).

The run log then prints, per binding, the edits applied and why, and per pair
the flag differences of each step. When bindings read different input tables,
it also prints a note naming them: the two sides then share their lineage only
by the operator's claim.

**Table edits** (different inputs or outputs per side, pinning the union of the
inputs, comparing by output name) are built in `dit.adjust` but have no real
case yet: no pipe-segment version changes its tables. They are tested with
synthetic interfaces only.

## B6. Drift spec

[`composer_drift.py`](composer_drift.py) compares the base command line with
the AIS v3 DAG's six tasks. Accepted differences: the four satellite flags,
DIT's own table names (`--in_segments_table`, `--in_fragments_table`) and a few
more, each with its reason. `--incremental` is compared.

The base plus the v5.0.3 adjustment is checked against the DAG at the last
composer commit where production ran v5.0.3 (`adjustments.FROZEN_AT`). That
spec is frozen and shows only with the drift runner's `--frozen` and in the
opt-in live test:

```bash
dit run workflows/composer_drift.py --frozen      # every spec, frozen ones included
dit run workflows/composer_drift.py --workflow pipe_segment/identity_match_key@v5.0.3
```

## B7. Commands

None of these has run in this form. In Cloud Build the pipe-segment checkout
is `/workspace`, so the cloud commands pass `--pipeline-dir /workspace` (the
default looks for a sibling checkout that is not uploaded). Inferred from how
`cross_version_ais.py` runs there; not tried for this workflow.

**DirectRunner smoke, laptop.** One binding, one day, a few vessels, `segment`
only:

```bash
cd "$DIT"
dit run workflows/pipe_segment/identity_match_key.py \
    --experiment-id seg-imk-smoke \
    --binding before=v5.1.0 \
    --date-range 2020-01-01,2020-01-01 \
    --runner DirectRunner \
    --ssvid-filter-query '"<ssvid>","<ssvid>"' \
    --skip-downstream \
    --build-from-source
```

Then drop `--skip-downstream` for all six steps, and `--build-from-source` to
run the default image. Expect per binding: a `worktree … @ v5.1.0` line, a
`gpsdio-segment pin -> …v3.0.0` line, a `commit=… image=…` line, one
`invoking pipe-segment <step>` line per step. With one binding nothing is
compared.

**Dataflow smoke, one binding, on the default image:**

```bash
make dit-cloud PIPELINE=pipe-segment \
    WORKFLOW=workflows/pipe_segment/identity_match_key.py \
    ARGS="--experiment-id seg-imk-cloud \
          --pipeline-dir /workspace \
          --binding before=v5.1.0 \
          --date-range 2020-01-01,2020-01-01 \
          --runner dataflow \
          --skip-downstream"
```

**Two pipe-segment versions, each on its own overlay image** (the older one
loses `--incremental` by the declared adjustment):

```bash
make dit-cloud PIPELINE=pipe-segment \
    WORKFLOW=workflows/pipe_segment/identity_match_key.py \
    ARGS="--experiment-id seg-503-510 \
          --pipeline-dir /workspace \
          --binding before=v5.0.3 \
          --binding-image before=gcr.io/world-fishing-827/dit/pipe-segment:v5.0.3-f4d39fe-temp-dataset-82b7f0a8 \
          --binding after=v5.1.0 \
          --allow-same-gpsdio-pin \
          --date-range 2020-01-01,2020-01-03 \
          --runner dataflow"
```

To run both on full history, add:

```bash
          --binding-adjust after=segment:remove:--incremental \
          --binding-adjust after=segment:remove:--in_segments_table \
          --binding-adjust after=segment:remove:--in_fragments_table
```

**The identity-match-key experiment.** First the two forks.

1. gpsdio-segment, from v3.0.0, branch
   `experiment/identity-match-key-transponder-only`: in
   `gpsdio_segment/msg_processor.py`, change
   `match_key = (transponder_type, receiver_type, source)` to
   `match_key = (transponder_type,)` at its three sites (`_store_info`, and the
   identities and destinations branches of `add_info_to_msg`). Push; note the
   SHA.
2. pipe-segment, from v5.1.0, the same branch name: repin gpsdio-segment to
   that SHA in `pyproject.toml`, regenerate `requirements.txt` (`make reqs`),
   push. The workflow reads each binding's pin from `pyproject.toml` (older
   tags: `requirements/prod.in`).
3. Build an overlay image of the pipe-segment branch (`build.sh`, label e.g.
   `v5.1.0-idmatch`) and push it.

Then:

```bash
make dit-cloud PIPELINE=pipe-segment \
    WORKFLOW=workflows/pipe_segment/identity_match_key.py \
    ARGS="--experiment-id idmatchkey \
          --pipeline-dir /workspace \
          --binding before=v5.1.0 \
          --binding after=experiment/identity-match-key-transponder-only \
          --binding-image after=<overlay image of the branch> \
          --date-range 2020-01-01,2020-01-03 \
          --runner dataflow"
```

Without `--binding-image after=…` the run is refused (unreviewed binding on
the default image). On the laptop, `--runner DirectRunner --build-from-source`
needs no image.

## B8. Reading the result

One line per (table, pair): IDENTICAL, DIFFERENT, SKIPPED (a binding failed)
or NOT COMPARED (only one binding writes the table). **Differences do not fail
the run**; the exit code is 1 only if a binding failed.

| Table | Key | Expected in the identity-match-key A/B |
|---|---|---|
| `messages_segmented` | `msgid` | DIFFERENT: identities and destinations per message are the metric |
| `segments` | `seg_id`, `frag_id` | identical or nearly: segmentation follows positions, not identities |
| `fragments` | `frag_id` | identical or nearly |
| `segment_identity_daily` | `seg_id`, `first_timestamp` | most informative: per-segment identities |
| `segment_vessel_daily` | `seg_id`, `ssvid`, `day` | most informative; a changed identity is a field difference, also in `vessel_id` |
| `segment_info` | `seg_id` | most informative; as above |
| `vessel_info` | `vessel_id`, `ssvid` | `vessel_id` hashes the ssvid and the most common identity, so a changed identity is one row only in each side, not a field difference |
| `segment_vessel` | `seg_id`, `ssvid`, `vessel_id` | as `vessel_info` |

Rows only in one side of `vessel_info` or `segment_vessel` are changed
vessels, not missing data. The keys of the last three tables come from each
step's final `GROUP BY`; no run has confirmed them unique.

## B9. Cleanup

This workflow does not stamp `dit_run_id`, so `make dit-cancel` finds nothing.

- **Output tables:** names end in `_<experiment id>_<binding name>`; labels
  `resource_creator=dit`, `experiment_id`, `binding`. No expiration.
- **Dataflow jobs:** `dit-pipe-segment-<step>-<experiment id>-<binding name>`.
  Cancel in the console or with `gcloud dataflow jobs cancel`.
- **The pinned copy** expires after 7 days; labels `dit_artifact=source_pin`,
  `dit_experiment_id`.

## B10. Troubleshooting

| Symptom | Likely cause |
|---|---|
| `gpsdio-segment pin -> …` identical across bindings | The fork branch was not pushed or fetched. `git -C "$PROJECTS/pipe-segment" fetch origin`. |
| All tables IDENTICAL in an A/B that should differ | Both bindings ran one image: no `--binding-image`, or one `--image-tag` for both. |
| `binding '…' is unreviewed …` | The ref is not merged into `origin/main` and would run the default image (B3). |
| `a binding would run an image of another pipe-segment version than its ref` | Give that binding `--binding-image NAME=<overlay of its version>` (B3). |
| Refused for the same gpsdio-segment pin | Expected for two pipe-segment versions; pass `--allow-same-gpsdio-pin`. |
| A flag is not declared by the step's command at the ref | A wrong `--binding-adjust`, or the ref's CLI differs from the base: add an adjustment (B4). |
| Rows only in one side of `messages_segmented` | The bindings read different copies, e.g. two invocations without `--reuse-pin`. |
| `TypeError: 'NoneType' object is not iterable` in `BigQueryHelper` | A step got no `--labels`: a hand-built command line. |
| `--binding name … is too long for the Dataflow job name` | At most 27 characters on Dataflow. |
| `pinning … would process … GB, above the … GB cap` | Narrow `--date-range`, or raise `--pin-max-gb` on purpose. |
| `pinned copy … does not exist` with `--reuse-pin` | The copy expired, or stamp, experiment id, date range or source differ. |
| The copy is denied `bigquery.tables.create` | The identity may not create tables in `tech_great_expectations` (for example gcloud impersonating `dq-monitoring@`). |
| A Dataflow job fails creating `beam_temp_dataset_…` | The image has no overlay. |
| `--include-satellite-offsets was removed` | Drop the flag ([No satellite timing offsets in DIT](#no-satellite-timing-offsets-in-dit)). |
| `… was removed with snapshot pinning` | The message names the replacement. |
| `--pipeline-dir … is not a git repo` in Cloud Build | `--pipeline-dir /workspace` is missing (B7). |
| `binding '…' ref '…' not found` | Fetch the ref in the pipe-segment checkout first. |
