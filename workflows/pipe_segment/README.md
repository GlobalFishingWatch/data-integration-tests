# pipe-segment in DIT

Entry page for the segmenter. Commands and details:
[`RUN_INSTRUCTIONS.md`](RUN_INSTRUCTIONS.md). Framework concepts (pinning by
copy, run cache, drift check, per-side adjustments, orchestration
declarations) are explained in the repository's [`README.md`](../../README.md)
and [`docs/architecture.md`](../../docs/architecture.md); this page covers
only what is specific to pipe-segment.

## What pipe-segment does

pipe-segment turns normalized AIS messages into tracks. It cuts each vessel's
messages of a day into **fragments**, matches fragments across days into
**segments**, and writes every message with the segment it belongs to. Then
it works out, per segment and per vessel, the most likely identity.

Production runs it as six steps (the `segment.*` tasks of the AIS v3 DAG):

| Step | Kind | Reads | Writes |
|---|---|---|---|
| `segment` | Beam | normalized messages | `messages_segmented`, `segments`, `fragments` |
| `segment_identity` | Beam | `segments`, `fragments` | `segment_identity_daily` |
| `segment_vessel_daily` | BigQuery SQL | `segment_identity_daily` | `segment_vessel_daily` |
| `segment_info` | BigQuery SQL | `segment_identity_daily`, `segment_vessel_daily` | `segment_info` |
| `vessel_info` | BigQuery SQL | `segment_identity_daily`, `segment_vessel_daily` | `vessel_info` |
| `segment_vessel` | BigQuery SQL | `segment_vessel_daily` | `segment_vessel` |

`segment` carries state from day to day: a job continues the segments of the
day before its range. It has two segmenters:

- **full history** (pipe-segment's default): each job reads every stored
  fragment and matches them all again;
- **incremental** (`--incremental`, added in v5.1.0 by pipe-segment PR #227):
  each job starts from the stored segments of the day before its range.

The owner's statement that `segment` must run sequentially (no parallel
slices) is recorded in [`orchestration.py`](orchestration.py), as `claimed`,
not verified. The other five steps are `unknown`.

## Which versions production is configured for

Read from composer-dags-production's rendered DAG snapshots, which is what the
drift check reads. These are statements about the DAGs, not about a job that
ran.

| DAG | pipe-segment | `segment` passes `--incremental` | Since |
|---|---|---|---|
| AIS v3 (segment group) | v5.1.0 | yes | 2026-11-20 (composer PR #3145); v5.0.3 without the flag before |
| AIS v5 | none: waits for the v3 segment group and reads its tables | n/a | n/a |
| VMS v5 (own segment stage) | v5.0.3 | no (full history) | unchanged |

DIT mirrors the AIS v3 segment group. No DIT workflow mirrors the VMS segment
stage.

## DIT's default image

Both workflows default to **DIT's temp-dataset overlay of pipe-segment
v5.1.0**: `gcr.io/world-fishing-827/dit/pipe-segment:v5.1.0-4ddde03-temp-dataset-04b40b54`.
An overlay of v5.0.3 exists too. Digests, build steps and why the overlay is
needed: [overlay README](../../docker/overlays/pipe-segment-temp-dataset/README.md).

In short: Beam stages each BigQuery query result in a dataset, and pipe-segment
cannot be told which one. Beam then tries to create one, which DIT's service
account may not. The overlay reads the dataset from `DIT_BQ_TEMP_DATASET`,
which both workflows set from `--bq-temp-dataset`.

Image rules shared by both workflows:

- reviewed code (a commit merged into pipe-segment `origin/main`) runs the
  default image, whatever that commit is;
- unreviewed code on the default image is refused before anything is pinned;
- DIT never auto-builds a pipe-segment image (a plain build has no overlay).
  Build an overlay image of the commit with the overlay's `build.sh`.

## The two workflows

| | [`ais.py`](ais.py) | [`identity_match_key.py`](identity_match_key.py) |
|---|---|---|
| Question | Does `segment` write the same tables however the window is cut into jobs, and with either segmenter? | Do two refs of pipe-segment (or of gpsdio-segment, through a pipe-segment branch) write the same tables? |
| Kind | mode equivalence | cross-version A/B |
| Steps run | `segment` only | all six (or `segment` only with `--skip-downstream`) |
| Unit | a variant: (mode, segmenter, side) | a binding: `name=ref` |
| Code compared | one image, plus an optional earlier version (`@base`, `--base-image`) | one image per binding (`--binding-image`) |
| Different flags per side | no: both sides get the same flags | yes: declared and ad hoc adjustments |
| Tables compared | `segments`, `messages_segmented`, `fragments` | up to eight (one per output) |
| Exit code on a difference | 1 | 0 (differences are information); 1 only if a binding failed |
| Run so far | yes, five Cloud Build runs (below) | one laptop smoke of `segment`, 2026-06-03, on v5.0.3; never on the overlay images |

Use `ais.py` to test a change to how `segment` chains days (PR #227 was such a
change), or to compare two versions on `segment` alone. Use
`identity_match_key.py` when the downstream identity tables matter, when the
two sides need different flags, or for the gpsdio-segment identity-match-key
experiment it was written for.

Both read a pinned copy of the source, so a difference comes from the code.
Neither uses the run cache.

## No satellite timing offsets in DIT

**DIT's segmenter workflows do not run pipe-segment's satellite timing
offsets** (owner decision, 2026-11-20). Production's `segment` task works out,
per receiver and hour, how far a satellite's clock is off, and drops the
messages of the hours above a threshold. A DIT run does neither: it passes none
of the four satellite flags and writes no offsets table. There is no option
for it. `identity_match_key.py` rejects the old `--include-satellite-offsets`
flag at argument parsing.

Why (measured or read on 2026-11-20):

- **The offsets must be computed from the full message feed, not from a test
  cohort.** The staging cohort's own offsets table
  (`pipe_ais_test_202408290000_published.satellite_timing_offsets`) was
  computed from the full feed when the cohort was built. Computed from the
  cohort's 142 vessels, the offsets come out different and noisy. For January
  2020: 990 receiver-hours above the segmenter's 30 s threshold, against 183
  in the cohort's table, with 39 in common. A run on them would drop the wrong
  messages.
- **The full feed is too large to read on every test run:** about 4.1 TB for
  2020.
- **pipe-segment has no option to read precomputed offsets.** `segment`
  computes them from three inputs and uses them in the same run (`pipeline.py`
  lines 195 to 222 at v5.0.3).
- Secondary: the satellite positions are date-sharded tables in another
  organisation (366 tables, 139 GB for 2020), and the offsets query reads its
  message source through a table wildcard that does not match a partitioned
  table.

What follows:

- On the 2020 staging cohort, DIT's `messages_segmented` keeps the messages
  the correction would drop: 1,361 receiver-hours in 2020, on 27 receivers and
  173 days, by the cohort's own offsets table. For those hours DIT's output is
  **not comparable with production's historical output**.
- **Comparisons between two DIT runs are not affected:** both sides run
  without the correction.
- The correction only ever applied to data that carries satellite receiver
  IDs. The current feed has none (since 2026-04-22), so production computes no
  offsets for current data either.
- The drift check accepts the four flags as differences of the `segment` step
  ([`composer_drift.py`](composer_drift.py)).

What would change this: an option in pipe-segment to read an offsets table
instead of computing one. Not planned work.

## Sources and sizes

| Source | Used by | Size | Notes |
|---|---|---|---|
| `world-fishing-827.pipe_ais_test_202408290000_internal.normalized_messages` (staging cohort, the default) | both | 10.7 MB for one day, 363 MB for January 2020, 3.78 GB for 2020 (dry runs, 2026-11-19/20) | 142 vessels; data is 2020 only, despite the 2024 snapshot date in the name. Partitioned by day on `timestamp`. Each day of January 2020 has 66,000 to 90,000 rows. |
| Production's normalized AIS messages (AIS datalake) | `ais.py` run `56af531e`, via `--source-normalized-table` | pin size not recorded | December 2012, 10 days: 81,357,979 rows in `messages_segmented` per variant. |

`--source-normalized-table` must be one table partitioned on `timestamp`: not a
view and not date-sharded tables. The incremental segmenter fails on a day
without messages, so every day of the window must have some.

Output tables go to `world-fishing-827.tech_great_expectations` by default
(`--dest-dataset`). They have no expiration; the pinned copy expires after 7
days by default.

## Run history

All runs of `ais.py`, in Cloud Build, on the overlay images, as the test
service account. Results checked with `table-check`.

| Date | Build | What ran | Result |
|---|---|---|---|
| 2026-11-24 | `2e57a93b` | Default run: January 2020, 4 variants, 10 Dataflow jobs, 9 comparisons | All IDENTICAL. Rows per variant: `segments` 5,612, `messages_segmented` 2,068,881, `fragments` 5,612 |
| 2026-11-24 | `4bb84ec0` | pipe-segment PR #227 test with v5.0.3 as base: 6 variants, 45 jobs, 15 comparisons against `1_bf:full-history@base` | All IDENTICAL; the same row counts in all 6 variants |
| 2026-11-24 | `967b1030` | `--direct-runner-max-days 1`: 3 one-day jobs on DirectRunner in the container | IDENTICAL to a Dataflow reference on all three tables |
| 2026-11-24/25 | `56af531e` | Production input, 2012-12-01 to 2012-12-10, v5.0.3 (`@base`) against v5.1.0: 4 variants, 10 jobs, production machine type, worker cap 4, about 1 h 50 min | `segments` and `fragments` (1,031,761 rows) IDENTICAL. `messages_segmented` reported 2 differences, both on one `msgid` that two vessels share, with the same two rows in both versions: a pairing bug in `table-check` with duplicated keys |
| 2026-11-25 | `de4e5831` | Compare-only rerun of `56af531e` (`--skip-pipelines`) with `table-check` v0.3.1, which fixed that bug | All 9 comparisons IDENTICAL |

Earlier: one laptop smoke of `identity_match_key.py`'s `segment` step on
2026-06-03, on v5.0.3.

**Durations measured:** a one-day staging job takes about 6 minutes on
Dataflow (about 2 min starting workers, 1 min of work, 2 min 10 s stopping;
build `4bb84ec0`) and about 1.5 minutes on DirectRunner (build `967b1030`).
The production-input run took about 1 h 50 min.

**Costs:** none measured. The figures in `RUN_INSTRUCTIONS.md` are estimates.

**What the runs show.** On the staging cohort and on ten days of production
input, v5.1.0's incremental segmenter writes the same three tables as its full
history, and v5.1.0 writes the same as v5.0.3 in full history. The array
columns of `segments` and `fragments` are compared as multisets by
`table-check`, so element order does not count as a difference.

## Known limits and open items

- **`identity_match_key.py` has not run on the overlay images**, and not in
  Cloud Build at all. Its argv, preflight and refusals are covered by tests
  with every cloud call mocked.
- **No comparison with production's own tables.** A DIT run starts from empty
  tables, applies no satellite offsets, and (in `identity_match_key.py`) sees
  only the run's days in `segment_vessel_daily`'s 30-day window. A
  production-baseline mode for pipe-segment does not exist.
- **No workflow for the VMS segment stage** (v5.0.3, full history).
- **No per-side adjustments in `ais.py`.** A base whose command line differs
  from the head's in more than the three incremental flags cannot run as
  `@base`; use `identity_match_key.py`.
- **No run cache**, and output tables have no expiration: drop them by hand.
- **No parallel-slices mode** for either workflow; `segment` is declared
  sequential.
- **`--direct-runner-max-days`:** memory on the build machine with many
  parallel DirectRunner jobs (`--parallel`) is not tested.

## Files here

| File | What it is |
|---|---|
| [`ais.py`](ais.py) | Mode equivalence of `segment` |
| [`identity_match_key.py`](identity_match_key.py) | Cross-version A/B over the six steps; also holds the shared constants (default image, source, compare keys) |
| [`adjustments.py`](adjustments.py) | Declared per-side adjustments of pipe-segment's interface, and the version reads |
| [`composer_drift.py`](composer_drift.py) | Drift specs of both workflows against the AIS v3 DAG, plus the frozen v5.0.3 spec |
| [`orchestration.py`](orchestration.py) | Orchestration declarations of the six steps |
| [`RUN_INSTRUCTIONS.md`](RUN_INSTRUCTIONS.md) | Setup, commands, how to read results, cleanup, troubleshooting |
