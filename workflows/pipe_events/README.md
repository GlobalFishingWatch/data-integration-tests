# `workflows/pipe_events/`

DIT's workflows for [pipe-events](https://github.com/GlobalFishingWatch/pipe-events), limited to **fishing events**. For the framework itself (runners, pinning, Cloud Build, auth in the cloud) see the top-level [`README.md`](../../README.md) and [`docs/architecture.md`](../../docs/architecture.md). The index of all workflows is [`workflows/README.md`](../README.md).

## What pipe-events does

pipe-events builds GFW's event tables with BigQuery SQL. A container runs the queries; there is no Beam and no Dataflow. It also publishes encounters, loitering and port-visit events; DIT drives none of those.

For fishing events, DIT drives a four-step chain per date slice:

| # | Command (v4.2.17) | Runs | Writes (prefix `<suffix>_<mode>`) |
|---|---|---|---|
| 1 | `incremental_events` | twice, for `nnet_score` and `night_loitering` | `_<score>_merged` |
| 2 | `incremental_filter_events` | twice | `_<score>_filtered` |
| 3 | `auth_and_regions_fishing_events` | once | table `_fishing_events_v<YYYYMMDD>` and view `_fishing_events` |
| 4 | `fishing_restrictive` | once | table `_product_events_fishing_v<YYYYMMDD>` and view `_product_events_fishing` |

`<YYYYMMDD>` is the slice's exclusive end. So one slice is six container runs. Each step 1 run works inside its own BigQuery session (`_SESSION` temp tables), which is why the modes can run in parallel.

Inputs: `research_messages` (step 1), `segs_activity`, `segment_vessel`, `product_vessel_info_summary` (step 2), `identity_core`, `identity_authorization`, `spatial_measures_20201105`, `event_regions` and `product_vessel_info_summary` again (step 3). Each has a `--source-<name>-fqn` flag.

## Versions: DIT is behind production

| | pipe-events version | Entrypoint and commands |
|---|---|---|
| DIT default image | v4.2.17 (the AIS v3 DAG's pin when the workflow was written, 2026-05) | `pipe`; `incremental_events`, `incremental_filter_events`, `auth_and_regions_fishing_events`, `fishing_restrictive` |
| Production, AIS v5 DAG (composer, 2026-11-19) | v9.0.0 | `pipe-events`; `fishing_events_incremental`, `fishing_events_incremental_filter`, `fishing_events_auth_and_regions`, `fishing_events_restrictive` |
| Production, AIS v3 DAG (composer, 2026-10-23) | v5.0.0 | not checked |

What catching up to v9.0.0 needs (read from pipe-events v9.0.0 and [`docs/composer-snapshots-2026-10.md`](../../docs/composer-snapshots-2026-10.md) § 8d; nothing run):

1. **New argv builders.** Global options (`--project`, `--dry-run`, `-v`) go before the command; flags are renamed from single-dash names to `--bq-in-*`, `--bq-out-*`, `--score-field`, `--reference-date` and so on. Only `--project` is unchanged.
2. **New inputs.** Production passes options DIT has no counterpart for: a field prefix and gear-type filters in the filter steps, and a UDF dataset, a regions registry and a nautical-time raster in the authorization step. Each needs a flag, a staging default, and a decision on whether to pin it.
3. **Re-derive the pins** of `cross_version_fishing.py` from the v9 SQL. The one-day lookback of step 1 is the same in every tag from v4.2.17 to v9.0.0.
4. **Check the output names.** Whether v9 still writes `_v<date>` tables with a view to the latest was not checked; the comparison targets depend on it.
5. **Rewrite the drift spec** (`composer_drift.py`) and set `in_sync=True` once the check passes.

Still true at v9.0.0: step 1 runs in a BigQuery session, there is no vessel filter, and the authorization query aggregates without `ORDER BY` (below).

## Workflows

| Workflow | Use it to | Container runs per default run |
|---|---|---|
| [`fishing.py`](fishing.py) | check that a backfill and the daily schedule produce the same fishing events | 54 (9 slices of 6 runs) |
| [`cross_version_fishing.py`](cross_version_fishing.py) | compare two or more pipe-events refs on the same pinned inputs | 54 per binding with all modes |

Neither has a run cache: the cache key is built around a Dataflow worker image digest, and the docker-runner shape is not settled. Neither submits Dataflow jobs, so `make dit-cancel` has nothing to stop; cancel the Cloud Build build or the local process.

### `fishing.py`

Port of pipe-events' three bash integration scripts (`integration_tests/staging-bf_bfd_bftruncate_async.sh`, `pipe3-bf_bfd_bftruncate.sh`, `pipe3-bf_bfd_bftruncate_async.sh`), with the comparison the scripts never had. Dates are **half-open** `[--start, --end)`; the default `2020-01-01` to `2021-01-01` covers 2020.

| Mode | Slices (defaults: `--tail-days 3`) | What it models |
|---|---|---|
| `1_bf` | one slice `[start, end)` | a single backfill; the reference |
| `2_bfd` | `[start, end - tail_days)`, then one-day slices `[d, d+1)` over the last `tail_days` days | a backfill followed by the daily DAG |
| `3_bftruncate` | `[start, end)`, then the same one-day slices | the daily DAG re-running days already processed |

- `--modes` selects a subset; a single mode runs and reports "no pair to compare".
- `--parallel` runs the modes at the same time.
- Exit code: 0 when every pair is identical on both views, non-zero otherwise.

### `cross_version_fishing.py`

Runs `fishing.py` once per `--binding name=ref` (each from a `git worktree` of `--pipeline-dir`, default `$PROJECTS/pipe-events`), on pinned copies of the inputs, and compares both views pairwise across bindings. Bindings run in parallel unless `--sequential-bindings`.

- **Pinning** (by copy since 2026-11-19, PIPELINE-4705). Seven of the eight inputs are pinned, copied into `world-fishing-827.tech_great_expectations` (the eighth, `spatial_measures_20201105`, is not pinned; see below): `research_messages` over `[--start - 1 day, --end)`, the other six whole. Views (on the staging cohort: `identity_core`, `identity_authorization`, `event_regions`) are copied as plain tables. `spatial_measures_20201105` is not copied: its name carries its version, and every binding reads the same table. Estimated 5.96 GB per default run (dry runs, 2026-11-19). `--reuse-pin <stamp>`, `--pin-max-gb`, `--pin-expiration-days`, `--dry-run` work as in the other pinning workflows.
- **The wrapper owns** `--start`, `--end`, `--tail-days`, `--modes`, `--suffix`, `--dest-dataset` and every source flag; it drops them from the extras it passes to `fishing.py`. It also drops `--image-tag` and `--build-from-source`, so each binding's image comes from its own ref.
- **Each binding's image.** An unreviewed ref (not merged into pipe-events `origin/main`) gets an auto-built `gcr.io/world-fishing-827/dit/pipe-events:dit-<sha>`. A **reviewed** ref runs the default image, v4.2.17, whatever its commit (read from `dit.worker_image.ensure_pipeline_image`). So `--binding main=main` compares against v4.2.17, not against `main`. See "Known limits".
- Exit code: non-zero when a binding fails. Differences are reported and do not fail the run.

## What a difference means

Compare keys: `event_id` on the `_fishing_events` and `_product_events_fishing` views (no `view_suffix`). The views hide each mode's different `_v<date>` table name. The schema has no SCD-2 columns.

**Expect differences even when the events are the same.** The first live run (2026-06-03) reported 92 to 97 % of rows differing, and inspection of more than 500 of them found no semantic difference. The cause is two `STRING` columns that hold JSON built by `TO_JSON_STRING` in `fishing-events-4-authorization.sql.j2`:

| Column | Why it differs between modes |
|---|---|
| `event_info` | averages and sums computed over different partitions differ in the last digits |
| `event_vessels` (its `public_authorizations` array) | built with `ARRAY_AGG(STRUCT(rfmo, has_publicly_listed_authorization))` without `ORDER BY`, so the element order varies (line 358 at v4.2.17, line 382 at v9.0.0) |

table-check compares these columns byte by byte. To read a run: if the only differing columns are `event_info` and `event_vessels`, parse the JSON and compare values before calling it a regression. Differences in any other column, or keys only on one side, are real. An `ORDER BY` in that `ARRAY_AGG` would remove about half the noise; that is pipe-events' change to make. JSON-aware comparison in table-check was considered and deferred.

## Source targets

The default is the AIS staging cohort (2020 data), as in pipe-events' staging script:

- `--internal-ds world-fishing-827.pipe_ais_test_202408290000_internal` (research_messages, segment_vessel)
- `--published-ds world-fishing-827.pipe_ais_test_202408290000_published` (segs_activity, identity views, product_vessel_info_summary)
- `--pipe-static world-fishing-827.pipe_static`, `--pipe-regions-layers world-fishing-827.pipe_regions_layers`

`research_messages` is partitioned by month and requires a partition filter; it is the only input read by date. Targets and sizes: [`README.md`](../../README.md) § "Source data targets".

The module docstring of `fishing.py` gives overrides for the production-scale `pipe3` scripts (`--internal-ds world-fishing-827.pipe_ais_v3_internal --published-ds world-fishing-827.pipe_ais_v3_published`, 2012). **Not verified:** those datasets were not checked. The `pipe3` bash scripts at v4.2.17 pass `--source_pipe_dataset pipe_ais_v3` to the generate script, which rejects that argument, so they are no reference for the right tables either. Check the tables before such a run, and size it first with `cross_version_fishing.py --dry-run`.

## Running

From a pipe-events checkout, with `$DIT` the path of this repository.

Cloud Build:

```bash
make -C $DIT dit-cloud PIPELINE=pipe-events \
    WORKFLOW=workflows/pipe_events/fishing.py \
    ARGS="--parallel"
```

Laptop. The container reads GCP credentials from the `gcp` docker volume at `/root/.config`; how to create and fill it once is in [`docs/usage.md`](../../docs/usage.md) § "Run a workflow".

```bash
cd $PROJECTS/pipe-events

# Cheapest smoke: one mode, image built from this checkout.
dit run $DIT/workflows/pipe_events/fishing.py --modes 1_bf --build-from-source

# All three modes on the published image (reviewed checkout) or an auto-built one (unreviewed).
dit run $DIT/workflows/pipe_events/fishing.py --parallel

# Two refs on pinned inputs; check the pin cost first.
dit run $DIT/workflows/pipe_events/cross_version_fishing.py \
    --experiment-id my-check --binding base=v4.2.17 --binding head=my-branch \
    --modes 1_bf --dry-run
```

`--build-from-source` builds the compose `pipeline` service from the working tree and ignores `--image-tag`. Without it, an unreviewed checkout triggers a kaniko build into `gcr.io/world-fishing-827/dit/pipe-events`, which needs gcloud and Cloud Build access.

## Orchestration and drift

- **Orchestration** ([`orchestration.py`](orchestration.py)): `incremental_events` must not run slices in parallel and depends on the previous run (claimed, owner, 2026-11-24). `incremental_filter_events`, `auth_and_regions_fishing_events` and `fishing_restrictive`: unknown, untested. `fishing.py` has no parallel-slices mode.
- **Drift** ([`composer_drift.py`](composer_drift.py)): `pipe_events/fishing` against six tasks of the `fishing_intervals` group of `core_ais_v5` (steps 1 and 2 once per score field). Not expected in sync: version, entrypoint, commands and flags differ. The DAG's longline-set tasks are not mirrored. `cross_version_fishing.py` has no spec.

## Run history

| Date | Run | Result | Build |
|---|---|---|---|
| 2026-06-03 | `fishing.py`, staging 2020, all three modes, Cloud Build | about 47 minutes; every container run succeeded; comparison reported differences, all traced to the two JSON columns above | `e6f06a00-7a0e-4533-aede-e136f97301a4` |
| 2026-11-19 | `cross_version_fishing.py` pin by copy | BigQuery dry runs, plus a live copy of the two small identity views | none |

No live run of `cross_version_fishing.py` is recorded.

## Known limits and open items

- **Behind production** (see above). Porting to v9.0.0 is the main open item.
- **A reviewed ref runs v4.2.17.** For `fishing.py` from a clean, merged checkout, and for a cross-version binding at a merged ref, the container is the default image, not the commit. Pass `--image-tag` to `fishing.py` to run another published version; the cross-version wrapper strips `--image-tag`, so a binding at a merged ref later than v4.2.17 cannot be tested as itself today.
- **JSON-string noise** makes every comparison report differences (above).
- **No vessel filter** in pipe-events; runs are made cheap by the staging cohort only.
- **No run cache.**
- **No parallel-slices mode**, and three of the four steps have no orchestration statement.
