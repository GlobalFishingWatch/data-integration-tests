# Onboarding `encounters_pipeline` into dit — audit + workflow plan (2026-07-30)

Step 1 of [`pipeline-contract.md`](pipeline-contract.md) § "Process: adding a new pipeline to dit's scope", for **encounters**. Records the prod orchestration + parameters (read out of `composer-dags-production`), the contract audit (matrix column added in the same commit), staging-cohort readiness, and the proposed workflow shape.

**Status (2026-07-30)**: audit complete; **`workflows/encounters/ais.py` written** (27 unit tests) — laptop smoke and cloud run both outstanding. Two upstream gaps block the cloud path (§ Blockers).

## Scope: generation only, not publication

Every GFW event type is produced in **two** halves:

1. **Generation** — detects the events, in a pipeline-specific repo.
2. **Product** — builds the `product_events_*` tables, in `pipe-events`.

| Event type | Generation step (repo) | Product step | dit coverage |
|---|---|---|---|
| port visits | `anchorages_pipeline` | pipe-events | `workflows/port_visits/ais.py` — generation only |
| fishing | pipe-events | pipe-events | `workflows/pipe_events/fishing.py` — **both** (same repo, hence its 4-step chain) |
| **encounters** | **`encounters_pipeline`** | pipe-events | **→ generation only (this plan)** |

**dit covers the generation half only.** `fishing.py` looks structurally different purely because both halves happen to live in pipe-events; it is not the template here. `port_visits/ais.py` is.

Consequence worth noting: `product_events_port_visit` (a *publication*-half input) is absent from the staging cohort, but that is **not** on this critical path.

## Version axes ("core AIS v3/v4")

Two different things are versioned, which is where the v3/v4 ambiguity comes from:

- **DAG**: encounters is configured in `dags/core/ais/v3.py` (and `dags/core/vms/config.py`). There is **no** `core/ais/v4.py`; `core/ais/v5.py` exists but has **no encounters** yet (measures + fishing_points only).
- **Image**: `Versions.DETECT_ENCOUNTERS = "v4.4.0"` — the `encounters_pipeline` image, which *is* v4.x. The local `encounters_pipeline` master is at **4.3.2**, i.e. slightly behind prod.

So: the **AIS v3 DAG** orchestrating the **encounters_pipeline v4.x** image.

AIS and VMS configs are near-identical in shape, differing only in table/dataset wiring (plus AIS's `vessel_info_field_prefix="ais_"` and `publish_events=True`). A `vms.py` sibling is therefore cheap once `ais.py` exists — the same relationship port-visits has.

## Prod orchestration (from `gfw/pipes/v3/detect_encounters.py`)

Task group `detect_encounters`, three tasks chained:

```
create_raw_encounters  ->  ensure_daily_encounters_table_exists  ->  merge_encounters  [-> publish]
```

`merge_encounters` has `depends_on_past=True`. The middle task just `bq mk`s the raw table with an explicit schema; dit does not need it (the pipeline creates with `CREATE_IF_NEEDED`).

### Step 1 — `create_raw_encounters`

| Arg | Prod value |
|---|---|
| `--source_table` | `measure_config.tables.measured_messages` |
| `--raw_table` | `raw_encounters` (partitioned; `raw_encounters_{ds_nodash}` when `raw_encounters_partitioned=False`) |
| `--start_date` | `{{ data_interval_start \| ds }}` |
| `--end_date` | `{{ calculate_data_interval_end(dag_run, days=-1) \| ds }}` |
| `--max_encounter_dist_km` | `0.5` |
| `--min_encounter_time_minutes` | `120` |

Plus labels + Dataflow args. Optional: `--raw_sink_write_disposition` (default `WRITE_APPEND`), `--ssvid_filter`, `--wait_for_job`.

### Step 2 — `merge_encounters`

| Arg | Prod value |
|---|---|
| `--start_date` | **`pipeline_config.data_available_from_date`** — i.e. the full history, every run |
| `--end_date` | `{{ calculate_data_interval_end(dag_run, days=-1) \| ds }}` |
| `--raw_table` | same raw table |
| `--sink_table` | `encounters` (public dataset) |
| `--vessel_id_table` | `segment_info` |
| `--spatial_measures_table` | `pipe_static.spatial_measures_20201105` |
| `--bad_segs_table` | subquery: `SELECT DISTINCT seg_id FROM <segs_activity> WHERE overlapping_and_short` |

Optional: `--min_hours_between_encounters` (default 4), `--min_encounter_time_minutes`, `--ssvid_filter`, `--wait_for_job`.

**`merge` is a full rebuild from `data_available_from_date` to `--end_date`, writing `WRITE_TRUNCATE`.** That shapes the mode-equivalence expectation (§ Workflow plan).

## Date + idempotency semantics (the parts that bite)

- **`--end_date` is INCLUSIVE on both steps**, stated in the `--end_date` help text ("Last date (inclusive)"). No ambiguity — contrast the pipe-gaps trap where `--date-range`'s upper bound is exclusive and cost us an off-by-one (PR #69). Still worth re-verifying against the SQL before the first live run, per the lesson from that PR: *verify date contracts against the pipeline's SQL, not its flag names*.
- **The raw-table pre-write DELETE is bounded on BOTH sides**: `DELETE FROM <raw> WHERE DATE(start_time) BETWEEN start AND end`. So re-running `create` over a window is idempotent and does **not** take ownership of the tail. **There is no reprocess-to-end contract here** — unlike pipe-gaps, whose delete is right-unbounded (see `workflows/pipe_gaps/CLAUDE.md`).
- **`merge` truncates its sink**, so it is idempotent by construction.

## Comparison contract

Truncate shape, keyed on **`encounter_id`** (`STRING`, first field of both `schemas/output.py` and the raw schema). No `valid_from`/`valid_to`, no `_last_versions` view → `view_suffix=""`, same family as port-visits' `visit_id` and pipe-events' `event_id`. NOT SCD-2.

```python
COMPARE_KEYS = ("encounter_id",)
COMPARE_VIEW_SUFFIX = ""
```

## Container / runner shape

`docker-compose.yaml` exposes three services, all built from `Dockerfile-scheduler`, all mounting the **`gcp:/root/.config/` external volume** — the same auth pattern pipe-events and pipe-segment use, so `dit.runners.docker` works unchanged (`volumes=["gcp:/root/.config"]`, and cloud mode's `--network=cloudbuild` applies automatically):

| Service | Entrypoint |
|---|---|
| `pipe_encounters` | `./main.py` (Dockerfile ENTRYPOINT) — takes the subcommand as argv[1] |
| `create_raw_encounters` | `python -m pipeline.create_raw_encounters` |
| `merge_encounters` | `python -m pipeline.merge_encounters` |

**Open item — the published-image entrypoint.** The DAG passes `cmds=["pipe-encounters"]`, but `setup.py` declares no `console_scripts` (package name is `encounters`) and `Dockerfile-scheduler`'s ENTRYPOINT is `./main.py`. The `pipe-encounters` executable presumably comes from the `gfw-pipeline` base image. **Verify against the published image before relying on it** — `port_visits/ais.py` uses `entrypoint="pipe-anchorages"` for the analogous case, so the convention probably holds, but it is unconfirmed here. `--build-from-source` can sidestep it entirely by using the per-step compose services.

Also note `Dockerfile-worker` (`apache/beam_python3.8_sdk:2.49.0`, deps only, **no package**) appears **unused by the v3 DAG**, which sets `sdk_container_image` to the *scheduler* image and `setup_file=None`. Don't mistake it for the worker image.

## Staging-cohort readiness (`pipe_ais_test_202408290000`)

| Input | Status |
|---|---|
| messages (`--source_table`) | ✓ `_internal.messages_positions` (prod uses `measured_messages`; the cohort's equivalent is what every other dit workflow reads) |
| `--vessel_id_table` | ✓ `_published.segment_info` |
| bad-segs subquery source | ✓ `_published.segs_activity` |
| `--spatial_measures_table` | ⚠️ **not in the cohort** — lives in prod `world-fishing-827.pipe_static.spatial_measures_20201105`. Precedent exists: `fishing.py` already reads `pipe_static` via a `--pipe-static` flag (read-only prod static reference), so this is a solved pattern. |
| output `encounters` / `raw_encounters` | n/a — dit creates these |

The `_internal.spatial_measures_thinned_0_0_5_20201105` referenced by `encounters_pipeline/scripts/generate_incremental_encounters.sh` **does not exist** in the cohort.

⚠️ **That script is not the prod path.** Despite living in `encounters_pipeline`, it drives pipe-events (`--entrypoint pipe`) and calls a `product_encounters` operation that is **absent** from the pipe-events CLI (which has only `incremental_events`, `incremental_filter_events`, `auth_and_regions_fishing_events`, `fishing_restrictive`). With `resource_creator: "chris"` and prefix `incremental_encounters_fix_daily_load_two`, it reads as in-progress experimentation. Treat the DAG as the source of truth.

## Blockers (both upstream, both already-seen shapes)

1. **No `--temp_dataset`** (contract #5). Beam's `ReadFromBigQuery` EXPORT staging will try to create `beam_temp_dataset_<uuid>`; the Cloud Build SA `automated-testing@` deliberately lacks `bigquery.datasets.create`, so **the cloud path fails**. Identical to the pipe-anchorages gap — fix is the same shape as that repo's `--temp_dataset` patch (still pending upstream). Laptop runs by a user with broader perms are unaffected.
2. **Labels not None-safe** (contract #6). `list_to_dict(cloud_opts.labels)` raises `TypeError` on `None`. Workflow-side mitigation is what `port_visits/ais.py` already does: always emit `--labels`. A 1-line upstream guard would remove the need.

Per the working agreement, workflow-side workarounds for missing contract items need a Plan-changelog entry explaining the trade-off — #2 gets one; #1 cannot be worked around workflow-side and gates the cloud path.

## Workflow: `workflows/encounters/ais.py` — AS BUILT (2026-07-30)

Modelled on `port_visits/ais.py` (two-step Beam-in-container generation, per-mode output tables, truncate-shape comparison).

- **Modes**: `1_bf` / `2_bfd` / `3_bftruncate`, via the shared `add_modes_arg` / `parse_modes` (landed 2026-06-11) so a cheap `--modes 1_bf` smoke works from day one.
- **Per slice**: `create_raw_encounters` → `merge_encounters`, each via `dit_docker.run(..., volumes=["gcp:/root/.config"])`.
- **Note on mode equivalence**: because `merge` always rebuilds the whole range with `WRITE_TRUNCATE`, the modes should agree *trivially* on the merged sink unless `create`'s raw table diverges. **The discriminating comparison is therefore the RAW table, not just the merged sink** — compare both, and expect the merged one to be the weaker signal. (Contrast pipe-gaps, where the SCD-2 tail is what diverges.) Worth confirming on the first run before trusting a green result.
- **Sources**: per-table FQN flags (the M4 convention), staging defaults, `--pipe-static` for spatial measures, `--ssvid-filter` passthrough for cheap runs.
- **Cache**: cacheable in principle (Dataflow worker-image digest exists), but follow pipe-events' precedent and defer until the workflow runs green.
- **Dates**: `--start` / `--end` **inclusive**, matching the pipeline. Default window inside the cohort's 2020 data, mirroring `port_visits/ais.py` (`2020-01-01` → `2020-12-31`) per the staging-cohort working agreement.

## Suggested order

1. ~~This audit~~ — **done**.
2. ~~`workflows/encounters/ais.py` + unit tests~~ — **done** (27 tests). Next: **laptop `--build-from-source` smoke** on a 1–2 day window with `--ssvid-filter`, which dodges the temp-dataset blocker. Suggested:
   ```
   PYTHONPATH=. dit run workflows/encounters/ais.py \
       --build-from-source --runner docker \
       --modes 1_bf --start 2020-01-01 --end 2020-01-02 \
       --ssvid-filter '<a few ssvids>' --experiment-id enc-smoke
   ```
   Two things to confirm on that first run: **(a)** the `--end_date` inclusivity against the emitted SQL (per the PR #69 lesson — don't trust the flag help alone); **(b)** the published-image entrypoint, if not using `--build-from-source`.
3. File the two upstream asks (`--temp_dataset`, None-safe labels).
4. Cloud smoke once the `--temp_dataset` ask lands upstream.
5. `vms.py` sibling; cache integration; then optionally the publication half if dit's scope ever extends there.
