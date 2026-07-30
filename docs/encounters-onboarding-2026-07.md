# Onboarding `encounters_pipeline` into dit — audit + workflow plan (2026-07-30)

Step 1 of [`pipeline-contract.md`](pipeline-contract.md) § "Process: adding a new pipeline to dit's scope", for **encounters**. Records the prod orchestration + parameters (read out of `composer-dags-production`), the contract audit (matrix column added in the same commit), staging-cohort readiness, and the proposed workflow shape.

**Status (2026-07-30)**: audit complete; **`workflows/encounters/ais.py` written** (30 unit tests) and **runs end-to-end on a laptop** (`--runner docker`, published v4.4.0 image) after six defects found by seven smokes (§ First-smoke findings). **Partially validated**: the RAW comparison is genuine (4 rows vs 4 rows, identical); the MERGED comparison is still 0-vs-0 because this cohort's only vessel pair involves a bad segment (§ Cohort sparsity). Cloud run remains blocked upstream (§ Blockers).

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

## First-smoke findings (2026-07-30) — what actually broke

Seven laptop smokes (`--runner docker`, published v4.4.0 image) produced **six distinct defects that the 30 unit tests could not have caught**. Recorded because most are re-encounterable on the next pipeline.

| # | Failure | Whose | Fix |
|---|---|---|---|
| 1 | `TypeError: 'NoneType' object is not iterable` (labels) | dit | `--labels` were emitted only on the Dataflow path, but `readers.py`/`writers.py` do `list_to_dict(cloud_opts.labels)` with no `None` guard and `ReadSources` is built on EVERY runner. Now on both paths; test parametrised over both runners. |
| 2 | `ReadFromBigQuery requires a GCS location` | dit | `--temp_location` missing on DirectRunner — EXPORT reads stage via GCS on any runner. Added, along with `--project` (the pipeline builds its own `bigquery.Client(project=cloud_opts.project)`). |
| 3 | `404 ... tables/raw_encounters_*` | dit (audit was wrong) | Both steps call `update_table_metadata()` → `get_table()` **before** the `CREATE_IF_NEEDED` sink creates anything. The audit had claimed dit did not need the DAG's `ensure_table_exists` task — it does. Prod never notices: its tables are long-lived. |
| 4 | **Clean exit, zero rows** | neither | 2020-01-01 has no encounters in this cohort. The run "passed" while never exercising the write path. See § Cohort sparsity. |
| 5 | `OSError: Project was not passed...` at `TriggerLoadJobs` | dit | Beam's `WriteToBigQuery` builds its own BQ client *inside the SDK worker*, which reads `GOOGLE_CLOUD_PROJECT` and never sees `--project`. Fixed with `container_env` — **verbatim the pipe-segment failure of 2026-06-03** that the parameter was added for. Encounters is its 2nd consumer; a 3rd should make it a runner default. Only reachable once there are rows to load, which is why #4 masked it. |
| 6 | `Incompatible table partitioning specification` | dit | The bootstrap mirrored the DAG's `ensure` task, which creates **unpartitioned** tables, but the sink declares `timePartitioning: MONTH on start_time` + `clustering`. **Mirroring the DAG is not sufficient** — the DAG task would fail too if it ever really created the table; it is a permanent no-op in prod. Spec now taken from the sink's own `additional_bq_parameters`. |

Plus one **upstream latent bug found in passing**: `writers.py` `delete_rows` calls `bqclient.query(DELETE_QUERY...)` with **no `.result()`** — fire-and-forget, so a failed pre-write DELETE is silently swallowed **in production too**. That is why #3 surfaced one call later than its actual cause.

Two environment notes:

- **gcloud impersonation blocks docker pulls.** With `impersonate_service_account` set in `gcloud config`, docker's credential helper authenticates as that SA, which lacked `artifactregistry.repositories.downloadArtifacts` on wf827's `gcr.io` — blocking `--build-from-source` (whose base image lives there). `CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT=` overrides per-command. The *published* image (different registry) was never blocked; an early 60s timeout on a 1.83 GB pull merely looked like a denial.
- **Published v4.4.0 ≠ local master 4.3.2**: Python 3.12 + `pipe_encounters` package vs Python 3.8 + `pipeline/`. So `--build-from-source` tests a different, older codebase. All flags were therefore re-verified against the **image**, not the source — and `--temp_dataset` is confirmed absent there too, so the cloud blocker is real for the version prod runs.

## Smoke result (2026-07-30, attempt 7)

`--runner docker --modes 1_bf,2_bfd --start 2020-01-19 --end 2020-01-22 --tail-days 1` completed: 3 slices x 2 steps across 2 modes, then both comparisons.

| Table | 1_bf | 2_bfd | Comparison |
|---|---|---|---|
| `raw_encounters_*` | 4 rows | 4 rows | `rc=0` — **genuine pass** |
| `encounters_*` (merged) | 0 rows | 0 rows | `rc=0` — **trivial**, not evidence |

So the **incrementality** comparison is validated on real data; the **non-determinism** comparison is not yet exercised.

**Why merged is empty — and it is CORRECT behaviour, not a bug.** The window's raw encounters are all one vessel pair (`663092000` ↔ `100900000`, recorded in both orderings, hence 4 rows for 2 encounters), lasting 900–1310 minutes — far above the 120-minute threshold. Both seg_ids ARE present in `segment_info`, so the vessel-id join is not the cause. But **one of the two segments is flagged `overlapping_and_short`** in `segs_activity`, so the `--bad_segs_table` filter drops it — and an encounter needs both vessels. `create` does not apply that filter, `merge` does; hence 4 raw, 0 merged.

To validate the merged comparison, a window is needed whose encounters involve **two good segments**. That is a cohort-selection problem, not a workflow problem.

## Dataflow run (2026-07-30) — the blocker is BROADER than first documented

A laptop-submitted **Dataflow** run (`--runner dataflow --modes 1_bf`, 2020-01-19..22, job `2026-07-30_06_57_39-11212595608470669383`) failed — and pins down exactly who is blocked:

```
POST .../projects/world-fishing-827/datasets                       -> 403 Forbidden
GET  .../datasets/beam_temp_dataset_0edb68e51a694395ae0cc867b9a32506 -> 404 Not Found
JOB_STATE_FAILED
```

The 403 is raised **from inside the Dataflow job**, i.e. by the WORKERS running as `automated-testing@` — not by the submitter. **Correction to the original audit**, which implied this was specific to the Cloud Build path: it applies to **any run whose Dataflow workers are `automated-testing@`**, including a laptop-submitted one. The DirectRunner laptop path works only because the temp dataset is then created under the *user's* ADC, which can create datasets.

**What this run DID validate** (none of it reachable from DirectRunner):

- Dataflow **submission** from the laptop container works.
- **Dataflow workers can pull the published image** from `gfw-int-infrastructure` as `automated-testing@` — a real unknown beforehand (same-project residency is not sufficient for SA pulls, and prod runs this image under a different SA). The workers got far enough to make BQ API calls, which requires the SDK harness to have started.
- The `--sdk_container_image` + boot-entrypoint arrangement works with one image serving as both submitter and worker.
- Placement options (region, subnetwork, temp/staging buckets) are accepted, and `--wait_for_job` propagates failure correctly.

So the **only** thing standing between dit and a working cloud encounters run is the temp dataset. Three possible fixes:

1. **Upstream (preferred, matches pipe-anchorages)** — expose `--temp_dataset` so dit can point it at `tech_great_expectations`, where the SA already has `bigquery.tables.create`.
2. **IAM** — grant `automated-testing@` `bigquery.datasets.create` on `world-fishing-827`. Deliberately narrow today (see CLAUDE.md § canonical-dataset policy), so this would be a policy change, not just a grant.
3. **Overlay image (done 2026-07-30, unblocks now)** — see below.

### Resolution: overlay image (2026-07-30)

The patch is written but unmerged, so waiting on option 1 would idle the whole cloud path. Instead the patch is layered over the published image:

```
gcr.io/world-fishing-827/dit/encounters:v4.4.0-temp-dataset-d2536aaf
sha256:5edc1f29b0197cacfd71eeb9ef87bf39a70d3b38b64f4ced4796be4c919ce964
```

`FROM` the published `v4.4.0`, five patched `.py` files `COPY`d over `site-packages/pipe_encounters/`. Base image, Beam 2.71.0 and the `/opt/apache/beam/boot` ENTRYPOINT are all inherited unchanged, so it still serves as both submitter and `sdk_container_image`. Build is in `scratchpad/encounters-overlay/`; it asserts the patched modules import and the flag parses, so a broken overlay fails the build rather than shipping.

**The patch was applied to the v4.4.0 sources extracted from the image, not to the local 4.3.2 checkout** — different package name and formatting. The local checkout carries the same change on branch `dit-temp-dataset-support` (commit `298f30f`, unpushed) for the eventual upstream PR; its `ReadSources` is structurally identical to v4.4.0's, so it should port mechanically.

Mechanism: Beam's `_CustomBigQuerySource._setup_temporary_dataset()` returns early when `temp_dataset` is set, so nothing is created. Note `ReadFromBigQuery` has **no named `temp_dataset` parameter** — it captures the kwarg and forwards it to the source at `expand()` time; a test in both repos pins that, so a Beam upgrade that stopped forwarding fails loudly rather than silently reverting to dataset creation.

Retire the overlay the moment upstream lands and publishes a new tag.

**Scope correction worth keeping.** The original framing here called this a *Cloud-Build* blocker. It is not: the 403 is raised from **inside the Dataflow job**, i.e. by workers running as `automated-testing@`, so it breaks any run with those workers. Laptop DirectRunner escapes it only because the dataset is then created under the user's own ADC.

### Dataflow smoke against the overlay — VERIFIED 2026-07-30

`--runner dataflow --modes 1_bf --start 2020-01-19 --end 2020-01-22`, overlay as both `--image-tag` and `--worker-image`. Both jobs reached `JOB_STATE_DONE` (last: `2026-07-30_08_12_48-14117319945993493700`). **No `POST /datasets` 403** — the blocker is gone, with workers running as `automated-testing@` exactly as before.

Row counts, checked rather than inferred from exit 0:

| table | rows | distinct `encounter_id` | window |
|---|---|---|---|
| `raw_encounters_tempds1_1_bf` | **4** | 4 | 2020-01-20 08:50 → 2020-01-21 00:00 |
| `encounters_tempds1_1_bf` | **0** | 0 | — |

Non-zero raw means Beam's load path genuinely executed — this is *not* the zero-row pass that made an earlier smoke look green while proving nothing.

**Merged = 0 is correct, and verified rather than assumed.** All four raw encounters involve the same segment `100900000-2020-01-01T00:02:47.000000Z-1`, which carries `overlapping_and_short = true`; `_bad_segs_sql` is literally `SELECT DISTINCT seg_id … WHERE overlapping_and_short`, so `merge_encounters` drops all four. Not a regression, and not a bug.

**But it caps what this cohort can validate.** Every encounter in the window comes from a single vessel pair, one of whose segments is flagged bad — so the *merged* comparison (the non-determinism signal the owner wants kept) can only ever be 0-vs-0 here, which is trivially identical. The *raw* comparison is viable: 4 real rows, so a two-mode run gives a genuine incrementality check. Concrete reinforcement of the cohort-sparsity finding above and of the `dit.cohorts` case.

## VMS A/B of the interpolation fix — DIFFERENCES FOUND (2026-07-30)

The positive control for the null recorded above. Same two overlay images, same
`encounter_id` comparison, but run through `workflows/encounters/vms.py` on a
laptop (`--runner docker`, DirectRunner in-container) against 4 co-located VMS
vessel pairs over `2026-06-01..07`.

**Vessel selection was the load-bearing step.** Filtering to vessels that are
*individually* co-located would not work — their partners can fall outside the
filter, yielding zero encounters. The probe selected co-located **pairs** where
at least one member carries exact-hour gaps; the top pair had 920 co-located
hours and 118 gaps of exactly 3600.000s.

| table | rows | value diffs | affected |
|---|---|---|---|
| `raw_encounters` | 60 v 60, all keys matched | 4 | 6.67% of encounters |
| `encounters` (merged) | 6 v 6, all keys matched | 1 | 16.7% |

Both tables non-empty, so unlike the AIS run *both* comparison signals were live.

**The diff is exactly the fix's signature, not noise.** `vessel_1_point_count` /
`vessel_2_point_count` move (+2 raw, +6 merged) — interpolating across a
previously-skipped exact-hour gap adds resampled points — and the aggregates
computed over those points follow: `mean_latitude`, `mean_longitude`,
`median_distance_km`, `median_speed_knots`. Everything defining the encounter's
identity and extent is **identical**: `start_time`, `end_time`, `start_lat/lon`,
`end_lat/lon`, `vessel_*_seg_id(s)`. So the fix refines point sampling *within*
existing encounters rather than creating or destroying any — and all 60/6 keys
matched, so there are no only-in-A/only-in-B rows.

Incidentally this also **fails to reproduce the pre-registered hypothesis**
about `vessel_N_seg_ids[0]` ordering non-determinism: the repeated fields
compared identical across both runs. Not disproven (one small sample, one
window), but no evidence for it yet.

**What this pair of runs establishes methodologically.** AIS staging said
IDENTICAL; VMS said DIFFERENCES FOUND, for the *same* code change. The AIS
verdict was truthful and worthless — the target could not reach the behaviour.
Choosing a target that *can* discriminate is part of the test, not a detail.

## Cohort sparsity — a constraint on encounters testing

`pipe_ais_test_202408290000` carries only **67–72 distinct vessels/day** (~47k msgs/day). Encounters need two vessels within 0.5 km for 120+ minutes, so they are rare here: a coarse proximity probe over January 2020 found **2–3 co-located pairs/day**, and **2020-01-01 none at all**. Densest window found: **2020-01-19..22**.

**This puts the default `--start 2020-01-01 / --end 2020-12-31` in question** — not because the window misses the data year, but because a mode-equivalence comparison over two empty tables passes trivially. Encounters may need a denser cohort than the shared AIS staging one; this is concrete input to the `dit.cohorts` proposal in [`workflow-orchestration-2026-06.md`](workflow-orchestration-2026-06.md).

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
3. File the upstream asks. `--temp_dataset` is ~~pending~~ **written** (`encounters_pipeline@dit-temp-dataset-support`, local/unpushed) — still needs a PR. None-safe labels and the missing `.result()` in `writers.py delete_rows` are still unfiled.
4. ~~Cloud smoke once the `--temp_dataset` ask lands upstream~~ — **done 2026-07-30 against the overlay image**, see below.
5. `vms.py` sibling; cache integration; then optionally the publication half if dit's scope ever extends there.
