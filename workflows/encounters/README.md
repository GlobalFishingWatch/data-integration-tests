# `workflows/encounters/`: encounters (pipe-encounters)

DIT's tests of the encounters pipeline in `encounters_pipeline` (pipe-encounters). This page covers what is specific to encounters. Framework concepts (drift check, orchestration declarations, run modes, source targets) are explained in the top-level [`README.md`](../../README.md) and [`docs/architecture.md`](../../docs/architecture.md). The onboarding record of July 2026, with the smoke-by-smoke findings, is [`docs/encounters-onboarding-2026-07.md`](../../docs/encounters-onboarding-2026-07.md).

> **DIT is one CLI generation behind production.** DIT drives pipe-encounters **v4.4.0** (`create_raw_encounters` / `merge_encounters`). Production's v5 DAGs, AIS and VMS, run **v5.0.0** (`partial-encounters` / `encounters`), a rewrite with new commands, flags and date semantics. A DIT result says something about v4.4.0, not about what production runs today. See [Behind production](#behind-production).

## The pipeline

Two steps, the generation half of the encounter events. The product step (`product_events_encounter`) belongs to pipe-events and is out of scope.

| step (v4.4.0) | reads | writes | how it writes |
|---|---|---|---|
| `create_raw_encounters` | messages over `[start, end]` | raw encounters | deletes `DATE(start_time) BETWEEN start AND end`, then appends |
| `merge_encounters` | raw encounters from the history start, `segment_info`, spatial measures, bad segments | encounters | replaces the whole table (`WRITE_TRUNCATE`) |

Both date bounds are **inclusive** in v4.4.0: both queries filter with `BETWEEN` (read in the v4.4.0 source).

## Versions

| | version | commands | source |
|---|---|---|---|
| Production, AIS v5 DAG (`core.ais.v5`) | **v5.0.0** | `partial-encounters`, `encounters` | drift check at composer `origin/main` 85700ebc3, 2026-11-25 |
| Production, VMS v5 daily DAG | **v5.0.0** | same | same |
| AIS v3 DAG configuration | v4.4.0 | `create_raw_encounters`, `merge_encounters` | `Versions.DETECT_ENCOUNTERS` in composer's `dags/core/ais/v3.py`, same commit |
| **DIT** | **v4.4.0** CLI | `create_raw_encounters`, `merge_encounters` | `ais.DEFAULT_WORKER_IMAGE` |

DIT's default `--image-tag` and `--worker-image` are the **stock published v4.4.0 image**. That image has no `--temp_dataset` option, and DIT always passes one, so a run on the defaults fails at once with an unknown flag. This is deliberate: it fails loudly instead of letting Beam try to create a dataset. Every real run passes DIT's overlay image as both images (below).

The drift spec (`composer_drift.py`) compares both workflows with the v5 DAGs and is marked `in_sync=False`, so the drift is reported and does not fail the check. On 2026-11-25 it reports, for both steps of both workflows: version `v4.4.0 -> v5.0.0`, a different command, and flag differences that are indicative only.

### The overlay image

```
gcr.io/world-fishing-827/dit/encounters:v4.4.0-temp-dataset-d2536aaf
sha256:5edc1f29b0197cacfd71eeb9ef87bf39a70d3b38b64f4ced4796be4c919ce964
```

Built on 2026-07-30 `FROM` the published v4.4.0, with five patched files copied over the installed package so that both steps pass a named staging dataset to Beam. Same Beam (2.71.0) and entrypoint, so one image serves as submitter and worker. Without it, a Dataflow job whose workers run as `automated-testing@` fails creating `beam_temp_dataset_*`.

**The build recipe is not in this repository.** It was built from a scratch directory, and the patch was never committed here (the same change sits on an unpushed local branch of `encounters_pipeline`, against v4.3.2). The image can be pulled, not rebuilt from DIT. Unlike pipe-anchorages, there is no `docker/overlays/` directory for encounters.

## Workflows

| file | source target | default window | use it when |
|---|---|---|---|
| `ais.py` | AIS staging cohort `pipe_ais_test_202408290000` (2020 data, 142 vessels) | 2020-01-01 to 2020-12-31 | a cheap check of re-run behaviour |
| `vms.py` | VMS production tables | 2026-06-01 to 2026-06-07 | behaviour the AIS cohort cannot reach (dense tracks, hourly reporting) |
| `composer_drift.py` | drift specs of both | | to see how far DIT is from production |
| `orchestration.py` | per-step parallel-loading declarations | | read only; no encounters workflow has a parallel mode |

`vms.py` reuses `ais.run(args)` and owns only its defaults and help. A test checks that its parser gives `run` every attribute it reads.

There is no cross-version wrapper and no run cache for encounters. To A/B two images, run the workflow twice with distinct `--binding-name` values (they go into the Dataflow job name and the `dit_binding` label; without them two runs with the same `--experiment-id` collide on the job name) and compare the tables yourself.

## Modes and comparisons

| mode | jobs per step |
|---|---|
| `1_bf` | one over `[start, end]` |
| `2_bfd` | one over `[start, end - tail_days]`, then one per tail day |
| `3_bftruncate` | one over `[start, end]`, then the same daily tail again |

In every slice, `create_raw_encounters` runs over the slice and `merge_encounters` rebuilds from `--start` to the slice's end, as production does. `--modes` selects a subset; `--parallel` runs the modes at once.

Both output tables are compared pairwise on `encounter_id`, and they catch different things:

| table | built | catches |
|---|---|---|
| `raw_encounters_<suffix>_<mode>` | slice by slice | **incrementality bugs**: a re-run that duplicates, drops or fails to replace rows. This is the discriminating comparison for re-run logic. |
| `encounters_<suffix>_<mode>` | rebuilt every call | **non-determinism**: the same inputs giving different output. Blind to incrementality by construction. |

A green merged result means "the recompute is reproducible", not "the modes agree".

`encounter_id` is `md5("encounter|<seg_1>|<seg_2>|<start_time>")`: unique, but a content hash. A changed encounter gets a new id, so table-check reports it as one row only in A and one only in B, not as a field difference. Do not read those rows as missing data.

## Sources and guards

| input | `ais.py` default | `vms.py` default | flag |
|---|---|---|---|
| messages | cohort `_internal.messages_positions` | VMS `messages_positions` | `--source-messages-fqn` |
| `segment_info` | cohort `_published.segment_info` | VMS `segment_info` (a third project) | `--source-segment-info-fqn` |
| bad segments | cohort `_published.segs_activity` | VMS `segs_activity` | `--source-segs-activity-fqn` |
| spatial measures | a static production reference table (not in the cohort) | the VMS version production uses | `--spatial-measures-fqn` |

`--ssvid-filter` (a subquery, a comma-separated list or `@path`) is passed to **both** steps. It is the cheapest way to shrink a run, and the preferred one on VMS.

Detection parameters default to production's: `--max-encounter-dist-km 0.5`, `--min-encounter-time-minutes 120`, `--min-hours-between-encounters 4`.

Checked before any cloud call: `--start` after `--end`, a negative `--tail-days`, and a tail longer than the window. An unknown mode in `--modes` is refused at parsing.

What the workflow does that production's DAG does not need:

- **Pre-creates both output tables** of every selected mode, MONTH-partitioned and clustered on `start_time`. Both v4.4.0 steps read their table's metadata before Beam creates it, which fails on a new table. The schemas are read out of the image, so they cannot drift from it.
- **Always passes `--labels`**, on every runner: v4.4.0 fails without them.
- **Sets `GOOGLE_CLOUD_PROJECT`** in the container: Beam's BigQuery write inside the container does not see `--project`.

Workers: `--max-num-workers` 50 (production's cap on the AIS DAGs), e2-highmem-4 and 50 GB on both steps (production's), private IPs. A Dataflow run on the staging cohort logs a hint to choose smaller machines.

### VMS specifics

- **The sources are production tables.** `messages_positions` is about 503 GB, but it is partitioned by month: the 7-day default reads less than the AIS default does. Widen the window on purpose, and prefer `--ssvid-filter`.
- **Three projects in two organisations.** Plain reads across organisations work; copies or snapshots into DIT's dataset would not, so `vms.py` pins nothing.
- **Not verified:** that `automated-testing@` can read the VMS projects. It has no direct binding there; access may come through a group. If a Dataflow run fails reading a source, the fix is an IAM grant. Laptop runs use the operator's credentials.

## Commands

```bash
DIT=<path to this repository>
IMG=gcr.io/world-fishing-827/dit/encounters:v4.4.0-temp-dataset-d2536aaf
cd "$PROJECTS/encounters_pipeline"

# Densest window of the AIS cohort; raw comparison is real, merged is 0 vs 0 here
dit run "$DIT/workflows/encounters/ais.py" --runner dataflow \
    --image-tag "$IMG" --worker-image "$IMG" \
    --modes 1_bf,2_bfd --start 2020-01-19 --end 2020-01-22 --tail-days 1 \
    --experiment-id enc-smoke

# VMS week, a few co-located vessel pairs
dit run "$DIT/workflows/encounters/vms.py" --runner dataflow \
    --image-tag "$IMG" --worker-image "$IMG" \
    --ssvid-filter '<ssvids>' --experiment-id enc-vms

# Local DirectRunner, same image; the staging dataset is then created with your credentials
dit run "$DIT/workflows/encounters/ais.py" --runner docker \
    --image-tag "$IMG" --worker-image "$IMG" \
    --modes 1_bf --start 2020-01-19 --end 2020-01-22
```

Always pass the image explicitly, as both flags:

- The submitter (`--image-tag`) must accept `--temp_dataset` too, so the overlay is needed there, not only on the workers.
- An explicit image also switches off the shared auto-build. With the default image and an unreviewed checkout (dirty, a snapshot, or not merged into `origin/main`), DIT would build a plain image of the checkout for the workers only. That image has no overlay, and a current checkout holds v5.0.0 code, which does not accept the v4.4.0 command lines DIT emits.

`--build-from-source` uses the compose service `pipe_encounters`, which exists in `encounters_pipeline` only up to v4.3.2. Against a checkout of v4.4.0 or later it fails, and even on v4.3.2 it tests older code than v4.4.0 (Python 3.8, package `pipeline`).

`make dit-cloud` has no encounters entry and no Beam pin for it. No encounters run has gone through Cloud Build.

### Reading the result

- Each pair logs `compare [raw_encounters / detects incrementality bugs] ...` and `compare [encounters (merged) / detects non-determinism] ...` with `rc`. Non-zero is a difference; the workflow exits non-zero.
- A single mode logs "no pair to compare" and exits 0. That is not a pass.
- Check the row counts before reading IDENTICAL as a pass. The AIS cohort has 2 to 3 co-located vessel pairs a day and many empty days; two empty tables compare identical.

### Cross-tenant self-encounters (VMS)

The same vessel reported by two VMS tenants gets two `vessel_id`s and `ssvid`s, and can "encounter itself" (for example the same shipname under two flags). On a full-year 2025 VMS run, about 3.9% of encounters had the same shipname on both sides, and about 96% of those were across tenants. This is in the pipeline, not caused by DIT. When counting encounters, report with and without `v1_shipname = v2_shipname AND v1_tenant <> v2_tenant`.

## Run history

Recorded in this repository. All on 2026-07-30 or 31, on v4.4.0; none in Cloud Build.

| what | where | result |
|---|---|---|
| seven AIS smokes, published image | laptop, DirectRunner | six workflow defects found and fixed; attempt 7 (`1_bf,2_bfd`, 2020-01-19..22): raw 4 vs 4 rows identical, merged 0 vs 0 |
| AIS `1_bf`, published image | Dataflow job `2026-07-30_06_57_39-11212595608470669383` | failed: workers denied creating the staging dataset |
| AIS `1_bf`, overlay image | Dataflow, last job `2026-07-30_08_12_48-14117319945993493700` | done: raw 4 rows, merged 0 (one segment is a bad segment, correctly dropped). One mode, no comparison |
| VMS A/B of the interpolation-boundary fix, 4 vessel pairs, 2026-06-01..07 | laptop, DirectRunner | DIFFERENT, as the fix intends: raw 60 vs 60 rows, 4 changed; merged 6 vs 6, 1 changed. The same fix on AIS staging was IDENTICAL: the cohort cannot reach that case |
| VMS, all vessels, 2025, `1_bf`, A/B | Dataflow, about 76 min | source of the self-encounter figures above |

The merged comparison has never been exercised on AIS staging with rows in both tables.

## Behind production

DIT drives v4.4.0; production runs v5.0.0. What differs, from the drift check and from reading v5.0.0 (not run):

- **Commands and flags.** `partial-encounters` and `encounters`; hyphenated flags (`--bq-in-*`, `--bq-out-*`, `--start-date`, `--end-date`); `--labels k=v k=v`.
- **Dates.** `--end-date` is exclusive in v5 ("Date ranges start at `--start-date` and end before `--end-date`"), as in pipe-anchorages v5.
- **Inputs.** VMS production passes `--max-gap-seconds-inclusive` to step 1 and no bad-segments filter to step 2; DIT's VMS workflow passes one. `--bad-segs` is optional in v5.
- **Image.** Python 3.12, Beam 2.76.0, a multi-stage Dockerfile with a Beam boot entrypoint; the compose services changed again.
- **Still no staging-dataset option.** v5.0.0 reads BigQuery through gfw-common's `ReadFromBigQuery.from_query` in both pipelines, with no temp dataset.

What catching up would take:

1. Port the two argv builders to the v5 CLI, with the exclusive end (`end + 1 day`, as `port_visits/ais.py` does) and the merge step's history start.
2. A source-built temp-dataset overlay for v5, like `docker/overlays/pipe-anchorages-temp-dataset/`, patching the two reads, with a self-check. The patch would live in this repository this time.
3. Replace the schema bootstrap: v5 ships its schemas as JSON, and `pipe_encounters.schemas.output`, which the bootstrap imports, no longer exists. v5 has a table configuration per pipeline that may create the tables itself (not checked).
4. Decide the VMS differences (`--max-gap-seconds-inclusive`, the bad-segments filter): mirror production or accept with a reason.
5. Make the default image the overlay of v5.0.0, guard unreviewed code as port visits does, rename the steps in `orchestration.py`, and set the drift specs to `in_sync=True` once they pass.
6. Rerun the smokes, AIS and VMS, on Dataflow.

Until then, use these workflows only for questions about v4.4.0 behaviour, or as a template.

## Known limits and open items

- One generation behind production (above).
- The overlay image cannot be rebuilt from this repository.
- No run cache, no cross-version wrapper, no parallel-slices mode.
- The AIS cohort is too sparse for the merged comparison; a denser cohort would be needed (the `dit.cohorts` proposal).
- VMS read access for `automated-testing@` is not verified.
- Upstream items found during onboarding (labels without a `None` guard, a pre-write delete that is not awaited) were found in v4.4.0 and not checked against v5.0.0.
