# Production baseline: a candidate against production's own output (2026-10)

**Jira:** [PIPELINE-4703](https://globalfishingwatch.atlassian.net/browse/PIPELINE-4703) (epic [PIPELINE-4698](https://globalfishingwatch.atlassian.net/browse/PIPELINE-4698)).
**Status:** code, unit tests and dry runs against production's 2026-10-20 run (2026-10-23): one on the laptop and one in Cloud Build as `automated-testing@` (build `947240a8`). No live run yet.
**Entry point:** `workflows/port_visits/cross_version_ais.py --baseline-production <target> --production-date D`.

## What it does

An ordinary cross-version run executes two bindings, for example main and a PR, on pinned inputs, then diffs their outputs. A production-baseline run executes **only the candidate**. It compares the candidate's output with the table production actually wrote, and the candidate reads exactly the inputs production read. Production is not re-run.

This makes the test:
- cheaper, because it runs one binding instead of two;
- more direct, because it compares with what production published, including differences between the deployed version and `main`. Production pins pipe-anchorages v4.6.4, 17 merged PRs behind `main` as of 2026-10-22.

## How the pins are chosen

Production reprocesses daily, so its inputs and its output both move. The comparison holds only if the inputs are pinned as of when production read them, and the output as of when production finished writing it.

1. **Find production's run.** `dit.production.find_dataflow_job` lists the Dataflow jobs named `<prefix>YYYYMMDD` for day D and takes the latest one in state `Done`; retries and cleared tasks reuse the name.
   - **T0** is the job's creation time. It precedes every BigQuery read.
   - **T1** is the time the job reached `Done`. It follows the output write.
   - Table metadata cannot stand in for either. The production step updates its output table's description, and with it `lastModifiedTime`, at job **start**, before it writes.
   - `--production-inputs-as-of` and `--production-output-as-of` override T0 and T1. With both set, no job lookup happens.
2. **Pin with time-travel views.** `dit.bq.pin_view_into_experiment` creates `CREATE VIEW IF NOT EXISTS … AS SELECT * FROM src FOR SYSTEM_TIME AS OF TIMESTAMP(t) WHERE <window or TRUE>` in `tech_great_expectations`:
   - inputs as of T0: the thinned table over `[history start, D]`, plus `segment_info` and `segs_activity` whole;
   - production's output as of T1.

   Views copy nothing and work across organisations. The name carries source, window and `as_of`, so a view of a given name always has the same definition, and reruns are idempotent. They are labelled `dit_artifact=source_pin_view` and expire after `--pin-expiration-days` (default 7). Snapshots are not an option: BigQuery refuses them across organisations, and no DIT identity has `bigquery.tables.deleteSnapshot` on `tech_great_expectations`. Copies are not an option either, because they pin only "now".
3. **Run the candidate** as production runs: `--steps port_visits` on the pinned thinned table, `--modes 1_bf`, `--start` at the target's history start and `--end D`. These flags are fixed: left unset they take those values, and any other value is an error, because a different run cannot match production's output. Open visits get their `end_timestamp` and confidence from the end date, so D must match too.

   Every binding also runs on production's worker sizing: the target's `worker_machine_type` and `disk_size_gb` (e2-highmem-4 and 50 GB for `ais-v5-datalake`), forwarded to `ais.py` as `--worker-machine-type` / `--disk-size-gb`. They equal `ais.py`'s own production-mirrored defaults today; the target pins them for this production run. An explicit value of either on the command line wins, field by field. Sizing changes speed and out-of-memory behaviour, never output, so it is not fixed like the flags above. Workers get private IPs, as in every DIT Dataflow job. Since 2026-10-23.
4. **Diff** each binding's `port_visits_<exp>-<binding>_1_bf` against the production-output view on `visit_id` (table-check `summary`). The binding is table A and production table B. With several bindings, the usual pairwise diffs run as well.

## Guards (before anything is created)

- **Time travel.** `dit.bq.check_time_travel` reads each source dataset's `max_time_travel_hours` and refuses unless every pin stays readable for `--production-margin-hours` more (default 6). The run logs the earliest deadline. Past it, queries through the views fail.
- **Inputs changed during production's run.** If an input's `lastModifiedTime` falls in (T0, T1], which state production read is unknown, and the run refuses. A change after T1 is fine, since time travel returns the T0 state. Only the latest modification is visible, so an earlier change inside the run followed by a later one goes unnoticed.
- **Cost.** Each view's full read is dry-run and refused above `--pin-max-gb` (default 1000). Full-history thinned input exceeds that, so a live run needs an explicit cap (see Costs).
- **Fixed inputs.** `--thinned-message-table`, `--source-*-fqn`, a non-default `--source-dataset-stem` and `--reuse-pin` are rejected: the target defines its inputs, and views need no reuse.

## Targets

One target today, hand-written from `composer-dags-production` origin/main 0a644da (`dags/core/ais/v5.py`, `gfw/pipes/core/port_visits/v4.py`). Reading targets from composer as data belongs to [PIPELINE-4700](https://globalfishingwatch.atlassian.net/browse/PIPELINE-4700).

**`ais-v5-datalake`**: pipe-anchorages v4.6.4, v4 CLI. Dataflow in `world-fishing-827`, `us-central1`, 100 × e2-highmem-4 with 50 GB disks and private IPs (the sizing from the rendered snapshot `tests/render_dags/snapshots/core_ais_v5.py`, task `port_visits.detect_port_visits`). Job name `core-ais-v5--port-visits-detect-port-visits--YYYYMMDD`.

| role | table | time travel | full read (2026-10-23) |
|---|---|---|---|
| thinned input | `gfw-int-ais-datalake.port_visits_v1.raw_port_events` | 168 h | 1150.5 GB |
| segment info | `global-fishing-watch.pipe_ais_v3_published.segment_info` | **48 h** | 69.6 GB |
| bad segments | `gfw-int-ais-datalake.summarized_positions_v1.segs_activity` | 168 h | 103.4 GB |
| output | `gfw-int-ais-datalake.port_visits_v1.port_visits` | 168 h | 443.0 GB |

The production output's schema (22 fields, MONTH-partitioned and clustered on `end_timestamp`) is identical to DIT's v4.6.4 and PR #176 outputs from the 2026-07..09 runs.

All four tables are rewritten in place, not recreated: on 2026-10-23 they had been created between 2025-07 and 2026-07 and modified that day. Time travel cannot reach before a table's creation, so if production ever switched to recreating a table, its pins would only last until the next recreation.

## Timing

`segment_info`'s 48 h window decides the deadline: T0 + 48 h. Production's job for D runs about three days after D, recently starting around 07:46Z and finishing around 09:05Z. The 2026-10-20 run (job `2026-10-23_00_46_26-2424624824928045608`) can be pinned until 2026-10-25 07:46Z, minus the margin. If that is too tight, copy the inputs manually while they are still readable and run an ordinary cross-version run on the copies instead.

## Costs

For one candidate run over the full history, measured or estimated 2026-10-23:
- **BigQuery reads through the views**: ~1.32 TB (the table above, inputs only).
- **Dataflow**: about one production run. Production's 2026-10-20 job took 78 min at up to 100 workers. Pass `--max-num-workers 100` to match; the workflow default stays 4.
- **Diff**: table-check reads both ~440 GB tables.

A live run needs the owner's go-ahead on the cap (`--pin-max-gb 1300`) and the worker count.

## Running it

Dry run on the laptop: free, read-only, creates nothing. It resolves the job, prints T0, T1, the deadline, view names, estimates and the binding's `ais.py` argv:

```bash
CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT= dit run workflows/port_visits/cross_version_ais.py \
    --experiment-id pipeline-4690-prod \
    --baseline-production ais-v5-datalake --production-date 2026-10-20 \
    --binding pr=14b4be8 \
    --binding-worker-image pr=gcr.io/world-fishing-827/dit/pipe-anchorages:pipeline-4690-14b4be8-temp-dataset-905dc761 \
    --binding-image-tag pr=gcr.io/world-fishing-827/dit/pipe-anchorages:pipeline-4690-14b4be8-temp-dataset-905dc761 \
    --pin-max-gb 1300 --dry-run \
    --pipeline-cli v5 --max-num-workers 100 --runner dataflow
```

Live, in Cloud Build. `DIT_REF` is needed until this lands on `main`:

```bash
IMG=gcr.io/world-fishing-827/dit/pipe-anchorages:pipeline-4690-14b4be8-temp-dataset-905dc761
CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT= make dit-cloud PIPELINE=anchorages_pipeline \
    PROJECTS=<fresh clone parent> DIT_REF=feat/production-baseline BEAM_VERSION=2.76.0 \
    WORKFLOW=workflows/port_visits/cross_version_ais.py \
    ARGS="--experiment-id pipeline-4690-prod --baseline-production ais-v5-datalake \
          --production-date 2026-10-20 --binding pr=14b4be8 \
          --binding-image-tag pr=$IMG --binding-worker-image pr=$IMG \
          --pipeline-dir /workspace --pipeline-cli v5 --max-num-workers 100 \
          --pin-max-gb 1300 --runner dataflow"
```

Check `gcloud builds list` after submitting; a hung upload can retry and create a duplicate build.

## Reading the result

table-check's post-tolerance counts are inflated when keys repeat: its `_l2` step re-joins on the key only, so a key with n rows on each side yields n² pairs. Production's v4.6.4 output has duplicate `visit_id`s, which is the behaviour PIPELINE-4690 removes, so expect DIFFERENT. Break the result down with SQL rather than reading table-check's row counts. A fix is drafted for the table-check maintainer; per the working agreement, comparison features go upstream, not into `dit.compare`.

The breakdown used for the 2026-09 and 2026-07..09 runs, with `PR` the candidate's output and `PROD` the production-output view:

```sql
WITH a AS (SELECT visit_id, vessel_id, duration_hrs FROM `PROD` WHERE end_timestamp >= TIMESTAMP('1970-01-01')),
     b AS (SELECT visit_id, vessel_id, duration_hrs FROM `PR` WHERE end_timestamp >= TIMESTAMP('1970-01-01')),
     per_side AS (
       SELECT 'production' AS side, COUNT(*) AS n_rows, COUNT(DISTINCT visit_id) AS n_ids,
              COUNTIF(duration_hrs = 0) AS zero_dur FROM a
       UNION ALL
       SELECT 'pr', COUNT(*), COUNT(DISTINCT visit_id), COUNTIF(duration_hrs = 0) FROM b),
     only_a AS (SELECT * FROM a WHERE visit_id NOT IN (SELECT visit_id FROM b)),
     only_b AS (SELECT * FROM b WHERE visit_id NOT IN (SELECT visit_id FROM a))
SELECT side, n_rows, n_ids, n_rows - n_ids AS duplicate_rows, zero_dur, CAST(NULL AS INT64) AS vessels FROM per_side
UNION ALL SELECT 'only_in_production', COUNT(*), COUNT(DISTINCT visit_id), NULL, COUNTIF(duration_hrs = 0), COUNT(DISTINCT vessel_id) FROM only_a
UNION ALL SELECT 'only_in_pr', COUNT(*), COUNT(DISTINCT visit_id), NULL, COUNTIF(duration_hrs = 0), COUNT(DISTINCT vessel_id) FROM only_b
ORDER BY side
```

The `end_timestamp` filter satisfies tables that require a partition filter. Reading through the view keeps working only until the deadline the run logged.

## Out of scope

- An automatic trigger after the production DAG.
- The core AIS v3 target (`global-fishing-watch.pipe_ais_v3_published.port_visits`, 48 h window).
- Comparing step 1 (`thin_port_messages`) per day.
- An automated copy fallback when the time-travel window is too short.
