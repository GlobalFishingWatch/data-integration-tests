# identity_match_key workflow — setup and run instructions

End-to-end steps to A/B-test the gpsdio-segment identity-match-key change
(reducing `match_key = (transponder_type, receiver_type, source)` to
`(transponder_type,)`) via this workflow.

**Status, 2026-11-19.** The workflow's command lines match what production
runs (pipe-segment v5.0.3, the six tasks of the AIS v3 DAG's segment group;
see [`composer_drift.py`](composer_drift.py)). That was established by reading
code. The only recorded run is one laptop smoke of the `segment` step on
2026-06-03. Two things still stand between this file and a full run:

- **Dataflow as the test service account.** pipe-segment has no way to name
  the dataset Beam stages query results in, and `automated-testing@` may not
  create one. Sections 6 and 7 need an overlay image first, as port visits and
  encounters have. Inferred from the code; not tried.
- **Nothing here has run end to end.** Treat the expected results below as
  expectations.

Source pinning no longer stands in the way. The workflow pinned its source
with a BigQuery snapshot, which no DIT identity can create in
`tech_great_expectations`. Since 2026-11-19 it pins by copy
([PIPELINE-4705](https://globalfishingwatch.atlassian.net/browse/PIPELINE-4705));
see section 5. The copy statement was accepted by BigQuery dry runs. No copy
has been created yet.

## 1. Install dit

```bash
cd ~/git/data_integration_tests
make install
```

That is enough to run the workflow: it runs pipe-segment in its container and
never imports it. `make install-pipe-segment` adds an editable install of
`$(PROJECTS)/pipe-segment` (a sibling checkout by default; override via
`PROJECTS=…` or `.envrc`) for stepping through its source. pipe-segment v5.0.3
needs Python 3.12 or newer; the target pins apache-beam 2.71.0, the version
v5.0.3 pins.

For the laptop docker path, the auth named volume needs to be populated **once** before any docker run:

```bash
docker volume create gcp
```

Then log in through a `gcloud` compose service that mounts that volume.
pipe-segment v5.0.3's `docker-compose.yaml` has none (its services are `dev`,
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
git checkout -b experiment/identity-match-key-transponder-only v5.0.3
```

At v5.0.3 the dependencies live in `pyproject.toml`; `requirements.txt` is
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

## 4. Image resolution (automatic — no manual builds needed)

The workflow resolves a per-binding image via `dit.workflow.resolve_run_context`:

- **"before=v5.0.3"** binding: reviewed code at the default tag → docker
  runner pulls `us-central1-docker.pkg.dev/gfw-int-infrastructure/core/pipe-segment:v5.0.3`
  directly. No build.
- **"after=experiment/…"** binding: unreviewed code → `ensure_pipeline_image`
  auto-builds `gcr.io/world-fishing-827/dit/pipe-segment:dit-<commit>` via
  kaniko, idempotent (skipped if the tag already exists).
- **Explicit override**: `--image-tag <FQN>` is respected for all bindings.
- **Laptop fast-iter**: `--build-from-source` bypasses both the canonical
  pull and the kaniko build — compose builds the `dev` service from the
  worktree's working tree.

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

**Satellite offsets.** Production's `segment` also reads satellite positions
and writes clock offsets. No run here does. `--include-satellite-offsets`
asked for it and is refused at argument parsing, before any cloud call
(PIPELINE-4705). It could not work: the satellite positions are date-sharded
tables in another organisation, which DIT does not support, and the offsets
query reads its message source through a table wildcard, which DIT's
partitioned source does not match. Whether the option is deleted is an open
decision.

## 6. DirectRunner smoke (laptop, fast)

Single-binding orchestration check against `v5.0.3`, 1 day, a handful of
vessels, source = staging cohort. Start with the `segment` step:

```bash
cd ~/git/data_integration_tests
dit run workflows/pipe_segment/identity_match_key.py \
    --experiment-id pipe-segment-smoke \
    --binding before=v5.0.3 \
    --date-range 2020-01-01,2020-01-01 \
    --runner DirectRunner \
    --ssvid-filter-query '"<ssvid>","<ssvid>"' \
    --skip-downstream \
    --build-from-source
```

Then drop `--skip-downstream` to run all six steps, and drop
`--build-from-source` to run the published image.

Expected:

- `worktree … @ v5.0.3` log line per binding.
- `gpsdio-segment pin -> …v3.0.0` log line per binding.
- `commit=… image=…` log line per binding.
- One `invoking pipe-segment <step>` log line per step, six without
  `--skip-downstream`.
- Diff phase no-ops on a single binding (`itertools.combinations` is empty).
- Output tables in `tech_great_expectations`:
  `messages_segmented_pipe_segment_smoke_before`,
  `segments_pipe_segment_smoke_before`, etc.

## 7. Cloud-build smoke (one-binding, Dataflow)

Needs the overlay image named in the status note at the top.

```bash
make dit-cloud PIPELINE=pipe-segment \
    WORKFLOW=workflows/pipe_segment/identity_match_key.py \
    ARGS="--experiment-id pipe-segment-cloud-smoke \
          --binding before=v5.0.3 \
          --date-range 2020-01-01,2020-01-01 \
          --runner dataflow \
          --skip-downstream"
```

Cloud-mode auth is automatic (`--network=cloudbuild`).
Reviewed v5.0.3 → no kaniko build, pulls canonical image.

## 8. Full Dataflow A/B (after experiment branches are pushed)

```bash
make dit-cloud PIPELINE=pipe-segment \
    WORKFLOW=workflows/pipe_segment/identity_match_key.py \
    ARGS="--experiment-id idmatchkey \
          --binding before=v5.0.3 \
          --binding after=experiment/identity-match-key-transponder-only \
          --date-range 2020-01-01,2020-01-03 \
          --runner dataflow"
```

For the "after" binding, `ensure_pipeline_image` kicks off a kaniko build
of `gcr.io/world-fishing-827/dit/pipe-segment:dit-<commit>` (one-time per
ref; cached on subsequent runs). Expected results:

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
| All output-table diffs IDENTICAL | Either the "after" image wasn't actually built (check the kaniko build log for the relevant commit) or `--image-tag` was set to a single image overriding the per-binding auto-build. |
| `rows_only_in_a` / `rows_only_in_b` non-zero on `messages_segmented` | The bindings did not read the same copy, e.g. they ran in two invocations without `--reuse-pin`. Re-run them in one invocation, or the second with the first's `--reuse-pin <stamp>`. |
| `rows_only_in_a` / `rows_only_in_b` non-zero on `vessel_info` or `segment_vessel` | Expected when identities change: `vessel_id` is in the key (section 8). |
| A step fails with `TypeError: 'NoneType' object is not iterable` in `BigQueryHelper` | The step got no `--labels`. The workflow passes them on every step; check for a hand-built command line. |
| `--binding name … is too long for the Dataflow job name` | On Dataflow a binding name may be at most 27 characters. Shorten it. |
| `pinning … would process … GB, above the … GB cap` | The copy of the run's dates is larger than `--pin-max-gb`. Narrow `--date-range`, or raise the cap on purpose. |
| `pinned copy … does not exist` with `--reuse-pin` | The copy expired, or the stamp, `--experiment-id`, `--date-range` or source table differ from the run that made it. |
| The copy is denied `bigquery.tables.create` | The identity may not create tables in `tech_great_expectations`. With gcloud impersonating `dq-monitoring@`, that is the case. |
| A Dataflow job fails creating `beam_temp_dataset_…` | The test service account may not create datasets, and pipe-segment cannot be told which dataset to use (status note at the top). |
| `--include-satellite-offsets cannot run and is refused` | The option is refused at argument parsing (section 5). Drop the flag. |
| `… was removed with snapshot pinning` | `--pin-source-at`, `--snapshot-dest-project` or `--snapshot-expiration-days` was passed. The message names what to use instead. |
