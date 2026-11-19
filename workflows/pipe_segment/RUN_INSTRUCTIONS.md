# identity_match_key workflow — setup and run instructions

End-to-end steps to A/B-test the gpsdio-segment identity-match-key change
(reducing `match_key = (transponder_type, receiver_type, source)` to
`(transponder_type,)`) via this workflow.

**Status, 2026-11-19.** The workflow's command lines match what production
runs (pipe-segment v5.0.3, the six tasks of the AIS v3 DAG's segment group;
see [`composer_drift.py`](composer_drift.py)). That was established by reading
code. The only recorded run is one laptop smoke of the `segment` step on
2026-06-03. Three things still stand between this file and a full run:

- **Source pinning.** Step 2 of the workflow snapshots its source table, and
  no DIT identity can create a snapshot with an expiration in
  `tech_great_expectations`. Until the workflow pins by copy
  ([PIPELINE-4705](https://globalfishingwatch.atlassian.net/browse/PIPELINE-4705))
  it is expected to stop there.
- **Dataflow as the test service account.** pipe-segment has no way to name
  the dataset Beam stages query results in, and `automated-testing@` may not
  create one. Sections 6 and 7 need an overlay image first, as port visits and
  encounters have. Inferred from the code; not tried.
- **Nothing here has run end to end.** Treat the expected results below as
  expectations.

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

**Not production's state.** A binding starts from empty tables. `segment` has
no fragment history to stitch onto, and `segment_vessel_daily` sees only the
run's own days in its 30-day window. Both are the same for every binding, so
the A/B is fair; the tables are not comparable with production's.

**Satellite offsets.** Production's `segment` also reads satellite positions
and writes clock offsets. `--include-satellite-offsets` asks for that here. It
is off by default and should stay off: the satellite positions are
date-sharded, which DIT does not support, their snapshot is blocked across
organisations, and the offsets query reads its message source as date shards,
which DIT's partitioned source is not. Whether the option stays is an open
decision.

## 6. DirectRunner smoke (laptop, fast)

Single-binding orchestration check against `v5.0.3`, 1 day, a handful of
vessels, source = staging cohort. Start with the `segment` step:

```bash
cd ~/git/data_integration_tests
dit run workflows/pipe_segment/identity_match_key.py \
    --experiment-id pipe-segment-smoke \
    --pin-source-at 2026-06-03T10:00:00Z \
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

Needs the two prerequisites in the status note at the top.

```bash
make dit-cloud PIPELINE=pipe-segment \
    WORKFLOW=workflows/pipe_segment/identity_match_key.py \
    ARGS="--experiment-id pipe-segment-cloud-smoke \
          --pin-source-at 2026-06-03T10:00:00Z \
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
          --pin-source-at 2026-06-03T10:00:00Z \
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
- Source snapshots expire after 7 days (configurable via
  `--snapshot-expiration-days`).

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `gpsdio-segment pin -> …` identical across bindings | The "after" branch wasn't pushed, or the worktree didn't pick it up. Re-fetch the pipe-segment refs (`git -C ~/git/pipe-segment fetch origin`). |
| All output-table diffs IDENTICAL | Either the "after" image wasn't actually built (check the kaniko build log for the relevant commit) or `--image-tag` was set to a single image overriding the per-binding auto-build. |
| `rows_only_in_a` / `rows_only_in_b` non-zero on `messages_segmented` | Source data wasn't pinned (different snapshot for each binding). Re-run with the same `--pin-source-at`. |
| `rows_only_in_a` / `rows_only_in_b` non-zero on `vessel_info` or `segment_vessel` | Expected when identities change: `vessel_id` is in the key (section 8). |
| A step fails with `TypeError: 'NoneType' object is not iterable` in `BigQueryHelper` | The step got no `--labels`. The workflow passes them on every step; check for a hand-built command line. |
| `--binding name … is too long for the Dataflow job name` | On Dataflow a binding name may be at most 27 characters. Shorten it. |
| The source snapshot is denied (`deleteSnapshot`) | Source pinning by snapshot does not work in `tech_great_expectations` (status note at the top). |
| A Dataflow job fails creating `beam_temp_dataset_…` | The test service account may not create datasets, and pipe-segment cannot be told which dataset to use (status note at the top). |
| `Cannot snapshot tables across projects that are in different orgs` | `--include-satellite-offsets` reads satellite positions from another organisation. Leave the option off (section 5). |
