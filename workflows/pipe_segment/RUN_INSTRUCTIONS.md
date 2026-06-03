# identity_match_key workflow — setup and run instructions

End-to-end steps to A/B-test the gpsdio-segment identity-match-key change
(reducing `match_key = (transponder_type, receiver_type, source)` to
`(transponder_type,)`) via this workflow.

## 1. Install the dit + pipe-segment dev environment

```bash
cd ~/git/data_integration_tests
make install-pipe-segment
```

This installs dit in editable mode plus pipe-segment from `$(PROJECTS)/pipe-segment`
(defaults to a sibling checkout; override via `PROJECTS=…` or `.envrc`).

For the laptop docker path, the auth named volume needs to be populated **once** before any docker run:

```bash
docker volume create gcp
docker compose -f ~/git/pipe-segment/compose.yaml run --rm gcloud auth application-default login
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

Repin gpsdio-segment to the fork in three places:

### `requirements/prod.in` (line 3)

```diff
-gpsdio-segment @ https://codeload.github.com/GlobalFishingWatch/gpsdio-segment/tar.gz/v3.0.0
+gpsdio-segment @ git+https://github.com/GlobalFishingWatch/gpsdio-segment@<GPSDIO_FORK_SHA>
```

### `setup.py` (lines 34-35)

Same URL change.

### Regenerate `requirements.txt`

```bash
make upgrade-reqs    # pip-compile requirements/prod.in -o requirements.txt
grep gpsdio-segment requirements.txt
# should show: gpsdio-segment @ git+https://github.com/GlobalFishingWatch/gpsdio-segment@<GPSDIO_FORK_SHA>
```

Commit and push:

```bash
git add requirements/prod.in setup.py requirements.txt
git commit -m "experiment: repin gpsdio-segment to identity-match-key fork"
git push -u origin experiment/identity-match-key-transponder-only
```

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
run the same per-binding code.

## 5. DirectRunner smoke (laptop, fast)

Single-binding orchestration check against `v5.0.3`, 1 day, identity steps
skipped, source = staging cohort:

```bash
cd ~/git/data_integration_tests
dit run workflows/pipe_segment/identity_match_key.py \
    --experiment-id pipe-segment-smoke \
    --pin-source-at 2026-06-03T10:00:00Z \
    --binding before=v5.0.3 \
    --date-range 2020-01-01,2020-01-01 \
    --runner DirectRunner \
    --include-satellite-offsets \
    --skip-downstream \
    --build-from-source
```

Expected:

- `worktree … @ v5.0.3` log line per binding.
- `gpsdio-segment pin -> …v3.0.0` log line per binding.
- `commit=… image=…` log line per binding.
- pipe-segment runs to completion against the staging snapshot.
- Diff phase no-ops on a single binding (`itertools.combinations` is empty).
- Output tables in `tech_great_expectations`:
  `messages_segmented_pipe_segment_smoke_before`,
  `segments_pipe_segment_smoke_before`, etc.

## 6. Cloud-build smoke (one-binding, Dataflow)

```bash
make dit-cloud PIPELINE=pipe-segment \
    WORKFLOW=workflows/pipe_segment/identity_match_key.py \
    ARGS="--experiment-id pipe-segment-cloud-smoke \
          --pin-source-at 2026-06-03T10:00:00Z \
          --binding before=v5.0.3 \
          --date-range 2020-01-01,2020-01-01 \
          --runner dataflow \
          --include-satellite-offsets \
          --skip-downstream"
```

Cloud-mode auth is automatic (`--network=cloudbuild` + `GOOGLE_CLOUD_QUOTA_PROJECT=world-fishing-827`).
Reviewed v5.0.3 → no kaniko build, pulls canonical image.

## 7. Full Dataflow A/B (after experiment branches are pushed)

```bash
make dit-cloud PIPELINE=pipe-segment \
    WORKFLOW=workflows/pipe_segment/identity_match_key.py \
    ARGS="--experiment-id idmatchkey \
          --pin-source-at 2026-06-03T10:00:00Z \
          --binding before=v5.0.3 \
          --binding after=experiment/identity-match-key-transponder-only \
          --date-range 2020-01-01,2020-01-03 \
          --runner dataflow \
          --include-satellite-offsets"
```

For the "after" binding, `ensure_pipeline_image` kicks off a kaniko build
of `gcr.io/world-fishing-827/dit/pipe-segment:dit-<commit>` (one-time per
ref; cached on subsequent runs). Expected results:

- `messages_segmented`: **DIFFERENT** (per-row identity/destinations dict
  contents differ — that's the metric).
- `segments` / `fragments`: identical or very nearly so (segmentation is
  driven by position kinematics, not identity matching).
- `segment_identity_daily` / `segment_info`: most informative — they
  aggregate per-segment identity assignments, and that's where the fix
  most matters.

## 8. Cleanup

```bash
make dit-cancel RUN_ID=<run_id-printed-at-workflow-start>
```

Drops output tables (table-level only — never a snapshot dataset) and
cancels any in-flight Dataflow jobs by `dit_run_id` label.

The snapshot dataset auto-expires after 7 days (configurable via
`--snapshot-expiration-days`).

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `gpsdio-segment pin -> …` identical across bindings | The "after" branch wasn't pushed, or the worktree didn't pick it up. Re-fetch the pipe-segment refs (`git -C ~/git/pipe-segment fetch origin`). |
| All output-table diffs IDENTICAL | Either the "after" image wasn't actually built (check the kaniko build log for the relevant commit) or `--image-tag` was set to a single image overriding the per-binding auto-build. |
| `rows_only_in_a` / `rows_only_in_b` non-zero on `messages_segmented` | Source data wasn't pinned (different snapshot for each binding). Re-run with the same `--pin-source-at`. |
| `segment_info` step fails | This step expects a `segment_vessel` input but we wire it to `segment_identity_daily` instead. Use `--skip-downstream` for the smoke, or build a `segment_vessel` step beforehand for the full A/B. |
| `Cannot copy across regions` during snapshot | The cross-project `gfw-int-pipe-v3.satellite_positions.*` source is in a different BQ region than the snapshot dataset. Skip `--include-satellite-offsets` or create the snapshot dataset in `us-central1` explicitly. |
| BQ 403 "API has not been used in project …" in cloud-build | The `GOOGLE_CLOUD_QUOTA_PROJECT=world-fishing-827` injection didn't fire. Make sure you're calling via `dit_docker.run` (the workflow does) and that `DIT_CLOUD_MODE=1` is set. |
