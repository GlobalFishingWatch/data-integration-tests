# pipe-anchorages temp-dataset overlay

Test-only patch that lets DIT run current pipe-anchorages (the `--pipeline-cli v5` CLI) on Dataflow as `automated-testing@`. It is **never** proposed upstream (decision 2026-10-22, PIPELINE-4699).

## Why it exists

Port visits reads BigQuery with queries (joins and filters), not plain table reads. Beam's `ReadFromBigQuery` stages every query result in a table inside a temporary dataset, then exports that table to GCS. Unless the transform is given an existing dataset (`temp_dataset=`), Beam creates `beam_temp_dataset_<hex>` in the pipeline project. DIT's service account deliberately has no `bigquery.datasets.create`.

Nothing outside the code can redirect this, which was verified on 2026-10-22:

- Beam v2.76.0 (the version pipe-anchorages main pins) has no pipeline option or environment variable for it; the only input is the transform argument.
- The Python Dataflow runner adds nothing; its job-environment `dataset` field is hard-coded and unused by the read path.
- Prod confirms it: every Beam query job in the last three days wrote to its own fresh `beam_temp_dataset_<hex>`.
- No GFW pipeline exposes the option; pipe-gaps gets it from DIT's in-process DAG-factory injection, encounters from an overlay.

The v4.6.4 CLI's `--temp_dataset` only ever existed on DIT's local patch branch.

## What the patch does

`temp_dataset.patch`, against pipe-anchorages main (b36aed6):

- adds `src/pipe_anchorages/_dit_overlay.py`: `temp_dataset_kwargs()` returns `{"temp_dataset": DatasetReference(...)}` when `DIT_BQ_TEMP_DATASET=<project>.<dataset>` is set, `{}` otherwise;
- adds `**temp_dataset_kwargs()` to `read_from_bigquery_kwargs` in `pipelines/port_state_transitions/main.py` and `pipelines/port_visits/main.py`.

Unset, the image behaves exactly like the commit it was built from. The argument is set at pipeline construction and pickled into the source, which the workers split, so the overlay image must be **both** the submitter (`--image-tag`) and the worker (`--worker-image`). `workflows/port_visits/ais.py --pipeline-cli v5` sets the variable through the docker runner's `container_env` from `--bq-temp-dataset`.

## How it differs from the encounters overlay

The encounters overlay copies patched files over a published image (`docs/conventions.md`, overlay images). Here the commit under test is usually a PR branch with no published image, so `build.sh` builds from the commit's own source instead: `git archive <commit>`, `git apply` the patch, `docker build --target prod`. Baseline and candidate are built the same way, so the only difference between them is the change under test.

## Build

```bash
./build.sh <pipe-anchorages repo dir> <commit-ish> <label> [--push]
# e.g.
./build.sh "$PROJECTS/anchorages_pipeline" b36aed6 main --push
./build.sh "$PROJECTS/anchorages_pipeline" PIPELINE-4690-one-record-per-vessel-instant pipeline-4690 --push
```

Image: `gcr.io/world-fishing-827/dit/pipe-anchorages:<label>-<commit7>-temp-dataset-<patch8>`, where `<patch8>` is the first 8 hex characters of the patch's sha256.

`--push` uses docker's gcloud credential helper. If gcloud is set to impersonate a service account without upload rights (e.g. `dq-monitoring@`), the push fails with `uploadArtifacts` denied. Run it as your own account instead: `CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT= ./build.sh ... --push`.

Built so far (2026-10-22, PIPELINE-4690):

| side | commit | image | digest |
|---|---|---|---|
| baseline | pipe-anchorages main b36aed6 | `dit/pipe-anchorages:main-b36aed6-temp-dataset-21aed7a5` | `sha256:ebdd9fd4f9b78596534961197ef6ff060247ea81fa7dfaf919ae84a920a28c07` |
| candidate | PR #176 head 14b4be8 | `dit/pipe-anchorages:pipeline-4690-14b4be8-temp-dataset-21aed7a5` | `sha256:e06d29ee20a96e7c5d5b6937484618830a9b843b4d44c9ad14569010bdb76ff9` |

The build fails if the patch does not apply (`git apply --check`), and after building runs `verify_overlay.py` inside the image. That self-check asserts the variable is honoured, Beam forwards `temp_dataset` to the export source, the source then skips dataset creation, and both pipelines import the helper. It also checks the `pipe-anchorages` console script.

## Retire it

Delete this directory if pipe-anchorages ever gains a native staging-dataset option, and switch `ais.py --pipeline-cli v5` to pass that option instead of the container variable.
