# pipe-segment temp-dataset overlay

Test-only patches that let DIT run pipe-segment on Dataflow as `automated-testing@`. They are not proposed upstream, like the pipe-anchorages overlay next to this one (decision 2026-10-22, PIPELINE-4699).

**Status, 2026-11-24: both images are built and pushed** (table below), each self-checked inside the image when built. Dataflow jobs of `workflows/pipe_segment/ais.py` have run on them as the test service account (Cloud Build runs `2e57a93b`, `4bb84ec0` and `56af531e`, 2026-11-24/25; results in `workflows/pipe_segment/RUN_INSTRUCTIONS.md`).

**Which version is the default.** Both pipe-segment workflows default to the overlay of **v5.1.0**, the version the AIS v3 DAG's segment group is configured for since 2026-11-20 (composer-dags-production PR #3145). The v5.0.3 patch stays: the VMS v5 DAG still pins v5.0.3, and an overlay image of it is what a DIT run of that version needs.

## Why it exists

pipe-segment reads BigQuery with queries. Beam's `ReadFromBigQuery` stages every query result in a table inside a temporary dataset, then exports that table to GCS. Unless the transform is given an existing dataset (`temp_dataset=`), Beam creates `beam_temp_dataset_<hex>` in the pipeline project. DIT's service account deliberately has no `bigquery.datasets.create`.

Nothing outside the code can redirect this:

- pipe-segment has no option for it. Its read sites call `beam.io.ReadFromBigQuery` directly, with no staging-dataset argument (v5.0.3 and v5.1.0, read 2026-11-20).
- Beam has no pipeline option or environment variable for it; the only input is the transform argument. Checked in Beam 2.71.0, which pipe-segment pins at both tags (2026-11-20): `temp_dataset` appears only in `apache_beam/io/gcp/bigquery*.py`, as that argument, and nowhere under `options/` or the Dataflow runner. With it set, `_CustomBigQuerySource._setup_temporary_dataset` returns without creating anything, and the query result goes to a `beam_temp_table_<hex>` table in that dataset, deleted afterwards.

That a pipe-segment Dataflow job fails without the overlay is inferred from this and from the pipe-anchorages smoke of 2026-10-22, which failed that way. It has not been tried with pipe-segment; the jobs on the overlay images ran without the failure.

## What the patches do

One patch per pipe-segment generation, because the read sites differ between them:

| Patch | Written against | Reads patched | Pinned by (DAG configuration, 2026-11-20) |
|---|---|---|---|
| `temp_dataset.patch` | v5.1.0 (4ddde03) | 5 | the AIS v3 DAG's segment group; DIT's default |
| `temp_dataset_v5.0.3.patch` | v5.0.3 (f4d39fe) | 4 | the VMS v5 DAG's segment stage; the AIS DAG until 2026-11-20 |

Each patch applies to its own tag only (`git apply --check` on a `git archive` of each tag, 2026-11-20). v5.1.0 rewrote three of the four older call sites to pass job labels, and added a fifth read.

Both patches:

- add `src/pipe_segment/_dit_overlay.py`: `temp_dataset_kwargs()` returns `{"temp_dataset": DatasetReference(...)}` when `DIT_BQ_TEMP_DATASET=<project>.<dataset>` is set, `{}` otherwise. The file is the same in both patches;
- add `**temp_dataset_kwargs()` to every call of `ReadFromBigQuery`:

| Module (under `src/pipe_segment/`) | Reads | In |
|---|---|---|
| `transform/read_messages.py` | the normalized messages | both |
| `transform/read_fragments.py` | the fragment history (full-history segmenter) | both |
| `transform/read_prior_segments.py` | the segments of the day before the range (incremental segmenter) | v5.1.0 only |
| `transform/satellite_offsets.py` | satellite timing offsets. DIT never runs this read; it is patched so that no read is left out | both |
| `segment_identity/read_source.py` | segments joined with fragments (`segment_identity` step) | both |

Unset, the image behaves exactly like the commit it was built from. The argument is set when the pipeline is constructed and pickled into the source, which the workers split, so the overlay image must be **both** the submitter and the worker. Both workflows set the variable on every step, through the docker runner's `container_env`, from `--bq-temp-dataset`:

- `workflows/pipe_segment/ais.py` takes the two images as `--image-tag` and `--worker-image`;
- `workflows/pipe_segment/identity_match_key.py` takes one `--image-tag` and passes it as the workers' image too.

## Build

```bash
./build.sh <pipe-segment repo dir> <commit-ish> <label> [--push]
# e.g.
./build.sh "$PROJECTS/pipe-segment" v5.1.0 v5.1.0 --push    # the default image
./build.sh "$PROJECTS/pipe-segment" v5.0.3 v5.0.3 --push    # the version the VMS DAG pins
```

Image: `gcr.io/world-fishing-827/dit/pipe-segment:<label>-<commit7>-temp-dataset-<patch8>`, where `<patch8>` is the first 8 hex characters of the sha256 of the patch that was applied.

`build.sh` exports the commit (`git archive`), applies the first of the two patches that passes `git apply --check`, newest generation first, and builds the repo's own Dockerfile `prod` target. It fails if no patch applies.

**Start the label with the pipe-segment version the commit holds** (`v5.1.0`, or `v5.1.0-pr230` for a branch based on it). `workflows/pipe_segment/ais.py` reads the version from the start of the image tag to decide whether the image knows `--incremental`. An image whose tag names no version is refused for incremental runs unless `--assume-incremental-support` is given.

`--push` uses docker's gcloud credential helper. If gcloud is set to impersonate a service account without upload rights, the push fails with `uploadArtifacts` denied. Run it as your own account instead: `CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT= ./build.sh ... --push`.

Built and pushed on 2026-11-24, as the operator's own account:

| side | commit | image | digest | built |
|---|---|---|---|---|
| **default** of both workflows (the AIS DAG's version; knows `--incremental`) | pipe-segment v5.1.0, 4ddde03 | `gcr.io/world-fishing-827/dit/pipe-segment:v5.1.0-4ddde03-temp-dataset-04b40b54` | `sha256:f785658b99d8caea217be4575fb951336bfeab8c36d253d3b3309026ffadf8eb` | 2026-11-24; self-check passed (5 BigQuery reads, all patched) |
| the version the VMS DAG pins (full-history segmenter only) | pipe-segment v5.0.3, f4d39fe | `gcr.io/world-fishing-827/dit/pipe-segment:v5.0.3-f4d39fe-temp-dataset-82b7f0a8` | `sha256:96ac0dbc7103412e1017d0d27e78dc8630ee01ad2e0068abc93f2653bd13de9c` | 2026-11-24; self-check passed |

## Self-check

After building, `build.sh` runs `verify_overlay.py --patch <the patch it applied>` inside the image, then `pipe-segment segment --help`. The self-check asserts:

- **Every BigQuery read is patched, and there is no other.** It walks the installed `pipe_segment` package, counts the calls to `ReadFromBigQuery` per module, and compares them with what the patch covers (`EXPECTED_READS_BY_PATCH`: 4 reads in 4 modules for v5.0.3, 5 in 5 for v5.1.0). A read added upstream, in a patched module or in a new one, fails the build instead of the Dataflow job. So does a use of `ReadAllFromBigQuery`. The pipe-anchorages overlay's first patch missed one of two reads, and its first Dataflow smoke failed on it; that is why the reads are counted.
- The variable is honoured: unset adds nothing; set gives a `DatasetReference`.
- Beam forwards `temp_dataset` to the export source, and the source then skips dataset creation.
- Every patched module imports the helper.

The first check needs neither pipe-segment nor Beam (`--static-only --package-dir <dir>`), so DIT's tests run it: `tests/test_pipe_segment_overlay.py` checks each patch against the expected counts, and, when a pipe-segment checkout with both tags is present, applies each patch to its tag and runs the static check on the result.

Checked on 2026-11-20 without building an image: both patched trees pass the static check, the unpatched trees fail it, and the helper passes the Beam half of the runtime check under Beam 2.71.0 in DIT's own virtualenv. The build, the runtime check inside each image and Dataflow jobs on the images followed on 2026-11-24 (status at the top).

## Default image

Both pipe-segment workflows default to the row marked **default** above (`DEFAULT_OVERLAY_IMAGE` in `workflows/pipe_segment/identity_match_key.py`, shared with `ais.py`). The published image cannot be the default, because it has no staging-dataset option.

`workflows/pipe_segment/ais.py` follows the rules of the port-visits overlay:

- **A reviewed checkout runs v5.1.0.** With a clean checkout of a commit merged into pipe-segment `origin/main` and no image flags, the run executes the default image, whatever that commit is.
- **Unreviewed code needs its own overlay image.** For a dirty tree, a DIT snapshot or a commit not merged into `origin/main`, a run that would execute the default image refuses to start, before any cloud call. Build an overlay image of the commit with `build.sh` and pass it as both `--image-tag` and `--worker-image`.
- **DIT never auto-builds an image for this workflow.** The shared auto-build (`dit.worker_image`) builds plain source, without the overlay.
- **`--build-from-source` has no overlay either.** It runs `docker compose` on the working tree. That is fine for a local `--runner docker` check. On Dataflow the workflow logs a warning.
- **The default segmenter is `incremental`**, which the default image knows. A run on the v5.0.3 image must select full history only (`--segmenter full-history`); otherwise it is refused before any cloud call.

`workflows/pipe_segment/identity_match_key.py` follows the same rules per binding (since 2026-11-25; it auto-built a plain image and only warned before):

- **An unreviewed binding on the default image is refused** before anything is pinned, with the same two ways out: an overlay image of the commit for that binding (`--binding-image NAME=IMAGE`, or `--image-tag` for every binding), or `--runner DirectRunner --build-from-source`.

When the AIS DAG's pipe-segment version moves:

1. Build and push the overlay image of the new tag: `./build.sh <repo> <tag> <tag> --push`. The build fails if no patch applies or a BigQuery read was added. Then write a patch for the new generation and add it to `PATCHES` in `build.sh` and to `EXPECTED_READS_BY_PATCH` in `verify_overlay.py`.
2. Add its row to the table above, with the digest.
3. Bump `DEFAULT_PIPELINE_VERSION` and `DEFAULT_OVERLAY_IMAGE` in `workflows/pipe_segment/identity_match_key.py`. `ais.py` takes both from there.
4. If the Beam pin moved, bump `PIPE_SEGMENT_BEAM_VERSION` in the `Makefile` in the same commit.

`workflows/pipe_segment/composer_drift.py` reports the move: it compares the default version of both workflows with the image tag of the DAG's segment tasks. It did so on 2026-11-20, hours after the DAG moved from v5.0.3 to v5.1.0.

## Retire it

Delete this directory if pipe-segment ever gains a native staging-dataset option. Then `ais.py` passes that option instead of the container variable, and its default image can be production's own.
