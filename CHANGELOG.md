# Changelog

User-facing changes to `dit` (the framework, CLI, and workflows). For the internal record of plan-doc evolution, see the **Plan changelog** in [`CLAUDE.md`](CLAUDE.md).

The project is pre-1.0; entries are grouped chronologically under `[Unreleased]` rather than semver releases. Format inspired by [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### 2026-05-15

#### Added
- **Cloud Build ad-hoc runtime.** Ship `dit` runs to Cloud Build with `make dit-cloud PIPELINE=… WORKFLOW=… ARGS="…"`. The pipeline checkout flows through as the build source; dit is cloned fresh per run from `_DIT_REF` (default `main`). 24h timeout, runs as `automated-testing@`. See `cloudbuild-dit.yaml` substitutions for the full parameter shape.
- **`ditbox` Cloud Build tooling image.** `docker/ditbox/Dockerfile` plus `docker/ditbox/cloudbuild.yaml` and a `make publish-ditbox` target. The image (`us-central1-docker.pkg.dev/gfw-int-infrastructure/core/ditbox:<sha>` / `:latest`) carries Python 3.11 + git + gcloud (incl. `bq`) + docker CLI + dit's pre-cached runtime deps. Deliberately ships *without* dit itself or pipeline deps baked in — both install per-run from the cloudbuild yaml so iteration on either stays fast.
- `.gcloudignore` excluding venvs / pyc / coverage / `sa.json` / `.envrc` from `gcloud builds submit` uploads.
- `table-identical-checks @ git+https://github.com/GlobalFishingWatch/table_identical_checks.git@master` added to `requirements.txt` so a fresh install of dit brings `table-check` transitively (real dep of `dit.compare`).
- **Cross-version experiments capability.** Three-part feature for diffing pipeline outputs across multiple pipeline-version bindings against pinned source data:
  - `dit.bq.snapshot_table(src, dst, *, as_of=…, expiration=…, project=…, if_not_exists=False)` and `dit.bq.snapshot_dataset(src_dataset, dst_dataset, *, tables=…, as_of=…, expiration=…, project=…)` for source-data pinning via BQ `CREATE SNAPSHOT TABLE`.
  - `--experiment-id <slug>` / `DIT_EXPERIMENT_ID` flag in `workflows/pipe_gaps/mode_equivalence.py` and `workflows/port_visits/ais.py`. Slug becomes the leftmost slot of the output-table suffix; clusters N related runs under one BQ-prefix-scannable name. Auto-default `solo_<6-hex>` for non-experiment runs (clearly marks them as "not part of a cross-version experiment"). Regex `^[a-z0-9][a-z0-9_-]{0,31}$`.
  - `workflows/port_visits/cross_version_ais.py` — end-to-end orchestrator. Snapshots source datasets, runs `ais.py` once per binding inside a `git worktree`, diffs output tables pairwise on `visit_id`. Supports `--dry-run` for orchestration validation without Dataflow cost.
- **Structured Dataflow job names** (`dit-<repo>-<step>-<exp>-<binding>-<mode>`, capped at 63 chars with experiment-id truncation if needed) for `workflows/port_visits/ais.py`. Replaces Beam's `beamapp-root-<timestamp>-…` default.
- **Dynamic BQ labels** on Dataflow jobs *and* the BQ tables they write: `dit_repo`, `dit_step`, `dit_experiment_id`, `dit_binding` (optional), `dit_mode`. Additive to the existing five static labels. Propagates to BQ via pipe-anchorages' `BigQueryHelper` reading `cloud_options.labels`.
- **`--binding-name`** flag in `workflows/port_visits/ais.py` (defaults to empty). Used by `cross_version_ais.py` to surface the binding name in labels and job-name without bleeding into the table suffix.
- **`docs/pipeline-contract.md`** — what a GFW pipeline must expose to be integration-testable. 12 numbered requirements grouped by pipeline architecture (universal / Beam / Beam-in-container / non-Beam BQ-SQL), plus an adoption matrix tracking `pipe-gaps`, `anchorages_pipeline`, and `pipe-events` (with current gaps explicit).
- Plan changelog entries documenting the synthetic `tests/temp_dataset_for_integration_tests` and `tests/pipeline_1465_for_integration_tests` branches in `anchorages_pipeline` for the PIPELINE-1465 cross-version test.

#### Changed
- **Phase 2 AIS-staging verification: passed.** Mode-equivalence holds for port-visits across bf / bfd / bftruncate on the 2020 AIS-staging cohort. Three pairwise comparisons green on `visit_id`.

#### Fixed
- `dit.runners.docker` now tears down per-call compose networks in a `finally` block. Without this, `docker compose -p <unique-uuid> run --rm` leaves a `<project>_default` bridge network behind every call, eventually exhausting docker's default address pool (172.16-172.31, /24 each). Encountered as `all predefined address pools have been fully subnetted` during the AIS-staging verification.
- (pipe-anchorages, local patch) `transforms/sink.cloud_to_labels` was NPE-ing on `None` labels; workflows now always pass `--labels=k=v` Beam options.
- (pipe-anchorages, local patch) Workers in Dataflow couldn't `import pipe_anchorages` until we passed `--sdk_container_image=us-central1-docker.pkg.dev/gfw-int-infrastructure/core/pipe-anchorages:<tag>` (the published image with the package installed) explicitly.
- (pipe-anchorages, local patch on `dit-temp-dataset-support`) Added a `--temp_dataset` CLI flag and threaded it through `QuerySource` to `ReadFromBigQuery(temp_dataset=…)`. Lets dit point Beam's BQ EXPORT staging at an existing dataset so the worker SA doesn't need `bigquery.datasets.create`. Upstream PR pending team review.

### 2026-05-14

#### Added
- **Per-user infra knobs via `DIT_*` env vars**: `DIT_DEST_DATASET`, `DIT_DATAFLOW_SA`, `DIT_DATAFLOW_REGION`, `DIT_DATAFLOW_TEMP_BUCKET`, `DIT_DATAFLOW_SUBNETWORK`, `DIT_BQ_TEMP_DATASET`. Each has a corresponding CLI flag that overrides the env var for the invocation. `.envrc.example` documents the convention; direnv-friendly.
- **Reproducible install modes** alongside editable (`make install-<pipeline>`):
  - `make install-<pipeline>-ref REF=<sha-or-branch>` — `pip install --force-reinstall --no-deps git+file://<path>@<ref>`. Non-editable, exactly that commit. ~5-10s per ref.
  - `make snapshot-<pipeline>` — `git stash create` captures tracked working-tree changes into a commit anchored on a `dit-snapshot-<epoch>` branch, then installs from that ref. Useful for testing uncommitted state reproducibly.
  - `make clean-snapshots` — GC the `dit-snapshot-*` branches across all configured pipeline checkouts.
  - `FULLDEPS=1` env override on the `-ref`/snapshot targets drops `--no-deps` for the rare case where transitive deps drifted between refs.
- **`workflows/port_visits/ais.py`** — Phase 2 AIS-staging cross-mode-equivalence workflow. Three modes (bf / bfd / bftruncate) over the 2020 staging cohort; two-step thin_port_messages → port_visits chain per slice; pairwise comparison on `visit_id` with `view_suffix=""` (truncate shape, no SCD-2).
- README `## Features` and `## Roadmap` sections — operational dashboard for "what dit does today" and "what's next".

#### Changed
- `requirements.txt` no longer carries `gfw-common[bq,beam]` or `apache-beam[gcp]`. These are workflow-shaped and arrive transitively when a pipeline is installed via the `make install-<pipeline>` targets. Decouples consumers using only the docker runner from the GFW private package index.

### 2026-05-08

#### Added
- **`dit` framework, Phase 1**:
  - `dit run <workflow-path>` CLI (click-based) that loads a Python workflow module and invokes its `main(argv) -> int` entry point.
  - `dit.runners.docker.run(image_tag, args, *, env, project_name, build_from_source, entrypoint)` — invokes a pipeline via docker. Unique compose project name per call to avoid network races; `build_from_source` fallback for unpublished images; `entrypoint` opt-in for images without baked-in entrypoints.
  - `dit.runners.dataflow.run(args, *, image_tag, service_account, region, temp_bucket, subnetwork, bq_temp_dataset, env, pipeline_builder, dag_factory_cls)` — submits a Beam pipeline to Dataflow with lock-split submit/wait (so concurrent invocations serialise the submit step but parallelise the wait). `pipeline_builder` keeps the runner pipeline-agnostic; `dag_factory_cls` is wrapped to inject a pre-existing temp BQ dataset when set, avoiding `bigquery.datasets.create` on the worker SA.
  - `dit.compare.compare_tables(a, b, *, keys, view_suffix="", ignore_columns=(), tolerance=None)` — thin shim over `table-check summary` from the [`table_identical_checks`](https://github.com/GlobalFishingWatch/table_identical_checks) repo. `ignore_columns` raises `NotImplementedError` if non-empty (signal to push the feature upstream rather than reimplement in the shim).
  - `dit.bq.drop_tables(prefix, *, project)` and `dit.bq.query_for_restricted_ssvids(reference_table, *, mid, backfill_days_w, seed, project)`.
  - `dit.dates.daterange_inclusive(start, end)` — half-open `[start, end)` despite the name (preserved verbatim from the pipe-gaps source so the four-mode equivalence stays byte-equivalent across the move).
- **`workflows/pipe_gaps/mode_equivalence.py`** — Phase 1 port of `pipe-gaps/tests/integration/mode_equivalence.py` onto the `dit.*` library. Four modes (`bf`, `bfd`, `bftruncate`, `mutate_recover`) with `compute_restricted_ssvids` adapted to call `dit.bq.query_for_restricted_ssvids`. Drops `--runner=local`.
- Initial repo scaffolding: `pyproject.toml` (package `data-integration-tests`, console script `dit`), `requirements.txt`, `requirements-dev.txt`, README, `docs/{plan,context,framework-vision}.md`, and `CLAUDE.md` with working agreements + Plan changelog.
