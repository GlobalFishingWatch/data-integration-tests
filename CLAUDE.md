# CLAUDE.md — `data_integration_tests`

## Repo orientation

This repo houses `dit`, the cross-pipeline integration-test framework for GFW data pipelines (currently `pipe-gaps`, `anchorages_pipeline`/port-visits, `pipe-events`/fishing).

Read these in order before coding:

1. [`docs/context.md`](docs/context.md) — background, source bugs the framework caught, branch state at handoff.
2. [`docs/plan.md`](docs/plan.md) — the implementation plan. Sections worth bookmarking: **Architecture: three-repo split**, **Public API contracts (Phase 1)**, **Phase 1 subagent task breakdown**.
3. [`docs/framework-vision.md`](docs/framework-vision.md) — long-term shape. Don't optimise for it; Phase 1 stays imperative.

## Working agreements

- **`dit` is library-first, CLI-second.** Anything new must be importable Python; the CLI is one consumer of the library.
- **`dit.compare` is a thin shim** over `table-check`. Comparison features go upstream into `table_identical_checks`, not here.
- **`dit` reads composer-dags as data, not code.** No `import gfw.common.…` or `import gfw.pipes.…`. Sync via YAML (Phase 4).
- **No workflow lives in two places.** Canonical home is `dit/workflows/<pipeline>/`; in-repo workflows in processing repos are allowed for spikes only.
- **Plan changes get logged.** Whenever an architectural decision changes, update `docs/plan.md` and append the change to the **Plan changelog** below in the same commit. Subagents treat `docs/plan.md` + this changelog as the alignment surface.
- **README Features and Roadmap sections stay current.** The README is the operational dashboard for outsiders and future maintainers. Whenever a feature lands, drops, or shifts shape — or a roadmap phase advances status, completes, or gets re-scoped — update `README.md` § "Features" or § "Roadmap" in the same commit. Treat both sections with the same discipline as the Plan changelog: out-of-date is worse than under-detailed.
- **Pipeline-contract audits.** When adding a pipeline to `dit`'s scope (or when an existing pipeline's interface changes), audit it against `docs/pipeline-contract.md` and update the adoption matrix in the same commit. Workflow-side workarounds for missing contract items require a Plan-changelog entry explaining the trade-off — the integration-test workflow must not silently carry pipeline-specific workarounds.
- **Don't manually delete shared `dit_exp_*` datasets.** `cross_version_ais.py` snapshot datasets carry a 7-day `default_table_expiration_ms` and self-clean. Manual `bq rm` of these datasets can clobber in-flight runs that share an experiment-id namespace — a smoke-test cleanup with a colliding `--experiment-id` already broke one real run mid-flight (snapshot deleted out from under live Dataflow workers reading from it). Smoke tests must use disjoint experiment-ids (e.g. `dit-smoke-<timestamp>`) and let the TTL clean up; production runs should never `bq rm` snapshot datasets at all.
- **CHANGELOG.md is the user-facing change log.** `CHANGELOG.md` records what's available to users of `dit` (CLI flags, new helpers, new workflows, fixes). The Plan changelog in this file is dev-internal — plan-doc evolution, design refinements, why a commit happened. Both get an entry when a user-visible feature lands; CHANGELOG framed for users, Plan changelog framed for the next maintainer.

## Installing pipeline dependencies

`dit` is pipeline-agnostic; per-pipeline workflow deps (`pipe-gaps`, `anchorages_pipeline`, `pipe-events`) install separately via Makefile targets. `PROJECTS` (default: `$(realpath ..)`, i.e. sibling checkouts) tells the Makefile where to find them; override via env var or by copying `.envrc.example` → `.envrc` for direnv. See README for the full table; the operational summary:

| When | Target |
|---|---|
| Active dev on a pipeline (fast inner loop, edits picked up live) | `make install-<pipeline>` |
| Reproducible install of a specific committed ref (~5-10s; non-editable) | `make install-<pipeline>-ref REF=<sha-or-branch>` |
| Snapshot the current working tree onto a temp branch and install from it | `make snapshot-<pipeline>` |
| Target ref's transitive deps drifted (rare) — drop `--no-deps` | append `FULLDEPS=1` |
| GC the `dit-snapshot-*` branches across all pipeline checkouts | `make clean-snapshots` |

Notes:

- The framework-only `make install` works without any pipeline; the dataflow runner won't load until a workflow install brings `apache-beam[gcp]` transitively.
- `make snapshot-<pipeline>` uses `git stash create` which captures **tracked** changes only — `git add -A` in the pipeline repo first if untracked source files need to be in the snapshot.
- Snapshot branches stick around for traceability (recoverable via `git checkout dit-snapshot-<epoch>` in the pipeline repo) until `make clean-snapshots`.
- The non-editable install modes (`-ref`, snapshot) point the debugger at the installed copy under `venv/lib/python3.x/site-packages/<pipeline>/`, not your dev tree. If you're stepping through pipeline source, use the editable target instead.

## Plan changelog

Appended chronologically. Each entry is one commit's worth of plan-doc changes; cite which sections moved.

### 2026-05-15 — Cloud Build ad-hoc runtime + repo pushed public

Three pieces of Runtime & CI work landed together; tracked individually because they were sized as half a day collectively but had three separate review beats.

**GitHub remote.** Created `https://github.com/GlobalFishingWatch/data-integration-tests` (initially private). After a three-agent pre-publication review (credential scan / infra topology / prose-and-docs sensitivity) returned no blockers and two soft prose suggestions, flipped to public. The soft edits landed in `docs/context.md` (Bug 2 "Not yet fixed" -> "Fix proposed"; dropped the "Production VMS gaps continues to run with..." line) — see `5d21045`.

**`table_identical_checks` flipped public.** Prerequisite for clean `pip install` of `table-identical-checks @ git+https://...` from anywhere. The repo had no credentials in tracked files (`sa.json` was already gitignored); flip was a public-shape consistency move with the rest of the GFW pipeline ecosystem. Added the git URL to dit's `requirements.txt` so a fresh `pip install dit` brings `table-check` transitively (it's a real dep of `dit.compare`, was previously installed manually).

**`ditbox` image + `cloudbuild-dit.yaml` + `make dit-cloud`.** Per `docs/plan.md` § Runtime & CI items 1-3. Architecture revision from the original plan: dit itself is NOT baked into ditbox (the original plan said "dit pre-installed"). Now-public dit on GitHub makes `git clone @ _DIT_REF` per-run trivial and gives iteration on dit changes a faster inner loop (no ditbox rebuild required). Pipeline deps also install per-run via the source upload. Net: ditbox is a stable tooling layer that rarely changes; per-run installs are seconds-scale.

`cloudbuild-dit.yaml` runs as `automated-testing@world-fishing-827.iam.gserviceaccount.com` (matches the SA Dataflow already uses; avoids an impersonation hop). 24h timeout; `E2_HIGHCPU_8` machineType for the orchestrator (the actual compute is Dataflow worker-hours; the build VM just orchestrates). `options.logging: CLOUD_LOGGING_ONLY` to avoid the SA needing storage perms for the legacy GCS log bucket.

**Pending (Item 4 of validation, ahead of any cross-version PR-trigger work):** running `make publish-ditbox` for the first time, smoke-testing `make dit-cloud ARGS="--help"`, then a real AIS-staging single-binding run. Likely needs an IAM grant: the build-submitter principal needs `roles/iam.serviceAccountUser` on `automated-testing@`; the SA itself needs `roles/logging.logWriter`. Both surface naturally when the first build is submitted; documented in `docs/plan.md` § Next steps.

### 2026-05-15 — Synthetic branches for the PIPELINE-1465 cross-version test

To resolve the precondition flagged in the cross-version-glue entry below (every binding must support `--temp_dataset`), created two branches in `/mnt/encrypted_data/git/anchorages_pipeline`:

- **`tests/temp_dataset_for_integration_tests`** — points at `cb916bf` (current `dit-temp-dataset-support` HEAD). Has the `--temp_dataset` patch on top of `4df3726` (current main). No port-gap fix.
- **`tests/pipeline_1465_for_integration_tests`** — based on the above, with `c1906ec` (`Fix PORT_GAP_BEGIN anchorage when vessel silently changes port`) cherry-picked on top. New HEAD is `657c584`.

Minimal A-vs-B diff: 3 files, 115 insertions — `CHANGES.md` (entry), `pipe_anchorages/transforms/create_in_out_events.py` (the 6-line behaviour fix), and `tests/test_create_in_out_events.py` (regression test). Nothing else differs, so any output divergence from the cross-version run is attributable to the fix.

Dry-run validated through `cross_version_ais.py` with these bindings on 2026-05-15:

```
dit run workflows/port_visits/cross_version_ais.py \
    --experiment-id pipeline-1465 \
    --pin-source-at 2026-05-15T10:00:00Z \
    --binding before=tests/temp_dataset_for_integration_tests \
    --binding after=tests/pipeline_1465_for_integration_tests \
    --modes 1_bf \
    --runner dataflow --parallel --build-from-source
```

The dry-run goes through `git rev-parse` of both refs (`cb916bf` and `657c584`), creates `dit_exp_pipeline_1465_{internal,published}` snapshot datasets, snapshots the three input tables at the pin timestamp, sets up and tears down worktrees for each binding. Removing `--dry-run` flips this into the real run.

Both branches are **local-only and intentionally untracked upstream**; they're scaffolding for the integration test, not branches to be merged. When the `--temp_dataset` PR lands upstream, the better long-term shape is to rebase the bindings on top of the merged version and drop these synthetic branches.

### 2026-05-15 — Cross-version experiment glue (port-visits AIS)

`workflows/port_visits/cross_version_ais.py` ties together the BQ snapshot helpers (`42ef37f`) and experiment-ID linkage (`244521d`) into an end-to-end command. Given `--experiment-id`, `--pin-source-at <iso>`, and N `--binding name=ref` pairs, it:

1. Verifies refs exist in `$PROJECTS/anchorages_pipeline`.
2. Creates `dit_exp_<sanitized_exp_id>_{internal,published}` snapshot datasets (7-day default expiration).
3. Snapshots the three port-visits input tables (`messages_positions`, `segment_info`, `segs_activity`) at the pin timestamp into those datasets.
4. For each binding: `git worktree add` at the ref, runs `ais.py` from the worktree with `--source-dataset-stem=<snap>` and `--suffix=<exp>-<binding-name>` (deterministic so the diff step doesn't need INFORMATION_SCHEMA discovery), tears down the worktree.
5. For each mode in `--modes`, diffs the corresponding output tables pairwise across bindings on `visit_id`.

Diff outcomes are reported but do not fail the run — non-empty diff is *information* for cross-version testing, not error. Real failures (missing ref, snapshot error, ais.py exits non-zero) exit non-zero.

`--dry-run` runs every step except the ais.py invocations and the diff phase — useful for validating orchestration without Dataflow cost. Validated end-to-end this way against (`v4.6.4`, `fix/PIPELINE-1465_port_visit_start_location`) on 2026-05-15.

**Precondition for actually using this on real bindings.** Every binding's pipe-anchorages source must support the `--temp_dataset` CLI flag — without it, the Dataflow SA hits the BQ EXPORT-staging permission error (see the 2026-05-15 Phase 2 entry below). The flag lives on the local `dit-temp-dataset-support` branch; PR pending. For the immediate PIPELINE-1465 cross-version test we want to motivate, the cleanest path is to cherry-pick the `--temp_dataset` patch onto both comparison refs (or land it upstream first) before invoking `cross_version_ais.py`.

### 2026-05-15 — `--experiment-id` / `DIT_EXPERIMENT_ID` for cross-version run linkage

- Output-table suffix shape grows a leftmost slot: `<experiment_id>_<commit>[_dirty]_<uuid>`. Leftmost so BQ prefix scans cluster by experiment naturally. `<uuid>` slot preserved so parallel mode-equivalence runs sharing a commit still don't clobber each other. `--suffix` (full manual override) bypasses the experiment-id slot entirely — byte-equivalent backward-compat guarantee.
- New `--experiment-id <slug>` flag on both `workflows/pipe_gaps/mode_equivalence.py` and `workflows/port_visits/ais.py`. Env-var fallback `DIT_EXPERIMENT_ID` (matches the established `DIT_*` convention; empty string treated as unset). Auto-default `solo_<6-hex>` when neither flag nor env var is set — the literal `solo_` prefix marks "not part of a cross-version experiment" so BQ filtering can ignore them. Validation regex `^[a-z0-9][a-z0-9_-]{0,31}$` compiled once at module level; invalid input raises `SystemExit` with a clear message (applied to both CLI input and env-var defaults).
- This is **the second half of the cross-version experiments capability** that started with the `dit.bq.snapshot_*` helpers (entry above). The two halves are decoupled on purpose: snapshots pin source data; experiment-id clusters output tables. Either is useful alone; together they enable end-to-end byte-equivalence runs across pipeline versions.
- `_git_info` stays duplicated across the two workflows. Anchored in decision 7 (duplicate-until-3): defer extraction to `dit.git_info` until pipe-events lands (Phase 3); this change is parallel edits, not shared-behaviour drift.
- Backward-compat guarantees honoured: (1) when neither flag nor env var is set, the auto-generated `solo_<6-hex>_<commit>_<uuid>` is still unique-per-invocation (the `<uuid>` ensures uniqueness; `solo_<6-hex>` adds clustering); (2) `--suffix` full override produces byte-identical output to today; (3) `--allow-dirty-tree` semantics unchanged (`_dirty` still appears between commit and uuid).
- No changes to table-name builders, comparison logic, runners, or `dit.bq` / `dit.compare` / `dit.dates`. No `docs/plan.md` text changes — no architectural decision changed; this is a thin workflow-side feature. `README.md` § Features: "Workflow file conventions" bullet extended to mention the new flag/env var and the `solo_<6-hex>` default shape.

### 2026-05-15 — `dit.bq` snapshot helpers for source-data pinning

- Added `dit.bq.snapshot_table(source, dest, *, as_of=None, expiration=None, project=..., if_not_exists=False)` and `dit.bq.snapshot_dataset(source_dataset, dest_dataset, *, tables=None, as_of=None, expiration=None, project=...)`. Both shell out to `CREATE SNAPSHOT TABLE … CLONE …` DDL; the dataset variant lists tables and loops, skipping any already present in dest (idempotent) and raising if dest dataset doesn't exist.
- This is **the first half of an upcoming cross-version experiments capability** — snapshot the inputs once so cross-version test runs (`pipe-gaps@main` vs `@pr-NNN`, etc.) only see differences attributable to the pipeline code, not source-data drift. The second half (experiment-ID linkage into output-table suffixes) is being designed by a parallel agent and is intentionally not coupled to these helpers; consumers can use the snapshots today without the experiment plumbing.
- BQ snapshots chosen over time-travel-in-queries: pipeline-agnostic (no source changes to pipe-gaps / pipe-anchorages / pipe-events), persists beyond BQ's 7-day time-travel window, and storage is delta-only. Docstring on `snapshot_table` carries the rationale.
- Lazy-imports `google.cloud.bigquery` inside both helpers (deviates from the existing top-import in `dit/bq.py`, but matches the spec instruction; in-place rewrite of the existing helpers' imports was out of scope).
- `tests/test_bq.py` (new): 11 mock-based tests covering DDL shape (plain / `as_of` / `expiration` / `if_not_exists` / both clauses / custom project) plus dataset-level cases (list-and-snapshot, table filter, skip-existing, raise-on-missing-dest, forward kwargs).
- Docs updated in the same commit: `docs/plan.md` § "Public API contracts → `dit.bq`" now lists both signatures with one-line notes; `README.md` § Features extends the BQ utilities bullet to mention the snapshot helpers.

### 2026-05-15 — Fix `dit.runners.docker` network-teardown defect

Follow-up to the defect flagged in the prior 2026-05-15 entry. `dit.runners.docker.run` (build_from_source path) now wraps the docker invocation in a `try/finally` and calls a new `_teardown_compose_network()` helper that runs `docker network rm <project>_default` after each call. Used `docker network rm` directly rather than `docker compose -p <name> down` so cleanup doesn't depend on a compose file being present in CWD; external volumes (e.g. the `gcp` auth volume) aren't touched. Idempotent — silently no-ops if the network is gone, in use, or never existed. No contract change to `docker.run()`; signature identical.

### 2026-05-15 — Phase 2 AIS-staging verification: passed; integration findings

First end-to-end run of `workflows/port_visits/ais.py --runner dataflow --parallel --build-from-source` against the staging cohort. Suffix `cb916bf_94dde7`, output in `world-fishing-827.tech_great_expectations.port_visits_..._{1_bf,2_bfd,3_bftruncate}`. All three pairwise comparisons returned `rc=0` on `visit_id` — **port-visits is mode-equivalent across bf / bfd / bftruncate on the 2020 AIS-staging cohort.**

Four integration issues surfaced during the run; each is now fixed or documented:

1. **`--labels` required by pipe-anchorages** (fixed, `18689dc`). `transforms/sink.cloud_to_labels` iterates `cloud_options.labels` without a None guard. `_dataflow_pipeline_options` now emits five `--labels=k=v` flags matching the shape composer uses.
2. **No `--temp_dataset` plumbing in pipe-anchorages** (fixed by local upstream patch). The `automated-testing@` SA lacks `bigquery.datasets.create`, so Beam's auto-named `beam_temp_dataset_<uuid>` fails. Pipe-gaps' workflow sidesteps this in-process via `_DagFactoryWithTempDataset`; pipe-anchorages runs Beam inside a container with no equivalent hook. Local patch on `anchorages_pipeline@dit-temp-dataset-support` (commit `cb916bf`) adds a `--temp_dataset` CLI flag + threads it through `QuerySource` to `ReadFromBigQuery`. Our workflow surfaces it as `--bq-temp-dataset` with `DIT_BQ_TEMP_DATASET` env-var fallback (`9227cb8`). **Upstream PR pending team review.**
3. **Workers couldn't `import pipe_anchorages`** (fixed, `3a400ee`). Default Beam SDK image doesn't have pipe_anchorages installed. Workflow now passes `--sdk_container_image=us-central1-docker.pkg.dev/gfw-int-infrastructure/core/pipe-anchorages:v4.6.4`. Default is the published v4.6.4 image; workers don't need the local `--temp_dataset` patch because that change only affects local pipeline construction.
4. **SA needs dataEditor on the output dataset.** `scratch_christian_homberg_ttl120d` doesn't grant `automated-testing@` `bigquery.tables.create`. `.envrc` switched to `DIT_DEST_DATASET=tech_great_expectations` (team-shared, SA pre-blessed). Personal scratch usage requires a one-time IAM grant — documented in `.envrc`.

**Known follow-up — `dit.runners.docker` network-cleanup defect.** The `build_from_source` path runs `docker compose -p <unique-uuid> run --rm dev …` per invocation, which leaves a `<project>_default` bridge network behind even after the container exits. Across many runs Docker's address pool (default 172.16-172.31, /24 each) fills up and new networks fail with "all predefined address pools have been fully subnetted." Worked around once via `docker network prune`, but the runner should `docker compose -p <name> down` after each invocation. Not blocking, but worth fixing alongside Phase 2 (relevant to anyone running `--build-from-source` workflows repeatedly).

**No plan-doc text changes this entry** — purely a record of what worked and what got patched along the way. The `docs/plan.md` § Phase 2 verification path stands.

### 2026-05-14 — Per-user infra knobs via DIT_* env vars

- Both workflows now resolve user-overridable infra knobs through `os.environ.get("DIT_<NAME>", "<default>")` with corresponding CLI flags that override env vars per-invocation. Set up so a single `export DIT_DEST_DATASET=scratch_chris` in `.envrc` redirects all output tables for personal dev without editing source.
- The knob set (applies to both workflows except where noted):
  - `DIT_DEST_DATASET` -> output dataset for per-mode tables (new — was hardcoded).
  - `DIT_DATAFLOW_SA` -> service account for Dataflow workers.
  - `DIT_DATAFLOW_REGION` / `DIT_DATAFLOW_TEMP_BUCKET` / `DIT_DATAFLOW_SUBNETWORK` -> dataflow placement.
  - `DIT_BQ_TEMP_DATASET` -> pipe-gaps-only; defaults to `${PROJECT}.${DIT_DEST_DATASET}` if unset.
- Added `--dest-dataset` CLI flag to both workflows (was missing on both). Other infra flags already existed.
- Per-workflow `DEST_DATASET` constant removed; table-name helpers now take `args` and read `args.dest_dataset`. Callsites in `_run_slice` and `compare_all` updated.
- `PROJECT` and pipeline-specific knobs (image tag, source dataset stem, tuning params, dates) deliberately kept as constants/CLI flags only -- they're not "per-user infra" and an env-var explosion adds more noise than value.
- `.envrc.example` rewritten to document the full env-var set with team defaults inline; users uncomment what they want to override. README's install section already points at `.envrc.example`.

### 2026-05-14 — Phase 2 spike: AIS-staging port-visits workflow

- Added `workflows/port_visits/ais.py` — first port-visits workflow (AIS staging cohort, 2020-only, reduced data). Three modes (bf / bfd / bftruncate); two-step thin_port_messages → port_visits chain per slice; partitioned-write semantics in both pipe-anchorages steps mean re-runs over overlapping date ranges are idempotent (verified by reading pipe-anchorages source).
- This is the **abstraction-validation step**: first real exercise of `dit.compare.compare_tables(view_suffix="", keys=["visit_id"])` (truncate shape, no SCD-2) and the docker runner's `entrypoint` extension (`entrypoint="pipe-anchorages"`).
- Default `--runner=dataflow` matches what gaps recently used. The runner here is `dit.runners.docker` + Beam pipeline options inside the container (`--runner=DataflowRunner --wait_for_job ...`). Different from pipe-gaps' workflow which uses `dit.runners.dataflow` in-process — pipe-anchorages doesn't expose a `gfw.common.beam.pipeline.Pipeline`-shaped object the in-process runner could consume, so the workflow submits via the container CLI like composer's `KubernetesPodOperator` does. This divergence is worth knowing when Phase 5 considers extracting a unified runner primitive.
- Date semantics: AIS workflow uses **inclusive** `--start`/`--end` to match pipe-anchorages' CLI. Pipe-gaps' workflow uses **half-open** dates. The wart is unavoidable given the downstream tools' contracts and is documented in the workflow header.
- `_git_info` was lifted verbatim from `workflows/pipe_gaps/mode_equivalence.py`. When pipe-events lands as the third consumer (Phase 3), promote into `dit.git_info`.

### 2026-05-14 — Add reproducible-install targets (snapshot + specific-ref)

- Added six Makefile targets covering reproducible pipeline installs alongside the editable ones:
  - `install-<pipeline>-ref REF=<sha-or-branch>`: `pip install --force-reinstall --no-deps "git+file://<dir>@<ref>"` — non-editable, exactly that commit, ~5-10s.
  - `snapshot-<pipeline>`: uses `git stash create` to capture tracked working-tree changes into a real commit (working tree untouched), anchors on a `dit-snapshot-<epoch>` branch so git GC keeps it alive, then installs from that ref.
  - `clean-snapshots`: GCs the `dit-snapshot-*` branches across all three pipeline checkouts.
- Added `scripts/snapshot-install.sh` and `scripts/clean-snapshots.sh` to keep the git plumbing out of the Makefile recipes. `set -euo pipefail`, single-purpose, ~30 lines each.
- `FULLDEPS=1` toggles `--no-deps` off for the rare case where the target ref bumped or added a transitive dep.
- Trade-offs vs editable installs documented in `README.md`: the non-editable mode adds ~5-10s pip rebuild per iteration (acceptable given the integration-test cadence) and points the debugger at the installed snapshot rather than your dev tree (the only un-mitigated cost). `git stash create` captures **tracked** changes only — untracked source files need `git add` first.
- These targets are also the foundation for cross-version testing per `docs/framework-vision.md` § 6: same Makefile + script can install `pipe-gaps@main` and `pipe-gaps@pr-NNN` side-by-side for a single workflow invocation.

### 2026-05-08 — Restructure install: drop workflow deps from base, add Makefile

- `requirements.txt`: dropped `gfw-common[bq,beam]` and `apache-beam[gcp]`. Neither is imported by anything under `src/dit/` (verified by grep) — `gfw-common` is workflow-shaped and pipe-gaps declares it as a direct dep (`gfw-common[bq,beam]~=0.10`), so installing pipe-gaps brings it transitively. apache-beam is in the same boat (lazy-imported by `dit.runners.dataflow`; transitively pulled by `gfw-common[beam]`). Keeping them in the base required every consumer of `dit` to depend on the GFW private index even if they only used the docker runner.
- Added `Makefile` with `install-pipe-gaps` / `install-port-visits` / `install-pipe-events` / `install-all` targets. Each runs a single `pip install -e ".[dev]" -e $(PROJECTS)/<pipeline>` so workflow deps install **editable** — switching branches in `pipe-gaps` etc. is picked up without a reinstall. pyproject `[project.optional-dependencies]` was considered but rejected: PEP 508 `@ file://...` extras install as built wheels, defeating editable mode and creating a stale-snapshot footgun when iterating on pipe-gaps branches.
- `PROJECTS` defaults to `$(realpath ..)` (sibling checkouts). Override via env var or by copying `.envrc.example` → `.envrc` (gitignored; loaded by direnv).
- `docs/plan.md` § "Repo layout (Phase 1, concrete)": updated `requirements.txt` comment to reflect the framework-only deps and added `Makefile` / `.envrc.example` to the tree.
- `README.md`: install section now points at `make install-pipe-gaps` and documents the `PROJECTS` env var.

### 2026-05-08 — Initial architectural alignment (pre-implementation)

- Added `docs/plan.md` § **Architecture: three-repo split** — explicit ownership boundaries between processing repos, `composer-dags-production`, and `data_integration_tests`, plus where `table_identical_checks` sits.
- Extended `docs/plan.md` § **Decisions (recommended)** with items 6–9: `dit` library-first; workflow file location policy (canonical in `dit/workflows/`, in-repo allowed for spikes, no duplication); per-pipeline config dataclasses stay in composer-dags; `dit.compare` is a thin shim.
- Added `docs/plan.md` § **Public API contracts (Phase 1)** — typed signatures for `dit.runners.{base,docker,dataflow}`, `dit.compare.compare_tables`, `dit.bq` helpers, `dit.dates`, plus the workflow entry-point convention (`def main(argv=None) -> int`).
- Added `docs/plan.md` § **Phase 1 subagent task breakdown** — five tracks (1–3 parallelisable, 4 depends on 2+3, 5 last).
- Confirmed Phase 4 param sync is pull-based (`dit sync-params --from <composer-dags-checkout>`); plan text already aligned, recorded for traceability.

### 2026-05-08 — Track 3 dataflow-runner contract refinements

- `docs/plan.md` § **Public API contracts (Phase 1) → `dit.runners.dataflow`**: added two parameters not in the original signature.
  - `pipeline_builder: Callable[[Mapping[str, Any]], Any]` (required) -- the original `_run_dataflow` constructed `DetectGapsConfig` / `DetectGapsLinearDagFactory` directly. Hardcoding those into a shared runner would re-couple `dit` to pipe-gaps and break decision 5 (three consumers from day one). The workflow now passes a builder that returns a `gfw.common.beam.pipeline.Pipeline`-shaped object; the runner only owns the lock-split submit/wait around it.
  - `dag_factory_cls: type | None = None` (optional) -- `_DagFactoryWithTempDataset` ports across as a generic on-the-fly subclass that overrides `read_from_bigquery_factory`. Workflows pass their own factory class; the runner wraps it when `bq_temp_dataset` is set and forwards the wrapped class through the options mapping.
- `env` parameter is kept for `Runner`-protocol parity but the dataflow runner logs-and-ignores it (in-process; no subprocess to forward to).
- Docker runner: `build_from_source=True` switches to `docker compose -p <name>-<uuid> run --rm dev <args>`; the published path is `docker run --rm --name <name>-<uuid> <image_tag> <args>`. Both keep per-call uniquification to avoid the network race documented in the source.

### 2026-05-08 — Track 4 docker-runner contract extension

- `docs/plan.md` § **Public API contracts (Phase 1) → `dit.runners.docker`**: added `entrypoint: str | None = None`. Required by Track 4: pipe-gaps' dev image has no default `pipe-gaps` entrypoint baked in, so the original test passed `--entrypoint pipe-gaps` to `docker compose run`. Without runner-level support, the workflow would have to bypass the runner. The parameter is a clean opt-in: workflows whose images bake the right entrypoint omit it.

### 2026-05-08 — Track 6 review fix

- `pyproject.toml`: added `pythonpath = ["src"]` under `[tool.pytest.ini_options]`. Without it, `pytest tests/` fails to import `dit.*` unless run with `PYTHONPATH=src` or after `pip install -e .`. No code changes; tooling-only.

### 2026-05-08 — Track 2 utility-module contract refinements

- `docs/plan.md` § **Public API contracts (Phase 1) → `dit.bq`**: settled `query_for_restricted_ssvids` kwargs as `(reference_table, *, mid, backfill_days_w, seed=42, project=…)`. Dropped the source's `n_hours_before` argument (unused at the call site; logged as such by the source itself). Added a note that `drop_tables` requires `<project>.<dataset>.<stem>` form so the dataset can be enumerated.
- `dit.compare.compare_tables` keeps the `ignore_columns` parameter from the contract but raises `NotImplementedError` if passed non-empty: `table-check summary` does not yet support an ignore-columns flag, and CLAUDE.md prohibits reimplementing comparison features in the shim. The signal pushes the feature upstream into `table_identical_checks`. `tolerance` is forwarded as `--tolerance=<col>:<value>` (table-check's per-column syntax).
- `dit.dates.daterange_inclusive` is half-open (`start <= d < end`) despite the name -- preserved verbatim from `pipe-gaps` so the four-mode equivalence test stays byte-equivalent across the move. Pinned by `tests/test_dates.py`. If the migration ever wants true inclusive semantics, it has to flip the call sites simultaneously.
