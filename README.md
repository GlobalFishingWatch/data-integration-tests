# data_integration_tests

Cross-pipeline integration-test framework for GFW data pipelines (`pipe-gaps`, `anchorages_pipeline`/port-visits, `pipe-events`/fishing).

The `dit` CLI drives Python workflow files that orchestrate phases (pipeline invocations) across modes (trigger patterns: bf / bfd / bftruncate / mutate-recover) and assert equivalence on the resulting BQ tables via `table-check summary`.

## Features

The framework is intentionally thin — a small library plus per-pipeline workflow files. Capabilities, in scope today:

- **Pipeline-agnostic runners** (`dit.runners.docker`, `dit.runners.dataflow`). The docker runner invokes a published or locally-built image; the dataflow runner submits an in-process Beam pipeline with lock-split submit/wait. Both used by Phase 1; the docker runner doubles for "Beam-in-container submits to Dataflow" workflows too (port-visits' shape).
- **Comparison shim** (`dit.compare.compare_tables`) — thin wrapper over `table-check summary` from the [`table_identical_checks`](https://github.com/GlobalFishingWatch/table_identical_checks) repo. Per-column tolerances, `view_suffix` for SCD-2 last-versions vs. truncate-shape, `keys` for the comparison join.
- **BQ + date utilities** (`dit.bq`, `dit.dates`) — drop tables by prefix, query for restricted ssvids, snapshot a table or whole dataset for source-data pinning across cross-version runs, half-open date iteration. Used by workflows that need pre/post-run setup or computed inputs.
- **Library-first**: anything you can do via `dit run …` you can also do by importing `dit.*` from a pytest target or another Python script. The CLI is one consumer of the library, not the only one.
- **Workflow file conventions**: per-pipeline workflows live in `workflows/<pipeline>/<name>.py` and expose `main(argv) -> int`. Output tables tagged with `<experiment_id>_<commit>_<uuid>` for provenance; `--experiment-id` / `DIT_EXPERIMENT_ID` clusters N runs (e.g. `pipe-gaps@main` vs `@pr-NNN`) under one BQ-prefix-scannable slug, defaulting to `solo_<6-hex>` when unset. (`--allow-dirty-tree` exists today as a transition aid but is **scheduled for removal** — see [`docs/no-dirty-tree-pivot.md`](docs/no-dirty-tree-pivot.md). Going forward, dit auto-snapshots dirty trees to a pushed ref and runs reproducibly.)
- **Three install modes** per pipeline: editable (fast inner loop), specific-ref (`REF=<sha-or-branch>`), and snapshot (`git stash create` → anchored on a `dit-snapshot-<epoch>` branch). See Usage § Install modes below.
- **Per-user infra knobs via `DIT_*` env vars**: `DIT_DEST_DATASET`, `DIT_DATAFLOW_SA`, `DIT_DATAFLOW_REGION`, `DIT_DATAFLOW_TEMP_BUCKET`, `DIT_DATAFLOW_SUBNETWORK`, `DIT_BQ_TEMP_DATASET`. Plays cleanly with direnv via `.envrc.example`.
- **Pipeline integration contract** ([`docs/pipeline-contract.md`](docs/pipeline-contract.md)) — what a pipeline must expose to be cleanly testable by `dit`, with an adoption matrix tracking where each current pipeline stands.
- **Cross-version experiments** (`workflows/port_visits/cross_version_ais.py`) — pin source data via BQ snapshots at a fixed timestamp, run a workflow at N pipeline-version bindings (git refs in the pipeline checkout), then diff corresponding output tables pairwise. Foundation for PR-validation comparisons (`pipe-anchorages@main` vs `@pr-NNN`).
- **Content-addressable run cache** (`dit.cache` + `world-fishing-827.tech_great_expectations.dit_runs`) — every `execute_*` mode hashes `(pipeline_commit, worker_image_digest, workflow_file_sha1, params)` and looks up the run-cache table. On hit (output tables still present), the workflow skips Dataflow entirely and uses the cached FQN for downstream comparisons; on miss, it runs and inserts a row with output tables + provenance. The same table also serves as registry + cleanup source for `make dit-cancel` (M5, in flight). Pipe-gaps wired in 2026-05-22; port-visits next. Design: [`docs/run-cache.md`](docs/run-cache.md); milestone tracker: [`docs/run-cache-impl.md`](docs/run-cache-impl.md).
- **Centralised Dataflow job-name builder** (`dit.job_names.make_job_name`) — single source of truth for the `dit-<repo>-<step>-<exp>-<binding?>-<mode?>-<N?>-<M?>` shape, with 63-char truncation that preserves the load-bearing tail. Both workflows use it; the prefix lets a single label-filter find every dit-launched Dataflow job in the UI.
- **Auto-built worker images** (`dit.worker_image.ensure_worker_image`) — Dataflow runs load pipeline code from two sources (submitter from `/workspace`, workers from the `--worker-image` container). When a run executes unreviewed code against the default worker image, the workers would otherwise run the stale published code; `ensure_worker_image` builds a content-addressable worker image (`gcr.io/world-fishing-827/dit/<pipeline>:dit-<commit>`) from the source via a kaniko Cloud Build and points the run at it. Idempotent (existing tag → skip); no-op for reviewed code, an explicit `--worker-image`, or the docker runner. Replaces the old `warn_if_worker_image_misses_dirty_tree` warning (M-pivot-4) — closing the gap rather than flagging it.
- **Cloud Build ad-hoc runtime** (`cloudbuild-dit.yaml`, `make dit-cloud`, `make publish-ditbox`) — submit a workflow run to Cloud Build with one command. The pipeline checkout flows through as the build source; dit is cloned fresh per run at a configurable ref. ditbox builds via kaniko with a registry-backed layer cache; per-run pipeline installs use `uv` (~10× faster than pip). `make dit-cloud` is **fire-and-forget by default** (`gcloud builds submit --async`): it snapshots (if dirty), uploads the source, and triggers the build, returning a build id in seconds — the worker-image build + Dataflow run happen cloud-side. Pass `WAIT=1` to stream logs and block on the result (for CI exit codes or live debugging). The `_BEAM_VERSION` substitution pins apache-beam to match the worker image's SDK (per-pipeline defaults in the Makefile: `pipe-gaps=2.71.0`, `anchorages_pipeline=2.69.0`). Validated end-to-end 2026-05-22 — caught a real `pipe-gaps:v0.9.6` mode-equivalence bug and verified the fix against a custom-published worker image. PR-trigger integration in each pipeline repo comes as a follow-up.

Pipeline-shape primitives (`Phase`/`Mode`/`Mutation`/`Oracle` dataclasses, mutation library, phase-sharing via BQ COPY, golden-table regression mode) are **deliberately not extracted yet** — see Roadmap below for why and when.

## Read first

- [`docs/architecture.md`](docs/architecture.md) — visual reference: repo ownership, run modes, workflow flows, Cloud Build runtime, image namespace. Mermaid diagrams that render on GitHub.
- [`docs/plan.md`](docs/plan.md) — implementation plan, three-repo split, public API contracts, Phase 1 task breakdown.
- [`docs/conventions.md`](docs/conventions.md) — prod-infra boundary, dit image namespace (`gcr.io/world-fishing-827/dit/*`), standard build-and-push workflow.
- [`docs/pipeline-contract.md`](docs/pipeline-contract.md) — what a GFW pipeline must expose to be cleanly integration-testable; adoption matrix for the three current pipelines. Audience: pipeline maintainers.
- [`docs/context.md`](docs/context.md) — background, source bugs the framework caught, branch state at handoff.
- [`docs/framework-vision.md`](docs/framework-vision.md) — long-term shape (don't optimise for it; Phase 1 stays imperative).
- [`CLAUDE.md`](CLAUDE.md) — working agreements and Plan changelog.

## Usage

### Install

```bash
python3 -m venv venv && source venv/bin/activate
make install-pipe-gaps      # or install-port-visits / install-pipe-events / install-all
```

`dit` is pipeline-agnostic; workflow dependencies (`pipe-gaps`, `anchorages_pipeline`, `pipe-events`) are not in `dit`'s base `requirements.txt`. By default the Makefile assumes sibling checkouts (`$(realpath ..)`). If yours live elsewhere, prepend `PROJECTS=/path` to any target or copy `.envrc.example` → `.envrc` (gitignored; loaded by [direnv](https://direnv.net/)).

For the framework only (no workflow deps), `make install` works — but the dataflow runner won't load without a workflow install bringing `apache-beam[gcp]` transitively.

### Install modes

| When | Target | What happens |
|---|---|---|
| Active dev on a pipeline (fast inner loop) | `make install-<pipeline>` | `pip install -e <pipeline-dir>` — working-tree edits picked up immediately. |
| Reproducible run against a specific committed ref | `make install-<pipeline>-ref REF=<sha-or-branch>` | `pip install --force-reinstall --no-deps git+file://...@<ref>` — non-editable, exactly that commit, ~5-10s per ref. |
| Test what's currently in the working tree, reproducibly | `make snapshot-<pipeline>` | Builds a deterministic orphan commit from the dirty tracked-files (temp-index `git write-tree` + `git commit-tree` with frozen dates/identity) → pushes to `refs/dit-snapshots/<pipeline>/<commit-short-sha>` on origin → installs from that ref. Identical tree → identical SHA → cache hits on repeat invocations. Parent SHA recorded in the commit message. |
| Pipeline's transitive deps changed in target ref | add `FULLDEPS=1` | Drops `--no-deps`, lets pip reinstall the full dep tree (slower; only needed when the target ref bumped or added a dep). |
| Remove a specific snapshot ref (secret-leak remediation only) | `make clean-snapshot PIPELINE=<name> REF=<sha-or-full-ref>` | Deletes the ref locally and on origin in one step. Snapshots otherwise live forever by design (bytes-scale, hidden namespace). |

Notes on snapshot mode: only files **already tracked in HEAD** are captured (modifications + deletions). Untracked files — including files you `git add`-ed but haven't `git commit`-ed — are **not** included; commit a new file first if you want it in the snapshot. This is the deliberate safety default (see § Safety in Scenario B below); auto-push goes to the pipeline's origin (potentially public), so the capture boundary is "things you've explicitly chosen to track" rather than "anything in the working tree". Requires `git push` permission on the pipeline's origin and `gitleaks` on `PATH` (or `DIT_SKIP_SECRET_SCAN=1` to bypass with a loud banner).

### Run a workflow

```bash
dit run workflows/<pipeline>/<workflow>.py [workflow-args...]
```

The CLI loads the Python module at the given path and invokes its `main(argv) -> int` entry point. Workflows can also be imported directly from a pytest target — `dit` is library-first, CLI-second.

Example (Phase 2 AIS-staging, the verified mode-equivalence test for port-visits):

```bash
dit run workflows/port_visits/ais.py --runner dataflow --parallel --build-from-source
```

## Usage scenarios

dit covers a small set of orthogonal axes, summarised first, then walked through as concrete scenarios. **Best-practice paths are flagged ⭐.**

### The orthogonal axes

| Axis | Choices |
|---|---|
| **Where** | local (`dit run …`) / Cloud Build (`make dit-cloud …`) |
| **Pipeline ref** | clean HEAD of pipeline checkout / auto-created snapshot under `refs/dit-snapshots/<pipeline>/<commit-short-sha>` (content-addressable; one ref per distinct working-tree state) / a specific committed ref (`make install-<pipeline>-ref REF=…` or `make dit-cloud REF=…`) / a PR head SHA (automated, future) |
| **Worker image** | published default (e.g. `pipe-gaps:v0.9.6`) / custom-built from a ref (for changes that touch worker code) |
| **Single vs cross-version** | one ref / N refs compared (`workflows/port_visits/cross_version_ais.py`) |
| **Trigger** | manual CLI / PR event (automated, future) / scheduled (future) |

> **Future-state note** (see [`docs/no-dirty-tree-pivot.md`](docs/no-dirty-tree-pivot.md)): under the in-flight pivot, **every dit run executes a committed git ref**. If your working tree is dirty when you invoke dit, the workflow auto-snapshots to `refs/dit-snapshots/<pipeline>/<commit-short-sha>` (content-addressable — identical tree state always produces the same SHA, so repeat runs of unchanged uncommitted code hit the cache), auto-pushes, and uses that ref — no `--allow-dirty-tree` flag, no special-case logic, every cache row reproducible. The scenarios below describe the end state.

### Scenarios

#### ⭐ Scenario A — PR validation (the canonical automated path)

You're working on a pipeline change; you commit it; push to a branch; open a PR. dit fires automatically and compares the PR against `main`'s cached output.

```
$ cd $PROJECTS/pipe-gaps
$ git checkout -b fix/PIPELINE-1465-tiebreaker
$ git commit -am "Add deterministic tiebreaker to message sort"
$ git push -u origin fix/PIPELINE-1465-tiebreaker
$ gh pr create --title "Fix PIPELINE-1465 tiebreaker" --body "..."
                          # ← dit triggers here, automatically
                          # ← Check Run appears on the PR with the verdict
```

When to use: **default for any pipeline change you intend to land.** No commands beyond your normal git workflow. dit hits the cache for main's side (it's already been computed), runs the PR side fresh, posts the diff as a Check Run.

#### Scenario B — Iterating on uncommitted code (auto-snapshot)

You're mid-development; you want feedback before opening a PR. dit detects the dirty tree, auto-snapshots, auto-pushes, runs against the snapshot. Your working tree is untouched.

```
$ cd $PROJECTS/pipe-gaps && vim src/...   # edits, not committed
$ cd $PROJECTS/data_integration_tests
$ make dit-cloud PIPELINE=pipe-gaps WORKFLOW=workflows/pipe_gaps/mode_equivalence.py
                          # auto-snapshots → refs/dit-snapshots/pipe-gaps/<commit-short-sha>
                          # auto-pushes
                          # runs against the snapshot
```

When to use: **iterating on a fix that isn't PR-ready** (e.g. making a pipeline idempotent across modes). Each edit produces a fresh snapshot (new cache key, new MISS, full Dataflow workload). Once you're happy, commit + push the same code to a real branch + open a PR (Scenario A).

Caveats printed at snapshot time:
- First run against each new tree-state is a cache MISS; **repeat runs of an unchanged tree are a HIT** (snapshot SHA is content-addressable — derived from `git write-tree` with frozen author/committer dates, so identical tree → identical SHA → identical cache key).
- Only **tracked modifications + deletions** are captured. Untracked files (and files you `git add`-ed but haven't `git commit`-ed yet) are **never** in the snapshot — see § "Safety: auto-push and credential leakage" below for why this is the deliberate default. Commit a new file before running dit if you want it included.
- The cache row is tagged `unreviewed_code=TRUE` so PR-validation queries skip it.
- If your changes touch worker code, build+push a custom worker image too (`--worker-image=…`).
- **Requires `git push` permission on the pipeline repo.** If you're a read-only viewer, dit will fail at push time with a clear error pointing at `make install-<pipeline>-ref REF=<committed-ref>` or asking you to commit + push the changes via a normal branch first.

> **⚠ Safety: auto-push and credential leakage.** `make dit-cloud` pushes the snapshot to the pipeline repo's origin automatically. The pipeline repo may be a **public GitHub repository**. Treat every snapshot as potentially publicly visible.
>
> Three defense layers, smallest blast radius first:
>
> 1. **`git add -u` (tracked-only) is the default.** Only modifications + deletions to files already in HEAD enter the snapshot. Untracked files (and files you `git add`-ed but haven't `git commit`-ed yet) stay out. Rogue `.env`, downloaded `sa.json`, one-off `query_results.csv`, etc. are not captured.
> 2. **Pre-push banner shows you the changed paths** before the push happens. Visual review is the last-line-of-defence; if anything surprises you, Ctrl-C immediately.
> 3. **Pre-push secret scanner (gitleaks).** Before each push, the script extracts the snapshot tree to a temp dir and runs `gitleaks detect`. A finding aborts the snapshot. Required by default: if gitleaks isn't installed and no bypass env var is set, the snapshot refuses to proceed (`brew install gitleaks` / one-line install instructions in the error). Override is `export DIT_SKIP_SECRET_SCAN=1` and emits a loud `WARNING: secret scan BYPASSED` banner — for false positives only, never as a "make this work" shortcut. Ditbox has gitleaks pre-baked.
>
> Remediation if a snapshot containing secrets has already landed on origin:
>
> 1. **Rotate the credential.** Load-bearing step. Anything ever pushed to a public-shaped repo must be treated as compromised (history snapshots, forks, mirrors, indexers all make untoward pushes effectively permanent).
> 2. **`make clean-snapshot PIPELINE=<name> REF=<sha>`** — deletes the ref locally + on origin. Necessary but **not** sufficient on its own.
>
> If a tracked file contains a secret (the scanner caught one, or you noticed in the banner), the structural fix is `git rm --cached <file>` + add to `.gitignore` + commit the removal — then the next snapshot won't include it.

#### Scenario C — Running against any committed ref (testing main, a colleague's branch, an old commit)

You want to test a specific committed ref without changing what's installed.

```
$ make dit-cloud PIPELINE=pipe-gaps REF=main WORKFLOW=workflows/pipe_gaps/mode_equivalence.py
# or:
$ make dit-cloud PIPELINE=pipe-gaps REF=fix/PIPELINE-1465-tiebreaker WORKFLOW=...
# or to install locally and run from your venv (e.g. for ad-hoc inspection, or to drive Dataflow without Cloud Build):
$ make install-pipe-gaps-ref REF=fix/PIPELINE-1465-tiebreaker
$ dit run workflows/pipe_gaps/mode_equivalence.py --runner dataflow ...
```

When to use: testing main as a regression baseline; reviewing a teammate's PR locally; reproducing a historical run from a `dit_runs` row's `pipeline_commit`.

#### Scenario D — Cross-version testing (you want to compare two refs)

You have a fix and want to verify it doesn't regress anywhere it shouldn't.

```
$ dit run workflows/port_visits/cross_version_ais.py \
    --experiment-id pipeline-1465 \
    --pin-source-at 2026-05-15T10:00:00Z \
    --binding before=main \
    --binding after=fix/PIPELINE-1465-tiebreaker \
    --binding-worker-image after=gcr.io/world-fishing-827/dit/pipe-anchorages:fix-tiebreaker \
    --runner dataflow --parallel --build-from-source
```

When to use: **fix-verification before PR** (or as a separate verification run alongside Scenario A). Each binding runs against a frozen source snapshot at `--pin-source-at`; outputs are diffed pairwise.

Per-binding `--worker-image` is important when the change touches worker code — see memory `[[submitter-vs-worker-split]]`.

#### Scenario E — Reproducing a past run

A previous cache row says `pipeline_commit=<sha>`. You want to rerun against that exact code.

```
$ bq query --use_legacy_sql=false --project_id=world-fishing-827 \
    'SELECT pipeline_commit, params_json FROM `world-fishing-827.tech_great_expectations.dit_runs`
     WHERE run_id = "<rid>"'
$ make dit-cloud PIPELINE=pipe-gaps REF=<sha> WORKFLOW=workflows/pipe_gaps/mode_equivalence.py ARGS="..."
```

When to use: investigating a past diff; auditing a result; sanity-checking that a snapshot ref is still fetchable. Because every dit run executes a pushed ref, this works for anyone with read access to the pipeline repo — the row is genuinely portable.

#### Scenario F — Cancelling a stuck or unwanted run (M5)

```
$ make dit-cancel RUN_ID=<rid>
```

Looks up the run's Dataflow job IDs + output table FQNs in `dit_runs`, cancels the jobs, drops the tables, marks the row `cancelled`. Reads the same `dit_run_id` label dit stamps on every Dataflow job + BQ table from a run.

#### Scenario G — `make snapshot-<pipeline>` explicitly (advanced)

You can manually invoke the snapshot step before running dit. Equivalent to letting `make dit-cloud` auto-snapshot, but useful when you want to inspect the snapshot ref before the run, or when running multiple dit invocations against the same uncommitted code base (the second run hits cache against the first).

```
$ make snapshot-pipe-gaps
# Prints: Created refs/dit-snapshots/pipe-gaps/<commit-short-sha>; pushed to origin
#         (or: ref already exists on origin, skipping push — content-addressable).
$ make dit-cloud PIPELINE=pipe-gaps REF=refs/dit-snapshots/pipe-gaps/<that> ...
```

#### Cleanup

Snapshots live forever by design — bytes-scale storage on origin in a hidden ref namespace, no measurable cost. There's no periodic-cleanup target.

The one exception:

```
$ make clean-snapshot REF=refs/dit-snapshots/pipe-gaps/<sha>
```

Deletes the specified snapshot ref locally and on origin in one step. Intended for **secret-leak remediation** — e.g. you noticed a `.env` file or a credential was tracked in the working tree when you ran `make dit-cloud`. Surgical, user-invoked. The cache row in `dit_runs` retains `pipeline_commit_parent`, so removing the snapshot ref doesn't lose reproduce context for the rest of the row's lifetime.

### Decision tree

| Question | Answer | Scenario |
|---|---|---|
| Are you opening a PR? | Yes | **A** (best) |
| | Not yet — iterating | **B** (auto-snapshot) |
| | Testing an existing ref | **C** |
| Are you comparing two refs against pinned source? | Yes | **D** |
| Are you reproducing or auditing a past run? | Yes | **E** |
| Need to clean up a stuck run? | Yes | **F** (M5) |

## Roadmap

Each phase below is a short summary of what's planned and where we are. The canonical detail lives in [`docs/plan.md`](docs/plan.md); this section is the operational dashboard.

| Phase / capability | Scope | Status (2026-05-22) |
|---|---|---|
| **1 — pipe-gaps port** | Stand up the repo. Lift the four-mode mode-equivalence test from `pipe-gaps/tests/integration/mode_equivalence.py` onto `dit.*` helpers. Drop `--runner=local`. Replace the source file with a shim. | **Code complete + verified on real BQ 2026-05-22** (caught a real `pipe-gaps:v0.9.6` non-determinism bug; custom worker image with fix collapsed all 3 pairwise diffs to zero). Pending only: Track 5 shim swap in the `pipe-gaps` repo. |
| **2 — port-visits** | Ship AIS-staging, VMS, and AIS-full workflows for `anchorages_pipeline`'s two-step port-visits (`thin_port_messages` → `port_visits`). First real test of the `dit.compare` abstraction on the truncate-shape (`view_suffix=""`, `keys=["visit_id"]`). | **AIS-staging verified 2026-05-15** (3/3 pairwise green). VMS not started; AIS-full not started. |
| **Cross-version experiments** | BQ snapshot helpers (`dit.bq.snapshot_table`/`snapshot_dataset`), experiment-id linkage (`--experiment-id`/`DIT_EXPERIMENT_ID`), structured Dataflow job names + dynamic labels, and an end-to-end orchestrator (`workflows/port_visits/cross_version_ais.py`) for diffing pipeline outputs across versions against pinned input. | **Landed 2026-05-15.** Validated end-to-end via the PIPELINE-1465 cross-version test + the pipe-gaps `--worker-image` flow on 2026-05-22. |
| **Runtime & CI (Cloud Build)** | `ditbox` image + `cloudbuild-dit.yaml` + `make dit-cloud` / `make publish-ditbox` targets. Moves the orchestrator off the laptop; serves both `gcloud builds submit` ad-hoc and GitHub-webhook PR triggers. Tiered triggers (cheap AIS-staging on every PR, heavy AIS-full on label) come on top. | **Ad-hoc path battle-tested 2026-05-22** (kaniko cache, uv per-run installs, `_BEAM_VERSION` pin, `dit.job_names` centralisation, `dit.git_info` warn). Per-pipeline PR triggers pending. 30-concurrent-build slot cap is the next architectural ceiling — Cloud Run jobs flagged as the migration target if dit graduates to per-PR matrix testing. |
| **3 — pipe-events port** | Port `pipe-events/integration_tests/staging-bf_bfd_bftruncate.sh` (bash, no comparisons) to `workflows/pipe_events/fishing.py`. Add automated comparisons. Then decide whether to extract `Phase`/`Mode`/`Oracle` dataclasses based on three-consumer evidence. | Not started. |
| **4 — composer-dags param sync** | `dit sync-params --from <composer-dags-checkout>` reads production DAGs and regenerates `params.yaml`. Triggered when a real prod-vs-test param drift bug shows up. | Not started. |
| **5 — Mutation library** | Promote pipe-gaps' `compute_restricted_ssvids` into `dit.mutations` along with `drop_messages`, `shift_timestamps`, `set_segment_flag`. Cap at ~5 mutations. | Not started; waits for a second consumer. |
| **6 — Phase sharing** | Hash `(image-tag, phase-config, mutation-set)`; second invocation of an identical phase becomes a `BQ COPY` instead of a re-run. Cuts wall-clock for CI. | Not started; build only when duplicate-run cost matters operationally. |
| **7 — Golden-table mode** | Per-workflow reference `_1_bf` table keyed by `(image-tag, params-hash, date-range)`; future runs assert byte-equivalence vs. the golden table. Cheap PR-validation regression check. Implementable on top of the cross-version snapshot machinery. | Not started. |

**Operational next steps** (rolling, in priority order):

0. **No-dirty-tree pivot** ([`docs/no-dirty-tree-pivot.md`](docs/no-dirty-tree-pivot.md)). Deprecate `--allow-dirty-tree`; auto-snapshot+push dirty trees; rename `pipeline_dirty` → `unreviewed_code`; collapse the accumulated special-case logic before M5 inherits any of it into port-visits.
1. **Run cache M5 — port-visits integration + `make dit-cancel`.** Wire `dit.cache` into `workflows/port_visits/ais.py` via a `_run_with_cache`-style wrapper (structurally identical to the pipe-gaps M4 work that just landed in `workflows/pipe_gaps/mode_equivalence.py`). Add `make dit-cancel RUN_ID=<id>` reading the cache table to cancel Dataflow + drop output tables. Implement `cancel_run` in `dit.cache`.
2. **Run cache M6 — SIGTERM trap inside `dit run`** so Cloud Build cancellations cleanly tear down their own Dataflow + BQ artefacts via `cancel_run`.
3. **Per-pipeline PR triggers.** Wire `pipe-gaps` (most-verified) → `anchorages_pipeline` → `pipe-events` to fire dit on PRs. Trigger config lives in each pipeline repo; references `cloudbuild-dit.yaml` from the dit checkout. Path-filter on pipeline-relevant files; `dit:run` label as the escape-hatch override.
4. **PR comment integration via Check Runs.** `dit.report` module + GitHub Check Run API call from inside `dit run`. Typed contract end-to-end (no log parsing). Design doc TBD; separately, [`docs/llm-pr-gating.md`](docs/llm-pr-gating.md) covers the negative-signal-only LLM pre-filter that gates *whether* a PR triggers dit (orthogonal concern from how the verdict is posted).
5. **Track 5** — pipe-gaps repo shim, opportunistically (replace `pipe-gaps/tests/integration/mode_equivalence.py` with a thin re-export from `dit/workflows/pipe_gaps/...`).
6. VMS port-visits workflow, then AIS-full (the latter is what motivates Cloud Run jobs hardest once concurrent runs scale up).
7. PIPELINE-1465 cross-version full validation against newly-built pipe-anchorages images (parallel to the pipe-gaps validation just completed).
8. The first dit release tag (`v0.1.0`) once per-pipeline PR triggers + caching land — that's the natural "framework is usable by outsiders" milestone.

The canonical detailed version of this list lives in `docs/plan.md` § Next steps.
