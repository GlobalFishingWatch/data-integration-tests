# data_integration_tests

Cross-pipeline integration-test framework for GFW data pipelines (`pipe-gaps`, `anchorages_pipeline`/port-visits, `pipe-events`/fishing).

The `dit` CLI drives Python workflow files that orchestrate phases (pipeline invocations) across modes (trigger patterns: bf / bfd / bftruncate / mutate-recover) and assert equivalence on the resulting BQ tables via `table-check summary`.

## Features

The framework is intentionally thin — a small library plus per-pipeline workflow files. Capabilities, in scope today:

- **Pipeline-agnostic runners** (`dit.runners.docker`, `dit.runners.dataflow`). The docker runner invokes a published or locally-built image; the dataflow runner submits an in-process Beam pipeline with lock-split submit/wait. Both used by Phase 1; the docker runner doubles for "Beam-in-container submits to Dataflow" workflows too (port-visits' shape).
- **Comparison shim** (`dit.compare.compare_tables`) — thin wrapper over `table-check summary` from the [`table_identical_checks`](https://github.com/GlobalFishingWatch/table_identical_checks) repo. Per-column tolerances, `view_suffix` for SCD-2 last-versions vs. truncate-shape, `keys` for the comparison join.
- **BQ + date utilities** (`dit.bq`, `dit.dates`) — drop tables by prefix, query for restricted ssvids, snapshot a table or whole dataset for source-data pinning across cross-version runs, half-open date iteration. Used by workflows that need pre/post-run setup or computed inputs.
- **Library-first**: anything you can do via `dit run …` you can also do by importing `dit.*` from a pytest target or another Python script. The CLI is one consumer of the library, not the only one.
- **Workflow file conventions**: per-pipeline workflows live in `workflows/<pipeline>/<name>.py` and expose `main(argv) -> int`. Output tables tagged with `<experiment_id>_<commit>_<uuid>` for provenance; `--experiment-id` / `DIT_EXPERIMENT_ID` clusters N runs (e.g. `pipe-gaps@main` vs `@pr-NNN`) under one BQ-prefix-scannable slug, defaulting to `solo_<6-hex>` when unset; `--allow-dirty-tree` opt-in for dirty-tree runs.
- **Three install modes** per pipeline: editable (fast inner loop), specific-ref (`REF=<sha-or-branch>`), and snapshot (`git stash create` → anchored on a `dit-snapshot-<epoch>` branch). See Usage § Install modes below.
- **Per-user infra knobs via `DIT_*` env vars**: `DIT_DEST_DATASET`, `DIT_DATAFLOW_SA`, `DIT_DATAFLOW_REGION`, `DIT_DATAFLOW_TEMP_BUCKET`, `DIT_DATAFLOW_SUBNETWORK`, `DIT_BQ_TEMP_DATASET`. Plays cleanly with direnv via `.envrc.example`.
- **Pipeline integration contract** ([`docs/pipeline-contract.md`](docs/pipeline-contract.md)) — what a pipeline must expose to be cleanly testable by `dit`, with an adoption matrix tracking where each current pipeline stands.
- **Cross-version experiments** (`workflows/port_visits/cross_version_ais.py`) — pin source data via BQ snapshots at a fixed timestamp, run a workflow at N pipeline-version bindings (git refs in the pipeline checkout), then diff corresponding output tables pairwise. Foundation for PR-validation comparisons (`pipe-anchorages@main` vs `@pr-NNN`).
- **Cloud Build ad-hoc runtime** (`cloudbuild-dit.yaml`, `make dit-cloud`, `make publish-ditbox`) — submit a workflow run to Cloud Build with one command. The pipeline checkout flows through as the build source; dit is cloned fresh per run at a configurable ref. Moves long-running orchestration off the laptop. PR-trigger integration in each pipeline repo comes as a follow-up.

Pipeline-shape primitives (`Phase`/`Mode`/`Mutation`/`Oracle` dataclasses, mutation library, phase-sharing via BQ COPY, golden-table regression mode) are **deliberately not extracted yet** — see Roadmap below for why and when.

## Read first

- [`docs/plan.md`](docs/plan.md) — implementation plan, three-repo split, public API contracts, Phase 1 task breakdown.
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
| Test what's currently in the working tree, reproducibly | `make snapshot-<pipeline>` | `git stash create` captures tracked changes (working tree untouched), anchors on a `dit-snapshot-<epoch>` branch, installs from that ref. |
| Pipeline's transitive deps changed in target ref | add `FULLDEPS=1` | Drops `--no-deps`, lets pip reinstall the full dep tree (slower; only needed when the target ref bumped or added a dep). |
| GC the temp snapshot branches | `make clean-snapshots` | Removes `dit-snapshot-*` branches from all three pipeline checkouts. |

Notes on snapshot mode: `git stash create` captures **tracked** modifications only. Run `git add -A` in the pipeline repo first if untracked source files need to be in the snapshot. The snapshot branch persists for traceability until `make clean-snapshots`.

### Run a workflow

```bash
dit run workflows/<pipeline>/<workflow>.py [workflow-args...]
```

The CLI loads the Python module at the given path and invokes its `main(argv) -> int` entry point. Workflows can also be imported directly from a pytest target — `dit` is library-first, CLI-second.

Example (Phase 2 AIS-staging, the verified mode-equivalence test for port-visits):

```bash
dit run workflows/port_visits/ais.py --runner dataflow --parallel --build-from-source
```

## Roadmap

Each phase below is a short summary of what's planned and where we are. The canonical detail lives in [`docs/plan.md`](docs/plan.md); this section is the operational dashboard.

| Phase / capability | Scope | Status (2026-05-15) |
|---|---|---|
| **1 — pipe-gaps port** | Stand up the repo. Lift the four-mode mode-equivalence test from `pipe-gaps/tests/integration/mode_equivalence.py` onto `dit.*` helpers. Drop `--runner=local`. Replace the source file with a shim. | **Code complete.** Pending: real-BQ verification + Track 5 shim swap. |
| **2 — port-visits** | Ship AIS-staging, VMS, and AIS-full workflows for `anchorages_pipeline`'s two-step port-visits (`thin_port_messages` → `port_visits`). First real test of the `dit.compare` abstraction on the truncate-shape (`view_suffix=""`, `keys=["visit_id"]`). | **AIS-staging verified 2026-05-15** (3/3 pairwise green). VMS not started; AIS-full not started. |
| **Cross-version experiments** | BQ snapshot helpers (`dit.bq.snapshot_table`/`snapshot_dataset`), experiment-id linkage (`--experiment-id`/`DIT_EXPERIMENT_ID`), structured Dataflow job names + dynamic labels, and an end-to-end orchestrator (`workflows/port_visits/cross_version_ais.py`) for diffing pipeline outputs across versions against pinned input. | **Landed 2026-05-15.** Validated end-to-end via the PIPELINE-1465 cross-version test. |
| **Runtime & CI (Cloud Build)** | `ditbox` image + `cloudbuild-dit.yaml` + `make dit-cloud` / `make publish-ditbox` targets. Moves the orchestrator off the laptop; serves both `gcloud builds submit` ad-hoc and GitHub-webhook PR triggers. Tiered triggers (cheap AIS-staging on every PR, heavy AIS-full on label) come on top. | **Ad-hoc path landed 2026-05-15** (pending: first end-to-end smoke run + IAM grants on `automated-testing@`). Per-pipeline PR triggers pending. |
| **3 — pipe-events port** | Port `pipe-events/integration_tests/staging-bf_bfd_bftruncate.sh` (bash, no comparisons) to `workflows/pipe_events/fishing.py`. Add automated comparisons. Then decide whether to extract `Phase`/`Mode`/`Oracle` dataclasses based on three-consumer evidence. | Not started. |
| **4 — composer-dags param sync** | `dit sync-params --from <composer-dags-checkout>` reads production DAGs and regenerates `params.yaml`. Triggered when a real prod-vs-test param drift bug shows up. | Not started. |
| **5 — Mutation library** | Promote pipe-gaps' `compute_restricted_ssvids` into `dit.mutations` along with `drop_messages`, `shift_timestamps`, `set_segment_flag`. Cap at ~5 mutations. | Not started; waits for a second consumer. |
| **6 — Phase sharing** | Hash `(image-tag, phase-config, mutation-set)`; second invocation of an identical phase becomes a `BQ COPY` instead of a re-run. Cuts wall-clock for CI. | Not started; build only when duplicate-run cost matters operationally. |
| **7 — Golden-table mode** | Per-workflow reference `_1_bf` table keyed by `(image-tag, params-hash, date-range)`; future runs assert byte-equivalence vs. the golden table. Cheap PR-validation regression check. Implementable on top of the cross-version snapshot machinery. | Not started. |

**Operational next steps** (rolling, in priority order):

1. Validate the Cloud Build ad-hoc path end-to-end (`make publish-ditbox` → smoke `make dit-cloud ARGS="--help"` → real AIS-staging run on Cloud Build).
2. Wait on the PIPELINE-1465 cross-version run to validate the capability end-to-end.
3. Track 5 — pipe-gaps repo shim, opportunistically.
4. Pipe-gaps labels + structured job-name parity in `mode_equivalence.py`.
5. VMS port-visits workflow, then AIS-full (the latter motivates the Cloud Build runtime hardest).
6. Per-pipeline PR triggers (anchorages_pipeline first; tracks the architecture in `docs/plan.md` § Runtime & CI).

The canonical detailed version of this list lives in `docs/plan.md` § Next steps.
