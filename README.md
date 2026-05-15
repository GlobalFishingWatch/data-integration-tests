# data_integration_tests

Cross-pipeline integration-test framework for GFW data pipelines (`pipe-gaps`, `anchorages_pipeline`/port-visits, `pipe-events`/fishing).

The `dit` CLI drives Python workflow files that orchestrate phases (pipeline invocations) across modes (trigger patterns: bf / bfd / bftruncate / mutate-recover) and assert equivalence on the resulting BQ tables via `table-check summary`.

## Features

The framework is intentionally thin — a small library plus per-pipeline workflow files. Capabilities, in scope today:

- **Pipeline-agnostic runners** (`dit.runners.docker`, `dit.runners.dataflow`). The docker runner invokes a published or locally-built image; the dataflow runner submits an in-process Beam pipeline with lock-split submit/wait. Both used by Phase 1; the docker runner doubles for "Beam-in-container submits to Dataflow" workflows too (port-visits' shape).
- **Comparison shim** (`dit.compare.compare_tables`) — thin wrapper over `table-check summary` from the [`table_identical_checks`](https://github.com/GlobalFishingWatch/table_identical_checks) repo. Per-column tolerances, `view_suffix` for SCD-2 last-versions vs. truncate-shape, `keys` for the comparison join.
- **BQ + date utilities** (`dit.bq`, `dit.dates`) — drop tables by prefix, query for restricted ssvids, half-open date iteration. Used by workflows that need pre/post-run setup or computed inputs.
- **Library-first**: anything you can do via `dit run …` you can also do by importing `dit.*` from a pytest target or another Python script. The CLI is one consumer of the library, not the only one.
- **Workflow file conventions**: per-pipeline workflows live in `workflows/<pipeline>/<name>.py` and expose `main(argv) -> int`. Output tables tagged with `<commit>_<uuid>` for provenance; `--allow-dirty-tree` opt-in for dirty-tree runs.
- **Three install modes** per pipeline: editable (fast inner loop), specific-ref (`REF=<sha-or-branch>`), and snapshot (`git stash create` → anchored on a `dit-snapshot-<epoch>` branch). See Usage § Install modes below.
- **Per-user infra knobs via `DIT_*` env vars**: `DIT_DEST_DATASET`, `DIT_DATAFLOW_SA`, `DIT_DATAFLOW_REGION`, `DIT_DATAFLOW_TEMP_BUCKET`, `DIT_DATAFLOW_SUBNETWORK`, `DIT_BQ_TEMP_DATASET`. Plays cleanly with direnv via `.envrc.example`.
- **Pipeline integration contract** ([`docs/pipeline-contract.md`](docs/pipeline-contract.md)) — what a pipeline must expose to be cleanly testable by `dit`, with an adoption matrix tracking where each current pipeline stands.

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

| Phase | Scope | Status |
|---|---|---|
| **1 — pipe-gaps port** | Stand up the repo. Lift the four-mode mode-equivalence test from `pipe-gaps/tests/integration/mode_equivalence.py` onto `dit.*` helpers. Drop `--runner=local`. Replace the source file with a shim. | Code complete. **Pending: real-BQ byte-equivalence verification + Track 5 shim swap.** |
| **2 — port-visits** | Ship AIS-staging, VMS, and AIS-full workflows for `anchorages_pipeline`'s port-visits two-step (`thin_port_messages` → `port_visits`). First real test of the `dit.compare` abstraction on the truncate-shape (`view_suffix=""`, `keys=["visit_id"]`). | **AIS-staging verified 2026-05-15 (3/3 pairwise green).** VMS workflow not started; AIS-full pending VMS. |
| **3 — pipe-events port** | Port `pipe-events/integration_tests/staging-bf_bfd_bftruncate.sh` (bash, no comparisons) to `workflows/pipe_events/fishing.py`. Add automated comparisons. Then decide whether to extract `Phase`/`Mode`/`Oracle` dataclasses based on three-consumer evidence. | Not started. |
| **4 — composer-dags param sync** | `dit sync-params --from <composer-dags-checkout>` reads production DAGs and regenerates `params.yaml`. Triggered when a real prod-vs-test param drift bug shows up. | Not started. |
| **5 — Mutation library** | Promote pipe-gaps' `compute_restricted_ssvids` into `dit.mutations` along with `drop_messages`, `shift_timestamps`, `set_segment_flag`. Cap at ~5 mutations. | Not started; waits for a second consumer. |
| **6 — Phase sharing** | Hash `(image-tag, phase-config, mutation-set)`; second invocation of an identical phase becomes a `BQ COPY` instead of a re-run. Cuts wall-clock for CI. | Not started; build only when duplicate-run cost matters operationally. |
| **7 — Golden-table mode** | Per-workflow reference `_1_bf` table keyed by `(image-tag, params-hash, date-range)`; future runs assert byte-equivalence vs. the golden table. Cheap PR-validation regression check. | Not started. |

**Cross-version testing** (per-mode pipeline binding, e.g. `pipe-gaps@main` vs `pipe-gaps@pr-NNN` in one workflow) sits on top of the existing install-ref / snapshot machinery; promotable when a real PR-vs-`main` need arises.
