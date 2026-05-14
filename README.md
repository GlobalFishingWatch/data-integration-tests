# data_integration_tests

Cross-pipeline integration-test framework for GFW data pipelines (`pipe-gaps`, `anchorages_pipeline`/port-visits, `pipe-events`/fishing).

The `dit` CLI drives Python workflow files that orchestrate phases (pipeline invocations) across modes (trigger patterns: bf / bfd / bftruncate / mutate-recover) and assert equivalence on the resulting BQ tables via `table-check summary`.

## Read first

- [`docs/plan.md`](docs/plan.md) — implementation plan, three-repo split, public API contracts, Phase 1 task breakdown.
- [`docs/context.md`](docs/context.md) — background, source bugs the framework caught, branch state at handoff.
- [`docs/framework-vision.md`](docs/framework-vision.md) — long-term shape (don't optimise for it; Phase 1 stays imperative).
- [`CLAUDE.md`](CLAUDE.md) — working agreements and Plan changelog.

## Install

```bash
python3 -m venv venv && source venv/bin/activate
make install-pipe-gaps      # or install-port-visits / install-pipe-events / install-all
```

`dit` is intentionally pipeline-agnostic; workflow dependencies (`pipe-gaps`, `anchorages_pipeline`, `pipe-events`) are not in `dit`'s base `requirements.txt`. By default the Makefile assumes sibling checkouts (`$(realpath ..)`). If yours live elsewhere, prepend `PROJECTS=/path` to any target or copy `.envrc.example` → `.envrc` (gitignored; loaded by [direnv](https://direnv.net/)).

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

## Run

```bash
dit run workflows/<pipeline>/<workflow>.py [-- workflow-args...]
```

The CLI loads the Python module at the given path and invokes its `main(argv)` entry point (see `docs/plan.md` § "Public API contracts"). Workflows can also be imported directly from a pytest target — `dit` is library-first, CLI-second.

## Status

Phase 1 (pipe-gaps port) in progress. See `docs/plan.md` for phase scope and verification.
