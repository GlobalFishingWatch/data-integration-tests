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

`dit` is intentionally pipeline-agnostic; workflow dependencies (`pipe-gaps`, `anchorages_pipeline`, `pipe-events`) are not in `dit`'s base `requirements.txt`. The Makefile targets install them editable from local sibling checkouts, so switching branches in `pipe-gaps` etc. is picked up without a reinstall.

By default the Makefile assumes sibling checkouts (`$(realpath ..)`). If yours live elsewhere, either:

```bash
PROJECTS=/path/to/your/projects make install-pipe-gaps
```

or copy `.envrc.example` to `.envrc` (committed-untracked; loaded automatically by [direnv](https://direnv.net/)) and adjust the path.

For the framework only (no workflow deps), `make install` works — but the dataflow runner won't load without a workflow install bringing `apache-beam[gcp]` transitively.

## Run

```bash
dit run workflows/<pipeline>/<workflow>.py [-- workflow-args...]
```

The CLI loads the Python module at the given path and invokes its `main(argv)` entry point (see `docs/plan.md` § "Public API contracts"). Workflows can also be imported directly from a pytest target — `dit` is library-first, CLI-second.

## Status

Phase 1 (pipe-gaps port) in progress. See `docs/plan.md` for phase scope and verification.
