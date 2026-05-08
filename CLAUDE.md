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

## Plan changelog

Appended chronologically. Each entry is one commit's worth of plan-doc changes; cite which sections moved.

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

### 2026-05-08 — Track 2 utility-module contract refinements

- `docs/plan.md` § **Public API contracts (Phase 1) → `dit.bq`**: settled `query_for_restricted_ssvids` kwargs as `(reference_table, *, mid, backfill_days_w, seed=42, project=…)`. Dropped the source's `n_hours_before` argument (unused at the call site; logged as such by the source itself). Added a note that `drop_tables` requires `<project>.<dataset>.<stem>` form so the dataset can be enumerated.
- `dit.compare.compare_tables` keeps the `ignore_columns` parameter from the contract but raises `NotImplementedError` if passed non-empty: `table-check summary` does not yet support an ignore-columns flag, and CLAUDE.md prohibits reimplementing comparison features in the shim. The signal pushes the feature upstream into `table_identical_checks`. `tolerance` is forwarded as `--tolerance=<col>:<value>` (table-check's per-column syntax).
- `dit.dates.daterange_inclusive` is half-open (`start <= d < end`) despite the name -- preserved verbatim from `pipe-gaps` so the four-mode equivalence test stays byte-equivalent across the move. Pinned by `tests/test_dates.py`. If the migration ever wants true inclusive semantics, it has to flip the call sites simultaneously.
