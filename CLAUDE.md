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
