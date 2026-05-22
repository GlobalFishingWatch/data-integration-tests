# Run cache — implementation plan

**Status:** active. Companion to [`docs/run-cache.md`](run-cache.md) (the design) and [`src/dit/cache.py`](../src/dit/cache.py) (the scaffold).

The design doc explains *what* and *why*. This doc explains *how*, *in what order*, and *what's done / what's next*.

## Milestones

Each milestone is one PR's worth of work. Each ships on its own branch + PR, squash-merged. Earlier milestones never depend on later ones, so a partial implementation is safe to leave on `main`.

### Milestone 1 — Scaffold ← *this PR (`feat/dit-cache-scaffold`)*

- **`src/dit/cache.py`**: module + dataclasses (`CacheKey`, `CachedRun`) + implemented pure functions (`compute_cache_key`, `sha1_of_workflow_file`, `canonicalise_params`, `resolve_worker_image_to_digest`). BQ-touching functions raise `NotImplementedError` with TODO markers.
- **`tests/test_cache.py`**: 15 tests covering the pure functions: key determinism, sensitivity to each input, sha1 stability + edit-detection, params canonicalisation rules.
- **`migrations/001_dit_meta_runs.sql`**: `CREATE TABLE IF NOT EXISTS` for `world-fishing-827.dit_meta.runs`. Partitioned by `DATE(started_at)`, clustered on `pipeline, cache_key`.
- **`docs/run-cache-impl.md`** (this doc).

**Why scaffold-first**: the BQ-touching code is one third of the surface area but is hard to test without infra; the pure parts (hash key, file sha1, params canonicalisation) are 80% of the correctness-relevant logic and 100% testable in milliseconds. Land them first, then add the BQ shell around them. Workflow integration doesn't go in until M4 — wiring half-implemented cache into workflows risks subtle bugs that would land before they were verifiable.

### Milestone 2 — BQ read path

Implement `read_cache`, `verify_tables_exist`, `expires_at_for`.

**Tasks**:
- `bq mk dit_meta` (one-off, may need IAM coordination — see Open infra below).
- Apply `migrations/001_dit_meta_runs.sql` via `bq query`.
- Implement `read_cache(cache_key) -> CachedRun | None` using `google.cloud.bigquery.Client.query` with a parameterised `cache_key`. Returns `None` on no match.
- Implement `verify_tables_exist(table_fqns) -> list[bool]`. Group FQNs by dataset; one `INFORMATION_SCHEMA.TABLES` query per dataset.
- Implement `expires_at_for(table_fqns) -> datetime`. Same INFORMATION_SCHEMA join; `MIN(expiration_time)` across the supplied tables.
- Implement `CachedRun.from_bq_row(row)`. Coerce TIMESTAMP → datetime, ARRAY → list, JSON column → str.
- **Tests**: ~6 new tests in `tests/test_cache.py` using `unittest.mock.Mock` for the `bigquery.Client`. Verifies the query SQL string + parameter passing + row→object coercion. Real-BQ smoke is a follow-up; mocked tests cover the contract.

**Estimated effort**: ~half-day including the table creation.

### Milestone 3 — BQ write path + dirty-tree skip

Implement `write_cache`. Add the `pipeline_dirty` gate at the public API level.

**Tasks**:
- Implement `CachedRun.to_bq_row()` (inverse of `from_bq_row`).
- Implement `write_cache(row)` via `bigquery.Client.insert_rows_json`.
- Add a `record_run(row)` thin wrapper that calls `write_cache` only when `row.pipeline_dirty == False`; logs the skip otherwise. This is what workflows call — they don't think about the dirty gate themselves.
- **Tests**: ~4 new tests covering the round-trip (to_bq_row + from_bq_row), the dirty-skip behaviour, and the insert-error path (logged + raised, never silently swallowed).

**Estimated effort**: half-day.

### Milestone 4 — Workflow integration (pipe-gaps)

Wire `dit.cache` into `workflows/pipe_gaps/mode_equivalence.py`.

**Tasks**:
- Add a `canonical_params_dict(args) -> dict` function listing the output-affecting params for pipe-gaps mode-equivalence. Includes: `start`, `end`, `tail_days`, `backfill_days`, `min_gap_length`, `n_hours_before`, `window_period_d`, `filter_good_seg`, `skip_open_gaps`, `ssvids`, `source_messages`, `source_segments`. Excludes plumbing (`service_account`, `dataflow_region`, etc.) and naming (`experiment_id`, `suffix`).
- Wrap each `execute_*` helper with a cache lookup → hit-or-compute → record-run flow. Cache unit is the whole mode (not per-iteration), keyed on the mode constant `MODE_BF` / `MODE_BFD` / etc. as part of the params dict.
- Add the `dit_run_id` label to pipe-gaps' Dataflow job options (currently only port-visits has it). 12-hex UUID generated once per `main()`, stamped on every Dataflow job + BQ table for cleanup-by-label.
- Wire `cfg` to carry `run_id`, `experiment_id`, `pipeline_dirty`, `worker_image_digest` so `record_run` can fill the `CachedRun` row at exit.
- **Tests**: integration test against a stub `dit.cache` (mock `read_cache` returning Some/None) — confirms `execute_*` skips computation on hit, runs on miss. No real BQ.
- **Smoke**: one Cloud Build run against the AIS-staging cohort with `dit_meta.runs` empty (expect: miss, compute, write); a second run with the cache populated (expect: hit, skip, no Dataflow jobs).

**Estimated effort**: 1 day. The threading work (cfg attributes, label addition) is the bulk; the cache-call sites are 4 lines each.

### Milestone 5 — Workflow integration (port-visits) + `make dit-cancel`

Port-visits is structurally similar to pipe-gaps (same `execute_bf` / `execute_bfd` / `execute_bftruncate` shape; already has `dit_run_id`).

**Tasks**:
- `canonical_params_dict` for port_visits (similar to M4; ais-specific fields).
- Wrap `execute_*` (same shape as pipe-gaps).
- Add `make dit-cancel RUN_ID=<id>` Makefile target. Shell script that calls `dit cache-cancel <run-id>`; the CLI in turn calls `cancel_run(run_id)`.
- Implement `cancel_run(run_id)` in `dit.cache`: look up the row, cancel Dataflow jobs via `gcloud dataflow jobs cancel`, drop output tables via `bq rm -f`, UPDATE the row's status to `cancelled`. Idempotent.
- **Tests**: integration tests against a mocked `bigquery.Client` + mocked `subprocess.run`. Round-trip: a row at status='running' → cancel_run → status='cancelled' + jobs/tables cleanup attempted.

**Estimated effort**: 1 day.

### Milestone 6 — SIGTERM trap inside `dit run`

When Cloud Build cancels a build mid-flight, the orchestrator process gets SIGTERM. Catch it; run `cancel_run(self.run_id)`; exit.

**Tasks**:
- `dit.cli` (or a new `dit.signal_handlers` module) registers a SIGTERM handler at workflow entry. Handler is best-effort — log + cancel + exit even on errors.
- Pass `run_id` into the handler via closure or module-level state.
- **Tests**: subprocess-driven test that starts `dit run` with a mock workflow, sends SIGTERM, asserts cancel_run was called.

**Estimated effort**: half-day.

## Open infra prerequisites

- **`dit_meta` dataset.** Needs to exist in `world-fishing-827` with `dataEditor` for `automated-testing@`. One-time. Two paths: (a) terraform PR against the GFW cloud-platform-terraform repo (canonical; ~1-week turnaround); (b) `bq mk` + manual IAM grant via the console (immediate; not version-controlled). Recommendation: do (b) for the M2 implementation work, follow up with (a) for permanence.
- **`bigquery.Client` for read.** `read_cache` runs from inside the Cloud Build step (the dit orchestrator). The build's service account (`automated-testing@`) already has `dataViewer` on most datasets; needs explicit `dataViewer` on `dit_meta` (covered by the same dataEditor grant).
- **`gcloud dataflow jobs cancel` permission.** For `cancel_run`. `automated-testing@` likely already has `dataflow.jobs.cancel` (it can submit; cancel is the symmetric operation). Verify before M5.

## Decision points still open

- **`dit_run_id` for pipe-gaps**: scope of M4 confirms the label addition, but where the UUID is generated is open. Options: (a) inside `main()`, stamped onto each cfg; (b) per-execute helper, separate per mode. (a) is simpler and matches port_visits; (b) would let the user cancel one mode without cancelling the others. Going with (a) unless someone calls for (b).
- **Cache wrapper API**: design has been "wrap each `execute_*`". Could also be "wrap `_run_pipeline`" (more granular but higher per-call cost; less semantic). M4 will pick when the workflow code is in front of us.
- **Mode-equivalence on cache hits.** If 1_bf hits cache but 2_bfd misses, the comparison between them is still valid (the cached 1_bf was produced by the same pipeline_commit + worker_image as the fresh 2_bfd). But if the user passes `--experiment-id`, the cached row's `experiment_id` won't match — the comparison logic needs to look up output FQNs by `cache_key` (or by traversing the cache table) rather than by string-concatenating the experiment_id. The right shape is for `dit.compare` (the `dit.report` work in roadmap item 3) to take a `CachedRun | None` per side and use the cached row's `output_tables` field.

## What lands in this PR (M1) — checklist

- [x] `src/dit/cache.py` — module + dataclasses + pure functions + stubs.
- [x] `tests/test_cache.py` — 15 tests; all passing.
- [x] `migrations/001_dit_meta_runs.sql` — DDL.
- [x] `docs/run-cache-impl.md` — this doc.
- [ ] `CHANGELOG.md` — `[Unreleased]` entry under `#### Added`.

After this PR, the next branch is `feat/dit-cache-bq-read` for M2.

## Related

- [`docs/run-cache.md`](run-cache.md) — design.
- [`docs/plan.md`](plan.md) § Next steps item 1 — roadmap placement.
- [`docs/llm-pr-gating.md`](llm-pr-gating.md) — sibling design; the LLM pre-filter's audit query (`docs/llm-pr-gating.md` § Audit) joins against `dit_meta.runs`.
