# Run cache — implementation plan

**Status:** active. Companion to [`docs/run-cache.md`](run-cache.md) (the design) and [`src/dit/cache.py`](../src/dit/cache.py) (the scaffold).

The design doc explains *what* and *why*. This doc explains *how*, *in what order*, and *what's done / what's next*.

## Milestones

Each milestone is one PR's worth of work. Each ships on its own branch + PR, squash-merged. Earlier milestones never depend on later ones, so a partial implementation is safe to leave on `main`.

### Milestone 1 — Scaffold ← *this PR (`feat/dit-cache-scaffold`)*

- **`src/dit/cache.py`**: module + dataclasses (`CacheKey`, `CachedRun`) + implemented pure functions (`compute_cache_key`, `sha1_of_workflow_file`, `canonicalise_params`, `resolve_worker_image_to_digest`). BQ-touching functions raise `NotImplementedError` with TODO markers.
- **`tests/test_cache.py`**: 15 tests covering the pure functions: key determinism, sensitivity to each input, sha1 stability + edit-detection, params canonicalisation rules.
- **`migrations/001_dit_meta_runs.sql`**: `CREATE TABLE IF NOT EXISTS` for `world-fishing-827.tech_great_expectations.dit_runs`. Partitioned by `DATE(started_at)`, clustered on `pipeline, cache_key`. (Filename keeps `001_dit_meta_runs.sql` for migration-history stability; the table inside is `dit_runs`.)
- **`docs/run-cache-impl.md`** (this doc).

**Why scaffold-first**: the BQ-touching code is one third of the surface area but is hard to test without infra; the pure parts (hash key, file sha1, params canonicalisation) are 80% of the correctness-relevant logic and 100% testable in milliseconds. Land them first, then add the BQ shell around them. Workflow integration doesn't go in until M4 — wiring half-implemented cache into workflows risks subtle bugs that would land before they were verifiable.

### Milestone 2 — BQ read path ✓ (`feat/dit-cache-bq-read`)

Implement `read_cache`, `verify_tables_exist`, `expires_at_for`.

**Tasks**:
- Apply `migrations/001_dit_meta_runs.sql` via `bq query --use_legacy_sql=false < migrations/001_dit_meta_runs.sql`. No dataset creation or IAM grant needed — `tech_great_expectations` already exists and `automated-testing@` has `dataEditor` on it.
- Implement `read_cache(cache_key) -> CachedRun | None` using `google.cloud.bigquery.Client.query` with a parameterised `cache_key`. Returns `None` on no match.
- Implement `verify_tables_exist(table_fqns) -> list[bool]`. Group FQNs by dataset; one `INFORMATION_SCHEMA.TABLES` query per dataset.
- Implement `expires_at_for(table_fqns) -> datetime`. Same INFORMATION_SCHEMA join; `MIN(expiration_time)` across the supplied tables.
- Implement `CachedRun.from_bq_row(row)`. Coerce TIMESTAMP → datetime, ARRAY → list, JSON column → str.
- **Tests**: ~6 new tests in `tests/test_cache.py` using `unittest.mock.Mock` for the `bigquery.Client`. Verifies the query SQL string + parameter passing + row→object coercion. Real-BQ smoke is a follow-up; mocked tests cover the contract.

**Estimated effort**: ~half-day including the table creation.

### Milestone 3 — BQ write path ✓ (`feat/dit-cache-bq-write`)

Implement `CachedRun.to_bq_row` + `write_cache`.

**Reconciled with the design doc**: dirty-tree handling lives at the **read** side, not the write side. We INSERT every row for registry / cleanup purposes (`make dit-cancel` finds them via the run_id). No `record_run` wrapper needed — the earlier plan's "skip write when dirty" framing was inconsistent with the design. *(Updated by M-pivot-3: `read_cache` no longer filters at all — the `pipeline_dirty = FALSE` filter was dropped and the column renamed `unreviewed_code` so content-addressable snapshot rows are cacheable. See [`docs/no-dirty-tree-pivot.md`](no-dirty-tree-pivot.md).)*

**Tasks (done)**:
- `CachedRun.to_bq_row()` — datetime → ISO-8601 string; params dict → JSON string; nullables pass through. Kept as a "render as dict" helper for debug/log/serialisation; not on the write hot path.
- `write_cache(row, *, client=None)` — **parameterised DML INSERT** (`client.query("INSERT INTO ... VALUES (@a, ...)").result()`), with `PARSE_JSON(@params_json)` server-side over a STRING-typed parameter for the JSON column. Chosen over `insert_rows_json` because streaming inserts (a) sit in a 90-minute buffer that blocks UPDATE/DELETE — would break M5's `cancel_run` UPDATE — and (b) are at-least-once (retries can duplicate rows without explicit `row_ids=`). DML INSERT lands in permanent storage immediately, exactly-once, at the same cost (INSERT scans zero bytes). Raises whatever exception the BQ query job raises on failure.
- 9 unit tests: `to_bq_row` shape (all 18 columns), ISO timestamps, JSON-string params, nullable passthrough, `from_bq_row` round-trip; `write_cache` calls `client.query` with the right SQL + parameter bindings, raises on DML errors, dirty rows still written, NULL params bind correctly.
- Real-BQ smoke: write a row → `read_cache` it back → DELETE immediately. Affected rows: 1. (No streaming buffer = rows are immediately mutable.)

### Milestone 4 — Workflow integration (pipe-gaps) ✓ (`feat/dit-cache-workflow-pipe-gaps`)

Wire `dit.cache` into `workflows/pipe_gaps/mode_equivalence.py`.

**Tasks**:
- Add a `canonical_params_dict(args) -> dict` function listing the output-affecting params for pipe-gaps mode-equivalence. Includes: `start`, `end`, `tail_days`, `backfill_days`, `min_gap_length`, `n_hours_before`, `window_period_d`, `filter_good_seg`, `skip_open_gaps`, `ssvids`, `source_messages`, `source_segments`. Excludes plumbing (`service_account`, `dataflow_region`, etc.) and naming (`experiment_id`, `suffix`).
- Wrap each `execute_*` helper with a cache lookup → hit-or-compute → record-run flow. Cache unit is the whole mode (not per-iteration), keyed on the mode constant `MODE_BF` / `MODE_BFD` / etc. as part of the params dict.
- Add the `dit_run_id` label to pipe-gaps' Dataflow job options (currently only port-visits has it). 12-hex UUID generated once per `main()`, stamped on every Dataflow job + BQ table for cleanup-by-label.
- Wire `cfg` to carry `run_id`, `experiment_id`, `pipeline_dirty`, `worker_image_digest` so `record_run` can fill the `CachedRun` row at exit.
- **Tests**: integration test against a stub `dit.cache` (mock `read_cache` returning Some/None) — confirms `execute_*` skips computation on hit, runs on miss. No real BQ.
- **Smoke**: one Cloud Build run against the AIS-staging cohort with `tech_great_expectations.dit_runs` empty (expect: miss, compute, write); a second run with the cache populated (expect: hit, skip, no Dataflow jobs).

**Estimated effort**: 1 day. The threading work (cfg attributes, label addition) is the bulk; the cache-call sites are 4 lines each.

### Milestone 5 — port-visits cache + cleanup control plane

Refined 2026-05-29. Splits into two independent pieces. The `dit.workflow` harness extraction (PR #28) already moved `run_with_cache` + the run-context preamble into shared code, so M5b is now mostly "supply port-visits' own cache key" rather than copying the wrapper.

**M5a — cleanup control plane (`cancel_run` + `dit cache-cancel` + `make dit-cancel`).** Standalone; no port-visits dependency. `dit.cache.cancel_run(run_id)` is currently a `NotImplementedError` stub in `src/dit/cache.py`; `dit.cli` is a `click` group, so `dit cache-cancel` is one new `@main.command`.
- `cancel_run(run_id)` looks up **all rows for that run_id** (one per mode — they share the run_id; `cache_key` distinguishes modes), cancels their Dataflow jobs, `bq rm -f`s their `output_tables`, and `UPDATE`s `status='cancelled'`. Idempotent (tolerate already-gone jobs/tables).
- **Job discovery (the real design point).** Every written row has `dataflow_job_ids=[]` — the runner never captured them. So `cancel_run` discovers jobs **by label**: `gcloud dataflow jobs list --region=<r> --filter="labels.dit_run_id=<id>"`, NOT the stored list. **Prereq**: confirm both workflows stamp `dit_run_id` as a Dataflow label (port-visits does; verify pipe-gaps' submission does, add if not). Capturing job IDs back into the row from the in-process pipe-gaps runner is a later nice-to-have.
- `@main.command("cache-cancel")` → `cancel_run`; `make dit-cancel RUN_ID=<id>` shells it (validate RUN_ID non-empty).
- **Permissions to verify first**: `dataflow.jobs.cancel` + BQ table-delete on `tech_great_expectations` — for **both** the laptop user (interactive `make dit-cancel`) and `automated-testing@` (the M6 cloud SIGTERM path).
- **Tests**: mocked `bigquery.Client` + `subprocess.run`; round-trip `status='running'` → `cancel_run` → `status='cancelled'` + cancel/rm attempted; idempotency.

**M5b — wire port-visits caching.** `ais.py` currently passes `resolve_digest=False` (no cache).
- Add `WORKFLOW_FILE_SHA1` + a port-visits `canonical_params_dict` (output-affecting fields only: start/end, `source_dataset_stem`, `named_anchorages`, `tail_days`, mode).
- Flip `resolve_digest=True` (caching needs the digest), build a `CacheKey`, wrap each `execute_*` via the shared `dit.workflow.run_with_cache(..., log_label=mode)`. `compare_tables` still takes the returned (cached-or-fresh) FQN.
- **Tests**: stub `read_cache` Some/None → skip-on-hit / compute-on-miss (mirror `test_pipe_gaps_mode_equivalence.py`).

**Estimated effort**: ~1–1.5 days (1 PR each, or combined). M6 (SIGTERM → `cancel_run`) is the natural follow-on once M5a lands.

### Milestone 6 — SIGTERM trap inside `dit run`

When Cloud Build cancels a build mid-flight, the orchestrator process gets SIGTERM. Catch it; run `cancel_run(self.run_id)`; exit.

**Tasks**:
- `dit.cli` (or a new `dit.signal_handlers` module) registers a SIGTERM handler at workflow entry. Handler is best-effort — log + cancel + exit even on errors.
- Pass `run_id` into the handler via closure or module-level state.
- **Tests**: subprocess-driven test that starts `dit run` with a mock workflow, sends SIGTERM, asserts cancel_run was called.

**Estimated effort**: half-day.

## Open infra prerequisites

- **Cache table creation.** One-shot `bq query --use_legacy_sql=false < migrations/001_dit_meta_runs.sql`. No new dataset / IAM grant — uses the existing `tech_great_expectations` dataset that dit already writes outputs to. If/when retention separation or per-table IAM is needed, the table moves with a one-line `TABLE_FQN` change in `src/dit/cache.py`.
- **`gcloud dataflow jobs cancel` permission.** For `cancel_run`. `automated-testing@` likely already has `dataflow.jobs.cancel` (it can submit; cancel is the symmetric operation). Verify before M5.

## Decisions for M4 (resolved 2026-05-22)

### Decision A — `dit_run_id` is per-`main()`, asset traceability is per-`execute_*`

`dit_run_id` is generated once in `main()` (12-hex UUID) and stamped on every Dataflow job + BQ output from that invocation, matching the port-visits pattern. **It's the control-plane identifier**: "all jobs/tables from one `dit run`". `make dit-cancel RUN_ID=<id>` operates at this scope — cancels all sibling modes together.

**Asset traceability remains per-mode** without a separate identifier:

- One row in `tech_great_expectations.dit_runs` per `execute_*` call — `cache_key` distinguishes modes within an invocation. The row's `output_tables` + `dataflow_job_ids` columns are the per-mode asset list.
- BQ labels on each individual Dataflow job + output table carry `dit_mode` / `dit_step` / `dit_iteration` (from the per-iteration-labels work in PR #2 on port_visits, to be replicated for pipe-gaps in M4).
- So "what assets came from mode 2_bfd of run X" is one BQ filter: `WHERE dit_run_id = X AND dit_mode = "2_bfd"` — or equivalently `WHERE run_id = X AND cache_key = ...` on the cache table.

If we later discover that *cancelling one mode without cancelling siblings* is a real workflow (it isn't today), the right answer is a new `--mode <name>` flag on `make dit-cancel` that filters within a `run_id`, NOT splitting `run_id` itself. That keeps the run/mode hierarchy clean.

### Decision B — Cache wrap unit is `execute_*` (whole mode)

Wrap each `execute_bf` / `execute_bfd` / `execute_bftruncate` / `execute_mutate_recover` as the cache unit. **Not** per-`_run_pipeline` (individual Dataflow job).

Rationale: the storage boundary is at the mode level — bfd's 5 daily iterations all append to the *same* output table. A per-Dataflow-job cache would produce incoherent "half-cached half-fresh mode" states (iteration 3 has rows from a previous run; iterations 1-2 are re-run and truncate-overwrite them; arbitrary mismatch). Per-mode keeps the cache unit aligned with the storage unit: one cache_key = one BQ output table.

Loses: resume-from-failed-iteration semantics (a partial-failed bfd retries from scratch). Doesn't matter at AIS-staging scale; revisit if/when AIS-full makes a single mode's wall-clock painful.

### Decision C — Workflow resolves output FQN; `dit.compare` stays dumb

The cache wrapper returns the output FQN (either from `cached_row.output_tables[0]` on a hit, or the workflow's just-computed `bf_table` variable on a miss). The workflow then passes that FQN to `dit.compare.compare_tables(a, b, keys=..., view_suffix=...)` exactly as today.

**`dit.compare`'s signature does not change.** The working-agreement "`dit.compare` is a thin shim over `table-check`" is the deciding consideration — adding cache-row awareness contradicts it. The workflow has both the `CachedRun` and the local FQN in scope; the swap is a two-line conditional at the call site.

Sketch:

```python
def run_with_cache(workflow_fn, *, cache_key, output_fqn, **kwargs) -> str:
    """Return the FQN of the output table — from cache on hit, freshly computed on miss."""
    if cached := dit_cache.read_cache(cache_key):
        if all(dit_cache.verify_tables_exist(cached.output_tables)):
            logger.info("cache HIT: reusing %s", cached.output_tables[0])
            return cached.output_tables[0]
    workflow_fn(**kwargs)
    dit_cache.write_cache(...)   # every run is recorded (incl. unreviewed); read_cache no longer filters
    return output_fqn

bf_fqn  = run_with_cache(execute_bf,         cache_key=bf_key,  output_fqn=bf_table,  ...)
bfd_fqn = run_with_cache(execute_bfd,        cache_key=bfd_key, output_fqn=bfd_table, ...)
dit_compare.compare_tables(bf_fqn, bfd_fqn, keys=COMPARE_KEYS, view_suffix=COMPARE_VIEW_SUFFIX)
```

## What lands in this PR (M1) — checklist

- [x] `src/dit/cache.py` — module + dataclasses + pure functions + stubs.
- [x] `tests/test_cache.py` — 15 tests; all passing.
- [x] `migrations/001_dit_meta_runs.sql` — DDL.
- [x] `docs/run-cache-impl.md` — this doc.
- [x] `CHANGELOG.md` — `[Unreleased]` entry under `#### Added`.

After this PR, the next branch is `feat/dit-cache-bq-read` for M2.

## Related

- [`docs/run-cache.md`](run-cache.md) — design.
- [`docs/plan.md`](plan.md) § Next steps item 1 — roadmap placement.
- [`docs/llm-pr-gating.md`](llm-pr-gating.md) — sibling design; the LLM pre-filter's audit query (`docs/llm-pr-gating.md` § Audit) joins against `tech_great_expectations.dit_runs`.
