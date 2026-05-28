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

### Milestone 5 — port-visits cache + cleanup control plane ✓ (`feat/m5-cache-cleanup-and-portvisits`)

Refined 2026-05-29; **landed 2026-05-29** (M5a + M5b on one branch). The `dit.workflow` harness extraction (PR #28) already moved `run_with_cache` + the run-context preamble into shared code, so M5b was mostly "supply port-visits' own cache key" rather than copying the wrapper.

**M5a — cleanup control plane (`cancel_run` + `dit cache-cancel` + `make dit-cancel`).** ✓ Done.
- `cancel_run(run_id, *, region=None, client=None)` looks up **all rows for that run_id** via the new `read_rows_for_run` helper (one per mode — they share the run_id; `cache_key` distinguishes modes; the read deliberately does NOT filter `status`/`expires_at` so in-flight + already-cancelled rows are visible), cancels their Dataflow jobs, deletes their `output_tables`, and `UPDATE`s `status='cancelled'`. Idempotent (tolerates already-gone jobs/tables/datasets). Raises `ValueError` on an unknown `run_id`.
- **Job discovery (the real design point).** Every written row has `dataflow_job_ids=[]` — the runner never captured them. So `cancel_run` discovers jobs **by label**: `gcloud dataflow jobs list --region=<r> --filter=labels.dit_run_id=<id> --format=json(id,name,state)`, NOT the stored list. Only jobs in a cancellable state (`Running`/`Pending`) are cancelled; terminal jobs are skipped. A failed jobs-list call is tolerated (cleanup proceeds to table deletion + status update). Capturing job IDs back into the row from the in-process pipe-gaps runner remains a later nice-to-have.
- **Table-delete safety.** Output values are deleted **table-level only** via `Client.delete_table(fqn, not_found_ok=True)`, and ONLY when the value is a fully-qualified `project.dataset.table` (3 non-empty dot-parts; `_looks_like_table_fqn`). A dataset-shaped value (e.g. a `dit_exp_*` snapshot dataset) or any malformed ref is **skipped with a warning** — categorically refusing to escalate into a dataset delete (manual deletion of shared snapshot datasets has broken live runs; see CLAUDE.md). Tables are de-duped across the run's rows.
- `region` resolves from `DIT_DATAFLOW_REGION` → `us-central1` (the same knob the workflows use), overridable via `--region`.
- `@main.command("cache-cancel")` → `cancel_run`; `make dit-cancel RUN_ID=<id> [REGION=<r>]` shells `dit cache-cancel` (validates RUN_ID non-empty).
- **Pipe-gaps `dit_run_id` label (added here).** pipe-gaps submits Dataflow in-process via `dit.runners.dataflow`, so labels can't be `--labels=k=v` CLI flags (that's port-visits' docker-runner path). Instead a `labels` list (`dit_run_id`, `dit_commit_sha`, `dit_worker_image_tag`, `dit_launched_by`) is threaded through `_make_config` → `cfg.unknown_parsed_args["labels"]` → the `PipelineFactory` spread → Beam `GoogleCloudOptions.labels`. Verified end-to-end that the list reaches `GoogleCloudOptions.labels`.
- **Permissions to verify (user-gated; NOT exercised by tests, which mock gcloud + BQ)**: `dataflow.jobs.cancel` + BQ table-delete on `tech_great_expectations` — for **both** the laptop user (interactive `make dit-cancel`) and `automated-testing@` (the M6 cloud SIGTERM path).
- **Tests**: `tests/test_cache_cancel.py` — mocked `bigquery.Client` + `subprocess.run`; job discovery by label, cancel-running-skip-terminal, table deletion + de-dupe, the non-table-value safety skip, `status='cancelled'` UPDATE, region env default, gcloud-failure tolerance, idempotency, no-rows `ValueError`. Plus pipe-gaps label tests in `tests/test_pipe_gaps_mode_equivalence.py`.

**M5b — wire port-visits caching.** ✓ Done. `ais.py` previously passed `resolve_digest=False` (no cache).
- Added `WORKFLOW_FILE_SHA1`, `WORKFLOW_NAME`, mode constants (`MODE_BF`/`MODE_BFD`/`MODE_BFTRUNCATE`) + `_MODES_USING_TAIL`, a mode-aware `canonical_params_dict` (output-affecting fields only: `mode`, `start`/`end`, `source_dataset_stem`, `named_anchorages`, `thinned_message_table`, plus `tail_days` for the daily-slice modes — `1_bf` ignores it, matching pipe-gaps), `_build_cache_key`, and a `_run_with_cache` adapter over `dit.workflow.run_with_cache(..., log_label=mode)`.
- Flipped `resolve_digest=True` (caching needs the digest) and stamped `args.run_context` + `args.worker_image_digest`. `compare_all` now takes the per-mode cached-or-fresh FQNs (a hit reuses a prior run's UUID-suffixed table, so the FQN isn't derivable from the current suffix). The `--skip-pipelines` compare-only path falls back to the current-suffix FQNs (unchanged behaviour).
- **Tests**: `tests/test_port_visits_ais.py` — mirrors the pipe-gaps cache tests (params mode-awareness, key composition, hit/miss/stale, unreviewed rows).

**M6 (SIGTERM → `cancel_run`)** is the natural follow-on now M5a has landed — out of scope for this PR.

### Milestone 6 — SIGTERM trap inside `dit run`

When Cloud Build cancels a build mid-flight, the orchestrator process gets SIGTERM. Catch it; run `cancel_run(self.run_id)`; exit.

**Tasks**:
- `dit.cli` (or a new `dit.signal_handlers` module) registers a SIGTERM handler at workflow entry. Handler is best-effort — log + cancel + exit even on errors.
- Pass `run_id` into the handler via closure or module-level state.
- **Tests**: subprocess-driven test that starts `dit run` with a mock workflow, sends SIGTERM, asserts cancel_run was called.

**Estimated effort**: half-day.

## Open infra prerequisites

- **Cache table creation.** One-shot `bq query --use_legacy_sql=false < migrations/001_dit_meta_runs.sql`. No new dataset / IAM grant — uses the existing `tech_great_expectations` dataset that dit already writes outputs to. If/when retention separation or per-table IAM is needed, the table moves with a one-line `TABLE_FQN` change in `src/dit/cache.py`.
- **`gcloud dataflow jobs cancel` + BQ table-delete permissions.** For `cancel_run` (M5a landed; implementation mocks gcloud/BQ in tests, so this is unverified against live infra). `automated-testing@` likely already has `dataflow.jobs.cancel` (it can submit; cancel is the symmetric operation) + table-delete on `tech_great_expectations`. **User-gated follow-up**: verify both — for the laptop user (interactive `make dit-cancel`) and `automated-testing@` (the M6 cloud SIGTERM path) — before relying on `dit cache-cancel` in anger.

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
