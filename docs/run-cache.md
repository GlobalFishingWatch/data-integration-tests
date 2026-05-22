# Run cache (`tech_great_expectations.dit_runs`)

**Status:** design sketch, not implemented. Owner: dit. Drafted 2026-05-22.

## Why

Three operational needs collapse into one BQ table:

1. **Cache.** Most PR runs ask the same question: "compare my PR's 1_bf output vs main's 1_bf output". If main hasn't moved since the last run, we already produced main's 1_bf; recomputing is wasteful. A content-addressable lookup turns the second invocation into an `INFORMATION_SCHEMA.TABLES` probe.
2. **Registry.** When a Cloud Build is cancelled mid-flight, the orchestrator dies but Dataflow jobs continue and BQ tables get orphaned. A persistent record of "what did this run produce" is the source of truth for cleanup.
3. **Provenance.** When a stakeholder asks "which commit produced this table?", the answer is one BQ query away.

A single table serves all three. Naming is intentionally generic — this is not "golden tables" or "phase sharing"; it's the cache.

## Schema

```sql
CREATE TABLE `world-fishing-827.tech_great_expectations.dit_runs` (
  -- identity
  run_id           STRING    NOT NULL,  -- 12-hex; matches the `dit_run_id` BQ label that ais.py already emits (port_visits). mode_equivalence.py does NOT emit `dit_run_id` today (it generates a 6-hex experiment-id suffix instead) -- adding the label to pipe-gaps is part of the run-cache implementation work, not preexisting.
  cache_key        STRING    NOT NULL,  -- sha256, see below

  -- context (what produced this run)
  workflow         STRING    NOT NULL,  -- e.g. "workflows/pipe_gaps/mode_equivalence.py"
  pipeline         STRING    NOT NULL,  -- "pipe-gaps" / "anchorages_pipeline" / "pipe-events"
  experiment_id    STRING    NOT NULL,
  pipeline_commit  STRING    NOT NULL,  -- short or full SHA of the pipeline tree at submit time
  pipeline_dirty   BOOL      NOT NULL,  -- if true, the row is provenance-only -- writes skip the cache
  dit_commit       STRING    NOT NULL,  -- provenance only, NOT in cache_key (refactor-safe)
  workflow_file_sha1 STRING  NOT NULL,  -- IN cache_key -- behaviour-relevant
  worker_image     STRING    NOT NULL,  -- digest form: `<repo>@sha256:...` (tag form is unstable)

  -- params that affect output (canonical JSON, sorted keys)
  params_json      JSON,

  -- outputs the run produced (everything dit-cancel + cache-reuse needs)
  output_tables    ARRAY<STRING>,       -- FQN: "project.dataset.table"
  dataflow_job_ids ARRAY<STRING>,
  cloud_build_id   STRING,              -- nullable; null for local runs

  -- timing / status
  started_at       TIMESTAMP NOT NULL,
  finished_at      TIMESTAMP,
  status           STRING    NOT NULL,  -- "running" / "succeeded" / "failed" / "cancelled"
  expires_at       TIMESTAMP NOT NULL   -- moment the output_tables are no longer guaranteed to exist; see § expires_at below
)
PARTITION BY DATE(started_at)
CLUSTER BY pipeline, cache_key;
```

The table lives in `world-fishing-827.tech_great_expectations.dit_runs` — same dataset dit already writes workflow outputs to, no new dataset / IAM grant needed. The `dit_` prefix scopes the table within the shared dataset. Rows have no BQ-level TTL — they expire by the `expires_at` column at query time, because we need cancelled/failed-run rows around long enough for forensics. If/when retention separation or per-table IAM justifies a dedicated dataset, the table moves with a single `TABLE_FQN` change in `src/dit/cache.py`.

## Cache key

```
cache_key = sha256(json.dumps({
    "pipeline_commit":     args.pipeline_commit,
    "worker_image_digest": resolve_to_digest(args.worker_image),
    "workflow_file_sha1":  sha1_of_workflow_file_bytes(),
    "params":              canonical_params_dict(args),
}, sort_keys=True))
```

**Inputs that go in:**
- `pipeline_commit` — the obvious one. Different code → different output.
- `worker_image_digest` — resolved from the tag at submit time, since tags are mutable (a `:main` retag would silently change behaviour without the cache noticing).
- `workflow_file_sha1` — the sha1 of `workflows/<pipeline>/<workflow>.py`'s bytes. The dit-side cache buster. Any change to the workflow file (date ranges, mode order, ssvids defaults, etc.) invalidates. Pure dit-library refactors that don't touch the workflow file (PR #10's `dit.job_names` extraction is the model case) do not.
- `canonical_params_dict(args)` — the args namespace filtered to output-affecting parameters, JSON-serialised with sorted keys. Includes `start`, `end`, `tail_days`, `backfill_days_w`, `ssvids`, `min_gap_length`, `n_hours_before`, `window_period_d`, `filter_good_seg`, `skip_open_gaps`, `source_messages`, `source_segments`. Excludes plumbing (`service_account`, `dataflow_region`, `bq_temp_dataset`, etc.).

**Inputs that do NOT go in:**
- `dit_commit` — refactors shouldn't invalidate the cache.
- `experiment_id`, `run_id`, `suffix`, `dest_dataset` — output naming, not output content.
- `--parallel`, `--allow-dirty-tree`, `--skip-comparisons` — execution flags, not output content.

**Per-workflow filter.** Each workflow's `parse_args` exposes a `canonical_params_dict(args) -> dict` function listing the output-affecting keys. Reviewer ensures new args land in the right side (output vs plumbing). When in doubt, default to including — false invalidation costs one recompute; false cache hit costs a wrong verdict.

## Lookup + write flow

```
def maybe_cached_run(workflow_fn, args) -> RunReport:
    key = compute_cache_key(args)
    row = bq_query(
        "SELECT * FROM tech_great_expectations.dit_runs "
        "WHERE cache_key = @k AND status='succeeded' "
        "  AND expires_at > CURRENT_TIMESTAMP() "
        "  AND NOT pipeline_dirty "
        "ORDER BY started_at DESC LIMIT 1",
        params={"k": key},
    ).first()

    if row:
        existing = check_tables_exist(row.output_tables)   # INFORMATION_SCHEMA.TABLES
        if all(existing):
            logger.info("cache HIT: %s", row.run_id)
            return RunReport.from_cached(row)
        logger.info("cache STALE: tables expired, recomputing")

    # cache miss -- run the workflow
    report = workflow_fn(args)
    if not args.pipeline_dirty and report.status == "succeeded":
        bq_insert("tech_great_expectations.dit_runs", row_from(report, cache_key=key))
    return report
```

**Dirty-tree handling.** When `pipeline_dirty=True` (the submitter's pipeline tree had uncommitted changes), the run is non-reproducible by definition. We `INSERT` a provenance row (with `pipeline_dirty=True` so it's excluded from cache reads) but the row is registry/cleanup-only — never reused.

**Concurrency.** Two PRs hitting `main`'s 1_bf at the same time both miss the cache (no prior row), both compute, both insert. Idempotent: same key, different `run_id`s. Next PR hits cache. Worth-the-waste; alternative (advisory locks) is heavier than the duplication cost.

## `expires_at` — single rule

`expires_at` reflects when the **physical `output_tables` are no longer guaranteed to exist** — not the dataset-level TTL.

The two values aren't always the same. In the common case (output tables live in `scratch_*_ttl120d` or `tech_great_expectations` with a uniform `default_table_expiration_ms`), they coincide and `expires_at = started_at + default_table_expiration_ms`. But:

- A workflow can write outputs to a dataset with a different (or no) default expiration, in which case the rule is whatever TTL the workflow set explicitly on its `CREATE TABLE`.
- Cross-version runs write to `dit_exp_*` datasets with a 7-day default; the cache row mirrors that 7-day window for those outputs, not the dest dataset's 120-day default.

Implementation: when writing a cache row, query `INFORMATION_SCHEMA.TABLES` for `expiration_time` on each `output_table` and take the **minimum** — any earlier-expiring output invalidates the whole cache entry. Stored as an absolute `TIMESTAMP` (not a duration) so the lookup query is a simple `WHERE expires_at > CURRENT_TIMESTAMP()`.

The `INFORMATION_SCHEMA.TABLES` lookup on the read path is the second-line guard: even if `expires_at` is wrong (TTL got changed after write), the cache hit still verifies physical existence before reusing.

## Cleanup flow (`make dit-cancel RUN_ID=<id>`)

```
1. SELECT output_tables, dataflow_job_ids FROM tech_great_expectations.dit_runs WHERE run_id = <id>
2. For each dataflow_job_id: gcloud dataflow jobs cancel
3. For each output_table: bq rm -f
4. UPDATE tech_great_expectations.dit_runs SET status='cancelled', finished_at=CURRENT_TIMESTAMP() WHERE run_id=<id>
```

Idempotent: re-running on an already-cancelled run is a no-op (cancel-already-cancelled-jobs returns silently; `bq rm -f` on non-existent tables likewise).

A separate **SIGTERM trap inside `dit run`'s `main()`** handles the live case (Cloud Build cancellation → orchestrator gets SIGTERM → traps → runs the same cleanup → exits). The `make dit-cancel` target is the after-the-fact recovery path for when the trap didn't fire (SIGKILL, network glitch, etc.).

## Open questions / known limitations

- **Cross-version comparison semantics with cache hits.** When the PR's 1_bf is freshly computed but main's 1_bf is a cache hit, the `experiment_id` of the cached row doesn't match the PR run's `experiment_id`. The comparison logic must join on `cache_key` (or its components), not on `experiment_id`. Easy to get wrong; needs a clear API on the report side.
- **Workflow file changes that produce identical outputs.** A docstring edit invalidates the cache for no reason. Tolerable for now; can refine with a hand-bumped `BEHAVIOUR_VERSION` constant later if it becomes painful.
- ~~**`dit_meta` dataset creation + IAM.**~~ Resolved 2026-05-22 — the cache table lives in the existing `tech_great_expectations` dataset that dit already writes outputs to. No new dataset / IAM grant needed; bootstrap is a one-shot `bq query --use_legacy_sql=false < migrations/001_dit_meta_runs.sql`.
- **Multi-pipeline lookups.** `cache_key` is globally unique (sha256 over all inputs), but partition + cluster keys (`pipeline`, `started_at`) optimise the common per-pipeline scan. If we ever need cross-pipeline reuse (unlikely), the schema supports it.

## Implementation plan

1. **`dit.cache` module** — `compute_cache_key(args)`, `read_cache(key) -> Optional[CachedRun]`, `write_cache(report)`, `check_tables_exist(fqns) -> list[bool]`. Pure functions; no workflow-side imports.
2. **Hook into workflows** — `mode_equivalence.py` and `ais.py` wrap their per-mode computations with `maybe_cached_run`. Per-mode (not per-workflow), so individual modes can be cached independently.
3. **`make dit-cancel`** — shell wrapper around `bq query` + `gcloud dataflow jobs cancel` + `bq rm`.
4. **SIGTERM trap in `dit run`** — best-effort; cancellation is rare.
5. **Tests** — `tests/test_cache.py` covering key computation determinism, cache hit/miss/stale logic, dirty-tree write-skip. Real-BQ smoke via a throwaway `tech_great_expectations.dit_runs_test` table.

Likely ~3 days of work for items 1-3; item 4 is small; item 5 is the verifier.

## Related

- [`docs/plan.md`](plan.md) § Next steps — this is item 1 in the rewritten list.
- The `dit_run_id` label baked into every Dataflow job + BQ table (PR #2's per-iteration labels work) is what makes cleanup work without a registry-lookup-then-grep flow.
- Subsumes the Phase 6 (phase sharing) and Phase 7 (golden-table mode) items from the original plan numbering. Those phases stay in the longer-term section as opt-ins for the per-phase granularity case, but for workflow-level reuse the cache is the answer.
