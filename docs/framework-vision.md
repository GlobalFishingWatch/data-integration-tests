# A shared integration-testing library for GFW pipelines — vision

> Exploratory design doc, not an implementation spec. Originated as a subagent's
> response to a "what would a generalised version of pipe-gaps' three-way
> equivalence test look like across pipelines?" prompt, with input notes from
> the conversation that produced it (debugging the pipe-gaps Bug A / open-v1
> seed / close-path bugs, and adding the four-mode integration test).

## 1. Mental model

The framework has five primitives. Keep them separate; resist the urge to fuse them.

- **Pipeline**: an existing GFW pipeline (pipe-gaps' `detect`, pipe-events' `fishing`, etc.) treated as a black-box callable that accepts a config and writes to a destination. The framework never modifies pipelines; it wraps them.
- **Phase**: one invocation of a Pipeline with a fully-resolved config — date range, input table, output table, ssvid filter, etc. A phase is the atomic unit of work and the unit of caching/sharing.
- **Mode** (a.k.a. *trigger pattern*): an ordered DAG of Phases producing one terminal output table.
- **Mutation**: a deterministic transformation applied to a Phase's *inputs* before that phase runs — ssvid restriction, message removal, timestamp shift, segment-flag flip, schema-version downgrade. Mutations are first-class so "what changed between phase A and phase B" is inspectable, not buried inside an `execute_*` function.
- **Comparison** (a.k.a. *oracle*): a directed assertion between two terminal tables (or views), parameterised by key columns, ignored columns, tolerances, and an expected-diff predicate.

A **workflow** is a set of Modes, a shared mutation timeline, and a set of Comparisons. Running the workflow plans the union DAG, executes phases (sharing where possible), then evaluates oracles.

Key cleavage: **today's `three_way_equivalence.py` collapses Pipeline + Phase + Mode + Mutation into per-mode `execute_*` functions and bakes comparison into a hardcoded list of pairs.** Splitting them is what makes the framework reusable.

## 2. The DSL — sketch

A workflow is a declarative document (Python-DSL or YAML; bias toward Python since pipeline configs are Python dataclasses already):

```
workflow "pipe-gaps three-way + recovery":
  pipeline = pipe_gaps.detect
  range    = [2020-01-01, 2021-01-01)
  defaults = { min_gap_length: 1, n_hours_before: 12, W: 4, ... }

  mode "1_bf"             = backfill(range)
  mode "2_bfd"            = backfill(range - tail) >> daily_tail(tail, window=W)
  mode "3_bftruncate"     = backfill(range)        >> daily_tail(tail, window=W)
  mode "4_mutate_recover" =
        backfill(range - tail)
     >> daily_tail(tail, window=W) with mutation = restrict_ssvids(auto)
     >> daily_tail(tail, window=W)

  oracle equivalent_on("_last_versions", keys=[gap_id, start_timestamp]):
     all_pairs(["1_bf", "2_bfd", "3_bftruncate"])
     "1_bf" ~ "4_mutate_recover"
```

`backfill`, `daily_tail`, `monthly_truncate_reload`, etc. are **phase generators** — small functions returning Phase configs given a range and window strategy. They are the only pipeline-shape-aware code in a workflow file.

## 3. Adding a new mode

A "VMS 4-day overlapping with monthly truncate-and-reload checkpoint":

```
mode "5_monthly_checkpoint" =
     monthly_truncate_reload(range, checkpoint = "month-end")
  >> daily_tail(tail, window=4)
```

Only new code: a ~20-line `monthly_truncate_reload` phase generator. Workflow shape unchanged; oracle list unchanged.

## 4. Mutation injection

A mutation is `(phase_config) -> phase_config'` plus a stable name. The framework ships a small library:

- `restrict_ssvids(set | "auto")` — `"auto"` defers to a workflow-level callback that runs *after* a designated reference phase completes (today's `compute_restricted_ssvids` pattern, decoupled from the pipeline).
- `drop_messages(predicate)` — removes rows via a temp BQ view.
- `shift_timestamps(delta, predicate)` — late-arrival simulation.
- `set_segment_flag(predicate, value)` — flips good_seg / overlapping_and_short.
- `downgrade_schema(version)` — projects older shape via input view.

Mutations attach to phases with `with mutation = ...` and compose. Because mutations are values (not control flow), the executor can hash them — two phases with identical config and identical mutation set are share candidates. A pipeline never knows it's been mutated; mutations always materialise as either a temp input view or a config override.

## 5. Comparison oracles

```
oracle equivalent_on(view_suffix, *,
                     keys, ignore_columns=[], tolerance=None,
                     expected_diff=None,
                     materialiser=table_check_summary):
```

Default `materialiser`: shell out to `table-check summary` (already in production use). Tolerance is per-column. `expected_diff` is a SQL predicate or row-count assertion — encodes "expect exactly these 9 rows to differ", which is the right shape for partial-equivalence (regression fixes that intentionally change a small known set).

Aggregate to a single workflow result; emit JUnit XML stretch goal.

## 6. Cross-version testing (secondary)

Per-mode pipeline binding:

```
mode "1_bf"   = backfill(range) using pipeline@main
mode "1_bf'"  = backfill(range) using pipeline@PR-119
oracle "1_bf" ~ "1_bf'"
```

Implementation tiers (pick lowest):

1. **Docker tag**: each binding → published image. Almost free given the docker runner.
2. **Git worktree + sdist**: framework checks out the ref into a worktree, runs `python -m build`, points the runner at the sdist. Works for Dataflow. Uncommitted code = bind to path instead of ref.
3. **Process isolation** subprocess-per-binding: only required for incompatible deps. Don't build until someone needs it.

## 7. Execution model

Phase DAG, bottom-up. Two phases are *equivalent* (share-eligible) when `(pipeline binding, resolved config, mutation set)` hash matches. Materialise each class once; populate downstream via `BQ COPY` (or symlink for filesystem outputs). Generalises the COPY trick we discussed: 4 range loads → 2.

Same-depth phases run concurrently, capped by worker count. Failures sticky — downstream phases don't run; oracle reports `UPSTREAM_FAILED`.

Runners (`local` / `docker` / `dataflow`) behind a `Runner` interface. Today's three port directly.

## 8. Tradeoffs

In scope: workflow vocabulary, phase sharing, mutation library, oracle library, runner interface, cross-version tier 1.

Out of scope (deferred to userspace, possibly forever): orchestrator integration (Dagster/Airflow), test-data generation, performance benchmarking, non-tabular oracles. Workflow-first means the workflow object is the lingua franca — write a `to_dagster_assets()` adapter when somebody actually needs one. Don't pre-build them.

Risks: the mutation library bloating (keep it ~5 mutations; one-offs go via lambda) and the phase-generator vocabulary turning into a transform zoo (resist).

## 9. Prior art worth borrowing

- **dbt `--defer` / `--state`**: per-node hashes for cache invalidation, manifest-as-state for cross-version. Maps directly onto oracles and bindings.
- **Dagster assets + IO managers**: assets ≈ Phases, ops ≈ mutations preceding them. Borrow the split. **Don't** adopt UI/scheduler.
- **Airflow DataIntervals**: `[start, end)` arithmetic. We already use it; codify in the date-generator core.
- **Great Expectations / soda-core**: oracle composition vocabulary, JUnit-emit. Not the full DSL.
- **pytest-recording / pytest-regression**: "golden table" mode — store a reference output keyed by workflow hash; future runs assert equivalence. Cheap cross-version for the common case.

Take vocabulary, leave runtime.

## 10. Recommended path forward

Smallest first step: extract three concepts from `three_way_equivalence.py` into a tiny shared package, keep everything else in the existing script.

1. **`Runner` protocol** + three implementations (local/docker/dataflow). Lift the Dataflow knobs (SA, region, temp bucket, subnetwork, temp dataset). Pipe-events' bash script wraps over this immediately. ~200 LOC.
2. **`Phase` and `Mode` dataclasses** + `backfill` and `daily_tail` generators. Rewrite `execute_bf` / `execute_bfd` / `execute_bftruncate` as one-line declarations. Mode 4 demonstrates mutation. ~150 LOC.
3. **`Oracle` with `table_check_summary` backend.** Replaces `compare_tables`. ~100 LOC.

Stop there. Don't build phase sharing, cross-version, or new mutations until pipe-events adopts the package and surfaces a real second use case. **The library earns abstractions by being used, not by being designed.**

That's a reusable core in roughly a week, validation against two repos before generalisation, and every new mode is an additive change.

---

## Reviewer's meta-take (Claude, not the subagent)

- **The 5-primitive split is the right cleavage.** Today's `execute_*` functions fuse Phase + Mode + Mutation. Once you split them, today's "auto-restrict" stops being a special case and becomes "a Mutation whose value is computed from a reference Phase's output" — much less ad-hoc.

- **The phase-sharing argument is exactly the COPY redesign discussed in the conversation**, generalised. Worth doing the small version first (just for pipe-gaps), then promoting to a primitive once pipe-events validates the shape. Don't build the framework's hash-based sharing until the simple version is in production.

- **I'd push back gently on "cross-version is secondary".** Tier-1 (docker tag) is genuinely cheap given the docker runner exists, and it solves a real problem (you couldn't have caught the duration_h regression without running PR #119 against the same data — a workflow capable of `mode "1_bf'" using pipeline@PR-119` makes that a one-line invocation). I'd suggest tier-1 as part of the initial 450-LOC core.

- **The "earn abstractions by being used" close is the most important paragraph.** Resist the urge to design for hypothetical future requirements. Two real repos using a vocabulary is more valuable than a perfect abstraction over one.
