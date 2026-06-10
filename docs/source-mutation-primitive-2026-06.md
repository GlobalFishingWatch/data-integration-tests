# Synthetic source mutation primitive — implementation plan 2026-06-09

Tracking doc for the synthetic-source-mutation primitive proposed in [issue #59](https://github.com/GlobalFishingWatch/data-integration-tests/issues/59) (filed 2026-06-09). Follow-on to the snapshot/dataset migration tracked in [`snapshot-dataset-migration-2026-06.md`](snapshot-dataset-migration-2026-06.md); see Status line below for the per-stage sequencing (M6a ships independently of the migration; M6b is gated on workflow refactor rather than the migration).

**Status (2026-06-10)**: **COMPLETE** (pending the live cloud smoke). Design locked via issue #59 discussion; **M6a landed** (PR #61) — `dit.bq.derived_source_into_experiment(...)` helper; **M6b landed** (this commit) — `workflows/pipe_gaps/outage_recovery.py` gained the `--synthetic-outage` flag, integrating the helper against the 3-stage shape that landed in PR #63. The as-built M6b differs from the sketch below in two ways (the sketch predates the 3-stage refactor): (a) no new date flags — the 3-stage shape's existing `--outage-start` / `--outage-end` drive the WHERE clause; the new surface is a single `--synthetic-outage` boolean; (b) the filtered view's role encodes the outage geometry (`outage_filtered_<start>_<end>`), so a re-run with different dates derives a fresh view instead of silently reusing a stale filter. The cloud smoke that surfaces the 58-candidate-vessels signal remains the outstanding live verification (see acceptance criteria).

## Why this primitive

`workflows/pipe_gaps/outage_recovery.py` exists to surface a class of pipe-gaps bugs that only fire when source data has a real outage — one or more days where data is missing during initial processing and present later when a recovery backfill runs. On evolving prod sources (VMS with late arrivals), the workflow can naturally reproduce this via the pre/post-snapshot pair. On the static AIS staging cohort, the pre/post-snapshot pair produces byte-identical snapshots and no stage encounters the outage shape — so the bug class can't be triggered from staging. Concrete evidence: 58 candidate vessels in the staging cohort sit at the bug-trigger geometry but stay invisible today.

Synthetic source mutation is also a general capability the framework currently lacks. Future workflows wanting "what if a receiver type drops out", "what if a vessel's MMSI changes mid-range", etc. all hit the same gap.

## Core insight: snapshot and mutation are orthogonal, compose

- **Snapshot** (`dit.bq.snapshot_into_experiment`, already shipped via M1) — "freeze this source at a moment in time". Relevant for evolving sources; benign on static sources (no-op-shaped indirection).
- **Mutation** (this primitive — `dit.bq.derived_source_into_experiment`) — "mutate this source via a SQL transform". Independent of whether the input is a live table, a snapshot, or another derived view.

```
[live source]  ──snapshot──▶  [pinned source]  ──derive──▶  [mutated pinned source]
                                                                    │
                                                                    └──▶ stage reads here
```

Either step is optional; the workflow composes what it needs.

## API

```python
def derived_source_into_experiment(
    source_table: str,
    *,
    experiment_id: str,
    role: str,
    where_clause: str,
    expiration_days: int = 7,
    materialise: bool = False,
    if_existing: Literal["fail", "skip"] = "skip",
    project: str = DEFAULT_PROJECT,
) -> str:
    """Create a derived view (or materialised table) over ``source_table``
    with ``WHERE <where_clause>`` applied, at the canonical
    ``<project>.tech_great_expectations.dit_exp_<sanitised(experiment_id)>_<sanitised(role)>_<source_table_name>``
    address with per-table ``expiration_timestamp``. Returns the dest FQN.

    Composes with ``snapshot_into_experiment``: pass a snapshot FQN as
    ``source_table`` when both pin and filter are needed.
    """
```

### Locked design decisions (per issue #59 discussion)

1. **View by default; `materialise=True` for hot read paths.** BQ supports `expiration_timestamp` on both views and tables; predicate push-down on views handles partition pruning automatically. Materialised tables only pay for themselves when read cost dominates create cost (multi-pass reads over a large source).

2. **SQL-WHERE-string predicate, not typed filter constructor.** Simplest possible API for one consumer (outage_recovery). The typed `SourceFilter` constructor with composability surfaces when a second workflow needs it — duplicate-until-3.

3. **Cache integration via caller responsibility.** The helper takes the `where_clause` as a string; the caller is responsible for folding that same string into their `canonical_params_dict`. No content-hash machinery, no hidden index. The string is already stable per construction.

4. **Naming + lifecycle identical to `snapshot_into_experiment`.** Same `<project>.tech_great_expectations.dit_exp_<sanitised(experiment_id)>_<sanitised(role)>_<source_table_name>` shape, same `if_existing="skip" | "fail"` semantics, same 7-day default TTL. Shape-compatible by construction; the two helpers compose without naming-collision concerns as long as the caller uses disjoint `role` values for each layer (e.g. `outage_pre` for the snapshot, `outage_pre_filtered` for the filtered view).

5. **Returns the dest FQN string.** Caller can immediately use it for downstream pipeline args, or pass it back to another `derived_source_into_experiment` call to compose another transform on top.

## Implementation sequence

Two PRs, mirroring the M1 → M2 shape of the canonical-dataset migration.

| # | Title | Tier | LOC | Pre-merge check | Status |
|---|---|---|---|---|---|
| M6a | `dit.bq.derived_source_into_experiment(...)` library helper | A (additive library) | ~+95 LOC + 9 tests | Unit tests only — pure addition, no consumer yet | **Landed 2026-06-09** |
| M6b | `workflows/pipe_gaps/outage_recovery.py` synthetic-outage integration | B (workflow file) | ~+110 LOC (the 3 pure helpers + `--synthetic-outage` flag + stage-routing + docstring updates) + 8 tests | Cloud smoke against AIS staging cohort surfaces the 58 candidate-vessels signal (OUTSTANDING — user-gated live verification; the stage-routing unit test is the structural guarantee meanwhile) | **Landed 2026-06-10** |

M6a shipped in parallel with M4 (disjoint files). M6b landed after the outage_recovery 5→3-stage refactor (PR #63) settled the stage-routing model it integrates against.

### M6a — Library helper

Pure addition under `src/dit/bq.py`. Mirrors `snapshot_into_experiment`'s shape:

- Reuses `CANONICAL_DATASET` constant.
- Reuses `_utc_now()` for expiration timestamp computation.
- Reuses the sanitisation rule (`-` → `_` on both `experiment_id` and `role`).
- DDL: `CREATE [OR REPLACE] [VIEW|TABLE] [IF NOT EXISTS] \`<dest>\` OPTIONS(expiration_timestamp=TIMESTAMP("...")) AS SELECT * FROM \`<source>\` WHERE <where_clause>`.
- View path is the default; `materialise=True` switches to `CREATE TABLE`.
- `if_existing="skip"` → `IF NOT EXISTS`; `if_existing="fail"` drops it.

**Tests (mocked BQ client, same pattern as test_bq.py)**:
- Default dest + 7-day expiration computed from `_utc_now()`.
- View DDL shape (`CREATE VIEW IF NOT EXISTS ... AS SELECT * FROM ... WHERE ...`).
- `materialise=True` switches to `CREATE TABLE`.
- `experiment_id` and `role` sanitisation.
- `where_clause` interpolated verbatim into the SQL (no quoting / no escaping — caller's responsibility, same convention as existing helpers).
- Source FQN → table-name extraction.
- `if_existing="fail"` drops `IF NOT EXISTS`.
- Custom `project` threads through both the BQ client and the dest FQN.
- Return value matches the constructed dest FQN.

### M6b — outage_recovery integration (AS BUILT, 2026-06-10)

The original sketch (preserved in git history) predates the 3-stage refactor (PR #63); the as-built shape integrates against it:

**New CLI flag** — a single opt-in boolean; the 3-stage shape's existing `--outage-start` / `--outage-end` drive the WHERE clause:
```
--synthetic-outage
```

**As-built integration**:
```python
# In main(), after the source path resolves (snapshot / --skip-snapshots /
# --no-snapshot all compose -- the filter applies on top of whichever
# messages FQN the path produced):
filtered_msgs = None
if args.synthetic_outage:
    filtered_msgs = _derive_synthetic_outage_view(
        args, source_messages_fqn=src_msgs,
        outage_start=outage_start, outage_end=outage_end,
    )
    # -> dit_bq.derived_source_into_experiment(
    #        src_msgs,
    #        role=f"outage_filtered_{start:%Y%m%d}_{end:%Y%m%d}",
    #        where_clause="DATE(timestamp) NOT BETWEEN '<start>' AND '<end>'",
    #        ...)

# execute_outage_recovery(..., filtered_messages=filtered_msgs):
#   - Stages 1+2 read filtered_messages as bq_input_messages (the source
#     as it looked DURING the outage).
#   - Stage 3 (recovery) reads base_cfg's unfiltered messages (the source
#     after it healed).
#   - The oracle never sees the filter.
```

Three pure helpers carry the logic (`_synthetic_outage_where_clause`, `_synthetic_outage_role`, `_derive_synthetic_outage_view`) — each unit-testable without BQ.

**Design points beyond the sketch**:
- **Role encodes outage geometry** (`outage_filtered_<YYYYMMDD>_<YYYYMMDD>`): a re-run with the same `--experiment-id` but different outage dates derives a NEW view instead of silently reusing the stale filter (the same staleness class as the documented skip-existing snapshot footgun, dodged structurally). Same-geometry re-runs hit `IF NOT EXISTS` and are idempotent.
- **Only the messages source is filtered.** The segments input (`segs_activity`) is a per-segment summary used for good-seg filtering; a real outage would eventually dent it too, but the bug-trigger geometry is about message gaps and segments would need a different (non-`timestamp`) predicate shape. Known simplification — revisit if the cloud smoke shows segment-side artefacts.
- **View (not materialised).** pipe-gaps reads inputs via SQL queries (the EXPORT-staging `--bq-temp-dataset` machinery exists for exactly this), and BQ resolves views inside queries with predicate push-down. `materialise=True` stays available if read cost ever dominates.

**Cache key**: `synthetic_outage: bool` in `canonical_params_dict` — recovery mode only. The oracle reads the unfiltered source either way (output invariant to the flag), so `_RECOVERY_ONLY_KEYS` drops it from the oracle key; toggling the flag doesn't invalidate the oracle. The WHERE clause itself is fully determined by `(synthetic_outage, outage_start, outage_end)`, all already in the recovery key — no SQL string stored.

**Cloud smoke (OUTSTANDING)**: run against AIS staging with outage dates set to the date boundary where the 58 candidate vessels sit. The workflow should flag a meaningful set of those 58 against the oracle — the load-bearing live verification (M6b without that signal would mean the primitive isn't doing what the issue says it should). User-gated; the stage-routing unit test (`test_execute_outage_recovery_routes_filtered_messages_to_stages_1_2_only`) is the structural guarantee meanwhile.

## Adjacent items / future extensions

- **Composable `SourceFilter` API** (typed filter constructor with `.and_()` / `.or_()` shape) — lift from raw SQL strings when a second workflow wants composable mutations. Duplicate-until-3.
- **`if_existing="verify_as_of"` mode** — currently deferred for `snapshot_into_experiment` (post-M5 follow-up). When that lands, apply the same mode to `derived_source_into_experiment` for parity; both helpers share the `if_existing` parameter shape so the change is straightforward.
- **Richer mutation shapes** beyond `WHERE` filtering — e.g. row-level transforms (flip a segment-quality flag), MMSI rewrites, receiver-type substitutions. If they fit a `SELECT * REPLACE(...) FROM ... WHERE ...` shape, the same helper extends; otherwise a sibling helper.
- **Materialised refresh semantics** — for `materialise=True`, the table is created once and TTL-deleted. No incremental refresh is in scope; if a future workflow needs that, it's a separate primitive.

## Out of scope

- **Filtering during snapshot creation.** `CREATE SNAPSHOT TABLE ... CLONE` doesn't accept a `WHERE` clause; snapshots are full clones by design. Filtering happens in a separate DDL on top of the snapshot.
- **Cross-pipeline orchestration of mutations** — each workflow constructs its own WHERE clauses (and validates them). The helper doesn't know about specific pipelines.
- **Streaming-filter / change-data-capture shapes** — out of scope. The primitive is batch-DDL only.

## Acceptance criteria (when M6 is complete)

- [x] `dit.bq.derived_source_into_experiment(...)` available; unit tests pass; full suite green. (Landed 2026-06-09 as M6a.)
- [x] `workflows/pipe_gaps/outage_recovery.py` exposes the outage-geometry flags (`--outage-start` / `--outage-end`, from the 3-stage refactor) + the new `--synthetic-outage` opt-in, threads `synthetic_outage` through `canonical_params_dict` (recovery mode), and calls the helper after the source path resolves. (Landed 2026-06-10 as M6b.)
- [ ] Cloud smoke against AIS staging with outage dates set to the 58-candidate-vessels date boundary surfaces a meaningful divergence in the oracle comparison — the workflow's intended bug-surfacing behaviour is finally reachable from staging. (OUTSTANDING — user-gated live verification.)
- [x] `CHANGELOG.md` gains `#### Added` entries under `[Unreleased]` for both M6a (2026-06-09) and M6b (2026-06-10).
