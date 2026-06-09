# Synthetic source mutation primitive — implementation plan 2026-06-09

Tracking doc for the synthetic-source-mutation primitive proposed in [issue #59](https://github.com/GlobalFishingWatch/data-integration-tests/issues/59) (filed 2026-06-09). Follow-on to the snapshot/dataset migration tracked in [`snapshot-dataset-migration-2026-06.md`](snapshot-dataset-migration-2026-06.md); ships AFTER that migration (M4 + M5) closes.

**Status (2026-06-09)**: design locked via issue #59 discussion; **M6a landed** (this commit) — `dit.bq.derived_source_into_experiment(...)` helper available; **M6b not yet started**, gated on the in-flight `workflows/pipe_gaps/outage_recovery.py` 5→3-stage refactor (the integration's stage-routing model fundamentally differs between shapes; writing M6b twice would be wasteful). Sequencing relative to M4 + M5 of the canonical-dataset migration: parallel — they touch disjoint files.

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
| M6b | `workflows/pipe_gaps/outage_recovery.py` synthetic-outage integration | B (workflow file) | ~+30 LOC + CLI flag + WHERE-clause construction | Cloud smoke against AIS staging cohort surfaces the 58 candidate-vessels signal | Not started — gated on outage_recovery 5→3-stage refactor |

M6a ships in parallel with M4 (canonical-dataset migration's remaining stages) — they touch disjoint files (`src/dit/bq.py` vs `workflows/port_visits/ais.py`). M6b is gated on the in-flight 5→3-stage refactor of `outage_recovery.py` because the stage-routing model (which stages read filtered vs unfiltered) is fundamentally different between the two shapes.

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

### M6b — outage_recovery integration

Add a CLI flag for outage dates, build the WHERE clause in the workflow (workflow-specific shape), call the helper after `snapshot_into_experiment`, route pre-recovery stages at the filtered FQN, fold outage dates into `canonical_params_dict`.

**New CLI flag**:
```
--outage-start YYYY-MM-DD     # inclusive
--outage-end YYYY-MM-DD       # inclusive
```
(Or a single `--outage-dates YYYY-MM-DD,YYYY-MM-DD` flag, matching `--date-range`'s shape.)

**Workflow integration sketch**:
```python
# 1. Snapshot (unchanged from M2):
pre_snap = dit_bq.snapshot_into_experiment(
    args.source_messages,
    experiment_id=args.experiment_id,
    role="outage_pre",
    as_of=args.pre_outage_pin_at,
    project=args.snapshot_dest_project,
)

# 2. Filter on top:
pre_filtered = dit_bq.derived_source_into_experiment(
    pre_snap,
    experiment_id=args.experiment_id,
    role="outage_pre_filtered",
    where_clause=(
        f"DATE(timestamp) NOT BETWEEN "
        f"'{args.outage_start.isoformat()}' AND '{args.outage_end.isoformat()}'"
    ),
    project=args.snapshot_dest_project,
)

# 3. Stage routing:
#   - Pre-recovery stages (the ones that establish the open-v1) read pre_filtered.
#   - Recovery stage + oracle read the unfiltered post-pin snapshot.
# This is what creates the outage divergence the workflow exists to surface.
```

**Cache key**: include `outage_start` and `outage_end` in `canonical_params_dict`. Two runs with different outage windows must not share a cache row.

**Pre-merge cloud smoke**: run against AIS staging with outage dates set to the date boundary where the 58 candidate vessels sit. The workflow should now flag a meaningful set of those 58 against the oracle — that's the load-bearing pre-merge check (M6b without that signal would mean the primitive isn't doing what the issue says it should).

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
- [ ] `workflows/pipe_gaps/outage_recovery.py` exposes `--outage-start` / `--outage-end` (or equivalent), threads them through `canonical_params_dict`, and calls the helper after the snapshot step.
- [ ] Cloud smoke against AIS staging with outage dates set to the 58-candidate-vessels date boundary surfaces a meaningful divergence in the oracle comparison — the workflow's intended bug-surfacing behaviour is finally reachable from staging.
- [ ] `CHANGELOG.md` gains a `#### Added` entry under `[Unreleased]` for both M6a and M6b.
