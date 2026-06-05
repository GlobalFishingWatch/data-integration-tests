# dit snapshot mechanism — edge-case audit 2026-06-05

Empirical audit of `dit.bq.snapshot_table` / `snapshot_dataset` against two edge-case categories: **view sources** and **cross-project / cross-org snapshots**. Companion to [`workflow-reconciliation-2026-06.md`](workflow-reconciliation-2026-06.md).

**Method**: live `bq` probes against `world-fishing-827.scratch_christian_homberg_ttl120d.*` with 1-hour TTLs; results read back via `bq show --format=prettyjson`.

**Status**: point-in-time as of 2026-06-05. The semantics described here are properties of BigQuery's DDL, not of dit; they're expected to stay stable unless BigQuery's snapshot-source restrictions change.

## TL;DR

| Edge case | Result | Risk class |
|---|---|---|
| View source | Hard fail with clear BQ error | Not a silent risk; annoying-but-safe |
| Cross-project **same-org** | Works correctly; `baseTableReference` points at cross-project source | Safe |
| Cross-project **cross-org** | Hard fail; "different orgs" error | Real impact: latent bug in `pipe_segment/identity_match_key.py --include-satellite-offsets` |

**The biggest non-obvious finding**: "GFW" colloquially groups three GCP projects that span **two organizations**. `gfw-int-vms-v3` and `gfw-int-pipe-v3` live in org `115316357079`; `world-fishing-827` is in org `433637338589`. Anything that needs to snapshot across that boundary fails fast.

## 1. What the implementation does today

`src/dit/bq.py` exposes two snapshot primitives. Both emit `CREATE SNAPSHOT TABLE … CLONE` DDL and forward to `bigquery.Client(project=…).query(sql).result()`.

**`snapshot_table(source_table, dest_table, *, as_of=None, expiration=None, project=DEFAULT_PROJECT, if_not_exists=False) -> None`** builds the SQL by string concatenation: `CREATE SNAPSHOT TABLE [IF NOT EXISTS] \`<dest_table>\` CLONE \`<source_table>\` [FOR SYSTEM_TIME AS OF TIMESTAMP("…")] [OPTIONS(expiration_timestamp=TIMESTAMP("…"))]`. **No probing of the source object's `type:` before issuing the DDL** — the SQL goes straight to BQ. (`DEFAULT_PROJECT` is `world-fishing-827`.)

**`snapshot_dataset(source_dataset, dest_dataset, *, tables=None, as_of=None, expiration=None, project=DEFAULT_PROJECT) -> list[str]`** validates that the dest dataset exists (raises `ValueError` on `NotFound`), lists tables on both sides, optionally filters by an explicit `tables=` allowlist, and loops calling `snapshot_table` per source table. **Skips any name already present in dest** (silent — only `logger.info`), making re-runs idempotent at the table-id level. The footgun documented in both docstrings: skipping by name reuses prior snapshots even if `as_of` differs, so an A/B re-run with the same `--experiment-id` but a new `--pin-source-at` silently reads the wrong baseline.

The unit tests (`tests/test_bq.py`) are entirely mock-based: they exercise the DDL string shape, kwarg forwarding, dataset-listing behaviour, and `NotFound` on missing dest. **Nothing in the tests covers**: view sources, cross-project sources, cross-org sources, the actual BQ rejection paths, or the `as_of`-mismatch footgun.

## 2. Views — empirical findings

Probed against `world-fishing-827.pipe_ais_test_202408290000_published.identity_core`, confirmed `"type": "VIEW"`. BQ rejected the snapshot cleanly:

> `Cannot snapshot world-fishing-827:pipe_ais_test_202408290000_published.identity_core which has type VIEW. Allowed types: [TABLE, SNAPSHOT]`

This is the **good** failure mode: an explicit BQ error names the source, names its type, and names the allowed types. **No silent succeed-as-view-passthrough.** The source-pinning guarantee dit's design relies on is not at risk of being quietly undermined — a view source produces a hard error at snapshot time.

`MATERIALIZED_VIEW` and `EXTERNAL` were not probed; the error wording (`Allowed types: [TABLE, SNAPSHOT]`) suggests they'd be rejected too, with the same shape of message.

## 3. Cross-project / cross-org — empirical findings

Three probes against `world-fishing-827.scratch_christian_homberg_ttl120d.*` with 1-hour TTLs.

**Cross-project, same org** (`gfw-research.ais_global_v3_public_common.insights_coverage_blocks` → `world-fishing-827`): **succeeded**. Resulting object's metadata:
- `"type": "SNAPSHOT"`
- `snapshotDefinition.baseTableReference` correctly pointing at the cross-project source
- Populated `snapshotTime`
- `numRows=1156471135` (source-equal)

Both `gfw-research` and `world-fishing-827` sit in organization `433637338589` (confirmed via `gcloud projects get-ancestors`).

**Cross-org** (`gfw-int-vms-v3.pipe_vms_v3_internal.anchorages_visited_info` → `world-fishing-827`): **failed** with

> `Cannot snapshot tables across projects that are in different orgs.`

`gfw-int-vms-v3` is in org `115316357079`; `world-fishing-827` is in org `433637338589`. The "GFW" colloquial grouping spans two organizations — this is the trap behind PR #45. The same error reproduced for `gfw-int-pipe-v3.satellite_positions.…` (also org `115316357079`) → `world-fishing-827`.

**Quasi-cross-org** (`bigquery-public-data.samples.shakespeare` → `world-fishing-827`): **succeeded**. Looked like a cross-org test on the face of it, but `gcloud projects get-ancestors bigquery-public-data` reveals it's actually in the same org `433637338589` as wf827 — likely a Google-special accommodation for the public-data project. Not a counter-example to the cross-org rule.

**Net**: same-org cross-project is fully supported, including correct `baseTableReference` and `snapshotTime` metadata. Cross-org fails fast with an unambiguous error. **There is no silent-misbehave middle ground.**

## 4. Real impact today

Walking the three call sites, with each source FQN's `type:` verified today.

### `workflows/port_visits/cross_version_ais.py`

Snapshots `world-fishing-827.pipe_ais_test_202408290000_internal.messages_positions`, `…_published.segment_info`, `…_published.segs_activity`. All three are `TABLE`, all same-project. **Not at risk** from either category. Any future `--source-dataset-stem` override pointing at a cohort that includes views would hit the view error and fail fast — annoying for the user but not silent.

### `workflows/pipe_gaps/outage_recovery.py`

Defaults: `pipe_ais_test_202408290000_{internal,published}.{messages_positions,segs_activity}` — all `TABLE`, same-project. **Safe.**

Prod-VMS override path (`--source-messages gfw-int-vms-v3.pipe_vms_v3_internal.research_messages …`) is explicitly handled by `--snapshot-dest-project` (added in PR #45), routing both source and dest into `gfw-int-vms-v3` to dodge the cross-org block. The default is safe; the override has a sharp edge but is documented.

### `workflows/pipe_segment/identity_match_key.py` — **LATENT BUG**

Default `--source-normalized-table` (`pipe_ais_test_202408290000_internal.normalized_messages`) is fine — `TABLE`, same-project.

**But `--include-satellite-offsets` is a latent bug.** It snapshots `gfw-int-pipe-v3.satellite_positions.satellite_positions_one_second_resolution_<date>` shards into `world-fishing-827.dit_exp_*_pipe_segment`. `gfw-int-pipe-v3` is org `115316357079`, dest is org `433637338589`. Empirically verified: the cross-org snapshot fails with the same "different orgs" error.

Unlike outage_recovery, **there is no `--snapshot-dest-project` flag on pipe-segment** (`snapshot_table(..., project=PROJECT)` is hard-coded). Anyone enabling `--include-satellite-offsets` today gets a fail-fast crash, not silent corruption — but **it has never worked**.

The opt-in flag is listed today as "default OFF, opt-in"; the opt-in path is dead until either:
- (a) the source defaults move to a wf827-org sibling, or
- (b) pipe-segment grows an outage-recovery-style `--snapshot-dest-project gfw-int-pipe-v3`.

## 5. Recommendations (ranked)

### Tactical: add `--snapshot-dest-project` to pipe-segment

**~10 LOC, fixes a real latent bug right now, no library coordination.** Mirror `outage_recovery.py`'s shape: a CLI flag, threaded into the `snapshot_table(..., project=...)` call. Makes `--include-satellite-offsets` actually usable.

### Library Guard B: pre-flight cross-org check

In `dit.bq.snapshot_table` / `snapshot_dataset`. Resolve both source and dest project's `organization_id` via `gcloud projects get-ancestors` (or `resourcemanager.projects.get`); raise a clear error if they differ. Catches the failure at workflow-launch time instead of mid-snapshot, with actionable error text naming both orgs and recommending `--snapshot-dest-project`. Cache the lookup per-process for `snapshot_dataset`'s loop. **Highest-leverage library change**; addresses the PR #45 class structurally.

### Library `if_existing="verify_as_of"` mode

Already drafted in the docstrings — the FOOTGUN block. Reads the existing snapshot's `snapshot_definition.snapshot_time` and either skips (true idempotence) or raises naming both timestamps. Closes the silent snapshot-reuse bug all three call sites currently document but don't fix. Orthogonal to the views/cross-org audit but pairs naturally with the consolidation pass discussed in [`workflow-reconciliation-2026-06.md`](workflow-reconciliation-2026-06.md) § 3-a.

### Library Guard A: pre-flight source-type check

Low value because BQ already errors clearly on views. Skip unless you want dit's error attribution (workflow name, experiment-id) rather than BQ's.

### Proposed signature

```python
def snapshot_table(
    source_table: str,
    dest_table: str,
    *,
    as_of: datetime | None = None,
    expiration: datetime | None = None,
    project: str = DEFAULT_PROJECT,
    if_existing: Literal["fail", "skip", "verify_as_of"] = "fail",
    check_same_org: bool = True,       # Guard B; opt-out for testing
    check_source_type: bool = True,    # Guard A; opt-out for testing
) -> None: ...
```

`snapshot_dataset` threads `if_existing` (per table) and the two `check_*` flags identically. Both `check_*` flags default `True` so the safer behaviour ships by default; opt-out is the named-keyword form so it's grep-able.

### Suggested PR shape

The natural single PR is **tactical fix + Guard B + `if_existing`**: they touch overlapping code and all benefit from the same test-suite extension (the existing tests are entirely mock-based; adding live-BQ tests for these three guards is the right moment to land that pattern). The pipe-segment `--snapshot-dest-project` tactical fix on its own is much smaller if it ships first.

## 6. Two-org reality — institutional note

Worth stamping somewhere durable (probably `docs/conventions.md`): three production-shaped GFW projects span two GCP organizations:

| Project | Organization |
|---|---|
| `world-fishing-827` | `433637338589` |
| `gfw-research` | `433637338589` |
| `gfw-int-infrastructure` | `433637338589` (per the prod-infra boundary) |
| `gfw-int-vms-v3` | `115316357079` |
| `gfw-int-pipe-v3` | `115316357079` |

Cross-project snapshots within an org are fully supported by BigQuery. Cross-org is not. The split appears in PR #45's pipe-gaps incident, in pipe-segment's `--include-satellite-offsets` latent bug, and will reappear any time a workflow needs to snapshot data from a `gfw-int-*v3` source into `world-fishing-827`. The fix shape is `--snapshot-dest-project` routing both source and dest into the same org.
