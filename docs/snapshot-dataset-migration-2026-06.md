# Snapshot/dataset migration plan — 2026-06-08

Tracking doc for the migration of dit's BQ snapshot/dataset machinery onto the canonical-dataset policy locked 2026-06-08 (PR #54). Companion to [`snapshot-edge-cases-2026-06.md`](snapshot-edge-cases-2026-06.md) (the empirical audit) and [`workflow-reconciliation-2026-06.md`](workflow-reconciliation-2026-06.md) (the broader workflow review). Update as PRs land.

**Status (2026-06-08)**: policy landed (PR #54); **M1 landed in PR** (this commit) — `dit.bq.snapshot_into_experiment(...)` helper available; M2–M5 not yet started. Two adjacent items (cross-org Guard, pipe-segment satellite-offsets tactical fix) are tracked separately as they don't depend on the migration.

## What we're fixing

Three workflows create per-experiment BQ datasets (`dit_exp_<experiment_id>_*`) via `bigquery.Client.create_dataset`:

- `workflows/port_visits/cross_version_ais.py`
- `workflows/pipe_gaps/outage_recovery.py`
- `workflows/pipe_segment/identity_match_key.py`

Two problems:

1. The Cloud Build SA `automated-testing@` lacks `bigquery.datasets.create` on `world-fishing-827` (intentional — narrow IAM). The cloud path fails when these workflows try to provision a fresh snapshot dataset.
2. Per-experiment datasets spam the BQ project, even though they auto-clean via TTL.

## Policy (locked, PR #54)

**All dit BQ artifacts belong in `world-fishing-827.tech_great_expectations`.** Output tables, cache rows (`dit_runs`), and source-pinning snapshots all go there. No per-experiment dataset creation. Snapshot tables use per-table `expiration_timestamp` for TTL (BQ supports it natively on `CREATE SNAPSHOT TABLE OPTIONS(...)`). The table-level-only cancel-run guard (`_looks_like_table_fqn` in `dit.cache`) is the structural enforcer of the policy.

The policy is stated in `CLAUDE.md` § Working agreements (rewritten 2026-06-08). The legacy-dataset protection rule ("never `bq rm` a `dit_exp_*` dataset manually") is preserved as a transitional note until M5 lands.

## Migration sequence

Five PRs, ordered. Dependency graph:

```
M1  ─┬──▶  M2  (independent)
     ├──▶  M3  (independent)
     └──▶  M4  ──▶  M5
```

M2, M3, M4 can ship in parallel after M1; M5 must come last (it depends on M4's new flags on `ais.py`).

| # | Title | Tier | LOC | Pre-merge check | Status |
|---|---|---|---|---|---|
| M1 | `dit.bq.snapshot_into_experiment(...)` library helper | A (additive library) | ~+30 LOC + tests | Unit tests only — pure addition, no consumer yet | **Landed 2026-06-08** |
| M2 | Convert `workflows/pipe_gaps/outage_recovery.py` | B (workflow file) | ~-50 LOC net | Optional cloud smoke against staging cohort | Not started |
| M3 | Convert `workflows/pipe_segment/identity_match_key.py` | B | ~-30 LOC net | Optional cloud smoke; **fold in** the `--snapshot-dest-project` tactical fix (see Adjacent items) | Not started |
| M4 | Add per-table FQN flags to `workflows/port_visits/ais.py` | B (additive workflow change) | ~+30 LOC | Cloud smoke of `ais.py` standalone — backward compat critical | Not started |
| M5 | Convert `workflows/port_visits/cross_version_ais.py` | B | ~-60 LOC net | Cloud smoke of a real cross-version run | Not started |

Net effect when complete: ~150–200 LOC removed from workflows, one new library helper, one `ais.py` CLI extension, the `dit_exp_*` dataset spam eliminated, the transitional protection rule removed from CLAUDE.md.

### M1 — `dit.bq.snapshot_into_experiment(...)` library helper

Pure addition under `src/dit/bq.py`. Lets the workflows opt in one at a time.

**Signature (locked):**

```python
def snapshot_into_experiment(
    source_table: str,
    *,
    experiment_id: str,
    role: str,
    expiration_days: int = 7,
    as_of: datetime | None = None,
    if_existing: Literal["fail", "skip"] = "skip",
    project: str = DEFAULT_PROJECT,
) -> str:
    """Snapshot a source table into the canonical tech_great_expectations
    dataset with a per-table TTL. Returns the destination FQN."""
```

**Naming convention** (also locked): dest FQN is `<project>.tech_great_expectations.dit_exp_<sanitised(experiment_id)>_<sanitised(role)>_<source_table_name>` where:
- `sanitised(experiment_id)` and `sanitised(role)` both replace `-` with `_` (matches the existing `_sanitize_for_dataset` shape; also prevents a freeform `role` from producing a BQ table id that needs special quoting — Copilot review catch on PR #56).
- `role` is a caller-supplied label (e.g. `cross_version`, `outage_pre`, `outage_post`, `pipe_segment`); caller responsibility to keep roles disjoint per workflow.
- `source_table_name` is the last `.`-separated component of the `source_table` FQN. Helper splits internally.
- `project` defaults to `world-fishing-827` but is overridable (cross-org dodge path). The docstring describes the canonical home as `<project>.tech_great_expectations` accordingly.

**Tests** (mock BQ client like the existing `tests/test_bq.py`):
- DDL shape correct (CREATE SNAPSHOT TABLE … CLONE … OPTIONS(expiration_timestamp=…)).
- `experiment_id` sanitisation (`pipeline-1465` → `pipeline_1465`).
- `as_of` plumbed into `FOR SYSTEM_TIME AS OF TIMESTAMP(...)`.
- `if_existing="skip"` translates to `CREATE SNAPSHOT TABLE IF NOT EXISTS`.
- `if_existing="fail"` translates to `CREATE SNAPSHOT TABLE` (no IF NOT EXISTS).
- Custom `project` threads through.
- Return value matches the constructed dest FQN.

**Locked design decisions:**

1. **Single helper per source table; callers loop themselves.** A `snapshot_many_into_experiment` would save ~3 lines per consumer. Defer per duplicate-until-3 — if a fourth consumer wants the loop, lift then.
2. **`if_existing` only `skip`-only and `fail`-only in M1.** The audit recommends a `verify_as_of` mode that reads the existing snapshot's `snapshot_time` and either skips or raises naming both timestamps. That closes the documented FOOTGUN in all three call sites' comments. **Folded as a follow-up PR after M5** so the helper API surface stays small in M1.
3. **Returns dest FQN** (string). Callers immediately use it for downstream pipeline args.

### M2 — Convert `workflows/pipe_gaps/outage_recovery.py`

Drop the file-local helpers (`_ensure_snapshot_dataset`, `_snapshot_dataset_name`, the `_ensure_dataset` mimicry comment). Snapshot loops call `snapshot_into_experiment(source_table, experiment_id=..., role="outage_pre"|"outage_post", as_of=..., project=args.snapshot_dest_project or PROJECT)`. The `--snapshot-dest-project` flag (PR #45) stays — it routes both source and dest into the same project for the cross-org VMS override path.

Update workflow docstring (lines ~233) and the `dit_exp_*` references to describe the canonical-dataset shape. Update the leading comment block.

### M3 — Convert `workflows/pipe_segment/identity_match_key.py`

Same shape as M2: drop `_ensure_dataset` + `_snapshot_dataset_name`; call `snapshot_into_experiment(role="pipe_segment", ...)`.

**Fold in the `--snapshot-dest-project` tactical fix** for the `--include-satellite-offsets` latent cross-org bug (the audit's recommended ~10 LOC fix; see [`snapshot-edge-cases-2026-06.md`](snapshot-edge-cases-2026-06.md) § 4 and § 5). Same flag shape as `outage_recovery.py`'s.

Update workflow docstring (line ~18).

### M4 — Add per-table FQN flags to `workflows/port_visits/ais.py`

Additive, backward-compatible CLI extension. New flags:
- `--source-messages-fqn` (overrides the `<stem>_internal.messages_positions` derivation)
- `--source-segment-info-fqn` (overrides `<stem>_published.segment_info`)
- `--source-segs-activity-fqn` (overrides `<stem>_published.segs_activity`)

Resolution: when the per-table flag is set, it wins; otherwise the existing `<stem>_internal`/`<stem>_published` derivation from `--source-dataset-stem` applies. Document the precedence in the `--source-dataset-stem` help text.

**Backward-compat is the load-bearing concern here.** Run the standalone `port_visits/ais.py` cloud smoke from the feature branch before merging to verify the existing stem path is byte-identical.

### M5 — Convert `workflows/port_visits/cross_version_ais.py`

Drop `_ensure_dataset` and `_snapshot_stem`. Use `snapshot_into_experiment(source_table, experiment_id=..., role="cross_version", as_of=args.pin_source_at, ...)` for each of the three source tables; capture the returned dest FQNs and pass them to `ais.py` via the M4 flags (`--source-messages-fqn=...` etc.) instead of `--source-dataset-stem=...`.

Update workflow docstring (line ~24) and the leading comment block (steps 2 + 3 in the docstring describe creating snapshot datasets — flip to describing canonical-dataset snapshots).

**Cloud smoke**: a real cross-version run is the right pre-merge check (e.g. the PIPELINE-1465-style A/B that was the original motivation for cross_version_ais).

**When M5 lands**, remove the transitional legacy-dataset protection rule from `CLAUDE.md` § Working agreements (the "never `bq rm` a `dit_exp_*` dataset manually" sentence) — the legacy datasets stop being created.

## Adjacent items (related but separately tracked)

These aren't part of M1–M5 but live in the same problem space.

### Cross-org Guard (from snapshot-edge-cases audit § 5)

Add a pre-flight cross-org check in `dit.bq.snapshot_table`: resolve both source and dest project's `organization_id`; raise a clear error if they differ, naming both orgs and recommending `--snapshot-dest-project`. Catches PR #45-class failures at workflow-launch time. ~30 LOC + tests.

**Independent of the migration**: improves the underlying `snapshot_table`, which both old and new call paths use. Could ship in parallel with M1.

### `if_existing="verify_as_of"` mode (from snapshot-edge-cases audit § 5)

Add the third `if_existing` mode to `snapshot_table` (and via M1's helper, `snapshot_into_experiment`). Reads the existing snapshot's `snapshot_definition.snapshot_time` and either skips (true idempotence) or raises naming both timestamps. Closes the FOOTGUN documented in all three current call sites' comments.

**Ships as a follow-up PR after M5**, when the migration call sites can flip to `verify_as_of` together. Could come earlier if there's a strong reason, but bundling keeps `snapshot_into_experiment`'s API stable across M1–M5.

### pipe-segment `--include-satellite-offsets` cross-org tactical fix

Surfaced in the snapshot-edge-cases audit § 4 as a latent bug: the opt-in flag snapshots from `gfw-int-pipe-v3` (org `115316357079`) into `world-fishing-827` (org `433637338589`) and fails fast. Fix is a `--snapshot-dest-project` flag mirroring `outage_recovery.py` (~10 LOC).

**Folded into M3** since both touch `identity_match_key.py`. Worth ensuring the M3 PR description notes the latent-bug fix so reviewers know the file change is doing two things.

## Docs that update as each migration PR lands

These files describe current behaviour and are deliberately untouched until the corresponding code change ships, so the docs never lie about what the code does:

- `workflows/port_visits/cross_version_ais.py` docstring (line ~24) → update in M5.
- `workflows/pipe_gaps/outage_recovery.py` docstring (line ~233) → update in M2.
- `workflows/pipe_segment/identity_match_key.py` docstring (line ~18) → update in M3.
- `docs/architecture.md` Mermaid diagram (line ~146) showing `dit_exp_[exp-id]_{internal,published}` → update in M5 (when the last workflow flips).
- `docs/run-cache.md` (line ~122) and `docs/run-cache-impl.md` (line ~68) cross-version `dit_exp_*` references → update in M5.

The `CLAUDE.md` § Working agreements transitional protection note → removed in M5 (no more legacy datasets to protect).

## Out of scope

- Pre-creating `dit_exp_*` datasets via Terraform (Option 2 in the audit). Declined: adds permanent dit-owned datasets, requires admin coordination, doesn't realise the canonical-dataset policy.
- `dit.bq.snapshot_dataset` API change. The loop helper stays (still useful for the table-iterator semantics); its docstring updates with the migration to drop the dataset-creation framing.
- A `dit.snapshots` module separate from `dit.bq`. Could be cleaner long-term but adds a module without saving meaningful surface area; keep in `dit.bq` for now.

## Acceptance criteria (when migration is complete)

- [ ] `grep -rn 'create_dataset' src/dit/ workflows/` returns zero matches in code paths reachable from a workflow.
- [ ] All three migrated workflows produce snapshot tables under `world-fishing-827.tech_great_expectations.dit_exp_*` (verify by running each once against the staging cohort and listing `bq ls tech_great_expectations | grep dit_exp_`).
- [ ] `make dit-cloud PIPELINE=<each> WORKFLOW=workflows/<each>/<each>.py` succeeds from the cloud path for at least one of the three migrated workflows.
- [ ] Tests pass (unit suite + the targeted cloud smokes called out per PR above).
- [ ] `CLAUDE.md` § Working agreements bullet on dit BQ artifacts no longer carries the transitional `bq rm` protection note.
- [ ] `CHANGELOG.md` gains a final 2026-XX-XX entry summarising the completed migration.
