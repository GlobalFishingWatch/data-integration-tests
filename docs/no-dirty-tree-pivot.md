# Pivot: no dirty trees, push every snapshot

**Status:** planning / design. Not landed. Drafted 2026-05-22.

## Why

dit accumulated eight discrete pieces of dirty-tree-aware logic between the workflow Python files, the cache schema, the runner-time warnings, and the docs:

1. `--allow-dirty-tree` CLI flag on both workflows.
2. `_dirty` substring baked into output-table suffixes.
3. `pipeline_dirty` BOOL column in `tech_great_expectations.dit_runs`.
4. `WHERE pipeline_dirty = FALSE` filter inside `dit.cache.read_cache`.
5. `dit.git_info.warn_if_worker_image_misses_dirty_tree` warning helper.
6. `_run_with_cache` writing dirty rows that can never satisfy a lookup.
7. Tests pinning the dirty-row write/skip behaviour (`test_run_with_cache_writes_dirty_rows`, etc.).
8. Memories ([[submitter-vs-worker-split]], [[dit-runs-cache]]) documenting the dirty-tree behaviour as a "permanent feature".

Each was defensible individually; together they're a real surface area that consumes review attention and creates the kind of dead-end demo we hit on 2026-05-22 (build 1 and build 2 both ran the full Dataflow workload because the dirty filter blocked any hit — wasted ~60 min Dataflow and ~$10 USD with no value).

The original plan never called for this. `make snapshot-<pipeline>` has existed since 2026-05-08 as the canonical "test uncommitted changes reproducibly" pattern (`git stash create` → temp branch → install from there → working tree untouched). `--allow-dirty-tree` was added in the 2026-05-14 port-visits Phase 2 spike as a convenience escape-hatch; the cache schema then had to absorb it.

## End state

After this pivot:

- **Every cache row references a committed, fetchable git ref.** Pure reproducibility.
- **`--allow-dirty-tree` is removed.** A dirty tree at submit time → dit auto-snapshots, auto-pushes to `refs/dit-snapshots/<pipeline>/<epoch>-<hex>`, and uses that ref. The user doesn't need to think about snapshots; the workflow handles it.
- **`pipeline_dirty` column drops from the cache.** Replaced by a sharper-semantic `unreviewed_code` BOOL: `TRUE` for snapshot refs and ad-hoc branches, `FALSE` for merged-into-main commits. This carries the actual semantic the dirty-tree filter was a proxy for ("don't trust this row for cross-pipeline / PR-validation purposes") without conflating it with git state.
- **`dit.git_info.warn_if_worker_image_misses_dirty_tree` is removed.** No dirty trees possible; no warning needed. The submitter-vs-worker memory's content stays relevant (it's about worker-image staleness, not git state) but the warn helper is gone.
- **`_dirty` suffix gone from output table names.** Output suffix becomes `<experiment_id>_<commit>_<uuid>` — every byte traceable to a real git ref.
- **`make snapshot-<pipeline>` auto-pushes** to the `refs/dit-snapshots/*` namespace.
- **`make clean-snapshots` extended** to also delete the remote refs. User-invoked, same shape as today.
- **The User experiences zero extra ceremony** for the iterative-development path (today's two-command friction goes away because dit handles the snapshot automatically). They gain reproducibility and cache hits on repeat runs of the same uncommitted code.

## Migration plan

Each milestone is a separate PR. Ordering matters; earlier PRs can land independently.

### M-pivot-1 — `refs/dit-snapshots/*` namespace + auto-push in `make snapshot-<pipeline>`

- Update `scripts/snapshot-install.sh` (and `make snapshot-<pipeline>`) to:
  - Create the snapshot ref under `refs/dit-snapshots/<pipeline>/<epoch>-<hex>` instead of `refs/heads/dit-snapshot-<epoch>`.
  - `git push origin refs/dit-snapshots/<pipeline>/<epoch>-<hex>:refs/dit-snapshots/<pipeline>/<epoch>-<hex>` after creating the ref.
  - Print a one-liner banner with the four caveats: first-run-MISS, untracked-files-not-captured, unreviewed-code, worker-image-may-not-match.
- Update `make clean-snapshots` to also `git push --delete origin refs/dit-snapshots/...` for each cleaned local ref.
- Tests: smoke that the snapshot ref ends up at the right place locally + remotely; banner appears.

### M-pivot-2 — auto-snapshot inside `make dit-cloud` + `dit run`

- `make dit-cloud` detects a dirty pipeline checkout and runs the snapshot+push automatically before the Cloud Build submit. No user-visible flag for the "happy path" (dirty → just snapshot). Add a `--require-clean` opt-out for users who want the run to error rather than auto-snapshot (CI scripts, etc.).
- Same for local `dit run --runner=dataflow`: detect dirty tree, auto-snapshot, install from the snapshot, proceed.
- Local `dit run --runner=docker` (DirectRunner) keeps working against the working tree as today — it's the inner-loop fast iteration mode, and the snapshot isn't needed (no Cloud Build / no remote workers).
- `--allow-dirty-tree` becomes a no-op with a deprecation warning, then removed in a follow-up PR.

### M-pivot-3 — `unreviewed_code` column replaces `pipeline_dirty`

- Migration: `ALTER TABLE tech_great_expectations.dit_runs ADD COLUMN unreviewed_code BOOL`.
- `_run_with_cache` writes `unreviewed_code=TRUE` when `pipeline_commit` is a snapshot ref (matches `dit-snapshot-` prefix or lives under `refs/dit-snapshots/*`). `FALSE` otherwise.
- `read_cache` default behaviour: returns all rows (`unreviewed_code` is informational). PR-validation queries that want strict provenance filter `WHERE unreviewed_code = FALSE` explicitly.
- Drop the `pipeline_dirty = FALSE` filter from `read_cache`.
- Backfill existing rows: `UPDATE ... SET unreviewed_code = pipeline_dirty` (semantically equivalent for the existing data).
- Drop the `pipeline_dirty` column in a follow-up after one release cycle.
- Update `dit.cache.CachedRun` dataclass: rename `pipeline_dirty` → `unreviewed_code`.

### M-pivot-4 — remove `--allow-dirty-tree` + dead code

- Delete `--allow-dirty-tree` from the argparse blocks in both workflows.
- Delete `dit.git_info.warn_if_worker_image_misses_dirty_tree` + its tests.
- Delete the dirty-tree handling in `_resolve_suffix` (the `_dirty` substring branch).
- Drop the dirty-row tests from `test_pipe_gaps_mode_equivalence.py` and `test_cache.py`.
- Update memories: `[[dit-runs-cache]]` and `[[submitter-vs-worker-split]]` lose their dirty-tree paragraphs; new `[[no-dirty-tree-policy]]` memory documents the new state.

### M-pivot-5 — docs catch-up

- README Features: drop `--allow-dirty-tree` mention; add the auto-snapshot behaviour.
- README Usage Scenarios: new section (drafted alongside this plan; see README diff in this PR).
- `docs/run-cache.md` and `docs/run-cache-impl.md`: update for the `unreviewed_code` rename.
- `CHANGELOG.md`: `Removed` entries for `--allow-dirty-tree`, `warn_if_worker_image_misses_dirty_tree`, `pipeline_dirty` column; `Added` entries for auto-snapshot, `unreviewed_code`.

## Schema changes summary

| Column | Action | Note |
|---|---|---|
| `pipeline_dirty BOOL` | Renamed to `unreviewed_code BOOL` | Sharper semantic; backfilled identically from existing rows |
| (output_tables-suffix `_dirty`) | Removed from suffix construction | Existing rows' suffixes unchanged; only new rows differ |

No drops requiring a destructive migration. The rename is additive (ADD + UPDATE + DROP across releases).

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| **Existing dirty rows in `dit_runs` reference unreviewable code.** | Migration `UPDATE ... SET unreviewed_code = pipeline_dirty` preserves the semantic. PR-validation queries explicitly filter `unreviewed_code = FALSE`. |
| **`refs/dit-snapshots/*` accumulates on origin** if nobody runs `make clean-snapshots`. | Bytes-scale storage; UX is fine (hidden ref namespace). Optional weekly user habit. |
| **Auto-snapshot might surprise users who didn't realise their code is being pushed.** | Loud banner at snapshot time. Auto-snapshot is restricted to `--runner=dataflow` paths (the ones that need Cloud Build / remote workers). DirectRunner stays local-only as today. |
| **`make snapshot-<pipeline>` now requires git-push permission to the pipeline repo.** | Same scope of users who already need GCP AR push; no new permission class. Document the requirement in README. |
| **CI scripts that pass `--allow-dirty-tree` break.** | Deprecation cycle: M-pivot-2 keeps the flag as a no-op with a warning; M-pivot-4 removes it. One release of grace. |
| **Snapshot push + branch-protection rules.** | `refs/dit-snapshots/*` is outside `refs/heads/`; branch protection patterns typically don't apply. Confirm with whoever set up the pipeline-repo's protections. |
| **A user without push access (e.g. read-only viewer) tries to run dit-cloud against uncommitted code.** | Auto-snapshot will fail at push time with a clear error pointing at `make install-<pipeline>-ref REF=<committed-ref>` or to committing the changes first. Acceptable failure mode. |

## Open questions

1. **Auto-snapshot opt-out shape.** `--require-clean` (error if dirty) vs `--no-auto-snapshot` (proceed somehow else) — pick at M-pivot-2 implementation time. I'd argue `--require-clean` is the right name; the failure mode is clearer.
2. **Should `dit run --runner=docker` also auto-snapshot?** No — DirectRunner runs locally against the working tree directly; the snapshot adds zero value. Worth confirming.
3. **Snapshot ref retention policy.** Default = "keep forever, user runs `make clean-snapshots` when they want". Worth revisiting if we discover real friction.
4. **`unreviewed_code` semantics for `make install-<pipeline>-ref REF=<branch>`.** A branch that's a PR head is `unreviewed_code=TRUE`; a merged-to-main commit is `unreviewed_code=FALSE`. How do we tell? Cheap heuristic: `git merge-base --is-ancestor <ref> origin/main`. Implementation detail for M-pivot-3.

## Empirical case study: 2026-05-22 builds 1 and 2

These two `make dit-cloud` runs (experiment_ids `m4-build-1` and `m4-build-2`) ran against the same dirty pipe-gaps tree with the same params. Both wrote `pipeline_dirty=TRUE` rows; `read_cache` correctly filtered them from build 2's lookup; both did the full ~30 min Dataflow workload.

The dirty filter behaved exactly as specified — and that's the problem. The design correctly stopped a dirty row from masquerading as a clean one, but at the cost of forcing build 2 to recompute byte-identical results. Under this pivot, build 2 would have hit the cache and returned in seconds.

The total cost of these two runs was ~60 min × E2_HIGHCPU_8 + ~$10 of Dataflow, almost entirely attributable to the dirty-tree mode existing. The pivot eliminates that class of waste.

## Related

- [[no-dirty-tree-policy]] memory — pinned in the same PR as this plan.
- [`docs/run-cache.md`](run-cache.md) — design that will get the `unreviewed_code` rename in M-pivot-3.
- [`docs/run-cache-impl.md`](run-cache-impl.md) — milestone tracker; same.
- The post-pivot README **§ Usage scenarios** section lives in `README.md` alongside this PR.
