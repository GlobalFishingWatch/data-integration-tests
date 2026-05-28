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
- **`--allow-dirty-tree` is removed.** A dirty tree at submit time → dit auto-snapshots, auto-pushes to `refs/dit-snapshots/<pipeline>/<commit-short-sha>` (content-addressable — see § "Deterministic snapshots" below), and uses that ref. The user doesn't need to think about snapshots; the workflow handles it.
- **`pipeline_dirty` column drops from the cache.** Replaced by a sharper-semantic `unreviewed_code` BOOL: `TRUE` for snapshot refs and ad-hoc branches, `FALSE` for merged-into-main commits. This carries the actual semantic the dirty-tree filter was a proxy for ("don't trust this row for cross-pipeline / PR-validation purposes") without conflating it with git state.
- **`dit.git_info.warn_if_worker_image_misses_dirty_tree` is removed.** No dirty trees possible; no warning needed. The submitter-vs-worker memory's content stays relevant (it's about worker-image staleness, not git state) but the warn helper is gone.
- **`_dirty` suffix gone from output table names.** Output suffix becomes `<experiment_id>_<commit>_<uuid>` — every byte traceable to a real git ref.
- **`make snapshot-<pipeline>` auto-pushes** to the `refs/dit-snapshots/*` namespace.
- **`make clean-snapshots` (broad) replaced by `make clean-snapshot REF=<sha>` (surgical).** Snapshots live forever by design — bytes-scale storage in a hidden namespace, no measurable cost. The surgical target exists only for secret-leak remediation; the broad sweep is dropped.
- **The User experiences zero extra ceremony** for the iterative-development path (today's two-command friction goes away because dit handles the snapshot automatically). They gain reproducibility and cache hits on repeat runs of the same uncommitted code — see § "Deterministic snapshots" below for why this requires more than `git stash create`.

## Deterministic snapshots (required for the cache-hit story)

`git stash create` writes a new commit every invocation: the tree may be identical, but the committer timestamp changes, so the commit SHA changes. Under the run-cache key `sha256(pipeline_commit + worker_image_digest + workflow_file_sha1 + canonical_params_json)`, two stash commits of an unchanged working tree would hash to different keys → MISS → byte-identical recompute. That defeats the motivating "two identical dirty builds" scenario from § Why.

Fix: snapshot creation in M-pivot-1 / M-pivot-2 derives a **deterministic commit** from the working-tree content. The trickiest part is capturing the dirty working tree into a tree SHA *without polluting the user's index*. The reference implementation:

```bash
# 1. Build a temp index seeded from HEAD, stage tracked-file modifications
#    against it (same logic `git stash create` uses internally; isolating
#    via GIT_INDEX_FILE keeps the user's real index untouched).
TMP_INDEX=$(mktemp)
trap 'rm -f "$TMP_INDEX"' EXIT
GIT_INDEX_FILE="$TMP_INDEX" git read-tree HEAD
GIT_INDEX_FILE="$TMP_INDEX" git add -u
TREE_SHA=$(GIT_INDEX_FILE="$TMP_INDEX" git write-tree)

# 2. Build an ORPHAN commit (no `-p`) with frozen author/committer
#    identities so the commit SHA is purely a function of the tree.
#    Record the original HEAD in the commit message — it's the only
#    place the parent context is preserved when the commit itself is
#    orphan.
PARENT_SHA=$(git rev-parse HEAD)
SHA=$(GIT_AUTHOR_DATE="1970-01-01T00:00:00Z" \
      GIT_COMMITTER_DATE="1970-01-01T00:00:00Z" \
      GIT_AUTHOR_NAME=dit GIT_AUTHOR_EMAIL=dit@local \
      GIT_COMMITTER_NAME=dit GIT_COMMITTER_EMAIL=dit@local \
      git commit-tree -m "dit snapshot of $PARENT_SHA" "$TREE_SHA")

# 3. Idempotency: skip the push if the ref already resolves on origin.
REF="refs/dit-snapshots/<pipeline>/${SHA:0:12}"  # 12 chars > git default 7
if git ls-remote --exit-code origin "$REF" >/dev/null 2>&1; then
    echo "snapshot already present on origin: $REF"
else
    git update-ref "$REF" "$SHA"
    git push origin "$REF:$REF"
fi
```

Why each piece matters:
- **Temp index** (`GIT_INDEX_FILE=$(mktemp) + git read-tree HEAD + git add -u`): this is the equivalent of what `git stash create` does internally. A naïve `git write-tree` against the user's real index would either pollute it or write HEAD's tree (depending on whether the user had pre-staged changes). The temp index lets us capture the dirty working tree without side-effects on the user's repo state.
- **Frozen author/committer dates AND identities** (epoch 0; `dit` / `dit@local`): commits include author *and* committer (name, email, date) in their SHA. Both must be deterministic. Without freezing the identity too, two different users' snapshots of the same tree would have different SHAs.
- **Orphan commit (no `-p`)**: snapshot SHA is purely a function of the tree, not the user's branch history. Rebasing the user's branch doesn't invalidate the cache; same dirty tree from a different starting point produces the same snapshot SHA → cache hit. Side benefit: `git push` only transfers the snapshot commit + tree blobs; no unpushed HEAD ancestors leak to origin via reachability.
- **Parent SHA recorded in the commit message** (`dit snapshot of <parent-sha>`): preserves the reproduce context that an orphan commit otherwise loses. Anyone with the snapshot can `git show <snapshot>` to learn which committed ref the user's dirty tree was on top of. Same info also lands in the cache table — see § "Cache schema: parent SHA".
- **12-char SHA prefix**: collision-resistant for our scale (millions of snapshots needed before birthday-paradox concern) while keeping ref names readable. Bumping later is a one-line change in M-pivot-1.

The `<epoch>-<hex>` ref-naming scheme in earlier drafts of this doc / the policy memory is superseded by `<commit-short-sha>` for the content-addressable property. Epoch was an attempt to ensure uniqueness; tree-content hashing achieves uniqueness AND idempotency, which is what M-pivot-2 actually needs.

Only tracked files are captured (`git add -u` against the temp index only updates entries already in HEAD). This is **the deliberate safety default** — see § Safety below.

## Safety: auto-push, public origins, and credential leakage

The snapshot mechanism pushes to the pipeline repo's `origin` **automatically and unprompted**. Pipeline repos may be — or may become — public GitHub repositories. The safety story is built around that assumption:

- **Default capture is `add -u` (tracked files only).** Modifications + deletions to files already in HEAD go into the snapshot. Untracked files do NOT, even if the user has `git add`-ed them into their real index (the temp index is seeded from HEAD, so `add -u` only touches what's already tracked). This confines the snapshot's content to files the user has explicitly chosen to track + commit at least once.
- **The convenience alternative `add -A` was explicitly rejected.** It would silently capture any rogue `.env`, `sa.json`, downloaded test dataset, `.envrc`, debug log, or one-off artifact in the working tree and push it to origin. The cost of the chosen default (silent drop of new files the user might have wanted included) surfaces immediately as a wrong/failed Dataflow run; credential leaks may not surface for weeks or never. Louder failure mode wins.
- **The snapshot banner shows the user which paths are about to be pushed** before the push happens (a `git diff --name-only HEAD` listing). Visual review is the last-line-of-defence safety check: a tracked credential file or a surprise modification surfaces here, with Ctrl-C still available.
- **Already-tracked secrets must be untracked first.** If a credentials file was once committed by mistake, `git add -u` will include any subsequent modification. Remediation: `git rm --cached <file>` + add to `.gitignore` + commit the removal.
- **Post-leak remediation is `make clean-snapshot REF=<sha>` + rotate the credential.** The former removes the ref locally and on origin; the latter is the load-bearing step (anything ever pushed to a public repo must be treated as compromised even after the ref is gone — git history snapshots, forks, mirrors, indexers, etc. all make "untoward push to public origin" effectively permanent).
- **Pre-push secret scanner (gitleaks, pinned v8.30.1).** Before the push, the script extracts the snapshot tree to a temp dir and runs `gitleaks detect --no-git --redact --exit-code 1`. Any finding aborts the snapshot before either the commit object or the push are created. Defense-in-depth on top of `git add -u` for the case where a tracked file contains a newly-introduced credential the user didn't notice. Gitleaks is pre-baked into ditbox; for local-dev installs it's a single binary (one-line install instructions in the snapshot-error message). Override is `export DIT_SKIP_SECRET_SCAN=1`, which emits a loud `WARNING: secret scan BYPASSED` banner that's visible both in the user's terminal and in CI logs — quiet bypass would defeat the safety story. If gitleaks isn't on PATH and no bypass is set, the snapshot **refuses to proceed** (a snapshot push without a scan is exactly the failure mode this defends against).

## Cache schema: parent SHA

The orphan snapshot loses commit-graph context, so we mirror the parent SHA in two places:

1. **Commit message of the snapshot itself**: `dit snapshot of <40-char-parent-sha>`. Anyone with the snapshot ref can `git show <snapshot> --no-patch` to learn the context.
2. **A new column on `tech_great_expectations.dit_runs`**: `pipeline_commit_parent STRING` — populated for snapshot rows (NULL for non-snapshot runs). Lets queries reconstruct "which committed ref did this dirty-tree run sit on top of" without a git checkout.

Both write paths are no-cost (parent SHA is already known at snapshot creation time). Reads pick whichever is convenient: the commit-message form survives even if the cache table is dropped; the column form survives even if the snapshot ref is later deleted (e.g., via `make clean-snapshot REF=<sha>` for secret remediation — see § Cleanup below).

## Migration plan

Each milestone is a separate PR. Ordering matters; earlier PRs can land independently.

### M-pivot-1 — `refs/dit-snapshots/*` namespace + auto-push in `make snapshot-<pipeline>`

- Update `scripts/snapshot-install.sh` (and `make snapshot-<pipeline>`) to:
  - Build a **deterministic** snapshot commit from the working tree (`git write-tree` + `git commit-tree` with frozen dates — see § "Deterministic snapshots" above). Identical tree → identical commit SHA → idempotent.
  - Create the snapshot ref under `refs/dit-snapshots/<pipeline>/<commit-short-sha>` instead of `refs/heads/dit-snapshot-<epoch>`.
  - Skip the push if `git ls-remote origin refs/dit-snapshots/<pipeline>/<sha>` already resolves (the ref is content-addressable, so a re-push would be a no-op).
  - Otherwise `git push origin refs/dit-snapshots/<pipeline>/<sha>:refs/dit-snapshots/<pipeline>/<sha>`.
  - Print a one-liner banner with the caveats: untracked-files-not-captured, unreviewed-code, worker-image-may-not-match, requires-push-permission-on-pipeline-repo.
- **Replace** the existing broad `make clean-snapshots` with a **surgical** `make clean-snapshot REF=<sha>` target — deletes the specified snapshot ref locally and on origin, intended for secret-leak remediation only (see § Cleanup). The broad sweep is dropped; snapshots live forever by design (bytes-scale storage, hidden namespace).
- Tests: smoke that (a) the snapshot ref ends up at the right place locally + remotely, (b) two invocations against an unchanged tree produce the same SHA / skip the second push, (c) banner appears, (d) `make clean-snapshot REF=<sha>` removes the ref locally and on origin.

### M-pivot-2 — auto-snapshot inside `make dit-cloud` + `dit run` ✅ LANDED

- `make dit-cloud` detects a dirty pipeline checkout and runs the snapshot+push automatically **on the laptop** (where git-push creds live) before the Cloud Build submit, then threads the resolved snapshot commit into the build as `_PIPELINE_COMMIT` → `DIT_PIPELINE_COMMIT` env var. The workflow records that as `pipeline_commit`. `REQUIRE_CLEAN=1` opts out (errors on dirty). The build still uploads the (byte-identical) working tree as its source — `_PIPELINE_COMMIT` only changes what's *recorded*, not what's installed.
- Local `dit run --runner=dataflow`: `dit.snapshot.resolve_pipeline_commit` detects a dirty tree, auto-snapshots + pushes in-process, records the snapshot commit. `--require-clean` opts out.
- Local `dit run --runner=docker` keeps running against the working tree directly (no remote workers → no snapshot needed); recorded as an unreviewed run for provenance.
- `--allow-dirty-tree` is a deprecated no-op (logs a warning); removed in M-pivot-4. `--suffix` (manual / cross-version) bypasses auto-snapshot and records git state as-is — `cross_version_ais.py` relies on this for its committed worktree refs (and no longer passes `--allow-dirty-tree`).
- **Implementation note:** auto-snapshot requires an editable dit install so `scripts/snapshot.sh` is locatable. Not a real limitation — only editable installs can be dirty; `-ref`/snapshot installs are already committed (clean).

### M-pivot-3 — `unreviewed_code` + `pipeline_commit_parent` columns replace `pipeline_dirty` ✅ LANDED

Shipped as implemented (one deviation from the sketch below): the
`unreviewed_code` value is the `unreviewed` flag already resolved by
`dit.snapshot.resolve_pipeline_commit` in M-pivot-2 (snapshot / dirty / env-override → True; clean → False). The `git merge-base --is-ancestor origin/main`
refinement for "clean-but-not-on-main" branches is **deferred** — it adds a
`git fetch` round-trip per run and is awkward on the cloud env-override path,
and the column is now informational (no longer gates caching), so the
approximation is acceptable. Revisit if strict-provenance queries need the
precision. Migration in `migrations/002_unreviewed_code.sql`; the cacheability
win comes from dropping the read filter (below).

- Migration:
  ```sql
  ALTER TABLE tech_great_expectations.dit_runs ADD COLUMN unreviewed_code BOOL;
  ALTER TABLE tech_great_expectations.dit_runs ADD COLUMN pipeline_commit_parent STRING;
  ```
- `_run_with_cache` writes:
  - `unreviewed_code` — **as shipped**, this is the M-pivot-2 resolved flag: `TRUE` for snapshot refs / dirty trees / `DIT_PIPELINE_COMMIT` runs, `FALSE` for a clean checkout of any branch. *(Sketch, deferred: the sharper `TRUE` unless `git merge-base --is-ancestor <commit> origin/main` — after a `git fetch origin main` — would also flag clean-but-not-on-main branches. Deferred because it costs a per-run `git fetch`, is awkward on the cloud env-override path, and the column is informational now that it no longer gates caching. Until then, strict-provenance queries should read `FALSE` as "clean checkout", not "on main".)*
  - `pipeline_commit_parent`: the SHA the snapshot was based on (extracted from the snapshot commit message — pattern `dit snapshot of <40-char-sha>`, validated as 40-char hex). NULL for non-snapshot rows.
- `read_cache` default behaviour: returns all rows (`unreviewed_code` is informational). PR-validation queries that want strict provenance filter `WHERE unreviewed_code = FALSE` explicitly.
- Drop the `pipeline_dirty = FALSE` filter from `read_cache`.
- Backfill existing rows: `UPDATE ... SET unreviewed_code = pipeline_dirty` (semantically equivalent for the existing data). `pipeline_commit_parent` stays NULL for backfilled rows — pre-pivot snapshots used `git stash create` against the user's branch tip, so the parent info isn't structurally available.
- Drop the `pipeline_dirty` column in a follow-up after one release cycle.
- Update `dit.cache.CachedRun` dataclass: rename `pipeline_dirty` → `unreviewed_code`; add `pipeline_commit_parent: str | None = None`.

### M-pivot-4 — auto-build worker image + remove dead code ✅ LANDED

Scope grew during implementation: rather than just *delete* the worker-image-staleness warning, we **close the gap it guarded** by auto-building the worker image. (Discussion: the snapshot makes submitter code reproducible but workers still load from `--worker-image` — a separate container-registry artifact the snapshot never touches. So unreviewed code + default image = workers silently run stale code, regardless of git state. Deleting the warning without a replacement would lose a real, load-bearing signal.)

**Auto-build (new `dit.worker_image.ensure_worker_image`):** when a `--runner=dataflow` run executes unreviewed code (`unreviewed=True`) against the *default* worker image, build a content-addressable worker image from the pipeline source and use it; otherwise return `--worker-image` unchanged (no-op for reviewed code, an explicit override, or the docker runner).
- Build via kaniko Cloud Build (`docker/worker-image/cloudbuild.yaml`, same pinned executor + shared `kaniko-cache` repo as ditbox). Tag `gcr.io/world-fishing-827/dit/<pipeline>:dit-<pipeline_commit>` — content-addressable, so **idempotent**: an existing tag skips the build.
- Called from each workflow's `main()`, so **one mechanism covers both entry points**: `make dit-cloud` (workflow runs inside ditbox → a *nested* Cloud Build, kept fast by the shared cache) and local `dit run --runner=dataflow` (submits from the laptop). Done before the worker-image digest/label is derived so the cache key reflects the image actually used.
- **The nested-build path was validated empirically (2026-05-28).** An open question was whether `automated-testing@` (the SA the dit-cloud build runs as) could submit the nested worker-image build that specifies `automated-testing@` as its own service account — i.e. whether it has `iam.serviceAccounts.actAs` on itself. It is NOT in the project `roles/iam.serviceAccountUser` binding, but it IS in the project `roles/iam.serviceAccountTokenCreator` binding. A throwaway test (an outer build running as `automated-testing@` submitting an inner build specifying `automated-testing@`) **succeeded**, so the nested submit works with existing permissions — no IAM change required. (We considered an in-build kaniko `:debug` step and a laptop-submitted build to avoid the actAs requirement; both became unnecessary once the requirement turned out to be already satisfied. The nested design is the cleanest — the workflow decides *and* builds in one place for both paths.)
- Prerequisite: `pipe-gaps`'s Dockerfile is mis-layered for caching (source copied before the deps install), so source-only rebuilds bust the deps layer. A separate PR in the pipe-gaps repo reorders it (deps before `COPY src`) — `anchorages_pipeline` is already correctly layered. Auto-build *works* without the reorder, just slower (~1-2 min vs seconds) on source changes.

**Dead-code removal (the original M-pivot-4 scope):**
- Deleted `--allow-dirty-tree` from both workflows' argparse + the deprecation blocks.
- Deleted `dit.git_info.warn_if_worker_image_misses_dirty_tree` + its only test file (`tests/test_git_info.py`); `git_info` stays.
- Dropped the `_dirty` substring from `_resolve_suffix` in both workflows (suffix is always `<experiment_id>_<commit>_<uuid>`).
- `pipeline_dirty` column + dual-write **kept** (deferred drop — avoids the NOT-NULL-column migration/code coupling window; see the M-pivot-3 note).
- Memories updated (`[[dit-runs-cache]]`, `[[submitter-vs-worker-split]]`, `[[no-dirty-tree-policy]]`).

### M-pivot-5 — docs catch-up

- README Features: drop `--allow-dirty-tree` mention; add the auto-snapshot behaviour.
- README Usage Scenarios: new section (drafted alongside this plan; see README diff in this PR).
- `docs/run-cache.md` and `docs/run-cache-impl.md`: update for the `unreviewed_code` rename.
- `CHANGELOG.md`: `Removed` entries for `--allow-dirty-tree`, `warn_if_worker_image_misses_dirty_tree`, `pipeline_dirty` column; `Added` entries for auto-snapshot, `unreviewed_code`.

## Schema changes summary

| Column | Action | Note |
|---|---|---|
| `pipeline_dirty BOOL` | Renamed to `unreviewed_code BOOL` | Sharper semantic; backfilled identically from existing rows |
| `pipeline_commit_parent STRING` | **New, nullable** | Mirrors the parent SHA recorded in each orphan snapshot's commit message; NULL for non-snapshot rows |
| (output_tables-suffix `_dirty`) | Removed from suffix construction | Existing rows' suffixes unchanged; only new rows differ |

No drops requiring a destructive migration. The rename is additive (ADD + UPDATE + DROP across releases).

## Cleanup

**Snapshots live forever by design.** Bytes-scale storage on origin, hidden ref namespace (invisible to GitHub's UI), no measurable performance impact on `git ls-remote` at our scale. There is no periodic-cleanup burden and no cron.

The one case where removal is needed: **a secret accidentally lands in a snapshot.** For that, `make clean-snapshot REF=<sha>` deletes the specified ref locally (`git update-ref -d`) and on origin (`git push --delete`). Surgical, user-invoked, well-suited to "I just ran dit-cloud against a tree with a .env file in it, get it off origin now."

The previous broad-sweep `make clean-snapshots` target is dropped in M-pivot-1. Anyone with the muscle-memory will get a clear redirect message pointing at the surgical variant.

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| **Existing dirty rows in `dit_runs` reference unreviewable code.** | Migration `UPDATE ... SET unreviewed_code = pipeline_dirty` preserves the semantic. PR-validation queries explicitly filter `unreviewed_code = FALSE`. |
| **`refs/dit-snapshots/*` accumulates on origin.** | Accepted by design. Bytes-scale storage in a hidden ref namespace; no periodic cleanup. `make clean-snapshot REF=<sha>` covers the one case that matters (secret-leak remediation). |
| **Auto-snapshot might surprise users who didn't realise their code is being pushed.** | Loud banner at snapshot time. Auto-snapshot is restricted to `--runner=dataflow` paths (the ones that need Cloud Build / remote workers). The docker runner (`--runner=docker`) stays local-only as today — its container reads from the locally-mounted source, no remote ref needed. |
| **`make snapshot-<pipeline>` now requires git-push permission to the pipeline repo.** | Same scope of users who already need GCP AR push; no new permission class. Document the requirement in README. |
| **CI scripts that pass `--allow-dirty-tree` break.** | Deprecation cycle: M-pivot-2 keeps the flag as a no-op with a warning; M-pivot-4 removes it. One release of grace. |
| **Snapshot push + branch-protection rules.** | `refs/dit-snapshots/*` is outside `refs/heads/`; branch protection patterns typically don't apply. Confirm with whoever set up the pipeline-repo's protections. |
| **A user without push access (e.g. read-only viewer) tries to run dit-cloud against uncommitted code.** | Auto-snapshot will fail at push time with a clear error pointing at `make install-<pipeline>-ref REF=<committed-ref>` or to committing the changes first. Acceptable failure mode. |
| **Secret accidentally lands in a snapshot and gets pushed to origin.** | `make clean-snapshot REF=<sha>` deletes the ref locally and on origin in one step. Surgical, user-invoked, documented in § Cleanup. (Note: a separate `pipeline_commit_parent` column on `dit_runs` preserves the reproduce context independently, so removing the snapshot ref doesn't orphan the cache row.) |

## Open questions

1. **Auto-snapshot opt-out shape.** `--require-clean` (error if dirty) vs `--no-auto-snapshot` (proceed somehow else) — pick at M-pivot-2 implementation time. I'd argue `--require-clean` is the right name; the failure mode is clearer.
2. **Should `dit run --runner=docker` also auto-snapshot?** No — the docker runner executes the pipeline image locally inside a container reading from the mounted source; no remote workers, no remote ref needed; the snapshot adds zero value. Worth confirming.
3. **Snapshot ref retention policy.** Settled: keep forever by design. Bytes-scale storage in a hidden namespace; no measurable performance impact. `make clean-snapshot REF=<sha>` exists for secret-leak remediation only. Worth revisiting only if we discover concrete friction.
4. **`unreviewed_code` semantics for `make install-<pipeline>-ref REF=<branch>`.** A branch that's a PR head is `unreviewed_code=TRUE`; a merged-to-main commit is `unreviewed_code=FALSE`. How do we tell? Cheap heuristic: `git merge-base --is-ancestor <ref> origin/main`. Implementation detail for M-pivot-3.

## Empirical case study: 2026-05-22 builds 1 and 2

These two `make dit-cloud` runs (experiment_ids `m4-build-1` and `m4-build-2`) ran against the same dirty pipe-gaps tree with the same params. Both wrote `pipeline_dirty=TRUE` rows; `read_cache` correctly filtered them from build 2's lookup; both did the full ~30 min Dataflow workload.

The dirty filter behaved exactly as specified — and that's the problem. The design correctly stopped a dirty row from masquerading as a clean one, but at the cost of forcing build 2 to recompute byte-identical results. Under this pivot, build 2 would have hit the cache and returned in seconds.

The total cost of these two runs was ~60 min × E2_HIGHCPU_8 + ~$10 of Dataflow, almost entirely attributable to the dirty-tree mode existing. The pivot eliminates that class of waste.

## Related

- [`docs/run-cache.md`](run-cache.md) — design that will get the `unreviewed_code` rename in M-pivot-3.
- [`docs/run-cache-impl.md`](run-cache-impl.md) — milestone tracker; same.
- The post-pivot README **§ Usage scenarios** section lives in [`README.md`](../README.md) alongside this plan.
- A Claude Code session memory pin (`no-dirty-tree-policy`, in `.claude/.../memory/`) carries the policy across assistant sessions; not user-facing — the canonical sources are this doc + the README sections referenced above.
