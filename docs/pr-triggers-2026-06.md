# PR-triggered dit runs from pipeline repos — readiness + sequencing plan (2026-06-11)

Planning doc for the next major arc: making dit callable **from pipeline repos during PRs** (today it is only invoked ad-hoc from dit itself — laptop `dit run` / `make dit-cloud`). Consolidates the readiness audit done 2026-06-11 against `docs/plan.md` § Next steps (items 2, 3, 3.5), [`llm-pr-gating.md`](llm-pr-gating.md), the cloud-build architecture decision (plan decision 7), and the post-M6 state of the repo.

**Status (2026-06-11)**: plan filed; no implementation started. The prerequisite track (T1–T4 below) comes first, deliberately — none of it is speculative, each item closes a hole that PR cadence would turn from an annoyance into a standing leak.

## Goal — the two scenarios

From a PR on a pipeline repo (pipe-gaps first, then anchorages_pipeline, then pipe-events):

- **S1 — branch run.** Request a specific dit workflow against the PR's branch. Verdict + TIC results surface on the PR as an expandable GitHub **Check Run** (pass/fail status line; markdown summary carries the full comparison table).
- **S2 — branch vs main (cross-version).** Same, plus the workflow's output on current `main` and the branch-vs-main diff.

Both scenarios assume the **staging cohort defaults** (cheap, frozen source); heavier cohorts come later via the tiered-trigger design (label-gated).

## What is already in place (no work needed)

| Piece | State |
|---|---|
| **Trigger architecture decision** | Locked (plan decision 7 + cloud-build-architecture note): centralized `cloudbuild-dit.yaml` in dit ("how the test runs"); each pipeline repo owns its trigger ("what gets tested when") so status checks land in the right repo's UI and teams customize without touching dit. |
| **`REF=` cloud-path correctness** | Fixed PR #27 + live-revalidated 2026-05-28. Auto-build now builds the worker image from `git archive <commit>` (not the uploaded working tree), so a PR-SHA run executes PR code on submitter AND workers. This was the scariest prerequisite; it is done. |
| **Worker image for unreviewed refs** | M-pivot-4 kaniko auto-build → `gcr.io/world-fishing-827/dit/<pipeline>:dit-<commit>`, content-addressed. A PR branch is unreviewed by definition → auto-build is exactly the right behaviour. |
| **Run cache** | `dit_runs` content-addressed on `pipeline_commit + worker_image_digest + workflow_file_sha1 + params` (params include resolved source FQNs). This is what makes S2 cheap — see below. |
| **PR-gating design** | [`llm-pr-gating.md`](llm-pr-gating.md): path filters + `dit:run` label as the deterministic floor; LLM as negative-signal-only trimmer; two-phase rollout; fail-open. |
| **Concurrency safety** | Per-run UUID suffixes + experiment-ids (no table collisions); refactor-discipline working agreement (additive changes don't break in-flight runs); `_looks_like_table_fqn` cancel guard; snapshot auto-push is N/A for PR runs (committed refs, no dirty tree). |

## Prerequisite track (in order)

### T1 — Run-cache M6: SIGTERM trap in `dit run` (+ M5a cancel live-verification)

The remaining run-cache milestone (`docs/run-cache-impl.md` § M6), deliberately sequenced before triggers. **Why load-bearing here:** every force-push to a PR cancels the in-flight Cloud Build; today a cancelled build orphans its Dataflow jobs (4+ per run) and BQ artifacts. At ad-hoc cadence that is an occasional annoyance; at PR cadence it is a standing money + Dataflow-quota leak. The trap calls `cancel_run` (M5a, landed) on SIGTERM so the build tears down its own jobs/tables.

Gates: **M5a's cancel path is still only mock-tested.** One live verification of `dataflow.jobs.cancel` + BQ table-delete for both the laptop user (`make dit-cancel`) and `automated-testing@` (the cloud SIGTERM path) is required — user-gated (live infra).

### T2 — `--modes` on the three mode-family workflows

From the orchestration evaluation ([`workflow-orchestration-2026-06.md`](workflow-orchestration-2026-06.md), axis 2) — **graduates from nice-to-have to prerequisite**. The PR-run shape is "run the PR's `1_bf`, diff against main's `1_bf`" (cheap smoke), and the tiered-trigger design ("cheap staging bf on every PR; full 3-mode or heavy cohort on label") assumes mode-subset selection. Today `mode_equivalence` / `port_visits/ais` / `pipe_events/fishing` hardcode all modes.

Shape: additive `--modes` defaulting to the full set, mirroring `cross_version_ais`'s existing flag; comparisons run only pairs where both sides exist. Composes with the per-mode cache for free (each mode already has its own cache key). Three parallel small edits.

### T3 — `dit.report` + GitHub Check Run integration

Plan § Next steps item 2, already sketched: a `dit.report.VerdictReport` dataclass (hoisted from `dit.compare`'s return value) + `dit.report.github.post_check_run(report, repo, sha, token)` called from inside the same Python process that ran the comparisons — typed contract end-to-end, no log parsing, no second CI system. Cloud Build env supplies the PR head SHA; **Secret Manager supplies a GitHub App token**.

- **Start the GitHub App / token + Secret Manager ask first** — it is admin/IAM coordination with the longest lead time of anything in this plan.
- **Design point to settle in the report schema: the verdict contract per workflow family** (orchestration evaluation, axis 3). An *equivalence* workflow's diff means the check FAILS; a *cross-version* workflow's diff means the check PASSES with findings ("here is what your PR changes"). The Check Run conclusion (`success`/`failure`/`neutral`) must encode this distinction explicitly — S2's cross-version leg should never block a PR merely for behaving differently from main when behaviour change is the PR's purpose. Writing this contract down doubles as the axis-3 documentation item.

### T4 — `v0.1.0` release tag; flip `_DIT_REF` default to the tag

**Resequencing correction to the existing plan.** `docs/plan.md` / README currently place the first release tag *after* PR triggers land. For outward-facing checks that order is backwards: every triggered run clones dit@`main`, so one broken dit commit fails every pipeline PR across the org. The refactor-discipline working agreement already names tagging as "the natural maturity step that takes `_DIT_REF=main` off the default critical path." **Tag first; point triggers at the tag from day one**; `main` stays opt-in for dit-side testing. (Update plan.md § Next steps item ordering + README § Roadmap when this lands.)

### T5 — Trigger plumbing (the actual feature, small once T1–T4 exist)

1. **`cloudbuild-dit.yaml` trigger variant.** Today `_PIPELINE_COMMIT` / `_UNREVIEWED` are laptop-resolved (the PR #27 fix) and the pipeline source arrives via `--source` upload. A trigger-fired build instead: `/workspace` = the trigger's clone of the pipeline repo at the PR head; `_PIPELINE_COMMIT=$COMMIT_SHA` (trigger built-in); `_UNREVIEWED=true` unconditionally (a PR branch is unreviewed by definition — not merged to origin/main). Small yaml variant or substitution defaults.
2. **Per-pipeline trigger config** in each pipeline repo (Terraform-managed where the team already uses it, e.g. anchorages_pipeline). Path filters (`src/**`, `transforms/**`...) + `dit:run` label escape hatch + `ready_for_review` handling (drafts don't fire). Order: pipe-gaps (most-verified) → anchorages_pipeline → pipe-events.
3. **Main-branch trigger (or nightly)** per pipeline to keep main's staged results warm in the cache — this is what makes S2 cheap (next section).
4. **LLM pre-filter** ([`llm-pr-gating.md`](llm-pr-gating.md)) comes after the deterministic triggers work, in its designed two-phase rollout (log-only audit → enforce after ≥50 PRs with zero false negatives).

### T6 — Fork-PR security gate (NOT previously covered by any plan doc)

A PR-triggered run executes **unreviewed pipeline code under `automated-testing@`**, and the pipeline repos are public. Without protection, anyone who can open a PR can run arbitrary code with the SA's BQ/Dataflow permissions (read access to org data; writes to dit namespaces; Dataflow job submission). Cheap to configure, catastrophic to forget.

**Org survey (2026-06-11)** — how GFW handles this today:

- **Cloud Build (the layer dit will use): 99 triggers in wf827; 98 are push/tag-event** — structurally fork-safe because push events only fire for branches in the upstream repo (write access required; fork pushes land on the fork). dit's PR triggers will be only the second PR-event trigger in the org.
- **The one existing PR-event trigger** (`vessel-viewer-PR-tests`, frontend repo; currently disabled) is configured exactly right and is the in-org precedent to copy: `commentControl: COMMENTS_ENABLED_FOR_EXTERNAL_CONTRIBUTORS_ONLY` + `approvalConfig.approvalRequired: true`.
- **GitHub Actions reference** (`composer-dags-production/staging-deployment.yml`): fork-safe by construction — `pull_request` event (NOT `pull_request_target`), so GitHub withholds secrets AND the OIDC token from fork runs; its Workload-Identity auth step therefore *cannot succeed* from a fork. Plus `deploy-staging` label gating (triage+ only). Residual TOCTOU worth learning from: its `synchronize`-while-labeled clause re-deploys every push after a single labeling with no re-approval — acceptable in a private members-only repo, NOT a pattern to copy for public-repo code execution.
- **Exposure baseline**: `automated-testing@` already runs on any-branch *pushes* of a public repo (`anomaly-detection-dbt-any-branch`), so org members can already execute code as this SA by pushing a branch. dit's same-repo PR triggers add **no new exposure class**; the genuinely new class is **fork PRs** — exactly what comment control addresses.

**Required trigger config (non-negotiable defaults for every dit PR trigger):**

1. `commentControl: COMMENTS_ENABLED_FOR_EXTERNAL_CONTRIBUTORS_ONLY` — collaborator PRs run automatically; fork/external PRs run only after an owner/collaborator comments `/gcbrun`. Crucially this is **per-head**: a new push to the fork PR needs a fresh `/gcbrun`, which structurally avoids the label-once-then-push-malicious TOCTOU above.
2. `approvalConfig.approvalRequired: true` **during the bake-in period** (every build additionally needs a console approval); relax once the trigger behaviour is trusted.
3. Verify with a real fork-PR test before announcing availability; record the config in the trigger-setup runbook (T5).

**Design note for T5's escape hatch**: Cloud Build triggers cannot filter on PR *labels* natively (only branch regex + file-path filters + comment control), so the plan's `dit:run`-label escape hatch needs either an Actions shim or — simpler — adopting `/gcbrun`-style comment-driven manual runs as the escape hatch for everyone, since it is the mechanism Cloud Build already provides. Decide in T5.

## Scenario realization

**S1 (branch run)** = T1–T6 directly: trigger fires on the PR head SHA → ditbox clones dit@tag → installs the pipeline at `$COMMIT_SHA` → auto-builds the worker image from that commit (`git archive`-based, correct since #27) → runs the requested workflow (bf-only via T2 for the cheap tier) → posts the Check Run (T3).

**S2 (branch vs main, cross-version) — cache-based, not orchestrator-based.** Keep main's staged results warm via the main-branch trigger (T5.3); then a PR run computes only the PR leg and diffs against main's cached output table. That IS branch-vs-main:

- No need to generalize the worktree-based `cross_version_*` orchestrators to pipe-gaps/pipe-events — those remain ad-hoc A/B tools for arbitrary ref pairs. The PR case is the special case "B = current main", which the cache already holds.
- Soundness: valid because the staging cohort is frozen and the cache key includes the resolved source FQNs + worker-image digest — "both legs read identical inputs" is verifiable from the cache row, not assumed.
- The main leg is near-free (cache hit); a cold cache (e.g. just after a main merge) degrades gracefully to one extra staged run.
- The Check Run for S2 uses the cross-version verdict contract (T3): diff = findings, not failure.

## Sequencing summary

| # | Item | Size | Gated on |
|---|---|---|---|
| T1 | SIGTERM trap + cancel live-verification | Small-medium + user-gated live check | — |
| T2 | `--modes` on the 3 mode-family workflows | Small | — |
| T3 | `dit.report` + Check Run (+ verdict-contract doc) | Medium | GitHub App token in Secret Manager (admin lead time — **start the ask first**) |
| T4 | `v0.1.0` tag; `_DIT_REF` default → tag | Small | T1–T3 merged (tag a complete surface) |
| T5 | Trigger yaml variant + per-pipeline triggers + main-warmer | Small-medium per pipeline | T1–T4 |
| T6 | Fork-PR approval policy | Config + runbook | with T5, before announcing |

T1 and T2 are independent and can run in parallel; the T3 admin ask should be fired off immediately regardless.

## Known ceilings / non-blockers

- **Cloud Build 30-concurrent-build cap** — flagged in README § Roadmap; becomes real once per-PR matrix testing scales. Cloud Run jobs is the designated migration target; not needed for the first triggers.
- **dit.cohorts, `_detect.py` dedup** (orchestration-evaluation backlog) — orthogonal; not blocking this arc.
- **Track 5 / pipe-events bash shims** (pipeline repos pointing their old integration scripts at dit) — adjacent pipeline-repo hygiene, opportunistic.
- **Phase 6 "phase sharing" / Phase 7 "golden tables"** — largely subsumed by the run cache; the S2 cache-based design is exactly the Phase 7 idea realized with existing machinery.

## Acceptance criteria (when this arc is complete)

- [ ] A force-pushed/cancelled PR build leaves zero running Dataflow jobs and no orphaned output tables (T1, verified live).
- [ ] `--modes 1_bf` works on all three mode-family workflows; PR tier runs bf-only by default (T2).
- [ ] A dit run posts a Check Run with pass/fail + expandable TIC results table; equivalence vs cross-version verdict semantics are documented and encoded in the check conclusion (T3).
- [ ] Triggers reference `_DIT_REF=v0.X.Y`, not `main` (T4).
- [ ] A pipe-gaps PR with `dit:run` (or matching path filter) gets an S1 check; with main's cache warm, an S2 branch-vs-main diff (T5).
- [ ] Fork PRs cannot execute without explicit approval (T6, verified before announcement).
