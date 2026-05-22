# LLM PR-relevance pre-filter

**Status:** design sketch, not implemented. Owner: dit. Drafted 2026-05-22.

## Purpose

Skip dit's Cloud Build trigger on PRs whose diff is **obviously not behaviour-relevant** (docstring-only, comment-only, type-hint-only, test-fixture-only, formatting-only). The deterministic path-filter + label gate keeps the floor; the LLM is a second-line trimmer that runs *after* path filters match.

Goal: reduce trigger volume on cheap PRs without ever skipping a behaviour-relevant change. False negatives must remain zero.

## Why an LLM at all

Path filters (`includedFiles: ["src/**", "transforms/**"]`) are coarse: a docstring-only edit inside `src/**` triggers dit. The cost is ~$0.50–$5 of Dataflow time + ~30–60 min of wall clock per false-positive trigger. At even a modest PR cadence the wasted spend dwarfs an LLM call by 3–4 orders of magnitude.

But: **the LLM must never be the binary gate.** See § Asymmetric gate below.

## Asymmetric gate

CI gating has asymmetric error costs:

| Error | Cost | Recovery |
|---|---|---|
| **False negative** (LLM says "skip", change IS behaviour-relevant) | Regression lands; comparison run we'd have had is gone | Hard — find the regression downstream, by which point the wrong code is on `main` |
| **False positive** (LLM says "trigger", change is trivial) | One wasted dit run (~$0.50–$5, ~30–60 min) | Easy — costs money, not correctness |

→ **The LLM is a negative-signal-only filter.** It can downgrade `trigger → skip` only when the diff is unambiguously safe. It can never upgrade `skip → trigger` — that's the human's job (via the `dit:run` label).

This means:

```
final_decision = (path_filter_match OR label_match) AND NOT llm_says_safe_skip
```

Path filter and label are the source of truth. LLM is a refinement on the trigger side.

## Architecture

GHA workflow in **each pipeline repo**, fired on `pull_request`. Calls GitHub Models in-platform; gates the Cloud Build trigger downstream.

```yaml
# .github/workflows/dit-pr-gate.yml in pipe-gaps / anchorages_pipeline / pipe-events
name: dit / PR relevance gate
on:
  pull_request:
    types: [opened, synchronize, reopened, ready_for_review]
    paths:
      - 'src/**'
      - 'transforms/**'
permissions:
  pull-requests: read
  contents: read
  checks: write
  models: read
jobs:
  triage:
    if: github.event.pull_request.draft == false
    runs-on: ubuntu-latest
    outputs:
      should_run: ${{ steps.decide.outputs.should_run }}
      reasoning: ${{ steps.ai.outputs.reasoning }}
    steps:
      - uses: actions/checkout@v4
        with: { fetch-depth: 0 }
      - name: Compute diff
        run: |
          git diff --unified=3 origin/${{ github.base_ref }}...HEAD > /tmp/diff.txt
          # Hard cap to keep token cost bounded; cap-busting diffs fail-open
          head -c 200000 /tmp/diff.txt > /tmp/diff-capped.txt
          [ -s /tmp/diff.txt ] && [ ! -s /tmp/diff-capped.txt ] && echo "OVERFLOW" >> /tmp/diff-capped.txt
      - id: ai
        uses: actions/ai-inference@v1
        with:
          model: openai/gpt-4o-mini
          prompt: |
            You are a CI gate. Determine whether this PR's diff is OBVIOUSLY safe
            to skip integration tests for. Return strict JSON only:

            {"safe_to_skip": <bool>, "confidence": <0-1>, "reasoning": "<one sentence>"}

            safe_to_skip = TRUE only when ALL of the following hold:
            - No changes to .py files OUTSIDE tests/ that affect runtime control flow
              (function bodies, class methods, module-level statements).
            - Only allowed change types: docstrings, comments, type hints (PEP-484),
              README/docs/markdown, test files, formatting (whitespace, import order),
              dependency-version-only changes in requirements.txt or pyproject.toml
              that DON'T touch a pipeline package.
            - No changes to SQL, BigQuery schema, Dockerfile, Beam pipeline graph
              construction, Dataflow params, or composer-dags hand-offs.

            When in doubt, return safe_to_skip=FALSE. The cost of false-positive
            triggering (wasted Dataflow $) is far less than false-negative skipping
            (regression lands).

            Diff:
            ---
            ${{ env.DIFF }}
        env:
          DIFF: ${{ steps.diff.outputs.content }}
      - id: decide
        run: |
          response='${{ steps.ai.outputs.response }}'
          safe=$(echo "$response" | jq -r '.safe_to_skip // false')
          reasoning=$(echo "$response" | jq -r '.reasoning // "<no reasoning>"')
          if [ "$safe" = "true" ]; then
            echo "should_run=false" >> $GITHUB_OUTPUT
          else
            echo "should_run=true" >> $GITHUB_OUTPUT
          fi
          echo "reasoning=$reasoning" >> $GITHUB_OUTPUT
      - name: Post Check Run
        # Always post -- in log-only mode this is the only output;
        # in enforce mode it explains the skip decision.
        run: |
          gh api repos/${{ github.repository }}/check-runs -X POST \
            -F name='dit/pr-relevance' \
            -F head_sha=${{ github.event.pull_request.head.sha }} \
            -F status=completed \
            -F conclusion=neutral \
            -F 'output[title]=should_run=${{ steps.decide.outputs.should_run }}' \
            -F 'output[summary]=${{ steps.decide.outputs.reasoning }}'

  dit-cloud:
    needs: triage
    if: needs.triage.outputs.should_run == 'true' || contains(github.event.pull_request.labels.*.name, 'dit:run')
    runs-on: ubuntu-latest
    steps:
      - uses: google-github-actions/auth@v2
        with: { workload_identity_provider: ..., service_account: ... }
      - run: gcloud builds triggers run dit-pr-trigger ...
```

Key points:

- **Fail-open at every step.** If the AI step errors, `should_run` stays `true`. If JSON parsing fails, `should_run` stays `true`. If the diff exceeds the size cap, `should_run` stays `true`. Every error path defaults to running dit.
- **Label overrides everywhere.** `dit:run` forces the trigger regardless of LLM output. `dit:skip` is intentionally NOT supported — humans escalate to run, never to skip.
- **Draft PRs skip the gate entirely** (`if: github.event.pull_request.draft == false`). Saves cost and noise during work-in-progress.
- **The Check Run posts in both modes** (log-only and enforce). In log-only it's the only effect; in enforce it explains the skip.

## Rollout protocol

**Phase A — log-only (two weeks minimum, longer if PR volume is low).**

- LLM runs and posts the `dit/pr-relevance` Check Run.
- `should_run=false` is recorded but **does NOT gate the dit-cloud job**; dit-cloud always runs.
- After every dit run, we compare:
  - `pr-relevance` verdict (would-skip vs would-run)
  - Actual dit verdict (passing vs failing/diffs)
- A `safe_to_skip=true` PR where dit later produced diffs is a **false negative** — a prompt failure that would have been a regression in enforce mode.

Rollout exit criteria: **zero false negatives over N PRs** (suggest N=50, or 14 days, whichever later). If any false negative occurs, abandon and refine the prompt.

**Phase B — enforce (after Phase A passes).**

- Flip the dit-cloud job's `if:` to honour `should_run=false`.
- Continue posting the Check Run; continue collecting verdicts.
- A drift-detector (Cloud Function on the `cloud.build` Pub/Sub topic) flags any `dit:run`-label-forced PR where dit ended up passing identical — i.e., the LLM would have correctly skipped if humans hadn't overridden. Doesn't change behaviour; just signals the LLM's running-accuracy.

**Phase C — escalate model quality (only if Phase B's quality is insufficient).**

- Bump from `gpt-4o-mini` to `gpt-4o` (or `Claude Sonnet 4` via Claude Code Action).
- Cost goes from ~$0.001/PR to ~$0.05/PR — still negligible against Dataflow.

## Audit / observability

Two queries the operator should be able to run any day:

1. **False-negative scan** — `WHERE pr_relevance.should_run = 'false' AND dit.diff_rows > 0`. Returns the cases where the LLM said skip but dit found diffs. Should always be empty in Phase B+. Implemented as a saved BQ query joining `dit_meta.runs` (see [`run-cache.md`](run-cache.md)) with a new `dit_meta.pr_gate_decisions` table or with GHA Check Run history pulled via the API.
2. **Skip rate over time** — how often did the LLM say "safe to skip"? Should track PR-content drift; sudden spike or drop is worth investigating.

Both queries belong in a `scripts/dit-pr-gate-audit.sh` (or a small Looker dashboard) that anyone can run.

## Cost / latency

- **Latency**: 3–8 s per PR-event, including diff compute + LLM call + Check Run post.
- **Cost at `gpt-4o-mini`**: ~5K input + ~200 output tokens → ~$0.001 per PR.
- **At current GFW PR volume**: under $5/month. Well below the cost of a single skipped wasted Dataflow run.
- **Rate limits**: GitHub Models' free tier is per-user-per-model-per-day. The pre-filter consumes one request per PR event. Paid tier (`UserByModelByDay` ceiling raised) covers high-volume cases without per-call billing changes.

## What can break this design

- **Adversarial prompt injection** via PR description / commit messages → mitigated by feeding only the **diff** to the LLM, not free-form text. Diffs can still contain prompt-injection attempts, so add `safe_to_skip=FALSE` as the default for any diff that looks like it's trying to influence the output.
- **Large diffs that exceed the token budget** → caps + fail-open. Worst case is a wasted dit run.
- **GitHub Models outage / rate limit** → fail-open. dit runs.
- **Drift in pipeline conventions** (e.g., a new pipeline uses a non-`src/**` layout) → path filter needs updating; LLM rule update is a prompt-only change, no schema migration.

## Explicit non-goals

- **LLM doesn't override `dit:run` label.** Ever. Human-labelled `dit:run` always triggers.
- **LLM doesn't decide which workflow to run.** That's the pipeline-repo trigger config's job. The LLM only decides whether the configured workflow fires at all.
- **LLM doesn't do failure triage in this design.** Separate concern; covered in a future doc (LLM reads `dit.report` JSON, suggests a likely cause). Sharing the `actions/ai-inference` plumbing is fine; sharing the policy is not.

## Where this fits in the roadmap

Slots into [`docs/plan.md`](plan.md) § Next steps as **item 4.5 — LLM PR-relevance pre-filter**, after item 4 (per-pipeline PR triggers) lands. Doesn't make sense before — there's no trigger to gate yet.

| Step | What it depends on |
|---|---|
| Phase A (log-only) | Item 4 (PR triggers exist), item 3 (`dit.report` JSON to compare against), item 1 (`dit_meta.runs` to audit against) |
| Phase B (enforce) | Phase A's audit passes |
| Phase C (escalate model) | Only if Phase B reveals quality gaps |

Future related work (separate docs):
- **Failure triage on red Check Runs** — reuses the `actions/ai-inference` step, reads `dit.report` output, posts a "likely cause" PR comment.
- **dit's own PR review for framework-shaped concerns** — Claude Code Action with a system prompt that references `CLAUDE.md` + the plan. Complementary to (not a replacement for) `copilot-pull-request-reviewer`, which already handles generic code review.

## Implementation plan

1. **Phase A scaffolding** (~half-day): add `.github/workflows/dit-pr-gate.yml` to one pipeline repo (pipe-gaps first), with `should_run` writing to a Check Run but the dit-cloud job unconditionally running.
2. **Audit query** (~half-day): saved BQ query joining Check Run history with `dit_meta.runs.status` and diff counts.
3. **Two-week soak.**
4. **Flip to enforce** (~5 minutes): single `if:` change in the workflow file.
5. **Replicate to anchorages_pipeline + pipe-events** (~half-day each): same workflow file, different path filters and trigger names.

Total: ~2 days of glue work spread over ~3 weeks of soak time.

## Related

- [`docs/plan.md`](plan.md) § Next steps item 4.5.
- [`docs/run-cache.md`](run-cache.md) — `dit_meta.runs` is what the audit query joins against.
- [`docs/architecture.md`](architecture.md) § Cloud Build runtime — the trigger this filter precedes.
