# CLAUDE.md — `data_integration_tests`

## Repo orientation

This repo houses `dit`, the cross-pipeline integration-test framework for GFW data pipelines (currently `pipe-gaps`, `anchorages_pipeline`/port-visits, `pipe-events`/fishing).

Read these in order before coding:

1. [`docs/architecture.md`](docs/architecture.md) — visual reference: repo ownership, run modes, workflow flows, Cloud Build runtime, image namespace. Mermaid diagrams; renders on GitHub.
2. [`docs/context.md`](docs/context.md) — background, source bugs the framework caught, branch state at handoff.
3. [`docs/plan.md`](docs/plan.md) — the implementation plan. Sections worth bookmarking: **Architecture: three-repo split**, **Public API contracts (Phase 1)**, **Phase 1 subagent task breakdown**.
4. [`docs/framework-vision.md`](docs/framework-vision.md) — long-term shape. Don't optimise for it; Phase 1 stays imperative.

## Working agreements

- **`dit` is library-first, CLI-second.** Anything new must be importable Python; the CLI is one consumer of the library.
- **`dit.compare` is a thin shim** over `table-check`. Comparison features go upstream into `table_identical_checks`, not here.
- **`dit` reads composer-dags as data, not code.** No `import gfw.common.…` or `import gfw.pipes.…`. Sync via YAML (Phase 4).
- **No workflow lives in two places.** Canonical home is `dit/workflows/<pipeline>/`; in-repo workflows in processing repos are allowed for spikes only.
- **Plan changes get logged.** Whenever an architectural decision changes, update `docs/plan.md` and append the change to the **Plan changelog** below in the same commit. Subagents treat `docs/plan.md` + this changelog as the alignment surface.
- **README Features and Roadmap sections stay current.** The README is the operational dashboard for outsiders and future maintainers. Whenever a feature lands, drops, or shifts shape — or a roadmap phase advances status, completes, or gets re-scoped — update `README.md` § "Features" or § "Roadmap" in the same commit. Treat both sections with the same discipline as the Plan changelog: out-of-date is worse than under-detailed.
- **Pipeline-contract audits.** When adding a pipeline to `dit`'s scope (or when an existing pipeline's interface changes), audit it against `docs/pipeline-contract.md` and update the adoption matrix in the same commit. Workflow-side workarounds for missing contract items require a Plan-changelog entry explaining the trade-off — the integration-test workflow must not silently carry pipeline-specific workarounds.
- **Stay clear of prod-relevant infrastructure.** dit is a testing-shaped consumer of GFW infrastructure: read access is unrestricted, but writes go only to dit-namespaced paths. Concretely — never push to `gfw-int-infrastructure/*` (the canonical pipeline image registry) and never push to prod-shaped namespaces inside wf827 (`gcr.io/world-fishing-827/anchorages_pipeline/`, `encounters_pipeline/`, `advanced_fishing_detection/`, etc.). All dit images go under `gcr.io/world-fishing-827/dit/*` instead — see [`docs/conventions.md`](docs/conventions.md) for the namespace + standard build-and-push workflow. The only explicit exception: creating branches in pipeline repos with potential fixes that might eventually merge to prod is in scope (branch existing is normal dev workflow; the merge is owned by whoever holds the button, not dit).
- **Snapshot auto-push: assume origin is public.** `make dit-cloud` against a dirty pipeline checkout auto-creates a snapshot commit and pushes it to the pipeline repo's `origin`. Any pipeline repo could be (or become) a public GitHub repo. Three defense layers:
  - **`git add -u` (tracked-only) is the default capture.** Modifications + deletions to files already in HEAD; never untracked files (including `git add`-ed-but-uncommitted ones). The convenience alternative (`git add -A`) was explicitly rejected — it would auto-push any rogue `.env`/`sa.json`/dataset/log in the working tree.
  - **Pre-push banner** lists the changed paths so a tracked credential or a surprise modification surfaces before the push happens. Ctrl-C is still available.
  - **Pre-push secret scanner (gitleaks v8.30.1, pinned).** Extracts the snapshot tree to a temp dir, runs `gitleaks detect`; a finding aborts the snapshot before either the commit or the push are created. Required by default — if gitleaks isn't on PATH and no bypass is set, the snapshot refuses to proceed. Override is `DIT_SKIP_SECRET_SCAN=1` with a loud `WARNING: secret scan BYPASSED` banner. Pre-baked into ditbox.

  When designing related features:
  - Never widen what's captured beyond `add -u` without a corresponding loud safety story (banner, scanner, opt-in flag).
  - The banner output must show the user exactly which paths are about to be pushed before the push happens.
  - For files that contain secrets and were once tracked by mistake, the remediation is `git rm --cached <file>` + `.gitignore` + `make clean-snapshot REF=<sha>` if a snapshot already landed on origin.
  - Treat any committed-once secret as compromised; rotation is the load-bearing step, not the `clean-snapshot`.
- **Workflows default to the `pipe_ais_test_202408290000` staging cohort.** Every per-workflow source-data CLI flag (e.g. `--source-dataset-stem`, `--source-normalized-table`, the implicit `_internal`/`_published` dataset pairs in `pipe_events/fishing.py`) defaults to a table or dataset in this cohort. Why: the cohort is small (~few GB per input), pinned to a known shape, mirrors a representative slice of prod, and is safe to query freely — so the default-no-flag run never accidentally hits prod-volume data or a moving target. The canonical inventory of staging tables lives in `README.md` § "Staging data sources"; **document new staging tables there in the same commit that lands the consuming workflow** (the README is the discoverability surface — if a new table only lives in workflow code, the next person re-derives it or worse, points at prod by default). For prod-only inputs not mirrored to staging (`satellite_positions_one_second_resolution_*`, `norad_to_receiver_v20230510`, etc.), expose an opt-in flag (e.g. `--include-satellite-offsets`), default off, with help text noting that enabling it snapshots from prod.
- **Don't manually delete shared `dit_exp_*` datasets.** `cross_version_ais.py` snapshot datasets carry a 7-day `default_table_expiration_ms` and self-clean. Manual `bq rm` of these datasets can clobber in-flight runs that share an experiment-id namespace — a smoke-test cleanup with a colliding `--experiment-id` already broke one real run mid-flight (snapshot deleted out from under live Dataflow workers reading from it). Smoke tests must use disjoint experiment-ids (e.g. `dit-smoke-<timestamp>`) and let the TTL clean up; production runs should never `bq rm` snapshot datasets at all.
- **Git workflow: feature branches + squash-merge.** Each non-trivial change lives on a short-lived branch named `<type>/<short-slug>` where `type` ∈ {`feat`, `fix`, `docs`, `refactor`, `test`, `chore`}. Iterate on the branch with small commits — that history is useful working state. When the work is done, open a PR (`gh pr create`) and request **Copilot** as a reviewer; after sign-off, **squash-merge** to main so `main` gets one clean commit per feature. Trivial one-line fixes (typos, broken links) can go straight to `main` and don't need a branch. Delete branches after merge. Why: with the volume of feedback per change, direct-to-main produced 5–10 small commits per feature on `main`; feature-branch + squash gives `main` one commit per feature while preserving iteration history in the PR view.
- **Releases tag `main` as `v0.X.Y`** when a meaningful set of features has landed (pre-1.0 incrementing minor freely). At tag time, move `CHANGELOG.md`'s `[Unreleased]` content under a new `## [v0.X.Y] — YYYY-MM-DD` heading and start a fresh `[Unreleased]` block. GitHub Releases auto-populate from tags via `gh release create v0.X.Y --notes-from-tag` or by writing release notes manually.
- **CHANGELOG.md is the user-facing change log.** `CHANGELOG.md` records what's available to users of `dit` (CLI flags, new helpers, new workflows, fixes). The Plan changelog in this file is dev-internal — plan-doc evolution, design refinements, why a commit happened. Both get an entry when a user-visible feature lands; CHANGELOG framed for users, Plan changelog framed for the next maintainer.

## Installing pipeline dependencies

`dit` is pipeline-agnostic; per-pipeline workflow deps (`pipe-gaps`, `anchorages_pipeline`, `pipe-events`) install separately via Makefile targets. `PROJECTS` (default: `$(realpath ..)`, i.e. sibling checkouts) tells the Makefile where to find them; override via env var or by copying `.envrc.example` → `.envrc` for direnv. See README for the full table; the operational summary:

| When | Target |
|---|---|
| Active dev on a pipeline (fast inner loop, edits picked up live) | `make install-<pipeline>` |
| Reproducible install of a specific committed ref (~5-10s; non-editable) | `make install-<pipeline>-ref REF=<sha-or-branch>` |
| Snapshot the current dirty working tree to `refs/dit-snapshots/<pipeline>/<sha>` on origin and install from it | `make snapshot-<pipeline>` |
| Target ref's transitive deps drifted (rare) — drop `--no-deps` | append `FULLDEPS=1` |
| Remove a single snapshot ref locally + on origin (secret-leak remediation only) | `make clean-snapshot PIPELINE=<name> REF=<sha-or-full-ref>` |

Notes:

- The framework-only `make install` works without any pipeline; the dataflow runner won't load until a workflow install brings `apache-beam[gcp]` transitively.
- `make snapshot-<pipeline>` produces a **deterministic orphan commit** from the dirty working tree (`git write-tree` against a temp index seeded from HEAD + `git add -u` against the temp index → captures modifications + deletions to **tracked files only**; then `git commit-tree` with frozen author/committer dates+identity → identical tree → identical SHA). Parent SHA recorded in the commit message as `dit snapshot of <40-char-sha>`. Requires `git push` permission on the pipeline's origin.
- Snapshots live forever by design (bytes-scale, hidden namespace under `refs/dit-snapshots/*`). No periodic cleanup. `make clean-snapshot` exists only for accidental-secret remediation; the previous broad-sweep `make clean-snapshots` was removed in M-pivot-1.
- The non-editable install modes (`-ref`, snapshot) point the debugger at the installed copy under `venv/lib/python3.x/site-packages/<pipeline>/`, not your dev tree. If you're stepping through pipeline source, use the editable target instead.

## Plan changelog

Most-recent-first: prepend new entries above the existing ones. Each entry is one commit's worth of plan-doc changes; cite which sections moved.

### 2026-06-03 — `dit_docker.run` gains `container_env` for passing env vars *into* the container

First pipe-segment workflow smoke surfaced a real gap in the runner API: `dit_docker.run(env={...})` sets env on the HOST `docker` / `docker compose` subprocess but never reaches the inner container, and there was no clean way to inject `-e KEY=VALUE` flags. Beam's `WriteToBigQuery` inside the pipe-segment v5.0.x dev image constructs its own `google-cloud-bigquery` client whose default-project resolution walks `GOOGLE_CLOUD_PROJECT` env → ADC metadata; the pipeline option `--project=...` is read earlier by Beam and isn't forwarded to that internal client. Without the env, the smoke failed at `WriteFragments/BigQueryBatchFileLoads/...` with `OSError: Project was not passed and could not be determined from the environment`. `examples/example_segment.sh` already documented the workaround (inline `-e GOOGLE_CLOUD_PROJECT=...` on the docker compose command); the runner just didn't expose it.

**Added `container_env: dict | None = None` to `dit.runners.docker.run`.** Emits sorted `-e KEY=VALUE` flags on both the published-image (`docker run`) and `build_from_source` (`docker compose run`) paths, after `--entrypoint` and before the image/service positional (docker rejects `-e` after the positional). Default `None` produces no `-e` flags — byte-identical to existing callers (pipe-gaps, port-visits, pipe-events). 7 new tests in `tests/test_runners_docker.py` cover the default-no-emission, both runner paths, ordering-before-positional, sorted-multi-key, and `env` vs `container_env` separation cases. Full suite: 306 passing.

**Sections moved.** `src/dit/runners/docker.py` `run()` signature and docstring (`env` explicitly contrasted with `container_env`). `docs/conventions.md` gains a new "Container env vars (workflow-driven, via `container_env=...`)" subsection after "Auth in the cloud path (ditbox)" — distinguishes the two similarly-named parameters and cites the pipe-segment smoke as the surfacing case. `CHANGELOG.md` § [Unreleased] gains a top "Added" entry. No behaviour change for any existing workflow; this is purely a new capability that the in-flight pipe-segment workflow (PR #44) will adopt in a follow-up change to its `_run_pipe_subcommand`.

**Trade-off accepted.** `container_env` is a public parameter on the runner surface; the docstring + conventions.md doc both call out the "env vs container_env" footgun explicitly so callers don't mix them up. If a third workflow needs this for the same `GOOGLE_CLOUD_PROJECT` use case, we'd consider lifting it into a shared default rather than each workflow re-passing it — flagged but not pursued yet (one consumer is too few to extract a pattern).

**Followup (Copilot PR #48 comment, addressed).** The runner emits an INFO-level docker command log line (`"docker: %s" % cmd`) that would have leaked whatever values a workflow passes via `container_env`. Today's sole consumer is the non-sensitive `GOOGLE_CLOUD_PROJECT` project id, but the runner shouldn't be a silent leak vector if a future workflow ever wires a token-shaped value. Added `_redact_e_flags(cmd)` that joins `cmd` for logging with every `-e KEY=VALUE` rewritten to `-e KEY=<redacted>` — structural redaction ("any `-e` flag's value"), not key-allowlist based, so the runner stays safe by default rather than relying on every new caller to opt out. The real `cmd` still goes to `subprocess.run` unchanged. One additional test in `tests/test_runners_docker.py` (`test_container_env_value_redacted_in_log`) pins the redaction (asserts neither `hunter2` nor `world-fishing-827` appear in the log text while `SECRET_TOKEN=<redacted>` + `GOOGLE_CLOUD_PROJECT=<redacted>` both do, AND the real subprocess call still received the un-redacted values). Total runner-test count: 32; full suite 307.

### 2026-06-03 — ditbox-for-pipe-events live verification + pipe-events SQL non-determinism finding

First successful end-to-end run of `make dit-cloud PIPELINE=pipe-events` against the AIS-staging cohort (build `e6f06a00-7a0e-4533-aede-e136f97301a4`, ~47 minutes). Closes the "user-gated validation" follow-up flagged across PR #34, #36, #39, #40, #42. Every layer of the stack now verified live: Cloud Build submission → ditbox step → canonical pipe-events image pull (read-only from `gfw-int-infrastructure/publication/...:v4.2.17`) → 12 nested `docker run` invocations under `--network=cloudbuild` → fake metadata server → `automated-testing@` ADC → BQ calls → 4-step incremental fishing-events chain × 3 modes × 2 score fields → output tables + views → `compare_all` verdict → exit code propagation.

**Five-build debugging cascade got us there.** Each build's failure exposed a real bug or staging-data issue, fixed before the next iteration:
1. `gcloud builds submit` rejected `$DIT_ADC_TOKEN` as a build substitution → escape with `$$` (PR #38).
2. Pipe-events parser rejected `--labels` at the wrong nesting → move to per-operation block as `-labels` (also PR #38).
3. google-auth `invalid_client` on refresh of placeholder-OAuth ADC JSON → pivot to `--network=host` (PR #39, later corrected).
4. BQ 403 "API not used in project 1034185025654" → inject `GOOGLE_CLOUD_QUOTA_PROJECT=world-fishing-827` (PR #40, later removed).
5. BQ 403 `USER_PROJECT_DENIED` even after IAM grant → diagnosed via metadata-server probe that `--network=host` lands the sibling on the docker-daemon-host's network (Google-managed `cloudbuild-untrusted@argo-prod-*`), not the build step's. Pivot to `--network=cloudbuild` (PR #42).

After PR #42 landed and the user fixed two staging-cohort data issues (stale PVIS schema; broken `identity_core`/`identity_authorization` views pointing at a non-existent `pipe_ais_v3_published` dataset), the run completed cleanly.

**Pipe-events SQL non-determinism finding (real, upstream, low-priority).** The successful run reported 92-97% of rows "differing" across modes, but inspection of 500+ rows showed **zero semantic differences**. The signal traces to two `STRING` columns where pipe-events serialises structured data via `TO_JSON_STRING(...)` inside its SQL:

- `event_info`: float-precision drift from non-associative `SUM`/`AVG` across different partition boundaries (`bf` = full year window; `bfd`/`bftruncate` = day-by-day). Values agree to ~15 significant digits; last-bit drift only.
- `event_vessels.public_authorizations`: `ARRAY_AGG(STRUCT(rfmo, has_publicly_listed_authorization))` without `ORDER BY` (line 358 of `assets/bigquery/fishing-events-4-authorization.sql.j2`). Same multiset of authorisations, different element order per partition shape.

These are byte-level diffs in JSON-encoded strings, so `table_identical_checks`'s native-ARRAY canonicalisation (`_array_canonical_sql` in `backend/query_builder.py`) doesn't apply — the columns reach table-check as opaque `STRING` and get byte-compared.

**Recommendation for pipe-events** (out of dit's scope): add `ORDER BY` to the `ARRAY_AGG` — eliminates ~50% of the noise, zero risk. Float noise harder to chase; possible to ROUND() before serialising if bit-perfect matching is desired.

**Decision on the dit / `table_identical_checks` side: DEFERRED.** A detailed feature spec for "JSON-aware comparison for STRING columns containing serialised JSON" was drafted in-session (parse + recursive key sort + opt-in deep array sort + opt-in float tolerance on nested numerics; Python-side post-process for the diff set). The spec is filed as institutional context but **not pursued** — this is a pipe-events SQL non-determinism issue, not a comparison-tool gap. Reopen the feature only if another pipeline starts pushing structures through JSON-string columns.

**Sections moved.**
- `CHANGELOG.md` § `[Unreleased]` gains a 2026-06-03 `#### Verified` block above the existing `#### Fixed` (the PR #42 entry).
- This Plan changelog gains the entry you're reading.
- `README.md` § "Roadmap" Phase 3 row updated: status flips from "Code complete + pending live e2e" to "Verified live 2026-06-03".
- Memory updates: new [[cloudbuild-metadata-server-topology]] (architectural lesson + the two-metadata-server topology + which network to attach siblings to); new [[dit-pipe-events-cloud-verified]] (milestone record with the five-build cascade + pipe-events finding); [[next-stages-m5-pipe-events]] rewritten — auth section reflects final `--network=cloudbuild` (not the falsified `file-mount` or `--network=host`); "remaining work" updated.

**Operational follow-up.** The PR #40-era `roles/serviceusage.serviceUsageConsumer` binding on `automated-testing@` can be revoked — it addressed nothing real (the actual caller under `--network=host` was a Google-managed SA we couldn't have granted anything to anyway).

**Method note.** Both prior pivots (PR #34 file-mount, PR #39 `--network=host`) were theoretical decisions falsified by live evidence; the corrected understanding came from a targeted metadata-server probe + a focused web-research pass on Cloud Build's nested-docker auth patterns. Worth remembering: **when a design touches Cloud Build's runtime topology, probe the actual identity the runtime delivers before committing to the design** — the documentation lags reality, and "looks like it should work" is not "works." The probe is a self-contained yaml that runs in ~30 seconds.

### 2026-06-03 — Cloud auth re-pivot: `--network=host` → `--network=cloudbuild` (build-step's fake metadata server, not the daemon-host's real one)

The 2026-06-02 `--network=host` pivot was based on an incomplete model of Cloud Build's runtime topology. Live evidence from build #4 (`USER_PROJECT_DENIED` even after granting `roles/serviceusage.serviceUsageConsumer` to `automated-testing@`) and a focused metadata-server probe revealed that **two metadata servers coexist on the build VM**:

- A **fake metadata server** on the docker network literally named `cloudbuild`, returning OAuth tokens for the user-configured `serviceAccount:` (`automated-testing@`). Every build-step container is auto-attached to this network — which is why the build step itself sees `automated-testing@`.
- The **real metadata server** on the VM's default network namespace, returning the Google-managed `cloudbuild-untrusted@argo-prod-*` identity (the docker daemon host).

`docker run --network=host` puts the sibling container on the daemon's host network — i.e. the real metadata server — so it sees the Google-managed identity, NOT the build SA. We confirmed this empirically with two probes:

```
host build step:                automated-testing@world-fishing-827        ✓
nested w/ --network=host:       cloudbuild-untrusted@argo-prod-us-west1    ✗
nested w/ --network=cloudbuild: automated-testing@world-fishing-827        ✓
```

`--network=cloudbuild` is the documented sibling-container pattern (see `cloud-build-local`'s open-source `metadata.go`, [earthly/earthly#1628](https://github.com/earthly/earthly/issues/1628), Imre Rad's "Google Cloud Build under the hood") that re-attaches the sibling to the fake metadata server. This is the architectural fix the previous two pivots (file-mount ADC → `--network=host` → `--network=cloudbuild`) were converging on; we got there by elimination + targeted research after the second falsification.

**Decision: pivot to `--network=cloudbuild`.** Same shape of design — no on-disk credential material, the inner workload identifies as the build SA via standard ADC discovery — but using the docker network where the build SA's tokens are actually served. The PR #40 `GOOGLE_CLOUD_QUOTA_PROJECT=world-fishing-827` env injection is also **removed**: it was treating a symptom of the wrong-identity bug (the host-network metadata server returns a Google-managed SA whose default quota project isn't ours), not a real issue. The fake metadata server returns tokens whose default quota project is already `world-fishing-827`.

**Sections moved.**
- `src/dit/runners/docker.py`: `_apply_cloud_mode` now emits `["--network=cloudbuild", ...kept volumes]` (no quota-project env); constants renamed (`_CLOUD_MODE_QUOTA_PROJECT` → `_CLOUDBUILD_NETWORK`); module + function docstrings rewritten with the corrected fake-vs-real metadata-server model + both prior designs (file-mount, `--network=host`) preserved as institutional memory.
- `tests/test_runners_docker.py`: 18 cloud-mode tests updated — assertions flipped from `--network=host` to `--network=cloudbuild`; the `_QUOTA_PROJECT_FLAGS` constant + every reference to it removed; new `test_apply_cloud_mode_no_quota_project_env` test pins the removal so it doesn't sneak back; assertion names updated. Full suite: 299 passing.
- `cloudbuild-dit.yaml`: comments rewritten to describe the fake-vs-real architecture and reference both falsified prior designs.
- `docs/conventions.md` § "Auth in the cloud path (ditbox)": three-context table updated (ditbox row now describes `--network=cloudbuild`); "Why" paragraph rewritten to explain the fake-vs-real metadata server distinction; two falsified designs preserved as institutional memory with date+PR references.
- `README.md` § ditbox auth paragraph rewritten.
- `CHANGELOG.md` § `[Unreleased]` gains a top 2026-06-03 `#### Fixed` block.

**Operational follow-up.** The PR #40-era `roles/serviceusage.serviceUsageConsumer` IAM binding on `automated-testing@` can be revoked — it addressed nothing real (the actual caller under `--network=host` was a Google-managed SA we couldn't have granted anything to anyway).

**Trade-off accepted.** None new — the surface area is smaller than `--network=host`'s was: the sibling container shares only a docker network with the build's fake metadata server, not the VM's network namespace. Live-evidence-confirmed, documented, and matches Cloud Build's intended sibling-container contract.

**Future architectural upgrade (unchanged).** Migrating ditbox to GKE / Cloud Run Jobs would recover prod's literal keyless model (Workload Identity scoped to a KSA). Reserved for when longer-term needs co-justify the architectural shift. For now `--network=cloudbuild` is the right answer.

**Method note.** Both prior pivots (PR #34, PR #39) were theoretical decisions falsified by live evidence; the corrected understanding came from a targeted metadata-server probe + a focused web-research pass on Cloud Build's nested-docker auth patterns. Worth remembering: when a design touches Cloud Build's runtime topology, probe the actual identity the runtime delivers before committing to the design — the documentation lags reality, and "looks like it should work" is not "works."

### 2026-06-02 — Cloud auth pivot: bind-mounted ADC file → `--network=host` (metadata-server access)

First live cloud run (`make dit-cloud PIPELINE=pipe-events`, build `dab02540`) falsified the assumption underpinning PR #34's cloud-auth design. The placeholder-`authorized_user` ADC JSON we bind-mounted was rejected by the older `google-auth` in pipe-events' Python 3.8 image — it tries to refresh `authorized_user` credentials **before** the first API call, ignores the pre-issued `token` field, and the refresh against placeholder OAuth client material failed with `invalid_client`. The intended "refresh fails loudly after the ~60-min TTL" failure mode actually fires before the first API call.

**Decision: pivot to `--network=host`** (Option C from the earlier auth-options exploration, previously declined for security reasons). dit's docker runner now adds `--network=host` to the inner container when cloud mode is on, so the container shares the build VM's network namespace and reaches Cloud Build's metadata server (`169.254.169.254`) for ADC. No on-disk credential material; the container never holds a long-lived secret — same shape as prod (which gets ADC via GKE's metadata server through Workload Identity).

**Why the earlier "declined" decision now changes.** The argument against `--network=host` was that it "trades a cosmetic-JSON smell for a structural shared-network-namespace concession." That argument was theoretical; the failure of the file-mount approach is concrete. And the practical surface increase on an ephemeral per-build VM with no co-tenancy and only the build's own steps running is essentially zero. Reviewing the trade-off honestly with live evidence in hand: the cosmetic-JSON shape doesn't matter if it doesn't *work*.

**Sections moved.**
- `src/dit/runners/docker.py`: `_apply_cloud_auth_mode` → `_apply_cloud_mode`; helper now returns `["--network=host", ...kept volumes]` when active (no bind-mount); env var renamed `DIT_CLOUD_AUTH_ADC` → `DIT_CLOUD_MODE` (any non-empty value triggers); docstrings rewritten; references to the file-mount approach kept as historical context in the helper's docstring.
- `cloudbuild-dit.yaml`: the `write-adc` step + its substantial comment block dropped entirely; the `dit-run` step's `DIT_CLOUD_AUTH_ADC=/workspace/dit-adc.json` env replaced with `DIT_CLOUD_MODE=1`; `waitFor: ['write-adc']` removed (dit-run is now the only step).
- `tests/test_runners_docker.py`: 18 cloud-mode tests rewritten to assert `--network=host` presence (helper + both run-paths + laptop-mount-drop + log hygiene).
- `tests/test_pipe_events_fishing.py`: the single `monkeypatch.delenv("DIT_CLOUD_AUTH_ADC", ...)` updated to `DIT_CLOUD_MODE`.
- `docs/conventions.md` § "Auth in the cloud path (ditbox)": three-context table updated (ditbox row now describes `--network=host`); "Triggered by" paragraph updated for the new env var; "Why metadata-server" paragraph added recording the live-evidence pivot + the trade-off accepted; the "Hardening considered and declined" paragraph rewritten — the dedicated-runner-SA case stands, but the `--network=host`-was-also-declined claim is now stale (it's the path we just adopted).
- `CHANGELOG.md` § `[Unreleased]` gains a top 2026-06-02 `#### Changed` bullet. The 279-test suite continues to pass.
- `README.md` § "ditbox-for-pipe-events" paragraph rewritten to reflect `--network=host` mechanism.

**Trade-off accepted.** Inner container shares the build VM's network namespace. Mitigated by Cloud Build's per-build-ephemeral VM model + no co-tenancy + sequential build steps. Worth re-evaluating if ditbox ever moves to a shared-runtime environment.

**Future architectural upgrade (still on the table).** Migrating ditbox to GKE / Cloud Run Jobs would recover prod's literal keyless model: the inner workload gets metadata-server ADC via Workload Identity, scoped to its own KSA, without any host-network sharing. Reserved for when longer-term needs co-justify it. For now, `--network=host` matches the security profile we actually need.

### 2026-06-02 — `workflows/pipe_events/fishing.py` defaults: pipe3 → staging; one workflow covers all three bash variants

After landing both halves of ditbox-for-pipe-events and being ready for the first live run, surfaced that pipe-events ships **three** bash integration scripts (`staging-bf_bfd_bftruncate_async.sh`, `pipe3-bf_bfd_bftruncate.sh`, `pipe3-bf_bfd_bftruncate_async.sh`) and `fishing.py`'s defaults were inherited from the most expensive of the three (pipe3 sync = full prod cohort over 2012). pipe-events' own `CLAUDE.md` says "Always run staging first" — the wrong shape to fire by default.

Diffed the three scripts: they differ **only in defaults** — date window, tail days, source cohort, static-measures table. Same modes (`1_bf` / `2_bfd` / `3_bftruncate`), same 4-step docker chain, same comparison contract. So:

**Decision: one workflow file with staging defaults; pipe3 reachable via CLI overrides.** A separate `pipe3_fishing.py` would be ~40 lines of duplicate boilerplate for different default values — not worth it. The module docstring records the exact override invocations for pipe3-sync and pipe3-async, so the production-scale capability is preserved without a duplicate file.

**Sections moved.** `workflows/pipe_events/fishing.py`: module docstring rewritten (cite all three bash scripts; document the pipe3 override commands); `DEFAULT_START`/`DEFAULT_END` flipped (`2012-01-01`/`2013-01-01` → `2020-01-01`/`2021-01-01`); comment block above date constants rewritten to reference staging + cite the override path; the "Modes (...mirroring pipe3-...)" header generalised to "mirroring the staging/pipe3 bash scripts". `CHANGELOG.md` § [Unreleased] gains a top 2026-06-02 `#### Changed` bullet. Tests unchanged — they use explicit fixtures (`_args(start="2012-01-01", ...)`), not the new defaults — so the suite (279 tests) continues to pass.

**Trade-off accepted.** A `dit run workflows/pipe_events/fishing.py` with no overrides now hits the AIS test cohort in 2020 (cheap, intended), not pipe_ais_v3 in 2012 (expensive, unintended-by-default). Anyone explicitly running pipe3 has to type the overrides — friction in proportion to cost. The reverse (pipe3 by default) had the opposite ergonomics: cheap-day-by-default and expensive-day-by-default were both options; chose the cheap-by-default.

### 2026-06-02 — Image-availability half of ditbox-for-pipe-events: M-pivot-4 generalised; symmetric trigger across both consumers

Closes the second half of the ditbox-for-pipe-events story sketched in the 2026-06-01 planning entry (the auth half landed earlier the same day in PR #34). The existing M-pivot-4 kaniko auto-build (`src/dit/worker_image.py`, today produces Dataflow worker images for unreviewed Beam code) now serves dit's docker runner identically — same namespace (`gcr.io/world-fishing-827/dit/<pipeline>`), same kaniko machinery, same `:dit-<pipeline_commit>` tag scheme; only the consumer differs. With this PR `make dit-cloud PIPELINE=pipe-events` is image-side ready (live e2e is user-gated).

**Design correction along the way.** A first cut introduced a docker-runner-only `need_registry_image` switch with an asymmetric trigger (build for docker-runner cloud mode regardless of `unreviewed`, because pipe-events has "no upstream published image"). Then we verified, by tracing `composer-dags-production/gfw/pipes/v3/fishing_events.py` and listing the registry, that **pipe-events DOES publish canonical versioned images** at `us-central1-docker.pkg.dev/gfw-int-infrastructure/publication/github-globalfishingwatch-pipe-events:vX.Y.Z` (and a legacy mirror at `gcr.io/world-fishing-827/github.com/globalfishingwatch/pipe-events`). dit has read-only IAM to both (per the absolute prod-infra boundary). So the asymmetric-trigger justification evaporated and we **symmetrised**: same trigger for both consumers — build when `worker_image == default_worker_image` AND `unreviewed`; otherwise pull the canonical default. pipe-events's `DEFAULT_IMAGE_TAG` is now the prod-pinned canonical path (`...:v4.2.17`, matching `Versions.PIPE_EVENTS` in composer-dags).

**Sections moved.** `src/dit/worker_image.py`: renames + drop the docker-runner branch in `ensure_pipeline_image` + module docstring rewritten to describe the symmetric trigger. `src/dit/workflow.py:resolve_run_context`: drop `need_registry_image` parameter; drop `runner=runner` forwarding to `ensure_pipeline_image`. `workflows/pipe_events/fishing.py`: change `DEFAULT_IMAGE_TAG` to the canonical published path; drop the `cloud_mode` calculation; `args.image_tag = ctx.worker_image` unconditional; module docstring rewritten. `docs/conventions.md` § "Image namespace": drop the "(planned)" marker from pipe-events; restate the symmetric trigger; add a "Canonical upstream defaults" list. `tests/test_worker_image.py`: collapse the per-consumer trigger matrices into one symmetric matrix. `tests/test_pipe_events_fishing.py`: retarget the 4 cloud-mode tests around the unconditional stamping behaviour. `tests/test_workflow.py`: drop `need_registry_image` from the harness-forwarding assertion. `CHANGELOG.md` § [Unreleased] gains a top 2026-06-02 bullet.

**Trade-off accepted.** On laptop, a default-image-tag run of unreviewed pipe-events code (no `--build-from-source`) now triggers the M-pivot-4 kaniko build, requiring laptop gcloud + Cloud Build perms. This matches how Beam workflows on laptop already work for unreviewed code — same cost, same ergonomics, same submit path. The natural laptop inner-loop pattern for pipe-events remains `--build-from-source` (compose builds the working tree); the symmetric design changes only the default-no-flag behaviour.

**`--build-from-source` opts out at two levels.** Beyond the runtime opt-out (docker runner ignores `image_tag` and runs the compose `pipeline` service), `resolve_run_context` gains a `build_from_source: bool = False` parameter that, when True, bypasses `ensure_pipeline_image` entirely — avoiding an unnecessary kaniko submission for an unreviewed build-from-source run where the produced image would never be pulled. Beam consumers don't pass this; default keeps their behaviour unchanged.

**User-gated validation (out of scope here).** The live `gcloud builds submit` inside a real ditbox-in-Cloud-Build run, the resulting registry pull, and the docker-in-docker invocation with the auto-built image have not been exercised against live infra. Same gating shape as M5 / Phase 3 / PR #34 (cloud-auth half). The next step is a user-driven live `make dit-cloud PIPELINE=pipe-events ...` against the AIS-staging cohort.

### 2026-06-02 — Auth hardening (dedicated runner SA) considered and declined

After landing PR #34 (cloud-auth mode with `authorized_user`-shaped ADC + literal placeholder strings in `client_id`/`client_secret`/`refresh_token`), the documented next step had been to replace the placeholder shape with an `impersonated_service_account` ADC pointing at a new `dit-pipe-events-runner@` SA. Reconsidered: **drop it.** Reasoning:

- `automated-testing@`'s perms are already scoped — the absolute prod-infra boundary (see [[prod-infra-boundary]]) keeps it out of `gfw-int-infrastructure`, writes are dit-namespaced, tokens are ~1h-lived. A narrower runner SA reduces blast radius marginally but adds an SA + 2 IAM bindings to maintain without addressing a real threat.
- The placeholder-JSON shape is a *cosmetic* concern (literal `"placeholder"` strings + loud `invalid_grant` failure mode), not a security one. No silent fallback path.
- `--network=host` (Option C, metadata-server access from the inner container) was the obvious alternative once the new-SA path was off the table; reconsidered honestly and also declined — trades the cosmetic-JSON smell for a structural shared-network-namespace concession that reviews worse and gives a compromised container token-refresh-on-demand instead of a fixed-TTL leak window.
- Only path that materially changes the auth model is Option A (migrate ditbox to GKE / Cloud Run Jobs) — reserved as a future *architectural* upgrade.

**Sections moved:** `docs/conventions.md` § "Auth in the cloud path (ditbox)" — "Future hardening" subsection rewritten from "not yet implemented" to "considered and declined" with the explicit reasoning above. Memory `[[next-stages-m5-pipe-events]]` auth bullet updated to match (no further hardening planned). No code change.

### 2026-06-02 — Implement ditbox cloud-auth mode (token-only ADC bind-mount; short-lived; future hardening = impersonation SA)

Implementing the **auth half** of the ditbox-for-pipe-events story laid out in the 2026-06-01 entry. The **image-availability half** (M-pivot-4 generalisation: dit's existing kaniko auto-build pushing under `gcr.io/world-fishing-827/dit/<pipeline>` extended to feed dit's docker runner, not just Dataflow workers) **remains pending** — this PR is the auth half only. The two halves are independent on purpose: the auth mode lets a Cloud-Build-spawned `pipe-events` container reach BQ regardless of which image it pulls; the image-availability half decides what tag that image carries.

**Three execution contexts converge on the standard ADC path** (`/root/.config/gcloud/application_default_credentials.json`) inside the container — workflow code is identical across them. Laptop: workflow mounts the user's `gcp` named volume (`gcloud auth application-default login` populates it). Prod (Composer/GKE): metadata server via Workload Identity (no file mount). Ditbox (Cloud Build): a write-ADC step produces a short-lived JSON to `/workspace/dit-adc.json`; `src/dit/runners/docker.py`'s cloud-auth mode bind-mounts it `:ro` at the standard path inside the inner container. **Triggered by env var** (`DIT_CLOUD_AUTH_ADC` set → on), **not a `run(...)` parameter** — the workflow stays unaware of context, calling `dit_docker.run(..., volumes=["gcp:/root/.config"])` identically everywhere; the runner overrides at the laptop mount slot when the env var is set.

**Sections moved:** `cloudbuild-dit.yaml` gained the `write-adc` step + the `DIT_CLOUD_AUTH_ADC` env wiring in `dit-run`; `src/dit/runners/docker.py` gained `_apply_cloud_auth_mode(volumes)` (pure helper) + cloud-auth docstring on `run`; `docs/conventions.md` gained an "Auth in the cloud path (ditbox)" subsection with the three-context table + the impersonation-SA follow-up; `CHANGELOG.md` § [Unreleased] gained a 2026-06-02 `#### Added` block. 16 new tests in `tests/test_runners_docker.py` (helper + both runner paths + logging hygiene); full suite 270 passes.

**ADC JSON format research (the one place real design judgement was needed).** `gcloud iam service-accounts keys create` would have been simple but is **forbidden** (permanent credential on disk; spec-rejected). The cleanest google-recommended pattern that satisfies "short-lived, no SA key" without an out-of-band setup hop is: write an `authorized_user`-shaped JSON where the `token` field carries the access token from `gcloud auth application-default print-access-token` (which the Cloud Build SA's metadata-server identity provides for free), and leave the refresh-related fields as clearly-marked placeholders. The google-auth library's `from_authorized_user_info` validates schema (refresh_token must be present, content not checked at load time), so the file loads; the pre-issued token works for ~60 minutes; the refresh path is **designed to fail loudly** (`invalid_grant`) — if a build outlives the TTL, no silent fallback to any permanent identity is possible. Forbidden alternatives considered explicitly: `impersonated_service_account` (requires `source_credentials` with a real refresh_token the build SA doesn't have — issuing one means generating an SA key first), `external_account` / WIF (Cloud Build uses direct SA attachment, not federation).

**Future hardening (documented, not implemented).** Spawn a dedicated impersonation SA with the narrowest set of permissions pipe-events needs; have the write-ADC step impersonate that SA instead of issuing a token directly against the (broader-scoped) build SA `automated-testing@`. The current shape re-uses the build SA's identity as a pragmatic first step; the impersonation hop is a cleaner long-term shape but does not change the on-disk credential model (token-only, no key).

**User-gated validation (out of scope here).** The `gcloud auth application-default print-access-token` call inside Cloud Build, the actual `:ro` bind-mount into the inner docker container, and the end-to-end pipe-events run against the inner container reaching BQ have not been exercised against live Cloud Build in this change — tests mock subprocess. Same gating shape as M5 (live `dataflow.jobs.cancel`) and Phase 3 (real pipe-events live run): user-driven follow-up.

### 2026-06-01 — Generalise dit auto-build to docker-runner pipelines (ditbox-for-pipe-events design)

Investigating ditbox-for-pipe-events surfaced two friction areas — **image availability** and **auth model** — neither of which requires writing to `gfw-int-infrastructure` (see [[prod-infra-boundary]], strengthened to absolute in the same session). Decision: dit's existing **M-pivot-4 auto-build** (`src/dit/worker_image.py` — today produces Dataflow worker images for unreviewed code via a kaniko sub-build pushing to `gcr.io/world-fishing-827/dit/<pipeline>:dit-<pipeline_commit>`) **will generalise to docker-runner pipelines**. Same `dit/` namespace, same kaniko machinery, same `:dit-<pipeline_commit>` tag scheme; what will differ is the consumer (dit's docker runner pulls + runs vs. Dataflow workers pull). For pipe-events the target is `gcr.io/world-fishing-827/dit/pipe-events:dit-<pipeline_commit>`. Auth in the cloud path: `src/dit/runners/docker.py` will gain a mode that swaps the `gcp:/root/.config` named-volume mount for a bind-mount to a Cloud-Build-written ADC file — no pipe-events code change (the container already uses standard `google-cloud-bigquery` ADC).

**Sections moved:** `docs/conventions.md` § Image namespace gained an "Auto-built pipeline image" row covering both Beam-worker (today's M-pivot-4 use) and docker-runner (planned) consumers; memory entries [[dit-image-namespace]] and [[next-stages-m5-pipe-events]] mirror the row + the design plan. **No code change in this commit** — this is a planning record before implementation lands; the actual implementation is sequenced after M6 (SIGTERM trap), see [[next-stages-m5-pipe-events]].

**Why this is the right shape:** I initially sketched a different option — "have pipe-events publish to `gcr.io/world-fishing-827/pipe-events:<sha>`" — which was wrong on two counts: (a) that's a *prod-shaped* top-level namespace inside wf827, reserved for canonical pipelines, not dit (`docs/conventions.md` § Prod-infra boundary); (b) canonical pipeline publications land in `gfw-int-infrastructure/core/` (mirroring pipe-anchorages' existing trigger), which dit can **read** but never **write to** (absolute rule). The corrected design avoids any cross-namespace ask: dit auto-builds under its own `dit/` namespace using machinery it already has, with `pipe-events` as the first non-Beam consumer of M-pivot-4. If pipe-events ever publishes canonically that's an upstream-team change (their CI, their trigger, their IAM), independent of dit; dit's auto-build remains the right answer for unreviewed code regardless.

### 2026-05-29 — Phase 3 landed: pipe-events port (third consumer); docker-runner `volumes`; `add_infra_args` split; SCD-2 plan correction; framework-extraction deferred

Phase 3 ports `pipe-events/integration_tests/pipe3-bf_bfd_bftruncate.sh` to `workflows/pipe_events/fishing.py` on `feat/pipe-events-port` (commits A–E). pipe-events is the **third consumer** and the first non-Beam one (BQ-SQL / `_SESSION` / docker-runner), so it stresses the harness exactly where the two Beam consumers couldn't. **Sections moved/added:** `docs/plan.md` § "Public API contracts → `dit.runners.docker`" (added `volumes` + `service` params); § "Phase 3 — pipe-events port" "Refined plan (2026-05-29)" block (SCD-2 → truncate-shape correction); the adoption matrix in `docs/pipeline-contract.md` (pipe-events row); `CHANGELOG.md` § [Unreleased] (workflow + runner `volumes` + `add_infra_args` split); `README.md` § Features (pipe-events workflow) + § Roadmap (Phase 3 row → done) + a pipe-events usage scenario; `workflows/README.md` (new — records the framework-extraction deferral).

**Commit A — docker-runner `volumes` + `service`.** pipe-events authenticates to GCP via a docker **named volume** `gcp` mounted at `/root/.config` (created out-of-band: `docker volume create gcp` + `gcloud auth login`). The runner's `docker run` (published) path mounted no volumes and the `build_from_source` path hardcoded the `dev` compose service, so it couldn't authenticate pipe-events. Added `volumes: Sequence[str] = ()` (threads `-v <spec>` into BOTH paths, before the image/service positional) + `service: str = "dev"` (configurable compose service; pipe-events' is `pipeline`). Default empty tuple + default `dev` keep the two Beam consumers byte-identical. **Decision:** the simple `-v`-on-`docker run` approach sufficed for the published path; `service` was added too because the build-from-source path needs the right compose service name. 7 unit tests in `tests/test_runners_docker.py`.

**Commit B — `add_infra_args` split.** `dit.workflow.add_infra_args` bundled Dataflow placement knobs (`--dataflow-region/temp-bucket/subnetwork`) a BQ-SQL pipeline never uses. Split into `add_dataset_args(parser)` (dest-dataset + project-shaped knobs: `--dest-dataset`, `--service-account`) + `add_dataflow_args(parser)` (`--dataflow-region/temp-bucket/subnetwork`); `add_infra_args` is now `add_dataset_args(p); add_dataflow_args(p)` so the two Beam workflows' parsed namespaces are byte-identical (verified by test). pipe-events calls only `add_dataset_args`. **Note:** `--service-account` stays in the dataset group (it's the BQ job SA pipe-events uses too, not Dataflow-specific).

**Commit C — the pipe-events workflow.** `workflows/pipe_events/fishing.py` mirrors `port_visits/ais.py`'s structure (arg parsing via the harness, `execute_bf/bfd/bftruncate`, `compare_all`, `main() -> int`) but **caches NOTHING** (deferred per plan — no Dataflow worker image to digest; `resolve_digest=False`, no `run_with_cache`). Each mode runs the **4-step docker chain** per slice (`incremental_events` ×2 score fields → `incremental_filter_events` ×2 → `auth_and_regions_fishing_events` → `fishing_restrictive`) via `dit_docker.run(image_tag, step_args, entrypoint="pipe", volumes=["gcp:/root/.config"], service="pipeline", build_from_source=...)`. `resolve_run_context` still runs for provenance (`ensure_worker_image` no-ops for the docker runner). Date semantics replicate the bash exactly (see Commit C findings below).

**Comparison target (Commit C decision).** CORRECTED the plan's SCD-2 claim. The fishing-events schema (`assets/bigquery/fishing-events-4-authorization-schema.json`) is keyed by **`event_id`** with NO `valid_from/valid_to/is_current` and NO `_last_versions` view — versioning is **table-level** (date-suffixed `_v{YYYYMMDD}` + a view to the latest). So the cross-mode comparison is the **truncate shape**: `compare_tables(a, b, keys=("event_id",), view_suffix="")` — like port-visits, NOT pipe-gaps. **Compare the `_fishing_events` view** (not the `_v{date}` table): the view abstracts the per-mode date-suffix (each mode's final `rdate`/`end_d` differs, so the versioned table name differs per mode; the view always points to the latest), and it's what downstream consumers query. Second comparison target: the `_product_events_fishing` (restrictive) view, also compared on `event_id`.

**Bash date semantics replicated (Commit C finding).** The generate script's auth step writes `{prefix}_fishing_events_v{rdate-with-hyphens-stripped}` (confirmed: `parse.py` does `reference_date.replace('-', '')`). `end_d` is **exclusive** in the bash's incremental query (`-end $end_d`) and the daily loop uses `end_d = current_day + 1` (1-day slices `[d, d+1)`). bf = single full window `[start, end_year+1-01-01)`; bfd = backfill `[start, end-tail)` then `tail_days` daily 1-day slices; bftruncate = full backfill `[start, end)` then the same daily slices (re-merge/truncate path). The plan had said bfd ends at `12-28`; the actual bash ends the initial bfd backfill at `12-29` with `end` exclusive (3 days short of `01-01`). Replicated `tail_days=3` default, daily slices `[d, d+1)`.

**Commit E — framework-extraction decision: DEFERRED (documented in `workflows/README.md`).** Putting `execute_bf/bfd/bftruncate` from all THREE consumers side-by-side: pipe-gaps (in-process Beam, `_make_config`/`_run_pipeline`, `backfill_days_w`-windowed dailies), port-visits (Beam-via-container, two-step `thin → visits` per slice, 1-day-end dailies), pipe-events (BQ-SQL-via-container, four-step chain per slice, `[d, d+1)` dailies). The per-slice **execution** bodies are <20% similar (different runners, step counts, table-naming, arg shapes). The genuinely-shared part is only the **date-slice arithmetic** (bf=full; bfd=range−tail + daily loop; bftruncate=full + daily loop), but even that diverges in the daily-window definition (pipe-gaps subtracts `backfill_days_w`; the other two use 1-day slices). A `dit.phases` yielding `(start,end)` slices would save ~6 lines per workflow at the cost of a leaky abstraction that has to model three different daily-window conventions. **Verdict: do not extract; revisit if a fourth consumer shares pipe-gaps' or port-visits' exact daily-window shape.** Three was the right number to decide, and the decision is "the shapes are different enough to keep them explicit."

### 2026-05-29 — M5 landed: cleanup control plane (M5a) + port-visits cache (M5b)

Implemented both halves of Milestone 5 on `feat/m5-cache-cleanup-and-portvisits` (M5a + M5b as two commits). **Sections moved:** `docs/run-cache-impl.md` § Milestone 5 rewritten from a plan into a ✓-landed record (M5a + M5b done, with the implemented design points + the user-gated permission follow-up called out); `CHANGELOG.md` § [Unreleased] gained a 2026-05-29 `#### Added` block (three bullets: `dit cache-cancel`/`make dit-cancel`, pipe-gaps `dit_run_id` label, port-visits caching); `README.md` § Features gained a "Run cleanup control plane" bullet + updated the run-cache bullet to "both workflows", § Usage Scenario F dropped its "(M5)" provisional marker, § Operational item 1 flipped to "landed" + item 2 (M6) reworded to "natural follow-on".

**M5a key decisions.** (a) **Job discovery is by the `dit_run_id` label, not the stored `dataflow_job_ids`** (always `[]` — neither runner captures the submitted job id back). `cancel_run` shells `gcloud dataflow jobs list --filter=labels.dit_run_id=<id> --format=json(id,name,state)` and cancels every **non-terminal** job — anything not in the terminal denylist (`Done`/`Failed`/`Cancelled`/`Drained`/`Updated`), so `Queued`/`Pending`/`Running`/`Cancelling` are all caught (a `Running`/`Pending` allowlist would have leaked queued jobs). A jobs-list call that *errors* (vs returns empty) is held distinct from "no jobs": with no rows it raises a discovery-failure `RuntimeError` (check auth/region), not the unknown-id `ValueError`, so a transient gcloud failure can't masquerade as a typo'd run_id. (b) **Table deletes are table-level only, gated by `_looks_like_table_fqn`** (exactly 3 non-empty dot-parts) — a dataset-shaped or malformed `output_tables` value is skipped with a warning, so a `dit_exp_*` snapshot dataset can never be deleted (the "don't manually delete shared `dit_exp_*` datasets" working agreement, enforced in code). (c) **pipe-gaps now stamps the `dit_run_id` label** — but via a different mechanism than port-visits: pipe-gaps submits Dataflow **in-process** (`dit.runners.dataflow`), so labels can't be `--labels=k=v` CLI flags; instead a `labels` list is threaded `_make_config` → `cfg.unknown_parsed_args["labels"]` → gfw-common `PipelineFactory` (spreads `**unknown_parsed_args`) → Beam `GoogleCloudOptions.labels`. Verified end-to-end against beam 2.73 that the list reaches `GoogleCloudOptions.labels`. (d) New `read_rows_for_run` deliberately does NOT filter `status`/`expires_at` (cleanup must see in-flight + cancelled rows). Idempotent throughout; `ValueError` on unknown run_id. **NOT exercised live** (tests mock gcloud + BQ): the `dataflow.jobs.cancel` + BQ table-delete perms for the laptop user + `automated-testing@` remain a user-gated verification.

**M5b key findings.** The PR #28 harness extraction made this small: port-visits supplies its own `WORKFLOW_FILE_SHA1` + mode-aware `canonical_params_dict` and calls the shared `dit.workflow.run_with_cache`. **Cache-relevant params for port-visits**: `mode`, `start`/`end`, `source_dataset_stem`, `named_anchorages`, `thinned_message_table` (it swaps step-2's input → output-affecting), plus `tail_days` for the daily-slice modes only (`1_bf` ignores it — same mode-aware rule pipe-gaps uses for `tail_days`/`backfill_days`). Flipped `ais.py` to `resolve_digest=True` (caching needs the worker-image digest in the key). `compare_all` now takes the per-mode cached-or-fresh FQNs rather than re-deriving from the current suffix (a hit reuses a prior run's UUID-suffixed table); the `--skip-pipelines` compare-only path keeps deriving from the current suffix. No architectural decision reversed; the existing run-cache design (Decisions A/B/C from M4) carried over unchanged. M6 (SIGTERM → `cancel_run`) is the next stage.

### 2026-05-29 — Refine M5 (port-visits cache + cleanup) + Phase 3 (pipe-events) plans

After landing the cloud-path REF fix (#27) and the `dit.workflow` harness extraction (#28) this session, refined the next two stages so they're build-ready and discoverable. **Sections moved:** `docs/run-cache-impl.md` § Milestone 5 rewritten into M5a (cleanup control plane) + M5b (port-visits cache); `docs/plan.md` § Phase 3 augmented with a "Refined plan (2026-05-29)" block; `README.md` § Roadmap Phase-3 row + Operational items 0–1 updated (item 0's "first real auto-build run" follow-up marked done); new memory [[next-stages-m5-pipe-events]].

**M5 key decisions.** (a) The harness extraction made M5b's wrapper free — port-visits just supplies its own `canonical_params_dict` + `CacheKey` and calls the shared `dit.workflow.run_with_cache` (flipping `ais.py`'s `resolve_digest=True`). (b) `cancel_run` must discover Dataflow jobs by the `dit_run_id` label (stored `dataflow_job_ids` are always `[]`), so M5a needs both workflows to stamp that label — port-visits does, pipe-gaps to verify. (c) Verify `dataflow.jobs.cancel` + BQ-delete for both the laptop user (`make dit-cancel`) and `automated-testing@` (M6 cloud SIGTERM).

**Phase 3 key findings.** pipe-events is a BQ-SQL / `_SESSION` / docker-runner consumer (not Beam/Dataflow), so it stresses the harness where the two Beam consumers couldn't: `add_infra_args` bundles Dataflow knobs it doesn't use (→ maybe split into dataset/dataflow groups), and the run-cache `worker_image_digest` doesn't map to a build-from-source BQ-SQL run (→ defer pipe-events caching). The bash original does no comparison, so the port's value-add is adding it (SCD-2 `_last_versions`). Refined extraction hypothesis: likely a small `dit.phases` for the per-mode `(start,end)` date-slice arithmetic, leaving per-slice execution per-workflow — more surgical than full `Phase`/`Mode`/`Oracle` dataclasses. No architectural decision reversed; plan-doc refinement + reference plumbing only.

### 2026-05-22 — No-dirty-tree pivot (planning); deprecate `--allow-dirty-tree`, auto-snapshot+push

After the M1-M4 cache rollout we ran two `make dit-cloud` builds against the same dirty pipe-gaps tree to demo cache hits — both produced cache MISSes because the cache's `pipeline_dirty=TRUE` filter (correctly) skipped both rows. Wasted ~60 min of Dataflow + ~$10 on byte-identical recomputes. The demo dead-ended on exactly the feature we'd designed to defend against, which surfaces a real cost: eight discrete pieces of dirty-tree-aware logic across `--allow-dirty-tree`, the `_dirty` table-suffix, the `pipeline_dirty` cache column, the `read_cache` filter, the `warn_if_worker_image_misses_dirty_tree` helper, the per-mode-write-but-don't-read pattern, the dirty-row tests, and the relevant memories.

The original plan never called for this. `make snapshot-<pipeline>` has existed since 2026-05-08 as the "test uncommitted changes reproducibly" pattern (git stash → temp branch → install from there). `--allow-dirty-tree` was a 2026-05-14 convenience shortcut that grew the surrounding scaffolding.

**Decision**: deprecate `--allow-dirty-tree`; under the pivot, every dit run executes a committed git ref. Dirty trees → auto-snapshot+push to `refs/dit-snapshots/<pipeline>/<commit-short-sha>` as an **orphan, content-addressable** commit (no `-p HEAD` parent — that would propagate unpushed ancestors and tie the snapshot SHA to the branch history; instead the parent SHA is recorded in the commit message `dit snapshot of <parent-sha>` and mirrored in a new nullable `pipeline_commit_parent` column on `dit_runs`). `git write-tree` against a temp index + `git commit-tree` with frozen author/committer dates and identities, so identical tree state always resolves to the same SHA — repeat runs of unchanged uncommitted code hit the cache; hidden ref namespace, invisible to GitHub UI, fetchable by anyone with repo read access. `pipeline_dirty` column renamed to `unreviewed_code` (sharper semantic — distinguishes "snapshot or ad-hoc branch" from "merged to main", which is what the dirty flag was a proxy for). The broad `make clean-snapshots` is dropped in favour of a surgical `make clean-snapshot REF=<sha>` aimed at secret-leak remediation (snapshots live forever by design otherwise). Five-PR migration spelled out in [`docs/no-dirty-tree-pivot.md`](docs/no-dirty-tree-pivot.md).

**User-facing impact** at the end of the pivot:
- Zero new ceremony for the common case (auto-snapshot is transparent — `make dit-cloud` works the same way for a dirty tree, just routes through a real ref).
- Every cache row is reproducible by anyone with repo read access.
- Cross-version + PR-validation queries can filter on `unreviewed_code = FALSE` for strict provenance, but cache hits still work for the user's own iteration loop.
- ~30 sec extra at first snapshot creation per new uncommitted state (snapshot + push); subsequent runs against same snapshot hit cache instantly.

**Trade-off accepted**: the auto-push pattern requires git-push permission on the pipeline repos. Same scope of users as the existing GCP Artifact Registry push permission (everyone who already runs `make publish-ditbox` or builds custom worker images). No new permission class.

**Cleanup**: `make clean-snapshots` (existing user-invoked target) extended to also delete the remote refs. No cron, no inline cleanup. Bytes-scale storage on origin makes "never clean up" acceptable too; cleanup target is for UX hygiene.

New memory [[no-dirty-tree-policy]] persists the policy across sessions. README § Usage scenarios concretises the end-state user flows. Plan-doc impact: `docs/run-cache.md` / `docs/run-cache-impl.md` will get the `unreviewed_code` rename in M-pivot-3; CHANGELOG gets Removed entries when each PR lands.

### 2026-05-22 — Run cache landed (M1–M4); content-addressable cache + registry + provenance in one table

`dit.cache` ships in four PRs landed in sequence (#16 → #17 → #18 → #19), implementing the design sketched in [`docs/run-cache.md`](docs/run-cache.md) and tracked in [`docs/run-cache-impl.md`](docs/run-cache-impl.md).

**Single table serves three jobs**: cache (skip recomputing main's 1_bf on every PR), registry (find Dataflow jobs + output tables a run produced — for cleanup), provenance ("which commit produced this table?"). Lives at `world-fishing-827.tech_great_expectations.dit_runs` (same dataset dit already writes to, so no new dataset / IAM grant — replaced the original `dit_meta` proposal which would have needed a terraform PR).

**Cache key** is `sha256(pipeline_commit + worker_image_digest + workflow_file_sha1 + canonical_params_json)`. The `workflow_file_sha1` makes it dit-side cache-buster: pure dit-library refactors don't invalidate (good — PR #10's `dit.job_names` extraction shouldn't have); workflow-file edits do. `worker_image_digest` is resolved from the tag at submit time so `:main` retags invalidate cleanly. `params` is mode-aware: `MODE_BF` doesn't include `tail_days`/`backfill_days` because that mode doesn't read them (Copilot catch on PR #19; including them would have dropped BF's hit rate for no behavioural reason).

**M3 pivot away from streaming inserts.** Initial write path used `bigquery.Client.insert_rows_json`; the first M3 smoke-test cleanup hit the 90-minute streaming-buffer rule that blocks UPDATE/DELETE against freshly-inserted rows. That would have broken M5's `cancel_run` UPDATE entirely (cancellations are by definition close to the write). Switched to parameterised DML INSERT (`client.query("INSERT INTO ... VALUES (@a, ...)").result()`), with `PARSE_JSON(@params_json)` server-side for the JSON column. Same cost (INSERT scans zero bytes), few-seconds latency invisible inside multi-minute Dataflow workflows, exactly-once per submission, rows immediately mutable. Documented under "Why DML INSERT" in `src/dit/cache.py` and the M3 CHANGELOG entry.

**Workflow integration (M4) in `workflows/pipe_gaps/mode_equivalence.py`.** Each mode's `execute_*` now flows through a `_run_with_cache(...)` wrapper that returns the FQN to use for downstream comparisons (cached or fresh). `dit.compare` is untouched — the cache-or-fresh swap happens at the workflow level, matching decision C from the M4 design discussion. Per-`main()` context (`run_id`, `pipeline_commit`, `pipeline_dirty`, `dit_commit`, `worker_image_digest`) is stamped on `args` early so all downstream code reads from one place. Dirty-tree runs still write rows (registry purpose); `read_cache` filters them out of lookups so they can't poison future caches.

**Still-stubbed in M5 + M6**:
- `cancel_run(run_id)` — looks up the row, cancels Dataflow jobs, drops output tables. Needs the runner to surface Dataflow job IDs (currently `dataflow_job_ids` is `[]` in every written row — TODO M5).
- `make dit-cancel RUN_ID=<id>` — the user-facing cleanup target.
- SIGTERM trap inside `dit run` — best-effort cleanup when Cloud Build cancels mid-flight.
- Port-visits cache integration (M5; structurally similar to pipe-gaps' M4, runs against `workflows/port_visits/ais.py`).

Six PRs total when M5/M6 land; the four merged today take 290 LOC of `dit.cache` + 295 LOC of workflow integration + 184 LOC of tests + a 40-line BQ migration. New memory [[dit-runs-cache]] persists the table location + cache-key shape across sessions.

### 2026-05-22 — First end-to-end validation: framework caught a real bug, custom worker image proved the fix

dit's first complete proof-of-value run, against the 2020 AIS-staging cohort:

1. `make dit-cloud PIPELINE=pipe-gaps WORKFLOW=workflows/pipe_gaps/mode_equivalence.py ARGS="--runner dataflow --parallel --allow-dirty-tree"` against the registry's `pipe-gaps:v0.9.6` worker image produced **320 / 307 / 331 differing rows** across the three pairwise mode comparisons (1_bf vs 2_bfd, 1_bf vs 3_bftruncate, 2_bfd vs 3_bftruncate). Diff pattern (identical `end_timestamp`, divergent `end_msgid` / `end_lat` / `end_lon`) matched the textbook signature of non-deterministic message-sort tie-breaking at boundary handoffs.
2. Diagnosis: workers ran v0.9.6's `_sort_messages` (SHA-1 `4c4d2de9941e` — just `operator.itemgetter(KEY_TIMESTAMP)`); the user's local tree on `PIPELINE-3974/source_table_time_travel` (committed `dfe662d` + uncommitted Boundaries extension) had the 5-tuple tiebreaker (SHA-1 `9a06bdb5659b`). The submitter's dirty changes never reached workers.
3. Built `gcr.io/world-fishing-827/dit/pipe-gaps:dit-fix-msg-sort-dfe662d-dirty-6720a4` from the dirty tree (`docker build --target=prod`, ~5 min) → pushed to the dit-namespace. Re-ran with `--worker-image=<that-image> --experiment-id=fix-tiebreaker`. **All three pairwise comparisons collapsed to 0 differing rows** in 41 minutes (build `df159e3f-042d-474c-9bb7-b1fef6069f67`, exit 0).

This validates: (a) the Cloud Build path end-to-end (`make publish-ditbox` → kaniko ditbox build → `make dit-cloud` → uv per-run install → Dataflow submission → BQ output → `dit.compare` verdict → exit-code propagation); (b) the mode-equivalence test surfaces real bugs rather than noise; (c) `dit.compare` returns the real diff count (PR #3 fix from 2026-05-18 actually working in production); (d) the recommended "custom worker image → `--worker-image` override" pattern works for testing pipeline-code changes.

Plan-doc impact: no architectural changes needed; `docs/plan.md` Phase 2 verification path stands. Memory entries [[dit-first-real-catch]] and [[submitter-vs-worker-split]] persist this across sessions.

### 2026-05-21 — Cloud Build runtime hardening (six PRs: #6–#11)

Six PRs land together as the Cloud Build runtime moves from "submitted; let's see what happens" to "production-shaped tooling". Listed in the order they hit `main`:

**PR #7 (`fix: pipe-gaps Dataflow job-name prefix dit-pipe-gaps`)**: `workflows/pipe_gaps/mode_equivalence.py` was still hardcoding `three-way-eq-...` as the Dataflow job_name prefix; only `port_visits/ais.py` had been migrated to `dit-<repo>-...` during the per-iteration labels work. One-line rename. BQ output-table prefix (`three_way_<suffix>`) intentionally left untouched so existing diff tables / ad-hoc queries against them keep working.

**PR #8 (`fix: split DEFAULT_WORKER_IMAGE from local image tag in pipe-gaps workflow`)**: pipe-gaps' workflow was passing `DEFAULT_IMAGE_TAG = "gfw/pipe-gaps:dev"` (unqualified local tag) as `sdk_container_image`, putting Dataflow workers into permanent `ImagePullBackOff`. Split the local-docker tag from the registry-published worker image (`us-central1-docker.pkg.dev/gfw-int-infrastructure/core/pipe-gaps:v0.9.6`), added `--worker-image` CLI flag, threaded it via `cfg.worker_image`. Dataflow branch in `_run_pipeline` now raises a clear `RuntimeError` if worker image is unset (no silent fallback to the local tag — that's what produced the original bug).

**PR #6 (`feat: kaniko-cached ditbox build + uv per-run pipeline installs`)**: `docker/ditbox/cloudbuild.yaml` switched from `gcr.io/cloud-builders/docker` to `gcr.io/kaniko-project/executor` (pinned by digest, since that registry doesn't publish versioned tags) with a registry-backed layer cache at `gcr.io/world-fishing-827/github.com/globalfishingwatch/kaniko-cache` (shared with `monitoring/cloudbuild`). `cloudbuild-dit.yaml` uses `uv pip install --system` for per-run pipeline installs; uv is baked into ditbox at `0.11.15`. Ditbox rebuilds drop from ~3 min to seconds on cache-hits; per-run installs drop from ~30–90s to ~5–10s.

**PR #9 (`fix: pin apache-beam at install time to match Dataflow worker image`)**: without an explicit constraint, the per-run uv install picked the newest apache-beam matching the pipeline's `~=` requirement; that drifted past whatever the published worker image was built with, and Dataflow rejected the submission with `Pipeline construction environment and pipeline runtime environment are not compatible`. New `_BEAM_VERSION` substitution in `cloudbuild-dit.yaml` writes a uv `--constraint` file; per-pipeline defaults wired into the `Makefile` (pipe-gaps → 2.71.0; anchorages_pipeline → 2.69.0). `--no-deps` dropped from both install paths (Copilot caught that it made the constraint a no-op).

**PR #10 (`refactor: centralise Dataflow job-name builder in dit.job_names`)**: new module `src/dit/job_names.py` with `to_safe_for_job_name` + `make_job_name(*, repo, step, experiment_id, mode=None, binding=None, iteration=None, total_iterations=None, max_len=63)`. Pipe-gaps now builds the job name from explicit semantic parts (`repo=pipe-gaps`, `step=detect`, mode constant, iteration counter) instead of synthesising from the output table's `three_way_<suffix>` name — the stale `three-way-eq` / `three_way` strings no longer leak into Dataflow job names. Port-visits' local `_make_job_name` now delegates. `execute_bfd` / `execute_bftruncate` / `execute_mutate_recover` count iterations explicitly (matching the `N-of-M` suffix port-visits already had). Sanitizer strengthened to a real `[a-z0-9-]` regex; overflow raises rather than slicing the load-bearing tail. 7 unit tests in `tests/test_job_names.py`.

**PR #11 (`feat: warn when submitter tree is dirty but worker image is the default`)**: new `src/dit/git_info.py` with `warn_if_worker_image_misses_dirty_tree(...)`. Logs a prominent banner when `runner == "dataflow"`, `worker_image == default_worker_image`, and the submitter's tree is dirty. Warning-only (legitimate cases exist — dirty workflow harness / docs / tests). Takes a `dirty_fn: Callable[[], bool]` (Copilot caught that an eager `dirty` parameter would force a `git status` shell-out even on the `--suffix` path). Both workflows call it from `main()`. 4 unit tests.

**Why all at once.** The 2026-05-22 validation run exercises everything in this PR train end-to-end. Without #7/#8 the run wouldn't start; without #9 the SDK versions wouldn't match; without #11 the failure mode of "submitter dirty, workers don't see it" would only surface during result analysis. The Cloud Build runtime is now load-bearing, not aspirational.

Memory entry [[submitter-vs-worker-split]] persists the cross-cutting lesson across sessions.

### 2026-05-15 — Image-namespace convention codified: `gcr.io/world-fishing-827/dit/*`

Two related decisions consolidated into a single convention, documented in [`docs/conventions.md`](docs/conventions.md) and reinforced by a Working agreement bullet above:

**Prod-infra boundary.** dit's user (christian.homberg@globalfishingwatch.org) is not in `gcp-backend-engineering-team`. Project-level `uploadArtifacts` on `world-fishing-827`: yes; on `gfw-int-infrastructure`: no; `repositories.create` anywhere: no. This isn't a deficiency to work around — it's the right shape: dit is testing, not production, and the IAM mirrors that. The boundary is now explicit in working agreements: stay in `world-fishing-827`, stay out of `gfw-int-infrastructure`, and stay clear of prod-shaped image namespaces even inside wf827. The one exception is creating branches in pipeline repos with potential fixes; the branch existing is dev workflow, not prod-touching.

**Image namespace.** All dit images go under `gcr.io/world-fishing-827/dit/*`. Path within the existing `gcr.io` AR repo, not a new repo — needs only `uploadArtifacts` (which we have). Tag conventions: `<image>:<sha>` for ditbox, `<pipeline>:<experiment-id>-<binding-name>` for per-binding worker images. Wf827 + same-project = no cross-project IAM for Dataflow worker pulls.

**Code changes in the same commit:** updated `docker/ditbox/cloudbuild.yaml` `_IMAGE` substitution and `cloudbuild-dit.yaml` step `name:` from the previous (unreachable) `gfw-int-infrastructure/core/ditbox` to `gcr.io/world-fishing-827/dit/ditbox`. The first `make publish-ditbox` attempt failed with `Permission 'artifactregistry.repositories.uploadArtifacts' denied` against the old path; the corrected target works under existing perms.

Memory entries [[prod-infra-boundary]] and [[dit-image-namespace]] persist this across sessions.

### 2026-05-15 — Cross-version worker-image gap surfaced; parallel bindings + per-binding override

Two related changes to `cross_version_ais.py` triggered by the first real-world use of the cross-version capability against PIPELINE-1465.

**Worker-image gap.** The first PIPELINE-1465 run reported 0 diff across all 12 output-table pairs (port_visits + port_events × 3 modes × 2 bindings). Investigation: `ais.py` hardcodes `--worker-image=us-central1-docker.pkg.dev/gfw-int-infrastructure/core/pipe-anchorages:v4.6.4` as the Dataflow `--sdk_container_image`. The `--build-from-source` flag rebuilds only the *submission-side* image; workers always pull v4.6.4. The PIPELINE-1465 fix lives in `pipe_anchorages/transforms/create_in_out_events.py` — a Beam PTransform that runs on workers — so both bindings ran identical worker code. TIC's "identical" verdict was the truthful answer for the data it was given; the test methodology was the failure.

This is an architectural gap in the cross-version capability for any change that lives in worker code (i.e. most pipeline fixes). Phase 2's mode-equivalence test wasn't affected because there only the mode-flags differ, not the underlying code.

Fix shape adopted: per-binding `--worker-image` override. New `--binding-worker-image NAME=IMAGE` flag (repeatable); the orchestrator threads it through `_ais_args_for_binding` so each binding's `ais.py` invocation sees the right image. Per-binding image build + push remains manual for now (target: `gcr.io/world-fishing-827/<image>:<tag>` since we have project-level `uploadArtifacts` there, and the registry is auto-created on first push). An in-orchestrator builder is a follow-up — blocked on the same IAM coordination that gates the proper Option-C ditbox repo (someone with `roles/artifactregistry.admin` on `world-fishing-827` needs to create a shared AR repo).

**Parallel bindings.** Same commit since both touch `cross_version_ais.py` and were independently desired. Bindings now run via `ThreadPoolExecutor` by default; `--sequential-bindings` opts out. Per-binding stdout is line-prefixed `[<binding>] ` via a reader thread. Failure semantics flipped from "abort siblings on first failure" to "let all complete, skip diff pairs touching a failed binding"; overall exit code non-zero iff any binding failed. `_run_binding` switched from `subprocess.run` to `subprocess.Popen` + a daemon reader thread to support per-subprocess stdout streaming.

Hazards considered: concurrent `git worktree add` against the same source repo (safe — git uses per-worktree lockfiles), `--build-from-source` cache races (each binding has its own worktree → own Dockerfile context), Beam temp-dataset table collisions (Beam EXPORT-staging uses UUID-suffixed table names → safe).

Next: rebuild + push the after-binding pipe-anchorages image to `gcr.io/world-fishing-827/pipe-anchorages-1465-after`, re-run cross_version_ais with the override, see what TIC surfaces with actually-differing worker code.

### 2026-05-15 — Cloud Build ad-hoc runtime + repo pushed public

Three pieces of Runtime & CI work landed together; tracked individually because they were sized as half a day collectively but had three separate review beats.

**GitHub remote.** Created `https://github.com/GlobalFishingWatch/data-integration-tests` (initially private). After a three-agent pre-publication review (credential scan / infra topology / prose-and-docs sensitivity) returned no blockers and two soft prose suggestions, flipped to public. The soft edits landed in `docs/context.md` (Bug 2 "Not yet fixed" -> "Fix proposed"; dropped the "Production VMS gaps continues to run with..." line) — see `5d21045`.

**`table_identical_checks` flipped public.** Prerequisite for clean `pip install` of `table-identical-checks @ git+https://...` from anywhere. The repo had no credentials in tracked files (`sa.json` was already gitignored); flip was a public-shape consistency move with the rest of the GFW pipeline ecosystem. Added the git URL to dit's `requirements.txt` so a fresh `pip install dit` brings `table-check` transitively (it's a real dep of `dit.compare`, was previously installed manually).

**`ditbox` image + `cloudbuild-dit.yaml` + `make dit-cloud`.** Per `docs/plan.md` § Runtime & CI items 1-3. Architecture revision from the original plan: dit itself is NOT baked into ditbox (the original plan said "dit pre-installed"). Now-public dit on GitHub makes `git clone @ _DIT_REF` per-run trivial and gives iteration on dit changes a faster inner loop (no ditbox rebuild required). Pipeline deps also install per-run via the source upload. Net: ditbox is a stable tooling layer that rarely changes; per-run installs are seconds-scale.

`cloudbuild-dit.yaml` runs as `automated-testing@world-fishing-827.iam.gserviceaccount.com` (matches the SA Dataflow already uses; avoids an impersonation hop). 24h timeout; `E2_HIGHCPU_8` machineType for the orchestrator (the actual compute is Dataflow worker-hours; the build VM just orchestrates). `options.logging: CLOUD_LOGGING_ONLY` to avoid the SA needing storage perms for the legacy GCS log bucket.

**Pending (Item 4 of validation, ahead of any cross-version PR-trigger work):** running `make publish-ditbox` for the first time, smoke-testing `make dit-cloud ARGS="--help"`, then a real AIS-staging single-binding run. Likely needs an IAM grant: the build-submitter principal needs `roles/iam.serviceAccountUser` on `automated-testing@`; the SA itself needs `roles/logging.logWriter`. Both surface naturally when the first build is submitted; documented in `docs/plan.md` § Next steps.

### 2026-05-15 — Synthetic branches for the PIPELINE-1465 cross-version test

To resolve the precondition flagged in the cross-version-glue entry below (every binding must support `--temp_dataset`), created two branches in `/mnt/encrypted_data/git/anchorages_pipeline`:

- **`tests/temp_dataset_for_integration_tests`** — points at `cb916bf` (current `dit-temp-dataset-support` HEAD). Has the `--temp_dataset` patch on top of `4df3726` (current main). No port-gap fix.
- **`tests/pipeline_1465_for_integration_tests`** — based on the above, with `c1906ec` (`Fix PORT_GAP_BEGIN anchorage when vessel silently changes port`) cherry-picked on top. New HEAD is `657c584`.

Minimal A-vs-B diff: 3 files, 115 insertions — `CHANGES.md` (entry), `pipe_anchorages/transforms/create_in_out_events.py` (the 6-line behaviour fix), and `tests/test_create_in_out_events.py` (regression test). Nothing else differs, so any output divergence from the cross-version run is attributable to the fix.

Dry-run validated through `cross_version_ais.py` with these bindings on 2026-05-15:

```
dit run workflows/port_visits/cross_version_ais.py \
    --experiment-id pipeline-1465 \
    --pin-source-at 2026-05-15T10:00:00Z \
    --binding before=tests/temp_dataset_for_integration_tests \
    --binding after=tests/pipeline_1465_for_integration_tests \
    --modes 1_bf \
    --runner dataflow --parallel --build-from-source
```

The dry-run goes through `git rev-parse` of both refs (`cb916bf` and `657c584`), creates `dit_exp_pipeline_1465_{internal,published}` snapshot datasets, snapshots the three input tables at the pin timestamp, sets up and tears down worktrees for each binding. Removing `--dry-run` flips this into the real run.

Both branches are **local-only and intentionally untracked upstream**; they're scaffolding for the integration test, not branches to be merged. When the `--temp_dataset` PR lands upstream, the better long-term shape is to rebase the bindings on top of the merged version and drop these synthetic branches.

### 2026-05-15 — Cross-version experiment glue (port-visits AIS)

`workflows/port_visits/cross_version_ais.py` ties together the BQ snapshot helpers (`42ef37f`) and experiment-ID linkage (`244521d`) into an end-to-end command. Given `--experiment-id`, `--pin-source-at <iso>`, and N `--binding name=ref` pairs, it:

1. Verifies refs exist in `$PROJECTS/anchorages_pipeline`.
2. Creates `dit_exp_<sanitized_exp_id>_{internal,published}` snapshot datasets (7-day default expiration).
3. Snapshots the three port-visits input tables (`messages_positions`, `segment_info`, `segs_activity`) at the pin timestamp into those datasets.
4. For each binding: `git worktree add` at the ref, runs `ais.py` from the worktree with `--source-dataset-stem=<snap>` and `--suffix=<exp>-<binding-name>` (deterministic so the diff step doesn't need INFORMATION_SCHEMA discovery), tears down the worktree.
5. For each mode in `--modes`, diffs the corresponding output tables pairwise across bindings on `visit_id`.

Diff outcomes are reported but do not fail the run — non-empty diff is *information* for cross-version testing, not error. Real failures (missing ref, snapshot error, ais.py exits non-zero) exit non-zero.

`--dry-run` runs every step except the ais.py invocations and the diff phase — useful for validating orchestration without Dataflow cost. Validated end-to-end this way against (`v4.6.4`, `fix/PIPELINE-1465_port_visit_start_location`) on 2026-05-15.

**Precondition for actually using this on real bindings.** Every binding's pipe-anchorages source must support the `--temp_dataset` CLI flag — without it, the Dataflow SA hits the BQ EXPORT-staging permission error (see the 2026-05-15 Phase 2 entry below). The flag lives on the local `dit-temp-dataset-support` branch; PR pending. For the immediate PIPELINE-1465 cross-version test we want to motivate, the cleanest path is to cherry-pick the `--temp_dataset` patch onto both comparison refs (or land it upstream first) before invoking `cross_version_ais.py`.

### 2026-05-15 — `--experiment-id` / `DIT_EXPERIMENT_ID` for cross-version run linkage

- Output-table suffix shape grows a leftmost slot: `<experiment_id>_<commit>[_dirty]_<uuid>`. Leftmost so BQ prefix scans cluster by experiment naturally. `<uuid>` slot preserved so parallel mode-equivalence runs sharing a commit still don't clobber each other. `--suffix` (full manual override) bypasses the experiment-id slot entirely — byte-equivalent backward-compat guarantee.
- New `--experiment-id <slug>` flag on both `workflows/pipe_gaps/mode_equivalence.py` and `workflows/port_visits/ais.py`. Env-var fallback `DIT_EXPERIMENT_ID` (matches the established `DIT_*` convention; empty string treated as unset). Auto-default `solo_<6-hex>` when neither flag nor env var is set — the literal `solo_` prefix marks "not part of a cross-version experiment" so BQ filtering can ignore them. Validation regex `^[a-z0-9][a-z0-9_-]{0,31}$` compiled once at module level; invalid input raises `SystemExit` with a clear message (applied to both CLI input and env-var defaults).
- This is **the second half of the cross-version experiments capability** that started with the `dit.bq.snapshot_*` helpers (entry above). The two halves are decoupled on purpose: snapshots pin source data; experiment-id clusters output tables. Either is useful alone; together they enable end-to-end byte-equivalence runs across pipeline versions.
- `_git_info` stays duplicated across the two workflows. Anchored in decision 7 (duplicate-until-3): defer extraction to `dit.git_info` until pipe-events lands (Phase 3); this change is parallel edits, not shared-behaviour drift.
- Backward-compat guarantees honoured: (1) when neither flag nor env var is set, the auto-generated `solo_<6-hex>_<commit>_<uuid>` is still unique-per-invocation (the `<uuid>` ensures uniqueness; `solo_<6-hex>` adds clustering); (2) `--suffix` full override produces byte-identical output to today; (3) `--allow-dirty-tree` semantics unchanged (`_dirty` still appears between commit and uuid).
- No changes to table-name builders, comparison logic, runners, or `dit.bq` / `dit.compare` / `dit.dates`. No `docs/plan.md` text changes — no architectural decision changed; this is a thin workflow-side feature. `README.md` § Features: "Workflow file conventions" bullet extended to mention the new flag/env var and the `solo_<6-hex>` default shape.

### 2026-05-15 — `dit.bq` snapshot helpers for source-data pinning

- Added `dit.bq.snapshot_table(source, dest, *, as_of=None, expiration=None, project=..., if_not_exists=False)` and `dit.bq.snapshot_dataset(source_dataset, dest_dataset, *, tables=None, as_of=None, expiration=None, project=...)`. Both shell out to `CREATE SNAPSHOT TABLE … CLONE …` DDL; the dataset variant lists tables and loops, skipping any already present in dest (idempotent) and raising if dest dataset doesn't exist.
- This is **the first half of an upcoming cross-version experiments capability** — snapshot the inputs once so cross-version test runs (`pipe-gaps@main` vs `@pr-NNN`, etc.) only see differences attributable to the pipeline code, not source-data drift. The second half (experiment-ID linkage into output-table suffixes) is being designed by a parallel agent and is intentionally not coupled to these helpers; consumers can use the snapshots today without the experiment plumbing.
- BQ snapshots chosen over time-travel-in-queries: pipeline-agnostic (no source changes to pipe-gaps / pipe-anchorages / pipe-events), persists beyond BQ's 7-day time-travel window, and storage is delta-only. Docstring on `snapshot_table` carries the rationale.
- Lazy-imports `google.cloud.bigquery` inside both helpers (deviates from the existing top-import in `dit/bq.py`, but matches the spec instruction; in-place rewrite of the existing helpers' imports was out of scope).
- `tests/test_bq.py` (new): 11 mock-based tests covering DDL shape (plain / `as_of` / `expiration` / `if_not_exists` / both clauses / custom project) plus dataset-level cases (list-and-snapshot, table filter, skip-existing, raise-on-missing-dest, forward kwargs).
- Docs updated in the same commit: `docs/plan.md` § "Public API contracts → `dit.bq`" now lists both signatures with one-line notes; `README.md` § Features extends the BQ utilities bullet to mention the snapshot helpers.

### 2026-05-15 — Fix `dit.runners.docker` network-teardown defect

Follow-up to the defect flagged in the prior 2026-05-15 entry. `dit.runners.docker.run` (build_from_source path) now wraps the docker invocation in a `try/finally` and calls a new `_teardown_compose_network()` helper that runs `docker network rm <project>_default` after each call. Used `docker network rm` directly rather than `docker compose -p <name> down` so cleanup doesn't depend on a compose file being present in CWD; external volumes (e.g. the `gcp` auth volume) aren't touched. Idempotent — silently no-ops if the network is gone, in use, or never existed. No contract change to `docker.run()`; signature identical.

### 2026-05-15 — Phase 2 AIS-staging verification: passed; integration findings

First end-to-end run of `workflows/port_visits/ais.py --runner dataflow --parallel --build-from-source` against the staging cohort. Suffix `cb916bf_94dde7`, output in `world-fishing-827.tech_great_expectations.port_visits_..._{1_bf,2_bfd,3_bftruncate}`. All three pairwise comparisons returned `rc=0` on `visit_id` — **port-visits is mode-equivalent across bf / bfd / bftruncate on the 2020 AIS-staging cohort.**

Four integration issues surfaced during the run; each is now fixed or documented:

1. **`--labels` required by pipe-anchorages** (fixed, `18689dc`). `transforms/sink.cloud_to_labels` iterates `cloud_options.labels` without a None guard. `_dataflow_pipeline_options` now emits five `--labels=k=v` flags matching the shape composer uses.
2. **No `--temp_dataset` plumbing in pipe-anchorages** (fixed by local upstream patch). The `automated-testing@` SA lacks `bigquery.datasets.create`, so Beam's auto-named `beam_temp_dataset_<uuid>` fails. Pipe-gaps' workflow sidesteps this in-process via `_DagFactoryWithTempDataset`; pipe-anchorages runs Beam inside a container with no equivalent hook. Local patch on `anchorages_pipeline@dit-temp-dataset-support` (commit `cb916bf`) adds a `--temp_dataset` CLI flag + threads it through `QuerySource` to `ReadFromBigQuery`. Our workflow surfaces it as `--bq-temp-dataset` with `DIT_BQ_TEMP_DATASET` env-var fallback (`9227cb8`). **Upstream PR pending team review.**
3. **Workers couldn't `import pipe_anchorages`** (fixed, `3a400ee`). Default Beam SDK image doesn't have pipe_anchorages installed. Workflow now passes `--sdk_container_image=us-central1-docker.pkg.dev/gfw-int-infrastructure/core/pipe-anchorages:v4.6.4`. Default is the published v4.6.4 image; workers don't need the local `--temp_dataset` patch because that change only affects local pipeline construction.
4. **SA needs dataEditor on the output dataset.** `scratch_christian_homberg_ttl120d` doesn't grant `automated-testing@` `bigquery.tables.create`. `.envrc` switched to `DIT_DEST_DATASET=tech_great_expectations` (team-shared, SA pre-blessed). Personal scratch usage requires a one-time IAM grant — documented in `.envrc`.

**Known follow-up — `dit.runners.docker` network-cleanup defect.** The `build_from_source` path runs `docker compose -p <unique-uuid> run --rm dev …` per invocation, which leaves a `<project>_default` bridge network behind even after the container exits. Across many runs Docker's address pool (default 172.16-172.31, /24 each) fills up and new networks fail with "all predefined address pools have been fully subnetted." Worked around once via `docker network prune`, but the runner should `docker compose -p <name> down` after each invocation. Not blocking, but worth fixing alongside Phase 2 (relevant to anyone running `--build-from-source` workflows repeatedly).

**No plan-doc text changes this entry** — purely a record of what worked and what got patched along the way. The `docs/plan.md` § Phase 2 verification path stands.

### 2026-05-14 — Per-user infra knobs via DIT_* env vars

- Both workflows now resolve user-overridable infra knobs through `os.environ.get("DIT_<NAME>", "<default>")` with corresponding CLI flags that override env vars per-invocation. Set up so a single `export DIT_DEST_DATASET=scratch_chris` in `.envrc` redirects all output tables for personal dev without editing source.
- The knob set (applies to both workflows except where noted):
  - `DIT_DEST_DATASET` -> output dataset for per-mode tables (new — was hardcoded).
  - `DIT_DATAFLOW_SA` -> service account for Dataflow workers.
  - `DIT_DATAFLOW_REGION` / `DIT_DATAFLOW_TEMP_BUCKET` / `DIT_DATAFLOW_SUBNETWORK` -> dataflow placement.
  - `DIT_BQ_TEMP_DATASET` -> pipe-gaps-only; defaults to `${PROJECT}.${DIT_DEST_DATASET}` if unset.
- Added `--dest-dataset` CLI flag to both workflows (was missing on both). Other infra flags already existed.
- Per-workflow `DEST_DATASET` constant removed; table-name helpers now take `args` and read `args.dest_dataset`. Callsites in `_run_slice` and `compare_all` updated.
- `PROJECT` and pipeline-specific knobs (image tag, source dataset stem, tuning params, dates) deliberately kept as constants/CLI flags only -- they're not "per-user infra" and an env-var explosion adds more noise than value.
- `.envrc.example` rewritten to document the full env-var set with team defaults inline; users uncomment what they want to override. README's install section already points at `.envrc.example`.

### 2026-05-14 — Phase 2 spike: AIS-staging port-visits workflow

- Added `workflows/port_visits/ais.py` — first port-visits workflow (AIS staging cohort, 2020-only, reduced data). Three modes (bf / bfd / bftruncate); two-step thin_port_messages → port_visits chain per slice; partitioned-write semantics in both pipe-anchorages steps mean re-runs over overlapping date ranges are idempotent (verified by reading pipe-anchorages source).
- This is the **abstraction-validation step**: first real exercise of `dit.compare.compare_tables(view_suffix="", keys=["visit_id"])` (truncate shape, no SCD-2) and the docker runner's `entrypoint` extension (`entrypoint="pipe-anchorages"`).
- Default `--runner=dataflow` matches what gaps recently used. The runner here is `dit.runners.docker` + Beam pipeline options inside the container (`--runner=DataflowRunner --wait_for_job ...`). Different from pipe-gaps' workflow which uses `dit.runners.dataflow` in-process — pipe-anchorages doesn't expose a `gfw.common.beam.pipeline.Pipeline`-shaped object the in-process runner could consume, so the workflow submits via the container CLI like composer's `KubernetesPodOperator` does. This divergence is worth knowing when Phase 5 considers extracting a unified runner primitive.
- Date semantics: AIS workflow uses **inclusive** `--start`/`--end` to match pipe-anchorages' CLI. Pipe-gaps' workflow uses **half-open** dates. The wart is unavoidable given the downstream tools' contracts and is documented in the workflow header.
- `_git_info` was lifted verbatim from `workflows/pipe_gaps/mode_equivalence.py`. When pipe-events lands as the third consumer (Phase 3), promote into `dit.git_info`.

### 2026-05-14 — Add reproducible-install targets (snapshot + specific-ref)

- Added six Makefile targets covering reproducible pipeline installs alongside the editable ones:
  - `install-<pipeline>-ref REF=<sha-or-branch>`: `pip install --force-reinstall --no-deps "git+file://<dir>@<ref>"` — non-editable, exactly that commit, ~5-10s.
  - `snapshot-<pipeline>`: uses `git stash create` to capture tracked working-tree changes into a real commit (working tree untouched), anchors on a `dit-snapshot-<epoch>` branch so git GC keeps it alive, then installs from that ref.
  - `clean-snapshots`: GCs the `dit-snapshot-*` branches across all three pipeline checkouts.
- Added `scripts/snapshot-install.sh` and `scripts/clean-snapshots.sh` to keep the git plumbing out of the Makefile recipes. `set -euo pipefail`, single-purpose, ~30 lines each.
- `FULLDEPS=1` toggles `--no-deps` off for the rare case where the target ref bumped or added a transitive dep.
- Trade-offs vs editable installs documented in `README.md`: the non-editable mode adds ~5-10s pip rebuild per iteration (acceptable given the integration-test cadence) and points the debugger at the installed snapshot rather than your dev tree (the only un-mitigated cost). `git stash create` captures **tracked** changes only — untracked source files need `git add` first.
- These targets are also the foundation for cross-version testing per `docs/framework-vision.md` § 6: same Makefile + script can install `pipe-gaps@main` and `pipe-gaps@pr-NNN` side-by-side for a single workflow invocation.

### 2026-05-08 — Restructure install: drop workflow deps from base, add Makefile

- `requirements.txt`: dropped `gfw-common[bq,beam]` and `apache-beam[gcp]`. Neither is imported by anything under `src/dit/` (verified by grep) — `gfw-common` is workflow-shaped and pipe-gaps declares it as a direct dep (`gfw-common[bq,beam]~=0.10`), so installing pipe-gaps brings it transitively. apache-beam is in the same boat (lazy-imported by `dit.runners.dataflow`; transitively pulled by `gfw-common[beam]`). Keeping them in the base required every consumer of `dit` to depend on the GFW private index even if they only used the docker runner.
- Added `Makefile` with `install-pipe-gaps` / `install-port-visits` / `install-pipe-events` / `install-all` targets. Each runs a single `pip install -e ".[dev]" -e $(PROJECTS)/<pipeline>` so workflow deps install **editable** — switching branches in `pipe-gaps` etc. is picked up without a reinstall. pyproject `[project.optional-dependencies]` was considered but rejected: PEP 508 `@ file://...` extras install as built wheels, defeating editable mode and creating a stale-snapshot footgun when iterating on pipe-gaps branches.
- `PROJECTS` defaults to `$(realpath ..)` (sibling checkouts). Override via env var or by copying `.envrc.example` → `.envrc` (gitignored; loaded by direnv).
- `docs/plan.md` § "Repo layout (Phase 1, concrete)": updated `requirements.txt` comment to reflect the framework-only deps and added `Makefile` / `.envrc.example` to the tree.
- `README.md`: install section now points at `make install-pipe-gaps` and documents the `PROJECTS` env var.

### 2026-05-08 — Initial architectural alignment (pre-implementation)

- Added `docs/plan.md` § **Architecture: three-repo split** — explicit ownership boundaries between processing repos, `composer-dags-production`, and `data_integration_tests`, plus where `table_identical_checks` sits.
- Extended `docs/plan.md` § **Decisions (recommended)** with items 6–9: `dit` library-first; workflow file location policy (canonical in `dit/workflows/`, in-repo allowed for spikes, no duplication); per-pipeline config dataclasses stay in composer-dags; `dit.compare` is a thin shim.
- Added `docs/plan.md` § **Public API contracts (Phase 1)** — typed signatures for `dit.runners.{base,docker,dataflow}`, `dit.compare.compare_tables`, `dit.bq` helpers, `dit.dates`, plus the workflow entry-point convention (`def main(argv=None) -> int`).
- Added `docs/plan.md` § **Phase 1 subagent task breakdown** — five tracks (1–3 parallelisable, 4 depends on 2+3, 5 last).
- Confirmed Phase 4 param sync is pull-based (`dit sync-params --from <composer-dags-checkout>`); plan text already aligned, recorded for traceability.

### 2026-05-08 — Track 3 dataflow-runner contract refinements

- `docs/plan.md` § **Public API contracts (Phase 1) → `dit.runners.dataflow`**: added two parameters not in the original signature.
  - `pipeline_builder: Callable[[Mapping[str, Any]], Any]` (required) -- the original `_run_dataflow` constructed `DetectGapsConfig` / `DetectGapsLinearDagFactory` directly. Hardcoding those into a shared runner would re-couple `dit` to pipe-gaps and break decision 5 (three consumers from day one). The workflow now passes a builder that returns a `gfw.common.beam.pipeline.Pipeline`-shaped object; the runner only owns the lock-split submit/wait around it.
  - `dag_factory_cls: type | None = None` (optional) -- `_DagFactoryWithTempDataset` ports across as a generic on-the-fly subclass that overrides `read_from_bigquery_factory`. Workflows pass their own factory class; the runner wraps it when `bq_temp_dataset` is set and forwards the wrapped class through the options mapping.
- `env` parameter is kept for `Runner`-protocol parity but the dataflow runner logs-and-ignores it (in-process; no subprocess to forward to).
- Docker runner: `build_from_source=True` switches to `docker compose -p <name>-<uuid> run --rm dev <args>`; the published path is `docker run --rm --name <name>-<uuid> <image_tag> <args>`. Both keep per-call uniquification to avoid the network race documented in the source.

### 2026-05-08 — Track 4 docker-runner contract extension

- `docs/plan.md` § **Public API contracts (Phase 1) → `dit.runners.docker`**: added `entrypoint: str | None = None`. Required by Track 4: pipe-gaps' dev image has no default `pipe-gaps` entrypoint baked in, so the original test passed `--entrypoint pipe-gaps` to `docker compose run`. Without runner-level support, the workflow would have to bypass the runner. The parameter is a clean opt-in: workflows whose images bake the right entrypoint omit it.

### 2026-05-08 — Track 6 review fix

- `pyproject.toml`: added `pythonpath = ["src"]` under `[tool.pytest.ini_options]`. Without it, `pytest tests/` fails to import `dit.*` unless run with `PYTHONPATH=src` or after `pip install -e .`. No code changes; tooling-only.

### 2026-05-08 — Track 2 utility-module contract refinements

- `docs/plan.md` § **Public API contracts (Phase 1) → `dit.bq`**: settled `query_for_restricted_ssvids` kwargs as `(reference_table, *, mid, backfill_days_w, seed=42, project=…)`. Dropped the source's `n_hours_before` argument (unused at the call site; logged as such by the source itself). Added a note that `drop_tables` requires `<project>.<dataset>.<stem>` form so the dataset can be enumerated.
- `dit.compare.compare_tables` keeps the `ignore_columns` parameter from the contract but raises `NotImplementedError` if passed non-empty: `table-check summary` does not yet support an ignore-columns flag, and CLAUDE.md prohibits reimplementing comparison features in the shim. The signal pushes the feature upstream into `table_identical_checks`. `tolerance` is forwarded as `--tolerance=<col>:<value>` (table-check's per-column syntax).
- `dit.dates.daterange_inclusive` is half-open (`start <= d < end`) despite the name -- preserved verbatim from `pipe-gaps` so the four-mode equivalence test stays byte-equivalent across the move. Pinned by `tests/test_dates.py`. If the migration ever wants true inclusive semantics, it has to flip the call sites simultaneously.
