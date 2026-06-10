# dit workflow orchestration evaluation — 2026-06-10

Point-in-time evaluation of how the six workflows handle **orchestration configuration**, on three axes: (1) flavours (source cohort + date ranges), (2) mode composability (bf-only / bf+bfd mix-and-match), (3) evaluation configuration (TIC keys, comparison skippability, exit-code semantics). Follow-up to [`workflow-reconciliation-2026-06.md`](workflow-reconciliation-2026-06.md) (the 2026-06-05 structural review); this doc is narrower (config surface, not code sharing) and post-dates the canonical-dataset migration (M1–M5), the outage_recovery 3-stage reshape (PR #63), and the source-mutation primitive (M6a/M6b).

**Verdict: locally sensible, globally drifted.** Each workflow's choices are defensible in isolation; the inconsistencies are accidents of accretion order, and they cluster differently per axis. Axis 2 is the genuinely inconsistent one; axis 1 has a missing concept (`dit.cohorts`) rather than wrong choices; axis 3 is mostly healthy with one deliberate asymmetry worth documenting.

## Axis 1 — Flavours (source cohort + date ranges)

| Workflow | Source addressing | Date shape |
|---|---|---|
| `pipe_gaps/mode_equivalence` | per-table FQN (`--source-messages`, `--source-segments`) | `--start/--end` **half-open** + tail/backfill days |
| `pipe_gaps/outage_recovery` | per-table FQN (same idiom) | 5-date outage geometry (`--start/--backfill-end/--outage-start/--outage-end/--end`) |
| `port_visits/ais` | stem **+** per-table FQN overrides (since M4) | `--start/--end` **inclusive** + tail days |
| `port_visits/cross_version_ais` | stem only (snapshots FROM the stem) | none (delegates to ais.py) |
| `pipe_events/fishing` | **dataset-pair** (`--internal-ds`, `--published-ds`) + aux tables | `--start/--end` + tail days |
| `pipe_segment/identity_match_key` | single table + opt-in prod satellite shards | **`--date-range start,end`** (single flag) |

### Findings

1. **Source addressing converged organically toward per-table FQNs** (pipe-gaps always; port-visits since M4; pipe-segment trivially single-table). The two holdouts have partial justification: pipe-events genuinely consumes many tables by dataset convention (BQ-SQL pipeline); cross_version_ais snapshots *from* a stem. But no policy states "per-table FQN is the canon" — it's convention by accretion. The dit owner's stated principle: **datasets-as-flags should be rare** — only for pipelines with many input tables or steps; small-input pipelines take FQNs.
2. **Date-flag shape varies three ways**, and `--start/--end` **semantics silently differ** (half-open in pipe-gaps, inclusive in port-visits/pipe-events — a documented wart driven by the downstream CLIs' contracts). Unavoidable in semantics; the only guard today is help-text discipline.
3. **"Flavour" doesn't exist as a concept.** Staging-vs-prod-VMS is "defaults vs manually override every source flag + know the right date window yourself". That gap is exactly what bit twice (PR #37 pipe-events flip; PR #45 outage-recovery flip — cohort name read as data date) and is what the reconciliation review's **`dit.cohorts`** recommendation addresses: a named bundle of (per-pipeline source FQNs, `data_start`/`data_end`, `snapshot_date`). A `--cohort staging-ais` switch, with per-table FQN overrides retained, would make flavours first-class and kill the date-window bug class structurally. **Highest-leverage gap on this axis — already recommended 2026-06-05, never built.**

## Axis 2 — Mode composability

| Workflow | Mode selection |
|---|---|
| `cross_version_ais` | ✅ `--modes` (comma list) |
| `mode_equivalence` | ❌ always all (bf+bfd+bftruncate, + mutate_recover conditionally) |
| `port_visits/ais` | ❌ hardcoded `[MODE_BF, MODE_BFD, MODE_BFTRUNCATE]` |
| `pipe_events/fishing` | ❌ runs all three unconditionally |

**This is the genuinely inconsistent axis.** One workflow has the flag; the three workflows where a bf-only smoke would be most wanted don't. Today's mitigation is the run cache (re-runs of unchanged modes are hits), but a **first** run on new params always pays for all modes — there is no cheap bf-only smoke against a new cohort / pin / image.

**Recommended fix (small, additive):** a `--modes` flag on the three mode-family workflows, defaulting to the full set, mirroring `cross_version_ais`'s exact shape. Because each mode is already cached *independently* (per-mode cache keys), `--modes 1_bf` today + `--modes 1_bf,2_bfd` tomorrow gives a bf cache hit on the second run — **the mix-and-match composability falls out of the existing per-mode cache design for free.** Comparisons run only pairs where both sides exist (copy `cross_version_ais`'s skip-pairs pattern).

**Explicit non-goal:** do NOT generalize into a `dit.phases` / stage-composition framework. That extraction was declined 2026-05-29 (`workflows/README.md`: <20% similarity across the three execute-body families), and mode-subset selection doesn't need it.

## Axis 3 — Evaluation / TIC configuration

1. **Compare keys are hardcoded per workflow — and that's correct.** Keys are a property of the output schema (`("gap_id", "start_timestamp")` for SCD-2 gaps, `visit_id`, `event_id`, pipe-segment's per-output-table dict), not a user choice; CLI-configurable keys would invite wrong-key comparisons that report false IDENTICAL. **pipe-segment's `COMPARE_KEYS` dict (per output table) is the most evolved shape** — the right template if a workflow grows multiple compare targets. No `tolerance`/`ignore_columns` used anywhere; the `dit.compare` shim's `NotImplementedError` on `ignore_columns` keeps comparison features flowing upstream into `table_identical_checks` per the working agreement.
2. **Exit-code semantics split by family — deliberate and consistent:** *equivalence* workflows (mode family + outage_recovery) FAIL on diff; *cross-version* workflows (cross_version_ais, identity_match_key) report diff as INFORMATION (the diff is the experiment's finding, not an error). Right design, consistent within family — but documented only in docstrings; nothing a new workflow author would naturally trip over. Should be one section in `docs/conventions.md` or `workflows/README.md`.
3. **Ragged edge — harness-flag coverage.** `--skip-comparisons` / `--skip-pipelines` (compare-only) exist on the 4 equivalence-family workflows but not the 2 cross-version ones (which carry `--dry-run` instead). Partially justified by lifecycle differences, but "can I re-run just the comparison phase?" has a different answer per workflow for no principled reason.

## Recommendations (by leverage)

| # | Item | Axis | Effort | Status |
|---|---|---|---|---|
| 1 | **`dit.cohorts`** — named (source FQNs, data window, snapshot date) bundles; `--cohort` flag with per-table overrides retained | 1 | Medium | Recommended 2026-06-05, re-affirmed here; not started |
| 2 | **`--modes` on mode_equivalence / ais / fishing** — mirror cross_version_ais's flag; comparisons skip absent pairs | 2 | Small (3 parallel edits) | Not started |
| 3 | **Document the equivalence-vs-cross-version exit-code contract** (+ optionally unify skip-flag coverage) | 3 | Small | Not started |
| — | `workflows/pipe_gaps/_detect.py` dedup (reconciliation § 3-d) | orthogonal | Small-medium | Open; correctness hygiene, not orchestration design |

## Out of scope

- Making compare keys CLI-configurable (correctness anti-feature; see Axis 3 finding 1).
- `dit.phases` stage-composition framework (declined 2026-05-29; unchanged).
- Normalising `--start/--end` half-open vs inclusive semantics (driven by downstream CLI contracts; the wart is the honest representation).
