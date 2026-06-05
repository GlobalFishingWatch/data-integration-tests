# dit workflow reconciliation review — 2026-06-05

Point-in-time review of dit's workflow inventory, the trajectory that produced it, and concrete reconciliation opportunities. Companion to [`snapshot-edge-cases-2026-06.md`](snapshot-edge-cases-2026-06.md) (the empirical audit of `dit.bq.snapshot_*` that came out of the same investigation).

**Scope**: the six workflow files under `workflows/`, the shared scaffolding in `src/dit/workflow.py` and `src/dit/runners/*`, and the question of where source-dataset / time-range defaults should live.

**Status**: snapshot of the trajectory and design space as of 2026-06-05. Revisit when a fourth workflow lands in any pipeline family or when composer-dags configs become a more frequent moving target.

## 1. Workflow inventory + side-by-side comparison

| File | Pipeline | Runner | Mode set | Default window | Default source | Compare shape | Compare keys | Cache? |
|---|---|---|---|---|---|---|---|---|
| `pipe_gaps/mode_equivalence.py` | pipe-gaps detect | `dit.runners.dataflow` (in-process Beam) | `1_bf` / `2_bfd` / `3_bftruncate` / `4_mutate_recover` | `2020-01-01` → `2021-01-01` (half-open) | Two FQN flags: messages in `pipe_ais_test_..._internal`, segs in `..._published` | SCD-2 (`view_suffix="_last_versions"`) | `(gap_id, start_timestamp)` | Yes (mode-aware key) |
| `pipe_gaps/outage_recovery.py` | pipe-gaps detect | `dit.runners.dataflow` (also `docker`) | `5_outage_recovery` / `5_outage_oracle` | `2020-08-22` → `2020-08-29` (7 days, just fixed) | Same staging FQNs as mode_equivalence | SCD-2 (`view_suffix="_last_versions"`) | `(gap_id, start_timestamp)` | Yes |
| `port_visits/ais.py` | pipe-anchorages port-visits | `dit.runners.docker` (Beam in container) | `1_bf` / `2_bfd` / `3_bftruncate` | `2020-01-01` → `2020-12-31` (inclusive both ends) | `--source-dataset-stem` (`_internal`/`_published` appended) | Truncate (`view_suffix=""`) | `(visit_id,)` | Yes (M5b) |
| `port_visits/cross_version_ais.py` | pipe-anchorages | Wraps `ais.py` per binding via `subprocess.Popen` + git worktree | Modes forwarded from `ais.py` | `--pin-source-at` required | Snapshotted from `--source-dataset-stem` | Same as `ais.py`, pairwise across bindings | `(visit_id,)` | n/a (delegates) |
| `pipe_events/fishing.py` | pipe-events fishing | `dit.runners.docker` (BQ-SQL via container) | `1_bf` / `2_bfd` / `3_bftruncate` | `2020-01-01` → `2021-01-01` (half-open) | Two FQN flags: `--internal-ds` / `--published-ds`; plus `--pipe-static`, `--pipe-regions-layers` | Truncate (compares `_fishing_events` + `_product_events_fishing` views) | `(event_id,)` | No (deferred) |
| `pipe_segment/identity_match_key.py` | pipe-segment | `dit.runners.docker` (Beam in container) | Cross-version A/B over `segment` + optional downstream | `--date-range` required | `--source-normalized-table` (single FQN; staging cohort default) | Truncate, per-output-table; keys vary by table (e.g. `msgid`, `seg_id`, `frag_id`) | Multiple, per-table | No |

**Non-obvious knobs by file** (the easy-to-miss configuration surface):

- **`mode_equivalence.py`**: `--enable-pipeline-4` gates the mutate-recover mode; `--auto-restrict` randomly samples ssvids via `dit.bq.query_for_restricted_ssvids`; in-process Beam threads labels via `unknown_parsed_args["labels"]` (not CLI flags).
- **`outage_recovery.py`**: `--pre-outage-pin-at` / `--post-outage-pin-at` (tz-aware required); `--offset-days`, `--snapshot-dest-project`, `--snapshot-expiration-days`, `--skip-snapshots`. Implements its own snapshot dataset/table helpers (parallel to `cross_version_ais.py`'s — see [§3-a](#a-snapshot-dataset-machinery-is-duplicated-three-times)).
- **`port_visits/ais.py`**: `--thinned-message-table` (skip step 1, point step 2 at external table); `--binding-name` (so cross-version can pass through); both submitter image AND worker image identity in cache key.
- **`port_visits/cross_version_ais.py`**: `--binding-worker-image NAME=IMAGE` for per-binding sdk_container_image; `--thinned-message-table` snapshotted (not idempotent — fail-fast).
- **`pipe_events/fishing.py`**: no `_run_with_cache` (deferred); calls `add_dataset_args` only (no Dataflow knobs); `GCP_VOLUME = "gcp:/root/.config"` named volume; runs `pipe` CLI inside `pipeline` compose service.
- **`pipe_segment/identity_match_key.py`**: `--include-satellite-offsets` (date-sharded prod tables — see snapshot audit for latent cross-org bug); `_verify_distinct_gpsdio_pins` pre-flight; `container_env={"GOOGLE_CLOUD_PROJECT": PROJECT}`; `_chdir` context manager (bindings run sequentially).

Same pipeline family but distinctly different at the date-semantics layer: pipe-gaps mode_equivalence uses half-open dates, port-visits AIS uses fully-inclusive, pipe-events uses half-open with exclusive end. These divergences are **pipeline-CLI-shaped** (each upstream CLI dictates its own semantics), not stylistic — recorded for awareness, not as a reconciliation candidate.

## 2. Trajectory — the five decisions that shaped the current shape

Most consequential first, with anchors into `CLAUDE.md` § Plan changelog.

**2026-06-04 — Outage-recovery defaults flipped twice.** First flip: source FQNs moved from VMS-prod to AIS-staging because `CREATE SNAPSHOT TABLE ... CLONE` refused cross-org. Second flip: date defaults moved from `2024-08-22`/`29` to `2020-08-22`/`29` after the smoke processed zero rows — the cohort *name* is the snapshot date (2024); the *data window* inside is 2020. Generated a new working-agreement sub-bullet: "mirror the existing workflow in the same family for source FQNs AND date defaults; verify the data window against `INFORMATION_SCHEMA.PARTITIONS`."

**2026-06-02 — pipe-events `fishing.py` defaults flipped from pipe3-prod to staging.** Before: 2012 full year against `pipe_ais_v3_*`. After: 2020 against `pipe_ais_test_*`. Driven by pipe-events' own `CLAUDE.md` ("always run staging first") and the cost asymmetry. Pipe3 stays reachable via documented `--start/--end/--internal-ds/--published-ds` overrides in the module docstring (no separate workflow file — 40 LOC of duplicate boilerplate not worth it).

**2026-05-29 — Framework-extraction DEFERRED** (`workflows/README.md`). With three consumers in hand, the `Phase`/`Mode`/`Oracle` dataclasses sketched in `framework-vision.md` were explicitly rejected: per-slice bodies share <20% (different runners, step counts, table-naming, arg shapes), and even the date-slice arithmetic diverges (pipe-gaps `[d-W, d)`, port-visits `[d, d]`, pipe-events `[d, d+1)`). Verdict: keep the three `execute_*` explicit. Revisit only when a fourth consumer matches an existing daily-window shape exactly.

**2026-05-29 — `dit.workflow` harness extraction (PR #28).** `resolve_run_context`, `add_dataset_args` / `add_dataflow_args` / `add_infra_args`, `add_experiment_id_arg`, and `run_with_cache` lifted out of duplicated code in the two then-existing workflows. The `add_infra_args` was simultaneously split (Phase 3 / PR #31) so pipe-events could call only `add_dataset_args` (no Dataflow knobs). This is the single biggest shared-infrastructure consolidation in dit's history; everything since has consumed it.

**2026-05-22 — Run cache + no-dirty-tree pivot (PRs #16–#19).** Cache table `world-fishing-827.tech_great_expectations.dit_runs` content-addresses `pipeline_commit` + `worker_image_digest` + `workflow_file_sha1` + `params`. Killed `--allow-dirty-tree` and `_dirty` table-suffix in favour of auto-snapshot-and-push to `refs/dit-snapshots/<pipeline>/<sha>`. Now every workflow's `main()` calls `resolve_run_context` and inherits the same submitter-vs-worker / dirty-tree / `unreviewed_code` semantics.

## 3. Reconciliation analysis

### Already shared (good)

| Shared piece | Where | Consumers |
|---|---|---|
| `resolve_run_context(...) → RunContext` | `src/dit/workflow.py` | All 4 pipeline-leaf workflows |
| `add_dataset_args` / `add_dataflow_args` / `add_infra_args` | `src/dit/workflow.py` | pipe-gaps mode_eq + outage_recovery + port-visits AIS use `add_infra_args`; pipe-events uses only `add_dataset_args`; pipe-segment hand-rolls (see [§3-e](#e-pipe-segment-doesnt-use-add_dataset_args-or-add_experiment_id_arg-consistently)) |
| `add_experiment_id_arg` + `EXPERIMENT_ID_RE` | `src/dit/workflow.py` | All 4 leaf workflows; pipe-segment also uses `EXPERIMENT_ID_RE` to validate binding names |
| `run_with_cache` (CacheKey + read/write logic) | `src/dit/workflow.py`, backed by `src/dit/cache.py` | pipe-gaps mode_eq + outage_recovery + port-visits AIS. Pipe-events + pipe-segment opt out (`resolve_digest=False`) |
| `make_job_name` (Dataflow job name) | `src/dit/job_names.py` | pipe-gaps mode_eq + outage_recovery + port-visits AIS + pipe-segment |
| `dit.bq.snapshot_table` / `snapshot_dataset` | `src/dit/bq.py` | port-visits cross-version + outage_recovery + pipe-segment |
| `dit.compare.compare_tables` | `src/dit/compare.py` | All workflows (the integration test's purpose) |
| `dit.dates.daterange_inclusive` | `src/dit/dates.py` | pipe-gaps (both), port-visits, pipe-events |
| `dit.runners.{dataflow,docker}` | `src/dit/runners/` | dataflow: pipe-gaps; docker: port-visits, pipe-events, pipe-segment |

This is a healthy amount of shared scaffolding for six files / ~5k LOC. The 2026-05-29 harness extraction (PR #28) is doing the bulk of the work.

### Diverging by necessity (don't force together)

1. **`dit.runners.dataflow` vs `dit.runners.docker`.** pipe-gaps' detect pipeline exposes a `gfw.common.beam.pipeline.Pipeline`-shaped object the in-process runner can consume; pipe-anchorages and pipe-events don't, so they submit via the container CLI. Pipe-segment is Beam-shaped like pipe-gaps but goes through the docker runner because pipe-segment's CLI submits Dataflow from inside the container (composer's pattern). **Pipeline-shape-driven**; this is the right split.

2. **SCD-2 vs truncate comparison shape.** Pipe-gaps has `valid_from`/`valid_to`/`is_current` and a `_last_versions` view; port-visits and pipe-events don't (versioning is table-level via `_v{date}` + a view). The comparison call site differs accordingly. Schema fact, not a reconciliation candidate.

3. **Cache-key shape on/off.** pipe-events and pipe-segment have `resolve_digest=False` because there's no Dataflow worker image to digest (BQ-SQL pipeline) or because Dataflow workers pull from the docker compose service. Forcing a cache there is a separate design question (docker-runner cache-key shape).

4. **Date semantics (half-open vs inclusive).** Pinned by pipeline CLIs. Documented in each workflow.

### Diverging but could reconcile

These are the highest-leverage candidates.

#### (a) Snapshot dataset machinery is duplicated three times

`port_visits/cross_version_ais.py`, `pipe_gaps/outage_recovery.py`, and `pipe_segment/identity_match_key.py` all carry the same `_sanitize_for_dataset` / `_ensure_dataset` / `_snapshot_source` helpers. All three also carry the same FOOTGUN comment ("`if_not_exists=True` silently reuses snapshot when `--pin-source-at` changes").

**Reconciliation**: lift `ensure_experiment_dataset(experiment_id, *, label=None, project=PROJECT, expiration_days=7) → fqn` and a `snapshot_into_experiment(...)` wrapper into `dit.bq` (or a new `dit.snapshots`). **~80 LOC consolidated → ~40 LOC in `dit.bq`, ~5 LOC removed from each consumer (~100 LOC net reduction) and one shared FOOTGUN solved.**

Pairs naturally with the snapshot-mechanism work in [`snapshot-edge-cases-2026-06.md`](snapshot-edge-cases-2026-06.md) (cross-org guard, `if_existing="verify_as_of"` mode).

#### (b) Cross-version orchestrator is now a pattern, not a one-off

`port_visits/cross_version_ais.py` and `pipe_segment/identity_match_key.py` share: `_parse_binding`, `_parse_iso8601`, `_verify_refs`, the git-worktree-per-binding loop, the `_SKIPPED = -1` sentinel + `_run_diffs` + `_summarize`, and the `<experiment_id>-<binding>` (or `_`) suffix shape. The differences are which pipeline runs and parallel-vs-sequential.

**Reconciliation**: a `dit.cross_version` module exposing `verify_refs`, `parse_binding`, `run_bindings(callable_per_binding, parallel=...)`, `summarize_pairwise_diffs(...)`. ~150 LOC consolidated → ~80 LOC in `dit.cross_version` + ~25 LOC saved per orchestrator.

**Defer per "duplicate-until-3"**, but the shape is clear, so future-lift will be cheap. Revisit when a third cross-version workflow appears.

#### (c) Label helpers duplicated 3-4×

`_dit_run_labels`, `_safe_label_value`, `_worker_image_tag`, `_UNSAFE_LABEL_CHAR_RE`, `_DIGEST_RE` are duplicated across `pipe_gaps/mode_equivalence.py`, `pipe_gaps/outage_recovery.py`, `port_visits/ais.py`, and partially `pipe_segment/identity_match_key.py`.

**Reconciliation**: lift into `dit.labels`. ~50 LOC consolidated, ~30 saved across consumers. Pipe-events has its own JSON-shaped `_LABELS_JSON` (different pipeline contract — its CLI takes labels as a JSON blob, not `--labels=k=v` flags) so it would not be a consumer.

#### (d) `_make_config` / `_cfg_to_cli_flags` / `_build_pipeline_for` / `_run_pipeline` duplicated between the two pipe-gaps workflows

`outage_recovery.py` explicitly imports `DEFAULT_*` constants from `mode_equivalence.py` but copy-pastes ~200 LOC of helpers. The forked-with-intent comment on `_run_pipeline` says it's done "so we can keep the workflow self-contained and not import private helpers from a sibling workflow."

The "duplicate until 3" rule says wait, but **this is two clones inside one pipeline directory**, and when pipe-gaps' `DetectGapsConfig` shape evolves these will drift silently. **This is the most expensive correctness risk in tree right now.**

**Reconciliation**: a sibling-level `workflows/pipe_gaps/_detect.py` hosting the shared helpers; each workflow imports. ~200 LOC duplicated → ~120 LOC in `_detect.py`, ~80 LOC saved.

#### (e) pipe-segment doesn't use `add_dataset_args` or `add_experiment_id_arg` consistently

pipe-segment calls `add_experiment_id_arg`, but it hand-rolls `--dest-dataset`, `--service-account` rather than calling `add_dataset_args` + a new `add_service_account_arg`. The reason is real (pipe-segment as cross-version orchestrator passes `--service-account` through to the inner pipe-segment process; it isn't using `dit.workflow`'s Dataflow-shaped knobs). But the inconsistency makes the harness feel partial.

**Reconciliation**: extend `dit.workflow` with `add_service_account_arg(parser, default=...)` so docker-runner consumers can grab just that one knob. ~10 LOC.

#### (f) Date-defaults convention with a lesson baked into CLAUDE.md

The 2026-06-04 outage-recovery failure is canonical: defaults derived from "what's the cohort name year?" instead of "what does mode_equivalence use?". `INFORMATION_SCHEMA.PARTITIONS` is the verification path; `mode_equivalence.py`'s `DEFAULT_START`/`DEFAULT_END` is the cited canon.

**Reconciliation**: a `dit.cohorts.PIPE_AIS_TEST_202408290000` named tuple/dataclass with `internal_ds`, `published_ds`, `data_start`, `data_end`, `snapshot_date` fields, imported by each workflow. ~80 LOC for the module + ~5 LOC per workflow; failure mode is **structurally** prevented. See [§4](#4-staging--ais-prod--vms-as-a-design-question) for the broader design context.

### Patterns NOT worth lifting yet

- The `_run_slice` shape (per-slice execution); explicitly the framework-extraction-deferred decision. Each workflow's per-slice body is the load-bearing fingerprint of its pipeline.
- The mode-aware `canonical_params_dict` (cache-key params): two consumers (pipe-gaps mode_eq + port-visits) have it, but they differ enough that lifting would be premature.

## 4. Staging / AIS-prod / VMS as a design question

Current state per workflow:

- **`mode_equivalence.py`**: two per-table FQN flags (`--source-messages`, `--source-segments`); defaults are staging-cohort FQNs (messages in `_internal`, segs in `_published`).
- **`outage_recovery.py`**: imports its defaults from `mode_equivalence.py`, reshapes for the outage scenario; documents prod-VMS opt-in in the docstring with explicit `--snapshot-dest-project` flag.
- **`port_visits/ais.py`**: single `--source-dataset-stem` flag; appends `_internal`/`_published`. Cleaner abstraction because port-visits reads multiple tables from each half symmetrically.
- **`pipe_events/fishing.py`**: four FQN flags (`--internal-ds`, `--published-ds`, `--pipe-static`, `--pipe-regions-layers`); production override path documented in module docstring.
- **`pipe_segment/identity_match_key.py`**: single `--source-normalized-table` FQN; `--include-satellite-offsets` opt-in pulls from prod (`gfw-int-pipe-v3.satellite_positions.*`) — explicit cross-project, explicit opt-in.

Three coherent shapes, three different mechanisms (per-table FQN, dataset stem, mixed). All independently sensible; the inconsistency is what bites when adding a new workflow.

### Option 1 — Status quo (per-workflow defaults)

**What works**: each workflow's defaults are self-contained, locally readable, and reflect that pipeline's CLI shape exactly.
**What bites**: discoverability decays as workflows multiply. 6 files, 4 different conventions; the next workflow's author has to read all 4 to know which to mirror. The cohort-name-vs-data-window confusion is a recurring failure mode (happened twice in 2026 alone, both documented in CLAUDE.md). README's "Staging data sources" section is doc-only — easy to drift out of date with workflows.

**Migration cost: 0.** This is what's in tree.

### Option 2 — In-tree shared config (`dit.cohorts`)

**Shape**: a `dit/cohorts.py` module with:

```python
@dataclass(frozen=True)
class Cohort:
    name: str                    # "pipe_ais_test_202408290000"
    project: str
    internal_dataset: str
    published_dataset: str
    data_start: date             # 2020-01-01
    data_end: date               # 2021-01-01
    snapshot_date: date          # 2024-08-29 (frozen)

STAGING_AIS = Cohort(...)
PROD_AIS_V3 = Cohort(...)
PROD_VMS_V3 = Cohort(...)
```

Plus a tiny per-pipeline accessor (`STAGING_AIS.table_fqn("research_messages", half="internal")`) that knows the `_internal`/`_published` naming convention.

Workflows then `from dit.cohorts import STAGING_AIS` and `DEFAULT_START = STAGING_AIS.data_start.isoformat()`. Per-workflow CLI flags still exist; only the defaults reach into the cohort.

**Gained**: the data-window-vs-snapshot-date confusion is structurally impossible. README's "Staging data sources" section can be regenerated from the cohort module. New workflow defaults: "import STAGING_AIS, done."
**Lost**: a layer of indirection between the workflow and "what is my default". Workflows that need a non-standard slicing (e.g. outage_recovery's 7-day sub-window) need to override anyway.

**Migration cost**: ~80 LOC for `dit.cohorts.py`, ~5 LOC of replacement per workflow. Backward-compatible.

**Best pick when**: the cohort list is small and known (today: 3) and the discoverability problem outweighs the indirection cost. **This is the case today.**

### Option 3 — Composer-derived config (Phase 4 `dit sync-params`)

The DAG configs in `composer-dags-production/dags/core/ais/v3.py` are real, structured Python: `Versions.PIPE_EVENTS = "v4.2.17"`, `Versions.SEGMENT = "v5.0.3"`, plus full `InputTables.*` / `InputDatasets.*`. Three of four workflows currently hand-mirror version pins from this file — if composer flips `Versions.GAPS` to `v0.9.7`, no dit workflow notices until someone updates each `DEFAULT_WORKER_IMAGE` constant by hand.

**Gained**: prod-config drift is structurally caught (next `dit sync-params` run would detect the new pin). Cross-version "before = current composer pin, after = next pin" becomes trivial.
**Lost**: complexity. Composer's configs are Python objects that reference each other; reading them as data requires running them under a stub harness or YAML-projecting them via a materialise step.

**Migration cost**: high — 200-400 LOC for the projector, plus an ongoing maintenance burden. Real risk: composer-dags is itself a moving target.

**Best pick when**: dit is regularly testing against the *current* prod config, not against a pinned snapshot. **Not today** — three of four workflows default to staging (intentionally cheap), and prod paths are explicit opt-ins documented per workflow.

### Option 4 — Hybrid

**Shape**: `dit.cohorts.STAGING_AIS` hand-defined in dit (the staging cohort is dit-owned). `dit.cohorts.PROD_AIS_V3` and `PROD_VMS_V3` generated from composer-dags via a `dit sync-params` step (separate offline command — checked-in YAML, regenerated when composer changes, never imported at runtime).

**Gained**: staging defaults stay close to the workflow author; prod defaults stay aligned with composer without runtime coupling.
**Lost**: two storage locations (but that's already true).

**Migration cost**: medium. Probably the right end state; Option 2 first.

### Recommendation

If the cohort count stays at 3 and prod-side defaults are rarely-used opt-ins (today's state), **Option 2 (in-tree `dit.cohorts`) is the right next step**, with Option 4 reachable later when sync-params becomes worth building. Option 3 alone is over-engineering for today.

## 5. Recommended next steps

Ranked by leverage; concrete file paths + LOC estimates.

1. **Item (d) — sibling-level `workflows/pipe_gaps/_detect.py`.** Currently ~200 LOC duplicated between `mode_equivalence.py` and `outage_recovery.py`. **Highest correctness risk in tree** (silent drift if `DetectGapsConfig` shape evolves). ~200 LOC consolidated → ~120 LOC in `_detect.py`, ~80 LOC saved. No cross-pipeline coordination needed.

2. **Item (f) — `dit.cohorts`** (Option 2 from §4). Eliminates the cohort-name-vs-data-window failure mode structurally. ~80 LOC + ~5 LOC per workflow. Doing this first makes item (a) cheaper (`snapshot_into_experiment(cohort, ...)` is cleaner than `snapshot_into_experiment(experiment_id, project, dataset_stem, ...)`).

3. **Item (a) — `dit.bq` / `dit.snapshots` consolidation.** Three near-identical implementations + a shared FOOTGUN that fixes once. ~80 LOC consolidated → ~40 LOC in `dit.bq`, ~5 LOC saved per consumer. Pairs with the snapshot-mechanism guards in [`snapshot-edge-cases-2026-06.md`](snapshot-edge-cases-2026-06.md).

4. **Item (b) — `dit.cross_version` when a third orchestrator appears.** Defer per duplicate-until-3. Two consumers today; if pipe-events grows a cross-version workflow this becomes natural to lift.

5. **Item (c) — `dit.labels`** for `_safe_label_value`, `_worker_image_tag`, `dit_run_labels`. 3-4 consumers; low-leverage individually but cheap. Bundle with #2 if convenient.

### Not recommended

- The full framework-extraction (`dit.phases`, `Phase`/`Mode`/`Oracle`) stays deferred per `workflows/README.md`; per-slice execution stays per-workflow.
- The composer-derived defaults (Option 3) is overkill for today's cohort count and coupling cost.
