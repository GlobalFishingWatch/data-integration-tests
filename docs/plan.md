# Migrate integration tests to a shared repo: `data_integration_tests`

## Context

Three GFW data pipelines (`pipe-gaps`, `pipe-events`, `anchorages_pipeline`) duplicate or lack the same kind of integration test. The pipe-gaps repo recently grew a Python-first mode-equivalence test (`tests/integration/mode_equivalence.py`, branch `testing/orchestration_equivalence_integration_tests`); pipe-events has a bash-only ancestor of the same idea; `anchorages_pipeline` has no integration tests, and a new port-visits feature needs them next.

Goal: a standalone repo at `/mnt/encrypted_data/git/data_integration_tests` that **first** absorbs the existing pipe-gaps test and extracts its reusable parts (Phase 1 — the refactor), **then** builds port-visits tests on top of those extracted parts (Phase 2 — the new pipeline consumer that validates the abstractions), **then** absorbs pipe-events (Phase 3).

This ordering — refactor first, new consumer second — is the conservative path. The pipe-gaps test is already working code; lifting it forces only mechanical extraction. Port visits then becomes the first real test of whether the extracted helpers (`Runner`, `Compare`) generalise. If they don't, we find out before pipe-events is in the picture.

User-confirmed decisions:
- Phase 2 (port visits) ships **both AIS and VMS** workflows — production runs both.
- The `--runner=local` Python-import path **is dropped** in the migrated repo. Docker (DirectRunner-in-container) replaces it.
- Pipeline docker images are **published on a registry**; workflows reference tagged images. Build-from-source is not the Phase 1 default.

## Status at a glance (2026-05-15)

| Phase / capability | Status |
|---|---|
| Phase 1 — pipe-gaps port | **Code complete.** Real-BQ verification + Track 5 (pipe-gaps repo shim) pending. |
| Phase 2 — port-visits | **AIS-staging verified 2026-05-15** (3/3 pairwise green on `visit_id`). VMS not started; AIS-full not started. |
| Phase 3 — pipe-events port | Not started. |
| Phase 4 — composer-dags param sync | Not started. |
| Phase 5 — mutation library | Not started. |
| Phase 6 — phase sharing / hash-based caching | Not started. |
| Phase 7 — golden-table regression mode | Not started. |
| **Cross-version experiments capability** | Landed mid-Phase 2; see § below. Validated end-to-end via the PIPELINE-1465 cross-version test. |
| **Runtime & CI (Cloud Build)** | Designed; implementation pending. See § below. |

Per-commit history of plan-doc evolution lives in `CLAUDE.md` § Plan changelog. User-facing change log lives in `CHANGELOG.md`. The **Next steps** section near the bottom of this doc is the rolling operational list.

## Architecture: three-repo split

Integration-test infrastructure is distributed across three repos by ownership. Aligning subagents on this boundary is the single most important alignment step before code lands.

**Processing repos** (`pipe-gaps`, `anchorages_pipeline`, `pipe-events`) own pipeline business logic, the CLI surface, and docker image publishing. They do **not** own integration-test orchestration code (Phase 1 evicts pipe-gaps' existing test; Phase 3 does the same for pipe-events' bash). They MAY carry an in-repo workflow file that imports `dit` as a library — see decision 7 — but the canonical home for long-lived equivalence tests is `data_integration_tests/`.

**`composer-dags-production`** owns the production parameter source-of-truth (`dags/core/{ais,vms,...}/v3.py` and friends), DAG topology, and the shared abstractions in `gfw/common/config/*` and `gfw/pipes/v3/*` (`PipelineConfig`, `Dataset`, `<Step>Config`, etc.). `dit` consumes composer-dags as a *data source* via the Phase 4 sync — never as a code dependency. No `import gfw.…` from `dit`.

**`data_integration_tests`** (this repo) owns the runners (`dit.runners.{docker,dataflow}`), the comparison shim (`dit.compare`, wrapping `table-check`), BQ/date utilities (`dit.bq`, `dit.dates`), the CLI (`dit run …`), and — from Phase 5 onward — mode generators (`dit.modes.{backfill,daily_tail,…}`) and a mutation library (`dit.mutations.*`). Per-pipeline workflow files live in `workflows/<pipeline>/`.

**`table_identical_checks`** (the `table-check` CLI, separate repo) is the comparison primitive. `dit.compare` is a thin subprocess wrapper; comparison features (new tolerances, output formats, dimensional breakdowns) go upstream there, not into `dit.compare`.

## Decisions (recommended)

1. **SCD-2 vs `WRITE_TRUNCATE` mismatch** is absorbed by the `Compare` helper as kwargs (`keys=`, `view_suffix=`). pipe-gaps/pipe-events pass `view_suffix="_last_versions"` and SCD-2 keys; port visits passes `view_suffix=""` and `keys=["visit_id"]`. **The kwargs are designed in Phase 1, even though only the SCD-2 shape is exercised then.** Phase 2 exercises the truncate shape and validates the design.
2. **pipe-events porting deferred to Phase 3.** Bash → Python + adding comparisons is a from-scratch rewrite; it isn't blocking either of the first two phases.
3. **Source repos lose their integration test files.** Phase 1 replaces `pipe-gaps/tests/integration/mode_equivalence.py` with a shim. Phase 3 does the same for `pipe-events/integration_tests/staging-bf_bfd_bftruncate.sh`.
4. **Cross-version testing via docker tags** from day one. Each workflow declares an `--image-tag` parameter.
5. **Phase 1 keeps fishing events and port visits in mind.** The extracted helpers must be parameterised enough to serve all three consumers, even though Phase 1 only exercises pipe-gaps. Concrete parameterisation points: `Compare.keys/view_suffix`, `Runner.image_tag`, no module-level constants for SA/region/temp-bucket/subnetwork (constructor parameters).
6. **`dit` is a library first, CLI second.** All functionality is importable Python (`dit.runners.docker.run(...)`, `dit.compare.compare_tables(...)`, etc.). The CLI (`dit run <workflow>`) is one consumer; processing repos can also `import dit` directly from a pytest target.
7. **Workflow file location policy.** Canonical home is `data_integration_tests/workflows/<pipeline>/`. Processing repos MAY carry in-repo workflows that import `dit` as a library — appropriate for one-off spikes or feature-specific tests — but **the same workflow must never exist in both places.** Long-lived equivalence tests live in `dit`. PR-triggered CI works for either: a centralised workflow is invoked from the processing repo's CI as `pipx install data-integration-tests && dit run workflows/<pipeline>/<workflow>.py --image-tag pr-${PR}`.
8. **Per-pipeline config dataclasses stay in `composer-dags`.** `GapsConfig`, `DetectPortVisitsConfig`, `FishingEventsConfig`, etc. remain in `gfw/pipes/v3/*` for the foreseeable future. `dit` reads parameter values from `params.yaml` (synced via Phase 4 pull) and never imports the dataclasses. This decouples `dit`'s code dependencies from composer-dags' structure; if those configs ever migrate, `dit` switches from YAML to imports without restructuring workflows.
9. **`dit.compare` is a thin shim.** It wraps `table-check summary` via subprocess. Comparison features go upstream into `table_identical_checks`, not into `dit.compare`.

## Repo layout (Phase 1, concrete)

```
/mnt/encrypted_data/git/data_integration_tests/
├── README.md                          # how to run; SA/perms checklist; image-tag conventions
├── pyproject.toml                     # package: data-integration-tests, console: dit
├── requirements.txt                   # click, pyyaml, google-cloud-bigquery (framework-only; workflow deps come from Makefile targets)
├── requirements-dev.txt               # pytest, ruff
├── Makefile                           # install-pipe-gaps / install-port-visits / install-pipe-events / install-all
├── .envrc.example                     # direnv template; sets PROJECTS for the Makefile
├── src/
│   └── dit/
│       ├── __init__.py
│       ├── cli.py                     # `dit run <workflow-path>` (click)
│       ├── runners/
│       │   ├── __init__.py
│       │   ├── base.py                # Runner protocol: run(image_tag, args, env) -> int
│       │   ├── docker.py              # `docker run --rm <image> <cmd...>`; unique compose project per call
│       │   └── dataflow.py            # in-process orchestrator; lock-split submission/wait
│       ├── compare.py                 # compare_tables(a, b, *, keys, view_suffix="") -> int
│       ├── bq.py                      # drop_tables(prefix); query helpers (e.g. for restricted ssvids)
│       └── dates.py                   # daterange_inclusive
└── workflows/
    └── pipe_gaps/
        ├── README.md
        └── mode_equivalence.py        # 4 modes + auto-restrict; calls dit.runners + dit.compare
```

Phase 2 adds `workflows/port_visits/{ais.py,vms.py,params.yaml}`. Phase 3 adds `workflows/pipe_events/fishing.py`.

Workflows are **Python files, not YAML or DSL**. Pipeline configs are Python dataclasses already; jump-to-definition is preserved. `params.yaml` (Phase 2 onward) exists only to hold AIS-vs-VMS parameter deltas in a place a future cron job can re-sync from `composer-dags-production`.

## Public API contracts (Phase 1)

Subagents working on Phase 1 build to these signatures. Treat them as the contract; deviations require updating this section first (and a Plan-changelog entry in `CLAUDE.md`).

### `dit.runners.base`

```python
class Runner(Protocol):
    def run(self, args: list[str], *, env: dict | None = None, **kwargs) -> int: ...
```

### `dit.runners.docker`

```python
def run(
    image_tag: str,
    args: list[str],
    *,
    env: dict | None = None,
    project_name: str | None = None,    # docker compose -p; auto-uniquified if None
    build_from_source: bool = False,    # opt-in fallback for unpublished images
    entrypoint: str | None = None,      # overrides image's default ENTRYPOINT
) -> int:
```

Source: `pipe-gaps/tests/integration/mode_equivalence.py` lines 257–281 (`_run_docker`). Each invocation gets a unique compose project name to avoid network races. `entrypoint` lets workflows override the image's default (pipe-gaps passes `entrypoint="pipe-gaps"` because its dev image has no baked-in entrypoint).

### `dit.runners.dataflow`

```python
def run(
    args: list[str],
    *,
    image_tag: str | None,
    service_account: str,
    region: str,
    temp_bucket: str,
    subnetwork: str,
    bq_temp_dataset: str,
    env: dict | None = None,
    pipeline_builder: Callable[[Mapping[str, Any]], Any],
    dag_factory_cls: type | None = None,
) -> int:
```

Source: `mode_equivalence.py` lines 286–399 (`_run_dataflow`). Submission and wait must be split via an internal lock (`_DATAFLOW_SUBMIT_LOCK`); the `_DagFactoryWithTempDataset` override that injects a pre-existing temp dataset must port across.

Two parameters were added during Track 3 to make the runner pipeline-agnostic without re-importing pipe-gaps internals:

* `pipeline_builder` -- required callable. Given the merged Beam options mapping, the workflow returns a built `gfw.common.beam.pipeline.Pipeline`-shaped object (exposes `_pre_hooks`, `_post_hooks`, `apply_dag()`, `.pipeline.run()`). The runner does the lock-split submit/wait around whatever pipeline the workflow built.
* `dag_factory_cls` -- optional DAG factory class. When `bq_temp_dataset` is set, the runner subclasses it to inject `temp_dataset` into `read_from_bigquery_factory` (the `_DagFactoryWithTempDataset` pattern, ported as a generic wrapper). The wrapped class is forwarded to `pipeline_builder` via the `dag_factory_cls` key in the options mapping; workflows that don't need temp-dataset injection can omit it and ignore the key.

`env` is reserved for parity with the `Runner` protocol but logged-and-ignored: the dataflow runner is in-process so there is no subprocess to forward env to.

### `dit.compare`

```python
def compare_tables(
    table_a: str,
    table_b: str,
    *,
    keys: Sequence[str],
    view_suffix: str = "",
    ignore_columns: Sequence[str] = (),
    tolerance: dict[str, float] | None = None,
) -> int:
```

Returns 0 on identical, non-zero on diff. Shells out to `table-check summary --table-a=… --table-b=…`. `keys` and `view_suffix` are required parameters (not pipe-gaps-specific defaults) — Phase 2 will pass `keys=["visit_id"], view_suffix=""`.

### `dit.bq`

```python
def drop_tables(prefix: str, *, project: str = "world-fishing-827") -> None: ...
def query_for_restricted_ssvids(
    reference_table: str,
    *,
    mid: date,
    backfill_days_w: int,
    seed: int = 42,
    project: str = "world-fishing-827",
) -> list[str]: ...
def snapshot_table(
    source_table: str,
    dest_table: str,
    *,
    as_of: datetime | None = None,
    expiration: datetime | None = None,
    project: str = "world-fishing-827",
    if_not_exists: bool = False,
) -> None: ...
def snapshot_dataset(
    source_dataset: str,
    dest_dataset: str,
    *,
    tables: Sequence[str] | None = None,
    as_of: datetime | None = None,
    expiration: datetime | None = None,
    project: str = "world-fishing-827",
) -> list[str]: ...
```

`drop_tables` requires the prefix to include project and dataset (`<proj>.<dataset>.<stem>`); it lists the dataset and drops every table/view starting with `<stem>`.

`query_for_restricted_ssvids` ports `compute_restricted_ssvids` (lines ~540–640): queries the reference `_last_versions` view for triggering closed gaps, picks ~|G|/2 non-triggering ssvids so the complement contains every triggering ssvid. The source's `n_hours_before` argument is dropped — it was unused at the call site and the source code itself logged it as such.

`snapshot_table` emits `CREATE SNAPSHOT TABLE [IF NOT EXISTS] <dest> CLONE <src> [FOR SYSTEM_TIME AS OF …] [OPTIONS(expiration_timestamp=…)]`. Used for source-data pinning so cross-version pipeline runs compare against frozen inputs instead of drifting upstream tables.

`snapshot_dataset` snapshots every table in `source_dataset` (or just `tables` if specified) into an existing `dest_dataset`. Idempotent — skips tables already present in dest; raises if dest dataset is missing. Returns the fully-qualified ids of the snapshots it created.

### `dit.dates`

```python
def daterange_inclusive(start: date, end: date) -> Iterator[date]: ...
```

Lifted from `mode_equivalence._daterange_inclusive`.

### Workflow entry-point convention

A workflow file is a Python module with a `main(argv: Sequence[str] | None = None) -> int` entry point. `dit run <path>` discovers and invokes it; an in-repo workflow can call its own `main()` from a pytest fixture without going through the CLI.

## Phase 1 — Refactor: pipe-gaps moves to the shared repo

**Status:** Code complete; Track 5 cutover + real-BQ byte-equivalence verification still pending.

**Scope.** Stand up the new repo. Port `mode_equivalence.py` from `pipe-gaps/tests/integration/` to `data_integration_tests/workflows/pipe_gaps/`. Extract runners (`docker`, `dataflow`), `compare_tables`, and `_daterange_inclusive` into `dit/`. Drop `--runner=local`. Replace pipe-gaps' file with a shim. Verify the four-mode equivalence test produces output identical to a pre-move reference run.

The extracted helpers are **designed for three consumers from day one** even though only pipe-gaps exercises them in Phase 1:
- `dit.compare.compare_tables(a, b, *, keys, view_suffix="", ignore_columns=())` — `keys` and `view_suffix` are required parameters, not pipe-gaps-specific defaults. Phase 2 will pass `keys=["visit_id"], view_suffix=""` for port visits.
- `dit.runners.docker.run(image_tag, args, env=None, project_name=None)` — `image_tag` is required. Defaults to a published tag; no build-from-source baked in. Phase 2 will pass a different tag for port visits.
- `dit.runners.dataflow.run(image_tag=None, args, ..., service_account=None, region=None, temp_bucket=None, subnetwork=None, bq_temp_dataset=None)` — Dataflow knobs are constructor parameters, not module constants.

**Deliverables.**
- `pyproject.toml` declaring `data-integration-tests` package and `dit` console script.
- `src/dit/runners/docker.py` modeled on `mode_equivalence._run_docker` (lines 257–281). Uses published image tag; keeps the unique `-p <name>-<uuid>` per-invocation compose project to avoid network races. Exposes `--build-from-source` as an explicit override for local dev (fallback when an image isn't published yet).
- `src/dit/runners/dataflow.py` ported from `_run_dataflow` (lines 286–399). Lock-split submission/wait pattern preserved. SA/region/temp-bucket/subnetwork as parameters.
- `src/dit/compare.py` with `compare_tables(...)` function. Same `subprocess.run(["table-check", "summary", ...])` shape as `mode_equivalence.compare_tables` (lines 632–650), parameterised on `keys` and `view_suffix`.
- `src/dit/bq.py` with `drop_tables(prefix)` and `query_for_restricted_ssvids(...)` (ported from `compute_restricted_ssvids`).
- `src/dit/dates.py` with `daterange_inclusive(start, end)` lifted from `mode_equivalence._daterange_inclusive`.
- `workflows/pipe_gaps/mode_equivalence.py` — port of the existing 898-line file minus `_run_local` (dropped) and the runner/compare internals (now in `dit/`). Keeps the 4-mode logic (`execute_bf` / `execute_bfd` / `execute_bftruncate` / `execute_mutate_recover`) and `compute_restricted_ssvids` callsite.
- In `pipe-gaps`: replace `tests/integration/mode_equivalence.py` with a 10-line shim that prints "moved to `data_integration_tests`; run `dit run workflows/pipe_gaps/mode_equivalence.py`" and exits 1. Update `CLAUDE.md` and any other internal docs.

**Out of scope for Phase 1.**
- No port-visits, no pipe-events.
- No `Phase`/`Mode`/`Mutation`/`Oracle` dataclasses. Workflows remain imperative scripts.
- No CI for workflows themselves; lint/unit tests on `src/dit/` only.
- No `composer-dags` sync; no `params.yaml` (no second-pipeline parameter delta to capture yet).

**Critical paths.**
- `/mnt/encrypted_data/git/pipe-gaps/tests/integration/mode_equivalence.py` (read for porting; replace with shim after) — runners (lines 207–209, 257–281, 286–399), modes (lines 430–476), `compute_restricted_ssvids`, `compare_tables` (lines 632–650).
- `/mnt/encrypted_data/git/data_integration_tests/src/dit/runners/{docker,dataflow}.py` (new).
- `/mnt/encrypted_data/git/data_integration_tests/src/dit/compare.py` (new).
- `/mnt/encrypted_data/git/data_integration_tests/workflows/pipe_gaps/mode_equivalence.py` (new).
- `/mnt/encrypted_data/git/pipe-gaps/CLAUDE.md` (update).

**Verification.** `dit run workflows/pipe_gaps/mode_equivalence.py --runner dataflow --parallel --enable-pipeline-4 --auto-restrict --start 2020-01-01 --end 2021-01-01 --tail-days 2 --suffix <hex>` produces output tables and `table-check` results identical to a pre-move reference run with the same arguments. Run both against the same suffix once during cutover and diff the resulting BQ tables to confirm byte-equivalence.

### Phase 1 subagent task breakdown

Five tracks. 1–3 are independent and parallelisable; 4 depends on 2+3; 5 depends on 4 having been verified once locally. Each subagent codes against the contracts in "Public API contracts (Phase 1)".

1. **Repo scaffolding.** `pyproject.toml` (package `data-integration-tests`, console script `dit`), `requirements.txt`, `requirements-dev.txt`, top-level `README.md`, package skeleton (`src/dit/__init__.py`, empty submodules), `dit run` CLI stub in `cli.py`. No business logic yet.
2. **Pure-Python utilities.** `dit/compare.py`, `dit/bq.py`, `dit/dates.py`. No Docker or Dataflow deps. Match the contracts. Unit tests where reasonable (`tests/test_dates.py` is the obvious one).
3. **Runners.** `dit/runners/base.py` (`Runner` Protocol), `dit/runners/docker.py` (port `_run_docker`), `dit/runners/dataflow.py` (port `_run_dataflow`, including `_DATAFLOW_SUBMIT_LOCK` and `_DagFactoryWithTempDataset`). Match the contracts.
4. **Pipe-gaps workflow port.** `workflows/pipe_gaps/mode_equivalence.py`. Port the four-mode logic (`execute_bf` / `execute_bfd` / `execute_bftruncate` / `execute_mutate_recover`) and the `compute_restricted_ssvids` callsite from the existing 949-line file, calling `dit.runners` and `dit.compare`. Drop `_run_local`. Conforms to the workflow entry-point convention (`def main(argv=None) -> int`).
5. **Cutover.** Replace `pipe-gaps/tests/integration/mode_equivalence.py` with a 10-line shim that prints "moved to `data_integration_tests`; run `dit run workflows/pipe_gaps/mode_equivalence.py`" and exits 1. Update `pipe-gaps/CLAUDE.md` and any internal docs that reference the old path. Run the verification command above.

## Phase 2 — Port visits

**Status:** AIS-staging verified 2026-05-15 (3/3 pairwise green on `visit_id`). VMS workflow not started. AIS-full not started.

**Scope.** Build port-visits workflows for AIS and VMS, exercising the helpers extracted in Phase 1. This is the abstraction-validation step: if `dit.compare`'s `view_suffix=""` path doesn't work, or if `dit.runners.docker` can't accept a different image tag without surprise, Phase 1's design needs adjustment.

`dit run workflows/port_visits/ais.py --image-tag <tag> --start <date> --end <date> --suffix <hex> --parallel`:
1. Pulls the published `gfw/pipe-anchorages:<tag>` image.
2. Runs three pipeline invocations in parallel: `_1_bf`, `_2_bfd`, `_3_bftruncate`.
3. Compares all three pairs with `dit.compare.compare_tables(..., keys=["visit_id"], view_suffix="")`.
4. Returns 0 if all three match.

`workflows/port_visits/vms.py` does the same with VMS-shape orchestration: 4-day window `[d-W, d)`, VMS-flavored param values from `params.yaml`. The two scripts share `params.yaml` and a local `_make_phase_call()` helper inside `workflows/port_visits/`. Code duplication between `ais.py` and `vms.py` is **deliberate** — two copies are not a framework; framework decisions wait for the third consumer (pipe-events in Phase 3).

**Deliverables.**
- `workflows/port_visits/ais.py` and `vms.py` (~120 lines each) — execute_bf/bfd/bftruncate logic adapted from `mode_equivalence` (lines 430–476), invoking `dit.runners.docker.run` against `gfw/pipe-anchorages:<tag>`.
- `workflows/port_visits/params.yaml` — two rows (AIS, VMS), columns: `min_gap_length`, `aggregate_segs_stabilization_days`, `generate_events`, `window_width_days`, `cadence`, `service_account`. Source-of-truth comments pointing at `composer-dags-production/dags/core/{ais,vms}/...`.
- Any adjustments to `dit.compare.compare_tables` or `dit.runners.docker.run` if Phase 1's design has rough edges when the second consumer arrives.
- `workflows/port_visits/README.md` — what each workflow tests, how to run, expected runtime.

**Critical paths.**
- `/mnt/encrypted_data/git/data_integration_tests/workflows/port_visits/ais.py` (new).
- `/mnt/encrypted_data/git/data_integration_tests/workflows/port_visits/vms.py` (new).
- `/mnt/encrypted_data/git/anchorages_pipeline/src/pipe_anchorages/cli/main.py` (read-only) — confirms CLI surface to invoke via docker.
- `/mnt/encrypted_data/git/anchorages_pipeline/docker-compose.yaml` (read-only) — confirms `gfw/pipe-anchorages` image identifier.
- `/mnt/encrypted_data/git/composer-dags-production/dags/core/{ais,vms}/...` (read-only) — source for `params.yaml`.

**Verification.** A teammate clones, follows `README.md`, runs `dit run workflows/port_visits/ais.py --image-tag <published-tag> --start 2024-01-01 --end 2024-02-01`, gets a green result (or surfaces a real bug). Same for VMS workflow. Tables created in `world-fishing-827.tech_great_expectations`.

## Cross-version experiments capability (landed 2026-05-15)

A capability not in the original Phase 1-7 numbering, emerged from the PIPELINE-1465 question: *"validate that a fix changes what we want it to change, and only that."* Sits architecturally between Phase 2 (validates abstractions on one pipeline) and Phases 6-7 (formalizes caching and golden tables).

**What it ships:**

- `dit.bq.snapshot_table` / `snapshot_dataset` — source-data pinning via BQ `CREATE SNAPSHOT TABLE`. Pipeline-agnostic (no source changes needed); persists beyond BQ's 7-day time-travel window; storage is delta-only.
- `--experiment-id <slug>` / `DIT_EXPERIMENT_ID` flag in workflows. Output-table suffix grows a leftmost slot so N runs cluster under one BQ-prefix-scannable name. Auto-default `solo_<6-hex>` keeps non-experiment runs disjoint.
- `workflows/port_visits/cross_version_ais.py` — orchestrator. Snapshot source datasets, run `ais.py` per binding inside a git worktree, diff outputs pairwise on `visit_id`. `--dry-run` validates orchestration without Dataflow cost.
- Structured Dataflow job names (`dit-<repo>-<step>-<exp>-<binding>-<mode>`) and dynamic BQ labels (`dit_repo`, `dit_step`, `dit_experiment_id`, `dit_binding`, `dit_mode`) propagating to both Dataflow jobs and the BQ tables they write.

**Where it goes beyond the original plan:**

- Treats source-data drift as a first-class concern. Phase 7 ("golden-table regression") didn't address how to keep input stable across compared runs; snapshots solve that.
- Provides the foundation for PR-tier CI (compare against baseline) and reference caching (Phase 7's golden-table becomes implementable on top of this).

**What it explicitly does NOT include yet:**

- **Reference catalog (deferred V2).** Today's cross-version runs both bindings every time. The optimisation that caches main's output as a no-TTL reference and lets PRs run only the candidate side is sketched in the 2026-05-15 Plan changelog and explicitly deferred until the 12-hour-run cost becomes operationally painful.
- **`--fail-on-diff` mode for PR gating.** Today's wrapper treats diff as information; PR-gating semantics flip this. One-flag addition when needed.
- **Pipe-gaps parity.** `workflows/pipe_gaps/mode_equivalence.py` doesn't yet use the experiment-id-driven labels or structured job names. A cross-version workflow file for pipe-gaps would mirror `cross_version_ais.py` for the detect-gaps shape.
- **`--temp_dataset` upstream.** Currently a local patch on `anchorages_pipeline@dit-temp-dataset-support`; the PIPELINE-1465 cross-version test uses synthetic branches that cherry-pick it. Upstream PR pending.

## Runtime & CI (planned)

Today the orchestrator runs on the user's laptop. Babysitting a 12-hour cross-version run is impractical (suspending the laptop kills the run). Design settled on **Cloud Build** for both ad-hoc and PR-triggered runs.

**Why Cloud Build:** GFW already uses it (per `anchorages_pipeline/cloudbuild/`); 24h step timeout covers AIS-full; serverless billing (~$4/day worst case for a 24h run; dominated by Dataflow worker-hours anyway); native docker host; auth via build-step SA; triggerable three ways without code change (`gcloud builds submit` ad-hoc, GitHub webhook for PRs, Cloud Scheduler for nightly).

**What needs to land** (rough estimate: half a day of plumbing):

1. **`ditbox` container image** in `us-central1-docker.pkg.dev/gfw-int-infrastructure/core/ditbox:<tag>` — Python + gcloud CLI + docker CLI + `dit` pre-installed. Pre-publishes the workflows too so Cloud Build steps don't need to clone `data_integration_tests`.
2. **`cloudbuild-dit.yaml`** in `data_integration_tests` — parameterised single yaml driven by substitutions; serves cross-version, single-run, and PR-check use cases.
3. **`make dit-cloud-…`** target — wraps `gcloud builds submit --source=$(PIPELINE_DIR)` so the pipeline working tree (including uncommitted changes via `git stash create` + temp tag) flows through as the Cloud Build context. Solves the "python editable emulation" need for on-demand runs.
4. **Result reporting.** Cloud Build URL on submit; Slack/email on completion; PR comment with diff summary in PR-mode (later).

**Latency budget for on-demand runs:** laptop submit ~15-30 sec; first Dataflow job running 3-5 min from keystroke (cold), 1-2 min (warm).

**Tiered trigger plan** (after Cloud Build is in place):

- Cheap PR tier: AIS-staging cohort on every PR with a `src/**.py` change, paths-filtered. ~30 min wall clock, ~$5.
- Heavy tier: AIS-full + VMS on `dit:full` label or `/dit run-full` comment. 6-12h.
- Nightly/scheduled: full-cohort against main; Slack on drift.

**Stretch (Phase 6.5-ish): async orchestration.** Decouple submit-from-wait so the babysitter becomes ephemeral and the runtime question disappears. Substantial refactor; out of scope until Cloud Build runtime is in production and the 24h-job pattern is what's actually hurting.

## Phase 3 — pipe-events port + framework extraction decision

**Scope.** Port `staging-bf_bfd_bftruncate.sh` to `workflows/pipe_events/fishing.py`. Add the comparisons that the bash never had. Then look at all three consumers and decide whether to extract `Phase`/`Mode`/`Oracle` dataclasses.

**Decision rule.** Open `workflows/pipe_gaps/mode_equivalence.py`, `workflows/port_visits/{ais,vms}.py`, and `workflows/pipe_events/fishing.py` side-by-side. If `execute_bf` / `execute_bfd` / `execute_bftruncate` are ≥80% identical: extract `dit.phases.backfill(...)` and `dit.phases.daily_tail(...)` per the vision doc. If they look meaningfully different per pipeline (which is plausible given pipe-events' BQ-session model vs Beam): record the explicit deferral in `dit/phases.py` (or `workflows/README.md`) and don't extract. Three is the right number to decide.

**Deliverables.**
- `workflows/pipe_events/fishing.py` — calls `dit.runners.docker.run(...)` against `gfw/pipe-events:<tag>`. Adds automated comparisons.
- Replacement shim in `pipe-events/integration_tests/staging-bf_bfd_bftruncate.sh`.
- Optional: `dit/phases.py` if extraction warranted.

## Phase 4+ vision (one paragraph each)

**Phase 4 — `composer-dags` sync.** `dit sync-params --from <composer-dags-checkout>` parses production DAGs and regenerates `params.yaml` rows so AIS/VMS shapes never drift between integration tests and prod. Trigger condition: a real prod-vs-test param drift bug.

**Phase 5 — Mutation library.** Promote `restrict_ssvids` (currently `compute_restricted_ssvids` inside pipe-gaps' workflow) plus `drop_messages`, `shift_timestamps`, `set_segment_flag` into `dit.mutations` once a second consumer needs the same input-transform mechanism. Cap at ~5.

**Phase 6 — Phase sharing / hash-based caching.** Hash `(image-tag, phase-config, mutation-set)`; second invocation of an identical phase is a `BQ COPY` instead of a re-run. Cuts wall-clock for CI/PR validation. Build only when the duplicate-run cost matters operationally.

**Phase 7 — Golden-table regression mode.** Per-workflow reference `_1_bf` table keyed by `(image-tag, params-hash, date-range)`; future runs assert byte-equivalence vs the golden table. Cheap PR-validation regression check; doesn't replace the four-mode test on `main`.

## Next steps

In rough priority order:

1. **Validate the cross-version capability end-to-end.** PIPELINE-1465 cross-version test currently in flight (`before=tests/temp_dataset_for_integration_tests`, `after=tests/pipeline_1465_for_integration_tests`). Result determines confidence in the broader cross-version framing.
2. **Move runtime to Cloud Build** per § Runtime & CI. Unblocks 12-hour runs from the laptop; sets up the PR-CI surface.
3. **Track 5 cutover** — replace pipe-gaps' `tests/integration/mode_equivalence.py` with a 10-line shim. Blocked on real-BQ verification of Phase 1; can be done opportunistically once you've run the pipe-gaps four-mode test once.
4. **Pipe-gaps cross-version parity** — apply the same labels + structured job-names + cross-version wrapper pattern to `workflows/pipe_gaps/mode_equivalence.py`. Mostly mechanical; half a day of work.
5. **VMS port-visits workflow** — `workflows/port_visits/vms.py`. Near-copy of `ais.py` with VMS-shape source datasets and VMS-specific param overrides from `composer-dags-production/dags/core/vms/`.
6. **AIS-full port-visits cohort.** Multi-year data. First real motivation for Cloud Build + reference caching.
7. **Upstream the `--temp_dataset` pipe-anchorages PR.** Removes the synthetic-branch scaffolding; cross-version bindings collapse to "just use main and the fix branch directly".
8. **Phase 3 — pipe-events port** + framework-extraction decision based on three-consumer evidence.

Longer-term, no committed timeline:

- Reference catalog (V2 of cross-version) — when full-cohort PR runs become common.
- PR-tier CI integration — pipeline-repo cloudbuild yaml + webhook trigger.
- Async orchestration refactor — when 24h Cloud Build jobs become operationally painful.
- Phases 4, 5, 6, 7 — see original numbering. Each waits for the trigger condition we identified for it.

## Open items (resolved or accepted as risks)

- ✅ Port-visits scope (Phase 2): AIS + VMS.
- ✅ Local-Python runner: dropped in Phase 1.
- ✅ Image-tag convention: published images.
- ✅ **`pipe-gaps` published image availability** (Phase 1 prereq): resolved by making `--build-from-source` the default for pipe-gaps' workflow.
- ✅ **Port-visits expected-equivalence** (Phase 2 risk): AIS-staging is mode-equivalent on `visit_id` (verified 2026-05-15, 3/3 pairwise green).
- ⚠️ **`table-check` distribution**: still manual install. Declare as a `data-integration-tests` install dependency when we publish dit.
- ⚠️ **pipe-anchorages `--temp_dataset` PR**: local-only patch on `dit-temp-dataset-support`. PIPELINE-1465 cross-version test relies on synthetic branches that cherry-pick this; long-term solution is upstream merge.
- ⚠️ **Runtime is the laptop.** See § Runtime & CI for the Cloud Build plan.
- ⚠️ **Pipe-gaps labels/job-name parity.** `workflows/pipe_gaps/mode_equivalence.py` doesn't yet have the structured Dataflow job names + `dit_*` labels that `port-visits/ais.py` has. Workflow-level refactor pending (item 4 of Next steps).

## Critical files referenced

- `/mnt/encrypted_data/git/pipe-gaps/tests/integration/mode_equivalence.py` — runners, modes, `compute_restricted_ssvids`, `compare_tables`. Phase 1 lifts directly.
- `/mnt/encrypted_data/git/anchorages_pipeline/src/pipe_anchorages/cli/main.py` — port-visits CLI surface (Phase 2).
- `/mnt/encrypted_data/git/anchorages_pipeline/docker-compose.yaml` — `gfw/pipe-anchorages` image identifier (Phase 2).
- `/mnt/encrypted_data/git/pipe-gaps/docs/integration-test-framework-vision.md` — destination architecture; consult before Phase 3 framework-extraction decision.
- `/mnt/encrypted_data/git/pipe-events/integration_tests/staging-bf_bfd_bftruncate.sh` — bash original, replaced in Phase 3.
- `/mnt/encrypted_data/git/composer-dags-production/dags/core/{ais,vms}/...` — production parameter source (Phase 2 onward).

## Verification per phase

**Phase 1:** pipe-gaps four-mode test produces output identical to a pre-move reference run on the same suffix/date-range. Repo is fully standalone (no symlinks back to pipe-gaps; no path-dependent imports). The `dit/` helpers are designed for three consumers' shapes, not just pipe-gaps' shape — verifiable by code review of `compare.py`'s signature and `runners/docker.py`'s parameterisation.

**Phase 2:** Both AIS and VMS port-visits workflows run end-to-end, produce comparison results, return 0 (or surface a real bug). The truncate-shape comparison path (`view_suffix=""`) works without modification to Phase 1's `compare.py`. If it doesn't: that's the abstraction-validation feedback Phase 1 was set up to receive, and `compare.py` gets adjusted before Phase 3.

**Phase 3:** pipe-events workflow runs with automated comparisons (strict improvement over bash-only). Framework-extraction decision recorded explicitly in `dit/phases.py` (if extracted) or `workflows/README.md` (if explicitly deferred).
