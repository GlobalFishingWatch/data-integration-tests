# DIT conventions

The rules a change to DIT must keep: where DIT may write, how images are named, how Dataflow settings are handled, how credentials reach a container. Referenced from `CLAUDE.md` § Working agreements and from the Cloud Build yamls.

For what the pieces are, read [`framework.md`](framework.md) first. This page states the rules and their reasons.

| Section | Rule in one line |
|---|---|
| [Prod-infra boundary](#prod-infra-boundary) | Read anything; write only to DIT's namespaces in `world-fishing-827`. |
| [Runner-only Dataflow knobs](#runner-only-dataflow-knobs) | Worker cap and sizing never change output and never enter a cache key. |
| [Bindings that differ in their interface](#bindings-that-differ-in-their-interface) | Same interface by default; differences are declared adjustments with a reason. |
| [Image namespace](#image-namespace) | Every DIT image lives under `gcr.io/world-fishing-827/dit/`. |
| [Auth in the cloud path (ditbox)](#auth-in-the-cloud-path-ditbox) | No credential files; `--network=cloudbuild` in Cloud Build. |
| [Container env vars](#container-env-vars-workflow-driven-via-container_env) | `container_env` reaches the container; `env` does not. |
| [Standard build-and-push workflow](#standard-build-and-push-workflow) | How to build an image for one binding. |
| [Cleanup](#cleanup) | Images are deleted by hand. |

## Prod-infra boundary

**Absolute rule.** ditbox / dit will **never** request, design toward, or write to resources in `gfw-int-infrastructure` — covering builds (image registries), data (BQ datasets, GCS buckets), Terraform, and IAM. All dit writes stay in `world-fishing-827` namespaces. If a proposed design seems to require writing to `gfw-int-infrastructure`, the design is wrong, not the IAM — re-route through a dit-namespaced path, or treat the gap as an upstream ask of whoever owns that infrastructure.

dit is a **testing-shaped consumer** of GFW infrastructure. Reading from prod tables, pinning source data by copying it into DIT's dataset, and submitting Dataflow jobs as `automated-testing@world-fishing-827` are in scope. Modifying anything in production-shaped paths is not.

**Prod-shaped, do not touch:**

- The `gfw-int-infrastructure` project (canonical pipeline image registry, prod Terraform). dit never pushes images or writes IAM there.
- Top-level image namespaces inside `gcr.io/world-fishing-827/` that belong to canonical pipelines: `anchorages_pipeline/`, `encounters_pipeline/`, `advanced_fishing_detection/`, etc. The project is the same; the namespace is reserved.
- Production Composer DAGs.
- Release branches of pipeline repos.
- Production's own prefixes in shared GCS buckets. The default Dataflow bucket (`DIT_DATAFLOW_TEMP_BUCKET=pipe-temp-us-central-ttl7`) is also production's, and production writes directly under `dataflow_temp/` and `dataflow_staging/`. Every dit job writes under `gs://<bucket>/dit/` instead. Build those paths only through `dit.dataflow_paths`, never by hand. Since 2026-10-22.
- Production's regional Dataflow quota in `world-fishing-827`, which also runs production jobs. Every Dataflow workflow takes `--max-num-workers` through `dit.workflow.add_max_num_workers_arg`, resolves it with `worker_cap` in `parse_args`, logs it once with `log_worker_caps`, and emits it only through `max_num_workers_options`. A new workflow picks its own default and never raises an existing one. Precedence, the more specific setting winning: the explicit flag, else `min(default, DIT_MAX_NUM_WORKERS_CEILING)`, else the default. The ceiling env var reaches only processes started from the operator's shell: `cloudbuild-dit.yaml` passes no such variable into the build step, so cloud runs need `--max-num-workers` in `ARGS` until that is wired (follow-up). Since 2026-10-23 (PIPELINE-4701).
- Production's external-IP quota, likewise shared. Every Dataflow job runs with private worker IPs, as production does: a workflow emits `dit.workflow.NO_PUBLIC_IPS_OPTION` (`--no_use_public_ips`) in its Dataflow options. pipe-gaps adds nothing, because gfw-common's own default is already private and dit passes nothing that overrides it (a test pins both). Since 2026-10-23.

**In scope (explicit exception):**

- **Creating branches in pipeline repos** with potential fixes that may eventually merge to prod. The branch existing is normal dev workflow; only an actual merge is prod-touching, and that's owned by whoever holds the merge button, not by dit.
- Reading anything anywhere — BQ tables, registry images, GCS objects.

The boundary is enforced by IAM. DIT's identities may upload images to `world-fishing-827` only; a push to `gfw-int-infrastructure` fails with a permission error. If you find yourself wanting more permissions to make something work, the design is probably wrong — re-route through a dit-namespaced path first.

## Runner-only Dataflow knobs

The worker cap (`--max-num-workers`), the worker machine type (`--worker-machine-type`) and the boot disk size (`--disk-size-gb`) change how fast a job runs and whether it runs out of memory. **Output must not depend on them.** So they are never part of a cache key, and a workflow whose output changes with them has a bug. Each goes through its `dit.workflow` helpers (`add_max_num_workers_arg` / `worker_cap`, `add_worker_sizing_args` / `resolve_worker_sizing` / `worker_sizing_options`), and each resolves the same way: the explicit flag wins, else the workflow's default. Since 2026-10-23.

**Sizing defaults mirror production; overrides are encouraged for non-production data.**
- Every Dataflow workflow defaults each step's machine type and disk to production's for the production step it mirrors. They live in a per-step `STEP_WORKER_SIZING` constant in the workflow, hand-written with a citation of composer-dags-production's rendered snapshot (`tests/render_dags/snapshots/<dag>.py`, task id, commit), until PIPELINE-4700 reads them as data. A step production runs without sizing options gets none (`WorkerSizing()`).
- Where production's value is a Jinja conditional, the constant's comment says which branch it mirrors and why.
- A new workflow follows the same rule. Worker caps do not: they keep their own defaults (above).
- Production machine types cost more per worker-hour than a staging run needs. Pass a smaller `--worker-machine-type` / `--disk-size-gb` for non-production data: every flag's help says so, and a Dataflow run that keeps a production-mirrored field while reading the AIS staging cohort logs a `cost hint:` warning (`dit.workflow.log_production_sizing_hint`). The hint never prompts or blocks.

Resolution order and the defaults per workflow: [`framework.md`](framework.md) § 8.

## Bindings that differ in their interface

In a cross-version workflow every binding gets the base interface by default: the same command line, inputs and outputs, mirroring production today. Where a binding's pipeline differs, it gets adjustments (`dit.adjust`). How they work: [`framework.md`](framework.md) § 10; design record: [`plan.md`](plan.md) § "Per-side adjustments". Since 2026-11-24, first in `workflows/pipe_segment/identity_match_key.py`.

- **Shared in a run, never adjusted:** the DIT checkout, experiment id, date window, pin stamp, destination dataset, and the comparison tool.
- **Declared adjustments live next to the workflow** (`workflows/<pkg>/adjustments.py`), each with a `reason` that says what differs in the pipeline. Its `applies` asks the ref's own CLI source first (an unreleased head carries the previous version number), its version second.
- **Ad hoc edits** (`--binding-adjust NAME=<step>:<op>:<flag>[=<value>]`) are flag edits only and are for refs not released yet. A change that holds for a version range becomes a declared adjustment.
- **An edit that finds nothing to change is an error.** Do not catch it to make a rule optional; a stale rule must be loud.
- **Every declared adjustment for an older version names a composer commit** at which production last ran that version, and gets a frozen drift spec (`WorkflowSpec.composer_ref`) checked in `tests/test_composer_drift_live.py`.
- **Preflight against the ref's CLI source** before anything is pinned: a pipeline that hands unknown flags to Beam ignores a wrong flag silently. Beam's options are exempt on Beam steps only, from a hand-written list next to the workflow that says how it was worked out.
- **Removal:** keep an adjustment and its frozen spec only while their version is production's current or previous one (owner decision, 2026-11-24).
- **Later, the pipeline repos define their own interface** ([PIPELINE-4819](https://globalfishingwatch.atlassian.net/browse/PIPELINE-4819)); DIT's version-range table becomes the fallback for older refs. See `docs/plan.md` § "Per-side adjustments".

## Image namespace

All dit container images live under **`gcr.io/world-fishing-827/dit/`**. Path-within-existing-repo, not a new repo. Only `uploadArtifacts` needed (project-level grant we already have).

Which image a run uses, and when one is built automatically, is decided per run: [`framework.md`](framework.md) § 7. Tree view of the namespace: [`architecture.md`](architecture.md) § 8.

| Purpose | Path | Tag scheme |
|---|---|---|
| ditbox tooling image | `gcr.io/world-fishing-827/dit/ditbox` | `:latest` + `:<short-sha>` |
| Auto-built pipeline image (M-pivot-4 + docker-runner generalisation) | `gcr.io/world-fishing-827/dit/<pipeline>` | `:dit-<pipeline_commit>` |
| Per-binding pipeline image (cross-version) | `gcr.io/world-fishing-827/dit/<pipeline>` | `:<experiment-id>-<binding-name>` |
| Overlay image (published base + unmerged patch) | `gcr.io/world-fishing-827/dit/<pipeline>` | `:<base-version>-<what>-<patch-sha>` |
| Source-built overlay image (commit + test-only patch) | `gcr.io/world-fishing-827/dit/<pipeline>` | `:<label>-<commit7>-<what>-<patch-sha>` |
| Smoke / push-test | `gcr.io/world-fishing-827/dit/pushtest` | `:<anything>` |

**Overlay images** carry a patch a pipeline needs before it has been merged upstream, so dit is not blocked on someone else's review cycle. `FROM` the canonical published tag, `COPY` the patched files over site-packages — base image, Beam version and ENTRYPOINT inherited unchanged, so the result drops into every slot the original filled (submitter, `sdk_container_image`, docker runner). Rules: patch the sources **extracted from that image**, not a local checkout at a different version; make the build self-verifying (`RUN python -c` asserting the patched modules import and behave) so a broken overlay fails the build rather than shipping; put the patch's content hash in the tag; and retire it as soon as upstream publishes a tag containing the change. First instance: `dit/encounters:v4.4.0-temp-dataset-d2536aaf` (2026-07-30), documented in [`encounters-onboarding-2026-07.md`](encounters-onboarding-2026-07.md).

**Source-built variant** (2026-10-22, pipe-anchorages, [`docker/overlays/pipe-anchorages-temp-dataset/`](../docker/overlays/pipe-anchorages-temp-dataset/README.md)). When the commit under test has no published image, which is the normal case for a PR branch since pipelines build images only for `main` and tags, there is nothing to `FROM`. The overlay is then a patch file kept in dit and applied to the commit's own source: `git archive <commit>`, `git apply --check` then `git apply`, and `docker build --target prod` with the pipeline's Dockerfile. Baseline and candidate are built the same way, so the overlay never differs between the two sides of an A/B. The same rules apply: the build fails if the patch does not apply, it runs a self-check inside the built image, and the patch's content hash goes in the tag. This patch is test-only by decision and is not retired by an upstream release. Retire it only if upstream ever gains the capability natively.

**An overlay image as a workflow's default** (2026-11-19, port visits). `workflows/port_visits/ais.py` defaults its submitter and worker image to the source-built overlay of pipe-anchorages v5.0.1, the version production runs. Two rules follow, and apply to any workflow that does the same:
- The workflow opts out of the auto-build below (it passes `check_images` to `dit.workflow.resolve_run_context`). An auto-built image is plain source without the overlay, so it cannot stand in for the default.
- For unreviewed code the workflow refuses to run a default image and names the overlay build command. The operator builds the overlay image of that commit and passes it explicitly.

The default overlay image must be rebuilt, and the workflow's constants bumped, whenever production bumps the pipeline version. The overlay README lists the steps.

**Second instance, with one patch per pipeline generation** (2026-11-20, pipe-segment, [`docker/overlays/pipe-segment-temp-dataset/`](../docker/overlays/pipe-segment-temp-dataset/README.md)). Both pipe-segment workflows default to the source-built overlay of pipe-segment v5.1.0, the version the AIS DAG is configured for; Both follow the same two rules: `workflows/pipe_segment/ais.py` from the start, and `identity_match_key.py` since 2026-11-25 (its `check_images` refuses an unreviewed binding on the default image, before anything is pinned; until then it auto-built a plain image). What is new:
- **The read sites differ between the two versions dit drives**, so the directory holds one patch for each, and `build.sh` applies the first that passes `git apply --check`. The self-check is told which patch was applied.
- **The self-check counts every BigQuery read in the whole package**, not only in the patched modules, so a read added upstream in a new module also fails the build.
- **Start the image label with the pipeline version** (`v5.1.0`, `v5.1.0-pr230`). The workflow reads it from the tag to decide whether the image knows a flag that older code would ignore silently.

The auto-built pipeline image row covers both **Dataflow worker images** (Beam workers pull the image) and **docker-runner pipeline images** (dit's docker runner pulls and runs the image directly). Same namespace, same kaniko build machinery, same `:dit-<pipeline_commit>` tag scheme; what changes is the consumer. **Trigger is symmetric across both consumers**: `dit.worker_image.ensure_pipeline_image` builds when ALL of (a) `worker_image == default_worker_image` (no explicit override) and (b) the run is `unreviewed`. Reviewed code at the pinned default is pulled from the canonical upstream registry (`us-central1-docker.pkg.dev/gfw-int-infrastructure/<repo>/<package>:vX.Y.Z`); only unreviewed code triggers a fresh build under the dit namespace. Canonical upstream publications are read-only to dit — never a write target.

Examples:

- `gcr.io/world-fishing-827/dit/ditbox:latest`
- `gcr.io/world-fishing-827/dit/pipe-gaps:dit-<pipeline_commit>` (auto-built for unreviewed Beam code)
- `gcr.io/world-fishing-827/dit/pipe-anchorages:v5.0.1-767770f-temp-dataset-905dc761` (source-built overlay; the port-visits default)
- `gcr.io/world-fishing-827/dit/pipe-anchorages:pipeline-4690-14b4be8-temp-dataset-905dc761` (source-built overlay of a PR head; cross-version, per-binding)
- `gcr.io/world-fishing-827/dit/pipe-events:dit-<pipeline_commit>` (auto-built for unreviewed docker-runner code)
- `gcr.io/world-fishing-827/dit/pipe-segment:v5.1.0-4ddde03-temp-dataset-04b40b54` (source-built overlay; the default of both pipe-segment workflows; built and pushed 2026-11-24, `sha256:f785658b99d8caea217be4575fb951336bfeab8c36d253d3b3309026ffadf8eb`)
- `gcr.io/world-fishing-827/dit/pipe-segment:v5.0.3-f4d39fe-temp-dataset-82b7f0a8` (source-built overlay of the version the VMS DAG pins; built and pushed 2026-11-24, `sha256:96ac0dbc7103412e1017d0d27e78dc8630ee01ad2e0068abc93f2653bd13de9c`)

Canonical upstream defaults the workflows pin (read-only to dit):

- pipe-events: `us-central1-docker.pkg.dev/gfw-int-infrastructure/publication/github-globalfishingwatch-pipe-events:v4.2.17`

pipe-anchorages is no longer in this list: until 2026-11-19 the port-visits workflows pinned the upstream v4.6.4 image, and now they default to dit's own overlay image (above).

## Auth in the cloud path (ditbox)

Three execution contexts for any docker-runner pipeline, all converging on the standard ADC path (`/root/.config/gcloud/application_default_credentials.json`) inside the container — workflow code is identical across them, only the runtime environment differs:

| Context | How ADC reaches the container |
|---|---|
| Laptop | Workflow passes `volumes=["gcp:/root/.config"]`; docker runner mounts the user's named volume populated by `gcloud auth application-default login`. |
| Production (Composer/GKE) | Pod runs in GKE with Workload Identity; container finds ADC via the GKE **metadata server**. **No file mount, no env var, no credential material on disk.** |
| ditbox (Cloud Build) | dit's docker runner adds `--network=cloudbuild` so the inner container attaches to the per-build `cloudbuild` docker network where a fake metadata server returns OAuth tokens for the build SA (`automated-testing@`). google-auth's ADC chain finds the fake metadata server at `metadata.google.internal` and obtains a fresh token. The workflow-passed `gcp:/root/.config` mount is dropped (the named volume doesn't exist in Cloud Build). **No credential material on disk.** |

**Triggered by `DIT_CLOUD_MODE` env var, not a workflow parameter.** When the env var is set (any non-empty value), `src/dit/runners/docker.py` adds `--network=cloudbuild` to the docker invocation and drops any caller-supplied mount targeting `/root/.config` (or below). When unset (laptop), the workflow's `volumes=[...]` is used verbatim. Workflows stay unaware of the execution context.

**Why `--network=cloudbuild` and not `--network=host`.** Cloud Build runs each build step on a VM where two metadata servers coexist on different docker networks. The **fake metadata server** sits on a network literally named `cloudbuild`; it returns tokens for the user-configured `serviceAccount:` (here, `automated-testing@`). Every build-step container is auto-attached to that network — which is why the build step itself sees `automated-testing@`. The **real metadata server** lives on the VM's default network namespace and returns the Google-managed `cloudbuild-untrusted@argo-prod-*` identity (the docker daemon host). `docker run --network=host` puts the sibling container on the daemon's host network — i.e. the real metadata server — so it sees the Google-managed identity, not the build SA. `--network=cloudbuild` is the documented sibling-container pattern (see `cloud-build-local`'s open-source `metadata.go`, [earthly/earthly#1628](https://github.com/earthly/earthly/issues/1628)) that re-attaches the sibling to the fake metadata server.

**Two earlier designs falsified by live evidence (preserved here as institutional memory).**

1. **Bind-mounted `authorized_user` ADC JSON** (2026-06-02 PR #34). The older `google-auth` baked into pipe-events' Python 3.8 image refreshes `authorized_user` credentials before the first API call (ignoring the pre-issued `token` field). Refresh against placeholder OAuth client material failed with `invalid_client`. Abandoned for `--network=host`.
2. **`docker run --network=host`** (2026-06-02 PR #39). Attached the inner container to the docker daemon's host network — the real metadata server's network — instead of the build step's. The inner container saw `cloudbuild-untrusted@argo-prod-*` instead of `automated-testing@`. Symptom was `USER_PROJECT_DENIED` even after granting `roles/serviceusage.serviceUsageConsumer` to `automated-testing@` (the grant was applied to the wrong principal because the actual caller wasn't `automated-testing@`). Confirmed empirically by a metadata-server probe: under `--network=host` the sibling returns `cloudbuild-untrusted@argo-prod-us-west1.iam.gserviceaccount.com`; under `--network=cloudbuild` it returns `automated-testing@world-fishing-827.iam.gserviceaccount.com`. Abandoned for `--network=cloudbuild`.

The chain reveals two recurring lessons: live evidence trumps theoretical reasoning about Cloud Build's runtime topology, and "Cloud Build runs as my SA" is true at the build-step level but breaks down for nested docker without `--network=cloudbuild`.

**Hardening considered and declined.** A dedicated narrower-scoped impersonation SA (e.g. `dit-pipe-events-runner@`) was on the table. We decided against it: the build SA (`automated-testing@`) is *already* scoped by design — the absolute prod-infra boundary keeps it out of `gfw-int-infrastructure`, writes are dit-namespaced, tokens are ~1h-lived — and a narrower runner SA would marginally tighten that without addressing a real threat. The only path that would *materially* change the auth model is migrating ditbox to GKE / Cloud Run Jobs so the inner workload gets keyless metadata-server auth via Workload Identity scoped to its own KSA — reserved as a future *architectural* upgrade if longer-term needs co-justify it.

This is testing-shaped infrastructure that respects the absolute [prod-infra boundary](#prod-infra-boundary): the build SA lives in `world-fishing-827`, no writes to `gfw-int-infrastructure`, no permanent credentials anywhere.

## Container env vars (workflow-driven, via `container_env=...`)

Distinct from the env-triggered cloud-mode plumbing above, workflows can also pass arbitrary env vars *into* the inner container via `dit_docker.run(container_env={"KEY": "VALUE", ...})`. The runner emits `-e KEY=VALUE` flags into the docker / docker compose invocation. Two parameters with similar names live on `dit_docker.run` — easy to conflate:

| Param | Reaches | Use case |
|---|---|---|
| `env={...}` | The HOST `docker` / `docker compose` subprocess | Tweak docker's own behaviour (rare; e.g. `DOCKER_BUILDKIT=0`). Does NOT reach the inner container. |
| `container_env={...}` | The inner container's process env | What workflows want when the pipeline's CLI relies on env-var defaults the `--flag` surface doesn't cover. |

Concrete case (pipe-segment v5.0.x smoke, 2026-06-03): Beam's `WriteToBigQuery` constructs its own `google-cloud-bigquery` client whose default-project resolution walks `GOOGLE_CLOUD_PROJECT` env → ADC metadata; the pipeline option `--project=...` is read earlier by Beam and never forwarded to that internal client. `examples/example_segment.sh` documents the same escape hatch via inline `-e GOOGLE_CLOUD_PROJECT=...` on the docker compose command — `container_env` lifts that into the harness so workflows that go through `dit_docker.run` get it cleanly. Default `None` means no `-e` flags are emitted, byte-identical to existing callers.

## Standard build-and-push workflow

For a workflow whose default image is an overlay (port visits, pipe-segment), build the binding's image with the `build.sh` of that overlay under [`docker/overlays/`](../docker/overlays/), not with the plain build below.

For per-binding pipeline images of a pipeline without an overlay, used by `--binding-worker-image`:

```bash
WORKTREE=$(mktemp -d)
git -C $PROJECTS/<pipeline-repo> worktree add --force $WORKTREE <ref>
docker build -t gcr.io/world-fishing-827/dit/<pipeline>:<experiment-id>-<binding-name> $WORKTREE
docker push   gcr.io/world-fishing-827/dit/<pipeline>:<experiment-id>-<binding-name>
git -C $PROJECTS/<pipeline-repo> worktree remove --force $WORKTREE
```

For ditbox: `make publish-ditbox` (runs via Cloud Build; no local docker daemon needed).

Sanity check before the first real push to a new image name in the namespace:

```bash
docker pull alpine:3.19
docker tag  alpine:3.19 gcr.io/world-fishing-827/dit/pushtest:1
docker push             gcr.io/world-fishing-827/dit/pushtest:1
gcloud artifacts docker images delete \
    us-docker.pkg.dev/world-fishing-827/gcr.io/dit/pushtest --delete-tags --quiet
```

## Cleanup

No Artifact Registry cleanup policy in place yet. Manual cleanup:

```bash
gcloud artifacts docker images delete \
    us-docker.pkg.dev/world-fishing-827/gcr.io/dit/<image> \
    --delete-tags --quiet
```

Adding a policy keyed on the `dit/` prefix with a 30-day TTL is a TODO for when experiment volume warrants it. Pinned source copies already clean themselves up this way, through their table expiration.
