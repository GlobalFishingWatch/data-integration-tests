# dit conventions

Naming + path conventions used across dit. Referenced from `CLAUDE.md` § Working agreements and the cloudbuild yamls. Keep this short and operational.

## Prod-infra boundary

**Absolute rule.** ditbox / dit will **never** request, design toward, or write to resources in `gfw-int-infrastructure` — covering builds (image registries), data (BQ datasets, GCS buckets), Terraform, and IAM. All dit writes stay in `world-fishing-827` namespaces. If a proposed design seems to require writing to `gfw-int-infrastructure`, the design is wrong, not the IAM — re-route through a dit-namespaced path, or treat the gap as an upstream ask of whoever owns that infrastructure.

dit is a **testing-shaped consumer** of GFW infrastructure. Reading from prod tables, snapshotting source data, and submitting Dataflow jobs as `automated-testing@world-fishing-827` are in scope. Modifying anything in production-shaped paths is not.

**Prod-shaped, do not touch:**

- The `gfw-int-infrastructure` project (canonical pipeline image registry, prod Terraform). dit never pushes images or writes IAM there.
- Top-level image namespaces inside `gcr.io/world-fishing-827/` that belong to canonical pipelines: `anchorages_pipeline/`, `encounters_pipeline/`, `advanced_fishing_detection/`, etc. The project is the same; the namespace is reserved.
- Production Composer DAGs.
- Release branches of pipeline repos.

**In scope (explicit exception):**

- **Creating branches in pipeline repos** (`pipe-anchorages`, `pipe-gaps`, `pipe-events`) with potential fixes that may eventually merge to prod. The branch existing is normal dev workflow; only an actual merge is prod-touching, and that's owned by whoever holds the merge button, not by dit.
- Reading anything anywhere — BQ tables, registry images, GCS objects.

The boundary is mechanically enforced by IAM. `christian.homberg@globalfishingwatch.org` has `uploadArtifacts` on `world-fishing-827` only; trying to push to `gfw-int-infrastructure` fails with a permission error. If you find yourself wanting to elevate permissions to make something work, the design is probably wrong — re-route through a dit-namespaced path first.

## Image namespace

All dit container images live under **`gcr.io/world-fishing-827/dit/`**. Path-within-existing-repo, not a new repo. Only `uploadArtifacts` needed (project-level grant we already have).

| Purpose | Path | Tag scheme |
|---|---|---|
| ditbox tooling image | `gcr.io/world-fishing-827/dit/ditbox` | `:latest` + `:<short-sha>` |
| Auto-built pipeline image (M-pivot-4 + docker-runner generalisation) | `gcr.io/world-fishing-827/dit/<pipeline>` | `:dit-<pipeline_commit>` |
| Per-binding pipeline image (cross-version) | `gcr.io/world-fishing-827/dit/<pipeline>` | `:<experiment-id>-<binding-name>` |
| Smoke / push-test | `gcr.io/world-fishing-827/dit/pushtest` | `:<anything>` |

The auto-built pipeline image row covers both **Dataflow worker images** (Beam workers pull the image) and **docker-runner pipeline images** (dit's docker runner pulls and runs the image directly). Same namespace, same kaniko build machinery, same `:dit-<pipeline_commit>` tag scheme; what changes is the consumer. **Trigger is symmetric across both consumers**: `dit.worker_image.ensure_pipeline_image` builds when ALL of (a) `worker_image == default_worker_image` (no explicit override) and (b) the run is `unreviewed`. Reviewed code at the pinned default is pulled from the canonical upstream registry (`us-central1-docker.pkg.dev/gfw-int-infrastructure/<repo>/<package>:vX.Y.Z`); only unreviewed code triggers a fresh build under the dit namespace. Canonical upstream publications are read-only to dit — never a write target.

Examples:

- `gcr.io/world-fishing-827/dit/ditbox:latest`
- `gcr.io/world-fishing-827/dit/pipe-gaps:dit-<pipeline_commit>` (auto-built for unreviewed Beam code)
- `gcr.io/world-fishing-827/dit/pipe-anchorages:pipeline-1465-after` (cross-version, per-binding)
- `gcr.io/world-fishing-827/dit/pipe-events:dit-<pipeline_commit>` (auto-built for unreviewed docker-runner code)

Canonical upstream defaults the workflows pin (read-only to dit):

- pipe-anchorages: `us-central1-docker.pkg.dev/gfw-int-infrastructure/core/pipe-anchorages:v4.6.4`
- pipe-events: `us-central1-docker.pkg.dev/gfw-int-infrastructure/publication/github-globalfishingwatch-pipe-events:v4.2.17`

## Auth in the cloud path (ditbox)

Three execution contexts for any docker-runner pipeline, all converging on the standard ADC path (`/root/.config/gcloud/application_default_credentials.json`) inside the container — workflow code is identical across them, only the runtime environment differs:

| Context | How ADC reaches the container |
|---|---|
| Laptop | Workflow passes `volumes=["gcp:/root/.config"]`; docker runner mounts the user's named volume populated by `gcloud auth application-default login`. |
| Production (Composer/GKE) | Pod runs in GKE with Workload Identity; container finds ADC via the GKE **metadata server**. **No file mount, no env var, no credential material on disk.** |
| ditbox (Cloud Build) | dit's docker runner adds `--network=host` so the inner container shares the build VM's network namespace and reaches Cloud Build's metadata server at `169.254.169.254`; google-auth's ADC chain finds the metadata server and obtains a fresh OAuth token bound to the build SA. The workflow-passed `gcp:/root/.config` mount is dropped (the named volume doesn't exist in Cloud Build). **No credential material on disk.** |

**Triggered by `DIT_CLOUD_MODE` env var, not a workflow parameter.** When the env var is set (any non-empty value), `src/dit/runners/docker.py` adds `--network=host` to the docker invocation and drops any caller-supplied mount targeting `/root/.config` (or below). When unset (laptop), the workflow's `volumes=[...]` is used verbatim. Workflows stay unaware of the execution context.

**Why metadata-server (host networking) instead of a credential file.** The original 2026-06-02 design wrote a short-lived `authorized_user`-shaped ADC JSON to `/workspace/dit-adc.json` and bind-mounted it `:ro` into the inner container. The first live cloud run falsified the assumption underpinning that design: the older `google-auth` baked into pipe-events' Python 3.8 image refreshes `authorized_user` credentials before the first API call (ignoring the pre-issued `token` field), and the refresh against placeholder OAuth client material failed with `invalid_client`. `--network=host` sidesteps the problem — the container never holds a long-lived secret, just like prod (which gets ADC via GKE's metadata server through Workload Identity). The trade-off accepted: the inner container shares the build VM's network namespace. For an ephemeral per-build VM with no co-tenancy and only the build's own steps running, the practical surface increase is essentially zero.

**Hardening considered and declined.** A dedicated narrower-scoped impersonation SA (e.g. `dit-pipe-events-runner@`) was on the table. We decided against it: the build SA (`automated-testing@`) is *already* scoped by design — the absolute prod-infra boundary keeps it out of `gfw-int-infrastructure`, writes are dit-namespaced, tokens are ~1h-lived — and a narrower runner SA would marginally tighten that without addressing a real threat. The only path that would *materially* change the auth model is migrating ditbox to GKE / Cloud Run Jobs so the inner workload gets keyless metadata-server auth via Workload Identity (rather than the build VM's metadata server) — reserved as a future *architectural* upgrade if longer-term needs co-justify it.

This is testing-shaped infrastructure that respects the absolute [prod-infra boundary](#prod-infra-boundary): the build SA lives in `world-fishing-827`, no writes to `gfw-int-infrastructure`, no permanent credentials anywhere.

## Standard build-and-push workflow

For per-binding pipeline images used by `--binding-worker-image`:

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

Adding a policy keyed on the `dit/` prefix with a 30-day TTL is a TODO for when experiment volume warrants it (the same logic that lets snapshot datasets auto-clean — let TTL do the work).
