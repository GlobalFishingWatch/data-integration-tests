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

The auto-built pipeline image row covers both **Dataflow worker images** (M-pivot-4, today's use — Beam workers pull the image) and **docker-runner pipeline images** (planned for ditbox-for-pipe-events — dit's docker runner pulls and runs the image directly). Same namespace, same kaniko build machinery, same `:dit-<pipeline_commit>` tag scheme; what changes is the consumer. Canonical upstream publications (when they exist, e.g. `us-central1-docker.pkg.dev/gfw-int-infrastructure/core/pipe-anchorages:v4.6.4`) are read-only to dit — never a write target.

Examples:

- `gcr.io/world-fishing-827/dit/ditbox:latest`
- `gcr.io/world-fishing-827/dit/pipe-gaps:dit-<pipeline_commit>` (M-pivot-4 worker image)
- `gcr.io/world-fishing-827/dit/pipe-anchorages:pipeline-1465-after` (cross-version, per-binding)
- `gcr.io/world-fishing-827/dit/pipe-events:dit-<pipeline_commit>` (planned — docker-runner auto-build)

## Auth in the cloud path (ditbox)

Three execution contexts for any docker-runner pipeline, all converging on the standard ADC path (`/root/.config/gcloud/application_default_credentials.json`) inside the container — workflow code is identical across them, only the runtime environment differs:

| Context | How ADC reaches the container |
|---|---|
| Laptop | Workflow passes `volumes=["gcp:/root/.config"]`; docker runner mounts the user's named volume populated by `gcloud auth application-default login`. |
| Production (Composer/GKE) | Pod runs in GKE with Workload Identity; container finds ADC via the GKE **metadata server**. **No file mount, no env var, no credential material on disk.** |
| ditbox (Cloud Build) | Cloud Build step writes a **short-lived** ADC file to `/workspace`; docker runner bind-mounts it `:ro` at the standard path inside the inner container. The workflow-passed `gcp:/root/.config` mount is dropped (the named volume doesn't exist in Cloud Build). |

**Triggered by `DIT_CLOUD_AUTH_ADC` env var, not a workflow parameter.** When the env var is set (path to a readable ADC file on the build host), `src/dit/runners/docker.py` drops any caller-supplied mount whose target is `/root/.config` (or below) and adds a `:ro` bind-mount of the ADC file at the standard ADC path. When unset (laptop), the workflow's `volumes=[...]` is used verbatim. Workflows stay unaware of the execution context.

**Short-lived token, not an SA key.** The Cloud Build write-ADC step uses the build SA's metadata-server identity (`gcloud auth application-default print-access-token`) to produce an ~60-minute access token, written into an `authorized_user`-shaped JSON. `gcloud iam service-accounts keys create` is **not** an acceptable substitute — a permanent key on disk is exactly the failure mode this design avoids. If a build outlives the token TTL, the next BQ call gets a loud `invalid_grant` error (the refresh-related fields in the JSON are placeholders that fail the OAuth refresh endpoint by design); silent fallback to any other identity is not possible.

**Hardening considered and declined (2026-06-02).** Spawning a dedicated narrower-scoped impersonation SA (e.g. `dit-pipe-events-runner@`) was on the table to reduce blast radius if the on-disk ADC file leaked. We decided against it: the build SA (`automated-testing@`) is *already* scoped by design — the absolute prod-infra boundary keeps it out of `gfw-int-infrastructure`, writes are dit-namespaced, tokens are ~1h-lived — and a narrower runner SA would marginally tighten that without addressing a real threat (it adds an SA + 2 IAM bindings to maintain instead). The placeholder-JSON shape is a *cosmetic* concern (literal `"placeholder"` strings + loud `invalid_grant` failure mode), not a security one. **`--network=host` (metadata-server access from the inner container) was also considered and declined** — it trades the cosmetic-JSON smell for a structural shared-network-namespace concession that reviews worse and gives a compromised container token-refresh-on-demand instead of a fixed-TTL leak window. The only path that would *materially* change the auth model is migrating ditbox to GKE / Cloud Run Jobs so the inner workload gets keyless metadata-server auth the way prod does — reserved as a future *architectural* upgrade if longer-term needs co-justify it.

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
