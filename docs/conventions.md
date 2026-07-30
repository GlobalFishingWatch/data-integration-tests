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
| Overlay image (published base + unmerged patch) | `gcr.io/world-fishing-827/dit/<pipeline>` | `:<base-version>-<what>-<patch-sha>` |
| Smoke / push-test | `gcr.io/world-fishing-827/dit/pushtest` | `:<anything>` |

**Overlay images** carry a patch a pipeline needs before it has been merged upstream, so dit is not blocked on someone else's review cycle. `FROM` the canonical published tag, `COPY` the patched files over site-packages — base image, Beam version and ENTRYPOINT inherited unchanged, so the result drops into every slot the original filled (submitter, `sdk_container_image`, docker runner). Rules: patch the sources **extracted from that image**, not a local checkout at a different version; make the build self-verifying (`RUN python -c` asserting the patched modules import and behave) so a broken overlay fails the build rather than shipping; put the patch's content hash in the tag; and retire it as soon as upstream publishes a tag containing the change. First instance: `dit/encounters:v4.4.0-temp-dataset-d2536aaf` (2026-07-30), documented in [`encounters-onboarding-2026-07.md`](encounters-onboarding-2026-07.md).

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
