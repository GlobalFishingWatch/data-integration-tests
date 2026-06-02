# dit conventions

Naming + path conventions used across dit. Referenced from `CLAUDE.md` § Working agreements and the cloudbuild yamls. Keep this short and operational.

## Prod-infra boundary

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
