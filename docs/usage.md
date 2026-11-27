# Using DIT

How to install DIT, run a workflow, and handle the common situations. For what the pieces are and why, read [`framework.md`](framework.md) first. For what a particular workflow does, see [`workflows/README.md`](../workflows/README.md).

This page was moved out of the README on 2026-11-25. The commands are unchanged unless a note says so.

- [Install](#install)
- [Run a workflow](#run-a-workflow)
- [The choices in a run](#the-choices-in-a-run)
- [Scenarios](#scenarios)
- [Snapshots and credential safety](#snapshots-and-credential-safety)
- [Cleaning up](#cleaning-up)

---

## Install

```bash
python3 -m venv venv && source venv/bin/activate
make install-pipe-gaps      # or install-port-visits / install-pipe-events / install-pipe-segment
```

DIT itself is pipeline-agnostic. A pipeline's package is not in DIT's `requirements.txt`; the Makefile targets install it next to DIT.

- The Makefile expects the pipeline checkouts next to this repo (`PROJECTS`, default: the parent directory). If yours are elsewhere, prefix any target with `PROJECTS=/path`, or copy `.envrc.example` to `.envrc` (gitignored; loaded by [direnv](https://direnv.net/)).
- `make install` installs the framework only. The Dataflow runner then cannot load, because `apache-beam` comes with a pipeline install.
- `make install-all` pins pipe-gaps' Beam version, 2.76.0 since 2026-11-27. Until then it was 2.71.0, which a pipe-anchorages checkout at v5 does not accept; with 2.76.0 the pipelines' package requirements should all accept it (read, not run; comment in the [`Makefile`](../Makefile)). If it does not resolve, install the pipelines one by one. The port-visits workflows do not import pipe-anchorages; they run it in its image.

### Install modes

| When | Target | What happens |
|---|---|---|
| Active development on a pipeline | `make install-<pipeline>` | Editable install. Edits in the working tree are picked up at once. |
| A reproducible run of a committed ref | `make install-<pipeline>-ref REF=<sha-or-branch>` | Non-editable install of exactly that commit, from `git+file://`. |
| A reproducible run of the working tree | `make snapshot-<pipeline>` | Builds a snapshot commit of the tracked files, pushes it to `refs/dit-snapshots/<pipeline>/<sha>` on origin, and installs from that ref. |
| The target ref changed its dependencies | add `FULLDEPS=1` | Drops `--no-deps`, so pip reinstalls the full dependency tree. |
| Remove one snapshot ref | `make clean-snapshot PIPELINE=<name> REF=<sha-or-full-ref>` | Deletes the ref locally and on origin. For secret remediation only. |

Notes:

- The non-editable modes point a debugger at the installed copy under `venv/`, not at your working tree. Use the editable install when stepping through pipeline source.
- The Beam version of each install is pinned to the one in that pipeline's worker image (the `*_BEAM_VERSION` constants in the Makefile). This matters for pipe-gaps, whose jobs are submitted from the venv.
- Snapshot mode needs `git push` permission on the pipeline's origin and `gitleaks` on `PATH`. See [Snapshots and credential safety](#snapshots-and-credential-safety).

---

## Run a workflow

```bash
dit run workflows/<pipeline>/<workflow>.py [workflow-args...]
```

`dit run` loads the Python file and calls its `main(argv) -> int`. A workflow can also be imported and called from a test: DIT is a library first.

Every workflow has `--help`. Seven of the ten have `--dry-run`, which reports what would be pinned and run and creates nothing: the cross-version wrappers, both pipe-gaps workflows, `port_visits/ais.py` and both pipe-segment workflows.

**Example: Beam in a container, submitting to Dataflow.** Mode equivalence for port visits on the staging cohort. Run it from a clean pipe-anchorages checkout of a commit merged into `origin/main`; it then executes DIT's default image.

```bash
dit run workflows/port_visits/ais.py --runner dataflow --parallel
```

**Example: BigQuery SQL in a container.** Mode equivalence for fishing events.

```bash
# One-time GCP auth into a docker named volume (in the pipe-events checkout):
docker volume create gcp
docker compose run gcloud auth application-default login

dit run workflows/pipe_events/fishing.py --parallel --build-from-source
```

On a laptop the workflow mounts that `gcp` volume into the container. In Cloud Build there is no such volume, and the docker runner attaches the container to Cloud Build's metadata server instead. The workflow code is the same in both places ([`framework.md`](framework.md) § 9, Credentials).

**In Cloud Build:**

```bash
make dit-cloud PIPELINE=<checkout dir> WORKFLOW=workflows/<pkg>/<file>.py ARGS="..."
```

returns a build id in seconds. What it does, its variables, and what to check after submitting: [`framework.md`](framework.md) § 9.

---

## The choices in a run

| Choice | Options |
|---|---|
| Where | Local (`dit run ...`) or Cloud Build (`make dit-cloud ...`) |
| Which pipeline code | A clean `HEAD`; an automatic snapshot of a dirty tree; a named committed ref (`make install-<pipeline>-ref REF=...` or `make dit-cloud REF=...`) |
| Which image | The workflow's default; an image built automatically from the commit; an image you name |
| One version or several | One ref, or several refs compared (a cross-version workflow) |
| How it is started | By hand. Pull-request and scheduled triggers are planned, not built. |

**Every run executes a committed git ref.** If the pipeline checkout is dirty, a Dataflow run snapshots it first and runs the snapshot. There is no flag to run a dirty tree as it is. The reasons are in [`no-dirty-tree-pivot.md`](no-dirty-tree-pivot.md).

---

## Scenarios

| Situation | Scenario |
|---|---|
| Iterating on uncommitted code | [A](#a-iterating-on-uncommitted-code) |
| Testing an existing ref | [B](#b-running-a-committed-ref) |
| Comparing two refs on the same input | [C](#c-comparing-two-refs) |
| Reproducing or auditing a past run | [D](#d-reproducing-a-past-run) |
| Stopping a run | [E](#e-cancelling-a-run) |
| Snapshotting by hand | [F](#f-snapshotting-by-hand) |
| Validating a pull request automatically | [Planned](#planned-pull-request-validation) |

### A. Iterating on uncommitted code

You are mid-development and want feedback before opening a pull request.

```bash
cd $PROJECTS/pipe-gaps && $EDITOR src/...        # edits, not committed
cd $PROJECTS/data_integration_tests
make dit-cloud PIPELINE=pipe-gaps WORKFLOW=workflows/pipe_gaps/mode_equivalence.py
```

`make dit-cloud` sees the dirty tree, snapshots it to `refs/dit-snapshots/pipe-gaps/<sha>`, pushes the ref, and runs the snapshot. Your working tree is not touched.

What to expect:

- **Cache.** The first run of a new tree state is a miss. A repeat run of an unchanged tree is a hit: the snapshot's SHA depends only on the tree. Each edit gives a new snapshot and a full run.
- **What is captured.** Changes and deletions of tracked files only. A new file must be committed first; staging it is not enough.
- **Provenance.** The cache row is marked `unreviewed_code = TRUE`.
- **Workers run your code.** For a workflow with a plain default image, an image is built from the snapshot automatically. A workflow whose default is an overlay image refuses to start instead and names the build command; build the overlay image and pass it ([`framework.md`](framework.md) § 7).
- **Permission.** You need `git push` on the pipeline repo. Without it the push fails; commit and push through a normal branch and use scenario B.

### B. Running a committed ref

Test `main`, a colleague's branch or an old commit, without changing what is installed locally.

```bash
make dit-cloud PIPELINE=pipe-gaps REF=main WORKFLOW=workflows/pipe_gaps/mode_equivalence.py
make dit-cloud PIPELINE=pipe-gaps REF=<branch-or-sha> WORKFLOW=...

# Or install locally and run from your venv:
make install-pipe-gaps-ref REF=<branch-or-sha>
dit run workflows/pipe_gaps/mode_equivalence.py --runner dataflow ...
```

Use it for a regression baseline on `main`, for reviewing a branch, or for rerunning a commit a `dit_runs` row names.

### C. Comparing two refs

You have a fix and want to see exactly what it changes.

```bash
dit run workflows/port_visits/cross_version_ais.py \
    --experiment-id pipeline-1465 \
    --start 2020-01-01 --end 2020-12-31 \
    --binding before=v5.0.1 \
    --binding after=<branch-or-sha> \
    --binding-image-tag after=gcr.io/world-fishing-827/dit/pipe-anchorages:<label>-<commit7>-temp-dataset-<patch> \
    --binding-worker-image after=gcr.io/world-fishing-827/dit/pipe-anchorages:<label>-<commit7>-temp-dataset-<patch> \
    --runner dataflow --parallel
```

- `before` runs the default image. `after` is not merged, so it needs its own image, as submitter and as worker. Build it with `docker/overlays/pipe-anchorages-temp-dataset/build.sh <repo> <ref> <label> --push`. Without the two flags the `after` binding refuses to start.
- Every binding reads the same copies of the sources, taken at run start. Outputs are compared pairwise.
- The run logs its pin stamp. `--reuse-pin <stamp>` reruns on the same copies while they exist (seven days by default).
- `--dry-run` reports what the copies would cost and creates nothing.
- A difference does not fail the run. The exit code is non-zero only if a binding failed.

How cross-version runs work: [`framework.md`](framework.md) § 4. Flags differ per workflow; see each wrapper's `--help`.

### D. Reproducing a past run

A cache row names the commit a run executed.

```bash
bq query --use_legacy_sql=false --project_id=world-fishing-827 \
    'SELECT pipeline_commit, params_json FROM `world-fishing-827.tech_great_expectations.dit_runs`
     WHERE run_id = "<run id>"'
make dit-cloud PIPELINE=pipe-gaps REF=<sha> WORKFLOW=workflows/pipe_gaps/mode_equivalence.py ARGS="..."
```

Every run executes a pushed ref, so this works for anyone with read access to the pipeline repo. For a snapshot whose ref was deleted, the row's `pipeline_commit_parent` still names the commit the tree was based on.

### E. Cancelling a run

```bash
make dit-cancel RUN_ID=<run id>          # or: dit cache-cancel <run id> [--region <region>]
```

The run id is the 12 hex characters the workflow logs as `run_id=`. The command cancels the run's Dataflow jobs, deletes the tables recorded for it, and marks its rows `cancelled`. It covers all modes of the run and can be repeated safely. Details and limits: [`framework.md`](framework.md) § 6, Cleanup.

Cancelling the Cloud Build does the same for the jobs: `dit run` traps the signal and cleans up.

### F. Snapshotting by hand

`make snapshot-<pipeline>` does the snapshot step alone. Use it to look at the snapshot ref before a run, or to make several runs against one uncommitted state.

```bash
make snapshot-pipe-gaps
make dit-cloud PIPELINE=pipe-gaps REF=refs/dit-snapshots/pipe-gaps/<sha> ...
```

### Planned: pull-request validation

The intended main path is automatic: a pull request in a pipeline repo starts DIT, which compares the pull request's output with `main`'s cached output and posts the verdict as a check on the pull request.

This is **not built**. Its parts (per-pipeline triggers, a report module, check runs) are in [`pr-triggers-2026-06.md`](pr-triggers-2026-06.md) and in the README's Roadmap. Until then, scenarios A to C are done by hand.

---

## Snapshots and credential safety

A snapshot is pushed to the pipeline repo's origin automatically. That repository may be public. Treat every snapshot as publicly visible.

Three safeguards, in order:

1. **Tracked files only.** Only changes and deletions of files already in `HEAD` enter the snapshot. A stray `.env`, a downloaded key file or a result file that was never committed stays out.
2. **A banner** lists the changed paths before the push. If a path surprises you, press Ctrl-C.
3. **A secret scan.** Before the push, the snapshot tree is scanned with gitleaks. A finding aborts the snapshot. If gitleaks is not installed, the snapshot refuses to proceed. `DIT_SKIP_SECRET_SCAN=1` bypasses the scan with a loud warning; use it for false positives only.

If a secret was pushed anyway:

1. **Rotate the credential.** This is the step that matters. Anything pushed to a public-shaped repository must be treated as compromised.
2. `make clean-snapshot PIPELINE=<name> REF=<sha>` deletes the ref locally and on origin. Necessary, not sufficient.
3. If a tracked file holds the secret: `git rm --cached <file>`, add it to `.gitignore`, and commit the removal. The next snapshot then leaves it out.

How snapshots are built: [`framework.md`](framework.md) § 7.

---

## Cleaning up

| What | How | When |
|---|---|---|
| A run's jobs and tables | `make dit-cancel RUN_ID=<id>` | A stuck or unwanted run |
| Output tables | No shared rule. Some workflows set no expiration on their outputs (README § Roadmap, item 7e, names one). `make dit-cancel` drops the tables a run recorded. | |
| Pinned copies and views | They expire after seven days by default | |
| Snapshot refs | Never, by design. They are a few bytes in a hidden ref namespace. `make clean-snapshot` exists for secret remediation only. | |
| Images under `gcr.io/world-fishing-827/dit/` | By hand; there is no cleanup policy ([`conventions.md`](conventions.md) § Cleanup) | |

Do not delete tables in `tech_great_expectations` by hand while runs are in flight: other runs may be reading pinned copies there.
