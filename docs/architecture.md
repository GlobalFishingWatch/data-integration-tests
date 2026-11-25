# DIT architecture

Diagrams of how the pieces fit together. Each section stands alone. The text that goes with them is in [`framework.md`](framework.md).

1. [System context](#1-system-context)
2. [The library](#2-the-library)
3. [Ways to start a run](#3-ways-to-start-a-run)
4. [Workflow flows](#4-workflow-flows)
5. [Run context: commit and image](#5-run-context-commit-and-image)
6. [Cloud Build runtime](#6-cloud-build-runtime)
7. [The drift check](#7-the-drift-check)
8. [Image namespace](#8-image-namespace)

The diagrams are Mermaid and render on GitHub. Locally, most editors render them in a Markdown preview with a Mermaid extension.

---

## 1. System context

Three kinds of repo, by owner. Pipeline repos hold the pipelines. `composer-dags-production` holds what production runs. This repo and `table_identical_checks` hold the tests.

```mermaid
flowchart LR
    subgraph PR["Processing repos (one per pipeline)"]
        direction TB
        pgaps[pipe-gaps]
        anchorages["pipe-anchorages<br/><i>port visits</i>"]
        pevents["pipe-events<br/><i>fishing events</i>"]
        psegment[pipe-segment]
        encounters[encounters]
    end

    subgraph CD["Orchestration repo"]
        composer["composer-dags-production<br/><i>production DAGs</i>"]
    end

    subgraph TR["Testing repos"]
        direction TB
        dit["<b>data-integration-tests</b><br/><i>dit library + workflows</i>"]
        tic["table_identical_checks<br/><i>table-check</i>"]
    end

    subgraph GCP["GCP runtime (world-fishing-827)"]
        direction TB
        cb[Cloud Build]
        df[Dataflow]
        bq[("BigQuery<br/>tech_great_expectations")]
        ar["Artifact Registry<br/>gcr.io/world-fishing-827/dit/"]
    end

    composer -->|schedules and configures| PR

    dit -.->|runs at any git ref,<br/>never changes| PR
    dit -.->|reads rendered DAG snapshots<br/>with git show, never imports| composer
    dit -->|subprocess| tic

    cb -->|runs dit run| dit
    dit -->|submits jobs| df
    dit -->|pins sources, writes outputs,<br/>run cache| bq
    dit -->|pushes test images| ar
    df -->|pulls image| ar
    df -->|reads and writes| bq
    tic -->|comparison query| bq

    style dit fill:#fff5cc,stroke:#c89,color:#000
    style tic fill:#fff5cc,stroke:#c89,color:#000
```

| Owns | Repo |
|---|---|
| Pipeline code, its command line, its published images | Processing repos |
| Production DAGs and parameters | `composer-dags-production` |
| The `dit` library, the workflows, the cloud runtime | `data-integration-tests` (this repo) |
| The table comparison | `table_identical_checks` |

Dotted arrows are read-only. DIT reads production data and production DAG configuration. It writes only to its own namespaces ([`conventions.md`](conventions.md) § Prod-infra boundary).

---

## 2. The library

A workflow file holds what is specific to a pipeline: the steps' command lines, the output tables, the compare keys. Everything else comes from `src/dit/`.

```mermaid
flowchart TB
    cli["dit.cli<br/><i>dit run, cache-cancel, composer-dag</i>"]
    wf["workflows/&lt;pkg&gt;/&lt;name&gt;.py<br/><i>main(argv) returns an exit code</i>"]
    cli --> wf

    subgraph HARNESS["Run harness"]
        direction TB
        workflow["dit.workflow<br/><i>arguments, worker cap, sizing,<br/>run context, run_with_cache</i>"]
        snapshot["dit.snapshot, dit.git_info<br/><i>commit, snapshot, reviewed?</i>"]
        image["dit.worker_image<br/><i>auto-built image</i>"]
        cache["dit.cache<br/><i>dit_runs: key, read, write, cancel</i>"]
        workflow --> snapshot
        workflow --> image
        workflow --> cache
    end

    subgraph EXEC["Execution"]
        direction TB
        docker["dit.runners.docker"]
        dataflow["dit.runners.dataflow"]
        names["dit.job_names, dit.dataflow_paths"]
    end

    subgraph DATA["Data"]
        direction TB
        bq["dit.bq<br/><i>pins: copies and views</i>"]
        production["dit.production<br/><i>find a production job</i>"]
        compare["dit.compare<br/><i>shim over table-check</i>"]
    end

    subgraph SYNC["In step with production"]
        direction TB
        composer["dit.composer<br/><i>read DAG snapshots</i>"]
        drift["dit.drift<br/><i>version, command, flags</i>"]
        adjust["dit.adjust<br/><i>per-side adjustments</i>"]
        drift --> composer
        adjust --> composer
    end

    subgraph ORCH["Orchestration"]
        direction TB
        orchestration["dit.orchestration<br/><i>declarations, judge</i>"]
        slices["dit.slices<br/><i>date slices</i>"]
    end

    wf --> HARNESS
    wf --> EXEC
    wf --> DATA
    wf --> SYNC
    wf --> ORCH
```

Module by module: [`framework.md`](framework.md) § 14.

---

## 3. Ways to start a run

| How | Command | Use | State |
|---|---|---|---|
| By hand, locally | `dit run workflows/<pkg>/<name>.py` | Development, small cohorts, dry runs | Works |
| By hand, in Cloud Build | `make dit-cloud PIPELINE=... WORKFLOW=... ARGS="..."` | Long runs; nothing depends on a laptop staying awake | Works |
| From a pull request | A Cloud Build trigger in the pipeline repo that uses `cloudbuild-dit.yaml` | A check on the pull request | Planned |
| On a schedule | Cloud Scheduler to Cloud Build, same yaml | Nightly runs against `main` | Planned |

All four use the same `cloudbuild-dit.yaml` or the same `dit run`. Only what starts them differs.

---

## 4. Workflow flows

### 4a. Mode equivalence

One pipeline version, several ways of loading the same window. A difference fails the run.

```mermaid
sequenceDiagram
    actor U as User
    participant W as workflow
    participant C as dit_runs (run cache)
    participant DF as Dataflow or container
    participant BQ as BigQuery
    participant TIC as table-check

    U->>W: dit run ... --modes 1_bf,2_bfd,3_bftruncate
    W->>W: check arguments, resolve run context
    par one branch per mode
        W->>C: look up cache key
        alt hit, tables still exist
            C-->>W: cached table
        else miss
            W->>DF: run the mode's jobs
            DF-->>BQ: write the mode's tables
            W->>C: record the run
        end
    end
    loop each pair of modes
        W->>TIC: table-check summary a b --keys ...
        TIC->>BQ: comparison query
        TIC-->>W: differing rows
    end
    W-->>U: IDENTICAL or DIFFERENT per pair, exit 0 only if all identical
```

Not every workflow uses the run cache ([`framework.md`](framework.md) § 6).

### 4b. Cross-version

Several git refs of one pipeline on the same pinned input. A difference is the result, not a failure.

```mermaid
sequenceDiagram
    actor U as User
    participant XV as cross-version wrapper
    participant Git as pipeline checkout
    participant BQ as BigQuery
    participant R as single-version workflow
    participant TIC as table-check

    U->>XV: dit run ... --binding before=REF_A --binding after=REF_B
    XV->>Git: check that every ref exists
    opt bindings differ in their interface
        XV->>Git: git show of each ref's CLI source (preflight)
        XV->>XV: base interface + declared + ad hoc adjustments
    end
    XV->>BQ: pin sources once (copy at run start, dry-run against the cap)
    Note over BQ: tech_great_expectations.dit_exp_[experiment]_[role]_[table]_[window]_[pin stamp]

    par one branch per binding
        XV->>Git: git worktree add at the ref
        XV->>R: run on the pinned copies, with this binding's image
        R-->>BQ: write this binding's tables
    end

    loop each pair of bindings (skip pairs with a failed binding)
        XV->>TIC: table-check summary
        TIC-->>XV: differing rows
    end
    XV-->>U: verdict per pair, exit non-zero only if a binding failed
```

Each binding needs an image that holds its own code. Otherwise all bindings run the same worker code and the comparison passes without testing anything ([`framework.md`](framework.md) § 7).

### 4c. Production baseline

One candidate against the table production wrote. Production is not re-run.

```mermaid
sequenceDiagram
    actor U as User
    participant XV as cross-version wrapper
    participant DFP as production's Dataflow job (metadata)
    participant BQ as BigQuery
    participant R as single-version workflow
    participant TIC as table-check

    U->>XV: dit run ... --baseline-production TARGET --production-date D
    XV->>DFP: find the job for D
    DFP-->>XV: T0 = job created, T1 = job done
    XV->>BQ: refuse if an input changed between T0 and T1
    XV->>BQ: refuse if a pin would leave time travel during the run
    XV->>BQ: views of the inputs FOR SYSTEM_TIME AS OF T0
    XV->>BQ: view of production's output FOR SYSTEM_TIME AS OF T1
    XV->>R: run the candidate on the input views, in production's run shape
    R-->>BQ: write the candidate's table
    XV->>TIC: candidate table against the output view
    TIC-->>XV: differing rows
    XV-->>U: verdict
```

### 4d. Parallel slices

```mermaid
flowchart LR
    window["window<br/>[start, end]"] --> plan["dit.slices.plan<br/>day, week, month, Nd, list"]
    plan --> check["check_partition<br/><i>covers the window exactly once</i>"]
    check --> jobs["one job per slice,<br/>all submitted before any is awaited"]
    jobs --> table[("one table")]
    window --> one["one job over the window (1_bf)"]
    one --> ref[("reference table")]
    table --> cmp["compare"]
    ref --> cmp
    cmp --> judge["dit.orchestration.judge<br/><i>outcome against the step's declaration</i>"]
    decl["workflows/&lt;pkg&gt;/orchestration.py"] --> judge
    judge --> verdict["verdict and exit code"]
```

---

## 5. Run context: commit and image

What `dit.workflow.resolve_run_context` decides before any job starts.

```mermaid
flowchart TB
    start([run starts]) --> env{"DIT_PIPELINE_COMMIT set?<br/><i>cloud path</i>"}
    env -->|yes| recorded["record that commit"]
    env -->|no| dirty{"checkout dirty?<br/><i>tracked files only</i>"}
    dirty -->|no| head["record HEAD"]
    dirty -->|yes, docker runner| headU["record HEAD, unreviewed"]
    dirty -->|yes, Dataflow runner| snap["scripts/snapshot.sh:<br/>orphan commit of tracked files,<br/>secret scan, push to<br/>refs/dit-snapshots/&lt;pipeline&gt;/&lt;sha&gt;"]
    snap --> snapU["record snapshot commit, unreviewed"]

    recorded --> reviewed
    head --> reviewed{"ancestor of origin/main?"}
    reviewed -->|yes| rev["reviewed"]
    reviewed -->|no, or cannot tell| unrev["unreviewed"]
    headU --> unrev
    snapU --> unrev

    rev --> img
    unrev --> img{"image policy"}
    img -->|workflow passes check_images| guard["workflow decides:<br/>refuse unreviewed code on a default image"]
    img -->|build from source| compose["docker compose builds the working tree"]
    img -->|explicit image given| explicit["use it"]
    img -->|default image, reviewed| default["use the default"]
    img -->|default image, unreviewed| build["kaniko build:<br/>dit/&lt;pipeline&gt;:dit-&lt;commit&gt;<br/><i>reused if the tag exists</i>"]
```

Two simplifications in this diagram:

- In the cloud path the laptop has already answered "reviewed?" and passes it in as `DIT_UNREVIEWED`. The ancestry check runs in the build only when that variable is missing.
- A run with an explicit `--suffix` (every binding of a cross-version run) never snapshots. It records the checkout's state as it is, and counts as unreviewed when the tree is dirty or the commit is not merged.

Text: [`framework.md`](framework.md) § 7.

---

## 6. Cloud Build runtime

How `make dit-cloud` runs a workflow. The same `cloudbuild-dit.yaml` would serve pull-request triggers.

```mermaid
flowchart LR
    subgraph LAPTOP["Laptop: make dit-cloud"]
        direction TB
        l1["resolve the pipeline commit<br/><i>REF, snapshot of a dirty tree, or HEAD</i>"]
        l2["resolve reviewed or not<br/><i>fetch origin/main, ancestry check</i>"]
        l3["gcloud builds submit --async<br/><i>source: the pipeline checkout</i>"]
        l1 --> l2 --> l3
    end

    subgraph BUILD["Cloud Build: one step in ditbox, as automated-testing@"]
        direction TB
        b1["git clone dit at DIT_REF"]
        b2["uv pip install dit (no deps)"]
        b3["uv pip install the pipeline<br/><i>at REF, or the uploaded tree; Beam pinned</i>"]
        b4["dit run WORKFLOW ARGS<br/><i>DIT_PIPELINE_COMMIT, DIT_UNREVIEWED, DIT_CLOUD_MODE=1</i>"]
        b1 --> b2 --> b3 --> b4
    end

    l3 --> b1

    b4 -->|"docker run --network=cloudbuild"| inner["pipeline container<br/><i>identity from Cloud Build's metadata server</i>"]
    b4 -->|in-process submit| df[Dataflow]
    inner -->|submits| df
    inner -->|SQL steps| bq[(BigQuery)]
    df -->|pulls image| ar["gcr.io/world-fishing-827/dit/..."]
    df --> bq
    b4 -->|table-check| bq
    b4 -.->|"SIGTERM: cancel_run"| df
```

- **`cloudbuild-dit.yaml` is the one definition of how DIT runs in the cloud.** It lives here. Triggers in pipeline repos would reference it.
- **The laptop does what the builder cannot:** push a snapshot and fetch the pipeline's origin.
- **ditbox holds tools only.** DIT and the pipeline are installed per run.

Text: [`framework.md`](framework.md) § 9. Credentials: [`conventions.md`](conventions.md) § "Auth in the cloud path (ditbox)".

---

## 7. The drift check

```mermaid
flowchart LR
    subgraph COMPOSER["composer-dags-production checkout (fetched by hand)"]
        snap["tests/render_dags/snapshots/&lt;dag&gt;.py<br/><i>rendered_dag = {...}</i>"]
    end

    subgraph DIT["this repo"]
        direction TB
        spec["workflows/&lt;pkg&gt;/composer_drift.py<br/><i>WorkflowSpec, StepSpec</i>"]
        builders["the workflow's own<br/>command-line builders"]
        spec --> builders
    end

    snap -->|"git show, ast.literal_eval<br/>(dit.composer)"| prod["production's task:<br/>image, command, flags"]
    builders -->|"same run dates"| mine["DIT's tokens:<br/>image, command, flags"]

    prod --> cmp["dit.drift.check_step"]
    mine --> cmp
    cmp --> v["version"]
    cmp --> c["command"]
    cmp --> f["flags<br/><i>minus DIT-owned and accepted</i>"]
    v --> out["summary, exit 0 / 1 / 2"]
    c --> out
    f --> out
```

Text: [`composer-drift.md`](composer-drift.md).

---

## 8. Image namespace

Where DIT's images live, next to the namespaces of the canonical pipeline images in the same project.

```
gcr.io/world-fishing-827/
│
├── anchorages_pipeline/, encounters_pipeline/, ...   canonical pipeline namespaces: read-only for DIT
│
└── dit/                                              DIT's namespace: read and write
    ├── ditbox:latest, :<short-sha>
    │       tooling image for Cloud Build runs
    │
    ├── <pipeline>:dit-<pipeline_commit>
    │       built automatically for unreviewed code
    │
    ├── <pipeline>:<label>-<commit7>-<what>-<patch-sha>
    │       overlay image built from a commit plus a test-only patch,
    │       e.g. pipe-anchorages:v5.0.1-767770f-temp-dataset-905dc761
    │
    ├── <pipeline>:<base-version>-<what>-<patch-sha>
    │       overlay image built on a published image
    │
    ├── <pipeline>:<experiment-id>-<binding-name>
    │       image built by hand for one binding of a cross-version run
    │
    └── pushtest:<anything>
            smoke test of a push; delete after use
```

Canonical upstream images that some workflows pull live in another project's registry. DIT reads them and never writes there.

Naming rules, the overlay rules and the build commands: [`conventions.md`](conventions.md) § Image namespace.
