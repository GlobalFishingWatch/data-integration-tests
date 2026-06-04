# Install targets for the dit framework + per-pipeline workflow deps.
#
# Three modes of pipeline install (per pipeline):
#   - editable       (make install-<pipeline>)               -- fast inner loop, working tree picks up changes immediately
#   - specific ref   (make install-<pipeline>-ref REF=<ref>) -- reproducible, non-editable, from any committed ref
#   - snapshot       (make snapshot-<pipeline>)              -- auto-snapshot current working tree + install (reproducible inner loop)
#
# PROJECTS defaults to the parent directory of this repo (sibling checkouts).
# Override with PROJECTS=/path or copy .envrc.example -> .envrc.

PROJECTS ?= $(realpath ..)
PIP ?= pip

PIPE_GAPS_DIR    = $(PROJECTS)/pipe-gaps
PORT_VISITS_DIR  = $(PROJECTS)/anchorages_pipeline
PIPE_EVENTS_DIR  = $(PROJECTS)/pipe-events
PIPE_SEGMENT_DIR = $(PROJECTS)/pipe-segment

# Default --no-deps on -ref / snapshot installs (assumes pipeline's transitive
# deps haven't changed). Set FULLDEPS=1 to drop the flag and let pip reinstall
# the full dep tree -- needed only when the target ref bumped or added a dep.
DEPS_FLAG = $(if $(FULLDEPS),,--no-deps)

# === Per-pipeline apache-beam pins (load-bearing for in-process runners) ===
#
# Each value MUST match the apache-beam version baked into that pipeline's
# canonical published worker image -- the value Beam workers actually run.
# Pipe-gaps' workflows submit IN-PROCESS via dit.runners.dataflow (the
# laptop venv constructs the Beam pipeline graph), so submitter beam must
# equal worker beam or Dataflow rejects the job with "Pipeline construction
# environment and pipeline runtime environment are not compatible". The
# other workflows use dit.runners.docker (submitter is the worker image
# itself -- no laptop-venv beam in the picture), so the pin is "academic"
# for them but kept here for symmetry.
#
# These constants serve BOTH paths:
#   - laptop installs (install-<pipeline> / install-<pipeline>-ref /
#     snapshot-<pipeline>): consumed via the target-specific BEAM_VERSION
#     overrides below + the WITH_BEAM_CONSTRAINT macro that writes a pip
#     --constraint file.
#   - cloud builds (make dit-cloud PIPELINE=<name>): consumed via the
#     `ifeq ($(PIPELINE),...)` block lower down, which sets BEAM_VERSION
#     and threads it into cloudbuild-dit.yaml as the _BEAM_VERSION
#     substitution -- mirror-image of the laptop path.
#
# Symmetry rule: when bumping a pipeline's worker image to a version with a
# different beam pin, bump the constant here too. cloudbuild-dit.yaml's
# substitution declaration has no default value -- it's empty there and the
# Makefile is the single source of truth. See CLAUDE.md "Beam-version pin
# symmetry across laptop and cloud installs" under Working agreements.
# pipe-gaps:v0.10.0 -> apache-beam==2.71.0
PIPE_GAPS_BEAM_VERSION            := 2.71.0
# pipe-anchorages:v4.6.4 -> apache-beam==2.69.0
ANCHORAGES_PIPELINE_BEAM_VERSION  := 2.69.0
# pipe-segment:v5.0.3 -> apache-beam==2.56.0
PIPE_SEGMENT_BEAM_VERSION         := 2.56.0
# pipe-events deliberately omitted: it's a BQ-SQL-via-container pipeline,
# the laptop venv never constructs a Beam graph for it, and no published
# canonical beam version is recorded for the pipe-events image today.
#
# NOTE: don't put `# comment` on the same line as `:= 2.71.0` -- Make keeps
# the trailing spaces before `#` in the value, which leaks into the
# constraint file as `apache-beam==2.71.0  ` (pip mostly tolerates but
# it's a footgun; constants stay clean).

# Target-specific BEAM_VERSION for laptop install targets. Make's
# target-specific-variable mechanism auto-applies the right pin to each
# `make install-<pipeline>` invocation without forcing the caller to pass
# PIPELINE= on the command line (the cloud-path ifeq block below DOES
# require PIPELINE= since it's used by dit-cloud).
install-pipe-gaps install-pipe-gaps-ref snapshot-pipe-gaps:        BEAM_VERSION := $(PIPE_GAPS_BEAM_VERSION)
install-port-visits install-port-visits-ref snapshot-port-visits:  BEAM_VERSION := $(ANCHORAGES_PIPELINE_BEAM_VERSION)
install-pipe-segment install-pipe-segment-ref snapshot-pipe-segment: BEAM_VERSION := $(PIPE_SEGMENT_BEAM_VERSION)
# install-all installs all four pipelines into one venv: the only one
# whose laptop submitter actually IS the laptop venv is pipe-gaps (others
# use docker runner), so pin to pipe-gaps' beam version. This also
# satisfies the other pipelines' ~= ranges (pipe-anchorages ~=2.69 and
# pipe-segment ~=2.56 both accept 2.71.0).
install-all: BEAM_VERSION := $(PIPE_GAPS_BEAM_VERSION)

# Helper: write a beam==$(BEAM_VERSION) constraint file to a temp path,
# run the command in $1 (which must reference $$CONSTRAINT for the file
# path), and clean up on exit. Single-shell recipe with `\` continuations
# so $$CONSTRAINT survives between lines.
#
# Uses POSIX `mktemp` rather than bash `<(echo ...)` process substitution
# because Make recipes run under /bin/sh by default and dash/ash don't
# implement <(...). The trap fires on shell exit, including the implicit
# exit at end-of-recipe; the temp file is removed even if the install
# fails. `set -e` ensures any failed step (mktemp, echo, the inner pip
# command) aborts the recipe rather than silently continuing.
define WITH_BEAM_CONSTRAINT
	@set -e; \
	test -n "$(BEAM_VERSION)" || { echo "BEAM_VERSION not set for target $@ (no per-target override)" >&2; exit 1; }; \
	CONSTRAINT=$$(mktemp -t dit-beam-constraint.XXXXXXXX); \
	trap "rm -f $$CONSTRAINT" EXIT; \
	echo "apache-beam==$(BEAM_VERSION)" > $$CONSTRAINT; \
	$(1)
endef

.PHONY: install \
    install-pipe-gaps install-port-visits install-pipe-events install-pipe-segment install-all \
    install-pipe-gaps-ref install-port-visits-ref install-pipe-events-ref install-pipe-segment-ref \
    snapshot-pipe-gaps snapshot-port-visits snapshot-pipe-events snapshot-pipe-segment \
    clean-snapshot clean-snapshots \
    publish-ditbox dit-cloud dit-cancel

# === Framework only ===

install:
	$(PIP) install -e ".[dev]"

# === Editable installs (active dev) ===
#
# Beam-pinned for pipe-gaps / port-visits / pipe-segment via target-specific
# BEAM_VERSION overrides above + WITH_BEAM_CONSTRAINT. pipe-events skips
# the pin (no canonical beam version recorded for its image; it's a
# BQ-SQL-via-container pipeline whose laptop venv never constructs Beam).

install-pipe-gaps:
	$(call WITH_BEAM_CONSTRAINT,$(PIP) install --constraint $$CONSTRAINT -e ".[dev]" -e "$(PIPE_GAPS_DIR)")

install-port-visits:
	$(call WITH_BEAM_CONSTRAINT,$(PIP) install --constraint $$CONSTRAINT -e ".[dev]" -e "$(PORT_VISITS_DIR)")

install-pipe-events:
	$(PIP) install -e ".[dev]" -e "$(PIPE_EVENTS_DIR)"

install-pipe-segment:
	$(call WITH_BEAM_CONSTRAINT,$(PIP) install --constraint $$CONSTRAINT -e ".[dev]" -e "$(PIPE_SEGMENT_DIR)")

install-all:
	$(call WITH_BEAM_CONSTRAINT,$(PIP) install --constraint $$CONSTRAINT \
	    -e ".[dev]" \
	    -e "$(PIPE_GAPS_DIR)" \
	    -e "$(PORT_VISITS_DIR)" \
	    -e "$(PIPE_EVENTS_DIR)" \
	    -e "$(PIPE_SEGMENT_DIR)")

# === Specific-ref installs (reproducible, non-editable) ===
#
# Constraint is a no-op when DEPS_FLAG = --no-deps (pip resolves nothing
# transitively), but applies cleanly when FULLDEPS=1 reinstalls the full
# dep tree -- which is exactly when the drift risk would otherwise return.

install-pipe-gaps-ref:
	@test -n "$(REF)" || { echo "REF is required: make $@ REF=<sha-or-branch>"; exit 1; }
	$(call WITH_BEAM_CONSTRAINT,$(PIP) install --force-reinstall --constraint $$CONSTRAINT $(DEPS_FLAG) "git+file://$(PIPE_GAPS_DIR)@$(REF)")

install-port-visits-ref:
	@test -n "$(REF)" || { echo "REF is required: make $@ REF=<sha-or-branch>"; exit 1; }
	$(call WITH_BEAM_CONSTRAINT,$(PIP) install --force-reinstall --constraint $$CONSTRAINT $(DEPS_FLAG) "git+file://$(PORT_VISITS_DIR)@$(REF)")

install-pipe-events-ref:
	@test -n "$(REF)" || { echo "REF is required: make $@ REF=<sha-or-branch>"; exit 1; }
	$(PIP) install --force-reinstall $(DEPS_FLAG) "git+file://$(PIPE_EVENTS_DIR)@$(REF)"

install-pipe-segment-ref:
	@test -n "$(REF)" || { echo "REF is required: make $@ REF=<sha-or-branch>"; exit 1; }
	$(call WITH_BEAM_CONSTRAINT,$(PIP) install --force-reinstall --constraint $$CONSTRAINT $(DEPS_FLAG) "git+file://$(PIPE_SEGMENT_DIR)@$(REF)")

# === Snapshot + install ===
#
# Build a deterministic orphan snapshot of the pipeline checkout's tracked
# state under refs/dit-snapshots/<pipeline>/<commit-short-sha>, push it to
# origin (skipped if the ref already exists), then pip install from that ref.
# Identical tree state -> identical SHA -> cache hits on repeat runs.
#
# Same constraint-passthrough as the -ref targets above: snapshot-install.sh
# forwards extra args verbatim to pip, so `--constraint $$CONSTRAINT` flows
# through. No script-side change required.

snapshot-pipe-gaps:
	$(call WITH_BEAM_CONSTRAINT,scripts/snapshot-install.sh pipe-gaps "$(PIPE_GAPS_DIR)" --constraint $$CONSTRAINT $(DEPS_FLAG))

snapshot-port-visits:
	$(call WITH_BEAM_CONSTRAINT,scripts/snapshot-install.sh anchorages_pipeline "$(PORT_VISITS_DIR)" --constraint $$CONSTRAINT $(DEPS_FLAG))

snapshot-pipe-events:
	scripts/snapshot-install.sh pipe-events "$(PIPE_EVENTS_DIR)" $(DEPS_FLAG)

snapshot-pipe-segment:
	$(call WITH_BEAM_CONSTRAINT,scripts/snapshot-install.sh pipe-segment "$(PIPE_SEGMENT_DIR)" --constraint $$CONSTRAINT $(DEPS_FLAG))

# === Cleanup ===
#
# Snapshots live forever by design (bytes-scale, hidden namespace). The
# surgical target below exists only for secret-leak remediation -- e.g. a
# .env file accidentally ended up in a snapshot's tree and got pushed.
#
#   make clean-snapshot PIPELINE=pipe-gaps REF=<sha-or-full-ref>

clean-snapshot:
	@test -n "$(PIPELINE)" || { echo "PIPELINE required: PIPELINE=pipe-gaps | anchorages_pipeline | pipe-events | pipe-segment" >&2; exit 1; }
	@test -n "$(REF)" || { echo "REF required: REF=<sha> or REF=refs/dit-snapshots/<pipeline>/<sha>" >&2; exit 1; }
	@test -d "$(PROJECTS)/$(PIPELINE)" || { echo "pipeline dir not found: $(PROJECTS)/$(PIPELINE)" >&2; exit 1; }
	scripts/clean-snapshot.sh "$(PROJECTS)/$(PIPELINE)" "$(REF)"

# Redirect for muscle-memory: the broad sweep was removed in M-pivot-1.
clean-snapshots:
	@echo "make clean-snapshots was removed in the no-dirty-tree pivot." >&2
	@echo "Snapshots live forever by design (bytes-scale, hidden namespace)." >&2
	@echo "For secret-leak remediation: make clean-snapshot PIPELINE=<name> REF=<sha>" >&2
	@exit 1

# === Cloud Build: ditbox image ===
#
# Builds and pushes the ditbox tooling image used by cloudbuild-dit.yaml runs.
# Tags both :latest and :<short-sha>. The build context is the repo root so the
# Dockerfile can COPY requirements.txt.

publish-ditbox:
	gcloud builds submit \
	    --config=docker/ditbox/cloudbuild.yaml \
	    --substitutions=_GIT_SHA=$$(git rev-parse --short HEAD) \
	    .

# === Cloud Build: ad-hoc dit run ===
#
# Submits a dit workflow run to Cloud Build using cloudbuild-dit.yaml. The
# pipeline checkout at $(PROJECTS)/$(PIPELINE) flows through as the build
# source; dit itself is cloned fresh from GitHub at DIT_REF (default main).
#
# Required:
#   WORKFLOW=workflows/<pipeline>/<name>.py
#   PIPELINE=pipe-gaps | anchorages_pipeline | pipe-events | pipe-segment
# Optional:
#   ARGS="..."        # appended verbatim to `dit run <workflow>`
#   REF=<sha-or-tag>  # pipeline ref to install non-editably; empty = editable from source upload
#   DIT_REF=<ref>     # dit ref to clone (default main)
#   BEAM_VERSION=<x.y.z>  # pin apache-beam to match the worker image's beam version
#                          # (pipe-gaps:v0.10.0 = 2.71.0; pipe-anchorages:v4.6.4 = 2.69.0;
#                          # pipe-segment:v5.0.3 = 2.56.0). Auto-defaulted below
#                          # per PIPELINE from the constants at the top of this
#                          # Makefile (same values the laptop install targets use).
#
# Example:
#   make dit-cloud \
#       PIPELINE=anchorages_pipeline \
#       WORKFLOW=workflows/port_visits/ais.py \
#       ARGS="--runner dataflow --parallel --build-from-source"

# Per-pipeline default for BEAM_VERSION on the cloud path. Override by
# passing BEAM_VERSION=x.y.z on the make command line. Empty -> no
# constraint (uv picks newest matching ~=; expect SDK-version mismatch
# errors unless the workflow runs locally via --runner docker).
#
# Values come from the constants at the top of this Makefile (also used
# by the laptop install targets via target-specific BEAM_VERSION), so a
# pipe-gaps worker-image bump that changes the beam pin is a one-place
# constant update -- both laptop and cloud paths track in lockstep.
ifeq ($(PIPELINE),pipe-gaps)
  BEAM_VERSION ?= $(PIPE_GAPS_BEAM_VERSION)
endif
ifeq ($(PIPELINE),anchorages_pipeline)
  BEAM_VERSION ?= $(ANCHORAGES_PIPELINE_BEAM_VERSION)
endif
ifeq ($(PIPELINE),pipe-segment)
  BEAM_VERSION ?= $(PIPE_SEGMENT_BEAM_VERSION)
endif

# Resolve the pipeline_commit on the laptop (where git-push creds live) and
# always thread it into the build as _PIPELINE_COMMIT, so the in-cloud workflow
# NEVER attempts a snapshot/push from the builder (it has no push creds):
#   - REF set      -> the resolved short sha of that committed ref.
#   - dirty (no REF) -> auto-snapshot via scripts/snapshot.sh
#                       (refs/dit-snapshots/<pipeline>/<sha>) and record the
#                       snapshot's short sha. REQUIRE_CLEAN=1 errors instead.
#   - clean (no REF) -> HEAD short sha.
# The build still uploads the (byte-identical) working tree as its source;
# _PIPELINE_COMMIT only changes what the workflow records as pipeline_commit.
#
# _UNREVIEWED (not-merged-into-origin/main) is ALSO resolved here, for the same
# reason: the builder can't fetch the pipeline's (SSH) origin to run the
# ancestor check itself, so it would treat every cloud run as unreviewed --
# wasting a worker-image build for a reviewed ref and breaking its cache reuse.
# `merge-base --is-ancestor` against a freshly-fetched origin/main gives the
# right answer on the laptop; a snapshot orphan / unmerged ref is correctly
# not-an-ancestor -> unreviewed. Defaults to true (build-when-unsure) if the
# check can't run.
#
# Fire-and-forget by default: `gcloud builds submit --async` uploads the source
# + creates the build, then returns immediately with a build id (the cold
# worker-image build + Dataflow run happen cloud-side, so your laptop is free in
# seconds). Pass WAIT=1 to stream logs and block on the result instead -- needed
# when you want the build's exit code (CI) or to watch/debug a run live.
ASYNC_FLAG = $(if $(WAIT),,--async)

dit-cloud:
	@test -n "$(WORKFLOW)" || { echo "WORKFLOW required: WORKFLOW=workflows/..." >&2; exit 1; }
	@test -n "$(PIPELINE)" || { echo "PIPELINE required: anchorages_pipeline | pipe-gaps | pipe-events | pipe-segment" >&2; exit 1; }
	@test -d "$(PROJECTS)/$(PIPELINE)" || { echo "pipeline dir not found: $(PROJECTS)/$(PIPELINE)" >&2; exit 1; }
	@PIPELINE_COMMIT=""; \
	if [ -n "$(REF)" ]; then \
	    PIPELINE_COMMIT=$$(git -C "$(PROJECTS)/$(PIPELINE)" rev-parse --short "$(REF)"); \
	elif [ -n "$$(git -C "$(PROJECTS)/$(PIPELINE)" status --porcelain --untracked-files=no)" ]; then \
	    if [ -n "$(REQUIRE_CLEAN)" ]; then \
	        echo "error: $(PIPELINE) checkout is dirty and REQUIRE_CLEAN=1 was set." >&2; \
	        echo "       commit + push, or drop REQUIRE_CLEAN to auto-snapshot." >&2; \
	        exit 1; \
	    fi; \
	    echo ">>> $(PIPELINE) checkout is dirty; auto-snapshotting..." >&2; \
	    SNAP_REF=$$(scripts/snapshot.sh "$(PIPELINE)" "$(PROJECTS)/$(PIPELINE)"); \
	    PIPELINE_COMMIT=$$(git -C "$(PROJECTS)/$(PIPELINE)" rev-parse --short "$$SNAP_REF"); \
	    echo ">>> snapshot pipeline_commit=$$PIPELINE_COMMIT" >&2; \
	else \
	    PIPELINE_COMMIT=$$(git -C "$(PROJECTS)/$(PIPELINE)" rev-parse --short HEAD); \
	fi; \
	UNREVIEWED=true; \
	git -C "$(PROJECTS)/$(PIPELINE)" fetch --quiet origin main 2>/dev/null || true; \
	if git -C "$(PROJECTS)/$(PIPELINE)" merge-base --is-ancestor "$$PIPELINE_COMMIT" origin/main 2>/dev/null; then \
	    UNREVIEWED=false; \
	fi; \
	echo ">>> pipeline_commit=$$PIPELINE_COMMIT unreviewed=$$UNREVIEWED" >&2; \
	gcloud builds submit $(ASYNC_FLAG) \
	    --config=cloudbuild-dit.yaml \
	    --ignore-file=$(CURDIR)/.gcloudignore \
	    --substitutions="^@@^_WORKFLOW=$(WORKFLOW)@@_PIPELINE=$(PIPELINE)@@_ARGS=$(ARGS)@@_REF=$(REF)@@_DIT_REF=$(or $(DIT_REF),main)@@_BEAM_VERSION=$(BEAM_VERSION)@@_PIPELINE_COMMIT=$$PIPELINE_COMMIT@@_UNREVIEWED=$$UNREVIEWED" \
	    "$(PROJECTS)/$(PIPELINE)"

# === Run cleanup: cancel a run's Dataflow jobs + drop its output tables ===
#
# Cancels all sibling modes of a run (they share the per-run 12-hex RUN_ID,
# stamped as the dit_run_id Dataflow label + recorded on every dit_runs row).
# Dataflow jobs are discovered by that label; output tables come off the rows
# (table-level deletes only -- never a dataset). Marks the rows cancelled.
# Idempotent.
#
# Required:
#   RUN_ID=<12-hex>   # the run_id= the workflow logs at startup
# Optional:
#   REGION=<region>   # Dataflow region to search; defaults to DIT_DATAFLOW_REGION then us-central1
#
# Example:
#   make dit-cancel RUN_ID=a1b2c3d4e5f6

dit-cancel:
	@test -n "$(RUN_ID)" || { echo "RUN_ID required: make dit-cancel RUN_ID=<12-hex run id>" >&2; exit 1; }
	dit cache-cancel "$(RUN_ID)" $(if $(REGION),--region "$(REGION)",)
