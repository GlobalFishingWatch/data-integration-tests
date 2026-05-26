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

# Default --no-deps on -ref / snapshot installs (assumes pipeline's transitive
# deps haven't changed). Set FULLDEPS=1 to drop the flag and let pip reinstall
# the full dep tree -- needed only when the target ref bumped or added a dep.
DEPS_FLAG = $(if $(FULLDEPS),,--no-deps)

.PHONY: install \
    install-pipe-gaps install-port-visits install-pipe-events install-all \
    install-pipe-gaps-ref install-port-visits-ref install-pipe-events-ref \
    snapshot-pipe-gaps snapshot-port-visits snapshot-pipe-events \
    clean-snapshot clean-snapshots \
    publish-ditbox dit-cloud

# === Framework only ===

install:
	$(PIP) install -e ".[dev]"

# === Editable installs (active dev) ===

install-pipe-gaps:
	$(PIP) install -e ".[dev]" -e "$(PIPE_GAPS_DIR)"

install-port-visits:
	$(PIP) install -e ".[dev]" -e "$(PORT_VISITS_DIR)"

install-pipe-events:
	$(PIP) install -e ".[dev]" -e "$(PIPE_EVENTS_DIR)"

install-all:
	$(PIP) install -e ".[dev]" \
	    -e "$(PIPE_GAPS_DIR)" \
	    -e "$(PORT_VISITS_DIR)" \
	    -e "$(PIPE_EVENTS_DIR)"

# === Specific-ref installs (reproducible, non-editable) ===

install-pipe-gaps-ref:
	@test -n "$(REF)" || { echo "REF is required: make $@ REF=<sha-or-branch>"; exit 1; }
	$(PIP) install --force-reinstall $(DEPS_FLAG) "git+file://$(PIPE_GAPS_DIR)@$(REF)"

install-port-visits-ref:
	@test -n "$(REF)" || { echo "REF is required: make $@ REF=<sha-or-branch>"; exit 1; }
	$(PIP) install --force-reinstall $(DEPS_FLAG) "git+file://$(PORT_VISITS_DIR)@$(REF)"

install-pipe-events-ref:
	@test -n "$(REF)" || { echo "REF is required: make $@ REF=<sha-or-branch>"; exit 1; }
	$(PIP) install --force-reinstall $(DEPS_FLAG) "git+file://$(PIPE_EVENTS_DIR)@$(REF)"

# === Snapshot + install ===
#
# Build a deterministic orphan snapshot of the pipeline checkout's tracked
# state under refs/dit-snapshots/<pipeline>/<commit-short-sha>, push it to
# origin (skipped if the ref already exists), then pip install from that ref.
# Identical tree state -> identical SHA -> cache hits on repeat runs.

snapshot-pipe-gaps:
	scripts/snapshot-install.sh pipe-gaps "$(PIPE_GAPS_DIR)" $(DEPS_FLAG)

snapshot-port-visits:
	scripts/snapshot-install.sh anchorages_pipeline "$(PORT_VISITS_DIR)" $(DEPS_FLAG)

snapshot-pipe-events:
	scripts/snapshot-install.sh pipe-events "$(PIPE_EVENTS_DIR)" $(DEPS_FLAG)

# === Cleanup ===
#
# Snapshots live forever by design (bytes-scale, hidden namespace). The
# surgical target below exists only for secret-leak remediation -- e.g. a
# .env file accidentally ended up in a snapshot's tree and got pushed.
#
#   make clean-snapshot PIPELINE=pipe-gaps REF=<sha-or-full-ref>

clean-snapshot:
	@test -n "$(PIPELINE)" || { echo "PIPELINE required: PIPELINE=pipe-gaps | anchorages_pipeline | pipe-events" >&2; exit 1; }
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
#   PIPELINE=pipe-gaps | anchorages_pipeline | pipe-events
# Optional:
#   ARGS="..."        # appended verbatim to `dit run <workflow>`
#   REF=<sha-or-tag>  # pipeline ref to install non-editably; empty = editable from source upload
#   DIT_REF=<ref>     # dit ref to clone (default main)
#   BEAM_VERSION=<x.y.z>  # pin apache-beam to match the worker image's beam version
#                          # (pipe-gaps:v0.9.6 = 2.71.0; pipe-anchorages:v4.6.4 = 2.69.0).
#                          # Auto-defaulted below by PIPELINE when unset.
#
# Example:
#   make dit-cloud \
#       PIPELINE=anchorages_pipeline \
#       WORKFLOW=workflows/port_visits/ais.py \
#       ARGS="--runner dataflow --parallel --build-from-source"

# Per-pipeline default for BEAM_VERSION. Override by passing BEAM_VERSION=x.y.z.
# Empty -> no constraint (uv picks newest; expect SDK-version mismatch errors
# unless the workflow runs locally via --runner docker).
ifeq ($(PIPELINE),pipe-gaps)
  BEAM_VERSION ?= 2.71.0
endif
ifeq ($(PIPELINE),anchorages_pipeline)
  BEAM_VERSION ?= 2.69.0
endif

# When the pipeline checkout is dirty, auto-snapshot it to a pushed ref
# (scripts/snapshot.sh -> refs/dit-snapshots/<pipeline>/<sha>) on the laptop
# (where git-push creds live) BEFORE the Cloud Build submit, then thread the
# resolved commit into the build as _PIPELINE_COMMIT. The build still uploads
# the (byte-identical) working tree as its source; _PIPELINE_COMMIT is what the
# workflow records as pipeline_commit, so the run is traceable to the pushed
# snapshot. A clean checkout records HEAD and skips the snapshot. Set
# REQUIRE_CLEAN=1 to error on a dirty tree instead of snapshotting.
dit-cloud:
	@test -n "$(WORKFLOW)" || { echo "WORKFLOW required: WORKFLOW=workflows/..." >&2; exit 1; }
	@test -n "$(PIPELINE)" || { echo "PIPELINE required: anchorages_pipeline | pipe-gaps | pipe-events" >&2; exit 1; }
	@test -d "$(PROJECTS)/$(PIPELINE)" || { echo "pipeline dir not found: $(PROJECTS)/$(PIPELINE)" >&2; exit 1; }
	@PIPELINE_COMMIT=""; \
	if [ -z "$(REF)" ] && [ -n "$$(git -C "$(PROJECTS)/$(PIPELINE)" status --porcelain --untracked-files=no)" ]; then \
	    if [ -n "$(REQUIRE_CLEAN)" ]; then \
	        echo "error: $(PIPELINE) checkout is dirty and REQUIRE_CLEAN=1 was set." >&2; \
	        echo "       commit + push, or drop REQUIRE_CLEAN to auto-snapshot." >&2; \
	        exit 1; \
	    fi; \
	    echo ">>> $(PIPELINE) checkout is dirty; auto-snapshotting..." >&2; \
	    SNAP_REF=$$(scripts/snapshot.sh "$(PIPELINE)" "$(PROJECTS)/$(PIPELINE)"); \
	    PIPELINE_COMMIT=$$(git -C "$(PROJECTS)/$(PIPELINE)" rev-parse --short "$$SNAP_REF"); \
	    echo ">>> snapshot pipeline_commit=$$PIPELINE_COMMIT" >&2; \
	fi; \
	gcloud builds submit \
	    --config=cloudbuild-dit.yaml \
	    --ignore-file=$(CURDIR)/.gcloudignore \
	    --substitutions="^@@^_WORKFLOW=$(WORKFLOW)@@_PIPELINE=$(PIPELINE)@@_ARGS=$(ARGS)@@_REF=$(REF)@@_DIT_REF=$(or $(DIT_REF),main)@@_BEAM_VERSION=$(BEAM_VERSION)@@_PIPELINE_COMMIT=$$PIPELINE_COMMIT" \
	    "$(PROJECTS)/$(PIPELINE)"
