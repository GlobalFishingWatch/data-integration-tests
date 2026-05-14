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
    clean-snapshots

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

# === Snapshot + install (auto-commit dirty tree on dit-snapshot-<epoch> branch) ===

snapshot-pipe-gaps:
	scripts/snapshot-install.sh "$(PIPE_GAPS_DIR)" $(DEPS_FLAG)

snapshot-port-visits:
	scripts/snapshot-install.sh "$(PORT_VISITS_DIR)" $(DEPS_FLAG)

snapshot-pipe-events:
	scripts/snapshot-install.sh "$(PIPE_EVENTS_DIR)" $(DEPS_FLAG)

# === Cleanup ===

clean-snapshots:
	scripts/clean-snapshots.sh "$(PIPE_GAPS_DIR)" "$(PORT_VISITS_DIR)" "$(PIPE_EVENTS_DIR)"
