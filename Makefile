# Install targets for the dit framework + per-pipeline workflow deps.
#
# Workflow dependencies (pipe-gaps, anchorages_pipeline, pipe-events) are
# installed editable from local checkouts so switching branches in those
# repos is picked up without a reinstall.
#
# PROJECTS defaults to the parent directory of this repo (sibling checkouts).
# Override with PROJECTS=/somewhere/else, or copy .envrc.example to .envrc
# and edit (direnv loads it automatically).

PROJECTS ?= $(realpath ..)
PIP ?= pip

.PHONY: install install-pipe-gaps install-port-visits install-pipe-events install-all

install:
	$(PIP) install -e ".[dev]"

install-pipe-gaps:
	$(PIP) install -e ".[dev]" -e "$(PROJECTS)/pipe-gaps"

install-port-visits:
	$(PIP) install -e ".[dev]" -e "$(PROJECTS)/anchorages_pipeline"

install-pipe-events:
	$(PIP) install -e ".[dev]" -e "$(PROJECTS)/pipe-events"

install-all:
	$(PIP) install -e ".[dev]" \
	    -e "$(PROJECTS)/pipe-gaps" \
	    -e "$(PROJECTS)/anchorages_pipeline" \
	    -e "$(PROJECTS)/pipe-events"
