# ditbox -- tooling layer for dit Cloud Build runs.
#
# Contents:
#   - Python 3.12 (matches pipe-anchorages' requires-python>=3.12; pipe-gaps
#     and pipe-events install fine on the same base)
#   - git, make, curl, gnupg
#   - gcloud CLI (incl. bq) for BigQuery operations in workflows
#   - docker CLI (talks to the host /var/run/docker.sock mounted by cloudbuild-dit.yaml)
#   - dit's own runtime deps pre-installed (so the runtime `pip install dit` is a no-op for them)
#
# Deliberately NOT baked in:
#   - dit itself -- cloned fresh per run from a configurable ref so iteration on dit changes
#     is testable in Cloud Build without rebuilding the image.
#   - pipeline deps (pipe-gaps / anchorages_pipeline / pipe-events) -- installed per-run by
#     the workflow's --binding ref.

FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        gnupg \
        make \
 && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
        | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
 && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
        > /etc/apt/sources.list.d/google-cloud-sdk.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends google-cloud-cli \
 && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://download.docker.com/linux/debian/gpg \
        | gpg --dearmor -o /usr/share/keyrings/docker.gpg \
 && echo "deb [signed-by=/usr/share/keyrings/docker.gpg] https://download.docker.com/linux/debian bookworm stable" \
        > /etc/apt/sources.list.d/docker.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends \
        docker-ce-cli \
        docker-compose-plugin \
 && rm -rf /var/lib/apt/lists/*

# uv is used by cloudbuild-dit.yaml for the per-run pipeline install. It
# resolves and installs much faster than pip (~10x on cold cache), which is
# the bottleneck for short Cloud Build runs. Pinned to the system Python via
# the `--system` flag when invoked.
# Version-pinned for reproducibility; bump intentionally when a new uv brings
# a desired change.
RUN pip install --no-cache-dir uv==0.11.15

COPY requirements.txt /tmp/dit-requirements.txt
RUN pip install -r /tmp/dit-requirements.txt \
 && rm /tmp/dit-requirements.txt

WORKDIR /workspace

CMD ["bash"]
