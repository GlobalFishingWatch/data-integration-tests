#!/usr/bin/env bash
# Build (and optionally push) a DIT overlay image of pipe-anchorages: an exact commit
# plus temp_dataset.patch, built from the repo's own Dockerfile `prod` target.
#
# Usage: build.sh <pipe-anchorages repo dir> <commit-ish> <label> [--push]
#   label  short name for what the commit is, e.g. "main" or "pipeline-4690"
#
# Image: gcr.io/world-fishing-827/dit/pipe-anchorages:<label>-<commit7>-temp-dataset-<patch8>
# (dit namespace only; never gfw-int-infrastructure or prod-shaped paths).
#
# Fails before building if the patch does not apply, and after building if the
# self-check (verify_overlay.py) fails inside the image, so a broken overlay never ships.
set -euo pipefail

if [[ $# -lt 3 || $# -gt 4 || ( $# -eq 4 && "$4" != "--push" ) ]]; then
  echo "usage: $0 <pipe-anchorages repo dir> <commit-ish> <label> [--push]" >&2
  exit 2
fi

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$1"
ref="$2"
label="$3"
push="${4:-}"

if [[ ! "$label" =~ ^[a-z0-9][a-z0-9.-]*$ ]]; then
  echo "label must be lowercase [a-z0-9.-]; got: $label" >&2
  exit 2
fi

commit="$(git -C "$repo" rev-parse --verify "${ref}^{commit}")"
patch_sha="$(sha256sum "$here/temp_dataset.patch" | cut -c1-8)"
image="gcr.io/world-fishing-827/dit/pipe-anchorages:${label}-${commit:0:7}-temp-dataset-${patch_sha}"

context="$(mktemp -d)"
trap 'rm -rf "$context"' EXIT

echo "==> exporting ${commit} from ${repo}"
git -C "$repo" archive "$commit" | tar -x -C "$context"

echo "==> applying temp_dataset.patch"
(cd "$context" && git apply --check "$here/temp_dataset.patch" && git apply "$here/temp_dataset.patch")

echo "==> building ${image} (target prod)"
docker build --target prod \
  --label "dit.overlay=pipe-anchorages-temp-dataset" \
  --label "dit.pipeline_commit=${commit}" \
  --label "dit.patch_sha256_8=${patch_sha}" \
  -t "$image" "$context"

echo "==> self-check inside the image"
docker run --rm \
  -v "$here/verify_overlay.py:/tmp/verify_overlay.py:ro" \
  --entrypoint python "$image" /tmp/verify_overlay.py
docker run --rm --entrypoint pipe-anchorages "$image" port-visits --help > /dev/null
echo "    pipe-anchorages console script OK"

if [[ "$push" == "--push" ]]; then
  echo "==> pushing ${image}"
  docker push "$image"
  digest="$(docker inspect --format '{{index .RepoDigests 0}}' "$image")"
  echo "pushed: ${digest}"
else
  echo "built (not pushed): ${image}"
fi
