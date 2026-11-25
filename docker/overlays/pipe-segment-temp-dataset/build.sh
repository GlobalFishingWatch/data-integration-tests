#!/usr/bin/env bash
# Build (and optionally push) a DIT overlay image of pipe-segment: an exact commit plus
# the temp-dataset patch, built from the repo's own Dockerfile `prod` target.
#
# Usage: build.sh <pipe-segment repo dir> <commit-ish> <label> [--push]
#   label  short name for what the commit is. Start it with the pipe-segment version the
#          commit holds, e.g. "v5.1.0" or "v5.1.0-pr230": workflows/pipe_segment/ais.py
#          reads the version from the image tag to decide whether the image knows
#          --incremental.
#
# Image: gcr.io/world-fishing-827/dit/pipe-segment:<label>-<commit7>-temp-dataset-<patch8>
# (dit namespace only; never gfw-int-infrastructure or prod-shaped paths).
#
# One patch per pipe-segment generation (the read sites differ): the script uses the
# first of PATCHES that applies to the commit, and fails if none does.
#
# Fails before building if no patch applies, and after building if the self-check
# (verify_overlay.py) fails inside the image, so a broken overlay never ships.
set -euo pipefail

if [[ $# -lt 3 || $# -gt 4 || ( $# -eq 4 && "$4" != "--push" ) ]]; then
  echo "usage: $0 <pipe-segment repo dir> <commit-ish> <label> [--push]" >&2
  exit 2
fi

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$1"
ref="$2"
label="$3"
push="${4:-}"

# Newest generation first. temp_dataset.patch: v5.1.0 and later (five reads).
# temp_dataset_v5.0.3.patch: v5.0.3 (four reads; no prior-segments read).
PATCHES=(temp_dataset.patch temp_dataset_v5.0.3.patch)

if [[ ! "$label" =~ ^[a-z0-9][a-z0-9.-]*$ ]]; then
  echo "label must be lowercase [a-z0-9.-]; got: $label" >&2
  exit 2
fi

commit="$(git -C "$repo" rev-parse --verify "${ref}^{commit}")"

context="$(mktemp -d)"
trap 'rm -rf "$context"' EXIT

echo "==> exporting ${commit} from ${repo}"
git -C "$repo" archive "$commit" | tar -x -C "$context"

patch=""
for candidate in "${PATCHES[@]}"; do
  if (cd "$context" && git apply --check "$here/$candidate" 2>/dev/null); then
    patch="$candidate"
    break
  fi
done
if [[ -z "$patch" ]]; then
  echo "no patch applies to ${commit}; tried: ${PATCHES[*]}" >&2
  echo "a BigQuery read site changed upstream: write a patch for this generation" >&2
  exit 1
fi

patch_sha="$(sha256sum "$here/$patch" | cut -c1-8)"
image="gcr.io/world-fishing-827/dit/pipe-segment:${label}-${commit:0:7}-temp-dataset-${patch_sha}"

echo "==> applying ${patch}"
(cd "$context" && git apply "$here/$patch")

echo "==> building ${image} (target prod)"
docker build --target prod \
  --label "dit.overlay=pipe-segment-temp-dataset" \
  --label "dit.pipeline_commit=${commit}" \
  --label "dit.patch=${patch}" \
  --label "dit.patch_sha256_8=${patch_sha}" \
  -t "$image" "$context"

echo "==> self-check inside the image"
docker run --rm \
  -v "$here/verify_overlay.py:/tmp/verify_overlay.py:ro" \
  --entrypoint python "$image" /tmp/verify_overlay.py --patch "$patch"
docker run --rm --entrypoint pipe-segment "$image" segment --help > /dev/null
echo "    pipe-segment console script OK"

if [[ "$push" == "--push" ]]; then
  echo "==> pushing ${image}"
  docker push "$image"
  digest="$(docker inspect --format '{{index .RepoDigests 0}}' "$image")"
  echo "pushed: ${digest}"
else
  echo "built (not pushed): ${image}"
fi
