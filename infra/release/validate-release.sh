#!/bin/sh
set -eu

tag=${1:-}
if [ -z "$tag" ]; then
  echo "usage: $0 vMAJOR.MINOR.PATCH" >&2
  exit 2
fi

case "$tag" in
  v[0-9]*.[0-9]*.[0-9]*) ;;
  *)
    echo "release tag must start with v and contain a semantic version: $tag" >&2
    exit 1
    ;;
esac

metadata=$(cargo metadata --format-version 1 --no-deps)
version=$(printf '%s' "$metadata" | jq -er '
  [.packages[].version] | unique |
  if length == 1 then .[0] else error("workspace package versions differ") end
')

if ! printf '%s' "$version" | grep -Eq '^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'; then
  echo "release version must be a stable MAJOR.MINOR.PATCH version: $version" >&2
  exit 1
fi

if [ "$tag" != "v$version" ]; then
  echo "release tag $tag does not match workspace version $version" >&2
  exit 1
fi

for chart in infra/helm/ventstream-gateway; do
  chart_version=$(awk '$1 == "version:" { gsub(/"/, "", $2); print $2; exit }' "$chart/Chart.yaml")
  app_version=$(awk '$1 == "appVersion:" { gsub(/"/, "", $2); print $2; exit }' "$chart/Chart.yaml")
  if [ "$chart_version" != "$version" ] || [ "$app_version" != "$version" ]; then
    echo "$chart/Chart.yaml must use version and appVersion $version" >&2
    exit 1
  fi
done

if ! grep -q 'repository: ghcr.io/ventstream/ventstream$' infra/helm/ventstream-gateway/values.yaml; then
  echo "the supported gateway chart must default to the release image repository" >&2
  exit 1
fi

if ! grep -q '^ARG CARGO_AUDITABLE_VERSION=0.7.5$' infra/docker/engine.Dockerfile; then
  echo "the release image must use pinned cargo-auditable 0.7.5" >&2
  exit 1
fi

for dockerfile in infra/docker/*.Dockerfile; do
  if ! awk '
    toupper($1) == "FROM" {
      image_field = ($2 ~ /^--platform=/) ? 3 : 2
      image = $image_field
      if (image != "scratch" && !(image in stages) && image !~ /@sha256:[0-9a-f]{64}$/) {
        exit 1
      }
      for (i = image_field + 1; i < NF; i++) {
        if (toupper($i) == "AS") {
          stages[$(i + 1)] = 1
        }
      }
    }
  ' "$dockerfile"; then
    echo "$dockerfile contains an unpinned base image" >&2
    exit 1
  fi
done

if grep -R -n -E 'ghcr.io/REPLACE_ME|ventstream-engine:latest|repository:.*:latest|appVersion: *"?latest"?' \
  infra/helm infra/k8s; then
  echo "release packaging contains a placeholder or floating latest reference" >&2
  exit 1
fi

# The publisher only ever creates :$version and :sha-<commit>. Docs that tell
# people to pull :latest send them at a tag that has never existed, and demo
# manifests pinned to an old release quietly ship a stale engine. Both had
# drifted before this check existed, so assert on every surface a user copies
# from, not just the packaging under infra/.
user_facing='README.md docs docs-site demo infra/k8s infra/helm'
if grep -R -n -E 'ghcr\.io/ventstream/ventstream(-managed-engine)?:latest' \
  $user_facing; then
  echo "docs or demo manifests reference :latest, which the publisher never creates" >&2
  exit 1
fi

stale_pins=$(grep -R -h -o -E 'ghcr\.io/ventstream/ventstream(-managed-engine)?:[0-9]+\.[0-9]+\.[0-9]+' \
  $user_facing | grep -v ":${version}$" | sort -u || true)
# Bare version pins in install snippets. These are not image references, so
# the check above cannot see them: VENTSTREAM_VERSION=0.1.28 sailed through
# it while the Docker line four lines below was caught. The macOS and Windows
# install tabs are the first thing a reader copies.
# Every shape a version pin takes in a command a reader copies. The image
# check above only sees `repo:tag`, so `--set image.tag=0.1.24` and
# `--version 0.1.24` were structurally invisible to it — and by the time
# anyone noticed, GHCR had aged 0.1.24 out entirely, so that Helm command
# produced an ImagePullBackOff rather than an old-but-working engine.
#
# Scoped to this release's own series (${version%.*}.x). The fleet CLI is
# versioned separately (0.2.x) and its current version is not knowable from
# this repo, so asserting on it here would either fail every release or
# encode a number that goes stale on the fleet's schedule instead of ours.
# It needs its own check in the fleet repo.
series="${version%.*}."
stale_cmd=$(grep -R -h -o -E \
  "(image\.tag=|--version |VERSION=|VENTSTREAM_VERSION[= ]+\"?|download v)${series}[0-9]+" \
  $user_facing | grep -oE "${series}[0-9]+" | grep -v "^${version}$" | sort -u || true)
if [ -n "$stale_cmd" ]; then
  echo "docs pin a version that is not the release ($version) in a copyable command:" >&2
  printf '  %s\n' $stale_cmd >&2
  echo "readers copy these verbatim; an aged-out tag is an ImagePullBackOff, not an old engine" >&2
  exit 1
fi

if [ -n "$stale_pins" ]; then
  echo "docs or demo manifests pin an engine image that is not the release version ($version):" >&2
  printf '  %s\n' $stale_pins >&2
  echo "update them, or use a <version> placeholder if the reference is illustrative" >&2
  exit 1
fi

if grep -Eq '^[[:space:]]+COSIGN_(CERTIFICATE_IDENTITY|OIDC_ISSUER):' \
  .github/workflows/release.yml; then
  echo "release verification values must not use reserved COSIGN_* environment names" >&2
  exit 1
fi

if [ "${RELEASE_REQUIRE_MAIN:-0}" = "1" ]; then
  if ! git show-ref --verify --quiet refs/remotes/origin/main; then
    echo "origin/main is required for the release ancestry check" >&2
    exit 1
  fi
  if ! git merge-base --is-ancestor HEAD origin/main; then
    echo "release commit must be reachable from origin/main" >&2
    exit 1
  fi
fi

printf 'release contract valid for %s\n' "$tag"
