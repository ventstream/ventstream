#!/bin/sh
set -eu

if [ "$#" -ne 6 ]; then
  echo "usage: $0 IMAGE VERSION SOURCE_SHA OUTPUT_FILE AMD64_JSON ARM64_JSON" >&2
  exit 2
fi

image=$1
version=$2
source_sha=$3
output_file=$4
amd64_file=$5
arm64_file=$6

if ! printf '%s' "$version" | grep -Eq '^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'; then
  echo "release version must be a stable MAJOR.MINOR.PATCH version" >&2
  exit 1
fi

if ! printf '%s' "$source_sha" | grep -Eq '^[0-9a-f]{40}$'; then
  echo "source SHA must be one full lowercase Git commit" >&2
  exit 1
fi

for file in "$amd64_file" "$arm64_file"; do
  if [ ! -f "$file" ]; then
    echo "digest record not found: $file" >&2
    exit 1
  fi
done

validate_record() {
  record=$1
  expected_platform=$2
  jq -e \
    --arg image "$image" \
    --arg platform "$expected_platform" '
      .image == $image and
      .platform == $platform and
      (.digest | test("^sha256:[0-9a-f]{64}$"))
    ' "$record" >/dev/null || {
      echo "invalid digest record for $image $expected_platform: $record" >&2
      exit 1
    }
}

validate_record "$amd64_file" linux/amd64
validate_record "$arm64_file" linux/arm64
amd64_digest=$(jq -er '.digest' "$amd64_file")
arm64_digest=$(jq -er '.digest' "$arm64_file")

release_ref="$image:$version"
sha_ref="$image:sha-$source_sha"
assert_ref_absent() {
  ref=$1
  if inspect_output=$(docker buildx imagetools inspect "$ref" 2>&1); then
    echo "refusing to overwrite existing release image $ref" >&2
    exit 1
  fi
  case "$inspect_output" in
    *"not found"*|*"manifest unknown"*) ;;
    *)
      echo "could not prove release image reference is absent: $ref" >&2
      printf '%s\n' "$inspect_output" >&2
      exit 1
      ;;
  esac
}

for ref in "$release_ref" "$sha_ref"; do
  assert_ref_absent "$ref"
done

docker buildx imagetools create \
  --tag "$release_ref" \
  --tag "$sha_ref" \
  "$image@$amd64_digest" \
  "$image@$arm64_digest"

raw_manifest=$(docker buildx imagetools inspect --raw "$release_ref")
printf '%s' "$raw_manifest" | jq -e \
  --arg amd64 "$amd64_digest" \
  --arg arm64 "$arm64_digest" '
  (.manifests | length == 2) and
  (any(.manifests[]; .platform.os == "linux" and .platform.architecture == "amd64" and .digest == $amd64)) and
  (any(.manifests[]; .platform.os == "linux" and .platform.architecture == "arm64" and .digest == $arm64))
' >/dev/null

index_digest=$(docker buildx imagetools inspect "$release_ref" --format '{{json .Manifest}}' | jq -er '.digest')
sha_digest=$(docker buildx imagetools inspect "$sha_ref" --format '{{json .Manifest}}' | jq -er '.digest')
if [ "$sha_digest" != "$index_digest" ]; then
  echo "$release_ref and $sha_ref resolved to different manifests" >&2
  exit 1
fi
mkdir -p "$(dirname "$output_file")"
printf '%s@%s\n' "$image" "$index_digest" >"$output_file"

if [ -n "${GITHUB_OUTPUT:-}" ]; then
  printf 'digest=%s\n' "$index_digest" >>"$GITHUB_OUTPUT"
fi

printf 'published %s@%s\n' "$image" "$index_digest"
