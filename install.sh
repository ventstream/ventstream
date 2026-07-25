#!/bin/sh

set -eu

REPOSITORY=${VENTSTREAM_REPOSITORY:-bashiru98/ventstream}
INSTALL_DIR=${VENTSTREAM_INSTALL_DIR:-${HOME:?HOME is required}/.local/bin}
CONFIG_DIR=${VENTSTREAM_CONFIG_DIR:-${HOME}/.config/ventstream}
VERSION=${VENTSTREAM_VERSION:-}
DOWNLOAD_BASE_URL=${VENTSTREAM_DOWNLOAD_BASE_URL:-}

fail() {
  echo "ventstream installer: $*" >&2
  exit 1
}

require() {
  command -v "$1" >/dev/null 2>&1 || fail "$1 is required"
}

download() {
  url=$1
  destination=$2
  if [ -n "$DOWNLOAD_BASE_URL" ]; then
    curl -fsSL "$url" -o "$destination"
  else
    curl --proto '=https' --tlsv1.2 -fsSL "$url" -o "$destination"
  fi
}

require curl
require tar
require install
require awk
require grep
require mktemp
require mv

case $(uname -s) in
  Linux) os=linux ;;
  Darwin) os=darwin ;;
  *) fail "unsupported operating system: $(uname -s); use the OCI image instead" ;;
esac

case $(uname -m) in
  x86_64 | amd64) arch=amd64 ;;
  arm64 | aarch64) arch=arm64 ;;
  *) fail "unsupported CPU architecture: $(uname -m); use the OCI image instead" ;;
esac

if [ -z "$VERSION" ]; then
  [ -z "$DOWNLOAD_BASE_URL" ] || fail "VENTSTREAM_VERSION is required with VENTSTREAM_DOWNLOAD_BASE_URL"
  latest_url="https://github.com/$REPOSITORY/releases/latest"
  effective_url=$(curl --proto '=https' --tlsv1.2 -fsSL \
    -o /dev/null -w '%{url_effective}' "$latest_url") || \
    fail "could not resolve the latest release; private repositories require a manual authenticated download"
  tag=${effective_url##*/}
  VERSION=${tag#v}
fi

printf '%s' "$VERSION" | grep -Eq '^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$' || \
  fail "VENTSTREAM_VERSION must be a stable MAJOR.MINOR.PATCH version"

archive="ventstream-$VERSION-$os-$arch.tar.gz"
if [ -n "$DOWNLOAD_BASE_URL" ]; then
  base_url=${DOWNLOAD_BASE_URL%/}
else
  base_url="https://github.com/$REPOSITORY/releases/download/v$VERSION"
fi

temporary_directory=$(mktemp -d "${TMPDIR:-/tmp}/ventstream-install.XXXXXX")
temporary_target=

cleanup() {
  [ -z "$temporary_target" ] || rm -f "$temporary_target"
  rm -rf "$temporary_directory"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

download "$base_url/$archive" "$temporary_directory/$archive"
download "$base_url/SHA256SUMS" "$temporary_directory/SHA256SUMS"

expected_checksum=$(awk -v archive="$archive" '
  $2 == archive || $2 == "./" archive { print $1; exit }
' "$temporary_directory/SHA256SUMS")
[ -n "$expected_checksum" ] || fail "SHA256SUMS does not contain $archive"

if command -v sha256sum >/dev/null 2>&1; then
  actual_checksum=$(sha256sum "$temporary_directory/$archive" | awk '{print $1}')
elif command -v shasum >/dev/null 2>&1; then
  actual_checksum=$(shasum -a 256 "$temporary_directory/$archive" | awk '{print $1}')
else
  fail "sha256sum or shasum is required"
fi
[ "$actual_checksum" = "$expected_checksum" ] || fail "checksum verification failed for $archive"

archive_entries=$(tar -tzf "$temporary_directory/$archive") || fail "could not read $archive"
binary_entries=0
config_entries=0
readme_entries=0
while IFS= read -r entry; do
  normalized_entry=${entry#./}
  case "$normalized_entry" in
    ventstream) binary_entries=$((binary_entries + 1)) ;;
    ventstream.example.yaml) config_entries=$((config_entries + 1)) ;;
    README.md) readme_entries=$((readme_entries + 1)) ;;
    *) fail "archive contains unexpected path: $entry" ;;
  esac
done <<EOF
$archive_entries
EOF
[ "$binary_entries" -eq 1 ] || fail "archive must contain exactly one ventstream binary"
[ "$config_entries" -eq 1 ] || fail "archive must contain exactly one ventstream.example.yaml"
[ "$readme_entries" -eq 1 ] || fail "archive must contain exactly one README.md"

mkdir -p "$temporary_directory/package"
tar -xzf "$temporary_directory/$archive" -C "$temporary_directory/package"
[ -f "$temporary_directory/package/ventstream" ] && \
  [ ! -L "$temporary_directory/package/ventstream" ] && \
  [ -x "$temporary_directory/package/ventstream" ] || \
  fail "archive does not contain a regular executable ventstream binary"
[ -f "$temporary_directory/package/ventstream.example.yaml" ] && \
  [ ! -L "$temporary_directory/package/ventstream.example.yaml" ] || \
  fail "archive does not contain a regular ventstream.example.yaml"
[ -f "$temporary_directory/package/README.md" ] && \
  [ ! -L "$temporary_directory/package/README.md" ] || \
  fail "archive does not contain a regular README.md"

actual_version=$($temporary_directory/package/ventstream --version) || \
  fail "archive binary could not report its version"
[ "$actual_version" = "ventstream $VERSION" ] || \
  fail "archive version mismatch: expected ventstream $VERSION, received $actual_version"

mkdir -p "$INSTALL_DIR" "$CONFIG_DIR"
temporary_target=$(mktemp "$INSTALL_DIR/.ventstream.install.XXXXXX")
install -m 0755 "$temporary_directory/package/ventstream" "$temporary_target"
mv -f "$temporary_target" "$INSTALL_DIR/ventstream"
temporary_target=
if [ ! -e "$CONFIG_DIR/ventstream.example.yaml" ]; then
  install -m 0644 "$temporary_directory/package/ventstream.example.yaml" \
    "$CONFIG_DIR/ventstream.example.yaml"
fi

echo "Installed ventstream $VERSION to $INSTALL_DIR/ventstream"
echo "Example configuration: $CONFIG_DIR/ventstream.example.yaml"
case :$PATH: in
  *:"$INSTALL_DIR":*) ;;
  *) echo "Add $INSTALL_DIR to PATH before running ventstream" ;;
esac
echo "Validate: VS_ENGINE_CONFIG=./ventstream.yaml ventstream --validate-config"
echo "Run:      VS_ENGINE_CONFIG=./ventstream.yaml ventstream"
