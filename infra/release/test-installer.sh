#!/bin/sh

set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
VERSION=9.8.7

case $(uname -s) in
  Linux) os=linux ;;
  Darwin) os=darwin ;;
  *) echo "installer test requires Linux or macOS" >&2; exit 1 ;;
esac
case $(uname -m) in
  x86_64 | amd64) arch=amd64 ;;
  arm64 | aarch64) arch=arm64 ;;
  *) echo "installer test requires AMD64 or ARM64" >&2; exit 1 ;;
esac

work=$(mktemp -d "${TMPDIR:-/tmp}/ventstream-installer-test.XXXXXX")
trap 'rm -rf "$work"' EXIT INT TERM
release_dir="$work/release"
package_dir="$work/package"
archive="ventstream-$VERSION-$os-$arch.tar.gz"
mkdir -p "$release_dir" "$package_dir" "$work/home"

write_checksum() {
  if command -v sha256sum >/dev/null 2>&1; then
    checksum=$(sha256sum "$release_dir/$archive" | awk '{print $1}')
  else
    checksum=$(shasum -a 256 "$release_dir/$archive" | awk '{print $1}')
  fi
  printf '%s  ./%s\n' "$checksum" "$archive" > "$release_dir/SHA256SUMS"
}

cat > "$package_dir/ventstream" <<EOF
#!/bin/sh
if [ "\${1:-}" = "--version" ]; then
  echo "ventstream $VERSION"
  exit 0
fi
exit 1
EOF
chmod 0755 "$package_dir/ventstream"
cp "$ROOT_DIR/examples/standalone/ventstream.yaml" "$package_dir/ventstream.example.yaml"
cp "$ROOT_DIR/infra/release/BINARY_README.md" "$package_dir/README.md"
tar -czf "$release_dir/$archive" -C "$package_dir" ventstream ventstream.example.yaml README.md
write_checksum

HOME="$work/home" \
VENTSTREAM_VERSION="$VERSION" \
VENTSTREAM_INSTALL_DIR="$work/bin" \
VENTSTREAM_CONFIG_DIR="$work/config" \
VENTSTREAM_DOWNLOAD_BASE_URL="file://$release_dir" \
  sh "$ROOT_DIR/install.sh" > "$work/install.log"

test "$("$work/bin/ventstream" --version)" = "ventstream $VERSION"
test -f "$work/config/ventstream.example.yaml"

printf 'user-managed: true\n' > "$work/config/ventstream.example.yaml"
HOME="$work/home" \
VENTSTREAM_VERSION="$VERSION" \
VENTSTREAM_INSTALL_DIR="$work/bin" \
VENTSTREAM_CONFIG_DIR="$work/config" \
VENTSTREAM_DOWNLOAD_BASE_URL="file://$release_dir" \
  sh "$ROOT_DIR/install.sh" > "$work/reinstall.log"
grep -qx 'user-managed: true' "$work/config/ventstream.example.yaml"

printf 'unexpected\n' > "$package_dir/unexpected.txt"
tar -czf "$release_dir/$archive" -C "$package_dir" \
  ventstream ventstream.example.yaml README.md unexpected.txt
write_checksum
if HOME="$work/home" \
  VENTSTREAM_VERSION="$VERSION" \
  VENTSTREAM_INSTALL_DIR="$work/unexpected-bin" \
  VENTSTREAM_CONFIG_DIR="$work/unexpected-config" \
  VENTSTREAM_DOWNLOAD_BASE_URL="file://$release_dir" \
    sh "$ROOT_DIR/install.sh" > "$work/unexpected.log" 2>&1; then
  echo "installer accepted an archive with an unexpected file" >&2
  exit 1
fi
grep -q 'archive contains unexpected path: unexpected.txt' "$work/unexpected.log"

rm "$package_dir/unexpected.txt"
tar -czf "$release_dir/$archive" -C "$package_dir" ventstream ventstream.example.yaml README.md
write_checksum
printf 'tampered' >> "$release_dir/$archive"
if HOME="$work/home" \
  VENTSTREAM_VERSION="$VERSION" \
  VENTSTREAM_INSTALL_DIR="$work/tampered-bin" \
  VENTSTREAM_CONFIG_DIR="$work/tampered-config" \
  VENTSTREAM_DOWNLOAD_BASE_URL="file://$release_dir" \
    sh "$ROOT_DIR/install.sh" > "$work/tampered.log" 2>&1; then
  echo "installer accepted an archive with a mismatched checksum" >&2
  exit 1
fi
grep -q 'checksum verification failed' "$work/tampered.log"

echo "native installer tests passed"
