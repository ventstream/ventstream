# Engine release process

Engine releases are tag-driven and immutable. The supported release artifacts
are the multi-architecture VentStream image, Linux and macOS standalone engine
binaries, the native installer, and the realtime gateway chart. The deprecated
telemetry agent chart remains in source for migrations but is not published as
a supported OCI chart.

## Repository controls

Before creating a release tag:

1. Protect `main` and require the CI and dependency-policy workflows.
2. Protect tags matching `v*` so only release maintainers can create them.
3. Create the `production-release` GitHub environment with required reviewers.
4. Restrict workflow changes under `.github/workflows/` and `infra/release/`
   through CODEOWNERS or an equivalent review rule.
5. Confirm GitHub artifact attestations are available for the repository plan.

The workflow refuses a tag whose version differs from the Cargo workspace and
Helm chart versions, a commit not reachable from `origin/main`, placeholders or
floating `latest` defaults, and an existing semantic-version image or chart tag.

## Published artifacts

A tag such as `v0.1.12` publishes:

- `ghcr.io/ventstream/ventstream:0.1.12`
- `ghcr.io/ventstream/ventstream:sha-<40-character-commit>`
- `oci://ghcr.io/ventstream/charts/ventstream-gateway` version `0.1.12`
- `ventstream-0.1.12-{linux-amd64,linux-arm64,darwin-amd64,darwin-arm64}.tar.gz`
- `ventstream-installer.sh`
- a GitHub release containing those assets, the chart, image digest,
  per-platform SPDX JSON SBOMs, Trivy JSON reports, and `SHA256SUMS`

There is deliberately no `latest` tag. Promote the recorded image digest between
environments instead of rebuilding or resolving a mutable tag again.

Both `linux/amd64` and `linux/arm64` images are built on native GitHub runners.
The native archives are also built on native GitHub runners using auditable
release builds, stripped binaries, and the source commit timestamp as
`SOURCE_DATE_EPOCH`. Linux binaries target the Ubuntu 22.04/glibc 2.35 baseline;
macOS binaries target macOS 12 or newer. External base images, the Rust
toolchain, Cargo Chef, Debian snapshot, and apt package versions are pinned.

The release fails before semantic-version promotion when Trivy finds any HIGH or
CRITICAL operating-system or application-library vulnerability, including an
unfixed finding. A temporary exception requires a reviewed `.trivyignore.yaml`
entry with vulnerability ID, rationale, owner, and expiry; do not weaken the
workflow-wide threshold.

Each platform manifest and the final multi-architecture index receive keyless
Cosign signatures and GitHub build-provenance attestations. Platform SPDX SBOMs
are retained and attached as signed GitHub SBOM attestations. The OCI chart and
downloadable chart archive are signed or attested separately. Every native
archive receives GitHub build-provenance and SBOM attestations, and the installer
receives a build-provenance attestation.

## Create a release

Update the Cargo workspace version and both chart `version` and `appVersion`
fields in one reviewed pull request. After that commit is merged and all required
checks pass, run the native runtime-image gate from `main`:

```bash
gh workflow run runtime-image-gate.yml --ref main
gh run list --workflow runtime-image-gate.yml --branch main --limit 1
gh run watch RUN_ID --exit-status
```

Both architecture jobs must build the final image, execute its version command,
pass the release vulnerability policy, and produce an application-aware SBOM.
The keyless-signing job must also sign and verify a test payload through GitHub
OIDC and Sigstore. Only then create and push the signed release tag:

```bash
git switch main
git pull --ff-only
git tag -s v0.1.12 -m "VentStream v0.1.12"
git push origin v0.1.12
```

The GitHub environment approval occurs after both platform images pass scanning
and before semantic-version image and chart publication. In parallel, all four
native jobs verify the binary version and validate the packaged standalone
configuration. The GitHub release remains a draft until image, chart, and native
artifact publication, signing, attestation, and evidence collection all succeed.

## Verify a release

Use the immutable digest from `image-digests.txt` in the GitHub release:

```bash
cosign verify \
  --certificate-identity-regexp \
  '^https://github.com/ventstream/ventstream/.github/workflows/release.yml@refs/tags/v[0-9]+[.][0-9]+[.][0-9]+$' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  ghcr.io/ventstream/ventstream@sha256:<digest>

gh attestation verify \
  oci://ghcr.io/ventstream/ventstream@sha256:<digest> \
  --repo ventstream/ventstream

sha256sum --check SHA256SUMS
```

Download and verify a native archive and its GitHub attestations before running
it. Use the archive matching the operator's platform:

```bash
VERSION=0.1.12
PLATFORM=linux-amd64
ARCHIVE="ventstream-$VERSION-$PLATFORM.tar.gz"

gh release download "v$VERSION" \
  --repo ventstream/ventstream \
  --pattern "$ARCHIVE" \
  --pattern SHA256SUMS
grep "  ./$ARCHIVE\$" SHA256SUMS | sha256sum --check
gh attestation verify "$ARCHIVE" \
  --repo ventstream/ventstream
gh release download "v$VERSION" \
  --repo ventstream/ventstream \
  --pattern ventstream-installer.sh
gh attestation verify ventstream-installer.sh \
  --repo ventstream/ventstream
tar -xzf "$ARCHIVE"
test "$(./ventstream --version)" = "ventstream $VERSION"
```

On macOS, use `shasum -a 256 --check` instead of `sha256sum --check`. Release
assets in a private repository require authenticated `gh` access. The documented
anonymous `curl` installer works when the open-core release assets are public.

Install the chart by its immutable version and pin the image by digest:

```bash
helm upgrade --install realtime \
  oci://ghcr.io/ventstream/charts/ventstream-gateway \
  --version 0.1.12 \
  --set image.digest=sha256:<digest>
```

## Failed publication

Do not move or reuse a release tag. If the workflow fails before semantic image
or chart promotion, fix the cause in a new commit and create the next patch
version. Untagged platform manifests may be deleted by the package retention
policy. If promotion completed but final evidence publication failed, keep the
tag frozen, inspect the workflow logs and registry digests, and complete only the
draft GitHub release after confirming every published digest and signature. Never
overwrite a published GitHub release or semantic-version OCI reference.
