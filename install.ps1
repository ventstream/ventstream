# VentStream engine installer for Windows.
#
#   irm https://ventstream.dev/install.ps1 | iex
#
# Security posture mirrors install.sh: HTTPS-only fetches, strict version
# validation, SHA256 verification against the release's SHA256SUMS before
# anything is extracted, an archive-content whitelist, and a user-local
# install with no elevation. The Mark-of-the-Web is removed only after the
# checksum has been proven.

$ErrorActionPreference = "Stop"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 -bor [Net.SecurityProtocolType]::Tls13

$Repository = if ($env:VENTSTREAM_REPOSITORY) { $env:VENTSTREAM_REPOSITORY } else { "ventstream/ventstream" }
$InstallDir = if ($env:VENTSTREAM_INSTALL_DIR) { $env:VENTSTREAM_INSTALL_DIR } else { Join-Path $env:LOCALAPPDATA "Programs\ventstream" }
$ConfigDir = if ($env:VENTSTREAM_CONFIG_DIR) { $env:VENTSTREAM_CONFIG_DIR } else { Join-Path $env:APPDATA "ventstream" }
$Version = $env:VENTSTREAM_VERSION
$DownloadBaseUrl = $env:VENTSTREAM_DOWNLOAD_BASE_URL

function Fail([string] $Message) {
    Write-Error "ventstream installer: $Message"
    exit 1
}

if ([Environment]::Is64BitOperatingSystem -eq $false) {
    Fail "only 64-bit Windows is supported"
}
$architecture = $env:PROCESSOR_ARCHITECTURE
if ($architecture -ne "AMD64") {
    Fail "unsupported CPU architecture: $architecture (only amd64 builds are published; use WSL2 or the OCI image)"
}

if (-not $Version) {
    if ($DownloadBaseUrl) { Fail "VENTSTREAM_VERSION is required with VENTSTREAM_DOWNLOAD_BASE_URL" }
    $latest = Invoke-WebRequest -Uri "https://github.com/$Repository/releases/latest" -Method Head -MaximumRedirection 5 -UseBasicParsing
    $tag = ($latest.BaseResponse.ResponseUri, $latest.BaseResponse.RequestMessage.RequestUri |
        Where-Object { $_ } | Select-Object -First 1).AbsolutePath.Split("/")[-1]
    if (-not $tag) { Fail "could not resolve the latest release" }
    $Version = $tag.TrimStart("v")
}

if ($Version -notmatch '^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$') {
    Fail "VENTSTREAM_VERSION must be a stable MAJOR.MINOR.PATCH version"
}

$archive = "ventstream-$Version-windows-amd64.zip"
$baseUrl = if ($DownloadBaseUrl) { $DownloadBaseUrl.TrimEnd("/") } else { "https://github.com/$Repository/releases/download/v$Version" }
if ($baseUrl -notmatch '^https://') { Fail "downloads must use https" }

$temporaryDirectory = Join-Path ([IO.Path]::GetTempPath()) ("ventstream-install-" + [Guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $temporaryDirectory | Out-Null

try {
    $archivePath = Join-Path $temporaryDirectory $archive
    $sumsPath = Join-Path $temporaryDirectory "SHA256SUMS"
    Invoke-WebRequest -Uri "$baseUrl/$archive" -OutFile $archivePath -UseBasicParsing
    Invoke-WebRequest -Uri "$baseUrl/SHA256SUMS" -OutFile $sumsPath -UseBasicParsing

    # SHA256 verification BEFORE extraction or unblocking. sha256sum output
    # is "<hex>  <name>" with an optional ./ prefix on the name.
    $expected = $null
    foreach ($line in Get-Content $sumsPath) {
        $parts = $line -split '\s+' | Where-Object { $_ }
        if ($parts.Count -ge 2) {
            $name = $parts[1] -replace '^\./', '' -replace '^\*', ''
            if ($name -eq $archive) { $expected = $parts[0].ToLowerInvariant(); break }
        }
    }
    if (-not $expected) { Fail "SHA256SUMS does not contain $archive" }
    $actual = (Get-FileHash -Algorithm SHA256 -Path $archivePath).Hash.ToLowerInvariant()
    if ($actual -ne $expected) { Fail "checksum verification failed for $archive" }

    # The checksum has been proven against the published release manifest;
    # clearing the Mark-of-the-Web here is the user-consented install, not
    # SmartScreen evasion.
    Unblock-File -Path $archivePath -ErrorAction SilentlyContinue

    $packageDirectory = Join-Path $temporaryDirectory "package"
    Expand-Archive -Path $archivePath -DestinationPath $packageDirectory

    # Content whitelist: exactly the three files the release packages.
    $allowed = @("ventstream.exe", "ventstream.example.yaml", "README.md")
    $entries = Get-ChildItem -Path $packageDirectory -Recurse -File
    foreach ($entry in $entries) {
        if ($allowed -notcontains $entry.Name -or $entry.DirectoryName -ne $packageDirectory) {
            Fail "archive contains unexpected path: $($entry.FullName)"
        }
    }
    foreach ($name in $allowed) {
        if (-not (Test-Path (Join-Path $packageDirectory $name))) {
            Fail "archive is missing ${name}"
        }
    }

    New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
    New-Item -ItemType Directory -Path $ConfigDir -Force | Out-Null
    Move-Item -Path (Join-Path $packageDirectory "ventstream.exe") -Destination (Join-Path $InstallDir "ventstream.exe") -Force
    Move-Item -Path (Join-Path $packageDirectory "README.md") -Destination (Join-Path $InstallDir "README.md") -Force
    $examplePath = Join-Path $ConfigDir "ventstream.example.yaml"
    Move-Item -Path (Join-Path $packageDirectory "ventstream.example.yaml") -Destination $examplePath -Force

    # User-scoped PATH update; no elevation, idempotent.
    $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
    if (($userPath -split ";") -notcontains $InstallDir) {
        [Environment]::SetEnvironmentVariable("Path", "$userPath;$InstallDir", "User")
        Write-Host "Added $InstallDir to your user PATH (open a new terminal to pick it up)."
    }

    $installed = & (Join-Path $InstallDir "ventstream.exe") --version
    if ($installed -ne "ventstream $Version") { Fail "installed binary reports '$installed', expected 'ventstream $Version'" }

    Write-Host "ventstream $Version installed to $InstallDir"
    Write-Host "Example configuration: $examplePath"
    Write-Host "Note: the Kafka source is not available on Windows; all other sources and sinks are."
}
finally {
    Remove-Item -Recurse -Force $temporaryDirectory -ErrorAction SilentlyContinue
}
