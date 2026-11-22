[CmdletBinding()]
param([switch]$Live)
# Process/storage integration checks for the app-owned on-demand Windows relay.
$ErrorActionPreference = 'Stop'
$repo = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
Set-Location -LiteralPath $repo
$env:Path = (Join-Path $env:USERPROFILE '.cargo/bin') + ';' + $env:Path
$env:CARGO_ENCODED_RUSTFLAGS = '-L' + [char]31 + ('native=' + (Resolve-Path .build-tools/mpv).Path)
cargo build -p sync-server --offline
if ($LASTEXITCODE -ne 0) { throw 'Relay build failed' }
$artifacts = cargo test --all --offline --no-run --message-format=json
if ($LASTEXITCODE -ne 0) { throw 'Test build failed' }
$artifact = $artifacts | ForEach-Object { $_ | ConvertFrom-Json } |
    Where-Object { $_.reason -eq 'compiler-artifact' -and $_.profile.test -and $_.executable -and $_.target.name -eq 'app_lib' } |
    Select-Object -First 1
if (-not $artifact) { throw 'App test executable missing' }
$manifest = Join-Path $repo '.build-tools/local-relay-test.manifest'
@'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<assembly xmlns="urn:schemas-microsoft-com:asm.v1" manifestVersion="1.0">
  <dependency><dependentAssembly>
    <assemblyIdentity type="win32" name="Microsoft.Windows.Common-Controls"
      version="6.0.0.0" processorArchitecture="*"
      publicKeyToken="6595b64144ccf1df" language="*" />
  </dependentAssembly></dependency>
</assembly>
'@ | Set-Content -LiteralPath $manifest -Encoding utf8
$mt = Get-ChildItem "${env:ProgramFiles(x86)}/Windows Kits/10/bin/*/x64/mt.exe" |
    Sort-Object FullName -Descending | Select-Object -First 1
& $mt.FullName -nologo -manifest $manifest "-outputresource:$($artifact.executable);#1"
if ($LASTEXITCODE -ne 0) { throw 'Test manifest embedding failed' }
$filter = if ($Live) { 'listentogether::peer::tests::public_relay_discovery_approval_sync_and_restart' } else { 'listentogether::peer::tests::local_peer_approval_sync_and_restart' }
& $artifact.executable $filter --include-ignored --test-threads=1
exit $LASTEXITCODE
