[CmdletBinding()]
param(
    [switch]$SkipBuild,
    [AllowEmptyString()]
    [string]$ReleaseRepo = $(if ($env:LIMUSIC_PLUS_RELEASE_REPO) {
        $env:LIMUSIC_PLUS_RELEASE_REPO
    } else {
        'Bear-Techie/limusic-plus'
    })
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$repo = Split-Path $PSScriptRoot -Parent
Set-Location -LiteralPath $repo
$config = Get-Content src-tauri/tauri.conf.json -Raw | ConvertFrom-Json
$version = $config.version
$fileVersion = ($version -split '[-+]', 2)[0]
$output = Join-Path $repo 'dist'
$stage = Join-Path $repo ('.build-tools/windows-package-' + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Force -Path $output, $stage, "$stage/scripts" | Out-Null

$cargo = Get-Command cargo.exe -ErrorAction SilentlyContinue
if (-not $cargo) { $cargo = Get-Item (Join-Path $env:USERPROFILE '.cargo/bin/cargo.exe') }
$compiler = @(
    "$env:LOCALAPPDATA/Programs/Inno Setup 6/ISCC.exe",
    "${env:ProgramFiles(x86)}/Inno Setup 6/ISCC.exe",
    "$env:ProgramFiles/Inno Setup 6/ISCC.exe"
) | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
if (-not $compiler) { throw 'Install Inno Setup 6: winget install --id JRSoftware.InnoSetup --exact' }
$mpv = Join-Path $repo '.build-tools/mpv'
if (-not (Test-Path -LiteralPath "$mpv/mpv.lib")) { throw 'Prepare .build-tools/mpv/mpv.lib using docs/BUILD-PLATFORMS.md.' }

$oldConfig = $env:TAURI_CONFIG
$oldFlags = $env:CARGO_ENCODED_RUSTFLAGS
$oldPath = $env:Path
$oldCi = $env:CI
$oldBrand = $env:VITE_APP_NAME
$oldReleaseRepo = $env:LIMUSIC_PLUS_RELEASE_REPO
$buildStamp = Join-Path $repo '.build-tools/plus-release.json'
try {
    $env:CI = 'true'
    $env:VITE_APP_NAME = 'Limusic Plus'
    $env:LIMUSIC_PLUS_RELEASE_REPO = $ReleaseRepo
    $env:Path = "$env:ProgramFiles/nodejs;$repo/.build-tools/node_modules/.bin;" + $env:Path
    $env:TAURI_CONFIG = Get-Content packaging/windows/tauri-plus.json -Raw
    $env:CARGO_ENCODED_RUSTFLAGS = '-L' + [char]31 + "native=$mpv"
    if (-not $SkipBuild) {
        Push-Location ui
        try {
            & pnpm.cmd install --frozen-lockfile
            if ($LASTEXITCODE -ne 0) { throw 'UI dependency installation failed' }
            & pnpm.cmd check
            if ($LASTEXITCODE -ne 0) { throw 'UI checks failed' }
            & pnpm.cmd build
            if ($LASTEXITCODE -ne 0) { throw 'UI build failed' }
        } finally { Pop-Location }
        & $cargo build --release --locked -p limusic-app -p sync-server --features tauri/custom-protocol
        if ($LASTEXITCODE -ne 0) { throw 'Release build failed' }
        @{ mode = 'plus-release-v2'; version = $version; releaseRepo = $ReleaseRepo; appHash = (Get-FileHash target/release/limusic-app.exe -Algorithm SHA256).Hash } |
            ConvertTo-Json | Set-Content -LiteralPath $buildStamp -Encoding utf8
    } else {
        if (-not (Test-Path -LiteralPath $buildStamp)) { throw 'Run a full build before repackaging.' }
        $previous = Get-Content -LiteralPath $buildStamp -Raw | ConvertFrom-Json
        if ($previous.mode -ne 'plus-release-v2' -or $previous.version -ne $version -or
            $previous.releaseRepo -ne $ReleaseRepo -or
            $previous.appHash -ne (Get-FileHash target/release/limusic-app.exe -Algorithm SHA256).Hash) {
            throw 'Release binaries do not match this installer. Run a full build.'
        }
    }
    Copy-Item -LiteralPath target/release/limusic-app.exe -Destination "$stage/Limusic Plus.exe"
    Copy-Item -LiteralPath target/release/limusic-sync.exe -Destination $stage
    Copy-Item -LiteralPath src-tauri/libmpv-2.dll -Destination $stage
    Copy-Item -LiteralPath scripts/start-limusic.ps1 -Destination "$stage/scripts"
    Copy-Item -LiteralPath LICENSE -Destination $stage
    Copy-Item -LiteralPath packaging/windows/GETTING-STARTED.txt -Destination $stage
    Copy-Item -LiteralPath src-tauri/icons/icon.ico -Destination "$stage/app.ico"
    Copy-Item -LiteralPath src-tauri/icons/icon.png -Destination "$stage/limusic-icon.png"
    Add-Type -TypeDefinition (Get-Content packaging/windows/Launcher.cs -Raw) `
        -ReferencedAssemblies System.Windows.Forms, System `
        -OutputAssembly "$stage/LimusicPlus.exe" -OutputType WindowsApplication
    Add-Type -TypeDefinition (Get-Content packaging/windows/CompatibilityLauncher.cs -Raw) `
        -ReferencedAssemblies System.Windows.Forms, System `
        -OutputAssembly "$stage/limusic-app.exe" -OutputType WindowsApplication

    # App-local Microsoft CRT deployment: no development tools or separate CRT
    # installation is needed on a friend's PC.
    $vswhere = "${env:ProgramFiles(x86)}/Microsoft Visual Studio/Installer/vswhere.exe"
    $vsRoot = & $vswhere -latest -products '*' -property installationPath
    $crt = Get-ChildItem -LiteralPath "$vsRoot/VC/Redist/MSVC" -Directory |
        Sort-Object Name -Descending | ForEach-Object { Join-Path $_.FullName 'x64/Microsoft.VC143.CRT' } |
        Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
    if (-not $crt) { throw 'MSVC x64 redistributable DLL directory missing.' }
    Get-ChildItem -LiteralPath $crt -Filter '*.dll' | Copy-Item -Destination $stage

    $bootstrap = Join-Path $repo '.build-tools/MicrosoftEdgeWebview2Setup.exe'
    if (-not (Test-Path -LiteralPath $bootstrap)) {
        Invoke-WebRequest -UseBasicParsing 'https://go.microsoft.com/fwlink/p/?LinkId=2124703' -OutFile $bootstrap
    }
    $signature = Get-AuthenticodeSignature -LiteralPath $bootstrap
    if ($signature.Status -ne 'Valid' -or $signature.SignerCertificate.Subject -notmatch 'O=Microsoft Corporation') {
        throw 'WebView2 bootstrapper does not have a valid Microsoft signature.'
    }
    Copy-Item -LiteralPath $bootstrap -Destination "$stage/MicrosoftEdgeWebview2Setup.exe"

    # Package a private runtime. The installer only copies it when the task is selected.
    $nodeVersion = 'v24.22.3'
    $nodeHash = 'e612af99f626ab415ea225af6bce5c971feb448cb5cae250bb1c3fa50c5f7d9f'
    $nodeName = "node-$nodeVersion-win-x64"
    $nodeArchive = Join-Path $repo ".build-tools/$nodeName.zip"
    if (-not (Test-Path -LiteralPath $nodeArchive)) {
        & curl.exe --fail --silent --show-error --location --output "$nodeArchive.download" "https://nodejs.org/dist/$nodeVersion/$nodeName.zip"
        if ($LASTEXITCODE -ne 0) { throw 'Node.js runtime download failed.' }
        Move-Item -LiteralPath "$nodeArchive.download" -Destination $nodeArchive
    }
    if ((Get-FileHash -LiteralPath $nodeArchive -Algorithm SHA256).Hash -ne $nodeHash) {
        throw "Node.js runtime checksum mismatch. Remove $nodeArchive and rerun."
    }
    $nodeSource = Join-Path $repo ".build-tools/$nodeName"
    Expand-Archive -LiteralPath $nodeArchive -DestinationPath (Split-Path $nodeSource -Parent) -Force
    $voiceStage = Join-Path $stage 'discord-voice'
    New-Item -ItemType Directory -Path $voiceStage | Out-Null
    Copy-Item -LiteralPath "$nodeSource/node.exe" -Destination $voiceStage
    Copy-Item -LiteralPath "$nodeSource/LICENSE" -Destination "$voiceStage/NODE-LICENSE"
    Copy-Item -LiteralPath discord-voice/package.json,discord-voice/package-lock.json -Destination $voiceStage
    Push-Location -LiteralPath $voiceStage
    try {
        & "$nodeSource/node.exe" "$nodeSource/node_modules/npm/bin/npm-cli.js" ci --omit=dev --ignore-scripts --no-audit --no-fund
        if ($LASTEXITCODE -ne 0) { throw 'Discord voice dependencies could not be packaged.' }
        & ./node.exe --no-global-search-paths --input-type=module -e "import 'discord.js'; import 'opusscript'; import { generateDependencyReport } from '@discordjs/voice'; if (!/@snazzah\/davey: 0\./.test(generateDependencyReport())) process.exit(1);"
        if ($LASTEXITCODE -ne 0) { throw 'Packaged Discord voice dependencies could not be loaded.' }
    } finally { Pop-Location }

    # Include the modified application source, with no credentials, build output,
    # Git internals, dependencies, or user data from ignored directories.
    Add-Type -AssemblyName System.IO.Compression
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $sourcePath = Join-Path $output "Limusic-Plus-$version-source.zip"
    $files = @(& git -c "safe.directory=$($repo.Replace('\', '/'))" ls-files --cached)
    if ($LASTEXITCODE -ne 0) { throw 'Could not enumerate source files' }
    # Only known additions are included before they are committed. Never sweep
    # arbitrary untracked files (which may contain personal data) into a release.
    $files += @(
        'Start-Limusic.vbs', 'docs/SAVED-ROOMS.md', 'releases/plus.json',
        'scripts/start-limusic.ps1', 'scripts/tests/start-limusic.check.ps1',
        'scripts/tests/windows-installer.check.ps1',
        'docs/DISCORD-VOICE.md', 'discord-voice/bridge.mjs', 'discord-voice/catalog.mjs',
        'discord-voice/package.json', 'discord-voice/package-lock.json',
        'src-tauri/src/discord_voice.rs', 'src-tauri/src/discord_voice/worker.rs',
        'src-tauri/src/discord_voice/credentials.rs',
        'src-tauri/src/discord_voice/profile.rs',
        'src-tauri/src/discord_voice/pcm_capture.rs', 'crates/player/src/broadcast.rs',
        'ui/src/lib/components/DiscordVoice.svelte',
        'scripts/tests/local-relay.check.ps1', 'src-tauri/src/listentogether/local_relay.rs',
        'src-tauri/src/listentogether/peer.rs',
        'src-tauri/src/listentogether/recovery.rs',
        'src-tauri/src/listentogether/recovery_session.rs',
        'src-tauri/src/listentogether/bot_share.rs', 'ui/src/lib/discord-bots.svelte.ts',
        'ui/src/lib/components/RoomShareBot.svelte', 'ui/src/lib/components/RoomBotOffer.svelte',
        'ui/src/lib/components/DiscordQuickJoin.svelte', 'ui/src/lib/room-link.ts', 'ui/src/lib/room-link.check.ts',
        'scripts/tests/peer.check.ps1',
        'scripts/tests/recovery.check.ps1', 'scripts/tests/recovery-ui.check.mjs',
        'scripts/tests/RecoveryPreview.svelte',
        'scripts/tests/windows-app.check.ps1',
        'ui/src/lib/components/SavedRooms.svelte',
        'ui/src/lib/saved-rooms.ts', 'ui/src/lib/saved-rooms.check.ts',
        'ui/src/lib/components/OpenRoomsOnStartup.svelte',
        'ui/src/lib/components/RoomRecovery.svelte',
        'ui/src/lib/branding.svelte.ts',
        'packaging/windows/CompatibilityLauncher.cs',
        'scripts/build-windows-installer.ps1', 'packaging/windows/Launcher.cs',
        'packaging/windows/installer.iss', 'packaging/windows/tauri-plus.json',
        'packaging/windows/GETTING-STARTED.txt', 'ui/src/lib/plus-updater.check.mjs'
    )
    $files = $files | Sort-Object -Unique
    $sourceStream = [IO.File]::Open($sourcePath, [IO.FileMode]::Create, [IO.FileAccess]::Write)
    $zip = [IO.Compression.ZipArchive]::new($sourceStream, [IO.Compression.ZipArchiveMode]::Create)
    try {
        foreach ($file in $files) {
            if (Test-Path -LiteralPath $file -PathType Leaf) {
                [IO.Compression.ZipFileExtensions]::CreateEntryFromFile($zip, (Join-Path $repo $file), $file.Replace('\', '/')) | Out-Null
            }
        }
    } finally { $zip.Dispose() }
    Copy-Item -LiteralPath $sourcePath -Destination "$stage/limusic-plus-source.zip"

    & $compiler "/DStageDir=$stage" "/DOutputDir=$output" "/DAppVersion=$version" "/DAppFileVersion=$fileVersion" packaging/windows/installer.iss
    if ($LASTEXITCODE -ne 0) { throw 'Installer compilation failed' }
    $installer = Join-Path $output "Limusic-Plus-$version-setup-x64.exe"
    $hash = (Get-FileHash -LiteralPath $installer -Algorithm SHA256).Hash
    Set-Content -LiteralPath "$installer.sha256" -Value "$hash  $([IO.Path]::GetFileName($installer))" -Encoding ascii
    Write-Output "Installer: $installer"
    Write-Output "Staging files: $stage"
} finally {
    $env:TAURI_CONFIG = $oldConfig
    $env:CARGO_ENCODED_RUSTFLAGS = $oldFlags
    $env:Path = $oldPath
    $env:CI = $oldCi
    $env:VITE_APP_NAME = $oldBrand
    $env:LIMUSIC_PLUS_RELEASE_REPO = $oldReleaseRepo
}
