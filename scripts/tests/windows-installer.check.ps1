[CmdletBinding()]
param([string]$Installer)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$repo = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
if (-not $Installer) {
    $Installer = (Get-ChildItem -LiteralPath (Join-Path $repo 'dist') -Filter '*-setup-x64.exe' |
        Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName
}
$Installer = (Resolve-Path -LiteralPath $Installer).Path
$testRoot = Join-Path $repo ('.build-tools/installer-check-' + [Guid]::NewGuid().ToString('N'))
$installDir = Join-Path $testRoot 'Installed Limusic Plus'
$desktopLink = Join-Path ([Environment]::GetFolderPath('DesktopDirectory')) 'Limusic Plus.lnk'
$menuLink = Join-Path ([Environment]::GetFolderPath('Programs')) 'Limusic Plus.lnk'
$uninstallKey = 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall\{E19D7FF6-353B-4D23-9207-019E267D6F35}_is1'
if ((Test-Path -LiteralPath $desktopLink) -or (Test-Path -LiteralPath $menuLink) -or (Test-Path -LiteralPath $uninstallKey)) {
    throw 'An existing Plus install/shortcut was found. Refusing to replace it for a test.'
}
New-Item -ItemType Directory -Path $testRoot | Out-Null
$installed = $false
$relay = $null
$oldPort = $env:PORT
$oldDb = $env:LIMUSIC_ROOMS_DB
$oldPath = $env:Path
$sentinels = @()
try {
    $setup = Start-Process -FilePath $Installer -WindowStyle Hidden -PassThru -Wait -ArgumentList `
        "/VERYSILENT /SUPPRESSMSGBOXES /NORESTART /TASKS=desktopicon /DIR=`"$installDir`" /LOG=`"$testRoot/install.log`""
    if ($setup.ExitCode -ne 0) { throw "Installer failed with exit code $($setup.ExitCode)" }
    $installed = $true
    $voice = Join-Path $installDir 'discord-voice'
    if (Test-Path -LiteralPath $voice) { throw 'Discord runtime was installed without selecting its task' }
    $setup = Start-Process -FilePath $Installer -WindowStyle Hidden -PassThru -Wait -ArgumentList `
        "/VERYSILENT /SUPPRESSMSGBOXES /NORESTART /TASKS=desktopicon,discordvoice /DIR=`"$installDir`" /LOG=`"$testRoot/install-discord.log`""
    if ($setup.ExitCode -ne 0) { throw "Installer with Discord failed with exit code $($setup.ExitCode)" }
    if (-not (Test-Path -LiteralPath "$voice/NODE-LICENSE")) { throw 'Bundled Node license is missing' }
    $env:Path = "$env:SystemRoot\System32;$env:SystemRoot"
    Push-Location -LiteralPath $voice
    try {
        & ./node.exe --no-global-search-paths --input-type=module -e "import 'discord.js'; import 'opusscript'; import { generateDependencyReport } from '@discordjs/voice'; if (!/@snazzah\/davey: 0\./.test(generateDependencyReport())) process.exit(1);"
        if ($LASTEXITCODE -ne 0) { throw 'Installed Discord dependencies failed to load' }
    } finally {
        Pop-Location
        $env:Path = $oldPath
    }
    Write-Output 'PASS Discord dependencies install only when selected and run with no system Node on PATH'
    $setup = Start-Process -FilePath $Installer -WindowStyle Hidden -PassThru -Wait -ArgumentList `
        "/VERYSILENT /SUPPRESSMSGBOXES /NORESTART /TASKS=desktopicon /DIR=`"$installDir`" /LOG=`"$testRoot/remove-discord.log`""
    if ($setup.ExitCode -ne 0 -or (Test-Path -LiteralPath $voice)) { throw 'Deselecting Discord did not remove its runtime' }
    foreach ($file in @('LimusicPlus.exe', 'Limusic Plus.exe', 'limusic-app.exe', 'limusic-sync.exe', 'libmpv-2.dll',
        'vcruntime140.dll', 'vcruntime140_1.dll', 'msvcp140.dll', 'LICENSE', 'limusic-icon.png', 'limusic-plus-source.zip', 'scripts/start-limusic.ps1')) {
        if (-not (Test-Path -LiteralPath (Join-Path $installDir $file))) { throw "Missing installed file: $file" }
    }
    $shell = New-Object -ComObject WScript.Shell
    foreach ($link in @($desktopLink, $menuLink)) {
        if (-not (Test-Path -LiteralPath $link)) { throw "Shortcut missing: $link" }
        $shortcut = $shell.CreateShortcut($link)
        if ($shortcut.TargetPath -ne (Join-Path $installDir 'LimusicPlus.exe')) { throw 'Shortcut bypasses launcher' }
        if ($shortcut.WorkingDirectory -ne $installDir) { throw 'Shortcut working directory is incorrect' }
    }
    $check = Start-Process -FilePath (Join-Path $installDir 'LimusicPlus.exe') -ArgumentList '--check' -WindowStyle Hidden -PassThru -Wait
    if ($check.ExitCode -ne 0) { throw 'Installed native launcher self-check failed' }
    Write-Output 'PASS per-user install, bundled dependencies and both .lnk targets'

    # Test the installed optimized relay without a development toolchain on PATH.
    $probe = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
    $probe.Start()
    $env:PORT = [string]$probe.LocalEndpoint.Port
    $probe.Stop()
    $env:LIMUSIC_ROOMS_DB = Join-Path $testRoot 'relay-data.sqlite3'
    $env:Path = "$env:SystemRoot\System32;$env:SystemRoot"
    $relay = Start-Process -FilePath (Join-Path $installDir 'limusic-sync.exe') -WorkingDirectory $installDir `
        -WindowStyle Hidden -PassThru -RedirectStandardOutput "$testRoot/relay.log" -RedirectStandardError "$testRoot/relay.err"
    $deadline = [DateTime]::UtcNow.AddSeconds(10)
    while (-not (Test-Path -LiteralPath $env:LIMUSIC_ROOMS_DB)) {
        if ($relay.HasExited -or [DateTime]::UtcNow -gt $deadline) { throw 'Installed release relay failed to start' }
        Start-Sleep -Milliseconds 100
    }
    $relay.Kill()
    $relay.WaitForExit()
    $relay.Dispose()
    $relay = $null
    Write-Output 'PASS installed relay starts with only Windows system directories on PATH'

    foreach ($directory in @((Join-Path $env:LOCALAPPDATA 'LimusicRelay'), (Join-Path $env:APPDATA 'com.limusic.plus'))) {
        New-Item -ItemType Directory -Force -Path $directory | Out-Null
        $sentinel = Join-Path $directory ('.installer-test-' + [Guid]::NewGuid().ToString('N'))
        Set-Content -LiteralPath $sentinel -Value 'preserve user data'
        $sentinels += $sentinel
    }
} finally {
    $env:PORT = $oldPort
    $env:LIMUSIC_ROOMS_DB = $oldDb
    $env:Path = $oldPath
    if ($relay) { if (-not $relay.HasExited) { $relay.Kill(); $relay.WaitForExit() }; $relay.Dispose() }
    if ($installed) {
        $uninstall = Start-Process -FilePath (Join-Path $installDir 'unins000.exe') -WindowStyle Hidden -PassThru -Wait `
            -ArgumentList "/VERYSILENT /SUPPRESSMSGBOXES /NORESTART /LOG=`"$testRoot/uninstall.log`""
        if ($uninstall.ExitCode -ne 0) { throw 'Test uninstall failed' }
        $deadline = [DateTime]::UtcNow.AddSeconds(10)
        while ((Test-Path -LiteralPath $desktopLink) -or (Test-Path -LiteralPath $menuLink)) {
            if ([DateTime]::UtcNow -gt $deadline) { throw 'Uninstall left shortcuts behind' }
            Start-Sleep -Milliseconds 100
        }
        foreach ($sentinel in $sentinels) {
            if (-not (Test-Path -LiteralPath $sentinel)) { throw 'Uninstall removed user data' }
            Remove-Item -LiteralPath $sentinel
        }
        Write-Output 'PASS uninstall removes shortcuts and preserves app/relay data folders'
    }
}
Write-Output "Installer test logs: $testRoot"
