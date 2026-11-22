[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Installer,
    [Parameter(Mandatory)][string]$Tauri,
    [string]$ReleaseRepo = 'Bear-Techie/limusic-plus'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$file = Get-Item -LiteralPath $Installer
if ($file.Name -cnotmatch '^Limusic-Plus-(\d+\.\d+\.\d+)-setup-x64\.exe$') {
    throw 'Expected a stable Limusic Plus Windows installer.'
}
$version = $Matches[1]
if ($ReleaseRepo -notmatch '\A[\w.-]+/[\w.-]+\z') { throw 'Invalid GitHub repository.' }
& $Tauri signer sign $file.FullName | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Installer signing failed.' }
$signature = (Get-Content -LiteralPath "$($file.FullName).sig" -Raw).Trim()
if (-not $signature) { throw 'The installer signature is missing.' }
$manifest = @{
    version = $version
    platforms = @{
        'windows-x86_64' = @{
            signature = $signature
            url = "https://github.com/$ReleaseRepo/releases/download/v$version/$($file.Name)"
        }
    }
}
$manifestPath = $file.FullName + '.update.json'
$manifest | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $manifestPath -Encoding ascii
Write-Output $manifestPath
