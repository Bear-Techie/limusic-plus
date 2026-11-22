param([string]$Stage)
$ErrorActionPreference = 'Stop'
Add-Type @'
using System;
using System.Runtime.InteropServices;
public class PlusWindowCheck {
    [DllImport("user32.dll")] public static extern bool IsWindowVisible(IntPtr window);
}
'@
$app = $null
$relay = $null
function Wait-Check([scriptblock]$Check, [string]$Message, [int]$Seconds = 30) {
    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    while (-not (& $Check)) {
        if ([DateTime]::UtcNow -gt $deadline) { throw $Message }
        Start-Sleep -Milliseconds 200
    }
}
try {
    Start-Process -FilePath explorer.exe -ArgumentList ('"' + $Stage + '\LimusicPlus.exe"') -WindowStyle Hidden | Out-Null
    Wait-Check {
        $script:app = Get-Process -Name 'Limusic Plus' -ErrorAction SilentlyContinue | Where-Object { $_.Path -eq "$Stage\Limusic Plus.exe" } | Select-Object -First 1
        # Tauri also owns a visible-but-transparent single-instance helper HWND.
        # Wait for the actual main UI before sending Close or checking Restore.
        $script:app -and $script:app.MainWindowTitle -eq 'Limusic Plus' -and $script:app.MainWindowHandle -ne [IntPtr]::Zero
    } 'Packaged app did not show its main window'
    $window = $app.MainWindowHandle
    $relay = Get-Process -Name limusic-sync -ErrorAction SilentlyContinue | Where-Object { $_.Path -eq "$Stage\limusic-sync.exe" } | Select-Object -First 1
    if ($relay) { throw 'Normal startup unexpectedly started a relay' }
    Write-Output 'PASS native launcher opens the packaged app without a relay'
    $app.CloseMainWindow() | Out-Null
    Wait-Check { -not [PlusWindowCheck]::IsWindowVisible($window) } 'App did not close to tray'
    if ($app.HasExited) { throw 'Closing to tray stopped app or relay' }
    Write-Output 'PASS closing the packaged app window keeps it in the system tray'
    Start-Process -FilePath explorer.exe -ArgumentList ('"' + $Stage + '\LimusicPlus.exe"') -WindowStyle Hidden | Out-Null
    Wait-Check { [PlusWindowCheck]::IsWindowVisible($window) } 'Second shortcut launch did not restore app'
    Write-Output 'PASS second native launch restores the existing tray window'
} finally {
    if ($app -and -not $app.HasExited) { $app.Kill(); $app.WaitForExit() }
    if ($relay) {
        Wait-Check { $relay.HasExited } 'Owned relay survived app exit' 12
        Write-Output 'PASS packaged supervisor stops relay after app process exits'
    }
}
