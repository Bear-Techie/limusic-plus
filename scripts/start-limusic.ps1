# Opens just Limusic by default; the app starts its relay when hosting is requested.
# -HostRelay retains the explicit eager-relay supervisor for advanced/manual use.
[CmdletBinding()]
param(
    [string]$AppPath,
    [string]$RelayPath,
    [ValidateRange(1, 65535)][int]$Port = 8080,
    [string]$DataDirectory = (Join-Path $env:LOCALAPPDATA 'LimusicRelay'),
    [switch]$HostRelay,
    [switch]$NoDialogs
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path $PSScriptRoot -Parent
$relay = $null
$app = $null
$job = $null
$mutex = $null
$ownsMutex = $false
$exitCode = 0
$logPath = $null
$errorMessage = $null

function Write-LauncherLog([string]$Message) {
    if ($logPath) {
        Add-Content -LiteralPath $logPath -Value ('{0:u} {1}' -f (Get-Date), $Message)
    }
}

function Find-AppProcess([string]$Executable) {
    $name = [IO.Path]::GetFileNameWithoutExtension($Executable)
    foreach ($candidate in @(Get-Process -Name $name -ErrorAction SilentlyContinue)) {
        try {
            if ($candidate.HasExited) { continue }
            $actualPath = $candidate.MainModule.FileName
        } catch {
            throw "Cannot inspect an existing $name process. Quit it before using this launcher."
        }
        if (-not [string]::Equals($actualPath, $Executable, [StringComparison]::OrdinalIgnoreCase)) {
            # The packaged Plus build has a separate Tauri application identity
            # and can coexist with the upstream app. Development builds retain
            # the original identity, so a different build must still be refused.
            if (Test-Path -LiteralPath (Join-Path $repoRoot 'LimusicPlus.exe')) { continue }
            throw "Another version of $name is already running. Quit it from its tray icon first."
        }
        return $candidate
    }
}

function Test-RelayReady {
    $client = New-Object Net.Sockets.TcpClient
    try {
        $pending = $client.ConnectAsync('127.0.0.1', $Port)
        return ($pending.Wait(200) -and $client.Connected)
    } catch { return $false }
    finally { $client.Dispose() }
}

try {
    # One supervisor per desktop session. Repeated double-clicks cannot start a
    # second relay or mistake a short-lived single-instance handoff for app exit.
    $mutex = New-Object Threading.Mutex($false, 'Local\LimusicRelayLauncher')
    try { $ownsMutex = $mutex.WaitOne(0) }
    catch [Threading.AbandonedMutexException] { $ownsMutex = $true }
    if (-not $ownsMutex) { exit 0 }

    if (-not $AppPath -and -not $RelayPath) {
        # Installed packages put both binaries beside the launcher.
        if ((Test-Path -LiteralPath "$repoRoot\Limusic Plus.exe") -and
            (Test-Path -LiteralPath "$repoRoot\limusic-sync.exe")) {
            $AppPath = "$repoRoot\Limusic Plus.exe"
            $RelayPath = "$repoRoot\limusic-sync.exe"
        }
        if (-not $AppPath -and (Test-Path -LiteralPath "$repoRoot\limusic-app.exe") -and
            (Test-Path -LiteralPath "$repoRoot\limusic-sync.exe")) {
            $AppPath = "$repoRoot\limusic-app.exe"
            $RelayPath = "$repoRoot\limusic-sync.exe"
        }
        foreach ($buildProfile in @('debug', 'release')) {
            if ($AppPath) { break }
            $directory = Join-Path $repoRoot "target\$buildProfile"
            if ((Test-Path -LiteralPath "$directory\limusic-app.exe") -and
                (Test-Path -LiteralPath "$directory\limusic-sync.exe")) {
                $AppPath = "$directory\limusic-app.exe"
                $RelayPath = "$directory\limusic-sync.exe"
                break
            }
        }
    }
    if (-not $AppPath -or -not $RelayPath) {
        throw 'App and relay executables are required. Build both first, or supply -AppPath and -RelayPath. See docs/SAVED-ROOMS.md.'
    }
    $AppPath = (Resolve-Path -LiteralPath $AppPath).Path
    $RelayPath = (Resolve-Path -LiteralPath $RelayPath).Path
    $DataDirectory = [IO.Path]::GetFullPath($DataDirectory)
    New-Item -ItemType Directory -Force -Path $DataDirectory | Out-Null
    $logPath = Join-Path $DataDirectory 'launcher.log'
    $app = Find-AppProcess $AppPath

    if (-not $HostRelay) {
        # Invoking the app again restores its window through Tauri's existing
        # single-instance handler. No relay or resident supervisor is started.
        Start-Process -FilePath $AppPath -WorkingDirectory (Split-Path $AppPath) -WindowStyle Hidden | Out-Null
        Write-LauncherLog 'Opened Limusic without starting a relay.'
        exit 0
    }

    $listeners = [Net.NetworkInformation.IPGlobalProperties]::GetIPGlobalProperties().GetActiveTcpListeners()
    if (@($listeners | Where-Object { $_.Port -eq $Port }).Count) {
        throw "Port $Port is already in use. Stop your manually started relay, or use -Port with another port. No existing process was stopped."
    }

    # Windows closes the job handle even if this supervisor is terminated. The
    # job contains only our relay, so cleanup cannot stop someone else's server.
    Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
public sealed class LimusicRelayJob : IDisposable {
    [StructLayout(LayoutKind.Sequential)] struct Basic {
        public long ProcessTime, JobTime;
        public uint Flags;
        public UIntPtr MinWorkingSet, MaxWorkingSet;
        public uint ActiveProcesses;
        public UIntPtr Affinity;
        public uint Priority, Scheduling;
    }
    [StructLayout(LayoutKind.Sequential)] struct Counters {
        public ulong ReadOps, WriteOps, OtherOps, ReadBytes, WriteBytes, OtherBytes;
    }
    [StructLayout(LayoutKind.Sequential)] struct Extended {
        public Basic Basic;
        public Counters Counters;
        public UIntPtr ProcessMemory, JobMemory, PeakProcessMemory, PeakJobMemory;
    }
    [DllImport("kernel32.dll", SetLastError=true)] static extern IntPtr CreateJobObject(IntPtr attributes, string name);
    [DllImport("kernel32.dll", SetLastError=true)] static extern bool SetInformationJobObject(IntPtr job, int type, ref Extended info, uint size);
    [DllImport("kernel32.dll", SetLastError=true)] static extern bool AssignProcessToJobObject(IntPtr job, IntPtr process);
    [DllImport("kernel32.dll")] static extern bool CloseHandle(IntPtr handle);
    IntPtr handle;
    public LimusicRelayJob() {
        handle = CreateJobObject(IntPtr.Zero, null);
        if (handle == IntPtr.Zero) throw new Win32Exception();
        var info = new Extended();
        info.Basic.Flags = 0x2000; // JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        if (!SetInformationJobObject(handle, 9, ref info, (uint)Marshal.SizeOf(typeof(Extended)))) {
            int error = Marshal.GetLastWin32Error();
            Dispose();
            throw new Win32Exception(error);
        }
    }
    public void Attach(IntPtr process) {
        if (!AssignProcessToJobObject(handle, process)) throw new Win32Exception();
    }
    public void Dispose() {
        if (handle != IntPtr.Zero) { CloseHandle(handle); handle = IntPtr.Zero; }
    }
}
'@
    $job = New-Object LimusicRelayJob
    $env:PORT = [string]$Port
    $env:LIMUSIC_ROOMS_DB = Join-Path $DataDirectory 'saved-rooms.sqlite3'
    $relay = Start-Process -FilePath $RelayPath -WorkingDirectory $DataDirectory -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $DataDirectory 'relay.stdout.log') `
        -RedirectStandardError (Join-Path $DataDirectory 'relay.stderr.log')
    $job.Attach($relay.Handle)
    Write-LauncherLog "Started relay PID $($relay.Id), port $Port; database $env:LIMUSIC_ROOMS_DB"

    $deadline = [DateTime]::UtcNow.AddSeconds(15)
    while (-not (Test-RelayReady)) {
        if ($relay.HasExited) { throw 'Relay exited at startup. Check relay.stderr.log.' }
        if ([DateTime]::UtcNow -gt $deadline) { throw 'Relay did not become ready within 15 seconds.' }
        Start-Sleep -Milliseconds 150
    }
    if (-not $app) {
        $app = Start-Process -FilePath $AppPath -WorkingDirectory (Split-Path $AppPath) -WindowStyle Hidden -PassThru
    }
    Write-LauncherLog "Watching Limusic PID $($app.Id). Closing to tray keeps relay running."

    while ($true) {
        if ($relay.HasExited) { throw 'Relay stopped unexpectedly. Check relay.stderr.log.' }
        if (-not $app.WaitForExit(1000)) { continue }
        # The tray's Restart action replaces the app process. Give its successor
        # a short window to appear before treating this as a real Quit.
        $replacement = $null
        $deadline = [DateTime]::UtcNow.AddSeconds(5)
        do {
            $replacement = Find-AppProcess $AppPath
            if ($replacement) { break }
            Start-Sleep -Milliseconds 250
        } while ([DateTime]::UtcNow -lt $deadline)
        if (-not $replacement) { break }
        $app.Dispose()
        $app = $replacement
        Write-LauncherLog "Following restarted Limusic PID $($app.Id)."
    }
    Write-LauncherLog 'Limusic exited. Stopping owned relay.'
} catch {
    $exitCode = 1
    $errorMessage = $_.Exception.Message
    try { Write-LauncherLog "ERROR: $errorMessage" } catch {}
} finally {
    if ($job) { $job.Dispose() }
    if ($relay) {
        # Also covers failure between Start-Process and job assignment.
        try { if (-not $relay.HasExited) { $relay.Kill(); $relay.WaitForExit(3000) | Out-Null } } catch {}
        $relay.Dispose()
    }
    if ($app) { $app.Dispose() }
    if ($ownsMutex) { $mutex.ReleaseMutex() }
    if ($mutex) { $mutex.Dispose() }
}
if ($errorMessage) {
    if ($NoDialogs) { [Console]::Error.WriteLine($errorMessage) }
    else {
        try { (New-Object -ComObject WScript.Shell).Popup($errorMessage, 0, 'Limusic launcher', 16) | Out-Null }
        catch { [Console]::Error.WriteLine($errorMessage) }
    }
}
exit $exitCode
