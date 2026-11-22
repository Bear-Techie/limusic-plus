$ErrorActionPreference = 'Stop'
$repo = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$testRoot = Join-Path $repo ('.build-tools/launcher-test-' + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $testRoot | Out-Null
$fixture = Join-Path $testRoot 'launcher-fixture.exe'
$control = Join-Path $testRoot 'keep-running'
$started = Join-Path $testRoot 'app.pid'
$env:LIMUSIC_FIXTURE_CONTROL = $control
$env:LIMUSIC_FIXTURE_STARTED = $started
Add-Type -OutputAssembly $fixture -OutputType ConsoleApplication -TypeDefinition @'
using System;
using System.IO;
using System.Threading;
public class LauncherFixtureApp {
    public static void Main() {
        File.WriteAllText(Environment.GetEnvironmentVariable("LIMUSIC_FIXTURE_STARTED"), System.Diagnostics.Process.GetCurrentProcess().Id.ToString());
        while (File.Exists(Environment.GetEnvironmentVariable("LIMUSIC_FIXTURE_CONTROL"))) Thread.Sleep(100);
    }
}
'@
$probe = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
$probe.Start()
$port = $probe.LocalEndpoint.Port
$probe.Stop()
$data = Join-Path $testRoot 'data'
$supervisor = $null
$fixtureProcess = $null
$busy = $null

function Start-Supervisor {
    $arguments = '-NoProfile -ExecutionPolicy Bypass -File "' + (Join-Path $repo 'scripts/start-limusic.ps1') +
        '" -AppPath "' + $fixture + '" -RelayPath "' + (Join-Path $repo 'target/debug/limusic-sync.exe') +
        '" -DataDirectory "' + $data + '" -Port ' + $port + ' -NoDialogs -HostRelay'
    $process = Start-Process powershell.exe -ArgumentList $arguments -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $testRoot ('out-' + [Guid]::NewGuid() + '.log')) `
        -RedirectStandardError (Join-Path $testRoot ('err-' + [Guid]::NewGuid() + '.log'))
    $null = $process.Handle
    return $process
}
function Wait-Until([scriptblock]$Condition, [string]$Failure, [int]$Seconds = 20) {
    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    while (-not (& $Condition)) {
        if ([DateTime]::UtcNow -gt $deadline) { throw $Failure }
        Start-Sleep -Milliseconds 100
    }
}
function Test-Port {
    $socket = [Net.Sockets.TcpClient]::new()
    try { return ($socket.ConnectAsync('127.0.0.1', $port).Wait(200) -and $socket.Connected) }
    catch { return $false }
    finally { $socket.Dispose() }
}
function Send-Message($Socket, $Message) {
    $bytes = [Text.Encoding]::UTF8.GetBytes(($Message | ConvertTo-Json -Compress))
    $cancel = [Threading.CancellationTokenSource]::new(5000)
    try { $Socket.SendAsync([ArraySegment[byte]]::new($bytes), [Net.WebSockets.WebSocketMessageType]::Text, $true, $cancel.Token).GetAwaiter().GetResult() | Out-Null }
    finally { $cancel.Dispose() }
}
function Receive-Message($Socket) {
    $buffer = New-Object byte[] 65536
    $cancel = [Threading.CancellationTokenSource]::new(5000)
    try {
        $result = $Socket.ReceiveAsync([ArraySegment[byte]]::new($buffer), $cancel.Token).GetAwaiter().GetResult()
        if (-not $result.EndOfMessage) { throw 'Unexpected fragmented test message' }
        return ([Text.Encoding]::UTF8.GetString($buffer, 0, $result.Count) | ConvertFrom-Json)
    } finally { $cancel.Dispose() }
}
function Connect-RoomSocket {
    $socket = [Net.WebSockets.ClientWebSocket]::new()
    $cancel = [Threading.CancellationTokenSource]::new(5000)
    try { $socket.ConnectAsync([Uri]"ws://127.0.0.1:$port/ws", $cancel.Token).GetAwaiter().GetResult() | Out-Null }
    finally { $cancel.Dispose() }
    return $socket
}

try {
    Set-Content -LiteralPath $control 'running'
    $supervisor = Start-Supervisor
    Wait-Until { Test-Path -LiteralPath $started } 'App was not launched'
    $fixtureProcess = Get-Process -Id ([int](Get-Content -LiteralPath $started))
    if (-not (Test-Port)) { throw 'Relay not ready when app started' }
    $duplicate = Start-Supervisor
    if (-not $duplicate.WaitForExit(5000) -or $duplicate.ExitCode -ne 0) { throw 'Duplicate launcher did not exit cleanly' }
    if ($fixtureProcess.HasExited -or -not (Test-Port)) { throw 'Duplicate launch disturbed active processes' }
    Write-Output 'PASS hidden startup, readiness, duplicate launch and app lifetime'

    $socket = Connect-RoomSocket
    try {
        Send-Message $socket @{ type = 'create_room'; username = 'launcher-test' }
        $created = Receive-Message $socket
        if ($created.type -ne 'room_created') { throw 'Room creation failed' }
        Send-Message $socket @{ type = 'save_room' }
        $saved = Receive-Message $socket
        if ($saved.type -ne 'room_saved') { throw 'Room reservation failed' }
    } finally { $socket.Dispose() }

    Remove-Item -LiteralPath $control
    if (-not $supervisor.WaitForExit(15000) -or $supervisor.ExitCode -ne 0) { throw 'Supervisor did not exit after app Quit' }
    Wait-Until { -not (Test-Port) } 'Relay survived app exit'
    if (-not (Test-Path -LiteralPath (Join-Path $data 'saved-rooms.sqlite3'))) { throw 'Persistent relay DB missing' }
    Write-Output 'PASS app exit stops relay and retains database'

    Remove-Item -LiteralPath $started
    Set-Content -LiteralPath $control 'running'
    $supervisor = Start-Supervisor
    Wait-Until { Test-Path -LiteralPath $started } 'Second app launch failed'
    $fixtureProcess = Get-Process -Id ([int](Get-Content -LiteralPath $started))
    $socket = Connect-RoomSocket
    try {
        Send-Message $socket @{ type = 'open_saved_room'; room_code = $saved.room_code; owner_token = $saved.owner_token; username = 'launcher-test' }
        $reopened = Receive-Message $socket
        if ($reopened.type -ne 'room_created' -or $reopened.room_code -ne $saved.room_code -or -not $reopened.state.saved) { throw 'Saved identity lost across launcher restart' }
    } finally { $socket.Dispose() }
    Write-Output 'PASS saved room reopens after both processes restart'

    Remove-Item -LiteralPath $control
    if (-not $fixtureProcess.WaitForExit(5000)) { throw 'Fixture did not exit for restart' }
    Set-Content -LiteralPath $control 'running'
    $fixtureProcess = Start-Process -FilePath $fixture -WindowStyle Hidden -PassThru
    $null = $fixtureProcess.Handle
    Wait-Until { (Get-Content -LiteralPath (Join-Path $data 'launcher.log') -Raw).Contains("Following restarted Limusic PID $($fixtureProcess.Id).") } 'Supervisor did not follow app restart'
    if (-not (Test-Port)) { throw 'Relay stopped during app restart' }
    Write-Output 'PASS app Restart keeps the same relay running'

    $supervisor.Kill()
    $supervisor.WaitForExit()
    Wait-Until { -not (Test-Port) } 'Relay survived supervisor termination'
    if ($fixtureProcess.HasExited) { throw 'Relay job incorrectly killed app' }
    Remove-Item -LiteralPath $control
    $fixtureProcess.WaitForExit(5000) | Out-Null
    Write-Output 'PASS Windows job cleans relay after abrupt supervisor termination'

    Remove-Item -LiteralPath $started
    $busy = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, $port)
    $busy.Start()
    $supervisor = Start-Supervisor
    if (-not $supervisor.WaitForExit(10000) -or $supervisor.ExitCode -ne 1) { throw 'Occupied port was not refused' }
    if (Test-Path -LiteralPath $started) { throw 'App launched despite occupied port' }
    Write-Output 'PASS occupied port leaves existing listener alone and does not launch app'

    $busy.Stop()
    $busy = $null
    Set-Content -LiteralPath $control 'running'
    $fixtureProcess = Start-Process -FilePath $fixture -WindowStyle Hidden -PassThru
    $null = $fixtureProcess.Handle
    Wait-Until { Test-Path -LiteralPath $started } 'Existing app fixture did not start'
    $supervisor = Start-Supervisor
    Wait-Until {
        (Get-Content -LiteralPath (Join-Path $data 'launcher.log') -Raw).Contains("Watching Limusic PID $($fixtureProcess.Id).") -and (Test-Port)
    } 'Supervisor did not attach to existing app'
    $latestRelay = Select-String -LiteralPath (Join-Path $data 'launcher.log') -Pattern 'Started relay PID (\d+)' | Select-Object -Last 1
    $ownedRelay = Get-Process -Id ([int]$latestRelay.Matches[0].Groups[1].Value)
    $ownedRelay.Kill()
    if (-not $supervisor.WaitForExit(10000) -or $supervisor.ExitCode -ne 1) { throw 'Relay failure not reported' }
    if ($fixtureProcess.HasExited) { throw 'Relay failure killed the existing app' }
    Write-Output 'PASS existing app attachment and relay failure preserve the app process'
    Write-Output "Logs: $testRoot"
} finally {
    if ($busy) { $busy.Stop() }
    if (Test-Path -LiteralPath $control) { Remove-Item -LiteralPath $control }
    if ($supervisor -and -not $supervisor.HasExited) { $supervisor.Kill(); $supervisor.WaitForExit() }
    if ($fixtureProcess -and -not $fixtureProcess.HasExited) { $fixtureProcess.WaitForExit(5000) | Out-Null }
}
