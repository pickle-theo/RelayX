# Shared helpers for the Phase 4 cluster scripts (Windows PowerShell 5.1).
#
# Every scenario script dot-sources this file, calls Initialize-ClusterRun,
# performs its steps with Assert-True, and lets the finally block kill the
# gateway/client processes and write the evidence summary.
#
# Environment (Docker Desktop):
#   PostgreSQL relayx-phase2-pg  127.0.0.1:5432  (must already run)
#   Redis      relayx-phase4-redis 127.0.0.1:6379 (started on demand)

$ErrorActionPreference = 'Stop'

$script:Phase4Root = Split-Path -Parent $PSScriptRoot
$script:RedisContainer = 'relayx-phase4-redis'
$script:PgContainer = 'relayx-phase2-pg'
$script:Dsn = 'postgres://relayx:relayx@127.0.0.1:5432/relayx?sslmode=disable'
$script:EvidenceDir = $null
$script:Scenario = ''
$script:Steps = New-Object System.Collections.Generic.List[string]
$script:Params = New-Object System.Collections.Generic.List[string]
$script:StartedProcs = New-Object System.Collections.Generic.List[object]
$script:RunStart = Get-Date

function Initialize-ClusterRun {
    param([string]$Scenario)

    if (-not (Test-Path (Join-Path $script:Phase4Root 'go.mod'))) {
        throw "phase4/go.mod not found under $($script:Phase4Root)"
    }
    $script:Scenario = $Scenario
    $script:EvidenceDir = Join-Path $script:Phase4Root "docs/bench/raw/cluster/$Scenario"
    if (Test-Path $script:EvidenceDir) {
        Remove-Item -Recurse -Force $script:EvidenceDir
    }
    New-Item -ItemType Directory -Force -Path $script:EvidenceDir | Out-Null
}

function Add-Param {
    param([string]$Line)
    $script:Params.Add($Line)
}

function Write-Step-Pass {
    param([string]$Message)
    $line = "PASS: $Message"
    Write-Host $line
    $script:Steps.Add($line)
}

function Write-Step-Info {
    param([string]$Message)
    $line = "INFO: $Message"
    Write-Host $line
    $script:Steps.Add($line)
}

function Fail-Step {
    param([string]$Message)
    $line = "FAIL: $Message"
    Write-Host $line
    $script:Steps.Add($line)
    throw $line
}

function Assert-True {
    param([bool]$Condition, [string]$Message)
    if (-not $Condition) {
        Fail-Step $Message
    }
    Write-Step-Pass $Message
}

function Get-Elapsed {
    param([datetime]$Since)
    return [math]::Round(((Get-Date) - $Since).TotalSeconds, 2)
}

# --- environment -----------------------------------------------------------

function Test-DockerAvailable {
    $null = & docker version --format '{{.Server.Version}}' 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw 'docker is not available; Docker Desktop must be running'
    }
}

function Assert-PostgresRunning {
    $running = (& docker inspect -f '{{.State.Running}}' $script:PgContainer 2>$null | Out-String).Trim()
    if ($running -ne 'true') {
        throw "PostgreSQL container $($script:PgContainer) is not running; run 'make db-up' in phase4/ first"
    }
}

function Wait-RedisPong {
    param([int]$TimeoutSeconds = 30)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $out = (& docker exec $script:RedisContainer redis-cli ping 2>$null | Out-String)
        if ($LASTEXITCODE -eq 0 -and $out -match 'PONG') {
            return
        }
        Start-Sleep -Milliseconds 250
    }
    throw "Redis container $($script:RedisContainer) did not answer PONG within ${TimeoutSeconds}s"
}

function Ensure-RedisRunning {
    $names = (& docker ps -a --filter "name=^$($script:RedisContainer)$" --format '{{.Names}}' 2>$null | Out-String).Trim()
    if ($names -match [regex]::Escape($script:RedisContainer)) {
        $running = (& docker inspect -f '{{.State.Running}}' $script:RedisContainer 2>$null | Out-String).Trim()
        if ($running -ne 'true') {
            & docker start $script:RedisContainer | Out-Null
            if ($LASTEXITCODE -ne 0) {
                throw "docker start $($script:RedisContainer) failed"
            }
        }
    }
    else {
        & docker run -d --name $script:RedisContainer -p 6379:6379 redis:7-alpine | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "docker run $($script:RedisContainer) failed"
        }
    }
    Wait-RedisPong 30
}

function Start-RedisContainer {
    & docker start $script:RedisContainer | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "docker start $($script:RedisContainer) failed"
    }
    Wait-RedisPong 30
}

function Stop-RedisContainer {
    & docker stop -t 1 $script:RedisContainer | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "docker stop $($script:RedisContainer) failed"
    }
    $running = (& docker inspect -f '{{.State.Running}}' $script:RedisContainer 2>$null | Out-String).Trim()
    if ($running -ne 'false') {
        throw "Redis container $($script:RedisContainer) is still running after docker stop"
    }
}

# --- build and ports -------------------------------------------------------

function Build-Binaries {
    $env:GOPROXY = 'https://goproxy.cn,direct'
    $bin = Join-Path $script:Phase4Root 'bin'
    if (-not (Test-Path $bin)) {
        New-Item -ItemType Directory -Force -Path $bin | Out-Null
    }
    Push-Location $script:Phase4Root
    try {
        & go build -o 'bin/gateway.exe' ./cmd/gateway
        if ($LASTEXITCODE -ne 0) {
            throw 'go build ./cmd/gateway failed'
        }
        & go build -o 'bin/client.exe' ./cmd/client
        if ($LASTEXITCODE -ne 0) {
            throw 'go build ./cmd/client failed'
        }
    }
    finally {
        Pop-Location
    }
    if (-not (Test-Path (Join-Path $bin 'gateway.exe'))) {
        throw 'bin/gateway.exe was not produced'
    }
    if (-not (Test-Path (Join-Path $bin 'client.exe'))) {
        throw 'bin/client.exe was not produced'
    }
}

function Test-TcpPort {
    param([string]$HostName, [int]$Port, [int]$TimeoutMs = 200)

    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $iar = $client.BeginConnect($HostName, $Port, $null, $null)
        if (-not $iar.AsyncWaitHandle.WaitOne($TimeoutMs, $false)) {
            return $false
        }
        $client.EndConnect($iar)
        return $true
    }
    catch {
        return $false
    }
    finally {
        $client.Close()
    }
}

function Assert-PortsFree {
    param([int[]]$Ports)

    $busy = @()
    foreach ($p in $Ports) {
        if (Test-TcpPort '127.0.0.1' $p) {
            $busy += $p
        }
    }
    if ($busy.Count -gt 0) {
        throw "TCP ports already in use: $($busy -join ', '); stop the process holding them before running this scenario"
    }
}

function Wait-TcpOpen {
    param([string[]]$Endpoints, [int]$TimeoutSeconds = 15)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $pending = New-Object System.Collections.ArrayList
    foreach ($ep in $Endpoints) {
        [void]$pending.Add($ep)
    }
    while ((Get-Date) -lt $deadline) {
        foreach ($ep in @($pending)) {
            $idx = $ep.LastIndexOf(':')
            $hostName = $ep.Substring(0, $idx)
            $port = [int]$ep.Substring($idx + 1)
            if (Test-TcpPort $hostName $port 300) {
                [void]$pending.Remove($ep)
            }
        }
        if ($pending.Count -eq 0) {
            return
        }
        Start-Sleep -Milliseconds 200
    }
    throw "timed out after ${TimeoutSeconds}s waiting for: $($pending -join ', ')"
}

# --- processes -------------------------------------------------------------

function Start-GatewayProcess {
    param(
        [string]$Name,
        [string]$NodeID,
        [string]$Addr,
        [string]$NodeAddr,
        [string]$RedisAddr = '127.0.0.1:6379'
    )

    $stdout = Join-Path $script:EvidenceDir "$Name.out.log"
    $stderr = Join-Path $script:EvidenceDir "$Name.err.log"
    $argsList = @(
        '-addr', $Addr,
        '-node-id', $NodeID,
        '-node-addr', $NodeAddr,
        '-redis-addr', $RedisAddr,
        '-node-heartbeat-interval', '500ms',
        '-node-ttl', '1500ms',
        '-node-cleanup-interval', '500ms',
        '-stats-interval', '2s',
        '-dsn', $script:Dsn
    )
    $proc = Start-Process -FilePath (Join-Path $script:Phase4Root 'bin\gateway.exe') `
        -ArgumentList $argsList -PassThru -WindowStyle Hidden `
        -RedirectStandardOutput $stdout -RedirectStandardError $stderr
    $entry = [pscustomobject]@{
        Name    = $Name
        Process = $proc
        Stdout  = $stdout
        Stderr  = $stderr
        Args    = ($argsList -join ' ')
    }
    $script:StartedProcs.Add($entry)
    return $entry
}

function Start-ClientProcess {
    param([string]$Name, [string[]]$ClientArgs)

    # Start-Process -RedirectStandardOutput returns a Process whose ExitCode is
    # null on PowerShell 5.1, so the client runs through a small .cmd wrapper
    # that redirects the streams itself and forwards %ERRORLEVEL%.
    $exe = Join-Path $script:Phase4Root 'bin\client.exe'
    $stdout = Join-Path $script:EvidenceDir "$Name.out.log"
    $stderr = Join-Path $script:EvidenceDir "$Name.err.log"
    $cmdFile = Join-Path $script:EvidenceDir "$Name.cmd"
    $lines = @(
        '@echo off',
        ('"' + $exe + '" ' + ($ClientArgs -join ' ') + ' > "' + $stdout + '" 2> "' + $stderr + '"'),
        'exit /b %ERRORLEVEL%'
    )
    [System.IO.File]::WriteAllLines($cmdFile, $lines, (New-Object System.Text.ASCIIEncoding))

    $proc = Start-Process -FilePath $env:ComSpec -ArgumentList @('/c', "`"$cmdFile`"") `
        -PassThru -WindowStyle Hidden
    $entry = [pscustomobject]@{
        Name    = $Name
        Process = $proc
        Stdout  = $stdout
        Stderr  = $stderr
        Args    = ($ClientArgs -join ' ')
    }
    $script:StartedProcs.Add($entry)
    return $entry
}

function Read-LogText {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        return ''
    }
    try {
        $fs = [System.IO.File]::Open($Path, [System.IO.FileMode]::Open,
            [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
    }
    catch {
        return ''
    }
    $sr = New-Object System.IO.StreamReader($fs)
    try {
        return $sr.ReadToEnd()
    }
    finally {
        $sr.Dispose()
        $fs.Dispose()
    }
}

function Get-ClientLogText {
    param($Entry)

    $out = Read-LogText $Entry.Stdout
    $err = Read-LogText $Entry.Stderr
    if ($out -and $err) {
        return $out + "`n" + $err
    }
    if ($out) {
        return $out
    }
    return $err
}

function Count-LogMatches {
    param([string]$Text, [string]$Pattern)
    return ([regex]::Matches($Text, $Pattern)).Count
}

function Wait-ClientLogMatch {
    param(
        $Entry,
        [string]$Pattern,
        [int]$TimeoutSeconds = 10,
        [string]$Description
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $text = Get-ClientLogText $Entry
        if ($text -match $Pattern) {
            return $text
        }
        Start-Sleep -Milliseconds 100
    }
    throw "$Description (pattern '$Pattern' not found in $($Entry.Name) within ${TimeoutSeconds}s)"
}

function Wait-ClientLogCount {
    param(
        $Entry,
        [string]$Pattern,
        [int]$Minimum,
        [int]$TimeoutSeconds = 10,
        [string]$Description
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $count = 0
    while ((Get-Date) -lt $deadline) {
        $count = Count-LogMatches (Get-ClientLogText $Entry) $Pattern
        if ($count -ge $Minimum) {
            return $count
        }
        Start-Sleep -Milliseconds 100
    }
    throw "$Description (found $count matches for '$Pattern', expected at least $Minimum)"
}

function Wait-ProcessExit {
    param($Entry, [int]$TimeoutSeconds = 10, [string]$Description)

    if (-not $Entry.Process.WaitForExit($TimeoutSeconds * 1000)) {
        throw "$Description (process $($Entry.Name) is still running after ${TimeoutSeconds}s)"
    }
    $Entry.Process.WaitForExit()
    $code = $Entry.Process.ExitCode
    if ($null -eq $code) {
        throw "$Description (process $($Entry.Name) exit code is unavailable)"
    }
    return [int]$code
}

function Stop-ProcessEntry {
    param($Entry)

    if ($null -eq $Entry) {
        return
    }
    try {
        $Entry.Process.Refresh()
        if (-not $Entry.Process.HasExited) {
            # /T also kills the client started by the .cmd wrapper.
            & taskkill /PID $Entry.Process.Id /T /F 2>$null | Out-Null
        }
    }
    catch {
        # The process may have exited between Refresh and taskkill.
    }
}

function Stop-AllStartedProcesses {
    foreach ($entry in $script:StartedProcs) {
        Stop-ProcessEntry $entry
    }
    foreach ($entry in $script:StartedProcs) {
        try {
            [void]$entry.Process.WaitForExit(3000)
        }
        catch {
        }
    }
}

# --- Redis assertions ------------------------------------------------------

function Get-RedisRouteNodes {
    param([string]$UserID)

    $lua = "local ks=redis.call('KEYS','relayx:route:$UserID*'); local out={}; for i,k in ipairs(ks) do out[#out+1]=redis.call('HGET',k,'node') end; return out"
    $raw = (& docker exec $script:RedisContainer redis-cli --raw EVAL $lua 0 2>$null | Out-String)
    if ($LASTEXITCODE -ne 0) {
        throw "redis-cli EVAL failed while reading the route of user $UserID"
    }
    return @($raw -split "`r?`n" | Where-Object { $_ -ne '' } | ForEach-Object { $_.Trim() })
}

function Get-RedisExists {
    param([string]$Key)

    $out = (& docker exec $script:RedisContainer redis-cli EXISTS $Key 2>$null | Out-String).Trim()
    if ($LASTEXITCODE -ne 0) {
        throw "redis-cli EXISTS $Key failed"
    }
    return [int]$out
}

function Wait-RedisKeyGone {
    param([string]$Key, [int]$TimeoutSeconds = 10)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if ((Get-RedisExists $Key) -eq 0) {
            return
        }
        Start-Sleep -Milliseconds 200
    }
    throw "Redis key $Key still exists after ${TimeoutSeconds}s"
}

# --- evidence --------------------------------------------------------------

function Write-Summary {
    param([int]$ExitCode)

    if (-not $script:EvidenceDir -or -not (Test-Path $script:EvidenceDir)) {
        Write-Host 'summary: evidence directory is unavailable'
        return
    }

    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add("scenario: $($script:Scenario)")
    $lines.Add("started_at: $($script:RunStart.ToString('yyyy-MM-ddTHH:mm:sszzz'))")
    $lines.Add("finished_at: $((Get-Date).ToString('yyyy-MM-ddTHH:mm:sszzz'))")
    $lines.Add("duration_s: $([math]::Round(((Get-Date) - $script:RunStart).TotalSeconds, 2))")
    $lines.Add("host: $($env:COMPUTERNAME)  os: $([System.Environment]::OSVersion.VersionString)")
    $lines.Add("go: $(((& go version) -join ' ').Trim())")
    $lines.Add("postgres_container: $($script:PgContainer)")
    $lines.Add("redis_container: $($script:RedisContainer)")
    foreach ($p in $script:Params) {
        $lines.Add("param: $p")
    }
    foreach ($entry in $script:StartedProcs) {
        $lines.Add("process: $($entry.Name) args: $($entry.Args)")
    }
    $lines.Add('')
    foreach ($s in $script:Steps) {
        $lines.Add($s)
    }
    $lines.Add('')
    $lines.Add("exit_code: $ExitCode")
    $files = Get-ChildItem -File -Path $script:EvidenceDir | Where-Object { $_.Name -ne 'summary.txt' } | Select-Object -ExpandProperty Name
    $lines.Add("evidence_files: $($files -join ', ')")

    $path = Join-Path $script:EvidenceDir 'summary.txt'
    [System.IO.File]::WriteAllLines($path, $lines, (New-Object System.Text.UTF8Encoding($false)))
    Write-Host "evidence: $($script:EvidenceDir)"
}
