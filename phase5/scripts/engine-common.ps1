# Shared helpers for the Phase 5 delivery-engine verification scripts
# (Windows PowerShell 5.1).
#
# Every scenario script dot-sources this file, calls Initialize-EngineRun,
# performs its steps with Assert-True, and lets the finally block kill the
# started processes and write the evidence summary.
#
# Environment (Docker Desktop):
#   PostgreSQL relayx-phase2-pg     127.0.0.1:5432  (must already run)
#   Redis      relayx-phase4-redis  127.0.0.1:6379  (started on demand)
#   Kafka      relayx-phase5-kafka  127.0.0.1:9092  (started on demand)

$ErrorActionPreference = 'Stop'

# Invoke-Native runs a native command and returns its output and exit code.
# Windows PowerShell turns stderr output of a native command into an error
# record, which aborts the script while $ErrorActionPreference is Stop. Docker
# and the Kafka tools print warnings on stderr, so the preference is lowered for
# the duration of the call.
function Invoke-Native {
    param([string]$Exe, [string[]]$Arguments)

    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $output = (& $Exe @Arguments 2>&1 | Out-String)
        return [pscustomobject]@{ Output = $output; ExitCode = $LASTEXITCODE }
    }
    finally {
        $ErrorActionPreference = $previous
    }
}

$script:Phase5Root = Split-Path -Parent $PSScriptRoot
$script:RedisContainer = 'relayx-phase4-redis'
$script:PgContainer = 'relayx-phase2-pg'
$script:KafkaContainer = 'relayx-phase5-kafka'
$script:KafkaImage = 'apache/kafka:3.9.0'
$script:KafkaClusterID = 'MkU3OEVBNTcwNTJENDM2Qk'
$script:KafkaPort = 9092
$script:FanoutTopic = 'relayx.fanout.v1'
$script:DeliverTopic = 'relayx.deliver.v1'
$script:Dsn = 'postgres://relayx:relayx@127.0.0.1:5432/relayx?sslmode=disable'
$script:MessageServiceUrl = 'http://127.0.0.1:8081'
$script:EvidenceDir = $null
$script:Scenario = ''
$script:Steps = New-Object System.Collections.Generic.List[string]
$script:Params = New-Object System.Collections.Generic.List[string]
$script:StartedProcs = New-Object System.Collections.Generic.List[object]
$script:RunStart = Get-Date

function Initialize-EngineRun {
    param([string]$Scenario)

    if (-not (Test-Path (Join-Path $script:Phase5Root 'go.mod'))) {
        throw "phase5/go.mod not found under $($script:Phase5Root)"
    }
    $script:Scenario = $Scenario
    $script:EvidenceDir = Join-Path $script:Phase5Root "docs/bench/raw/engine/$Scenario"
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
    $result = Invoke-Native -Exe 'docker' -Arguments @('version', '--format', '{{.Server.Version}}')
    if ($result.ExitCode -ne 0) {
        throw 'docker is not available; Docker Desktop must be running'
    }
}

function Assert-PostgresRunning {
    $running = (Invoke-Native -Exe 'docker' -Arguments @('inspect', '-f', '{{.State.Running}}', $script:PgContainer)).Output.Trim()
    if ($running -ne 'true') {
        throw "PostgreSQL container $($script:PgContainer) is not running; run 'make db-up' in phase2/ first"
    }
}

function Wait-RedisPong {
    param([int]$TimeoutSeconds = 30)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $result = Invoke-Native -Exe 'docker' -Arguments @('exec', $script:RedisContainer, 'redis-cli', 'ping')
        if ($result.ExitCode -eq 0 -and $result.Output -match 'PONG') {
            return
        }
        Start-Sleep -Milliseconds 250
    }
    throw "Redis container $($script:RedisContainer) did not answer PONG within ${TimeoutSeconds}s"
}

function Ensure-RedisRunning {
    $names = (Invoke-Native -Exe 'docker' -Arguments @('ps', '-a', '--filter', "name=^$($script:RedisContainer)$", '--format', '{{.Names}}')).Output.Trim()
    if ($names -match [regex]::Escape($script:RedisContainer)) {
        $running = (Invoke-Native -Exe 'docker' -Arguments @('inspect', '-f', '{{.State.Running}}', $script:RedisContainer)).Output.Trim()
        if ($running -ne 'true') {
            $start = Invoke-Native -Exe 'docker' -Arguments @('start', $script:RedisContainer)
            if ($start.ExitCode -ne 0) {
                throw "docker start $($script:RedisContainer) failed: $($start.Output.Trim())"
            }
        }
    }
    else {
        $run = Invoke-Native -Exe 'docker' -Arguments @('run', '-d', '--name', $script:RedisContainer, '-p', '6379:6379', 'redis:7-alpine')
        if ($run.ExitCode -ne 0) {
            throw "docker run $($script:RedisContainer) failed: $($run.Output.Trim())"
        }
    }
    Wait-RedisPong 30
}

function Wait-KafkaReady {
    param([int]$TimeoutSeconds = 90)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $result = Invoke-Native -Exe 'docker' -Arguments @(
            'exec', $script:KafkaContainer, '/opt/kafka/bin/kafka-topics.sh',
            '--bootstrap-server', "127.0.0.1:$($script:KafkaPort)", '--list'
        )
        if ($result.ExitCode -eq 0) {
            return
        }
        Start-Sleep -Milliseconds 500
    }
    throw "Kafka container $($script:KafkaContainer) did not answer --list within ${TimeoutSeconds}s"
}

function Ensure-KafkaRunning {
    $names = (Invoke-Native -Exe 'docker' -Arguments @('ps', '-a', '--filter', "name=^$($script:KafkaContainer)$", '--format', '{{.Names}}')).Output.Trim()
    if ($names -match [regex]::Escape($script:KafkaContainer)) {
        $running = (Invoke-Native -Exe 'docker' -Arguments @('inspect', '-f', '{{.State.Running}}', $script:KafkaContainer)).Output.Trim()
        if ($running -ne 'true') {
            $start = Invoke-Native -Exe 'docker' -Arguments @('start', $script:KafkaContainer)
            if ($start.ExitCode -ne 0) {
                throw "docker start $($script:KafkaContainer) failed: $($start.Output.Trim())"
            }
        }
    }
    else {
        # Same single-node KRaft parameters as the phase5 Makefile kafka-up target.
        $argsList = @(
            'run', '-d', '--name', $script:KafkaContainer,
            '-p', "$($script:KafkaPort):9092",
            '-e', "CLUSTER_ID=$($script:KafkaClusterID)",
            '-e', 'KAFKA_NODE_ID=1',
            '-e', 'KAFKA_PROCESS_ROLES=broker,controller',
            '-e', 'KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093',
            '-e', "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:$($script:KafkaPort)",
            '-e', 'KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER',
            '-e', 'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
            '-e', 'KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093',
            '-e', 'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1',
            '-e', 'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1',
            '-e', 'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1',
            '-e', 'KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0',
            $script:KafkaImage
        )
        $run = Invoke-Native -Exe 'docker' -Arguments $argsList
        if ($run.ExitCode -ne 0) {
            $logs = (Invoke-Native -Exe 'docker' -Arguments @('logs', $script:KafkaContainer)).Output
            throw "docker run $($script:KafkaContainer) failed: $logs"
        }
    }
    Wait-KafkaReady 90
}

function Ensure-KafkaTopics {
    foreach ($topic in @($script:FanoutTopic, $script:DeliverTopic)) {
        $result = Invoke-Native -Exe 'docker' -Arguments @(
            'exec', $script:KafkaContainer, '/opt/kafka/bin/kafka-topics.sh',
            '--bootstrap-server', "127.0.0.1:$($script:KafkaPort)",
            '--create', '--if-not-exists', '--topic', $topic, '--partitions', '3', '--replication-factor', '1'
        )
        if ($result.ExitCode -ne 0) {
            throw "kafka-topics --create $topic failed: $($result.Output.Trim())"
        }
    }
}

# Reset-KafkaTopics removes both engine topics so every run starts from an empty
# log. Reusing a topic would replay old fan-out events whose messages were
# truncated from PostgreSQL, which fails the delivery foreign key.
function Reset-KafkaTopics {
    foreach ($topic in @($script:FanoutTopic, $script:DeliverTopic)) {
        $null = Invoke-Native -Exe 'docker' -Arguments @(
            'exec', $script:KafkaContainer, '/opt/kafka/bin/kafka-topics.sh',
            '--bootstrap-server', "127.0.0.1:$($script:KafkaPort)",
            '--delete', '--if-exists', '--topic', $topic
        )
    }
    $deadline = (Get-Date).AddSeconds(60)
    while ((Get-Date) -lt $deadline) {
        $listing = (Invoke-Native -Exe 'docker' -Arguments @(
                'exec', $script:KafkaContainer, '/opt/kafka/bin/kafka-topics.sh',
                '--bootstrap-server', "127.0.0.1:$($script:KafkaPort)", '--list'
            )).Output
        $remaining = @()
        foreach ($topic in @($script:FanoutTopic, $script:DeliverTopic)) {
            if ($listing -match [regex]::Escape($topic)) {
                $remaining += $topic
            }
        }
        if ($remaining.Count -eq 0) {
            break
        }
        Start-Sleep -Milliseconds 500
    }
    Ensure-KafkaTopics
}

# Stop-KafkaContainer stops the broker so the acceptance path can be observed
# while the event pipeline is unavailable.
function Stop-KafkaContainer {
    $stop = Invoke-Native -Exe 'docker' -Arguments @('stop', $script:KafkaContainer)
    if ($stop.ExitCode -ne 0) {
        throw "docker stop $($script:KafkaContainer) failed: $($stop.Output.Trim())"
    }
}

# Start-KafkaContainer restarts the broker and waits until it answers again.
# The topics live in the container filesystem, so a restart keeps the backlog.
function Start-KafkaContainer {
    $start = Invoke-Native -Exe 'docker' -Arguments @('start', $script:KafkaContainer)
    if ($start.ExitCode -ne 0) {
        throw "docker start $($script:KafkaContainer) failed: $($start.Output.Trim())"
    }
    Wait-KafkaReady 120
    Ensure-KafkaTopics
}

# --- database --------------------------------------------------------------

function Invoke-Sql {
    param([string]$Sql)

    $result = Invoke-Native -Exe 'docker' -Arguments @('exec', $script:PgContainer, 'psql', '-U', 'relayx', '-d', 'relayx', '-tAc', $Sql)
    if ($result.ExitCode -ne 0) {
        throw "psql failed: $($result.Output.Trim())"
    }
    return $result.Output.Trim()
}

function Wait-SqlValue {
    param(
        [string]$Sql,
        [string]$Expected,
        [int]$TimeoutSeconds = 10,
        [string]$Description
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $last = ''
    while ((Get-Date) -lt $deadline) {
        $last = Invoke-Sql $Sql
        if ($last -eq $Expected) {
            return $last
        }
        Start-Sleep -Milliseconds 200
    }
    throw "$Description (last value '$last', expected '$Expected')"
}

function Wait-SqlAtLeast {
    param(
        [string]$Sql,
        [int]$Minimum,
        [int]$TimeoutSeconds = 10,
        [string]$Description
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $last = -1
    while ((Get-Date) -lt $deadline) {
        $raw = Invoke-Sql $Sql
        $parsed = 0
        if ([int]::TryParse($raw, [ref]$parsed)) {
            $last = $parsed
            if ($last -ge $Minimum) {
                return $last
            }
        }
        Start-Sleep -Milliseconds 200
    }
    throw "$Description (last count $last, expected at least $Minimum)"
}

function Wait-SqlTrue {
    param(
        [string]$Sql,
        [int]$TimeoutSeconds = 10,
        [string]$Description
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $last = Invoke-Sql $Sql
        if ($last -eq 't') {
            return
        }
        Start-Sleep -Milliseconds 200
    }
    throw "$Description (predicate did not become true within ${TimeoutSeconds}s)"
}

# Wait-SqlUntil polls one scalar query until the predicate accepts the value.
# It is the generic form used by the fault scenarios where the expected value
# depends on what the engine decided to do.
function Wait-SqlUntil {
    param(
        [string]$Sql,
        [scriptblock]$Predicate,
        [int]$TimeoutSeconds = 30,
        [string]$Description
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $last = ''
    while ((Get-Date) -lt $deadline) {
        $last = Invoke-Sql $Sql
        if (& $Predicate $last) {
            return $last
        }
        Start-Sleep -Milliseconds 200
    }
    throw "$Description (last value '$last')"
}

function Clear-EngineDatabase {
    Invoke-Sql 'TRUNCATE deliveries, messages, endpoints, outbox RESTART IDENTITY' | Out-Null
    Invoke-Sql "DELETE FROM tenants WHERE id <> 'default'" | Out-Null
}

# --- build and ports -------------------------------------------------------

function Build-Binaries {
    $env:GOPROXY = 'https://goproxy.cn,direct'
    $bin = Join-Path $script:Phase5Root 'bin'
    if (-not (Test-Path $bin)) {
        New-Item -ItemType Directory -Force -Path $bin | Out-Null
    }
    Push-Location $script:Phase5Root
    try {
        foreach ($cmd in @('gateway', 'client', 'message-service', 'delivery-worker')) {
            $build = Invoke-Native -Exe 'go' -Arguments @('build', '-o', "bin/$cmd.exe", "./cmd/$cmd")
            if ($build.ExitCode -ne 0) {
                throw "go build ./cmd/$cmd failed: $($build.Output.Trim())"
            }
        }
    }
    finally {
        Pop-Location
    }
    foreach ($cmd in @('gateway', 'client', 'message-service', 'delivery-worker')) {
        if (-not (Test-Path (Join-Path $bin "$cmd.exe"))) {
            throw "bin/$cmd.exe was not produced"
        }
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

function Wait-HttpOK {
    param([string]$Url, [int]$TimeoutSeconds = 20)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $lastError = ''
    while ((Get-Date) -lt $deadline) {
        try {
            $resp = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 3
            if ([int]$resp.StatusCode -eq 200) {
                return
            }
            $lastError = "status $($resp.StatusCode)"
        }
        catch {
            $lastError = $_.Exception.Message
        }
        Start-Sleep -Milliseconds 200
    }
    throw "$Url did not return 200 within ${TimeoutSeconds}s (last error: $lastError)"
}

# --- processes -------------------------------------------------------------

function Start-EngineProcess {
    param(
        [string]$Name,
        [string]$Exe,
        [string[]]$Arguments
    )

    $stdout = Join-Path $script:EvidenceDir "$Name.out.log"
    $stderr = Join-Path $script:EvidenceDir "$Name.err.log"
    $proc = Start-Process -FilePath $Exe -ArgumentList $Arguments -PassThru -WindowStyle Hidden `
        -RedirectStandardOutput $stdout -RedirectStandardError $stderr
    $entry = [pscustomobject]@{
        Name    = $Name
        Process = $proc
        Stdout  = $stdout
        Stderr  = $stderr
        Args    = ($Arguments -join ' ')
    }
    $script:StartedProcs.Add($entry)
    return $entry
}

function Start-GatewayProcess {
    param(
        [string]$Name,
        [string]$Addr,
        [string]$NodeID = 'node-a',
        [string]$NodeAddr = '127.0.0.1:9100',
        [string]$NodeAdvertiseAddr = '',
        [string]$RedisAddr = '127.0.0.1:6379',
        [string]$KafkaBrokers = '127.0.0.1:9092',
        [string]$Dsn = $script:Dsn
    )

    $argsList = @(
        '-addr', $Addr,
        '-node-id', $NodeID,
        '-node-addr', $NodeAddr,
        '-redis-addr', $RedisAddr,
        '-kafka-brokers', $KafkaBrokers,
        '-stats-interval', '1s',
        '-dsn', $Dsn
    )
    if ($NodeAdvertiseAddr) {
        $argsList += @('-node-advertise-addr', $NodeAdvertiseAddr)
    }
    return Start-EngineProcess -Name $Name -Exe (Join-Path $script:Phase5Root 'bin\gateway.exe') -Arguments $argsList
}

function Start-WorkerProcess {
    param(
        [string]$Name,
        [string]$WorkerID,
        [string]$LeaseTTL = '5s',
        [string]$ScanInterval = '200ms',
        [string]$KafkaBrokers = '127.0.0.1:9092',
        [string]$RedisAddr = '127.0.0.1:6379',
        [string]$TenantCacheTTL = '30s',
        [string]$ClaimBatch = '100',
        [string]$DefaultInflight = '256',
        [string]$NodeDialTimeout = '1s',
        [string]$Dsn = $script:Dsn
    )

    $argsList = @(
        '-dsn', $Dsn,
        '-kafka-brokers', $KafkaBrokers,
        '-redis-addr', $RedisAddr,
        '-node-id', $WorkerID,
        '-worker-shards', '1',
        '-worker-shard-index', '0',
        '-claim-batch', $ClaimBatch,
        '-lease-ttl', $LeaseTTL,
        '-scan-interval', $ScanInterval,
        '-tenant-cache-ttl', $TenantCacheTTL,
        '-default-inflight', $DefaultInflight,
        '-node-dial-timeout', $NodeDialTimeout,
        '-stats-interval', '1s',
        '-outbox-interval', '200ms',
        '-outbox-batch', '100',
        '-retry-backoff-base', '500ms',
        '-retry-backoff-max', '5s'
    )
    return Start-EngineProcess -Name $Name -Exe (Join-Path $script:Phase5Root 'bin\delivery-worker.exe') -Arguments $argsList
}

function Start-MessageServiceProcess {
    param(
        [string]$Name,
        [string]$HttpAddr = '127.0.0.1:8081',
        [string]$Dsn = $script:Dsn
    )

    $argsList = @(
        '-http-addr', $HttpAddr,
        '-dsn', $Dsn
    )
    return Start-EngineProcess -Name $Name -Exe (Join-Path $script:Phase5Root 'bin\message-service.exe') -Arguments $argsList
}

function Start-ClientProcess {
    param([string]$Name, [string[]]$ClientArgs)

    return Start-EngineProcess -Name $Name -Exe (Join-Path $script:Phase5Root 'bin\client.exe') -Arguments $ClientArgs
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

function Get-EntryLogText {
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

function Wait-LogMatch {
    param(
        $Entry,
        [string]$Pattern,
        [int]$TimeoutSeconds = 10,
        [string]$Description
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $text = Get-EntryLogText $Entry
        if ($text -match $Pattern) {
            return $text
        }
        Start-Sleep -Milliseconds 100
    }
    throw "$Description (pattern '$Pattern' not found in $($Entry.Name) within ${TimeoutSeconds}s)"
}

function Wait-LogCount {
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
        $count = Count-LogMatches (Get-EntryLogText $Entry) $Pattern
        if ($count -ge $Minimum) {
            return $count
        }
        Start-Sleep -Milliseconds 100
    }
    throw "$Description (found $count matches for '$Pattern', expected at least $Minimum)"
}

# Get-LogMaxCounter returns the largest value captured by a counter in a log
# line, e.g. 'throttled=(\d+)'. It is used to read the worker statistics that
# are emitted as slog key/value pairs.
function Get-LogMaxCounter {
    param($Entry, [string]$Pattern)

    $text = Get-EntryLogText $Entry
    $max = 0
    foreach ($m in [regex]::Matches($text, $Pattern)) {
        $value = 0
        if ([int]::TryParse($m.Groups[1].Value, [ref]$value)) {
            if ($value -gt $max) {
                $max = $value
            }
        }
    }
    return $max
}

# Wait-LogMaxCounter waits until a counter in the log reaches the minimum. The
# worker writes its statistics once per interval, so a counter that was just
# incremented is only visible after the next statistics line.
function Wait-LogMaxCounter {
    param(
        $Entry,
        [string]$Pattern,
        [int]$Minimum,
        [int]$TimeoutSeconds = 10,
        [string]$Description
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $max = 0
    while ((Get-Date) -lt $deadline) {
        $max = Get-LogMaxCounter $Entry $Pattern
        if ($max -ge $Minimum) {
            return $max
        }
        Start-Sleep -Milliseconds 200
    }
    throw "$Description (counter '$Pattern' reached $max, expected at least $Minimum)"
}

function Wait-ProcessExit {
    param($Entry, [int]$TimeoutSeconds = 10, [string]$Description)

    if (-not $Entry.Process.WaitForExit($TimeoutSeconds * 1000)) {
        throw "$Description (process $($Entry.Name) is still running after ${TimeoutSeconds}s)"
    }
    $Entry.Process.WaitForExit()
    $code = $Entry.Process.ExitCode
    if ($null -eq $code) {
        return $null
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

# --- HTTP acceptance -------------------------------------------------------

function Invoke-AcceptMessage {
    param(
        [string]$TenantID,
        [string]$FromUser,
        [string]$FromDevice = '',
        [string]$Target,
        [string]$TargetType = 'user',
        [string]$Payload = 'hello-phase5',
        [long]$ClientMsgID,
        [int]$TTLSeconds = 300,
        [string]$ServiceUrl = $script:MessageServiceUrl
    )

    $body = @{
        tenant_id     = $TenantID
        from_user     = $FromUser
        from_device   = $FromDevice
        target_type   = $TargetType
        target        = $Target
        payload       = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($Payload))
        ttl_seconds   = $TTLSeconds
        client_msg_id = $ClientMsgID
    } | ConvertTo-Json -Compress

    $headers = @{}
    if ($TenantID) {
        $headers['X-Tenant-ID'] = $TenantID
    }
    $resp = Invoke-WebRequest -Uri "$ServiceUrl/v1/messages" -Method Post `
        -ContentType 'application/json' -Headers $headers -Body $body -UseBasicParsing -TimeoutSec 10
    $json = $resp.Content | ConvertFrom-Json
    return [pscustomobject]@{
        StatusCode = [int]$resp.StatusCode
        MessageID  = [uint64]$json.message_id
        Duplicate  = [bool]$json.duplicate
        TenantID   = $json.tenant_id
    }
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
    $lines.Add("kafka_container: $($script:KafkaContainer)")
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
