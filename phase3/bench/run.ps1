# RelayX Phase 3 bench wrapper (Windows host).
#
# Runs bench/run.sh inside a disposable golang:1.27 Linux container with the
# module root bind mounted at /app. PostgreSQL is reached through
# host.docker.internal, which Docker Desktop maps to the host gateway.
#
# The Go module/build cache lives in the named volume relayx-phase3-gocache
# mounted at /go, so repeated runs do not re-download dependencies.
#
# The loadtest exit code is returned unchanged.

[CmdletBinding()]
param(
    [int]$Conns = 1000,
    [string]$Duration = "60s",
    [string]$Interval = "1s",
    [string]$Ramp = "5s",
    [string]$Timeout = "5s",
    [string]$Scenario = "steady-low",
    [int]$Payload = 64,
    [string]$Mode = "auto",
    [string]$Ack = "both",
    [double]$SlowRatio = 0.1,
    [string]$ReconnectAfter = "10s",
    [string]$ReconnectWindow = "5s",
    [ValidateSet(0, 1)]
    [int]$Profile = 0,
    [string]$Out = ""
)

$ErrorActionPreference = "Stop"

# Module root: the parent of the directory holding this script (phase3/).
$root = Split-Path -Parent $PSScriptRoot
if (-not (Test-Path (Join-Path $root "go.mod"))) {
    Write-Error "go.mod not found under $root; expected to run from phase3/bench"
    exit 2
}

if ([string]::IsNullOrWhiteSpace($Out)) {
    $stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
    $Out = "docs/bench/raw/bench-$Scenario-$stamp.txt"
}

# run.sh resolves OUT relative to /app. Keep absolute host paths inside the
# module root usable by converting them to a relative container path.
if ([System.IO.Path]::IsPathRooted($Out)) {
    $rootFull = [System.IO.Path]::GetFullPath($root)
    $outFull = [System.IO.Path]::GetFullPath($Out)
    if (-not $outFull.StartsWith($rootFull, [System.StringComparison]::OrdinalIgnoreCase)) {
        Write-Error "-Out must be inside $root (got $Out)"
        exit 2
    }
    $Out = $outFull.Substring($rootFull.Length).TrimStart('\', '/') -replace '\\', '/'
}

$rawDir = Join-Path $root "docs/bench/raw"
$profileDir = Join-Path $rawDir "profiles"
New-Item -ItemType Directory -Force -Path $rawDir, $profileDir | Out-Null

$docker = Get-Command docker -ErrorAction SilentlyContinue
if (-not $docker) {
    Write-Error "docker not found in PATH"
    exit 2
}

# Report header assembled on the host: container environment details are added
# by run.sh, host details can only be collected here.
$headerName = ".bench-header-$([guid]::NewGuid().ToString('N')).txt"
$headerHostPath = Join-Path $rawDir $headerName
$headerContainerPath = "docs/bench/raw/$headerName"

$dockerClient = ""
$dockerServer = ""
try { $dockerClient = (& docker version --format '{{.Client.Version}}' 2>$null | Out-String).Trim() } catch { }
try { $dockerServer = (& docker version --format '{{.Server.Version}}' 2>$null | Out-String).Trim() } catch { }

$osCaption = "unknown"
$osVersion = ""
$cpuName = "unknown"
$cpuCores = "unknown"
$memGb = "unknown"
try {
    $os = Get-CimInstance Win32_OperatingSystem
    $osCaption = $os.Caption
    $osVersion = $os.Version
    $memGb = [math]::Round($os.TotalVisibleMemorySize / 1MB, 1)
} catch { }
try {
    $cpu = Get-CimInstance Win32_Processor | Select-Object -First 1
    $cpuName = $cpu.Name.Trim()
    $cpuCores = "$($cpu.NumberOfCores) (logical $($cpu.NumberOfLogicalProcessors))"
} catch { }

$startedAt = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")

$header = @(
    "== bench host ==",
    "started_at: $startedAt",
    "docker_client_version: $dockerClient",
    "docker_server_version: $dockerServer",
    "host_os: $osCaption $osVersion",
    "host_cpu: $cpuName",
    "host_cpu_cores: $cpuCores",
    "host_memory_gb: $memGb",
    "repo_root: $root",
    "runner: bench/run.ps1 -> golang:1.27 -> bash bench/run.sh",
    "out: $Out"
) -join "`n"

[System.IO.File]::WriteAllText($headerHostPath, $header + "`n", (New-Object System.Text.UTF8Encoding($false)))

$dockerArgs = @(
    "run", "--rm",
    "--ulimit", "nofile=1048576:1048576",
    "--add-host", "host.docker.internal:host-gateway",
    "-v", "${root}:/app",
    "-v", "relayx-phase3-gocache:/go",
    "-w", "/app",
    "-e", "GOPATH=/go",
    "-e", "GOCACHE=/go/.cache",
    "-e", "GOPROXY=https://goproxy.cn,direct",
    "-e", "CONNS=$Conns",
    "-e", "DURATION=$Duration",
    "-e", "INTERVAL=$Interval",
    "-e", "RAMP=$Ramp",
    "-e", "TIMEOUT=$Timeout",
    "-e", "SCENARIO=$Scenario",
    "-e", "PAYLOAD=$Payload",
    "-e", "MODE=$Mode",
    "-e", "ACK=$Ack",
    "-e", "SLOW_RATIO=$SlowRatio",
    "-e", "RECONNECT_AFTER=$ReconnectAfter",
    "-e", "RECONNECT_WINDOW=$ReconnectWindow",
    "-e", "PROFILE=$Profile",
    "-e", "OUT=$Out",
    "-e", "BENCH_HEADER=$headerContainerPath",
    "golang:1.27",
    "bash", "bench/run.sh"
)

Write-Host "bench: scenario=$Scenario conns=$Conns duration=$Duration interval=$Interval ramp=$Ramp timeout=$Timeout payload=$Payload mode=$Mode ack=$Ack profile=$Profile"
Write-Host "bench: slow_ratio=$SlowRatio reconnect_after=$ReconnectAfter reconnect_window=$ReconnectWindow"
Write-Host "bench: out=$Out"

$code = 1
try {
    & docker @dockerArgs
    $code = $LASTEXITCODE
}
finally {
    Remove-Item -Force -ErrorAction SilentlyContinue $headerHostPath
}

exit $code
