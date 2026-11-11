# Compare the stdlib and gnet echo servers with phase3/cmd/loadtest.
#
# Usage (from anywhere):
#   pwsh -File bench/compare.ps1 [-Conns 200] [-Duration 20s] [-Interval 100ms] [-PayloadSize 64]

param(
    [int]$Conns = 200,
    [string]$Duration = "20s",
    [string]$Interval = "100ms",
    [int]$PayloadSize = 64
)

$ErrorActionPreference = "Stop"

$moduleDir = Split-Path -Parent $PSScriptRoot
$phase3Dir = Split-Path -Parent (Split-Path -Parent $moduleDir)
$binDir = Join-Path $moduleDir "bin"
$reportDir = Join-Path $moduleDir "reports"
New-Item -ItemType Directory -Force -Path $binDir, $reportDir | Out-Null

# The experiment module is not listed in the repository go.work; build it
# standalone without touching the workspace file.
$env:GOWORK = "off"
Push-Location $moduleDir
try {
    go build -o (Join-Path $binDir "stdlib-echo.exe") ./stdlib
    go build -o (Join-Path $binDir "gnet-echo.exe") ./gnet
} finally {
    Pop-Location
}

Push-Location $phase3Dir
try {
    go build -o (Join-Path $binDir "loadtest.exe") ./cmd/loadtest
} finally {
    Pop-Location
}

$loadtest = Join-Path $binDir "loadtest.exe"
$cases = @(
    @{ Name = "stdlib"; Exe = (Join-Path $binDir "stdlib-echo.exe"); Addr = "127.0.0.1:19000" },
    @{ Name = "gnet";   Exe = (Join-Path $binDir "gnet-echo.exe");   Addr = "127.0.0.1:19001" }
)

foreach ($case in $cases) {
    $report = Join-Path $reportDir ("{0}-conns{1}.txt" -f $case.Name, $Conns)
    Write-Host "== $($case.Name) @ $($case.Addr) =="
    $server = Start-Process -FilePath $case.Exe -ArgumentList "-addr", $case.Addr -PassThru `
        -RedirectStandardOutput (Join-Path $reportDir "$($case.Name)-server.out") `
        -RedirectStandardError (Join-Path $reportDir "$($case.Name)-server.err")
    try {
        Start-Sleep -Milliseconds 1000
        & $loadtest -addr $case.Addr -conns $Conns -duration $Duration -interval $Interval `
            -ramp 0s -payload-size $PayloadSize -out $report
        Write-Host "loadtest exit=$LASTEXITCODE report=$report"
    } finally {
        Stop-Process -Id $server.Id -Force -ErrorAction SilentlyContinue
    }
}
