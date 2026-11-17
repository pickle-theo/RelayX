# S5 一键基准复跑（Windows / PowerShell 版本，等价于 harness.sh）。
#
# 用法：
#   .\benchmark\harness.ps1 baseline
#   .\benchmark\harness.ps1 after
#   .\benchmark\harness.ps1 ab          # 生成 A/B 对照表（同一二进制内前后对比）
#   .\benchmark\harness.ps1 compare     # 生成两次运行（不同 commit）的对比
#   .\benchmark\harness.ps1 profiles    # 采集五类 pprof
#
# 注意：本环境 PowerShell 会把 -bench=. 的 `=` 之后内容当成包参数，
# 因此基准过滤统一写成 -bench Benchmark（见 CODEBUDDY.md 的同名说明）。
param(
  [Parameter(Position = 0)]
  [string]$Mode = "after"
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

$Reports = Join-Path $Root "profiling\reports"
$Profiles = Join-Path $Root "profiling\profiles"
New-Item -ItemType Directory -Force -Path $Reports, $Profiles | Out-Null

if (-not $env:GOMAXPROCS) { $env:GOMAXPROCS = "4" }
if (-not $env:GOGC) { $env:GOGC = "100" }
$BenchPattern = if ($env:BENCH_PATTERN) { $env:BENCH_PATTERN } else { "Benchmark" }
$BenchTime = if ($env:BENCHTIME) { $env:BENCHTIME } else { "300ms" }
$Count = if ($env:COUNT) { $env:COUNT } else { "3" }

# PowerShell 不会像 bash 的 set -e 那样在原生命令失败时中止（除非显式检查），
# 否则 go test/go run 失败后仍会继续写报告，产出「零基准」的假结果。
function Assert-ExitCode([string]$Step) {
  if ($LASTEXITCODE -ne 0) {
    throw "$Step failed with exit code $LASTEXITCODE"
  }
}

switch ($Mode) {
  { $_ -in "baseline", "after" } {
    $out = Join-Path $Reports "$Mode.json"
    if ((Test-Path $out) -and ($env:FORCE -ne "1")) {
      Write-Error "refusing to overwrite frozen report: $out (set FORCE=1 to overwrite)"
    }
    $txt = Join-Path $Reports "$Mode.txt"
    Write-Host "== go test -bench $BenchPattern (label=$Mode, count=$Count) =="
    # 必须显式 UTF8 落盘：Tee-Object 默认 UTF-16，会让报告解析器读不到基准行。
    $benchOut = & go test -run '^$' -bench $BenchPattern -benchmem -count $Count -benchtime $BenchTime ./benchmark/ 2>&1
    Assert-ExitCode "go test -bench ($Mode)"
    $benchOut | Out-File -FilePath $txt -Encoding utf8
    $benchOut | ForEach-Object { Write-Host $_ }
    go run ./profiling/cmd/report -mode=parse -in $txt -out $out -label $Mode
    Assert-ExitCode "report -mode=parse"
  }
  "ab" {
    go run ./profiling/cmd/report -mode=ab -in (Join-Path $Reports "after.json") -out (Join-Path $Reports "ab.md")
    Assert-ExitCode "report -mode=ab"
    Get-Content (Join-Path $Reports "ab.md")
  }
  "compare" {
    go run ./profiling/cmd/report -mode=compare -before (Join-Path $Reports "baseline.json") -after (Join-Path $Reports "after.json") -out (Join-Path $Reports "compare.md")
    Assert-ExitCode "report -mode=compare"
    Get-Content (Join-Path $Reports "compare.md")
  }
  "profiles" {
    go run ./profiling/cmd/report -mode=profiles -dir $Profiles -pkg ./benchmark -bench $BenchPattern -benchtime $BenchTime
    Assert-ExitCode "report -mode=profiles"
    # 第五类：goroutine profile（go test 无对应参数，用专用用例归档）。
    $env:S5_DUMP_GOROUTINE = "1"
    $env:S5_PROFILE_DIR = $Profiles
    go test -run TestGoroutineProfileSnapshot ./benchmark/
    Assert-ExitCode "goroutine profile snapshot"
    Remove-Item Env:S5_DUMP_GOROUTINE, Env:S5_PROFILE_DIR
  }
  default {
    Write-Error "usage: .\benchmark\harness.ps1 [baseline|after|ab|compare|profiles]"
  }
}
