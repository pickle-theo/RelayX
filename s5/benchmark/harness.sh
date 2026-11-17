#!/usr/bin/env bash
# S5 一键基准 / Profiling 复跑脚本（LLD §3.1 / §3.2，验收「可重复执行的一键流程」）。
#
# 用法：
#   ./benchmark/harness.sh baseline        # 优化前基线（冻结一次，之后只读）
#   ./benchmark/harness.sh after           # 优化后
#   ./benchmark/harness.sh compare         # 生成 reports/compare.md
#   ./benchmark/harness.sh profiles        # 采集五类 pprof（CPU/Heap/Block/Mutex/Goroutine）
#
# 纪律：
#   - 固定 GOOS/GOARCH/GOMAXPROCS/GOGC，报告记录完整环境（S5 §7.3）；
#   - -count=3 后取中位数（不挑最好的一次）；
#   - 基线报告不可被覆盖重写（已存在则拒绝，除非 FORCE=1）。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

LABEL="${1:-after}"
REPORTS="$ROOT/profiling/reports"
PROFILES="$ROOT/profiling/profiles"
BENCH_PATTERN="${BENCH_PATTERN:-Benchmark}"
BENCHTIME="${BENCHTIME:-300ms}"
COUNT="${COUNT:-3}"

export GOMAXPROCS="${GOMAXPROCS:-4}"
export GOGC="${GOGC:-100}"

mkdir -p "$REPORTS" "$PROFILES"

run_bench_and_parse() {
  local label="$1"
  local out="$REPORTS/$label.json"
  if [[ -f "$out" && "${FORCE:-0}" != "1" ]]; then
    echo "refusing to overwrite frozen report: $out (set FORCE=1 to overwrite)" >&2
    return 1
  fi
  echo "== go test -bench $BENCH_PATTERN (label=$label, count=$COUNT) =="
  go test -run '^$' -bench "$BENCH_PATTERN" -benchmem -count "$COUNT" -benchtime "$BENCHTIME" ./benchmark/ \
    | tee "$REPORTS/$label.txt"
  go run ./profiling/cmd/report -mode=parse \
    -in "$REPORTS/$label.txt" -out "$out" -label "$label"
}

case "$LABEL" in
  baseline|after)
    run_bench_and_parse "$LABEL"
    ;;
  compare)
    go run ./profiling/cmd/report -mode=compare \
      -before "$REPORTS/baseline.json" -after "$REPORTS/after.json" \
      -out "$REPORTS/compare.md"
    cat "$REPORTS/compare.md"
    ;;
  profiles)
    go run ./profiling/cmd/report -mode=profiles \
      -dir "$PROFILES" -pkg "./benchmark" -bench "$BENCH_PATTERN" -benchtime "${BENCHTIME}"
    # 第五类：goroutine profile（go test 无对应参数，用专用用例归档）。
    S5_DUMP_GOROUTINE=1 S5_PROFILE_DIR="$PROFILES" \
      go test -run TestGoroutineProfileSnapshot ./benchmark/
    ;;
  *)
    echo "usage: $0 [baseline|after|compare|profiles]" >&2
    exit 2
    ;;
esac
