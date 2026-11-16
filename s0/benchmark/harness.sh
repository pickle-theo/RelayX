#!/usr/bin/env bash
# S0 一键基准脚本：固定环境变量，输出可归档的结构化报告（S5 对比基线）。
set -euo pipefail

cd "$(dirname "$0")/.."

export GOOS=${GOOS:-linux}
export GOARCH=${GOARCH:-amd64}
export GOMAXPROCS=${GOMAXPROCS:-4}

echo "=== S0 Benchmark Harness ==="
echo "GOOS=$GOOS GOARCH=$GOARCH GOMAXPROCS=$GOMAXPROCS"
echo "commit: $(git rev-parse --short HEAD 2>/dev/null || echo 'n/a')"
echo "date:   $(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo "--- go test -race ---"
go test -race ./...

echo "--- benchmarks (-benchmem) ---"
go test -bench=. -benchmem ./benchmark/... | tee benchmark/result-$(date -u +%Y%m%dT%H%M%SZ).txt

echo "=== done ==="
