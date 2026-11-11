#!/usr/bin/env bash
# Compare the stdlib and gnet echo servers with phase3/cmd/loadtest.
#
# Usage: ./bench/compare.sh
# Env:   CONNS (default 200), DURATION (20s), INTERVAL (100ms), PAYLOAD_SIZE (64)
set -euo pipefail

CONNS="${CONNS:-200}"
DURATION="${DURATION:-20s}"
INTERVAL="${INTERVAL:-100ms}"
PAYLOAD_SIZE="${PAYLOAD_SIZE:-64}"
RAMP="${RAMP:-0s}"

MODULE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PHASE3_DIR="$(cd "$MODULE_DIR/../.." && pwd)"
BIN_DIR="$MODULE_DIR/bin"
REPORT_DIR="$MODULE_DIR/reports"
mkdir -p "$BIN_DIR" "$REPORT_DIR"

# The experiment module is not listed in the repository go.work; build it
# standalone without touching the workspace file.
export GOWORK=off
(cd "$MODULE_DIR" && go build -o "$BIN_DIR/stdlib-echo" ./stdlib)
(cd "$MODULE_DIR" && go build -o "$BIN_DIR/gnet-echo" ./gnet)
(cd "$PHASE3_DIR" && go build -o "$BIN_DIR/loadtest" ./cmd/loadtest)

run_case() {
    local name="$1" exe="$2" addr="$3"
    echo "== $name @ $addr =="
    "$exe" -addr "$addr" >"$REPORT_DIR/$name-server.log" 2>&1 &
    local pid=$!
    sleep 1
    set +e
    "$BIN_DIR/loadtest" -addr "$addr" -conns "$CONNS" -duration "$DURATION" \
        -interval "$INTERVAL" -ramp "$RAMP" -payload-size "$PAYLOAD_SIZE" \
        -out "$REPORT_DIR/$name-conns$CONNS.txt"
    local code=$?
    # Sample the server while it is still running: resident memory, average CPU
    # over its lifetime and OS thread count (gnet's event loops are visible as
    # threads).
    ps -o rss=,%cpu=,nlwp= -p "$pid" >"$REPORT_DIR/$name-server-resource.txt" 2>&1
    echo "loadtest exit=$code report=$REPORT_DIR/$name-conns$CONNS.txt"
    set -e
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
}

run_case stdlib "$BIN_DIR/stdlib-echo" 127.0.0.1:19000
run_case gnet "$BIN_DIR/gnet-echo" 127.0.0.1:19001
