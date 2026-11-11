#!/usr/bin/env bash
# RelayX Phase 3 bench runner.
#
# Runs inside the Linux load-test container mounted at /app (module root).
# Every input is an environment variable so the Windows wrapper (bench/run.ps1)
# can drive a run without extra arguments. A report is always written, even
# when a step fails; in that case the script exits non-zero.
#
# Inputs:
#   CONNS, DURATION, INTERVAL, RAMP, SCENARIO, PAYLOAD, MODE, ACK, PROFILE,
#   OUT, ADDR, PPROF_ADDR, DSN, STATS_INTERVAL
#
# Every value above is copied into the report header so a run is reproducible.

set -uo pipefail

CONNS="${CONNS:-1000}"
DURATION="${DURATION:-60s}"
INTERVAL="${INTERVAL:-1s}"
RAMP="${RAMP:-5s}"
TIMEOUT="${TIMEOUT:-5s}"
SCENARIO="${SCENARIO:-steady-low}"
PAYLOAD="${PAYLOAD:-64}"
MODE="${MODE:-auto}"
ACK="${ACK:-both}"
SLOW_RATIO="${SLOW_RATIO:-0.1}"
RECONNECT_AFTER="${RECONNECT_AFTER:-10s}"
RECONNECT_WINDOW="${RECONNECT_WINDOW:-5s}"
PROFILE="${PROFILE:-0}"
ADDR="${ADDR:-127.0.0.1:19330}"
PPROF_ADDR="${PPROF_ADDR:-127.0.0.1:16060}"
DSN="${DSN:-postgres://relayx:relayx@host.docker.internal:5432/relayx?sslmode=disable}"
STATS_INTERVAL="${STATS_INTERVAL:-5s}"
BENCH_HEADER="${BENCH_HEADER:-}"

if [ -z "${OUT:-}" ]; then
  OUT="docs/bench/raw/bench-${SCENARIO}-$(date -u +%Y%m%dT%H%M%SZ).txt"
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)" || exit 1
cd "$ROOT" || exit 1

case "$OUT" in
  /*) : ;;
  *) OUT="$ROOT/$OUT" ;;
esac

STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
RUN_ID="$$"
BUILD_DIR="/tmp/relayx-phase3-bin"
GW_LOG="/tmp/relayx-phase3-gateway-${RUN_ID}.log"
LT_LOG="/tmp/relayx-phase3-loadtest-${RUN_ID}.log"
LT_BODY="/tmp/relayx-phase3-loadtest-body-${RUN_ID}.txt"
BUILD_LOG="/tmp/relayx-phase3-build-${RUN_ID}.log"
PROFILE_DIR="$ROOT/docs/bench/raw/profiles"

GW_PID=""
PROFILE_PID=""
PROFILE_DELAY="${PROFILE_DELAY:-10}"
PROFILE_SECONDS="${PROFILE_SECONDS:-15}"
FAIL_REASON=""
PROFILE_NOTES=""
LT_RC=1
GW_ALIVE="no"

mkdir -p "$(dirname "$OUT")"

# The container is started with --ulimit nofile=1048576:1048576, but the soft
# limit may differ. Raise it and keep the value that actually applies.
ulimit -n 1048576 2>/dev/null || true
FD_LIMIT="$(ulimit -n 2>/dev/null || echo unknown)"
if [ "$FD_LIMIT" != "1048576" ]; then
  echo "bench: ulimit -n 未能提升到 1048576，实际为 $FD_LIMIT" >&2
fi

# Effective transport mode: idle/heartbeat only log in, every other scenario
# exercises the reliable send path unless the caller overrides MODE.
# (Resolved after the report helpers are defined so an invalid value still
# produces a report.)
EFFECTIVE_MODE="$MODE"

write_env() {
  echo "go: $(go version 2>&1)"
  echo "uname: $(uname -a 2>&1)"
  echo "nproc: $(nproc 2>&1)"
  echo "meminfo:"
  head -n 5 /proc/meminfo 2>&1 | sed 's/^/  /'
  echo "ulimit_nofile: $FD_LIMIT (requested 1048576)"
  echo "ip_local_port_range: $(cat /proc/sys/net/ipv4/ip_local_port_range 2>&1)"
  echo "gateway_alive_at_end: $GW_ALIVE"
}

write_report() {
  local tmp="$OUT.tmp.$$"
  {
    echo "# RelayX Phase 3 bench report"
    echo
    echo "generated_at: $STARTED_AT"
    echo "scenario: $SCENARIO"
    echo "effective_mode: $EFFECTIVE_MODE"
    echo "exit_code: $LT_RC"
    if [ -n "$FAIL_REASON" ]; then
      echo "failure: $FAIL_REASON"
    fi
    echo
    if [ -n "$BENCH_HEADER" ] && [ -f "$BENCH_HEADER" ]; then
      cat "$BENCH_HEADER"
      echo
    fi
    echo "## parameters"
    echo '```'
    echo "CONNS=$CONNS"
    echo "DURATION=$DURATION"
    echo "INTERVAL=$INTERVAL"
    echo "RAMP=$RAMP"
    echo "TIMEOUT=$TIMEOUT"
    echo "SCENARIO=$SCENARIO"
    echo "PAYLOAD=$PAYLOAD"
    echo "MODE=$MODE"
    echo "ACK=$ACK"
    echo "SLOW_RATIO=$SLOW_RATIO"
    echo "RECONNECT_AFTER=$RECONNECT_AFTER"
    echo "RECONNECT_WINDOW=$RECONNECT_WINDOW"
    echo "PROFILE=$PROFILE"
    echo "ADDR=$ADDR"
    echo "PPROF_ADDR=$PPROF_ADDR"
    echo "DSN=$DSN"
    echo "STATS_INTERVAL=$STATS_INTERVAL"
    echo "OUT=$OUT"
    echo "ulimit_nofile=$FD_LIMIT (requested 1048576)"
    echo '```'
    echo
    echo "## loadtest output (raw)"
    echo '```'
    if [ -f "$LT_BODY" ]; then
      cat "$LT_BODY"
    else
      echo "(loadtest 未产生报告)"
    fi
    echo '```'
    echo
    echo "## gateway stats (tail)"
    echo '```'
    if [ -f "$GW_LOG" ]; then
      grep -F 'gateway stats' "$GW_LOG" | tail -n 60 || true
    else
      echo "(no gateway log)"
    fi
    echo '```'
    echo
    echo "## gateway log (tail)"
    echo '```'
    if [ -f "$GW_LOG" ]; then
      tail -n 40 "$GW_LOG"
    else
      echo "(no gateway log)"
    fi
    echo '```'
    echo
    if [ -f "$BUILD_LOG" ]; then
      echo "## build log"
      echo '```'
      tail -n 40 "$BUILD_LOG"
      echo '```'
      echo
    fi
    if [ -n "$PROFILE_NOTES" ]; then
      echo "## profile notes"
      echo '```'
      printf '%s' "$PROFILE_NOTES"
      echo '```'
      echo
    fi
    echo "## environment"
    echo '```'
    write_env
    echo '```'
  } >"$tmp" && mv "$tmp" "$OUT"
}

fail() {
  FAIL_REASON="$1"
  LT_RC="${2:-1}"
  write_report
  echo "bench: $1" >&2
  exit "$LT_RC"
}

case "$MODE" in
  auto)
    case "$SCENARIO" in
      idle | heartbeat) EFFECTIVE_MODE="login" ;;
      *) EFFECTIVE_MODE="reliable" ;;
    esac
    ;;
  login | echo | reliable | route) EFFECTIVE_MODE="$MODE" ;;
  *)
    EFFECTIVE_MODE="$MODE"
    fail "MODE 必须是 auto/login/echo/reliable/route，实际为 $MODE" 2
    ;;
esac

wait_port() {
  local host="${ADDR%:*}" port="${ADDR##*:}"
  case "$host" in
    "" | 0.0.0.0) host="127.0.0.1" ;;
  esac
  local deadline=$((SECONDS + 30))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then
      return 0
    fi
    if [ -n "$GW_PID" ] && ! kill -0 "$GW_PID" 2>/dev/null; then
      return 1
    fi
    sleep 0.2
  done
  return 1
}

# start_profile_collector waits for the warm-up to finish so every profile is
# sampled while the load test is actually running, not before or after it.
start_profile_collector() {
  [ "$PROFILE" = "1" ] || return 0
  mkdir -p "$PROFILE_DIR"
  rm -f "$PROFILE_DIR/notes.txt"
  (
    sleep "$PROFILE_DELAY"
    if ! go tool pprof -proto -output "$PROFILE_DIR/cpu.proto" \
      "http://$PPROF_ADDR/debug/pprof/profile?seconds=$PROFILE_SECONDS" \
      >"$PROFILE_DIR/cpu.log" 2>&1; then
      echo "cpu profile 采集失败（见 $PROFILE_DIR/cpu.log）" >>"$PROFILE_DIR/notes.txt"
    fi
    local name
    for name in heap goroutine block mutex; do
      if ! go tool pprof -proto -output "$PROFILE_DIR/$name.proto" \
        "http://$PPROF_ADDR/debug/pprof/$name" >"$PROFILE_DIR/$name.log" 2>&1; then
        echo "profile $name 采集失败（见 $PROFILE_DIR/$name.log）" >>"$PROFILE_DIR/notes.txt"
      fi
    done
  ) &
  PROFILE_PID=$!
}

stop_gateway() {
  [ -n "$GW_PID" ] || return 0
  kill -TERM "$GW_PID" 2>/dev/null || true
  local i=0
  while [ "$i" -lt 100 ]; do
    if ! kill -0 "$GW_PID" 2>/dev/null; then
      wait "$GW_PID" 2>/dev/null
      GW_PID=""
      return 0
    fi
    sleep 0.1
    i=$((i + 1))
  done
  kill -KILL "$GW_PID" 2>/dev/null || true
  wait "$GW_PID" 2>/dev/null
  GW_PID=""
}

trap 'stop_gateway' EXIT

# 1. Build both binaries into a throw-away directory so the workspace stays clean.
mkdir -p "$BUILD_DIR"
if ! go build -o "$BUILD_DIR/gateway" ./cmd/gateway >"$BUILD_LOG" 2>&1; then
  fail "go build ./cmd/gateway 失败" 1
fi
if ! go build -o "$BUILD_DIR/loadtest" ./cmd/loadtest >>"$BUILD_LOG" 2>&1; then
  fail "go build ./cmd/loadtest 失败" 1
fi

# 2. Start the gateway and wait for its TCP port.
GW_ARGS=(-addr "$ADDR" -stats-interval "$STATS_INTERVAL" -dsn "$DSN")
if [ "$PROFILE" = "1" ]; then
  GW_ARGS+=(-pprof-addr "$PPROF_ADDR" -block-profile-rate 1 -mutex-profile-fraction 1)
fi

"$BUILD_DIR/gateway" "${GW_ARGS[@]}" >"$GW_LOG" 2>&1 &
GW_PID=$!

if ! wait_port; then
  fail "gateway 未在 30s 内监听 $ADDR（见 gateway log）" 1
fi
GW_ALIVE="yes"

# 3. Start profiling shortly after the load begins, so CPU and heap snapshots
#    describe the steady state instead of an idle gateway.
start_profile_collector

# 4. Run the load test; its own report goes to OUT and is copied aside before
#    this script rewrites OUT with the full record.
LT_ARGS=(
  -addr "$ADDR"
  -conns "$CONNS"
  -duration "$DURATION"
  -interval "$INTERVAL"
  -ramp "$RAMP"
  -timeout "$TIMEOUT"
  -payload-size "$PAYLOAD"
  -ack "$ACK"
  -slow-ratio "$SLOW_RATIO"
  -reconnect-after "$RECONNECT_AFTER"
  -reconnect-window "$RECONNECT_WINDOW"
  -scenario "$SCENARIO"
  -out "$OUT"
)
case "$EFFECTIVE_MODE" in
  login) LT_ARGS+=(-login) ;;
  reliable) LT_ARGS+=(-reliable) ;;
  route) LT_ARGS+=(-route) ;;
  echo) : ;;
esac

rm -f "$OUT"
"$BUILD_DIR/loadtest" "${LT_ARGS[@]}" >"$LT_LOG" 2>&1
LT_RC=$?

if [ -f "$OUT" ]; then
  cp "$OUT" "$LT_BODY"
else
  # A crash before -out was written still needs a record.
  cp "$LT_LOG" "$LT_BODY"
fi

if ! kill -0 "$GW_PID" 2>/dev/null; then
  GW_ALIVE="no"
fi

# 5. Wait for the profile collector (CPU + mid-run snapshots).
if [ -n "$PROFILE_PID" ]; then
  if ! wait "$PROFILE_PID"; then
    PROFILE_NOTES="${PROFILE_NOTES}profile collector 失败
"
  fi
  PROFILE_PID=""
fi
if [ -f "$PROFILE_DIR/notes.txt" ]; then
  PROFILE_NOTES="${PROFILE_NOTES}$(cat "$PROFILE_DIR/notes.txt")
"
fi

# 6. Stop the gateway so the final stats line is flushed, then write the report.
stop_gateway
write_report

exit "$LT_RC"
