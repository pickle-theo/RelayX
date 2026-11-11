# RelayX Phase 3 Makefile
#
# 依赖：Go 1.27+；PostgreSQL 仅通过本机 Docker 运行；fmt/fmt-check 需要 sh
# （Windows 请在 Git Bash / MSYS2 / WSL 中执行 make）。

GO       ?= go
DOCKER   ?= docker
GO_IMAGE ?= golang:1.27
BIN_DIR  ?= bin
ADDR     ?= 127.0.0.1:9000
ARGS     ?=
PG_CONTAINER ?= relayx-phase2-pg
TEST_DSN ?= postgres://relayx:relayx@127.0.0.1:5432/relayx?sslmode=disable

# Load-test container settings (used by bench* targets; see bench/run.ps1 for
# the Windows wrapper, since Windows hosts often have no make).
BENCH_RAW ?= docs/bench/raw
CONNS    ?= 1000
SCENARIO ?= steady-low
INTERVAL ?= 1s
DURATION ?= 60s

ifeq ($(OS),Windows_NT)
EXE := .exe
endif

.PHONY: all help build gateway client loadtest test test-db race vet fmt fmt-check \
	run-gateway run-client load bench bench-go bench-1k bench-10k bench-50k \
	bench-profile profile clean db-up db-wait db-down db-reset

all: fmt-check vet test build

help:
	@echo "RelayX Phase 3 targets:"
	@echo "  make build        构建 gateway/client/loadtest 到 $(BIN_DIR)/"
	@echo "  make test         go test ./...（不设置 DSN 时跳过数据库用例）"
	@echo "  make test-db      设置 RELAYX_TEST_DSN 后执行全量测试"
	@echo "  make db-up        启动本机 Docker PostgreSQL 并等待 healthy"
	@echo "  make db-down      停止容器（保留数据卷）"
	@echo "  make db-reset     删除数据卷后重建数据库"
	@echo "  make race         在 Docker 中执行 go test -race ./..."
	@echo "  make vet          go vet ./..."
	@echo "  make fmt          格式化代码"
	@echo "  make fmt-check    检查格式（CI 用）"
	@echo "  make run-gateway  启动 gateway（ADDR/ARGS 可覆盖）"
	@echo "  make run-client   启动 client（ADDR/ARGS 可覆盖）"
	@echo "  make load         1000 连接压测（需先启动 gateway）"
	@echo "  make bench        Docker 容器压测（CONNS/SCENARIO/INTERVAL/DURATION 可覆盖）"
	@echo "  make bench-1k     1000 连接、steady-low、interval 1s、60s"
	@echo "  make bench-10k    10000 连接、steady-low、interval 5s、60s"
	@echo "  make bench-50k    50000 连接、idle、60s"
	@echo "  make bench-profile  10000 连接、steady-low、interval 5s、60s、采集 profile"
	@echo "  make bench-go     Go 微基准（含内存分配统计）"
	@echo "  make profile      采集 BenchmarkSend64 的 CPU/内存 profile"
	@echo "  make clean        清理构建产物与 profile 文件"

build: gateway client loadtest

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

gateway: | $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/gateway$(EXE) ./cmd/gateway

client: | $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/client$(EXE) ./cmd/client

loadtest: | $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/loadtest$(EXE) ./cmd/loadtest

test:
	$(GO) test ./...

test-db:
	RELAYX_TEST_DSN='$(TEST_DSN)' $(GO) test ./... -count=1

# 启动本机 PostgreSQL 并等待 healthy。优先使用 docker compose --wait，
# 老版本不支持该参数时回退为轮询容器健康状态（最长 60s）。
db-up:
	@if $(DOCKER) compose up -d --wait; then \
		echo "$(PG_CONTAINER) 已 healthy"; \
	else \
		echo "docker compose --wait 不可用，回退为轮询健康状态..."; \
		$(DOCKER) compose up -d || exit 1; \
		$(MAKE) db-wait; \
	fi

db-wait:
	@i=0; \
	while [ $$i -lt 60 ]; do \
		status=$$($(DOCKER) inspect -f '{{.State.Health.Status}}' $(PG_CONTAINER) 2>/dev/null); \
		if [ "$$status" = "healthy" ]; then echo "$(PG_CONTAINER) 已 healthy"; exit 0; fi; \
		i=$$((i+1)); sleep 1; \
	done; \
	echo "$(PG_CONTAINER) 在 60s 内未进入 healthy"; exit 1

db-down:
	$(DOCKER) compose down

db-reset:
	$(DOCKER) compose down -v
	$(MAKE) db-up

race:
	$(DOCKER) run --rm -v "$(CURDIR):/app" -w /app $(GO_IMAGE) $(GO) test -race -count=1 ./...

vet:
	$(GO) vet ./...

fmt:
	gofmt -l -w .

fmt-check:
	@files="$$(gofmt -l .)"; \
	if [ -n "$$files" ]; then echo "gofmt needed:"; echo "$$files"; exit 1; fi

run-gateway:
	$(GO) run ./cmd/gateway -addr $(ADDR) $(ARGS)

run-client:
	$(GO) run ./cmd/client -addr $(ADDR) $(ARGS)

load:
	$(GO) run ./cmd/loadtest -addr $(ADDR) -conns 1000 -duration 60s -ramp 5s

# Containerised load tests. These targets document the standard runs and work
# on Linux/CI; on Windows use bench/run.ps1 (make is often unavailable).
$(BENCH_RAW)/profiles:
	mkdir -p $(BENCH_RAW)/profiles

bench: | $(BENCH_RAW)/profiles
	CONNS=$(CONNS) SCENARIO=$(SCENARIO) INTERVAL=$(INTERVAL) DURATION=$(DURATION) \
		OUT=$(BENCH_RAW)/bench-$(SCENARIO)-$(CONNS).txt bash bench/run.sh

bench-1k: | $(BENCH_RAW)/profiles
	CONNS=1000 SCENARIO=steady-low INTERVAL=1s DURATION=60s \
		OUT=$(BENCH_RAW)/bench-1k.txt bash bench/run.sh

bench-10k: | $(BENCH_RAW)/profiles
	CONNS=10000 SCENARIO=steady-low INTERVAL=5s DURATION=60s \
		OUT=$(BENCH_RAW)/bench-10k.txt bash bench/run.sh

bench-50k: | $(BENCH_RAW)/profiles
	CONNS=50000 SCENARIO=idle DURATION=60s \
		OUT=$(BENCH_RAW)/bench-50k.txt bash bench/run.sh

bench-profile: | $(BENCH_RAW)/profiles
	CONNS=10000 SCENARIO=steady-low INTERVAL=5s DURATION=60s PROFILE=1 \
		OUT=$(BENCH_RAW)/bench-profile.txt bash bench/run.sh

bench-go:
	$(GO) test -run '^$$' -bench . -benchmem ./internal/protocol ./internal/transport

profile:
	$(GO) test -run '^$$' -bench BenchmarkSend64 -benchmem -cpuprofile cpu.out -memprofile mem.out ./internal/transport

clean:
	$(GO) clean ./...
	rm -rf $(BIN_DIR) cpu.out mem.out
