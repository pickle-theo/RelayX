# RelayX Phase 2 Makefile
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

ifeq ($(OS),Windows_NT)
EXE := .exe
endif

.PHONY: all help build gateway client loadtest test test-db race vet fmt fmt-check \
	run-gateway run-client load bench profile clean db-up db-wait db-down db-reset

all: fmt-check vet test build

help:
	@echo "RelayX Phase 2 targets:"
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
	@echo "  make bench        基准测试（含内存分配统计）"
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

bench:
	$(GO) test -run '^$$' -bench . -benchmem ./internal/protocol ./internal/transport

profile:
	$(GO) test -run '^$$' -bench BenchmarkSend64 -benchmem -cpuprofile cpu.out -memprofile mem.out ./internal/transport

clean:
	$(GO) clean ./...
	rm -rf $(BIN_DIR) cpu.out mem.out
