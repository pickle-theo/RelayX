# RelayX Phase 0 Makefile
#
# 依赖：Go 1.27+；race 目标需要本机 Docker；fmt/fmt-check 需要 sh
# （Windows 请在 Git Bash / MSYS2 / WSL 中执行 make）。

GO       ?= go
DOCKER   ?= docker
GO_IMAGE ?= golang:1.27
BIN_DIR  ?= bin
ADDR     ?= 127.0.0.1:9000
ARGS     ?=

ifeq ($(OS),Windows_NT)
EXE := .exe
endif

.PHONY: all help build gateway client loadtest test race vet fmt fmt-check \
	run-gateway run-client load clean

all: fmt-check vet test build

help:
	@echo "RelayX Phase 0 targets:"
	@echo "  make build        构建 gateway/client/loadtest 到 $(BIN_DIR)/"
	@echo "  make test         go test ./..."
	@echo "  make race         在 Docker 中执行 go test -race ./..."
	@echo "  make vet          go vet ./..."
	@echo "  make fmt          格式化代码"
	@echo "  make fmt-check    检查格式（CI 用）"
	@echo "  make run-gateway  启动 gateway（ADDR/ARGS 可覆盖）"
	@echo "  make run-client   启动 client（ADDR/ARGS 可覆盖）"
	@echo "  make load         1000 连接压测（需先启动 gateway）"
	@echo "  make clean        清理构建产物"

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

clean:
	$(GO) clean ./...
	rm -rf $(BIN_DIR)
