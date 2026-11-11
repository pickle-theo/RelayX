# RelayX 各阶段统一入口
#
# 每个 Phase 是独立子项目（独立 go.mod / Makefile / docs），
# 在此维护阶段列表，新增阶段时只需把目录名加入 PHASES。
#
# 用法示例：make test / make race / make phase0-build

PHASES := phase0 phase0.5 phase1 phase2 phase3 phase4 phase5 phase5.5

.PHONY: all help test race vet fmt-check build clean $(PHASES)

all: fmt-check vet test build

help:
	@echo "RelayX phases: $(PHASES)"
	@echo "  make build      构建所有阶段"
	@echo "  make test       运行所有阶段测试"
	@echo "  make race       在 Docker 中运行所有阶段 race 测试"
	@echo "  make vet        go vet 所有阶段"
	@echo "  make fmt-check  检查所有阶段代码格式"
	@echo "  make clean      清理所有阶段构建产物"
	@echo "  make phase0     查看 Phase 0 子项目可用目标"

test:
	@for p in $(PHASES); do echo "==> $$p: test"; $(MAKE) -C $$p test || exit 1; done

race:
	@for p in $(PHASES); do echo "==> $$p: race"; $(MAKE) -C $$p race || exit 1; done

vet:
	@for p in $(PHASES); do echo "==> $$p: vet"; $(MAKE) -C $$p vet || exit 1; done

fmt-check:
	@for p in $(PHASES); do echo "==> $$p: fmt-check"; $(MAKE) -C $$p fmt-check || exit 1; done

build:
	@for p in $(PHASES); do echo "==> $$p: build"; $(MAKE) -C $$p build || exit 1; done

clean:
	@for p in $(PHASES); do echo "==> $$p: clean"; $(MAKE) -C $$p clean || exit 1; done

$(PHASES):
	@$(MAKE) -C $@ help
