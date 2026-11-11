# Tasks

- [x] Task 1: 创建 Phase 3 独立子项目与 Linux 容器压测环境
  - [x] SubTask 1.1: 复制 `phase2/` 为 `phase3/`，新建 `phase3/go.mod`（`module relayx/phase3`，`go 1.27`）并替换全部 import 前缀；根 `go.work` 增加 `use ./phase3`，根 `Makefile` 的 `PHASES` 增加 `phase3`
  - [x] SubTask 1.2: 验证复制基线：`gofmt -l .`、`go vet ./...`、`go test ./...` 全绿（DB 用例按 `RELAYX_TEST_DSN` 规则 skip/执行）
  - [x] SubTask 1.3: 新增 `phase3/bench/` 压测脚本与 Makefile 目标：`bench-1k`、`bench-10k`、`bench-50k`、`bench-profile`；统一 `ulimit -n`、输出目录与参数
  - [x] SubTask 1.4: 脚本自动采集并写入环境信息（Go/内核/CPU/内存/Docker 版本、参数、原始输出、退出码）

- [x] Task 2: 可观测性与压测工具扩展
  - [x] SubTask 2.1: Gateway 增加 `-pprof-addr`（默认关闭）、`-block-profile-rate`、`-mutex-profile-fraction`，挂载 CPU/heap/goroutine/block/mutex/trace 端点
  - [x] SubTask 2.2: 周期性 stats 增加协程数、`heap_alloc`、`heap_sys`、GC 次数与最近暂停、活跃连接/会话、DB 连接数
  - [x] SubTask 2.3: `cmd/loadtest` 增加 `-scenario`（`idle`/`heartbeat`/`steady-low`/`steady-high`/`slow-consumer`/`large-payload`/`reconnect-storm`）、慢消费者比例、重连风暴参数
  - [x] SubTask 2.4: 压测报告增加客户端 RSS、协程数、每连接内存、按场景分类的错误计数与静默丢失计数
  - [x] SubTask 2.5: 为新增 flag/场景补单元测试，并用 1k 连接冒烟验证

- [x] Task 3: Phase 2 基线测量
  - [x] SubTask 3.1: 在 Linux 容器内以 1k / 10k 跑完整场景矩阵，保留原始输出
  - [x] SubTask 3.2: 尝试 50k 并记录最大稳定连接数；尝试 100k 并记录环境限制与失败阶段
  - [x] SubTask 3.3: 整理基线报告：吞吐、RTT 分位、每连接内存/协程、GC、DB 连接、错误分类

- [x] Task 4: 瓶颈分析
  - [x] SubTask 4.1: 采集 10k 稳定期 CPU profile、`alloc_space`、goroutine，并记录 top-N
  - [x] SubTask 4.2: 采集慢消费者与重连风暴下的 block/mutex profile（trace 端点已验证可用，未单独采样）
  - [x] SubTask 4.3: 输出至少三个有证据的瓶颈（现象、指标、profile 证据、影响范围、候选方案）
  - [x] SubTask 4.4: 按预期收益与风险排序，确定两轮优化的具体改动清单

- [x] Task 5: 第一轮优化（写路径 / 读路径）
  - [x] SubTask 5.1: 按 Task 4 结论实施第一轮改动，保持协议与行为不变
  - [x] SubTask 5.2: 补充或更新对应并发测试，`go test -race` 通过
  - [x] SubTask 5.3: 同参数复测 1k / 10k，产出前后对比表并给出保留或回退结论

- [x] Task 6: 第二轮优化（内存 / GC / 锁 / 编解码）
  - [x] SubTask 6.1: 按 profile 数据实施第二轮改动（对象复用、缓冲、定时器或锁粒度等）
  - [x] SubTask 6.2: 补充泄漏与并发测试，`go test -race` 通过
  - [x] SubTask 6.3: 同参数复测并对比第一轮与基线，更新优化报告

- [x] Task 7: gnet / netpoll 对比实验
  - [x] SubTask 7.1: 在 `phase3/experiments/netlib-compare/` 建独立 module，实现可对比的最小 echo 服务（标准库、gnet，必要时 netpoll）
  - [x] SubTask 7.2: 用同一压测参数在 1k / 10k 下测量吞吐、P99、内存与协程模型
  - [x] SubTask 7.3: 输出 ADR-012：收益、代价、适用条件与明确结论；确认主 module 无新增第三方依赖

- [x] Task 8: 容量边界、文档与最终门禁
  - [x] SubTask 8.1: 完成 50k 尽力验证与 100k 尝试，整理每连接内存、协程、FD、临时端口/TIME_WAIT 资源账本
  - [x] SubTask 8.2: 编写 `phase3/docs/README.md`、基线报告、优化报告、容量边界报告与 ADR-010/011/012
  - [x] SubTask 8.3: 最终 `gofmt`、`go vet`、`go test ./...`、Docker `go test -race ./...`、协程泄漏检查全部通过并记录实际输出
  - [x] SubTask 8.4: 复跑关键场景确认文档数据与二进制一致，清理临时产物

# Task Dependencies

- Task 2 依赖 Task 1
- Task 3 依赖 Task 2
- Task 4 依赖 Task 3
- Task 5 依赖 Task 4
- Task 6 依赖 Task 5
- Task 7 可与 Task 5/6 并行（依赖 Task 2 的压测工具）
- Task 8 依赖 Task 3–7
