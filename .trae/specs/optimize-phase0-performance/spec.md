# Phase 0.5：Phase 0 极致优化 Spec

## Why

Phase 0 已通过功能与正确性验收，但热路径仍存在明确的浪费：`Conn.Send` 每帧新建 `bytes.Buffer` 并整帧拷贝，写循环每帧一次 `Write`，读循环每帧一次 `SetReadDeadline`，每连接固定 3 个协程与 ticker，发送队列只按帧数限制。需要在不更换标准库 `net` 架构的前提下，用 Benchmark 与 Profiling 数据驱动地把这些开销降到该架构的合理上限，为 Phase 1 提供高性能传输层基线。

## What Changes

- 新建独立子项目 `phase0.5/`（`module relayx/phase0.5`），完整保留 `phase0/` 不动；根 `go.work` 与根 `Makefile` 纳入新阶段。
- 建立可重复的 Benchmark 与 Profiling 流程，先记录 Phase 0 基线，再逐项优化并记录前后数据。
- 协议层新增低分配 API（`AppendFrame` / `DecodeInto`），保持 21 字节线格式、CRC32、错误语义完全兼容；`Encode` / `Decode` 保留。
- 引入编码/解码 buffer 池（分级、上限受控），明确 buffer 所有权与归还时机。
- 写路径：多条帧合并写出、发送队列增加字节预算、控制帧优先、写 Deadline。
- 读路径：读缓冲区大小可配置、降低读 Deadline 重置频率、减少每帧时间读取。
- 心跳循环并入 Writer，每连接协程从 3 个降为 2 个。
- 新增配置项：`send-queue-bytes`、`write-timeout`、`read-buffer-size`、`write-batch-frames`、`write-batch-bytes`。
- 输出 Benchmark/Profiling 报告、Phase 0 vs Phase 0.5 对比数据与 ADR-003/ADR-004。

非目标（留给 Phase 3）：gnet/netpoll、自研事件循环、多 Reactor、CPU 绑核、GC 参数硬调、服务端连接池。

## Impact

- Affected specs: `docs/通用实时消息 Gateway Platform.md` 第 5.6–5.10 节（Phase 0.5）。
- Affected code（全部在 `phase0.5/` 内，Phase 0 不改动）：
  - `phase0.5/go.mod`、`phase0.5/Makefile`
  - `phase0.5/internal/protocol/frame.go`、`frame_test.go`、新增 `bench_test.go`
  - `phase0.5/internal/transport/conn.go`、`server.go`、`server_test.go`、新增 `pool.go`、`bench_test.go`
  - `phase0.5/internal/config/config.go`
  - `phase0.5/cmd/gateway`、`cmd/client`、`cmd/loadtest`
  - `phase0.5/docs/README.md`、`phase0.5/docs/benchmark.md`、`phase0.5/docs/adr/ADR-003.md`、ADR-004
- 仓库级：`go.work` 增加 `use ./phase0.5`；根 `Makefile` 的 `PHASES` 增加 `phase0.5`。

## ADDED Requirements

### Requirement: 独立子项目

系统 SHALL 以 `phase0.5/` 作为独立 Go module 提供 Phase 0.5 实现，不得修改 `phase0/` 下的任何文件。

#### Scenario: 独立构建与测试
- **WHEN** 在 `phase0.5/` 目录执行 `go build ./...`、`go vet ./...`、`go test ./...`
- **THEN** 全部通过，且 `phase0/` 的测试仍独立通过

#### Scenario: 工作区接入
- **WHEN** 在仓库根执行 `make test` / `make race`
- **THEN** 同时覆盖 `phase0` 与 `phase0.5`

### Requirement: 基准与可观测性

系统 SHALL 提供可重复执行的 Benchmark 与 Profiling 入口，并在优化前记录基线。

#### Scenario: 协议与发送路径基准
- **WHEN** 执行 `go test -bench . -benchmem ./internal/protocol ./internal/transport`
- **THEN** 输出 Encode、Decode、Send、批量写路径的 `ns/op`、`B/op`、`allocs/op`，可重复运行且无外部依赖

#### Scenario: 基线可追溯
- **WHEN** 查看 `phase0.5/docs/benchmark.md`
- **THEN** 包含测试环境、命令、Phase 0 基线数据，以及每项优化后的对比数据

### Requirement: 缓冲池与低分配编码

系统 SHALL 通过 buffer 池与池化友好的编解码 API 降低每消息分配与拷贝，且不得改变线格式。

#### Scenario: 编码零额外拷贝到池化缓冲
- **WHEN** 调用 `AppendFrame(dst, f, maxBody)`，其中 `dst` 来自缓冲池
- **THEN** 直接在 `dst` 上追加完整帧，不新建整帧切片，CRC32 与字段布局与 Phase 0 一致

#### Scenario: 池化缓冲可归还
- **WHEN** Writer 完成一次 `Write` 或批写
- **THEN** 所用 buffer 被 Reset 后归还池，超过保留上限的大 buffer 不进入池

#### Scenario: 解码复用
- **WHEN** 调用 `DecodeInto(r, f, maxBody)` 并复用 `*Frame`
- **THEN** 成功解码后 `f` 字段正确，Body 缓冲区来自池且容量不超过上限

#### Scenario: 兼容性
- **WHEN** 用 Phase 0 客户端与 Phase 0.5 服务端（或反向）互通
- **THEN** DATA/PING/PONG/ERROR/CLOSE 与非法帧处理行为不变

### Requirement: 批量写与背压

系统 SHALL 在保持单 Writer 的前提下合并写、按字节限流，并保证控制帧不被数据帧长期阻塞。

#### Scenario: 批量合并
- **WHEN** 发送队列中同时存在多条待写帧
- **THEN** Writer 按 `write-batch-frames` / `write-batch-bytes` 上限合并，一次 `Write` 写出多条帧，帧边界与顺序不变

#### Scenario: 字节预算
- **WHEN** 队列中待写字节数超过 `send-queue-bytes`
- **THEN** 连接按背压策略关闭并返回错误，内存不随大帧数量线性放大

#### Scenario: 控制帧优先
- **WHEN** 数据帧积压且需要发送 PONG 或 CLOSE
- **THEN** 控制帧先于后续数据帧写出

#### Scenario: 写超时
- **WHEN** 对端长时间不读取导致写入超过 `write-timeout`
- **THEN** Writer 返回错误并关闭连接，不永久阻塞

### Requirement: 读路径与协程模型

系统 SHALL 降低读路径的定时器与分配开销，并将每连接协程数降为 2。

#### Scenario: 读 Deadline 低频重置
- **WHEN** 连接持续收到帧且距离 `idle-timeout` 仍有余量
- **THEN** 读循环不每帧调用 `SetReadDeadline`，空闲超时语义仍与 Phase 0 一致

#### Scenario: 读缓冲可配置
- **WHEN** 设置 `read-buffer-size`
- **THEN** 每连接按该值创建读缓冲，且不影响粘包/半包处理

#### Scenario: 两协程模型
- **WHEN** 建立 N 条连接并稳定运行
- **THEN** 服务端连接相关协程为 2N（读 + 写/心跳），心跳仍按 `heartbeat-interval` 发送 PING 并在超时后关闭连接

### Requirement: 量化验收

系统 SHALL 用前后对比数据证明优化有效，并保持 Phase 0 的质量门禁。

#### Scenario: 优化效果
- **WHEN** 对比 Phase 0 与 Phase 0.5 的 Benchmark 与千连接压测
- **THEN** 每消息内存分配数与写系统调用数有量化下降，P99 不劣化，报告记录差值

#### Scenario: 质量门禁
- **WHEN** 执行 `go test -race ./...` 与协程泄漏检查
- **THEN** 全部通过，测试结束后协程数相对基线偏差不超过 5

## MODIFIED Requirements

无（Phase 0 行为与协议保持兼容，仅在新子项目中优化实现）。

## REMOVED Requirements

无。
