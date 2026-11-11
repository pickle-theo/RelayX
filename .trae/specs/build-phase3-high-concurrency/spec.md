# Phase 3：单机高并发与性能优化 Spec

## Why

Phase 2 已经证明可靠投递在 1000 连接下可以闭环，但性能数据只覆盖单一规模（946 条/秒、P99 467ms），既不知道 1 万、5 万连接时的行为，也没有 Profiling 证据说明开销到底在哪里。Phase 3 要在不改变协议语义的前提下，用可重复的压测与 Profiling 找到真实瓶颈，完成至少两轮有数据对比的优化，并给出单机容量边界。

本阶段不是「重写网络层」，而是先用数据回答「慢在哪里」，再决定改什么；任何优化都必须有前后对比，无收益就回退。

## What Changes

- 新建独立子项目 `phase3/`，复制 Phase 2 代码作为基线；`phase0/`、`phase0.5/`、`phase1/`、`phase2/` 保持不变
- 新增 Linux 容器化压测环境与固定参数的场景矩阵（1k / 10k 必测，50k 尽力，100k 记录限制）
- Gateway 增加可关闭的 pprof 观测端点与运行时指标（协程、堆、GC、DB 连接）
- `cmd/loadtest` 扩展为多场景压测器：空闲、心跳、低频、高频、慢消费者、大 Payload、集中断线重连
- 完成至少两轮数据驱动的优化，覆盖写路径、读路径、心跳定时器、编解码、内存分配或锁竞争中的真实热点
- 用独立 Go module 做 gnet / netpoll 对比实验，结论只作为技术评估，不进入主线依赖
- 产出基线报告、优化报告、容量边界报告与 ADR-010 / ADR-011 / ADR-012

## Impact

- 受影响能力：传输层并发模型、协议编解码、Session 查找、可靠投递链路的资源占用
- 受影响代码：`phase3/internal/{transport,protocol,session,gateway,store,delivery}`、`phase3/cmd/{gateway,loadtest}`、`phase3/bench/`、`phase3/docs/`
- 不受影响：对外帧格式与编号、Phase 2 的可靠投递语义、数据库 schema

## 明确不做

- 不引入 Redis、Kafka、多节点路由、服务发现（留给 Phase 4 / 5）
- 不新增 WebSocket、TLS、鉴权等业务能力
- 不改变既有帧格式、帧编号与 ACK 语义
- 不把 gnet / netpoll 作为主线网络层；只做对比实验
- 不做 CPU 绑核、GC 参数硬调等不可复现的「优化」
- 不为了指标好看而筛选、截断或拼接压测数据

## ADDED Requirements

### Requirement: 独立子项目与基线一致性

系统 SHALL 在 `phase3/` 提供独立 Go module（`relayx/phase3`），内容为 Phase 2 的完整基线，根 `go.work` 与根 `Makefile` 的 `PHASES` SHALL 包含 `phase3`。复制后功能、协议与测试 SHALL 与 Phase 2 一致，且不得修改 `phase0/`、`phase0.5/`、`phase1/`、`phase2/`。

#### Scenario: 基线可构建可测试
- **WHEN** 在 `phase3/` 执行 `gofmt -l .`、`go vet ./...`、`go test ./...`
- **THEN** 无格式问题、无 vet 告警、测试全部通过（数据库用例按 `RELAYX_TEST_DSN` 规则 skip 或执行）

#### Scenario: 协议保持兼容
- **WHEN** 运行 Phase 2 遗留的协议与网关测试
- **THEN** 全部通过，帧编号、帧头与 Body 布局未发生任何变化

### Requirement: Linux 容器化压测环境

所有基准数据 SHALL 在 Linux 容器内采集，避免 Windows IOCP 与 Linux epoll 数据混用。环境 SHALL 提供可重复执行的脚本与 Makefile 目标，固定并记录 `ulimit -n`、连接数、爬坡时间、发送间隔、Payload 大小、时长、Go 版本、内核版本、CPU/内存、Docker 版本。

#### Scenario: 一条命令复现某档压测
- **WHEN** 执行 `make bench-10k`（或等价脚本）
- **THEN** 生成包含环境信息、参数、原始指标与退出码的报告文件，重复执行得到同口径数据

#### Scenario: 环境差异被显式记录
- **WHEN** 查看任一压测报告
- **THEN** 能明确看到 Gateway、负载生成器、PostgreSQL 是否同机、CPU 核数与限制、以及该结果不能外推的原因

### Requirement: 可观测性与 Profiling

Gateway SHALL 支持通过 `-pprof-addr` 开启 `net/http/pprof`（默认关闭），暴露 CPU、heap、goroutine、block、mutex 与 trace；SHALL 支持配置 `block-profile-rate` 与 `mutex-profile-fraction`。周期性 stats SHALL 增加协程数、`heap_alloc`、`heap_sys`、GC 次数与最近一次 GC 暂停、活跃连接/会话、DB 连接数。未开启 pprof 时主路径行为 SHALL 不变。

#### Scenario: 稳定运行时采集 profile
- **WHEN** 10k 连接稳定运行且访问 `/debug/pprof/profile`、`/debug/pprof/heap`、`/debug/pprof/goroutine`
- **THEN** 能获得可分析的 profile，服务端错误计数不增加

#### Scenario: pprof 关闭无副作用
- **WHEN** 不设置 `-pprof-addr`
- **THEN** 不监听额外端口，压测指标与 Phase 2 基线同口径

### Requirement: 压测场景矩阵

`cmd/loadtest` SHALL 支持以下场景，并输出每个场景的建连成功率、吞吐、RTT 分位（p50/p90/p99/max）、错误分类、客户端资源占用：

- `idle`：建立连接后不发送业务消息
- `heartbeat`：仅依赖心跳保活
- `steady-low`：低频消息（每连接数秒一条）
- `steady-high`：高频消息（每连接每秒一条或更高）
- `slow-consumer`：指定比例连接不读取，验证背压与内存不增长
- `large-payload`：64KB / 1MB Payload
- `reconnect-storm`：集中断开并重连

连接规模 SHALL 至少覆盖 1,000 与 10,000；50,000 为尽力目标；100,000 只记录尝试结果与环境限制，不作为验收门槛。

#### Scenario: 场景可独立执行
- **WHEN** 以 `-scenario slow-consumer` 启动压测
- **THEN** 报告能区分慢消费者数量、被背压丢弃/关闭的连接数，且 Gateway 内存不随时间单调增长

#### Scenario: 集中重连可恢复
- **WHEN** 执行 `reconnect-storm`
- **THEN** 所有客户端在超时内重新建连并登录成功，服务端协程数回落且无泄漏

### Requirement: 基线测量与瓶颈分析

在修改任何性能相关代码前，系统 SHALL 先完成 Phase 2 代码在 1k 与 10k 下的基线测量，并保留原始输出。分析 SHALL 结合 CPU profile、`alloc_space`、goroutine、block/mutex profile 与 trace，给出**至少三个**有证据的真实瓶颈；每个瓶颈必须包含现象、指标、profile 证据、影响范围与候选方案。

#### Scenario: 瓶颈有据可查
- **WHEN** 阅读基线分析报告
- **THEN** 每个瓶颈都能对应到具体 profile 热点、指标变化或可复现实验，而不是主观判断

#### Scenario: 先测后改
- **WHEN** 检查优化提交顺序
- **THEN** 基线数据早于第一轮优化代码，且基线参数与复测参数一致

### Requirement: 至少两轮有数据对比的优化

系统 SHALL 完成至少两轮优化。每轮 SHALL 按「假设 → 改动 → 同参数复测 → 前后对比 → 保留或回退」执行，并记录到优化报告。至少两项核心指标 SHALL 相对基线有可量化改进（目标 ≥20%）；若未达到，SHALL 给出数据解释并明确回退或保留的理由。优化不得改变协议语义，且全部功能测试与 `-race` 必须保持通过。

候选优化方向（按数据选择，不要求全部实施）：写路径批量与队列、读路径 deadline 与缓冲、心跳定时器合并、编解码分配、Session 查找锁、GC 压力与对象复用、连接建立/关闭路径。

#### Scenario: 每轮可复核
- **WHEN** 查看任一轮优化
- **THEN** 能找到改动点、对应假设、同参数前后数据和结论

#### Scenario: 无收益即回退
- **WHEN** 某项改动复测无改进或引入回归
- **THEN** 代码回退，并在报告中记录被否决的方案与数据

### Requirement: gnet / netpoll 对比实验

系统 SHALL 在 `phase3/experiments/netlib-compare/` 提供独立 Go module 的对比实验，实现与主项目可对比的最小 echo 服务，并使用同一套压测参数在 1k 与 10k 下测量吞吐、P99、内存与协程模型。实验模块 SHALL NOT 进入 `phase3` 主 module 的依赖。

#### Scenario: 对比结论明确
- **WHEN** 阅读 ADR-012
- **THEN** 能回答：在本机环境下第三方网络库是否带来可量化收益、代价是什么、在什么条件下才值得替换

#### Scenario: 主线依赖不膨胀
- **WHEN** 执行 `go list -m all`（`phase3` 主 module）
- **THEN** 不包含 gnet、netpoll 或实验模块的依赖

### Requirement: 容量边界与资源账本

系统 SHALL 记录 50k 连接的最大稳定规模与失败原因，并尝试 100k 记录环境限制。报告 SHALL 给出每连接内存、协程数、文件描述符、临时端口与 TIME_WAIT 等资源账本，以及达到边界时的首个瓶颈。

#### Scenario: 边界可解释
- **WHEN** 50k 或 100k 未达到目标
- **THEN** 报告给出具体失败阶段（建连、登录、心跳、消息）与资源上限证据，而不是笼统的「机器不行」

### Requirement: 文档、ADR 与质量门禁

系统 SHALL 提供 `phase3/docs/README.md`、基线报告、优化报告、容量报告与 ADR-010（压测方法与 Linux 容器基准）、ADR-011（瓶颈与优化决策）、ADR-012（标准库 vs gnet/netpoll）。最终 SHALL 通过 `gofmt`、`go vet`、`go test ./...`、Docker `go test -race ./...`，协程泄漏相对基线偏差不超过 5。

#### Scenario: 门禁全绿
- **WHEN** 执行最终质量门禁
- **THEN** 全部命令退出码为 0，且报告记录实际输出

## MODIFIED Requirements

### Requirement: 每连接协程与心跳模型

Phase 2 的约束是每连接 2 个协程（读 + 写）且心跳由写协程用独立 Timer 驱动。Phase 3 SHALL 保持「单 Socket 单 Writer、无并发写、心跳可被及时发送」的正确性约束；允许在数据证明下改为全局时间轮或批量心跳，但 SHALL 证明：无并发写、无心跳饥饿、协程泄漏不高于基线、慢消费者不影响其他连接。

#### Scenario: 模型变更可验证
- **WHEN** 心跳或协程模型被修改
- **THEN** 既有心跳、超时、慢消费者、单写者测试全部通过，并新增对应的并发与泄漏测试

## REMOVED Requirements

无。Phase 3 不删除 Phase 2 的任何能力。
