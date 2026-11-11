# Phase 0：Go TCP 网络编程基础 Spec

## Why

项目当前只有规划文档，没有任何代码。Phase 0 的目标是用最小可运行代码打通 Go 标准库 TCP 服务端/客户端、自定义二进制协议、粘包半包处理、心跳与连接生命周期管理，为 Phase 1 的单机 Gateway 提供可靠的传输层与协议层基础。

## What Changes

- 新建单 module Go 项目骨架（`module relayx/phase0`，Go 1.27），作为独立子项目放在 `phase0/`，采用 `cmd/` + `internal/` 分层。
- 实现长度编码的二进制帧协议：Magic / Version / Type / Flags / MsgID / BodyLen / CRC32 / Body。
- 实现 TCP Server：多客户端接入、每连接读循环、单 Writer 协程 + 有界发送队列、连接注册与断线清理、心跳与超时、优雅退出。
- 实现 CLI 客户端：连接、发送、接收、自动回 PONG、退出。
- 实现协议单元测试（含粘包/半包/非法帧）与传输层集成测试，并通过 `go test -race ./...`。
- 完成 1,000 并发连接压测并记录结果；补充协议说明、ADR-001、ADR-002 与测试小结。

**范围约束（严格 Phase 0）**：不做 Session、身份绑定、消息路由、离线消息、数据库、Redis、Kafka、分布式 Gateway。服务端收到 DATA 帧仅做回显（echo）与日志，不转发给其他客户端。

## Impact

- Affected specs: `docs/通用实时消息 Gateway Platform.md` 中 Phase 0（5.1–5.5）与第九章「第一批任务」中属于 Phase 0 的条目。
- Affected code（全部为新增）：
  - `phase0/go.mod`
  - `phase0/cmd/gateway/main.go`
  - `phase0/cmd/client/main.go`
  - `phase0/internal/protocol/frame.go`
  - `phase0/internal/protocol/frame_test.go`
  - `phase0/internal/transport/server.go`
  - `phase0/internal/transport/conn.go`
  - `phase0/internal/transport/server_test.go`
  - `phase0/internal/config/config.go`
  - `phase0/docs/README.md`、`phase0/docs/adr/ADR-001.md`、`phase0/docs/adr/ADR-002.md`

## 关键设计决策

| 决策项 | 选择 | 说明 |
|---|---|---|
| 网络库 | 标准库 `net` | 建立可对比的基准版本，性能优化留到 Phase 3 |
| 帧头 | 21 字节定长，大端 | 完整帧头，后续阶段直接复用 |
| 写模型 | 每连接单一 Writer 协程 | 杜绝同一 Socket 并发写 |
| 发送队列 | 有界 channel，满则关闭连接 | 用背压而非无限缓冲保护内存 |
| 并发模型 | 每连接 1 读 + 1 写协程 | 结构直观，便于后续 Profiling |
| 日志 | 标准库 `log/slog` | 结构化日志，无第三方依赖 |

### 帧格式（Header 固定 21 字节）

| 字段 | 长度 | 说明 |
|---|---|---|
| Magic | 2 | 固定 `0x52 0x58`（"RX"） |
| Version | 1 | 固定 `0x01` |
| FrameType | 1 | `0x01` PING / `0x02` PONG / `0x03` DATA / `0x04` ERROR / `0x05` CLOSE |
| Flags | 1 | 保留字段，必须为 `0x00` |
| MsgID | 8 | uint64，客户端生成 |
| BodyLen | 4 | uint32，0 ~ `maxFrameBody`（默认 1 MiB） |
| CRC32 | 4 | IEEE 多项式，仅对 Body 计算 |
| Body | BodyLen | 平台不解析的字节载荷 |

## ADDED Requirements

### Requirement: 二进制帧编解码

系统 SHALL 提供 `phase0/internal/protocol` 包，把 `Frame` 编码为字节流并从 `io.Reader` 解码出完整帧，任何非法输入都返回可判断的错误而不是 panic。

#### Scenario: 正常编解码
- **WHEN** 对任意合法 `Frame`（Body 长度 0 ~ maxBody）执行 `Encode` 后由 `Decode` 读取
- **THEN** 得到与原值完全一致的 `MsgID`、`FrameType`、`Flags` 与 `Body`

#### Scenario: 拒绝非法帧
- **WHEN** 输入 Magic 错误、Version 不支持、Flags 非 0、BodyLen 超过上限或 CRC32 不匹配
- **THEN** `Decode` 返回对应的哨兵错误（`ErrBadMagic` / `ErrBadVersion` / `ErrBadFlags` / `ErrFrameTooLarge` / `ErrChecksum`），且不分配超过上限的缓冲区

#### Scenario: 粘包
- **WHEN** 单次读取源中包含两个连续完整帧
- **THEN** 连续两次 `Decode` 分别返回这两个帧，无数据丢失或串帧

#### Scenario: 半包
- **WHEN** 读取源每次只返回部分字节（含帧头跨两次读取、Body 分片）
- **THEN** `Decode` 阻塞等待直到读满整个帧后返回，结果与完整读取一致

#### Scenario: 对端关闭
- **WHEN** 读取源在帧中途返回 `io.EOF`
- **THEN** `Decode` 返回 `io.ErrUnexpectedEOF` 或 `io.EOF`，由调用方决定断开

### Requirement: TCP Server 连接管理

系统 SHALL 提供 `phase0/cmd/gateway` 与 `phase0/internal/transport`，支持多个客户端同时连接、收发 DATA 帧，并对每个连接维护完整生命周期。

#### Scenario: 多客户端并发连接
- **WHEN** 启动 gateway 后 100 个客户端同时连接并各发送 10 条 DATA 帧
- **THEN** 每个客户端都收到对应的回显帧，服务端活动连接数在全部断开后回到 0

#### Scenario: 单 Writer 串行写
- **WHEN** 多个业务来源同时向同一连接投递消息
- **THEN** 所有写入只发生在一个 Writer 协程中，`go test -race` 无数据竞争报告

#### Scenario: 慢消费者背压
- **WHEN** 发送队列已满
- **THEN** 服务端不会阻塞读取循环或无限增长内存，而是记录告警并关闭该连接

#### Scenario: 断线检测
- **WHEN** 客户端进程被强制终止或调用 `Close`
- **THEN** 服务端在 1 秒内感知读错误，注销连接、关闭发送队列并释放两个协程

### Requirement: 心跳与超时

系统 SHALL 支持服务端主动心跳与空闲超时，避免半开连接长期占用资源。

#### Scenario: 心跳正常
- **WHEN** 客户端正常在线并自动回复 PONG
- **THEN** 服务端按 `-heartbeat-interval` 周期发送 PING，连接保持不被关闭

#### Scenario: 心跳丢失
- **WHEN** 客户端超过 `-heartbeat-timeout` 未回复 PONG
- **THEN** 服务端判定连接失效并主动关闭，日志记录原因

#### Scenario: 空闲超时
- **WHEN** 连接超过 `-idle-timeout` 没有任何入站帧
- **THEN** 读循环因读 Deadline 到期而退出，并触发连接清理

### Requirement: 优雅关闭

系统 SHALL 在收到中断信号后停止接入新连接，并在限定时间内完成存量连接清理后退出。

#### Scenario: 进程收到 Ctrl+C
- **WHEN** 向 gateway 发送 `os.Interrupt`（Windows 下为 Ctrl+C）
- **THEN** 停止 Accept、向存量连接发送 CLOSE 帧、等待清理完成（默认 5 秒）后进程以 0 退出码结束，无残留协程

### Requirement: CLI 客户端

系统 SHALL 提供 `phase0/cmd/client`，可连接 gateway、周期发送消息、自动回复 PONG，并在收到 CLOSE 帧或信号时正常退出。

#### Scenario: 交互收发
- **WHEN** 运行 `go run ./cmd/client -addr 127.0.0.1:9000 -name demo -interval 1s`（在 `phase0/` 目录）
- **THEN** 终端持续打印服务端回显与服务端 PING/PONG 交互日志，Ctrl+C 后正常退出

### Requirement: 可验证的交付质量

系统 SHALL 提供可重复执行的测试与压测入口，并给出量化结果。

#### Scenario: 竞态与泄漏检查
- **WHEN** 执行 `go test -race ./...`
- **THEN** 全部通过；集成测试结束时活动协程数与测试前基线偏差不超过 5

#### Scenario: 千连接压测
- **WHEN** 执行压测入口，建立 1,000 个并发连接并保持心跳 60 秒
- **THEN** 生成记录文件，包含连接成功率、内存占用、协程数量与消息往返延迟分位数

## MODIFIED Requirements

无。

## REMOVED Requirements

无。
