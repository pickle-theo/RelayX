# RelayX

通用实时消息 Gateway Platform —— 一个只负责「传输事实」的实时消息投递平台。

## 项目定位

平台是**传输事实提供者（Transport Fact Provider）**，不是业务系统：

- **输入**：业务提交的不透明字节 + 送达目标
- **输出**：传输事实（受理、推送、回执、失败、过期、结算），不是业务结论

平台**明确不做**：用户注册、好友/群组/会话、业务权限、payload 解析、已读与完成判定、通知聚合。任何让平台替业务回答「业务上算不算成功」的功能都属于越界。

三条贯穿性不变量：

- **I1 语义先于传输**：语义变更只能发生在冻结线之前
- **I2 正确构造**：不做已知错误的实现
- **I3 横切关注点在引入点落地**：鉴权/TLS 随首个外部接口，mTLS 随多节点

## 仓库结构

本仓库是**设计文档 + 分阶段落地代码**的混合体：

```
docs/                           设计文档（语义契约的单一事实来源）
  └─ 通用实时消息 Gateway Platform - 架构设计方案.md
  └─ S0 … S8 ... - 详细设计 LLD.md
  └─ contracts/semantic-contract-v1.md    冻结的语义契约快照
  └─ 归档/                                 三轮对抗性审查报告（只读过程记录）

s0/                             传输层与协议          Go module `relayx/s0`
s1/                             单机 Gateway 核心     Go module `relayx/s1`
s2/                             可靠消息内核          Go module `relayx/s2`
s3/                             单机订阅与广播        Go module `relayx/s3`
s4/                             Go SDK 与生命周期事件  Go module `relayx/s4`
```

每个阶段是**独立 Go module**（Go 1.27+），由能力位与配置开关渐进启用，阶段快照用 Git Tag，不做目录级代码复制。S1/S2/S3 的 `internal/protocol` 是 S0 冻结协议的快照复用；S2 通过新增 body 布局与继承包实现阶段增量；S3 复用 S1 传输层骨架并新增 `internal/broadcast`；S4 把协议提升为公共 `protocol/` 供 `sdk/` 引用，新增 `internal/projector` / `internal/query` / `internal/core`（S2 兼容事实生产者）与公共 `sdk/`。

## 阶段进展

| 阶段 | 内容 | 状态 |
| --- | --- | --- |
| S0 | 传输层与协议：帧编解码、分片/合并、截断与超限处理 | 已实现 |
| S1 | 单机 Gateway 核心：四通道统一发送路径、鉴权前移、单播/多设备路由 | 已实现 |
| S2 | 可靠消息内核：统一受理入口、双 store、两个状态机与 CAS、确定性事实 | 已实现，**语义契约在此冻结** |
| S3 | 单机订阅与广播：`0x10`–`0x14` 帧、共享只读字节引用计数、per-topic 有序扇出 | 已实现 |
| S4 | Go SDK 与生命周期事件：`0x20`–`0x22` 帧、CLOSE 原因码、事件投影、端侧查询 | 已实现 |
| S5 | 单机性能工程 | 仅设计 |
| S6 | 分布式 Gateway（只解决「连接在哪」） | 仅设计 |
| S7 | 分布式投递引擎与总线 | 仅设计 |
| S8 | 生产就绪加固（唯一一次门禁） | 仅设计 |

S5–S8 目前只有设计文档，没有对应 Go module；实现后续阶段时应按对应 LLD 新建阶段 module。

## 核心设计要点

### 语义契约（冻结于 S2）

完整快照见 [`docs/contracts/semantic-contract-v1.md`](docs/contracts/semantic-contract-v1.md)，S3–S8 只允许引用、不得重新定义：

- **身份**：`tenant_id + producer_id`（用户令牌与服务令牌分流）+ 可选 `sender`；来源端点 `(producer_id, source_device_id)`，其中 `source_device_id` 由服务端从连接上下文派生，客户端不可指定
- **幂等键**：接入 `(tenant_id, producer_id, client_msg_id)`；投递 `(tenant_id, message_id, user_id, device_id)`
- **两个状态机分离**：消息级 `ACCEPTED → SETTLED`/`EXPIRED`；投递级 `PENDING → SENT → RECEIPT_ACKED`，终态含 `FAILED`/`EXPIRED`。投递级**没有** `SETTLED`，全局**没有** `COMPLETED`
- **时间仲裁**：所有到期/超时判定在 SQL `now()` 内完成，接口不得传入 `now` 参数
- **事实**：`event_id` 确定性派生，发布顺序 `(occurred_at, event_id)`，至少一次 + `event_id` 幂等
- **聚合守卫**：`ACCEPTED → SETTLED` 必须 `fanout_done_at` 已置位
- **广播**：在线、Best-Effort、不重试、不补发、单 topic 内有序、不落库

### 统一发送路径（S1 定型）

每个连接**单 Writer**（唯一 Socket 写者），出站帧按优先级分四通道，各自独立预算、互不占用：

| 通道 | 内容 | 预算耗尽时 |
| --- | --- | --- |
| P0 CONTROL | PING/PONG/CLOSE | 最高优先，不被拖死 |
| P1 RELIABLE | 可靠消息 | 背压 / 断连 |
| P2 BROADCAST | 广播（约占 25%） | 丢弃 + 计数，不断连 |
| P3 EVENT | 客户端事件 | 丢弃低优先级事件 |

新增「寻址 × 语义」组合不得新造发送路径，只能接入已有通道。广播帧一次编码、N 个订阅者共享只读字节，字节所有权由引用计数管理（队列持有 +1 / 写完 -1 / 归零归还池）。

## 快速开始

前置：Go 1.27+。以下命令在各自阶段目录下执行（Windows / PowerShell 示例）。

### S0 传输层与协议

```powershell
cd .\s0
go run ./cmd/server -addr 127.0.0.1:9700 -enable-echo
# 另开终端
go run ./cmd/echo-client -mode roundtrip   # 或 fragmented | coalesced | truncated | oversized
```

基线结果见 [`s0/benchmark/result-s0-baseline.txt`](s0/benchmark/result-s0-baseline.txt)。

### S1 单机 Gateway 核心

```powershell
cd .\s1
go run ./cmd/gateway -addr 127.0.0.1:9800 -metrics-addr 127.0.0.1:9801
# 另开终端
go run ./cmd/im -scenario login-and-chat   # 或 multi-device | presence-simple
```

### S2 可靠消息内核

```powershell
cd .\s2
go run ./cmd/gateway -addr 127.0.0.1:9800 -metrics-addr 127.0.0.1:9801 -access-addr 127.0.0.1:9802 -test-hooks
# 另开终端
go run ./cmd/scenario -gateway 127.0.0.1:9800 -access 127.0.0.1:9802 -scenario reliable-online-delivery
```

默认内存 store；生产使用 `-store postgres -dsn ...`。需要时钟推进的场景必须加 `-test-hooks`（仅本机内存模式提供 `/debug/clock`）。

可选真实数据库契约测试：

```powershell
$env:S2_TEST_DSN='postgres://relayx:relayx@127.0.0.1:55432/relayx_phase6?sslmode=disable'
go test ./internal/store/postgres
Remove-Item Env:S2_TEST_DSN
```

### S3 单机订阅与广播

```powershell
cd .\s3
go run ./cmd/gateway -addr 127.0.0.1:9800 -metrics-addr 127.0.0.1:9801 -access-addr 127.0.0.1:9802
# 另开终端
go run ./cmd/live -scenario topic-fanout
```

可用场景：`topic-fanout`、`fanout-order`、`unsubscribe-race`、`compat-old-client`、`shared-buffer-integrity`、`slow-consumer-drop`。协议说明见 [`s3/docs/broadcast-protocol.md`](s3/docs/broadcast-protocol.md)，架构决策记录见 [`s3/docs/adr/`](s3/docs/adr/)。

### S4 Go SDK 与生命周期事件

```powershell
cd .\s4
go run ./cmd/gateway -addr 127.0.0.1:9800 -metrics-addr 127.0.0.1:9801 -access-addr 127.0.0.1:9802 -test-hooks
# 另开终端：五个验收场景
go run ./cmd/scenario -gateway 127.0.0.1:9800 -access http://127.0.0.1:9802 -scenario all
```

可用场景：`lifecycle-hooks`、`close-reasons`、`message-settled-event`、`event-gap-query`、`compat-matrix`。两个客户端均改用 SDK：`go run ./cmd/im -mode send|receive ...`、`go run ./cmd/live ...`。协议与投影说明见 [`s4/docs/client-events.md`](s4/docs/client-events.md)，SDK API 见 [`s4/docs/sdk-api.md`](s4/docs/sdk-api.md)，投影 ADR 见 [`s4/docs/adr/`](s4/docs/adr/)。

## 质量门禁

改动某个阶段目录后，必须在该目录重跑全部门禁：

```powershell
gofmt -w .
go vet ./...
go test ./...
go test -race ./...        # 并发代码强制项
```

S4 的事件投影与 SDK 测试包含在 `go test ./...` / `go test -race ./...` 中（协议、内核、投影、查询、SDK、网关集成）。

基准骨架（S0 / S3 已有）：

```powershell
go test -bench=Benchmark -benchmem ./benchmark/...        # s0
go test -bench=Benchmark -benchmem ./internal/broadcast  # s3
```

> 用 `-bench=Benchmark` 而非 `-bench=.`：PowerShell 会把 `-bench=.` 中 `=` 后的 `.` 当作包参数。

验收判据是「客户端跑通场景且断言通过」，不是服务端日志看起来正确。

## 文档指引

- 从 [`docs/通用实时消息 Gateway Platform - 架构设计方案.md`](docs/通用实时消息%20Gateway%20Platform%20-%20架构设计方案.md) 开始，它是全部语义契约的单一事实来源
- 每个 LLD 开头的「上位文档 / 前置契约 / 冻结产物 / 明确不做」四段用于确认阶段边界
- 修改任何契约项必须同时更新架构方案与被引用 LLD 的冻结清单
- `docs/归档/` 是只读的审查过程记录，不作为现行设计基线

## License

[MIT](LICENSE)
