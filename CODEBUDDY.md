# CODEBUDDY.md This file provides guidance to CodeBuddy when working with code in this repository.

## 仓库性质（先读这一条）

本仓库是**设计文档 + 分阶段落地代码**的混合体：

- `docs/通用实时消息 Gateway Platform - 架构设计方案.md`：架构主文档（下称「架构方案」），是全部语义契约的**单一事实来源**。
- `docs/S0 … S8 ... - 详细设计 LLD.md`：九个阶段的实现级详细设计。
- `docs/归档/`：三轮对抗性审查报告（v1/v2/v3），是历史过程记录，结论为 v3「无 P0，文档可作为实现基线，不阻塞 S0–S2 开工」。
- `s0/`：S0 传输层与协议的 Go 实现（独立 module `relayx/s0`），已通过 gofmt / vet / test / -race / bench 全量门禁与端到端验证。
- `s1/`：S1 单机 Gateway 核心的 Go 实现（独立 module `relayx/s1`），含四通道统一发送路径、鉴权前移、单播/多设备路由、`/metrics`+`/healthz`、`cmd/gateway` 与 `cmd/im`；已通过 gofmt / vet / test / -race 与三个验收场景端到端验证。
- `s2/`：S2 可靠消息内核的 Go 实现（独立 module `relayx/s2`），含统一受理入口、PostgreSQL/内存双 store、两个状态机与 CAS、确定性事实、只读查询 API、`cmd/gateway` / `cmd/scenario` v0 / `cmd/maintenance`；语义契约已冻结于 `docs/contracts/semantic-contract-v1.md`。已通过 gofmt / vet / test / -race、九个验收场景与真实 PostgreSQL 契约测试。
- `s3/`：S3 单机订阅与广播的 Go 实现（独立 module `relayx/s3`），复用 S1 协议快照与四通道骨架，含 `0x10`–`0x14` 帧、订阅 token 只验签、纯内存双向索引、共享只读字节引用计数（入队 +1 / 写完 -1 / 归零归还）、per-topic 有序扇出、P2 预算耗尽丢弃而非断连、`POST /v1/broadcasts`（202）与 `cmd/live` 六个验收场景；协议说明与 ADR 在 `s3/docs/`。已通过 gofmt / vet / test / -race / bench 与六个端到端场景。

S0、S1、S2、S3 已落地；**S4–S8 只有设计文档，尚无实现代码，也没有对应的 Go module**。不要为「S4+ 编译失败」寻找不存在的源码——实现后续阶段时应按对应 LLD 新建阶段 module。改动 `s0/`…`s3/` 时要跑代码验证；改动文档时主要维护**跨文档一致性**。

## 命令

### 当前可对文档运行的校验

环境为 Windows / PowerShell，仓库根目录为工作目录。文档之间存在强约束（帧号段、能力位掩码、幂等键、状态枚举、冻结表），改文档后应扫描一致性：

```powershell
# 列出全部现行文档与归档文档
Get-ChildItem -LiteralPath .\docs -Recurse -Filter *.md | ForEach-Object FullName

# 检查是否残留被禁止的旧措辞（应无输出；/v1/ 与 relayx.*.v1 等合法技术命名不算）
Get-ChildItem -LiteralPath .\docs -Recurse -Filter *.md |
  Select-String -Pattern '架构重设计方案|Architecture Redesign|重设计|差异总表|相对 v1|Phase 0–10'

# 核对能力位掩码全局一致（S1 冻结：CAP_TOPIC_SUBSCRIBE=0x02 / CAP_RELIABLE_POLICY=0x04 / CAP_CLIENT_EVENTS=0x08 / CAP_CLOSE_REASON=0x10）
Get-ChildItem -LiteralPath .\docs -Filter 'S*.md' |
  Select-String -Pattern 'CAP_(TOPIC_SUBSCRIBE|RELIABLE_POLICY|CLIENT_EVENTS|CLOSE_REASON)\s*=\s*0x[0-9A-F]+'
```

### 已落地代码：可运行门禁

S0 / S1 / S2 / S3 各是独立 Go module（Go 1.27+），工作目录分别为 `s0/`、`s1/`、`s2/`、`s3/`。改动对应目录后必须在该目录重跑：

```powershell
cd .\s0   # 或 cd .\s1 / .\s2 / .\s3
gofmt -w .                        # 格式化（S0–S8 每阶段通用）
go vet ./...                      # 静态检查（每阶段通用）
go test ./...                     # 单元/集成测试（每阶段通用）
go test -race ./...               # 并发代码强制竞态检查（S1–S8 强制项）
```

S2 额外有可选真实数据库契约测试（默认跳过，设置环境变量后运行）：

```powershell
$env:S2_TEST_DSN='postgres://relayx:relayx@127.0.0.1:55432/relayx_phase6?sslmode=disable'
go test ./internal/store/postgres
Remove-Item Env:S2_TEST_DSN
```

S0 额外有基准骨架：`go test -bench=Benchmark -benchmem ./benchmark/...`（见 S0 §9、S5 §3）；S3 的扇出基准为 `go test -bench=Benchmark -benchmem ./internal/broadcast`（1/100/1000 订阅者均 8 allocs/op，证明共享编码不随订阅者数线性增长）。

> 基准命令用 `-bench=Benchmark` 而非 `-bench=.`：本环境 PowerShell 会把 `-bench=.` 的 `=` 后的 `.` 当成包参数导致 `[setup failed]`。bash 脚本 `s0/benchmark/harness.sh` 内仍用标准 `-bench=.` 写法。

端到端验证：
- S0：`go run ./cmd/server -addr 127.0.0.1:9700 -enable-echo` + `go run ./cmd/echo-client -mode <roundtrip|fragmented|coalesced|truncated|oversized>`。基线结果在 `s0/benchmark/result-s0-baseline.txt`。
- S1：`go run ./cmd/gateway -addr 127.0.0.1:9800 -metrics-addr 127.0.0.1:9801` + `go run ./cmd/im -scenario <login-and-chat|multi-device|presence-simple>`；`/metrics` 与 `/healthz` 在 metrics 端口。
- S2：`go run ./cmd/gateway -addr 127.0.0.1:9800 -metrics-addr 127.0.0.1:9801 -access-addr 127.0.0.1:9802`（默认内存 store；生产用 `-store postgres -dsn ...`）+ `go run ./cmd/scenario -gateway 127.0.0.1:9800 -access 127.0.0.1:9802 -scenario <reliable-online-delivery|multi-device-fanout|offline-recovery|zero-endpoint-settles|business-ack-passthrough|ttl-expiry|retry-exhaustion|clock-skew-immunity|duplicate-settle-cas>`。需要时钟推进的场景必须给网关加 `-test-hooks`（仅本机内存模式提供 `/debug/clock`）。`/metrics` 与 `/healthz` 在 metrics 端口，`/v1/messages*` 在 access 端口。
- S3：`go run ./cmd/gateway -addr 127.0.0.1:9800 -metrics-addr 127.0.0.1:9801 -access-addr 127.0.0.1:9802` + `go run ./cmd/live -scenario <topic-fanout|fanout-order|unsubscribe-race|compat-old-client|shared-buffer-integrity|slow-consumer-drop>`；`/metrics` 与 `/healthz` 在 metrics 端口（含 `broadcast_*` 指标），`POST /v1/broadcasts` 在 access 端口。

### S4–S8 规划门禁（当前无代码）

以下命令来自各 LLD 的「验收标准」章节，描述后续阶段落地后必须通过的门禁；当前只有 S0、S1、S2、S3 有代码可运行：

```bash
gofmt -w .                 # 格式化（每阶段通用）
go vet ./...               # 静态检查（每阶段通用）
go test ./...              # 单元/集成测试（每阶段通用）
go test -race ./...        # 并发代码强制竞态检查（S1–S8 强制项）
go test -bench=. -benchmem ./...   # 基准骨架，S5 用于优化前后对比（见 S5 §3）
```

`cmd/echo-client`（S0）、`cmd/gateway` 与 `cmd/im`（S1）、`cmd/gateway`、`cmd/scenario` v0 与 `cmd/maintenance`（S2）、`cmd/gateway` 与 `cmd/live`（S3）已实现。`cmd/scenario` v1（S4 扩展）、`cmd/loadgen`（S5 起）仍是规划的验证客户端，尚未实现。

## 架构大图景

### 平台本质与边界

平台是**传输事实提供者（Transport Fact Provider）**，不是业务系统。输入是业务提交的「不透明字节 + 送达目标」；输出是**传输事实**（受理、推送、回执、失败、过期、结算），不是业务结论。平台明确不做：用户注册、好友/群组/会话、业务权限、payload 解析、已读与完成判定、通知聚合。任何让平台替业务回答「业务上算不算成功」的候选功能都属于越界（架构方案 §2.1）。

### 文档间的依赖结构

架构方案是顶点，其第四部分「语义契约」被所有 LLD 引用。**修改任何契约项必须同时更新架构方案 + 引用它的 LLD 的冻结清单**，否则会破坏一致性。各 LLD 开头都有「上位文档 / 前置契约 / 冻结产物 / 明确不做」四段，改某阶段前先读这四段确认边界。

### 语义契约（单一事实来源，冻结于 S2）

这是全项目最关键的资产，S2 已验收并冻结于 `docs/contracts/semantic-contract-v1.md`（架构方案 §4.1 的独立快照）。S3–S8 只允许引用、不得重新定义。要点（架构方案 §4.1，以原文为准）：

- **身份**：`tenant_id + producer_id`（用户令牌与服务令牌分流）+ 可选 `sender`。**来源端点** `source_endpoint = (producer_id, source_device_id)`，其中 `source_device_id` 由服务端从连接上下文派生并落库，客户端不可指定；仅客户端 `SEND` 有来源端点，服务令牌 / HTTP 提交写空串且不路由其事件。
- **幂等键**：接入 `(tenant_id, producer_id, client_msg_id)`；投递 `(tenant_id, message_id, user_id, device_id)`。
- **两个状态机分离，不得画成一张图**：消息级 `ACCEPTED → SETTLED`（唯一正常出口）/`EXPIRED`（唯一失败出口）；投递级 `PENDING → SENT → RECEIPT_ACKED`，终态 `RECEIPT_ACKED`/`FAILED`/`EXPIRED`。**投递级没有 `SETTLED`**，`SETTLED` 只属于消息级；无 `COMPLETED`。`SETTLED` 表示「平台侧已无更多投递动作」，不代表业务成功。
- **Business ACK** 只写投递级 `application_status` / `application_acked_at`，不进状态机、不改重试计划。
- **时间仲裁**：所有到期/超时判定在 SQL `now()` 内完成；Go 本地时钟只用于「多久扫一次」与日志/指标，**接口不得传入 `now` 参数**。
- **事实**：`event_id` 确定性派生 `hash(tenant, message_id, fact_type, endpoint_id, state_version)`；有发布水位 `delivery_facts.published_at`（NULL = 待发布），发布顺序 `(occurred_at, event_id)`，至少一次 + `event_id` 幂等。
- **聚合守卫**：`ACCEPTED → SETTLED` 必须 `fanout_done_at` 已置位（防止扇出未完成时误结算）。
- **策略**：`ttl_seconds`/`max_attempts`/`delivery_mode`/`fanout` 消息级可覆盖，平台有硬上限。
- **广播**：在线、Best-Effort、不重试、不补发、单 topic 内有序、不落库。
- **客户端事件**：在线 Best-Effort、self-scoped、默认只推聚合终态、不持久化；路由键为 `(tenant_id, producer_id, source_device_id)`。

三条贯穿性不变量：I1 语义先于传输（语义变更只能发生在冻结线之前）；I2 正确构造，不做已知错误的实现；I3 横切关注点在引入点落地（鉴权/TLS 随 S1 首个外部接口，mTLS 随 S6 多节点）。

### 统一发送路径（S1 定型，S2–S4 复用）

每个连接**单 Writer**（唯一 Socket 写者），出站帧按优先级分四通道、各自独立预算、互不占用：

- P0 CONTROL（PING/PONG/CLOSE）最高优先，不被拖死；
- P1 RELIABLE（可靠消息）帧数+字节双上限，满则背压/断连；
- P2 BROADCAST 约占 25%，满则丢弃+计数、不断连；
- P3 EVENT 独立份额，满则丢弃低优先级事件。

广播帧一次编码、N 个订阅者共享只读字节，**字节所有权用引用计数管理**：队列持有 +1、写完 -1、归零归还池。新增「寻址 × 语义」组合不得新造发送路径，只能接入已有通道（架构方案 §3.3）。这是 D5 的核心，改动发送逻辑前务必读 S1 §4。S1 已落地四通道骨架于 `s1/internal/sendpath`（P0–P3 优先级、双预算、单 Writer）；S3 已在 `s3/internal/sendpath/immutable.go` 与 `s3/internal/broadcast/encoder.go` 落地共享只读字节引用计数，全链路只传 `*ImmutableFrame`。

### 北极星拓扑与三平面

终局拓扑：客户端 → Gateway Cluster →（控制面）Access API / Message Service / Scheduler + Rebalancer →（数据面）Fan-out Worker / Delivery Worker / Client Event Router →（基础设施）PostgreSQL（messages/deliveries/facts/outbox）、Redis（路由/全局令牌桶）、Kafka（command/broadcast/facts/dlq 四种 subject）。节点间 mTLS，客户端 TLS + 令牌鉴权。

三平面职责：控制面失败可重试、不丢已受理消息；数据面 At-Least-Once + 幂等，Best-Effort 链路可丢但可观测；事实面与状态更新同事务写入、消费端按 `event_id` 幂等。Kafka 总线是**一个 Envelope + 四种 subject**（`event_id`/`subject`/`tenant_id`/`shard_key`/`occurred_at`/`schema_ver`/`payload`），共用一套消费/重试/幂等/观测框架（S7 §3）。

### 阶段划分与顺序约束

S0 传输层与协议 → S1 单机 Gateway 核心 → S2 单机可靠消息内核（**语义契约在此冻结**）→ S3 单机订阅与广播 → S4 Go SDK 与生命周期事件 → S5 单机性能工程 → S6 分布式 Gateway（只解决「连接在哪」）→ S7 分布式投递引擎与总线 → S8 生产就绪加固（**唯一一次门禁**）；S8 之后为千万级规模演进（无独立 LLD，由容量模型触发）。

顺序由依赖方向决定，三条硬约束：语义冻结线 S2 必须早于 S6/S7；统一发送路径 S1 必须早于 S2/S3/S4；S8 必须晚于全部功能阶段。S5 是单机功能冻结后唯一的性能窗口，只改实现效率、不动语义——若某项优化需要改语义，正确动作是停止并重开冻结线。

每个阶段采用**单一代码库 + 能力位 + 配置开关**渐进启用，阶段快照用 Git Tag，**不做目录级代码复制**。目录 `s0/`、`s1/`、`s2/`、`s3/` 已作为独立 Go module 落地（S1/S2/S3 的 `internal/protocol` 是 S0 冻结协议的快照复用，非阶段代码复制；S2 通过新增 body 布局与继承包实现阶段增量；S3 复用 S1 传输层骨架并新增 `internal/broadcast`）；`s4/`…`s8/` 出现在阶段表的「目录」列仍是规划命名，尚未创建。

### 端到端验证体系

三层客户端：L0 协议栈（帧编解码，S0，已落地于 `s0/internal/protocol` 与 `s0/cmd/echo-client`）→ L1 SDK（状态机/重连/去重/hooks/查询补齐，S1 有 `s1/cmd/im` 雏形、S4 正式）→ L2 验证客户端（IM、弹幕、场景运行器、负载生成器；S2 已交付 `s2/cmd/scenario` v0，仅 IM/可靠消息用例；S3 已交付 `s3/cmd/live`，覆盖六个广播场景）。硬约束：L2 不得绕过 L1 直接拼帧，仅 `cmd/scenario` 的协议一致性用例可用 L0 构造非法帧。验收判据是「客户端跑通场景且断言通过」，不是服务端日志看起来正确。IM 的「已结算」必须来自服务端事实，不得由客户端依据 RECEIPT_ACK 自行推导。

## 处理文档时的注意事项

- 修改契约相关措辞时，用架构方案 §4.1 作为准绳；LLD 中的「冻结清单」是契约的镜像，必须同步。
- `v1` 出现在 `/v1/messages`、`relayx.command.v1`、`cmd/loadgen v1`、`Semantic Contract v1`、`stateDiagram-v2` 中是合法技术命名，不是旧版本对比措辞，不要误删。
- 历史审查报告在 `docs/归档/`，是只读的过程记录；不要把它们当作现行设计基线引用。
- `.workbuddy/` 是项目数据目录，不要删除。
