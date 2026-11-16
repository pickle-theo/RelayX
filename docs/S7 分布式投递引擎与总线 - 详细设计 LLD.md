# S7 分布式投递引擎与三条总线
# 详细设计（Low-Level Design）

> 本文是《通用实时消息 Gateway Platform — 架构设计方案 v1》中 **S7 阶段**的实现级展开。
>
> - **上位文档**：`通用实时消息 Gateway Platform - 架构设计方案.md`（架构方案）
> - **前置契约**：S2（可靠语义 + facts schema）、S3（广播语义）、S4（事件 schema）、S6（节点路由 + mTLS）全部冻结。
> - **阶段定位**：把单机 Delivery Core 演进为分布式可靠投递系统，并把广播与客户端事件接入总线。**只替换传输与调度，不改变任何语义**。
> - **明确不做**：全局限流（S8）、安全基线与审计（S8）、可观测四件套完善（S8）、优雅停机完善（S8）、数据生命周期（S8）、分库分表与多地域（终局）。

| 项 | 内容 |
|---|---|
| 阶段编号 | S7 |
| 依赖阶段 | S2–S6（语义与路由冻结） |
| 核心机制 | Kafka + Transactional Outbox、Worker Lease、虚拟分片、统一 Envelope |
| 三条数据流 | 可靠分发（command）、广播（broadcast）、事实（facts） |
| 新增进程 | `message-service`、`fanout-worker`、`delivery-worker`、`client-event-router`、维护 CLI |
| 硬约束 | 语义零变更；复用 S6 节点路由与 mTLS；事实 schema 不变 |

---

## 目录

1. [设计目标与契约边界](#1-设计目标与契约边界)
2. [模块划分与进程拓扑](#2-模块划分与进程拓扑)
3. [统一 Envelope 与 Topic 设计](#3-统一-envelope-与-topic-设计)
4. [可靠消息链路：Outbox + Worker](#4-可靠消息链路outbox--worker)
5. [Worker 租约与虚拟分片](#5-worker-租约与虚拟分片)
6. [跨节点广播](#6-跨节点广播)
7. [客户端事件总线](#7-客户端事件总线)
8. [幂等边界与一致性](#8-幂等边界与一致性)
9. [语义零变更约束](#9-语义零变更约束)
10. [故障处理矩阵](#10-故障处理矩阵)
11. [可观测性与本阶段限流](#11-可观测性与本阶段限流)
12. [测试用例设计](#12-测试用例设计)
13. [验收标准与冻结清单](#13-验收标准与冻结清单)

---

# 1. 设计目标与契约边界

## 1.1 本阶段要解决的问题

把单机 Delivery Core 演进为独立的分布式可靠投递系统，同时把 S3 的广播与 S4 的客户端事件接入分布式总线。

本阶段是「替换传输，不改变语义」的集中体现：

- 可靠消息的语义在 S2 已定（状态机、幂等键、facts）。
- 广播的语义在 S3 已定（Best-Effort、单 topic 有序、不落库）。
- 客户端事件的协议在 S4 已定（self-scoped、聚合终态、event schema）。
- 节点路由与 mTLS 在 S6 已定。

S7 只增加 Kafka、Outbox、租约、分片与跨节点转发。

## 1.2 统一总线设计

本方案采用「一个 Envelope + 四种 subject」的统一总线设计：

- command / broadcast / facts / dlq 四种 subject 共用**一套消费/重试/幂等/观测框架**。
- 语义绑定在 subject + payload schema 上，schema 来自冻结契约。

收益：重试、DLQ、幂等、观测只实现一次。

## 1.3 契约落地对照

| 冻结契约 | 本阶段处理 |
|---|---|
| S2 `delivery_facts` schema | **不变**：Relay 只把 facts 搬到 Kafka，字段与 `event_id` 原样；**新增使用** `published_at` 作为出口水位（列在 S2 已建） |
| S2 幂等键 | **不变**：消费端按既有键去重 |
| S2 `messages.source_device_id` | **不变**：受理写入；随 `message.*` 事实的 `payload_summary` 带出，供 router 使用 |
| S2 `messages.fanout_done_at` | **新增写入方**：由 fanout-worker 在 fanout 事务内置位（S2 §4.1 已登记为例外） |
| S3 广播语义 | **不变**：分区键 `tenant\|topic` 保单 topic 有序，扇出逻辑沿用 |
| S4 事件 schema | **不变**：router 直接投影 facts，帧定义零改动 |
| S6 节点路由 + mTLS | **复用**：回推 Gateway 走 S6 通道 |
| **deliveries 创建者** | **变更（唯一的语义接缝）**：从 S2 受理事务改为 fanout 事务，已登记在 S2 §4.1 例外表 |

---

# 2. 模块划分与进程拓扑

## 2.1 进程拓扑

```mermaid
graph TB
    API[Access API / Message Service]
    PG[(PostgreSQL<br/>messages/deliveries/facts/outbox)]
    KF[[Kafka<br/>relayx.v1: command/broadcast/facts/dlq]]

    subgraph Workers["投递层"]
        RELAY[Outbox Relay<br/>group=relayx.outbox]
        FAN[Fan-out Worker<br/>group=relayx.fanout]
        DW[Delivery Worker 池<br/>DB 分片扫描者，非 Kafka 消费者]
        ER[Client Event Router<br/>group=relayx.event-router]
    end

    RD[(Redis<br/>节点路由/租约协调)]
    GW[Gateway Cluster<br/>S6 继承 + S3 本地扇出]
    SC[Shard Scheduler]

    API -->|受理：message + message.accepted + outbox(command)| PG
    API -->|broadcast publish| KF
    PG --> RELAY --> KF
    KF -->|command 唯一消费者| FAN
    KF -->|facts| ER
    KF -->|broadcast 全量消费，每 Gateway 独立 group| GW
    FAN -->|建 deliveries + delivery.created + fanout_done_at| PG
    DW -->|SQL 扫描到期 deliveries（无 Kafka 依赖）| PG
    DW -->|查路由| RD
    DW -->|回推 DELIVER| GW
    ER -->|回推 CLIENT_EVENT| GW
    SC -->|分配分片/租约| PG
    SC --> RD
```

## 2.2 关键变化

1. **Message Service 与投递解耦**：受理只写 `message` + `message.accepted` 事实 + `outbox(command)`，**不建 deliveries**；投递行由 Fan-out Worker 在自己的事务内创建。
2. **统一 Envelope**：四类 subject 共用一套 schema 与消费框架。
3. **Fan-out Worker**：`command` 的**唯一消费者**，按 `tenant|user` 分区展开多设备投递，并置 `fanout_done_at`（含 0 端点即结算）。
4. **Delivery Worker**：DB 分片扫描者（不是 Kafka 消费者），租约 + 虚拟分片并行，回推 Gateway；**取代 S2 的 `RetryScheduler`**。
5. **Client Event Router**：消费 facts，投影为客户端事件（复用 S4 schema）。

### 2.2.1 subject ↔ 消费者 ↔ 消费者组（唯一事实）

| subject | 唯一消费者 | 消费者组 | offset 语义 | 重复消费边界 |
|---|---|---|---|---|
| `relayx.command.v1` | `fanout-worker` | `relayx.fanout` | 处理完再提交；重复消费由投递幂等键 + `fanout_done_at` 吸收 | 至少一次；不会产生重复 delivery |
| `relayx.broadcast.v1` | 每个 Gateway | `relayx.gw.{node_id}`（每组独立） | 全量消费，跳过超新鲜度窗口的积压 | 至少一次；广播本身 Best-Effort |
| `relayx.delivery-facts.v1` | `client-event-router` | `relayx.event-router` | 处理完再提交 | 重复投影由 `event_id` 幂等吸收 |
| `relayx.dlq.v1` | 维护 CLI（人工/定时重放） | — | 无自动消费 | 重放幂等由原 subject 的键保证 |

**关键边界**：`relayx.command.v1` 的消费者**只有** `fanout-worker`。`delivery-worker` 不订阅任何 topic；它通过 SQL `next_attempt_at <= now()` 领取任务。这样"谁负责扇出"与"谁负责投递"各有唯一归属，Kafka 分区数与 Worker 并行度之间也不存在隐含耦合（并行度由虚拟分片数决定）。

## 2.3 目录结构

```text
s7/
├── cmd/
│   ├── message-service/       # 受理 API
│   ├── outbox-relay/          # Outbox → Kafka
│   ├── fanout-worker/
│   ├── delivery-worker/
│   ├── client-event-router/
│   └── maintenance/           # DLQ 重放、租约修复
├── internal/
│   ├── envelope/              # 统一 Envelope + subject 路由
│   ├── consumer/              # 统一消费框架（重试/DLQ/幂等）
│   ├── outbox/
│   ├── worker/
│   │   ├── lease.go           # Worker Lease
│   │   ├── shard.go           # 虚拟分片
│   │   └── pipeline.go        # 并行 Pipeline
│   ├── broadcast/             # Kafka 广播消费 + 本地扇出（复用 S3）
│   ├── router/                # 客户端事件投影（复用 S4）
│   └── config/
├── migrations/
│   └── 0002_outbox_shards.sql
├── docs/
│   ├── distributed-delivery.md
│   ├── envelope-schema.md
│   └── adr/
└── test/
    ├── integration/
    ├── failure/
    └── consistency/
```

## 2.4 核心接口

```go
// ===== 统一 Envelope =====
type Envelope struct {
    EventID     string            // 幂等键
    Subject     Subject           // command/broadcast/facts/dlq
    TenantID    string
    ShardKey    string            // 分区键
    OccurredAt  time.Time
    SchemaVer   int
    Payload     []byte            // subject-specific schema
}

type Subject string
const (
    SubCommand   Subject = "relayx.command.v1"
    SubBroadcast Subject = "relayx.broadcast.v1"
    SubFacts     Subject = "relayx.delivery-facts.v1"
    SubDLQ       Subject = "relayx.dlq.v1"
)

// ===== 统一消费框架 =====
type Consumer interface {
    Consume(ctx context.Context, subject Subject, handler Handler) error
    // 内置：错误分级、退避重试、DLQ、幂等、supervisor（基础版，S8 完善）
}

type Handler interface {
    Handle(ctx context.Context, env Envelope) error
}

// ===== 租约与分片 =====
type LeaseManager interface {
    Acquire(ctx context.Context, shardID int, workerID string, ttl time.Duration) (bool, error)
    Renew(ctx context.Context, shardID int, workerID string) error
    Release(ctx context.Context, shardID int, workerID string) error
}

type ShardAllocator interface {
    Assignments(ctx context.Context, workerID string) ([]int, error)
    ShardOf(key string) int            // 稳定哈希 → 虚拟分片
}
```

---

# 3. 统一 Envelope 与 Topic 设计

## 3.1 Envelope 字段约束

- `EventID` 全局唯一，消费端幂等键；facts 来源复用 S2 `event_id` 派生规则。
- `ShardKey` 决定分区与分片：`command` 用 `tenant|user`，`broadcast` 用 `tenant|topic`。
- `SchemaVer` 冻结后只允许追加兼容字段；与 S2 facts schema 版本对齐。
- `Payload` 按 subject 使用冻结 schema，平台不解析业务 payload。

## 3.2 Topic 配置

| subject | 分区键 | 分区数 | RF | 保留 |
|---|---|---|---|---|
| `relayx.command.v1` | `tenant\|user` | 按容量模型 | 3 | 按策略 |
| `relayx.broadcast.v1` | `tenant\|topic` | 24（生产）/ 3（本地） | 3 / 1 | 10 分钟 |
| `relayx.delivery-facts.v1` | `tenant\|message` | 按容量模型 | 3 | 按保留策略 |
| `relayx.dlq.v1` | 原 subject | 同原 | 3 | 较长 |

- 生产 topic 预创建，不自动扩分区（避免单 topic 顺序被打破）。
- 广播分区键保证**单 topic 有序**（S3 语义）。

## 3.3 统一消费框架

所有 subject 共用一个 `Consumer`，内置：

- 错误分级：可重试（超时、连接中断）vs 不可重试（反序列化失败、协议非法）。
- 退避重试（基础指数退避 + jitter；S8 完善 supervisor 与 DLQ 重放）。
- 幂等：按 `EventID` 去重。
- 基础 DLQ 写入（S8 完善重放 CLI）。

---

# 4. 可靠消息链路：Outbox + Worker

## 4.1 受理解耦（受理只负责"消息存在"）

```text
Message Service 受理（替代 S2 的同步投递）：
  1. 校验身份与策略（复用 S2 契约）；派生 source_device_id（服务端侧，客户端不可指定）
  2. 事务内（只做三件事）：
     a. INSERT message（幂等，含 source_device_id、expires_at=now()+ttl）
     b. INSERT delivery_facts: message.accepted
        （payload_summary 携带 producer_id + source_device_id，供事件路由使用）
     c. INSERT outbox(event_id, subject=command, payload={message_id, tenant, user})
     d. COMMIT
  3. 返回受理（202/ACK），**此时 deliveries 尚不存在**
```

> **关键点**：Outbox 与消息在同一事务提交，杜绝「消息已受理但从未投递」。
>
> **受理事务里没有端点解析、也没有 deliveries**：`EndpointRegistry.Resolve` 需要读 `endpoints` 表并可能触及大量设备，把它塞进受理事务会让受理延迟与用户设备数线性相关；更关键的是，一旦 fanout 异步化，"受理时建的 deliveries"与"fanout 时建的 deliveries"就变成两个可能同时发生的写入方。因此归属必须唯一：**deliveries 只由 fanout 创建**。

## 4.2 三条链路的异步化（归属唯一）

| 链路 | 触发 | 唯一执行者 | 事务内动作 | 事务外动作 |
|---|---|---|---|---|
| Fan-out | 受理写 `outbox(command)` → Relay 发布 → 消费 | `fanout-worker`（group `relayx.fanout`） | INSERT deliveries 批次（含 `shard_id`，`next_attempt_at=now()`）+ `delivery.created` 事实 + `fanout_done_at` + （0 端点时）结算 + `message.settled` | 无（`next_attempt_at=now()` 已在 insert 内，首次投递由下一轮扫描领取，§4.4.2） |
| 可靠投递 | **DB 扫描**（`next_attempt_at <= now()`） | `delivery-worker`（租约 + 分片） | UPDATE delivery→SENT + `delivery.sent` 事实 | mTLS 回推 Gateway |
| 事实出口 | `delivery_facts` 写入（`published_at IS NULL`） | `outbox-relay`（group `relayx.outbox`） | 读未发布行 → 发布 → UPDATE `published_at=now()` | 无 |

> 三条链路各有**唯一**执行者：fanout 是 Kafka 消费者，投递是 DB 扫描者，事实出口是独立的 Relay。任何一个动作都不存在两个归属（这正是 S2 §8.1 同步实现在 S7 被替换的边界）。

## 4.3 Fan-out Worker 流程（含结算点）

```text
Handle(command{message_id}):
  1. 快路径短路（事务外读）：若 platform_state != 'ACCEPTED' 或 fanout_done_at IS NOT NULL
        → 判定已展开过，直接提交 offset 返回
        （这只是省一次事务；真正的幂等由步骤 3 的唯一约束 + coalesce + CAS 保证，
          因此 rebalance 导致的并发消费也不会产生重复 delivery）
  2. 读 message（tenant/user/policy），EndpointRegistry.Resolve(...) 得端点列表
  3. 事务内：
     a. INSERT deliveries 批次（ON CONFLICT DO NOTHING，next_attempt_at = now()，
        **shard_id = stableHash(tenant|message|endpoint) % 1024**，与 §5.2 公式同一实现）
     b. INSERT delivery_facts: delivery.created（逐条，ON CONFLICT DO NOTHING）
     c. UPDATE messages SET fanout_done_at = coalesce(fanout_done_at, now())
     d. 若本批 0 条 delivery → 同一事务内执行 settle_message CAS + message.settled
     e. COMMIT
  4. 提交 offset；本轮产生的在线投递由 delivery-worker 立即领取
     （next_attempt_at=now() 已使它们可被下一次扫描命中，见 §4.4.2）
```

**三个结算点，缺一不可**：

| 结算点 | 触发 | 作用 |
|---|---|---|
| fanout 完成 | 步骤 c | 让聚合守卫从"还不知道有多少投递"变为"可以判定" |
| 0 端点 | 步骤 d | 用户从未登录时立即 SETTLED，不留在 ACCEPTED 等 TTL |
| 最后一个 delivery 终态 | delivery 的 ACK/失败/过期路径（S2 §7.4） | 常规结算 |

> 没有步骤 c/d 会发生什么：消息进入"没有 deliveries"的中间态，之后没有任何事件再触发聚合；查询 API 长期返回 ACCEPTED，S4 的 `PendingTracker` 永不释放，`MESSAGE_SETTLED` 永不出现——而契约要求"无端点即结算"。这是异步化引入的真实语义缺口，不是实现细节。

## 4.4 Delivery Worker 流程

```text
1. 领取分片租约（§5）→ ownedShards = 本 Worker 当前持有的分片集合
2. 扫描本分片的到期 deliveries（SQL now() 判定，S2 不变）：
     WHERE delivery_state IN ('PENDING','SENT')
       AND next_attempt_at IS NOT NULL
       AND next_attempt_at <= now()
       AND shard_id = ANY($ownedShards)          -- 分片过滤：多实例不扫同一批行
     ORDER BY next_attempt_at LIMIT batch
3. 查 S6 EndpointRouter 得到节点位置
4. mTLS 回推 Gateway（DELIVER）
5. 同事务写 delivery.sent 事实 + 更新状态（S2 语义不变）
6. 续租、循环
```

**为什么必须有 `shard_id` 过滤**：CAS + 乐观锁只保证"同一行不会被推进两次"，**不保证两个 Worker 不会同时 `Dispatch` 同一行**——重复回推发生在 CAS 之前，`delivery.sent` 事实也会写两份（`state_version` 不同 → `event_id` 不同 → 下游按 `event_id` 去重失效）。分片过滤把"同一行只属于一个 Worker"提到**扫描阶段**，租约才有意义。

| 层 | 保证 | 手段 |
|---|---|---|
| 谁扫这一行 | 唯一 Worker | `shard_id = ANY($ownedShards)` + 租约互斥 |
| 谁能推进状态 | 唯一事务 | `Transition` CAS（`fromStates` + `state_version`） |
| 重复 Dispatch 是否可能 | 仅在本 Worker 崩溃后的接管窗口 | 客户端按 `message_id` 去重吸收（At-Least-Once） |

### 4.4.1 DW 取代 S2 `RetryScheduler`（必须显式声明）

| 项 | S2 | S7 |
|---|---|---|
| 到期扫描者 | `RetryScheduler`（单 goroutine，单实例） | `delivery-worker`（多实例，按虚拟分片切分） |
| 状态 | 启用 | **下线**，不再部署 |
| 领取条件 | 相同 SQL（`next_attempt_at <= now()`） | 相同 SQL + 分片归属过滤 |
| 退避公式 | `backoff(attempts)` | 完全沿用，不重新定义 |

> 两个调度器同时扫同一批行虽然能靠 CAS 保证不出错，但会让"实际并发度"与"配置的 Worker 数"无关，P99 无法归因。因此 S7 起必须显式停用 `RetryScheduler`，而不是"留着也无害"。

### 4.4.2 受理即时性（新消息不等待一个扫描周期）

- fanout 事务把 `next_attempt_at` 设为 `now()`，因此**下一轮扫描**即可领取，延迟上界 = 扫描周期。
- 扫描参数与目标：默认 `scan_interval = 100ms`，`batch = 200`，单分片并行度 4；**受理到首次投递 P99 ≤ 300ms**（单区域、节点间 RTT < 10ms 的前提下）。
- 如需更低延迟，可在 fanout 事务提交后向本机 DW 发一个"分片有货"的本地信号（仅唤醒，不传递任务）；正确性仍由 SQL 扫描保证。

## 4.5 事实出口与发布水位（`published_at` 游标）

事实出口不是一个"没有状态"的搬运工：它必须有**发布水位**，否则无法回答"哪些事实已发出"、"重启后从哪继续"、"局部有序是否退化"。

### 4.5.1 归属方式：默认单实例，扩展时按 `tenant_id` 区间（唯一合法多实例形态）

| 形态 | 归属谓词 | SQL 可执行性 | 顺序保证 | 代价 |
|---|---|---|---|---|
| **单实例（默认）** | 无 | 是 | 全局 `(occurred_at, event_id)` 单调 | 事实吞吐上限 = 单进程 |
| **多实例（扩展）** | `tenant_id >= $lo AND tenant_id < $hi`，区间来自 `relay_ranges` 配置 | 是（现有索引直接可用） | **同 `tenant` 内单调，蕴含同 `message` 内单调** | 租户倾斜：大租户只能由一个 Relay 承载 |

**被明确禁止的做法**：按 `hash(shard_key) % relay_count` 归属。`shard_key` 是逻辑键（`tenant|message`），不是物理列；要在 SQL 里按它过滤，必须让 PostgreSQL 与 Go 实现同一套哈希，且无法命中现有索引——规则写得再清楚，实现也做不出来。

```text
Outbox Relay（单实例，或按 tenant 区间归属的多实例）：
  BEGIN
    1. 取一批未发布事实（键集，不 OFFSET；多实例时带区间谓词）：
         SELECT * FROM delivery_facts
         WHERE published_at IS NULL
           AND tenant_id >= $lo AND tenant_id < $hi      -- 单实例时去掉本行
         ORDER BY occurred_at, event_id
         LIMIT n                                          -- n ≤ 500
         FOR UPDATE SKIP LOCKED
    2. PublishBatch(rows) → relayx.delivery-facts.v1（一次批量 produce，分区键 = shard_key）
         任一发布失败 → 不执行步骤 3，直接 ROLLBACK（published_at 保持 NULL）
    3. UPDATE delivery_facts SET published_at = now() WHERE event_id = ANY($ids)
  COMMIT
```

| 维度 | 规则 |
|---|---|
| 发布语义 | **至少一次**；重复由 `event_id`（确定性派生）在消费端吸收 |
| **事务边界** | 事务包含三段：`SELECT ... FOR UPDATE SKIP LOCKED` → **批量 produce（在行锁内）** → `UPDATE published_at`。批 ≤ 500 且一次 produce，目标锁持有 P99 < 50ms（S8 容量报告给实测值）；未达标则必须先写 ADR 引入"领取租约 + 事务外发布"，不得静默放宽隔离 |
| 失败处理 | produce 失败或超时 → `ROLLBACK`，`published_at` 保持 NULL，下一轮重发（重复由 `event_id` 吸收） |
| 有序性（单实例） | 严格按 `(occurred_at, event_id)` 全局单调 |
| **有序性（多实例）** | 同一 `tenant_id` 只落在一个 Relay 的区间内（区间不得重叠，启动校验）→ 该租户所有事实按 `(occurred_at, event_id)` 单调 → 同消息单调 |
| 重启恢复 | 未发布行仍在表中（`published_at IS NULL`），不丢；不需要额外游标表 |
| 与 outbox 表的分工 | `outbox` 承载 **command/broadcast** 等"要执行的命令"；`delivery_facts.published_at` 承载**事实**的出口水位。两者不混用（命令需要"消费完成"语义，事实只需要"已发出"语义） |
| 清理 | 已发布且超过保留窗口的 facts 由 S8 的保留策略删除；`published_at` 是清理的判据之一 |

**扩容与重平衡的边界**：区间是**部署期静态配置**，运行期不改；扩容 = 停旧进程 → 改 `relay_ranges` → 滚动重启（顺序不可颠倒：**同一时刻只允许一个进程扫描同一区间**，重叠配置会在启动校验时被拒绝）。若终局需要**运行期无缝重平衡**，唯一路径是给 `delivery_facts` 增加物理列 `shard_key` 并改一致性哈希——那是一次表结构变更，必须走 ADR 与冻结线变更流程，S7 不预留。

> **并行维度必须显式声明**：`SKIP LOCKED` 解决的是"两个 Relay 抢同一行"，**不解决**"两个 Relay 各拿同一 `shard_key` 的相邻两批、按不同节奏发布"。前者是重复（幂等可吸收），后者是乱序（不可修复）。因此多实例只能靠"区间不重叠"从根上排除后者。

> 为什么不用 `outbox` 表搬事实：事实是**只追加的观察结果**，重发安全（`event_id` 幂等）；命令是**要发生的动作**，重发需要额外判据（`fanout_done_at` 等）。把两者塞进同一张表，会让"已发布但未处理"与"已处理但未发布"两种状态混在一个 `published_at` 字段里，谁都说不清语义。

## 4.6 并发维度表（"单实例 → 多实例"演进点必填）

每一处从单机变并行的机制，都必须回答三件事：**谁在并行、按什么切分、顺序与幂等由谁保证**。缺任一项即为并发缺陷（设计评审中 DW 扫描、Relay 归属、重复上界三个问题都出在这里）。

| 机制 | 并行度来源 | 切分键 | 顺序保证 | 幂等/互斥保证 |
|---|---|---|---|---|
| `fanout-worker` | Kafka 分区 | `tenant\|user`（command 分区键） | 同一用户的消息按分区顺序展开 | `deliveries` 唯一键 + `fanout_done_at` 短路 |
| `delivery-worker` | 虚拟分片（1024） | `shard_id` | 不需要全局顺序；同一 delivery 只被一个 Worker 领取 | 扫描 `shard_id = ANY(owned)` + 租约 + `Transition` CAS |
| `outbox-relay`（事实） | 实例数（默认 1） | **`tenant_id` 字典序区间**（`relay_ranges`，不重叠） | 同一 `tenant` 内 `(occurred_at, event_id)` 单调（蕴含同 `message` 单调） | `published_at` 水位 + `event_id` 幂等 + 区间启动校验 |
| `outbox-relay`（命令） | 分区 | `tenant\|user` / `tenant\|topic` | 同分区有序 | `fanout_done_at`、投递唯一键 |
| `broadcast` 消费 | Gateway 节点数 | 每节点独立 group（全量消费） | 单 topic 内由分区键保序 | 广播本身 Best-Effort，无幂等要求 |
| `client-event-router` | 消费组内分区 | `shard_key`（tenant\|message） | 同一消息的事件按 `(occurred_at, event_id)` | `event_id` 去重 + 端侧 `seq` |

> 维护规则：任何"把单实例改成多实例"的改动，**必须同时提交本表的一行**；没有这一行的并行化不得进入评审。


---

# 5. Worker 租约与虚拟分片

## 5.1 为什么需要

S2 的单机串行扫描在分布式下会出现「多个 Worker 抢同一任务」与「扩容必须停机改配置」两个问题。解法是**租约 + 虚拟分片**。

## 5.2 虚拟分片

- 虚拟分片空间（默认 1024），`shard_id = 稳定哈希(tenant|message|endpoint) % 1024`。
- **`shard_id` 在 fanout 建投递行时一次性写入，此后不可变**：迁移搬的是"分片区间的归属"，不是行本身；行不需要重写分片列。
- `shard_assignments` 表记录 Worker↔分片区间、租约、版本；DW 的扫描 SQL 用 `shard_id = ANY($ownedShards)` 过滤（§4.4）。
- 扩容时劈裂区间、缩容时合并：迁移按「先在 `shard_assignments` 内原子改写归属 → 新 Worker 扩扫描集合 → 旧 Worker 缩扫描集合」的顺序执行。
- **"先扩后缩"必然存在双持窗口**：新 Worker 已扩集合、旧 Worker 尚未缩集合之间，同一个 `shard_id` 会被两边各扫一次，窗口长度 ≤ 一个扫描周期（`scan_interval`）。这不是异常，是顺序化迁移的固有代价；由 `Transition` CAS（状态只推进一次）+ 客户端按 `message_id` 去重（重复渲染为 0）兜底，**不承诺"正常路径下永不双持"**。
- 双持窗口必须可观测：迁移期间 `shard_dual_hold_seconds` 暴露给运维；窗口超过 3×`scan_interval` 视为迁移卡住并告警。
- 每进程令牌桶限流（全局限流留 S8）。

## 5.3 租约机制

- Worker 领取分片后持租约；按 `lease_ttl/3` 周期续期。
- 崩溃后租约超时自动释放，其他 Worker 可接管。
- 优雅停机主动释放（S8 完善）。
- 租约修复 CLI（S8 完善）。

## 5.4 与 S2 时间仲裁的关系

- 到期/超时判定仍用 SQL `now()`（S2 冻结）——租约只决定「谁来处理」，不改变「是否到期」。
- 租约的 clock 用于 Lease 时长，不影响消息状态判定。

---

# 6. 跨节点广播

## 6.1 从本地直连到 Kafka

```text
S3（单机）：POST /v1/broadcasts → 进程内扇出
S7（分布式）：POST /v1/broadcasts → 写 relayx.broadcast.v1（不再本地直连）
             每个 Gateway 用稳定唯一 consumer group 消费全量广播
             → 按本地订阅索引过滤 → 扇出（扇出逻辑沿用 S3，零改动）
```

## 6.2 分区键与顺序

- 分区键 `tenant|topic`，保证单 topic 内有序（S3 语义）。
- 生产 topic 预创建 24 分区 / RF=3 / 保留 10 分钟；本地 3 分区 / RF=1。
- 不自动扩分区。

## 6.3 新鲜度窗口与可用性

- 消费端跳过超过新鲜度窗口的积压消息，避免 Gateway 重启后补发历史弹幕（S3 的 Best-Effort + 不补发语义）。
- Kafka 不可用时受理返回 503，不产生「已受理但永不投递」的静默失败。
- 广播消费 + 本地过滤的带宽成本：由容量模型决定是否升级为定向路由（阈值触发，留待终局评估）。

## 6.4 扇出逻辑不变

- 订阅索引、共享编码、丢弃策略、单 topic 有序全部沿用 S3。
- S7 只替换「broadcast 从哪来」——从 HTTP 直连换成 Kafka 消费。

---

# 7. 客户端事件总线

## 7.1 从进程内投影到独立进程

```text
S4（单机）：进程内 EventProjector 读 delivery_facts → CLIENT_EVENT
S7（分布式）：client-event-router 消费 relayx.delivery-facts.v1
             → 投影为客户端事件（schema 不变）
             → 查 S6 节点路由 → 跨节点回推 Gateway
```

## 7.2 路由与降级

- 路由键 `(tenant_id, producer_id, source_device_id)`：三项**全部从事实取**（`payload_summary.source_device_id`，S2 §3.4），router **不回查 `messages` 表**。
- 跨节点**只复用 S6 的 mTLS 通道与节点寻址**，**不进入 S6 §4.4 的失败映射与退避重试**：事件回推失败（节点不可达、对端无会话、NACK）→ 直接丢弃 + 计 `client_event_route_failed{reason}`，不重试、不落库、不改变任何 S6/S2 的投递状态。
- 理由：S6 §4.4 是**可靠消息**的语义（失败要退避、不能误写终态）；客户端事件是 Best-Effort，套用它会让一次节点抖动把事件链路放大成"重试风暴"，并违背 S4 "离线即丢弃、不重试"的冻结语义。两者共用通道，不共用策略。
- `source_device_id` 为空（HTTP 提交）→ 直接丢弃并计 `client_event_no_route_total`，不放大为"推给该 producer 全部设备"。
- 发送设备离线直接丢弃，不重试、不落库（S4 语义）。
- router 故障时事件延迟/丢失可观测，查询 API 仍可用（S4 兜底）。
- 事件 schema 与帧定义直接复用 S4，不新增字段。

> 这是「来源设备必须落成持久事实」的收口点：在 S4 单机阶段，路由键可以"读同进程的 messages"；到了 S7，router 是独立进程、只消费 facts。如果事实里不带来源设备，S7 只能回查业务库（引入耦合）或者放弃 self-scoped 精确性（语义退化）——两者都不可接受。

## 7.3 语义一致性

- 客户端事件仍 self-scoped、默认只推聚合终态、不持久化。
- 不输出 `read` / `completed` / `business_acked`（S4 冻结）。

---

# 8. 幂等边界与一致性

| 边界 | 保证 | 手段 |
|---|---|---|
| DB + Kafka 双写 | 消息与 Outbox 原子提交 | 事务 |
| Kafka 重复消费 | 不破坏业务状态 | `EventID` 幂等 + 唯一约束 |
| Worker 并发 | 不同 Worker 不抢同一分片 | 租约 + 虚拟分片 |
| Fan-out 展开 | 不重复建 delivery | S2 投递幂等键 + `fanout_done_at` 前置短路（§4.3 步骤 1） |
| **fanout 完成 → 结算** | 0 端点与"最后一条终态"都能结算，消息不停在 ACCEPTED | `fanout_done_at` 守卫 + `settle_message` CAS（S2 §4.2 / §7.4） |
| facts 搬运 | `event_id` 幂等、同 `shard_key` 内有序不退化 | `published_at` 水位 + `(occurred_at, event_id)` 顺序 + 复用 S2 派生规则 |
| 广播消费 | 单 topic 有序 | 分区键 |
| 事件投影 | 不产生重复事件 | 复用 S4 event_id |

## 8.1 一致性声明

- 端到端仍是 At-Least-Once + 消费端幂等，**不实现 Exactly-Once**。
- 跨消息/跨 endpoint 全局顺序不保证，只保证单分区内与单 topic 内有序。

---

# 9. 语义零变更约束

## 9.1 不得改动

| 冻结资产 | 本阶段处理 | 检查 |
|---|---|---|
| S2 状态机 / 幂等键 / facts schema | 原样运行；facts 只搬到 Kafka | 场景回归 + schema 比对 |
| S3 广播语义 | 扇出逻辑零改动，只换消息来源 | 广播场景 |
| S4 事件 schema / 帧定义 | router 直接复用 | 事件场景 |
| S6 节点路由 / mTLS | 复用回推通道 | 路由用例 |
| 对外帧号 / 能力位 | 不改 | 冻结表比对 |

## 9.2 允许新增

- Kafka topic 与统一 Envelope。
- Outbox 表、租约、虚拟分片、DLQ 基础。
- 独立 Worker / Router 进程。
- **列追加**：`deliveries.shard_id`（A §8.1.1 已登记）。
- **索引追加**（只增不改）：
  - `deliveries (shard_id, next_attempt_at) WHERE delivery_state IN ('PENDING','SENT') AND next_attempt_at IS NOT NULL` — 支撑 §4.4 的分片扫描；
  - `delivery_facts (tenant_id, occurred_at, event_id) WHERE published_at IS NULL` — 直接支撑 §4.5 的取批查询：区间谓词 `tenant_id >= $lo AND tenant_id < $hi` 走索引前缀，`ORDER BY occurred_at, event_id` 走索引后缀顺序，**不需要排序全部未发布行**；不新增列（`shard_key` 只是 Kafka 分区键，不是物理列）。

> 全部属于「传输与调度机制」，不触碰冻结语义（I1）。

---

# 10. 故障处理矩阵

| # | 场景 | 行为 | 恢复 | 验收口径 |
|---|---|---|---|---|
| D1 | 消息/Outbox 非原子 | 事务保证 | 回滚 | 无「受理但从未投递」 |
| D2 | Kafka 重复消费 | 幂等去重 | — | 业务状态不破坏 |
| D3 | Worker 崩溃 | 租约超时释放 | 其他 Worker 接管 | 任务重新领取 |
| D4 | 并发抢任务 | 租约 + 分片 | — | 无重复处理 |
| D5 | Fan-out 过大 | 分批展开 | — | 内存不无限增长 |
| D6 | 大租户占用资源 | 每进程令牌桶 | — | 基础隔离（全局留 S8） |
| D7 | ACK/Retry/Expiration 并发 | S2 状态机 CAS | — | 状态正确 |
| D8 | 广播积压超窗口 | 跳过旧消息 | — | 无历史回放 |
| D9 | Kafka 不可用（广播） | 受理返回 503 | 恢复后继续 | 无静默失败 |
| D10 | 事件 router 故障 | 事件延迟/丢失可观测 | 查询 API 兜底 | 客户端可补齐 |
| D11 | facts 搬运乱序/重复 | 按 `(occurred_at, event_id)` 发布，`event_id` 幂等 | Relay 重启后重发未发布行 | 同一 `shard_key` 内有序；消费端不重复处理 |
| D12 | fanout 展开 0 条 delivery | 同事务置 `fanout_done_at` 并结算 | — | 消息立即 `SETTLED`，无"永久 ACCEPTED" |
| D13 | fanout 事务崩溃 / command 重投 | 投递幂等键重建；`fanout_done_at` 随提交置位 | 消费重试 | 无重复 delivery、消息最终结算 |

---

# 11. 可观测性与本阶段限流

## 11.1 本阶段可观测（基础，S8 完善四件套）

- 关键指标：Outbox 积压深度、Kafka consumer lag、租约持有/接管数、重试次数分布、广播丢弃数、事件投递延迟、**`shard_dual_hold_seconds`（rebalance 双持窗口）**、**Relay 区间 lag（按区间分列）**。
- 结构化日志注入 traceID / tenantID / messageID / workerID / shardID。
- 沿袭 I3 的两层约定：**最小集**（`/metrics`、`/healthz`、日志字段）自 S1 起已存在，本阶段每个新进程必须继承，不得缺失；**全量集**（`/readyz` 分依赖态、告警规则、容量报告）留 S8 统一收敛。

## 11.2 本阶段限流

- 每进程令牌桶（多租户基础隔离）。
- **全局限流（Redis Lua 批量令牌）留 S8**——本阶段不实现，明确标注。

---

# 12. 测试用例设计

## 12.1 一致性测试

| 用例 | 断言 |
|---|---|
| Outbox 原子提交 | 消息与 Outbox 同提交或同回滚 |
| Fan-out 可恢复 | 崩溃后重新展开，无重复 delivery |
| 重复消费幂等 | 同一 Envelope 多次消费状态不变 |
| facts 搬运幂等 | event_id 唯一，局部有序 |
| **受理不建 deliveries** | 受理提交后 `deliveries` 为空、`fanout_done_at` 为 NULL |
| **零端点结算** | `Resolve` 返回空集 → 消息 `SETTLED` + `message.settled` 事实，无 delivery |
| **fanout 崩溃后收敛** | fanout 事务中途 kill → 重投 command → 投递行不重复、消息最终 `SETTLED` |
| **发布水位** | Relay 崩溃重启后未发布行仍被发布；已发布行不重复搬运（`published_at` 非空） |

## 12.2 并发与故障测试

| 用例 | 注入 | 断言 |
|---|---|---|
| Worker 崩溃接管 | kill Worker | 租约超时后任务被接管 |
| 并发领取 | 多 Worker 抢分片 | 无重复处理 |
| **分片过滤** | 两个 DW 实例持不同分片，同时扫描 | 各自只捞 `shard_id ∈ owned`；同一 delivery 不被两个实例同时 Dispatch；`delivery.sent` 每个 `state_version` 只一条 |
| **多 Relay 归属（区间不重叠）** | 启动 2 个 Relay，各持 `[lo,mid)` / `[mid,hi)`，同时跑 10 分钟 | 各自只发布本区间租户的事实；同一 `tenant` 内 `(occurred_at, event_id)` 无倒置；**无饿死**（两边 `published_at` 都在推进，lag 不单调增长） |
| **区间重叠拒绝启动** | 配置两个 Relay 区间重叠 | 启动校验失败，进程退出并报明确错误（不允许"重叠也能跑"） |
| **锁持有时间** | 批 500、Kafka `acks=all` | `SELECT...UPDATE` 事务 P99 < 50ms（超出则必须走 ADR 变更方案，不得静默放宽） |
| 大规模投递 | 高 fanout | 内存不无限增长 |
| 广播跨节点一致 | 多 Gateway | 全量在线订阅者收到 |
| 事件跨节点送达 | 发送方与接收方不同节点 | CLIENT_EVENT 正确送达 |
| **事件回推失败不重试** | 目标节点宕机 | `client_event_route_failed` 增长，**无重试放大**，S2 投递状态不受影响 |

## 12.3 语义回归（关键）

| 用例 | 断言 |
|---|---|
| S2 可靠流程 | 状态机、幂等、facts 不变 |
| S3 广播 | 扇出逻辑零改动，单 topic 有序 |
| S4 事件 | schema、帧定义、self-scoped 不变 |
| 冻结表比对 | 对外帧号/能力位/schema 无改动 |

## 12.4 验收场景（`cmd/scenario`）

- `atomic-outbox`：受理与 Outbox 原子性（受理后无 deliveries）。
- `worker-failover`：Worker 崩溃任务接管。
- `cross-node-broadcast`：跨节点广播一致。
- `cross-node-event`：事件跨节点送达（**发送设备与接收方不同节点，验证来源设备取自事实**）。
- `idempotent-consume`：重复消费幂等。
- `zero-endpoint-settles`：目标用户从未登录 → 消息 `SETTLED`，无 delivery 行，`MESSAGE_SETTLED` 事件送达发送设备。
- `fanout-crash-recovery-settles`：fanout 事务提交前 kill → 重投后消息收敛到 `SETTLED`，无重复 `delivery.created`。
- `facts-watermark`：Relay 停止/重启后，未发布事实不丢、已发布事实不重复搬运。

---

# 13. 验收标准与冻结清单

## 13.1 验收标准

**功能与正确性**
- 消息与 Outbox 原子提交；Fan-out 可恢复；Worker 并行处理；崩溃后任务重新领取。
- **受理事务不创建 deliveries**；deliveries 由 fanout 事务唯一创建，`fanout_done_at` 与投递行同事务。
- **零端点即结算**：目标无端点时消息直接 `SETTLED`；**fanout 崩溃重投后仍结算**。
- 重复消费不破坏业务状态；大规模投递不无限占用内存。
- 多租户基础隔离（每进程级）。
- 一条 publish 被同节点与跨节点全部在线订阅者收到；订阅积压超窗口跳过旧消息。
- 一条 `message.settled` 事实能投影为客户端事件并跨节点送达（路由键来自事实中的 `source_device_id`）。
- **S2 的 `RetryScheduler` 已下线**，到期投递只由 `delivery-worker` 领取；受理到首次投递 P99 达标（§4.4.2）。
- 事实出口有发布水位：Relay 重启不丢事实、不重复搬运已发布行。
- **Relay 扩展可执行**：默认单实例；启用多实例时按 `tenant_id` 区间切分，区间重叠拒绝启动，两实例并行 10 分钟无倒序、无饿死（§12.2）。

**语义零变更**
- S2/S3/S4/S6 场景回归全绿；冻结表（帧号/能力位/schema/幂等键）无改动。

**工程质量**
- `gofmt`、`go vet`、`go test ./...`、`go test -race ./...` 通过。
- 关键状态转换有故障注入覆盖；新增决策记录 ADR。

## 13.2 冻结清单（S7 验收通过后生效）

| 冻结项 | 值 / 位置 |
|---|---|
| Envelope schema | §3.1（`event_id` / `subject` / `tenant_id` / `shard_key` / `schema_ver` / `occurred_at` / `payload`，与 A §6.2 一致） |
| subject 集合 | §3.2（command / broadcast / facts / dlq），只增不改 |
| **消费者归属** | §2.2.1：`command` → `fanout-worker`(group `relayx.fanout`) 唯一；DW 是 DB 扫描者、不消费 Kafka |
| 分区键 | command=`tenant\|user`，broadcast=`tenant\|topic` |
| 广播消费模式 | 全量消费 + 本地过滤；升级阈值由容量模型触发 |
| 租约/分片机制 | §5（虚拟分片 1024，租约续期 lease_ttl/3） |
| **DW 扫描归属** | §4.4：`shard_id = ANY($ownedShards)` 过滤 + `shard_id` 由 fanout 写入（公式 `stableHash(tenant\|message\|endpoint) % 1024`） |
| **fanout 结算点** | §4.3（`fanout_done_at` + 0 端点同事务结算 + 最后终态结算） |
| **事实发布水位** | §4.5（`delivery_facts.published_at` + `(occurred_at, event_id)` 顺序，至少一次） |
| **Relay 并行归属** | §4.5：**默认单实例**；多实例唯一合法形态是**按 `tenant_id` 字典序区间**（`relay_ranges`，区间不得重叠、启动校验），谓词 `tenant_id >= $lo AND tenant_id < $hi`。**禁止**按 `hash(shard_key)` 归属（SQL 不可执行） |
| **调度器归属** | §4.4.1：`delivery-worker` 取代 S2 `RetryScheduler`，后者下线 |
| **事件失败语义** | §7.2：只复用 mTLS 通道，失败直接丢弃 + 计数；不进入 S6 §4.4 退避重试 |
| **并发维度表** | §4.6：任何"单实例 → 多实例"改动必须同时补一行 |
| 一致性语义 | At-Least-Once + 幂等，不实现 Exactly-Once |

> **冻结后纪律**：S8 只做加固——全局限流、安全、可观测、优雅停机、数据生命周期、DLQ 重放完善；不再新增协议帧、表或消费者。本阶段定义的传输与调度机制在 S8 只修缺陷、不换结构。

---

> **结语**：S7 把三条数据流搬到 Kafka，靠的不是重写语义，而是**复用 S2/S3/S4 已冻结的 schema 与 S6 的路由**。Envelope 统一了消费框架，租约与分片解决了并行与扩容，而事实 schema、广播语义、事件协议一行未改。这正是把语义冻结线放在 S2 的回报——分布式阶段只做它该做的事：换传输。
