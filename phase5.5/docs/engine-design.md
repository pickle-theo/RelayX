# Engine 设计

本文描述 Phase 5 投递引擎的受理事务、幂等键、投递状态机、租约、分片、路由解析、重试、过期扫描与租户限流。所有行为以 `internal/store`、`internal/engine`、`internal/tenant`、`internal/message` 的现有实现为准。

## 1. 受理事务与幂等键

Gateway 的 SEND 与 Message Service 的 HTTP 受理共用 `internal/message.Service.Accept`，最终落到 `store.AcceptMessage`。

### 1.1 事务内容

单个事务内依次执行：

1. `INSERT INTO messages (tenant_id, client_msg_id, from_user, from_device, target_type, target, payload, expire_at) ... ON CONFLICT (tenant_id, from_user, client_msg_id) DO NOTHING RETURNING id`。
2. 若返回 `sql.ErrNoRows`（冲突）：回查原 `id`，置 `Duplicate = true`，事务内不再写任何行。
3. 若请求带显式端点（`TargetUser`/`TargetDevice` 均非空）：`INSERT INTO endpoints ... ON CONFLICT (user_id, device_id) DO NOTHING`。
4. `INSERT INTO outbox (topic, partition_key, payload) VALUES ($1, $2, $3::jsonb)`，payload 由回调在拿到新 `message_id` 后编码。
5. 提交。提交后消息可被 PULL 与 fan-out 看见，但此时还没有 delivery 行。

不变量：

- 不存在「消息已写入但没有 outbox 行」的中间状态；任一步失败整体回滚。
- 受理阶段不写 `deliveries`，因此 SEND 的延迟与收件人数量无关。
- 重复受理不更新时间戳、不追加 outbox 行。

### 1.2 幂等键

唯一索引（`migrations/0002_outbox_tenants_leases.sql`）：

```sql
CREATE UNIQUE INDEX IF NOT EXISTS messages_tenant_client_msg_idx
    ON messages (tenant_id, from_user, client_msg_id);
```

| 项 | 说明 |
| --- | --- |
| 键 | `(tenant_id, from_user, client_msg_id)` |
| 客户端来源 | `SEND` 帧头 `MsgID` 作为 `client_msg_id`；HTTP 用请求体 `client_msg_id` |
| 缺省 | HTTP 未提供或为 0 时，服务端用 `crypto/rand` 生成非零 uint64 |
| 租户来源 | Gateway 用 `-tenant-id`；HTTP 用 `X-Tenant-ID` > 请求体 `tenant_id` > `-default-tenant` |
| 冲突结果 | 返回首次受理的 `message_id` 与 `duplicate=true`，不新增消息、不新增事件 |

Phase 4 的唯一约束 `messages_from_user_client_msg_id_key` 在迁移中被删除。不同租户可以复用同一个 `client_msg_id`；这是 BREAKING 变更。

## 2. 投递状态机

### 2.1 七态（数值不变）

`store.DeliveryState`：

| 值 | 名称 | 含义 | 是否终态 |
| --- | --- | --- | --- |
| 0 | PENDING | 已创建，等待首次推送 | 否 |
| 1 | SENT | 已推送，等待回执 | 否 |
| 2 | RECEIPT_ACKED | 客户端已回执接收 | 否 |
| 3 | BUSINESS_ACKED | 业务处理成功 | 是 |
| 4 | BUSINESS_FAILED | 业务处理失败 | 是 |
| 5 | EXPIRED | 消息过期 | 是 |
| 6 | FAILED | 达到最大推送次数 | 是 |

`messages.state` 仍为四态：`0 ACCEPTED`、`1 PARTIAL`、`2 COMPLETED`、`3 EXPIRED`。

### 2.2 谁推进状态

| 迁移 | 触发者 | SQL |
| --- | --- | --- |
| `0 → 1` | Delivery Worker 推送成功 | `MarkDeliverySentLeased` |
| `0 → 1` | PULL 补投 | `PullPending` |
| `0/1 → 保持原值`（attempts+1） | Worker 推送失败 | `RetryLeased(increment=true)` |
| `0/1 → 保持原值` | Worker 无路由/被限流 | `RetryLeased(increment=false)`，只改 `next_retry_at` |
| `0/1 → 6` | 推送失败且 `attempts + 1 >= maxAttempts` | `RetryLeased` 的 `CASE` |
| `1 → 2` | 客户端 RECEIPT_ACK | `ApplyReceiptAck` |
| `1/2 → 3/4` | 客户端 BUSINESS_ACK | `ApplyBusinessAck` |
| `0/1/2 → 5` | 过期扫描 | `ExpireMessage` |

关键点：**PENDING → SENT 由 Worker 推进**。Phase 4 在受理事务里就把在线目标写成 SENT，Phase 5 改为全部先 PENDING，由 fan-out 创建、由 Worker 领取并推送后变成 SENT。这样「是否在线」不再在受理路径上判定。

状态更新都带条件，写冲突时以先到者为准：

- `MarkDeliverySentLeased` / `RetryLeased` / `ReleaseLease` 都要求 `worker_id = $workerID AND state IN (0,1)`；租约已被接管时更新 0 行，调用方把它计为 `released` 而不是 `sent`。
- `ApplyReceiptAck` 只作用于 `state = 1`；重复回执影响 0 行。
- `ApplyBusinessAck` 只作用于 `state IN (1,2)`；重复业务回执影响 0 行。
- `PullPending` 只取 `state IN (0,1)` 且 `id > cursor`，用 `FOR UPDATE OF d` 锁行。

### 2.3 消息聚合

`recomputeMessageState`（`internal/store/message.go`）在改投递行的同一事务内重算：

```sql
UPDATE messages
SET state = CASE
    WHEN messages.state = 3 THEN 3
    WHEN agg.total > 0 AND agg.terminal = agg.total THEN 2
    WHEN agg.terminal > 0 THEN 1
    ELSE 0
END
FROM agg ...
```

其中 `terminal = count(*) FILTER (WHERE state IN (3,4,5,6))`。ACK 路径 `ApplyReceiptAck` / `ApplyBusinessAck` 用等价的单语句 `WITH upd AS (UPDATE ... RETURNING message_id) UPDATE messages ...` 实现，避免额外的 BEGIN/COMMIT。

## 3. 租约

### 3.1 字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `lease_until` | `TIMESTAMPTZ` | 租约到期时间；`NULL` 表示未租用 |
| `worker_id` | `TEXT` | 持有者标识，来自 `-node-id`，为空时生成 `worker-<12 位十六进制>` |

索引：

```sql
CREATE INDEX IF NOT EXISTS deliveries_due_idx
    ON deliveries (next_retry_at, id)
    WHERE state IN (0, 1);
```

### 3.2 领取

`ClaimDueDeliveries` 的判定条件：

- `state IN (0, 1)`
- `next_retry_at IS NULL OR next_retry_at <= now`
- `lease_until IS NULL OR lease_until <= now`
- `mod(d.id, shards) = shard_index`
- 排序 `next_retry_at NULLS FIRST, id`，上限 `-claim-batch`

CTE 先 `FOR UPDATE SKIP LOCKED` 选出 `id`，再 `UPDATE ... SET lease_until = $now + lease_ttl, worker_id = ... FROM due`，最后 join `messages` 返回投递与消息字段。`SKIP LOCKED` 让多个 Worker 并发扫描时不会等待彼此已锁定的行；`now` 由 Engine 传入（`time.Now()`），因此接管时机受 Worker 本机时钟影响。

### 3.3 TTL、续期与接管

- `-lease-ttl` 默认 30s，必须大于 0；应明显大于单批处理耗时。
- `store.RenewLeases` 提供按 `worker_id` 条件续期，但当前 `Engine` 未调用它：一批投递在单轮内处理完，租约只用于崩溃接管。
- Worker 崩溃后 `lease_until` 到期，行重新满足领取条件，由任意覆盖该分片的 Worker 接管。
- 接管后旧 Worker 的条件更新会失败（`worker_id` 已变化），不会把状态改回去；旧 Worker 的计数记为 `released`。
- 主动放弃：`ReleaseLease` 在行已过期但尚未被过期扫描处理时清空租约，让其它 Worker 或过期扫描处理。

## 4. 分片调度

规则：`mod(delivery_id, shards) = shard_index`，即 `internal/store/lease.go`：

```sql
AND mod(d.id, $2) = $3
```

| 参数 | 说明 | 约束 |
| --- | --- | --- |
| `-worker-shards` | 分片总数 | 至少 1 |
| `-worker-shard-index` | 本 Worker 的分片 | `0 <= index < shards` |

- 分片只决定「谁有资格扫描」，不保证行固定在某个进程；多个 Worker 使用相同 `shard_index` 也能正确工作（`SKIP LOCKED` 串行化领取），但会浪费扫描。
- 推荐 `shards` 等于 Worker 数量，每个 Worker 一个互不重复的 `shard_index`。
- 扩容注意：`shards` 变化后 `mod(id, shards)` 的结果整体变化，原有行会迁移到新分片。必须在所有 Worker 上同时切换，否则新分片集合可能只被部分覆盖。详细权衡见 [ADR-017](adr/ADR-017.md)。

## 5. 路由解析

`Engine.push` 先尝试本地会话，再走 Redis 路由。四种结果：

| 路径 | 条件 | 动作 | 结果 |
| --- | --- | --- | --- |
| 1. 本地会话 | `sessions` 与 `deliverer` 均非 nil，且 `FindByDevice` 命中 | `deliverer.Deliver` 入队 DELIVER 帧 | 成功 `pushSent`；入队失败 `pushFailed` |
| 2. 远端存活节点 | 本地未命中，`LookupRoute` 命中，`NodeAlive` 为真，`LookupNode` 命中 | `ForwardDeliver` 发送 `NODE_DELIVER` | 成功 `pushSent`；转发失败 `pushFailed` |
| 3. 无路由 | 未配置 `-redis-addr`（Registry/Forwarder 为 nil）、`LookupRoute` 返回不存在、`NodeAlive` 为假、`LookupNode` 返回不存在 | 不推送，仅延后 | `pushNoRoute`，用 `-retry-backoff-base` 延后且不增加 attempts |
| 4. 路由/转发故障 | `LookupRoute`、`NodeAlive`、`LookupNode` 返回错误，或 `ForwardDeliver` 返回错误 | 视为可重试故障 | `pushFailed`，消耗一次 attempt |

`NODE_DELIVER` 的 body 由 Worker 填充：`UserID`、`DeviceID`、`FromSessionID = 0`、`FromUser`、`FromDevice`、`Payload`。对端 Gateway 在 `HandleNodeDeliver` 中查本地会话：不存在返回 `NOT_FOUND`，入队失败返回 `INTERNAL`，成功返回 `OK`。Hub 的转发错误会体现为 `pushFailed`。

## 6. 重试退避与最大尝试

### 6.1 推送成功

`MarkDeliverySentLeased`：

```sql
UPDATE deliveries
SET state = 1,
    attempts = attempts + 1,
    next_retry_at = now() + make_interval(secs => LEAST(
        $base::float8 * power(2::float8, attempts::float8), $max::float8)),
    lease_until = NULL,
    worker_id = NULL,
    updated_at = now()
WHERE id = $1 AND worker_id = $2 AND state IN (0, 1)
RETURNING attempts
```

即下一次重试间隔从 `attempts` 开始翻倍，`base = -retry-backoff-base`（默认 2s），上限 `-retry-backoff-max`（默认 1m）。推送成功不等于送达，客户端仍需回执；未回执的行会按此时间再次被领取并重推。

### 6.2 推送失败

`Engine.process` 在 `pushFailed` 时调用：

```go
e.retry(ctx, d, now.Add(e.backoff(d.Attempts)), true)
```

`backoff(attempts)` 从 `BackoffBase` 开始翻倍 `attempts` 次，达到 `BackoffMax` 即封顶。`RetryLeased`：

```sql
UPDATE deliveries
SET state = CASE WHEN $4 AND attempts + 1 >= $5 THEN 6 ELSE state END,
    attempts = attempts + CASE WHEN $4 THEN 1 ELSE 0 END,
    next_retry_at = $3,
    lease_until = NULL,
    worker_id = NULL,
    updated_at = now()
WHERE id = $1 AND worker_id = $2 AND state IN (0, 1)
RETURNING state
```

`$4 = incrementAttempts`、`$5 = maxAttempts`。达到上限时同一语句把 `state` 置 6（FAILED），并清空租约；`next_retry_at` 虽被写入，但扫描只取 `state IN (0,1)`，不会再被领取。

### 6.3 不消耗 attempts 的延后

以下两种情况用 `increment=false`：

- **无路由**：`nextAt = now + BackoffBase`，状态保持 PENDING/SENT，`attempts` 不变。这样离线用户不会因为长期不在线而把投递推成 FAILED。
- **租户限流**：`nextAt = now + retryAfter`，`retryAfter` 由 `Limiter.Acquire` 计算（令牌不足时为补满一个令牌所需时间 + 1ms；在途已满时固定 50ms）。同样不消耗 attempts。

## 7. 过期扫描

每轮 `RunOnce` 在领取与推送之后执行 `expireDue`：

1. `ListExpiredMessages(now, -expire-batch)`：

```sql
SELECT id FROM messages
WHERE expire_at <= $1 AND state IN (0, 1)
ORDER BY expire_at
LIMIT $2
```

2. 对每个 ID 调用 `ExpireMessage`，单事务内：

```sql
UPDATE deliveries
SET state = 5, next_retry_at = NULL, lease_until = NULL, worker_id = NULL, updated_at = now()
WHERE message_id = $1 AND state IN (0, 1, 2);

UPDATE messages SET state = 3 WHERE id = $1;
```

3. 任一条失败只记录警告并继续下一条；成功的计入 `expired` 计数。

已经在 `process` 中检测到 `ExpireAt <= now` 的行不会推送：Worker 调用 `ReleaseLease` 并计入 `released`，把最终状态交给过期扫描（可能由另一个 Worker 执行）。把过期清理放在 Worker 而不是受理路径，避免长事务与热点行竞争。

## 8. 租户令牌桶与在途信号量

`internal/tenant.Limiter` 是每个 Worker 进程内的限流器，配额来自 `tenants` 表。

### 8.1 配额

| 列 | 含义 | 为 0 时 |
| --- | --- | --- |
| `rate_per_sec` | 每秒令牌数 | 不限速 |
| `burst` | 桶容量 | 回退为 `rate_per_sec` |
| `max_inflight` | 并发推送上限 | 回退为 `-default-inflight`（默认 256，`0` 表示不限） |

`GetTenant` 查不到行时返回 `(Tenant{}, false, nil)`，调用方按全零配额处理：不限速 + 默认在途上限。迁移已插入 `id = 'default'` 的零配额行。

### 8.2 Acquire

`Acquire(ctx, tenantID, now)` 的顺序：

1. 按 `-tenant-cache-ttl`（默认 30s）读取或刷新配额；查询失败时保留上一次配额，首次失败则用零配额继续服务，不让配额查询故障阻断投递。
2. 在途检查：若 `inflight >= limit`，返回 `(nil, 50ms, false, nil)`。
3. 令牌检查：`tokens += elapsed * rate`，上限 `burst`；`tokens < 1` 时返回补满所需时间，`false`；否则 `tokens--`。
4. 通过时 `inflight++` 并返回 `release`。`release` 用 `sync.Once` 保证幂等，`inflight` 不会减到负数。

`Engine.process` 在 `Acquire` 成功后 `defer rel()`，保证每个成功路径都归还令牌槽位。

### 8.3 与状态机的关系

- 限流拒绝只调用 `RetryLeased(..., increment=false)`：**不增加 attempts，不改 state，只改 `next_retry_at`**。
- 计数上区分 `throttled`（被限流）与 `retried`（其它延后）。
- 配额按租户隔离；一个租户被限流不会影响其它租户的桶与在途计数。

## 9. At-least-once 与重复推送

链路中每个环节都可能重复：

| 环节 | 重复原因 | 吸收方式 |
| --- | --- | --- |
| 受理 | 客户端重发同一 `client_msg_id` | 唯一索引 + `ON CONFLICT DO NOTHING`，返回原 ID |
| Relay | 发布成功但事务提交前崩溃，下一轮再发 | `FOR UPDATE SKIP LOCKED` + 至多一次成功标记；重复事件被下游幂等吸收 |
| Fan-out | 同一事件重投；分页过程中途失败后整事件重放 | `InsertDeliveries` 的 `ON CONFLICT (message_id, user_id, device_id) DO NOTHING` |
| 领取 | Worker 崩溃后租约过期被接管 | 条件更新要求 `worker_id` 匹配 |
| 推送 | 帧已入队但状态未落库就崩溃 | 重推，客户端按 `message_id` 去重 |
| ACK | 客户端重发回执 | `state = 1` / `state IN (1,2)` 条件更新，第二次为空操作 |

因此系统保证「至少一次」而不是「恰好一次」：

- DELIVER 帧的 `MsgID` 是平台 `message_id`，客户端应把它作为去重键。
- 受理幂等键只保证消息不重复入库，不保证推送不重复。
- 重复推送不会让消息状态倒退：已 ACK 的行不满足 `state IN (0,1)`，Worker 无法再改它。
- 失败与过期为终态后，Worker 不再领取。

## 10. 计数与可观测性

`Engine.Snapshot()` 暴露本轮累计：`claimed`、`sent`、`retried`、`failed`、`throttled`、`expired`、`no_route`、`released`。`cmd/delivery-worker` 的 `-stats-interval`（默认 10s）额外打印 `outbox_pending`、`leases_held` 与 `tenant_inflight` 快照。
