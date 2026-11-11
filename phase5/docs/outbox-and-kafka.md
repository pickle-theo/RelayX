# Outbox 与 Kafka

本文描述事务性 outbox 的表结构、Relay 的领取与发布算法、发布失败/崩溃后的行为、两个主题的事件体、分区键、Kafka 生产与消费参数，以及消费端幂等。实现位于 `internal/store/outbox.go`、`internal/outbox/relay.go`、`internal/broker`。

## 1. outbox 表

`migrations/0002_outbox_tenants_leases.sql`：

```sql
CREATE TABLE IF NOT EXISTS outbox (
    id              BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic           TEXT        NOT NULL,
    partition_key   TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    state           SMALLINT    NOT NULL DEFAULT 0,
    attempts        INT         NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS outbox_pending_idx
    ON outbox (next_attempt_at, id)
    WHERE state = 0;
```

| 列 | 用途 |
| --- | --- |
| `id` | 自增主键，也是扫描排序键；保证先受理先发布 |
| `topic` | 目标主题，受理时写 `relayx.fanout.v1` |
| `partition_key` | Kafka 消息 key，决定分区 |
| `payload` | 预编码的 JSON 事件体（`jsonb` 存储，按原字符串发布） |
| `state` | `0` 待发布、`1` 已发布（`store.OutboxPending/OutboxPublished`） |
| `attempts` | 发布尝试次数，成功后也会 +1 |
| `next_attempt_at` | 下一次可发布时间，失败时按指数退避推后 |
| `published_at` | 发布成功时间 |

索引是**部分索引**，只覆盖 `state = 0`：已发布的行不进入扫描路径，表变大也不影响 relay 的索引成本。当前没有清理已发布行的任务，行会一直保留。

## 2. RelayOutbox 算法

`store.RelayOutbox` 在一个事务内完成「领取 → 逐条发布 → 标记」：

```sql
SELECT id, topic, partition_key, payload, attempts
FROM outbox
WHERE state = 0 AND next_attempt_at <= now()
ORDER BY id
LIMIT $1
FOR UPDATE SKIP LOCKED
```

1. `limit <= 0` 直接返回 `(0, nil)`。
2. 事务内执行上面的查询，一次拿到至多 `limit` 行；`FOR UPDATE` 给这些行加行锁，`SKIP LOCKED` 跳过其它 relay 实例已锁定的行。
3. 按 `id` 升序逐条调用注入的 `publish(ctx, OutboxEvent)`（生产环境是 `broker.Producer.Publish`）。
4. 发布成功：

```sql
UPDATE outbox
SET state = 1, published_at = now(), attempts = attempts + 1
WHERE id = $1
```

5. 发布失败：

```sql
UPDATE outbox
SET attempts = attempts + 1,
    next_attempt_at = now() + make_interval(secs => LEAST(
        $2::float8 * power(2::float8, attempts::float8),
        $3::float8))
WHERE id = $1
```

   记录失败后**立即停止本批**，跳出循环并返回 `publishErr`；该行保持 `state = 0`。
6. 事务提交后行锁释放。由于发布发生在事务内，**行锁会一直持有到提交**，两个 relay 实例不会同时发布同一行。
7. 返回值 `published` 是本批成功条数；`err` 是事务错误，`publishErr` 通过第二个返回值交给调用方。

`outbox.Relay.Run` 用固定间隔驱动：

```go
ticker := time.NewTicker(r.interval)   // 默认 500ms
for {
    if _, err := r.drain(ctx); err != nil && ctx.Err() == nil {
        r.logger.Warn("outbox relay failed", "error", err)
    }
    select {
    case <-ctx.Done(): return
    case <-ticker.C:
    }
}
```

Relay 的默认值与兜底（`outbox.NewRelay`）：`batch <= 0 → 100`、`interval <= 0 → 500ms`、`backoffBase <= 0 → 1s`、`backoffMax < backoffBase → backoffBase`。

## 3. 发布成功、失败与崩溃

| 时机 | 数据库状态 | 是否重复发布 | 恢复 |
| --- | --- | --- | --- |
| 发布成功且事务提交 | `state = 1`，`published_at` 有值 | 不会 | 无需处理 |
| 发布调用返回错误 | `state = 0`，`attempts + 1`，`next_attempt_at` 推后 | 未发布 | 到点后重新领取；退避上限 `-retry-backoff-max`（默认 1m） |
| 发布成功但提交前进程崩溃 | `state = 0`，`attempts` 未变 | 会重复发布 | 下一轮重新发布；下游 fan-out 幂等吸收 |
| 本批前几行已标记、后续行失败 | 已成功行保持 `state = 1`，失败行 `state = 0` | 仅失败行重发 | 下一轮从失败行继续 |
| 事务整体回滚（含已成功的标记） | 全部保持 `state = 0` | 已发布的行会再次发布 | 下游幂等，不产生重复投递行 |

结论：outbox 提供**至少一次**发布。它不承诺「不重复」，只承诺「不丢失」。重复由 fan-out 的 `ON CONFLICT` 与投递引擎的数据库真值读取吸收。

Relay 的运行位置：

- `cmd/delivery-worker` 总是启动一个 Relay（`-outbox-batch`、`-outbox-interval`）。
- `cmd/gateway` 在 `-kafka-brokers` 非空时启动一个 Relay，退出时先停止 Relay 再关闭连接池。
- 多个 Relay 可以同时运行，依靠 `FOR UPDATE SKIP LOCKED` 与行锁互斥。

## 4. 主题

| 主题 | 生产方 | 消费方 | 消费组 | 分区数 | 内容 |
| --- | --- | --- | --- | --- | --- |
| `relayx.fanout.v1` | Outbox Relay | Fan-out Worker | `relayx-fanout` | 3 | 一条受理消息一个事件 |
| `relayx.deliver.v1` | Fan-out Worker | Delivery Worker | `relayx-delivery` | 3 | 一个收件端点一个唤醒事件 |

常量定义在 `internal/broker/events.go`：`TopicFanout = "relayx.fanout.v1"`、`TopicDeliver = "relayx.deliver.v1"`、`EventVersion = 1`。

## 5. 事件体

### 5.1 FanoutEvent（version = 1）

| JSON 字段 | 类型 | 说明 |
| --- | --- | --- |
| `version` | int | 固定 1；解码时校验，不匹配即报错 |
| `tenant_id` | string | 租户 |
| `message_id` | uint64 | 平台消息 ID，由受理事务分配后写入 |
| `from_user` | string | 发送者 |
| `from_device` | string | 发送设备 |
| `target_type` | int16 | `1` = user、`2` = endpoint |
| `target` | string | user 目标为用户名；endpoint 目标为 `user:device` 或原始目标串 |
| `target_user` | string（omitempty） | 已解析的端点用户，Gateway 解析 session 后写入 |
| `target_device` | string（omitempty） | 已解析的端点设备 |
| `payload` | base64 string | `[]byte` 的 JSON 编码 |
| `expire_at` | RFC3339 时间 | 消息过期时间，来自 `ttl` 计算 |

示例：

```json
{
  "version": 1,
  "tenant_id": "acme",
  "message_id": 42,
  "from_user": "alice",
  "from_device": "d1",
  "target_type": 1,
  "target": "bob",
  "payload": "aGVsbG8=",
  "expire_at": "2026-11-11T10:00:00Z"
}
```

### 5.2 DeliverEvent（version = 1）

| JSON 字段 | 类型 | 说明 |
| --- | --- | --- |
| `version` | int | 固定 1 |
| `tenant_id` | string | 租户 |
| `message_id` | uint64 | 消息 ID |
| `user_id` | string | 收件用户 |
| `device_id` | string | 收件设备 |

它只是**唤醒提示**：`cmd/delivery-worker` 解码后忽略内容并调用 `pool.Notify()`，Engine 始终重新扫描 PostgreSQL。丢失或重复该事件只改变唤醒延迟。

## 6. 分区键

| 函数 | 拼接方式 | 使用方 |
| --- | --- | --- |
| `FanoutKey(tenantID, userID)` | `tenantID` 与 `userID`，以竖线字符分隔 | 受理事务写入 `outbox.partition_key` |
| `DeliverKey(tenantID, userID, deviceID)` | 三个字段，以竖线字符分隔 | fan-out 发布唤醒事件 |

- 受理时 `user` 取已解析的 `targetUser`，否则取原始 `target`（`internal/message/service.go`）。
- 相同键的事件进入同一分区，从而获得分区内顺序；不同用户/设备之间没有顺序保证。
- 生产者使用 `kafka.Hash` balancer，按 key 计算分区。
- 分区数 3 由 `make kafka-up` / `EnsureTopics` 创建，修改分区数会改变 key 到分区的映射。

## 7. 生产参数

`broker.NewProducer`：

| 参数 | 值 | 作用 |
| --- | --- | --- |
| `Balancer` | `&kafka.Hash{}` | 按 key 哈希分区 |
| `RequiredAcks` | `kafka.RequireAll` | `acks=all`，所有 ISR 确认后才返回成功 |
| `BatchSize` | 100 | 单批记录数 |
| `BatchTimeout` | 20ms | 凑批等待上限 |
| `WriteTimeout` / `ReadTimeout` | 10s | 单次写入/读取超时 |
| `Async` | `false` | 同步发布，`Publish` 阻塞到 broker 确认 |

`acks=all` 保证 `Publish` 返回 nil 时记录已被 broker 接受；Relay 只有在此之后才把 outbox 行标记为已发布。

## 8. 消费与 offset 提交

`broker.NewConsumer`：

| 参数 | 值 | 说明 |
| --- | --- | --- |
| `GroupID` | `relayx-fanout` / `relayx-delivery` | 两个独立消费组 |
| `CommitInterval` | `0` | 关闭自动提交，全部手动 |
| `MinBytes` / `MaxBytes` | 1 / 1 MiB | 单次拉取范围 |
| `MaxWait` | 500ms | 拉取等待 |
| `PartitionWatchInterval` | 500ms | 分区变化检测 |
| `WatchPartitionChanges` | `true` | 支持再均衡 |
| `Dialer.Timeout` | 5s | 连接超时 |

`Consumer.Run` 的顺序：

1. `FetchMessage`；`ctx` 取消时返回 nil。
2. 构造 `broker.Message`（`Value` 只在 handler 返回前有效）。
3. 调用 `handler`。
4. **只有 handler 返回 nil 才** `CommitMessages`；提交失败视为致命错误返回。

handler 返回错误时 `Run` 立即返回，offset 不提交，记录会在消费者重启或重新定位后重投。`cmd/delivery-worker` 把该错误发送到 `errCh` 并退出主循环，因此需要外部进程管理器重启 Worker。

## 9. EnsureTopics

`broker.EnsureTopics(ctx, brokers, partitions, topics...)`：

- 用 `kafka.Client.CreateTopics` 创建缺失主题，副本因子固定 `DefaultReplicas = 1`。
- `partitions <= 0` 时回退 `DefaultPartitions = 3`。
- `TopicAlreadyExists` 视为成功，可被每个进程安全重复调用。
- `cmd/delivery-worker` 启动时调用：`EnsureTopics(ctx, cfg.KafkaBrokers, broker.DefaultPartitions, cfg.FanoutTopic, cfg.DeliverTopic)`，失败则进程退出。
- Gateway 与 Message Service 不调用 `EnsureTopics`；它们只写 outbox / 不直接发布。

## 10. 消费幂等

| 消费方 | 重复/异常事件 | 处理 |
| --- | --- | --- |
| Fan-out | 同一 `message_id` 重复 | `InsertDeliveries` 使用 `ON CONFLICT (message_id, user_id, device_id) DO NOTHING`，不产生重复投递 |
| Fan-out | 分页处理到一半失败 | 返回错误，offset 不提交，整个事件重放；已插入的行被冲突忽略 |
| Fan-out | 非法 JSON 或 `version != 1` | 记录警告并**提交**（这类事件永远不会成功） |
| Fan-out | `target_type` 未知 | 记录警告并提交 |
| Fan-out | endpoint 目标在 `endpoints` 中不存在 | 记录警告并提交，不创建投递 |
| Fan-out | user 目标没有任何端点 | 记录警告，事件成功提交 |
| Delivery | 重复唤醒事件 | `Notify()` 非阻塞、可合并，重复无副作用 |
| Delivery | 非法事件体 | 记录警告后仍 `Notify()` 并提交 |
| Delivery | 事件丢失 | 定时扫描仍在 `-scan-interval` 内推进，只增加延迟 |

Fan-out 的「基础设施错误」（数据库或 Kafka 发布失败）才返回错误触发重投；业务上无法成功的事件一律提交并告警。
