# S4 客户端事件协议与投影说明

> 上位文档：`docs/S4 SDK 与生命周期事件 - 详细设计 LLD.md`（下称 LLD）。
> 本文只记录 S4 module 的实现口径；任何语义冲突以 LLD §15.2 冻结清单为准。

## 1. 帧号与 Body（冻结）

| 帧号 | 名称 | 方向 | Body |
|---|---|---|---|
| `0x20` | SUBSCRIBE_EVENTS | C→S | `event_types: uint32` |
| `0x21` | SUBSCRIBE_EVENTS_ACK | S→C | `result: uint8` + `granted_mask: uint32` |
| `0x22` | CLIENT_EVENT | S→C | `event_id` + `seq: uint64` + `type` + `tenant_id` + `message_id` + `state_version: int32` + `counts?` + `occurred_at: int64(ms)` |

`CLOSE (0x0A)` 不新增帧号，只在 body 追加：

```
reason_code: uint16
reason_text: string
```

空 body 等价于 `NORMAL`，旧客户端忽略 body。

字符串一律 2 字节长度前缀，整数大端序。`CLIENT_EVENT` 不携带 payload、接收方设备 ID 或业务状态。

## 2. 事件类型位（与能力位是两套空间）

| 位 | 掩码 | 事件 | 默认 |
|---|---|---|---|
| bit0 | `0x01` | `MESSAGE_SETTLED` | 开，**服务端不得拒绝** |
| bit1 | `0x02` | `MESSAGE_EXPIRED` | 关 |
| bit2 | `0x04` | `DELIVERY_RECEIPT_ACKED` | 关（高体积） |
| bit3 | `0x08` | `DELIVERY_FAILED` | 关（高体积） |
| bit4 | `0x10` | `DELIVERY_EXPIRED` | 关（高体积） |
| bit5 | `0x20` | `MESSAGE_PARTIAL` | 关 |

- `granted = (requested & 0x3F) | 0x01`：未分配位静默忽略，`MESSAGE_SETTLED` 强制授予。
- 服务端必须回 `granted_mask`，客户端不得假定"请求即授予"。
- 能力位 `CAP_CLIENT_EVENTS=0x08` / `CAP_CLOSE_REASON=0x10` 表示"服务端支持哪些帧"，
  与事件位数值重叠但不通用；代码中分别是 `protocol.Cap*`（uint32）与
  `protocol.EventMask`（具名类型）。

## 3. 门控与降级

| 组合 | 行为 |
|---|---|
| 旧客户端 + 新服务端 | 不订阅 → 服务端不推任何 `CLIENT_EVENT`；CLOSE body 被忽略 |
| 新客户端 + 旧服务端 | 能力位缺 `CAP_CLIENT_EVENTS` → SDK 不发订阅帧，仅基础 hooks，不启用 `PendingTracker` |
| 新 + 新 | 完整事件 + 查询补齐 |

`PendingTracker` 的硬规则：`CAP_CLIENT_EVENTS` 已授予 **且** 查询 API 探测成功才启用；
缺一即不记录 pending（否则 pending 永不清空，内存泄漏 + 终态 hook 永不触发）。

## 4. 投影（服务端）

- 触发源：游标轮询 `delivery_facts`，间隔默认 100 ms，批 500；事实写入信号只是加速唤醒。
- 顺序：`ORDER BY occurred_at, event_id`，键集分页 `(occurred_at, event_id) > cursor`。
- 先路由全部事件，再 `Save` 游标：崩溃只重复、不遗漏；重复由 `event_id` 幂等吸收。
- 投影只消费终态事实：`message.settled` / `message.expired`（默认只投影前者），以及按订阅
  开启的三类投递级事实。`business_ack` 不产生事件。
- 聚合计数唯一权威来源是 `payload_summary.counts`（S2 结算事务内算好）；本 module 的
  内存事实内核与回查 `deliveries` 的数值由单测做双源比对。

### 4.1 事实时间戳单调性

`(occurred_at, event_id)` 作为游标键，要求 `occurred_at` 与插入顺序一致。内存事实内核在
每次 append 时保证 `occurred_at` 严格递增（同批事实共享同一时间戳，批间至少 +1 ns）。
否则会出现：前一批已把游标推到同时间戳、更大 `event_id` 的事实，后一批中 `event_id` 更小
的新事实永远落在游标之外（实测过的漏读）。SQL 侧同理：同一事务内共享 `now()`，跨事务要求
时钟单调或由 outbox 发布水位兜底。

## 5. self-scoped 路由

```
key = (tenant_id, producer_id, source_device_id)
```

- `producer_id` 与 `source_device_id` 取自事实 `payload_summary`；S4 单机缺失时回查
  `messages.source_device_id`（等价实现，不是第二套定义）。
- `source_device_id` 为空（HTTP 提交 / 服务令牌）→ 丢弃并计 `client_event_no_route_total`，
  **绝不**降级为"推给该用户全部设备"。
- 设备离线 → 丢弃，不重试不落库；计 `client_event_route_failed_total` 与
  `client_event_dropped_total{reason="offline"}`。
- 会话未订阅对应位 → 丢弃并计 `not_subscribed`。

### 5.1 seq 分配点（冻结）

| 阶段 | 产出 | 分配 seq |
|---|---|---|
| `Project(fact)` | `ClientEvent{Seq:0}` | 否 |
| `Route(ev)` 且会话在线 | `allocator.Next(sessionID)` | **是（唯一分配点）** |
| P3 入队 | 帧写出 | 否 |

会话结束即销毁计数器。若在投影器（全局单消费者）分配，单会话看到的序号会跳跃，
gap 检测退化为"每次都报警"。

## 6. P3 通道与丢弃

- 事件走 `sendpath.P3`，预算独立，不占用 P1/P2。
- 预算耗尽：`TryEnqueueEvent` 返回 `ErrBudgetExhausted`，丢弃 + 计数，连接保持。
- `SUBSCRIBE_EVENTS_ACK` 走 P0（握手确认，不能被事件预算丢弃）。
- 事件不写数据库，不计入受理/ACK 写放大。

## 7. CLOSE 原因码（冻结，只追加）

| 码 | 名称 | SDK hook | 是否重连 |
|---|---|---|---|
| 1000 | NORMAL | `closed` | 否 |
| 1001 | REPLACED | `session.replaced` + `closed` | 否 |
| 1002 | KICKED | `session.kicked` + `closed` | 否 |
| 1003 | TOKEN_EXPIRED | `session.token_expired` + `closed` | 否 |
| 1004 | AUTH_FAILED | `auth_failed` + `closed` | 否 |
| 1005 | SERVER_DRAINING | `server.draining` + `disconnected` | **是** |
| 1006 | HEARTBEAT_TIMEOUT | `heartbeat_timeout` + `disconnected` | **是** |
| 1007 | POLICY_VIOLATION | `closed` | 否 |
| 1008 | INTERNAL_ERROR | `closed` | 否 |

未知名按 `NORMAL` 处理并记录。CLOSE 码空间与 S2 `ERROR` 码空间（1001–1005）相互独立，
SDK 按帧类型分派解析。

## 8. 指标

```
client_event_produced_total{type}
client_event_delivered_total
client_event_dropped_total{reason="queue_full"|"offline"|"stale"|"not_subscribed"|"injected"}
client_event_route_failed_total
client_event_no_route_total
client_event_queue_depth
client_query_latency_seconds_{bucket,sum,count}
reconnect_reason_total{reason}
```

`stale` 由端侧去重判定（`state_version` 回退），服务端不产生该原因的丢弃，
因此 `reason="stale"` 恒 0、但仍在标签集内输出——标签集是冻结契约，不能因为当前恒 0 就删掉序列。
`injected` 仅 `-test-hooks` 的 gap 注入使用。
