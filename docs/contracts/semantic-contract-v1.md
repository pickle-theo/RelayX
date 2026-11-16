# Semantic Contract v1（S2 冻结）

> 本文件是架构方案第四部分「语义契约」在 S2 验收时冻结的独立快照。
> 上位事实来源仍是 `docs/通用实时消息 Gateway Platform - 架构设计方案.md` §4；
> 实现级展开见 `docs/S2 可靠消息内核 - 详细设计 LLD.md`。
> **S3–S8 只允许引用，不得重新定义本文件任何一项。**

## 1. 身份与来源

| 项 | 冻结值 |
|---|---|
| 生产身份 | `tenant_id + producer_id`，用户令牌 `u:*` / 服务令牌 `svc:*` 分流；`producer_kind ∈ {user, service}` |
| 可选展示身份 | `sender`，不参与幂等 |
| 来源端点 | `source_endpoint = (producer_id, source_device_id)`；`source_device_id` 由服务端从连接 session 派生并落库，客户端不可指定 |
| 空来源约定 | 服务令牌 / HTTP 提交写空串 `''`（**不是 NULL**），其客户端事件不路由 |
| 收件端点 | `endpoint = user_id + device_id`，`endpoint_id = user_id || ":" || device_id` |

## 2. 幂等键

| 类型 | 键 |
|---|---|
| 接入（逻辑消息） | `(tenant_id, producer_id, client_msg_id)` |
| 投递（每端点） | `(tenant_id, message_id, user_id, device_id)` |
| 事实 | `event_id`（确定性派生，主键幂等） |

## 3. 状态机（两张图，不得合并）

### 3.1 消息级 `messages.platform_state`

`ACCEPTED → SETTLED`（唯一正常出口）；`ACCEPTED → EXPIRED`（唯一失败出口）。
终态 `SETTLED` / `EXPIRED` 无出边。**不存在 `COMPLETED`。**

`SETTLED` 的含义是「平台侧已无更多投递动作」，**不代表业务成功、已读或完成**。
对外字段统一命名 `platform_state`，禁止 `success` / `completed` / `done`。

### 3.2 投递级 `deliveries.delivery_state`

`PENDING → SENT → RECEIPT_ACKED`；终态 `RECEIPT_ACKED` / `FAILED` / `EXPIRED`。
**投递级没有 `SETTLED`。**

### 3.3 聚合守卫（SETTLED 唯一判定）

当且仅当：

1. `platform_state = 'ACCEPTED'`；
2. `fanout_done_at IS NOT NULL`；
3. `NOT EXISTS (delivery WHERE message_id = ? AND delivery_state IN ('PENDING','SENT'))`。

满足后由 `settle_message` 单语句 CAS 写入 `SETTLED`、`state_version+1`、`settled_at=now()`；
0 行即放弃，不自旋、不重试推进。全部失败 / 全部过期 / 0 端点同样 `SETTLED`。

### 3.4 过期 CAS（与结算对称）

`expire_message`：`WHERE platform_state='ACCEPTED' AND expires_at <= now()`，0 行即放弃。
`expire_delivery`：`WHERE delivery_state IN ('PENDING','SENT')`，终态行不可改写。
`message.settled` 与 `message.expired` 靠两条 CAS 争同一行的 `ACCEPTED` 前置，永不同时存在。

## 4. ACK 语义

- **Receipt ACK**：传输事实，从 `SENT` 经 CAS 转 `RECEIPT_ACKED`，驱动消息聚合；端点由 session 隐含，客户端不可伪造。
- **Business ACK**：只写 `deliveries.application_status` / `application_acked_at`；**不进状态机、不改重试计划、不写传输事实、不触发聚合**。

## 5. 策略

`ttl_seconds` / `max_attempts` / `delivery_mode` / `fanout` 消息级可覆盖，平台硬上限：
TTL ≤ 2592000s（30 天）、attempts ≤ 10、payload ≤ 61440B（60 KiB）、fanout ≥ 0。
策略随 `SEND` 帧携带，由 `CAP_RELIABLE_POLICY (0x04)` 协商，不引入 `SEND_EX`。

## 6. 时间仲裁

所有到期 / 超时判定在**存储层的 `now()`** 内完成（PostgreSQL SQL `now()`；语义内存存储使用注入的 store clock）。
Go 本地时钟只决定「多久扫一次」与日志 / 指标时间戳。
**任何业务接口签名不得接收 `now` 参数。**

## 7. 事实

- `event_id = sha256(tenant|message_id|fact_type|coalesce(endpoint_id,"")|state_version)` 的 hex 前 32 字符。
- `fact_type` 只增不改：`message.accepted/settled/expired`、`delivery.created/sent/receipt_acked/failed/expired`。
- 消息级事实的 `payload_summary` 必须含 `producer_id`、`source_device_id` 与 `counts{total,receipt_acked,failed,expired}`；
  `counts` 必须由 CAS 的 `RETURNING` 一并返回，不得事后二次查询。
- 事实与状态变更**同事务**写入；`delivery_facts.published_at IS NULL` 表示待发布，发布顺序 `(occurred_at, event_id)`，至少一次 + `event_id` 幂等。
- `payload_summary` 绝不含 payload。

## 8. 帧号段

`0x01` LOGIN、`0x02` LOGIN_ACK、`0x03` SEND、`0x04` SEND_ACK、`0x05` DELIVER、
`0x06` RECEIPT_ACK、`0x07` BUSINESS_ACK、`0x08` PING、`0x09` PONG、
`0x0A` CLOSE、`0x0B` ERROR、`0x0C` ECHO（测试帧，S2 不注册）、`0x0D`–`0x0E` 预留。
**无 `SEND_EX`（`0x0F` 不设）。**

## 9. 其余阶段契约（仅引用）

- 广播：在线、Best-Effort、不重试、不补发、单 topic 内有序、不落库。
- 客户端事件：在线 Best-Effort、self-scoped、默认只推聚合终态、不持久化；路由键 `(tenant_id, producer_id, source_device_id)`。
- Error 帧错误码与 Close 原因码是两套独立编号空间，数值重叠不代表同义。

---

冻结于 S2 验收；后续阶段若需变更任一项，视为「重开冻结线」，必须列出穿越的异步边界清单并回退到单机阶段重做。
