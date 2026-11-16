# S2 传输事实 schema

表：`delivery_facts`（只追加）。主键 `event_id`，唯一键
`(tenant_id, message_id, fact_type, endpoint_id, state_version)`。

## event_id

```
event_id = sha256(tenant|message_id|fact_type|coalesce(endpoint_id,"")|state_version)
           的 hex 前 32 字符
```

实现：`internal/facts.Derive`，store 与 engine 共用同一函数。

## fact_type（冻结，只增不改）

`message.accepted` / `message.settled` / `message.expired` /
`delivery.created` / `delivery.sent` / `delivery.receipt_acked` /
`delivery.failed` / `delivery.expired`

## payload_summary

| 事实 | 内容 |
|---|---|
| 消息级（三类） | `producer_id`、`source_device_id`、`counts` |
| `message.accepted` | `counts` 只有 `total` |
| `message.settled` / `message.expired` | `counts = {total, receipt_acked, failed, expired}`，取自 CAS `RETURNING` |
| 投递级 | `user_id`、`device_id`、`attempts` |

绝不写入 payload。

## 发布水位

`published_at IS NULL` 表示未发布。S2 恒为 NULL；S7 Outbox Relay 按
`(occurred_at, event_id)` 顺序发布并置位，至少一次 + event_id 幂等。
