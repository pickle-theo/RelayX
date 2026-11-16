# ADR 0003：事实 schema 与权威计数来源

- 状态：Accepted（S2）
- 决策：消息级事实的 `payload_summary` 必带 `producer_id`、`source_device_id` 与
  `counts`；`counts` 只能取自 `settle_message` / `expire_message` CAS 的 `RETURNING`。
- 理由：S4 事件投影与 S7 独立 router 只消费 facts，不回查业务库。`endpoint_id` 是
  **收件人**端点，不能冒充发送设备；没有 `source_device_id` 就无法把结算事件路由回
  发送者。事后二次查询 counts 会读到已继续变化的投递状态，与本次 `state_version` 不匹配。
- 后果：`payload_summary` 永不含 payload；`event_id = sha256(...)` 前 32 hex，
  同一事实重算得到同一键，消费端按主键幂等。
