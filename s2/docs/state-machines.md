# S2 状态机与转换表（实现视图）

两个状态机分属不同对象，不得画成一张图。

## 消息级 `messages.platform_state`

| 当前 | 事件 | 守卫 | 新状态 | 事实 |
|---|---|---|---|---|
| — | 受理 | 幂等未命中 | `ACCEPTED` | `message.accepted` |
| `ACCEPTED` | fanout 完成且无在途投递 | `fanout_done_at IS NOT NULL` + `NOT EXISTS(PENDING/SENT)` + CAS | `SETTLED` | `message.settled` |
| `ACCEPTED` | 过期扫描 | `expires_at <= now()` + CAS | `EXPIRED` | `message.expired` |

`SETTLED` 与 `EXPIRED` 均无出边；不存在 `COMPLETED`。零端点在同一受理事务内置
`fanout_done_at` 并直接结算。

## 投递级 `deliveries.delivery_state`

| 当前 | 事件 | 守卫 | 新状态 | 事实 |
|---|---|---|---|---|
| — | fanout | 投递幂等键 | `PENDING` | `delivery.created` |
| `PENDING` | Dispatch 成功 | 端点在线 | `SENT` | `delivery.sent` |
| `PENDING` | Dispatch 离线 | 端点不在线 | `PENDING`（`next_attempt_at=NULL`） | 无 |
| `SENT` | Receipt ACK | `state_version` CAS | `RECEIPT_ACKED` | `delivery.receipt_acked` |
| `SENT` | 重试 | `next_attempt_at <= now()` 且 `attempts < max` | `SENT`（attempts+1） | `delivery.sent` |
| `SENT` | 重试耗尽 | `attempts >= max` | `FAILED` | `delivery.failed` |
| `PENDING`/`SENT` | 过期 | CAS，仅非终态可转 | `EXPIRED` | `delivery.expired` |

投递级没有 `SETTLED`。Business ACK 不出现在这张表里：它只写
`application_status` / `application_acked_at`。
