# S2 Access API

所有接口要求 `Authorization: Bearer <token>`。令牌由共享 HMAC 密钥签发，
`producer_id` 从令牌派生，请求体无法伪造。

## POST `/v1/messages`

统一受理入口，与 TCP `SEND` 走同一个 `IngressService.Submit`。HTTP 提交没有连接上下文，
`source_device_id` 写空串（不是 NULL），其客户端事件不路由。

请求：

```json
{
  "client_msg_id": "c1",
  "target_user_id": "bob",
  "target_devices": ["phone"],
  "payload": "<base64>",
  "policy": {"ttl_seconds": 3600, "max_attempts": 3, "delivery_mode": "all_endpoints", "fanout": 0}
}
```

响应 `201`：

```json
{"message_id":"...","platform_state":"ACCEPTED","duplicate":false,"accepted_at":"..."}
```

错误：`400` 策略超限；`409` 幂等键冲突但 payload 不同；`503` 数据库不可用。

## GET `/v1/messages/{id}`

返回 `platform_state`、`state_version`、`producer_id`、`source_device_id`、
时间戳与 `counts`。**不含 payload**。

## GET `/v1/messages/{id}/deliveries`

分页（`limit` / `offset`）返回每个端点的 `delivery_state`、attempts、
`application_status`、`terminal_at`。

## GET `/v1/messages/{id}/facts`

返回 `event_id` / `fact_type` / `state_version` / `occurred_at` / `payload_summary`，
供业务只凭事实还原投递结果。

## 授权

跨租户一律 `404`（不暴露存在性）；用户令牌只能读自己生产的消息，服务令牌可读本租户全部。
查询接口不触发任何状态变化。

## 观测

`/metrics` 与 `/healthz` 在独立的 metrics 端口，四通道 depth/dropped/bytes 聚合自
`internal/sendpath`。
