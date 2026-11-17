# S3 广播协议与 QoS 边界

> 上位契约：`docs/S3 订阅与广播 - 详细设计 LLD.md`、`docs/contracts/semantic-contract-v1.md`。
> 本文是 S3 实现（`relayx/s3`）的落地说明，不重新定义语义。

## 1. 帧号段与 Body 布局

帧头继承 S0 冻结格式（14 字节，Magic `RX`，大端 Length/Sequence，CRC-16）。
S3 使用 `0x10`–`0x1F` 号段：

| 帧号 | 名称 | 方向 | Body |
|---|---|---|---|
| `0x10` | SUBSCRIBE | C→S | `[2B topicLen][topic][2B tokenLen][token]` |
| `0x11` | SUBSCRIBE_ACK | S→C | `[2B topicLen][topic][1B result][2B reasonLen][reason]` |
| `0x12` | UNSUBSCRIBE | C→S | `[2B topicLen][topic]` |
| `0x13` | UNSUBSCRIBE_ACK | S→C | `[2B topicLen][topic][1B result]` |
| `0x14` | BROADCAST | S→C | `[2B topicLen][topic][8B server_received_at ms][8B seq][payload...]` |

`SUBSCRIBE_ACK.result`：`0` 成功、`1` 验签失败、`2` 超限、`3` 重复订阅、`4` 跨租户、`5` topic 不一致、`6` 未登录。
`UNSUBSCRIBE_ACK.result`：`0` 成功、`1` 未订阅。

**关于共享帧的传输层 Sequence**：BROADCAST 是一次编码、N 个订阅者共享字节，无法按连接分配帧头
`Sequence`。实现中帧头 `Sequence = uint32(body.Seq)`。**有序/丢失判据是 Body 中的 `seq`（单 topic 单调递增），
不是帧头 Sequence**；多 topic 订阅时帧头 Sequence 可能跨 topic 回退，客户端不应据此判乱序。

## 2. 订阅鉴权

平台只验签，不查业务成员库。token 格式：

```
base64url(header).base64url(payload).signature
header  = {"alg":"HS256","kid":"<key-id>"}
payload = {"tid":"<tenant>","uid":"<user>","topic":"<topic>","exp":<unix_ts>}
signature = HMAC-SHA256(tenant_secret[kid], header + "." + payload)
```

验签顺序：alg 白名单 → kid 取密钥（支持轮转期新旧并存）→ 常量时间比签 → exp →
`tid == 连接 tenant` → `token.topic == 帧 topic`。任一步失败返回 `result=1/4/5`，
不影响已建立的订阅。默认开发密钥：`kid=dev`, `relayx-s3-dev-subscribe-secret`。

## 3. HTTP 受理 API

```
POST /v1/broadcasts
Authorization: Bearer <service-token>
Content-Type: application/json

{"topic":"room-1","payload":"<base64>","idempotency_key":"optional"}
```

- `payload` 在 JSON 中是 base64 字符串，上限 **8 KiB**；平台不解析内容。
- `202 Accepted` = 已进入内存扇出流程，**不代表任何订阅者已收到**。
- `idempotency_key` 仅用于 30 秒内存去重窗口，窗口外不保证；不落库。
- 401 未授权 / 400 topic 非法或 payload 超限；不写 PostgreSQL、不产生 delivery 记录。

## 4. QoS 与背压

广播复用 S1 四通道的 **P2 BROADCAST**（默认 128 帧 / 2 MiB，约 25% 预算），与 P1 可靠消息物理隔离。

- P2 预算充足：入队 +1 引用，正常广播。
- P2 预算耗尽：`TrySendBroadcast` 返回 `ErrBudgetExhausted`，丢弃当前帧 + 计数，**不关连接**。
- 连续丢弃超过阈值（默认 32）：该连接降级为只保留可靠消息，指标 `broadcast_slow_consumers` 反映；预算回落后自动恢复。
- P0 心跳始终最高优先，广播不得拖死心跳。

## 5. 共享字节与引用计数

`*ImmutableFrame` 是全链路唯一形态（接口、编码器返回值、队列元素、接收者），禁止值传递：
`atomic.Int32` 被复制会造成计数器分家、buffer 提前归还与跨帧串数据。

| 角色 | Retain | Release |
|---|---|---|
| 扇出方 | `NewImmutableFrame` 初始 refs=1 | 临界区外 `Release()` |
| P2 队列 | `TrySendBroadcast` 入队成功 +1 | 预算耗尽不持有；`Close` 排空残留 |
| Writer | 借用 | `WriteTo(w)` 后立即 -1 |

`buf` 不导出；`Release` 见负计数或 nil pool 立即 panic；归零时 `buf` 置 nil 并归还 pool，
此后 `WriteTo` 写 0 字节、`Len()` 返回 0。`go vet` 的 `passes lock by value` 是 CI 门禁。

## 6. 并发与有序

**per-(tenant, topic) 临界区内一次性完成**：① `seq` 分配 → ② 共享编码 → ③ 逐订阅者入队。
不同 (tenant, topic) 可并发。临界区内 `TrySendBroadcast` 非阻塞，无网络 I/O。
索引用 `RWMutex`：快照在读锁内复制、释放锁后投递；投递前校验连接存活与 `Version` 未变。

**租户隔离**：索引键与 `seq` 都以 `(tenant_id, topic)` 为单位——`tenant_id` 来自发布方服务令牌、
订阅方来自 token 的 `tid`。同名 topic 在不同租户下互不可见，一个租户的 publish 绝不投给其他租户的订阅者。

## 7. 指标

`/metrics` 端口同时暴露 `gateway_*` 与以下 `broadcast_*`：

`broadcast_subscribers`、`broadcast_active_topics`、`broadcast_publish_tps_total`、
`broadcast_publish_bytes_total`、`broadcast_fanout_frames_total`、
`broadcast_dropped_total{reason="budget|stale|degraded"}`、`broadcast_slow_consumers`、
`broadcast_index_full_total`、`broadcast_subscribe_failed_total`、
`broadcast_fanout_latency_us_bucket|sum|count`。

## 8. 验证

```powershell
cd .\s3
gofmt -w . ; go vet ./... ; go test ./... ; go test -race ./...
go test -bench=Benchmark -benchmem ./internal/broadcast

# 端到端（终端 A）
go run ./cmd/gateway -addr 127.0.0.1:9800 -metrics-addr 127.0.0.1:9801 -access-addr 127.0.0.1:9802
# 终端 B，场景：topic-fanout / fanout-order / unsubscribe-race /
#          compat-old-client / shared-buffer-integrity / slow-consumer-drop
go run ./cmd/live -scenario topic-fanout
```
