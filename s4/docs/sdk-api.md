# S4 Go SDK API

> 包路径：`relayx/s4/sdk`（可独立 import）。
> 语义准绳：`docs/S4 SDK 与生命周期事件 - 详细设计 LLD.md` 与
> `docs/contracts/semantic-contract-v1.md`。

## 1. 最小用法

```go
c := sdk.New(sdk.Config{
    Addr:         "127.0.0.1:9800",
    Token:        token,              // 业务签发的 HMAC 令牌
    DeviceID:     "dev-a",
    QueryBaseURL: "http://127.0.0.1:9802", // 为空则不启用 pending 追踪
    Hooks: sdk.Hooks{
        Ready: func() { /* 可以 Send 了 */ },
        MessageSettled: func(i sdk.MessageSettledInfo) {
            // 平台侧已无更多投递动作；不代表业务成功。
            log.Printf("settled %s %+v", i.MessageID, i.Counts)
        },
    },
})
if err := c.Connect(ctx); err != nil { return err }
defer c.Close()

id, err := c.Send(ctx, "bob", []byte("hello"), sdk.WithClientMsgID("biz-1"))
```

接收方：

```go
Hooks: sdk.Hooks{
    Delivered: func(i sdk.DeliveredInfo) {
        _ = c.SendReceipt(i.MessageID) // RECEIPT_ACK 驱动发送方 MESSAGE_SETTLED
    },
}
```

## 2. `SDKClient` 接口（冻结）

```go
type SDKClient interface {
    Connect(ctx context.Context) error
    Close() error
    Send(ctx context.Context, to string, payload []byte, opts ...SendOption) (string, error)
    Subscribe(ctx context.Context, topic, token string) error
    QueryMessage(ctx context.Context, id string) (*ClientMessageView, error)
    State() ConnState
}
```

`*Client` 额外提供：

| 方法 | 说明 |
|---|---|
| `SendReceipt(messageID) error` | 接收方回 `RECEIPT_ACK`，触发 `receipt.ack_sent` |
| `Caps() uint32` | 最近一次 `LOGIN_ACK` 能力位 |
| `GrantedEventMask() protocol.EventMask` | 服务端实际授予的事件位 |
| `PendingEnabled() bool` | 能力位 + 查询 API 双条件 |
| `PendingLen() int` / `GapQueries() int64` / `Backfills() int64` / `Reconnects() int64` | 验证与观测 |

## 3. 连接状态机

```
IDLE → CONNECTING → CONNECTED → AUTHENTICATING → READY
READY → DISCONNECTED → RECONNECTING → CONNECTING → ... → READY
任意非终态 → CLOSED
```

非法转换被拒绝（`StateMachine.Transition` 返回错误），关闭路径使用 `ForceTransition`。

### 3.1 重连退避（冻结）

| 项 | 值 |
|---|---|
| 初始 | 500 ms |
| 增长 | ×2（500ms → 1s → … → 16s → 30s） |
| 上限 | 30 s（固定） |
| 抖动 | `[0.5×delay, 1.5×delay]`，必须开启 |
| 放弃 | `Close()`；不可恢复 CLOSE（1000/1001/1002/1003/1004/1007/1008）；`MaxReconnectAttempts` 耗尽 |
| 不放弃 | `SERVER_DRAINING(1005)`、`HEARTBEAT_TIMEOUT(1006)`、网络错误 |

重连成功后退避计数归零。

## 4. Hooks 契约

| hook | 时机 | 优先级 |
|---|---|---|
| `Connecting` | 首次发起连接前 | 普通 |
| `Connected` | TCP 建立 | 普通 |
| `Authenticated` | LOGIN_ACK 成功 | 普通 |
| `Ready` | 能力协商完成、可收发 | 普通 |
| `Reconnecting` | 每次重连前（含 delay） | 普通 |
| `Disconnected` | 连接断开（可恢复） | 普通 |
| `Closed` | 终态关闭（含原因码） | **critical** |
| `AuthFailed` | 1004 / 令牌被拒 | **critical** |
| `HeartbeatTimeout` | 1006 | **critical** |
| `SessionReplaced` / `SessionKicked` / `SessionTokenExpired` / `ServerDraining` | 对应 CLOSE 码 | **critical** |
| `MessageAccepted` | SEND_ACK | 普通 |
| `MessageSettled` / `MessageExpired` | 终态事件或查询补齐 | **critical，每条消息最多一次** |
| `Delivered` | 收到 DELIVER | critical |
| `ReceiptAckSent` | 本端发出 RECEIPT_ACK | 普通 |
| `DeliveryReceiptAcked` / `DeliveryFailed` / `DeliveryExpired` | 高体积事件（需订阅） | 普通 |
| `BroadcastReceived` | 收到 BROADCAST | 普通（可丢） |
| `EventDropped` | 普通 hook 队列满 | — |

约束：

- 所有 hook 在独立 goroutine 的**有界队列**中异步执行，绝不在读循环内同步执行。
- 普通队列满时丢弃新任务并回调 `EventDropped`；critical 永不丢弃。
- hook panic 被 recover，不影响后续 hook 与连接。
- `MessageSettled` 只表达传输结算，不是业务完成；SDK 不会产生 `read` / `completed` /
  `business_acked` 事件。

## 5. 事件订阅与降级

SDK 在 LOGIN_ACK 后按 `Config.EventMask`（默认 `MESSAGE_SETTLED`）发 `SUBSCRIBE_EVENTS`，
以服务端 `granted_mask` 为准。能力位缺失时**不发送订阅帧**，降级为无事件模式。

`PendingTracker` 启用条件（硬规则）：`CAP_CLIENT_EVENTS` 已授予 **且**
`GET /v1/client/messages?ids=` 探测 2xx。缺一即关闭，`Send` 返回后只认为"平台已受理"。

## 6. 去重、stale 与 gap

```
if event_id in seen: drop
if state_version < localVersion[message_id]: drop
if seq != lastSeq + 1: triggerQueryBackfill()
```

- 首次收到事件不做 gap 判定（允许中途订阅）。
- 重连后先 `ResetSeq()`，新会话 seq 从 1 重新开始。
- 终态 hook 由 `message_id` 二次守卫，事件与查询补齐共用，保证每条消息最多触发一次。
- 去重窗口有界（默认 8192 个 event_id），窗口外视为新事件。

## 7. 发送与 pending

```
Send → SEND_ACK
  ├─ platform_state=ACCEPTED → 加入 pending
  └─ platform_state=SETTLED  → 不加入 pending，直接触发 message.settled（counts.total=0）
收到 MESSAGE_SETTLED / MESSAGE_EXPIRED → 移出 pending
重连成功 / seq gap → GET /v1/client/messages?ids=<pending> 补齐
pending 上限 10000，FIFO 淘汰（淘汰项不再承诺 hook）
```

## 8. 查询 API

```
GET /v1/client/messages/{id}
GET /v1/client/messages?ids=a,b,c
Authorization: Bearer <token>
```

返回 `message_id` / `platform_state` / `state_version` / `counts` / `settled_at` /
`expires_at`，**不含 payload**。仅发送者或同租户服务令牌可见；跨租户与未知消息一律 404。

## 9. 验证

```powershell
cd .\s4
gofmt -l .
go vet ./...
go test ./...
go test -race ./...

# 端到端（需要 -test-hooks）
go run ./cmd/gateway -addr 127.0.0.1:9800 -metrics-addr 127.0.0.1:9801 `
  -access-addr 127.0.0.1:9802 -test-hooks
go run ./cmd/scenario -gateway 127.0.0.1:9800 -access http://127.0.0.1:9802 -scenario all

# 两个客户端
go run ./cmd/im -mode receive -user bob -device dev-b -peer alice
go run ./cmd/im -mode send -user alice -device dev-a -peer bob -count 3
go run ./cmd/live -user viewer -device dev-live -topic room-1 -count 3
```
