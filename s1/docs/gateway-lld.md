# S1 单机 Gateway 核心实现说明

本文是 S1 实现备忘，契约来源见 `../../docs/S1 单机 Gateway 核心 - 详细设计 LLD.md`。
S1 继承 S0 冻结的 Frame 协议（`internal/protocol` 为 S0 文件的冻结快照 + S1 `body.go`）。

## 模块与依赖方向

```
cmd/gateway ──> gateway（Handler 编排）
                 ├── identity（令牌鉴权 → Producer）
                 ├── session（Endpoint/Session/Registry）
                 ├── route（Router，注入 itinerary 避免反向依赖 connection）
                 ├── connection（读循环 + Sequence 观测 + ConnectionManager）
                 │     └── sendpath（四通道 + 单 Writer）
                 └── telemetry（指标 + HTTP）
```

依赖单向：`connection` 不依赖 `gateway`（用 `Handler` 接口回调），`route` 不依赖
`connection`（用 `Destination.Deliver` 闭包注入）。这是可测且无环的关键。

## 冻结资产（S2–S8 复用，不重造）

1. **四通道 SendPath**（`internal/sendpath`）：P0 CONTROL / P1 RELIABLE /
   P2 BROADCAST / P3 EVENT，各自独立队列 + 帧数/字节双预算；Writer 严格
   P0→P1→P2→P3 取帧。P1 满背压（超时 = 慢消费者断连），P2/P3 满丢弃+计数。
2. **能力位分配**：`0x02` TOPIC_SUBSCRIBE / `0x04` RELIABLE_POLICY /
   `0x08` CLIENT_EVENTS / `0x10` CLOSE_REASON。S1 的 LOGIN_ACK 为 0。
3. **鉴权前移**：握手必鉴权，producer_id 由令牌派生，客户端不可伪造。
4. **领域模型**：Endpoint / Session / Connection / Producer。

## 出站帧通道归属（只追加）

| 通道 | 出站帧 | 阶段 |
|---|---|---|
| P0 | PING PONG CLOSE ERROR | S0/S4 |
| P1 | SEND_ACK DELIVER | S2 |
| P2 | BROADCAST | S3 |
| P3 | CLIENT_EVENT SUBSCRIBE_EVENTS_ACK | S4 |

入站帧（LOGIN/SEND/PING/SUBSCRIBE 等）不经过 SendPath。

## Wire format（S1 最小骨架）

见 `adr/0001-wire-format.md` 与 `internal/protocol/body.go`。帧头严格继承 S0。

## 验证

- 单元：四通道优先级/预算隔离/并发、鉴权、body 编解码、Sequence 观测。
- 集成：登录点对点、多设备 fan-out、鉴权拒绝、重复登录替换、断线清理。
- 并发（`go test -race`）：并发登录/断线、多连接并发。
- 验收场景（`cmd/im`）：`login-and-chat` / `multi-device` / `presence-simple`。

```powershell
cd .\s1
go test ./...
go test -race ./...
```
