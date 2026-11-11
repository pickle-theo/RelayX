# RelayX Phase 2：单机 At-Least-Once 可靠投递

- 状态：功能实现完成；`gofmt` / `go vet` / `go test` / Docker race 四项门禁通过；**1000 连接可靠模式压测未达到“零丢失”目标**，原因与数据见[压测报告](压测报告.md)
- 模块：`relayx/phase2`（`go 1.27`，唯一直接第三方依赖 `github.com/jackc/pgx/v5 v5.12.0`）
- 前序阶段：[Phase 1 README](../../phase1/docs/README.md)（单机实时 Gateway，无持久化）
- 数据：[千连接可靠模式压测报告](压测报告.md) / [loadtest 原始输出](loadtest-reliable.txt)
- 决策记录：[ADR-007](adr/ADR-007.md)、[ADR-008](adr/ADR-008.md)、[ADR-009](adr/ADR-009.md)

## 1. 定位与范围

Phase 2 在 Phase 1 的 **Connection → Session → Router** 之上，把“消息是否送达”从**进程内尽力而为**升级为**单机 At-Least-Once**：

1. `SEND` 先在一个 PostgreSQL 事务里写入 `messages` 与每个收件端点的 `deliveries`，事务提交后才回 `SEND_ACK(ACCEPTED)`。
2. 在线目标立即收到 `DELIVER`，客户端先回 `RECEIPT_ACK`（已收到），业务处理完再回 `BUSINESS_ACK`（业务结果）。
3. 目标离线时投递保持 `PENDING`，目标下次登录后通过 `PULL` 按投递 ID 游标拉取。
4. 已推送但未确认的投递由 Retry Worker 按指数退避重推，超过最大次数转 `FAILED`。
5. 消息超过 TTL 后由 Worker 标记 `EXPIRED`，不再推送。

明确**不包含**：

- Exactly-Once、事务性业务处理、跨设备去重
- 多节点 Gateway、跨节点路由、分布式锁
- 鉴权、TLS、Metrics/Prometheus 端点（仍只有日志 stats）
- 消息历史查询 API、管理后台、死信队列的可视化

“交付语义”边界：平台保证**至少一次入库与至少一次尝试推送**；客户端可能收到重复 `DELIVER`，业务必须按平台 `message_id` 自行幂等。平台不保证全局有序。

## 2. 与 Phase 1 的差异

| 维度 | Phase 1 | Phase 2 |
| --- | --- | --- |
| 持久化 | 无，全部进程内 | PostgreSQL：`endpoints` / `messages` / `deliveries` / `schema_migrations` |
| 投递保证 | 在线尽力而为，目标离线只回 `ERROR` | 先落库再推送，At-Least-Once；离线保留 `PENDING` |
| 幂等键 | 无 | 帧头 `MsgID` + `(from_user, client_msg_id)` 唯一约束 |
| ACK | 无 | 两级：`RECEIPT_ACK(0x0B)` + `BUSINESS_ACK(0x0C)` |
| 离线恢复 | 不支持 | `PULL(0x0D)` / `PULL_DONE(0x0E)`，按 `deliveries.id` 游标分页 |
| 重试 | 无 | Worker 每 `1s` 扫描，`2s` 起指数退避、上限 `60s`、最多 `5` 次 |
| TTL | 无 | 默认 `24h`，Worker 每 `1s` 扫描过期消息 |
| 协议帧 | 9 种（`0x01`–`0x09`） | 14 种（新增 `0x0A`–`0x0E`） |
| 状态模型 | 只有 Session | 消息 4 态 + 投递 7 态 |
| 第三方依赖 | 零 | pgx v5（PostgreSQL 驱动） |
| 每连接服务端协程 | 2（read + write） | 仍为 2；数据库调用在 `readLoop` 上同步执行 |
| 失败语义 | 路由失败回 `ERROR` | 路由失败回 `SEND_ACK(INVALID/INTERNAL)`；已入库消息不受影响 |
| 观测字段 | 5 个 | 11 个（新增 6 个可靠投递计数） |
| 1000 连接基线 | 57003/57003，P99 0.552 ms | 56994/56994，P99 0.467 s，`ack_missed=0`（见压测报告） |

## 3. 架构

```text
                  cmd/client          cmd/loadtest
                       │                   │
                       └─────────┬─────────┘
                                 │ TCP：21 B 大端帧头 + CRC32
                                 ▼
┌──────────────────────────── internal/transport ────────────────────────────┐
│  每条连接 2 个协程                                                          │
│  readLoop（解码、PONG、心跳/空闲超时）        writeLoop（唯一写 socket）      │
│        │                                          ▲                        │
│        │  gateway.Handler.Handle                  │ 有界队列（256 帧/4 MiB）│
│        ▼                                          │ 批量写（16 帧/64 KiB）   │
│  internal/gateway.Handler ─────────────── Conn.Send ┘                       │
└────────┬───────────────────────────────────────────────────────────────────┘
         │
         ├─ LOGIN ──────► store.UpsertEndpoint ──► session.Manager.Register
         │                                        （同 (user,device) 替换旧连接）
         │
         ├─ SEND ───────► store.ListEndpoints（TargetUser）
         │                store.AcceptMessage ── 单条语句：messages + deliveries
         │                │   在线目标直接插入为 SENT/attempts=1/首次重试时间，
         │                │   离线目标插入为 PENDING
         │                ├─ 新消息：SEND_ACK(ACCEPTED, message_id)
         │                └─ 幂等冲突：SEND_ACK(DUPLICATE, 原 message_id)，不重复推送
         │                delivery.Deliverer ──► 目标 Session.Send(DELIVER)
         │
         ├─ RECEIPT_ACK ─► store.ApplyReceiptAck ── 单条语句：条件更新 + 重算消息状态
         ├─ BUSINESS_ACK ► store.ApplyBusinessAck ─ 单条语句：条件更新 + 重算消息状态
         │
         └─ PULL ────────► store.PullPending ── 单事务：SELECT ... FOR UPDATE
                            │                    + 逐行标记 SENT
                            ├─ N × DELIVER（header MsgID = 平台 message_id）
                            └─ PULL_DONE(count, next_cursor, has_more)

 internal/delivery.Worker（1 个常驻协程，1s ticker）
   ├─ DueRetries   → 对在线目标重推 → MarkDeliveryRetried（attempts+1）
   ├─ 超过 5 次    → MarkDeliveryFailed（投递 FAILED，消息重算）
   └─ ListExpired  → ExpireMessage（投递 EXPIRED，消息 EXPIRED）

                                 │
                                 ▼
                         PostgreSQL（本机 Docker）
   endpoints / messages / deliveries / schema_migrations
   索引：deliveries_pull_idx、deliveries_retry_idx、messages_expire_idx
```

关键点：

- **事务提交先于 ACK**：`SEND_ACK(ACCEPTED)` 只表示“消息与全部投递行已持久化”，不表示“已送达”。
- **推送不阻塞发送方**：`Deliverer` 只做编码与目标队列 `Send` 入队；目标队列满时该目标推送失败，已入库的投递仍可由重试或 PULL 恢复。
- **数据库调用在连接协程内同步执行**，每次调用受 `5s` 超时约束；不新增每连接常驻协程。
- **每条消息只有 3 次数据库往返**：受理（含全部投递行）、RECEIPT_ACK、BUSINESS_ACK 各是一条自包含语句（隐式事务），没有显式的 `BEGIN`/`COMMIT` 往返。压测表明在本机 Windows + Docker PostgreSQL 上，正是这些往返而非 SQL 本身决定了吞吐（详见[压测报告](压测报告.md)）。
- **重试/TTL 是单协程扫描**：不做分布式协调，适合单机单实例。

## 4. 目录结构

```text
phase2/
├── go.mod / go.sum               # module relayx/phase2, go 1.27, pgx v5.12.0
├── Makefile                      # build/test/race/vet/fmt/db-up/db-down/load
├── docker-compose.yml            # postgres:17-alpine，容器 relayx-phase2-pg
├── cmd/
│   ├── gateway/main.go           # 连库→迁移→transport→Worker；信号优雅退出；周期 stats
│   ├── client/main.go            # 演示客户端：LOGIN、SEND、PULL、两级 ACK、PONG
│   └── loadtest/main.go          # 压测：echo/login/route/reliable 四种模式
├── internal/
│   ├── protocol/
│   │   ├── frame.go              # 21 B 帧头、14 种帧类型、Encode/Decode
│   │   ├── route.go              # LOGIN/LOGIN_ACK/SEND/DELIVER body
│   │   ├── ack.go                # SEND_ACK/RECEIPT_ACK/BUSINESS_ACK body
│   │   └── pull.go               # PULL/PULL_DONE body
│   ├── transport/                # conn.go / server.go / pool.go（沿用 Phase 1）
│   ├── session/                  # Session 注册表（byID + byUser）
│   ├── gateway/                  # 登录、SEND 受理、ACK、PULL、连接关闭注销
│   ├── store/                    # PostgreSQL 连接池、迁移、消息/投递/端点查询
│   │   └── migrations/0001_init.sql
│   ├── delivery/                 # Deliverer（在线推送）+ Worker（重试/TTL）
│   └── config/                   # 服务端/客户端 flag 默认值与校验
└── docs/
    ├── README.md                 # 本文件
    ├── 数据模型.md                # DDL、索引、事务边界、幂等与游标
    ├── 压测报告.md                # 1000 连接可靠模式实测（含失败分析）
    ├── loadtest-reliable.txt     # 本轮 loadtest 原始输出
    └── adr/
        ├── ADR-007.md            # PostgreSQL / pgx / 事务边界 / ACCEPTED
        ├── ADR-008.md            # 两级 ACK / 幂等键 / 条件更新状态机
        └── ADR-009.md            # 离线 PULL / 游标 / TTL / 重试与背压
```

## 5. 协议

### 5.1 帧头（与 Phase 0 / 0.5 / 1 完全一致）

固定 21 字节，大端：

| 偏移 | 长度 | 字段 | 说明 |
| --- | --- | --- | --- |
| 0 | 2 | Magic | `0x5258`（"RX"） |
| 2 | 1 | Version | `0x01` |
| 3 | 1 | Type | 帧类型 |
| 4 | 1 | Flags | 必须为 0 |
| 5 | 8 | MsgID | SEND 中作为幂等键；DELIVER 中为平台 `message_id` |
| 13 | 4 | BodyLen | Body 字节数，解码前按 `max-body` 校验 |
| 17 | 4 | CRC32 | IEEE，仅覆盖 Body |

### 5.2 全部 14 种帧

| Type | 名称 | 方向 | Body 布局与语义 |
| --- | --- | --- | --- |
| `0x01` | `PING` | 双向 | 不透明 Body；transport 内部应答 `PONG`，不进 handler |
| `0x02` | `PONG` | 双向 | 不透明 Body；transport 内部处理 |
| `0x03` | `DATA` | 双向 | 不透明 Body；服务端原样回显（兼容 Phase 0/0.5） |
| `0x04` | `ERROR` | 服务端→客户端 | UTF-8 文本；解码/未登录等错误 |
| `0x05` | `CLOSE` | 双向 | UTF-8 原因；写出后断开 |
| `0x06` | `LOGIN` | 客户端→服务端 | `userLen(2) + user + deviceLen(2) + device` |
| `0x07` | `LOGIN_ACK` | 服务端→客户端 | `status(1) + sessionID(8) + msgLen(2) + msg` |
| `0x08` | `SEND` | 客户端→服务端 | `targetType(1) + targetLen(2) + target + payload`；header `MsgID` = 幂等键 |
| `0x09` | `DELIVER` | 服务端→客户端 | `fromSessionID(8) + fromUserLen(2) + fromUser + fromDeviceLen(2) + fromDevice + payload`；header `MsgID` = 平台 `message_id` |
| `0x0A` | `SEND_ACK` | 服务端→客户端 | `status(1) + messageID(8) + msgLen(2) + msg` |
| `0x0B` | `RECEIPT_ACK` | 客户端→服务端 | `messageID(8)` |
| `0x0C` | `BUSINESS_ACK` | 客户端→服务端 | `messageID(8) + status(1)` |
| `0x0D` | `PULL` | 客户端→服务端 | `cursor(8) + limit(2)`；`limit=0` 用服务端默认值 |
| `0x0E` | `PULL_DONE` | 服务端→客户端 | `count(2) + nextCursor(8) + hasMore(1)`；`hasMore=1` 表示还有下一页 |

约束：

- 身份/目标字符串长度 1..64 字节（`MaxIdentityLen=64`）。
- `LOGIN_ACK` / `SEND_ACK` 的文本上限 128 字节；`PULL_DONE.count` 是 `uint16`，单页最多 65535。
- `targetType`：`0x01` = user（fan-out 到该用户全部端点）、`0x02` = endpoint（十进制 session ID）。
- `DELIVER.fromSessionID` 当前恒为 0：重试可能发生在原发送连接消失之后，接收方应以 `fromUser/fromDevice` 识别来源。
- 帧 Body 上限由 `-max-body` 控制，硬上限 16 MiB。

### 5.3 状态码

`LOGIN_ACK.status`：

| 值 | 含义 | 触发条件 |
| --- | --- | --- |
| 0 | OK | 登录成功 |
| 1 | already logged in | 同一连接重复 `LOGIN` |
| 2 | invalid | Body 解码失败 |
| 3 | internal | `UpsertEndpoint` 失败（数据库不可用） |

`SEND_ACK.status`：

| 值 | 含义 | `messageID` |
| --- | --- | --- |
| 0 | ACCEPTED | 新消息的平台 ID |
| 1 | DUPLICATE | 原消息的平台 ID |
| 2 | INVALID | 0（目标不存在/未登录/target 非法） |
| 3 | INTERNAL | 0（数据库错误） |

`BUSINESS_ACK.status`：`0` 表示业务成功；非 0 由业务自定义，平台只记录并映射为 `BUSINESS_FAILED`。

## 6. 状态机

### 6.1 消息（`messages.state`，4 态）

```text
                    ┌───────────────┐
      受理事务提交 ─►│ 0 ACCEPTED    │
                    └──────┬────────┘
                           │ 任一投递进入终态（3/4/5/6）
                           ▼
                    ┌───────────────┐
                    │ 1 PARTIAL     │◄──┐
                    └──────┬────────┘   │ 仍有非终态投递
                           │ 全部投递终态 │
                           ▼              │
                    ┌───────────────┐   │
                    │ 2 COMPLETED   │   │
                    └───────────────┘   │
                                        │
   过期扫描（expire_at <= now 且 state ∈ {0,1}）──► 3 EXPIRED（终态，重算不覆盖）
```

| 值 | 名称 | 含义 |
| --- | --- | --- |
| 0 | `ACCEPTED` | 已持久化，尚无任何投递进入终态 |
| 1 | `PARTIAL` | 至少一个投递终态，但并非全部 |
| 2 | `COMPLETED` | 全部投递均为终态（3/4/5/6） |
| 3 | `EXPIRED` | 超过 TTL；重算时保持 3，不被覆盖 |

### 6.2 投递（`deliveries.state`，7 态）

```text
 0 PENDING ──实时推送/ PULL──► 1 SENT ──RECEIPT_ACK──► 2 RECEIPT_ACKED
     │                         │  │                         │
     │                         │  └──BUSINESS_ACK(0)───────►3 BUSINESS_ACKED
     │                         │  └──BUSINESS_ACK(!=0)─────►4 BUSINESS_FAILED
     │                         │
     │                         ├──重试（attempts+1，仍为 1）
     │                         └──超过 5 次──► 6 FAILED
     └──消息 TTL 到期──► 5 EXPIRED
```

| 值 | 名称 | 是否终态 | 说明 |
| --- | --- | --- | --- |
| 0 | `PENDING` | 否 | 已入库，尚未推送 |
| 1 | `SENT` | 否 | 已入队/已 PULL 发出，等待确认 |
| 2 | `RECEIPT_ACKED` | 否 | 客户端确认收到，等待业务结果 |
| 3 | `BUSINESS_ACKED` | 是 | 业务成功 |
| 4 | `BUSINESS_FAILED` | 是 | 业务失败（`status != 0`） |
| 5 | `EXPIRED` | 是 | 消息 TTL 到期 |
| 6 | `FAILED` | 是 | 推送次数达到上限（默认 5） |

终态定义：`state IN (3,4,5,6)`。重复 `RECEIPT_ACK`/`BUSINESS_ACK` 是幂等空操作；条件更新不满足时事务只提交一个 no-op。

## 7. 配置项

### 7.1 服务端（`cmd/gateway`，`internal/config.ParseServer`）

| Flag | 默认值 | 含义与约束 |
| --- | --- | --- |
| `-addr` | `127.0.0.1:9000` | 监听地址 |
| `-max-body` | `1048576` | 单帧 Body 上限，`>0` 且 `<=16 MiB` |
| `-heartbeat-interval` | `30s` | PING 间隔 |
| `-heartbeat-timeout` | `90s` | 心跳超时 |
| `-idle-timeout` | `120s` | 读空闲超时 |
| `-send-queue-size` | `256` | 每连接队列帧数，满则关闭连接 |
| `-send-queue-bytes` | `4194304` | 每连接队列字节预算 |
| `-write-timeout` | `10s` | 单次 socket 写超时 |
| `-write-batch-frames` | `16` | 批量写最大帧数 |
| `-write-batch-bytes` | `65536` | 批量写目标字节数 |
| `-read-buffer-size` | `4096` | 读缓冲大小 |
| `-shutdown-timeout` | `5s` | 优雅关闭上限 |
| `-stats-interval` | `10s` | stats 日志间隔；`<=0` 关闭 |
| `-dsn` | `postgres://relayx:relayx@127.0.0.1:5432/relayx?sslmode=disable` | PostgreSQL DSN |
| `-db-max-open-conns` | `16` | 连接池上限 |
| `-db-max-idle-conns` | `8` | 空闲连接上限 |
| `-db-connect-timeout` | `5s` | 启动连接与迁移超时 |
| `-message-ttl` | `24h` | 消息有效期，决定 `expire_at` |
| `-retry-interval` | `1s` | 重试/TTL 扫描间隔 |
| `-retry-max-attempts` | `5` | 单投递最大推送次数，`>=1` |
| `-retry-backoff-base` | `2s` | 指数退避基数 |
| `-retry-backoff-max` | `1m` | 退避上限，不得小于基数 |
| `-retry-batch` | `100` | 每轮最多处理的到期重试 |
| `-expire-batch` | `100` | 每轮最多处理的过期消息 |
| `-pull-limit` | `100` | `PULL.limit=0` 时的默认页大小 |
| `-pull-max-limit` | `500` | 客户端可请求的最大页大小，`<=65535` |

`gateway stats` 的 11 个字段：`goroutines`、`heap_alloc_mb`、`heap_sys_mb`、`active_conns`、`active_sessions`、`messages_accepted`、`messages_duplicate`、`deliveries_pending`、`deliveries_retried`、`deliveries_expired`、`db_open_conns`。`deliveries_pending=-1` 表示本次统计查询在 1s 内超时（哨兵值，不是真实负数）。

### 7.2 演示客户端（`cmd/client`）

| Flag | 默认值 | 含义 |
| --- | --- | --- |
| `-addr` | `127.0.0.1:9000` | 服务端地址 |
| `-name` | `demo` | 客户端名称 |
| `-user` | `""` | 登录 userID；为空则不发送 LOGIN |
| `-device` | `""` | 登录 deviceID；为空回退为 `-name` |
| `-to` | `""` | 目标：userID 或 `endpoint:<sessionID>`；为空则发送 DATA 回显 |
| `-interval` | `1s` | 发送间隔 |
| `-count` | `0` | 发送条数，`0` 不限 |
| `-payload-size` | `64` | payload 字节数 |
| `-max-body` | `1048576` | 接收 Body 上限 |
| `-idle-timeout` | `120s` | 读 Deadline |
| `-pull` | `true` | 登录后自动 PULL，并按 `hasMore` 连续翻页 |
| `-ack` | `both` | `receipt` / `business` / `both` / `none` |

### 7.3 压测工具（`cmd/loadtest`）

| Flag | 默认值 | 含义 |
| --- | --- | --- |
| `-addr` | `127.0.0.1:9000` | gateway 地址 |
| `-conns` | `1000` | 并发连接数 |
| `-duration` | `60s` | 压测时长 |
| `-interval` | `1s` | 每连接发送间隔 |
| `-payload-size` | `64` | payload 字节数 |
| `-max-body` | `1048576` | 接收 Body 上限 |
| `-ramp` | `5s` | 建连爬坡时间 |
| `-timeout` | `5s` | 拨号与单条消息等待超时 |
| `-out` | `""` | 原始报告输出路径 |
| `-login` | `false` | 先 LOGIN 再发 DATA |
| `-route` | `false` | Phase 1 的 SEND→DELIVER 路由模式 |
| `-route-to` | `""` | 路由目标 userID；空表示自路由 |
| `-reliable` | `false` | 可靠模式：SEND → SEND_ACK → DELIVER → 两级 ACK |
| `-ack` | `both` | 可靠模式的 ACK 类型：`receipt` / `business` / `both` / `none` |

压测身份固定为 `user=load-<连接序号>`、`device=d0`；可靠模式默认自路由。

## 8. 快速开始

前置：Go 1.27+、Docker Desktop。以下命令在 `phase2/` 下执行（Windows PowerShell）。

```powershell
# 1. 启动本机 PostgreSQL（postgres:17-alpine，容器 relayx-phase2-pg）
#    docker-compose.yml 以 command 追加 -c synchronous_commit=off -c max_connections=100
make db-up

# 2. 构建三个二进制
New-Item -ItemType Directory -Force bin | Out-Null
go build -o bin/gateway.exe ./cmd/gateway
go build -o bin/client.exe ./cmd/client
go build -o bin/loadtest.exe ./cmd/loadtest

# 3. 启动 gateway（启动时自动连接并迁移 schema）
.\bin\gateway.exe -addr 127.0.0.1:9000 -stats-interval 5s
```

```powershell
# 4a. bob 先登录一次，注册 (bob, pc) 端点；随后进程退出
.\bin\client.exe -addr 127.0.0.1:9000 -name bob-pc -user bob -device pc -count 1

# 4b. alice 给离线的 bob 发 3 条消息（bob 不在线，投递保持 PENDING）
.\bin\client.exe -addr 127.0.0.1:9000 -name alice-phone -user alice -device phone -to bob -count 3

# 5. bob 重新上线；-pull 自动拉取离线消息，-ack both 回两级 ACK
.\bin\client.exe -addr 127.0.0.1:9000 -name bob-pc -user bob -device pc -pull -ack both -interval 1h
```

注意：目标用户必须至少登录过一次，`endpoints` 中才有可解析的端点；否则 `SEND` 会得到 `SEND_ACK(INVALID, "target has no endpoint")`。这是“离线”与“从未注册”的区别。

### 8.1 开发环境的持久化取舍（`synchronous_commit=off`）

`docker-compose.yml` 为 PostgreSQL 显式设置了 `synchronous_commit=off` 与 `max_connections=100`。这是**仅限开发/压测环境**的取舍：

- 关闭同步提交后，事务提交不再等待 WAL fsync，而是由后台 WAL 写入完成。本机 Docker（Windows + WSL2 虚拟磁盘）上，`synchronous_commit=on` 时每次提交约 1.6–1.7 ms。仅关闭同步提交**不足以**让 1000 连接通过（实测 `ack_missed` 仍约 19100），还必须把每条消息的数据库往返从 12 次降到 3 次；两者都做到后，同一台机器可以稳定通过（详见[压测报告](压测报告.md)第 7 节）。
- 语义影响：**PostgreSQL 进程崩溃不丢已提交事务**（WAL 已写入操作系统）；**操作系统崩溃或断电可能丢失最近若干毫秒到数百毫秒的已提交事务**。对 At-Least-Once 演示可接受，对真实资金/订单场景不可接受。
- **生产环境必须使用 `synchronous_commit=on`**，并通过批量提交、组提交、更高规格存储或分库分表来获得吞吐；本仓库**不**声称代码在 `on` 模式下能达到同样的 1000 msg/s。
- 该设置只影响持久化等待，不改变任何应用层语义：受理成功后 ACK 的规则、ACK 幂等、重试/TTL 全部不变。

预期：

- gateway 打印 `gateway listening on 127.0.0.1:9000`，stderr 每 5s 一条 `gateway stats`。
- alice 收到 `SEND_ACK`（`ACCEPTED` 或 `DUPLICATE`）。
- bob 重新登录后收到 `DELIVER`，客户端立即发送 `RECEIPT_ACK` 与 `BUSINESS_ACK`；数据库状态从 `PENDING` 依次变为 `SENT` → `RECEIPT_ACKED` → `BUSINESS_ACKED`，消息聚合为 `COMPLETED`。
- 同一 `(user, device)` 再次登录会替换旧连接（旧连接收到 `CLOSE`）。

## 9. 质量门禁

在 `phase2/` 下执行。2026-11-11 实测结果：

```powershell
gofmt -l .
# 无输出

go vet ./...
# 退出码 0，无告警

$env:RELAYX_TEST_DSN='postgres://relayx:relayx@127.0.0.1:5432/relayx?sslmode=disable'
go test ./... -count=1
# ?   	relayx/phase2/cmd/client	[no test files]
# ?   	relayx/phase2/cmd/gateway	[no test files]
# ?   	relayx/phase2/cmd/loadtest	[no test files]
# ok  	relayx/phase2/internal/config	2.275s
# ok  	relayx/phase2/internal/delivery	3.798s
# ok  	relayx/phase2/internal/gateway	8.530s
# ok  	relayx/phase2/internal/protocol	1.988s
# ok  	relayx/phase2/internal/session	1.766s
# ok  	relayx/phase2/internal/store	5.907s
# ok  	relayx/phase2/internal/transport	3.348s

go test ./... -count=3          # 同上 DSN，重复三次，全部 ok（gateway 17.721s / store 10.649s）

go test ./... -count=1          # 不设 DSN，数据库用例 skip
# config 1.780s / delivery 1.901s / gateway 2.292s / protocol 1.378s
# session 1.220s / store 2.021s / transport 3.732s，退出码 0

docker run --rm -v "${PWD}:/src" -w /src `
  -e RELAYX_TEST_DSN="postgres://relayx:relayx@host.docker.internal:5432/relayx?sslmode=disable" `
  -e GOPROXY="https://goproxy.cn,direct" `
  golang:1.27 go test ./... -race -count=1
# ok  relayx/phase2/internal/config     1.021s
# ok  relayx/phase2/internal/delivery   2.310s
# ok  relayx/phase2/internal/gateway    9.215s
# ok  relayx/phase2/internal/protocol   1.058s
# ok  relayx/phase2/internal/session    1.025s
# ok  relayx/phase2/internal/store      6.017s
# ok  relayx/phase2/internal/transport  2.622s
# 退出码 0，未发现 data race
```

注意：`phase2/docs/` 下的耗时是实际执行的输出，会随机器负载波动；数值变化不代表失败，以退出码为准。

1000 连接可靠模式压测（SubTask 8.3）**已通过**：`loadtest` 退出码 0、`sent=send_ack=delivered=56994`、`ack_missed=0`、`accept_rtt p99=467.273 ms`。优化前的失败轮（`ack_missed=32005`）与两轮对比、实验数据、复现命令见[压测报告](压测报告.md)。

## 10. 文档索引

- [数据模型：表、索引、事务边界、幂等与游标](数据模型.md)
- [千连接可靠模式压测报告（含优化前后对比、归因实验与已知限制）](压测报告.md)
- [最终通过轮的 loadtest 原始输出](loadtest-reliable.txt)
- [优化前失败轮的 loadtest 原始输出（`synchronous_commit=on`）](loadtest-reliable-sync-on.txt)
- [ADR-007：PostgreSQL、pgx、事务边界与 ACCEPTED 语义](adr/ADR-007.md)
- [ADR-008：两级 ACK、幂等键与条件更新状态机](adr/ADR-008.md)
- [ADR-009：离线 PULL、游标、TTL、重试与背压](adr/ADR-009.md)
- 前序阶段：[Phase 1 README](../../phase1/docs/README.md)
