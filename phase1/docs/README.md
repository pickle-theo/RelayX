# RelayX Phase 1：单机实时消息 Gateway

- 状态：已完成（2026-11-10）
- 模块：`relayx/phase1`（`go 1.27`，零第三方依赖）
- 定位：在 Phase 0.5 的传输基线之上建立 **Connection → Session → Router** 三层，支持登录与在线消息投递
- 前序基线：[Phase 0.5 文档](../../phase0.5/docs/README.md)（传输层优化，线格式与本阶段一致）
- 数据：[千连接压测报告](压测报告.md)

## 1. 项目定位

Phase 1 仍是**单机** Gateway。它在 Phase 0.5 的 TCP 长连接、21 字节定长帧头、CRC32、心跳、批量写与有界发送队列之上，新增：

- **登录**：客户端用 `LOGIN` 声明 `(userID, deviceID)`，服务端返回 `LOGIN_ACK` 与进程内唯一的 `sessionID`。
- **消息路由**：`SEND` 可以按用户（fan-out 到该用户全部在线设备）或按端点（十进制 `sessionID`）定向投递，接收方收到 `DELIVER`。
- **Session 注册表**：按 `(user, device)` 唯一，同一对身份重复登录时新连接生效、旧连接收到 `CLOSE` 后被踢下线。

明确**不包含**（属于后续阶段）：

- 鉴权与身份校验（任意客户端可声明任意 `userID` / `deviceID`）
- 离线消息与可靠投递（目标不在线时只回 `ERROR`）
- 消息持久化、历史查询
- 跨节点路由 / 分布式 Gateway / 服务发现
- TLS、Metrics 端点

设计取舍见 [ADR-005](adr/ADR-005.md)（Session 与 Connection 分离）与 [ADR-006](adr/ADR-006.md)（路由模型与失败处理）。

## 2. 架构

```text
                        ┌──────────── 客户端进程 ────────────┐
                        │  cmd/client        cmd/loadtest     │
                        └─────────────────┬──────────────────┘
                                          │ TCP：21 B 大端帧头 + CRC32
                                          ▼
 internal/transport.Server：Accept → 每连接固定 2 个协程
   ┌─────────────────────────────────────────────────────────────┐
   │  readLoop（调用协程）                  writeLoop（唯一写 socket）│
   │    DecodeInto / 回 PONG                  ▲  control：ctrlCh      │
   │    空闲/心跳超时判定                      │  data：sendCh         │
   │        │                                 │  drainData 批量拼接    │
   │        ▼                                 │                       │
   │  gateway.Handler.Handle ──── Conn.Send ──┘                       │
   └────────┬────────────────────────────────────────────────────────┘
            │ LOGIN / SEND / DATA
            ▼
 internal/gateway.Handler
   ├── LOGIN → session.Manager.Register → LOGIN_ACK(status, sessionID)
   ├── SEND  → router.Router（未登录直接回 ERROR "not logged in"）
   └── DATA  → 原样回显（Phase 0/0.5 兼容）
            │
            ▼
 internal/router.Router
   ├── DeliverToUser     按 userID 查全部在线设备，逐个投递（fan-out）
   └── DeliverToEndpoint 按十进制 sessionID 精确定向
            │ session.Manager.FindByUser / Get（byID + byUser 双索引，RWMutex）
            ▼
 internal/session.Manager ── 目标 *Session.Conn.Send(DELIVER)
            │
            ▼
 目标连接的 sendCh → 目标连接的 writeLoop → 目标 socket
```

要点：

- **每连接仍只有 2 个协程**：`readLoop`（解码、PONG、超时）+ `writeLoop`（唯一写者，兼管心跳 Timer）。路由不新增常驻协程。
- **投递是非阻塞入队**：`Conn.Send` 编码后写入目标连接的有界队列；队列满（帧数或字节预算）时关闭**目标**连接并返回错误，不阻塞发送方的 `readLoop`。
- **业务处理在 readLoop 上同步执行**：`Handler.Handle` 只做解码、注册表查找与 `Send` 入队，不等待对端写出。

## 3. 目录结构

```text
phase1/
├── go.mod                        # module relayx/phase1, go 1.27
├── Makefile                      # build/test/race/vet/fmt/bench/profile（make 为可选，Windows 可直接用 go）
├── bin/                          # 本地构建产物（临时，不提交）
├── cmd/
│   ├── gateway/main.go           # 组装 session.Manager + gateway.Handler + transport.Server；信号优雅退出；周期 stats
│   ├── client/main.go            # 演示客户端：可选 LOGIN、定时 DATA 回显或 SEND 定向、自动回 PONG
│   └── loadtest/main.go          # 压测：ramp 建连、echo/login/route 三种模式、RTT 分位、错误分类、可选报告文件
├── internal/
│   ├── protocol/frame.go         # 帧头、帧类型、AppendFrame/Encode、DecodeInto/Decode、哨兵错误
│   ├── protocol/route.go         # LOGIN/LOGIN_ACK/SEND/DELIVER body 编解码与长度约束
│   ├── transport/conn.go         # Conn：readLoop + writeLoop、Send、ctrlCh 优先、批量写、CloseWithFrame
│   ├── transport/pool.go         # 编码缓冲分级池、Frame 池与所有权约定
│   ├── transport/server.go       # Server：Accept、连接注册表、Shutdown、ActiveConnections
│   ├── session/session.go        # Session、Manager（byID/byUser）、NextSessionID、CloseReplaced
│   ├── router/router.go          # DeliverToUser / DeliverToEndpoint 与路由错误
│   ├── gateway/gateway.go        # 登录握手、SEND 分发、DATA 回显、连接关闭时注销 Session
│   └── config/config.go          # 服务端/客户端 flag 解析、默认值与校验
└── docs/
    ├── README.md                 # 本文件
    ├── 压测报告.md                # 千连接回显 / 登录+路由压测
    ├── loadtest-echo.txt         # A 组（回显）loadtest 原始报告
    ├── loadtest-route.txt        # B 组（登录+路由）loadtest 原始报告
    └── adr/
        ├── ADR-005.md            # Session 与 Connection 分离
        └── ADR-006.md            # 路由模型与失败处理
```

## 4. 协议

### 4.1 帧头（与 Phase 0 / 0.5 完全一致）

固定 21 字节，大端：

| 偏移 | 长度 | 字段 | 说明 |
| --- | --- | --- | --- |
| 0 | 2 | Magic | `0x5258`（"RX"） |
| 2 | 1 | Version | `0x01` |
| 3 | 1 | Type | 帧类型 |
| 4 | 1 | Flags | 必须为 0 |
| 5 | 8 | MsgID | 由发送方分配，回包携带同一 MsgID |
| 13 | 4 | BodyLen | Body 字节数，解码前先按 `max-body` 校验 |
| 17 | 4 | CRC32 | IEEE 多项式，仅覆盖 Body |

帧头之后紧跟 `BodyLen` 字节 Body。任何字段不合法都以哨兵错误拒绝，不解码、不分发。

### 4.2 帧类型与扩展

| Type | 名称 | 方向 | Phase 1 语义 |
| --- | --- | --- | --- |
| `0x01` | `PING` | 双向 | 心跳；transport 内部处理，不进业务 handler |
| `0x02` | `PONG` | 双向 | 心跳应答；transport 内部处理 |
| `0x03` | `DATA` | 客户端→服务端 | **仍原样回显**（Phase 0/0.5 兼容） |
| `0x04` | `ERROR` | 服务端→客户端 | 业务/路由失败原因，UTF-8 文本 Body |
| `0x05` | `CLOSE` | 双向 | 关闭通知；写出后断开 |
| `0x06` | `LOGIN` | 客户端→服务端 | **新增**：声明身份 |
| `0x07` | `LOGIN_ACK` | 服务端→客户端 | **新增**：登录结果 |
| `0x08` | `SEND` | 客户端→服务端 | **新增**：定向/按用户发送 |
| `0x09` | `DELIVER` | 服务端→客户端 | **新增**：投递消息并携带来源身份 |

### 4.3 Body 布局（大端，长度前缀为 `uint16`）

| 帧 | 布局 |
| --- | --- |
| `LOGIN` | `userLen(2B) + userID + deviceLen(2B) + deviceID` |
| `LOGIN_ACK` | `status(1B) + sessionID(8B) + msgLen(2B) + msg` |
| `SEND` | `targetType(1B) + targetLen(2B) + target + payload` |
| `DELIVER` | `fromSessionID(8B) + fromUserLen(2B) + fromUser + fromDeviceLen(2B) + fromDevice + payload` |

约束：

- 身份与目标字符串长度均为 **1..64** 字节（`protocol.MaxIdentityLen = 64`）。
- `LOGIN_ACK.msg` 上限 **128** 字节（`protocol.MaxLoginMessageLen = 128`）。
- `targetType`：`0x01` = user（按用户 fan-out）、`0x02` = endpoint（十进制 session ID 字符串）。
- 帧 Body 上限由 `-max-body` 控制，硬上限 16 MiB。

### 4.4 LOGIN_ACK 状态码

| status | 含义 | 触发条件 |
| --- | --- | --- |
| `0` | OK | 登录成功 |
| `1` | already logged in | 同一连接重复发送 `LOGIN` |
| `2` | invalid identity | Body 解码失败（长度越界、截断等） |
| `3` | internal | 保留；当前实现未返回 |

常量定义在 `internal/gateway/gateway.go`（`LoginStatusAlreadyLoggedIn` / `LoginStatusInvalid` / `LoginStatusInternal`）。

### 4.5 路由失败语义

| 场景 | 发送方收到 |
| --- | --- |
| 未登录就发 `SEND` | `ERROR`，Body 为 `not logged in` |
| 目标用户无在线设备 | `ERROR`，Body 含 `route: target offline` |
| 目标 endpoint 不存在 / 已关闭 / 非十进制 | `ERROR`，Body 含 `route: target not found` |
| `targetType` 非法 | `ERROR`，Body 为 `protocol: invalid route target: <n>`（由 `SEND` 解码返回） |
| `SEND` Body 解码失败 | `ERROR`，Body 为具体解码错误（如 `protocol: truncated body: ...`） |

所有路由失败都**只回 ERROR，不断开连接**。目标连接自身发送队列打满时，transport 关闭的是**目标连接**，发送方仍只看到 `route: target offline`（全部投递失败）。

### 4.6 兼容性

- 帧头布局、Magic、Version、Flags 校验与 Phase 0.5 完全一致，Phase 0.5 客户端可继续连接。
- 旧帧类型 `PING/PONG/DATA/ERROR/CLOSE` 的编号与语义不变；未登录连接发送 `DATA` 仍会收到回显。
- 新帧类型只做**增量扩展**，旧客户端不会收到无法解析的帧，除非主动发送 `SEND` 或 `LOGIN`。

## 5. 配置项

### 5.1 服务端（`cmd/gateway`，`internal/config.ParseServer`）

| Flag | 默认值 | 含义与约束 |
| --- | --- | --- |
| `-addr` | `127.0.0.1:9000` | 监听地址；端口写 `0` 由系统分配 |
| `-max-body` | `1048576`（1 MiB） | 单帧 Body 上限；`>0` 且 `<= 16777216`（16 MiB 硬上限） |
| `-heartbeat-interval` | `30s` | PING 发送间隔；`>0` |
| `-heartbeat-timeout` | `90s` | 距最近入站帧超过该值即判心跳超时；`>0` |
| `-idle-timeout` | `120s` | 读空闲超时；`>0` |
| `-send-queue-size` | `256` | 每连接数据帧队列容量（帧数）；`>0`，满则关闭连接 |
| `-send-queue-bytes` | `4194304`（4 MiB） | 每连接发送队列字节预算（含控制帧）；`>0`，超限关闭连接 |
| `-write-timeout` | `10s` | 单次 `Write` 的 Deadline；`>0` |
| `-write-batch-frames` | `16` | 一次 `Write` 最多合并的数据帧数；`>0` |
| `-write-batch-bytes` | `65536`（64 KiB） | 一次 `Write` 的目标字节数；`>0` |
| `-read-buffer-size` | `4096` | `bufio.NewReaderSize` 读缓冲大小；`>0` |
| `-shutdown-timeout` | `5s` | 优雅关闭等待上限；`>0` |
| `-stats-interval` | `10s` | `gateway stats` 日志间隔；`<=0` 关闭统计协程（不参与校验） |

`stats` 日志字段：`goroutines`、`heap_alloc_mb`、`heap_sys_mb`、`active_conns`、`active_sessions`。其中 `active_sessions` 是 Phase 1 新增观测项。

### 5.2 客户端（`cmd/client`，`internal/config.ParseClient`）

| Flag | 默认值 | 含义与约束 |
| --- | --- | --- |
| `-addr` | `127.0.0.1:9000` | 服务端地址 |
| `-name` | `demo` | 客户端名称，仅用于日志与默认设备名 |
| `-user` | `""` | 登录 userID；为空则**不发送 LOGIN**（只做 DATA 回显） |
| `-device` | `""` | 登录 deviceID；为空时回退为 `-name` |
| `-to` | `""` | 发送目标：userID 或 `endpoint:<sessionID>`；为空则发送 `DATA` 回显 |
| `-interval` | `1s` | 发送间隔；`>0` |
| `-count` | `0` | 发送条数，`0` 表示不限；不得为负 |
| `-payload-size` | `64` | payload 字节数；`>=0` 且 `<= -max-body` |
| `-max-body` | `1048576` | 接收帧 Body 上限；`>0` 且 `<= 16 MiB` |
| `-idle-timeout` | `120s` | 客户端读 Deadline；`>0` |

### 5.3 压测工具（`cmd/loadtest`）

| Flag | 默认值 | 含义与约束 |
| --- | --- | --- |
| `-addr` | `127.0.0.1:9000` | gateway 地址 |
| `-conns` | `1000` | 并发连接数；`>0` |
| `-duration` | `60s` | 压测时长；`>0` |
| `-interval` | `1s` | 每连接发送间隔；`>0` |
| `-payload-size` | `64` | 固定 payload 字节数；`>=0` 且 `<= -max-body` |
| `-max-body` | `1048576` | 接收帧 Body 上限；`>0` |
| `-ramp` | `5s` | 建连爬坡时间；`>=0` |
| `-timeout` | `5s` | 单连接拨号与单条消息等待超时；`>0` |
| `-out` | `""` | 额外把报告写入该文件 |
| `-login` | `false` | 每条连接先 LOGIN 并等待 `LOGIN_ACK`，随后发送 DATA 回显 |
| `-route` | `false` | 发送 `SEND` 而非 `DATA`；隐含 `-login` |
| `-route-to` | `""` | 路由目标 userID；为空表示自路由（`load-<id>` 发给自己），非空隐含 `-route` |

压测连接的身份固定为 `user=load-<连接序号>`、`device=d0`。

## 6. 快速开始

在 `phase1/` 目录执行（本机可不用 `make`）：

```powershell
# 构建
New-Item -ItemType Directory -Force bin | Out-Null
go build -o bin/gateway.exe ./cmd/gateway
go build -o bin/client.exe ./cmd/client
go build -o bin/loadtest.exe ./cmd/loadtest

# 终端 1：启动 gateway（slog 写 stderr）
.\bin\gateway.exe -addr 127.0.0.1:9000 -stats-interval 5s

# 终端 2：alice/phone 登录，每秒给 bob 发一条 SEND
.\bin\client.exe -addr 127.0.0.1:9000 -name alice-phone -user alice -device phone -to bob

# 终端 3：bob/pc 登录，观察 DELIVER；同时每秒给 alice 发一条
.\bin\client.exe -addr 127.0.0.1:9000 -name bob-pc -user bob -device pc -to alice
```

预期：

- gateway stdout 打印 `gateway listening on 127.0.0.1:9000`；stderr 每 5s 一条 `gateway stats`（含 `active_conns`、`active_sessions`）。
- 客户端打印 `logged in as alice/phone session=<id>`；收到投递时打印 `deliver from=alice/phone msg_id=<n> body=...`。
- 用 `endpoint:<sessionID>` 可定向到具体会话；同一 `(user, device)` 再次登录会把旧连接踢下线（旧连接先收到 `CLOSE`）。

三种压测模式：

```powershell
# 1) 回显基线（Phase 0.5 兼容路径）
.\bin\loadtest.exe -addr 127.0.0.1:9000 -conns 1000 -duration 60s -interval 1s -ramp 5s -out echo-result.txt

# 2) 登录 + 回显
.\bin\loadtest.exe -addr 127.0.0.1:9000 -conns 1000 -duration 60s -interval 1s -ramp 5s -login -out login-result.txt

# 3) 登录 + 路由（默认自路由；-route-to bob 可跨用户）
.\bin\loadtest.exe -addr 127.0.0.1:9000 -conns 1000 -duration 60s -interval 1s -ramp 5s -route -out route-result.txt
```

报告含 `[connections]`、`[messages]`、`[errors]`、`[rtt ms]`、`[client process]` 五段。存在连接失败、登录失败、路由错误或 `sent > responded` 时退出码为 1。

## 7. 与 Phase 0.5 的差异

| 维度 | Phase 0.5 | Phase 1 |
| --- | --- | --- |
| 业务能力 | DATA 回显、PING/PONG | 登录、按用户 fan-out、按 endpoint 定向、DELIVER、DATA 回显 |
| 新增帧 | — | `LOGIN(0x06)`、`LOGIN_ACK(0x07)`、`SEND(0x08)`、`DELIVER(0x09)` |
| 新增包 | — | `internal/session`、`internal/router`、`internal/gateway` |
| 状态 | 无身份状态 | `Session` 注册表（`byID` + `byUser` 双索引），连接关闭即注销 |
| 重复登录 | — | 同一 `(user, device)` 新连接生效，旧连接收到 `CLOSE` 后断开 |
| 未登录发送 | 不适用 | `SEND` 返回 `ERROR "not logged in"`；`DATA` 仍回显 |
| 广播语义 | 无 | 按用户 fan-out 到其全部在线设备（用户→多设备） |
| 失败处理 | 协议/传输错误断连 | 路由失败只回 `ERROR`，连接保持 |
| 每连接服务端协程 | 2（读 + 写） | **仍为 2**（路由/map 查找在 readLoop 内同步完成，无新增常驻协程） |
| 观测 | `active_conns` | 额外 `active_sessions` |
| 不变量 | 21 B 帧头 + CRC32、`send-queue` 双预算、批量写、PONG 优先、幂等 Close | 全部保留 |

## 8. 质量门禁

在 `phase1/` 目录执行：

```powershell
gofmt -l .                 # 必须无输出
go vet ./...
go test ./... -count=1

# race 需要本机 Docker（Windows 本机无 C 编译器）
docker run --rm -v "C:/Users/Administrator/Documents/trae_projects/RelayX/phase1:/app" -w /app golang:1.27 go test -race -count=1 ./...
```

2026-11-10 实测（完整输出见[压测报告第 6 节](压测报告.md#6-质量门禁)）：

```text
gofmt -l .         # 无输出
go vet ./...       # 退出码 0，无告警
go test ./... -count=1
ok  	relayx/phase1/internal/config	1.455s
ok  	relayx/phase1/internal/gateway	2.232s
ok  	relayx/phase1/internal/protocol	1.161s
ok  	relayx/phase1/internal/router	1.355s
ok  	relayx/phase1/internal/session	1.017s
ok  	relayx/phase1/internal/transport	3.206s

docker run ... go test -race -count=1 ./...   # 退出码 0，全部 ok，未发现 data race
```

## 9. 文档索引

- [千连接压测报告（回显 vs 登录+路由，含与 Phase 0.5 对比）](压测报告.md)
- [A 组回显 loadtest 原始报告](loadtest-echo.txt)
- [B 组登录+路由 loadtest 原始报告](loadtest-route.txt)
- [ADR-005：Session 与 Connection 分离](adr/ADR-005.md)
- [ADR-006：路由模型与失败处理](adr/ADR-006.md)
- 前序阶段：[Phase 0.5 README](../../phase0.5/docs/README.md)
