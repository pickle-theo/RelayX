# RelayX Phase 0：Go 网络编程基础

- 状态：已完成（2026-11-10）
- 规划依据：[通用实时消息 Gateway Platform](../../docs/通用实时消息%20Gateway%20Platform.md) 第 5.1–5.5 节
- 压测数据：[Phase 0 千连接压测报告](压测报告.md)

## 1. 阶段目标与范围

目标是只用 Go 标准库，打通「TCP 长连接 + 自定义二进制协议 + 心跳 + 连接生命周期」这条最小链路，为 Phase 1 的 Session 与消息路由提供可复用的传输层。`go.mod` 只有 `module relayx/phase0` 与 `go 1.27`，无任何第三方依赖。

本阶段包含：

- TCP Server / Client / 长连接。
- 21 字节定长帧头、Body 长度编码、CRC32 校验，天然处理粘包与半包。
- 每连接读协程、写协程、心跳协程；心跳超时、空闲超时、断线检测。
- 有界发送队列 + 队列满关闭连接的背压策略。
- 服务端 `Shutdown(ctx)` 优雅退出与 Ctrl+C 信号处理。
- 千连接压测工具与压测报告。

本阶段明确不做（属于后续阶段）：

- Session 管理、客户端身份绑定、指定客户端消息转发（Phase 1）。
- 消息路由、ACK/重试、离线消息、可靠投递（Phase 1/2）。
- PostgreSQL / Redis / Kafka、分布式 Gateway、TLS、鉴权、Metrics 端点。

> 说明：规划文档第九章「第一批任务」共 15 项，其中第 8–10 项（Session 管理、客户端身份绑定、指定客户端消息转发）属于 Phase 1 范围，Phase 0 未实现；Phase 0 落地的是第 1–7、11–15 项（含单连接单 Writer、有界发送队列、单测/集成测试与首轮压测）。

## 2. 目录结构

```text
RelayX/
├── go.work                        # Go 工作区，聚合各阶段独立 module
├── Makefile                       # 各阶段统一入口（make test / make race）
├── docs/
│   └── 通用实时消息 Gateway Platform.md   # 总体规划
└── phase0/                        # Phase 0 独立子项目
    ├── go.mod                     # module relayx/phase0, go 1.27，无第三方依赖
    ├── Makefile                   # 本阶段 build/test/race/压测入口
    ├── cmd/
    │   ├── gateway/main.go        # 服务端：DATA 回显、PING→PONG、信号优雅退出、周期统计日志
    │   ├── client/main.go         # 演示客户端：定时发 DATA、自动回 PONG、打印回显
    │   └── loadtest/main.go       # 压测工具：ramp 建连、RTT 分位、错误分类、可选报告文件
    ├── internal/
    │   ├── protocol/frame.go      # Frame、常量、21 字节头编解码、哨兵错误
    │   ├── transport/conn.go      # Conn：读写心跳三协程、Send、幂等 Close
    │   ├── transport/server.go    # Server：Accept、连接注册表、Shutdown
    │   └── config/config.go       # 服务端/客户端 flag 解析与默认值校验
    └── docs/
        ├── README.md              # 本文件
        ├── 压测报告.md
        └── adr/ADR-001.md、ADR-002.md
```

Phase 0 是独立 Go module（`relayx/phase0`），仓库根目录的 `go.work` 将其纳入工作区；后续阶段以 `phase0.5/`、`phase1/` 等并列目录独立演进，互不依赖。根目录 `Makefile` 负责统一调度各阶段。

## 3. 快速开始

在 `phase0` 目录执行（三个终端）：

```powershell
# 1) 启动服务端
go run ./cmd/gateway -addr 127.0.0.1:9000

# 2) 启动演示客户端：每秒 1 条 DATA，收满 5 条回显后退出
go run ./cmd/client -addr 127.0.0.1:9000 -name demo -count 5

# 3) 千连接压测
go run ./cmd/loadtest -addr 127.0.0.1:9000 -conns 1000 -duration 60s
```

预期行为：

- 服务端 stdout 打印 `gateway listening on 127.0.0.1:9000`，运行日志经 `slog.Default()` 输出到 stderr；每 `-stats-interval`（默认 10s）输出一条 `gateway stats`，字段为 `goroutines`、`heap_alloc_mb`、`heap_sys_mb`、`active_conns`。
- 客户端打印 `connected to 127.0.0.1:9000 as demo`，随后每秒发送一条 Body 形如 `demo#1 abc...` 的 DATA，并打印服务端回显 `echo msg_id=1 body=demo#1 abc...`。`-count 5` 在收到 5 条回显后退出；若 10s 内未收齐则打印告警后正常退出；Ctrl+C 打印 `bye` 退出。
- 压测工具输出 `[connections]`、`[messages]`、`[errors]`、`[rtt ms]`、`[client process]` 小节；存在连接失败或 `sent > echoed` 时退出码为 1，否则为 0。`-out report.txt` 可把同一份报告写入文件。
- 服务端 Ctrl+C 进入优雅关闭：停止 Accept，向每个连接发送 CLOSE 帧，等待协程退出，超时（`-shutdown-timeout`，默认 5s）后强制关闭。

## 4. 配置项

### 4.1 服务端（`cmd/gateway`，解析于 `internal/config.ParseServer`）

| Flag | 默认值 | 含义与约束 |
| --- | --- | --- |
| `-addr` | `127.0.0.1:9000` | 监听地址；`host:port`，端口可写 `0` 由系统分配 |
| `-max-body` | `1048576`（1 MiB） | 单帧 Body 上限；必须 `>0` 且 `<= 16777216`（16 MiB 硬上限） |
| `-heartbeat-interval` | `30s` | 服务端 PING 发送间隔；必须 `>0` |
| `-heartbeat-timeout` | `90s` | 距最近一次成功入站帧超过该值则判定心跳超时并关闭连接；必须 `>0` |
| `-idle-timeout` | `120s` | 读 Deadline 时长，超时视为连接失效；必须 `>0` |
| `-send-queue-size` | `256` | 每连接发送队列容量（帧数）；必须 `>0`，队列满即关闭连接 |
| `-shutdown-timeout` | `5s` | 优雅关闭等待上限；必须 `>0` |
| `-stats-interval` | `10s` | `gateway stats` 日志间隔；`<=0` 关闭统计协程（该项不参与校验） |

校验失败时进程以退出码 2 结束并打印 `gateway: ...`。

### 4.2 客户端（`cmd/client`，解析于 `internal/config.ParseClient`）

| Flag | 默认值 | 含义与约束 |
| --- | --- | --- |
| `-addr` | `127.0.0.1:9000` | 服务端地址；连接超时固定 10s |
| `-name` | `demo` | 客户端名，写入每条 DATA 的 Body 前缀 |
| `-interval` | `1s` | 发送间隔；必须 `>0` |
| `-count` | `0` | 发送条数，`0` 表示不限；必须 `>=0` |
| `-payload-size` | `64` | 固定 payload 字节数；必须 `>=0` 且 `<= max-body` |
| `-max-body` | `1048576`（1 MiB） | 单帧 Body 上限；必须 `>0` 且 `<= 16 MiB` |
| `-idle-timeout` | `120s` | 读 Deadline 时长；必须 `>0` |

客户端与服务端的 `-max-body` 应保持一致，否则大帧会被对端以 `ErrFrameTooLarge` 拒绝。

### 4.3 压测工具（`cmd/loadtest`，flag 定义在 `cmd/loadtest/main.go`）

| Flag | 默认值 | 含义与约束 |
| --- | --- | --- |
| `-addr` | `127.0.0.1:9000` | Gateway 地址 |
| `-conns` | `1000` | 并发连接数；必须 `>0` |
| `-duration` | `60s` | 压测时长（含 ramp）；必须 `>0` |
| `-interval` | `1s` | 每连接发送间隔；必须 `>0` |
| `-payload-size` | `64` | 固定 payload 字节数；`>=0` 且 `<= max-body` |
| `-max-body` | `1048576`（1 MiB） | 单帧 Body 上限；必须 `>0` |
| `-ramp` | `5s` | 建连摊开时间；`>=0`，为 0 时立即全部建连 |
| `-timeout` | `5s` | 拨号超时、单条回显等待超时、读 Deadline；必须 `>0` |
| `-out` | 空 | 可选报告文件路径，空表示只打印到 stdout |

## 5. 帧格式

所有多字节整数均为大端序。帧头固定 21 字节，后接 Body。

| 偏移 | 长度 | 字段 | 说明 |
| --- | --- | --- | --- |
| 0–1 | 2 | Magic | 固定 `0x52 0x58`（ASCII `RX`），`binary.BigEndian.Uint16` 读出为 `0x5258` |
| 2 | 1 | Version | 固定 `0x01` |
| 3 | 1 | Type | 帧类型，见下表 |
| 4 | 1 | Flags | 保留位，必须为 `0`；编解码遇到非 0 值直接报错 |
| 5–12 | 8 | MsgID | uint64，请求/回显/心跳应答的关联标识 |
| 13–16 | 4 | BodyLen | uint32，Body 字节数 |
| 17–20 | 4 | CRC32 | IEEE 多项式，**仅覆盖 Body**，不覆盖帧头 |
| 21– | BodyLen | Body | 载荷，可为空 |

| Type | 值 | 语义 |
| --- | --- | --- |
| `PING` | `0x01` | 心跳。服务端每 `heartbeat-interval` 发送，MsgID 取连接内自增计数器 |
| `PONG` | `0x02` | 心跳应答。收到 PING 后原样回填 MsgID 与 Body |
| `DATA` | `0x03` | 业务数据。Phase 0 的 handler 将其原样回显（同 MsgID、同 Body） |
| `ERROR` | `0x04` | 错误帧。协议层已定义，客户端会打印其 Body；Phase 0 服务端尚未产生该帧 |
| `CLOSE` | `0x05` | 关闭通知。服务端优雅关闭时广播；收到即关闭连接 |

编解码规则（`internal/protocol/frame.go`）：

- `Encode` 把完整帧写入一个缓冲区后**一次 `Write` 发出**，拒绝 `nil` 帧、未知类型、非 0 Flags、超 `max-body` 的 Body；短写返回 `io.ErrShortWrite`。
- `Decode` 基于 `bufio.Reader` + `io.ReadFull`：先读满 21 字节头，依次校验 Magic、Version、Flags、Type，再用 BodyLen 与 `max-body` 比较，**通过后才分配 Body 内存**（避免长度字段放大内存），最后校验 CRC32。
- 粘包：一次 `Read` 返回多帧时，`bufio.Reader` 保留剩余字节，下一次 `Decode` 继续读取。半包：`io.ReadFull` 会等待补齐；帧中途 EOF 转为 `io.ErrUnexpectedEOF`。
- 哨兵错误：`ErrBadMagic`、`ErrBadVersion`、`ErrBadFlags`、`ErrFrameTooLarge`、`ErrChecksum`、`ErrBadType`。
- CRC 不覆盖帧头，因此帧头字段的合法性依赖上方显式校验：Magic/Version/Flags/Type 任一非法即拒绝；BodyLen 受 `max-body` 约束。

## 6. 连接生命周期

```text
listener.Accept
      │  （临时错误：10ms 起步、翻倍、上限 100ms 退避）
      ▼
handleConn：分配 conn_id → newConn → register（s.mu 内 wg.Add + 写入 map）
      │  服务正在关闭时拒绝注册并直接关闭 socket
      ▼
go c.run()
      ├── writeLoop      （唯一写 socket 的协程）
      ├── heartbeatLoop  （周期 PING + 心跳超时判定）
      └── readLoop       （在 run 的调用协程中执行：解帧、回 PONG、分发 handler）
      ▼
Close（sync.Once）：标记 closed → close(done) → nc.Close() → onClose 注销 → 记录关闭原因
      ▼
writeLoop / heartbeatLoop 退出 → wg.Wait() → 服务端 wg.Done()
```

关键语义：

- **单 Writer + 有界队列**：`Send` 先编码为 `[]byte`，再非阻塞投递到容量为 `send-queue-size` 的 `chan []byte`。队列满时打印告警、以 `send queue full` 关闭连接并返回 `ErrSendQueueFull`；连接已关闭返回 `ErrConnClosed`。`sendCh` 永不关闭，写协程通过 `done` 退出。
- **读路径**：每次循环按 `idle-timeout` 设置读 Deadline；成功解帧后刷新 `lastInbound`；PING 由读循环直接回 PONG（优先于业务 handler）；收到 CLOSE 帧关闭连接；其余帧交给 handler，handler 返回错误即关闭连接。
- **写路径**：写协程串行调用 `nc.Write`，写失败关闭连接；若刚写出的帧类型为 CLOSE，则在刷出该帧后退出。
- **心跳**：`heartbeatLoop` 按 `heartbeat-interval` 触发；若 `heartbeat-timeout > 0` 且距 `lastInbound` 已超时，先关闭连接；否则发送 PING。注意 `lastInbound` 只由成功入站帧刷新，服务端自己发出的 PING 不计入。
- **关闭原因**（按 `closeWithReason` 首个写入者胜出，日志字段 `reason`）：

  | 原因 | 触发点 |
  | --- | --- |
  | `local close` | 直接调用 `Close()`（未带原因） |
  | `peer closed` | 读端 EOF / `ErrUnexpectedEOF` / `net.ErrClosed` |
  | `idle timeout` | 读 Deadline 到期 |
  | `read error: ...` | 其他解码或 socket 读错误 |
  | `received close frame` | 收到对端 CLOSE |
  | `handler error` | 业务 handler 返回错误 |
  | `send queue full` | 发送队列满 |
  | `write error: ...` | 写 socket 失败 |
  | `close frame sent` | CLOSE 帧已刷出 |
  | `heartbeat timeout` | 超过心跳超时未收到入站帧 |
  | `shutdown: ...` / `shutdown timeout` | 服务端关闭时 CLOSE 发送失败 / 等待超时后强制关闭 |
  | `set read deadline: ...` | 设置读 Deadline 失败 |

- **服务端 Shutdown**：置 `shuttingDown` → 关闭 listener → 在 `s.mu` 下快照连接（该锁同时保证 `wg.Add` 与 `wg.Wait` 不竞争）→ 逐个 `Send(CLOSE)`（失败则带原因关闭）→ 等待所有连接协程退出；`ctx` 到期则 `closeAll("shutdown timeout")` 并返回 `ctx.Err()`。
- **资源模型**：每条连接固定 3 个协程（读、写、心跳），1000 连接对应约 3004 个服务端协程，实测见压测报告第 4 节。

## 7. 测试与验证

质量命令（2026-11-10、go1.27.3 windows/amd64 实测）：

```text
gofmt -l .                 # 无输出
go vet ./...               # 通过
go test ./... -count=1
ok  	relayx/phase0/internal/protocol	1.263s
ok  	relayx/phase0/internal/transport	2.397s
```

| 测试文件 | 覆盖点 |
| --- | --- |
| `internal/protocol/frame_test.go` | 编解码往返、空 Body、最大 Body 边界、Encode 非法输入、单次 Write、Decode 各类错误（Magic/Version/Flags/Type/长度/CRC）、粘包、分片读、EOF 与帧中途 EOF、CRC 仅覆盖 Body、类型字符串 |
| `internal/transport/server_test.go` | 30 客户端并发回显、PING→PONG、心跳保活、心跳超时清理、空闲超时清理、优雅关闭下发 CLOSE、发送队列满背压、50 连接后协程回落（`baseline+5` 内）、粘包与半包端到端 |

压测结论见 [压测报告](压测报告.md)：1000 连接、60s、57003 条 DATA 全部回显，错误计数全 0，结束后协程与 `active_conns` 回落基线。

**竞态检测（通过本机 Docker 运行）**：Windows 本机没有 C 编译器（`gcc` 不在 PATH，`CGO_ENABLED=0`），无法直接执行 `-race`；改用本机 Docker Desktop 的官方 Go 镜像：

```powershell
docker run --rm -v "C:/Users/Administrator/Documents/trae_projects/RelayX/phase0:/app" -w /app golang:1.27 go test -race -count=2 ./...
```

2026-11-10 实际结果（go1.27 linux/amd64，`-count=2`）：

```text
ok  	relayx/phase0/internal/protocol	1.025s
ok  	relayx/phase0/internal/transport	2.450s
```

`cmd/*` 与 `internal/config` 无测试文件。竞态检测未发现 data race；压测工具本身未以 `-race` 构建，race 覆盖范围为单元测试与集成测试。

## 8. 与规划文档 5.4 验收标准对照

| 5.4 验收标准 | Phase 0 实现 | 验证证据 |
| --- | --- | --- |
| 能够启动 TCP Server | `cmd/gateway` 调用 `transport.New`（内部 `net.Listen("tcp", addr)`）与 `Serve()` | 快速开始；压测报告第 4 节 `gateway listening` 与 stats 日志 |
| 支持多个客户端同时连接 | 每连接独立三协程 + `map[uint64]*Conn` 注册表（`RWMutex` 保护） | `TestServerEchoManyClients`（30 客户端 × 10 条）；压测 1000 连接成功率 100% |
| 客户端能够发送和接收消息 | `cmd/client` 定时发 DATA 并打印回显；服务端 `echoHandler` 原样回显 | 客户端示例输出；压测 57003 条全部 echoed |
| 正确处理粘包和半包 | 21 字节定长头 + `bufio.Reader` + `io.ReadFull` 精确读 Body | `TestDecodeStickyFrames`、`TestDecodeFragmentedReads`、`TestServerStickyAndFragmentedFrames` |
| 能够识别客户端断开 | 读端 EOF/`net.ErrClosed` → `peer closed`；空闲超时；心跳超时 | `TestServerHeartbeatTimeout`、`TestServerIdleTimeout`；压测结束后 `active_conns=0` |
| 不存在明显的 Goroutine 泄漏 | `Close` 幂等关闭并等待读写心跳协程退出，`run()` 返回前 `wg.Wait()` | `TestServerNoGoroutineLeak`（基线 +5 内）；压测服务端 3004→4、客户端 2001→1 |
| 支持服务优雅退出 | Ctrl+C → `Shutdown(ctx)`：停 Accept、广播 CLOSE、等待退出、超时强制关闭 | `TestServerGracefulShutdown`；`cmd/gateway` 信号处理；压测报告第 7 节复现步骤 |

已知差距与后续补足：

1. race 检测依赖 Docker/Linux 环境（本机 Windows 工具链无 C 编译器），已在本地 Docker 通过，建议 Phase 1 接入 CI 固化该步骤。
2. 压测只覆盖 60s 本机回环，未覆盖 90s 心跳超时、发送队列打满与大帧场景；这些路径由单测覆盖，缺少长时间稳定性数据。
3. Session、身份绑定与目标路由按规划留到 Phase 1。

## 9. 相关文档

- [Phase 0 千连接压测报告](压测报告.md)
- [ADR-001：为什么使用 Go 标准库 net](adr/ADR-001.md)
- [ADR-002：为什么每连接单 Writer](adr/ADR-002.md)
- [规划文档 Phase 0 章节](../../docs/通用实时消息%20Gateway%20Platform.md)
