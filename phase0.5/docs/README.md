# RelayX Phase 0.5：标准库 TCP Gateway（Phase 0 极致优化版）

- 状态：已完成（2026-11-10）
- 模块：`relayx/phase0.5`（`go 1.27`，零第三方依赖）
- 定位：在 Phase 0 的功能与**线格式完全不变**的前提下，用标准库手段把单机传输层压到更低的开销
- 基线：[Phase 0 文档](../../phase0/docs/README.md)（`relayx/phase0`，本阶段只做优化，不新增业务能力）
- 数据：[Benchmark 对比](benchmark.md) / [千连接压测报告](压测报告.md)

## 1. 项目定位

Phase 0.5 是 Phase 0 的极致优化版：仍然是「TCP 长连接 + 21 字节定长帧头 + CRC32 + 心跳 + 连接生命周期」这条最小链路，仍只使用 Go 标准库，`go.mod` 只有 `module` 与 `go` 两行。优化集中在热路径：

- 协议层新增 `AppendFrame(dst, f, maxBody)` 与 `DecodeInto(r, f, maxBody)`，复用调用方缓冲；`Encode` / `Decode` 变成薄封装，**线格式（21 B 头 + CRC32）与 Phase 0 完全一致**。
- 编码缓冲按 512 B / 4 KiB / 64 KiB 分级池化，`Frame` 对象也池化（见 [ADR-003](adr/ADR-003.md)）。
- 写协程把多个数据帧合并为一次 `Write`，控制帧走独立优先通道，并新增写 Deadline 与按字节计的发送预算（见 [ADR-004](adr/ADR-004.md)）。
- 心跳并入写协程，每连接服务端协程由 3 个降为 **2 个**。

不包含（仍属后续阶段）：Session/身份绑定/消息路由（Phase 1）、可靠投递与离线消息（Phase 2）、TLS、鉴权、Metrics 端点、分布式 Gateway。

## 2. 目录结构

```text
RelayX/
├── go.work                          # Go 工作区：use ./phase0 与 ./phase0.5
├── phase0/                          # 基线（只读对照，不修改）
└── phase0.5/                        # 本阶段独立 module
    ├── go.mod                       # module relayx/phase0.5, go 1.27
    ├── Makefile                     # build/test/race/bench/profile（依赖 make 与 sh）
    ├── bin/                         # 本地构建产物（gateway.exe / loadtest.exe / client.exe）
    ├── cmd/
    │   ├── gateway/main.go          # DATA 回显、PING→PONG、信号优雅退出、周期 stats 日志
    │   ├── client/main.go           # 演示客户端：定时发 DATA、自动回 PONG、打印回显
    │   └── loadtest/main.go         # 压测工具：ramp 建连、RTT 分位、错误分类、可选报告文件
    ├── internal/
    │   ├── protocol/frame.go        # Frame、常量、AppendFrame/Encode、DecodeInto/Decode、哨兵错误
    │   ├── transport/conn.go        # Conn：readLoop + writeLoop、Send、ctrlCh、批量写、幂等 Close
    │   ├── transport/pool.go        # 编码缓冲分级池与 Frame 池、所有权约定
    │   ├── transport/server.go      # Server：Accept、连接注册表、Shutdown
    │   └── config/config.go         # 服务端/客户端 flag 解析、默认值与校验
    └── docs/
        ├── README.md                # 本文件
        ├── benchmark.md             # Phase 0 vs Phase 0.5 基准对比
        ├── 压测报告.md               # 千连接压测
        └── adr/                     # ADR-001 ~ ADR-004
```

## 3. 快速开始

在 `phase0.5/` 目录执行（本机未安装 make，直接使用 `go`）：

```powershell
# 构建
New-Item -ItemType Directory -Force -Path bin | Out-Null
go build -o bin/gateway.exe ./cmd/gateway
go build -o bin/client.exe ./cmd/client
go build -o bin/loadtest.exe ./cmd/loadtest

# 终端 1：启动服务端
.\bin\gateway.exe -addr 127.0.0.1:9000

# 终端 2：演示客户端（每秒 1 条 DATA，收满 5 条回显后退出）
.\bin\client.exe -addr 127.0.0.1:9000 -name demo -count 5

# 终端 3：千连接压测
.\bin\loadtest.exe -addr 127.0.0.1:9000 -conns 1000 -duration 60s -interval 1s -ramp 5s
```

预期行为：

- 服务端 stdout 打印 `gateway listening on 127.0.0.1:9000`；运行日志经 `slog.Default()` 输出到 stderr，每 `-stats-interval`（默认 10s）输出一条 `gateway stats`（`goroutines`、`heap_alloc_mb`、`heap_sys_mb`、`active_conns`）。
- 客户端打印 `connected ... as demo`，每秒发送一条 Body 形如 `demo#1 abc...` 的 DATA，并打印回显；Ctrl+C 打印 `bye` 退出。
- 压测工具输出 `[connections]`、`[messages]`、`[errors]`、`[rtt ms]`、`[client process]`；存在连接失败或 `sent > echoed` 时退出码为 1。`-out report.txt` 可把报告写入文件。
- 服务端 Ctrl+C 触发 `Shutdown(ctx)`：停止 Accept、广播 CLOSE、等待协程退出，超时（`-shutdown-timeout`，默认 5s）后强制关闭。

也可以直接 `go run ./cmd/gateway`，无需先构建。

## 4. 配置项

### 4.1 服务端（`cmd/gateway`，`internal/config.ParseServer`）

★ 为 Phase 0.5 新增项：

| Flag | 默认值 | 含义与约束 |
| --- | --- | --- |
| `-addr` | `127.0.0.1:9000` | 监听地址；端口可写 `0` 由系统分配 |
| `-max-body` | `1048576`（1 MiB） | 单帧 Body 上限；`>0` 且 `<= 16777216`（16 MiB 硬上限） |
| `-heartbeat-interval` | `30s` | PING 发送间隔；`>0` |
| `-heartbeat-timeout` | `90s` | 距最近入站帧超过该值即判心跳超时；`>0` |
| `-idle-timeout` | `120s` | 读空闲超时；`>0` |
| `-send-queue-size` | `256` | 每连接数据帧队列容量（帧数）；`>0`，队列满即关闭连接 |
| ★ `-send-queue-bytes` | `4194304`（4 MiB） | 每连接发送队列的**字节预算**（含控制帧）；`>0`，超限回滚并入队失败 |
| ★ `-write-timeout` | `10s` | 单次 `Write` 的 Deadline；`>0`，超时以 `write timeout` 关闭连接 |
| ★ `-write-batch-frames` | `16` | 一次 `Write` 最多合并的数据帧数；`>0` |
| ★ `-write-batch-bytes` | `65536`（64 KiB） | 一次 `Write` 的目标字节数，可被最后一帧略微超出；`>0` |
| ★ `-read-buffer-size` | `4096` | `bufio.NewReaderSize` 的读缓冲大小；`>0` |
| `-shutdown-timeout` | `5s` | 优雅关闭等待上限；`>0` |
| `-stats-interval` | `10s` | `gateway stats` 间隔；`<=0` 关闭统计协程（不参与校验） |

校验失败时进程以退出码 2 结束并打印 `gateway: ...`。

### 4.2 客户端与压测工具

`cmd/client` 的 flag（`-addr`、`-name`、`-interval`、`-count`、`-payload-size`、`-max-body`、`-idle-timeout`）与 `cmd/loadtest` 的 flag（`-addr`、`-conns`、`-duration`、`-interval`、`-payload-size`、`-max-body`、`-ramp`、`-timeout`、`-out`）与 Phase 0 相同，未新增配置。

## 5. 架构说明

### 5.1 每连接两个协程

```text
handleConn → newConn → register → go c.run()
                                  ├── readLoop   （调用协程：解帧、回 PONG、分发 handler）
                                  └── writeLoop  （唯一写 socket 的协程，兼管心跳 Timer）
```

- **readLoop**：`bufio.NewReaderSize(nc, read-buffer-size)`；`getFrame` + `DecodeInto` 复用 Frame/Body；每帧只取一次 `time.Now()`；读 Deadline 只在剩余不足 `idle-timeout/2` 时刷新，超时且 `r.Buffered()==0` 时按 `lastInbound+idleTimeout` 重设，保持 Phase 0 的空闲语义。
- **writeLoop**：唯一调用 `nc.Write` 的协程。心跳不再有独立协程，而是 `time.Timer` 的 `select` 分支：到点先判断心跳超时，再 `Send(PING)` 并 `Reset`。CLOSE 帧刷出后退出。
- **关闭**：`Close` 由 `sync.Once` 保证幂等，`close(done)` + `nc.Close()` 唤醒所有阻塞点；`sendCh`/`ctrlCh` 永不关闭；`run()` 在 readLoop 返回后 `Close` 并 `wg.Wait()`。

### 5.2 ctrlCh 严格优先

控制帧（PING/PONG/CLOSE）进入容量 16 的 `ctrlCh`，数据帧（DATA/ERROR）进入 `sendCh`。writeLoop 每轮先做一次非阻塞的 ctrlCh 检查，再在 `select` 中把 ctrlCh 列在数据通道之前，因此心跳应答与关闭通知不会被数据积压饿死。控制帧同样计入 `send-queue-bytes` 预算。

### 5.3 批量写

数据帧的写出流程：从 `sendCh` 取到第一帧后，`drainData` 贪心继续取（非阻塞），直到达到 `write-batch-frames` 或 `write-batch-bytes`；`writeBatch` 把所有帧拼进一个池化缓冲，设置 `SetWriteDeadline(now+write-timeout)` 后单次 `Write`，写完清除 Deadline。10 帧合并为 1 次 `Write` 的单测证据见 [benchmark.md](benchmark.md)。

### 5.4 池化与所有权

- 编码缓冲：`getEncodeBuf(n)` 返回容量为 512 B / 4 KiB / 64 KiB 中最小的够用等级；`>64 KiB` 精确分配且不回收。`putEncodeBuf` 只回收容量恰好等于某个等级尺寸的缓冲。
- Frame：`getFrame` / `putFrame` 复用 `*protocol.Frame`；`putFrame` 清零 Type/Flags/MsgID，Body 截断为 0 长度，容量超过 64 KiB 的 Body 直接丢弃。
- **所有权**：`Send` 成功后，编码缓冲归 writeLoop，写协程把它复制进批次缓冲后即归还池；handler 返回后 `f.Body` 立即失效，需要保留必须自行拷贝。违反该约定会读到被其他连接复用的内存。
- 代价：`sync.Pool.Put([]byte)` 的接口装箱会产生固定 24 B/次分配，无法用标准库消除（实测见 [benchmark.md](benchmark.md)）。

## 6. 与 Phase 0 的差异摘要

| 维度 | Phase 0 | Phase 0.5 |
| --- | --- | --- |
| 协议 API | `Encode(io.Writer)` / `Decode(*bufio.Reader)`，每次分配 | `AppendFrame` / `DecodeInto` 复用缓冲，`Encode`/`Decode` 变薄封装；**线格式不变** |
| 编码缓冲 | 每帧新分配（`bytes.Buffer` 等） | 512 B / 4 KiB / 64 KiB 分级 `sync.Pool`，>64 KiB 不回收 |
| Frame 对象 | 每帧新建 | `getFrame` / `putFrame` 池化 |
| 每连接协程 | 3（读 + 写 + 心跳） | **2**（读 + 写，心跳并入写协程的 Timer） |
| 控制帧 | 与数据同一 FIFO，存在队头阻塞 | `ctrlCh`（容量 16）严格优先 |
| 写系统调用 | 1 次 / 帧 | 最多 16 帧 / 64 KiB 合并为 1 次（实测 10 帧 → 1 次） |
| 背压 | 仅 `send-queue-size`（帧数） | 帧数 + `send-queue-bytes`（字节预算）双约束，超限回滚并关闭 |
| 写超时 | 无，慢对端可长期阻塞写协程 | `write-timeout`（默认 10s），超时以 `write timeout` 关闭 |
| 读路径 | 每帧刷读 Deadline，`time.Now()` 多次 | 半窗口刷新 + 一次 `time.Now()`；`read-buffer-size` 可配 |
| 千连接协程峰值 | 3004 | 2004（实测） |
| 吞吐 | 57003 sent / 57003 echoed / 0 missed | 同参数结果相同（见[压测报告](压测报告.md)） |

帧格式、帧类型语义、连接生命周期、关闭原因集合与 Phase 0 一致；新增的关闭原因只有 `write timeout` 与 `set write deadline: ...`。

## 7. 质量门禁

在 `phase0.5/` 目录依次执行：

```powershell
gofmt -l .                 # 必须无输出
go vet ./...
go test ./... -count=1
go test ./... -run 'Leak|Goroutine' -count=1 -v

# race 需要本机 Docker（Windows 无 C 编译器）
docker run --rm -v "C:/Users/Administrator/Documents/trae_projects/RelayX/phase0.5:/app" -w /app golang:1.27 go test -race -count=1 ./...
```

2026-11-10 实测结果：

```text
gofmt -l .         # 无输出
go vet ./...       # 通过
go test ./... -count=1
ok  	relayx/phase0.5/internal/config	1.224s
ok  	relayx/phase0.5/internal/protocol	1.069s
ok  	relayx/phase0.5/internal/transport	3.077s
```

协程检查：`TestConnTwoGoroutinesPerConnection`（readLoop=20 / writeLoop=20 / heartbeatLoop=0）与 `TestServerNoGoroutineLeak` 均通过。race 结果见[压测报告第 8 节](压测报告.md#8-质量门禁)。

## 8. 文档索引

- [Benchmark 与 Profiling：Phase 0 vs Phase 0.5](benchmark.md)
- [千连接压测报告（含与 Phase 0 对比）](压测报告.md)
- [ADR-001：为什么使用 Go 标准库 net](adr/ADR-001.md)
- [ADR-002：为什么每连接单 Writer（有界队列）](adr/ADR-002.md)
- [ADR-003：池化与缓冲所有权](adr/ADR-003.md)
- [ADR-004：批量写与背压](adr/ADR-004.md)
