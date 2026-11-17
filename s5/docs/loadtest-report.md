# S5 系统级压测报告（连接规模矩阵）

> 对应 LLD：§7（压测矩阵）、§5（四通道隔离验证）、§9（`cmd/loadgen` v1 与延迟分解）。
>
> 原始数据（每轮一个 JSON + 一份 Markdown，均由 loadgen 自产，未手改）：
>
> ```text
> s5/profiling/reports/load-200-mixed.json        load-200-mixed.md
> s5/profiling/reports/load-500-idle.json         load-500-idle.md
> s5/profiling/reports/load-500-p1.json           load-500-p1.md
> s5/profiling/reports/load-500-broadcast.json    load-500-broadcast.md
> s5/profiling/reports/load-500-isolation.json    load-500-isolation.md
> ```

---

## 1. 结论摘要

1. **loadgen v1 可用**：单进程 1 万连接的能力已具备（每连接 2 个 goroutine + 共享读缓冲池 + 共享时间轮），本机实测规模受 fd/CPU 限制（见 §5），矩阵的 10k 以上档位需在 Linux 上执行（§6）。
2. **可靠路径（P1）在 500 连接 / 450 msg/s 下健康**：`send_ack` P50 1.23ms、P99 10.9ms，0 超时，8635 发 / 8634 收。
3. **广播扇出（P2）吞吐高**：500 连接、2 topic，**约 10 万条广播投递/秒**（10757 次 publish → 1,897,225 次投递），P50 1.08ms、P99 5.54ms，发布 0 失败、客户端 0 丢弃。
4. **四通道「队列层」隔离成立，「端到端」隔离在本机不成立**：P2 打满到 7.5 万投递/秒时，`send_ack` P99 从 10.9ms 涨到 20.3ms（+85%），P0 PING P99 从 1.16ms 涨到 7.05ms（+508%），超过 §5.3 建议的 ≤20% 阈值。根因与处置见 §4，这是**本阶段最重要的负面结论**。
5. **空闲连接成本**：500 空闲连接 82.6MB（约 **142.4KiB/连接**，含 GC 余量），按此推算 10 万连接需 ~14GB，必须按 `capacity-model.md` §3 调整读缓冲与队列预算。

---

## 2. 环境与目标

| 项 | 值 |
|---|---|
| commit | `a8cdcc3` |
| OS / Arch | windows / amd64 |
| CPU / 内存 | Intel i3-10105（8 vCPU 可见），16GB |
| Go | go1.27.3，GOMAXPROCS=8（loadgen 与 Gateway 同机） |
| 被压目标 | **S4 `cmd/gateway`**（`s4/`，`-addr :9800 -access :9802`） |
| 压测客户端 | S5 `cmd/loadgen` + `internal/loadclient`（L1 层，不拼裸帧） |
| 时钟粒度 | **514.1µs**（loadgen 实测，`clock_granularity_ns`） |

两条必须先读的限制：

- **同机压测**：Gateway 与 loadgen 共享 8 个 vCPU。任何「CPU 被打满」的结论都要先扣掉客户端自身的 CPU 占用；因此本报告的绝对数字是**同机联调上限**，不是单机服务能力上限。
- **时钟粒度 514µs**：小于该值的延迟被量化为 0（例如环回 PING P50 显示 0µs、编码耗时显示 0µs）。表里凡是 `0µs`，正确读法是「< 514µs」。

---

## 3. 压测矩阵（LLD §7.1）

### 3.1 阶梯定义

| 档位 | 连接 | 订阅 topic | 每连接速率 | 曲线 | 目的 |
|---|---|---|---|---|---|
| L1 | 1,000 | 8 | 0.5 ops/s | ramp 10s / steady 60s / down 5s | 基线延迟与内存 |
| L2 | 10,000 | 16 | 0.2 ops/s | ramp 30s / steady 120s / down 10s | 心跳与定时器开销 |
| L3 | 50,000 | 32 | 0.1 ops/s | ramp 60s / steady 180s / down 20s | 内存与 fd 上限 |
| L4 | 100,000 | 64 | 0.05 ops/s | ramp 120s / steady 300s / down 30s | 单机极限 |

每档位按 §7.2 的维度分别跑：空闲、心跳、低频、高频、慢消费者、大 payload、集中断线重连、广播扇出（单 topic 1 万订阅者）、混合打满。

### 3.2 本环境实际执行范围（诚实标注）

| 档位 | 状态 | 原因 |
|---|---|---|
| 200 / 500 连接 | **已执行**（§3.3 五组） | 本机可稳定运行 |
| 1,000 连接 | 可执行，未纳入本报告 | 需要先调高 `MaxUserPort`/fd 上限并隔离客户端 CPU，否则数字被环境噪声主导 |
| 10,000 / 50,000 / 100,000 | **未执行** | 需要：Linux + `ulimit -n`、`net.core.somaxconn`、`tcp_max_syn_backlog`、`fs.file-max` 调优；Gateway 与 loadgen 分离到不同机器；loadgen 多进程分布式（S7 起支持） |

> 矩阵模板与命令已固化（§6），在 Linux 上按同一脚本即可补齐。**不编造未执行档位的数字。**

### 3.3 已执行结果

| 场景 | 连接 | topic | 混合流量 | SEND/超时 | DELIVER | 广播 发布/接收 | ops/s | send_ack P50/P99 | send_e2e P50/P99 | broadcast P50/P99 | PING P50/P99 | 网关 RSS |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 空闲 + 心跳 | 500 | 4 | ping 1.0（0.05 ops/conn） | 0/0 | 0 | 0/0 | 18.7 | — | — | — | 0µs / 1.77ms | 82.6MB |
| 混合（基线） | 200 | 4 | 40/20/20/20 | 2294/0 | 2294 | 1239 / 57,277 | 284.5 | 0.97 / 8.08ms | 1.23 / 10.6ms | — | 0.32 / 4.40ms | 62.7MB |
| 可靠路径（P1） | 500 | 4 | 60/0/20/20 | 8635/0 | 8634 | 0/0 | 711.3 | 1.23 / 10.9ms | 3.92 / 61.5ms | — | 0µs / 1.16ms | 139.7MB |
| 广播扇出（P2） | 500 | 2 | 0/80/10/10 | 0/0 | 0 | 10,757 / 1,897,225 | 644.6 | — | — | 1.08 / 5.54ms | — | 200.1MB |
| 四通道混合 | 500 | 2 | 40/40/10/10 | 5618/0 | 5589 | 6200 / 1,432,470 | 754.3 | 4.46 / 20.3ms | 6.62 / 605ms | 2.96 / 9.30ms | 0.75 / 7.05ms | 157.9MB |

补充数字：

- 广播扇出组：19s 内 1,897,225 次投递 ≈ **99.9k 条/s**（平均每次 publish 命中 176 个订阅者）；客户端入站丢弃 0；publish HTTP 往返 P99 3.6ms。
- 四通道混合组：广播投递 ≈ **75.4k 条/s**；P2 客户端丢弃 0。
- 空闲组：连接建立 500/500 成功、0 失败；`142.4KiB/连接`（含 GOGC=100 的堆余量）。
- 所有组：`conns_failed=0`，`broadcast_publish_failed=0`；异常延迟样本仅混合基线组 39 条（占 1.7%，为墙钟小幅回拨，已按 0 处理）。

### 3.4 延迟分解口径

| 指标 | 定义 | 精度 |
|---|---|---|
| `send_ack` | 客户端 `Send()` 入队 → 收到 `SEND_ACK` | 纳秒（本地单调时钟），> 514µs 才可信 |
| `send_e2e` | payload 内 `sent_at` → `DELIVER` 到达；分段 `encode`（本地编码+入队）/ `server`（send→ACK）/ `egress`（ACK→DELIVER） | 同上 |
| `deliver_e2e` | payload 内 `sent_at` → 本连接收到 `DELIVER`（跨连接场景也成立） | 同上 |
| `broadcast_e2e` | publish 侧 `sent_at` → 任一订阅端收到 `BROADCAST` | 同上 |
| `publish_http` | `POST /v1/broadcasts` 往返（含 202） | 同上 |
| `ping` | PING → PONG | 同上 |

`sent_at` 由客户端写进 payload（平台不解析 payload，符合 S3 冻结语义）；服务端不新增任何字段。
广播的 `server_received_at` 只有毫秒精度，因此**不用它做分段**（只会得到 0/±1ms 噪声），只报端到端——这是本报告与"看起来很细"的伪分解的区别。

---

## 4. 四通道隔离验证（LLD §5，验收硬项）

### 4.1 队列层隔离：通过

`test/regression` 的机制级用例全部通过（`go test -race ./test/regression/`）：

| 用例 | 断言 |
|---|---|
| `TestIsolationBroadcastFloodKeepsReliable` | P2 灌满（预算 8 帧）后，P1 仍可入队并写出、P0 写延迟 < 500ms |
| `TestIsolationEventFloodKeepsReliable` | P3 洪峰（200 帧）后 P1/P0 正常 |
| `TestIsolationReliableFloodKeepsBroadcastPolicy` | P1 背压时 P2 按「丢弃 + 计数」处理、不断连、P0 仍可写出 |
| `TestIsolationAllChannelsSaturated` | 四通道同时打满，P0 不被饿死 |
| `TestWrittenAccountingMatchesFrames` / `TestFramesAreNotCorruptedByBatching` | 批量写不改变帧数记账与帧边界 |

即：**S1 冻结的"四通道独立队列 + 独立预算 + 单 Writer 优先级"语义在 S5 改造后未退化。**

### 4.2 端到端隔离：本机不达标（负面结论）

| 指标 | P2 空闲（可靠路径组） | P2 打满 75k/s（四通道组） | 劣化 |
|---|---|---|---|
| `send_ack` P50 | 1.23ms | 4.46ms | +262% |
| `send_ack` P99 | 10.9ms | 20.3ms | +85% |
| `ping` P99 | 1.16ms | 7.05ms | +508% |
| `send_e2e` P99 | 61.5ms | 605ms | +884% |

按 §5.3 的判据（并发相对单独运行劣化 ≤20%，且 P0 不被拖死），**这一组判定不通过**。

根因（按贡献排序）：

1. **共享 CPU，不是共享队列**：队列是隔离的，但 P2 扇出的编码/拷贝与每帧 `Write` 消耗的是同一批核心；75k 条/s 的投递把进程 CPU 打满后，P1 的 Writer 拿不到调度时间片。
2. **投递段本身有 200ms 扫描周期**（S2 `ScanInterval=200ms`）：`send_e2e` 的 `egress` 段 P99 达 596ms，说明 DELIVER 的入队时刻由扫描节奏决定，而不是由 ACK 之后立即触发。该段落在**被压目标（S4 内核）**，不属于 S5 的实现层改动范围。
3. **同机压测放大**：500 个客户端连接在同一个 8 vCPU 机器上处理 75k 条/s 入站广播，客户端自身也接近饱和。

正确的处置（不在这台机器上做决定）：

- 用**两台机器**（或 Linux + 足够核心）复跑同一条命令，判定客户端 CPU 与网关 CPU 各自的占比；
- 若网关侧仍劣化 > 20%，则是真实的 CPU 竞争，属 S5 允许的实现层优化范围（例如 P2 扇出按核心分区、批量写进一步降 syscall、或把 `egress` 改为事件驱动而非 200ms 扫描——最后一项属于 S2 语义层的调度实现，需单独 ADR）；
- **不允许**通过放宽 §5.3 阈值或删掉该用例来"通过"验收。

---

## 5. 规模上限的主要约束（本机实测推断）

| 约束 | 实测/推算 | 说明 |
|---|---|---|
| 每连接内存 | 142.4KiB | 读缓冲 64KiB + 四通道队列（S1 缺省预算 ~18KiB）+ 连接/会话对象 + GOGC 余量 |
| 文件描述符 | Windows 默认端口范围受限 | 10 万连接前必须调 `MaxUserPort`/`TcpTimedWaitDelay`（Linux：`ulimit -n`、`fs.file-max`） |
| 客户端 goroutine | 2/连接（读 + 写）+ 1/进程（时间轮） | 10 万连接 ≈ 20 万 goroutine；`loadclient` 已把读缓冲与心跳定时器全局共享 |
| 客户端 CPU | 500 连接 / 75k 入站条/s 已接近 8 vCPU 的一半 | 10 万连接需多进程 loadgen（S7）或专用压测机 |

---

## 6. 复跑命令（矩阵模板）

```powershell
# 1) 启动被压目标（S4 Gateway，本报告的目标版本）
cd ..\s4
go run ./cmd/gateway -addr 127.0.0.1:9800 -metrics-addr 127.0.0.1:9801 -access-addr 127.0.0.1:9802

# 2) 逐档位执行（在 s5 目录）
cd ..\s5
go run ./cmd/loadgen -gateway 127.0.0.1:9800 -conns 1000 -topics 8  -rate 0.5 `
  -curve "ramp:10s:0->1,steady:60s,down:5s:1->0" -out profiling\reports\load-1k.json -md profiling\reports\load-1k.md
go run ./cmd/loadgen -gateway 127.0.0.1:9800 -conns 10000 -topics 16 -rate 0.2 `
  -curve "ramp:30s:0->1,steady:120s,down:10s:1->0" -out profiling\reports\load-10k.json

# 3) 故障注入（LLD §9.3）
go run ./cmd/loadgen -conns 500 -inject-slow 0.05 -inject-big 0.05 -inject-churn 0.01 `
  -mix "send:0.6,receipt:0.2,ping:0.2" -curve "ramp:5s:0->1,steady:30s,down:5s:1->0"
```

Linux 前置（10k 以上必须）：

```bash
ulimit -n 200000
sysctl -w net.core.somaxconn=65535
sysctl -w net.ipv4.tcp_max_syn_backlog=65535
sysctl -w fs.file-max=2097152
# 客户端与服务端分机部署；loadgen 按 CPU 核数起多进程（每进程 ≤2 万连接）
```

---

## 7. 与 S8 的衔接

本报告的字段（`conns_*`、`sends_*`、`delivers_*`、`latency[]`、`clock_granularity_ns`、`load_window_seconds`）与 S8 容量报告口径一致，可直接横向对比；未执行的 10k/50k/100k 档位在 S8 前必须补齐，否则 `capacity-model.md` 的横向扩展系数只能停留在模型值。
