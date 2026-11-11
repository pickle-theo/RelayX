# Phase 3 优化报告（第一轮 / 第二轮）

本文记录 Phase 3 的两轮优化：每轮都按「假设 → 改动 → 同参数复测 → 判定」执行，
数据来自 `docs/bench/raw/` 下的原始报告。基线与瓶颈分析见
[baseline-report.md](baseline-report.md) 与 [bottleneck-analysis.md](bottleneck-analysis.md)。

## 环境与口径

| 项 | 值 |
| --- | --- |
| 宿主 | Windows 8 核 / 8GB，Docker Desktop |
| 容器 | `golang:1.27` Linux，`ulimit -n=1048576`，容器临时端口 32768–60999 |
| 拓扑 | gateway 与 loadtest 同容器；PostgreSQL 在本机 Docker，经 `host.docker.internal` 访问 |
| 数据 | 每轮复测只跑一次，参数与基线完全相同；不做外推 |
| profile | 压测开始 10s 后采集 CPU 15s，随后 heap/goroutine/block/mutex |

## 第一轮：写路径与数据库并发

### 假设

1. `writeBatch` 每批把所有帧拼接进一个临时缓冲区再写，带来每批一次的拷贝与大块分配；
2. 每次写成功后把写 deadline 重置为零值，是多余的一次系统调用；
3. 可靠链路 1000 msg/s 崩塌时，`goroutine-reliable-cum.txt` 显示 985 个协程停在
   `database/sql.(*DB).conn`，池只有 16 个连接，排队是吞吐上限。

### 改动

| 文件 | 改动 |
| --- | --- |
| `internal/transport/conn.go` | `writeBatch` 改用 `net.Buffers.WriteTo`（Linux 走 writev），删除拼接缓冲区；新增 `batchBuf` 复用帧批次、`bufs` 作为 writev 的临时切片（`WriteTo` 会消费传入切片，原批次保留用于归还 encode buffer） |
| `internal/transport/conn.go` | 删除写成功后的 `SetWriteDeadline(time.Time{})`，每次写之前都会重新设置绝对 deadline |
| `internal/config/config.go`、`internal/store/store.go` | `-db-max-open-conns` 与 `DefaultMaxOpenConns` 由 16 提升到 64 |

### 复测 1：可靠链路 1000 连接 / 1 msg/s（`-Mode reliable -Ack both`，40s）

| 指标 | 基线 | 第一轮 | 变化 |
| --- | --- | --- | --- |
| sent | 41,937 | 36,926 | -12.0% |
| send_ack | 28,058 | 34,917 | +24.4% |
| delivered | 28,125 | 35,010 | +24.5% |
| business_acked | 28,125 | 35,010 | +24.5% |
| ack_missed | 13,812 | 1,916 | **-86.1%** |
| p50 | 4323.400 ms | 1725.826 ms | **-60.1%** |
| p99 | 10724.000 ms | 6632.582 ms | **-38.2%** |
| exit_code | 1 | 1 | 仍失败 |

原始数据：`docs/bench/raw/baseline-1k-reliable.txt`、`docs/bench/raw/opt1-1k-reliable.txt`。

判定：**保留**。四项核心时延/丢失指标改善 38%–86%，但 1000 msg/s 仍未闭环：
`deliveries_pending` 仍从 82 增长到 3209，`messages_accepted` 稳态约 600–900/s，
说明每条 SEND 的 4 次数据库往返（`ListEndpoints`、`AcceptMessage`、
`ApplyReceiptAck`、`ApplyBusinessAck`）仍是上限。剩余候选（ACK 写入批处理、
把持久化移出 readLoop）见瓶颈分析文档，不在本轮范围内。

### 复测 2：10k 连接 / 1 msg/s echo（45s，`-Mode echo -Profile 1`）

| 指标 | 基线 | 第一轮 | 变化 |
| --- | --- | --- | --- |
| sent = echoed | 370,807 | 370,769 | -0.01% |
| p50 | 0.240 ms | 0.238 ms | -0.8% |
| p99 | 1.624 ms | 1.318 ms | -18.8% |
| CPU 采样 | 11.38s / 15s (75.86%) | 11.23s / 15s (74.86%) | -1.3% |
| heap_alloc（15s 时点，10k 连接） | 169.53 MB | 142.78 MB | **-15.8%** |
| heap_alloc（45s 结束） | 201.72 MB | 176.47 MB | -12.5% |

profile：`Syscall6` 34.53% → 34.73%，`futex` 29.53% → 30.54%，
写路径出现 `net.(*Buffers).WriteTo` / `internal/poll.(*FD).Writev`（cum 26.0%），
原来的拼接缓冲区不再出现在堆上。

判定：**保留**。吞吐与 p99 在噪声范围内（CPU 采样 -1.3%），
但同一负载下活跃堆下降 15.8%，说明每批拷贝与大缓冲区分配被消除；
这一收益为第二轮的内存优化打下基础。

## 第二轮：每连接固定内存

### 假设

10k 空闲连接时 `heap_alloc` 147.45MB ≈ 14.7KB/连接，构成大致为：
2 个协程栈、`sendCh`（默认 256 槽 ≈ 2KB）、`bufio` 读缓冲（默认 4KB）、
连接结构与 logger 字段。缩小前两者可以线性下降每连接内存。

### 改动

| 文件 | 改动 |
| --- | --- |
| `internal/config/config.go` | `-read-buffer-size` 默认 4096 → 1024（bufio 对大 body 直接读入目标切片，不经过缓冲） |
| `internal/config/config.go` | `-send-queue-size` **尝试** 256 → 64，随后回退到 256（原因见下） |
| `internal/config/config_test.go` | 默认值断言同步更新 |

> 回退记录：把发送队列降到 64 后，`TestPullPagination` 在真实 PostgreSQL 上稳定失败
> （`decode frame: EOF`）。PULL 单页最多 100 条 DELIVER 加 1 条 PULL_DONE，
> 在写协程排空之前会超过 64 槽，`Send` 按慢消费者策略关闭连接。
> 该契约还影响多设备 fan-out，因此回退到 Phase 2 的 256，只保留读缓冲优化。
> 这也解释了为什么每连接内存没有达到预期的 -45%。

### 复测 1：10k 空闲连接（40s，登录后静默，`-Scenario idle`）

| 指标 | 基线 | 第二轮 | 变化 |
| --- | --- | --- | --- |
| 建连成功 | 10000/10000 | 10000/10000 | — |
| 网关 goroutines | 20,007 | 20,007 | — |
| heap_alloc（稳态） | 147.45 MB | 129.08 MB | **-12.5%** |
| 每连接堆 | 14.75 KB | 12.91 KB | **-12.5%** |
| 客户端 rss_per_conn | 见基线报告 | 23.15 KB | — |

原始数据：`docs/bench/raw/baseline-10k-idle.txt`、`docs/bench/raw/opt2-10k-idle.txt`。

判定：**保留**，但没有达到 ≥20% 的目标（实际 -12.5%）。差距来自被回退的发送队列：
保留 256 槽是 PULL 单页 100 帧与 fan-out 的要求；剩余可下降空间主要在两个协程栈，
需要改变并发模型（见 ADR-012）而不是调小缓冲。

### 复测 2：10k 连接 / 1 msg/s echo 回归（45s，`-Profile 1`）

| 指标 | 第一轮 | 第二轮 | 变化 |
| --- | --- | --- | --- |
| sent = echoed | 370,769 | 370,630 | -0.04% |
| p50 | 0.238 ms | 0.242 ms | +0.004 ms |
| p99 | 1.318 ms | 1.377 ms | +0.059 ms |
| max | 124.901 ms | 118.335 ms | -6.6 ms |
| heap_alloc（45s 结束） | 176.47 MB | 119.83 MB | **-32.1%** |
| 错误（timeouts/decode/write） | 0 | 0 | — |

原始数据：`docs/bench/raw/opt1-10k-echo-high.txt`、`docs/bench/raw/opt2-10k-echo-high.txt`。

判定：**保留**。吞吐与尾延迟在噪声范围内、零错误，活跃堆相对第一轮再降 32.1%
（相对基线 201.72MB 下降 40.6%）。

## 前后总表

| 指标 | Phase 2 基线 | 两轮优化后 | 变化 |
| --- | --- | --- | --- |
| 10k 空闲每连接堆 | 14.75 KB | 12.91 KB | -12.5% |
| 10k echo 活跃堆（45s） | 201.72 MB | 119.83 MB | **-40.6%** |
| 10k echo 吞吐 | 370,807 / 45s | 370,630 / 45s | -0.05% |
| 10k echo p99 | 1.624 ms | 1.377 ms | -15.2% |
| 可靠 1k p50 | 4323.400 ms | 1725.826 ms | **-60.1%** |
| 可靠 1k ack_missed | 13,812 | 1,916 | **-86.1%** |
| 可靠 1k 是否闭环 | 否 | 否 | 仍待第三轮 |

核心指标中有 4 项达到 ≥20% 改进（可靠链路 p50/p99/ack_missed、10k 活跃堆）；
10k 空闲每连接内存只下降 12.5%，原因是 PULL 契约要求 256 槽发送队列，
缩容实验被集成测试否决并已回退，详见上文与回退记录。

## 回退记录

1. **发送队列 256 → 64（第二轮）**：10k 空闲堆一度降到 80.13MB（-45.7%），
   但 `TestPullPagination` 在真实 PostgreSQL 上稳定报 `decode frame: EOF`：
   PULL 单页 100 条 DELIVER 加 PULL_DONE 超过 64 槽，`Send` 按慢消费者关闭连接。
   该行为也会影响多设备 fan-out，因此回退到 256，保留读缓冲优化。
2. **写路径的 CPU 收益**：第一轮 `Syscall6`/`futex` 占比没有下降，CPU 采样只降 1.3%，
   属于噪声；保留写路径改动的理由是活跃堆下降 12.5%–15.8%，而不是 CPU 提升。
3. **无其它回退**：可靠链路的时延与丢失指标、10k 活跃堆与吞吐均通过复测。

## 未完成项与下一步

1. 可靠链路 1000 msg/s 仍受「每消息 4 次数据库往返 + 同步读路径」限制；
   候选方案是 ACK 批量写入（`unnest` 一次落多条）与持久化工作队列，需第三轮验证。
2. 心跳仍为每连接一个 `time.Timer`，50k 连接时定时器数量线性增长；
   如继续扩容，应评估共享时间轮。
3. 50k/100k 连接的容量边界见 [capacity-report.md](capacity-report.md)，瓶颈在
   容器临时端口而不是网关内存。
