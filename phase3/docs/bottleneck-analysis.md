# RelayX Phase 3 瓶颈分析

本文件基于 Phase 3 基线压测与 pprof 采样，定位 Phase 2 代码的性能瓶颈，并给出候选优化与优先级。所有 profile 数字均来自 `docs/bench/raw/profiles/`，压测数字来自 `docs/bench/raw/` 原始报告；原始数据未覆盖的项标注「未采集」。

## 1. 方法论

**采集方式**（`bench/run.sh`）

- `PROFILE=1` 时 Gateway 以 `-pprof-addr 127.0.0.1:16060 -block-profile-rate 1 -mutex-profile-fraction 1` 启动，使 block / mutex profile 全量采样。
- profile collector 先 `sleep $PROFILE_DELAY`（默认 **10s**，等待 ramp 结束、负载进入稳态），再抓 `/debug/pprof/profile?seconds=$PROFILE_SECONDS`（默认 **15s**）得到 CPU profile，随后依次抓取 `heap`、`goroutine`、`block`、`mutex`。
- CPU profile 文件头记录 `Duration: 15s`，与上述参数一致；`heap` 为 `inuse_space` 快照，`block`/`mutex` 为 `delay` 累计值。
- 每份 profile 以 `go tool pprof -top` 输出到 `profiles/*.txt`，下文引用其 flat / cum 百分比。

| 分组 | 报告文件 | CONNS | INTERVAL | DURATION | RAMP | 实际 sent / elapsed | 实测速率 | p50 / p99 (ms) | exit_code |
|------|----------|-------|----------|----------|------|---------------------|----------|----------------|-----------|
| 10k steady | `profile-10k-steady.txt` | 10000 | 5s | 60s | 15s | 100137 / 60.177s | ≈1664 msg/s（目标 2000） | 0.226 / 0.539 | 0 |
| 10k high | `profile-10k-high.txt` | 10000 | 1s | 45s | 15s | 370807 / 45.233s | ≈8198 msg/s（目标 10000） | 0.240 / 1.624 | 0 |
| 1k reliable | `profile-1k-reliable.txt` | 1000 | 1s | 40s | 5s | 36948 / 50.014s | ≈739 msg/s（目标 1000） | 4743.379 / 10816.473 | 1 |

10k steady 与 10k high 的 profile 在同一容器、同一 Gateway 构建（`profiles/*` 头部 `Build ID: c8126d5e0c458b94f02a08f418d41b123d6ec499`）下采集，可直接横向比较；1k reliable 为另一组（reliable 模式）采集。

| 瓶颈 | 优先级 | 关键量化指标 |
|------|--------|--------------|
| 1. 每连接固定内存与协程开销 | P1 | 10k idle `heap_alloc_mb=147.01`、`goroutines=20007` |
| 2. 写/读系统调用与调度开销 | P1 | high 档 `Syscall6` 34.53%、`futex` 29.53%、`writeLoop` cum 36.47% |
| 3. 可靠链路数据库连接池排队 | P0 | 1000 协程中 985 阻塞在 `DB.conn`，`DB.retry` 阻塞 47.49% |
| 4. 每连接心跳定时器 | P2 | `time.NewTimer` 1MB（1.72%）、timers 相关 CPU 约 1–2% |

## 2. 瓶颈 1：每连接固定内存与协程开销

**现象**：即使完全空闲，10k 连接也常驻约 147MB 堆与 20k 协程，单连接固定成本高，连接规模被内存与调度能力提前限制。

**指标**

- `baseline-10k-idle.txt` 稳定段（03:59:23–03:59:43）：`active_conns=10000`、`goroutines=20007`、`heap_alloc_mb=147.01→147.70`、`heap_sys_mb=153.94→157.91`、`gc_pause_ms=0.1`。换算约 **14.7KB 堆 / 连接**、**2 个协程 / 连接**。
- 同报告客户端 `[client process]`：`goroutines_pre_shutdown=20001`、`rss_per_conn_kb=23.09`。

**Profile 证据**

- `profiles/heap-top.txt`（10k steady，`inuse_space`，总量 70.29MB）：
  - `transport.newConn` flat **35.18MB / 50.05%**，cum 35.68MB / 50.76%
  - `bufio.NewReaderSize` flat **23.09MB / 32.85%**
  - `runtime.mallocgc` flat 6MB / 8.54%
  - `transport.(*Conn).writeLoop` flat **2.50MB / 3.56%**
  - 两者合计 cumulative 82.90%（与原始文件一致）
- `profiles/heap-top-high.txt`（10k high，总量 58.27MB）：`transport.newConn` 30.17MB / **51.78%**，`bufio.NewReaderSize` 17.57MB / **30.15%**，`transport.(*Conn).writeLoop` cum 2.01MB / 3.45%，`time.NewTimer` 1MB / 1.72%。
- `profiles/goroutine-top.txt`（10k steady，总量 20009）：`runtime.gopark` 20004 / 100%；`bufio.(*Reader).Read` → `transport.(*Conn).readLoop` **10000 / 49.98%**；`transport.(*Conn).writeLoop` → `runtime.selectgo` **10000 / 49.98%**。即每个连接恰有 1 个读协程阻塞在 `bufio.Read`、1 个写协程阻塞在 `selectgo`。

**影响范围**：连接密度（每连接 14.7KB 堆 + 2 个协程）。`baseline-50k-idle.txt` 在 28232 连接时已达 `goroutines=56471`、`heap_alloc_mb=459.64`；内存与协程数随连接数线性增长，是 50k 档的主要资源开销之一（该档实际先撞到临时端口上限）。

**候选方案**

- 缩小 `sendCh` 容量（当前默认 256）→ 降低每连接队列的固定分配。
- 缩小读缓冲（当前默认 4096 字节）→ 直接压缩 `bufio.NewReaderSize` 的 23.09MB / 32.85%。
- 批量写 slice 复用（pool）→ 降低 `writeLoop` 与 `mallocgc` 的分配压力。

**优先级**：P1。读缓冲与 sendCh 属于结构性内存收益，放第二轮（需回归大 payload 与慢消费者）；slice 复用风险低，随第一轮写路径改造一起做。

## 3. 瓶颈 2：写/读系统调用与调度开销

**现象**：10k high 下 CPU 时间高度集中在系统调用与调度原语，写路径（`writeLoop` → `writeBatch`）是最大单一热点；但锁竞争并不存在。

**指标**

- `profile-10k-high.txt`：45.233s 内 370807 条 echo，p50 0.240ms、p99 1.624ms；gateway `goroutines` 峰值 20011（稳态 20008）、`heap_alloc_mb` 峰值 201.72。
- `profile-10k-steady.txt`：60.177s 内 100137 条，p50 0.226ms、p99 0.539ms。

**Profile 证据**

- `profiles/cpu-top-high.txt`（`Duration: 15s, Total samples = 11.38s (75.86%)`）：
  - `internal/runtime/syscall/linux.Syscall6` flat **3.93s / 34.53%**
  - `runtime.futex` flat **3.36s / 29.53%**
  - `transport.(*Conn).writeLoop` cum **4.15s / 36.47%**
  - `transport.(*Conn).writeBatch` cum **3.43s / 30.14%**
  - `protocol.DecodeInto` cum **1.86s / 16.34%**
  - `internal/poll.(*FD).Write` cum 2.96s / 26.01%；`transport.(*Conn).readLoop` cum 2.70s / 23.73%
  - `internal/poll.runtime_pollSetDeadline` cum 0.23s / 2.02%（每次写后重置 deadline 的代价）
- `profiles/cpu-top.txt`（10k steady，`Total samples = 3.97s (26.46%)`）：`Syscall6` **1.28s / 32.24%**、`futex` **1.12s / 28.21%**、`readLoop` cum 1.06s / 26.70%、`DecodeInto` cum 0.73s / 18.39%、`internal/poll.(*FD).Write` cum 0.92s / 23.17%。
- `profiles/block-top.txt`：总量 **43.42hrs**，唯一阻塞点为 `runtime.selectgo` 100%，其调用方为 `transport.(*Conn).writeLoop`（43.40hrs）。43.42 小时 ÷ 10000 个写协程 ≈ **15.6 秒 / 协程**，与 15s 采样窗口同量级，说明写协程全程处于「无数据可写」的空闲等待，而非相互阻塞。
- `profiles/mutex-top.txt`：总量仅 **120.83ms**（`runtime.unlock` 120.28ms / 99.55%），其中 `readLoop` 32.03ms / 26.51%、`bufio.(*Reader).Read` 28.69ms / 23.74%。该量级说明**不存在有意义的锁竞争**。

**影响范围**：吞吐上限与尾延迟。`writeBatch` 占 high 档 CPU 的 30.14%，其中每批拼接与逐次 `Write` 系统调用（`Syscall6` 34.53%）是主要构成；`DecodeInto` 16.34% 为读侧成本。

**候选方案**

- 用 `net.Buffers`（writev）批量写出，消除 `writeBatch` 中每批的拼接拷贝与多次 `Write`。
- 去掉每次写后的写 deadline 重置（对应 `runtime_pollSetDeadline` 2.02%）。
- 复用 batch slice，减少 `writeLoop` 的堆分配与 GC 压力。

**优先级**：P1，全部进入第一轮。理由是改动局限在 `writeLoop` 内部、语义可保持，且直接作用于最大热点；block/mutex 数据已排除锁竞争，说明不需要动并发结构。

## 4. 瓶颈 3：可靠链路 1000 msg/s 的数据库连接池排队与读协程阻塞

**现象**：reliable 链路在 500 msg/s 正常，升到 1000 msg/s 出现断崖式退化：超时、ACK 丢失、投递积压持续增长，进程以 `exit_code=1` 结束。

**指标**

- `baseline-1k-reliable.txt`：`exit_code=1`；`sent=41937`、`send_ack=28058`、`delivered=28125`、`business_acked=28125`、`ack_missed=13812`、`timeouts=20834`、`unexpected_frames=13977`；accept p50 **4323.400ms**、p99 **10723.803ms**、max 12841.370ms。
- 同报告 gateway stats：`deliveries_pending` 99 → 1021 → 1970 → 3050 → 4390 → 5427 → 6379，随后两次 `-1`（`count pending deliveries: context deadline exceeded`），收尾 **10031**；`db_open_conns` 全程 **16**。
- 对照 `baseline-500-reliable.txt`：`ack_missed=0`、`timeouts=0`、p50 19.068ms / p99 329.174ms、`deliveries_pending` 5–9 波动、`db_open_conns=16`。

**Profile 证据**

- `profiles/goroutine-reliable-cum.txt`（总量 2009）：`database/sql.(*DB).retry` **1001 / 49.83%**；`database/sql.(*DB).conn` **985 / 49.03%**；`gateway.(*Handler).Handle` 1000 / 49.78%；`Handler.handleSend` **576 / 28.67%**；`Store.ListEndpoints` 279 / 13.89%；`Store.AcceptMessage` 297 / 14.78%；`Handler.handleReceiptAck` **224 / 11.15%**；`Handler.handleBusinessAck` **200 / 9.96%**。
- `profiles/block-reliable-cum.txt`（总量 12.27hrs）：`database/sql.(*DB).retry` **5.83hrs / 47.49%**、`database/sql.(*DB).conn` 5.83hrs / 47.49%；`readLoop` 5.82hrs / 47.45%；`QueryContext` 3.38hrs / 27.53%、`ExecContext` 2.45hrs / 19.96%；`ListEndpoints` 1.71hrs / 13.93%、`AcceptMessage` 1.67hrs / 13.58%、`ApplyReceiptAck` 1.22hrs / 9.93%、`ApplyBusinessAck` 1.22hrs / 9.91%；写侧 `writeLoop` 6.43hrs / 52.40%。
- `profiles/cpu-reliable.txt`（`Total samples = 11.87s (79.13%)`）：`transport.(*Conn).readLoop` cum **7.06s / 59.48%**；`pgx.(*Conn).Query` cum **2.36s / 19.88%**；`Syscall6` flat **3.51s / 29.57%**；`futex` flat 1.65s / 13.90%；`pgproto3.(*Frontend).Receive` 1.04s / 8.76%。

**根因结构**

每条 SEND 在 `readLoop` 内串行完成 4 次数据库往返：`resolveRecipients` → `ListEndpoints`、`handleSend` → `AcceptMessage`、`handleReceiptAck` → `ApplyReceiptAck`、`handleBusinessAck` → `ApplyBusinessAck`；而连接池仅 **16** 个连接（`db_open_conns=16`）。1000 个读协程并发进入时，985 个阻塞在 `DB.conn` 等池，`DB.retry` 累计阻塞 5.83 小时（47.49%），导致读协程无法及时读走 socket，进而引发超时与 ACK 丢失。

**影响范围**：reliable 模式的容量天花板位于 500–1000 msg/s 之间；在此之上 `ack_missed`、`deliveries_pending`、p99 同时恶化，属于功能性失败而非单纯变慢。

**候选方案**

- 提高 `db-max-open-conns` 默认值（16 → 更大）。
- ACK 写入批处理（合并 `ApplyReceiptAck` / `ApplyBusinessAck`）。
- 把持久化移出 `readLoop`（有界队列 + worker），让读协程只做协议解析与入队。
- 减少每消息的数据库往返次数（例如把 `ListEndpoints` 结果短期缓存、合并受理与回执写入）。

**优先级**：P0，先做「提高池并发」，其余进入后续轮次。

**为什么优先做「提高池并发」这一最小改动**

- **改动最小**：只调整连接池上限（配置/初始化参数），不改协议、不改存储语义、不改协程模型，可快速验证假设并随时回滚。
- **证据直接指向连接获取而非 CPU**：985/2009 协程阻塞在 `database/sql.(*DB).conn`，`DB.retry` 占 block 总时长 47.49%，而 CPU 采样为 11.87s / 15s（79.13%），`futex` 仅 13.90%（低于 10k high 的 29.53%），不足以解释秒级延迟。
- **风险**：① PostgreSQL 的 `max_connections` 未采集，扩容前必须先确认，否则只是把排队从应用侧推到数据库侧；② 单机 Docker 中的 PG 连接数上升会带来内存与上下文切换成本；③ 若真正瓶颈是跨 `host.docker.internal` 的往返时延或单条 SQL 执行，单纯扩池无收益。
- **因此**：池扩容与「减少往返 / 移出 readLoop」分开评估，若池扩容后 `DB.retry` 阻塞占比未明显下降或 p99 未改善，按第 8 节标准回退，再进入结构性改造。

## 5. 瓶颈 4（次要）：每连接一个 time.Timer 心跳

**现象**：每个连接维护独立的 `time.Timer` 用于心跳，10k 连接即 10k 个定时器；当前成本低，但随连接数线性增长。

**指标与 Profile 证据**

- `profiles/heap-top-high.txt`：`time.NewTimer` flat **1MB / 1.72%**（10k high，总量 58.27MB）。
- `profiles/cpu-top-high.txt`：`runtime.(*timers).cleanHead` cum **0.13s / 1.14%**；`runtime.(*timer).modify` cum **0.19s / 1.67%**。timers 相关合计约 **1–2%** CPU。
- 对照 `profiles/cpu-top.txt`（10k steady）：`time.Time.absSec` 与 `time.runtimeNow` 各 0.02s / 0.5%，top 列表中未出现 `timers` 包符号。

**影响范围**：当前次要（约 1–2% CPU、1MB 堆）。50k 时定时器数量按连接数线性增长，但 **50k / 100k 档未采集 profile**，其实际开销未采集，不能据此推断。

**候选方案**：改为全局时间轮或共享 ticker 统一驱动心跳。

**优先级**：P2。需先证明两件事才可实施：① 心跳及时性不退化（各场景 p99 与 `pings_received` 不低于现有水平）；② 共享定时器不引入并发写竞争（当前 mutex profile 仅 120.83ms，任何新增锁都可能把这一优势抹掉）。

### 5.1 慢消费者与重连风暴的补充采样

为确认前四个瓶颈在异常场景下是否出现新的阻塞点，补采了两组 profile
（1000 连接、250ms 间隔、30s、`-Mode echo -Profile 1`）：

| 场景 | 客户端结果 | block top | mutex top | 结论 |
| --- | --- | --- | --- | --- |
| slow-consumer（10% 连接停读） | sent 97,657 = echoed 97,657，timeouts 0 | 5.73hrs 全部在 `writeLoop` 的 `selectgo` | 1.30s，最大项仍是 `unlock` | 1k 规模下 100 个停读连接没有形成写背压，未触发慢消费者关闭；没有新的锁竞争 |
| reconnect-storm（第 10s 断开全部并 5s 内重连） | succeeded 1000 / failed 0，retries 0，窗口内完成 | 6.51hrs 全部在 `writeLoop` 的 `selectgo` | 1.85s，`DB.ExecContext` cum 19.07%（登录） | 重连无失败、无重试；DB 只在登录时出现短暂排队，没有持续阻塞 |

原始文件：`docs/bench/raw/profile-1k-slow.txt`、`profile-1k-reconnect.txt`、
`profiles/slow/*-top.txt`、`profiles/reconnect/*-top.txt`。
说明：1000 连接、64B 载荷下内核发送缓冲足以吸收慢消费者流量，
因此本轮没有观察到写背压；更大载荷或更长时间才会触发慢消费者路径。

## 6. 候选优化排序

| 改动 | 预期收益 | 风险 | 轮次 |
|------|----------|------|------|
| `net.Buffers`（writev）替代每批拼接写 | 消除 `writeBatch` 的拼接拷贝与多次 `Write`，直击 `Syscall6` 34.53% / `writeBatch` cum 30.14% | 写错误处理与部分写语义需保持；需回归 large-payload（p99 9.547ms）与 slow-consumer | 第一轮 |
| batch slice 复用（sync.Pool） | 降低 `writeLoop` 分配（flat 2.50MB / 3.56%）与 GC 压力 | 池化对象复用错误可能造成数据串包，需严格重置长度 | 第一轮 |
| 去掉每次写后的 deadline 重置 | 减少 `runtime_pollSetDeadline`（cum 0.23s / 2.02%）等系统调用 | 写超时语义变化，慢消费者连接的关闭时机可能延后 | 第一轮 |
| DB 池 16 → 64 | 缓解 `DB.conn` 排队（985 协程）与 `DB.retry` 阻塞（47.49%），改善 reliable p50/p99 | PG `max_connections` 未采集；连接数上升带来 PG 侧内存与切换成本 | 第一轮 |
| 读缓冲 4096 → 1024 | 压缩 `bufio.NewReaderSize` 23.09MB / 32.85%，目标 **10k idle `heap_alloc_mb` 下降 ≥20%**（由 147.01MB 降至 ≤117.6MB） | 大 payload 场景读循环次数与 syscall 增加，p99 可能退化 | 第二轮 |
| `sendCh` 256 → 64 | 降低每连接队列固定分配，配合读缓冲进一步压低单连接内存 | 突发流量与慢消费者下更易阻塞或触发丢弃，需以 slow-consumer 场景验证 | 第二轮 |

## 7. 第二轮内存目标

以 `baseline-10k-idle.txt` 的 `heap_alloc_mb=147.01`（10000 连接，约 14.7KB/连接）为基准，第二轮（读缓冲 4096→1024 + sendCh 256→64）的验收目标为：**10k idle 稳定态 `heap_alloc_mb` ≤ 117.6MB，即相对基线下降 ≥20%**；同时 `rss_per_conn_kb`（基线 23.09）不得上升。该目标为验收阈值，不是实测值。

### 7.1 实测结果（回填）

| 项 | 目标 | 实测 |
| --- | --- | --- |
| 10k idle `heap_alloc_mb` | ≤117.6MB（-20%） | 129.08MB（-12.5%），**未达标** |
| 10k idle 每连接堆 | ≤11.76KB | 12.91KB |
| 10k high 活跃堆 | 不退化 | 201.72MB → 119.83MB（-40.6%） |
| 10k high 吞吐 / p99 | 速率不降、p99 ≤1.624ms | 370,630 / 45s、p99 1.377ms |
| 客户端 `rss_per_conn_kb` | ≤23.09 | 23.15（+0.06KB，测量噪声） |

最终保留的改动只有读缓冲 4096→1024：sendCh 256→64 在真实 PostgreSQL 上导致
`TestPullPagination` 报 `decode frame: EOF`（PULL 单页 100 条 DELIVER + PULL_DONE
超过 64 槽），已回退。因此空闲堆未达到 -20% 的目标；保留读缓冲的依据是
活跃堆下降 40.6%、空闲堆下降 12.5%，且吞吐与错误率没有退化。
详细判定与原始文件见 [optimization-report.md](optimization-report.md)。

## 8. 「无收益即回退」判定标准

> 以下是优化前预登记的判定标准，用于约束改动。实际执行以用户批准的 spec 为准：
> 至少两项核心指标改善 ≥20%、无正确性/吞吐回归、未达标项给出数据解释；
> 最终结论与证据见 [optimization-report.md](optimization-report.md)。
> 其中「可靠 1k 必须闭环」未达成，已作为剩余瓶颈记录，而不是用回退掩盖。

任一改动在合并前需按以下标准判定，任一条不满足即回退：

1. **正确性**：所有基线场景 `missed=0`；原基线 timeouts 为 0 的场景不得出现 timeouts；echo 场景 `unexpected_frames` 保持 0；reconnect 场景 `reconnect_failed=0` 且 `in_window=yes`。
2. **可靠链路**：`baseline-1k-reliable.txt` 的 `ack_missed` 必须降为 0，`deliveries_pending` 不得单调增长（阈值：≤1000），`exit_code` 必须为 0；200 / 500 档维持 `ack_missed=0`、`timeouts=0`。
3. **吞吐与延迟**：10k high 的速率与 p99 需同时不低于基线（8198 msg/s、1.624ms）；若仅一项改善，则该项改善幅度需 ≥3% 且另一项不退化；10k steady 的 p99 不高于基线 0.539ms。
4. **内存**：第二轮改动后 10k idle `heap_alloc_mb` 未达到 ≤117.6MB，或 `rss_per_conn_kb` 高于 23.09，则回退。
5. **幅度门槛**：吞吐提升 <3%、p99 改善 <5%、内存下降 <20% 视为「无收益」。
6. **测量可信度**：原始基线每场景仅单次运行、未采集方差；判定时需在同一环境重复 **3 次取中位数**，若改动前后差异小于重复运行的波动范围，视为无收益并回退。
