# S5 基准与 Profiling 报告

> 对应 LLD：`docs/S5 单机性能工程 - 详细设计 LLD.md` §3（基线）、§4（分层优化）、§12（验收）。
>
> 本报告的**全部数字来自可复跑产物**，不是手抄估计：
> - 结构化数据：`s5/profiling/reports/after.json`（33 条基准，`-count=3` 取中位数）
> - A/B 对照表：`s5/profiling/reports/ab.md`（由 `report -mode=ab` 生成，本文直接引用）
> - pprof 归档：`s5/profiling/profiles/`
>
> 报告生成命令：`./benchmark/harness.ps1 after`（Windows）/ `./benchmark/harness.sh after`（Linux）→ `./benchmark/harness.ps1 ab`。

---

## 1. 结论摘要

四项优化、四个由数据证明的瓶颈，全部通过同一进程内的 A/B 对照量化（基线实现冻结在 `s5/benchmark/baseline`，不可改写）：

| # | 瓶颈（数据证明） | 优化 | 效果（中位数） |
|---|---|---|---|
| 1 | 两段式编码：先分配 Body、再整帧拷贝 | 追加式编码（`protocol.AppendFrame/AppendBroadcast/...`）直接写进池化缓冲 | **177.1 → 62.6 ns/op（-64.6%）**，**1 → 0 allocs/op**，576 → 0 B/op |
| 2 | 每帧一次 `SetWriteDeadline` + 一次 `Write` | 批量写：一批一次 deadline、合并成一次 Write（批 ≤32 帧 / ≤64KiB） | **syscalls/frame 2.000 → 0.0627（-96.9%）**，ns/frame 210.3 → 175.2（-16.7%） |
| 3 | 每连接各自 `make([]byte, 64KiB+)` + 每连接一个 ticker | 分级 buffer 池（句柄式，0 allocs）+ 全局时间轮 | 连接构造 **34.3 → 14.8 µs（-57.0%）**，**99.6KB → 25.7KB（-74.2%）**；心跳登记 213.4 → 102.6 ns（-51.9%） |
| 4 | 广播扇出每订阅者各编码一次 | 一次编码 + 引用计数共享（承接 S3 语义，S5 只优化编码实现） | 1000 订阅者：**395.2 → 64.8 µs（-83.6%）**，alloc 5000 → 2（-99.96%）；10000 订阅者：3997 → 727 µs，alloc 50000 → 2 |

附带收益：读循环 40.03 → 28.77 ns/frame（-28.1%），读 Deadline 刷新从 1.016 次/帧降到 0.0156 次/帧；buffer 取还 174 → 40.5 ns/op（-76.7%，1 → 0 allocs）。

**没有一项优化改动语义**：帧格式、状态机、幂等键、四通道预算与优先级全部未变，`test/regression` 用冻结表与逐字节等价用例守住这一点。

---

## 2. 方法与纪律

- **同进程 A/B**：基线实现（`s5/benchmark/baseline`）与优化实现编入同一个测试二进制，一次运行同时产出两组数字。这比「两次独立运行对比」更强：机器状态、GC 相位、编译选项完全一致。
- **中位数**：`-count=3` 后取中位（`profiling/cmd/report` 的 `medianBySuffix`），不挑最好的一次。
- **基线冻结**：`profiling/reports/*.json` 一旦生成即只读；脚本默认拒绝覆盖（`FORCE=1` 才能重写）。
- **归因规则**：只有 pprof 指出的热点才动（LLD §3.2）。本轮四类热点的 pprof 见 §6。
- **不测就不改**：`BenchmarkFactsWrite` 只测 CPU 侧（event_id 派生 + 行组装 + 幂等插入），不冒充数据库往返；DB 侧留给 S2 契约测试与 S8 容量报告。

---

## 3. 环境（可复现所需）

| 项 | 值 |
|---|---|
| commit | `a8cdcc3` |
| OS / Arch | windows / amd64 |
| CPU | Intel Core i3-10105 @ 3.70GHz（4 核 8 线程，`runtime.NumCPU()=8`） |
| Go | go1.27.3 |
| GOMAXPROCS | 4（基准固定，见 `harness.ps1` / `harness.sh`） |
| GOGC | 100（默认） |
| 基准时长 | `-benchtime 300ms`，`-count 3` |

> LLD §4.5 要求基准在 Linux 容器执行（避免 IOCP/epoll 数据混用）。本机只有 Windows，因此：
> - **相对收益（-64.6%、-96.9% 等）可跨平台外推**：它衡量的是「分配次数/系统调用次数」这类与平台无关的量；
> - **绝对 ns/op 不可外推**：Linux 下 syscall 成本更低，批量写与池化的绝对收益会缩小，但方向不变；
> - 亚毫秒延迟结论需在 Linux 复跑（本机 `time.Now()` 粒度为 0.5ms 级，见 `loadtest-report.md` §5）。

---

## 4. 基准清单与观测口径

| 基准 | 观测 | 口径说明 |
|---|---|---|
| `BenchmarkEncode` / `BenchmarkAppendEncode` | ns/op、B/op、allocs/op | 512B Body；前者含 Body 分配与整帧拷贝 |
| `BenchmarkEncodeBroadcast_Baseline` / `_Append` | ns/op、allocs/op | 256B payload 的 P2 共享帧编码 |
| `BenchmarkDecode` / `BenchmarkDecodeBatch` | ns/op、alloc、MB/s | 解码零拷贝（Body 引用输入缓冲） |
| `BenchmarkSend_PerFrame` / `BenchmarkSend` | ns/frame、syscalls/frame | **1 op = 8 帧突发**；ns/op 是「8 帧」的耗时，报告按 ns/frame 比较 |
| `BenchmarkWriteBatch/batch=N` | syscalls/frame | 批大小标定（§7） |
| `BenchmarkFanout_PerSubscriber` / `_Shared` | ns/sub、allocs/op | 1/100/1000/10000 订阅者，P2 队列满即丢弃 |
| `BenchmarkReadLoop_Baseline` / `_Pooled` | ns/frame、timecalls/frame、deadlines/frame | 64 帧一次读入 |
| `BenchmarkIdleConn_Baseline` / `_Pooled` | ns/op、B/op、allocs/op | 连接对象构造（S1 缺省预算，不含 OS socket） |
| `BenchmarkIdleConn_HeartbeatOps/*` | ns/op、allocs/op | 每连接 ticker vs 全局时间轮登记/取消 |
| `BenchmarkFactsWrite` | ns/op、allocs/op | S2 事实写入的 CPU 侧（不含 DB 往返） |
| `BenchmarkPoolGetPut` / `BenchmarkMakeBuffer` | ns/op、allocs/op | 分级池 vs `make` |

---

## 5. 优化前后对比（A/B）

> 下表由 `go run ./profiling/cmd/report -mode=ab -in profiling/reports/after.json` 生成（`profiling/reports/ab.md`），此处原样引用。

| 优化项 | 场景 | ns/op 前 | ns/op 后 | Δns | allocs/op 前 | allocs/op 后 | Δallocs | B/op 前 | B/op 后 | syscalls/frame 前→后 |
|---|---|---|---|---|---|---|---|---|---|---|
| 编码（帧 → 池化追加写） | — | 177.1 | 62.6 | -64.6% | 1 | 0 | -100.0% | 576 | 0 | — |
| 共享帧编码（两段式 → 追加式） | — | 258.4 | 125.0 | -51.6% | 6 | 2 | -66.7% | 419 | 72 | — |
| 写路径（逐帧 → 批量） | — | 1683.0 | 1402.0 | -16.7% | 0 | 0 | — | 35 | 2 | 2.0000 → 0.0627 (-96.9%) |
| 扇出（每订阅者编码 → 共享帧） | `subs=1` | 374.8 | 194.9 | -48.0% | 5 | 2 | -60.0% | 664 | 72 | — |
| 扇出（每订阅者编码 → 共享帧） | `subs=100` | 34432.0 | 6551.0 | -81.0% | 500 | 2 | -99.6% | 66400 | 72 | — |
| 扇出（每订阅者编码 → 共享帧） | `subs=1000` | 395172.0 | 64798.0 | -83.6% | 5000 | 2 | -100.0% | 664002 | 72 | — |
| 扇出（每订阅者编码 → 共享帧） | `subs=10000` | 3997300.0 | 726556.0 | -81.8% | 50000 | 2 | -100.0% | 6640017 | 74 | — |
| 读循环（逐帧时钟 → 批级时钟） | — | 2569.0 | 1761.0 | -31.5% | 0 | 2 | — | 0 | 226 | — |
| 空闲连接构造（make+ticker → 池+时间轮） | — | 34312.0 | 14750.0 | -57.0% | 21 | 18 | -14.3% | 99624 | 25731 | — |
| buffer 取还（make → 分级池） | — | 174.0 | 40.5 | -76.7% | 1 | 0 | -100.0% | 640 | 0 | — |
| 心跳登记（ticker → 时间轮） | — | 213.4 | 102.6 | -51.9% | 3 | 1 | -66.7% | 248 | 73 | — |

读表注意：

- **读循环的 `2 allocs/op`**：一行基准 = 一条连接的完整读生命周期（含 `readloop.Reader` 结构体与回调闭包），因此这 2 次分配是**每连接一次**、与帧数无关；同列的 `B/op 226` 同理。基线的 `0 allocs` 是因为它没有池化句柄，而它每连接要 `make` 一块 64KiB+ 的读缓冲（由 `BenchmarkIdleConn_*` 量化）。
- **扇出的 allocs 恒定 2**：一次共享编码（payload 拷贝 + `ImmutableFrame` 结构体）与订阅者数无关，这正是 S3「一次编码、N 个订阅者共享」的量化证据。
- **写路径 ns/op 是 8 帧突发的耗时**；按帧算是 210.3 → 175.2 ns/frame。批量写的主要收益在系统调用次数（-96.9%），ns 收益受本机假连接（`Write` 只是原子加）限制：真实 socket 上 `Write` 成本远高于此处，收益会更大。

---

## 6. 四个由数据证明的瓶颈

### 6.1 编码：Body 分配 + 整帧二次拷贝

- 证据：`BenchmarkEncode` 576 B/op、1 allocs/op；`BenchmarkEncodeBroadcast_Baseline` 419 B/op、6 allocs/op（256B payload）。
- pprof：`profiling/profiles/a8cdcc3-20261117T054119Z-mem.pprof`。
- 处理：追加式编码（ADR-0004），把 header + Body 一次写进池化缓冲，省掉中间 Body 与一次 payload 拷贝；`protocol.AppendXxx` 与 `Encode(EncodeXxx(...))` 逐字节等价由 `protocol/append_encode_test.go` 与 `test/regression/frozen_test.go` 双重守护。

### 6.2 写路径：每帧两次系统调用

- 证据：`BenchmarkWriteBatch/batch=1` 的 `syscalls/frame=2.000`（一次 `SetWriteDeadline` + 一次 `Write`）；批 32 时 `0.0628`。
- 处理：批量写（ADR-0001）。单帧仍走直接写（共享帧保持 S3 零拷贝，不经合并缓冲）；多帧合并进池化缓冲，一次 `Write`。**四通道优先级与双预算语义完全不变**：Writer 仍按 P0→P1→P2→P3 取帧，预算仍由 `SendPath` 在入队时独立扣减。

### 6.3 连接对象：每连接 64KiB 读缓冲 + 一个 ticker

- 证据：`BenchmarkIdleConn_Baseline` 99624 B/op、21 allocs/op（S1 缺省预算）；`BenchmarkIdleConn_HeartbeatOps/ticker-per-conn` 248 B/op、3 allocs/op。
- 处理：分级 buffer 池（ADR-0003）+ 全局时间轮（ADR-0002）。连接构造 99.6KB → 25.7KB，心跳登记成本减半，且 N 条连接只占 1 个 tick goroutine（`test/regression` 用 200 个 Probe 验证协程数偏差 ≤5）。

### 6.4 广播扇出：每订阅者重复编码

- 证据：`BenchmarkFanout_PerSubscriber` 在 1000 订阅者时 395µs、5000 allocs/op、664KB/op，**随订阅者数线性放大**；`BenchmarkFanout_Shared` 恒定 2 allocs/op。
- 处理：S3 的共享帧语义不变，S5 只把「一次编码」的实现换成追加式（§6.1），并核实引用计数闭环（`TestSendPathCloseReleasesSharedRefs`）。

---

## 7. 批大小标定（`BenchmarkWriteBatch`）

| 批大小 | ns/frame | syscalls/frame |
|---|---|---|
| 1（逐帧） | 231.0 | 2.0000 |
| 4 | 203.2 | 0.5000 |
| 8 | 197.0 | 0.2500 |
| 16 | 181.6 | 0.1250 |
| 32 | 179.9 | 0.0625 |

结论：**缺省 32 帧 / 64KiB**（`sendpath.DefaultBatchFrames/DefaultBatchBytes`）。收益在 16 帧后饱和（ns/frame 只差 1%），继续加大批只会拉长 P0 控制帧在批内的等待，损害 §5 的隔离指标。

---

## 8. Profiling 归档

五类 pprof 均由 `harness profiles` 产出（CPU/Heap/Block/Mutex 来自 `go test -cpuprofile/-memprofile/-blockprofile/-mutexprofile`，goroutine 来自 `TestGoroutineProfileSnapshot`）：

```text
profiling/profiles/a8cdcc3-20261117T054119Z-cpu.pprof
profiling/profiles/a8cdcc3-20261117T054119Z-mem.pprof
profiling/profiles/a8cdcc3-20261117T054119Z-block.pprof
profiling/profiles/a8cdcc3-20261117T054119Z-mutex.pprof
profiling/profiles/a8cdcc3-20261117T054155Z-goroutine.txt
```

查看方式：`go tool pprof -top s5/profiling/profiles/a8cdcc3-...-cpu.pprof`。
对运行中的进程也可在线采集（`profiling.Collector`，要求目标进程暴露 `/debug/pprof`）：`Collector.FetchAll` 一次取五类。

---

## 9. 一键复跑

```bash
# Linux / CI
cd s5
./benchmark/harness.sh after          # 跑基准 + 生成 profiling/reports/after.json
./benchmark/harness.sh ab             # 生成 A/B 表
./benchmark/harness.sh profiles       # 采集五类 pprof
```

```powershell
# Windows（本报告就是这条命令产出的）
cd s5
.\benchmark\harness.ps1 after
.\benchmark\harness.ps1 ab
.\benchmark\harness.ps1 profiles
```

固定项的覆盖方式：`GOMAXPROCS`/`GOGC`/`BENCH_PATTERN`/`BENCHTIME`/`COUNT` 环境变量；基线文件默认不可覆盖（`FORCE=1` 强制）。

---

## 10. S5 验收对照（LLD §12.1）

| 验收项 | 状态 | 证据 |
|---|---|---|
| 一键 Benchmark 与 Profiling 流程可复现、报告可归档 | **达成** | `benchmark/harness.sh`、`benchmark/harness.ps1`；`profiling/reports/*.json`、`profiling/profiles/*` |
| 至少三个数据证明的真实瓶颈 | **达成（4 个）** | §6（编码、写路径、连接对象、扇出） |
| 至少两轮优化有前后对比数据 | **达成（4 轮）** | §5 A/B 表 |
| 每消息内存分配与系统调用数量量化下降 | **达成** | allocs 1 → 0（编码）；syscalls/frame 2.000 → 0.0627 |
| 空闲连接内存与协程数量达到目标 | **部分达成** | 协程：2/连接 + 1/进程（目标 ≤2，达成，`TestNoGoroutineLeakWithGlobalWheel`）；内存：构造 99.6KB → 25.7KB（-74%），但**实测 142.4KiB/连接**（含 64KiB 读缓冲与 GC 余量），未达 96KiB 目标 → 需配置调优（`capacity-model.md` §3），不属于实现层遗留 |
| 广播扇出与可靠消息并发时 P99 互不显著影响 | **本机未通过** | `loadtest-report.md` §4.2：P2 7.5 万投递/s 时 `send_ack` P99 +85%、PING P99 +508%。根因是共享 CPU 与 S4 内核的 200ms 投递扫描，不是队列隔离失效（机制级用例全绿）。需两机/Linux 复跑后再判定 |
| `go test -race ./...` 通过，协程泄漏偏差 ≤5 | **达成** | `go test -race ./...` 全绿；`TestNoGoroutineLeakAfterWriters` / `TestNoGoroutineLeakWithGlobalWheel` 偏差 ≤5 |
| S1–S4 功能与协议兼容性不被破坏 | **达成** | `test/regression/frozen_test.go`（帧号/能力位/原因码/Body 布局/追加编码逐字节等价）；`test/regression/loadclient_test.go`（登录→订阅→发送→投递→回执→PING 全链路） |

**未通过项的处置**：§4.2 的端到端隔离必须在具备独立压测机的环境复跑；在复跑结果出来之前，不得把「四通道隔离已验收」写进任何上线材料。这一条的失败**不是**通过放宽阈值来消除的。

---

## 11. 已知限制（不许含糊）

1. **平台**：Windows 上 `Write` 不走 `writev`（Go 的 `net.Buffers` 在 Windows 退化为顺序写），因此批量写在本机只合并成「一次 `Write` 大缓冲」，不做 scatter/gather。Linux 上仍是一次 `writev`，语义更优。
2. **假连接**：`benchConn.Write` 只做原子计数，不触发真实 syscall。因此 `syscalls/frame` 是**调用次数**的精确度量，而 ns 收益被低估。
3. **无数据库**：`BenchmarkFactsWrite` 只覆盖 CPU 侧；DB 事务/索引成本以 S2 契约测试与 S8 容量报告为准（见 `capacity-model.md` §4）。
4. **时钟粒度**：本机 `time.Now()` 粒度约 0.5ms，压测里的亚毫秒分段（编码、环回 PING）会被量化到 0；微基准不受影响（`testing` 用 `runtime.nanotime`）。
5. **单机单进程**：全部基准不含网络栈排队、不含多租户并发干扰；这些属于 §7 系统级压测（见 `loadtest-report.md`）。
