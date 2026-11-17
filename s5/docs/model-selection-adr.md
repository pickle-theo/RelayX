# 网络模型选型结论（LLD §6）

> **决定**：S5 **不替换** Go 标准库 `net`（goroutine-per-connection + 单 Writer）。
> **依据**：先测后选（§6.1）+ 决策门槛（§6.3）+ 本阶段实测数据（§4）。
> **状态**：已接受。复审触发条件见 §6。

---

## 1. 决策原则（LLD §6.1 原文口径）

不预设某项技术更好。用**标准库优化后的数据**与**替代模型的实测收益**对比；若差距不足以抵消可维护性成本，记录「本阶段不替换」。门槛（§6.3）：

```text
若 (替代模型吞吐增益 < 20%) 或 (内存/协程收益不足以抵消引入复杂度)
则 不替换，记录 ADR：标准库在本阶段足够
```

---

## 2. 候选与评估维度

| 候选 | 连接模型 | 主要理论收益 | 主要成本 |
|---|---|---|---|
| Go `net`（基线） | 每连接 2 goroutine（读 + 写）+ 1 个全局时间轮 | 无 | goroutine 栈与调度开销；每连接读缓冲 |
| gnet | 事件循环（每核 1 goroutine）+ 用户态缓冲 | 省 goroutine/栈；批量读写更贴近 syscall | 重写连接生命周期；与 S1「单 Writer + 四通道」需要重新对齐；调试/pprof 生态变差 |
| netpoll | 同上，更底层 | 更低分配 | 同上 + API 更原始 |
| 更高效的协议编码 | 与连接模型正交 | 直接减少 CPU/分配 | 无（纯实现层） |

本阶段先做最后一行（追加式编码 + 池化 + 批量写），再判断前三行是否必要——这是门槛思想的核心：**先用便宜手段吃掉容易的收益，再决定是否付昂贵代价。**

---

## 3. S5 之后标准库的实际开销（本机实测）

| 项 | 数值 | 来源 |
|---|---|---|
| 每连接 goroutine（稳态） | **2**（读循环 + 单 Writer），心跳不占 goroutine（全局时间轮） | `internal/heartbeat` + `test/regression/TestNoGoroutineLeakWithGlobalWheel` |
| 每帧写系统调用 | **0.0627**（批 32）/ 0.25（批 8） | `BenchmarkSend` / `BenchmarkWriteBatch` |
| 每帧编码 CPU | 62.6 ns（0 allocs） | `BenchmarkAppendEncode` |
| 每帧解码 CPU | 17.1 ns（0 allocs，零拷贝） | `BenchmarkDecode` |
| 每连接内存（空闲） | **142.4 KiB**（含 64KiB 读缓冲与 GOGC 余量） | `loadtest-report.md` §3.3 |
| 广播扇出 CPU | 64.8 ns/订阅者（1000 订阅者），共享编码 2 allocs/次 | `BenchmarkFanout_Shared` |

三点结论：

1. **系统调用已不是热点**：批量写把每帧 2 次 syscall 压到 0.06 次；gnet/netpoll 的最大卖点（合并读写 syscall）在当前实现里已基本吃到。
2. **goroutine 数已收敛到 2/连接**：10 万连接 = 20 万 goroutine，Go 运行时对此量级（每栈 2–8KiB）是可控的，且 `GOGC` 默认下 GC 扫描的是栈对象而非 goroutine 本身。
3. **真正剩下的成本是内存而不是调度**：142.4KiB/连接 里 64KiB 是读缓冲、~18KiB 是四通道队列，**都可以通过配置而不是换网络模型解决**（见 `capacity-model.md` §3）。

---

## 4. 为什么不换（量化对照）

若要让替代模型"值得"，需要它带来 **≥20% 吞吐增益**或不可替代的内存收益。用本机数据做上界估计：

- 每帧总 CPU（编码 + 写 + 解码）≈ 62.6 + 175.2 + 17.1 ≈ 255 ns/frame（含假连接导致的低估）。即便 gnet 把这部分再省 20%（≈51 ns/frame），也仅相当于把 8 vCPU 的理论上限从 ~31M frame/s 提到 ~39M frame/s —— 而这个量级远高于本机实测的 ~450 msg/s（P1）+ ~100k 投递/s（P2）的真实载荷；**在真实载荷下，瓶颈是扇出与投递调度，不是 syscall 次数。**
- 内存侧：替代模型能省掉 goroutine 栈（~2–8KiB/连接），相对 142.4KiB/连接 只占 2–6%；而读缓冲（64KiB）与队列（18KiB）占比 57%。**换模型解决不了主要矛盾。**
- 可维护性：S1 冻结的「单 Writer + 四通道 + 引用计数共享帧」是在 goroutine 模型下证明过的；改为事件循环需要重新证明隔离性与引用计数闭环，并且 `-race` / pprof / 死锁检测的可用性都会下降。

因此：**替换的收益不足 20%，成本不可忽略 → 不替换。**

---

## 5. 与 LLD §6.4「明确不做」的一致性

- 不做 CPU 绑核、多 Reactor、GC 硬调：本阶段没有 pprof 证据表明 Go scheduler 或 GC 是热点（`profiling/profiles/*-cpu.pprof` 中 runtime 占比见归档；`*-mutex.pprof` 未显示 `SendPath.mu` 成为竞争点）。
- 服务端不做「连接池」：长连接即复用，与 LLD 一致。
- WebSocket 承载（§10）本阶段**未实现**：定位为可选扩展，不阻塞主线；实现时必须复用同一 Frame 编解码与能力位协商（不允许第二套协议）。这一条在 S5 代码中留了接口位置（`protocol` 包与 `loadclient` 均可复用），但没有交付代码——**明确记录为未做**。

---

## 6. 复审触发条件（何时必须重开这个决定）

任一条成立即重开：

1. Linux 上 100k 连接压测中，`pprof` 显示 `runtime.netpoll`/`runtime.schedule` 合计 > 30% CPU；
2. 单节点需要 > 200k 连接（goroutine 数与内存都无法靠配置压下去）；
3. 批量写与池化后，100k 连接下 P99 抖动仍由 GC（`runtime.gcBgMarkWorker`）主导；
4. 出现「单连接多路复用」的业务需求（那已不是本架构的假设，需要另立 ADR）。

复审时必须重跑 `benchmark/harness.sh`（Linux）+ `loadtest-report.md` §3.1 的 L4 档位，用**同一批数字**与本文对照，而不是凭印象。
