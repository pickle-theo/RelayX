# ADR-0002：心跳改用全局时间轮（每连接一个 ticker → 全进程一个 tick goroutine）

- 状态：已接受（S5）
- 日期：2026-11-17
- 关联：S1 §10（心跳语义）、S5 §4.4、`benchmark-report.md` §6.3

## 背景

S1 的心跳实现是「每连接一个 `time.Ticker` + 一个 goroutine」：

- 每条连接 1 个 runtime timer + 1 个 goroutine；
- `BenchmarkIdleConn_HeartbeatOps/ticker-per-conn`：213.4 ns、3 allocs/op；
- 10 万连接 = 10 万个 timer，全部落在 runtime 的 timer 堆上。

S5 §4.4 明确要求「心跳循环并入 Writer 或改用全局时间轮，降低协程与 timer 数量」。

## 决定

新增 `internal/heartbeat`：

- `Wheel`：`Tick=1s`、`Buckets=64`（跨度 64s），单 goroutine 驱动；条目按绝对刻度 `due` 入槽，扫到时再判到期（长周期靠多圈，不丢事件）；
- 回调在**锁外**执行，因此回调里可以再次 `After`/`Cancel`（时间轮自重排）；
- `Probe`：单连接的登记项，`Interval`/`PongTimeout` 语义与 S1 §10 完全一致（空闲 → P0 发 PING；PongTimeout 内无入站活动 → `OnTimeout` 断连）；`Stop` 幂等且取消未决条目。

## 后果

- 每连接 **0 个 goroutine、0 个 timer**（只在轮上占一个 `Entry`）；全进程 1 个 tick goroutine。
- 登记成本 213.4 → 102.6 ns（-51.9%），allocs 3 → 1（-66.7%）。
- 时间精度从「精确到纳秒的 timer」降为「1s 刻度」：对 30s 心跳 + 10s pong 超时无实质影响；**注意**：若未来需要 <1s 的心跳精度，必须调小 `Tick` 并按 §11.2 重新做协程/开销评估。
- `Probe` 的 `SendPing`/`OnTimeout` 回调在持有 `Probe.mu` 时执行，因此**必须是非阻塞操作**（实际实现只是 P0 入队与断连），否则会拖慢整轮扫描。
- `test/regression/TestNoGoroutineLeakWithGlobalWheel` 断言：200 个 Probe 只增加 ≤5 个 goroutine，`Stop` 后回到基线。

## 备选方案

| 方案 | 否决原因 |
|---|---|
| 心跳并入 Writer（S5 §4.4 的另一个选项） | Writer 阻塞在 `Write` 上时无法按时发 PING；需要额外唤醒机制，反而增加耦合 |
| 全局 `min-heap` + 单 timer | 语义等价、实现更简单，但每次增删是 O(log n) 且需要重置 timer；时间轮是 O(1) 入槽，符合「高频登记」的形态 |
| `time.AfterFunc` 全局复用 | 仍是 1 个 runtime timer/连接，没解决 timer 堆膨胀 |
