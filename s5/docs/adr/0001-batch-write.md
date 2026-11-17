# ADR-0001：Writer 批量写（一批一次 Deadline、合并成一次 Write）

- 状态：已接受（S5）
- 日期：2026-11-17
- 关联：S1 §4.2/§4.3（四通道双预算、单 Writer）、S5 §4.2、`benchmark-report.md` §6.2

## 背景

S1–S4 的 Writer 每帧执行一次 `SetWriteDeadline` + 一次 `Write`。profiling 与基准显示这是稳态消息路径上的固定开销：

- `BenchmarkWriteBatch/batch=1`：**syscalls/frame = 2.000**；
- 逐帧 Writer（冻结基线 `benchmark/baseline.Writer`）在 8 帧突发下 `syscalls/frame = 2.000`、ns/frame 210.3。

即：每投递一条消息，光写出就要两次系统调用，与 payload 大小无关。

## 决定

`internal/sendpath.Writer` 一次取一批帧（`MaxBatchFrames=32`、`MaxBatchBytes=64KiB`），然后：

1. **一批一次 `SetWriteDeadline`**（语义不变：对端不读仍会被超时打断）；
2. 单帧批次直接写：共享帧走 `ImmutableFrame.WriteTo`（保持 S3 零拷贝），独占字节走 `Write`；
3. 多帧批次把字节合并进池化缓冲，**一次 `Write`** 落盘；
4. 逐帧 `markWritten` 记账不变（`Written`/`Enqueued` 语义不变）。

批大小取值由 `BenchmarkWriteBatch` 标定：16 帧后 ns/frame 收益饱和（181.6 → 179.9），继续加大只会拉长 P0 控制帧在批内的等待时间。

## 后果

- `syscalls/frame` 2.000 → **0.0627（-96.9%）**，ns/frame 210.3 → 175.2。
- 多帧批次有一次内存拷贝（合并进池化缓冲）；单帧（含所有共享广播帧）不拷贝。
- `collect` 保证「单帧独占一批」，因此大帧永不付合并拷贝代价。
- **必须遵守的约束**：只有唯一 Writer 取帧与写出；通道优先级（P0→P1→P2→P3）与双预算由 `SendPath` 保持，`Writer` 只是改变写出方式。`test/regression` 的隔离与帧边界用例守护这一点。
- Writer 退出时必须释放 `pending` 与残余批次的共享帧引用（`releaseShared`），否则池化 buffer 永不归还——由 `TestWriterReleasesPendingSharedFrameOnExit` 覆盖。

## 备选方案

| 方案 | 否决原因 |
|---|---|
| `net.Buffers`（writev） | Windows 上退化为顺序写（`supportsWritev=false`），且共享帧不暴露裸切片；合并缓冲在两端都可用 |
| 后台 goroutine 攒批 | 破坏「单 Writer」原则与关闭顺序，收益不如「有帧就取满一批」 |
| 加大到 128 帧/256KiB | ns/frame 无收益，P0 延迟变差 |
