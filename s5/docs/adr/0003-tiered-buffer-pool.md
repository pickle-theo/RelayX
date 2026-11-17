# ADR-0003：句柄式分级 buffer 池（含大帧保留上限）

- 状态：已接受（S5）
- 日期：2026-11-17
- 关联：S5 §4.1、§11.2、`benchmark-report.md` §6.3

## 背景

两处热路径每连接/每次都要分配缓冲：

1. 连接读缓冲：每连接 `make([]byte, 0, 65550)`（S1–S4 形态）；
2. 批量写合并缓冲：每批一次分配。

基准显示：

- `BenchmarkIdleConn_Baseline`：99.6 KB/连接、21 allocs；
- `BenchmarkMakeBuffer`：174 ns、1 alloc、640 B/op。

## 决定

新增 `internal/pool`：

- **三档分级**：4 KiB / 16 KiB / **80 KiB**（80 KiB 必须 ≥ `protocol.MaxFrameSize=65550`，否则读缓冲会落进 oversize 档、每次建连都新分配）；
- **句柄式 API**：`Get(size) *Buf` / `Put(*Buf)`。不用「直接返回 `[]byte`」是因为把切片头装箱进 `sync.Pool` 的 `interface{}` 会每次 `Put` 逃逸出一次 24B 分配（实测 1 allocs/op），达不到「池化后 allocs/op 必须下降」的验收口径；句柄从池里取、回池里还，热路径 **0 allocs/op**。
- **大帧保留上限**：`cap > MaxRetain(128KiB)` 的 buffer 归还时直接丢弃，避免 RSS 随最大 payload 单调上涨；各档另有软上限（近似，`sync.Pool` 按 P 分片无法精确计数），超限丢弃。
- **统计可观测**：`Gets/Hits/News/Puts/Drops/InUse`，长跑检查 `InUse == 0`。

## 后果

- 连接构造 99.6 KB → 25.7 KB（-74.2%），ns/op 34.3 → 14.8 µs；buffer 取还 174 → 40.5 ns（0 allocs）。
- 读缓冲复用由 `TestReaderPooledBufferReused` 断言（两次建连拿到同一底层数组），`TestGetPutDoesNotAllocate` 断言 0 allocs/op。
- **使用契约**（写进包文档）：`Buf.Slice()` 得到 len=0、cap 足够的切片；append 后若发生扩容必须 `Buf.Set(...)` 回写，否则池内保留旧数组（不泄漏，只是失去复用）；
- `Put` 之后不得再使用该 `Buf` 或它给出的切片；
- 共享只读帧（P2）仍使用 S3 冻结的 `*sync.Pool` 接口，**不合并到分级池**——S3 的 `ImmutableFrame` 构造签名是冻结产物，S5 不改语义；两者各司其职（分级池服务独占字节与读缓冲，`sync.Pool` 服务共享帧）。

## 踩过的坑（记录以免重犯）

- 最初 `DefaultLarge = 64 KiB` < `MaxFrameSize=65550`，导致读缓冲每次都进 oversize 分支；`BenchmarkReadLoop_Pooled` 出现 73976 B/op、比基线慢 6 倍。修正为 80 KiB 后恢复正常。**任何调大帧上限的改动都必须同步检查池档位。**
- 最初 `Get` 返回 `[]byte`：`BenchmarkAppendEncode` 的 allocs 始终为 1（24 B/op 的切片头装箱）。改为句柄后才真正做到 0 allocs。
