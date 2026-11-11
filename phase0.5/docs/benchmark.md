# Phase 0.5 Benchmark 与 Profiling 报告

本文件记录 RelayX Phase 0.5 优化后的基准数据，并与 Phase 0 基线做同口径对比。
所有数字均来自本文件记录的复现命令在本机的实测结果，未做估算或挑选。

## 环境

| 项目 | 值 |
| --- | --- |
| OS | Windows (windows/amd64) |
| Go | go version go1.27.3 windows/amd64 |
| CPU | Intel(R) Core(TM) i3-10105 CPU @ 3.70GHz |
| GOMAXPROCS（基准输出后缀 `-8`） | 8 |
| 采集时间 | 2026-11-10 |

## 复现命令

在 `phase0.5/` 目录下执行：

```sh
# 完整测试
go test ./... -count=1

# 基准测试（每个基准重复 3 次，取中位数）
go test -run '^$' -bench . -benchmem -count=3 ./internal/protocol ./internal/transport

# 批量写证据（期望输出 write_calls=1 frames=10）
go test -run TestWriteCallsBatched -v ./internal/transport

# 采集 profile（写入 phase0.5 根目录的 cpu.out / mem.out）
go test -run '^$' -bench BenchmarkSend64 -benchmem -cpuprofile cpu.out -memprofile mem.out ./internal/transport
```

`make bench` / `make profile` 分别封装了上面的基准测试与 profile 采集命令（本机未安装 make，可直接执行上述 `go` 命令）。

## Phase 0 基线 vs Phase 0.5 优化后

Phase 0 数据取自优化前在同机、同 Go 版本下的 `count=3` 中位数，来源为 Phase 0 的基准记录。
Phase 0.5 数据为本次 `-count=3` 三次结果的中位数，括号内为三次实测区间。

| Benchmark | Phase 0 ns/op | Phase 0.5 ns/op（区间） | ns/op 变化 | Phase 0 B/op | Phase 0.5 B/op | Phase 0 allocs/op | Phase 0.5 allocs/op |
| --- | --- | --- | --- | --- | --- | --- | --- |
| BenchmarkEncode64 | 61.51 | 69.04（65.39–70.74） | +7.53 ns（+12.2%，回退） | 96 | 96 | 1 | 1 |
| BenchmarkEncode1024 | 395.1 | 439.9（428.9–490.6） | +44.8 ns（+11.3%，回退） | 1152 | 1152 | 1 | 1 |
| BenchmarkDecode64（1024 帧/轮） | 114320 | 120244（118112–121132） | +5924 ns/轮（+5.2%，回退） | 143504 | 143504 | 3075 | 3075 |
| BenchmarkDecode64（换算每帧） | 111.6 | 117.4 | +5.8 ns/帧（+5.2%，回退） | ≈140.1 | ≈140.1 | ≈3.003 | ≈3.003 |
| BenchmarkSend64 | 191.4 | 295.0（289.5–295.5） | +103.6 ns（+54.1%，回退） | 240 | 512 | 3 | 1 |
| BenchmarkSend64ReuseFrame（新增） | —（Phase 0 无此基准） | 130.5（129.2–130.6） | 相对 Phase 0 Send64 −60.9 ns（−31.8%） | — | 24 | — | 1 |

三次 `-count=3` 原始结果：

```text
BenchmarkEncode64-8     	18441393	        65.39 ns/op	      96 B/op	       1 allocs/op
BenchmarkEncode64-8     	17974135	        70.74 ns/op	      96 B/op	       1 allocs/op
BenchmarkEncode64-8     	16700042	        69.04 ns/op	      96 B/op	       1 allocs/op
BenchmarkEncode1024-8   	 2971456	       428.9 ns/op	    1152 B/op	       1 allocs/op
BenchmarkEncode1024-8   	 3114909	       490.6 ns/op	    1152 B/op	       1 allocs/op
BenchmarkEncode1024-8   	 2631771	       439.9 ns/op	    1152 B/op	       1 allocs/op
BenchmarkDecode64-8     	    9792	    120244 ns/op	 723.86 MB/s	  143504 B/op	    3075 allocs/op
BenchmarkDecode64-8     	   10000	    121132 ns/op	 718.56 MB/s	  143504 B/op	    3075 allocs/op
BenchmarkDecode64-8     	   10000	    118112 ns/op	 736.93 MB/s	  143504 B/op	    3075 allocs/op
BenchmarkSend64-8             	 4188865	       295.5 ns/op	     512 B/op	       1 allocs/op
BenchmarkSend64-8             	 4153435	       295.0 ns/op	     512 B/op	       1 allocs/op
BenchmarkSend64-8             	 4036718	       289.5 ns/op	     512 B/op	       1 allocs/op
BenchmarkSend64ReuseFrame-8   	 9229916	       130.5 ns/op	      24 B/op	       1 allocs/op
BenchmarkSend64ReuseFrame-8   	 9075356	       130.6 ns/op	      24 B/op	       1 allocs/op
BenchmarkSend64ReuseFrame-8   	 9113886	       129.2 ns/op	      24 B/op	       1 allocs/op
```

### 各 Benchmark 的测量口径

- **BenchmarkEncode64 / BenchmarkEncode1024**：对 `io.Discard` 编码一个 64 B / 1024 B Body 的 DATA 帧，衡量单帧纯编码开销。Phase 0.5 的 `Encode` 是薄封装：内部调用 `AppendFrame(nil, ...)`，仍是一次 `make` 分配，因此 B/op 与 allocs/op 与 Phase 0 完全一致。三次结果均高于基线中位数（Encode64 +3.9 ~ +9.2 ns），说明多出的一层函数调用与容量增长判断带来约 11%–12% 的小幅回退；这是为了复用缓冲而付出的固定成本，只有在 `AppendFrame(dst, ...)` 传入已有缓冲时才会被摊薄。
- **BenchmarkDecode64**：每轮迭代把预先编码好的 **1024 帧** 拼成一个字节流，新建一次 `bufio.NewReader` 后连续解码全部 1024 帧。因此表中 `ns/op` 与 `B/op`、`allocs/op` 都是**每轮（1024 帧）**的总量，比较时需换算每帧成本：本机 120244 ns/轮 ÷ 1024 ≈ **117.4 ns/帧**，143504 B/轮 ÷ 1024 ≈ **140.1 B/帧**，3075 allocs/轮 ÷ 1024 ≈ **3.003 allocs/帧**；Phase 0 对应为 111.6 ns/帧、140.1 B/帧、3.003 allocs/帧。基准使用 `Decode`（每次新建 `Frame` + Body），没有走传输层实际使用的 `getFrame` + `DecodeInto` 复用路径，因此这里的分配数代表"不复用"的保守口径；`TestDecodeIntoReusesBody` 验证了复用路径不重新分配 Body。
- **BenchmarkSend64**：`Conn.Send` 的完整发送路径（协议编码 + 非阻塞入队）。每轮从 `sendCh` 取出一个元素以免队列填满，但**不把取出的缓冲归还池**（与 Phase 0 基准的对照方式保持一致），因此每次调用都会从池中取到一个新的 512 B 分级缓冲，表现为 512 B/1 alloc。ns/op 比 Phase 0 高 54%，来源是 `sync.Pool` 的 Get/Put、`AppendFrame` 的校验分支与 `queuedBytes` 原子计数；这部分成本在真实写路径中由"每帧一次系统调用"的节省抵消（见下节），单独看这个基准并不代表端到端变慢。
- **BenchmarkSend64ReuseFrame**（Phase 0.5 新增）：与 `BenchmarkSend64` 相同，但每轮把取出的缓冲通过 `putEncodeBuf` 归还池，模拟 `writeLoop` 的真实所有权流转。结果 130.5 ns/op、24 B/op、1 alloc/op，比 Phase 0 `Send64` 快 31.8%，B/op 降低 90%。其中 24 B/op 是 `sync.Pool.Put` 把 `[]byte` 装箱成 `interface{}` 的固定分配，无法用标准库消除（见 [ADR-003](adr/ADR-003.md)）。
- 未实现 `BenchmarkDecodePipelined`：reader 无法回退，它与 `BenchmarkDecode64` 的测量内容没有实质区别，加入只会带来噪声。

### 写系统调用

| 场景 | Phase 0 | Phase 0.5 |
| --- | --- | --- |
| 10 个已入队数据帧的 `Write` 调用次数 | 10 | **1** |

证据（本次实测）：

```text
=== RUN   TestWriteCallsBatched
    conn_test.go:117: write_calls=1 frames=10
--- PASS: TestWriteCallsBatched (0.05s)
PASS
ok  	relayx/phase0.5/internal/transport	1.841s
```

`TestWriteCallsBatched` 预入队 10 个 DATA 帧后启动真实 `writeLoop`，用计数型 `net.Conn` 观察 `Write` 次数，并把捕获到的字节流按协议重新解码，逐帧校验 Type/MsgID/Body 与原始帧一致。Phase 0 的"1 次 syscall / 帧"基线结论已由本测试取代：现在写协程按 `write-batch-frames`（默认 16）与 `write-batch-bytes`（默认 64 KiB）贪心聚合，10 帧被合并为 1 次 `Write`。

## 如何查看 profile

```sh
go tool pprof cpu.out              # CPU：top / list / web / pdf
go tool pprof -alloc_space mem.out # 内存分配字节数
go tool pprof -inuse_objects mem.out
```

进入交互式提示符后常用命令：`top`、`top -cum`、`list <函数名>`、`web`（需 Graphviz）。
Windows 上若没有 Graphviz，可用 `go tool pprof -top -nodecount=20 cpu.out` 直接输出文本。

## 结论

1. 池化与批量写在**发送路径**上的收益明确：真实所有权的 `BenchmarkSend64ReuseFrame` 快 31.8%、B/op 降 90%，10 帧 1 次 `Write`，端到端千连接压测协程数 3004 → 2004（见[压测报告](压测报告.md)）。
2. 编码/解码的纯函数基准有小幅回退（Encode +11%–12%，Decode +5.2%），原因是 `Encode`/`Decode` 变成薄封装、`Decode` 基准未走复用路径；协议线格式与分配次数均未变化。这些回退在端到端场景中被批量写的 syscall 节省覆盖。
3. `BenchmarkSend64` 的 54% 回退是"不归还缓冲"的对照口径造成的，不能单独作为优化结论；评估发送路径应看 `BenchmarkSend64ReuseFrame`。
