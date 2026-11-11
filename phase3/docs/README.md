# Phase 3 文档索引

Phase 3 的目标是在 Phase 2 可靠投递的基础上回答：**单机网关能承载多少连接、
瓶颈在哪里、两轮优化是否真的有效、是否应该换网络库**。所有结论都来自
`docs/bench/raw/` 的原始压测报告，环境与方法见 [ADR-010](adr/ADR-010.md)。

## 阅读顺序

1. [baseline-report.md](baseline-report.md)：优化前的 1k/10k 场景矩阵、50k/100k
   连接尝试、200/500/1000 msg/s 可靠链路基线。
2. [bottleneck-analysis.md](bottleneck-analysis.md)：基于 pprof 的四个瓶颈
   （每连接内存、写/读系统调用与调度、可靠链路数据库排队、心跳定时器），
   含证据、影响与候选方案排序。
3. [optimization-report.md](optimization-report.md)：两轮优化的假设、改动、
   同参数复测与判定。
4. [capacity-report.md](capacity-report.md)：连接上限、资源账本与扩容前置条件。
5. ADR：
   - [ADR-010](adr/ADR-010.md)：性能工程与压测方法（容器、场景矩阵、pprof、口径）。
   - [ADR-011](adr/ADR-011.md)：writev、批次复用、deadline、队列/读缓冲、数据库池。
   - [ADR-012](adr/ADR-012.md)：标准库 `net` vs gnet 的实验与选型。

对照实验：[experiments/netlib-compare/ROUND-RESULTS.md](../experiments/netlib-compare/ROUND-RESULTS.md)。

## 关键结论

| 指标 | 基线 | 两轮优化后 |
| --- | --- | --- |
| 10k 空闲每连接堆 | 14.75 KB | 12.91 KB（-12.5%） |
| 10k 高频活跃堆 | 201.72 MB | 119.83 MB（-40.6%） |
| 10k 高频吞吐 | 370,807 / 45s | 370,630 / 45s（-0.05%） |
| 可靠 1k p50 / ack_missed | 4323 ms / 13,812 | 1726 ms / 1,916（-60% / -86%） |
| 单容器最大并发 | 28,232 | 28,224（临时端口上限） |
| 28k 连接网关堆 | 455 MB | 见 capacity-report.md |

## 如何复现

```powershell
# 全部测试（DB 用例在未设置 RELAYX_TEST_DSN 时跳过）
go test ./...

# 10k 高频 echo + profiling
powershell -ExecutionPolicy Bypass -File bench\run.ps1 `
  -Conns 10000 -Duration 45s -Scenario steady-high -Interval 1s `
  -Ramp 15s -Timeout 5s -Mode echo -Profile 1 `
  -Out docs/bench/raw/opt2-10k-echo-high.txt

# 可靠链路（先清库）
docker exec relayx-phase2-pg psql -U relayx -d relayx `
  -c "TRUNCATE deliveries, messages, endpoints RESTART IDENTITY CASCADE"
powershell -ExecutionPolicy Bypass -File bench\run.ps1 `
  -Conns 1000 -Duration 40s -Scenario steady-low -Interval 1s `
  -Mode reliable -Ack both -Timeout 10s `
  -Out docs/bench/raw/opt1-1k-reliable.txt
```

`Makefile` 也提供 `bench-1k`、`bench-10k`、`bench-50k`、`bench-profile`。

## 数据目录说明

- `bench/raw/baseline-*`：Phase 2 代码在容器中的基线。
- `bench/raw/opt1-*`：第一轮优化后（writev、批次复用、DB 池 64）。
- `bench/raw/opt2-*`：第二轮优化后（读缓冲 1KB；发送队列缩容已按集成测试回退）。
- `bench/raw/profile-*`：带 pprof 的专门运行。
- `bench/raw/profiles/`：pprof 原始 proto 与 `-top` 文本。
- `bench/raw/netlib/`：stdlib vs gnet 的对比报告与服务端资源采样。

`opt1-10k-high.txt` 是一次**可靠模式**的 10k 连接运行（模式参数误用），
不用于 echo 前后对比；它记录了 10k msg/s 可靠链路的崩溃边界，可作为压力上限参考。
