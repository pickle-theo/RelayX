# pprof 归档

本目录存放 S5 的 pprof 归档，文件名格式：

```text
<commit>-<UTC 时间戳>-<类型>.pprof   # cpu / mem / block / mutex
<commit>-<UTC 时间戳>-goroutine.txt  # goroutine（文本快照）
```

生成方式：

```bash
./benchmark/harness.sh profiles       # Linux
.\benchmark\harness.ps1 profiles      # Windows
```

查看：

```bash
go tool pprof -top <commit>-<ts>-cpu.pprof
go tool pprof -top -alloc_space <commit>-<ts>-mem.pprof
go tool pprof -top <commit>-<ts>-block.pprof
go tool pprof -top <commit>-<ts>-mutex.pprof
```

在线采集（目标进程暴露 `/debug/pprof`）用 `profiling.Collector`：

```go
c := profiling.NewCollector("http://127.0.0.1:9801", "profiling/profiles")
paths, err := c.FetchAll(ctx, 30*time.Second) // cpu 采样 30s，其余立即返回
```

归档与报告的对应关系写在 `docs/benchmark-report.md` §8；**不要删除**已经写进报告的 profile，否则结论不可复核。
