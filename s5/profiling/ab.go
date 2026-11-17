package profiling

import (
	"fmt"
	"sort"
	"strings"

	"relayx/s5/benchmark"
)

// Pair 是一组「优化前 → 优化后」的基准配对。
//
// 为什么要显式配对：S5 的基准在同一进程内同时编译了 baseline 与优化实现，
// 一次运行即得两组数字。它们的名字不同（_Baseline/_PerFrame/_PerSubscriber），
// 无法用同名匹配做前后对比，所以把配对关系写成常量表——
// 它是报告口径的一部分，改配对必须改这里，不能手改报告。
type Pair struct {
	Name   string
	Before string
	After  string
}

// DefaultPairs 返回本阶段全部优化项的配对表（顺序即报告顺序）。
func DefaultPairs() []Pair {
	return []Pair{
		{Name: "编码（帧 → 池化追加写）", Before: "BenchmarkEncode", After: "BenchmarkAppendEncode"},
		{Name: "共享帧编码（两段式 → 追加式）", Before: "BenchmarkEncodeBroadcast_Baseline", After: "BenchmarkEncodeBroadcast_Append"},
		{Name: "写路径（逐帧 → 批量）", Before: "BenchmarkSend_PerFrame", After: "BenchmarkSend"},
		{Name: "扇出（每订阅者编码 → 共享帧）", Before: "BenchmarkFanout_PerSubscriber", After: "BenchmarkFanout_Shared"},
		{Name: "读循环（逐帧时钟 → 批级时钟）", Before: "BenchmarkReadLoop_Baseline", After: "BenchmarkReadLoop_Pooled"},
		{Name: "空闲连接构造（make+ticker → 池+时间轮）", Before: "BenchmarkIdleConn_Baseline", After: "BenchmarkIdleConn_Pooled"},
		{Name: "buffer 取还（make → 分级池）", Before: "BenchmarkMakeBuffer", After: "BenchmarkPoolGetPut"},
		{Name: "心跳登记（ticker → 时间轮）", Before: "BenchmarkIdleConn_HeartbeatOps/ticker-per-conn", After: "BenchmarkIdleConn_HeartbeatOps/wheel-after-cancel"},
	}
}

// ABTable 用同一份报告里的两组基准生成前后对比表（Markdown）。
//
// 名称里的 `-8`（GOMAXPROCS 后缀）被去掉，便于报告阅读；
// 子基准（形如 `父/subs=1000-8`）按后缀匹配，保证 1/100/1000/10000 逐个对照。
func ABTable(report benchmark.Report) string {
	return ABTableWith(report, DefaultPairs())
}

// ABTableWith 用给定配对表生成对比表。
func ABTableWith(report benchmark.Report, pairs []Pair) string {
	rows := make([]abRow, 0, 32)
	for _, p := range pairs {
		befores := matchBench(report.Benchmarks, p.Before)
		afters := matchBench(report.Benchmarks, p.After)
		if len(befores) == 0 || len(afters) == 0 {
			continue
		}
		for _, b := range befores {
			suffix := strings.TrimPrefix(normalizeName(b.Name), normalizeName(p.Before))
			var a *benchmark.BenchReport
			for i := range afters {
				if strings.TrimPrefix(normalizeName(afters[i].Name), normalizeName(p.After)) == suffix {
					a = &afters[i]
					break
				}
			}
			if a == nil {
				continue
			}
			rows = append(rows, abRow{label: p.Name, suffix: suffix, before: b, after: *a})
		}
	}
	var out strings.Builder
	out.WriteString("| 优化项 | 场景 | ns/op 前 | ns/op 后 | Δns | allocs/op 前 | allocs/op 后 | Δallocs | B/op 前 | B/op 后 | syscalls/frame 前→后 |\n")
	out.WriteString("|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, r := range rows {
		out.WriteString(fmt.Sprintf("| %s | %s | %.1f | %.1f | %s | %d | %d | %s | %d | %d | %s |\n",
			r.label, displaySuffix(r.suffix),
			r.before.NsPerOp, r.after.NsPerOp, deltaPct(r.before.NsPerOp, r.after.NsPerOp),
			r.before.AllocsPerOp, r.after.AllocsPerOp, deltaPct(float64(r.before.AllocsPerOp), float64(r.after.AllocsPerOp)),
			r.before.BytesPerOp, r.after.BytesPerOp, syscallCell(r.before, r.after)))
	}
	return out.String()
}

// syscallCell 输出「每帧系统调用数」的前后值；两方都无该指标时给「—」。
func syscallCell(before, after benchmark.BenchReport) string {
	const key = "syscalls/frame"
	bv, bok := before.Metrics[key]
	av, aok := after.Metrics[key]
	switch {
	case bok && aok:
		return fmt.Sprintf("%.4f → %.4f (%s)", bv, av, deltaPct(bv, av))
	case aok:
		return fmt.Sprintf("— → %.4f", av)
	case bok:
		return fmt.Sprintf("%.4f → —", bv)
	default:
		return "—"
	}
}

type abRow struct {
	label  string
	suffix string
	before benchmark.BenchReport
	after  benchmark.BenchReport
}

// normalizeName 去掉 GOMAXPROCS 后缀（`-8`）。
func normalizeName(name string) string {
	if i := strings.LastIndex(name, "-"); i > 0 {
		if _, err := fmt.Sscanf(name[i+1:], "%d", new(int)); err == nil {
			return name[:i]
		}
	}
	return name
}

func displaySuffix(suffix string) string {
	if suffix == "" {
		return "—"
	}
	return "`" + strings.TrimPrefix(suffix, "/") + "`"
}

func deltaPct(before, after float64) string {
	if before <= 0 {
		return "—"
	}
	return fmt.Sprintf("%+.1f%%", (after-before)/before*100)
}

// matchBench 找出所有前缀匹配的基准（含子基准），按名字排序。
func matchBench(all []benchmark.BenchReport, prefix string) []benchmark.BenchReport {
	np := normalizeName(prefix)
	var out []benchmark.BenchReport
	for _, r := range all {
		n := normalizeName(r.Name)
		if n == np || strings.HasPrefix(n, np+"/") {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
