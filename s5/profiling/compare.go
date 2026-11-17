package profiling

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"relayx/s5/benchmark"
)

// Compare 生成「优化前后对比」Markdown：表格来自两份结构化 JSON，
// 不允许手写数字（S5 §3.3：基线冻结一次，后续对比只读基线文件）。
//
// 输出包含：
//   - 环境与 commit 一致性检查（不同机器/不同 commit 的对比必须显式标注）
//   - 每个基准的 ns/op、B/op、allocs/op 前后值与变化率
//   - 按 ns 改善幅度排序的 TopN（便于报告正文直接引用）
func Compare(before, after benchmark.Report) string {
	var b strings.Builder

	b.WriteString("### 对比环境\n\n")
	b.WriteString(fmt.Sprintf("- 基线：commit `%s`，%s，%s/%s，GOMAXPROCS=%d，GOGC=%s\n",
		before.Commit, before.Timestamp.Format("2006-01-02 15:04:05Z"),
		before.Env.GOOS, before.Env.GOARCH, before.Env.GOMAXPROCS, before.Env.GOGC))
	b.WriteString(fmt.Sprintf("- 优化后：commit `%s`，%s，%s/%s，GOMAXPROCS=%d，GOGC=%s\n\n",
		after.Commit, after.Timestamp.Format("2006-01-02 15:04:05Z"),
		after.Env.GOOS, after.Env.GOARCH, after.Env.GOMAXPROCS, after.Env.GOGC))
	if before.Env.GOOS != after.Env.GOOS || before.Env.GOARCH != after.Env.GOARCH ||
		before.Env.GOMAXPROCS != after.Env.GOMAXPROCS {
		b.WriteString("> ⚠️ 两次运行环境不一致，数字不可直接比较（S5 §7.3 要求固定环境）。\n\n")
	}

	b.WriteString("### 基准前后对比\n\n")
	b.WriteString(benchmark.MarkdownTable(before.Benchmarks, after.Benchmarks))

	deltas := deltas(before.Benchmarks, after.Benchmarks)
	if len(deltas) > 0 {
		b.WriteString("\n### 改善排序（按 ns/op 降幅）\n\n")
		b.WriteString("| 基准 | ns/op 降幅 | allocs/op 降幅 |\n|---|---|---|\n")
		for _, d := range deltas {
			b.WriteString(fmt.Sprintf("| `%s` | %+.1f%% | %+.1f%% |\n", d.name, d.ns, d.allocs))
		}
	}
	return b.String()
}

type delta struct {
	name   string
	ns     float64
	allocs float64
}

func deltas(before, after []benchmark.BenchReport) []delta {
	idx := make(map[string]benchmark.BenchReport, len(after))
	for _, r := range after {
		idx[r.Name] = r
	}
	var out []delta
	for _, b := range before {
		a, ok := idx[b.Name]
		if !ok || b.NsPerOp <= 0 {
			continue
		}
		d := delta{name: b.Name, ns: (a.NsPerOp - b.NsPerOp) / b.NsPerOp * 100}
		if b.AllocsPerOp > 0 {
			d.allocs = float64(a.AllocsPerOp-b.AllocsPerOp) / float64(b.AllocsPerOp) * 100
		}
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ns < out[j].ns })
	return out
}

// WriteCompare 把对比结果写入文件（报告正文引用该文件即可）。
func WriteCompare(path, md string) error {
	if err := os.MkdirAll(dirOf(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(md), 0o644)
}

func dirOf(path string) string {
	i := strings.LastIndexAny(path, `/\`)
	if i < 0 {
		return "."
	}
	return path[:i]
}

// OptimizationsTable 生成优化项清单表（Hotspot / Evidence / 前后 / ADR），
// 交给 docs/benchmark-report.md 引用，避免报告里手写「看起来变快了」。
func OptimizationsTable(ops []benchmark.Optimization) string {
	var b strings.Builder
	b.WriteString("| 热点 | 证据 | ns/op 前→后 | allocs/op 前→后 | ADR |\n|---|---|---|---|---|\n")
	for _, o := range ops {
		adr := o.ADR
		if adr == "" {
			adr = "—"
		}
		b.WriteString(fmt.Sprintf("| %s | %s | %.1f → %.1f (%+.1f%%) | %d → %d (%+.1f%%) | %s |\n",
			o.Hotspot, o.Evidence, o.Before.NsPerOp, o.After.NsPerOp, o.NsDelta,
			o.Before.AllocsPerOp, o.After.AllocsPerOp, o.AllocsDelta, adr))
	}
	return b.String()
}
