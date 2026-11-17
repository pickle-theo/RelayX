// Package benchmark 是 S5 的基准层：结构化报告模型、基准用例与 harness 的
// 解析/归档能力（LLD §2.3 BenchReport / Optimization，§3 基准清单）。
//
// 纪律（S5 §3.1）：先建基线、后优化；任何「优化」必须有 before/after 两份
// 结构与同机环境记录，否则不得写入报告。
package benchmark

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

// BenchReport 是一行基准的结构化结果（可归档、可对比、可复现）。
type BenchReport struct {
	Name        string    `json:"name"`
	NsPerOp     float64   `json:"ns_per_op"`
	BytesPerOp  int64     `json:"bytes_per_op"`
	AllocsPerOp int64     `json:"allocs_per_op"`
	Syscalls    int64     `json:"syscalls,omitempty"`
	RSSBytes    int64     `json:"rss_bytes,omitempty"`
	P50Ns       int64     `json:"p50_ns,omitempty"`
	P99Ns       int64     `json:"p99_ns,omitempty"`
	Commit      string    `json:"commit"`
	Timestamp   time.Time `json:"timestamp"`
	Env         Env       `json:"env"`
	Note        string    `json:"note,omitempty"`
	// Metrics 保存基准自定义指标（如 syscalls/frame、ns/frame、MB/s），
	// 使「每帧系统调用数」这类无法用 ns/op 表达的优化证据也能进报告。
	Metrics map[string]float64 `json:"metrics,omitempty"`
}

// Env 记录可复现所需的运行环境（S5 §7.3 环境一致性）。
type Env struct {
	GOOS       string `json:"goos"`
	GOARCH     string `json:"goarch"`
	GoVersion  string `json:"go_version"`
	NumCPU     int    `json:"num_cpu"`
	GOMAXPROCS int    `json:"gomaxprocs"`
	GOGC       string `json:"gogc"`
	Host       string `json:"host,omitempty"`
}

// CaptureEnv 采集当前进程的运行环境。
func CaptureEnv() Env {
	gogc := os.Getenv("GOGC")
	if gogc == "" {
		gogc = "default(100)"
	}
	host, _ := os.Hostname()
	return Env{
		GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GoVersion: runtime.Version(),
		NumCPU: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0),
		GOGC: gogc, Host: host,
	}
}

// CurrentCommit 返回 git commit（短哈希），取不到时返回 "unknown"。
// 报告必须能对应到具体代码版本，否则前后对比不可信（S5 §3.3）。
func CurrentCommit() string {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

// Optimization 是一项优化记录：热点、证据、前后数据、分配变化与 ADR。
type Optimization struct {
	Hotspot     string      `json:"hotspot"`
	Evidence    string      `json:"evidence"`
	Before      BenchReport `json:"before"`
	After       BenchReport `json:"after"`
	AllocsDelta float64     `json:"allocs_delta"`
	NsDelta     float64     `json:"ns_delta"`
	ADR         string      `json:"adr,omitempty"`
}

// NewOptimization 由前后两份报告计算量化改善幅度（负数表示下降）。
func NewOptimization(hotspot, evidence string, before, after BenchReport, adr string) Optimization {
	o := Optimization{Hotspot: hotspot, Evidence: evidence, Before: before, After: after, ADR: adr}
	if before.AllocsPerOp > 0 {
		o.AllocsDelta = float64(after.AllocsPerOp-before.AllocsPerOp) / float64(before.AllocsPerOp) * 100
	}
	if before.NsPerOp > 0 {
		o.NsDelta = (after.NsPerOp - before.NsPerOp) / before.NsPerOp * 100
	}
	return o
}

// Report 是一个可归档的报告文件（一次 harness 运行的产物）。
type Report struct {
	Commit        string         `json:"commit"`
	Timestamp     time.Time      `json:"timestamp"`
	Env           Env            `json:"env"`
	Label         string         `json:"label"` // baseline | after
	Benchmarks    []BenchReport  `json:"benchmarks"`
	Optimizations []Optimization `json:"optimizations,omitempty"`
}

// NewReport 创建带环境与 commit 的报告骨架。
func NewReport(label string, benches []BenchReport) Report {
	now := time.Now().UTC()
	env := CaptureEnv()
	commit := CurrentCommit()
	for i := range benches {
		benches[i].Commit = commit
		benches[i].Timestamp = now
		benches[i].Env = env
	}
	return Report{Commit: commit, Timestamp: now, Env: env, Label: label, Benchmarks: benches}
}

// WriteFile 写出 JSON 报告（缩进便于 diff 与归档审阅）。
func (r Report) WriteFile(path string) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(path, b, 0o644)
}

// ReadReport 读取 JSON 报告。
func ReadReport(path string) (Report, error) {
	var r Report
	b, err := os.ReadFile(path)
	if err != nil {
		return r, err
	}
	if err := json.Unmarshal(b, &r); err != nil {
		return r, fmt.Errorf("benchmark: parse %s: %w", path, err)
	}
	return r, nil
}

// ParseBenchOutput 解析 `go test -bench -benchmem` 的标准输出行。
// 只保留形如 `BenchmarkXxx-8   1234   567.8 ns/op   12 B/op   1 allocs/op` 的行，
// 其余输出（PASS/ok/包名）忽略，使 harness 能把文本输出转成结构化 JSON。
func ParseBenchOutput(in string) []BenchReport {
	var out []BenchReport
	sc := bufio.NewScanner(strings.NewReader(in))
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(line, "Benchmark") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		br := BenchReport{Name: fields[0], Env: CaptureEnv(), Commit: CurrentCommit(), Timestamp: time.Now().UTC()}
		ok := false
		for i := 1; i+1 < len(fields); i++ {
			v, err := strconv.ParseFloat(fields[i], 64)
			if err != nil {
				continue
			}
			switch fields[i+1] {
			case "ns/op":
				br.NsPerOp = v
				ok = true
			case "B/op":
				br.BytesPerOp = int64(v)
			case "allocs/op":
				br.AllocsPerOp = int64(v)
			default:
				// 自定义指标（ReportMetric），单位形如 syscalls/frame、MB/s。
				if strings.Contains(fields[i+1], "/") {
					if br.Metrics == nil {
						br.Metrics = make(map[string]float64, 4)
					}
					br.Metrics[fields[i+1]] = v
				}
			}
		}
		if ok {
			out = append(out, br)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// MarkdownTable 生成对比表（before → after，含 ns 与 allocs 变化百分比）。
// 报告模板统一用它，保证「优化前后对比表」不是手抄的数据。
func MarkdownTable(before, after []BenchReport) string {
	idx := make(map[string]BenchReport, len(after))
	for _, r := range after {
		idx[r.Name] = r
	}
	var b strings.Builder
	b.WriteString("| 基准 | ns/op (前) | ns/op (后) | ns 变化 | allocs/op (前) | allocs/op (后) | B/op (前) | B/op (后) |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|\n")
	seen := make(map[string]bool, len(before))
	for _, bef := range before {
		seen[bef.Name] = true
		aft, ok := idx[bef.Name]
		if !ok {
			continue
		}
		b.WriteString(fmt.Sprintf("| `%s` | %.1f | %.1f | %s | %d | %d | %d | %d |\n",
			bef.Name, bef.NsPerOp, aft.NsPerOp, fmtDelta(bef.NsPerOp, aft.NsPerOp),
			bef.AllocsPerOp, aft.AllocsPerOp, bef.BytesPerOp, aft.BytesPerOp))
	}
	for _, aft := range after {
		if seen[aft.Name] {
			continue
		}
		b.WriteString(fmt.Sprintf("| `%s` | — | %.1f | 新增 | — | %d | — | %d |\n",
			aft.Name, aft.NsPerOp, aft.AllocsPerOp, aft.BytesPerOp))
	}
	return b.String()
}

func fmtDelta(before, after float64) string {
	if before <= 0 {
		return "—"
	}
	d := (after - before) / before * 100
	return fmt.Sprintf("%+.1f%%", d)
}

// Lookup 按名称查找基准结果。
func Lookup(reports []BenchReport, name string) (BenchReport, bool) {
	for _, r := range reports {
		if r.Name == name {
			return r, true
		}
	}
	return BenchReport{}, false
}
