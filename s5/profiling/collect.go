// Package profiling 是 S5 的 Profiling 管线（LLD §3.2 / §2.2 profiling/）：
// 采集 CPU / Memory / Block / Mutex / Goroutine 五类 pprof，并把基准文本输出
// 转成结构化 JSON 报告，供 compare.go 生成前后对比表。
//
// 两种采集路径：
//  1. 进程内 HTTP pprof（Collector）：对开着 /debug/pprof 的进程在线采集，
//     文件落在 profiling/profiles/，文件名带 commit 与时间戳，便于与报告对应。
//  2. 基准二进制离线采集（RunBenchProfiles）：`go test -bench -cpuprofile ...`
//     一次性产出五类 profile，是微观优化的主要证据来源。
package profiling

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"relayx/s5/benchmark"
)

// ProfileKind 是 pprof 类型（S5 §3.2 要求五类全接）。
type ProfileKind string

const (
	KindCPU       ProfileKind = "cpu"
	KindHeap      ProfileKind = "heap"
	KindBlock     ProfileKind = "block"
	KindMutex     ProfileKind = "mutex"
	KindGoroutine ProfileKind = "goroutine"
)

// AllKinds 是采集顺序（CPU 必须最先，其他是即时快照）。
func AllKinds() []ProfileKind {
	return []ProfileKind{KindCPU, KindHeap, KindBlock, KindMutex, KindGoroutine}
}

// Collector 从运行中的进程采集 pprof。
// 目标进程必须自行注册 net/http/pprof（S5 不改协议与业务码，只在调试端口暴露）。
type Collector struct {
	BaseURL string // 例：http://127.0.0.1:9801
	Dir     string // 归档目录
	Client  *http.Client
	Commit  string
}

// NewCollector 创建采集器。
func NewCollector(baseURL, dir string) *Collector {
	if dir == "" {
		dir = filepath.Join("profiling", "profiles")
	}
	return &Collector{
		BaseURL: strings.TrimRight(baseURL, "/"),
		Dir:     dir,
		Client:  &http.Client{Timeout: 60 * time.Second},
		Commit:  benchmark.CurrentCommit(),
	}
}

// Fetch 采集一类 pprof 并写入 Dir，返回文件路径。
// cpu 需要 duration（采样时长），其余立即返回。
func (c *Collector) Fetch(ctx context.Context, kind ProfileKind, duration time.Duration) (string, error) {
	path := fmt.Sprintf("/debug/pprof/%s", kind)
	q := url.Values{}
	switch kind {
	case KindCPU:
		if duration <= 0 {
			duration = 10 * time.Second
		}
		q.Set("seconds", fmt.Sprintf("%.0f", duration.Seconds()))
	case KindGoroutine:
		// goroutine 直接用文本快照（debug=1），便于人读 goroutine 泄漏。
		q.Set("debug", "1")
	}
	u := c.BaseURL + path
	if enc := q.Encode(); enc != "" {
		u += "?" + enc
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", err
	}
	resp, err := c.Client.Do(req)
	if err != nil {
		return "", fmt.Errorf("profiling: fetch %s: %w", kind, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("profiling: fetch %s: status %d", kind, resp.StatusCode)
	}
	if err := os.MkdirAll(c.Dir, 0o755); err != nil {
		return "", err
	}
	name := fmt.Sprintf("%s-%s-%s.%s", c.Commit, time.Now().UTC().Format("20060102T150405Z"), kind, ext(kind))
	out := filepath.Join(c.Dir, name)
	f, err := os.Create(out)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	if _, err := io.Copy(f, resp.Body); err != nil {
		return "", err
	}
	return out, nil
}

// FetchAll 采集五类 pprof，任一失败立即返回错误（Profiling 缺失时不得假装有数据）。
func (c *Collector) FetchAll(ctx context.Context, cpuDuration time.Duration) ([]string, error) {
	out := make([]string, 0, len(AllKinds()))
	for _, k := range AllKinds() {
		p, err := c.Fetch(ctx, k, cpuDuration)
		if err != nil {
			return out, err
		}
		out = append(out, p)
	}
	return out, nil
}

func ext(k ProfileKind) string {
	if k == KindGoroutine {
		return "txt"
	}
	return "pprof"
}

// RunBenchProfiles 用 `go test -bench` 一次性采集五类 profile。
// 这是「基准 + Profiling 一键复跑」的实现：一次运行同时得到数字与火焰图输入。
func RunBenchProfiles(ctx context.Context, dir, pkg, benchPattern string, benchTime time.Duration) ([]string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	stamp := time.Now().UTC().Format("20060102T150405Z")
	commit := benchmark.CurrentCommit()
	base := filepath.Join(dir, fmt.Sprintf("%s-%s", commit, stamp))
	if benchPattern == "" {
		benchPattern = "Benchmark"
	}
	if benchTime <= 0 {
		benchTime = time.Second
	}
	args := []string{
		"test", "-run", "^$", "-bench", benchPattern,
		"-benchmem", "-benchtime", benchTime.String(),
		"-cpuprofile", base + "-cpu.pprof",
		"-memprofile", base + "-mem.pprof",
		"-blockprofile", base + "-block.pprof",
		"-mutexprofile", base + "-mutex.pprof",
		pkg,
	}
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("profiling: run bench profiles: %w", err)
	}
	return []string{
		base + "-cpu.pprof", base + "-mem.pprof",
		base + "-block.pprof", base + "-mutex.pprof",
	}, nil
}

// ParseBenchFile 读取 `go test -bench` 的文本输出并转成结构化报告。
// count>1 时同名基准会出现多行，这里保留全部，由调用方取中位数（见 harness.sh）。
//
// 兼容性：Windows PowerShell 的 Tee-Object 默认写 UTF-16LE，若文件带 BOM 则先转码，
// 否则解析器会看到每个字符间夹着 NUL 而读不到任何基准行（曾实测「parsed 0 benchmarks」）。
func ParseBenchFile(path string) ([]benchmark.BenchReport, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(b) >= 2 && b[0] == 0xFF && b[1] == 0xFE {
		b = utf16LEToUTF8(b[2:])
	}
	return benchmark.ParseBenchOutput(string(b)), nil
}

// utf16LEToUTF8 把 UTF-16LE 字节流转成 UTF-8（仅处理 BMP，报告场景足够）。
func utf16LEToUTF8(b []byte) []byte {
	out := make([]byte, 0, len(b)/2)
	for i := 0; i+1 < len(b); i += 2 {
		r := rune(uint16(b[i]) | uint16(b[i+1])<<8)
		switch {
		case r == '\r':
			out = append(out, '\r')
		default:
			out = append(out, []byte(string(r))...)
		}
	}
	return out
}
