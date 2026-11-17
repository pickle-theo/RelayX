package profiling

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"relayx/s5/benchmark"
)

const sampleBenchOutput = `goos: windows
goarch: amd64
pkg: relayx/s5/benchmark
cpu: Intel(R) Core(TM) i3-10105 CPU @ 3.70GHz
BenchmarkEncode-4                1326915	       199.6 ns/op	     576 B/op	       1 allocs/op
BenchmarkAppendEncode-4          2847285	        85.69 ns/op	      24 B/op	       1 allocs/op
BenchmarkSend-4                    79855	      1403 ns/op	      32.29 syscalls/frame	 0.06457 writes/frame*1e3	       0 B/op	       0 allocs/op
BenchmarkFanout_Shared/subs=1000-4  1893	     67833 ns/op	        67.83 ns/sub	      73 B/op	       2 allocs/op
PASS
ok  	relayx/s5/benchmark	2.042s
`

func TestParseBenchOutput(t *testing.T) {
	reports := benchmark.ParseBenchOutput(sampleBenchOutput)
	if len(reports) != 4 {
		t.Fatalf("reports=%d, want 4", len(reports))
	}
	byName := map[string]benchmark.BenchReport{}
	for _, r := range reports {
		byName[r.Name] = r
	}
	if r, ok := byName["BenchmarkEncode-4"]; !ok || r.NsPerOp != 199.6 || r.BytesPerOp != 576 || r.AllocsPerOp != 1 {
		t.Fatalf("BenchmarkEncode parsed wrong: %+v", r)
	}
	// 自定义指标（含 /）必须被采集，否则「系统调用下降」无法进报告。
	send := byName["BenchmarkSend-4"]
	if got := send.Metrics["syscalls/frame"]; got != 32.29 {
		t.Fatalf("syscalls/frame=%v, want 32.29", got)
	}
	fan := byName["BenchmarkFanout_Shared/subs=1000-4"]
	if got := fan.Metrics["ns/sub"]; got != 67.83 {
		t.Fatalf("ns/sub=%v", got)
	}
}

func TestParseBenchFileHandlesUTF16(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "utf16.txt")
	// PowerShell Tee-Object 默认 UTF-16LE + BOM：解析器必须能读。
	var buf []byte
	buf = append(buf, 0xFF, 0xFE)
	for _, r := range sampleBenchOutput {
		buf = append(buf, byte(r), byte(uint16(r)>>8))
	}
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
	reports, err := ParseBenchFile(path)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(reports) != 4 {
		t.Fatalf("utf16 reports=%d, want 4", len(reports))
	}
}

func TestABTablePairsBeforeAndAfter(t *testing.T) {
	now := time.Now().UTC()
	rep := benchmark.Report{
		Commit: "test", Timestamp: now,
		Benchmarks: []benchmark.BenchReport{
			{Name: "BenchmarkEncode-4", NsPerOp: 200, AllocsPerOp: 1, BytesPerOp: 576},
			{Name: "BenchmarkAppendEncode-4", NsPerOp: 60, AllocsPerOp: 0, BytesPerOp: 0},
			{Name: "BenchmarkSend_PerFrame-4", NsPerOp: 1683, Metrics: map[string]float64{"syscalls/frame": 2}},
			{Name: "BenchmarkSend-4", NsPerOp: 1402, Metrics: map[string]float64{"syscalls/frame": 0.0627}},
		},
	}
	md := ABTable(rep)
	if !strings.Contains(md, "编码（帧 → 池化追加写）") {
		t.Fatalf("missing encode pair row:\n%s", md)
	}
	if !strings.Contains(md, "-70.0%") || !strings.Contains(md, "-100.0%") {
		t.Fatalf("deltas missing:\n%s", md)
	}
	if !strings.Contains(md, "2.0000 → 0.0627") {
		t.Fatalf("syscall delta missing:\n%s", md)
	}
	// 未出现的配对不得产生空行。
	if strings.Contains(md, "BenchmarkFanout") {
		t.Fatalf("unexpected fanout row:\n%s", md)
	}
}

func TestABTableMatchesSubBenchmarks(t *testing.T) {
	rep := benchmark.Report{Benchmarks: []benchmark.BenchReport{
		{Name: "BenchmarkFanout_PerSubscriber/subs=100-4", NsPerOp: 34432, AllocsPerOp: 500},
		{Name: "BenchmarkFanout_PerSubscriber/subs=1000-4", NsPerOp: 395172, AllocsPerOp: 5000},
		{Name: "BenchmarkFanout_Shared/subs=100-4", NsPerOp: 6551, AllocsPerOp: 2},
		{Name: "BenchmarkFanout_Shared/subs=1000-4", NsPerOp: 64798, AllocsPerOp: 2},
	}}
	md := ABTable(rep)
	if strings.Count(md, "扇出（每订阅者编码 → 共享帧）") != 2 {
		t.Fatalf("expected two fanout rows:\n%s", md)
	}
	if !strings.Contains(md, "`subs=100`") || !strings.Contains(md, "`subs=1000`") {
		t.Fatalf("suffix matching failed:\n%s", md)
	}
}

func TestCompareWarnsOnEnvMismatch(t *testing.T) {
	before := benchmark.Report{
		Commit: "a", Env: benchmark.Env{GOOS: "windows", GOMAXPROCS: 4},
		Benchmarks: []benchmark.BenchReport{{Name: "X", NsPerOp: 100}},
	}
	after := benchmark.Report{
		Commit: "b", Env: benchmark.Env{GOOS: "linux", GOMAXPROCS: 8},
		Benchmarks: []benchmark.BenchReport{{Name: "X", NsPerOp: 80}},
	}
	md := Compare(before, after)
	if !strings.Contains(md, "环境不一致") {
		t.Fatalf("missing env mismatch warning:\n%s", md)
	}
	if !strings.Contains(md, "-20.0%") {
		t.Fatalf("missing delta:\n%s", md)
	}
}

func TestWriteCompareCreatesDirs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "compare.md")
	if err := WriteCompare(path, "hello"); err != nil {
		t.Fatalf("write: %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil || string(b) != "hello" {
		t.Fatalf("read back: %q err=%v", string(b), err)
	}
}
