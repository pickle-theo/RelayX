package benchmark

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"testing"
	"time"
)

// TestGoroutineProfileSnapshot 归档 goroutine profile（S5 §3.2 要求的第五类 pprof）。
//
// `go test` 没有 -goroutineprofile 参数，因此用一个默认 skip 的用例承载：
//
//	S5_DUMP_GOROUTINE=1 go test -run TestGoroutineProfileSnapshot ./benchmark/
//
// 默认 skip 保证普通门禁不受影响。
func TestGoroutineProfileSnapshot(t *testing.T) {
	if os.Getenv("S5_DUMP_GOROUTINE") != "1" {
		t.Skip("set S5_DUMP_GOROUTINE=1 to archive goroutine profile")
	}
	dir := os.Getenv("S5_PROFILE_DIR")
	if dir == "" {
		dir = filepath.Join("..", "profiling", "profiles")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	name := fmt.Sprintf("%s-%s-goroutine.txt", CurrentCommit(), time.Now().UTC().Format("20060102T150405Z"))
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { _ = f.Close() }()
	if err := pprof.Lookup("goroutine").WriteTo(f, 1); err != nil {
		t.Fatalf("write goroutine profile: %v", err)
	}
	t.Logf("goroutine profile archived: %s", path)
}
