package sendpath

import (
	"bytes"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
)

func TestImmutableFrameRefLifecycle(t *testing.T) {
	// sync.Pool 会在每次 GC 时清空私有/共享槽；本用例断言「归还后可取回同一块 buffer」，
	// 因此在用例期间关闭 GC，否则 -race 下偶发 New 被调用两次（与引用计数无关）。
	defer debug.SetGCPercent(debug.SetGCPercent(-1))

	var news atomic.Int32
	pool := &sync.Pool{New: func() any {
		news.Add(1)
		return make([]byte, 0, 128)
	}}
	buf := pool.Get().([]byte)
	buf = append(buf, []byte("shared-broadcast-frame")...)
	f := NewImmutableFrame(pool, buf)
	if got := f.Refs(); got != 1 {
		t.Fatalf("initial refs=%d want 1", got)
	}

	const subs = 5
	for i := 0; i < subs; i++ {
		f.Retain()
	}
	if got := f.Refs(); got != subs+1 {
		t.Fatalf("after retains refs=%d want %d", got, subs+1)
	}

	// 模拟 N 个 Writer 写完释放。
	for i := 0; i < subs; i++ {
		var out bytes.Buffer
		n, err := f.WriteTo(&out)
		if err != nil || n != int64(len("shared-broadcast-frame")) || out.String() != "shared-broadcast-frame" {
			t.Fatalf("write n=%d err=%v body=%q", n, err, out.String())
		}
		f.Release()
	}
	if got := f.Refs(); got != 1 {
		t.Fatalf("after writer releases refs=%d want 1 (fanout still holds)", got)
	}
	if f.Len() == 0 {
		t.Fatal("frame body must still be readable while fanout holds a reference")
	}

	// 扇出方释放最后 1 个引用 → 归零 → 归还 pool。
	f.Release()
	if got := f.Refs(); got != 0 {
		t.Fatalf("final refs=%d want 0", got)
	}
	if f.buf != nil {
		t.Fatal("buf must be set to nil after final release")
	}
	if f.Len() != 0 {
		t.Fatal("Len must be 0 after release")
	}
	var out bytes.Buffer
	n, err := f.WriteTo(&out)
	if err != nil || n != 0 || out.Len() != 0 {
		t.Fatalf("write after release n=%d err=%v len=%d", n, err, out.Len())
	}
	// pool 中应可取回同一块 buffer，且 New 未再被调用 → Put 恰好发生一次。
	reused := pool.Get().([]byte)
	if news.Load() != 1 {
		t.Fatalf("pool.New called %d times, want 1 (buffer was not returned)", news.Load())
	}
	_ = reused
}

func TestImmutableFrameDoubleReleasePanics(t *testing.T) {
	pool := &sync.Pool{New: func() any { return make([]byte, 0, 16) }}
	f := NewImmutableFrame(pool, append(pool.Get().([]byte), 'x'))
	f.Release()
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("double release must panic")
		}
	}()
	f.Release()
}

func TestImmutableFrameNilPoolPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("nil pool must panic on release")
		}
	}()
	f := &ImmutableFrame{buf: []byte("x")} // 故意绕开构造函数仿真漏设 pool
	f.refs.Store(1)
	f.Release()
}

func TestImmutableFrameNilFrameTrySendPanics(t *testing.T) {
	sp := New(DefaultChannels(4, 4, 1<<10, 4, 1<<10, 4, 1<<10))
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("nil frame must panic")
		}
	}()
	_ = sp.TrySendBroadcast(nil)
}
