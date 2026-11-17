package sendpath

import (
	"bytes"
	"sync"
	"testing"
)

func TestImmutableFrameRefLifecycle(t *testing.T) {
	pool := &sync.Pool{New: func() any { return make([]byte, 0, 128) }}
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
	// 这里刻意不断言「Put 之后一定能 Get 回同一块 buffer」：sync.Pool 契约允许
	// 运行时在任何时刻移除条目，-race 模式更是会随机丢弃 Put 的条目（sync.poolRaceAddr），
	// 那种断言在竞态模式下必然假失败。引用计数归零且 buf 被清空（只发生在 Put 之后）
	// 已能证明归还路径被走到；pool 命中率不属于可断言的契约。
	_ = pool.Get()
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
