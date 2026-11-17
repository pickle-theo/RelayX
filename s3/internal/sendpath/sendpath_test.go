package sendpath

import (
	"context"
	"sync"
	"testing"
)

func TestP2BudgetExhaustedDropsWithoutDisconnect(t *testing.T) {
	sp := New(DefaultChannels(4, 4, 1<<10, 2, 1<<12, 4, 1<<10))
	pool := &sync.Pool{New: func() any { return make([]byte, 0, 64) }}
	frames := make([]*ImmutableFrame, 0, 3)
	for i := 0; i < 3; i++ {
		frames = append(frames, NewImmutableFrame(pool, append(pool.Get().([]byte), []byte("bcast")...)))
	}
	if err := sp.TrySendBroadcast(frames[0]); err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	if err := sp.TrySendBroadcast(frames[1]); err != nil {
		t.Fatalf("second enqueue: %v", err)
	}
	if err := sp.TrySendBroadcast(frames[2]); err != ErrBudgetExhausted {
		t.Fatalf("third enqueue err=%v want ErrBudgetExhausted", err)
	}
	if got := sp.Dropped(ChBroadcast); got != 1 {
		t.Fatalf("dropped=%d want 1", got)
	}
	if got := sp.Depth(ChBroadcast); got != 2 {
		t.Fatalf("depth=%d want 2", got)
	}
	// 丢弃不额外持有引用：被丢弃的帧仍只有扇出方那 1 个引用。
	if frames[2].Refs() != 1 {
		t.Fatalf("dropped frame refs=%d want 1", frames[2].Refs())
	}
	for _, f := range frames {
		f.Release()
	}
	sp.Close()
}

func TestSharedFramesReleasedOnClose(t *testing.T) {
	sp := New(DefaultChannels(4, 4, 1<<10, 4, 1<<12, 4, 1<<10))
	pool := &sync.Pool{New: func() any { return make([]byte, 0, 64) }}
	f := NewImmutableFrame(pool, append(pool.Get().([]byte), []byte("pending")...))
	if err := sp.TrySendBroadcast(f); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if got := f.Refs(); got != 2 {
		t.Fatalf("refs=%d want 2 after enqueue", got)
	}
	sp.Close() // 队列残留引用必须被释放，否则 buffer 泄漏
	if got := f.Refs(); got != 1 {
		t.Fatalf("refs=%d want 1 after close drain", got)
	}
	f.Release()
}

func TestChannelPriorityAndIsolation(t *testing.T) {
	sp := New(DefaultChannels(8, 8, 1<<20, 2, 1<<12, 8, 1<<12))
	_ = sp.EnqueueBroadcast([]byte("p2"))
	_ = sp.EnqueueControl([]byte("p0"))
	_ = sp.EnqueueReliable(context.Background(), []byte("p1"))
	first, ok := sp.Pop()
	if !ok || string(first.Raw) != "p0" {
		t.Fatalf("first=%q want p0", first.Raw)
	}
	second, _ := sp.Pop()
	if string(second.Raw) != "p1" {
		t.Fatalf("second=%q want p1", second.Raw)
	}
	third, _ := sp.Pop()
	if string(third.Raw) != "p2" {
		t.Fatalf("third=%q want p2", third.Raw)
	}
	sp.Close()
}
