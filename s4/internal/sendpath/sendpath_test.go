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

// P0 预算耗尽必须返回错误而不是静默丢弃：CloseWithReason/ReplyError 依赖
// 这个错误判断 CLOSE/ERROR 是否真的进了发送路径。
func TestControlBudgetExhaustionReportsError(t *testing.T) {
	sp := New(DefaultChannels(2, 2, 1<<10, 2, 1<<12, 2, 1<<10))
	for i := 0; i < 2; i++ {
		if err := sp.EnqueueControl([]byte("c")); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	if err := sp.EnqueueControl([]byte("overflow")); err != ErrQueueFull {
		t.Fatalf("err=%v want ErrQueueFull", err)
	}
	if got := sp.Dropped(ChControl); got != 1 {
		t.Fatalf("dropped=%d want 1", got)
	}
	sp.Close()
}

// 预算写着「不限」时队列仍是有界的：入队绝不能持 sendMu 阻塞，
// 否则 Close 与其他入队方会被一起拖死（无 Writer 消费即死锁）。
func TestUnlimitedBudgetNeverBlocksEnqueue(t *testing.T) {
	sp := New(DefaultChannels(0, 0, 0, 0, 0, 0, 0))
	for i := 0; i < 100; i++ {
		if err := sp.EnqueueControl([]byte("c")); err != nil {
			if err != ErrQueueFull {
				t.Fatalf("iter %d: unexpected err %v", i, err)
			}
			if i == 0 {
				t.Fatal("first enqueue must succeed")
			}
			sp.Close()
			return
		}
	}
	t.Fatal("unbounded budget must still hit the physical queue limit")
}
