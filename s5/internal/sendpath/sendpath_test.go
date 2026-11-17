package sendpath

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func testChannels() [4]Channel {
	return DefaultChannels(8, 8, 1<<20, 8, 1<<20, 8, 1<<20)
}

func newTestSendPath() *SendPath { return New(testChannels()) }

func TestPopFollowsPriority(t *testing.T) {
	sp := newTestSendPath()
	defer sp.Close()
	// 逆序入队，Pop 必须按 P0→P1→P2→P3 取出。
	if err := sp.EnqueueEvent([]byte{3}); err != nil {
		t.Fatalf("enqueue event: %v", err)
	}
	if err := sp.EnqueueBroadcast([]byte{2}); err != nil {
		t.Fatalf("enqueue broadcast: %v", err)
	}
	if err := sp.EnqueueReliable(context.Background(), []byte{1}); err != nil {
		t.Fatalf("enqueue reliable: %v", err)
	}
	if err := sp.EnqueueControl([]byte{0}); err != nil {
		t.Fatalf("enqueue control: %v", err)
	}
	want := []ChannelClass{ChControl, ChReliable, ChBroadcast, ChEvent}
	for _, cls := range want {
		it, ok := sp.Pop()
		if !ok {
			t.Fatalf("pop for %v returned empty", cls)
		}
		if it.Class() != cls {
			t.Fatalf("got class %v, want %v", it.Class(), cls)
		}
	}
	if _, ok := sp.Pop(); ok {
		t.Fatal("pop on empty sendpath returned a frame")
	}
}

func TestBroadcastBudgetDropCounts(t *testing.T) {
	sp := New(DefaultChannels(8, 8, 1<<20, 2, 1<<20, 8, 1<<20))
	defer sp.Close()
	for i := 0; i < 2; i++ {
		if err := sp.EnqueueBroadcast([]byte{byte(i)}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	// 独占字节入口（S1 兼容）在 P2 满时丢弃 + 计数，不断连、不报错。
	if err := sp.EnqueueBroadcast([]byte{9}); err != nil {
		t.Fatalf("EnqueueBroadcast on full P2: %v", err)
	}
	if got := sp.Dropped(ChBroadcast); got != 1 {
		t.Fatalf("dropped=%d, want 1", got)
	}
	// 共享帧入口（S3 扇出）在 P2 满时显式返回 ErrBudgetExhausted。
	poolStub := &sync.Pool{New: func() any { return make([]byte, 0, 8) }}
	f := NewImmutableFrame(poolStub, append(poolStub.Get().([]byte), 0xEE))
	defer f.Release()
	if err := sp.TrySendBroadcast(f); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("want ErrBudgetExhausted from TrySendBroadcast, got %v", err)
	}
	if got := sp.Dropped(ChBroadcast); got != 2 {
		t.Fatalf("dropped=%d, want 2", got)
	}
	if got := f.Refs(); got != 1 {
		t.Fatalf("refs=%d, want 1 (dropped frame must not retain)", got)
	}
}

func TestEventBudgetDropDoesNotClose(t *testing.T) {
	sp := New(DefaultChannels(8, 8, 1<<20, 8, 1<<20, 1, 1<<20))
	defer sp.Close()
	if err := sp.TryEnqueueEvent([]byte{1}); err != nil {
		t.Fatalf("first event: %v", err)
	}
	if err := sp.TryEnqueueEvent([]byte{2}); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("want ErrBudgetExhausted, got %v", err)
	}
	// P3 丢弃不影响 P1：可靠通道仍可入队。
	if err := sp.EnqueueReliable(context.Background(), []byte{3}); err != nil {
		t.Fatalf("reliable after event drop: %v", err)
	}
}

func TestReliableBackpressureTimesOut(t *testing.T) {
	sp := New(DefaultChannels(8, 1, 1<<20, 8, 1<<20, 8, 1<<20))
	defer sp.Close()
	if err := sp.EnqueueReliable(context.Background(), []byte{1}); err != nil {
		t.Fatalf("first: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	if err := sp.EnqueueReliable(ctx, []byte{2}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("want ErrQueueFull, got %v", err)
	}
}

func TestReliableBackpressureResumesAfterPop(t *testing.T) {
	sp := New(DefaultChannels(8, 1, 1<<20, 8, 1<<20, 8, 1<<20))
	defer sp.Close()
	if err := sp.EnqueueReliable(context.Background(), []byte{1}); err != nil {
		t.Fatalf("first: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		done <- sp.EnqueueReliable(context.Background(), []byte{2})
	}()
	time.Sleep(20 * time.Millisecond)
	if _, ok := sp.Pop(); !ok {
		t.Fatal("pop returned empty")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("blocked enqueue: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked enqueue did not resume after pop")
	}
}

func TestControlNeverSilentlyDropped(t *testing.T) {
	sp := New(DefaultChannels(1, 8, 1<<20, 8, 1<<20, 8, 1<<20))
	defer sp.Close()
	if err := sp.EnqueueControl([]byte{1}); err != nil {
		t.Fatalf("first control: %v", err)
	}
	if err := sp.EnqueueControl([]byte{2}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("want ErrQueueFull for full P0, got %v", err)
	}
}

func TestCloseDrainsSharedReferences(t *testing.T) {
	sp := newTestSendPath()
	poolStub := &sync.Pool{New: func() any { return make([]byte, 0, 64) }}
	buf := poolStub.Get().([]byte)
	buf = append(buf, 1, 2, 3)
	f := NewImmutableFrame(poolStub, buf)
	if err := sp.TrySendBroadcast(f); err != nil {
		t.Fatalf("try send: %v", err)
	}
	if f.Refs() != 2 {
		t.Fatalf("refs=%d after enqueue, want 2", f.Refs())
	}
	sp.Close()
	if f.Refs() != 1 {
		t.Fatalf("refs=%d after close, want 1 (queue released its ref)", f.Refs())
	}
	f.Release()
	if f.Refs() != 0 {
		t.Fatalf("refs=%d after final release, want 0", f.Refs())
	}
}

func TestEnqueueAfterCloseReturnsErrClosed(t *testing.T) {
	sp := newTestSendPath()
	sp.Close()
	if err := sp.EnqueueControl([]byte{1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("want ErrClosed, got %v", err)
	}
	if err := sp.EnqueueReliable(context.Background(), []byte{1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("want ErrClosed, got %v", err)
	}
}

func TestNextSequenceIsMonotonic(t *testing.T) {
	sp := newTestSendPath()
	defer sp.Close()
	var last uint32
	for i := 0; i < 100; i++ {
		seq, err := sp.NextSequence()
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if seq <= last {
			t.Fatalf("seq=%d after %d", seq, last)
		}
		last = seq
	}
}

// TestEnqueueReliableFastPathDoesNotAllocate 守护 S5 的内存优化：
// 队列未满时 P1 入队不得分配（原实现每次 time.NewTicker）。
func TestEnqueueReliableFastPathDoesNotAllocate(t *testing.T) {
	sp := New(DefaultChannels(8, 4096, 1<<20, 8, 1<<20, 8, 1<<20))
	defer sp.Close()
	frame := []byte{1, 2, 3}
	ctx := context.Background()
	allocs := testing.AllocsPerRun(200, func() {
		if err := sp.EnqueueReliable(ctx, frame); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		if _, ok := sp.Pop(); !ok {
			t.Fatal("pop empty")
		}
	})
	if allocs != 0 {
		t.Fatalf("allocs/op=%v, want 0", allocs)
	}
}
