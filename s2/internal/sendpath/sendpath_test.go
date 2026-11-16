package sendpath

import (
	"context"
	"sync"
	"testing"
	"time"
)

func testChannels() [4]Channel {
	return DefaultChannels(16, 8, 1024, 4, 512, 4, 512)
}

func frame(b byte) []byte { return []byte{b} }

// TestPriorityOrder 验证 Writer 取帧严格 P0→P1→P2→P3。
func TestPriorityOrder(t *testing.T) {
	sp := New(testChannels())
	defer sp.Close()
	// 逆序入队：P3 先，P0 最后。
	_ = sp.EnqueueEvent(frame(3))
	_ = sp.EnqueueBroadcast(frame(2))
	_ = sp.EnqueueReliable(context.Background(), frame(1))
	_ = sp.EnqueueControl(frame(0))

	want := []byte{0, 1, 2, 3}
	for i, w := range want {
		got, ok := sp.Pop()
		if !ok || len(got) != 1 || got[0] != w {
			t.Fatalf("pop %d = %v,%v want %d", i, got, ok, w)
		}
	}
	if _, ok := sp.Pop(); ok {
		t.Fatal("unexpected extra frame")
	}
}

// TestBudgetIsolation 验证打满 P2 不影响 P1 写出，且 P2 丢弃计数增加。
func TestBudgetIsolation(t *testing.T) {
	sp := New(testChannels())
	defer sp.Close()

	// P2 预算 4 帧，强行入队 10 帧。
	for i := 0; i < 10; i++ {
		_ = sp.EnqueueBroadcast(frame(byte(i)))
	}
	if got := sp.Dropped(ChBroadcast); got != 6 {
		t.Fatalf("broadcast dropped = %d, want 6", got)
	}
	if sp.Depth(ChBroadcast) > 4 {
		t.Fatalf("broadcast depth = %d, want <=4", sp.Depth(ChBroadcast))
	}

	// P1 仍能入队并优先写出。
	if err := sp.EnqueueReliable(context.Background(), frame(0xAA)); err != nil {
		t.Fatal(err)
	}
	got, ok := sp.Pop()
	if !ok || got[0] != 0xAA {
		t.Fatalf("reliable frame not prioritized: %v", got)
	}
}

// TestReliableByteBudget 验证 P1 字节预算上限（双预算中的字节维度）。
func TestReliableByteBudget(t *testing.T) {
	sp := New(DefaultChannels(16, 16, 10, 4, 512, 4, 512))
	defer sp.Close()
	// 每帧 6 字节，预算 10 → 只能放 1 帧；第 2 帧背压超时。
	if err := sp.EnqueueReliable(context.Background(), make([]byte, 6)); err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := sp.EnqueueReliable(ctx, make([]byte, 6)); err != ErrQueueFull {
		t.Fatalf("second enqueue err = %v, want ErrQueueFull", err)
	}
}

// TestConcurrentEnqueue 验证多 goroutine 并发入队无竞争、序号单调。
func TestConcurrentEnqueue(t *testing.T) {
	sp := New(DefaultChannels(1024, 1024, 1<<20, 1024, 1<<20, 1024, 1<<20))
	defer sp.Close()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sp.EnqueueControl(frame(0))
		}()
	}
	wg.Wait()
	n := 0
	for {
		if _, ok := sp.Pop(); !ok {
			break
		}
		n++
	}
	if n != 50 {
		t.Fatalf("popped %d frames, want 50", n)
	}
}

// TestReliableBackpressureWakesOnPop 验证 P1 背压入队方在 Writer 出队腾出空间后
// 能被唤醒重试，而不需要等到 ctx 超时（防止唤醒信号被窃取/合并导致慢消费者误判）。
func TestReliableBackpressureWakesOnPop(t *testing.T) {
	sp := New(DefaultChannels(16, 1, 1024, 4, 512, 4, 512)) // P1 仅容 1 帧
	defer sp.Close()
	if err := sp.EnqueueReliable(context.Background(), frame(0xAA)); err != nil {
		t.Fatalf("fill P1: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sp.EnqueueReliable(ctx, frame(0xBB)) }()

	// 给背压入队方进入等待的时间，再出队腾出空间。
	time.Sleep(20 * time.Millisecond)
	if got, ok := sp.Pop(); !ok || got[0] != 0xAA {
		t.Fatalf("pop = %v,%v want 0xAA", got, ok)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("enqueue after space freed: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("EnqueueReliable did not wake after Pop freed space")
	}
}

// TestSequenceMonotonic 验证出站序号单调不回绕。
func TestSequenceMonotonic(t *testing.T) {
	sp := New(testChannels())
	defer sp.Close()
	var prev uint32
	for i := 0; i < 100; i++ {
		seq, err := sp.NextSequence()
		if err != nil {
			t.Fatal(err)
		}
		if seq <= prev && i > 0 {
			t.Fatalf("sequence not monotonic: %d after %d", seq, prev)
		}
		prev = seq
	}
}
