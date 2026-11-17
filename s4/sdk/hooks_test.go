package sdk

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestHookDispatcherRecoversPanicAndKeepsDispatching(t *testing.T) {
	var panics, after atomic.Int64
	d := NewHookDispatcher(16, nil, func(hook string, v any) { panics.Add(1) })
	d.Start()
	d.Enqueue("boom", true, func() { panic("hook blew up") })
	d.Enqueue("after", true, func() { after.Add(1) })
	d.Stop(time.Second)
	if panics.Load() != 1 {
		t.Fatalf("panic handler calls=%d, want 1", panics.Load())
	}
	if after.Load() != 1 {
		t.Fatal("panic in one hook must not block later hooks")
	}
}

func TestHookDispatcherDropsNormalButNeverCritical(t *testing.T) {
	gate := make(chan struct{})
	var normal, critical, dropped atomic.Int64
	d := NewHookDispatcher(1, func(i DropInfo) { dropped.Add(int64(i.Count)) }, nil)
	d.Start()
	d.Enqueue("blocking", true, func() { <-gate })
	time.Sleep(20 * time.Millisecond) // 让 worker 取到阻塞任务，normal 队列开始积压

	for i := 0; i < 50; i++ {
		d.Enqueue("normal", false, func() { normal.Add(1) })
	}
	close(gate)
	d.Enqueue("critical1", true, func() { critical.Add(1) })
	d.Enqueue("critical2", true, func() { critical.Add(1) })
	d.Stop(2 * time.Second)

	if dropped.Load() == 0 {
		t.Fatal("expected normal hooks to be dropped when queue is full")
	}
	if normal.Load() > 1 {
		t.Fatalf("normal hooks executed=%d, want <=1 (bounded queue)", normal.Load())
	}
	if critical.Load() != 2 {
		t.Fatalf("critical hooks executed=%d, want 2 (never dropped)", critical.Load())
	}
}

func TestHookDispatcherStopWithoutStartDrainsSynchronously(t *testing.T) {
	var ran atomic.Int64
	d := NewHookDispatcher(4, nil, nil)
	d.Enqueue("closed", true, func() { ran.Add(1) })
	d.Stop(time.Second)
	if ran.Load() != 1 {
		t.Fatal("critical hook must run even if dispatcher never started")
	}
}
