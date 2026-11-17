package heartbeat

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAfterFiresAtExactTick(t *testing.T) {
	w := New(Config{Tick: 10 * time.Millisecond, Buckets: 8})
	var fired atomic.Int64
	w.After(30*time.Millisecond, func() { fired.Add(1) })
	for i := 0; i < 2; i++ {
		w.Step()
	}
	if got := fired.Load(); got != 0 {
		t.Fatalf("fired=%d before deadline, want 0", got)
	}
	w.Step()
	if got := fired.Load(); got != 1 {
		t.Fatalf("fired=%d at deadline, want 1", got)
	}
	w.Step()
	if got := fired.Load(); got != 1 {
		t.Fatalf("fired=%d after deadline, want 1", got)
	}
}

func TestAfterRoundsUpToTick(t *testing.T) {
	w := New(Config{Tick: 10 * time.Millisecond, Buckets: 8})
	var fired atomic.Int64
	w.After(time.Millisecond, func() { fired.Add(1) }) // < 1 tick
	w.Step()
	if got := fired.Load(); got != 1 {
		t.Fatalf("fired=%d, want 1 (sub-tick deadline rounds up)", got)
	}
}

func TestAfterZeroDurationIsOneTick(t *testing.T) {
	w := New(Config{Tick: time.Millisecond, Buckets: 4})
	var fired atomic.Int64
	w.After(0, func() { fired.Add(1) })
	w.Step()
	if got := fired.Load(); got != 1 {
		t.Fatalf("fired=%d, want 1", got)
	}
}

func TestCancelPreventsFiring(t *testing.T) {
	w := New(Config{Tick: 10 * time.Millisecond, Buckets: 8})
	var fired atomic.Int64
	e := w.After(20*time.Millisecond, func() { fired.Add(1) })
	if !w.Cancel(e) {
		t.Fatal("cancel returned false")
	}
	if w.Cancel(e) {
		t.Fatal("second cancel returned true")
	}
	for i := 0; i < 5; i++ {
		w.Step()
	}
	if got := fired.Load(); got != 0 {
		t.Fatalf("fired=%d after cancel, want 0", got)
	}
	if st := w.Stats(); st.Canceled != 1 || st.Fired != 0 {
		t.Fatalf("stats=%+v", st)
	}
}

// TestMultiRevolution 验证超过轮盘跨度（Tick×Buckets）的登记不会提前触发。
func TestMultiRevolution(t *testing.T) {
	w := New(Config{Tick: time.Millisecond, Buckets: 4}) // 跨度 4ms
	var early, late atomic.Int64
	w.After(10*time.Millisecond, func() { late.Add(1) })
	w.After(1*time.Millisecond, func() { early.Add(1) })
	for i := 0; i < 9; i++ {
		w.Step()
	}
	if early.Load() != 1 {
		t.Fatalf("early=%d, want 1", early.Load())
	}
	if late.Load() != 0 {
		t.Fatalf("late fired after 9 ticks, want 0")
	}
	w.Step()
	if late.Load() != 1 {
		t.Fatalf("late=%d after 10 ticks, want 1", late.Load())
	}
}

func TestCallbackCanRescheduleAndCancel(t *testing.T) {
	w := New(Config{Tick: time.Millisecond, Buckets: 8})
	var count atomic.Int64
	var e *Entry
	e = w.After(time.Millisecond, func() {
		count.Add(1)
		if count.Load() < 3 {
			e = w.After(time.Millisecond, e.cb)
			return
		}
		_ = w.Cancel(e)
	})
	for i := 0; i < 10; i++ {
		w.Step()
	}
	if got := count.Load(); got != 3 {
		t.Fatalf("count=%d, want 3", got)
	}
	if st := w.Stats(); st.Pending != 0 {
		t.Fatalf("pending=%d, want 0", st.Pending)
	}
}

func TestAfterRequiresCallback(t *testing.T) {
	w := New(Config{})
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for nil callback")
		}
	}()
	w.After(time.Second, nil)
}

func TestStatsAccounting(t *testing.T) {
	w := New(Config{Tick: time.Millisecond, Buckets: 8})
	w.After(2*time.Millisecond, func() {})
	e := w.After(3*time.Millisecond, func() {})
	w.Cancel(e)
	w.Step()
	w.Step()
	st := w.Stats()
	if st.Scheduled != 2 || st.Fired != 1 || st.Canceled != 1 || st.Pending != 0 || st.Ticks != 2 {
		t.Fatalf("stats=%+v", st)
	}
}

func TestStartStopReleasesGoroutine(t *testing.T) {
	before := runtime.NumGoroutine()
	w := New(Config{Tick: time.Millisecond, Buckets: 8})
	w.Start()
	w.Start() // 幂等
	time.Sleep(5 * time.Millisecond)
	w.Stop()
	w.Stop() // 幂等
	if got := runtime.NumGoroutine(); got > before+1 {
		t.Fatalf("goroutines=%d, before=%d (tick goroutine leaked?)", got, before)
	}
}

// TestStopBeforeStartAndRestart 回归 stopOnce 的两个缺陷：
// Stop 在 Start 之前调用后，后续 Start 的 goroutine 永远停不下来；
// Stop 之后也无法再次 Start（与注释承诺不符）。
func TestStopBeforeStartAndRestart(t *testing.T) {
	w := New(Config{Tick: time.Millisecond, Buckets: 8})
	w.Stop() // 未启动：no-op，不得影响后续 Start

	w.Start()
	time.Sleep(5 * time.Millisecond)
	w.Stop()
	ticks := w.Stats().Ticks
	time.Sleep(20 * time.Millisecond)
	if after := w.Stats().Ticks; after != ticks {
		t.Fatalf("tick goroutine still running after Stop: %d -> %d", ticks, after)
	}

	w.Start() // Stop 返回后允许再次 Start
	time.Sleep(5 * time.Millisecond)
	w.Stop()
	ticks = w.Stats().Ticks
	time.Sleep(20 * time.Millisecond)
	if after := w.Stats().Ticks; after != ticks {
		t.Fatalf("restarted tick goroutine still running after Stop: %d -> %d", ticks, after)
	}
}

func TestConcurrentAfterCancelStep(t *testing.T) {
	w := New(Config{Tick: time.Millisecond, Buckets: 16})
	var fired atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				e := w.After(time.Duration(seed+j%5+1)*time.Millisecond, func() { fired.Add(1) })
				if j%3 == 0 {
					w.Cancel(e)
				}
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 400; j++ {
			w.Step()
		}
	}()
	wg.Wait()
	for i := 0; i < 20; i++ {
		w.Step()
	}
	st := w.Stats()
	if st.Pending != 0 {
		t.Fatalf("pending=%d after drain, want 0", st.Pending)
	}
	if st.Fired+st.Canceled != st.Scheduled {
		t.Fatalf("fired(%d)+canceled(%d) != scheduled(%d)", st.Fired, st.Canceled, st.Scheduled)
	}
}
