package heartbeat

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock 是可手动推进的时钟，让心跳用例完全确定性。
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(1_700_000_000, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// stepN 推进时间轮 N 个刻度，同时把假时钟推进相同时长（Tick 为 10ms）。
func stepN(w *Wheel, clock *fakeClock, n int) {
	for i := 0; i < n; i++ {
		w.Step()
		clock.Advance(w.Tick())
	}
}

func TestProbeSendsPingWhenIdle(t *testing.T) {
	clock := newFakeClock()
	w := New(Config{Tick: 10 * time.Millisecond, Buckets: 8})
	var pings, timeouts atomic.Int64
	idleSince := clock.Now() // 固定不动 = 持续空闲
	p := NewProbe(w, ProbeConfig{
		Interval: 30 * time.Millisecond, PongTimeout: 20 * time.Millisecond,
		LastActivity: func() time.Time { return idleSince }, Now: clock.Now,
		SendPing:  func() { pings.Add(1) },
		OnTimeout: func() { timeouts.Add(1) },
	})
	p.Start()
	stepN(w, clock, 5)
	if pings.Load() != 0 {
		t.Fatalf("pings=%d before interval", pings.Load())
	}
	stepN(w, clock, 1) // 累计 6 个 tick = 60ms > 30ms 空闲
	if pings.Load() != 1 {
		t.Fatalf("pings=%d after idle interval, want 1", pings.Load())
	}
}

func TestProbeNoPingWhileActive(t *testing.T) {
	clock := newFakeClock()
	w := New(Config{Tick: 10 * time.Millisecond, Buckets: 8})
	var pings atomic.Int64
	last := clock.Now()
	p := NewProbe(w, ProbeConfig{
		Interval: 30 * time.Millisecond, PongTimeout: 20 * time.Millisecond,
		LastActivity: func() time.Time { return last }, Now: clock.Now,
		SendPing: func() { pings.Add(1) },
	})
	p.Start()
	for i := 0; i < 10; i++ {
		last = clock.Now() // 持续有入站活动
		stepN(w, clock, 1)
	}
	if pings.Load() != 0 {
		t.Fatalf("pings=%d with continuous activity, want 0", pings.Load())
	}
}

func TestProbeTimeoutThenActivityRecovers(t *testing.T) {
	clock := newFakeClock()
	w := New(Config{Tick: 10 * time.Millisecond, Buckets: 8})
	var pings, timeouts atomic.Int64
	var last atomic.Int64
	last.Store(clock.Now().UnixNano())
	p := NewProbe(w, ProbeConfig{
		Interval: 20 * time.Millisecond, PongTimeout: 20 * time.Millisecond,
		LastActivity: func() time.Time { return time.Unix(0, last.Load()) },
		Now:          clock.Now,
		SendPing:     func() { pings.Add(1) },
		OnTimeout:    func() { timeouts.Add(1) },
	})
	p.Start()
	stepN(w, clock, 4) // 空闲超过 Interval → 发 PING
	if pings.Load() != 1 {
		t.Fatalf("pings=%d, want 1", pings.Load())
	}
	last.Store(clock.Now().UnixNano()) // PONG 在 PING 之后到达
	stepN(w, clock, 3)                 // 越过 PongTimeout 检查点
	if timeouts.Load() != 0 {
		t.Fatalf("timeouts=%d after pong, want 0", timeouts.Load())
	}
	if pings.Load() != 1 {
		t.Fatalf("pings=%d, want 1 (no duplicate ping)", pings.Load())
	}
}

func TestProbeTimesOutWithoutActivity(t *testing.T) {
	clock := newFakeClock()
	w := New(Config{Tick: 10 * time.Millisecond, Buckets: 8})
	var timeouts atomic.Int64
	var pings atomic.Int64
	idleSince := clock.Now()
	p := NewProbe(w, ProbeConfig{
		Interval: 20 * time.Millisecond, PongTimeout: 20 * time.Millisecond,
		LastActivity: func() time.Time { return idleSince }, Now: clock.Now,
		SendPing:  func() { pings.Add(1) },
		OnTimeout: func() { timeouts.Add(1) },
	})
	p.Start()
	stepN(w, clock, 3) // 发 PING
	stepN(w, clock, 3) // 无活动 → 超时
	if pings.Load() != 1 {
		t.Fatalf("pings=%d, want 1", pings.Load())
	}
	if timeouts.Load() != 1 {
		t.Fatalf("timeouts=%d, want 1", timeouts.Load())
	}
	if p.Active() {
		t.Fatal("probe still active after timeout")
	}
}

// TestProbeTimeoutCallbackCanStopProbe 回归「持 p.mu 执行 OnTimeout」的死锁：
// 连接层 OnTimeout 会调用 Close → Probe.Stop，必须在锁外回调。
func TestProbeTimeoutCallbackCanStopProbe(t *testing.T) {
	clock := newFakeClock()
	w := New(Config{Tick: 10 * time.Millisecond, Buckets: 8})
	idleSince := clock.Now()
	var p *Probe
	p = NewProbe(w, ProbeConfig{
		Interval: 20 * time.Millisecond, PongTimeout: 20 * time.Millisecond,
		LastActivity: func() time.Time { return idleSince },
		Now:          clock.Now,
		SendPing:     func() {},
		OnTimeout:    func() { p.Stop() },
	})
	p.Start()

	done := make(chan struct{})
	go func() {
		stepN(w, clock, 3) // 发 PING
		stepN(w, clock, 3) // 无活动 → 超时 → OnTimeout → Stop
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("OnTimeout callback deadlocked on probe lock")
	}
	if p.Active() {
		t.Fatal("probe still active after timeout+stop")
	}
}

func TestProbeStopCancelsPending(t *testing.T) {
	clock := newFakeClock()
	w := New(Config{Tick: 10 * time.Millisecond, Buckets: 8})
	var pings atomic.Int64
	p := NewProbe(w, ProbeConfig{
		Interval: 20 * time.Millisecond, PongTimeout: 20 * time.Millisecond,
		LastActivity: clock.Now, Now: clock.Now,
		SendPing: func() { pings.Add(1) },
	})
	p.Start()
	if !p.Active() {
		t.Fatal("probe should be active after Start")
	}
	p.Stop()
	if p.Active() {
		t.Fatal("probe still active after Stop")
	}
	if st := w.Stats(); st.Pending != 0 {
		t.Fatalf("pending=%d after stop, want 0", st.Pending)
	}
	stepN(w, clock, 10)
	if pings.Load() != 0 {
		t.Fatalf("pings=%d after stop, want 0", pings.Load())
	}
}

func TestProbeStartIsIdempotent(t *testing.T) {
	clock := newFakeClock()
	w := New(Config{Tick: 10 * time.Millisecond, Buckets: 8})
	p := NewProbe(w, ProbeConfig{
		Interval: 20 * time.Millisecond, PongTimeout: 20 * time.Millisecond,
		LastActivity: clock.Now, Now: clock.Now, SendPing: func() {},
	})
	p.Start()
	p.Start()
	if st := w.Stats(); st.Pending != 1 {
		t.Fatalf("pending=%d after double start, want 1", st.Pending)
	}
}
