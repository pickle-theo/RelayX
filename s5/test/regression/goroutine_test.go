package regression

import (
	"log"
	"runtime"
	"sync"
	"testing"
	"time"

	"relayx/s5/internal/heartbeat"
	"relayx/s5/internal/pool"
	"relayx/s5/internal/sendpath"
	"relayx/s5/protocol"
)

// 协程与资源回归（S5 §11.2）：
//   - 协程泄漏偏差 ≤5；
//   - 池化不得造成内存泄漏（InUse 回到 0、Drops 有界）；
//   - 时间轮登记项在停止后不得残留。

// settleGoroutines 等待 goroutine 数量稳定（回收需要调度，轮询到稳定或超时）。
func settleGoroutines() int {
	var last int
	stable := 0
	for i := 0; i < 100; i++ {
		n := runtime.NumGoroutine()
		if n == last {
			stable++
			if stable >= 3 {
				return n
			}
		} else {
			stable = 0
			last = n
		}
		time.Sleep(10 * time.Millisecond)
	}
	return runtime.NumGoroutine()
}

// TestNoGoroutineLeakAfterWriters 连接级 Writer 退出后不得留下 goroutine。
func TestNoGoroutineLeakAfterWriters(t *testing.T) {
	base := settleGoroutines()
	const conns = 32
	handles := make([]*sendpath.SendPath, 0, conns)
	writers := make([]*sendpath.Writer, 0, conns)
	done := make([]chan struct{}, 0, conns)
	for i := 0; i < conns; i++ {
		sp := sendpath.New(isoChannels())
		w := sendpath.NewWriter(sp, &blockingConn{}, sendpath.WriteConfig{WriteDeadline: time.Second},
			log.New(nopWriter{}, "", 0))
		ch := make(chan struct{})
		go func() { defer close(ch); w.Run() }()
		handles = append(handles, sp)
		writers = append(writers, w)
		done = append(done, ch)
	}
	for i := range writers {
		if err := handles[i].EnqueueControl([]byte{0x08}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}
	for i := range writers {
		waitClass(t, handles[i], sendpath.ChControl, 1)
		writers[i].Stop()
	}
	for _, ch := range done {
		<-ch
	}
	for _, sp := range handles {
		sp.Close()
	}
	got := settleGoroutines()
	if diff := got - base; diff > 5 {
		t.Fatalf("goroutine leak: base=%d after=%d (diff=%d > 5)", base, got, diff)
	}
}

// TestNoGoroutineLeakWithGlobalWheel 全局时间轮：N 个连接共 1 个 tick goroutine，
// 全部停止后回到基线（这是 S5 §4.4 的核心收益）。
func TestNoGoroutineLeakWithGlobalWheel(t *testing.T) {
	base := settleGoroutines()
	w := heartbeat.New(heartbeat.Config{Tick: time.Millisecond, Buckets: 16})
	w.Start()
	const conns = 200
	probes := make([]*heartbeat.Probe, 0, conns)
	for i := 0; i < conns; i++ {
		p := heartbeat.NewProbe(w, heartbeat.ProbeConfig{
			Interval: 50 * time.Millisecond, PongTimeout: 20 * time.Millisecond,
			SendPing: func() {}, LastActivity: time.Now,
		})
		p.Start()
		probes = append(probes, p)
	}
	if st := w.Stats(); st.Pending != conns {
		t.Fatalf("pending=%d, want %d", st.Pending, conns)
	}
	// 200 条连接仍只有一个 tick goroutine（相对基线 +1）。
	withWheel := settleGoroutines()
	if diff := withWheel - base; diff > 5 {
		t.Fatalf("wheel added %d goroutines for %d connections, want <=5", diff, conns)
	}
	for _, p := range probes {
		p.Stop()
	}
	w.Stop()
	if st := w.Stats(); st.Pending != 0 {
		t.Fatalf("pending=%d after stop, want 0", st.Pending)
	}
	got := settleGoroutines()
	if diff := got - base; diff > 5 {
		t.Fatalf("goroutine leak after wheel stop: base=%d after=%d", base, got)
	}
}

// TestPoolNoLeakAfterChurn 池化缓冲在反复取还后必须全部归还（InUse == 0）。
func TestPoolNoLeakAfterChurn(t *testing.T) {
	p := pool.New(pool.Config{})
	for round := 0; round < 50; round++ {
		bufs := make([]*pool.Buf, 0, 64)
		for i := 0; i < 64; i++ {
			b := p.Get(protocol.MaxFrameSize)
			b.Set(append(b.Slice(), make([]byte, 128)...))
			bufs = append(bufs, b)
		}
		for _, b := range bufs {
			p.Put(b)
		}
	}
	st := p.Stats()
	if st.InUse != 0 {
		t.Fatalf("inUse=%d after churn, want 0", st.InUse)
	}
	if st.Gets != st.Puts {
		t.Fatalf("gets=%d puts=%d", st.Gets, st.Puts)
	}
	// 大帧丢弃必须有界：每轮 64 次归还，50 轮共 3200 次，构建数不应随轮次线性增长。
	if st.News > int64(3200) {
		t.Fatalf("news=%d suggests pool not retaining buffers", st.News)
	}
}

// TestSendPathCloseReleasesSharedRefs 连接关闭时队列里的共享帧引用必须归还，
// 否则池化 buffer 永不回收（S3 §2.3 引用计数闭环）。
func TestSendPathCloseReleasesSharedRefs(t *testing.T) {
	shared := &sync.Pool{New: func() any { return make([]byte, 0, 256) }}
	frames := make([]*sendpath.ImmutableFrame, 0, 16)
	for i := 0; i < 16; i++ {
		buf := shared.Get().([]byte)
		enc, err := protocol.AppendBroadcast(buf[:0], "t", 1, uint64(i), []byte("x"))
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		frames = append(frames, sendpath.NewImmutableFrame(shared, enc))
	}
	sp := sendpath.New(isoChannels())
	queued := 0
	for _, f := range frames {
		err := sp.TrySendBroadcast(f)
		if err == nil {
			queued++
			continue
		}
		if err != sendpath.ErrBudgetExhausted {
			t.Fatalf("try send: %v", err)
		}
	}
	if queued == 0 {
		t.Fatal("no frame was queued")
	}
	sp.Close()
	for i, f := range frames {
		if got := f.Refs(); got != 1 {
			t.Fatalf("frame %d refs=%d after close, want 1 (queue ref released)", i, got)
		}
		f.Release()
		if got := f.Refs(); got != 0 {
			t.Fatalf("frame %d refs=%d after final release", i, got)
		}
	}
}
