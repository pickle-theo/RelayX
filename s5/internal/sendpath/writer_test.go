package sendpath

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeConn 记录 Write/SetWriteDeadline 的调用次数与累计字节，
// 使「系统调用次数」成为可断言的量而非靠推测（S5 §3.1）。
type fakeConn struct {
	mu        sync.Mutex
	buf       bytes.Buffer
	writes    atomic.Int64
	deadlines atomic.Int64
	closed    atomic.Bool
	failAfter int64 // >0 时第 N 次 Write 起返回错误

	writeCh chan struct{}
}

func newFakeConn() *fakeConn {
	return &fakeConn{writeCh: make(chan struct{}, 1)}
}

func (c *fakeConn) Write(p []byte) (int, error) {
	n := c.writes.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failAfter > 0 && n >= c.failAfter {
		return 0, errors.New("fake conn: write failure")
	}
	c.buf.Write(p)
	select {
	case c.writeCh <- struct{}{}:
	default:
	}
	return len(p), nil
}

func (c *fakeConn) Read([]byte) (int, error)         { return 0, net.ErrClosed }
func (c *fakeConn) Close() error                     { c.closed.Store(true); return nil }
func (c *fakeConn) LocalAddr() net.Addr              { return fakeAddr{} }
func (c *fakeConn) RemoteAddr() net.Addr             { return fakeAddr{} }
func (c *fakeConn) SetDeadline(time.Time) error      { return nil }
func (c *fakeConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fakeConn) SetWriteDeadline(time.Time) error { c.deadlines.Add(1); return nil }

func (c *fakeConn) bytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.buf.Bytes()...)
}

type fakeAddr struct{}

func (fakeAddr) Network() string { return "fake" }
func (fakeAddr) String() string  { return "fake" }

// startWriter 在后台启动 Writer，返回停止函数。
func startWriter(t *testing.T, sp *SendPath, nc net.Conn, cfg WriteConfig) (*Writer, func()) {
	t.Helper()
	w := NewWriter(sp, nc, cfg, log.New(&bytes.Buffer{}, "", 0))
	done := make(chan struct{})
	go func() { defer close(done); w.Run() }()
	return w, func() {
		w.Stop()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("writer did not stop")
		}
	}
}

func TestWriterBatchesFramesIntoSingleWrite(t *testing.T) {
	sp := New(testChannels())
	defer sp.Close()
	nc := newFakeConn()
	// 先入队 8 帧再启动 Writer：保证同一批取出（队列有界且未满）。
	for i := 0; i < 8; i++ {
		if err := sp.EnqueueReliable(context.Background(), []byte{byte(i)}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	w, stop := startWriter(t, sp, nc, WriteConfig{WriteDeadline: time.Second})
	defer stop()

	waitWritten(t, sp, ChReliable, 8)
	st := w.Stats()
	if st.WriteCalls != 1 {
		t.Fatalf("write syscalls=%d, want 1 (single coalesced write)", st.WriteCalls)
	}
	if st.DeadlineCalls != 1 {
		t.Fatalf("deadline syscalls=%d, want 1 per batch", st.DeadlineCalls)
	}
	if st.Frames != 8 {
		t.Fatalf("frames=%d, want 8", st.Frames)
	}
	if st.SyscallsPerFrame >= 0.5 {
		t.Fatalf("syscalls/frame=%.3f, want < 0.5", st.SyscallsPerFrame)
	}
	if got, want := len(nc.bytes()), 8; got != want {
		t.Fatalf("bytes written=%d, want %d", got, want)
	}
}

// TestWriterForcedPerFrameWhenBudgetTooSmall 验证小批字节预算下退化为逐帧写：
// 批量写只是实现细化，任何参数下都不能改变帧序与帧数。
func TestWriterForcedPerFrameWhenBudgetTooSmall(t *testing.T) {
	sp := New(testChannels())
	defer sp.Close()
	nc := newFakeConn()
	big := make([]byte, 900)
	for i := 0; i < 3; i++ {
		if err := sp.EnqueueReliable(context.Background(), big); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	w, stop := startWriter(t, sp, nc, WriteConfig{WriteDeadline: time.Second, MaxBatchBytes: 1024})
	defer stop()
	waitWritten(t, sp, ChReliable, 3)
	st := w.Stats()
	if st.WriteCalls != 3 {
		t.Fatalf("write syscalls=%d, want 3 (per-frame due to tiny batch budget)", st.WriteCalls)
	}
	if got, want := len(nc.bytes()), 3*len(big); got != want {
		t.Fatalf("bytes=%d, want %d", got, want)
	}
}

func TestWriterSingleFrameUsesDirectWrite(t *testing.T) {
	sp := New(testChannels())
	defer sp.Close()
	nc := newFakeConn()
	if err := sp.EnqueueControl([]byte{0xAB}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	w, stop := startWriter(t, sp, nc, WriteConfig{WriteDeadline: time.Second})
	defer stop()
	waitWritten(t, sp, ChControl, 1)
	if st := w.Stats(); st.WriteCalls != 1 || st.Batches != 1 {
		t.Fatalf("stats=%+v", st)
	}
	if !bytes.Equal(nc.bytes(), []byte{0xAB}) {
		t.Fatalf("bytes=%v", nc.bytes())
	}
}

func TestWriterPreservesPriorityOrderAcrossBatch(t *testing.T) {
	sp := New(testChannels())
	defer sp.Close()
	nc := newFakeConn()
	// 逆序入队；同一批内必须按 P0→P1→P2→P3 写出。
	if err := sp.EnqueueEvent([]byte{3}); err != nil {
		t.Fatal(err)
	}
	if err := sp.EnqueueBroadcast([]byte{2}); err != nil {
		t.Fatal(err)
	}
	if err := sp.EnqueueReliable(context.Background(), []byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := sp.EnqueueControl([]byte{0}); err != nil {
		t.Fatal(err)
	}
	w, stop := startWriter(t, sp, nc, WriteConfig{WriteDeadline: time.Second})
	defer stop()
	for _, cls := range []ChannelClass{ChControl, ChReliable, ChBroadcast, ChEvent} {
		waitWritten(t, sp, cls, 1)
	}
	if got, want := nc.bytes(), []byte{0, 1, 2, 3}; !bytes.Equal(got, want) {
		t.Fatalf("bytes=%v, want %v", got, want)
	}
	if st := w.Stats(); st.WriteCalls != 1 {
		t.Fatalf("write syscalls=%d, want 1", st.WriteCalls)
	}
}

func TestWriterReleasesSharedFrameAfterCoalescedWrite(t *testing.T) {
	sp := New(testChannels())
	defer sp.Close()
	nc := newFakeConn()
	poolStub := &sync.Pool{New: func() any { return make([]byte, 0, 64) }}
	buf := poolStub.Get().([]byte)
	buf = append(buf, 0xAA)
	f := NewImmutableFrame(poolStub, buf)
	if err := sp.TrySendBroadcast(f); err != nil {
		t.Fatalf("try send: %v", err)
	}
	// 再入队一个 P1 帧，强制走合并路径（共享帧 + 独占帧）。
	if err := sp.EnqueueReliable(context.Background(), []byte{0xBB}); err != nil {
		t.Fatalf("enqueue reliable: %v", err)
	}
	_, stop := startWriter(t, sp, nc, WriteConfig{WriteDeadline: time.Second})
	defer stop()
	waitWritten(t, sp, ChBroadcast, 1)
	waitWritten(t, sp, ChReliable, 1)
	if got := f.Refs(); got != 1 {
		t.Fatalf("refs=%d after write, want 1 (only fanout holder left)", got)
	}
	if !bytes.Equal(nc.bytes(), []byte{0xBB, 0xAA}) {
		t.Fatalf("bytes=%v, want priority order [0xBB 0xAA]", nc.bytes())
	}
	f.Release()
}

func TestWriterReleasesSharedFrameOnWriteError(t *testing.T) {
	sp := New(testChannels())
	defer sp.Close()
	nc := newFakeConn()
	nc.failAfter = 1
	poolStub := &sync.Pool{New: func() any { return make([]byte, 0, 64) }}
	buf := poolStub.Get().([]byte)
	buf = append(buf, 0xAA)
	f := NewImmutableFrame(poolStub, buf)
	if err := sp.TrySendBroadcast(f); err != nil {
		t.Fatalf("try send: %v", err)
	}
	if err := sp.EnqueueControl([]byte{0x01}); err != nil {
		t.Fatalf("enqueue control: %v", err)
	}
	w, stop := startWriter(t, sp, nc, WriteConfig{WriteDeadline: time.Second})
	defer stop()
	select {
	case <-w.Closed():
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not exit after write error")
	}
	// P0+P2 同批合并写出：写失败后 Writer 必须释放本批全部共享引用，
	// 只留扇出方那 1 个（否则池化 buffer 永不可回收）。
	if got := f.Refs(); got != 1 {
		t.Fatalf("refs=%d after failed batched write, want 1", got)
	}
	f.Release()
	_ = stop
}

// TestWriterReleasesPendingSharedFrameOnExit 验证「本批放不下、留待下批」的帧
// 在 Writer 因写失败退出时也被释放（pending 清理路径）。
func TestWriterReleasesPendingSharedFrameOnExit(t *testing.T) {
	sp := New(testChannels())
	defer sp.Close()
	nc := newFakeConn()
	nc.failAfter = 1
	poolStub := &sync.Pool{New: func() any { return make([]byte, 0, 64) }}
	buf := poolStub.Get().([]byte)
	buf = append(buf, 0xAA)
	f := NewImmutableFrame(poolStub, buf)
	if err := sp.TrySendBroadcast(f); err != nil {
		t.Fatalf("try send: %v", err)
	}
	if err := sp.EnqueueControl([]byte{0x01}); err != nil {
		t.Fatalf("enqueue control: %v", err)
	}
	// 批字节上限设为 1：P0(1B) 单独成批，P2(1B) 只能进入 pending。
	w, stop := startWriter(t, sp, nc, WriteConfig{WriteDeadline: time.Second, MaxBatchBytes: 1})
	defer stop()
	select {
	case <-w.Closed():
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not exit after write error")
	}
	if got := f.Refs(); got != 1 {
		t.Fatalf("refs=%d after exit, want 1 (pending must be released)", got)
	}
	f.Release()
	_ = stop
}

func TestWriterStopFlushesQueuedFrames(t *testing.T) {
	sp := New(testChannels())
	defer sp.Close()
	nc := newFakeConn()
	for i := 0; i < 4; i++ {
		if err := sp.EnqueueControl([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	_, stop := startWriter(t, sp, nc, WriteConfig{WriteDeadline: time.Second})
	defer stop()
	waitWritten(t, sp, ChControl, 4)
	if got := len(nc.bytes()); got != 4 {
		t.Fatalf("bytes=%d, want 4", got)
	}
}

// TestWriterDefaultsZeroDeadline 回归零值 WriteDeadline：零值直接传给
// SetWriteDeadline 等价于「立即超时」，必须改用缺省值。
func TestWriterDefaultsZeroDeadline(t *testing.T) {
	sp := New(testChannels())
	defer sp.Close()
	w := NewWriter(sp, newFakeConn(), WriteConfig{}, log.New(io.Discard, "", 0))
	if w.cfg.WriteDeadline != DefaultWriteDeadline {
		t.Fatalf("WriteDeadline=%v, want %v", w.cfg.WriteDeadline, DefaultWriteDeadline)
	}
}

// TestWriterStopConcurrentIsIdempotent 验证 Stop 的 check-then-close 竞态已修复。
func TestWriterStopConcurrentIsIdempotent(t *testing.T) {
	sp := New(testChannels())
	defer sp.Close()
	w := NewWriter(sp, newFakeConn(), WriteConfig{}, log.New(io.Discard, "", 0))
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.Stop()
		}()
	}
	wg.Wait()
	select {
	case <-w.done:
	default:
		t.Fatal("done not closed after Stop")
	}
}

func waitWritten(t *testing.T, sp *SendPath, cls ChannelClass, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if sp.Written(cls) >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("%v written=%d, want >=%d", cls, sp.Written(cls), want)
}
