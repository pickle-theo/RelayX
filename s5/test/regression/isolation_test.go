package regression

import (
	"context"
	"errors"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"relayx/s5/internal/sendpath"
	"relayx/s5/protocol"
)

// 四通道隔离验证（S5 §5 验收硬项）：P0–P3 物理隔离在 S5 优化后未退化。
// 判据：一类流量打满时，其他类仍能按时写出（不饿死），且 P0 永不丢弃。

// blockingConn 是一个可控的假连接：可以人为拖慢写，用于制造「某通道打满」。
type blockingConn struct {
	mu       sync.Mutex
	written  atomic.Int64
	delayNs  atomic.Int64
	failHard atomic.Bool
}

func (c *blockingConn) Write(p []byte) (int, error) {
	if d := c.delayNs.Load(); d > 0 {
		time.Sleep(time.Duration(d))
	}
	if c.failHard.Load() {
		return 0, errors.New("blockingConn: hard failure")
	}
	c.written.Add(int64(len(p)))
	return len(p), nil
}

func (c *blockingConn) Read([]byte) (int, error)         { return 0, net.ErrClosed }
func (c *blockingConn) Close() error                     { return nil }
func (c *blockingConn) LocalAddr() net.Addr              { return regressionAddr{} }
func (c *blockingConn) RemoteAddr() net.Addr             { return regressionAddr{} }
func (c *blockingConn) SetDeadline(time.Time) error      { return nil }
func (c *blockingConn) SetReadDeadline(time.Time) error  { return nil }
func (c *blockingConn) SetWriteDeadline(time.Time) error { return nil }

type regressionAddr struct{}

func (regressionAddr) Network() string { return "reg" }
func (regressionAddr) String() string  { return "reg" }

func isoChannels() [4]sendpath.Channel {
	return sendpath.DefaultChannels(64, 64, 1<<20, 8, 256<<10, 8, 256<<10)
}

type writerHandle struct {
	sp     *sendpath.SendPath
	w      *sendpath.Writer
	nc     *blockingConn
	closed chan struct{}
}

func startIsolationWriter(t *testing.T, sp *sendpath.SendPath, nc *blockingConn) *writerHandle {
	t.Helper()
	w := sendpath.NewWriter(sp, nc, sendpath.WriteConfig{WriteDeadline: time.Second}, log.New(nopWriter{}, "", 0))
	h := &writerHandle{sp: sp, w: w, nc: nc, closed: make(chan struct{})}
	go func() {
		defer close(h.closed)
		w.Run()
	}()
	return h
}

func (h *writerHandle) stop() {
	h.w.Stop()
	select {
	case <-h.closed:
	case <-time.After(2 * time.Second):
	}
	h.sp.Close()
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }

// TestIsolationBroadcastFloodKeepsReliable 广播（P2）打满不得影响可靠（P1）与心跳（P0）。
func TestIsolationBroadcastFloodKeepsReliable(t *testing.T) {
	sp := sendpath.New(isoChannels())
	nc := &blockingConn{}
	nc.delayNs.Store(int64(time.Millisecond)) // 拖慢写出，确保生产快于消费
	h := startIsolationWriter(t, sp, nc)
	defer h.stop()

	// 1) 灌爆 P2：预算 8 帧，入队远超预算。
	pool := &sync.Pool{New: func() any { return make([]byte, 0, 256) }}
	dropped := 0
	for i := 0; i < 200; i++ {
		buf := pool.Get().([]byte)
		f := sendpath.NewImmutableFrame(pool, append(buf[:0], byte(i)))
		err := sp.TrySendBroadcast(f)
		f.Release()
		switch {
		case err == nil:
		case errors.Is(err, sendpath.ErrBudgetExhausted):
			dropped++
		default:
			t.Fatalf("unexpected broadcast error: %v", err)
		}
	}
	if dropped == 0 {
		t.Fatalf("expected broadcast drops under flood (depth=%d)", sp.Depth(sendpath.ChBroadcast))
	}
	// 预算是「当前深度」上限：任何时刻 P2 队列都不得超过 8 帧。
	if depth := sp.Depth(sendpath.ChBroadcast); depth > 8 {
		t.Fatalf("P2 depth=%d exceeds frame budget 8", depth)
	}

	// 2) P1 与 P0 必须仍然可入队并写出。
	for i := 0; i < 4; i++ {
		if err := sp.EnqueueReliable(context.Background(), []byte{0x05}); err != nil {
			t.Fatalf("reliable enqueue after broadcast flood: %v", err)
		}
	}
	if err := sp.EnqueueControl([]byte{0x09}); err != nil {
		t.Fatalf("control enqueue after broadcast flood: %v", err)
	}
	waitClass(t, sp, sendpath.ChReliable, 4)
	waitClass(t, sp, sendpath.ChControl, 1)

	// 3) 心跳延迟：P0 从入队到写出的时间必须很小（不被 P2 拖死）。
	start := time.Now()
	if err := sp.EnqueueControl([]byte{0x08}); err != nil {
		t.Fatalf("control enqueue: %v", err)
	}
	waitClass(t, sp, sendpath.ChControl, 2)
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("control latency=%v under broadcast flood, want < 500ms", d)
	}
}

// TestIsolationEventFloodKeepsReliable 事件（P3）洪峰不得影响 P1 与 P0。
func TestIsolationEventFloodKeepsReliable(t *testing.T) {
	sp := sendpath.New(isoChannels())
	nc := &blockingConn{}
	h := startIsolationWriter(t, sp, nc)
	defer h.stop()

	dropped := 0
	for i := 0; i < 200; i++ {
		err := sp.TryEnqueueEvent([]byte{0x22})
		if errors.Is(err, sendpath.ErrBudgetExhausted) {
			dropped++
		} else if err != nil {
			t.Fatalf("event enqueue: %v", err)
		}
	}
	if dropped == 0 {
		t.Fatal("expected P3 drops under event flood")
	}
	for i := 0; i < 4; i++ {
		if err := sp.EnqueueReliable(context.Background(), []byte{0x05}); err != nil {
			t.Fatalf("reliable enqueue during event flood: %v", err)
		}
	}
	if err := sp.EnqueueControl([]byte{0x09}); err != nil {
		t.Fatalf("control enqueue during event flood: %v", err)
	}
	waitClass(t, sp, sendpath.ChReliable, 4)
	waitClass(t, sp, sendpath.ChControl, 1)
}

// TestIsolationReliableFloodKeepsBroadcastPolicy 可靠（P1）洪峰时：
// 广播仍按「丢弃 + 计数」处理且不断连，P0 仍能写出。
func TestIsolationReliableFloodKeepsBroadcastPolicy(t *testing.T) {
	sp := sendpath.New(isoChannels())
	nc := &blockingConn{} // 拖慢写出，制造背压
	nc.delayNs.Store(int64(200 * time.Microsecond))
	h := startIsolationWriter(t, sp, nc)
	defer h.stop()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	full := 0
	for i := 0; i < 200; i++ {
		err := sp.EnqueueReliable(ctx, []byte{0x05})
		if errors.Is(err, sendpath.ErrQueueFull) {
			full++
			break
		}
		if err != nil {
			t.Fatalf("reliable enqueue: %v", err)
		}
	}
	if full == 0 {
		t.Skip("writer drained too fast to build P1 backpressure on this machine")
	}
	// P2 丢弃但不断连。
	pool := &sync.Pool{New: func() any { return make([]byte, 0, 64) }}
	for i := 0; i < 50; i++ {
		f := sendpath.NewImmutableFrame(pool, append(pool.Get().([]byte)[:0], byte(i)))
		_ = sp.TrySendBroadcast(f)
		f.Release()
	}
	if sp.Dropped(sendpath.ChBroadcast) == 0 {
		t.Fatal("broadcast budget drops not counted under P1 flood")
	}
	// P0 仍可写出（不被 P1 背压拖死）。
	before := sp.Written(sendpath.ChControl)
	if err := sp.EnqueueControl([]byte{0x09}); err != nil {
		t.Fatalf("control enqueue under P1 flood: %v", err)
	}
	waitClass(t, sp, sendpath.ChControl, before+1)
}

// TestIsolationAllChannelsSaturated 四通道同时打满：各自在自己预算内，无饿死。
func TestIsolationAllChannelsSaturated(t *testing.T) {
	sp := sendpath.New(isoChannels())
	nc := &blockingConn{}
	nc.delayNs.Store(int64(50 * time.Microsecond))
	h := startIsolationWriter(t, sp, nc)
	defer h.stop()

	pool := &sync.Pool{New: func() any { return make([]byte, 0, 64) }}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	for i := 0; i < 64; i++ {
		_ = sp.EnqueueControl([]byte{0x08})
		_ = sp.EnqueueReliable(ctx, []byte{0x05})
		f := sendpath.NewImmutableFrame(pool, append(pool.Get().([]byte)[:0], byte(i)))
		_ = sp.TrySendBroadcast(f)
		f.Release()
		_ = sp.TryEnqueueEvent([]byte{0x22})
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if sp.Depth(sendpath.ChControl) == 0 && sp.Written(sendpath.ChControl) > 0 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("control channel starved: depth=%d written=%d",
		sp.Depth(sendpath.ChControl), sp.Written(sendpath.ChControl))
}

// TestWrittenAccountingMatchesFrames 批量写不得改变「写出帧数」的记账语义。
func TestWrittenAccountingMatchesFrames(t *testing.T) {
	sp := sendpath.New(isoChannels())
	nc := &blockingConn{}
	h := startIsolationWriter(t, sp, nc)
	defer h.stop()

	const frames = 32
	for i := 0; i < frames; i++ {
		if err := sp.EnqueueReliable(context.Background(), []byte{0x05, 0x06}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	waitClass(t, sp, sendpath.ChReliable, frames)
	if got := nc.written.Load(); got != frames*2 {
		t.Fatalf("bytes written=%d, want %d", got, frames*2)
	}
	if st := h.w.Stats(); st.Frames != frames {
		t.Fatalf("writer frames=%d, want %d", st.Frames, frames)
	}
}

// TestFramesAreNotCorruptedByBatching 批量合并必须保持帧边界与顺序。
func TestFramesAreNotCorruptedByBatching(t *testing.T) {
	sp := sendpath.New(isoChannels())
	nc := &recordingConn{}
	w := sendpath.NewWriter(sp, nc, sendpath.WriteConfig{WriteDeadline: time.Second}, log.New(nopWriter{}, "", 0))
	done := make(chan struct{})
	go func() { defer close(done); w.Run() }()
	for i := 0; i < 10; i++ {
		raw, err := protocol.AppendFrame(nil, protocol.TypePing, uint32(i+1), []byte{byte(i)})
		if err != nil {
			t.Fatal(err)
		}
		if err := sp.EnqueueReliable(context.Background(), raw); err != nil {
			t.Fatal(err)
		}
	}
	waitClass(t, sp, sendpath.ChReliable, 10)
	w.Stop()
	<-done
	sp.Close()

	pos := 0
	for i := 0; i < 10; i++ {
		f, used, err := protocol.Decode(nc.buf[pos:])
		if err != nil {
			t.Fatalf("decode frame %d: %v", i, err)
		}
		if f.Sequence != uint32(i+1) || len(f.Body) != 1 || f.Body[0] != byte(i) {
			t.Fatalf("frame %d corrupted: seq=%d body=%v", i, f.Sequence, f.Body)
		}
		pos += used
	}
	if pos != len(nc.buf) {
		t.Fatalf("trailing %d bytes after last frame", len(nc.buf)-pos)
	}
}

type recordingConn struct {
	mu  sync.Mutex
	buf []byte
}

func (c *recordingConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.buf = append(c.buf, p...)
	return len(p), nil
}

func (c *recordingConn) Read([]byte) (int, error)         { return 0, net.ErrClosed }
func (c *recordingConn) Close() error                     { return nil }
func (c *recordingConn) LocalAddr() net.Addr              { return regressionAddr{} }
func (c *recordingConn) RemoteAddr() net.Addr             { return regressionAddr{} }
func (c *recordingConn) SetDeadline(time.Time) error      { return nil }
func (c *recordingConn) SetReadDeadline(time.Time) error  { return nil }
func (c *recordingConn) SetWriteDeadline(time.Time) error { return nil }

func waitClass(t *testing.T, sp *sendpath.SendPath, cls sendpath.ChannelClass, want int64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if sp.Written(cls) >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("%v written=%d, want >=%d", cls, sp.Written(cls), want)
}
