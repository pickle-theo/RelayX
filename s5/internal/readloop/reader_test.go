package readloop

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"relayx/s5/internal/pool"
	"relayx/s5/protocol"
)

// pipeConn 把一个 io.Reader 包装成 net.Conn，并统计 Read/SetReadDeadline 次数。
// 用真实的 io.Reader 保证「半包/粘包」行为与 Socket 一致（内存管道语义等价）。
type pipeConn struct {
	r         io.Reader
	reads     atomic.Int64
	deadlines atomic.Int64
}

func (c *pipeConn) Read(p []byte) (int, error)  { c.reads.Add(1); return c.r.Read(p) }
func (c *pipeConn) Write(p []byte) (int, error) { return len(p), nil }
func (c *pipeConn) Close() error                { return nil }
func (c *pipeConn) LocalAddr() net.Addr         { return fakeAddr{} }
func (c *pipeConn) RemoteAddr() net.Addr        { return fakeAddr{} }
func (c *pipeConn) SetDeadline(time.Time) error { return nil }
func (c *pipeConn) SetReadDeadline(time.Time) error {
	c.deadlines.Add(1)
	return nil
}
func (c *pipeConn) SetWriteDeadline(time.Time) error { return nil }

type fakeAddr struct{}

func (fakeAddr) Network() string { return "pipe" }
func (fakeAddr) String() string  { return "pipe" }

// recorder 收集收到的帧类型与序号。
type recorder struct {
	mu     sync.Mutex
	types  []byte
	bodyLn []int
}

func (r *recorder) HandleFrame(f protocol.Frame) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.types = append(r.types, f.Type)
	r.bodyLn = append(r.bodyLn, len(f.Body))
}

func encodeFrames(t *testing.T, n int, bodySize int) []byte {
	t.Helper()
	var buf []byte
	for i := 0; i < n; i++ {
		body := bytes.Repeat([]byte{byte(i)}, bodySize)
		var err error
		buf, err = protocol.AppendFrame(buf, protocol.TypePing, uint32(i+1), body)
		if err != nil {
			t.Fatalf("append frame %d: %v", i, err)
		}
	}
	return buf
}

func TestReaderDecodesFramesAcrossBatch(t *testing.T) {
	data := encodeFrames(t, 16, 32)
	nc := &pipeConn{r: bytes.NewReader(data)}
	r := New(nc, Config{})
	rec := &recorder{}
	err := r.Run(rec)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("run err=%v, want EOF", err)
	}
	if len(rec.types) != 16 {
		t.Fatalf("frames=%d, want 16", len(rec.types))
	}
	for i, typ := range rec.types {
		if typ != protocol.TypePing {
			t.Fatalf("frame %d type=0x%02x", i, typ)
		}
		if rec.bodyLn[i] != 32 {
			t.Fatalf("frame %d bodyLen=%d", i, rec.bodyLn[i])
		}
	}
}

// TestReaderTimeCallPerBatchNotPerFrame 是 S5 §4.3 的量化断言：
// 一次 Read 返回的 N 帧只读一次时钟（S4 是每帧一次）。
func TestReaderTimeCallPerBatchNotPerFrame(t *testing.T) {
	data := encodeFrames(t, 64, 8)
	nc := &pipeConn{r: bytes.NewReader(data)}
	r := New(nc, Config{})
	rec := &recorder{}
	_ = r.Run(rec)
	st := r.Stats()
	if st.Frames != 64 {
		t.Fatalf("frames=%d, want 64", st.Frames)
	}
	if st.TimeCalls > st.Reads*1 { // 每次 Read 至多一次时钟读取
		t.Fatalf("time calls=%d for %d reads / %d frames", st.TimeCalls, st.Reads, st.Frames)
	}
	if st.TimeCalls >= st.Frames {
		t.Fatalf("time calls=%d, must be far below frames=%d", st.TimeCalls, st.Frames)
	}
}

// TestReaderCoarseDeadlineRefresh 断言读 Deadline 是粗粒度刷新：
// IdleTimeout 已配置时，每帧都刷新会让 deadlines == frames；粗粒度下应远小于帧数。
func TestReaderCoarseDeadlineRefresh(t *testing.T) {
	data := encodeFrames(t, 100, 8)
	nc := &pipeConn{r: bytes.NewReader(data)}
	r := New(nc, Config{IdleTimeout: time.Second, RefreshSlack: time.Hour})
	rec := &recorder{}
	_ = r.Run(rec)
	st := r.Stats()
	if st.DeadlineRefreshes > 1 {
		t.Fatalf("deadline refreshes=%d for one long read, want <=1", st.DeadlineRefreshes)
	}
	if nc.deadlines.Load() != st.DeadlineRefreshes {
		t.Fatalf("conn deadline calls=%d, stats=%d", nc.deadlines.Load(), st.DeadlineRefreshes)
	}
	if st.Frames != 100 {
		t.Fatalf("frames=%d", st.Frames)
	}
}

func TestReaderHandlesPartialFrame(t *testing.T) {
	full := encodeFrames(t, 1, 100)
	// 拆成 3 段喂入，验证半包累积。
	nc := &pipeConn{r: &chunkReader{chunks: [][]byte{full[:10], full[10:60], full[60:]}}}
	r := New(nc, Config{})
	rec := &recorder{}
	err := r.Run(rec)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("run err=%v, want EOF", err)
	}
	if len(rec.types) != 1 || rec.bodyLn[0] != 100 {
		t.Fatalf("types=%v bodyLn=%v", rec.types, rec.bodyLn)
	}
}

func TestReaderRejectsCorruptFrame(t *testing.T) {
	data := encodeFrames(t, 1, 8)
	data[0] = 0x00 // 破坏 Magic
	nc := &pipeConn{r: bytes.NewReader(data)}
	r := New(nc, Config{})
	err := r.Run(&recorder{})
	if !errors.Is(err, protocol.ErrBadMagic) {
		t.Fatalf("err=%v, want ErrBadMagic", err)
	}
	if st := r.Stats(); st.DecodeErrors != 1 {
		t.Fatalf("decode errors=%d, want 1", st.DecodeErrors)
	}
}

func TestReaderBufferFull(t *testing.T) {
	// 合法帧头声明 body=100，但缓冲仅 24 字节：填满后仍无完整帧 → ErrBufferFull。
	frame := encodeFrames(t, 1, 100)
	nc := &pipeConn{r: bytes.NewReader(frame[:24])}
	r := New(nc, Config{BufferSize: 24})
	err := r.Run(&recorder{})
	if !errors.Is(err, ErrBufferFull) {
		t.Fatalf("err=%v, want ErrBufferFull", err)
	}
}

func TestReaderPoolBufferReturnedOnClose(t *testing.T) {
	r := New(&pipeConn{r: bytes.NewReader(nil)}, Config{})
	if r.Buffer() == nil {
		t.Fatal("expected buffer")
	}
	r.Close()
	if r.Buffer() != nil {
		t.Fatal("expected buffer released on Close")
	}
}

// TestReaderPooledBufferReused 守护「连接级读缓冲不随建连分配」：
// 默认 MaxFrameSize 读缓冲必须落在大档内并被复用（0 allocs/op）。
func TestReaderPooledBufferReused(t *testing.T) {
	p := pool.New(pool.Config{})
	if p.Config().Large < protocol.MaxFrameSize {
		t.Fatalf("pool large tier=%d must cover MaxFrameSize=%d", p.Config().Large, protocol.MaxFrameSize)
	}
	nc := &pipeConn{r: bytes.NewReader(nil)}
	before := p.Stats().News
	r1 := New(nc, Config{Pool: p})
	if cap(r1.Buffer()) < protocol.MaxFrameSize {
		t.Fatalf("cap=%d", cap(r1.Buffer()))
	}
	addr1 := &r1.Buffer()[:1][0]
	r1.Close()
	r2 := New(nc, Config{Pool: p})
	addr2 := &r2.Buffer()[:1][0]
	r2.Close()
	if addr1 != addr2 {
		t.Fatal("read buffer was not reused from pool (new allocation per connection)")
	}
	if grown := p.Stats().News - before; grown > 1 {
		t.Fatalf("pool allocated %d new buffers for 2 connections, want <=1", grown)
	}
}

func TestReaderIdleTimeoutWhenConfigured(t *testing.T) {
	// 无数据 + IdleTimeout：Connect 的 Read 立即 EOF，用 timeoutReader 模拟静默连接。
	nc := &pipeConn{r: &silentReader{}}
	r := New(nc, Config{IdleTimeout: 10 * time.Millisecond, RefreshSlack: time.Millisecond})
	err := r.Run(&recorder{})
	if err == nil {
		t.Fatal("expected error from silent reader")
	}
}

func TestReaderLastActivityAdvances(t *testing.T) {
	data := encodeFrames(t, 1, 4)
	r := New(&pipeConn{r: bytes.NewReader(data)}, Config{})
	if r.LastActivity() != 0 {
		t.Fatal("lastActivity should start at 0")
	}
	_ = r.Run(&recorder{})
	if r.LastActivity() == 0 {
		t.Fatal("lastActivity not updated after read")
	}
}

// chunkReader 按块返回数据，块之间不自动合并（模拟分包到达）。
type chunkReader struct {
	chunks [][]byte
	idx    int
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if c.idx >= len(c.chunks) {
		return 0, io.EOF
	}
	n := copy(p, c.chunks[c.idx])
	c.idx++
	return n, nil
}

// silentReader 模拟一个永不返回数据的连接（Read 立即返回 timeout）。
type silentReader struct{}

func (s *silentReader) Read([]byte) (int, error) {
	var ne net.Error = timeoutError{}
	return 0, ne
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }
