package transport

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"relayx/phase5.5/internal/config"
	"relayx/phase5.5/internal/protocol"
)

// captureConn is a net.Conn that records every byte written to it, plus how
// many write deadlines were armed. It does not implement the internal writev
// fast path, so net.Buffers.WriteTo falls back to one Write per frame; the
// deadline counter therefore measures how many batches the writer flushed.
type captureConn struct {
	mu        sync.Mutex
	writes    int
	deadlines int
	data      []byte
}

func newCaptureConn() *captureConn { return &captureConn{} }

func (c *captureConn) Read([]byte) (int, error) { return 0, io.EOF }

func (c *captureConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.writes++
	c.data = append(c.data, p...)
	c.mu.Unlock()
	return len(p), nil
}

func (c *captureConn) Close() error        { return nil }
func (c *captureConn) LocalAddr() net.Addr { return testAddr("local") }
func (c *captureConn) RemoteAddr() net.Addr {
	return testAddr("remote")
}
func (c *captureConn) SetDeadline(time.Time) error     { return nil }
func (c *captureConn) SetReadDeadline(time.Time) error { return nil }
func (c *captureConn) SetWriteDeadline(t time.Time) error {
	if t.IsZero() {
		return nil
	}
	c.mu.Lock()
	c.deadlines++
	c.mu.Unlock()
	return nil
}

// snapshot returns the number of Write calls and a copy of the captured bytes.
func (c *captureConn) snapshot() (int, []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]byte, len(c.data))
	copy(out, c.data)
	return c.writes, out
}

func (c *captureConn) writeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writes
}

func (c *captureConn) deadlineCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deadlines
}

// TestWriteCallsBatched pre-enqueues 10 data frames, starts the writer and
// verifies that they are flushed as fewer than 10 batches while the
// concatenated stream still decodes to the original frames in order.
func TestWriteCallsBatched(t *testing.T) {
	const frames = 10

	cfg := config.ServerConfig{MaxBody: testMaxBody, SendQueueSize: frames}
	nc := newCaptureConn()
	c := newConn(1, nc, cfg, nil, testLogger(), nil)
	defer c.Close()

	want := make([]*protocol.Frame, 0, frames)
	total := 0
	for i := 0; i < frames; i++ {
		f := &protocol.Frame{
			Type:  protocol.Data,
			MsgID: uint64(i + 1),
			Body:  []byte(fmt.Sprintf("batched-%d", i)),
		}
		want = append(want, f)
		total += protocol.HeaderSize + len(f.Body)
		if err := c.Send(f); err != nil {
			t.Fatalf("Send frame %d: %v", i, err)
		}
	}

	c.wg.Add(1)
	go c.writeLoop()

	waitFor(t, 2*time.Second, func() bool {
		_, data := nc.snapshot()
		return len(data) >= total
	}, "all batches to be written")

	_, data := nc.snapshot()
	c.Close()
	c.wg.Wait()

	// Coalescing happens inside net.Buffers.WriteTo, which only hands the whole
	// batch to one writev syscall on a stream socket. captureConn takes the
	// per-frame fallback, so the batch count is observed through the single
	// write deadline writeBatch arms per batch.
	batches := nc.deadlineCount()
	if batches >= frames {
		t.Fatalf("write batches = %d, want < %d for %d frames", batches, frames, frames)
	}

	r := bufio.NewReader(bytes.NewReader(data))
	for i, f := range want {
		got, err := protocol.Decode(r, testMaxBody)
		if err != nil {
			t.Fatalf("decode frame %d: %v", i, err)
		}
		if got.Type != f.Type || got.MsgID != f.MsgID || !bytes.Equal(got.Body, f.Body) {
			t.Fatalf("frame %d = {%v msg_id=%d body=%q}, want {%v msg_id=%d body=%q}",
				i, got.Type, got.MsgID, got.Body, f.Type, f.MsgID, f.Body)
		}
	}
	t.Logf("write_batches=%d write_calls=%d frames=%d", batches, nc.writeCount(), frames)
}

// TestSendQueueByteBudget fills the byte budget without a writer and verifies
// that Send fails with ErrSendQueueFull once the budget would be exceeded and
// that the connection is closed.
func TestSendQueueByteBudget(t *testing.T) {
	const budget = 200

	cfg := config.ServerConfig{MaxBody: testMaxBody, SendQueueSize: 64, SendQueueBytes: budget}
	nc := newBlockingConn()
	c := newConn(1, nc, cfg, nil, testLogger(), nil)
	defer c.Close()

	f := &protocol.Frame{Type: protocol.Data, Body: make([]byte, 32)}
	var err error
	for i := 0; i < 16; i++ {
		f.MsgID = uint64(i + 1)
		err = c.Send(f)
		if reserved := c.queuedBytes.Load(); reserved > budget {
			t.Fatalf("reserved bytes = %d after Send %d, budget = %d", reserved, i, budget)
		}
		if err != nil {
			break
		}
	}

	if !errors.Is(err, ErrSendQueueFull) {
		t.Fatalf("Send() error = %v, want %v", err, ErrSendQueueFull)
	}
	if reserved := c.queuedBytes.Load(); reserved > budget {
		t.Fatalf("reserved bytes = %d after rejection, budget = %d", reserved, budget)
	}
	if !c.closed.Load() {
		t.Fatal("connection was not closed after the byte budget was exhausted")
	}
	if got := c.reasonString(); got != "send queue full" {
		t.Fatalf("close reason = %q, want %q", got, "send queue full")
	}
}

// TestControlFramePriority enqueues data frames before a PONG, starts the
// writer afterwards and verifies the control frame is flushed first.
func TestControlFramePriority(t *testing.T) {
	cfg := config.ServerConfig{MaxBody: testMaxBody, SendQueueSize: 16}
	nc := newCaptureConn()
	c := newConn(1, nc, cfg, nil, testLogger(), nil)
	defer c.Close()

	for i := 0; i < 5; i++ {
		if err := c.Send(&protocol.Frame{Type: protocol.Data, MsgID: uint64(i + 1), Body: []byte("data")}); err != nil {
			t.Fatalf("Send data %d: %v", i, err)
		}
	}
	const pongMsgID = 99
	if err := c.Send(&protocol.Frame{Type: protocol.Pong, MsgID: pongMsgID}); err != nil {
		t.Fatalf("Send PONG: %v", err)
	}

	c.wg.Add(1)
	go c.writeLoop()

	waitFor(t, 2*time.Second, func() bool {
		_, data := nc.snapshot()
		return len(data) >= protocol.HeaderSize
	}, "first frame to be written")

	_, data := nc.snapshot()
	if len(data) < protocol.HeaderSize {
		t.Fatalf("captured %d bytes, want at least %d", len(data), protocol.HeaderSize)
	}
	if got := protocol.FrameType(data[3]); got != protocol.Pong {
		t.Fatalf("first written frame type = %v, want PONG", got)
	}

	first, err := protocol.Decode(bufio.NewReader(bytes.NewReader(data)), testMaxBody)
	if err != nil {
		t.Fatalf("decode first frame: %v", err)
	}
	if first.Type != protocol.Pong || first.MsgID != pongMsgID {
		t.Fatalf("first frame = {%v msg_id=%d}, want {PONG msg_id=%d}", first.Type, first.MsgID, pongMsgID)
	}

	c.Close()
	c.wg.Wait()
}

// writeTimeoutError implements net.Error with Timeout() == true.
type writeTimeoutError struct{}

func (writeTimeoutError) Error() string   { return "i/o timeout" }
func (writeTimeoutError) Timeout() bool   { return true }
func (writeTimeoutError) Temporary() bool { return true }

// writeTimeoutConn is a net.Conn whose Write blocks until the configured write
// deadline expires and then fails with a timeout error.
type writeTimeoutConn struct {
	mu        sync.Mutex
	deadline  time.Time
	closed    chan struct{}
	closeOnce sync.Once
}

func newWriteTimeoutConn() *writeTimeoutConn {
	return &writeTimeoutConn{closed: make(chan struct{})}
}

func (c *writeTimeoutConn) Read([]byte) (int, error) { return 0, io.EOF }

func (c *writeTimeoutConn) Write([]byte) (int, error) {
	c.mu.Lock()
	deadline := c.deadline
	c.mu.Unlock()
	if deadline.IsZero() {
		deadline = time.Now().Add(time.Second)
	}

	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-timer.C:
		return 0, writeTimeoutError{}
	case <-c.closed:
		return 0, net.ErrClosed
	}
}

func (c *writeTimeoutConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func (c *writeTimeoutConn) LocalAddr() net.Addr  { return testAddr("local") }
func (c *writeTimeoutConn) RemoteAddr() net.Addr { return testAddr("remote") }
func (c *writeTimeoutConn) SetDeadline(time.Time) error {
	return nil
}
func (c *writeTimeoutConn) SetReadDeadline(time.Time) error { return nil }
func (c *writeTimeoutConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	c.deadline = t
	c.mu.Unlock()
	return nil
}

// TestWriteTimeout verifies that a blocking Write is aborted by the write
// deadline and closes the connection with the "write timeout" reason.
func TestWriteTimeout(t *testing.T) {
	cfg := config.ServerConfig{MaxBody: testMaxBody, SendQueueSize: 4, WriteTimeout: 50 * time.Millisecond}
	nc := newWriteTimeoutConn()
	c := newConn(1, nc, cfg, nil, testLogger(), nil)
	defer c.Close()

	c.wg.Add(1)
	go c.writeLoop()

	if err := c.Send(&protocol.Frame{Type: protocol.Data, MsgID: 7, Body: []byte("slow")}); err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	waitFor(t, 2*time.Second, func() bool { return c.closed.Load() }, "connection to close after write timeout")
	c.wg.Wait()

	if got := c.reasonString(); got != "write timeout" {
		t.Fatalf("close reason = %q, want %q", got, "write timeout")
	}
}

// deadlineCountingConn wraps a net.Conn and counts SetReadDeadline calls so the
// reader's deadline refresh strategy can be observed.
type deadlineCountingConn struct {
	net.Conn
	calls atomic.Int64
}

func (c *deadlineCountingConn) SetReadDeadline(t time.Time) error {
	c.calls.Add(1)
	return c.Conn.SetReadDeadline(t)
}

// TestReadDeadlineNotResetPerFrame sends a burst of frames through a net.Pipe
// and verifies the reader does not install a fresh deadline for every frame.
// With a one-minute idle window the half-window rule never fires, so only the
// initial deadline is set.
func TestReadDeadlineNotResetPerFrame(t *testing.T) {
	const (
		frames = 50
		gap    = 2 * time.Millisecond
	)

	serverSide, peer := net.Pipe()
	defer peer.Close()

	counting := &deadlineCountingConn{Conn: serverSide}
	var handled atomic.Int64
	handler := func(c *Conn, f *protocol.Frame) error {
		handled.Add(1)
		return nil
	}

	// A one-minute idle timeout keeps the refresh threshold at 30s, far beyond
	// the duration of this test, so no refresh is expected.
	cfg := config.ServerConfig{MaxBody: testMaxBody, IdleTimeout: time.Minute}
	c := newConn(1, counting, cfg, handler, testLogger(), nil)
	defer c.Close()

	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		c.readLoop()
	}()

	for i := 0; i < frames; i++ {
		if err := peer.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatalf("SetWriteDeadline: %v", err)
		}
		f := &protocol.Frame{Type: protocol.Data, MsgID: uint64(i + 1), Body: []byte("frame")}
		if err := protocol.Encode(peer, f, testMaxBody); err != nil {
			t.Fatalf("write frame %d: %v", i, err)
		}
		time.Sleep(gap)
	}

	waitFor(t, 2*time.Second, func() bool { return handled.Load() == frames }, "all frames to be handled")

	if err := peer.Close(); err != nil {
		t.Fatalf("close peer: %v", err)
	}
	select {
	case <-readDone:
	case <-time.After(2 * time.Second):
		t.Fatal("readLoop did not exit within 2s after the peer closed")
	}

	if got := counting.calls.Load(); got > 2 {
		t.Fatalf("SetReadDeadline calls = %d for %d frames, want <= 2", got, frames)
	}
}

// TestIdleTimeoutCountsFromLastFrame verifies that the reader's early deadline
// (which may be armed only half an idle window after the last frame) is
// extended so an active-then-silent connection is closed ~idleTimeout after
// the last frame, matching the Phase 0 idle-timeout semantics.
func TestIdleTimeoutCountsFromLastFrame(t *testing.T) {
	const idleTimeout = 300 * time.Millisecond

	cfg := config.ServerConfig{
		IdleTimeout:       idleTimeout,
		HeartbeatInterval: time.Hour,
		HeartbeatTimeout:  time.Hour,
	}
	srv := startTestServer(t, cfg)
	c := dialClient(t, srv.Addr())

	send := func(msgID uint64, body []byte) {
		t.Helper()
		if err := c.send(&protocol.Frame{Type: protocol.Data, MsgID: msgID, Body: body}); err != nil {
			t.Fatalf("send frame %d: %v", msgID, err)
		}
		if err := c.readData(msgID, body, false, testIOTimeout); err != nil {
			t.Fatalf("read echo %d: %v", msgID, err)
		}
	}

	send(1, []byte("first"))
	time.Sleep(150 * time.Millisecond)
	send(2, []byte("second"))

	// The client now goes silent. The server's read deadline may have been
	// armed only half an idle window after frame 2, so it must be extended to
	// a full idle window measured from that frame.
	lastFrameAt := time.Now()

	if err := c.conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	_, err := c.r.ReadByte()
	if err == nil {
		t.Fatal("connection stayed open, want idle timeout close")
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Fatal("connection still open after 3s, idle timeout did not fire")
	}

	elapsed := time.Since(lastFrameAt)
	t.Logf("connection closed %s after the last frame (idle-timeout=%s)", elapsed, idleTimeout)
	if elapsed < 250*time.Millisecond {
		t.Fatalf("connection closed %s after the last frame, want >= 250ms", elapsed)
	}
	if elapsed > 1500*time.Millisecond {
		t.Fatalf("connection closed %s after the last frame, want <= 1.5s", elapsed)
	}

	waitFor(t, 2*time.Second, func() bool { return srv.ActiveConnections() == 0 }, "connection cleanup after idle timeout")
}

// goroutineStack captures all goroutine stacks, growing the buffer if needed.
func goroutineStack() []byte {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return buf[:n]
		}
		buf = make([]byte, 2*len(buf))
	}
}

// TestConnTwoGoroutinesPerConnection verifies the two-goroutine model: every
// connection must own exactly one readLoop and one writeLoop goroutine and no
// dedicated heartbeat goroutine.
func TestConnTwoGoroutinesPerConnection(t *testing.T) {
	const conns = 20

	const (
		readSym  = "transport.(*Conn).readLoop"
		writeSym = "transport.(*Conn).writeLoop"
	)

	type fixture struct {
		c    *Conn
		peer net.Conn
		done chan struct{}
	}

	fixtures := make([]*fixture, 0, conns)
	for i := 0; i < conns; i++ {
		serverSide, peer := net.Pipe()
		cfg := config.ServerConfig{MaxBody: testMaxBody, SendQueueSize: 8}
		c := newConn(uint64(i+1), serverSide, cfg, nil, testLogger(), nil)
		f := &fixture{c: c, peer: peer, done: make(chan struct{})}
		go func() {
			c.run()
			close(f.done)
		}()
		fixtures = append(fixtures, f)
	}

	// Always tear the connections down so a failed assertion cannot leak
	// goroutines into the remaining tests.
	t.Cleanup(func() {
		for _, f := range fixtures {
			f.c.Close()
			_ = f.peer.Close()
		}
	})

	// Wait until both loops are running for every connection so the snapshot
	// taken below is deterministic.
	waitFor(t, 2*time.Second, func() bool {
		stack := string(goroutineStack())
		return strings.Count(stack, readSym) == conns && strings.Count(stack, writeSym) == conns
	}, "read/write loops to be running for every connection")

	stack := string(goroutineStack())
	reads := strings.Count(stack, readSym)
	writes := strings.Count(stack, writeSym)
	heartbeats := strings.Count(stack, "heartbeatLoop")
	t.Logf("goroutines: readLoop=%d writeLoop=%d heartbeatLoop=%d conns=%d", reads, writes, heartbeats, conns)

	if reads != conns {
		t.Fatalf("readLoop goroutines = %d, want %d", reads, conns)
	}
	if writes != conns {
		t.Fatalf("writeLoop goroutines = %d, want %d", writes, conns)
	}
	if heartbeats != 0 {
		t.Fatalf("heartbeatLoop goroutines = %d, want 0", heartbeats)
	}

	for _, f := range fixtures {
		f.c.Close()
		_ = f.peer.Close()
	}
	for i, f := range fixtures {
		select {
		case <-f.done:
		case <-time.After(2 * time.Second):
			t.Fatalf("conn %d: run did not return after Close", i)
		}
	}

	// All loops must disappear from a subsequent snapshot. A short retry
	// window absorbs the scheduler delay between loop exit and stack capture.
	deadline := time.Now().Add(2 * time.Second)
	for {
		stack = string(goroutineStack())
		reads = strings.Count(stack, readSym)
		writes = strings.Count(stack, writeSym)
		if reads == 0 && writes == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("loops still present after shutdown: readLoop=%d writeLoop=%d", reads, writes)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestHeartbeatSentByWriter verifies the writer goroutine drives the heartbeat
// timer and emits PING frames even without a dedicated heartbeat goroutine.
func TestHeartbeatSentByWriter(t *testing.T) {
	cfg := config.ServerConfig{
		MaxBody:           testMaxBody,
		SendQueueSize:     8,
		HeartbeatInterval: 20 * time.Millisecond,
		HeartbeatTimeout:  time.Hour,
	}
	nc := newCaptureConn()
	c := newConn(1, nc, cfg, nil, testLogger(), nil)
	defer func() {
		c.Close()
		c.wg.Wait()
	}()

	c.wg.Add(1)
	go c.writeLoop()

	var data []byte
	waitFor(t, time.Second, func() bool {
		_, data = nc.snapshot()
		return len(data) >= protocol.HeaderSize
	}, "heartbeat PING to be written")

	f, err := protocol.Decode(bufio.NewReader(bytes.NewReader(data)), testMaxBody)
	if err != nil {
		t.Fatalf("decode captured heartbeat frame: %v", err)
	}
	if f.Type != protocol.Ping {
		t.Fatalf("captured frame type = %v, want PING", f.Type)
	}
	if f.MsgID == 0 {
		t.Fatal("PING msg_id = 0, want a positive message ID")
	}
}

// TestHeartbeatTimeoutClosesConn verifies the writer closes the connection
// with the heartbeat timeout reason when no inbound frame arrives in time.
func TestHeartbeatTimeoutClosesConn(t *testing.T) {
	cfg := config.ServerConfig{
		MaxBody:           testMaxBody,
		SendQueueSize:     8,
		HeartbeatInterval: 20 * time.Millisecond,
		HeartbeatTimeout:  40 * time.Millisecond,
	}
	nc := newCaptureConn()
	c := newConn(1, nc, cfg, nil, testLogger(), nil)
	defer func() {
		c.Close()
		c.wg.Wait()
	}()

	// Pretend the last inbound frame arrived long ago.
	c.lastInbound.Store(time.Now().Add(-time.Hour).UnixNano())

	c.wg.Add(1)
	go c.writeLoop()

	waitFor(t, 2*time.Second, func() bool { return c.closed.Load() }, "heartbeat timeout to close the connection")

	if got := c.reasonString(); !strings.Contains(got, "heartbeat timeout") {
		t.Fatalf("close reason = %q, want it to contain %q", got, "heartbeat timeout")
	}
}

// TestCloseWithFrameFlushesClose verifies that CloseWithFrame delivers the
// CLOSE frame to the peer before the socket is closed: the client must decode
// CLOSE first and only then see EOF. It runs over a real TCP connection, so it
// exercises the writer path that a duplicate login kicks a connection through.
func TestCloseWithFrameFlushesClose(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		nc, err := ln.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- nc
	}()

	client, err := net.DialTimeout("tcp", ln.Addr().String(), testIOTimeout)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	var serverSide net.Conn
	select {
	case serverSide = <-accepted:
		if serverSide == nil {
			t.Fatal("accept failed")
		}
	case <-time.After(testIOTimeout):
		t.Fatal("accept timed out")
	}

	cfg := config.ServerConfig{MaxBody: testMaxBody, SendQueueSize: 8}
	c := newConn(1, serverSide, cfg, nil, testLogger(), nil)
	c.wg.Add(1)
	go c.writeLoop()
	defer func() {
		c.Close()
		c.wg.Wait()
	}()

	if err := c.CloseWithFrame(&protocol.Frame{Type: protocol.Close}, "replaced by new login"); err != nil {
		t.Fatalf("CloseWithFrame() = %v, want nil", err)
	}

	if err := client.SetReadDeadline(time.Now().Add(testIOTimeout)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	r := bufio.NewReader(client)
	f, err := protocol.Decode(r, testMaxBody)
	if err != nil {
		t.Fatalf("read CLOSE frame: %v", err)
	}
	if f.Type != protocol.Close {
		t.Fatalf("frame type = %v, want CLOSE", f.Type)
	}

	if err := client.SetReadDeadline(time.Now().Add(testIOTimeout)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if _, err := protocol.Decode(r, testMaxBody); !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("read after CLOSE = %v, want EOF/net.ErrClosed", err)
	}

	// The reason passed to CloseWithFrame must survive the writer's own close.
	if got := c.reasonString(); got != "replaced by new login" {
		t.Fatalf("close reason = %q, want %q", got, "replaced by new login")
	}
	c.wg.Wait()
}

// encodeQueuedFrame encodes f the way Send does and charges the connection's
// queue budget, so writeBatch's release path can be observed.
func encodeQueuedFrame(t *testing.T, c *Conn, f *protocol.Frame) []byte {
	t.Helper()
	b := getEncodeBuf(protocol.HeaderSize + len(f.Body))
	b, err := protocol.AppendFrame(b[:0], f, c.maxBody)
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	c.queuedBytes.Add(int64(len(b)))
	return b
}

// loopbackPair returns both ends of a local TCP connection.
func loopbackPair(t *testing.T) (server, client net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	accepted := make(chan net.Conn, 1)
	go func() {
		nc, err := ln.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- nc
	}()

	client, err = net.DialTimeout("tcp", ln.Addr().String(), testIOTimeout)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	select {
	case server = <-accepted:
		if server == nil {
			t.Fatal("accept failed")
		}
	case <-time.After(testIOTimeout):
		t.Fatal("accept timed out")
	}
	return server, client
}

// TestNodeFrameRejectedOnClientConn verifies that a node-to-node frame on a
// client connection is treated as a protocol violation: the handler is never
// called and the connection is closed with an explicit reason.
func TestNodeFrameRejectedOnClientConn(t *testing.T) {
	serverSide, peer := net.Pipe()
	defer peer.Close()

	var handled atomic.Int64
	handler := func(c *Conn, f *protocol.Frame) error {
		handled.Add(1)
		return nil
	}

	cfg := config.ServerConfig{MaxBody: testMaxBody, IdleTimeout: time.Minute}
	c := newConn(1, serverSide, cfg, handler, testLogger(), nil)
	defer c.Close()

	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		c.readLoop()
	}()

	body, err := protocol.AppendNodeHelloBody(nil, &protocol.NodeHelloBody{
		ProtocolVersion: protocol.Version,
		NodeID:          "node-b",
		AdvertiseAddr:   "127.0.0.1:9100",
	})
	if err != nil {
		t.Fatalf("AppendNodeHelloBody() error = %v", err)
	}
	if err := peer.SetWriteDeadline(time.Now().Add(testIOTimeout)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}
	if err := protocol.Encode(peer, &protocol.Frame{Type: protocol.NodeHello, MsgID: 1, Body: body}, testMaxBody); err != nil {
		t.Fatalf("write NODE_HELLO: %v", err)
	}

	select {
	case <-readDone:
	case <-time.After(testIOTimeout):
		t.Fatal("readLoop did not exit after a node frame")
	}

	if got := handled.Load(); got != 0 {
		t.Fatalf("handler calls = %d, want 0 for a node frame", got)
	}
	if got := c.reasonString(); got != "node frame on client connection" {
		t.Fatalf("close reason = %q, want %q", got, "node frame on client connection")
	}
}

// TestWriteBatchOrderAndClose verifies that successive batches reach the wire
// frame by frame and in order, that a batch mixing a CLOSE frame stops the
// writer with the CLOSE flush reason, and that the queue budget is fully
// released on every batch.
func TestWriteBatchOrderAndClose(t *testing.T) {
	server, client := loopbackPair(t)

	cfg := config.ServerConfig{MaxBody: testMaxBody, SendQueueSize: 8}
	c := newConn(1, server, cfg, nil, testLogger(), nil)
	defer c.Close()

	batches := [][]*protocol.Frame{
		{
			{Type: protocol.Data, MsgID: 1, Body: []byte("one")},
			{Type: protocol.Data, MsgID: 2, Body: []byte("two")},
		},
		{
			{Type: protocol.Data, MsgID: 3, Body: []byte("three")},
		},
		{
			{Type: protocol.Data, MsgID: 4, Body: []byte("four")},
			{Type: protocol.Close},
			{Type: protocol.Data, MsgID: 5, Body: []byte("five")},
		},
	}

	var want []*protocol.Frame
	for i, frames := range batches {
		batch := make([][]byte, 0, len(frames))
		for _, f := range frames {
			batch = append(batch, encodeQueuedFrame(t, c, f))
		}
		want = append(want, frames...)

		if keep := c.writeBatch(batch); keep != (i < len(batches)-1) {
			t.Fatalf("batch %d: writeBatch() = %v, want %v", i, keep, i < len(batches)-1)
		}
		if got := c.queuedBytes.Load(); got != 0 {
			t.Fatalf("batch %d: queuedBytes = %d, want 0", i, got)
		}
	}

	if got := c.reasonString(); got != "close frame sent" {
		t.Fatalf("close reason = %q, want %q", got, "close frame sent")
	}

	r := bufio.NewReader(client)
	for i, f := range want {
		if err := client.SetReadDeadline(time.Now().Add(testIOTimeout)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		got, err := protocol.Decode(r, testMaxBody)
		if err != nil {
			t.Fatalf("decode frame %d: %v", i, err)
		}
		if got.Type != f.Type || got.MsgID != f.MsgID || !bytes.Equal(got.Body, f.Body) {
			t.Fatalf("frame %d = {%v msg_id=%d body=%q}, want {%v msg_id=%d body=%q}",
				i, got.Type, got.MsgID, got.Body, f.Type, f.MsgID, f.Body)
		}
	}
}
