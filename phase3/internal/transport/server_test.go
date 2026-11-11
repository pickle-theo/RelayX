package transport

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"relayx/phase3/internal/config"
	"relayx/phase3/internal/protocol"
)

const (
	testMaxBody   = 1 << 20
	testIOTimeout = 3 * time.Second
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testEchoHandler(c *Conn, f *protocol.Frame) error {
	if f.Type != protocol.Data {
		return nil
	}
	return c.Send(&protocol.Frame{Type: protocol.Data, MsgID: f.MsgID, Body: f.Body})
}

// serverFixture owns a test server and the outcome of its Serve goroutine.
type serverFixture struct {
	*Server
	done chan struct{}
	err  error
}

func startTestServer(t *testing.T, cfg config.ServerConfig) *serverFixture {
	t.Helper()
	srv := New("127.0.0.1:0", cfg, testEchoHandler, testLogger())
	f := &serverFixture{Server: srv, done: make(chan struct{})}
	go func() {
		f.err = srv.Serve()
		close(f.done)
	}()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
		f.waitServe(t, 2*time.Second)
	})
	return f
}

func (f *serverFixture) waitServe(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-f.done:
		if f.err != nil {
			t.Errorf("Serve() error = %v", f.err)
		}
	case <-time.After(timeout):
		t.Errorf("Serve() did not return within %s", timeout)
	}
}

// testClient is a minimal protocol client used by the integration tests.
type testClient struct {
	conn net.Conn
	r    *bufio.Reader
}

func dialClientAddr(addr string) (*testClient, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	return &testClient{conn: conn, r: bufio.NewReader(conn)}, nil
}

func dialClient(t *testing.T, addr string) *testClient {
	t.Helper()
	c, err := dialClientAddr(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(c.Close)
	return c
}

func (c *testClient) Close() { _ = c.conn.Close() }

func (c *testClient) writeRaw(p []byte) error {
	if err := c.conn.SetWriteDeadline(time.Now().Add(testIOTimeout)); err != nil {
		return err
	}
	n, err := c.conn.Write(p)
	if err != nil {
		return err
	}
	if n != len(p) {
		return io.ErrShortWrite
	}
	return nil
}

func (c *testClient) send(f *protocol.Frame) error {
	var buf bytes.Buffer
	if err := protocol.Encode(&buf, f, testMaxBody); err != nil {
		return err
	}
	return c.writeRaw(buf.Bytes())
}

func (c *testClient) read(timeout time.Duration) (*protocol.Frame, error) {
	if err := c.conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	return protocol.Decode(c.r, testMaxBody)
}

// readData waits for the echo of msgID, optionally answering server PINGs so
// heartbeat-enabled servers keep the connection alive.
func (c *testClient) readData(msgID uint64, body []byte, replyPing bool, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("timed out waiting for DATA msg_id=%d", msgID)
		}
		f, err := c.read(remaining)
		if err != nil {
			return fmt.Errorf("read waiting for DATA msg_id=%d: %w", msgID, err)
		}
		switch f.Type {
		case protocol.Ping:
			if replyPing {
				if err := c.send(&protocol.Frame{Type: protocol.Pong, MsgID: f.MsgID}); err != nil {
					return fmt.Errorf("reply to PING msg_id=%d: %w", f.MsgID, err)
				}
			}
		case protocol.Data:
			if f.MsgID != msgID {
				return fmt.Errorf("DATA msg_id = %d, want %d", f.MsgID, msgID)
			}
			if !bytes.Equal(f.Body, body) {
				return fmt.Errorf("DATA body = %q, want %q", f.Body, body)
			}
			return nil
		default:
			return fmt.Errorf("unexpected frame %v while waiting for DATA", f.Type)
		}
	}
}

// waitFor polls cond until it returns true or timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, description)
}

func TestServerEchoManyClients(t *testing.T) {
	srv := startTestServer(t, config.ServerConfig{})

	const (
		clients  = 30
		messages = 10
	)

	errCh := make(chan error, clients)
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := dialClientAddr(srv.Addr())
			if err != nil {
				errCh <- fmt.Errorf("client %d dial: %w", i, err)
				return
			}
			defer c.Close()

			for j := 0; j < messages; j++ {
				msgID := uint64(i*1000 + j)
				body := []byte(fmt.Sprintf("client-%d-msg-%d", i, j))
				if err := c.send(&protocol.Frame{Type: protocol.Data, MsgID: msgID, Body: body}); err != nil {
					errCh <- fmt.Errorf("client %d send msg %d: %w", i, j, err)
					return
				}
				if err := c.readData(msgID, body, false, testIOTimeout); err != nil {
					errCh <- fmt.Errorf("client %d msg %d: %w", i, j, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	waitFor(t, 3*time.Second, func() bool { return srv.ActiveConnections() == 0 }, "connections to be unregistered")
}

func TestServerPingPong(t *testing.T) {
	srv := startTestServer(t, config.ServerConfig{})
	c := dialClient(t, srv.Addr())

	const msgID = 42
	if err := c.send(&protocol.Frame{Type: protocol.Ping, MsgID: msgID}); err != nil {
		t.Fatalf("send PING: %v", err)
	}
	f, err := c.read(testIOTimeout)
	if err != nil {
		t.Fatalf("read PONG: %v", err)
	}
	if f.Type != protocol.Pong {
		t.Fatalf("frame type = %v, want PONG", f.Type)
	}
	if f.MsgID != msgID {
		t.Fatalf("PONG msg_id = %d, want %d", f.MsgID, msgID)
	}
}

func TestServerHeartbeatKeepsConnectionAlive(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heartbeat test in short mode")
	}

	cfg := config.ServerConfig{
		HeartbeatInterval: 50 * time.Millisecond,
		HeartbeatTimeout:  2 * time.Second,
		IdleTimeout:       5 * time.Second,
	}
	srv := startTestServer(t, cfg)
	c := dialClient(t, srv.Addr())

	deadline := time.Now().Add(300 * time.Millisecond)
	pings := 0
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		f, err := c.read(remaining)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				break
			}
			t.Fatalf("read heartbeat: %v", err)
		}
		if f.Type != protocol.Ping {
			t.Fatalf("unexpected frame %v while waiting for heartbeat PING", f.Type)
		}
		pings++
		if err := c.send(&protocol.Frame{Type: protocol.Pong, MsgID: f.MsgID}); err != nil {
			t.Fatalf("reply to PING %d: %v", f.MsgID, err)
		}
	}
	if pings < 2 {
		t.Fatalf("received %d PING frames in 300ms, want at least 2", pings)
	}

	body := []byte("still alive")
	if err := c.send(&protocol.Frame{Type: protocol.Data, MsgID: 1, Body: body}); err != nil {
		t.Fatalf("send DATA after heartbeats: %v", err)
	}
	if err := c.readData(1, body, true, testIOTimeout); err != nil {
		t.Fatalf("connection was not kept alive: %v", err)
	}
	if n := srv.ActiveConnections(); n != 1 {
		t.Fatalf("ActiveConnections() = %d, want 1", n)
	}
}

func TestServerHeartbeatTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heartbeat timeout test in short mode")
	}

	cfg := config.ServerConfig{
		HeartbeatInterval: 50 * time.Millisecond,
		HeartbeatTimeout:  150 * time.Millisecond,
		IdleTimeout:       5 * time.Second,
	}
	srv := startTestServer(t, cfg)
	c := dialClient(t, srv.Addr())

	deadline := time.Now().Add(time.Second)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatal("connection was not closed within 1s")
		}
		_, err := c.read(remaining)
		if err == nil {
			// Deliberately do not answer PING frames.
			continue
		}
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			t.Fatal("connection still open after 1s, heartbeat timeout did not fire")
		}
		break
	}

	waitFor(t, 2*time.Second, func() bool { return srv.ActiveConnections() == 0 }, "connection cleanup after heartbeat timeout")
}

func TestServerIdleTimeout(t *testing.T) {
	srv := startTestServer(t, config.ServerConfig{IdleTimeout: 200 * time.Millisecond})
	c := dialClient(t, srv.Addr())

	// Send nothing; the server's read deadline should expire around 200ms.
	_, err := c.read(time.Second)
	if err == nil {
		t.Fatal("read succeeded, want connection closed by idle timeout")
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Fatal("connection still open after 1s, idle timeout did not fire")
	}

	waitFor(t, 2*time.Second, func() bool { return srv.ActiveConnections() == 0 }, "connection cleanup after idle timeout")
}

func TestServerOnCloseCalledOnce(t *testing.T) {
	var calls atomic.Int64

	// SetOnClose must run before Serve, so this test builds the server by hand
	// instead of using startTestServer.
	srv := New("127.0.0.1:0", config.ServerConfig{}, testEchoHandler, testLogger())
	srv.SetOnClose(func(*Conn) { calls.Add(1) })

	done := make(chan struct{})
	var serveErr error
	go func() {
		serveErr = srv.Serve()
		close(done)
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
		select {
		case <-done:
			if serveErr != nil {
				t.Errorf("Serve() error = %v", serveErr)
			}
		case <-time.After(2 * time.Second):
			t.Error("Serve() did not return within 2s")
		}
	})

	c := dialClient(t, srv.Addr())
	waitFor(t, 2*time.Second, func() bool { return srv.ActiveConnections() == 1 }, "client to be registered")

	c.Close()
	waitFor(t, 2*time.Second, func() bool { return srv.ActiveConnections() == 0 }, "connection to be unregistered")
	waitFor(t, 2*time.Second, func() bool { return calls.Load() > 0 }, "onClose callback")

	// Give a duplicate invocation a chance to show up before asserting.
	time.Sleep(100 * time.Millisecond)
	if n := calls.Load(); n != 1 {
		t.Fatalf("onClose called %d times, want 1", n)
	}
}

func TestServerGracefulShutdown(t *testing.T) {
	srv := startTestServer(t, config.ServerConfig{})

	clients := make([]*testClient, 3)
	for i := range clients {
		clients[i] = dialClient(t, srv.Addr())
	}
	waitFor(t, 2*time.Second, func() bool { return srv.ActiveConnections() == len(clients) }, "clients to be registered")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Shutdown() took %s, want at most 2s", elapsed)
	}
	if n := srv.ActiveConnections(); n != 0 {
		t.Fatalf("ActiveConnections() = %d after Shutdown, want 0", n)
	}

	for i, c := range clients {
		f, err := c.read(testIOTimeout)
		if err != nil {
			t.Fatalf("client %d read CLOSE: %v", i, err)
		}
		if f.Type != protocol.Close {
			t.Fatalf("client %d frame type = %v, want CLOSE", i, f.Type)
		}
	}
}

// blockingConn is a net.Conn whose reads and writes block until Close is
// called. It lets the send queue test fill the queue without a writer.
type blockingConn struct {
	closeOnce sync.Once
	closed    chan struct{}
}

func newBlockingConn() *blockingConn {
	return &blockingConn{closed: make(chan struct{})}
}

func (b *blockingConn) Read([]byte) (int, error) {
	<-b.closed
	return 0, io.EOF
}

func (b *blockingConn) Write([]byte) (int, error) {
	<-b.closed
	return 0, io.ErrClosedPipe
}

func (b *blockingConn) Close() error {
	b.closeOnce.Do(func() { close(b.closed) })
	return nil
}

func (b *blockingConn) LocalAddr() net.Addr              { return testAddr("local") }
func (b *blockingConn) RemoteAddr() net.Addr             { return testAddr("remote") }
func (b *blockingConn) SetDeadline(time.Time) error      { return nil }
func (b *blockingConn) SetReadDeadline(time.Time) error  { return nil }
func (b *blockingConn) SetWriteDeadline(time.Time) error { return nil }
func (b *blockingConn) isClosed() bool {
	select {
	case <-b.closed:
		return true
	default:
		return false
	}
}

type testAddr string

func (a testAddr) Network() string { return "test" }
func (a testAddr) String() string  { return string(a) }

func TestSendQueueFullBackpressure(t *testing.T) {
	cfg := config.ServerConfig{MaxBody: testMaxBody, SendQueueSize: 1}
	nc := newBlockingConn()
	c := newConn(1, nc, cfg, nil, testLogger(), nil)

	if err := c.Send(&protocol.Frame{Type: protocol.Data, MsgID: 1, Body: []byte("first")}); err != nil {
		t.Fatalf("first Send() error = %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.Send(&protocol.Frame{Type: protocol.Data, MsgID: 2, Body: []byte("second")})
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrSendQueueFull) {
			t.Fatalf("second Send() error = %v, want %v", err, ErrSendQueueFull)
		}
	case <-time.After(time.Second):
		t.Fatal("Send() blocked instead of failing fast when the queue is full")
	}

	if !c.closed.Load() {
		t.Fatal("connection was not marked closed after send queue overflow")
	}
	select {
	case <-c.done:
	default:
		t.Fatal("done channel was not closed after send queue overflow")
	}
	if !nc.isClosed() {
		t.Fatal("underlying connection was not closed after send queue overflow")
	}
}

func TestServerNoGoroutineLeak(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping goroutine leak test in short mode")
	}

	baseline := runtime.NumGoroutine()
	srv := startTestServer(t, config.ServerConfig{})

	const clients = 50
	errCh := make(chan error, clients)
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := dialClientAddr(srv.Addr())
			if err != nil {
				errCh <- fmt.Errorf("client %d dial: %w", i, err)
				return
			}
			defer c.Close()

			msgID := uint64(i + 1)
			body := []byte(fmt.Sprintf("leak-%d", i))
			if err := c.send(&protocol.Frame{Type: protocol.Data, MsgID: msgID, Body: body}); err != nil {
				errCh <- fmt.Errorf("client %d send: %w", i, err)
				return
			}
			if err := c.readData(msgID, body, false, testIOTimeout); err != nil {
				errCh <- fmt.Errorf("client %d: %w", i, err)
				return
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	waitFor(t, 3*time.Second, func() bool { return srv.ActiveConnections() == 0 }, "connections to be unregistered")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	srv.waitServe(t, 2*time.Second)

	deadline := time.Now().Add(2 * time.Second)
	for {
		current := runtime.NumGoroutine()
		if current <= baseline+5 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutine count = %d after cleanup, baseline = %d (possible leak)", current, baseline)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestServerStickyAndFragmentedFrames(t *testing.T) {
	srv := startTestServer(t, config.ServerConfig{})
	c := dialClient(t, srv.Addr())

	// Two complete frames in a single write (sticky packets).
	first := &protocol.Frame{Type: protocol.Data, MsgID: 11, Body: []byte("first")}
	second := &protocol.Frame{Type: protocol.Data, MsgID: 12, Body: []byte("second")}
	var sticky bytes.Buffer
	for _, f := range []*protocol.Frame{first, second} {
		if err := protocol.Encode(&sticky, f, testMaxBody); err != nil {
			t.Fatalf("encode sticky frame: %v", err)
		}
	}
	if err := c.writeRaw(sticky.Bytes()); err != nil {
		t.Fatalf("write sticky frames: %v", err)
	}
	for _, f := range []*protocol.Frame{first, second} {
		if err := c.readData(f.MsgID, f.Body, false, testIOTimeout); err != nil {
			t.Fatalf("sticky frames: %v", err)
		}
	}

	// One frame split across two writes with a pause in between.
	third := &protocol.Frame{Type: protocol.Data, MsgID: 13, Body: []byte("fragmented")}
	var fragmented bytes.Buffer
	if err := protocol.Encode(&fragmented, third, testMaxBody); err != nil {
		t.Fatalf("encode fragmented frame: %v", err)
	}
	raw := fragmented.Bytes()
	cut := len(raw) / 2
	if err := c.writeRaw(raw[:cut]); err != nil {
		t.Fatalf("write first half: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if err := c.writeRaw(raw[cut:]); err != nil {
		t.Fatalf("write second half: %v", err)
	}
	if err := c.readData(third.MsgID, third.Body, false, testIOTimeout); err != nil {
		t.Fatalf("fragmented frame: %v", err)
	}
}
