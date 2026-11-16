package server

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"relayx/s0/internal/protocol"
)

func startTestServer(t *testing.T, cfg Config) *Server {
	t.Helper()
	srv := New(cfg, log.New(os.Stderr, "[s0-test] ", log.LstdFlags))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	// Serve 在独立 goroutine 中发布 listener；等待其就绪，避免 serverAddr 竞态读到空地址。
	deadline := time.Now().Add(2 * time.Second)
	for serverAddr(srv) == "" {
		if time.Now().After(deadline) {
			t.Fatal("server listener was not published in time")
		}
		time.Sleep(time.Millisecond)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return srv
}

func serverAddr(s *Server) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func encodeFrame(t *testing.T, typ byte, seq uint32, body []byte) []byte {
	t.Helper()
	b, err := protocol.Encode(protocol.Frame{Type: typ, Sequence: seq, Body: body}, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return b
}

func readFrame(t *testing.T, nc net.Conn) protocol.Frame {
	t.Helper()
	// 先精确读固定头，再从 Length 得到 body 长度按量读取：
	// 这样一次 readFrame 只读一帧，不会把粘包的后续帧吞进缓冲。
	header := make([]byte, protocol.HeaderSize)
	if _, err := io.ReadFull(nc, header); err != nil {
		t.Fatalf("read header: %v", err)
	}
	length := binary.BigEndian.Uint32(header[4:8])
	if length > protocol.MaxBodySize {
		t.Fatalf("declared body length %d exceeds max", length)
	}
	frameBuf := make([]byte, protocol.HeaderSize+int(length))
	copy(frameBuf, header)
	if _, err := io.ReadFull(nc, frameBuf[protocol.HeaderSize:]); err != nil {
		t.Fatalf("read body: %v", err)
	}
	f, used, derr := protocol.Decode(frameBuf)
	if derr != nil || used != len(frameBuf) {
		t.Fatalf("decode response: used=%d err=%v", used, derr)
	}
	return f
}

func TestEchoRoundtrip(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HeartbeatInterval = time.Hour // 测试中不触发心跳
	cfg.EnableEcho = true
	srv := startTestServer(t, cfg)

	nc, err := net.Dial("tcp", serverAddr(srv))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer nc.Close()
	_ = nc.SetDeadline(time.Now().Add(2 * time.Second))

	// PING -> PONG
	if _, err := nc.Write(encodeFrame(t, protocol.TypePing, 1, nil)); err != nil {
		t.Fatal(err)
	}
	if got := readFrame(t, nc); got.Type != protocol.TypePong {
		t.Fatalf("expect PONG, got 0x%02X", got.Type)
	}

	// ECHO -> ECHO
	body := []byte("hello")
	if _, err := nc.Write(encodeFrame(t, protocol.TypeEcho, 2, body)); err != nil {
		t.Fatal(err)
	}
	got := readFrame(t, nc)
	if got.Type != protocol.TypeEcho || string(got.Body) != string(body) {
		t.Fatalf("echo mismatch: type=0x%02X body=%q", got.Type, got.Body)
	}
}

func TestFragmentedFrame(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HeartbeatInterval = time.Hour
	cfg.EnableEcho = true
	srv := startTestServer(t, cfg)

	nc, _ := net.Dial("tcp", serverAddr(srv))
	defer nc.Close()
	_ = nc.SetDeadline(time.Now().Add(2 * time.Second))

	frame := encodeFrame(t, protocol.TypeEcho, 1, []byte("fragment"))
	cut := protocol.HeaderSize + 4
	parts := [][]byte{frame[:protocol.HeaderSize], frame[protocol.HeaderSize:cut], frame[cut:]}
	for _, p := range parts {
		_, _ = nc.Write(p)
		time.Sleep(5 * time.Millisecond)
	}
	if got := readFrame(t, nc); string(got.Body) != "fragment" {
		t.Fatalf("fragmented body = %q", got.Body)
	}
}

func TestCoalescedFrames(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HeartbeatInterval = time.Hour
	cfg.EnableEcho = true
	srv := startTestServer(t, cfg)

	nc, _ := net.Dial("tcp", serverAddr(srv))
	defer nc.Close()
	_ = nc.SetDeadline(time.Now().Add(2 * time.Second))

	var all []byte
	for i := 0; i < 3; i++ {
		all = append(all, encodeFrame(t, protocol.TypeEcho, uint32(i+1), []byte(fmt.Sprintf("f%d", i)))...)
	}
	_, _ = nc.Write(all)
	for i := 0; i < 3; i++ {
		got := readFrame(t, nc)
		want := fmt.Sprintf("f%d", i)
		if string(got.Body) != want {
			t.Fatalf("frame %d body = %q, want %q", i, got.Body, want)
		}
	}
}

func TestEchoDisabledReturnsError(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HeartbeatInterval = time.Hour
	cfg.EnableEcho = false
	srv := startTestServer(t, cfg)

	nc, _ := net.Dial("tcp", serverAddr(srv))
	defer nc.Close()
	_ = nc.SetDeadline(time.Now().Add(2 * time.Second))

	_, _ = nc.Write(encodeFrame(t, protocol.TypeEcho, 1, []byte("x")))
	got := readFrame(t, nc)
	if got.Type != protocol.TypeError {
		t.Fatalf("expect ERROR with echo disabled, got 0x%02X", got.Type)
	}
	if len(got.Body) < 2 || binary.BigEndian.Uint16(got.Body[:2]) != protocol.ErrorUnknownFrame {
		t.Fatalf("unexpected error code: %v", got.Body)
	}
}

func TestOversizedFrameRejected(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HeartbeatInterval = time.Hour
	srv := startTestServer(t, cfg)

	nc, _ := net.Dial("tcp", serverAddr(srv))
	defer nc.Close()
	_ = nc.SetDeadline(time.Now().Add(2 * time.Second))

	header := make([]byte, protocol.HeaderSize)
	header[0], header[1], header[2], header[3] = protocol.Magic0, protocol.Magic1, protocol.Version, protocol.TypeEcho
	binary.BigEndian.PutUint32(header[4:8], protocol.MaxBodySize+1)
	binary.BigEndian.PutUint32(header[8:12], 1)
	binary.BigEndian.PutUint16(header[12:14], protocol.CRC16(header[:12]))
	_, _ = nc.Write(header)

	got := readFrame(t, nc)
	if got.Type != protocol.TypeError {
		t.Fatalf("expect ERROR for oversized frame, got 0x%02X", got.Type)
	}
}

func TestConcurrentClients(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HeartbeatInterval = time.Hour
	cfg.EnableEcho = true
	srv := startTestServer(t, cfg)

	const clients = 20
	var wg sync.WaitGroup
	errCh := make(chan error, clients)
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			nc, err := net.Dial("tcp", serverAddr(srv))
			if err != nil {
				errCh <- err
				return
			}
			defer nc.Close()
			_ = nc.SetDeadline(time.Now().Add(3 * time.Second))
			body := []byte(fmt.Sprintf("client-%d", id))
			if _, err := nc.Write(encodeFrame(t, protocol.TypeEcho, 1, body)); err != nil {
				errCh <- err
				return
			}
			got := readFrame(t, nc)
			if string(got.Body) != string(body) {
				errCh <- fmt.Errorf("client %d: body = %q", id, got.Body)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

func TestTruncatedFrameClosesConnection(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HeartbeatInterval = time.Hour
	cfg.EnableEcho = true
	srv := startTestServer(t, cfg)

	nc, _ := net.Dial("tcp", serverAddr(srv))
	defer nc.Close()

	header := make([]byte, protocol.HeaderSize)
	header[0], header[1], header[2], header[3] = protocol.Magic0, protocol.Magic1, protocol.Version, protocol.TypeEcho
	binary.BigEndian.PutUint32(header[4:8], 50) // 声明 50 字节但只发 5 字节
	binary.BigEndian.PutUint32(header[8:12], 1)
	binary.BigEndian.PutUint16(header[12:14], protocol.CRC16(header[:12]))
	_, _ = nc.Write(header)
	_, _ = nc.Write([]byte("12345"))

	_ = nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 16)
	_, err := nc.Read(buf)
	if !errors.Is(err, io.EOF) {
		var ne net.Error
		if !(errors.As(err, &ne) && ne.Timeout()) {
			t.Fatalf("expect EOF/timeout on truncated frame, got %v", err)
		}
	}
}

func TestGracefulShutdown(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HeartbeatInterval = time.Hour
	cfg.EnableEcho = true
	srv := New(cfg, log.New(os.Stderr, "[s0-test] ", log.LstdFlags))
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go func() { _ = srv.Serve(ln) }()

	nc, _ := net.Dial("tcp", ln.Addr().String())
	defer nc.Close()
	_, _ = nc.Write(encodeFrame(t, protocol.TypeEcho, 1, []byte("x")))
	_ = readFrame(t, nc)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// TestConcurrentEnqueueWithHeartbeat 让 heartbeatLoop 与 readLoop 同时调用 enqueue：
// 短周期心跳触发出站 PING，客户端持续发 ECHO 触发出站 ECHO 响应。
// 在 -race 下验证 tracker 无数据竞争，并断言出站序号严格单调（序号与入队顺序一致）。
func TestConcurrentEnqueueWithHeartbeat(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HeartbeatInterval = 10 * time.Millisecond
	cfg.PongTimeout = 3 * time.Second // 足够长，避免测试期间被误判心跳超时
	cfg.EnableEcho = true
	srv := startTestServer(t, cfg)

	nc, err := net.Dial("tcp", serverAddr(srv))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	const frames = 50
	var sendSeq protocol.SequenceTracker // 仅主线程使用
	errCh := make(chan error, 1)

	go func() {
		buf := make([]byte, protocol.MaxFrameSize)
		pending := buf[:0]
		var lastSeq uint32
		got := 0
		_ = nc.SetReadDeadline(time.Now().Add(4 * time.Second))
		for got < frames {
			n, err := nc.Read(buf[len(pending):cap(buf)])
			if n > 0 {
				pending = pending[:len(pending)+n]
				pos := 0
				for pos < len(pending) {
					f, used, derr := protocol.Decode(pending[pos:])
					if errors.Is(derr, protocol.ErrIncomplete) {
						break
					}
					if derr != nil {
						errCh <- derr
						return
					}
					pos += used
					if f.Sequence <= lastSeq {
						errCh <- fmt.Errorf("outbound sequence not monotonic: %d after %d", f.Sequence, lastSeq)
						return
					}
					lastSeq = f.Sequence
					switch f.Type {
					case protocol.TypePing:
						// 服务端不校验入站序号连续性，回固定序号即可。
						b, _ := protocol.Encode(protocol.Frame{Type: protocol.TypePong, Sequence: 1}, nil)
						if _, werr := nc.Write(b); werr != nil {
							errCh <- werr
							return
						}
					case protocol.TypeEcho:
						got++
					}
				}
				remaining := copy(buf, pending[pos:])
				pending = buf[:remaining]
			}
			if err != nil {
				var ne net.Error
				if errors.As(err, &ne) && ne.Timeout() {
					errCh <- fmt.Errorf("read timeout after %d/%d echo responses", got, frames)
				} else {
					errCh <- err
				}
				return
			}
		}
		errCh <- nil
	}()

	for i := 0; i < frames; i++ {
		seq, _ := sendSeq.Next()
		b, _ := protocol.Encode(protocol.Frame{Type: protocol.TypeEcho, Sequence: seq, Body: []byte("x")}, nil)
		if _, err := nc.Write(b); err != nil {
			t.Fatal(err)
		}
		// 慢于心跳间隔，确保两次 ECHO 之间 heartbeatLoop 会发出 PING。
		time.Sleep(30 * time.Millisecond)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("concurrent enqueue test timed out")
	}
}

// raceListener 让测试精确控制 Accept 的返回时机：Close 不打断 Accept，
// 以便在 Shutdown 快照之后才放回一个连接，复现「Accept 与 Shutdown 竞态」窗口。
type raceListener struct {
	addr   net.Addr
	accept chan net.Conn
}

func (l *raceListener) Accept() (net.Conn, error) {
	nc, ok := <-l.accept
	if !ok {
		return nil, errors.New("listener closed")
	}
	return nc, nil
}
func (l *raceListener) Close() error   { return nil } // 故意不打断 Accept
func (l *raceListener) Addr() net.Addr { return l.addr }

// TestShutdownClosesInFlightAccept 验证 Shutdown 已完成 conns 快照后，
// Accept 才返回的连接会被 Serve 直接关闭、不注册、不启动 goroutine，
// 从而不会造成优雅停机遗漏与 goroutine 泄漏。
func TestShutdownClosesInFlightAccept(t *testing.T) {
	ln := &raceListener{addr: &net.TCPAddr{}, accept: make(chan net.Conn, 1)}
	srv := New(DefaultConfig(), log.New(os.Stderr, "[s0-test] ", log.LstdFlags))

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	// 等待 Serve 完成 listener 安装并检查 done（未关闭），随后阻塞在 Accept。
	deadline := time.Now().Add(2 * time.Second)
	for {
		srv.mu.Lock()
		installed := srv.listener != nil
		srv.mu.Unlock()
		if installed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Serve did not install listener")
		}
		time.Sleep(time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- srv.Shutdown(ctx) }()

	// done 关闭即表示 Shutdown 已持锁置位；此后放行 Accept 返回连接，
	// Serve 必须在该锁竞争后看到 done 已关闭并拒绝注册。
	select {
	case <-srv.done:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not close done")
	}

	srvConn, cliConn := net.Pipe()
	defer cliConn.Close()
	ln.accept <- srvConn

	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not exit after in-flight accept during Shutdown")
	}

	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return promptly")
	}

	// 被拒绝的连接应已关闭：客户端读立即得到 EOF。
	_ = cliConn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := cliConn.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("in-flight accepted conn was not closed, read err = %v", err)
	}
}

// TestShutdownBeforeServe 验证 Shutdown 先于 Serve 时，Serve 会自行关闭 listener 并退出，
// 而不是在 Accept 上永久阻塞（#2 竞态回归）。
func TestShutdownBeforeServe(t *testing.T) {
	srv := New(DefaultConfig(), log.New(os.Stderr, "[s0-test] ", log.LstdFlags))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown before Serve: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve after Shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve blocked in Accept after Shutdown (listener not closed)")
	}
}
