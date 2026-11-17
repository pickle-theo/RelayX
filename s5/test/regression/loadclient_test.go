package regression

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"relayx/s5/internal/loadclient"
	"relayx/s5/internal/readloop"
	"relayx/s5/protocol"
)

// TestLoadClientEndToEnd 用进程内最小服务端验证 L1 客户端契约：
// LOGIN/LOGIN_ACK → SUBSCRIBE/ACK → SEND/SEND_ACK → DELIVER → RECEIPT_ACK → PING/PONG。
// 它保证 cmd/loadgen 的协议交互有自动化覆盖，而不只依赖手工压测。
func TestLoadClientEndToEnd(t *testing.T) {
	srv := startMiniServer(t)
	defer srv.Close()

	cli, err := loadclient.Dial(loadclient.Config{
		Addr: srv.Addr(), Token: "tok", DeviceID: "d1",
		Tenant: "t1", User: "u1", AckTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cli.Close()

	caps, err := cli.Login()
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	if caps != protocol.S4Capabilities {
		t.Fatalf("caps=0x%02X, want 0x%02X", caps, protocol.S4Capabilities)
	}

	res, err := cli.Subscribe("topic-1", "sub-token")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if res != protocol.SubscribeOK {
		t.Fatalf("subscribe result=%d", res)
	}

	payload := loadclient.EncodeLatencyPayload(time.Now().UnixNano(), 1, []byte("hello"))
	sendRes, err := cli.Send("u1", []string{"d1"}, payload)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if sendRes.MessageID == "" || sendRes.State != "ACCEPTED" {
		t.Fatalf("send result=%+v", sendRes)
	}

	select {
	case d := <-cli.Deliver():
		if d.MessageID != sendRes.MessageID {
			t.Fatalf("deliver message=%q, want %q", d.MessageID, sendRes.MessageID)
		}
		sentAt, seq, ok := loadclient.DecodeLatencyPayload(d.Payload)
		if !ok || seq != 1 || sentAt <= 0 {
			t.Fatalf("payload decode: ok=%v seq=%d sentAt=%d", ok, seq, sentAt)
		}
		if err := cli.ReceiptAck(d.MessageID); err != nil {
			t.Fatalf("receipt ack: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("deliver not received")
	}

	if _, err := cli.PingWait(2 * time.Second); err != nil {
		t.Fatalf("ping: %v", err)
	}

	// 服务端应收到 RECEIPT_ACK 与 PING 各一次。
	srv.expect(t, "receipt", 1)
	srv.expect(t, "ping", 1)
}

// TestLoadClientLoginRejected 非法令牌不得拿到 LOGIN_ACK（鉴权前移）。
func TestLoadClientLoginRejected(t *testing.T) {
	srv := startMiniServer(t)
	defer srv.Close()
	cli, err := loadclient.Dial(loadclient.Config{
		Addr: srv.Addr(), Token: "bad", DeviceID: "d1",
		Tenant: "t1", User: "u1", AckTimeout: 300 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cli.Close()
	if _, err := cli.Login(); err == nil {
		t.Fatal("expected login failure for rejected token")
	}
}

// ---------- 最小服务端 ----------

type miniServer struct {
	ln net.Listener

	mu     sync.Mutex
	counts map[string]int
	wg     sync.WaitGroup
}

func startMiniServer(t *testing.T) *miniServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &miniServer{ln: ln, counts: make(map[string]int)}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.serve(nc)
			}()
		}
	}()
	return s
}

func (s *miniServer) Addr() string { return s.ln.Addr().String() }

func (s *miniServer) Close() {
	_ = s.ln.Close()
	s.wg.Wait()
}

func (s *miniServer) bump(kind string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counts[kind]++
}

func (s *miniServer) expect(t *testing.T, kind string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		got := s.counts[kind]
		s.mu.Unlock()
		if got >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	s.mu.Lock()
	got := s.counts[kind]
	s.mu.Unlock()
	t.Fatalf("%s count=%d, want >=%d", kind, got, want)
}

func (s *miniServer) serve(nc net.Conn) {
	defer func() { _ = nc.Close() }()
	sp := newServerSendPath()
	w := newServerWriter(sp, nc)
	go w.run()
	defer w.stop()

	rd := readloop.New(nc, readloop.Config{})
	defer rd.Close()

	var messageSeq int
	_ = rd.Run(handlerFunc(func(f protocol.Frame) {
		switch f.Type {
		case protocol.TypeLogin:
			if lb, err := protocol.DecodeLogin(f.Body); err == nil && lb.Token == "bad" {
				// 模拟鉴权失败：直接断连（S1：鉴权失败不得进入业务路径）。
				_ = nc.Close()
				return
			}
			body := protocol.EncodeLoginAck(protocol.S4Capabilities)
			sp.enqueueControl(protocol.TypeLoginAck, body)
		case protocol.TypeSubscribe:
			sp.enqueueControl(protocol.TypeSubscribeAck, protocol.EncodeSubscribeAck("topic-1", protocol.SubscribeOK, ""))
		case protocol.TypeSend:
			sb, err := protocol.DecodeSend(f.Body)
			if err != nil {
				return
			}
			messageSeq++
			messageID := "m-" + itoa(messageSeq)
			sp.enqueueReliable(protocol.TypeSendAck, protocol.EncodeSendAck(protocol.SendAckBody{
				ClientMsgID: sb.ClientMsgID, MessageID: messageID, PlatformState: "ACCEPTED",
				AcceptedAt: time.Now().UnixMilli(),
			}))
			sp.enqueueReliable(protocol.TypeDeliver, protocol.EncodeDeliver(protocol.DeliverBody{
				MessageID: messageID, ProducerID: "u:u1", Sender: "u1",
				ExpiresAt: time.Now().Add(time.Hour).UnixMilli(), Payload: sb.Payload,
			}))
		case protocol.TypeReceiptAck:
			s.bump("receipt")
		case protocol.TypePing:
			s.bump("ping")
			sp.enqueueControl(protocol.TypePong, nil)
		}
	}))
}

type handlerFunc func(protocol.Frame)

func (h handlerFunc) HandleFrame(f protocol.Frame) { h(f) }

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := v < 0
	if neg {
		v = -v
	}
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// serverSendPath / serverWriter 是测试用的极简写端（不引入 S5 优化路径，
// 保证测试验证的是客户端契约而不是服务端实现）。
type serverSendPath struct {
	mu   sync.Mutex
	cond *sync.Cond
	buf  []byte
}

func newServerSendPath() *serverSendPath {
	sp := &serverSendPath{}
	sp.cond = sync.NewCond(&sp.mu)
	return sp
}

func (s *serverSendPath) push(typ byte, body []byte) {
	raw, err := protocol.AppendFrame(nil, typ, 0, body)
	if err != nil {
		return
	}
	s.mu.Lock()
	s.buf = append(s.buf, raw...)
	s.mu.Unlock()
	s.cond.Signal()
}

func (s *serverSendPath) enqueueControl(typ byte, body []byte)  { s.push(typ, body) }
func (s *serverSendPath) enqueueReliable(typ byte, body []byte) { s.push(typ, body) }

func (s *serverSendPath) take() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.buf
	s.buf = nil
	return out
}

func (s *serverSendPath) wait() {
	s.mu.Lock()
	for len(s.buf) == 0 {
		s.cond.Wait()
	}
	s.mu.Unlock()
}

type serverWriter struct {
	sp     *serverSendPath
	nc     net.Conn
	done   chan struct{}
	closed chan struct{}
}

func newServerWriter(sp *serverSendPath, nc net.Conn) *serverWriter {
	return &serverWriter{sp: sp, nc: nc, done: make(chan struct{}), closed: make(chan struct{})}
}

func (w *serverWriter) run() {
	defer close(w.closed)
	for {
		select {
		case <-w.done:
			w.flush()
			return
		default:
		}
		w.flush()
		time.Sleep(time.Millisecond)
	}
}

func (w *serverWriter) flush() {
	chunk := w.sp.take()
	if len(chunk) == 0 {
		return
	}
	if _, err := w.nc.Write(chunk); err != nil && !errors.Is(err, io.ErrClosedPipe) {
		return
	}
}

func (w *serverWriter) stop() {
	select {
	case <-w.done:
	default:
		close(w.done)
	}
	<-w.closed
}
