package gateway

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"relayx/s1/internal/config"
	"relayx/s1/internal/identity"
	"relayx/s1/internal/protocol"
	"relayx/s1/internal/session"
)

var testSecret = []byte("test-secret")

type gwClient struct {
	t      *testing.T
	nc     net.Conn
	seq    protocol.SequenceTracker
	auth   *identity.Authenticator
	tenant string
	user   string
	device string
}

func startGateway(t *testing.T) (*Gateway, string) {
	t.Helper()
	cfg := config.Default()
	cfg.TokenSecret = testSecret
	cfg.HeartbeatInterval = time.Hour // 测试中不触发心跳
	gw := New(Options{Cfg: cfg, Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = gw.ConnManager().Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = gw.ConnManager().Shutdown(ctx)
	})
	time.Sleep(20 * time.Millisecond)
	return gw, ln.Addr().String()
}

func newGWClient(t *testing.T, addr, tenant, user, device string) *gwClient {
	t.Helper()
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	_ = nc.SetDeadline(time.Now().Add(3 * time.Second))
	return &gwClient{t: t, nc: nc, auth: identity.New(testSecret), tenant: tenant, user: user, device: device}
}

func (c *gwClient) send(t byte, body []byte) {
	seq, _ := c.seq.Next()
	b, err := protocol.Encode(protocol.Frame{Type: t, Sequence: seq, Body: body}, nil)
	if err != nil {
		c.t.Fatal(err)
	}
	if _, err := c.nc.Write(b); err != nil {
		c.t.Fatal(err)
	}
}

func (c *gwClient) read() protocol.Frame {
	header := make([]byte, protocol.HeaderSize)
	if _, err := io.ReadFull(c.nc, header); err != nil {
		c.t.Fatalf("read header: %v", err)
	}
	length := int(binary.BigEndian.Uint32(header[4:8]))
	if length > protocol.MaxBodySize {
		c.t.Fatalf("frame too large: %d", length)
	}
	buf := make([]byte, protocol.HeaderSize+length)
	copy(buf, header)
	if _, err := io.ReadFull(c.nc, buf[protocol.HeaderSize:]); err != nil {
		c.t.Fatalf("read body: %v", err)
	}
	f, _, err := protocol.Decode(buf)
	if err != nil {
		c.t.Fatal(err)
	}
	return f
}

func (c *gwClient) login() {
	p := identity.Producer{ID: "u:" + c.user, Kind: "user", TenantID: c.tenant}
	tok, _ := c.auth.Issue(p, time.Hour)
	body, _ := protocol.EncodeLogin(tok, c.device)
	c.send(protocol.TypeLogin, body)
	ack := c.read()
	if ack.Type != protocol.TypeLoginAck {
		c.t.Fatalf("expect LOGIN_ACK, got 0x%02X", ack.Type)
	}
	caps, err := protocol.DecodeLoginAck(ack.Body)
	if err != nil || caps.Capabilities != 0 {
		c.t.Fatalf("capabilities = %+v err=%v, want 0", caps, err)
	}
}

func sendBody(tenant, user, device, payload string) []byte {
	b, _ := protocol.EncodeSend(protocol.SendBody{
		TenantID: tenant, UserID: user, DeviceID: device, Payload: []byte(payload),
	})
	return b
}

func TestLoginAndDeliver(t *testing.T) {
	_, addr := startGateway(t)
	a := newGWClient(t, addr, "t1", "alice", "phone")
	defer a.nc.Close()
	b := newGWClient(t, addr, "t1", "bob", "phone")
	defer b.nc.Close()
	a.login()
	b.login()
	a.send(protocol.TypeSend, sendBody("t1", "bob", "phone", "hello"))
	f := b.read()
	if f.Type != protocol.TypeDeliver {
		t.Fatalf("expect DELIVER, got 0x%02X", f.Type)
	}
	d, err := protocol.DecodeDeliver(f.Body)
	if err != nil || d.From != "u:alice" || string(d.Payload) != "hello" {
		t.Fatalf("deliver = %+v err=%v", d, err)
	}
}

func TestMultiDeviceFanout(t *testing.T) {
	_, addr := startGateway(t)
	sender := newGWClient(t, addr, "t1", "alice", "phone")
	defer sender.nc.Close()
	sender.login()
	d1 := newGWClient(t, addr, "t1", "bob", "p1")
	d2 := newGWClient(t, addr, "t1", "bob", "p2")
	defer d1.nc.Close()
	defer d2.nc.Close()
	d1.login()
	d2.login()

	sender.send(protocol.TypeSend, sendBody("t1", "bob", "", "fanout"))
	for _, c := range []*gwClient{d1, d2} {
		f := c.read()
		if f.Type != protocol.TypeDeliver {
			t.Fatalf("expect DELIVER, got 0x%02X", f.Type)
		}
	}
}

func TestAuthRejected(t *testing.T) {
	_, addr := startGateway(t)
	nc, _ := net.Dial("tcp", addr)
	defer nc.Close()
	_ = nc.SetDeadline(time.Now().Add(3 * time.Second))
	c := &gwClient{t: t, nc: nc, seq: protocol.SequenceTracker{}}
	c.send(protocol.TypeLogin, mustLoginBody("invalid.token"))
	f := c.read()
	if f.Type != protocol.TypeClose {
		t.Fatalf("expect CLOSE on auth failure, got 0x%02X", f.Type)
	}
	if len(f.Body) != 0 {
		t.Fatalf("S1 CLOSE body must be empty, got %x", f.Body)
	}
}

func mustLoginBody(token string) []byte {
	b, _ := protocol.EncodeLogin(token, "d")
	return b
}

func TestDuplicateLoginReplacesOld(t *testing.T) {
	_, addr := startGateway(t)
	old := newGWClient(t, addr, "t1", "bob", "phone")
	defer old.nc.Close()
	old.login()

	newC := newGWClient(t, addr, "t1", "bob", "phone")
	defer newC.nc.Close()
	newC.login()

	// 旧连接应收到 CLOSE（body 空）。
	f := old.read()
	if f.Type != protocol.TypeClose || len(f.Body) != 0 {
		t.Fatalf("old session expect empty CLOSE, got 0x%02X body=%x", f.Type, f.Body)
	}

	// 路由应指向新会话：alice 发消息，newC 收到，old 收不到。
	a := newGWClient(t, addr, "t1", "alice", "phone")
	defer a.nc.Close()
	a.login()
	a.send(protocol.TypeSend, sendBody("t1", "bob", "phone", "to-new"))
	got := newC.read()
	if got.Type != protocol.TypeDeliver {
		t.Fatalf("new session expect DELIVER, got 0x%02X", got.Type)
	}
}

func TestRouteMissAfterDisconnect(t *testing.T) {
	_, addr := startGateway(t)
	a := newGWClient(t, addr, "t1", "alice", "phone")
	defer a.nc.Close()
	a.login()
	b := newGWClient(t, addr, "t1", "bob", "phone")
	b.login()
	b.nc.Close()
	time.Sleep(50 * time.Millisecond)

	a.send(protocol.TypeSend, sendBody("t1", "bob", "phone", "x"))
	f := a.read()
	if f.Type != protocol.TypeError {
		t.Fatalf("expect ERROR on route miss, got 0x%02X", f.Type)
	}
}

func TestRegistryLookup(t *testing.T) {
	r := session.NewRegistry()
	ep := session.Endpoint{TenantID: "t", UserID: "u", DeviceID: "d"}
	s := session.NewSession("s1", ep, "u:u", 0)
	s.Activate()
	if _, err := r.Register(&session.Entry{Session: s}); err != nil {
		t.Fatal(err)
	}
	if got, err := r.Lookup(ep); err != nil || got != s {
		t.Fatalf("lookup got %v err=%v", got, err)
	}
	if !r.Unregister(ep, s) {
		t.Fatal("unregister failed")
	}
	if _, err := r.Lookup(ep); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("expect ErrNotFound, got %v", err)
	}
}
