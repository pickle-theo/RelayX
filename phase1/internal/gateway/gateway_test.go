package gateway

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"relayx/phase1/internal/config"
	"relayx/phase1/internal/protocol"
	"relayx/phase1/internal/session"
	"relayx/phase1/internal/transport"
)

const (
	testMaxBody   = 1 << 20
	testIOTimeout = 3 * time.Second
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// gatewayFixture is a real gateway listening on a loopback port.
type gatewayFixture struct {
	srv      *transport.Server
	sessions *session.Manager
	done     chan struct{}
	serveErr error
}

func startGateway(t *testing.T) *gatewayFixture {
	t.Helper()
	return startGatewayWithConfig(t, config.ServerConfig{})
}

// startGatewayWithConfig is startGateway with an explicit transport config; it
// lets tests shrink the send queue to exercise backpressure.
func startGatewayWithConfig(t *testing.T, cfg config.ServerConfig) *gatewayFixture {
	t.Helper()

	sessions := session.NewManager()
	h := New(sessions, testLogger())
	srv := transport.New("127.0.0.1:0", cfg, h.Handle, testLogger())
	srv.SetOnClose(h.OnConnClosed)

	f := &gatewayFixture{srv: srv, sessions: sessions, done: make(chan struct{})}
	go func() {
		f.serveErr = srv.Serve()
		close(f.done)
	}()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
		select {
		case <-f.done:
			if f.serveErr != nil {
				t.Errorf("Serve() error = %v", f.serveErr)
			}
		case <-time.After(2 * time.Second):
			t.Error("Serve() did not return within 2s")
		}
	})
	return f
}

type testClient struct {
	conn net.Conn
	r    *bufio.Reader
}

func dialGateway(t *testing.T, addr string) *testClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	c := &testClient{conn: conn, r: bufio.NewReader(conn)}
	t.Cleanup(c.close)
	return c
}

func (c *testClient) close() { _ = c.conn.Close() }

func (c *testClient) send(t *testing.T, f *protocol.Frame) {
	t.Helper()
	var buf bytes.Buffer
	if err := protocol.Encode(&buf, f, testMaxBody); err != nil {
		t.Fatalf("encode %v frame: %v", f.Type, err)
	}
	if err := c.conn.SetWriteDeadline(time.Now().Add(testIOTimeout)); err != nil {
		t.Fatalf("set write deadline: %v", err)
	}
	if _, err := c.conn.Write(buf.Bytes()); err != nil {
		t.Fatalf("write %v frame: %v", f.Type, err)
	}
}

func (c *testClient) read(t *testing.T) *protocol.Frame {
	t.Helper()
	if err := c.conn.SetReadDeadline(time.Now().Add(testIOTimeout)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	f, err := protocol.Decode(c.r, testMaxBody)
	if err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	return f
}

// sendLogin sends a LOGIN frame with the given raw body and returns the
// decoded LOGIN_ACK.
func (c *testClient) sendLogin(t *testing.T, msgID uint64, body []byte) *protocol.LoginAckBody {
	t.Helper()
	c.send(t, &protocol.Frame{Type: protocol.Login, MsgID: msgID, Body: body})

	f := c.read(t)
	if f.Type != protocol.LoginAck {
		t.Fatalf("frame type = %v, want LOGIN_ACK", f.Type)
	}
	if f.MsgID != msgID {
		t.Fatalf("LOGIN_ACK msg_id = %d, want %d", f.MsgID, msgID)
	}
	ack, err := protocol.DecodeLoginAck(f.Body)
	if err != nil {
		t.Fatalf("decode LOGIN_ACK body: %v", err)
	}
	return ack
}

func loginBody(t *testing.T, userID, deviceID string) []byte {
	t.Helper()
	body, err := protocol.AppendLoginBody(nil, &protocol.LoginBody{UserID: userID, DeviceID: deviceID})
	if err != nil {
		t.Fatalf("encode LOGIN body: %v", err)
	}
	return body
}

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

func TestLoginSuccess(t *testing.T) {
	f := startGateway(t)
	c := dialGateway(t, f.srv.Addr())

	ack := c.sendLogin(t, 1, loginBody(t, "alice", "phone"))
	if ack.Status != protocol.LoginStatusOK {
		t.Fatalf("login status = %d, want %d (message %q)", ack.Status, protocol.LoginStatusOK, ack.Message)
	}
	if ack.SessionID == 0 {
		t.Fatal("session id = 0, want a non-zero id")
	}
	if n := f.sessions.ActiveSessions(); n != 1 {
		t.Fatalf("ActiveSessions() = %d, want 1", n)
	}
}

func TestLoginInvalidIdentity(t *testing.T) {
	f := startGateway(t)
	c := dialGateway(t, f.srv.Addr())

	// user "u" followed by a zero-length device id: the decoder rejects it.
	invalid := []byte{0x00, 0x01, 'u', 0x00, 0x00}
	ack := c.sendLogin(t, 2, invalid)
	if ack.Status != LoginStatusInvalid {
		t.Fatalf("login status = %d, want %d", ack.Status, LoginStatusInvalid)
	}
	if ack.Message == "" {
		t.Fatal("LOGIN_ACK message is empty, want a decoder error description")
	}
	if n := f.sessions.ActiveSessions(); n != 0 {
		t.Fatalf("ActiveSessions() = %d, want 0", n)
	}

	// The connection must stay usable after a rejected login.
	body := []byte("still alive")
	c.send(t, &protocol.Frame{Type: protocol.Data, MsgID: 3, Body: body})
	reply := c.read(t)
	if reply.Type != protocol.Data || !bytes.Equal(reply.Body, body) {
		t.Fatalf("after invalid login got frame %v body %q, want DATA %q", reply.Type, reply.Body, body)
	}
}

func TestSendBeforeLogin(t *testing.T) {
	f := startGateway(t)
	c := dialGateway(t, f.srv.Addr())

	sendBody, err := protocol.AppendSendBody(nil, &protocol.SendBody{
		TargetType: protocol.TargetUser,
		Target:     "bob",
		Payload:    []byte("hello"),
	})
	if err != nil {
		t.Fatalf("encode SEND body: %v", err)
	}
	c.send(t, &protocol.Frame{Type: protocol.Send, MsgID: 7, Body: sendBody})

	reply := c.read(t)
	if reply.Type != protocol.Error {
		t.Fatalf("frame type = %v, want ERROR", reply.Type)
	}
	if reply.MsgID != 7 {
		t.Fatalf("ERROR msg_id = %d, want 7", reply.MsgID)
	}
	if got := string(reply.Body); got != "not logged in" {
		t.Fatalf("ERROR body = %q, want %q", got, "not logged in")
	}
	if n := f.sessions.ActiveSessions(); n != 0 {
		t.Fatalf("ActiveSessions() = %d, want 0", n)
	}
}

func TestLoginTwice(t *testing.T) {
	f := startGateway(t)
	c := dialGateway(t, f.srv.Addr())

	first := c.sendLogin(t, 1, loginBody(t, "alice", "phone"))
	if first.Status != protocol.LoginStatusOK {
		t.Fatalf("first login status = %d, want %d", first.Status, protocol.LoginStatusOK)
	}

	second := c.sendLogin(t, 2, loginBody(t, "alice", "tablet"))
	if second.Status != LoginStatusAlreadyLoggedIn {
		t.Fatalf("second login status = %d, want %d", second.Status, LoginStatusAlreadyLoggedIn)
	}
	if second.Message != "already logged in" {
		t.Fatalf("second login message = %q, want %q", second.Message, "already logged in")
	}
	if n := f.sessions.ActiveSessions(); n != 1 {
		t.Fatalf("ActiveSessions() = %d, want 1", n)
	}
	if got := f.sessions.FindByDevice("alice", "phone"); got == nil {
		t.Fatal("original session was replaced by an already-logged-in request")
	}
}

func TestDataEcho(t *testing.T) {
	f := startGateway(t)
	c := dialGateway(t, f.srv.Addr())

	body := []byte("legacy echo payload")
	c.send(t, &protocol.Frame{Type: protocol.Data, MsgID: 11, Body: body})
	reply := c.read(t)
	if reply.Type != protocol.Data {
		t.Fatalf("frame type = %v, want DATA", reply.Type)
	}
	if reply.MsgID != 11 {
		t.Fatalf("DATA msg_id = %d, want 11", reply.MsgID)
	}
	if !bytes.Equal(reply.Body, body) {
		t.Fatalf("DATA body = %q, want %q", reply.Body, body)
	}
}

func TestDisconnectClearsSession(t *testing.T) {
	f := startGateway(t)
	c := dialGateway(t, f.srv.Addr())

	if ack := c.sendLogin(t, 1, loginBody(t, "alice", "phone")); ack.Status != protocol.LoginStatusOK {
		t.Fatalf("login status = %d, want %d", ack.Status, protocol.LoginStatusOK)
	}
	if n := f.sessions.ActiveSessions(); n != 1 {
		t.Fatalf("ActiveSessions() = %d, want 1", n)
	}

	c.close()
	waitFor(t, 2*time.Second, func() bool { return f.sessions.ActiveSessions() == 0 }, "session to be unregistered")
}
