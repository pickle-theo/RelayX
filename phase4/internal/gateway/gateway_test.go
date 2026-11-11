package gateway

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"relayx/phase4/internal/config"
	"relayx/phase4/internal/protocol"
	"relayx/phase4/internal/session"
	"relayx/phase4/internal/store"
	"relayx/phase4/internal/transport"
)

const (
	testMaxBody   = 1 << 20
	testIOTimeout = 3 * time.Second
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// testDBLockKey serializes the database tests of every package. Package test
// binaries run in parallel and share one PostgreSQL database, so each test
// holds this session-level advisory lock while it truncates and uses the
// tables. 0x5258 is the protocol magic "RX".
const testDBLockKey int64 = 0x5258

// lockTestDatabase acquires the cross-package advisory lock on its own
// connection and releases it from t.Cleanup. The lock connection is separate
// from the store pool so a test may close the pool (failure tests) without
// dropping the lock; the lock is always held for exactly one test.
func lockTestDatabase(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open advisory lock pool: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		t.Fatalf("advisory lock connection: %v", err)
	}
	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", testDBLockKey); err != nil {
		_ = conn.Close()
		_ = db.Close()
		t.Fatalf("pg_advisory_lock: %v", err)
	}
	t.Cleanup(func() {
		unlockCtx, unlockCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer unlockCancel()
		if _, err := conn.ExecContext(unlockCtx, "SELECT pg_advisory_unlock($1)", testDBLockKey); err != nil {
			t.Logf("pg_advisory_unlock: %v", err)
		}
		_ = conn.Close()
		_ = db.Close()
	})
}

func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("RELAYX_TEST_DSN")
	if dsn == "" {
		t.Skip("RELAYX_TEST_DSN not set")
	}
	return dsn
}

// openTestStore connects to the local Docker PostgreSQL and resets the three
// Phase 2 tables. Tests are skipped when RELAYX_TEST_DSN is not set.
func openTestStore(t *testing.T) *store.Store {
	t.Helper()
	return openTestStoreWithRetry(t, 0)
}

// openTestStoreWithRetry is openTestStore with an explicit retry backoff base,
// which lets retry tests use a short schedule.
func openTestStoreWithRetry(t *testing.T, retryBase time.Duration) *store.Store {
	t.Helper()
	dsn := testDSN(t)
	lockTestDatabase(t, dsn)

	ctx := context.Background()
	st, err := store.Open(ctx, store.Config{
		DSN:              dsn,
		ConnectTimeout:   5 * time.Second,
		RetryBackoffBase: retryBase,
	})
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("store.Migrate() error = %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, "TRUNCATE deliveries, messages, endpoints RESTART IDENTITY CASCADE"); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
	return st
}

// gatewayFixture is a real gateway listening on a loopback port, backed by the
// test database.
type gatewayFixture struct {
	srv      *transport.Server
	sessions *session.Manager
	store    *store.Store
	handler  *Handler
	done     chan struct{}
	serveErr error
}

func startGateway(t *testing.T) *gatewayFixture {
	t.Helper()
	return startGatewayWith(t, gatewayOptions{})
}

// startGatewayWithConfig is startGateway with an explicit transport config; it
// lets tests shrink the send queue to exercise backpressure.
func startGatewayWithConfig(t *testing.T, cfg config.ServerConfig) *gatewayFixture {
	t.Helper()
	return startGatewayWith(t, gatewayOptions{server: cfg})
}

// gatewayOptions bundles the handler, transport and store settings a test may
// need to override.
type gatewayOptions struct {
	server    config.ServerConfig
	handler   Config
	retryBase time.Duration
}

// startGatewayWith starts a real gateway with the given overrides.
func startGatewayWith(t *testing.T, opts gatewayOptions) *gatewayFixture {
	t.Helper()

	st := openTestStoreWithRetry(t, opts.retryBase)
	sessions := session.NewManager()
	h := New(sessions, st, opts.handler, testLogger())
	srv := transport.New("127.0.0.1:0", opts.server, h.Handle, testLogger())
	srv.SetOnClose(h.OnConnClosed)

	f := &gatewayFixture{srv: srv, sessions: sessions, store: st, handler: h, done: make(chan struct{})}
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

	var count int
	if err := f.store.DB().QueryRow(
		"SELECT count(*) FROM endpoints WHERE user_id = $1 AND device_id = $2", "alice", "phone").Scan(&count); err != nil {
		t.Fatalf("query endpoints: %v", err)
	}
	if count != 1 {
		t.Fatalf("endpoint rows = %d, want 1", count)
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
