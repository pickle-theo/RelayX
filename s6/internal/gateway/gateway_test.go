package gateway

import (
	"bufio"
	"context"
	"crypto/tls"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"relayx/s6/internal/backend"
	"relayx/s6/internal/config"
	"relayx/s6/internal/model"
	"relayx/s6/protocol"
)

type testClient struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
	seq  uint32
}

func (c *testClient) send(typ byte, body []byte) {
	c.t.Helper()
	c.seq++
	raw, err := protocol.Encode(protocol.Frame{Type: typ, Sequence: c.seq, Body: body}, nil)
	if err != nil {
		c.t.Fatal(err)
	}
	if _, err := c.conn.Write(raw); err != nil {
		c.t.Fatal(err)
	}
}

func (c *testClient) login(user, device string) {
	c.t.Helper()
	body, err := protocol.EncodeLogin(user, device)
	if err != nil {
		c.t.Fatal(err)
	}
	c.send(protocol.TypeLogin, body)
	f, _, err := readFrame(c.r)
	if err != nil {
		c.t.Fatalf("read login ack: %v", err)
	}
	if f.Type != protocol.TypeLoginAck {
		c.t.Fatalf("expected LOGIN_ACK, got 0x%02x", f.Type)
	}
}

func (c *testClient) readWithin(d time.Duration) (protocol.Frame, error) {
	if err := c.conn.SetReadDeadline(time.Now().Add(d)); err != nil {
		return protocol.Frame{}, err
	}
	defer c.conn.SetReadDeadline(time.Time{})
	f, _, err := readFrame(c.r)
	return f, err
}

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestConfig() config.Config {
	cfg := config.Default()
	cfg.NodeID = "gw-test"
	cfg.Backend = "memory"
	cfg.ClientAddr = "127.0.0.1:0"
	cfg.InternalAddr = "127.0.0.1:0"
	cfg.HeartbeatInterval = 0 // 测试不跑后台心跳，避免 TTL 抖动
	return cfg
}

func newTestGateway(t *testing.T) *Gateway {
	t.Helper()
	gw, err := New(Options{Cfg: newTestConfig(), Log: newTestLogger(), Backend: backend.NewMemoryBackend()})
	if err != nil {
		t.Fatal(err)
	}
	if err := gw.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := gw.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})
	return gw
}

func (g *Gateway) dialClient(t *testing.T) *testClient {
	t.Helper()
	g.mu.Lock()
	addr := g.listener.Addr().String()
	g.mu.Unlock()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &testClient{t: t, conn: conn, r: bufio.NewReader(conn)}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !cond() {
		t.Fatal("condition not met before timeout")
	}
}

func deliverBody(id, payload string) []byte {
	return protocol.EncodeDeliver(protocol.DeliverBody{
		MessageID: id, ProducerID: "alice", Sender: "alice", Payload: []byte(payload),
	})
}

// TestStaleUnbindKeepsNewSession 守护 LLD §5.1：旧连接的延迟解绑不能把
// 同端点新连接从本节点会话表中移除（否则新连接在线却收不到投递）。
func TestStaleUnbindKeepsNewSession(t *testing.T) {
	gw := newTestGateway(t)
	oldConn := gw.dialClient(t)
	oldConn.login("alice", "d1")

	newConn := gw.dialClient(t)
	newConn.login("alice", "d1")

	// 关闭旧连接：其 handler 退出时会执行延迟 unbind。
	if err := oldConn.conn.Close(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool {
		gw.mu.Lock()
		defer gw.mu.Unlock()
		return len(gw.clients) == 1
	})

	if err := gw.Enqueue(context.Background(), "default", "alice", "d1", protocol.TypeDeliver, deliverBody("m1", "hi")); err != nil {
		t.Fatalf("new session must survive stale unbind: %v", err)
	}
	f, err := newConn.readWithin(2 * time.Second)
	if err != nil {
		t.Fatalf("new connection did not receive delivery: %v", err)
	}
	if f.Type != protocol.TypeDeliver {
		t.Fatalf("expected DELIVER, got 0x%02x", f.Type)
	}
}

// TestReloginSameConnectionKeepsSession 守护同连接重登录：新会话先登记再解绑旧会话。
func TestReloginSameConnectionKeepsSession(t *testing.T) {
	gw := newTestGateway(t)
	c := gw.dialClient(t)
	c.login("alice", "d1")
	c.login("alice", "d1")

	if err := gw.Enqueue(context.Background(), "default", "alice", "d1", protocol.TypeDeliver, deliverBody("m1", "hi")); err != nil {
		t.Fatalf("session must be usable after re-login: %v", err)
	}
	if f, err := c.readWithin(2 * time.Second); err != nil || f.Type != protocol.TypeDeliver {
		t.Fatalf("re-login delivery: frame=%v err=%v", f, err)
	}
}

// TestShutdownClosesIdleClients 守护停机：未登录/空闲连接也必须被关闭，
// 否则 handleClient 阻塞在 Read，wg.Wait 永不返回。
func TestShutdownClosesIdleClients(t *testing.T) {
	gw := newTestGateway(t)
	idle := gw.dialClient(t) // 不发送 LOGIN

	done := make(chan error, 1)
	go func() { done <- gw.Shutdown(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown blocked on idle client")
	}

	_ = idle.conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := idle.conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("idle client connection must be closed on shutdown")
	}
}

// TestOnNackClearsLocalCache 守护 LLD §4.4 C：对端回 NACK 即证明本地路由过期，
// 下一次重试必须重新查 Redis。
func TestOnNackClearsLocalCache(t *testing.T) {
	gw := newTestGateway(t)
	ep := model.EndpointKey{TenantID: "default", UserID: "alice", DeviceID: "d1"}
	gw.router.SetLocalCache(ep, model.NodeBinding{NodeID: "gw-other", Version: 7})

	gw.OnNack(context.Background(), "gw-other", protocol.NackBody{
		TenantID: ep.TenantID, UserID: ep.UserID, DeviceID: ep.DeviceID,
		Reason: protocol.NackVersionMismatch, VersionHint: 7,
	})
	if _, ok := gw.router.GetLocalCache(ep); ok {
		t.Fatal("NACK must clear the stale local route cache")
	}
}

// TestStartFailsWhenInternalAddrBusy 守护启动顺序：内部端口绑定失败必须在
// Start 返回错误，而不是后台日志里悄悄失败、对外仍宣称启动成功。
func TestStartFailsWhenInternalAddrBusy(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()

	cfg := newTestConfig()
	cfg.InternalAddr = busy.Addr().String()
	b := backend.NewMemoryBackend()
	gw, err := New(Options{
		Cfg: cfg, Log: newTestLogger(), Backend: b,
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := gw.Start(); err == nil {
		t.Fatal("Start must fail when internal listener cannot bind")
	}
	// 客户端监听必须在失败路径上被关闭，且节点不应注册进后端。
	if _, ok, _ := b.Node(context.Background(), cfg.NodeID, time.Now()); ok {
		t.Fatal("node must not be registered when Start fails")
	}
}
