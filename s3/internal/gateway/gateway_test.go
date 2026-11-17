package gateway

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"relayx/s3/internal/broadcast"
	"relayx/s3/internal/config"
	"relayx/s3/internal/identity"
	"relayx/s3/internal/protocol"
)

var (
	testLoginSecret = []byte("test-login-secret")
	testSubSecret   = []byte("test-subscribe-secret")
)

func startGateway(t *testing.T) (*Gateway, string, *httptest.Server) {
	t.Helper()
	cfg := config.Default()
	cfg.TokenSecret = testLoginSecret
	cfg.BroadcastTokenKeys = map[string][]byte{"dev": testSubSecret}
	gw := New(Options{Cfg: cfg})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = gw.ConnManager().Serve(ln) }()
	access := httptest.NewServer(gw.AccessHandler())
	t.Cleanup(func() {
		access.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = gw.ConnManager().Shutdown(ctx)
	})
	return gw, ln.Addr().String(), access
}

type testClient struct {
	nc       net.Conn
	seq      protocol.SequenceTracker
	tenant   string
	user     string
	device   string
	verifier *broadcast.TokenVerifier
	loginSec []byte
}

func newTestClient(t *testing.T, addr, tenant, user, device string) *testClient {
	t.Helper()
	nc, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return &testClient{nc: nc, tenant: tenant, user: user, device: device,
		verifier: broadcast.NewTokenVerifier(nil), loginSec: testLoginSecret}
}

func (c *testClient) send(t byte, body []byte) {
	seq, _ := c.seq.Next()
	b, err := protocol.Encode(protocol.Frame{Type: t, Sequence: seq, Body: body}, nil)
	if err != nil {
		panic(err)
	}
	if _, err := c.nc.Write(b); err != nil {
		panic(err)
	}
}

func (c *testClient) read(d time.Duration) (protocol.Frame, error) {
	_ = c.nc.SetReadDeadline(time.Now().Add(d))
	h := make([]byte, protocol.HeaderSize)
	if _, err := io.ReadFull(c.nc, h); err != nil {
		return protocol.Frame{}, err
	}
	n := int(h[4])<<24 | int(h[5])<<16 | int(h[6])<<8 | int(h[7])
	buf := make([]byte, protocol.HeaderSize+n)
	copy(buf, h)
	if _, err := io.ReadFull(c.nc, buf[protocol.HeaderSize:]); err != nil {
		return protocol.Frame{}, err
	}
	f, _, err := protocol.Decode(buf)
	return f, err
}

func (c *testClient) login() {
	a := identity.New(c.loginSec)
	tok, err := a.Issue(identity.Producer{ID: "u:" + c.user, Kind: "user", TenantID: c.tenant}, time.Hour)
	if err != nil {
		panic(err)
	}
	body, _ := protocol.EncodeLogin(tok, c.device)
	c.send(protocol.TypeLogin, body)
	f, err := c.read(3 * time.Second)
	if err != nil || f.Type != protocol.TypeLoginAck {
		panic("login ack failed")
	}
}

func (c *testClient) subscribe(topic string, secret []byte) protocol.SubscribeResult {
	tok, err := c.verifier.Issue("dev", secret, broadcast.SubscriptionClaims{
		TenantID: c.tenant, UserID: c.user, Topic: topic,
	}, time.Hour)
	if err != nil {
		panic(err)
	}
	body, _ := protocol.EncodeSubscribe(topic, tok)
	c.send(protocol.TypeSubscribe, body)
	f, err := c.read(3 * time.Second)
	if err != nil || f.Type != protocol.TypeSubscribeAck {
		panic("subscribe ack failed")
	}
	ack, _ := protocol.DecodeSubscribeAck(f.Body)
	return ack.Result
}

func (c *testClient) unsubscribe(topic string) protocol.UnsubscribeResult {
	body, _ := protocol.EncodeUnsubscribe(topic)
	c.send(protocol.TypeUnsubscribe, body)
	for i := 0; i < 500; i++ {
		f, err := c.read(3 * time.Second)
		if err != nil {
			panic("unsubscribe ack failed")
		}
		if f.Type == protocol.TypeUnsubscribeAck {
			ack, _ := protocol.DecodeUnsubscribeAck(f.Body)
			return ack.Result
		}
		// 并发 publish 下 ACK 之前可能先收到若干 BROADCAST，跳过即可。
	}
	panic("unsubscribe ack not received")
}

func (c *testClient) waitBroadcast(topic, payload string) (protocol.BroadcastBody, error) {
	for i := 0; i < 10; i++ {
		f, err := c.read(2 * time.Second)
		if err != nil {
			return protocol.BroadcastBody{}, err
		}
		if f.Type == protocol.TypeBroadcast {
			b, err := protocol.DecodeBroadcast(f.Body)
			if err == nil && b.Topic == topic && (payload == "" || string(b.Payload) == payload) {
				return b, nil
			}
		}
	}
	return protocol.BroadcastBody{}, io.EOF
}

func (c *testClient) noBroadcast(d time.Duration) error {
	_ = c.nc.SetReadDeadline(time.Now().Add(d))
	for {
		f, err := c.read(d)
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return nil
		}
		if err != nil {
			return err
		}
		if f.Type == protocol.TypeBroadcast {
			return io.ErrUnexpectedEOF
		}
	}
}

func publishHTTP(t *testing.T, accessURL, tenant, topic, payload string) int {
	t.Helper()
	body, _ := json.Marshal(map[string]string{
		"topic": topic, "payload": base64.StdEncoding.EncodeToString([]byte(payload)),
	})
	req, _ := http.NewRequest(http.MethodPost, accessURL+"/v1/broadcasts", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	a := identity.New(testLoginSecret)
	tok, _ := a.Issue(identity.Producer{ID: "svc:pub", Kind: "service", TenantID: tenant}, time.Hour)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func TestSubscribeFanoutIntegration(t *testing.T) {
	_, addr, access := startGateway(t)
	a := newTestClient(t, addr, "t1", "alice", "phone")
	b := newTestClient(t, addr, "t1", "bob", "phone")
	z := newTestClient(t, addr, "t1", "carol", "phone")
	defer a.nc.Close()
	defer b.nc.Close()
	defer z.nc.Close()
	a.login()
	b.login()
	z.login()
	if got := a.subscribe("room", testSubSecret); got != protocol.SubscribeOK {
		t.Fatalf("subscribe a = %d", got)
	}
	if got := b.subscribe("room", testSubSecret); got != protocol.SubscribeOK {
		t.Fatalf("subscribe b = %d", got)
	}
	if got := publishHTTP(t, access.URL, "t1", "room", "hi"); got != http.StatusAccepted {
		t.Fatalf("publish status=%d", got)
	}
	for _, c := range []*testClient{a, b} {
		bb, err := c.waitBroadcast("room", "hi")
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		if bb.Seq != 1 {
			t.Fatalf("seq=%d want 1", bb.Seq)
		}
	}
	if err := z.noBroadcast(200 * time.Millisecond); err != nil {
		t.Fatalf("non-subscriber got broadcast: %v", err)
	}
}

func TestSubscribeAckResults(t *testing.T) {
	_, addr, _ := startGateway(t)
	c := newTestClient(t, addr, "t1", "alice", "phone")
	defer c.nc.Close()
	c.login()
	if got := c.subscribe("dup", testSubSecret); got != protocol.SubscribeOK {
		t.Fatalf("first subscribe = %d", got)
	}
	if got := c.subscribe("dup", testSubSecret); got != protocol.SubscribeDuplicate {
		t.Fatalf("duplicate = %d", got)
	}
	if got := c.subscribe("bad", []byte("wrong-secret")); got != protocol.SubscribeBadToken {
		t.Fatalf("bad token = %d", got)
	}
	// token.topic 与帧 topic 不一致 → result=5
	tok, _ := c.verifier.Issue("dev", testSubSecret, broadcast.SubscriptionClaims{
		TenantID: "t1", Topic: "another-topic",
	}, time.Hour)
	body, _ := protocol.EncodeSubscribe("frame-topic", tok)
	c.send(protocol.TypeSubscribe, body)
	f, err := c.read(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ack, _ := protocol.DecodeSubscribeAck(f.Body)
	if ack.Result != protocol.SubscribeBadTopic {
		t.Fatalf("wrong topic = %d", ack.Result)
	}
}

func TestUnsubscribeAndDisconnectCleanup(t *testing.T) {
	gw, addr, _ := startGateway(t)
	c := newTestClient(t, addr, "t1", "alice", "phone")
	_ = c.subscribe("cleanup", testSubSecret) // 未登录连接
	c.login()
	_ = c.subscribe("cleanup", testSubSecret)
	if got := c.unsubscribe("cleanup"); got != protocol.UnsubscribeOK {
		t.Fatalf("unsubscribe = %d", got)
	}
	if got := c.unsubscribe("cleanup"); got != protocol.UnsubscribeNotExists {
		t.Fatalf("double unsubscribe = %d", got)
	}
	if got := gw.Broadcast().Index.Stats().Subscriptions; got != 0 {
		t.Fatalf("subscriptions after unsubscribe=%d", got)
	}
	_ = c.subscribe("leftover", testSubSecret)
	c.nc.Close()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if gw.Broadcast().Index.Stats().Subscriptions == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("connection close did not clean subscriptions: %+v conns=%d", gw.Broadcast().Index.Stats(), gw.ConnManager().Count())
}

// TestSendRejectsCrossTenantTarget 覆盖跨租户注入：已认证生产者不得通过 SEND body
// 指定其他租户的 tenant_id 完成投递；同租户投递必须照常工作。
func TestSendRejectsCrossTenantTarget(t *testing.T) {
	_, addr, _ := startGateway(t)
	alice := newTestClient(t, addr, "t1", "alice", "phone")
	carol := newTestClient(t, addr, "t1", "carol", "phone")
	bob := newTestClient(t, addr, "t2", "bob", "phone")
	defer alice.nc.Close()
	defer carol.nc.Close()
	defer bob.nc.Close()
	alice.login()
	carol.login()
	bob.login()

	// 跨租户：目标 t2/bob，必须被拒绝且不产生 DELIVER。
	body, _ := protocol.EncodeSend(protocol.SendBody{
		TenantID: "t2", UserID: "bob", DeviceID: "phone", Payload: []byte("injected"),
	})
	alice.send(protocol.TypeSend, body)
	f, err := alice.read(3 * time.Second)
	if err != nil {
		t.Fatalf("read ack: %v", err)
	}
	if f.Type != protocol.TypeError {
		t.Fatalf("cross-tenant SEND: frame=0x%02X want ERROR", f.Type)
	}
	if f, err := bob.read(200 * time.Millisecond); err == nil && f.Type == protocol.TypeDeliver {
		t.Fatalf("cross-tenant DELIVER leaked to t2: %q", f.Body)
	}

	// 同租户：正常投递仍然工作。
	body, _ = protocol.EncodeSend(protocol.SendBody{
		TenantID: "t1", UserID: "carol", DeviceID: "phone", Payload: []byte("ok"),
	})
	alice.send(protocol.TypeSend, body)
	f, err = carol.read(3 * time.Second)
	if err != nil {
		t.Fatalf("read deliver: %v", err)
	}
	if f.Type != protocol.TypeDeliver {
		t.Fatalf("same-tenant SEND: frame=0x%02X want DELIVER", f.Type)
	}
	if db, err := protocol.DecodeDeliver(f.Body); err != nil || string(db.Payload) != "ok" {
		t.Fatalf("deliver body=%+v err=%v", db, err)
	}
}

func TestPayloadLimitAndMetrics(t *testing.T) {
	gw, _, access := startGateway(t)
	if got := publishHTTP(t, access.URL, "t1", "big", strings.Repeat("x", (8<<10)+1)); got != http.StatusBadRequest {
		t.Fatalf("oversized publish status=%d", got)
	}
	rendered := gw.Metrics().Render()
	if !strings.Contains(rendered, "broadcast_subscribers") ||
		!strings.Contains(rendered, "broadcast_fanout_frames_total") ||
		!strings.Contains(rendered, "broadcast_fanout_latency_us_bucket") {
		t.Fatalf("broadcast metrics missing from /metrics:\n%s", rendered)
	}
}
