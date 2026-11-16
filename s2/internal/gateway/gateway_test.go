package gateway_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"relayx/s2/internal/config"
	"relayx/s2/internal/gateway"
	"relayx/s2/internal/identity"
	"relayx/s2/internal/protocol"
	"relayx/s2/internal/store"
	"relayx/s2/internal/store/memory"
)

type testClient struct {
	conn   net.Conn
	seq    protocol.SequenceTracker
	frames chan protocol.Frame
	mu     sync.Mutex
}

func dial(t *testing.T, addr string) *testClient {
	t.Helper()
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c := &testClient{conn: nc, frames: make(chan protocol.Frame, 128)}
	go func() {
		buf := make([]byte, 1<<16)
		pending := buf[:0]
		for {
			n, err := nc.Read(buf[len(pending):cap(buf)])
			if n > 0 {
				pending = pending[:len(pending)+n]
				pos := 0
				for pos < len(pending) {
					f, used, derr := protocol.Decode(pending[pos:])
					if derr != nil {
						return
					}
					pos += used
					f.Body = append([]byte(nil), f.Body...)
					select {
					case c.frames <- f:
					default:
					}
				}
				remain := copy(buf, pending[pos:])
				pending = buf[:remain]
			}
			if err != nil {
				return
			}
		}
	}()
	return c
}

func (c *testClient) write(f protocol.Frame) error {
	seq, err := c.seq.Next()
	if err != nil {
		return err
	}
	f.Sequence = seq
	b, err := protocol.Encode(f, nil)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err = c.conn.Write(b)
	return err
}

func (c *testClient) wait(typ byte, timeout time.Duration) (protocol.Frame, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case f := <-c.frames:
			if f.Type == typ {
				return f, nil
			}
		case <-timer.C:
			return protocol.Frame{}, context.DeadlineExceeded
		}
	}
}

func (c *testClient) login(t *testing.T, token, device string) {
	t.Helper()
	body, _ := protocol.EncodeLogin(token, device)
	if err := c.write(protocol.Frame{Type: protocol.TypeLogin, Body: body}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.wait(protocol.TypeLoginAck, 3*time.Second); err != nil {
		t.Fatal(err)
	}
}

func (c *testClient) send(t *testing.T, clientMsgID, target string, payload []byte) protocol.SendAckBody {
	t.Helper()
	body, err := protocol.EncodeSend(protocol.SendBody{
		ClientMsgID: clientMsgID, TargetUserID: target, Payload: payload,
		Policy: &protocol.SendPolicy{TTLSeconds: 60, MaxAttempts: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.write(protocol.Frame{Type: protocol.TypeSend, Body: body}); err != nil {
		t.Fatal(err)
	}
	f, err := c.wait(protocol.TypeSendAck, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ack, err := protocol.DecodeSendAck(f.Body)
	if err != nil {
		t.Fatal(err)
	}
	return ack
}

func startGateway(t *testing.T) (*gateway.Gateway, string, *memory.Store, *identity.Authenticator) {
	t.Helper()
	cfg := config.Default()
	cfg.TokenSecret = []byte("test-secret")
	cfg.ScanInterval = 50 * time.Millisecond
	st := memory.New(nil)
	gw := gateway.New(gateway.Options{Cfg: cfg, Store: st})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = gw.Serve(ln) }()
	gw.Start()
	t.Cleanup(func() { _ = gw.Shutdown(context.Background()) })
	return gw, ln.Addr().String(), st, identity.New(cfg.TokenSecret)
}

func token(t *testing.T, auth *identity.Authenticator, name string) string {
	t.Helper()
	s, err := auth.Issue(identity.Producer{ID: "u:" + name, Kind: "user", TenantID: "t1"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func waitState(t *testing.T, st *memory.Store, id, want string) *store.Message {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		m, err := st.Messages().GetByID(context.Background(), id)
		if err == nil && m.PlatformState == want {
			return m
		}
		time.Sleep(20 * time.Millisecond)
	}
	m, _ := st.Messages().GetByID(context.Background(), id)
	t.Fatalf("message %s state=%v want=%s", id, m, want)
	return nil
}

func TestLoginAndReliableDelivery(t *testing.T) {
	_, addr, st, auth := startGateway(t)
	alice, bob := token(t, auth, "alice"), token(t, auth, "bob")
	ca, cb := dial(t, addr), dial(t, addr)
	ca.login(t, alice, "phone")
	cb.login(t, bob, "phone")

	ack := ca.send(t, "c1", "bob", []byte("hello"))
	if ack.PlatformState != store.MessageAccepted {
		t.Fatalf("ack=%+v", ack)
	}
	f, err := cb.wait(protocol.TypeDeliver, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	d, err := protocol.DecodeDeliver(f.Body)
	if err != nil || string(d.Payload) != "hello" || d.ProducerID != "u:alice" {
		t.Fatalf("deliver=%+v err=%v", d, err)
	}
	if err := cb.write(protocol.Frame{Type: protocol.TypeReceiptAck, Body: protocol.EncodeReceiptAck(ack.MessageID)}); err != nil {
		t.Fatal(err)
	}
	m := waitState(t, st, ack.MessageID, store.MessageSettled)
	if m.SourceDeviceID != "phone" {
		t.Fatalf("source_device_id=%q", m.SourceDeviceID)
	}
}

func TestAuthenticationRejection(t *testing.T) {
	_, addr, _, _ := startGateway(t)
	c := dial(t, addr)
	body, _ := protocol.EncodeLogin("bad.token", "phone")
	_ = c.write(protocol.Frame{Type: protocol.TypeLogin, Body: body})
	if _, err := c.wait(protocol.TypeLoginAck, 300*time.Millisecond); err == nil {
		t.Fatal("invalid token was accepted")
	}
}

func TestMultiDeviceFanoutAndIdempotency(t *testing.T) {
	_, addr, st, auth := startGateway(t)
	alice, bob := token(t, auth, "alice"), token(t, auth, "bob")
	ca, b1, b2 := dial(t, addr), dial(t, addr), dial(t, addr)
	ca.login(t, alice, "phone")
	b1.login(t, bob, "phone")
	b2.login(t, bob, "laptop")
	ack := ca.send(t, "fan", "bob", []byte("x"))
	if _, err := b1.wait(protocol.TypeDeliver, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := b2.wait(protocol.TypeDeliver, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	ds, _ := st.Deliveries().ListByMessage(context.Background(), ack.MessageID)
	if len(ds) != 2 {
		t.Fatalf("deliveries=%d", len(ds))
	}
	dup := ca.send(t, "fan", "bob", []byte("x"))
	if !dup.Duplicate || dup.MessageID != ack.MessageID {
		t.Fatalf("idempotency failed: %+v", dup)
	}
}

func TestAccessAPISameIngressPath(t *testing.T) {
	gw, _, _, auth := startGateway(t)
	handler := gw.AccessHandler()
	srv := httptest.NewServer(handler)
	defer srv.Close()

	svcToken, err := auth.Issue(identity.Producer{ID: "svc:billing", Kind: "service", TenantID: "t1"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(map[string]any{
		"client_msg_id": "http-1", "target_user_id": "nobody", "payload": []byte("x"),
	})
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/messages", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+svcToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST status=%d", resp.StatusCode)
	}
	var out struct {
		MessageID     string `json:"message_id"`
		PlatformState string `json:"platform_state"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.PlatformState != store.MessageSettled {
		t.Fatalf("state=%s", out.PlatformState)
	}

	req2, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/messages/"+out.MessageID, nil)
	req2.Header.Set("Authorization", "Bearer "+svcToken)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("GET status=%d", resp2.StatusCode)
	}

	other, _ := auth.Issue(identity.Producer{ID: "svc:other", Kind: "service", TenantID: "t2"}, time.Hour)
	req3, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/messages/"+out.MessageID, nil)
	req3.Header.Set("Authorization", "Bearer "+other)
	resp3, err := http.DefaultClient.Do(req3)
	if err != nil {
		t.Fatal(err)
	}
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-tenant status=%d", resp3.StatusCode)
	}
}
