package gateway_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"relayx/s4/internal/config"
	"relayx/s4/internal/gateway"
	"relayx/s4/internal/identity"
	"relayx/s4/protocol"
	"relayx/s4/sdk"
)

type testEnv struct {
	gw     *gateway.Gateway
	tcp    string
	access string
	auth   *identity.Authenticator
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	cfg := config.Default()
	cfg.EventPollInterval = 5 * time.Millisecond
	cfg.ScanInterval = 50 * time.Millisecond
	cfg.DrainGrace = 20 * time.Millisecond
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	gw := gateway.New(gateway.Options{Cfg: cfg, Log: logger})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = gw.ConnManager().Serve(ln) }()
	srv := httptest.NewServer(gw.AccessHandler())
	gw.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = gw.Shutdown(ctx)
		srv.Close()
		_ = ln.Close()
	})
	return &testEnv{gw: gw, tcp: ln.Addr().String(), access: srv.URL, auth: identity.New(cfg.TokenSecret)}
}

func (e *testEnv) token(t *testing.T, tenant, user string) string {
	t.Helper()
	tok, err := e.auth.Issue(identity.Producer{ID: "u:" + user, Kind: "user", TenantID: tenant}, time.Hour)
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}
	return tok
}

type hooksRecorder struct {
	mu       sync.Mutex
	ready    int
	closed   []sdk.CloseInfo
	settled  []sdk.MessageSettledInfo
	accepted []sdk.MessageAcceptedInfo
}

func (h *hooksRecorder) Hooks(deliver func(sdk.DeliveredInfo)) sdk.Hooks {
	return sdk.Hooks{
		Ready: func() {
			h.mu.Lock()
			h.ready++
			h.mu.Unlock()
		},
		Closed: func(i sdk.CloseInfo) {
			h.mu.Lock()
			h.closed = append(h.closed, i)
			h.mu.Unlock()
		},
		MessageAccepted: func(i sdk.MessageAcceptedInfo) {
			h.mu.Lock()
			h.accepted = append(h.accepted, i)
			h.mu.Unlock()
		},
		MessageSettled: func(i sdk.MessageSettledInfo) {
			h.mu.Lock()
			h.settled = append(h.settled, i)
			h.mu.Unlock()
		},
		Delivered: deliver,
	}
}

func (h *hooksRecorder) snapshot() (int, []sdk.CloseInfo, []sdk.MessageSettledInfo) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.ready, append([]sdk.CloseInfo(nil), h.closed...), append([]sdk.MessageSettledInfo(nil), h.settled...)
}

func waitFor(t *testing.T, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func connectSDK(t *testing.T, addr, access, tok, user, device string, h *hooksRecorder, deliver func(sdk.DeliveredInfo), mask protocol.EventMask) *sdk.Client {
	t.Helper()
	c := sdk.New(sdk.Config{
		Addr: addr, Token: tok, DeviceID: device,
		TenantID: "t1", ProducerID: "u:" + user,
		QueryBaseURL: access, EventMask: mask,
		Hooks:  h.Hooks(deliver),
		Logger: func(format string, args ...any) { t.Logf("sdk[%s]: "+format, append([]any{user}, args...)...) },
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		t.Fatalf("connect %s: %v", user, err)
	}
	return c
}

func TestMessageSettledEventEndToEnd(t *testing.T) {
	e := newTestEnv(t)
	recvHooks := &hooksRecorder{}
	var recv *sdk.Client
	recv = connectSDK(t, e.tcp, e.access, e.token(t, "t1", "bob"), "bob", "dev-b", recvHooks, func(d sdk.DeliveredInfo) {
		if err := recv.SendReceipt(d.MessageID); err != nil {
			t.Logf("receipt: %v", err)
		}
	}, 0)
	defer recv.Close()

	sendHooks := &hooksRecorder{}
	send := connectSDK(t, e.tcp, e.access, e.token(t, "t1", "alice"), "alice", "dev-a", sendHooks, nil, 0)
	defer send.Close()

	waitFor(t, "both ready", func() bool {
		r1, _, _ := recvHooks.snapshot()
		r2, _, _ := sendHooks.snapshot()
		return r1 == 1 && r2 == 1
	})
	if !send.PendingEnabled() {
		t.Fatal("pending tracker should be enabled")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id, err := send.Send(ctx, "bob", []byte("hello"), sdk.WithClientMsgID("e2e-1"))
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	waitFor(t, "settled hook", func() bool {
		_, _, settled := sendHooks.snapshot()
		return len(settled) == 1
	})
	_, _, settled := sendHooks.snapshot()
	if settled[0].MessageID != id {
		t.Fatalf("settled message=%s, want %s", settled[0].MessageID, id)
	}
	if settled[0].Counts != (protocol.DeliveryCounts{Total: 1, ReceiptAcked: 1}) {
		t.Fatalf("counts=%+v", settled[0].Counts)
	}

	view, err := send.QueryMessage(ctx, id)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if view.PlatformState != "SETTLED" || view.Counts.ReceiptAcked != 1 {
		t.Fatalf("query view=%+v", view)
	}

	metrics := e.gw.Metrics().Render()
	for _, want := range []string{
		`client_event_produced_total{type="MESSAGE_SETTLED"} 1`,
		"client_event_delivered_total 1",
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics)
		}
	}
}

func TestUnsubscribedClientGetsNoClientEvent(t *testing.T) {
	e := newTestEnv(t)
	raw, err := net.DialTimeout("tcp", e.tcp, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer raw.Close()
	var seq protocol.SequenceTracker
	sendFrame := func(typ byte, body []byte) {
		s, _ := seq.Next()
		b, _ := protocol.Encode(protocol.Frame{Type: typ, Sequence: s, Body: body}, nil)
		if _, err := raw.Write(b); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	readFrame := func(d time.Duration) (protocol.Frame, error) {
		_ = raw.SetReadDeadline(time.Now().Add(d))
		header := make([]byte, protocol.HeaderSize)
		if _, err := io.ReadFull(raw, header); err != nil {
			return protocol.Frame{}, err
		}
		length := binary.BigEndian.Uint32(header[4:8])
		buf := make([]byte, protocol.HeaderSize+int(length))
		copy(buf, header)
		if _, err := io.ReadFull(raw, buf[protocol.HeaderSize:]); err != nil {
			return protocol.Frame{}, err
		}
		f, _, err := protocol.Decode(buf)
		return f, err
	}

	body, _ := protocol.EncodeLogin(e.token(t, "t1", "raw"), "dev-raw")
	sendFrame(protocol.TypeLogin, body)
	f, err := readFrame(3 * time.Second)
	if err != nil || f.Type != protocol.TypeLoginAck {
		t.Fatalf("login ack: type=%d err=%v", f.Type, err)
	}
	ack, _ := protocol.DecodeLoginAck(f.Body)
	if ack.Capabilities&protocol.CapClientEvents == 0 {
		t.Fatal("S4 server must advertise CAP_CLIENT_EVENTS")
	}
	// 不订阅事件，直接发一条零端点消息（会立即结算并产生 message.settled 事实）。
	sb, _ := protocol.EncodeSend(protocol.SendBody{ClientMsgID: "raw-1", TargetUserID: "ghost", Payload: []byte("x")})
	sendFrame(protocol.TypeSend, sb)
	f, err = readFrame(3 * time.Second)
	if err != nil || f.Type != protocol.TypeSendAck {
		t.Fatalf("send ack: type=%d err=%v", f.Type, err)
	}
	deadline := time.Now().Add(800 * time.Millisecond)
	for time.Now().Before(deadline) {
		f, err := readFrame(150 * time.Millisecond)
		if err != nil {
			continue
		}
		if f.Type == protocol.TypeClientEvent {
			t.Fatal("unsubscribed client must not receive CLIENT_EVENT")
		}
		if f.Type == protocol.TypePing {
			sendFrame(protocol.TypePong, nil)
		}
	}
	waitFor(t, "not_subscribed drop metric", func() bool {
		return strings.Contains(e.gw.Metrics().Render(), `client_event_dropped_total{reason="not_subscribed"} 1`)
	})
}

func TestClientEventMaskGrantsOnlyAllocatedBits(t *testing.T) {
	e := newTestEnv(t)
	raw, err := net.DialTimeout("tcp", e.tcp, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer raw.Close()
	var seq protocol.SequenceTracker
	write := func(typ byte, body []byte) {
		s, _ := seq.Next()
		b, _ := protocol.Encode(protocol.Frame{Type: typ, Sequence: s, Body: body}, nil)
		_, _ = raw.Write(b)
	}
	read := func(d time.Duration) (protocol.Frame, error) {
		_ = raw.SetReadDeadline(time.Now().Add(d))
		header := make([]byte, protocol.HeaderSize)
		if _, err := io.ReadFull(raw, header); err != nil {
			return protocol.Frame{}, err
		}
		length := binary.BigEndian.Uint32(header[4:8])
		buf := make([]byte, protocol.HeaderSize+int(length))
		copy(buf, header)
		if _, err := io.ReadFull(raw, buf[protocol.HeaderSize:]); err != nil {
			return protocol.Frame{}, err
		}
		f, _, err := protocol.Decode(buf)
		return f, err
	}
	loginBody, _ := protocol.EncodeLogin(e.token(t, "t1", "masker"), "dev-m")
	write(protocol.TypeLogin, loginBody)
	if _, err := read(3 * time.Second); err != nil {
		t.Fatalf("login: %v", err)
	}
	write(protocol.TypeSubscribeEvents, protocol.EncodeSubscribeEvents(0xFFFFFFFF))
	f, err := read(3 * time.Second)
	if err != nil || f.Type != protocol.TypeSubscribeEventsAck {
		t.Fatalf("subscribe_events ack: type=%d err=%v", f.Type, err)
	}
	ack, err := protocol.DecodeSubscribeEventsAck(f.Body)
	if err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	if ack.Result != protocol.SubscribeEventsOK {
		t.Fatalf("result=%d", ack.Result)
	}
	if ack.GrantedMask != protocol.EventMaskAll {
		t.Fatalf("granted=%#x, want all allocated bits %#x (unknown bits must be ignored)", ack.GrantedMask, protocol.EventMaskAll)
	}
	if ack.GrantedMask&protocol.EventBitMessageSettled == 0 {
		t.Fatal("MESSAGE_SETTLED must be granted")
	}
}

func TestCrossTenantQueryReturns404(t *testing.T) {
	e := newTestEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// 直接通过 HTTP 受理一条 t1 消息（source_device_id 为空）。
	tok := e.token(t, "t1", "alice")
	body, _ := json.Marshal(map[string]any{
		"client_msg_id": "http-1", "target_user_id": "nobody", "payload": []byte("x"),
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, e.access+"/v1/messages", strings.NewReader(string(body)))
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	var submitted struct {
		MessageID string `json:"message_id"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&submitted)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated || submitted.MessageID == "" {
		t.Fatalf("submit status=%d id=%q", resp.StatusCode, submitted.MessageID)
	}

	other := e.token(t, "t2", "alice")
	get, _ := http.NewRequestWithContext(ctx, http.MethodGet, e.access+"/v1/client/messages/"+submitted.MessageID, nil)
	get.Header.Set("Authorization", "Bearer "+other)
	resp2, err := http.DefaultClient.Do(get)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-tenant query status=%d, want 404", resp2.StatusCode)
	}
}

func TestReplacedCloseReason(t *testing.T) {
	e := newTestEnv(t)
	h1 := &hooksRecorder{}
	c1 := connectSDK(t, e.tcp, e.access, e.token(t, "t1", "carol"), "carol", "dev-c", h1, nil, 0)
	defer c1.Close()
	waitFor(t, "first ready", func() bool { r, _, _ := h1.snapshot(); return r == 1 })

	h2 := &hooksRecorder{}
	c2 := connectSDK(t, e.tcp, e.access, e.token(t, "t1", "carol"), "carol", "dev-c", h2, nil, 0)
	defer c2.Close()
	waitFor(t, "first closed", func() bool {
		_, closed, _ := h1.snapshot()
		return len(closed) == 1
	})
	_, closed, _ := h1.snapshot()
	if closed[0].ReasonCode != uint16(protocol.CloseReplaced) {
		t.Fatalf("close code=%d, want REPLACED", closed[0].ReasonCode)
	}
}

func TestLegacyServerDegradesSDK(t *testing.T) {
	e := newTestEnv(t)
	e.gw.SetLegacyMode(true)
	h := &hooksRecorder{}
	c := connectSDK(t, e.tcp, e.access, e.token(t, "t1", "legacy"), "legacy", "dev-l", h, nil, 0)
	defer c.Close()
	waitFor(t, "ready", func() bool { r, _, _ := h.snapshot(); return r == 1 })
	if c.Caps()&protocol.CapClientEvents != 0 {
		t.Fatal("legacy mode must not advertise CAP_CLIENT_EVENTS")
	}
	if c.PendingEnabled() {
		t.Fatal("PendingTracker must be disabled without CAP_CLIENT_EVENTS")
	}
}

func TestDrainTriggersReconnect(t *testing.T) {
	e := newTestEnv(t)
	h := &hooksRecorder{}
	c := connectSDK(t, e.tcp, e.access, e.token(t, "t1", "drainer"), "drainer", "dev-d", h, nil, 0)
	defer c.Close()
	waitFor(t, "first ready", func() bool { r, _, _ := h.snapshot(); return r == 1 })
	if n := e.gw.Drain(); n == 0 {
		t.Fatal("drain closed no connections")
	}
	waitFor(t, "second ready after reconnect", func() bool { r, _, _ := h.snapshot(); return r >= 2 })
	_, closed, _ := h.snapshot()
	if len(closed) != 0 {
		t.Fatalf("SERVER_DRAINING must not be terminal, got %+v", closed)
	}
}

func TestQueryBatchForPendingBackfill(t *testing.T) {
	e := newTestEnv(t)
	// 受理后立即结算一条消息，再用自己的令牌批量查询。
	tok := e.token(t, "t1", "alice")
	body, _ := json.Marshal(map[string]any{"client_msg_id": "batch-1", "target_user_id": "ghost", "payload": []byte("x")})
	req, _ := http.NewRequest(http.MethodPost, e.access+"/v1/messages", strings.NewReader(string(body)))
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusCreated {
		t.Fatalf("submit: %v status=%v", err, resp)
	}
	var submitted struct {
		MessageID string `json:"message_id"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&submitted)
	resp.Body.Close()

	get, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/v1/client/messages?ids=%s", e.access, submitted.MessageID), nil)
	get.Header.Set("Authorization", "Bearer "+tok)
	resp2, err := http.DefaultClient.Do(get)
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	defer resp2.Body.Close()
	var out struct {
		Messages []sdk.ClientMessageView `json:"messages"`
	}
	if err := json.NewDecoder(resp2.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out.Messages) != 1 || out.Messages[0].PlatformState != "SETTLED" {
		t.Fatalf("batch=%+v", out)
	}
}
