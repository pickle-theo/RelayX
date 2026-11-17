// Command scenario 是 S4 的场景运行器 v1（LLD §14.5），用真实 SDK 驱动
// 真实 Gateway，覆盖 lifecycle-hooks / close-reasons / message-settled-event /
// event-gap-query / compat-matrix 五个验收场景。
//
// 需要 Gateway 以 -test-hooks 启动（close-reasons 与 event-gap-query 依赖
// /debug/kick、/debug/expire-token、/debug/drain、/debug/events/skip）。
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"relayx/s4/internal/identity"
	"relayx/s4/protocol"
	"relayx/s4/sdk"
)

type env struct {
	gateway string
	access  string
	secret  []byte
	tenant  string
}

func main() {
	gatewayAddr := flag.String("gateway", "127.0.0.1:9800", "Gateway TCP 地址")
	accessAddr := flag.String("access", "http://127.0.0.1:9802", "HTTP access base URL")
	secret := flag.String("token-secret", "relayx-s4-dev-secret", "登录令牌 HMAC 密钥")
	tenant := flag.String("tenant", "t1", "租户")
	name := flag.String("scenario", "all", "场景名或 all")
	flag.Parse()

	e := &env{gateway: *gatewayAddr, access: *accessAddr, secret: []byte(*secret), tenant: *tenant}
	all := []struct {
		name string
		fn   func(context.Context, *env) error
	}{
		{"lifecycle-hooks", scenarioLifecycleHooks},
		{"close-reasons", scenarioCloseReasons},
		{"message-settled-event", scenarioMessageSettled},
		{"event-gap-query", scenarioEventGap},
		{"compat-matrix", scenarioCompatMatrix},
	}

	failed := 0
	ran := 0
	for _, s := range all {
		if *name != "all" && *name != s.name {
			continue
		}
		ran++
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		fmt.Printf("=== scenario %s ===\n", s.name)
		err := s.fn(ctx, e)
		cancel()
		if err != nil {
			failed++
			fmt.Printf("FAIL %s: %v\n", s.name, err)
		} else {
			fmt.Printf("PASS %s\n", s.name)
		}
	}
	if ran == 0 {
		fmt.Fprintf(os.Stderr, "unknown scenario %q\n", *name)
		os.Exit(2)
	}
	if *name != "all" {
		return
	}
	if failed > 0 {
		fmt.Printf("%d/%d scenarios failed\n", failed, ran)
		os.Exit(1)
	}
	fmt.Printf("all %d scenarios passed\n", ran)
}

// ---------- observer：hooks → 日志 + 通道 ----------

type hookLog struct {
	mu    sync.Mutex
	items []string
}

func (h *hookLog) add(s string) {
	h.mu.Lock()
	h.items = append(h.items, s)
	h.mu.Unlock()
}

func (h *hookLog) snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.items))
	copy(out, h.items)
	return out
}

func (h *hookLog) count(prefix string) int {
	n := 0
	for _, it := range h.snapshot() {
		if len(it) >= len(prefix) && it[:len(prefix)] == prefix {
			n++
		}
	}
	return n
}

type observer struct {
	log *hookLog

	ready        chan struct{}
	closed       chan sdk.CloseInfo
	reconnecting chan sdk.ReconnectInfo
	disconnected chan sdk.DisconnectInfo

	replaced  chan struct{}
	kicked    chan struct{}
	expired   chan struct{}
	draining  chan struct{}
	heartbeat chan struct{}
	authFail  chan struct{}

	accepted  chan sdk.MessageAcceptedInfo
	settled   chan sdk.MessageSettledInfo
	msgExpire chan sdk.MessageSettledInfo
	delivered chan sdk.DeliveredInfo
	receipt   chan sdk.ReceiptAckInfo
	dropped   chan sdk.DropInfo
	broadcast chan sdk.BroadcastInfo
}

func newObserver() *observer {
	return &observer{
		log:          &hookLog{},
		ready:        make(chan struct{}, 8),
		closed:       make(chan sdk.CloseInfo, 8),
		reconnecting: make(chan sdk.ReconnectInfo, 8),
		disconnected: make(chan sdk.DisconnectInfo, 8),
		replaced:     make(chan struct{}, 4),
		kicked:       make(chan struct{}, 4),
		expired:      make(chan struct{}, 4),
		draining:     make(chan struct{}, 4),
		heartbeat:    make(chan struct{}, 4),
		authFail:     make(chan struct{}, 4),
		accepted:     make(chan sdk.MessageAcceptedInfo, 64),
		settled:      make(chan sdk.MessageSettledInfo, 64),
		msgExpire:    make(chan sdk.MessageSettledInfo, 64),
		delivered:    make(chan sdk.DeliveredInfo, 64),
		receipt:      make(chan sdk.ReceiptAckInfo, 64),
		dropped:      make(chan sdk.DropInfo, 64),
		broadcast:    make(chan sdk.BroadcastInfo, 64),
	}
}

func signal(ch chan struct{}) func() {
	return func() {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (o *observer) hooks() sdk.Hooks {
	return sdk.Hooks{
		Connecting: func(i sdk.ConnectingInfo) { o.log.add("connecting") },
		Connected:  func(i sdk.ConnectedInfo) { o.log.add("connected") },
		Authenticated: func(i sdk.AuthenticatedInfo) {
			o.log.add("authenticated")
		},
		Ready: func() {
			o.log.add("ready")
			signal(o.ready)()
		},
		Reconnecting: func(i sdk.ReconnectInfo) {
			o.log.add("reconnecting")
			select {
			case o.reconnecting <- i:
			default:
			}
		},
		Disconnected: func(i sdk.DisconnectInfo) {
			o.log.add("disconnected")
			select {
			case o.disconnected <- i:
			default:
			}
		},
		Closed: func(i sdk.CloseInfo) {
			o.log.add("closed:" + i.Reason)
			select {
			case o.closed <- i:
			default:
			}
		},
		AuthFailed: func(i sdk.AuthFailedInfo) {
			o.log.add("auth_failed")
			signal(o.authFail)()
		},
		HeartbeatTimeout: func() {
			o.log.add("heartbeat_timeout")
			signal(o.heartbeat)()
		},
		SessionReplaced: func(i sdk.CloseInfo) {
			o.log.add("session.replaced")
			signal(o.replaced)()
		},
		SessionKicked: func(i sdk.CloseInfo) {
			o.log.add("session.kicked")
			signal(o.kicked)()
		},
		SessionTokenExpired: func(i sdk.CloseInfo) {
			o.log.add("session.token_expired")
			signal(o.expired)()
		},
		ServerDraining: func(i sdk.CloseInfo) {
			o.log.add("server.draining")
			signal(o.draining)()
		},
		MessageAccepted: func(i sdk.MessageAcceptedInfo) {
			o.log.add("message.accepted")
			select {
			case o.accepted <- i:
			default:
			}
		},
		MessageSettled: func(i sdk.MessageSettledInfo) {
			o.log.add("message.settled:" + i.MessageID)
			select {
			case o.settled <- i:
			default:
			}
		},
		MessageExpired: func(i sdk.MessageSettledInfo) {
			o.log.add("message.expired:" + i.MessageID)
			select {
			case o.msgExpire <- i:
			default:
			}
		},
		Delivered: func(i sdk.DeliveredInfo) {
			o.log.add("delivered:" + i.MessageID)
			select {
			case o.delivered <- i:
			default:
			}
		},
		ReceiptAckSent: func(i sdk.ReceiptAckInfo) {
			o.log.add("receipt.ack_sent")
			select {
			case o.receipt <- i:
			default:
			}
		},
		EventDropped: func(i sdk.DropInfo) {
			o.log.add("hook.dropped")
			select {
			case o.dropped <- i:
			default:
			}
		},
		BroadcastReceived: func(i sdk.BroadcastInfo) {
			o.log.add("broadcast.received:" + i.Topic)
			select {
			case o.broadcast <- i:
			default:
			}
		},
	}
}

// ---------- env helpers ----------

func (e *env) userToken(user string) (string, error) {
	return identity.New(e.secret).Issue(identity.Producer{
		ID: "u:" + user, Kind: "user", TenantID: e.tenant,
	}, time.Hour)
}

func (e *env) newClient(user, device string, o *observer, query bool) (*sdk.Client, error) {
	tok, err := e.userToken(user)
	if err != nil {
		return nil, err
	}
	cfg := sdk.Config{
		Addr: e.gateway, Token: tok, DeviceID: device,
		TenantID: e.tenant, ProducerID: "u:" + user,
		Hooks: o.hooks(),
		Logger: func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, "  [%s] "+format+"\n", append([]any{user}, args...)...)
		},
	}
	if query {
		cfg.QueryBaseURL = e.access
	}
	return sdk.New(cfg), nil
}

func (e *env) debug(path string, body any) error {
	return e.debugJSON(http.MethodPost, path, body)
}

func (e *env) debugJSON(method, path string, body any) error {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, e.access+path, reader)
	if err != nil {
		return err
	}
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return fmt.Errorf("debug %s: %w", path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("debug %s: status %d (%s); 网关需以 -test-hooks 启动", path, resp.StatusCode, bytes.TrimSpace(raw))
	}
	return nil
}

func waitSignal(ctx context.Context, ch <-chan struct{}, what string) error {
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("timeout waiting for %s", what)
	}
}

func waitReady(ctx context.Context, o *observer) error {
	return waitSignal(ctx, o.ready, "ready")
}

func waitClosed(ctx context.Context, o *observer) (sdk.CloseInfo, error) {
	select {
	case ci := <-o.closed:
		return ci, nil
	case <-ctx.Done():
		return sdk.CloseInfo{}, errors.New("timeout waiting for closed")
	}
}

func waitSettled(ctx context.Context, o *observer, n int) ([]sdk.MessageSettledInfo, error) {
	out := make([]sdk.MessageSettledInfo, 0, n)
	for len(out) < n {
		select {
		case i := <-o.settled:
			out = append(out, i)
		case <-ctx.Done():
			return nil, fmt.Errorf("timeout waiting for settled (%d/%d)", len(out), n)
		}
	}
	return out, nil
}

func waitDelivered(ctx context.Context, o *observer, n int) ([]sdk.DeliveredInfo, error) {
	out := make([]sdk.DeliveredInfo, 0, n)
	for len(out) < n {
		select {
		case i := <-o.delivered:
			out = append(out, i)
		case <-ctx.Done():
			return nil, fmt.Errorf("timeout waiting for delivered (%d/%d)", len(out), n)
		}
	}
	return out, nil
}

// requireOrder 断言 want 是 hook 日志的有序子序列。
func requireOrder(log *hookLog, want ...string) error {
	items := log.snapshot()
	i := 0
	for _, w := range want {
		found := false
		for i < len(items) {
			if items[i] == w {
				found = true
				i++
				break
			}
			i++
		}
		if !found {
			return fmt.Errorf("hook order missing %q; got %v", w, items)
		}
	}
	return nil
}

// ---------- 场景 1：lifecycle-hooks ----------

func scenarioLifecycleHooks(ctx context.Context, e *env) error {
	o := newObserver()
	c, err := e.newClient("life-a", "dev-a", o, false)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := c.Connect(ctx); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	if err := waitReady(ctx, o); err != nil {
		return err
	}
	// SERVER_DRAINING 是预期行为：SDK 必须重连并再次 READY。
	if err := e.debug("/debug/drain", nil); err != nil {
		return err
	}
	if err := waitReady(ctx, o); err != nil {
		return fmt.Errorf("reconnect after drain: %w", err)
	}
	if err := c.Close(); err != nil {
		return err
	}
	ci, err := waitClosed(ctx, o)
	if err != nil {
		return err
	}
	if ci.ReasonCode != uint16(protocol.CloseNormal) || ci.Reason != "NORMAL" {
		return fmt.Errorf("unexpected close info: %+v", ci)
	}
	return requireOrder(o.log,
		"connecting", "connected", "authenticated", "ready",
		"server.draining", "disconnected", "reconnecting",
		"connected", "authenticated", "ready", "closed:NORMAL")
}

// ---------- 场景 2：close-reasons ----------

func scenarioCloseReasons(ctx context.Context, e *env) error {
	// REPLACED：同 user/device 新登录替换旧会话。
	{
		o1 := newObserver()
		c1, err := e.newClient("c-replace", "dev-r", o1, false)
		if err != nil {
			return err
		}
		if err := c1.Connect(ctx); err != nil {
			return err
		}
		if err := waitReady(ctx, o1); err != nil {
			return err
		}
		o2 := newObserver()
		c2, err := e.newClient("c-replace", "dev-r", o2, false)
		if err != nil {
			return err
		}
		if err := c2.Connect(ctx); err != nil {
			return err
		}
		if err := waitReady(ctx, o2); err != nil {
			return err
		}
		ci, err := waitClosed(ctx, o1)
		if err != nil {
			return err
		}
		if ci.ReasonCode != uint16(protocol.CloseReplaced) {
			return fmt.Errorf("REPLACED: got %d", ci.ReasonCode)
		}
		if err := waitSignal(ctx, o1.replaced, "session.replaced"); err != nil {
			return err
		}
		_ = c2.Close()
	}

	// KICKED / TOKEN_EXPIRED：验证钩子触发。
	for _, tc := range []struct {
		user string
		path string
		code protocol.CloseReason
		name string
	}{
		{"c-kick", "/debug/kick", protocol.CloseKicked, "KICKED"},
		{"c-token", "/debug/expire-token", protocol.CloseTokenExpired, "TOKEN_EXPIRED"},
	} {
		o := newObserver()
		c, err := e.newClient(tc.user, "dev-k", o, false)
		if err != nil {
			return err
		}
		if err := c.Connect(ctx); err != nil {
			return err
		}
		if err := waitReady(ctx, o); err != nil {
			return err
		}
		if err := e.debug(tc.path, map[string]string{
			"tenant": e.tenant, "user": tc.user, "device": "dev-k",
		}); err != nil {
			return err
		}
		ci, err := waitClosed(ctx, o)
		if err != nil {
			return err
		}
		if ci.ReasonCode != uint16(tc.code) {
			return fmt.Errorf("%s: got code %d reason %s", tc.name, ci.ReasonCode, ci.Reason)
		}
		wantLog := "session.kicked"
		if tc.code == protocol.CloseTokenExpired {
			wantLog = "session.token_expired"
		}
		if o.log.count(wantLog) != 1 {
			return fmt.Errorf("%s: hook %s not fired: %v", tc.name, wantLog, o.log.snapshot())
		}
		_ = c.Close()
	}

	// SERVER_DRAINING：可恢复，重连后再次 READY。
	{
		o := newObserver()
		c, err := e.newClient("c-drain", "dev-d", o, false)
		if err != nil {
			return err
		}
		if err := c.Connect(ctx); err != nil {
			return err
		}
		if err := waitReady(ctx, o); err != nil {
			return err
		}
		if err := e.debug("/debug/drain", nil); err != nil {
			return err
		}
		if err := waitSignal(ctx, o.draining, "server.draining"); err != nil {
			return err
		}
		if err := waitReady(ctx, o); err != nil {
			return fmt.Errorf("draining reconnect: %w", err)
		}
		if n := len(o.closed); n != 0 {
			return fmt.Errorf("draining must not enter terminal CLOSED, got %d", n)
		}
		_ = c.Close()
	}

	// AUTH_FAILED：坏令牌，Connect 返回错误且 closed(1004)。
	{
		o := newObserver()
		c := sdk.New(sdk.Config{
			Addr: e.gateway, Token: "bad.token", DeviceID: "dev-x", Hooks: o.hooks(),
			Logger: func(format string, args ...any) {},
		})
		err := c.Connect(ctx)
		if err == nil {
			return errors.New("AUTH_FAILED: Connect unexpectedly succeeded")
		}
		ci, err := waitClosed(ctx, o)
		if err != nil {
			return err
		}
		if ci.ReasonCode != uint16(protocol.CloseAuthFailed) {
			return fmt.Errorf("AUTH_FAILED: got %d", ci.ReasonCode)
		}
		if o.log.count("auth_failed") == 0 {
			return errors.New("AUTH_FAILED: auth_failed hook not fired")
		}
	}

	// NORMAL：客户端主动关闭。
	{
		o := newObserver()
		c, err := e.newClient("c-normal", "dev-n", o, false)
		if err != nil {
			return err
		}
		if err := c.Connect(ctx); err != nil {
			return err
		}
		if err := waitReady(ctx, o); err != nil {
			return err
		}
		_ = c.Close()
		ci, err := waitClosed(ctx, o)
		if err != nil {
			return err
		}
		if ci.ReasonCode != uint16(protocol.CloseNormal) {
			return fmt.Errorf("NORMAL: got %d", ci.ReasonCode)
		}
	}
	return nil
}

// ---------- 场景 3：message-settled-event ----------

func scenarioMessageSettled(ctx context.Context, e *env) error {
	recvObs := newObserver()
	recv, err := e.newClient("bob", "dev-b", recvObs, true)
	if err != nil {
		return err
	}
	defer recv.Close()
	if err := recv.Connect(ctx); err != nil {
		return err
	}
	if err := waitReady(ctx, recvObs); err != nil {
		return err
	}

	sendObs := newObserver()
	send, err := e.newClient("alice", "dev-a", sendObs, true)
	if err != nil {
		return err
	}
	defer send.Close()
	if err := send.Connect(ctx); err != nil {
		return err
	}
	if err := waitReady(ctx, sendObs); err != nil {
		return err
	}
	if !send.PendingEnabled() {
		return errors.New("PendingTracker should be enabled (caps + query API)")
	}

	const n = 3
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		id, err := send.Send(ctx, "bob", []byte(fmt.Sprintf("m-%d", i)),
			sdk.WithClientMsgID(fmt.Sprintf("settled-%d-%d", time.Now().UnixNano(), i)))
		if err != nil {
			return fmt.Errorf("send %d: %w", i, err)
		}
		ids = append(ids, id)
	}
	deliveries, err := waitDelivered(ctx, recvObs, n)
	if err != nil {
		return err
	}
	for _, d := range deliveries {
		if err := recv.SendReceipt(d.MessageID); err != nil {
			return err
		}
	}
	settled, err := waitSettled(ctx, sendObs, n)
	if err != nil {
		return err
	}
	seen := make(map[string]int)
	for _, s := range settled {
		seen[s.MessageID]++
		if s.Counts.Total != 1 || s.Counts.ReceiptAcked != 1 || s.Counts.Failed != 0 || s.Counts.Expired != 0 {
			return fmt.Errorf("message %s counts=%+v, want {1,1,0,0}", s.MessageID, s.Counts)
		}
		if s.FromQuery {
			return fmt.Errorf("message %s settled should come from event, not query", s.MessageID)
		}
	}
	for _, id := range ids {
		if seen[id] != 1 {
			return fmt.Errorf("message %s got %d settled hooks, want exactly 1", id, seen[id])
		}
	}
	if n := send.PendingLen(); n != 0 {
		return fmt.Errorf("pending should be empty, got %d", n)
	}

	// 零端点：SEND_ACK 直接返回 SETTLED，不进入 pending，但仍触发一次终态 hook。
	if _, err := send.Send(ctx, "ghost", []byte("nowhere"),
		sdk.WithClientMsgID(fmt.Sprintf("ghost-%d", time.Now().UnixNano()))); err != nil {
		return err
	}
	ghost, err := waitSettled(ctx, sendObs, 1)
	if err != nil {
		return err
	}
	if ghost[0].Counts.Total != 0 {
		return fmt.Errorf("zero-endpoint settled counts.total=%d, want 0", ghost[0].Counts.Total)
	}
	return nil
}

// ---------- 场景 4：event-gap-query ----------

func scenarioEventGap(ctx context.Context, e *env) error {
	recvObs := newObserver()
	recv, err := e.newClient("bob-gap", "dev-gb", recvObs, true)
	if err != nil {
		return err
	}
	defer recv.Close()
	if err := recv.Connect(ctx); err != nil {
		return err
	}
	if err := waitReady(ctx, recvObs); err != nil {
		return err
	}

	sendObs := newObserver()
	send, err := e.newClient("alice-gap", "dev-ga", sendObs, true)
	if err != nil {
		return err
	}
	defer send.Close()
	if err := send.Connect(ctx); err != nil {
		return err
	}
	if err := waitReady(ctx, sendObs); err != nil {
		return err
	}

	const n = 3
	for i := 0; i < n; i++ {
		if _, err := send.Send(ctx, "bob-gap", []byte(fmt.Sprintf("gap-%d", i)),
			sdk.WithClientMsgID(fmt.Sprintf("gap-%d-%d", time.Now().UnixNano(), i))); err != nil {
			return err
		}
	}
	deliveries, err := waitDelivered(ctx, recvObs, n)
	if err != nil {
		return err
	}

	// 先让第一条终态事件正常到达，再注入丢弃：客户端先看到 seq=1，
	// 随后看到 seq=3，gap 判定才是确定性的（若丢首条则按"中途订阅"处理不告警）。
	if err := recv.SendReceipt(deliveries[0].MessageID); err != nil {
		return err
	}
	first, err := waitSettled(ctx, sendObs, 1)
	if err != nil {
		return err
	}
	if err := e.debug("/debug/events/skip", map[string]int{"count": 1}); err != nil {
		return err
	}
	for _, d := range deliveries[1:] {
		if err := recv.SendReceipt(d.MessageID); err != nil {
			return err
		}
	}

	rest, err := waitSettled(ctx, sendObs, n-1)
	if err != nil {
		return err
	}
	all := append(first, rest...)
	fromQuery := 0
	ids := map[string]bool{}
	for _, s := range all {
		ids[s.MessageID] = true
		if s.FromQuery {
			fromQuery++
		}
	}
	if len(ids) != n {
		return fmt.Errorf("expected %d distinct settled messages, got %d", n, len(ids))
	}
	if send.GapQueries() != 1 {
		return fmt.Errorf("gap queries=%d, want exactly 1", send.GapQueries())
	}
	if fromQuery == 0 {
		return errors.New("dropped event was not backfilled by query API")
	}
	if n := send.PendingLen(); n != 0 {
		return fmt.Errorf("pending should be drained after backfill, got %d", n)
	}
	return nil
}

// ---------- 场景 5：compat-matrix ----------

func scenarioCompatMatrix(ctx context.Context, e *env) error {
	// (a) 旧客户端 + 新服务端：不订阅事件 → 不收 CLIENT_EVENT。
	raw, err := dialRaw(e.gateway)
	if err != nil {
		return err
	}
	defer raw.close()
	tok, err := e.userToken("raw-old")
	if err != nil {
		return err
	}
	caps, err := raw.login(tok, "dev-raw")
	if err != nil {
		return err
	}
	if caps&protocol.CapClientEvents == 0 {
		return errors.New("new server should advertise CAP_CLIENT_EVENTS")
	}
	if _, err := raw.sendTo(ctx, "ghost-raw", []byte("legacy"), "raw-1"); err != nil {
		return err
	}
	deadline := time.Now().Add(1200 * time.Millisecond)
	for time.Now().Before(deadline) {
		f, err := raw.readFrame(200 * time.Millisecond)
		if err != nil {
			continue
		}
		switch f.Type {
		case protocol.TypeClientEvent:
			return errors.New("old client must not receive CLIENT_EVENT")
		case protocol.TypePing:
			_ = raw.sendFrame(protocol.TypePong, nil)
		}
	}

	// (b) 新客户端 + 旧服务端：能力位缺失 → SDK 降级，不启用 PendingTracker。
	if err := e.debug("/debug/legacy-mode", map[string]bool{"enabled": true}); err != nil {
		return err
	}
	defer func() { _ = e.debug("/debug/legacy-mode", map[string]bool{"enabled": false}) }()

	o := newObserver()
	c, err := e.newClient("new-old", "dev-no", o, true)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := c.Connect(ctx); err != nil {
		return fmt.Errorf("degraded connect: %w", err)
	}
	if err := waitReady(ctx, o); err != nil {
		return err
	}
	if c.Caps()&protocol.CapClientEvents != 0 {
		return errors.New("legacy server must not advertise CAP_CLIENT_EVENTS")
	}
	if c.PendingEnabled() {
		return errors.New("PendingTracker must stay disabled without CAP_CLIENT_EVENTS")
	}
	if _, err := c.Send(ctx, "ghost-old", []byte("degraded"),
		sdk.WithClientMsgID(fmt.Sprintf("degraded-%d", time.Now().UnixNano()))); err != nil {
		return fmt.Errorf("send in degraded mode: %w", err)
	}
	if _, err := waitSettled(ctx, o, 1); err != nil {
		return err
	}
	return nil
}

// ---------- raw 协议客户端（仅协议一致性用例允许绕 SDK） ----------

type rawClient struct {
	nc  net.Conn
	seq protocol.SequenceTracker
}

func dialRaw(addr string) (*rawClient, error) {
	nc, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return nil, err
	}
	return &rawClient{nc: nc}, nil
}

func (r *rawClient) close() { _ = r.nc.Close() }

func (r *rawClient) sendFrame(t byte, body []byte) error {
	seq, err := r.seq.Next()
	if err != nil {
		return err
	}
	b, err := protocol.Encode(protocol.Frame{Type: t, Sequence: seq, Body: body}, nil)
	if err != nil {
		return err
	}
	_, err = r.nc.Write(b)
	return err
}

func (r *rawClient) readFrame(d time.Duration) (protocol.Frame, error) {
	_ = r.nc.SetReadDeadline(time.Now().Add(d))
	header := make([]byte, protocol.HeaderSize)
	if _, err := io.ReadFull(r.nc, header); err != nil {
		return protocol.Frame{}, err
	}
	length := binary.BigEndian.Uint32(header[4:8])
	if length > protocol.MaxBodySize {
		return protocol.Frame{}, fmt.Errorf("frame too large: %d", length)
	}
	total := protocol.HeaderSize + int(length)
	buf := make([]byte, total)
	copy(buf, header)
	if _, err := io.ReadFull(r.nc, buf[protocol.HeaderSize:]); err != nil {
		return protocol.Frame{}, err
	}
	f, _, err := protocol.Decode(buf)
	return f, err
}

func (r *rawClient) login(token, device string) (uint32, error) {
	body, err := protocol.EncodeLogin(token, device)
	if err != nil {
		return 0, err
	}
	if err := r.sendFrame(protocol.TypeLogin, body); err != nil {
		return 0, err
	}
	f, err := r.readFrame(5 * time.Second)
	if err != nil {
		return 0, err
	}
	if f.Type == protocol.TypeClose {
		cb, _ := protocol.DecodeCloseBody(f.Body)
		return 0, fmt.Errorf("login rejected: close %d %s", cb.ReasonCode, cb.ReasonText)
	}
	if f.Type != protocol.TypeLoginAck {
		return 0, fmt.Errorf("expected LOGIN_ACK, got 0x%02x", f.Type)
	}
	ack, err := protocol.DecodeLoginAck(f.Body)
	if err != nil {
		return 0, err
	}
	return ack.Capabilities, nil
}

func (r *rawClient) sendTo(_ context.Context, user string, payload []byte, clientMsgID string) (string, error) {
	body, err := protocol.EncodeSend(protocol.SendBody{
		ClientMsgID: clientMsgID, TargetUserID: user, Payload: payload,
	})
	if err != nil {
		return "", err
	}
	if err := r.sendFrame(protocol.TypeSend, body); err != nil {
		return "", err
	}
	f, err := r.readFrame(5 * time.Second)
	if err != nil {
		return "", err
	}
	if f.Type == protocol.TypeClose {
		return "", errors.New("connection closed while awaiting send_ack")
	}
	if f.Type != protocol.TypeSendAck {
		return "", fmt.Errorf("expected SEND_ACK, got 0x%02x", f.Type)
	}
	ack, err := protocol.DecodeSendAck(f.Body)
	if err != nil {
		return "", err
	}
	return ack.MessageID, nil
}
