package engine

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"relayx/phase5/internal/broker"
	"relayx/phase5/internal/delivery"
	"relayx/phase5/internal/protocol"
	"relayx/phase5/internal/registry"
	"relayx/phase5/internal/session"
	"relayx/phase5/internal/store"
	"relayx/phase5/internal/tenant"
)

const testDBLockKey int64 = 0x5258

func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("RELAYX_TEST_DSN")
	if dsn == "" {
		t.Skip("RELAYX_TEST_DSN not set")
	}
	return dsn
}

func openTestStore(t *testing.T) *store.Store {
	t.Helper()
	dsn := testDSN(t)

	lockDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open advisory lock pool: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := lockDB.Conn(ctx)
	if err != nil {
		_ = lockDB.Close()
		t.Fatalf("advisory lock connection: %v", err)
	}
	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", testDBLockKey); err != nil {
		_ = conn.Close()
		_ = lockDB.Close()
		t.Fatalf("pg_advisory_lock: %v", err)
	}
	t.Cleanup(func() {
		unlockCtx, unlockCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer unlockCancel()
		if _, err := conn.ExecContext(unlockCtx, "SELECT pg_advisory_unlock($1)", testDBLockKey); err != nil {
			t.Logf("pg_advisory_unlock: %v", err)
		}
		_ = conn.Close()
		_ = lockDB.Close()
	})

	st, err := store.Open(context.Background(), store.Config{DSN: dsn, ConnectTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	if _, err := st.DB().ExecContext(ctx,
		"TRUNCATE deliveries, messages, endpoints, outbox RESTART IDENTITY CASCADE"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, "DELETE FROM tenants WHERE id <> 'default'"); err != nil {
		t.Fatalf("reset tenants: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx,
		"UPDATE tenants SET rate_per_sec = 0, burst = 0, max_inflight = 0 WHERE id = 'default'"); err != nil {
		t.Fatalf("reset default tenant: %v", err)
	}
	return st
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

type fakeProducer struct {
	mu     sync.Mutex
	events []struct {
		topic string
		key   string
		value []byte
	}
	err error
}

func (p *fakeProducer) Publish(_ context.Context, topic, key string, value []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.events = append(p.events, struct {
		topic string
		key   string
		value []byte
	}{topic, key, append([]byte(nil), value...)})
	return nil
}

func (p *fakeProducer) published() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.events)
}

type fakeForwarder struct {
	mu      sync.Mutex
	calls   []forwardCall
	err     error
	counter int
}

type forwardCall struct {
	nodeID string
	addr   string
	msgID  uint64
	body   *protocol.NodeDeliverBody
}

func (f *fakeForwarder) ForwardDeliver(_ context.Context, nodeID, addr string, msgID uint64, body *protocol.NodeDeliverBody) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, forwardCall{nodeID, addr, msgID, body})
	return f.err
}

func (f *fakeForwarder) snapshot() []forwardCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]forwardCall(nil), f.calls...)
}

type fakeRegistry struct {
	routes      map[string]registry.Route
	nodes       map[string]registry.Node
	alive       map[string]bool
	routeErr    error
	lookupCalls int
	mu          sync.Mutex
}

func (r *fakeRegistry) LookupRoute(_ context.Context, userID, deviceID string) (registry.Route, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.routeErr != nil {
		return registry.Route{}, false, r.routeErr
	}
	route, ok := r.routes[userID+"|"+deviceID]
	return route, ok, nil
}

func (r *fakeRegistry) LookupNode(_ context.Context, nodeID string) (registry.Node, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	node, ok := r.nodes[nodeID]
	return node, ok, nil
}

func (r *fakeRegistry) NodeAlive(_ context.Context, nodeID string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.alive[nodeID], nil
}

func (r *fakeRegistry) RegisterNode(context.Context, registry.Node) error { return nil }
func (r *fakeRegistry) DeregisterNode(context.Context, string) error      { return nil }
func (r *fakeRegistry) ListNodes(context.Context) ([]registry.Node, error) {
	return nil, nil
}
func (r *fakeRegistry) UpsertRoute(context.Context, registry.Route) error { return nil }
func (r *fakeRegistry) DeleteRouteIfVersion(context.Context, registry.Route) (bool, error) {
	return false, nil
}
func (r *fakeRegistry) NextVersion(context.Context, string, string) (uint64, error) { return 1, nil }
func (r *fakeRegistry) SweepDeadNodes(context.Context) ([]string, error)            { return nil, nil }
func (r *fakeRegistry) Close() error                                                { return nil }

func acceptMessage(t *testing.T, st *store.Store, tenantID, fromUser string, clientMsgID uint64, targetType store.TargetType, target, targetUser, targetDevice string) uint64 {
	t.Helper()
	res, err := st.AcceptMessage(context.Background(), store.AcceptRequest{
		TenantID:     tenantID,
		ClientMsgID:  clientMsgID,
		FromUser:     fromUser,
		FromDevice:   "phone",
		TargetType:   targetType,
		Target:       target,
		Payload:      []byte("payload"),
		ExpireAt:     time.Now().Add(time.Hour),
		TargetUser:   targetUser,
		TargetDevice: targetDevice,
		OutboxTopic:  broker.TopicFanout,
		OutboxKey:    broker.FanoutKey(tenantID, target),
		OutboxPayload: func(messageID uint64) ([]byte, error) {
			return broker.EncodeFanout(broker.FanoutEvent{
				TenantID:     tenantID,
				MessageID:    messageID,
				FromUser:     fromUser,
				FromDevice:   "phone",
				TargetType:   int16(targetType),
				Target:       target,
				TargetUser:   targetUser,
				TargetDevice: targetDevice,
				Payload:      []byte("payload"),
				ExpireAt:     time.Now().Add(time.Hour),
			})
		},
	})
	if err != nil {
		t.Fatalf("AcceptMessage() error = %v", err)
	}
	return res.MessageID
}

func fanoutBody(t *testing.T, tenantID string, messageID uint64, targetType store.TargetType, target, targetUser, targetDevice string) []byte {
	t.Helper()
	body, err := broker.EncodeFanout(broker.FanoutEvent{
		TenantID:     tenantID,
		MessageID:    messageID,
		FromUser:     "alice",
		FromDevice:   "phone",
		TargetType:   int16(targetType),
		Target:       target,
		TargetUser:   targetUser,
		TargetDevice: targetDevice,
		Payload:      []byte("payload"),
		ExpireAt:     time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("EncodeFanout() error = %v", err)
	}
	return body
}

// fakeConn is a session.Conn that records the frames the local deliverer
// enqueues.
type fakeConn struct {
	mu     sync.Mutex
	frames []*protocol.Frame
	err    error
}

func (c *fakeConn) ID() uint64         { return 1 }
func (c *fakeConn) RemoteAddr() string { return "test" }
func (c *fakeConn) Send(f *protocol.Frame) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	c.frames = append(c.frames, f)
	return nil
}
func (c *fakeConn) Close()                                       {}
func (c *fakeConn) CloseWithReason(string)                       {}
func (c *fakeConn) CloseWithFrame(*protocol.Frame, string) error { return nil }

func (c *fakeConn) sent() []*protocol.Frame {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*protocol.Frame(nil), c.frames...)
}

func mustCount(t *testing.T, st *store.Store, query string) int64 {
	t.Helper()
	var n int64
	if err := st.DB().QueryRow(query).Scan(&n); err != nil {
		t.Fatalf("count query %q: %v", query, err)
	}
	return n
}

func TestFanoutUserMultiDevice(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	for _, device := range []string{"pc", "phone", "tablet"} {
		if err := st.UpsertEndpoint(ctx, "bob", device); err != nil {
			t.Fatalf("UpsertEndpoint(%s) error = %v", device, err)
		}
	}
	msgID := acceptMessage(t, st, "default", "alice", 1, store.TargetUser, "bob", "", "")

	producer := &fakeProducer{}
	fanout := NewFanout(st, producer, 2, testLogger())
	if err := fanout.Handle(ctx, fanoutBody(t, "default", msgID, store.TargetUser, "bob", "", "")); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if got := mustCount(t, st, `SELECT count(*) FROM deliveries`); got != 3 {
		t.Fatalf("deliveries = %d, want 3", got)
	}
	if got := producer.published(); got != 3 {
		t.Fatalf("publish calls = %d, want 3", got)
	}
	for _, ev := range producer.events {
		if ev.topic != broker.TopicDeliver {
			t.Fatalf("published topic = %s, want %s", ev.topic, broker.TopicDeliver)
		}
		deliver, err := broker.DecodeDeliver(ev.value)
		if err != nil {
			t.Fatalf("DecodeDeliver() error = %v", err)
		}
		if ev.key != broker.DeliverKey("default", "bob", deliver.DeviceID) {
			t.Fatalf("key = %s, want %s", ev.key, broker.DeliverKey("default", "bob", deliver.DeviceID))
		}
		if deliver.MessageID != msgID {
			t.Fatalf("deliver message_id = %d, want %d", deliver.MessageID, msgID)
		}
	}

	// Redelivery inserts nothing new and re-publishes the same hints.
	if err := fanout.Handle(ctx, fanoutBody(t, "default", msgID, store.TargetUser, "bob", "", "")); err != nil {
		t.Fatalf("second Handle() error = %v", err)
	}
	if got := mustCount(t, st, `SELECT count(*) FROM deliveries`); got != 3 {
		t.Fatalf("deliveries after redelivery = %d, want 3", got)
	}
}

func TestFanoutEndpointMissingIsSkipped(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	msgID := acceptMessage(t, st, "default", "alice", 1, store.TargetEndpoint, "bob:ghost", "bob", "ghost")
	// Endpoint rows are shared with LOGIN, so an explicit send keeps a
	// placeholder row. Simulate a device that stopped being reachable.
	if _, err := st.DB().Exec(`DELETE FROM endpoints WHERE user_id = 'bob' AND device_id = 'ghost'`); err != nil {
		t.Fatalf("delete endpoint: %v", err)
	}

	producer := &fakeProducer{}
	fanout := NewFanout(st, producer, 10, testLogger())
	if err := fanout.Handle(ctx, fanoutBody(t, "default", msgID, store.TargetEndpoint, "bob:ghost", "bob", "ghost")); err != nil {
		t.Fatalf("Handle() error = %v (a missing endpoint must commit the offset)", err)
	}
	if got := mustCount(t, st, `SELECT count(*) FROM deliveries`); got != 0 {
		t.Fatalf("deliveries = %d, want 0", got)
	}
	if got := producer.published(); got != 0 {
		t.Fatalf("publish calls = %d, want 0", got)
	}
}

func TestFanoutEndpointResolvedFromTarget(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	if err := st.UpsertEndpoint(ctx, "bob", "pc"); err != nil {
		t.Fatalf("UpsertEndpoint() error = %v", err)
	}
	msgID := acceptMessage(t, st, "default", "alice", 1, store.TargetEndpoint, "bob:pc", "", "")

	producer := &fakeProducer{}
	fanout := NewFanout(st, producer, 10, testLogger())
	// target_user/target_device are empty, so the event's "bob:pc" target is used.
	if err := fanout.Handle(ctx, fanoutBody(t, "default", msgID, store.TargetEndpoint, "bob:pc", "", "")); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if got := mustCount(t, st, `SELECT count(*) FROM deliveries`); got != 1 {
		t.Fatalf("deliveries = %d, want 1", got)
	}
}

func TestFanoutBatchesAboveLimit(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if err := st.UpsertEndpoint(ctx, "bob", "d"+string(rune('a'+i))); err != nil {
			t.Fatalf("UpsertEndpoint() error = %v", err)
		}
	}
	msgID := acceptMessage(t, st, "default", "alice", 1, store.TargetUser, "bob", "", "")

	producer := &fakeProducer{}
	fanout := NewFanout(st, producer, 2, testLogger())
	if err := fanout.Handle(ctx, fanoutBody(t, "default", msgID, store.TargetUser, "bob", "", "")); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if got := mustCount(t, st, `SELECT count(*) FROM deliveries`); got != 5 {
		t.Fatalf("deliveries = %d, want 5", got)
	}
	if got := producer.published(); got != 5 {
		t.Fatalf("publish calls = %d, want 5", got)
	}
}

func TestEngineDeliversToLocalSession(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	if err := st.UpsertEndpoint(ctx, "bob", "pc"); err != nil {
		t.Fatalf("UpsertEndpoint() error = %v", err)
	}
	msgID := acceptMessage(t, st, "default", "alice", 1, store.TargetUser, "bob", "", "")
	if _, err := st.InsertDeliveries(ctx, msgID, []store.Endpoint{{UserID: "bob", DeviceID: "pc"}}); err != nil {
		t.Fatalf("InsertDeliveries() error = %v", err)
	}

	sessions := session.NewManager()
	conn := &fakeConn{}
	sess := sessions.Register(&session.Session{ID: sessions.NextSessionID(), UserID: "bob", DeviceID: "pc", Conn: conn})
	defer sessions.Unregister(sess)
	eng := New(Config{WorkerID: "worker-a", Batch: 10, LeaseTTL: time.Minute}, st, sessions,
		delivery.NewDeliverer(sessions), nil, nil, nil)
	if err := eng.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if frames := conn.sent(); len(frames) != 1 || frames[0].Type != protocol.Deliver || frames[0].MsgID != msgID {
		t.Fatalf("sent frames = %+v, want one DELIVER for message %d", frames, msgID)
	}

	var (
		state    int16
		attempts int
		lease    sql.NullTime
	)
	if err := st.DB().QueryRow(`SELECT state, attempts, lease_until FROM deliveries`).Scan(&state, &attempts, &lease); err != nil {
		t.Fatalf("select delivery: %v", err)
	}
	if store.DeliveryState(state) != store.DeliverySent || attempts != 1 || lease.Valid {
		t.Fatalf("delivery = state %d attempts %d lease %v, want SENT/1/none", state, attempts, lease)
	}
	stats := eng.Snapshot()
	if stats.Sent != 1 || stats.Claimed != 1 {
		t.Fatalf("stats = %+v, want 1 claimed and 1 sent", stats)
	}
}

func TestEngineNoRouteKeepsPendingWithoutAttempt(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	msgID := acceptMessage(t, st, "default", "alice", 1, store.TargetUser, "bob", "", "")
	if _, err := st.InsertDeliveries(ctx, msgID, []store.Endpoint{{UserID: "bob", DeviceID: "ghost"}}); err != nil {
		t.Fatalf("InsertDeliveries() error = %v", err)
	}

	eng := New(Config{WorkerID: "worker-a", Batch: 10, LeaseTTL: time.Minute, BackoffBase: time.Second}, st,
		session.NewManager(), delivery.NewDeliverer(session.NewManager()), nil, nil, nil)
	if err := eng.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}

	var (
		state    int16
		attempts int
		next     sql.NullTime
	)
	if err := st.DB().QueryRow(`SELECT state, attempts, next_retry_at FROM deliveries`).Scan(&state, &attempts, &next); err != nil {
		t.Fatalf("select delivery: %v", err)
	}
	if store.DeliveryState(state) != store.DeliveryPending {
		t.Fatalf("state = %d, want PENDING", state)
	}
	if attempts != 0 {
		t.Fatalf("attempts = %d, want 0 (no route must not consume a push attempt)", attempts)
	}
	if !next.Valid {
		t.Fatal("next_retry_at is NULL, want a backoff")
	}
}

func TestEngineForwardsToAliveRemoteNode(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	msgID := acceptMessage(t, st, "default", "alice", 1, store.TargetUser, "bob", "", "")
	if _, err := st.InsertDeliveries(ctx, msgID, []store.Endpoint{{UserID: "bob", DeviceID: "tv"}}); err != nil {
		t.Fatalf("InsertDeliveries() error = %v", err)
	}

	routes := &fakeRegistry{
		routes: map[string]registry.Route{"bob|tv": {UserID: "bob", DeviceID: "tv", NodeID: "node-b"}},
		nodes:  map[string]registry.Node{"node-b": {ID: "node-b", Addr: "127.0.0.1:9300"}},
		alive:  map[string]bool{"node-b": true},
	}
	fwd := &fakeForwarder{}
	eng := New(Config{WorkerID: "worker-a", Batch: 10, LeaseTTL: time.Minute}, st,
		session.NewManager(), delivery.NewDeliverer(session.NewManager()), routes, fwd, nil)
	if err := eng.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}

	calls := fwd.snapshot()
	if len(calls) != 1 {
		t.Fatalf("forward calls = %d, want 1", len(calls))
	}
	if calls[0].nodeID != "node-b" || calls[0].addr != "127.0.0.1:9300" || calls[0].msgID != msgID {
		t.Fatalf("forward = %+v, want node-b/127.0.0.1:9300 msg %d", calls[0], msgID)
	}
	if calls[0].body.UserID != "bob" || calls[0].body.DeviceID != "tv" {
		t.Fatalf("forward body = %+v, want bob/tv", calls[0].body)
	}
}

func TestEngineSkipsDeadNode(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	msgID := acceptMessage(t, st, "default", "alice", 1, store.TargetUser, "bob", "", "")
	if _, err := st.InsertDeliveries(ctx, msgID, []store.Endpoint{{UserID: "bob", DeviceID: "tv"}}); err != nil {
		t.Fatalf("InsertDeliveries() error = %v", err)
	}

	routes := &fakeRegistry{
		routes: map[string]registry.Route{"bob|tv": {UserID: "bob", DeviceID: "tv", NodeID: "node-dead"}},
		nodes:  map[string]registry.Node{"node-dead": {ID: "node-dead", Addr: "127.0.0.1:9301"}},
		alive:  map[string]bool{"node-dead": false},
	}
	fwd := &fakeForwarder{}
	eng := New(Config{WorkerID: "worker-a", Batch: 10, LeaseTTL: time.Minute}, st,
		session.NewManager(), delivery.NewDeliverer(session.NewManager()), routes, fwd, nil)
	if err := eng.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}

	if got := len(fwd.snapshot()); got != 0 {
		t.Fatalf("forward calls = %d, want 0 for a dead node", got)
	}
	var attempts int
	if err := st.DB().QueryRow(`SELECT attempts FROM deliveries`).Scan(&attempts); err != nil {
		t.Fatalf("select attempts: %v", err)
	}
	if attempts != 0 {
		t.Fatalf("attempts = %d, want 0", attempts)
	}
}

func TestEngineFailedForwardConsumesAttempt(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	msgID := acceptMessage(t, st, "default", "alice", 1, store.TargetUser, "bob", "", "")
	if _, err := st.InsertDeliveries(ctx, msgID, []store.Endpoint{{UserID: "bob", DeviceID: "tv"}}); err != nil {
		t.Fatalf("InsertDeliveries() error = %v", err)
	}

	routes := &fakeRegistry{
		routes: map[string]registry.Route{"bob|tv": {UserID: "bob", DeviceID: "tv", NodeID: "node-b"}},
		nodes:  map[string]registry.Node{"node-b": {ID: "node-b", Addr: "127.0.0.1:9302"}},
		alive:  map[string]bool{"node-b": true},
	}
	fwd := &fakeForwarder{err: errors.New("connection refused")}
	eng := New(Config{WorkerID: "worker-a", Batch: 10, LeaseTTL: time.Minute, MaxAttempts: 2}, st,
		session.NewManager(), delivery.NewDeliverer(session.NewManager()), routes, fwd, nil)
	if err := eng.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}

	var (
		state    int16
		attempts int
	)
	if err := st.DB().QueryRow(`SELECT state, attempts FROM deliveries`).Scan(&state, &attempts); err != nil {
		t.Fatalf("select delivery: %v", err)
	}
	if attempts != 1 || store.DeliveryState(state) != store.DeliveryPending {
		t.Fatalf("delivery = state %d attempts %d, want PENDING/1", state, attempts)
	}

	// Force the row due again; the second failure reaches max attempts.
	if _, err := st.DB().Exec(`UPDATE deliveries SET next_retry_at = now() - interval '1 second'`); err != nil {
		t.Fatalf("force due: %v", err)
	}
	if err := eng.RunOnce(ctx); err != nil {
		t.Fatalf("second RunOnce() error = %v", err)
	}
	if err := st.DB().QueryRow(`SELECT state, attempts FROM deliveries`).Scan(&state, &attempts); err != nil {
		t.Fatalf("select delivery: %v", err)
	}
	if store.DeliveryState(state) != store.DeliveryFailed {
		t.Fatalf("state = %d attempts = %d, want FAILED", state, attempts)
	}
}

func TestEngineLeaseExcludesSecondWorker(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	msgID := acceptMessage(t, st, "default", "alice", 1, store.TargetUser, "bob", "", "")
	if _, err := st.InsertDeliveries(ctx, msgID, []store.Endpoint{{UserID: "bob", DeviceID: "pc"}}); err != nil {
		t.Fatalf("InsertDeliveries() error = %v", err)
	}

	now := time.Now()
	first, err := st.ClaimDueDeliveries(ctx, now, 1, 0, 10, "worker-a", time.Minute)
	if err != nil || len(first) != 1 {
		t.Fatalf("first claim = %d, err=%v", len(first), err)
	}
	second, err := st.ClaimDueDeliveries(ctx, now, 1, 0, 10, "worker-b", time.Minute)
	if err != nil || len(second) != 0 {
		t.Fatalf("second claim = %d, err=%v, want 0", len(second), err)
	}
}

func TestEngineTenantThrottleDoesNotConsumeAttempt(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	if err := st.UpsertEndpoint(ctx, "bob", "pc"); err != nil {
		t.Fatalf("UpsertEndpoint() error = %v", err)
	}
	msgID := acceptMessage(t, st, "acme", "alice", 1, store.TargetUser, "bob", "", "")
	if _, err := st.InsertDeliveries(ctx, msgID, []store.Endpoint{{UserID: "bob", DeviceID: "pc"}}); err != nil {
		t.Fatalf("InsertDeliveries() error = %v", err)
	}
	if err := st.UpsertTenant(ctx, store.Tenant{ID: "acme", RatePerSec: 1, Burst: 1, MaxInflight: 10}); err != nil {
		t.Fatalf("UpsertTenant() error = %v", err)
	}

	sessions := session.NewManager()
	sess := sessions.Register(&session.Session{ID: sessions.NextSessionID(), UserID: "bob", DeviceID: "pc"})
	defer sessions.Unregister(sess)
	limiter := tenant.NewLimiter(st, time.Minute, 10)
	// Drain the single burst token so the engine has to postpone.
	if _, _, ok, err := limiter.Acquire(ctx, "acme", time.Now()); err != nil || !ok {
		t.Fatalf("priming Acquire() = (%v, %v), want ok", ok, err)
	}

	eng := New(Config{WorkerID: "worker-a", Batch: 10, LeaseTTL: time.Minute, BackoffBase: time.Second},
		st, sessions, delivery.NewDeliverer(sessions), nil, nil, limiter)
	if err := eng.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if got := eng.Snapshot().Throttled; got != 1 {
		t.Fatalf("throttled = %d, want 1", got)
	}

	var (
		state    int16
		attempts int
	)
	if err := st.DB().QueryRow(`SELECT state, attempts FROM deliveries`).Scan(&state, &attempts); err != nil {
		t.Fatalf("select delivery: %v", err)
	}
	if store.DeliveryState(state) != store.DeliveryPending || attempts != 0 {
		t.Fatalf("delivery = state %d attempts %d, want PENDING/0 (throttling is not a failure)", state, attempts)
	}
}

func TestEngineExpiresOverdueMessages(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	res, err := st.AcceptMessage(ctx, store.AcceptRequest{
		ClientMsgID:   1,
		FromUser:      "alice",
		FromDevice:    "phone",
		TargetType:    store.TargetUser,
		Target:        "bob",
		Payload:       []byte("old"),
		ExpireAt:      time.Now().Add(-time.Second),
		OutboxTopic:   broker.TopicFanout,
		OutboxKey:     "default|bob",
		OutboxPayload: func(uint64) ([]byte, error) { return []byte(`{"version":1}`), nil },
	})
	if err != nil {
		t.Fatalf("AcceptMessage() error = %v", err)
	}
	if _, err := st.InsertDeliveries(ctx, res.MessageID, []store.Endpoint{{UserID: "bob", DeviceID: "pc"}}); err != nil {
		t.Fatalf("InsertDeliveries() error = %v", err)
	}

	eng := New(Config{WorkerID: "worker-a", Batch: 10, LeaseTTL: time.Minute}, st,
		session.NewManager(), delivery.NewDeliverer(session.NewManager()), nil, nil, nil)
	if err := eng.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if got := eng.Snapshot().Expired; got != 1 {
		t.Fatalf("expired = %d, want 1", got)
	}
	var msgState, deliveryState int16
	if err := st.DB().QueryRow(`SELECT state FROM messages`).Scan(&msgState); err != nil {
		t.Fatalf("select message state: %v", err)
	}
	if err := st.DB().QueryRow(`SELECT state FROM deliveries`).Scan(&deliveryState); err != nil {
		t.Fatalf("select delivery state: %v", err)
	}
	if store.MessageState(msgState) != store.MessageExpired || store.DeliveryState(deliveryState) != store.DeliveryExpired {
		t.Fatalf("states = message %d delivery %d, want EXPIRED/EXPIRED", msgState, deliveryState)
	}
}
