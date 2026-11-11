package gateway

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"relayx/phase4/internal/protocol"
	"relayx/phase4/internal/registry"
	"relayx/phase4/internal/store"
)

type forwardedDelivery struct {
	nodeID string
	addr   string
	msgID  uint64
	body   protocol.NodeDeliverBody
}

type forwardedKick struct {
	nodeID string
	addr   string
	body   protocol.NodeKickBody
}

// fakeForwarder records cross-node calls so tests can assert what the gateway
// would send over the real hub connection.
type fakeForwarder struct {
	mu         sync.Mutex
	deliveries []forwardedDelivery
	kicks      []forwardedKick
	deliverErr error
	kickErr    error
}

func (f *fakeForwarder) ForwardDeliver(ctx context.Context, nodeID, addr string, msgID uint64, body *protocol.NodeDeliverBody) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec := forwardedDelivery{nodeID: nodeID, addr: addr, msgID: msgID, body: *body}
	rec.body.Payload = append([]byte(nil), body.Payload...)
	f.deliveries = append(f.deliveries, rec)
	return f.deliverErr
}

func (f *fakeForwarder) Kick(ctx context.Context, nodeID, addr string, body *protocol.NodeKickBody) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.kicks = append(f.kicks, forwardedKick{nodeID: nodeID, addr: addr, body: *body})
	return f.kickErr
}

func (f *fakeForwarder) forwarded() []forwardedDelivery {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]forwardedDelivery(nil), f.deliveries...)
}

func (f *fakeForwarder) kicked() []forwardedKick {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]forwardedKick(nil), f.kicks...)
}

// newClusterFixtureNodeID builds a gateway wired to an in-memory registry so the
// cluster paths can run against the real PostgreSQL test store. The node ID is
// configurable for tests that need a specific local identity.
func newClusterFixtureNodeID(t *testing.T, nodeID string) (*gatewayFixture, *registry.Memory, *fakeForwarder) {
	t.Helper()
	reg := registry.NewMemory(time.Minute)
	t.Cleanup(func() { _ = reg.Close() })
	fwd := &fakeForwarder{}
	f := startGatewayWith(t, gatewayOptions{handler: Config{
		NodeID:    nodeID,
		Registry:  reg,
		Forwarder: fwd,
	}})
	return f, reg, fwd
}

// newClusterFixture builds a gateway wired to an in-memory registry so the
// cluster paths can run against the real PostgreSQL test store.
func newClusterFixture(t *testing.T) (*gatewayFixture, *registry.Memory, *fakeForwarder) {
	t.Helper()
	return newClusterFixtureNodeID(t, "node-a")
}

func routeOf(t *testing.T, reg registry.Registry, userID, deviceID string) registry.Route {
	t.Helper()
	route, ok, err := reg.LookupRoute(context.Background(), userID, deviceID)
	if err != nil {
		t.Fatalf("LookupRoute(%s, %s): %v", userID, deviceID, err)
	}
	if !ok {
		t.Fatalf("LookupRoute(%s, %s) = missing, want a route", userID, deviceID)
	}
	return route
}

func TestClusterLoginWritesRoute(t *testing.T) {
	f, reg, _ := newClusterFixture(t)

	_, ack := loginOK(t, f, "alice", "phone")

	route := routeOf(t, reg, "alice", "phone")
	if route.NodeID != "node-a" {
		t.Errorf("route node_id = %q, want %q", route.NodeID, "node-a")
	}
	if route.SessionID != ack.SessionID {
		t.Errorf("route session_id = %d, want %d", route.SessionID, ack.SessionID)
	}
	if route.Version != 1 {
		t.Errorf("route version = %d, want 1", route.Version)
	}
	if route.CreatedAt.IsZero() {
		t.Error("route created_at is zero, want the acceptance time")
	}
}

func TestClusterDuplicateLoginBumpsVersion(t *testing.T) {
	f, reg, _ := newClusterFixture(t)

	connA, ackA := loginOK(t, f, "carol", "tv")
	_, ackB := loginOK(t, f, "carol", "tv")

	if ackB.SessionID == ackA.SessionID {
		t.Fatalf("second login reused session id %d, want a new one", ackA.SessionID)
	}
	route := routeOf(t, reg, "carol", "tv")
	if route.Version != 2 {
		t.Fatalf("route version = %d, want 2", route.Version)
	}
	if route.SessionID != ackB.SessionID {
		t.Fatalf("route session_id = %d, want %d", route.SessionID, ackB.SessionID)
	}

	// The replaced local connection still receives CLOSE and then EOF.
	if err := connA.conn.SetReadDeadline(time.Now().Add(testIOTimeout)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	closeFrame, err := protocol.Decode(connA.r, testMaxBody)
	if err != nil {
		t.Fatalf("read from replaced connection = %v, want CLOSE", err)
	}
	if closeFrame.Type != protocol.Close {
		t.Fatalf("replaced connection received %v, want CLOSE", closeFrame.Type)
	}
}

func TestClusterHandleNodeKick(t *testing.T) {
	f, _, _ := newClusterFixture(t)

	conn, _ := loginOK(t, f, "dave", "tv")

	// An equal version is a stale kick for this session and must be ignored.
	f.handler.HandleNodeKick(&protocol.NodeKickBody{UserID: "dave", DeviceID: "tv", Version: 1})
	if f.sessions.FindByDevice("dave", "tv") == nil {
		t.Fatal("session closed by a kick with the same version")
	}

	f.handler.HandleNodeKick(&protocol.NodeKickBody{UserID: "dave", DeviceID: "tv", Version: 2})
	if err := conn.conn.SetReadDeadline(time.Now().Add(testIOTimeout)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	closeFrame, err := protocol.Decode(conn.r, testMaxBody)
	if err != nil {
		t.Fatalf("read from kicked connection = %v, want CLOSE", err)
	}
	if closeFrame.Type != protocol.Close {
		t.Fatalf("kicked connection received %v, want CLOSE", closeFrame.Type)
	}
	waitFor(t, 2*time.Second, func() bool {
		return f.sessions.FindByDevice("dave", "tv") == nil
	}, "kicked session to unregister")
}

func TestClusterStaleCloseKeepsNewRoute(t *testing.T) {
	f, reg, _ := newClusterFixture(t)

	connA, _ := loginOK(t, f, "erin", "tv")
	_, ackB := loginOK(t, f, "erin", "tv")
	waitFor(t, 2*time.Second, func() bool {
		route, ok, err := reg.LookupRoute(context.Background(), "erin", "tv")
		return err == nil && ok && route.Version == 2
	}, "route to reach version 2")

	// The replaced connection is already closed by the new login. Its close
	// handler must not delete the route owned by the newer session.
	connA.close()
	time.Sleep(300 * time.Millisecond)

	route := routeOf(t, reg, "erin", "tv")
	if route.Version != 2 || route.SessionID != ackB.SessionID {
		t.Fatalf("route after stale close = %+v, want version 2 session %d", route, ackB.SessionID)
	}
}

func TestClusterSendToRemoteEndpointForwards(t *testing.T) {
	f, reg, fwd := newClusterFixture(t)

	ctx := context.Background()
	if err := reg.RegisterNode(ctx, registry.Node{ID: "node-b", Addr: "127.0.0.1:9200", StartedAt: time.Now()}); err != nil {
		t.Fatalf("RegisterNode(node-b): %v", err)
	}
	if err := reg.UpsertRoute(ctx, registry.Route{
		UserID:    "bob",
		DeviceID:  "d0",
		NodeID:    "node-b",
		SessionID: 99,
		Version:   1,
		CreatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("UpsertRoute(bob/d0): %v", err)
	}
	// The endpoints table is shared by every node, so bob's login on node-b is
	// visible here even though the session itself is remote.
	if err := f.store.UpsertEndpoint(ctx, "bob", "d0"); err != nil {
		t.Fatalf("UpsertEndpoint(bob/d0): %v", err)
	}

	alice, aliceAck := loginOK(t, f, "alice", "phone")

	payload := []byte("remote payload")
	ack := alice.sendRouteExpectAck(t, 61, protocol.TargetUser, "bob", payload)
	if ack.Status != protocol.SendAckAccepted {
		t.Fatalf("SEND_ACK status = %d, want ACCEPTED (message %q)", ack.Status, ack.Message)
	}

	waitFor(t, 2*time.Second, func() bool { return len(fwd.forwarded()) == 1 }, "one forward")
	got := fwd.forwarded()[0]
	if got.nodeID != "node-b" || got.addr != "127.0.0.1:9200" {
		t.Errorf("forward target = %s/%s, want node-b/127.0.0.1:9200", got.nodeID, got.addr)
	}
	if got.msgID != ack.MessageID {
		t.Errorf("forward msg_id = %d, want %d", got.msgID, ack.MessageID)
	}
	if got.body.UserID != "bob" || got.body.DeviceID != "d0" {
		t.Errorf("forward body = %+v, want bob/d0", got.body)
	}
	if got.body.FromSessionID != aliceAck.SessionID {
		t.Errorf("forward from_session_id = %d, want %d", got.body.FromSessionID, aliceAck.SessionID)
	}
	if got.body.FromUser != "alice" || got.body.FromDevice != "phone" {
		t.Errorf("forward from = %s/%s, want alice/phone", got.body.FromUser, got.body.FromDevice)
	}
	if !bytes.Equal(got.body.Payload, payload) {
		t.Errorf("forward payload = %q, want %q", got.body.Payload, payload)
	}

	// The remote session is not local, so the sender must not receive a DELIVER.
	alice.expectSilence(t, 300*time.Millisecond)
	if got := deliveryState(t, f, ack.MessageID); got != store.DeliverySent {
		t.Fatalf("delivery state = %d, want SENT", got)
	}
}

func TestClusterHandleNodeDeliver(t *testing.T) {
	f, _, _ := newClusterFixture(t)

	bob, _ := loginOK(t, f, "bob", "d0")

	status, message := f.handler.HandleNodeDeliver(555, &protocol.NodeDeliverBody{
		UserID:        "bob",
		DeviceID:      "d0",
		FromSessionID: 7,
		FromUser:      "alice",
		FromDevice:    "phone",
		Payload:       []byte("delivered by the owner node"),
	})
	if status != protocol.NodeStatusOK {
		t.Fatalf("HandleNodeDeliver status = %d (%q), want OK", status, message)
	}
	d := bob.expectDeliver(t, 555)
	if d.FromUser != "alice" || d.FromDevice != "phone" {
		t.Errorf("DELIVER from = %s/%s, want alice/phone", d.FromUser, d.FromDevice)
	}
	if !bytes.Equal(d.Payload, []byte("delivered by the owner node")) {
		t.Errorf("DELIVER payload = %q, want %q", d.Payload, "delivered by the owner node")
	}

	status, _ = f.handler.HandleNodeDeliver(556, &protocol.NodeDeliverBody{
		UserID:   "bob",
		DeviceID: "ghost",
		Payload:  []byte("nobody home"),
	})
	if status != protocol.NodeStatusNotFound {
		t.Fatalf("HandleNodeDeliver(unknown) status = %d, want NotFound", status)
	}
	// Inbound node deliveries must never persist a second delivery row.
	if n := mustCount(t, f, "SELECT count(*) FROM messages"); n != 0 {
		t.Fatalf("messages count = %d, want 0", n)
	}
}

// TestClusterLoginSucceedsWhenOldNodeUnreachable verifies the login is not held
// hostage by a failing kick: the route is taken over locally even when the old
// node cannot be reached.
func TestClusterLoginSucceedsWhenOldNodeUnreachable(t *testing.T) {
	f, reg, fwd := newClusterFixtureNodeID(t, "node-local")
	fwd.mu.Lock()
	fwd.kickErr = errors.New("node-old unreachable")
	fwd.mu.Unlock()

	ctx := context.Background()
	if err := reg.RegisterNode(ctx, registry.Node{ID: "node-old", Addr: "127.0.0.1:9101", StartedAt: time.Now()}); err != nil {
		t.Fatalf("RegisterNode(node-old): %v", err)
	}
	// Allocate the old version through the registry so the counter is ahead of
	// the seeded route, exactly as a previous login would have left it.
	var oldVersion uint64
	for i := 0; i < 3; i++ {
		v, err := reg.NextVersion(ctx, "frank", "tv")
		if err != nil {
			t.Fatalf("NextVersion(frank/tv): %v", err)
		}
		oldVersion = v
	}
	if err := reg.UpsertRoute(ctx, registry.Route{
		UserID:    "frank",
		DeviceID:  "tv",
		NodeID:    "node-old",
		SessionID: 42,
		Version:   oldVersion,
		CreatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("UpsertRoute(frank/tv): %v", err)
	}

	// loginOK fails the test unless LOGIN_ACK reports accepted.
	_, ack := loginOK(t, f, "frank", "tv")

	route := routeOf(t, reg, "frank", "tv")
	if route.NodeID != "node-local" {
		t.Errorf("route node_id = %q, want %q", route.NodeID, "node-local")
	}
	if route.SessionID != ack.SessionID {
		t.Errorf("route session_id = %d, want %d", route.SessionID, ack.SessionID)
	}
	if route.Version <= oldVersion {
		t.Errorf("route version = %d, want greater than the old %d", route.Version, oldVersion)
	}

	waitFor(t, 2*time.Second, func() bool { return len(fwd.kicked()) >= 1 }, "a kick attempt")
	kick := fwd.kicked()[0]
	if kick.nodeID != "node-old" || kick.addr != "127.0.0.1:9101" {
		t.Errorf("kick target = %s/%s, want node-old/127.0.0.1:9101", kick.nodeID, kick.addr)
	}
	if kick.body.UserID != "frank" || kick.body.DeviceID != "tv" {
		t.Errorf("kick body = %+v, want frank/tv", kick.body)
	}
	if kick.body.Version != route.Version {
		t.Errorf("kick version = %d, want the new route version %d", kick.body.Version, route.Version)
	}
}

// TestClusterForwardFailureLeavesDeliveryRetryable verifies a failed cross-node
// push consumes no retry attempt: the delivery stays SENT with its schedule so
// the owner node or PULL can still recover it.
func TestClusterForwardFailureLeavesDeliveryRetryable(t *testing.T) {
	f, reg, fwd := newClusterFixture(t)
	fwd.mu.Lock()
	fwd.deliverErr = errors.New("node-remote unreachable")
	fwd.mu.Unlock()

	ctx := context.Background()
	if err := reg.RegisterNode(ctx, registry.Node{ID: "node-remote", Addr: "127.0.0.1:9202", StartedAt: time.Now()}); err != nil {
		t.Fatalf("RegisterNode(node-remote): %v", err)
	}
	if err := reg.UpsertRoute(ctx, registry.Route{
		UserID:    "bob",
		DeviceID:  "d0",
		NodeID:    "node-remote",
		SessionID: 77,
		Version:   1,
		CreatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("UpsertRoute(bob/d0): %v", err)
	}
	if err := f.store.UpsertEndpoint(ctx, "bob", "d0"); err != nil {
		t.Fatalf("UpsertEndpoint(bob/d0): %v", err)
	}

	alice, _ := loginOK(t, f, "alice", "phone")
	ack := alice.sendRouteExpectAck(t, 71, protocol.TargetUser, "bob", []byte("retry me"))
	if ack.Status != protocol.SendAckAccepted {
		t.Fatalf("SEND_ACK status = %d, want ACCEPTED (message %q)", ack.Status, ack.Message)
	}
	waitFor(t, 2*time.Second, func() bool { return len(fwd.forwarded()) == 1 }, "a failed forward attempt")

	var (
		state    int16
		attempts int
		next     sql.NullTime
	)
	if err := f.store.DB().QueryRow(
		"SELECT state, attempts, next_retry_at FROM deliveries WHERE message_id = $1 AND user_id = 'bob' AND device_id = 'd0'",
		int64(ack.MessageID)).Scan(&state, &attempts, &next); err != nil {
		t.Fatalf("select delivery: %v", err)
	}
	if store.DeliveryState(state) != store.DeliverySent {
		t.Errorf("delivery state = %d, want SENT (%d)", state, store.DeliverySent)
	}
	if attempts != 1 {
		t.Errorf("delivery attempts = %d, want 1 (a failed forward must not consume a retry)", attempts)
	}
	if !next.Valid {
		t.Error("next_retry_at is NULL, want a retry schedule")
	}
}
