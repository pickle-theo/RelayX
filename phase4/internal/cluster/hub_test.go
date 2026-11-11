package cluster

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"runtime"
	"testing"
	"time"

	"relayx/phase4/internal/protocol"
)

const testMaxBody = 1 << 20

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// testHandler records deliveries and kicks and answers deliveries with a
// configurable status.
type testHandler struct {
	deliveries chan protocol.NodeDeliverBody
	payloads   chan []byte
	msgIDs     chan uint64
	kicks      chan protocol.NodeKickBody
	status     uint8
	message    string
	block      chan struct{}
}

func newTestHandler() *testHandler {
	return &testHandler{
		deliveries: make(chan protocol.NodeDeliverBody, 8),
		payloads:   make(chan []byte, 8),
		msgIDs:     make(chan uint64, 8),
		kicks:      make(chan protocol.NodeKickBody, 8),
		status:     protocol.NodeStatusOK,
	}
}

func (h *testHandler) HandleNodeDeliver(p *Peer, msgID uint64, body *protocol.NodeDeliverBody) (uint8, string) {
	if h.block != nil {
		<-h.block
	}
	h.deliveries <- *body
	h.payloads <- append([]byte(nil), body.Payload...)
	h.msgIDs <- msgID
	return h.status, h.message
}

func (h *testHandler) HandleNodeKick(p *Peer, body *protocol.NodeKickBody) {
	h.kicks <- *body
}

func newTestHub(t *testing.T, nodeID string) *Hub {
	t.Helper()
	hub, err := NewHub(Config{NodeID: nodeID, ListenAddr: "127.0.0.1:0", Logger: testLogger()})
	if err != nil {
		t.Fatalf("NewHub(%s): %v", nodeID, err)
	}
	t.Cleanup(func() { _ = hub.Close() })
	hub.Start()
	return hub
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", description)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func writeFrame(t *testing.T, nc net.Conn, f *protocol.Frame) {
	t.Helper()
	if err := nc.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}
	if err := protocol.Encode(nc, f, testMaxBody); err != nil {
		t.Fatalf("write %s frame: %v", f.Type, err)
	}
}

func readFrame(t *testing.T, r *bufio.Reader, nc net.Conn) *protocol.Frame {
	t.Helper()
	if err := nc.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	f, err := protocol.Decode(r, testMaxBody)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	return f
}

// rawPeer is a hand-driven node connection used to exercise the wire protocol
// directly.
type rawPeer struct {
	nc net.Conn
	r  *bufio.Reader
}

// dialNode performs a NODE_HELLO handshake and asserts the hub accepted it.
func dialNode(t *testing.T, hubAddr, nodeID string) *rawPeer {
	t.Helper()
	nc, err := net.DialTimeout("tcp", hubAddr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial node hub: %v", err)
	}
	t.Cleanup(func() { _ = nc.Close() })

	body, err := protocol.AppendNodeHelloBody(nil, &protocol.NodeHelloBody{
		ProtocolVersion: protocol.Version,
		NodeID:          nodeID,
		AdvertiseAddr:   "127.0.0.1:1",
	})
	if err != nil {
		t.Fatalf("AppendNodeHelloBody: %v", err)
	}
	writeFrame(t, nc, &protocol.Frame{Type: protocol.NodeHello, Body: body})

	r := bufio.NewReaderSize(nc, 4096)
	f := readFrame(t, r, nc)
	if f.Type != protocol.NodeHelloAck {
		t.Fatalf("handshake frame = %v, want NODE_HELLO_ACK", f.Type)
	}
	ack, err := protocol.DecodeNodeHelloAck(f.Body)
	if err != nil {
		t.Fatalf("DecodeNodeHelloAck: %v", err)
	}
	if ack.Status != protocol.NodeStatusOK {
		t.Fatalf("handshake status = %d, want OK", ack.Status)
	}
	return &rawPeer{nc: nc, r: r}
}

// TestNodeDeliverBetweenHubs verifies the full path: two hubs connect on
// demand, the delivery reaches the remote handler with all fields intact and
// both sides converge on one peer.
func TestNodeDeliverBetweenHubs(t *testing.T) {
	hubA := newTestHub(t, "node-a")
	hubB := newTestHub(t, "node-b")

	handlerA := newTestHandler()
	handlerB := newTestHandler()
	hubA.SetHandler(handlerA)
	hubB.SetHandler(handlerB)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := &protocol.NodeDeliverBody{
		UserID:        "bob",
		DeviceID:      "phone",
		FromSessionID: 77,
		FromUser:      "alice",
		FromDevice:    "laptop",
		Payload:       []byte("hello node"),
	}
	if err := hubA.ForwardDeliver(ctx, "node-b", hubB.Addr(), 123, body); err != nil {
		t.Fatalf("ForwardDeliver a->b: %v", err)
	}
	select {
	case got := <-handlerB.deliveries:
		if got.UserID != body.UserID || got.DeviceID != body.DeviceID ||
			got.FromSessionID != body.FromSessionID || got.FromUser != body.FromUser ||
			got.FromDevice != body.FromDevice {
			t.Fatalf("delivery = %+v, want %+v", got, *body)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handler B did not receive the delivery")
	}
	select {
	case payload := <-handlerB.payloads:
		if !bytes.Equal(payload, body.Payload) {
			t.Fatalf("payload = %q, want %q", payload, body.Payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handler B did not report the payload")
	}

	if err := hubB.ForwardDeliver(ctx, "node-a", hubA.Addr(), 124, body); err != nil {
		t.Fatalf("ForwardDeliver b->a: %v", err)
	}
	select {
	case <-handlerA.deliveries:
	case <-time.After(3 * time.Second):
		t.Fatal("handler A did not receive the delivery")
	}

	waitFor(t, 3*time.Second, func() bool {
		return hubA.PeerCount() == 1 && hubB.PeerCount() == 1
	}, "both hubs to report one peer")
}

// TestNodeDeliverAck verifies that a NODE_DELIVER is answered with a
// NODE_DELIVER_ACK carrying the request MsgID and the handler outcome.
func TestNodeDeliverAck(t *testing.T) {
	hub := newTestHub(t, "node-a")
	handler := newTestHandler()
	handler.status = protocol.NodeStatusNotFound
	handler.message = "device offline"
	hub.SetHandler(handler)

	peer := dialNode(t, hub.Addr(), "node-b")
	waitFor(t, 3*time.Second, func() bool { return hub.PeerCount() == 1 }, "peer registration")

	body, err := protocol.AppendNodeDeliverBody(nil, &protocol.NodeDeliverBody{
		UserID: "bob", DeviceID: "phone", FromSessionID: 3, FromUser: "alice", FromDevice: "laptop", Payload: []byte("hi"),
	})
	if err != nil {
		t.Fatalf("AppendNodeDeliverBody: %v", err)
	}
	writeFrame(t, peer.nc, &protocol.Frame{Type: protocol.NodeDeliver, MsgID: 42, Body: body})

	f := readFrame(t, peer.r, peer.nc)
	if f.Type != protocol.NodeDeliverAck {
		t.Fatalf("response type = %v, want NODE_DELIVER_ACK", f.Type)
	}
	if f.MsgID != 42 {
		t.Fatalf("response msg_id = %d, want 42", f.MsgID)
	}
	ack, err := protocol.DecodeNodeDeliverAck(f.Body)
	if err != nil {
		t.Fatalf("DecodeNodeDeliverAck: %v", err)
	}
	if ack.Status != protocol.NodeStatusNotFound || ack.Message != "device offline" {
		t.Fatalf("ack = %+v, want status %d message %q", *ack, protocol.NodeStatusNotFound, "device offline")
	}

	select {
	case got := <-handler.deliveries:
		if got.UserID != "bob" || got.DeviceID != "phone" {
			t.Fatalf("delivery = %+v, want bob/phone", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handler did not receive the delivery")
	}
	select {
	case msgID := <-handler.msgIDs:
		if msgID != 42 {
			t.Fatalf("handler msg_id = %d, want the frame msg_id 42", msgID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handler did not report the msg_id")
	}
}

// TestNodeKickReachesPeer verifies that Kick travels over the direct channel.
func TestNodeKickReachesPeer(t *testing.T) {
	hubA := newTestHub(t, "node-a")
	hubB := newTestHub(t, "node-b")
	handlerB := newTestHandler()
	hubB.SetHandler(handlerB)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := hubA.Kick(ctx, "node-b", hubB.Addr(), &protocol.NodeKickBody{UserID: "bob", DeviceID: "phone", Version: 9}); err != nil {
		t.Fatalf("Kick: %v", err)
	}

	select {
	case got := <-handlerB.kicks:
		if got.UserID != "bob" || got.DeviceID != "phone" || got.Version != 9 {
			t.Fatalf("kick = %+v, want bob/phone version 9", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handler B did not receive the kick")
	}
}

// TestSelfConnectionRejected verifies that a node announcing the hub's own ID
// is refused during the handshake.
func TestSelfConnectionRejected(t *testing.T) {
	hub := newTestHub(t, "node-a")

	nc, err := net.DialTimeout("tcp", hub.Addr(), 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer nc.Close()

	body, err := protocol.AppendNodeHelloBody(nil, &protocol.NodeHelloBody{
		ProtocolVersion: protocol.Version,
		NodeID:          "node-a",
		AdvertiseAddr:   "127.0.0.1:1",
	})
	if err != nil {
		t.Fatalf("AppendNodeHelloBody: %v", err)
	}
	writeFrame(t, nc, &protocol.Frame{Type: protocol.NodeHello, Body: body})

	r := bufio.NewReaderSize(nc, 4096)
	f, err := protocol.Decode(r, testMaxBody)
	if err != nil {
		// Closing without an ack is an acceptable rejection too.
		return
	}
	if f.Type != protocol.NodeHelloAck {
		t.Fatalf("self-connect response = %v, want NODE_HELLO_ACK", f.Type)
	}
	ack, err := protocol.DecodeNodeHelloAck(f.Body)
	if err != nil {
		t.Fatalf("DecodeNodeHelloAck: %v", err)
	}
	if ack.Status != protocol.NodeStatusInvalid {
		t.Fatalf("self-connect status = %d, want %d", ack.Status, protocol.NodeStatusInvalid)
	}

	if err := nc.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, err := r.ReadByte(); err == nil {
		t.Fatal("self connection stayed open after the rejection")
	}
	if got := hub.PeerCount(); got != 0 {
		t.Fatalf("PeerCount = %d, want 0 after a rejected self connection", got)
	}
}

// TestDuplicateNodeIDReplacesPeer verifies that a new inbound connection for an
// existing node ID replaces the old one, converging to a single peer.
func TestDuplicateNodeIDReplacesPeer(t *testing.T) {
	hub := newTestHub(t, "node-a")
	handler := newTestHandler()
	hub.SetHandler(handler)

	oldPeer := dialNode(t, hub.Addr(), "dup")
	waitFor(t, 3*time.Second, func() bool { return hub.PeerCount() == 1 }, "first peer registration")

	newPeer := dialNode(t, hub.Addr(), "dup")
	waitFor(t, 3*time.Second, func() bool { return hub.PeerCount() == 1 }, "peer count after replacement")

	// The old connection must be closed by the hub.
	if err := oldPeer.nc.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	var buf [64]byte
	_, err := oldPeer.r.Read(buf[:])
	if err == nil {
		t.Fatal("old peer received data, want it to be closed")
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Fatal("old peer was not closed after replacement")
	}

	// The new connection stays usable.
	writeFrame(t, newPeer.nc, &protocol.Frame{Type: protocol.NodeKick, Body: mustKickBody(t, "bob", "phone", 1)})
	select {
	case <-handler.kicks:
	case <-time.After(3 * time.Second):
		t.Fatal("replacement peer did not deliver the kick")
	}
	if got := hub.PeerCount(); got != 1 {
		t.Fatalf("PeerCount = %d, want 1", got)
	}
}

// TestReconnectAfterHubRestart verifies that a failed dial is reported, that
// the backoff window expires and that a later send reconnects to a hub
// restarted on the same address.
func TestReconnectAfterHubRestart(t *testing.T) {
	first := newTestHub(t, "node-a")
	addr := first.Addr()
	if err := first.Close(); err != nil {
		t.Fatalf("close first hub: %v", err)
	}

	hubB := newTestHub(t, "node-b")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	body := &protocol.NodeDeliverBody{UserID: "bob", DeviceID: "phone", FromUser: "alice", FromDevice: "laptop"}

	if err := hubB.ForwardDeliver(ctx, "node-a", addr, 1, body); err == nil {
		t.Fatal("ForwardDeliver to a closed hub succeeded, want an error")
	}
	// The first failure arms a 100ms backoff; wait it out before retrying.
	time.Sleep(150 * time.Millisecond)

	restarted, err := NewHub(Config{NodeID: "node-a", ListenAddr: addr, Logger: testLogger()})
	if err != nil {
		t.Fatalf("restart hub on %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	handler := newTestHandler()
	restarted.SetHandler(handler)
	restarted.Start()

	if err := hubB.ForwardDeliver(ctx, "node-a", addr, 2, body); err != nil {
		t.Fatalf("ForwardDeliver after restart: %v", err)
	}
	select {
	case got := <-handler.deliveries:
		if got.UserID != "bob" || got.DeviceID != "phone" {
			t.Fatalf("delivery = %+v, want bob/phone", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("restarted hub did not receive the delivery")
	}
}

// TestSendQueueFullDoesNotBlock verifies that a peer which stops reading
// eventually causes ForwardDeliver to return an error instead of blocking.
func TestSendQueueFullDoesNotBlock(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })

	go func() {
		nc, err := ln.Accept()
		if err != nil {
			return
		}
		defer nc.Close()

		r := bufio.NewReaderSize(nc, 4096)
		if _, err := protocol.Decode(r, testMaxBody); err != nil {
			return
		}
		ack, err := protocol.AppendNodeHelloAckBody(nil, &protocol.NodeHelloAckBody{Status: protocol.NodeStatusOK})
		if err != nil {
			return
		}
		if err := protocol.Encode(nc, &protocol.Frame{Type: protocol.NodeHelloAck, Body: ack}, testMaxBody); err != nil {
			return
		}
		// Stop reading: the hub's write path must hit its queue budget.
		<-stop
	}()

	hub, err := NewHub(Config{
		NodeID:      "node-a",
		ListenAddr:  "127.0.0.1:0",
		QueueSize:   1,
		QueueBytes:  64 << 10,
		DialTimeout: 500 * time.Millisecond,
		Logger:      testLogger(),
	})
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	t.Cleanup(func() { _ = hub.Close() })
	hub.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	payload := bytes.Repeat([]byte("x"), 32<<10)

	start := time.Now()
	var sendErr error
	for i := 0; i < 100; i++ {
		sendErr = hub.ForwardDeliver(ctx, "node-b", ln.Addr().String(), uint64(i+1), &protocol.NodeDeliverBody{
			UserID: "bob", DeviceID: "phone", FromUser: "alice", FromDevice: "laptop", Payload: payload,
		})
		if sendErr != nil {
			break
		}
	}
	if sendErr == nil {
		t.Fatal("ForwardDeliver kept succeeding against a peer that does not read")
	}
	if !errors.Is(sendErr, ErrQueueFull) && !errors.Is(sendErr, ErrPeerClosed) {
		t.Fatalf("ForwardDeliver error = %v, want %v or %v", sendErr, ErrQueueFull, ErrPeerClosed)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("ForwardDeliver blocked for %s, want a prompt error", elapsed)
	}
}

// TestHubCloseNoGoroutineLeak verifies that Close stops the listener and every
// peer goroutine.
func TestHubCloseNoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	hubA := newTestHub(t, "node-a")
	hubB := newTestHub(t, "node-b")
	handlerB := newTestHandler()
	hubB.SetHandler(handlerB)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	body := &protocol.NodeDeliverBody{UserID: "bob", DeviceID: "phone", FromUser: "alice", FromDevice: "laptop", Payload: []byte("bye")}
	if err := hubA.ForwardDeliver(ctx, "node-b", hubB.Addr(), 1, body); err != nil {
		t.Fatalf("ForwardDeliver: %v", err)
	}
	select {
	case <-handlerB.deliveries:
	case <-time.After(3 * time.Second):
		t.Fatal("delivery did not arrive")
	}

	if err := hubA.Close(); err != nil {
		t.Fatalf("close hub A: %v", err)
	}
	if err := hubB.Close(); err != nil {
		t.Fatalf("close hub B: %v", err)
	}

	// A small allowance covers runtime bookkeeping goroutines that are not
	// owned by the hubs.
	deadline := time.Now().Add(3 * time.Second)
	for {
		got := runtime.NumGoroutine()
		if got <= before+5 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines after Close = %d, before = %d", got, before)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func mustKickBody(t *testing.T, userID, deviceID string, version uint64) []byte {
	t.Helper()
	body, err := protocol.AppendNodeKickBody(nil, &protocol.NodeKickBody{UserID: userID, DeviceID: deviceID, Version: version})
	if err != nil {
		t.Fatalf("AppendNodeKickBody: %v", err)
	}
	return body
}
