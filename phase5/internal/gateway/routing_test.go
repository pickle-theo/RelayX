package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"relayx/phase5/internal/config"
	"relayx/phase5/internal/protocol"
	"relayx/phase5/internal/store"
)

// sendRoute sends a SEND frame addressed to targetType/target.
func (c *testClient) sendRoute(t *testing.T, msgID uint64, targetType protocol.RouteTargetType, target string, payload []byte) {
	t.Helper()
	body, err := protocol.AppendSendBody(nil, &protocol.SendBody{
		TargetType: targetType,
		Target:     target,
		Payload:    payload,
	})
	if err != nil {
		t.Fatalf("encode SEND body: %v", err)
	}
	c.send(t, &protocol.Frame{Type: protocol.Send, MsgID: msgID, Body: body})
}

// expectSendAck reads one frame, asserts it is a SEND_ACK carrying msgID and
// returns the decoded body.
func (c *testClient) expectSendAck(t *testing.T, msgID uint64) *protocol.SendAckBody {
	t.Helper()
	f := c.read(t)
	if f.Type != protocol.SendAck {
		t.Fatalf("frame type = %v (body %q), want SEND_ACK", f.Type, f.Body)
	}
	if f.MsgID != msgID {
		t.Fatalf("SEND_ACK msg_id = %d, want %d", f.MsgID, msgID)
	}
	ack, err := protocol.DecodeSendAck(f.Body)
	if err != nil {
		t.Fatalf("decode SEND_ACK body: %v", err)
	}
	return ack
}

// sendRouteExpectAck sends a SEND frame and returns its SEND_ACK.
func (c *testClient) sendRouteExpectAck(t *testing.T, msgID uint64, targetType protocol.RouteTargetType, target string, payload []byte) *protocol.SendAckBody {
	t.Helper()
	c.sendRoute(t, msgID, targetType, target, payload)
	return c.expectSendAck(t, msgID)
}

// expectDeliver reads one frame, asserts it is a DELIVER carrying msgID and
// returns the decoded body.
func (c *testClient) expectDeliver(t *testing.T, msgID uint64) *protocol.DeliverBody {
	t.Helper()
	f := c.read(t)
	if f.Type != protocol.Deliver {
		t.Fatalf("frame type = %v (body %q), want DELIVER", f.Type, f.Body)
	}
	if f.MsgID != msgID {
		t.Fatalf("DELIVER msg_id = %d, want %d", f.MsgID, msgID)
	}
	d, err := protocol.DecodeDeliver(f.Body)
	if err != nil {
		t.Fatalf("decode DELIVER body: %v", err)
	}
	return d
}

// expectPullDone reads one frame, asserts it is a PULL_DONE and returns the
// decoded body.
func (c *testClient) expectPullDone(t *testing.T, msgID uint64) *protocol.PullDoneBody {
	t.Helper()
	f := c.read(t)
	if f.Type != protocol.PullDone {
		t.Fatalf("frame type = %v (body %q), want PULL_DONE", f.Type, f.Body)
	}
	if f.MsgID != msgID {
		t.Fatalf("PULL_DONE msg_id = %d, want %d", f.MsgID, msgID)
	}
	done, err := protocol.DecodePullDone(f.Body)
	if err != nil {
		t.Fatalf("decode PULL_DONE body: %v", err)
	}
	return done
}

// expectError reads one frame, asserts it is an ERROR carrying msgID whose
// body contains substr and returns the body.
func (c *testClient) expectError(t *testing.T, msgID uint64, substr string) string {
	t.Helper()
	f := c.read(t)
	if f.Type != protocol.Error {
		t.Fatalf("frame type = %v, want ERROR", f.Type)
	}
	if f.MsgID != msgID {
		t.Fatalf("ERROR msg_id = %d, want %d", f.MsgID, msgID)
	}
	got := string(f.Body)
	if !strings.Contains(got, substr) {
		t.Fatalf("ERROR body = %q, want it to contain %q", got, substr)
	}
	return got
}

// expectSilence verifies that nothing arrives within d.
func (c *testClient) expectSilence(t *testing.T, d time.Duration) {
	t.Helper()
	if err := c.conn.SetReadDeadline(time.Now().Add(d)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	frame, err := protocol.Decode(c.r, testMaxBody)
	if err == nil {
		t.Fatalf("received unexpected %v frame %q", frame.Type, frame.Body)
	}
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("read error = %v, want a timeout", err)
	}
}

// isClosedError reports whether err belongs to the "connection is gone" family
// returned while reading from a closed socket.
func isClosedError(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed)
}

// trySendRoute is sendRoute but returns the socket write error instead of
// failing the test, so a burst can be sent while tolerating writes that the
// gateway rejects with a closed connection.
func (c *testClient) trySendRoute(t *testing.T, msgID uint64, targetType protocol.RouteTargetType, target string, payload []byte) error {
	t.Helper()
	body, err := protocol.AppendSendBody(nil, &protocol.SendBody{
		TargetType: targetType,
		Target:     target,
		Payload:    payload,
	})
	if err != nil {
		t.Fatalf("encode SEND body: %v", err)
	}
	var buf bytes.Buffer
	if err := protocol.Encode(&buf, &protocol.Frame{Type: protocol.Send, MsgID: msgID, Body: body}, testMaxBody); err != nil {
		t.Fatalf("encode SEND frame: %v", err)
	}
	if err := c.conn.SetWriteDeadline(time.Now().Add(testIOTimeout)); err != nil {
		return err
	}
	_, err = c.conn.Write(buf.Bytes())
	return err
}

// loginOK dials the gateway and logs in, failing the test on a non-OK status.
func loginOK(t *testing.T, f *gatewayFixture, userID, deviceID string) (*testClient, *protocol.LoginAckBody) {
	t.Helper()
	c := dialGateway(t, f.srv.Addr())
	ack := c.sendLogin(t, 1, loginBody(t, userID, deviceID))
	if ack.Status != protocol.LoginStatusOK {
		t.Fatalf("login %s/%s status = %d, want %d (message %q)",
			userID, deviceID, ack.Status, protocol.LoginStatusOK, ack.Message)
	}
	return c, ack
}

func mustCount(t *testing.T, f *gatewayFixture, query string, args ...any) int {
	t.Helper()
	var n int
	if err := f.store.DB().QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return n
}

func deliveryState(t *testing.T, f *gatewayFixture, messageID uint64) store.DeliveryState {
	t.Helper()
	var state int16
	if err := f.store.DB().QueryRow(
		"SELECT state FROM deliveries WHERE message_id = $1 AND user_id = 'bob' AND device_id = 'd0'",
		int64(messageID)).Scan(&state); err != nil {
		t.Fatalf("select delivery state: %v", err)
	}
	return store.DeliveryState(state)
}

func messageState(t *testing.T, f *gatewayFixture, messageID uint64) store.MessageState {
	t.Helper()
	var state int16
	if err := f.store.DB().QueryRow("SELECT state FROM messages WHERE id = $1", int64(messageID)).Scan(&state); err != nil {
		t.Fatalf("select message state: %v", err)
	}
	return store.MessageState(state)
}

// TestFanOutToUserDevices verifies that a SEND addressed to a user persists one
// message with two deliveries and reaches every online device.
func TestFanOutToUserDevices(t *testing.T) {
	f := startGateway(t)

	bobPC, _ := loginOK(t, f, "bob", "pc")
	bobTablet, _ := loginOK(t, f, "bob", "tablet")
	alice, _ := loginOK(t, f, "alice", "phone")

	payload := []byte("fan-out payload")
	ack := alice.sendRouteExpectAck(t, 42, protocol.TargetUser, "bob", payload)
	if ack.Status != protocol.SendAckAccepted {
		t.Fatalf("SEND_ACK status = %d, want ACCEPTED (message %q)", ack.Status, ack.Message)
	}
	if ack.MessageID == 0 {
		t.Fatal("SEND_ACK message_id = 0, want the platform message id")
	}
	flushPipeline(t, f)

	for name, conn := range map[string]*testClient{"bob/pc": bobPC, "bob/tablet": bobTablet} {
		d := conn.expectDeliver(t, ack.MessageID)
		if d.FromUser != "alice" {
			t.Errorf("%s: DELIVER from_user = %q, want %q", name, d.FromUser, "alice")
		}
		if d.FromDevice != "phone" {
			t.Errorf("%s: DELIVER from_device = %q, want %q", name, d.FromDevice, "phone")
		}
		if !bytes.Equal(d.Payload, payload) {
			t.Errorf("%s: DELIVER payload = %q, want %q", name, d.Payload, payload)
		}
	}

	if n := mustCount(t, f, "SELECT count(*) FROM messages"); n != 1 {
		t.Fatalf("messages count = %d, want 1", n)
	}
	if n := mustCount(t, f, "SELECT count(*) FROM deliveries"); n != 2 {
		t.Fatalf("deliveries count = %d, want 2", n)
	}
}

// TestEndpointDeliveryIsolated verifies that a SEND addressed to one endpoint
// reaches only that session.
func TestEndpointDeliveryIsolated(t *testing.T) {
	f := startGateway(t)

	bobPC, pcAck := loginOK(t, f, "bob", "pc")
	bobTablet, _ := loginOK(t, f, "bob", "tablet")
	alice, _ := loginOK(t, f, "alice", "phone")

	ack := alice.sendRouteExpectAck(t, 7, protocol.TargetEndpoint, strconv.FormatUint(pcAck.SessionID, 10), []byte("only pc"))
	if ack.Status != protocol.SendAckAccepted {
		t.Fatalf("SEND_ACK status = %d, want ACCEPTED", ack.Status)
	}
	flushPipeline(t, f)

	d := bobPC.expectDeliver(t, ack.MessageID)
	if d.FromUser != "alice" || d.FromDevice != "phone" {
		t.Fatalf("DELIVER from %s/%s, want alice/phone", d.FromUser, d.FromDevice)
	}
	if !bytes.Equal(d.Payload, []byte("only pc")) {
		t.Fatalf("DELIVER payload = %q, want %q", d.Payload, "only pc")
	}
	bobTablet.expectSilence(t, 300*time.Millisecond)

	if n := mustCount(t, f, "SELECT count(*) FROM deliveries"); n != 1 {
		t.Fatalf("deliveries count = %d, want 1", n)
	}
}

// TestSendToSelf verifies that a user may send to itself.
func TestSendToSelf(t *testing.T) {
	f := startGateway(t)

	alice, _ := loginOK(t, f, "alice", "phone")

	payload := []byte("note to self")
	ack := alice.sendRouteExpectAck(t, 41, protocol.TargetUser, "alice", payload)
	if ack.Status != protocol.SendAckAccepted {
		t.Fatalf("SEND_ACK status = %d, want ACCEPTED", ack.Status)
	}
	flushPipeline(t, f)
	d := alice.expectDeliver(t, ack.MessageID)
	if d.FromUser != "alice" || d.FromDevice != "phone" {
		t.Fatalf("DELIVER from %s/%s, want alice/phone", d.FromUser, d.FromDevice)
	}
	if !bytes.Equal(d.Payload, payload) {
		t.Fatalf("DELIVER payload = %q, want %q", d.Payload, payload)
	}
}

// TestDuplicateSendIsIdempotent verifies that resending the same (fromUser,
// MsgID) returns DUPLICATE with the original message ID, writes no extra rows
// and does not push the message again.
func TestDuplicateSendIsIdempotent(t *testing.T) {
	f := startGateway(t)

	bob, _ := loginOK(t, f, "bob", "d0")
	alice, _ := loginOK(t, f, "alice", "phone")

	first := alice.sendRouteExpectAck(t, 99, protocol.TargetUser, "bob", []byte("once"))
	if first.Status != protocol.SendAckAccepted {
		t.Fatalf("first SEND_ACK status = %d, want ACCEPTED", first.Status)
	}
	flushPipeline(t, f)
	bob.expectDeliver(t, first.MessageID)

	second := alice.sendRouteExpectAck(t, 99, protocol.TargetUser, "bob", []byte("once"))
	if second.Status != protocol.SendAckDuplicate {
		t.Fatalf("second SEND_ACK status = %d, want DUPLICATE", second.Status)
	}
	if second.MessageID != first.MessageID {
		t.Fatalf("second SEND_ACK message_id = %d, want %d", second.MessageID, first.MessageID)
	}
	if second.Message != "duplicate" {
		t.Fatalf("second SEND_ACK message = %q, want %q", second.Message, "duplicate")
	}
	bob.expectSilence(t, 300*time.Millisecond)

	if n := mustCount(t, f, "SELECT count(*) FROM messages"); n != 1 {
		t.Fatalf("messages count = %d, want 1", n)
	}
	if n := mustCount(t, f, "SELECT count(*) FROM outbox"); n != 1 {
		t.Fatalf("outbox count = %d, want 1 (a duplicate must not enqueue another event)", n)
	}
}

// TestSendToUnknownUserRejected verifies that a user without any registered
// endpoint produces SEND_ACK INVALID and no persisted rows.
func TestSendToUnknownUserRejected(t *testing.T) {
	f := startGateway(t)

	alice, _ := loginOK(t, f, "alice", "phone")

	ack := alice.sendRouteExpectAck(t, 21, protocol.TargetUser, "nobody", []byte("anyone there?"))
	if ack.Status != protocol.SendAckInvalid {
		t.Fatalf("SEND_ACK status = %d, want INVALID", ack.Status)
	}
	if !strings.Contains(ack.Message, "target has no endpoint") {
		t.Fatalf("SEND_ACK message = %q, want it to mention the missing endpoint", ack.Message)
	}
	if n := mustCount(t, f, "SELECT count(*) FROM messages"); n != 0 {
		t.Fatalf("messages count = %d, want 0", n)
	}

	// The connection must stay usable.
	body := []byte("still alive")
	alice.send(t, &protocol.Frame{Type: protocol.Data, MsgID: 22, Body: body})
	reply := alice.read(t)
	if reply.Type != protocol.Data || reply.MsgID != 22 || !bytes.Equal(reply.Body, body) {
		t.Fatalf("after INVALID got frame %v msg_id %d body %q, want DATA 22 %q",
			reply.Type, reply.MsgID, reply.Body, body)
	}
}

// TestSendToUnknownEndpointRejected verifies the INVALID status reported for
// endpoints that do not exist and for targets that are not session IDs.
func TestSendToUnknownEndpointRejected(t *testing.T) {
	f := startGateway(t)

	alice, _ := loginOK(t, f, "alice", "phone")

	for _, target := range []string{"99999", "abc"} {
		ack := alice.sendRouteExpectAck(t, 31, protocol.TargetEndpoint, target, []byte("nobody home"))
		if ack.Status != protocol.SendAckInvalid {
			t.Fatalf("target %q: SEND_ACK status = %d, want INVALID", target, ack.Status)
		}
		if ack.Message != "target not found" {
			t.Fatalf("target %q: SEND_ACK message = %q, want %q", target, ack.Message, "target not found")
		}
	}
}

// TestReceiptAndBusinessAck verifies that both ACK levels move the delivery
// state, that repeated ACKs are harmless and that the connection stays usable.
// The message is recovered through PULL so the delivery is SENT, which is the
// only state the ACK updates accept.
func TestReceiptAndBusinessAck(t *testing.T) {
	f := startGateway(t)

	offline, _ := loginOK(t, f, "bob", "d0")
	offline.close()
	waitFor(t, 2*time.Second, func() bool {
		return f.sessions.FindByDevice("bob", "d0") == nil
	}, "bob/d0 to go offline")

	alice, _ := loginOK(t, f, "alice", "phone")
	ack := alice.sendRouteExpectAck(t, 5, protocol.TargetUser, "bob", []byte("ack me"))
	if ack.Status != protocol.SendAckAccepted {
		t.Fatalf("SEND_ACK status = %d, want ACCEPTED", ack.Status)
	}
	flushPipeline(t, f)

	bob, _ := loginOK(t, f, "bob", "d0")
	pullBody, err := protocol.AppendPullBody(nil, &protocol.PullBody{Cursor: 0, Limit: 100})
	if err != nil {
		t.Fatalf("encode PULL body: %v", err)
	}
	bob.send(t, &protocol.Frame{Type: protocol.Pull, MsgID: 9, Body: pullBody})
	bob.expectDeliver(t, ack.MessageID)
	bob.expectPullDone(t, 9)

	receiptBody, err := protocol.AppendReceiptAckBody(nil, &protocol.ReceiptAckBody{MessageID: ack.MessageID})
	if err != nil {
		t.Fatalf("encode RECEIPT_ACK body: %v", err)
	}
	bob.send(t, &protocol.Frame{Type: protocol.ReceiptAck, MsgID: 100, Body: receiptBody})
	waitFor(t, 2*time.Second, func() bool {
		return deliveryState(t, f, ack.MessageID) == store.DeliveryReceiptAcked
	}, "delivery to become RECEIPT_ACKED")

	businessBody, err := protocol.AppendBusinessAckBody(nil, &protocol.BusinessAckBody{MessageID: ack.MessageID})
	if err != nil {
		t.Fatalf("encode BUSINESS_ACK body: %v", err)
	}
	bob.send(t, &protocol.Frame{Type: protocol.BusinessAck, MsgID: 101, Body: businessBody})
	waitFor(t, 2*time.Second, func() bool {
		return deliveryState(t, f, ack.MessageID) == store.DeliveryBusinessAcked
	}, "delivery to become BUSINESS_ACKED")

	if got := messageState(t, f, ack.MessageID); got != store.MessageCompleted {
		t.Fatalf("message state = %d, want COMPLETED (%d)", got, store.MessageCompleted)
	}

	// A repeated ACK is a no-op and must not produce a response frame: the next
	// frame read is the DATA echo.
	bob.send(t, &protocol.Frame{Type: protocol.BusinessAck, MsgID: 102, Body: businessBody})
	echo := []byte("still alive")
	bob.send(t, &protocol.Frame{Type: protocol.Data, MsgID: 103, Body: echo})
	reply := bob.read(t)
	if reply.Type != protocol.Data || reply.MsgID != 103 || !bytes.Equal(reply.Body, echo) {
		t.Fatalf("after repeated ACK got frame %v msg_id %d body %q, want DATA 103 %q",
			reply.Type, reply.MsgID, reply.Body, echo)
	}
	if got := deliveryState(t, f, ack.MessageID); got != store.DeliveryBusinessAcked {
		t.Fatalf("delivery state after repeated ACK = %d, want BUSINESS_ACKED", got)
	}
}

// TestAckBeforeLoginRejected verifies that ACK frames from an unauthenticated
// connection are answered with ERROR and change nothing.
func TestAckBeforeLoginRejected(t *testing.T) {
	f := startGateway(t)
	c := dialGateway(t, f.srv.Addr())

	body, err := protocol.AppendReceiptAckBody(nil, &protocol.ReceiptAckBody{MessageID: 1})
	if err != nil {
		t.Fatalf("encode RECEIPT_ACK body: %v", err)
	}
	c.send(t, &protocol.Frame{Type: protocol.ReceiptAck, MsgID: 8, Body: body})
	c.expectError(t, 8, "not logged in")
}

// TestPullRestoresOfflineMessages verifies that a message accepted while the
// target was offline is persisted and later recovered through PULL.
func TestPullRestoresOfflineMessages(t *testing.T) {
	f := startGateway(t)

	// Register bob/d0, then take the device offline.
	offline, _ := loginOK(t, f, "bob", "d0")
	offline.close()
	waitFor(t, 2*time.Second, func() bool {
		return f.sessions.FindByDevice("bob", "d0") == nil
	}, "bob/d0 to go offline")

	alice, _ := loginOK(t, f, "alice", "phone")
	ack := alice.sendRouteExpectAck(t, 5, protocol.TargetUser, "bob", []byte("while you were away"))
	if ack.Status != protocol.SendAckAccepted {
		t.Fatalf("SEND_ACK status = %d, want ACCEPTED", ack.Status)
	}
	flushPipeline(t, f)
	if n := mustCount(t, f, "SELECT count(*) FROM deliveries"); n != 1 {
		t.Fatalf("deliveries count = %d, want 1", n)
	}

	bob, _ := loginOK(t, f, "bob", "d0")
	pullBody, err := protocol.AppendPullBody(nil, &protocol.PullBody{Cursor: 0, Limit: 100})
	if err != nil {
		t.Fatalf("encode PULL body: %v", err)
	}
	bob.send(t, &protocol.Frame{Type: protocol.Pull, MsgID: 9, Body: pullBody})

	d := bob.expectDeliver(t, ack.MessageID)
	if !bytes.Equal(d.Payload, []byte("while you were away")) {
		t.Fatalf("DELIVER payload = %q, want %q", d.Payload, "while you were away")
	}
	done := bob.expectPullDone(t, 9)
	if done.Count != 1 {
		t.Fatalf("PULL_DONE count = %d, want 1", done.Count)
	}
	if done.NextCursor == 0 {
		t.Fatal("PULL_DONE next_cursor = 0, want the delivery id")
	}
	if done.HasMore {
		t.Fatal("PULL_DONE has_more = true, want false")
	}
	if got := deliveryState(t, f, ack.MessageID); got != store.DeliverySent {
		t.Fatalf("delivery state after PULL = %d, want SENT", got)
	}

	// A second page must be empty and keep the cursor.
	nextBody, err := protocol.AppendPullBody(nil, &protocol.PullBody{Cursor: done.NextCursor, Limit: 100})
	if err != nil {
		t.Fatalf("encode PULL body: %v", err)
	}
	bob.send(t, &protocol.Frame{Type: protocol.Pull, MsgID: 10, Body: nextBody})
	empty := bob.expectPullDone(t, 10)
	if empty.Count != 0 || empty.HasMore || empty.NextCursor != done.NextCursor {
		t.Fatalf("second PULL_DONE = %+v, want count=0 has_more=false cursor=%d", empty, done.NextCursor)
	}
}

// TestDuplicateLoginKicksOldConnection verifies that a second login for the
// same (user, device) replaces the first connection.
func TestDuplicateLoginKicksOldConnection(t *testing.T) {
	f := startGateway(t)

	connA, ackA := loginOK(t, f, "carol", "tv")
	_, ackB := loginOK(t, f, "carol", "tv")

	if ackB.SessionID == ackA.SessionID {
		t.Fatalf("second login reused session id %d, want a new one", ackA.SessionID)
	}

	// The old connection is kicked: CloseReplaced enqueues a CLOSE frame and the
	// writer closes the socket only after the frame has been flushed. The old
	// client must therefore always see CLOSE first and the EOF afterwards.
	if err := connA.conn.SetReadDeadline(time.Now().Add(testIOTimeout)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	closeFrame, err := protocol.Decode(connA.r, testMaxBody)
	if err != nil {
		t.Fatalf("read from kicked connection = %v, want a CLOSE frame first", err)
	}
	if closeFrame.Type != protocol.Close {
		t.Fatalf("old connection received %v, want CLOSE", closeFrame.Type)
	}
	if err := connA.conn.SetReadDeadline(time.Now().Add(testIOTimeout)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if _, err := protocol.Decode(connA.r, testMaxBody); !isClosedError(err) {
		t.Fatalf("read after CLOSE = %v, want EOF/net.ErrClosed", err)
	}

	got := f.sessions.FindByDevice("carol", "tv")
	if got == nil {
		t.Fatal("FindByDevice(carol, tv) = nil, want the new session")
	}
	if got.ID != ackB.SessionID {
		t.Fatalf("FindByDevice(carol, tv).ID = %d, want %d", got.ID, ackB.SessionID)
	}
	if n := f.sessions.ActiveSessions(); n != 1 {
		t.Fatalf("ActiveSessions() = %d, want 1", n)
	}
	if n := len(f.sessions.FindByUser("carol")); n != 1 {
		t.Fatalf("FindByUser(carol) returned %d sessions, want 1", n)
	}
}

// TestSlowConsumerDoesNotBlockSender verifies that a target which never reads
// cannot stall the sender. SEND only writes the message and its outbox row, so a
// burst never touches the target socket; the drop happens later when the
// delivery engine pushes into the tiny queue (1 frame / 64 bytes).
//
// What is asserted is the property that matters: no write blocks (no deadline is
// ever missed), the slow consumer is dropped by the engine's push, and the
// gateway keeps serving new clients. Messages persisted before the drop stay
// recoverable through PULL, which is the Phase 2 fallback for a missed push.
func TestSlowConsumerDoesNotBlockSender(t *testing.T) {
	f := startGatewayWithConfig(t, config.ServerConfig{
		SendQueueSize:  1,
		SendQueueBytes: 64,
	})

	// dave/lazy logs in and then never reads from its socket.
	loginOK(t, f, "dave", "lazy")
	sender, _ := loginOK(t, f, "erin", "phone")

	payload := bytes.Repeat([]byte("x"), 64)
	start := time.Now()
	writeErrs := 0
	for i := 0; i < 50; i++ {
		if err := sender.trySendRoute(t, uint64(100+i), protocol.TargetUser, "dave", payload); err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				t.Fatalf("SEND %d blocked for more than %s: %v", i, testIOTimeout, err)
			}
			writeErrs++
		}
	}
	elapsed := time.Since(start)
	if elapsed >= testIOTimeout {
		t.Fatalf("burst of 50 SENDs took %s, want less than %s", elapsed, testIOTimeout)
	}
	t.Logf("sent 50 SENDs in %s, %d write errors (gateway closed a saturated connection)", elapsed, writeErrs)

	// The engine now pushes 50 deliveries into a queue that holds one frame, so
	// the transport drops the session instead of blocking.
	flushPipeline(t, f)
	waitFor(t, 3*time.Second, func() bool {
		return f.sessions.FindByDevice("dave", "lazy") == nil
	}, "slow consumer session to be dropped")

	// The undelivered rows stay recoverable.
	pending := mustCount(t, f, "SELECT count(*) FROM deliveries WHERE state IN (0, 1)")
	if pending == 0 {
		t.Fatal("deliveries count = 0, want the messages to stay recoverable through PULL")
	}

	// The gateway is still healthy: a fresh client can log in and get an echo.
	fresh, _ := loginOK(t, f, "frank", "pc")
	body := []byte("gateway still alive")
	fresh.send(t, &protocol.Frame{Type: protocol.Data, MsgID: 300, Body: body})
	reply := fresh.read(t)
	if reply.Type != protocol.Data || reply.MsgID != 300 || !bytes.Equal(reply.Body, body) {
		t.Fatalf("fresh client got frame %v msg_id %d body %q, want DATA 300 %q",
			reply.Type, reply.MsgID, reply.Body, body)
	}
}

// TestNoGoroutineLeak verifies that connections and the server release their
// goroutines after a graceful shutdown.
func TestNoGoroutineLeak(t *testing.T) {
	baseline := runtime.NumGoroutine()

	f := startGateway(t)
	conns := make([]*testClient, 0, 10)
	for i := 0; i < 10; i++ {
		c := dialGateway(t, f.srv.Addr())
		ack := c.sendLogin(t, 1, loginBody(t, fmt.Sprintf("leak-user-%d", i), "dev"))
		if ack.Status != protocol.LoginStatusOK {
			t.Fatalf("login %d status = %d, want %d", i, ack.Status, protocol.LoginStatusOK)
		}
		conns = append(conns, c)
	}
	waitFor(t, 2*time.Second, func() bool { return f.sessions.ActiveSessions() == 10 }, "all sessions to register")

	for _, c := range conns {
		c.close()
	}

	// Shutdown is idempotent; the fixture's cleanup Shutdown is a no-op then.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := f.srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		got := runtime.NumGoroutine()
		if got <= baseline+5 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("runtime.NumGoroutine() = %d after shutdown, want <= %d (baseline %d)", got, baseline+5, baseline)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
