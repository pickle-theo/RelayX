package gateway

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"relayx/phase2/internal/config"
	"relayx/phase2/internal/delivery"
	"relayx/phase2/internal/protocol"
	"relayx/phase2/internal/store"
)

// seedMessages persists one message per payload for userID/deviceID without
// going through the gateway, so tests can build large or oversized backlogs.
func seedMessages(t *testing.T, f *gatewayFixture, userID, deviceID string, payloads [][]byte) []uint64 {
	t.Helper()
	ids := make([]uint64, len(payloads))
	for i, payload := range payloads {
		res, err := f.store.AcceptMessage(context.Background(), store.AcceptRequest{
			ClientMsgID: uint64(i + 1),
			FromUser:    "alice",
			FromDevice:  "a0",
			TargetType:  store.TargetUser,
			Target:      userID,
			Payload:     payload,
			ExpireAt:    time.Now().Add(time.Hour),
			Endpoints:   []store.Endpoint{{UserID: userID, DeviceID: deviceID}},
		})
		if err != nil {
			t.Fatalf("AcceptMessage(%d) error = %v", i, err)
		}
		ids[i] = res.MessageID
	}
	return ids
}

// pullPage sends one PULL and reads DELIVER frames until PULL_DONE.
func pullPage(t *testing.T, c *testClient, msgID, cursor uint64, limit uint16) ([]*protocol.Frame, *protocol.PullDoneBody) {
	t.Helper()
	body, err := protocol.AppendPullBody(nil, &protocol.PullBody{Cursor: cursor, Limit: limit})
	if err != nil {
		t.Fatalf("encode PULL body: %v", err)
	}
	c.send(t, &protocol.Frame{Type: protocol.Pull, MsgID: msgID, Body: body})

	deliveries := make([]*protocol.Frame, 0, limit)
	for {
		f := c.read(t)
		switch f.Type {
		case protocol.Deliver:
			deliveries = append(deliveries, f)
		case protocol.PullDone:
			if f.MsgID != msgID {
				t.Fatalf("PULL_DONE msg_id = %d, want %d", f.MsgID, msgID)
			}
			done, err := protocol.DecodePullDone(f.Body)
			if err != nil {
				t.Fatalf("decode PULL_DONE body: %v", err)
			}
			return deliveries, done
		default:
			t.Fatalf("unexpected %v frame while pulling: %q", f.Type, f.Body)
		}
	}
}

func sendReceiptAck(t *testing.T, c *testClient, msgID, messageID uint64) {
	t.Helper()
	body, err := protocol.AppendReceiptAckBody(nil, &protocol.ReceiptAckBody{MessageID: messageID})
	if err != nil {
		t.Fatalf("encode RECEIPT_ACK body: %v", err)
	}
	c.send(t, &protocol.Frame{Type: protocol.ReceiptAck, MsgID: msgID, Body: body})
}

func sendBusinessAck(t *testing.T, c *testClient, msgID, messageID uint64, status uint8) {
	t.Helper()
	body, err := protocol.AppendBusinessAckBody(nil, &protocol.BusinessAckBody{MessageID: messageID, Status: status})
	if err != nil {
		t.Fatalf("encode BUSINESS_ACK body: %v", err)
	}
	c.send(t, &protocol.Frame{Type: protocol.BusinessAck, MsgID: msgID, Body: body})
}

// expectEcho sends a DATA frame and asserts the gateway echoes it, which also
// acts as an ordering fence: every frame the client sent before it has already
// been handled.
func expectEcho(t *testing.T, c *testClient, msgID uint64) {
	t.Helper()
	body := []byte("fence")
	c.send(t, &protocol.Frame{Type: protocol.Data, MsgID: msgID, Body: body})
	reply := c.read(t)
	if reply.Type != protocol.Data || reply.MsgID != msgID || !bytes.Equal(reply.Body, body) {
		t.Fatalf("fence got frame %v msg_id %d body %q, want DATA %d %q",
			reply.Type, reply.MsgID, reply.Body, msgID, body)
	}
}

// duplicateSendResult is the outcome of one goroutine-safe SEND.
type duplicateSendResult struct {
	status    protocol.SendAckStatus
	messageID uint64
	err       error
}

// sendConcurrent writes one SEND frame and reads its SEND_ACK without using
// testing.T helpers, so it is safe to call from a goroutine.
func sendConcurrent(c *testClient, msgID uint64, target string) duplicateSendResult {
	body, err := protocol.AppendSendBody(nil, &protocol.SendBody{
		TargetType: protocol.TargetUser,
		Target:     target,
		Payload:    []byte("concurrent"),
	})
	if err != nil {
		return duplicateSendResult{err: err}
	}
	var buf bytes.Buffer
	if err := protocol.Encode(&buf, &protocol.Frame{Type: protocol.Send, MsgID: msgID, Body: body}, testMaxBody); err != nil {
		return duplicateSendResult{err: err}
	}
	if err := c.conn.SetWriteDeadline(time.Now().Add(testIOTimeout)); err != nil {
		return duplicateSendResult{err: err}
	}
	if _, err := c.conn.Write(buf.Bytes()); err != nil {
		return duplicateSendResult{err: err}
	}
	if err := c.conn.SetReadDeadline(time.Now().Add(testIOTimeout)); err != nil {
		return duplicateSendResult{err: err}
	}
	f, err := protocol.Decode(c.r, testMaxBody)
	if err != nil {
		return duplicateSendResult{err: err}
	}
	if f.Type != protocol.SendAck {
		return duplicateSendResult{err: fmt.Errorf("frame %v, want SEND_ACK", f.Type)}
	}
	ack, err := protocol.DecodeSendAck(f.Body)
	if err != nil {
		return duplicateSendResult{err: err}
	}
	return duplicateSendResult{status: ack.Status, messageID: ack.MessageID}
}

// TestConcurrentDuplicateSendIsIdempotent exercises the database unique
// constraint: ten concurrent SENDs from the same user with the same MsgID must
// create one message, answer exactly one ACCEPTED and nine DUPLICATE with the
// same platform message ID.
func TestConcurrentDuplicateSendIsIdempotent(t *testing.T) {
	f := startGateway(t)

	bob, _ := loginOK(t, f, "bob", "d0")

	const senders = 10
	const msgID uint64 = 777
	clients := make([]*testClient, senders)
	for i := range clients {
		clients[i], _ = loginOK(t, f, "alice", fmt.Sprintf("d%d", i))
	}

	start := make(chan struct{})
	results := make([]duplicateSendResult, senders)
	var wg sync.WaitGroup
	for i := range clients {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i] = sendConcurrent(clients[i], msgID, "bob")
		}(i)
	}
	close(start)
	wg.Wait()

	var accepted, duplicate int
	var messageID uint64
	for i, res := range results {
		if res.err != nil {
			t.Fatalf("sender %d error = %v", i, res.err)
		}
		switch res.status {
		case protocol.SendAckAccepted:
			accepted++
		case protocol.SendAckDuplicate:
			duplicate++
		default:
			t.Fatalf("sender %d status = %d, want ACCEPTED or DUPLICATE", i, res.status)
		}
		if messageID == 0 {
			messageID = res.messageID
		}
		if res.messageID != messageID {
			t.Fatalf("sender %d message_id = %d, want %d", i, res.messageID, messageID)
		}
	}
	if accepted != 1 || duplicate != senders-1 {
		t.Fatalf("ACCEPTED=%d DUPLICATE=%d, want 1 and %d", accepted, duplicate, senders-1)
	}

	if n := mustCount(t, f, "SELECT count(*) FROM messages"); n != 1 {
		t.Fatalf("messages count = %d, want 1", n)
	}
	if n := mustCount(t, f, "SELECT count(*) FROM deliveries"); n != 1 {
		t.Fatalf("deliveries count = %d, want 1", n)
	}

	// Only the accepted SEND is pushed to the online device.
	bob.expectDeliver(t, messageID)
}

// TestRealtimeTwoLevelAck verifies the online path: a DELIVER pushed straight
// from SEND can be acknowledged with RECEIPT_ACK and BUSINESS_ACK without PULL.
func TestRealtimeTwoLevelAck(t *testing.T) {
	f := startGateway(t)

	bob, _ := loginOK(t, f, "bob", "d0")
	alice, _ := loginOK(t, f, "alice", "phone")

	ack := alice.sendRouteExpectAck(t, 5, protocol.TargetUser, "bob", []byte("online ack"))
	if ack.Status != protocol.SendAckAccepted {
		t.Fatalf("SEND_ACK status = %d, want ACCEPTED", ack.Status)
	}
	bob.expectDeliver(t, ack.MessageID)

	// The realtime push must mark the delivery SENT so receipt ACKs apply.
	waitFor(t, 2*time.Second, func() bool {
		return deliveryState(t, f, ack.MessageID) == store.DeliverySent
	}, "realtime delivery to become SENT")

	sendReceiptAck(t, bob, 100, ack.MessageID)
	waitFor(t, 2*time.Second, func() bool {
		return deliveryState(t, f, ack.MessageID) == store.DeliveryReceiptAcked
	}, "delivery to become RECEIPT_ACKED")

	sendBusinessAck(t, bob, 101, ack.MessageID, 0)
	waitFor(t, 2*time.Second, func() bool {
		return deliveryState(t, f, ack.MessageID) == store.DeliveryBusinessAcked
	}, "delivery to become BUSINESS_ACKED")

	if got := messageState(t, f, ack.MessageID); got != store.MessageCompleted {
		t.Fatalf("message state = %d, want COMPLETED (%d)", got, store.MessageCompleted)
	}
}

// TestRepeatedReceiptAckKeepsStateAndTimestamp verifies that four extra receipt
// ACKs leave the delivery state, the message aggregation and updated_at
// untouched.
func TestRepeatedReceiptAckKeepsStateAndTimestamp(t *testing.T) {
	f := startGateway(t)

	bob, _ := loginOK(t, f, "bob", "d0")
	alice, _ := loginOK(t, f, "alice", "phone")

	ack := alice.sendRouteExpectAck(t, 6, protocol.TargetUser, "bob", []byte("ack me once"))
	bob.expectDeliver(t, ack.MessageID)

	sendReceiptAck(t, bob, 200, ack.MessageID)
	waitFor(t, 2*time.Second, func() bool {
		return deliveryState(t, f, ack.MessageID) == store.DeliveryReceiptAcked
	}, "delivery to become RECEIPT_ACKED")

	var updatedAt time.Time
	if err := f.store.DB().QueryRow(
		"SELECT updated_at FROM deliveries WHERE message_id = $1", int64(ack.MessageID)).Scan(&updatedAt); err != nil {
		t.Fatalf("select updated_at: %v", err)
	}

	for i := 0; i < 4; i++ {
		sendReceiptAck(t, bob, uint64(201+i), ack.MessageID)
	}
	// The echo is an ordering fence: all four ACKs have been handled.
	expectEcho(t, bob, 210)

	var updatedAtAfter time.Time
	if err := f.store.DB().QueryRow(
		"SELECT updated_at FROM deliveries WHERE message_id = $1", int64(ack.MessageID)).Scan(&updatedAtAfter); err != nil {
		t.Fatalf("select updated_at: %v", err)
	}
	if !updatedAtAfter.Equal(updatedAt) {
		t.Fatalf("updated_at changed on repeated ACK: %v -> %v", updatedAt, updatedAtAfter)
	}
	if got := deliveryState(t, f, ack.MessageID); got != store.DeliveryReceiptAcked {
		t.Fatalf("delivery state = %d, want RECEIPT_ACKED", got)
	}
	if got := messageState(t, f, ack.MessageID); got != store.MessageAccepted {
		t.Fatalf("message state = %d, want ACCEPTED", got)
	}
}

// TestAckAndPullBeforeLoginRejected verifies that RECEIPT_ACK, BUSINESS_ACK and
// PULL from an unauthenticated connection are rejected and change nothing.
func TestAckAndPullBeforeLoginRejected(t *testing.T) {
	f := startGateway(t)

	ids := seedMessages(t, f, "bob", "d0", [][]byte{[]byte("untouched")})
	messageID := ids[0]

	c := dialGateway(t, f.srv.Addr())
	sendReceiptAck(t, c, 300, messageID)
	c.expectError(t, 300, "not logged in")
	sendBusinessAck(t, c, 301, messageID, 0)
	c.expectError(t, 301, "not logged in")

	pullBody, err := protocol.AppendPullBody(nil, &protocol.PullBody{Cursor: 0, Limit: 100})
	if err != nil {
		t.Fatalf("encode PULL body: %v", err)
	}
	c.send(t, &protocol.Frame{Type: protocol.Pull, MsgID: 302, Body: pullBody})
	c.expectError(t, 302, "not logged in")

	var state int16
	var attempts int
	if err := f.store.DB().QueryRow(
		"SELECT state, attempts FROM deliveries WHERE message_id = $1", int64(messageID)).Scan(&state, &attempts); err != nil {
		t.Fatalf("select delivery: %v", err)
	}
	if store.DeliveryState(state) != store.DeliveryPending || attempts != 0 {
		t.Fatalf("delivery = state %d attempts %d, want PENDING/0", state, attempts)
	}

	expectEcho(t, c, 303)
}

// TestPullPagination verifies the 250-message, three-page PULL contract.
func TestPullPagination(t *testing.T) {
	f := startGateway(t)

	const total = 250
	payloads := make([][]byte, total)
	for i := range payloads {
		payloads[i] = []byte{byte(i)}
	}
	ids := seedMessages(t, f, "bob", "d0", payloads)

	bob, _ := loginOK(t, f, "bob", "d0")

	wants := []struct {
		count   int
		hasMore bool
	}{{100, true}, {100, true}, {50, false}}

	var (
		cursor uint64
		seen   int
		prev   uint64
	)
	for page, want := range wants {
		deliveries, done := pullPage(t, bob, uint64(400+page), cursor, 100)
		if len(deliveries) != want.count || int(done.Count) != want.count {
			t.Fatalf("page %d: DELIVERs=%d PULL_DONE.count=%d, want %d",
				page, len(deliveries), done.Count, want.count)
		}
		if done.HasMore != want.hasMore {
			t.Fatalf("page %d: has_more=%v, want %v", page, done.HasMore, want.hasMore)
		}
		if done.NextCursor <= prev {
			t.Fatalf("page %d: next_cursor=%d, want > %d", page, done.NextCursor, prev)
		}
		for i, frame := range deliveries {
			wantID := ids[seen+i]
			if frame.MsgID != wantID {
				t.Fatalf("page %d frame %d msg_id = %d, want platform message ID %d",
					page, i, frame.MsgID, wantID)
			}
		}
		prev = done.NextCursor
		cursor = done.NextCursor
		seen += len(deliveries)
	}
	if seen != total {
		t.Fatalf("pulled %d messages, want %d", seen, total)
	}
}

// TestPullStopsWhenDeliverEnqueueFails verifies that a page stops at the first
// DELIVER the connection refuses to encode: PULL_DONE reports the successful
// prefix, has_more stays true and the cursor points at the last delivered row.
//
// The connection rejects an oversized body deterministically (body >
// -max-body); queue saturation would instead close the connection, which makes
// PULL_DONE unobservable. The queue-full path is covered by
// TestSlowConsumerDoesNotBlockSender.
func TestPullStopsWhenDeliverEnqueueFails(t *testing.T) {
	f := startGatewayWith(t, gatewayOptions{server: config.ServerConfig{MaxBody: 64}})

	ids := seedMessages(t, f, "bob", "d0", [][]byte{
		bytes.Repeat([]byte("a"), 8),
		bytes.Repeat([]byte("b"), 8),
		bytes.Repeat([]byte("c"), 64), // DELIVER body exceeds -max-body
	})
	bob, _ := loginOK(t, f, "bob", "d0")

	deliveries, done := pullPage(t, bob, 500, 0, 100)
	if len(deliveries) != 2 || done.Count != 2 {
		t.Fatalf("DELIVERs=%d PULL_DONE.count=%d, want 2", len(deliveries), done.Count)
	}
	if deliveries[0].MsgID != ids[0] || deliveries[1].MsgID != ids[1] {
		t.Fatalf("DELIVER MsgIDs = %d,%d, want %d,%d",
			deliveries[0].MsgID, deliveries[1].MsgID, ids[0], ids[1])
	}
	if !done.HasMore {
		t.Fatal("PULL_DONE has_more = false, want true after an enqueue failure")
	}
	var secondDeliveryID uint64
	if err := f.store.DB().QueryRow(
		"SELECT id FROM deliveries WHERE message_id = $1", int64(ids[1])).Scan(&secondDeliveryID); err != nil {
		t.Fatalf("select delivery id: %v", err)
	}
	if done.NextCursor != secondDeliveryID {
		t.Fatalf("PULL_DONE next_cursor = %d, want last enqueued delivery %d", done.NextCursor, secondDeliveryID)
	}

	// Retrying from the cursor still stops on the same undeliverable row.
	retry, retryDone := pullPage(t, bob, 501, done.NextCursor, 100)
	if len(retry) != 0 || retryDone.Count != 0 || retryDone.NextCursor != done.NextCursor {
		t.Fatalf("retry page = %d DELIVERs, done %+v, want none at cursor %d",
			len(retry), retryDone, done.NextCursor)
	}
	if !retryDone.HasMore {
		t.Fatal("retry PULL_DONE has_more = false, want true")
	}
	if got := deliveryState(t, f, ids[2]); got != store.DeliverySent {
		t.Fatalf("third delivery state = %d, want SENT (recoverable, not lost)", got)
	}
}

// TestRetryRepushesAfterMissingAck verifies the ACK-loss path: the target keeps
// the DELIVER unacknowledged and the worker re-pushes the same platform message
// ID after the backoff, incrementing attempts.
func TestRetryRepushesAfterMissingAck(t *testing.T) {
	f := startGatewayWith(t, gatewayOptions{retryBase: 100 * time.Millisecond})

	bob, _ := loginOK(t, f, "bob", "d0")
	alice, _ := loginOK(t, f, "alice", "phone")

	ack := alice.sendRouteExpectAck(t, 8, protocol.TargetUser, "bob", []byte("no ack"))
	bob.expectDeliver(t, ack.MessageID)

	deadline := time.Now().Add(3 * time.Second)
	for {
		due, err := f.store.DueRetries(context.Background(), time.Now(), 10)
		if err == nil && len(due) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("delivery did not become due: %d rows, err=%v", len(due), err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	worker := delivery.NewWorker(f.store, f.sessions, delivery.NewDeliverer(f.sessions), delivery.Config{}, testLogger())
	worker.RunOnce(context.Background())

	bob.expectDeliver(t, ack.MessageID)

	var attempts int
	if err := f.store.DB().QueryRow(
		"SELECT attempts FROM deliveries WHERE message_id = $1", int64(ack.MessageID)).Scan(&attempts); err != nil {
		t.Fatalf("select attempts: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
	if got := worker.Stats(); got.Retried != 1 {
		t.Fatalf("worker Stats().Retried = %d, want 1", got.Retried)
	}
}

// TestMessageTTLExpiresDelivery verifies that an accepted message past its TTL
// becomes EXPIRED together with its deliveries and leaves every scan.
func TestMessageTTLExpiresDelivery(t *testing.T) {
	f := startGatewayWith(t, gatewayOptions{handler: Config{MessageTTL: 100 * time.Millisecond}})

	offline, _ := loginOK(t, f, "bob", "d0")
	offline.close()
	waitFor(t, 2*time.Second, func() bool {
		return f.sessions.FindByDevice("bob", "d0") == nil
	}, "bob/d0 to go offline")

	alice, _ := loginOK(t, f, "alice", "phone")
	ack := alice.sendRouteExpectAck(t, 9, protocol.TargetUser, "bob", []byte("short lived"))
	if ack.Status != protocol.SendAckAccepted {
		t.Fatalf("SEND_ACK status = %d, want ACCEPTED", ack.Status)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		ids, err := f.store.ListExpiredMessages(context.Background(), time.Now(), 10)
		if err == nil && len(ids) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("message did not expire: %d candidates, err=%v", len(ids), err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	worker := delivery.NewWorker(f.store, f.sessions, delivery.NewDeliverer(f.sessions), delivery.Config{}, testLogger())
	worker.RunOnce(context.Background())

	if got := messageState(t, f, ack.MessageID); got != store.MessageExpired {
		t.Fatalf("message state = %d, want EXPIRED (%d)", got, store.MessageExpired)
	}
	if got := deliveryState(t, f, ack.MessageID); got != store.DeliveryExpired {
		t.Fatalf("delivery state = %d, want EXPIRED (%d)", got, store.DeliveryExpired)
	}
	items, hasMore, err := f.store.PullPending(context.Background(), "bob", "d0", 0, 100)
	if err != nil || len(items) != 0 || hasMore {
		t.Fatalf("PullPending() = %d items, hasMore=%v, err=%v, want empty", len(items), hasMore, err)
	}
	due, err := f.store.DueRetries(context.Background(), time.Now().Add(time.Hour), 100)
	if err != nil || len(due) != 0 {
		t.Fatalf("DueRetries() = %d rows, err=%v, want none", len(due), err)
	}

	// A later login must not receive the expired message.
	bob, _ := loginOK(t, f, "bob", "d0")
	deliveries, done := pullPage(t, bob, 502, 0, 100)
	if len(deliveries) != 0 || done.Count != 0 || done.HasMore {
		t.Fatalf("after expiry PULL got %d DELIVERs, done %+v, want empty", len(deliveries), done)
	}
}

// TestSendWhenDatabaseClosed verifies the runtime database failure path: SEND
// answers INTERNAL, the connection stays usable and DATA still echoes.
func TestSendWhenDatabaseClosed(t *testing.T) {
	f := startGateway(t)

	loginOK(t, f, "bob", "d0")
	alice, _ := loginOK(t, f, "alice", "phone")

	if err := f.store.DB().Close(); err != nil {
		t.Fatalf("close database: %v", err)
	}

	ack := alice.sendRouteExpectAck(t, 10, protocol.TargetUser, "bob", []byte("db down"))
	if ack.Status != protocol.SendAckInternal {
		t.Fatalf("SEND_ACK status = %d, want INTERNAL (%d)", ack.Status, protocol.SendAckInternal)
	}
	expectEcho(t, alice, 11)
}

// TestPerConnectionGoroutineBudget verifies the transport still uses exactly
// two goroutines per connection and releases them on close.
func TestPerConnectionGoroutineBudget(t *testing.T) {
	f := startGateway(t)

	// Warm up a connection so accept-loop and session bookkeeping settle.
	warm, _ := loginOK(t, f, "warm", "d0")
	warm.close()
	waitFor(t, 2*time.Second, func() bool { return f.sessions.ActiveSessions() == 0 }, "warm session to close")

	baseline := runtime.NumGoroutine()

	const conns = 8
	clients := make([]*testClient, 0, conns)
	for i := 0; i < conns; i++ {
		c, _ := loginOK(t, f, fmt.Sprintf("user-%d", i), "d0")
		clients = append(clients, c)
	}
	waitFor(t, 2*time.Second, func() bool { return f.sessions.ActiveSessions() == conns }, "all sessions to register")

	got := runtime.NumGoroutine()
	if delta := got - baseline; delta > 2*conns+5 {
		t.Fatalf("goroutine delta = %d for %d connections, want at most %d (2 per connection + 5)",
			delta, conns, 2*conns+5)
	}

	for _, c := range clients {
		c.close()
	}
	waitFor(t, 2*time.Second, func() bool { return f.sessions.ActiveSessions() == 0 }, "sessions to close")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := f.srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		if runtime.NumGoroutine() <= baseline+5 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines after close = %d, want at most %d (baseline %d)",
				runtime.NumGoroutine(), baseline+5, baseline)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
