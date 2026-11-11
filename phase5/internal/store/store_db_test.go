package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"
)

// testDBLockKey serializes the database tests of every package. Package test
// binaries run in parallel and share one PostgreSQL database, so each test
// holds this session-level advisory lock while it truncates and uses the
// tables. 0x5258 is the protocol magic "RX".
const testDBLockKey int64 = 0x5258

// lockTestDatabase acquires the cross-package advisory lock on its own
// connection and releases it from t.Cleanup. The lock connection is separate
// from the store pool so a test may close the pool and still hold the lock.
func lockTestDatabase(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open advisory lock pool: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		t.Fatalf("advisory lock connection: %v", err)
	}
	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", testDBLockKey); err != nil {
		_ = conn.Close()
		_ = db.Close()
		t.Fatalf("pg_advisory_lock: %v", err)
	}
	t.Cleanup(func() {
		unlockCtx, unlockCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer unlockCancel()
		if _, err := conn.ExecContext(unlockCtx, "SELECT pg_advisory_unlock($1)", testDBLockKey); err != nil {
			t.Logf("pg_advisory_unlock: %v", err)
		}
		_ = conn.Close()
		_ = db.Close()
	})
}

func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("RELAYX_TEST_DSN")
	if dsn == "" {
		t.Skip("RELAYX_TEST_DSN not set")
	}
	return dsn
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	dsn := testDSN(t)
	lockTestDatabase(t, dsn)

	ctx := context.Background()
	st, err := Open(ctx, Config{DSN: dsn, ConnectTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, "TRUNCATE deliveries, messages, endpoints RESTART IDENTITY CASCADE"); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
	return st
}

func mustCount(t *testing.T, st *Store, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := st.DB().QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return n
}

func messageState(t *testing.T, st *Store, messageID uint64) MessageState {
	t.Helper()
	var state int16
	if err := st.DB().QueryRow("SELECT state FROM messages WHERE id = $1", int64(messageID)).Scan(&state); err != nil {
		t.Fatalf("select message state: %v", err)
	}
	return MessageState(state)
}

func deliveryState(t *testing.T, st *Store, id uint64) (DeliveryState, int, sql.NullTime) {
	t.Helper()
	var (
		state    int16
		attempts int
		next     sql.NullTime
	)
	if err := st.DB().QueryRow(
		"SELECT state, attempts, next_retry_at FROM deliveries WHERE id = $1", int64(id)).
		Scan(&state, &attempts, &next); err != nil {
		t.Fatalf("select delivery: %v", err)
	}
	return DeliveryState(state), attempts, next
}

func acceptMessage(t *testing.T, st *Store, req AcceptRequest) AcceptResult {
	t.Helper()
	if req.ExpireAt.IsZero() {
		req.ExpireAt = time.Now().Add(time.Hour)
	}
	res, err := st.AcceptMessage(context.Background(), req)
	if err != nil {
		t.Fatalf("AcceptMessage() error = %v", err)
	}
	return res
}

func TestAcceptMessageIdempotent(t *testing.T) {
	st := openTestStore(t)

	req := AcceptRequest{
		ClientMsgID: 7,
		FromUser:    "alice",
		FromDevice:  "d0",
		TargetType:  TargetUser,
		Target:      "bob",
		Payload:     []byte("hello"),
		Endpoints:   []Endpoint{{UserID: "bob", DeviceID: "d0"}},
	}
	first := acceptMessage(t, st, req)

	req.Payload = []byte("different payload")
	second := acceptMessage(t, st, req)

	if first.Duplicate || first.MessageID == 0 {
		t.Fatalf("first AcceptMessage() = %+v, want new message", first)
	}
	if !second.Duplicate {
		t.Fatalf("second AcceptMessage() = %+v, want Duplicate=true", second)
	}
	if second.MessageID != first.MessageID {
		t.Fatalf("duplicate message ID = %d, want %d", second.MessageID, first.MessageID)
	}
	if got := mustCount(t, st, "SELECT count(*) FROM messages"); got != 1 {
		t.Fatalf("messages count = %d, want 1", got)
	}
	if got := mustCount(t, st, "SELECT count(*) FROM deliveries"); got != 1 {
		t.Fatalf("deliveries count = %d, want 1", got)
	}

	var payload []byte
	if err := st.DB().QueryRow("SELECT payload FROM messages WHERE id = $1", int64(first.MessageID)).Scan(&payload); err != nil {
		t.Fatalf("select payload: %v", err)
	}
	if string(payload) != "hello" {
		t.Fatalf("stored payload = %q, want %q (original must not be modified)", payload, "hello")
	}
}

func TestAcceptMessageRequiresRecipients(t *testing.T) {
	st := openTestStore(t)

	_, err := st.AcceptMessage(context.Background(), AcceptRequest{
		ClientMsgID: 1,
		FromUser:    "alice",
		FromDevice:  "d0",
		TargetType:  TargetUser,
		Target:      "nobody",
		Payload:     []byte("x"),
		ExpireAt:    time.Now().Add(time.Hour),
	})
	if !errors.Is(err, ErrNoRecipients) {
		t.Fatalf("AcceptMessage() error = %v, want ErrNoRecipients", err)
	}
	if got := mustCount(t, st, "SELECT count(*) FROM messages"); got != 0 {
		t.Fatalf("messages count = %d, want 0", got)
	}
	if got := mustCount(t, st, "SELECT count(*) FROM deliveries"); got != 0 {
		t.Fatalf("deliveries count = %d, want 0", got)
	}
}

func TestAcceptMessageDedupesEndpointsAndFansOut(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	res := acceptMessage(t, st, AcceptRequest{
		ClientMsgID: 11,
		FromUser:    "alice",
		FromDevice:  "d0",
		TargetType:  TargetUser,
		Target:      "bob",
		Payload:     []byte("fan-out"),
		Endpoints: []Endpoint{
			{UserID: "bob", DeviceID: "d0"},
			{UserID: "bob", DeviceID: "d0"},
			{UserID: "bob", DeviceID: "d1"},
		},
	})
	if res.Duplicate {
		t.Fatalf("AcceptMessage() = %+v, want Duplicate=false", res)
	}
	if got := mustCount(t, st, "SELECT count(*) FROM deliveries WHERE message_id = $1", int64(res.MessageID)); got != 2 {
		t.Fatalf("delivery rows = %d, want 2", got)
	}

	items, hasMore, err := st.PullPending(ctx, "bob", "d0", 0, 10)
	if err != nil {
		t.Fatalf("PullPending() error = %v", err)
	}
	if hasMore || len(items) != 1 {
		t.Fatalf("PullPending() = %d items, hasMore=%v, want 1, false", len(items), hasMore)
	}
	if items[0].MessageID != res.MessageID || items[0].FromUser != "alice" ||
		items[0].FromDevice != "d0" || string(items[0].Payload) != "fan-out" {
		t.Fatalf("pulled delivery = %+v, want message %d from alice/d0", items[0], res.MessageID)
	}
}

func TestEndpointsUpsertAndList(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	for _, device := range []string{"d1", "d0", "d1"} {
		if err := st.UpsertEndpoint(ctx, "bob", device); err != nil {
			t.Fatalf("UpsertEndpoint(%q) error = %v", device, err)
		}
	}
	got, err := st.ListEndpoints(ctx, "bob")
	if err != nil {
		t.Fatalf("ListEndpoints() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("ListEndpoints() = %+v, want 2 endpoints", got)
	}
	if got[0].DeviceID != "d0" || got[1].DeviceID != "d1" {
		t.Fatalf("ListEndpoints() order = %+v, want d0 then d1", got)
	}
	for _, ep := range got {
		if ep.UserID != "bob" {
			t.Fatalf("ListEndpoints() user = %q, want bob", ep.UserID)
		}
	}
}

func TestReceiptAckConditionalAndIdempotent(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	res := acceptMessage(t, st, AcceptRequest{
		ClientMsgID: 21,
		FromUser:    "alice",
		FromDevice:  "d0",
		TargetType:  TargetUser,
		Target:      "bob",
		Payload:     []byte("ack-me"),
		Endpoints:   []Endpoint{{UserID: "bob", DeviceID: "d0"}},
	})
	items, _, err := st.PullPending(ctx, "bob", "d0", 0, 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("PullPending() = %d items, err = %v", len(items), err)
	}
	deliveryID := items[0].ID

	changed, err := st.ApplyReceiptAck(ctx, "bob", "d0", res.MessageID)
	if err != nil {
		t.Fatalf("ApplyReceiptAck() error = %v", err)
	}
	if !changed {
		t.Fatal("first ApplyReceiptAck() changed = false, want true")
	}
	state, attempts, next := deliveryState(t, st, deliveryID)
	if state != DeliveryReceiptAcked {
		t.Fatalf("delivery state = %d, want %d", state, DeliveryReceiptAcked)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
	if next.Valid {
		t.Fatalf("next_retry_at = %v, want NULL", next.Time)
	}

	var updatedAt time.Time
	if err := st.DB().QueryRow("SELECT updated_at FROM deliveries WHERE id = $1", int64(deliveryID)).Scan(&updatedAt); err != nil {
		t.Fatalf("select updated_at: %v", err)
	}

	changed, err = st.ApplyReceiptAck(ctx, "bob", "d0", res.MessageID)
	if err != nil {
		t.Fatalf("repeated ApplyReceiptAck() error = %v", err)
	}
	if changed {
		t.Fatal("repeated ApplyReceiptAck() changed = true, want false")
	}
	state, _, _ = deliveryState(t, st, deliveryID)
	if state != DeliveryReceiptAcked {
		t.Fatalf("delivery state after repeated ack = %d, want %d", state, DeliveryReceiptAcked)
	}
	var updatedAtAfter time.Time
	if err := st.DB().QueryRow("SELECT updated_at FROM deliveries WHERE id = $1", int64(deliveryID)).Scan(&updatedAtAfter); err != nil {
		t.Fatalf("select updated_at: %v", err)
	}
	if !updatedAtAfter.Equal(updatedAt) {
		t.Fatalf("updated_at changed on repeated ack: %v -> %v", updatedAt, updatedAtAfter)
	}

	// A receipt ACK for an unknown endpoint must not change anything.
	changed, err = st.ApplyReceiptAck(ctx, "bob", "d1", res.MessageID)
	if err != nil {
		t.Fatalf("ApplyReceiptAck(other device) error = %v", err)
	}
	if changed {
		t.Fatal("ApplyReceiptAck(other device) changed = true, want false")
	}
}

func TestBusinessAckJumpsFromSentAndAggregates(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	res := acceptMessage(t, st, AcceptRequest{
		ClientMsgID: 31,
		FromUser:    "alice",
		FromDevice:  "d0",
		TargetType:  TargetUser,
		Target:      "bob",
		Payload:     []byte("two devices"),
		Endpoints: []Endpoint{
			{UserID: "bob", DeviceID: "d0"},
			{UserID: "bob", DeviceID: "d1"},
		},
	})

	if _, _, err := st.PullPending(ctx, "bob", "d0", 0, 10); err != nil {
		t.Fatalf("PullPending(d0) error = %v", err)
	}
	if _, _, err := st.PullPending(ctx, "bob", "d1", 0, 10); err != nil {
		t.Fatalf("PullPending(d1) error = %v", err)
	}
	if state := messageState(t, st, res.MessageID); state != MessageAccepted {
		t.Fatalf("message state after pull = %d, want %d", state, MessageAccepted)
	}

	// Business ACK straight from SENT (no receipt ACK) on d0 -> PARTIAL.
	changed, err := st.ApplyBusinessAck(ctx, "bob", "d0", res.MessageID, 0)
	if err != nil {
		t.Fatalf("ApplyBusinessAck() error = %v", err)
	}
	if !changed {
		t.Fatal("ApplyBusinessAck() changed = false, want true")
	}
	if state := messageState(t, st, res.MessageID); state != MessagePartial {
		t.Fatalf("message state = %d, want %d (PARTIAL)", state, MessagePartial)
	}
	if changed, err := st.ApplyBusinessAck(ctx, "bob", "d0", res.MessageID, 0); err != nil || changed {
		t.Fatalf("repeated ApplyBusinessAck() = %v, %v, want false, nil", changed, err)
	}

	// Non-zero status on d1 -> BUSINESS_FAILED, all terminals -> COMPLETED.
	if changed, err := st.ApplyBusinessAck(ctx, "bob", "d1", res.MessageID, 9); err != nil || !changed {
		t.Fatalf("ApplyBusinessAck(d1) = %v, %v, want true, nil", changed, err)
	}
	if state := messageState(t, st, res.MessageID); state != MessageCompleted {
		t.Fatalf("message state = %d, want %d (COMPLETED)", state, MessageCompleted)
	}

	var states []int16
	rows, err := st.DB().Query("SELECT state FROM deliveries WHERE message_id = $1 ORDER BY device_id", int64(res.MessageID))
	if err != nil {
		t.Fatalf("select delivery states: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var state int16
		if err := rows.Scan(&state); err != nil {
			t.Fatalf("scan state: %v", err)
		}
		states = append(states, state)
	}
	if len(states) != 2 || states[0] != int16(DeliveryBusinessAcked) || states[1] != int16(DeliveryBusinessFailed) {
		t.Fatalf("delivery states = %v, want [3 4]", states)
	}
}

func TestPullPendingCursorPagination(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	const total = 250
	for i := uint64(1); i <= total; i++ {
		acceptMessage(t, st, AcceptRequest{
			ClientMsgID: i,
			FromUser:    "alice",
			FromDevice:  "d0",
			TargetType:  TargetUser,
			Target:      "bob",
			Payload:     []byte{byte(i)},
			Endpoints:   []Endpoint{{UserID: "bob", DeviceID: "d0"}},
		})
	}

	wants := []struct {
		count   int
		hasMore bool
	}{{100, true}, {100, true}, {50, false}}

	var (
		cursor uint64
		seen   int
		prev   uint64
	)
	for round, want := range wants {
		items, hasMore, err := st.PullPending(ctx, "bob", "d0", cursor, 100)
		if err != nil {
			t.Fatalf("round %d PullPending() error = %v", round, err)
		}
		if len(items) != want.count || hasMore != want.hasMore {
			t.Fatalf("round %d PullPending() = %d items, hasMore=%v, want %d, %v",
				round, len(items), hasMore, want.count, want.hasMore)
		}
		for _, item := range items {
			if item.ID <= prev {
				t.Fatalf("round %d delivery ID %d is not increasing (prev %d)", round, item.ID, prev)
			}
			prev = item.ID
			if item.State != DeliverySent || item.Attempts != 1 {
				t.Fatalf("round %d delivery %+v, want state SENT with 1 attempt", round, item)
			}
		}
		cursor = prev
		seen += len(items)
	}
	if seen != total {
		t.Fatalf("pulled %d deliveries, want %d", seen, total)
	}

	items, hasMore, err := st.PullPending(ctx, "bob", "d0", cursor, 100)
	if err != nil || len(items) != 0 || hasMore {
		t.Fatalf("final PullPending() = %d items, hasMore=%v, err=%v, want 0, false", len(items), hasMore, err)
	}
}

func TestPullPendingMarksSentAndSchedulesRetry(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	acceptMessage(t, st, AcceptRequest{
		ClientMsgID: 41,
		FromUser:    "alice",
		FromDevice:  "d0",
		TargetType:  TargetUser,
		Target:      "bob",
		Payload:     []byte("retry"),
		Endpoints:   []Endpoint{{UserID: "bob", DeviceID: "d0"}},
	})

	items, _, err := st.PullPending(ctx, "bob", "d0", 0, 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("PullPending() = %d items, err = %v", len(items), err)
	}
	state, attempts, next := deliveryState(t, st, items[0].ID)
	if state != DeliverySent || attempts != 1 {
		t.Fatalf("delivery = state %d, attempts %d, want SENT with 1 attempt", state, attempts)
	}
	if !next.Valid {
		t.Fatal("next_retry_at is NULL after pull, want scheduled")
	}
	if wait := time.Until(next.Time); wait < time.Second || wait > 5*time.Second {
		t.Fatalf("next_retry_at in %v, want about 2s", wait)
	}

	// Not due yet, then due after the backoff window.
	due, err := st.DueRetries(ctx, time.Now(), 10)
	if err != nil {
		t.Fatalf("DueRetries(now) error = %v", err)
	}
	if len(due) != 0 {
		t.Fatalf("DueRetries(now) = %d items, want 0", len(due))
	}
	due, err = st.DueRetries(ctx, time.Now().Add(time.Minute), 10)
	if err != nil {
		t.Fatalf("DueRetries(future) error = %v", err)
	}
	if len(due) != 1 {
		t.Fatalf("DueRetries(future) = %d items, want 1", len(due))
	}
	if due[0].ID != items[0].ID || due[0].MessageID != items[0].MessageID ||
		due[0].FromUser != "alice" || string(due[0].Payload) != "retry" {
		t.Fatalf("DueRetries() row = %+v, want joined delivery %+v", due[0], items[0])
	}

	// MarkDeliveryRetried bumps attempts and pushes next_retry_at out.
	if err := st.MarkDeliveryRetried(ctx, due[0].ID); err != nil {
		t.Fatalf("MarkDeliveryRetried() error = %v", err)
	}
	state, attempts, next = deliveryState(t, st, due[0].ID)
	if state != DeliverySent || attempts != 2 {
		t.Fatalf("delivery after retry = state %d, attempts %d, want SENT with 2 attempts", state, attempts)
	}
	if !next.Valid {
		t.Fatal("next_retry_at is NULL after MarkDeliveryRetried, want scheduled")
	}

	// Clearing the schedule removes it from DueRetries but keeps SENT.
	if err := st.ClearRetrySchedule(ctx, due[0].ID); err != nil {
		t.Fatalf("ClearRetrySchedule() error = %v", err)
	}
	_, _, next = deliveryState(t, st, due[0].ID)
	if next.Valid {
		t.Fatalf("next_retry_at = %v, want NULL", next.Time)
	}
	due, err = st.DueRetries(ctx, time.Now().Add(time.Hour), 10)
	if err != nil || len(due) != 0 {
		t.Fatalf("DueRetries() after clear = %d items, err=%v, want 0", len(due), err)
	}
}

func TestMarkDeliveryFailedAggregates(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	res := acceptMessage(t, st, AcceptRequest{
		ClientMsgID: 51,
		FromUser:    "alice",
		FromDevice:  "d0",
		TargetType:  TargetUser,
		Target:      "bob",
		Payload:     []byte("fail"),
		Endpoints: []Endpoint{
			{UserID: "bob", DeviceID: "d0"},
			{UserID: "bob", DeviceID: "d1"},
		},
	})

	items, _, err := st.PullPending(ctx, "bob", "d0", 0, 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("PullPending() = %d items, err = %v", len(items), err)
	}
	if err := st.MarkDeliveryFailed(ctx, items[0].ID); err != nil {
		t.Fatalf("MarkDeliveryFailed() error = %v", err)
	}
	state, _, next := deliveryState(t, st, items[0].ID)
	if state != DeliveryFailed {
		t.Fatalf("delivery state = %d, want %d", state, DeliveryFailed)
	}
	if next.Valid {
		t.Fatalf("next_retry_at = %v, want NULL", next.Time)
	}
	if state := messageState(t, st, res.MessageID); state != MessagePartial {
		t.Fatalf("message state = %d, want %d (PARTIAL)", state, MessagePartial)
	}

	// Already terminal: no error, no change.
	if err := st.MarkDeliveryFailed(ctx, items[0].ID); err != nil {
		t.Fatalf("repeated MarkDeliveryFailed() error = %v", err)
	}

	remaining, _, err := st.PullPending(ctx, "bob", "d1", 0, 10)
	if err != nil || len(remaining) != 1 {
		t.Fatalf("PullPending(d1) = %d items, err = %v", len(remaining), err)
	}
	if err := st.MarkDeliveryFailed(ctx, remaining[0].ID); err != nil {
		t.Fatalf("MarkDeliveryFailed(d1) error = %v", err)
	}
	if state := messageState(t, st, res.MessageID); state != MessageCompleted {
		t.Fatalf("message state = %d, want %d (COMPLETED)", state, MessageCompleted)
	}
}

func TestMarkDeliverySentOnlyMovesPending(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	res := acceptMessage(t, st, AcceptRequest{
		ClientMsgID: 71,
		FromUser:    "alice",
		FromDevice:  "d0",
		TargetType:  TargetUser,
		Target:      "bob",
		Payload:     []byte("realtime"),
		Endpoints:   []Endpoint{{UserID: "bob", DeviceID: "d0"}},
	})
	var deliveryID uint64
	if err := st.DB().QueryRow(
		"SELECT id FROM deliveries WHERE message_id = $1", int64(res.MessageID)).Scan(&deliveryID); err != nil {
		t.Fatalf("select delivery id: %v", err)
	}

	if err := st.MarkDeliverySent(ctx, res.MessageID, "bob", "d0"); err != nil {
		t.Fatalf("MarkDeliverySent() error = %v", err)
	}
	state, attempts, next := deliveryState(t, st, deliveryID)
	if state != DeliverySent || attempts != 1 {
		t.Fatalf("delivery = state %d attempts %d, want SENT/1", state, attempts)
	}
	if !next.Valid {
		t.Fatal("next_retry_at is NULL after a realtime push, want scheduled")
	}
	if got := messageState(t, st, res.MessageID); got != MessageAccepted {
		t.Fatalf("message state = %d, want ACCEPTED", got)
	}

	// A second push must not overwrite a row that already left PENDING.
	if err := st.MarkDeliverySent(ctx, res.MessageID, "bob", "d0"); err != nil {
		t.Fatalf("repeated MarkDeliverySent() error = %v", err)
	}
	if _, attempts, _ := deliveryState(t, st, deliveryID); attempts != 1 {
		t.Fatalf("attempts after repeated push = %d, want 1", attempts)
	}

	// A receipt ACK from the SENT state must apply.
	if changed, err := st.ApplyReceiptAck(ctx, "bob", "d0", res.MessageID); err != nil || !changed {
		t.Fatalf("ApplyReceiptAck() after realtime push = %v, %v, want true, nil", changed, err)
	}
}

// TestReopenRecoversAcceptedMessage verifies that ACCEPTED is durable: closing
// the pool after acceptance and reopening it still exposes the message to PULL.
func TestReopenRecoversAcceptedMessage(t *testing.T) {
	dsn := testDSN(t)
	st := openTestStore(t)

	res := acceptMessage(t, st, AcceptRequest{
		ClientMsgID: 81,
		FromUser:    "alice",
		FromDevice:  "d0",
		TargetType:  TargetUser,
		Target:      "bob",
		Payload:     []byte("survives restart"),
		Endpoints:   []Endpoint{{UserID: "bob", DeviceID: "d0"}},
	})
	if res.MessageID == 0 {
		t.Fatal("AcceptMessage() message ID = 0")
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := Open(context.Background(), Config{DSN: dsn, ConnectTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("Open() after close error = %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	items, hasMore, err := reopened.PullPending(context.Background(), "bob", "d0", 0, 10)
	if err != nil {
		t.Fatalf("PullPending() after reopen error = %v", err)
	}
	if len(items) != 1 || hasMore {
		t.Fatalf("PullPending() after reopen = %d items, hasMore=%v, want 1, false", len(items), hasMore)
	}
	if items[0].MessageID != res.MessageID || items[0].FromUser != "alice" ||
		items[0].FromDevice != "d0" || string(items[0].Payload) != "survives restart" {
		t.Fatalf("recovered delivery = %+v, want message %d from alice/d0", items[0], res.MessageID)
	}
	if items[0].State != DeliverySent || items[0].Attempts != 1 {
		t.Fatalf("recovered delivery = state %d attempts %d, want SENT/1", items[0].State, items[0].Attempts)
	}
}

func TestExpireMessageHidesFromPullAndRetry(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	res := acceptMessage(t, st, AcceptRequest{
		ClientMsgID: 61,
		FromUser:    "alice",
		FromDevice:  "d0",
		TargetType:  TargetUser,
		Target:      "bob",
		Payload:     []byte("ttl"),
		ExpireAt:    time.Now().Add(-time.Minute),
		Endpoints:   []Endpoint{{UserID: "bob", DeviceID: "d0"}},
	})

	expired, err := st.ListExpiredMessages(ctx, time.Now(), 100)
	if err != nil {
		t.Fatalf("ListExpiredMessages() error = %v", err)
	}
	if len(expired) != 1 || expired[0] != res.MessageID {
		t.Fatalf("ListExpiredMessages() = %v, want [%d]", expired, res.MessageID)
	}

	if _, _, err := st.PullPending(ctx, "bob", "d0", 0, 10); err != nil {
		t.Fatalf("PullPending() error = %v", err)
	}
	if err := st.ExpireMessage(ctx, res.MessageID); err != nil {
		t.Fatalf("ExpireMessage() error = %v", err)
	}
	if state := messageState(t, st, res.MessageID); state != MessageExpired {
		t.Fatalf("message state = %d, want %d (EXPIRED)", state, MessageExpired)
	}
	var deliveryDeliveryState int16
	if err := st.DB().QueryRow(
		"SELECT state FROM deliveries WHERE message_id = $1", int64(res.MessageID)).Scan(&deliveryDeliveryState); err != nil {
		t.Fatalf("select delivery state: %v", err)
	}
	if DeliveryState(deliveryDeliveryState) != DeliveryExpired {
		t.Fatalf("delivery state = %d, want %d", deliveryDeliveryState, DeliveryExpired)
	}

	// Expired rows must not be pulled or retried anymore.
	items, hasMore, err := st.PullPending(ctx, "bob", "d0", 0, 10)
	if err != nil || len(items) != 0 || hasMore {
		t.Fatalf("PullPending() after expire = %d items, hasMore=%v, err=%v, want 0, false", len(items), hasMore, err)
	}
	due, err := st.DueRetries(ctx, time.Now().Add(time.Hour), 10)
	if err != nil || len(due) != 0 {
		t.Fatalf("DueRetries() after expire = %d items, err=%v, want 0", len(due), err)
	}

	// An already expired message is no longer a cleanup candidate.
	expired, err = st.ListExpiredMessages(ctx, time.Now(), 100)
	if err != nil {
		t.Fatalf("ListExpiredMessages() error = %v", err)
	}
	if len(expired) != 0 {
		t.Fatalf("ListExpiredMessages() after expire = %v, want empty", expired)
	}

	// Aggregation must not resurrect an EXPIRED message.
	changed, err := st.ApplyBusinessAck(ctx, "bob", "d0", res.MessageID, 0)
	if err != nil {
		t.Fatalf("ApplyBusinessAck(expired) error = %v", err)
	}
	if changed {
		t.Fatal("ApplyBusinessAck(expired) changed = true, want false")
	}
	if state := messageState(t, st, res.MessageID); state != MessageExpired {
		t.Fatalf("message state = %d, want %d (EXPIRED)", state, MessageExpired)
	}

	if got, err := st.CountPendingDeliveries(ctx); err != nil || got != 0 {
		t.Fatalf("CountPendingDeliveries() = %d, %v, want 0, nil", got, err)
	}
}
