package delivery

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"relayx/phase5/internal/protocol"
	"relayx/phase5/internal/registry"
	"relayx/phase5/internal/session"
	"relayx/phase5/internal/store"
)

// fakeConn records the frames enqueued by the delivery layer.
type fakeConn struct {
	id      uint64
	sendErr error

	mu     sync.Mutex
	frames []*protocol.Frame
}

func (c *fakeConn) ID() uint64         { return c.id }
func (c *fakeConn) RemoteAddr() string { return "fake" }

func (c *fakeConn) Send(f *protocol.Frame) error {
	if c.sendErr != nil {
		return c.sendErr
	}
	c.mu.Lock()
	c.frames = append(c.frames, f)
	c.mu.Unlock()
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

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func registerSession(mgr *session.Manager, userID, deviceID string, conn *fakeConn) *session.Session {
	sess := &session.Session{
		ID:       mgr.NextSessionID(),
		UserID:   userID,
		DeviceID: deviceID,
		Conn:     conn,
	}
	mgr.Register(sess)
	return sess
}

// testDBLockKey serializes the database tests of every package so parallel
// package test binaries never TRUNCATE each other's rows. 0x5258 is "RX".
const testDBLockKey int64 = 0x5258

// lockTestDatabase holds the cross-package advisory lock for one test on a
// dedicated connection, independent of the store pool under test.
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

func openTestStore(t *testing.T) *store.Store {
	t.Helper()
	dsn := os.Getenv("RELAYX_TEST_DSN")
	if dsn == "" {
		t.Skip("RELAYX_TEST_DSN not set")
	}
	lockTestDatabase(t, dsn)

	ctx := context.Background()
	st, err := store.Open(ctx, store.Config{DSN: dsn, ConnectTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
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

// seedSentDelivery creates one message and pulls it once, leaving a SENT
// delivery with attempts=1 and a retry scheduled in the future.
func seedSentDelivery(t *testing.T, st *store.Store, userID, deviceID string, expireAt time.Time) (messageID, deliveryID uint64) {
	t.Helper()
	ctx := context.Background()

	res, err := st.AcceptMessage(ctx, store.AcceptRequest{
		ClientMsgID: 1,
		FromUser:    "alice",
		FromDevice:  "a0",
		TargetType:  store.TargetUser,
		Target:      userID,
		Payload:     []byte("payload"),
		ExpireAt:    expireAt,
		Endpoints:   []store.Endpoint{{UserID: userID, DeviceID: deviceID}},
	})
	if err != nil {
		t.Fatalf("AcceptMessage() error = %v", err)
	}
	items, _, err := st.PullPending(ctx, userID, deviceID, 0, 10)
	if err != nil {
		t.Fatalf("PullPending() error = %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("PullPending() = %d items, want 1", len(items))
	}
	return res.MessageID, items[0].ID
}

func makeDue(t *testing.T, st *store.Store, deliveryID uint64) {
	t.Helper()
	if _, err := st.DB().Exec(
		"UPDATE deliveries SET next_retry_at = now() - interval '1 second' WHERE id = $1", int64(deliveryID)); err != nil {
		t.Fatalf("make delivery due: %v", err)
	}
}

func deliveryRow(t *testing.T, st *store.Store, deliveryID uint64) (store.DeliveryState, int, sql.NullTime) {
	t.Helper()
	var (
		state    int16
		attempts int
		next     sql.NullTime
	)
	if err := st.DB().QueryRow(
		"SELECT state, attempts, next_retry_at FROM deliveries WHERE id = $1", int64(deliveryID)).
		Scan(&state, &attempts, &next); err != nil {
		t.Fatalf("select delivery: %v", err)
	}
	return store.DeliveryState(state), attempts, next
}

func messageState(t *testing.T, st *store.Store, messageID uint64) store.MessageState {
	t.Helper()
	var state int16
	if err := st.DB().QueryRow("SELECT state FROM messages WHERE id = $1", int64(messageID)).Scan(&state); err != nil {
		t.Fatalf("select message state: %v", err)
	}
	return store.MessageState(state)
}

func TestRunOnceRetriesOnlineDelivery(t *testing.T) {
	st := openTestStore(t)
	messageID, deliveryID := seedSentDelivery(t, st, "bob", "d0", time.Now().Add(time.Hour))
	makeDue(t, st, deliveryID)

	sessions := session.NewManager()
	conn := &fakeConn{id: 1}
	registerSession(sessions, "bob", "d0", conn)

	w := NewWorker(st, sessions, NewDeliverer(sessions), Config{}, discardLogger())
	w.RunOnce(context.Background())

	frames := conn.sent()
	if len(frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(frames))
	}
	if frames[0].Type != protocol.Deliver {
		t.Fatalf("frame type = %s, want DELIVER", frames[0].Type)
	}
	if frames[0].MsgID != messageID {
		t.Fatalf("frame MsgID = %d, want platform message ID %d", frames[0].MsgID, messageID)
	}
	body, err := protocol.DecodeDeliver(frames[0].Body)
	if err != nil {
		t.Fatalf("DecodeDeliver() error = %v", err)
	}
	if body.FromSessionID != 0 {
		t.Fatalf("FromSessionID = %d, want 0", body.FromSessionID)
	}
	if body.FromUser != "alice" || body.FromDevice != "a0" {
		t.Fatalf("from = %s/%s, want alice/a0", body.FromUser, body.FromDevice)
	}
	if string(body.Payload) != "payload" {
		t.Fatalf("payload = %q, want %q", body.Payload, "payload")
	}

	state, attempts, next := deliveryRow(t, st, deliveryID)
	if state != store.DeliverySent {
		t.Fatalf("delivery state = %d, want SENT", state)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
	if !next.Valid {
		t.Fatal("next_retry_at is NULL, want scheduled")
	}
	// attempts=1 before the increment, so backoff = base * 2^1 = 4s.
	if wait := time.Until(next.Time); wait < 3*time.Second || wait > 6*time.Second {
		t.Fatalf("next_retry_at in %v, want about 4s", wait)
	}
	if got := w.Stats(); got.Retried != 1 || got.Expired != 0 {
		t.Fatalf("Stats() = %+v, want Retried=1", got)
	}
}

func TestRunOnceClearsScheduleWhenOffline(t *testing.T) {
	st := openTestStore(t)
	_, deliveryID := seedSentDelivery(t, st, "bob", "d0", time.Now().Add(time.Hour))
	makeDue(t, st, deliveryID)

	sessions := session.NewManager()
	w := NewWorker(st, sessions, NewDeliverer(sessions), Config{}, discardLogger())
	w.RunOnce(context.Background())

	state, attempts, next := deliveryRow(t, st, deliveryID)
	if state != store.DeliverySent {
		t.Fatalf("delivery state = %d, want SENT", state)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (unchanged)", attempts)
	}
	if next.Valid {
		t.Fatalf("next_retry_at = %v, want NULL", next.Time)
	}
	if got := w.Stats(); got.Retried != 0 {
		t.Fatalf("Stats().Retried = %d, want 0", got.Retried)
	}
}

func TestRunOnceMarksFailedAtMaxAttempts(t *testing.T) {
	st := openTestStore(t)
	_, deliveryID := seedSentDelivery(t, st, "bob", "d0", time.Now().Add(time.Hour))
	if _, err := st.DB().Exec(
		"UPDATE deliveries SET attempts = 5, next_retry_at = now() - interval '1 second' WHERE id = $1",
		int64(deliveryID)); err != nil {
		t.Fatalf("set attempts: %v", err)
	}

	sessions := session.NewManager()
	conn := &fakeConn{id: 1}
	registerSession(sessions, "bob", "d0", conn)

	w := NewWorker(st, sessions, NewDeliverer(sessions), Config{MaxAttempts: 5}, discardLogger())
	w.RunOnce(context.Background())

	state, attempts, next := deliveryRow(t, st, deliveryID)
	if state != store.DeliveryFailed {
		t.Fatalf("delivery state = %d, want FAILED", state)
	}
	if attempts != 5 {
		t.Fatalf("attempts = %d, want 5", attempts)
	}
	if next.Valid {
		t.Fatalf("next_retry_at = %v, want NULL", next.Time)
	}
	if frames := conn.sent(); len(frames) != 0 {
		t.Fatalf("frames = %d, want 0 (no push at the retry limit)", len(frames))
	}

	// A FAILED delivery is never scanned again.
	w.RunOnce(context.Background())
	if got, _, _ := deliveryRow(t, st, deliveryID); got != store.DeliveryFailed {
		t.Fatalf("delivery state after second round = %d, want FAILED", got)
	}
	if got := w.Stats(); got.Retried != 0 {
		t.Fatalf("Stats().Retried = %d, want 0", got.Retried)
	}
}

func TestRunOnceExpiresMessages(t *testing.T) {
	st := openTestStore(t)
	messageID, deliveryID := seedSentDelivery(t, st, "bob", "d0", time.Now().Add(-time.Minute))
	// Make it due as well: expiry must win over a pending retry.
	makeDue(t, st, deliveryID)

	sessions := session.NewManager()
	conn := &fakeConn{id: 1}
	registerSession(sessions, "bob", "d0", conn)

	w := NewWorker(st, sessions, NewDeliverer(sessions), Config{}, discardLogger())
	w.RunOnce(context.Background())

	if state := messageState(t, st, messageID); state != store.MessageExpired {
		t.Fatalf("message state = %d, want EXPIRED", state)
	}
	state, _, next := deliveryRow(t, st, deliveryID)
	if state != store.DeliveryExpired {
		t.Fatalf("delivery state = %d, want EXPIRED", state)
	}
	if next.Valid {
		t.Fatalf("next_retry_at = %v, want NULL", next.Time)
	}
	if frames := conn.sent(); len(frames) != 0 {
		t.Fatalf("frames = %d, want 0 for an expired message", len(frames))
	}
	if got := w.Stats(); got.Expired != 1 || got.Retried != 0 {
		t.Fatalf("Stats() = %+v, want Expired=1", got)
	}

	items, hasMore, err := st.PullPending(context.Background(), "bob", "d0", 0, 10)
	if err != nil || len(items) != 0 || hasMore {
		t.Fatalf("PullPending() after expire = %d items, hasMore=%v, err=%v, want 0, false", len(items), hasMore, err)
	}
}

func TestWorkerStopIsIdempotentAndLeavesNoGoroutine(t *testing.T) {
	st := openTestStore(t)
	if err := st.Ping(context.Background()); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}

	sessions := session.NewManager()
	w := NewWorker(st, sessions, NewDeliverer(sessions), Config{Interval: 20 * time.Millisecond}, discardLogger())

	baseline := runtime.NumGoroutine()
	w.Stop() // before Start must be a no-op
	w.Start()
	// The worker owns exactly one goroutine: repeated Start must not add more.
	waitForGoroutines(t, baseline+1, "worker goroutine to start")
	w.Start()
	if got := runtime.NumGoroutine(); got != baseline+1 {
		t.Fatalf("goroutines after repeated Start = %d, want %d", got, baseline+1)
	}
	w.Stop()
	w.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for {
		got := runtime.NumGoroutine()
		if got <= baseline+2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines = %d, want at most %d", got, baseline+2)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitForGoroutines waits until the process runs exactly want goroutines.
func waitForGoroutines(t *testing.T, want int, description string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if got := runtime.NumGoroutine(); got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s: %d goroutines, want %d",
				description, runtime.NumGoroutine(), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestRunOnceSurvivesDatabaseFailure verifies that a broken database only makes
// one worker round log and return; the process must not panic or exit.
func TestRunOnceSurvivesDatabaseFailure(t *testing.T) {
	st := openTestStore(t)
	if err := st.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	sessions := session.NewManager()
	w := NewWorker(st, sessions, NewDeliverer(sessions), Config{}, discardLogger())
	w.RunOnce(context.Background())
	w.RunOnce(context.Background())

	if got := w.Stats(); got.Retried != 0 || got.Expired != 0 {
		t.Fatalf("Stats() = %+v, want zero counters", got)
	}
}

func TestDeliverToUserFansOut(t *testing.T) {
	sessions := session.NewManager()
	d0 := &fakeConn{id: 1}
	d1 := &fakeConn{id: 2}
	other := &fakeConn{id: 3}
	registerSession(sessions, "bob", "d0", d0)
	registerSession(sessions, "bob", "d1", d1)
	registerSession(sessions, "alice", "d0", other)

	d := NewDeliverer(sessions)
	n, err := d.DeliverToUser("bob", 42, "alice", "a0", []byte("hi"))
	if err != nil {
		t.Fatalf("DeliverToUser() error = %v", err)
	}
	if n != 2 {
		t.Fatalf("DeliverToUser() = %d, want 2", n)
	}
	for _, conn := range []*fakeConn{d0, d1} {
		frames := conn.sent()
		if len(frames) != 1 {
			t.Fatalf("device %d frames = %d, want 1", conn.id, len(frames))
		}
		if frames[0].Type != protocol.Deliver || frames[0].MsgID != 42 {
			t.Fatalf("device %d frame = %s/%d, want DELIVER/42", conn.id, frames[0].Type, frames[0].MsgID)
		}
	}
	if frames := other.sent(); len(frames) != 0 {
		t.Fatalf("unrelated session frames = %d, want 0", len(frames))
	}

	// A device whose Send fails is skipped without aborting the fan-out.
	failing := &fakeConn{id: 4, sendErr: errors.New("queue full")}
	registerSession(sessions, "bob", "d2", failing)
	n, err = d.DeliverToUser("bob", 43, "alice", "a0", []byte("hi"))
	if err != nil {
		t.Fatalf("DeliverToUser() with failing device error = %v", err)
	}
	if n != 2 {
		t.Fatalf("DeliverToUser() with failing device = %d, want 2", n)
	}

	n, err = d.DeliverToUser("nobody", 44, "alice", "a0", nil)
	if !errors.Is(err, ErrOffline) {
		t.Fatalf("DeliverToUser(offline) error = %v, want ErrOffline", err)
	}
	if n != 0 {
		t.Fatalf("DeliverToUser(offline) = %d, want 0", n)
	}
}

// Ensure the fake conn satisfies the session.Conn contract at compile time.
var _ session.Conn = (*fakeConn)(nil)

// fakeRetryStore records the row updates so the retry decisions can be checked
// without a database.
type fakeRetryStore struct {
	mu      sync.Mutex
	due     []store.Delivery
	calls   []string
	cleared []uint64
	retried []uint64
	failed  []uint64
}

func (s *fakeRetryStore) record(name string) {
	s.mu.Lock()
	s.calls = append(s.calls, name)
	s.mu.Unlock()
}

func (s *fakeRetryStore) DueRetries(ctx context.Context, now time.Time, limit int) ([]store.Delivery, error) {
	s.record("DueRetries")
	return s.due, nil
}

func (s *fakeRetryStore) ClearRetrySchedule(ctx context.Context, id uint64) error {
	s.record("ClearRetrySchedule")
	s.mu.Lock()
	s.cleared = append(s.cleared, id)
	s.mu.Unlock()
	return nil
}

func (s *fakeRetryStore) MarkDeliveryRetried(ctx context.Context, id uint64) error {
	s.record("MarkDeliveryRetried")
	s.mu.Lock()
	s.retried = append(s.retried, id)
	s.mu.Unlock()
	return nil
}

func (s *fakeRetryStore) MarkDeliveryFailed(ctx context.Context, id uint64) error {
	s.record("MarkDeliveryFailed")
	s.mu.Lock()
	s.failed = append(s.failed, id)
	s.mu.Unlock()
	return nil
}

func (s *fakeRetryStore) ListExpiredMessages(ctx context.Context, now time.Time, limit int) ([]uint64, error) {
	s.record("ListExpiredMessages")
	return nil, nil
}

func (s *fakeRetryStore) ExpireMessage(ctx context.Context, id uint64) error {
	s.record("ExpireMessage")
	return nil
}

func (s *fakeRetryStore) callsOf(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.calls {
		if c == name {
			n++
		}
	}
	return n
}

type fakeRouteLookup struct {
	route     registry.Route
	found     bool
	lookupErr error
	alive     bool
	aliveErr  error
}

func (r fakeRouteLookup) LookupRoute(ctx context.Context, userID, deviceID string) (registry.Route, bool, error) {
	return r.route, r.found, r.lookupErr
}

func (r fakeRouteLookup) NodeAlive(ctx context.Context, nodeID string) (bool, error) {
	return r.alive, r.aliveErr
}

func dueDelivery() *store.Delivery {
	return &store.Delivery{
		ID:         7,
		MessageID:  70,
		UserID:     "bob",
		DeviceID:   "d0",
		FromUser:   "alice",
		FromDevice: "a0",
		Payload:    []byte("payload"),
		Attempts:   1,
		ExpireAt:   time.Now().Add(time.Hour),
	}
}

func TestRetryOneRemoteAliveLeavesRowUntouched(t *testing.T) {
	st := &fakeRetryStore{}
	sessions := session.NewManager()
	routes := fakeRouteLookup{
		route: registry.Route{UserID: "bob", DeviceID: "d0", NodeID: "node-b", Version: 1},
		found: true,
		alive: true,
	}
	w := NewWorker(st, sessions, NewDeliverer(sessions), Config{Routes: routes}, discardLogger())

	w.retryOne(context.Background(), dueDelivery(), time.Now())

	if len(st.calls) != 0 {
		t.Fatalf("store calls = %v, want none while the owner node is alive", st.calls)
	}
}

func TestRetryOneRouteLookupErrorLeavesRowUntouched(t *testing.T) {
	st := &fakeRetryStore{}
	sessions := session.NewManager()
	w := NewWorker(st, sessions, NewDeliverer(sessions), Config{
		Routes: fakeRouteLookup{lookupErr: errors.New("redis down")},
	}, discardLogger())

	w.retryOne(context.Background(), dueDelivery(), time.Now())

	if len(st.calls) != 0 {
		t.Fatalf("store calls = %v, want none after a lookup error", st.calls)
	}
}

func TestRetryOneNodeAliveErrorLeavesRowUntouched(t *testing.T) {
	st := &fakeRetryStore{}
	sessions := session.NewManager()
	w := NewWorker(st, sessions, NewDeliverer(sessions), Config{
		Routes: fakeRouteLookup{
			route:    registry.Route{UserID: "bob", DeviceID: "d0", NodeID: "node-b"},
			found:    true,
			aliveErr: errors.New("timeout"),
		},
	}, discardLogger())

	w.retryOne(context.Background(), dueDelivery(), time.Now())

	if len(st.calls) != 0 {
		t.Fatalf("store calls = %v, want none after a liveness error", st.calls)
	}
}

func TestRetryOneNoRouteClearsSchedule(t *testing.T) {
	st := &fakeRetryStore{}
	sessions := session.NewManager()
	w := NewWorker(st, sessions, NewDeliverer(sessions), Config{
		Routes: fakeRouteLookup{found: false},
	}, discardLogger())

	w.retryOne(context.Background(), dueDelivery(), time.Now())

	if st.callsOf("ClearRetrySchedule") != 1 {
		t.Fatalf("ClearRetrySchedule calls = %d, want 1", st.callsOf("ClearRetrySchedule"))
	}
	if len(st.retried) != 0 || len(st.failed) != 0 {
		t.Fatalf("retried = %v, failed = %v, want none", st.retried, st.failed)
	}
}

func TestRetryOneDeadNodeClearsSchedule(t *testing.T) {
	st := &fakeRetryStore{}
	sessions := session.NewManager()
	w := NewWorker(st, sessions, NewDeliverer(sessions), Config{
		Routes: fakeRouteLookup{
			route: registry.Route{UserID: "bob", DeviceID: "d0", NodeID: "node-b"},
			found: true,
			alive: false,
		},
	}, discardLogger())

	w.retryOne(context.Background(), dueDelivery(), time.Now())

	if st.callsOf("ClearRetrySchedule") != 1 {
		t.Fatalf("ClearRetrySchedule calls = %d, want 1", st.callsOf("ClearRetrySchedule"))
	}
}

func TestRetryOneLocalSessionDeliversAndMarksRetried(t *testing.T) {
	st := &fakeRetryStore{}
	sessions := session.NewManager()
	conn := &fakeConn{id: 1}
	registerSession(sessions, "bob", "d0", conn)

	// A route pointing at another node must be ignored when the session is local.
	w := NewWorker(st, sessions, NewDeliverer(sessions), Config{
		Routes: fakeRouteLookup{route: registry.Route{NodeID: "node-b"}, found: true, alive: true},
	}, discardLogger())

	w.retryOne(context.Background(), dueDelivery(), time.Now())

	frames := conn.sent()
	if len(frames) != 1 || frames[0].Type != protocol.Deliver || frames[0].MsgID != 70 {
		t.Fatalf("frames = %+v, want one DELIVER for message 70", frames)
	}
	if st.callsOf("MarkDeliveryRetried") != 1 {
		t.Fatalf("MarkDeliveryRetried calls = %d, want 1", st.callsOf("MarkDeliveryRetried"))
	}
	if st.callsOf("ClearRetrySchedule") != 0 {
		t.Fatalf("ClearRetrySchedule calls = %d, want 0", st.callsOf("ClearRetrySchedule"))
	}
}
