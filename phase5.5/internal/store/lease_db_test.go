package store

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// acceptFanout seeds one accepted message with a fan-out event.
func acceptFanout(t *testing.T, st *Store, tenant, fromUser string, clientMsgID uint64, targetType TargetType, target, targetUser, targetDevice string) uint64 {
	t.Helper()
	res, err := st.AcceptMessage(context.Background(), AcceptRequest{
		TenantID:     tenant,
		ClientMsgID:  clientMsgID,
		FromUser:     fromUser,
		FromDevice:   "d0",
		TargetType:   targetType,
		Target:       target,
		Payload:      []byte("payload"),
		ExpireAt:     time.Now().Add(time.Hour),
		TargetUser:   targetUser,
		TargetDevice: targetDevice,
		OutboxTopic:  "relayx.fanout.v1",
		OutboxKey:    "default|" + target,
		OutboxPayload: func(messageID uint64) ([]byte, error) {
			return []byte(`{"version":1,"message_id":` + itoa(messageID) + `}`), nil
		},
	})
	if err != nil {
		t.Fatalf("AcceptMessage() error = %v", err)
	}
	return res.MessageID
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}

func TestOutboxRelayPublishesAndMarks(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	acceptFanout(t, st, "default", "alice", 1, TargetUser, "bob", "", "")

	var published []OutboxEvent
	n, err := st.RelayOutbox(ctx, 10, time.Second, time.Minute, func(_ context.Context, ev OutboxEvent) error {
		published = append(published, ev)
		return nil
	})
	if err != nil {
		t.Fatalf("RelayOutbox() error = %v", err)
	}
	if n != 1 || len(published) != 1 {
		t.Fatalf("RelayOutbox() published %d events (n=%d), want 1", len(published), n)
	}
	if published[0].Topic != "relayx.fanout.v1" || published[0].PartitionKey != "default|bob" {
		t.Fatalf("event = %s/%s, want relayx.fanout.v1/default|bob", published[0].Topic, published[0].PartitionKey)
	}

	var state int16
	var publishedAt sql.NullTime
	if err := st.DB().QueryRow(`SELECT state, published_at FROM outbox`).Scan(&state, &publishedAt); err != nil {
		t.Fatalf("select outbox: %v", err)
	}
	if OutboxState(state) != OutboxPublished || !publishedAt.Valid {
		t.Fatalf("outbox row = state %d published %v, want PUBLISHED with published_at", state, publishedAt)
	}

	// A second pass finds nothing.
	if n, err := st.RelayOutbox(ctx, 10, time.Second, time.Minute, func(context.Context, OutboxEvent) error { return nil }); err != nil || n != 0 {
		t.Fatalf("second RelayOutbox() = (%d, %v), want (0, nil)", n, err)
	}
}

func TestOutboxRelayPublishFailureKeepsPending(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	acceptFanout(t, st, "default", "alice", 1, TargetUser, "bob", "", "")

	wantErr := errors.New("kafka down")
	n, err := st.RelayOutbox(ctx, 10, time.Second, time.Minute, func(context.Context, OutboxEvent) error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("RelayOutbox() error = %v, want %v", err, wantErr)
	}
	if n != 0 {
		t.Fatalf("RelayOutbox() published = %d, want 0", n)
	}

	var (
		state    int16
		attempts int
		nextWait float64
	)
	if err := st.DB().QueryRow(`
SELECT state, attempts, EXTRACT(EPOCH FROM (next_attempt_at - now()))::float8
FROM outbox`).Scan(&state, &attempts, &nextWait); err != nil {
		t.Fatalf("select outbox: %v", err)
	}
	if OutboxState(state) != OutboxPending {
		t.Fatalf("outbox state = %d, want PENDING", state)
	}
	if attempts != 1 {
		t.Fatalf("outbox attempts = %d, want 1", attempts)
	}
	if nextWait <= 0 {
		t.Fatalf("next_attempt_at is %fs away, want a future backoff", nextWait)
	}
}

// TestOutboxRelayTwoInstancesDoNotShareRows checks the SKIP LOCKED guarantee: a
// slow publisher keeps its row locked, so a concurrent relay publishes other
// rows and never the one already in flight.
func TestOutboxRelayTwoInstancesDoNotShareRows(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	for i := uint64(1); i <= 2; i++ {
		acceptFanout(t, st, "default", "alice", i, TargetUser, "bob", "", "")
	}

	inFlight := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan struct{})
	var firstEvents atomic.Int32

	go func() {
		defer close(firstDone)
		_, _ = st.RelayOutbox(ctx, 1, time.Second, time.Minute, func(context.Context, OutboxEvent) error {
			firstEvents.Add(1)
			close(inFlight)
			<-release
			return nil
		})
	}()

	select {
	case <-inFlight:
	case <-time.After(3 * time.Second):
		t.Fatal("first relay never reached its publisher")
	}

	var secondEvents atomic.Int32
	n, err := st.RelayOutbox(ctx, 10, time.Second, time.Minute, func(context.Context, OutboxEvent) error {
		secondEvents.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("second RelayOutbox() error = %v", err)
	}
	if n != 1 || secondEvents.Load() != 1 {
		t.Fatalf("second relay published %d (n=%d), want the single unlocked row", secondEvents.Load(), n)
	}

	close(release)
	<-firstDone
	if got := firstEvents.Load(); got != 1 {
		t.Fatalf("first relay published %d events, want 1", got)
	}
	if got := mustCount(t, st, "SELECT count(*) FROM outbox WHERE state = 0"); got != 0 {
		t.Fatalf("pending outbox rows = %d, want 0", got)
	}
}

func TestClaimDueDeliveriesShardFilter(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	msgID := acceptFanout(t, st, "default", "alice", 1, TargetUser, "bob", "", "")

	var endpoints []Endpoint
	for _, device := range []string{"d0", "d1", "d2", "d3"} {
		endpoints = append(endpoints, Endpoint{UserID: "bob", DeviceID: device})
	}
	if _, err := st.InsertDeliveries(ctx, msgID, endpoints); err != nil {
		t.Fatalf("InsertDeliveries() error = %v", err)
	}

	now := time.Now()
	claims := map[int][]uint64{}
	for shard := 0; shard < 2; shard++ {
		items, err := st.ClaimDueDeliveries(ctx, now, 2, shard, 10, "worker-a", time.Minute)
		if err != nil {
			t.Fatalf("ClaimDueDeliveries(shard %d) error = %v", shard, err)
		}
		for _, d := range items {
			claims[shard] = append(claims[shard], d.ID)
		}
	}
	total := len(claims[0]) + len(claims[1])
	if total != 4 {
		t.Fatalf("claimed %d deliveries across shards, want 4", total)
	}
	for _, id := range claims[0] {
		if id%2 != 0 {
			t.Fatalf("shard 0 claimed delivery %d with odd id", id)
		}
	}
	for _, id := range claims[1] {
		if id%2 != 1 {
			t.Fatalf("shard 1 claimed delivery %d with even id", id)
		}
	}
}

func TestClaimDueDeliveriesIsExclusive(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	msgID := acceptFanout(t, st, "default", "alice", 1, TargetUser, "bob", "", "")
	endpoints := make([]Endpoint, 0, 20)
	for i := 0; i < 20; i++ {
		endpoints = append(endpoints, Endpoint{UserID: "bob", DeviceID: "d" + itoa(uint64(i))})
	}
	if _, err := st.InsertDeliveries(ctx, msgID, endpoints); err != nil {
		t.Fatalf("InsertDeliveries() error = %v", err)
	}

	now := time.Now()
	var (
		mu     sync.Mutex
		seen   = map[uint64]int{}
		wg     sync.WaitGroup
		errsMu sync.Mutex
		errs   []error
	)
	for worker := 0; worker < 4; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			items, err := st.ClaimDueDeliveries(ctx, now, 1, 0, 10, "worker-"+itoa(uint64(worker)), time.Minute)
			if err != nil {
				errsMu.Lock()
				errs = append(errs, err)
				errsMu.Unlock()
				return
			}
			mu.Lock()
			for _, d := range items {
				seen[d.ID]++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if len(errs) > 0 {
		t.Fatalf("concurrent ClaimDueDeliveries() errors = %v", errs)
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("delivery %d claimed %d times, want exactly once", id, count)
		}
	}
}

func TestLeaseReclaimAfterExpiry(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	msgID := acceptFanout(t, st, "default", "alice", 1, TargetUser, "bob", "", "")
	if _, err := st.InsertDeliveries(ctx, msgID, []Endpoint{{UserID: "bob", DeviceID: "d0"}}); err != nil {
		t.Fatalf("InsertDeliveries() error = %v", err)
	}

	now := time.Now()
	claimed, err := st.ClaimDueDeliveries(ctx, now, 1, 0, 10, "worker-a", 50*time.Millisecond)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("first ClaimDueDeliveries() = %d rows, err=%v, want 1", len(claimed), err)
	}

	// The lease is still valid: another worker gets nothing.
	if items, err := st.ClaimDueDeliveries(ctx, now, 1, 0, 10, "worker-b", 50*time.Millisecond); err != nil || len(items) != 0 {
		t.Fatalf("claim during valid lease = %d rows, err=%v, want 0", len(items), err)
	}

	// After the lease expires (worker crash), the row is reclaimable.
	if items, err := st.ClaimDueDeliveries(ctx, now.Add(100*time.Millisecond), 1, 0, 10, "worker-b", time.Minute); err != nil || len(items) != 1 {
		t.Fatalf("claim after lease expiry = %d rows, err=%v, want 1", len(items), err)
	}

	// The original owner no longer owns the row.
	owned, _, err := st.MarkDeliverySentLeased(ctx, claimed[0].ID, "worker-a")
	if err != nil {
		t.Fatalf("MarkDeliverySentLeased() error = %v", err)
	}
	if owned {
		t.Fatal("worker-a still owned a delivery taken over by worker-b")
	}
}

func TestRetryLeasedDoesNotCountThrottlingAsAttempt(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	msgID := acceptFanout(t, st, "default", "alice", 1, TargetUser, "bob", "", "")
	if _, err := st.InsertDeliveries(ctx, msgID, []Endpoint{{UserID: "bob", DeviceID: "d0"}}); err != nil {
		t.Fatalf("InsertDeliveries() error = %v", err)
	}
	items, err := st.ClaimDueDeliveries(ctx, time.Now(), 1, 0, 10, "worker-a", time.Minute)
	if err != nil || len(items) != 1 {
		t.Fatalf("ClaimDueDeliveries() = %d rows, err=%v, want 1", len(items), err)
	}

	failed, err := st.RetryLeased(ctx, items[0].ID, "worker-a", time.Now().Add(time.Second), false, 5)
	if err != nil {
		t.Fatalf("RetryLeased() error = %v", err)
	}
	if failed {
		t.Fatal("RetryLeased() reported FAILED for a throttled row")
	}

	var (
		state    int16
		attempts int
		lease    sql.NullTime
		worker   sql.NullString
	)
	if err := st.DB().QueryRow(
		`SELECT state, attempts, lease_until, worker_id FROM deliveries`).Scan(&state, &attempts, &lease, &worker); err != nil {
		t.Fatalf("select delivery: %v", err)
	}
	if state != int16(DeliveryPending) || attempts != 0 {
		t.Fatalf("delivery = state %d attempts %d, want PENDING with 0 attempts", state, attempts)
	}
	if lease.Valid || worker.Valid {
		t.Fatalf("lease = %v worker = %v, want both cleared", lease, worker)
	}
}

func TestRetryLeasedFailsAtMaxAttempts(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	msgID := acceptFanout(t, st, "default", "alice", 1, TargetUser, "bob", "", "")
	if _, err := st.InsertDeliveries(ctx, msgID, []Endpoint{{UserID: "bob", DeviceID: "d0"}}); err != nil {
		t.Fatalf("InsertDeliveries() error = %v", err)
	}
	if _, err := st.DB().Exec(`UPDATE deliveries SET state = 1, attempts = 4`); err != nil {
		t.Fatalf("seed attempts: %v", err)
	}
	items, err := st.ClaimDueDeliveries(ctx, time.Now(), 1, 0, 10, "worker-a", time.Minute)
	if err != nil || len(items) != 1 {
		t.Fatalf("ClaimDueDeliveries() = %d rows, err=%v, want 1", len(items), err)
	}

	failed, err := st.RetryLeased(ctx, items[0].ID, "worker-a", time.Now(), true, 5)
	if err != nil {
		t.Fatalf("RetryLeased() error = %v", err)
	}
	if !failed {
		t.Fatal("RetryLeased() did not reach FAILED at max attempts")
	}
	var state int16
	if err := st.DB().QueryRow(`SELECT state FROM deliveries`).Scan(&state); err != nil {
		t.Fatalf("select state: %v", err)
	}
	if DeliveryState(state) != DeliveryFailed {
		t.Fatalf("state = %d, want FAILED", state)
	}
}

func TestAckAndExpiryRaceOnlyOneTransition(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	msgID := acceptFanout(t, st, "default", "alice", 1, TargetUser, "bob", "", "")
	if _, err := st.InsertDeliveries(ctx, msgID, []Endpoint{{UserID: "bob", DeviceID: "d0"}}); err != nil {
		t.Fatalf("InsertDeliveries() error = %v", err)
	}
	items, err := st.ClaimDueDeliveries(ctx, time.Now(), 1, 0, 10, "worker-a", time.Minute)
	if err != nil || len(items) != 1 {
		t.Fatalf("ClaimDueDeliveries() = %d rows, err=%v, want 1", len(items), err)
	}
	if owned, _, err := st.MarkDeliverySentLeased(ctx, items[0].ID, "worker-a"); err != nil || !owned {
		t.Fatalf("MarkDeliverySentLeased() = (%v, %v), want owned", owned, err)
	}

	// A business ACK wins the race and moves the row to a terminal state.
	changed, err := st.ApplyBusinessAck(ctx, "bob", "d0", msgID, 0)
	if err != nil || !changed {
		t.Fatalf("ApplyBusinessAck() = (%v, %v), want changed", changed, err)
	}
	// The expiry sweep must not rewrite a terminal row.
	if err := st.ExpireMessage(ctx, msgID); err != nil {
		t.Fatalf("ExpireMessage() error = %v", err)
	}
	var state int16
	if err := st.DB().QueryRow(`SELECT state FROM deliveries`).Scan(&state); err != nil {
		t.Fatalf("select delivery state: %v", err)
	}
	if DeliveryState(state) != DeliveryBusinessAcked {
		t.Fatalf("delivery state = %d, want BUSINESS_ACKED (terminal rows are immutable)", state)
	}

	var msgState int16
	if err := st.DB().QueryRow(`SELECT state FROM messages`).Scan(&msgState); err != nil {
		t.Fatalf("select message state: %v", err)
	}
	if MessageState(msgState) != MessageExpired {
		t.Fatalf("message state = %d, want EXPIRED", msgState)
	}
}

func TestTenantQuotaRoundTrip(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	quota, found, err := st.GetTenant(ctx, "default")
	if err != nil {
		t.Fatalf("GetTenant(default) error = %v", err)
	}
	if !found {
		t.Fatal("GetTenant(default) not found, want the migration-seeded row")
	}
	if quota.RatePerSec != 0 || quota.MaxInflight != 0 {
		t.Fatalf("default quota = %+v, want unlimited defaults", quota)
	}

	if err := st.UpsertTenant(ctx, Tenant{ID: "acme", RatePerSec: 5, Burst: 10, MaxInflight: 2}); err != nil {
		t.Fatalf("UpsertTenant() error = %v", err)
	}
	got, found, err := st.GetTenant(ctx, "acme")
	if err != nil || !found {
		t.Fatalf("GetTenant(acme) = (%+v, %v, %v), want found", got, found, err)
	}
	if got.RatePerSec != 5 || got.Burst != 10 || got.MaxInflight != 2 {
		t.Fatalf("tenant = %+v, want rate 5 burst 10 inflight 2", got)
	}

	if _, found, err := st.GetTenant(ctx, "missing"); err != nil || found {
		t.Fatalf("GetTenant(missing) = (_, %v, %v), want not found", found, err)
	}
}

func TestEndpointExists(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	if err := st.UpsertEndpoint(ctx, "bob", "d0"); err != nil {
		t.Fatalf("UpsertEndpoint() error = %v", err)
	}
	exists, err := st.EndpointExists(ctx, "bob", "d0")
	if err != nil || !exists {
		t.Fatalf("EndpointExists(bob/d0) = (%v, %v), want true", exists, err)
	}
	exists, err = st.EndpointExists(ctx, "bob", "ghost")
	if err != nil || exists {
		t.Fatalf("EndpointExists(bob/ghost) = (%v, %v), want false", exists, err)
	}
}
