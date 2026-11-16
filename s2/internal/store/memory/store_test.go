package memory

import (
	"context"
	"sync"
	"testing"
	"time"

	"relayx/s2/internal/facts"
	"relayx/s2/internal/store"
)

func newMessage(id, clientMsgID string, ttl int32) *store.Message {
	return &store.Message{
		ID: id, TenantID: "t1", ProducerID: "u:alice", ProducerKind: store.ProducerUser,
		SourceDeviceID: "phone", ClientMsgID: clientMsgID,
		Payload: []byte("payload"), PayloadSize: 7,
		DeliveryMode: store.ModeAllEndpoints, TTLSeconds: ttl, MaxAttempts: 3,
		PlatformState: store.MessageAccepted,
	}
}

func insertMessage(t *testing.T, s *Store, m *store.Message) {
	t.Helper()
	if err := s.WithinTx(context.Background(), func(tx store.Tx) error {
		_, _, err := tx.Messages().Insert(context.Background(), tx, m)
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func insertDeliveries(t *testing.T, s *Store, msgID string, states ...string) []*store.Delivery {
	t.Helper()
	ds := make([]*store.Delivery, 0, len(states))
	for i, st := range states {
		ds = append(ds, &store.Delivery{
			ID: msgID + "-d" + string(rune('a'+i)), TenantID: "t1", MessageID: msgID,
			UserID: "bob", DeviceID: string(rune('a' + i)), EndpointID: "bob:" + string(rune('a'+i)),
			DeliveryState: st,
		})
	}
	if err := s.WithinTx(context.Background(), func(tx store.Tx) error {
		return tx.Deliveries().InsertBatch(context.Background(), tx, ds)
	}); err != nil {
		t.Fatal(err)
	}
	return ds
}

func TestSettleGuardAndCounts(t *testing.T) {
	s := New(nil)
	m := newMessage("m1", "c1", 60)
	insertMessage(t, s, m)

	if _, hit, _ := s.Messages().SettleIfEligible(context.Background(), nil, "m1"); hit {
		t.Fatal("must not settle before fanout_done")
	}
	if first, _ := s.Messages().MarkFanoutDone(context.Background(), nil, "m1"); !first {
		t.Fatal("first fanout marker must win")
	}
	if second, _ := s.Messages().MarkFanoutDone(context.Background(), nil, "m1"); second {
		t.Fatal("fanout marker must be idempotent")
	}

	insertDeliveries(t, s, "m1", store.DeliveryReceiptAcked, store.DeliveryFailed, store.DeliveryExpired)
	res, hit, err := s.Messages().SettleIfEligible(context.Background(), nil, "m1")
	if err != nil || !hit {
		t.Fatalf("settle CAS: hit=%v err=%v", hit, err)
	}
	if res.Counts != (store.FinalCounts{Total: 3, ReceiptAcked: 1, Failed: 1, Expired: 1}) {
		t.Fatalf("counts=%+v", res.Counts)
	}
	got, _ := s.Messages().GetByID(context.Background(), "m1")
	if got.PlatformState != store.MessageSettled || got.StateVersion != 1 || got.SettledAt == nil {
		t.Fatalf("message=%+v", got)
	}
	if _, hit, _ := s.Messages().ExpireIfEligible(context.Background(), nil, "m1"); hit {
		t.Fatal("SETTLED must not be overwritten by expire CAS")
	}
}

func TestExpireSettleMutualExclusion(t *testing.T) {
	for i := 0; i < 50; i++ {
		s := New(nil)
		id := "m" + string(rune('a'+i%26))
		m := newMessage(id, id, 1)
		insertMessage(t, s, m)
		_, _ = s.Messages().MarkFanoutDone(context.Background(), nil, id)

		var wg sync.WaitGroup
		var settleHit, expireHit int
		var mu sync.Mutex
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, hit, _ := s.Messages().SettleIfEligible(context.Background(), nil, id)
			if hit {
				mu.Lock()
				settleHit++
				mu.Unlock()
			}
		}()
		go func() {
			defer wg.Done()
			_, hit, _ := s.Messages().ExpireIfEligible(context.Background(), nil, id)
			if hit {
				mu.Lock()
				expireHit++
				mu.Unlock()
			}
		}()
		wg.Wait()
		if settleHit+expireHit != 1 {
			t.Fatalf("round %d: settle=%d expire=%d", i, settleHit, expireHit)
		}
		got, _ := s.Messages().GetByID(context.Background(), id)
		want := store.MessageSettled
		if expireHit == 1 {
			want = store.MessageExpired
		}
		if got.PlatformState != want {
			t.Fatalf("round %d state=%s want=%s", i, got.PlatformState, want)
		}
	}
}

func TestDeliveryTerminalImmutable(t *testing.T) {
	s := New(nil)
	insertMessage(t, s, newMessage("m1", "c1", 60))
	ds := insertDeliveries(t, s, "m1", store.DeliveryReceiptAcked)
	d := ds[0]

	version, hit, err := s.Deliveries().ExpireIfEligible(context.Background(), nil, d.ID)
	if err != nil || hit || version != 0 {
		t.Fatalf("terminal delivery expired: hit=%v version=%d err=%v", hit, version, err)
	}
	if ok, err := s.Deliveries().Transition(context.Background(), nil, d.ID,
		[]string{store.DeliverySent}, store.DeliveryReceiptAcked, store.DeliveryPatch{}, d.StateVersion); err != nil || ok {
		t.Fatalf("illegal transition accepted: ok=%v err=%v", ok, err)
	}
}

func TestIdempotencyAndSourceDevice(t *testing.T) {
	s := New(nil)
	m := newMessage("m1", "c1", 60)
	insertMessage(t, s, m)
	insertMessage(t, s, newMessage("m2", "c1", 60))

	got, err := s.Messages().GetByIdempotencyKey(context.Background(), "t1", "u:alice", "c1")
	if err != nil || got.ID != "m1" || got.SourceDeviceID != "phone" {
		t.Fatalf("idempotency lookup: %+v %v", got, err)
	}
}

func TestStoreClockControlsExpiry(t *testing.T) {
	clock := NewSkewClock()
	s := New(clock)
	insertMessage(t, s, newMessage("m1", "c1", 10))

	due, _ := s.Messages().ListExpired(context.Background(), 10)
	if len(due) != 0 {
		t.Fatal("message must not be due before clock advance")
	}
	clock.SetOffset(20 * time.Second)
	due, _ = s.Messages().ListExpired(context.Background(), 10)
	if len(due) != 1 {
		t.Fatalf("expected one expired message, got %d", len(due))
	}
}

func TestFactEventIDIdempotent(t *testing.T) {
	s := New(nil)
	ep := "bob:a"
	f := &store.Fact{
		TenantID: "t1", MessageID: "m1", EndpointID: &ep,
		FactType: store.FactDeliveryCreated, StateVersion: 0,
		PayloadSummary: map[string]any{"user_id": "bob"},
	}
	if err := s.Facts().InsertBatch(context.Background(), nil, []*store.Fact{f}); err != nil {
		t.Fatal(err)
	}
	want := facts.Derive("t1", "m1", store.FactDeliveryCreated, &ep, 0)
	list, _ := s.Facts().ListByMessage(context.Background(), "m1")
	if len(list) != 1 || list[0].EventID != want {
		t.Fatalf("facts=%+v want=%s", list, want)
	}
	if err := s.Facts().InsertBatch(context.Background(), nil, []*store.Fact{f}); err != nil {
		t.Fatal(err)
	}
	list, _ = s.Facts().ListByMessage(context.Background(), "m1")
	if len(list) != 1 {
		t.Fatalf("event_id idempotency broken: %d facts", len(list))
	}
}
