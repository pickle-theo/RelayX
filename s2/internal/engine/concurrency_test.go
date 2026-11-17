package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"relayx/s2/internal/config"
	"relayx/s2/internal/protocol"
	"relayx/s2/internal/store"
	"relayx/s2/internal/store/memory"
)

type testSink struct{}

func (testSink) EnqueueReliable(context.Context, protocol.Frame) error { return nil }

type testResolver struct{}

func (testResolver) Lookup(string, string, string) (Sink, bool) { return testSink{}, true }

func setupMessage(t *testing.T, id string) (*memory.Store, *Engine, *memory.SkewClock) {
	t.Helper()
	clock := memory.NewSkewClock()
	st := memory.New(clock)
	cfg := config.Default()
	eng := New(st, cfg, testResolver{}, nil)
	ctx := context.Background()
	m := &store.Message{
		ID: id, TenantID: "t1", ProducerID: "u:alice", ProducerKind: store.ProducerUser,
		SourceDeviceID: "phone", ClientMsgID: id, Payload: []byte("x"), PayloadSize: 1,
		DeliveryMode: store.ModeAllEndpoints, TTLSeconds: 60, MaxAttempts: 3,
		PlatformState: store.MessageAccepted,
	}
	if err := st.WithinTx(ctx, func(tx store.Tx) error {
		if _, _, err := tx.Messages().Insert(ctx, tx, m); err != nil {
			return err
		}
		if _, err := tx.Messages().MarkFanoutDone(ctx, tx, id); err != nil {
			return err
		}
		return tx.Deliveries().InsertBatch(ctx, tx, []*store.Delivery{{
			ID: id + "-d", TenantID: "t1", MessageID: id,
			UserID: "bob", DeviceID: "phone", EndpointID: "bob:phone",
			DeliveryState: store.DeliverySent, Attempts: 1,
		}})
	}); err != nil {
		t.Fatal(err)
	}
	return st, eng, clock
}

func countFacts(t *testing.T, st *memory.Store, id, typ string) int {
	t.Helper()
	fs, _ := st.Facts().ListByMessage(context.Background(), id)
	n := 0
	for _, f := range fs {
		if f.FactType == typ {
			n++
		}
	}
	return n
}

func TestConcurrentDuplicateReceiptACK(t *testing.T) {
	st, eng, _ := setupMessage(t, "m-ack")
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = eng.HandleReceiptAck(context.Background(), "t1", "m-ack", "bob", "phone")
		}()
	}
	wg.Wait()
	m, _ := st.Messages().GetByID(context.Background(), "m-ack")
	if m.PlatformState != store.MessageSettled || m.StateVersion != 1 {
		t.Fatalf("message=%+v", m)
	}
	if got := countFacts(t, st, "m-ack", store.FactDeliveryReceiptAcked); got != 1 {
		t.Fatalf("receipt facts=%d", got)
	}
	if got := countFacts(t, st, "m-ack", store.FactMessageSettled); got != 1 {
		t.Fatalf("settled facts=%d", got)
	}
}

func TestAckExpiryTerminalExclusivity(t *testing.T) {
	for i := 0; i < 50; i++ {
		id := "m-race"
		st, eng, clock := setupMessage(t, id)
		clock.SetOffset(120 * time.Second) // message TTL is 60s
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = eng.HandleReceiptAck(context.Background(), "t1", id, "bob", "phone")
		}()
		go func() {
			defer wg.Done()
			_ = eng.ExpireMessageWithDeliveries(context.Background(), id)
		}()
		wg.Wait()

		settled := countFacts(t, st, id, store.FactMessageSettled)
		expired := countFacts(t, st, id, store.FactMessageExpired)
		if settled+expired != 1 {
			t.Fatalf("round %d: settled=%d expired=%d", i, settled, expired)
		}
		m, _ := st.Messages().GetByID(context.Background(), id)
		want := store.MessageSettled
		if expired == 1 {
			want = store.MessageExpired
		}
		if m.PlatformState != want {
			t.Fatalf("round %d: state=%s want=%s", i, m.PlatformState, want)
		}
	}
}

func TestConcurrentSubmitSameIdempotencyKey(t *testing.T) {
	st := memory.New(nil)
	cfg := config.Default()
	eng := New(st, cfg, testResolver{}, nil)
	// Ingress 位于其他包；此处用与受理路径相同的插入语义
	// 直接验证 store 幂等键。
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := "m" + string(rune('a'+i%26)) + string(rune('0'+i/26))
			m := &store.Message{
				ID: id, TenantID: "t1", ProducerID: "u:alice",
				ClientMsgID: "same", Payload: []byte("x"), PayloadSize: 1,
				TTLSeconds: 60, MaxAttempts: 3, DeliveryMode: store.ModeAllEndpoints,
				PlatformState: store.MessageAccepted,
			}
			_ = st.WithinTx(context.Background(), func(tx store.Tx) error {
				_, _, err := tx.Messages().Insert(context.Background(), tx, m)
				return err
			})
		}(i)
	}
	wg.Wait()
	got, err := st.Messages().GetByIdempotencyKey(context.Background(), "t1", "u:alice", "same")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		id := "m" + string(rune('a'+i%26)) + string(rune('0'+i/26))
		other, err := st.Messages().GetByID(context.Background(), id)
		if err == nil && other.ID != got.ID {
			t.Fatalf("duplicate logical message: %s", other.ID)
		}
	}
	_ = eng
}
