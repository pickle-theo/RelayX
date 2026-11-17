package ingress

import (
	"context"
	"errors"
	"sync"
	"testing"

	"relayx/s2/internal/config"
	"relayx/s2/internal/engine"
	"relayx/s2/internal/protocol"
	"relayx/s2/internal/store"
	"relayx/s2/internal/store/memory"
)

type sink struct {
	mu     sync.Mutex
	frames []protocol.Frame
}

func (s *sink) EnqueueReliable(_ context.Context, f protocol.Frame) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := f
	c.Body = append([]byte(nil), f.Body...)
	s.frames = append(s.frames, c)
	return nil
}

func (s *sink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.frames)
}

type resolver struct {
	s *sink
}

func (r *resolver) Lookup(_, _, _ string) (engine.Sink, bool) { return r.s, true }

func fixture() (*config.Config, *memory.Store, *engine.Engine, *Service, *sink) {
	cfg := config.Default()
	st := memory.New(nil)
	sk := &sink{}
	eng := engine.New(st, cfg, &resolver{s: sk}, nil)
	return &cfg, st, eng, New(st, cfg, eng), sk
}

func factsOf(t *testing.T, st *memory.Store, id string) []*store.Fact {
	t.Helper()
	fs, err := st.Facts().ListByMessage(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return fs
}

func factTypes(fs []*store.Fact) map[string]bool {
	out := make(map[string]bool, len(fs))
	for _, f := range fs {
		out[f.FactType] = true
	}
	return out
}

func TestZeroEndpointSettlesInAdmission(t *testing.T) {
	_, st, _, svc, _ := fixture()
	res, err := svc.Submit(context.Background(), SubmitInput{
		TenantID: "t1", ProducerID: "u:alice", ProducerKind: store.ProducerUser,
		SourceDeviceID: "phone", ClientMsgID: "c1", TargetUserID: "nobody",
		Payload: []byte("x"), Policy: config.DefaultPolicy(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.State != store.MessageSettled {
		t.Fatalf("state=%s", res.State)
	}
	ds, _ := st.Deliveries().ListByMessage(context.Background(), res.MessageID)
	if len(ds) != 0 {
		t.Fatalf("zero endpoints produced %d deliveries", len(ds))
	}
	types := factTypes(factsOf(t, st, res.MessageID))
	if !types[store.FactMessageAccepted] || !types[store.FactMessageSettled] || types[store.FactDeliveryCreated] {
		t.Fatalf("facts=%v", types)
	}
	fs := factsOf(t, st, res.MessageID)
	for _, f := range fs {
		if f.FactType == store.FactMessageAccepted || f.FactType == store.FactMessageSettled {
			if f.PayloadSummary["source_device_id"] != "phone" || f.PayloadSummary["producer_id"] != "u:alice" {
				t.Fatalf("message fact missing source: %+v", f.PayloadSummary)
			}
		}
	}
}

func TestOnlineDeliveryReceiptAndBusinessAck(t *testing.T) {
	_, st, eng, svc, sk := fixture()
	if err := st.Endpoints().MarkOnline(context.Background(), "t1", "bob", "phone", "s1"); err != nil {
		t.Fatal(err)
	}
	res, err := svc.Submit(context.Background(), SubmitInput{
		TenantID: "t1", ProducerID: "u:alice", ProducerKind: store.ProducerUser,
		SourceDeviceID: "phone", ClientMsgID: "c1", TargetUserID: "bob",
		TargetDevices: []string{"phone"}, Payload: []byte("hello"),
		Policy: config.Policy{TTLSeconds: 60, MaxAttempts: 3, DeliveryMode: store.ModeAllEndpoints},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.State != store.MessageAccepted {
		t.Fatalf("state=%s", res.State)
	}
	if err := eng.DispatchMessage(context.Background(), res.MessageID); err != nil {
		t.Fatal(err)
	}
	if sk.count() != 1 {
		t.Fatalf("deliver frames=%d", sk.count())
	}
	f := sk.recorded()[0]
	if f.Type != protocol.TypeDeliver {
		t.Fatalf("frame type=0x%02x", f.Type)
	}
	body, err := protocol.DecodeDeliver(f.Body)
	if err != nil || string(body.Payload) != "hello" {
		t.Fatalf("deliver body=%+v err=%v", body, err)
	}
	d, err := st.Deliveries().GetByKey(context.Background(), "t1", res.MessageID, "bob", "phone")
	if err != nil || d.DeliveryState != store.DeliverySent {
		t.Fatalf("delivery=%+v err=%v", d, err)
	}

	if err := eng.HandleBusinessAck(context.Background(), "t1", res.MessageID, "bob", "phone", "seen"); err != nil {
		t.Fatal(err)
	}
	d2, _ := st.Deliveries().GetByKey(context.Background(), "t1", res.MessageID, "bob", "phone")
	if d2.DeliveryState != store.DeliverySent || d2.ApplicationStatus == nil || *d2.ApplicationStatus != "seen" {
		t.Fatalf("business ack changed state: %+v", d2)
	}
	if factTypes(factsOf(t, st, res.MessageID))["business.ack"] {
		t.Fatal("business ack must not create a fact")
	}

	if err := eng.HandleReceiptAck(context.Background(), "t1", res.MessageID, "bob", "phone"); err != nil {
		t.Fatal(err)
	}
	if err := eng.HandleReceiptAck(context.Background(), "t1", res.MessageID, "bob", "phone"); err != nil {
		t.Fatal(err)
	}
	m, _ := st.Messages().GetByID(context.Background(), res.MessageID)
	if m.PlatformState != store.MessageSettled {
		t.Fatalf("state=%s", m.PlatformState)
	}
	var settled, ackFacts int
	for _, f := range factsOf(t, st, res.MessageID) {
		switch f.FactType {
		case store.FactMessageSettled:
			settled++
			counts, ok := f.PayloadSummary["counts"].(map[string]any)
			if !ok || counts["receipt_acked"] != 1 {
				t.Fatalf("counts=%v", f.PayloadSummary["counts"])
			}
		case store.FactDeliveryReceiptAcked:
			ackFacts++
		}
	}
	if settled != 1 || ackFacts != 1 {
		t.Fatalf("settled=%d acked=%d", settled, ackFacts)
	}
}

func TestPolicyAndPayloadLimits(t *testing.T) {
	_, st, _, svc, _ := fixture()
	in := SubmitInput{
		TenantID: "t1", ProducerID: "u:alice", ProducerKind: store.ProducerUser,
		ClientMsgID: "c1", TargetUserID: "bob", Payload: []byte("x"),
		Policy: config.Policy{TTLSeconds: 2592001, MaxAttempts: 3, DeliveryMode: store.ModeAllEndpoints},
	}
	if _, err := svc.Submit(context.Background(), in); err == nil {
		t.Fatal("TTL above hard limit accepted")
	}
	in.Policy = config.DefaultPolicy()
	in.Payload = make([]byte, 61441)
	if _, err := svc.Submit(context.Background(), in); err == nil {
		t.Fatal("oversized payload accepted")
	}
	ds, _ := st.Deliveries().ListByMessage(context.Background(), "unused")
	if len(ds) != 0 {
		t.Fatal("rejected submissions must not write deliveries")
	}
}

func (s *sink) recorded() []protocol.Frame {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.frames
}

// 并发重放必须全部收敛到同一条逻辑消息：恰好一个受理者，其余为幂等重放。
// 旧实现在事务外先查后写，会导致后到的请求拿新 message_id 写投递后回滚并报错。
func TestConcurrentSubmitSameIdempotencyKey(t *testing.T) {
	_, st, _, svc, _ := fixture()
	const n = 64
	results := make([]SubmitResult, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = svc.Submit(context.Background(), SubmitInput{
				TenantID: "t1", ProducerID: "u:alice", ProducerKind: store.ProducerUser,
				SourceDeviceID: "phone", ClientMsgID: "same-key", TargetUserID: "bob",
				Payload: []byte("x"), Policy: config.DefaultPolicy(),
			})
		}(i)
	}
	close(start)
	wg.Wait()

	canonical := ""
	winners := 0
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("submit %d: %v", i, errs[i])
		}
		if canonical == "" {
			canonical = results[i].MessageID
		}
		if results[i].MessageID != canonical {
			t.Fatalf("submit %d id=%s want %s", i, results[i].MessageID, canonical)
		}
		if !results[i].Duplicate {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("winners=%d, want exactly 1", winners)
	}
	accepted := 0
	for _, f := range factsOf(t, st, canonical) {
		if f.FactType == store.FactMessageAccepted {
			accepted++
		}
	}
	if accepted != 1 {
		t.Fatalf("message.accepted facts=%d, want 1", accepted)
	}
}

func TestDuplicateReplayAndMismatch(t *testing.T) {
	_, _, _, svc, _ := fixture()
	in := SubmitInput{
		TenantID: "t1", ProducerID: "u:alice", ProducerKind: store.ProducerUser,
		SourceDeviceID: "phone", ClientMsgID: "c1", TargetUserID: "bob",
		Payload: []byte("x"), Policy: config.DefaultPolicy(),
	}
	first, err := svc.Submit(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	again, err := svc.Submit(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !again.Duplicate || again.MessageID != first.MessageID {
		t.Fatalf("replay=%+v first=%+v", again, first)
	}
	in.Payload = []byte("different")
	if _, err := svc.Submit(context.Background(), in); !errors.Is(err, store.ErrDuplicateMismatch) {
		t.Fatalf("mismatch err=%v", err)
	}
}
