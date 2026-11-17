package projector

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"relayx/s4/internal/core"
	"relayx/s4/internal/sendpath"
	"relayx/s4/protocol"
)

type fakeMessages struct {
	producer, device string
	ok               bool
}

func (f fakeMessages) Route(string) (string, string, bool) {
	return f.producer, f.device, f.ok
}

type recordingRouter struct {
	mu     sync.Mutex
	events []ClientEvent
	err    error
}

func (r *recordingRouter) Route(_ context.Context, ev ClientEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.events = append(r.events, ev)
	return nil
}

func (r *recordingRouter) snapshot() []ClientEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]ClientEvent, len(r.events))
	copy(out, r.events)
	return out
}

// staticSource 只用于单条 Project 测试；分页行为由真实内核覆盖。
type staticSource struct{}

func (staticSource) ListFactsAfter(time.Time, string, []string, int) []core.Fact { return nil }

func settledFact() core.Fact {
	return core.Fact{
		EventID: "ev-1", TenantID: "t1", MessageID: "msg-1",
		FactType: core.FactMessageSettled, StateVersion: 2,
		OccurredAt: time.Unix(1700000000, 0).UTC(),
		PayloadSummary: map[string]any{
			"producer_id":      "u:alice",
			"source_device_id": "dev-a",
			"counts":           map[string]any{"total": 2, "receipt_acked": 1, "failed": 1, "expired": 0},
		},
	}
}

func TestProjectSettledHasSeqZeroAndCounts(t *testing.T) {
	p := New(Options{Source: &staticSource{}, Router: &recordingRouter{}, Cursor: NewMemoryCursor()})
	events, err := p.Project(settledFact())
	if err != nil {
		t.Fatalf("project: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events=%d", len(events))
	}
	ev := events[0]
	if ev.Seq != 0 {
		t.Fatalf("projection must not allocate seq, got %d", ev.Seq)
	}
	if ev.ProducerID != "u:alice" || ev.SourceDeviceID != "dev-a" {
		t.Fatalf("route key=%s/%s", ev.ProducerID, ev.SourceDeviceID)
	}
	if ev.Counts == nil || *ev.Counts != (protocol.DeliveryCounts{Total: 2, ReceiptAcked: 1, Failed: 1}) {
		t.Fatalf("counts=%+v", ev.Counts)
	}
	if ev.Type != protocol.EvMessageSettled {
		t.Fatalf("type=%s", ev.Type)
	}
}

func TestProjectFallbackToMessageSourceDevice(t *testing.T) {
	f := settledFact()
	f.PayloadSummary = map[string]any{"counts": map[string]any{"total": 1}}
	p := New(Options{
		Source:   &staticSource{},
		Messages: fakeMessages{producer: "u:bob", device: "dev-b", ok: true},
		Router:   &recordingRouter{},
		Cursor:   NewMemoryCursor(),
	})
	events, err := p.Project(f)
	if err != nil || len(events) != 1 {
		t.Fatalf("project: %v %d", err, len(events))
	}
	if events[0].SourceDeviceID != "dev-b" || events[0].ProducerID != "u:bob" {
		t.Fatalf("fallback route key=%s/%s", events[0].ProducerID, events[0].SourceDeviceID)
	}
}

func TestProjectIgnoresNonTerminalFacts(t *testing.T) {
	p := New(Options{Source: &staticSource{}, Router: &recordingRouter{}, Cursor: NewMemoryCursor()})
	f := settledFact()
	f.FactType = core.FactMessageAccepted
	events, err := p.Project(f)
	if err != nil {
		t.Fatalf("project: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("accepted fact must not produce client events, got %d", len(events))
	}
}

func TestProjectDeliveryEvent(t *testing.T) {
	p := New(Options{Source: &staticSource{}, Router: &recordingRouter{}, Cursor: NewMemoryCursor()})
	f := settledFact()
	f.FactType = core.FactDeliveryReceiptAcked
	events, err := p.Project(f)
	if err != nil || len(events) != 1 {
		t.Fatalf("project: %v %d", err, len(events))
	}
	if events[0].Type != protocol.EvDeliveryReceiptAcked || events[0].Counts != nil {
		t.Fatalf("unexpected event: %+v", events[0])
	}
}

func TestRunOnceAdvancesCursorOnlyAfterRoute(t *testing.T) {
	k := core.New(core.Options{
		MaxPayload: 1024, MaxTTL: 3600, MaxAttempts: 5, DefaultTTL: 60, DefaultMaxAttempts: 3,
	}, nil)
	for i := 0; i < 3; i++ {
		if _, err := k.Submit(context.Background(), core.Input{
			TenantID: "t1", ProducerID: "u:alice", ProducerKind: core.ProducerUser,
			SourceDeviceID: "dev-a", ClientMsgID: "c" + string(rune('a'+i)),
			TargetUserID: "ghost", Payload: []byte("x"),
		}); err != nil {
			t.Fatalf("submit: %v", err)
		}
	}
	cursor := NewMemoryCursor()
	router := &recordingRouter{}
	p := New(Options{Source: k, Messages: k, Router: router, Cursor: cursor, Batch: 2, Interval: time.Millisecond})

	total := 0
	for i := 0; i < 10; i++ {
		n, err := p.RunOnce(context.Background())
		if err != nil {
			t.Fatalf("run once: %v", err)
		}
		total += n
		if n == 0 {
			break
		}
	}
	if total != 3 {
		t.Fatalf("projected facts=%d, want 3 (default types only include settled/expired)", total)
	}
	cur, _ := cursor.Load(context.Background(), ProjectorName)
	if cur.EventID == "" {
		t.Fatal("cursor not advanced")
	}
	// 每个 message.settled 恰好一次。
	seen := map[string]int{}
	for _, ev := range router.snapshot() {
		if ev.Type == protocol.EvMessageSettled {
			seen[ev.MessageID]++
		}
	}
	if len(seen) != 3 {
		t.Fatalf("settled messages=%d, want 3", len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("message %s projected %d times", id, n)
		}
	}
}

func TestRunOnceDoesNotAdvanceCursorOnRouteError(t *testing.T) {
	k := core.New(core.Options{MaxPayload: 1024, MaxTTL: 3600, MaxAttempts: 5, DefaultTTL: 60, DefaultMaxAttempts: 3}, nil)
	if _, err := k.Submit(context.Background(), core.Input{
		TenantID: "t1", ProducerID: "u:alice", ProducerKind: core.ProducerUser,
		SourceDeviceID: "dev-a", ClientMsgID: "c1", TargetUserID: "ghost", Payload: []byte("x"),
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	cursor := NewMemoryCursor()
	router := &recordingRouter{err: errors.New("boom")}
	p := New(Options{Source: k, Messages: k, Router: router, Cursor: cursor, Batch: 10})
	if _, err := p.RunOnce(context.Background()); err == nil {
		t.Fatal("expected route error to fail the batch")
	}
	cur, _ := cursor.Load(context.Background(), ProjectorName)
	if !cur.At.IsZero() || cur.EventID != "" {
		t.Fatalf("cursor must not advance on failure, got %+v", cur)
	}
}

// ---------- Router ----------

type fakeSink struct {
	id     string
	mask   protocol.EventMask
	err    error
	mu     sync.Mutex
	frames []protocol.Frame
}

func (s *fakeSink) SessionID() string             { return s.id }
func (s *fakeSink) EventMask() protocol.EventMask { return s.mask }
func (s *fakeSink) EnqueueClientEvent(f protocol.Frame) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.frames = append(s.frames, f)
	return nil
}

type fakeLocator map[string]EventSink

func (l fakeLocator) Locate(tenant, producer, device string) (EventSink, bool) {
	s, ok := l[tenant+"/"+producer+"/"+device]
	return s, ok
}

func routeEvent(msgID string) ClientEvent {
	return ClientEvent{
		EventID: "ev-" + msgID, Type: protocol.EvMessageSettled, TenantID: "t1",
		MessageID: msgID, StateVersion: 1, ProducerID: "u:alice",
		SourceDeviceID: "dev-a", OccurredAt: time.Now().UTC(),
		Counts: &protocol.DeliveryCounts{Total: 1, ReceiptAcked: 1},
	}
}

func TestRouterPerSessionSeqIndependent(t *testing.T) {
	sinkA := &fakeSink{id: "sa", mask: protocol.EventMaskDefault}
	sinkB := &fakeSink{id: "sb", mask: protocol.EventMaskDefault}
	loc := fakeLocator{
		"t1/u:alice/dev-a": sinkA,
		"t1/u:bob/dev-b":   sinkB,
	}
	metrics := NewMetrics(nil)
	r := NewRouter(loc, NewSeqAllocator(), metrics, nil)
	for i := 0; i < 3; i++ {
		if err := r.Route(context.Background(), routeEvent("a"+string(rune('1'+i)))); err != nil {
			t.Fatalf("route: %v", err)
		}
	}
	ev := routeEvent("b1")
	ev.ProducerID = "u:bob"
	ev.SourceDeviceID = "dev-b"
	if err := r.Route(context.Background(), ev); err != nil {
		t.Fatalf("route b: %v", err)
	}

	assertSeq := func(s *fakeSink, want []uint64) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if len(s.frames) != len(want) {
			t.Fatalf("%s frames=%d", s.id, len(s.frames))
		}
		for i, f := range s.frames {
			body, err := protocol.DecodeClientEvent(f.Body)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if body.Seq != want[i] {
				t.Fatalf("%s frame %d seq=%d, want %d", s.id, i, body.Seq, want[i])
			}
		}
	}
	assertSeq(sinkA, []uint64{1, 2, 3})
	assertSeq(sinkB, []uint64{1})
}

func TestRouterNoRouteWhenSourceDeviceEmpty(t *testing.T) {
	metrics := NewMetrics(nil)
	r := NewRouter(fakeLocator{}, NewSeqAllocator(), metrics, nil)
	ev := routeEvent("m")
	ev.SourceDeviceID = ""
	if err := r.Route(context.Background(), ev); err != nil {
		t.Fatalf("route: %v", err)
	}
	if !strings.Contains(metrics.Render(), "client_event_no_route_total 1") {
		t.Fatalf("no_route metric missing: %s", metrics.Render())
	}
	if !strings.Contains(metrics.Render(), "client_event_dropped_total{reason=\"offline\"} 0") {
		t.Fatal("empty source must not be counted as offline drop")
	}
}

func TestRouterOfflineAndMaskAndBudget(t *testing.T) {
	t.Run("offline", func(t *testing.T) {
		metrics := NewMetrics(nil)
		r := NewRouter(fakeLocator{}, NewSeqAllocator(), metrics, nil)
		if err := r.Route(context.Background(), routeEvent("m")); err != nil {
			t.Fatalf("route: %v", err)
		}
		if !strings.Contains(metrics.Render(), "client_event_route_failed_total 1") {
			t.Fatal("route_failed metric missing")
		}
	})
	t.Run("not_subscribed", func(t *testing.T) {
		sink := &fakeSink{id: "s", mask: 0}
		metrics := NewMetrics(nil)
		r := NewRouter(fakeLocator{"t1/u:alice/dev-a": sink}, NewSeqAllocator(), metrics, nil)
		if err := r.Route(context.Background(), routeEvent("m")); err != nil {
			t.Fatalf("route: %v", err)
		}
		if len(sink.frames) != 0 {
			t.Fatal("unsubscribed session must not receive events")
		}
		if !strings.Contains(metrics.Render(), "client_event_dropped_total{reason=\"not_subscribed\"} 1") {
			t.Fatalf("metric missing: %s", metrics.Render())
		}
	})
	t.Run("queue_full", func(t *testing.T) {
		sink := &fakeSink{id: "s", mask: protocol.EventMaskDefault, err: sendpath.ErrBudgetExhausted}
		metrics := NewMetrics(nil)
		r := NewRouter(fakeLocator{"t1/u:alice/dev-a": sink}, NewSeqAllocator(), metrics, nil)
		if err := r.Route(context.Background(), routeEvent("m")); err != nil {
			t.Fatalf("queue-full must not fail the batch: %v", err)
		}
		if !strings.Contains(metrics.Render(), "client_event_dropped_total{reason=\"queue_full\"} 1") {
			t.Fatalf("metric missing: %s", metrics.Render())
		}
	})
}

func TestRouterInjectedSkipKeepsSeqGap(t *testing.T) {
	sink := &fakeSink{id: "s", mask: protocol.EventMaskDefault}
	metrics := NewMetrics(nil)
	r := NewRouter(fakeLocator{"t1/u:alice/dev-a": sink}, NewSeqAllocator(), metrics, nil)
	r.SkipNext(1)
	if err := r.Route(context.Background(), routeEvent("m1")); err != nil {
		t.Fatalf("route: %v", err)
	}
	if err := r.Route(context.Background(), routeEvent("m2")); err != nil {
		t.Fatalf("route: %v", err)
	}
	if len(sink.frames) != 1 {
		t.Fatalf("frames=%d, want 1 (first injected drop)", len(sink.frames))
	}
	body, err := protocol.DecodeClientEvent(sink.frames[0].Body)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Seq != 2 {
		t.Fatalf("seq=%d, want 2 (dropped event still consumed seq 1)", body.Seq)
	}
}
