package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"relayx/s4/protocol"
)

type sinkRecorder struct {
	mu     sync.Mutex
	frames []protocol.Frame
}

func (s *sinkRecorder) EnqueueReliable(_ context.Context, f protocol.Frame) error {
	s.mu.Lock()
	s.frames = append(s.frames, f)
	s.mu.Unlock()
	return nil
}

func (s *sinkRecorder) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.frames)
}

type onlineResolver struct {
	mu     sync.Mutex
	sinks  map[string]*sinkRecorder
	online map[string]bool
}

func newResolver() *onlineResolver {
	return &onlineResolver{sinks: make(map[string]*sinkRecorder), online: make(map[string]bool)}
}

func (r *onlineResolver) add(tenant, user, device string) *sinkRecorder {
	s := &sinkRecorder{}
	r.mu.Lock()
	key := tenant + "/" + user + "/" + device
	r.sinks[key] = s
	r.online[key] = true
	r.mu.Unlock()
	return s
}

func (r *onlineResolver) setOnline(tenant, user, device string, on bool) {
	r.mu.Lock()
	r.online[tenant+"/"+user+"/"+device] = on
	r.mu.Unlock()
}

func (r *onlineResolver) Lookup(tenant, user, device string) (Sink, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := tenant + "/" + user + "/" + device
	if !r.online[key] {
		return nil, false
	}
	s := r.sinks[key]
	if s == nil {
		return nil, false
	}
	return s, true
}

func testOptions() Options {
	return Options{
		MaxPayload: 1024, MaxTTL: 3600, MaxAttempts: 5,
		DefaultTTL: 60, DefaultMaxAttempts: 3,
	}
}

func input(target string) Input {
	return Input{
		TenantID: "t1", ProducerID: "u:alice", ProducerKind: ProducerUser,
		Sender: "alice", SourceDeviceID: "dev-a",
		ClientMsgID: "cm-1", TargetUserID: target,
		Payload: []byte("hello"), Policy: Policy{},
	}
}

func TestSubmitZeroEndpointsSettlesImmediately(t *testing.T) {
	k := New(testOptions(), nil)
	res, err := k.Submit(context.Background(), input("ghost"))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if res.State != MessageSettled {
		t.Fatalf("zero-endpoint message must settle at accept, got %s", res.State)
	}
	if res.Counts.Total != 0 {
		t.Fatalf("counts.total=%d, want 0", res.Counts.Total)
	}
	facts := k.ListFactsByMessage(res.MessageID)
	var settled *Fact
	for i := range facts {
		if facts[i].FactType == FactMessageSettled {
			settled = &facts[i]
		}
	}
	if settled == nil {
		t.Fatal("message.settled fact missing")
	}
	if got := settled.PayloadSummary["source_device_id"]; got != "dev-a" {
		t.Fatalf("fact source_device_id=%v, want dev-a", got)
	}
	if got := settled.PayloadSummary["producer_id"]; got != "u:alice" {
		t.Fatalf("fact producer_id=%v", got)
	}
}

func TestIdempotency(t *testing.T) {
	k := New(testOptions(), nil)
	first, err := k.Submit(context.Background(), input("ghost"))
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}
	dup, err := k.Submit(context.Background(), input("ghost"))
	if err != nil {
		t.Fatalf("duplicate submit: %v", err)
	}
	if !dup.Duplicate || dup.MessageID != first.MessageID {
		t.Fatalf("expected idempotent replay, got %+v", dup)
	}
	bad := input("ghost")
	bad.Payload = []byte("different")
	if _, err := k.Submit(context.Background(), bad); !errors.Is(err, ErrDuplicateMismatch) {
		t.Fatalf("expected ErrDuplicateMismatch, got %v", err)
	}
}

func TestReceiptSettlesAndCountsMatch(t *testing.T) {
	k := New(testOptions(), nil)
	resolver := newResolver()
	sink := resolver.add("t1", "bob", "dev-b")
	k.SetOnlineResolver(resolver)
	k.MarkOnline("t1", "bob", "dev-b", "s1")

	res, err := k.Submit(context.Background(), input("bob"))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if res.State != MessageAccepted {
		t.Fatalf("state=%s, want ACCEPTED", res.State)
	}
	if err := k.Dispatch(context.Background(), res.MessageID); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if sink.count() != 1 {
		t.Fatalf("deliver frames=%d, want 1", sink.count())
	}
	if err := k.ReceiptAck(context.Background(), "t1", res.MessageID, "bob", "dev-b"); err != nil {
		t.Fatalf("receipt: %v", err)
	}
	m, err := k.GetMessage(res.MessageID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if m.PlatformState != MessageSettled {
		t.Fatalf("state=%s, want SETTLED", m.PlatformState)
	}

	want := protocol.DeliveryCounts{Total: 1, ReceiptAcked: 1}
	if got := k.Counts(res.MessageID); got != want {
		t.Fatalf("recomputed counts=%+v, want %+v", got, want)
	}
	facts := k.ListFactsByMessage(res.MessageID)
	var settled *Fact
	for i := range facts {
		if facts[i].FactType == FactMessageSettled {
			settled = &facts[i]
		}
	}
	if settled == nil {
		t.Fatal("message.settled fact missing")
	}
	fromSummary, ok := countsFromSummary(settled.PayloadSummary)
	if !ok {
		t.Fatal("settled fact missing counts summary")
	}
	if fromSummary != want {
		t.Fatalf("payload_summary counts=%+v, recomputed=%+v (双源必须一致)", fromSummary, want)
	}
	// 重复 ACK 不得重复结算或重复写事实。
	before := len(k.ListFactsByMessage(res.MessageID))
	if err := k.ReceiptAck(context.Background(), "t1", res.MessageID, "bob", "dev-b"); err != nil {
		t.Fatalf("dup receipt: %v", err)
	}
	if after := len(k.ListFactsByMessage(res.MessageID)); after != before {
		t.Fatalf("duplicate ACK wrote facts: %d -> %d", before, after)
	}
}

func TestOfflineEndpointStaysPending(t *testing.T) {
	k := New(testOptions(), nil)
	resolver := newResolver()
	resolver.add("t1", "bob", "dev-b")
	resolver.setOnline("t1", "bob", "dev-b", false)
	k.SetOnlineResolver(resolver)
	k.MarkOnline("t1", "bob", "dev-b", "s1")
	k.MarkOffline("t1", "bob", "dev-b", "s1")

	res, err := k.Submit(context.Background(), input("bob"))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if res.State != MessageAccepted {
		t.Fatalf("offline target must stay ACCEPTED, got %s", res.State)
	}
	if err := k.Dispatch(context.Background(), res.MessageID); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	ds := k.ListDeliveries(res.MessageID)
	if len(ds) != 1 || ds[0].DeliveryState != DeliveryPending {
		t.Fatalf("offline delivery must stay PENDING, got %+v", ds)
	}
	if ids := k.PendingMessagesFor("t1", "bob", "dev-b"); len(ids) != 1 || ids[0] != res.MessageID {
		t.Fatalf("pending messages=%v", ids)
	}
}

func TestExpiryWithSkewClock(t *testing.T) {
	clock := NewSkewClock()
	k := New(testOptions(), clock)
	resolver := newResolver()
	resolver.add("t1", "carol", "dev-c")
	resolver.setOnline("t1", "carol", "dev-c", false)
	k.SetOnlineResolver(resolver)
	k.MarkOnline("t1", "carol", "dev-c", "s1")
	k.MarkOffline("t1", "carol", "dev-c", "s1") // 目录保留，仅在离线

	in := input("carol")
	in.Policy = Policy{TTLSeconds: 1, MaxAttempts: 1}
	res, err := k.Submit(context.Background(), in)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if res.State != MessageAccepted {
		t.Fatalf("state=%s", res.State)
	}
	clock.SetOffset(2 * time.Second)
	k.ExpireDue()
	m, err := k.GetMessage(res.MessageID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if m.PlatformState != MessageExpired {
		t.Fatalf("state=%s, want EXPIRED", m.PlatformState)
	}
	var expired *Fact
	for _, f := range k.ListFactsByMessage(res.MessageID) {
		if f.FactType == FactMessageExpired {
			ff := f
			expired = &ff
		}
	}
	if expired == nil {
		t.Fatal("message.expired fact missing")
	}
	if got := k.Counts(res.MessageID); got.Expired != 1 {
		t.Fatalf("expired counts=%+v", got)
	}
}

func TestListFactsAfterKeySetPaging(t *testing.T) {
	// 固定时钟 → 所有事实 occurred_at 相同，键集分页只能依赖 event_id，
	// 这是最严格的"不重不漏"场景。
	clock := NewSkewClock()
	k := New(testOptions(), clock)
	const n = 12
	for i := 0; i < n; i++ {
		in := input("ghost")
		in.ClientMsgID = "page-" + string(rune('a'+i))
		if _, err := k.Submit(context.Background(), in); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	types := []string{FactMessageAccepted, FactMessageSettled}
	seen := map[string]int{}
	var at time.Time
	id := ""
	for page := 0; ; page++ {
		rows := k.ListFactsAfter(at, id, types, 3)
		if len(rows) == 0 {
			break
		}
		if len(rows) > 3 {
			t.Fatalf("page size %d > 3", len(rows))
		}
		for _, f := range rows {
			seen[f.EventID]++
		}
		last := rows[len(rows)-1]
		at, id = last.OccurredAt, last.EventID
		if page > 100 {
			t.Fatal("paging did not terminate")
		}
	}
	if len(seen) != n*2 {
		t.Fatalf("seen %d distinct facts, want %d", len(seen), n*2)
	}
	for id, cnt := range seen {
		if cnt != 1 {
			t.Fatalf("fact %s returned %d times", id, cnt)
		}
	}
}

func TestEventIDDeterministic(t *testing.T) {
	ep := "bob:dev-b"
	a := DeriveEventID("t1", "msg-1", FactDeliveryReceiptAcked, &ep, 2)
	b := DeriveEventID("t1", "msg-1", FactDeliveryReceiptAcked, &ep, 2)
	if a != b {
		t.Fatal("event_id must be deterministic")
	}
	if a == DeriveEventID("t1", "msg-1", FactDeliveryReceiptAcked, &ep, 3) {
		t.Fatal("state_version must change event_id")
	}
	if a == DeriveEventID("t2", "msg-1", FactDeliveryReceiptAcked, &ep, 2) {
		t.Fatal("tenant must change event_id")
	}
}

// countsFromSummary 是本包内的双源比对辅助（投影层有等价实现）。
func countsFromSummary(summary map[string]any) (protocol.DeliveryCounts, bool) {
	v, ok := summary["counts"]
	if !ok {
		return protocol.DeliveryCounts{}, false
	}
	m, ok := v.(map[string]any)
	if !ok {
		return protocol.DeliveryCounts{}, false
	}
	toInt := func(x any) int {
		switch n := x.(type) {
		case int:
			return n
		case int64:
			return int(n)
		case float64:
			return int(n)
		default:
			return 0
		}
	}
	return protocol.DeliveryCounts{
		Total: toInt(m["total"]), ReceiptAcked: toInt(m["receipt_acked"]),
		Failed: toInt(m["failed"]), Expired: toInt(m["expired"]),
	}, true
}
