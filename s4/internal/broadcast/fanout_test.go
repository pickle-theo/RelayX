package broadcast

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"relayx/s4/internal/sendpath"
	"relayx/s4/protocol"
)

type countingEncoder struct {
	inner *Encoder
	calls atomic.Int64
}

func (e *countingEncoder) Encode(topic string, payload []byte, at time.Time, seq uint64) (*sendpath.ImmutableFrame, error) {
	e.calls.Add(1)
	return e.inner.Encode(topic, payload, at, seq)
}

type recordingSender struct {
	mu       sync.Mutex
	seqs     []uint64
	payloads []string
}

func (s *recordingSender) TrySendBroadcast(f *sendpath.ImmutableFrame) error {
	var buf bytes.Buffer
	if _, err := f.WriteTo(&buf); err != nil {
		return err
	}
	fr, _, err := protocol.Decode(buf.Bytes())
	if err != nil {
		return err
	}
	bb, err := protocol.DecodeBroadcast(fr.Body)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.seqs = append(s.seqs, bb.Seq)
	s.payloads = append(s.payloads, string(bb.Payload))
	s.mu.Unlock()
	return nil
}

func (s *recordingSender) snapshot() ([]uint64, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.seqs...), append([]string(nil), s.payloads...)
}

func addSubscriber(t *testing.T, idx *Index, sessID, topic string, sender P2Sender) {
	t.Helper()
	_, err := idx.Subscribe(sessID, claims("t1", topic), func(string, string, uint64) P2Sender { return sender })
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
}

func TestFanoutEncodesOnceForAllSubscribers(t *testing.T) {
	idx := NewIndex(IndexLimits{MaxTopicsPerSession: 100, MaxSubscribersPerTopic: 1000, MaxGlobalSubscriptions: 10000})
	enc := &countingEncoder{inner: NewSharedEncoder(DefaultMaxPayload)}
	fan := NewFanout(idx, enc, &Metrics{})
	const n = 8
	for i := 0; i < n; i++ {
		addSubscriber(t, idx, fmt.Sprintf("s%d", i), "topic", &recordingSender{})
	}
	res := fan.Fanout(context.Background(), BroadcastInput{TenantID: "t1", Topic: "topic", Payload: []byte("hello")})
	if got := enc.calls.Load(); got != 1 {
		t.Fatalf("Encode called %d times want 1", got)
	}
	if res.Targets != n || res.Enqueued != n || res.Dropped != 0 {
		t.Fatalf("result=%+v", res)
	}
	if res.EncodedBytes == 0 {
		t.Fatal("EncodedBytes must be reported")
	}
}

func TestFanoutSeqMonotonicUnderConcurrency(t *testing.T) {
	idx := NewIndex(IndexLimits{MaxTopicsPerSession: 100, MaxSubscribersPerTopic: 1000, MaxGlobalSubscriptions: 10000})
	fan := NewFanout(idx, NewSharedEncoder(DefaultMaxPayload), &Metrics{})
	rec := &recordingSender{}
	addSubscriber(t, idx, "s1", "ordered", rec)

	const n = 100
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			fan.Fanout(context.Background(), BroadcastInput{TenantID: "t1", Topic: "ordered", Payload: []byte(fmt.Sprintf("m-%03d", i))})
		}(i)
	}
	wg.Wait()

	seqs, payloads := rec.snapshot()
	if len(seqs) != n {
		t.Fatalf("received %d frames want %d", len(seqs), n)
	}
	for i, seq := range seqs {
		if seq != uint64(i+1) {
			t.Fatalf("seq[%d]=%d want %d (out of order)", i, seq, i+1)
		}
	}
	// 并发 publish 下 seq 顺序 ≠ goroutine 编号；只断言 payload 集合完整且唯一。
	seen := make(map[string]bool, n)
	for _, p := range payloads {
		if seen[p] {
			t.Fatalf("duplicate payload %q", p)
		}
		seen[p] = true
	}
	for i := 0; i < n; i++ {
		if !seen[fmt.Sprintf("m-%03d", i)] {
			t.Fatalf("missing payload m-%03d", i)
		}
	}
}

func TestFanoutSharedBufferIntegrity(t *testing.T) {
	idx := NewIndex(IndexLimits{MaxTopicsPerSession: 100, MaxSubscribersPerTopic: 1000, MaxGlobalSubscriptions: 10000})
	fan := NewFanout(idx, NewSharedEncoder(DefaultMaxPayload), &Metrics{})
	r1 := &recordingSender{}
	r2 := &recordingSender{}
	addSubscriber(t, idx, "s1", "integrity", r1)
	addSubscriber(t, idx, "s2", "integrity", r2)

	const n = 60
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			fan.Fanout(context.Background(), BroadcastInput{
				TenantID: "t1", Topic: "integrity", Payload: []byte(fmt.Sprintf("payload-%03d", i)),
			})
		}(i)
	}
	wg.Wait()

	s1, p1 := r1.snapshot()
	s2, p2 := r2.snapshot()
	m1 := map[uint64]string{}
	for i, seq := range s1 {
		m1[seq] = p1[i]
	}
	if len(m1) != n {
		t.Fatalf("distinct seq=%d want %d", len(m1), n)
	}
	for i, seq := range s2 {
		if m1[seq] != p2[i] {
			t.Fatalf("cross-subscriber mismatch at seq=%d: %q vs %q", seq, m1[seq], p2[i])
		}
	}
}

// TestFanoutTenantIsolation 断言扇出只落在发布者自己的租户内。
func TestFanoutTenantIsolation(t *testing.T) {
	idx := NewIndex(IndexLimits{MaxTopicsPerSession: 10, MaxSubscribersPerTopic: 10, MaxGlobalSubscriptions: 10})
	r1 := &recordingSender{}
	r2 := &recordingSender{}
	_, _ = idx.Subscribe("s1", claims("t1", "room"), func(string, string, uint64) P2Sender { return r1 })
	_, _ = idx.Subscribe("s2", claims("t2", "room"), func(string, string, uint64) P2Sender { return r2 })
	fan := NewFanout(idx, NewSharedEncoder(DefaultMaxPayload), &Metrics{})

	fan.Fanout(context.Background(), BroadcastInput{TenantID: "t1", Topic: "room", Payload: []byte("secret")})
	s1, _ := r1.snapshot()
	s2, _ := r2.snapshot()
	if len(s1) != 1 {
		t.Fatalf("t1 subscriber got %d frames want 1", len(s1))
	}
	if len(s2) != 0 {
		t.Fatalf("t2 subscriber leaked %d frames from t1", len(s2))
	}
}

func TestFanoutNoSubscribersDoesNotEncode(t *testing.T) {
	idx := NewIndex(IndexLimits{MaxTopicsPerSession: 10, MaxSubscribersPerTopic: 10, MaxTopicBytes: 256, MaxGlobalSubscriptions: 10})
	enc := &countingEncoder{inner: NewSharedEncoder(DefaultMaxPayload)}
	fan := NewFanout(idx, enc, &Metrics{})
	res := fan.Fanout(context.Background(), BroadcastInput{TenantID: "t1", Topic: "empty", Payload: []byte("x")})
	if enc.calls.Load() != 0 {
		t.Fatal("no subscribers must not encode (B8)")
	}
	if res.Targets != 0 || res.EncodedBytes != 0 {
		t.Fatalf("result=%+v", res)
	}
	if res.Seq != 1 {
		t.Fatalf("seq=%d want 1", res.Seq)
	}
}

func TestFanoutRejectsOversizedPayload(t *testing.T) {
	idx := NewIndex(IndexLimits{MaxTopicsPerSession: 10, MaxSubscribersPerTopic: 10, MaxGlobalSubscriptions: 10})
	fan := NewFanout(idx, NewSharedEncoder(8), &Metrics{})
	addSubscriber(t, idx, "s1", "big", &recordingSender{})
	res := fan.Fanout(context.Background(), BroadcastInput{TenantID: "t1", Topic: "big", Payload: []byte("0123456789")})
	if res.Enqueued != 0 {
		t.Fatalf("oversized payload was enqueued: %+v", res)
	}
}
