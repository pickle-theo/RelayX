package broadcast

import (
	"errors"
	"testing"
	"time"

	"relayx/s4/internal/sendpath"
)

type fakeSender struct{}

func (fakeSender) TrySendBroadcast(*sendpath.ImmutableFrame) error { return nil }

func testIndex() *Index {
	return NewIndex(IndexLimits{
		MaxTopicsPerSession: 10, MaxSubscribersPerTopic: 100,
		MaxTopicBytes: 256, MaxGlobalSubscriptions: 1000,
	})
}

func claims(tenant, topic string) SubscriptionClaims {
	return SubscriptionClaims{TenantID: tenant, UserID: "u", Topic: topic, ExpiresAt: time.Now().Add(time.Hour)}
}

func factory(sessID, topic string, version uint64) P2Sender { return fakeSender{} }

func TestIndexBidirectionalConsistency(t *testing.T) {
	idx := testIndex()
	if created, err := idx.Subscribe("s1", claims("t1", "a"), factory); err != nil || !created {
		t.Fatalf("subscribe a: created=%v err=%v", created, err)
	}
	if created, err := idx.Subscribe("s1", claims("t1", "b"), factory); err != nil || !created {
		t.Fatalf("subscribe b: created=%v err=%v", created, err)
	}
	if created, err := idx.Subscribe("s2", claims("t1", "a"), factory); err != nil || !created {
		t.Fatalf("subscribe s2/a: created=%v err=%v", created, err)
	}
	if created, err := idx.Subscribe("s1", claims("t1", "a"), factory); err != nil || created {
		t.Fatalf("duplicate: created=%v err=%v", created, err)
	}
	if got := len(idx.Snapshot("t1", "a")); got != 2 {
		t.Fatalf("snapshot(a)=%d want 2", got)
	}
	if got := idx.Stats().Subscriptions; got != 3 {
		t.Fatalf("subscriptions=%d want 3", got)
	}

	if ok, err := idx.Unsubscribe("s1", "a"); err != nil || !ok {
		t.Fatalf("unsubscribe: ok=%v err=%v", ok, err)
	}
	if got := len(idx.Snapshot("t1", "a")); got != 1 {
		t.Fatalf("snapshot(a) after unsub=%d want 1", got)
	}
	if _, err := idx.Unsubscribe("s1", "a"); !errors.Is(err, ErrNotSubscribed) {
		t.Fatalf("double unsubscribe err=%v want ErrNotSubscribed", err)
	}
}

func TestIndexSnapshotIsolation(t *testing.T) {
	idx := testIndex()
	_, _ = idx.Subscribe("s1", claims("t1", "a"), factory)
	snap := idx.Snapshot("t1", "a")
	if len(snap) != 1 {
		t.Fatalf("snap=%d", len(snap))
	}
	oldVersion := snap[0].Version
	_, _ = idx.Unsubscribe("s1", "a")
	if len(snap) != 1 || snap[0].Version != oldVersion {
		t.Fatal("snapshot must be an isolated copy")
	}
	if idx.VersionMatches("s1", "a", oldVersion) {
		t.Fatal("version must not match after unsubscribe")
	}
}

func TestIndexRemoveSession(t *testing.T) {
	idx := testIndex()
	for _, topic := range []string{"a", "b", "c"} {
		_, _ = idx.Subscribe("s1", claims("t1", topic), factory)
	}
	_, _ = idx.Subscribe("s2", claims("t1", "a"), factory)
	topics := idx.RemoveSession("s1")
	if len(topics) != 3 {
		t.Fatalf("removed topics=%v want 3", topics)
	}
	if got := idx.Stats().Subscriptions; got != 1 {
		t.Fatalf("subscriptions=%d want 1", got)
	}
	if got := len(idx.Snapshot("t1", "a")); got != 1 {
		t.Fatalf("snapshot(a)=%d want 1", got)
	}
	if got := len(idx.Snapshot("t1", "b")); got != 0 {
		t.Fatalf("snapshot(b)=%d want 0", got)
	}
	if idx.Stats().Topics != 1 {
		t.Fatalf("active topics=%d want 1", idx.Stats().Topics)
	}
}

// TestIndexTenantIsolation 覆盖租户隔离：同 topic 名在不同租户下互不可见，
// 防止广播把某个租户的 payload 投给其他租户的订阅者。
func TestIndexTenantIsolation(t *testing.T) {
	idx := testIndex()
	if _, err := idx.Subscribe("s1", claims("t1", "room"), factory); err != nil {
		t.Fatalf("subscribe t1: %v", err)
	}
	if _, err := idx.Subscribe("s2", claims("t2", "room"), factory); err != nil {
		t.Fatalf("subscribe t2: %v", err)
	}
	if got := len(idx.Snapshot("t1", "room")); got != 1 {
		t.Fatalf("snapshot(t1,room)=%d want 1", got)
	}
	if got := len(idx.Snapshot("t2", "room")); got != 1 {
		t.Fatalf("snapshot(t2,room)=%d want 1", got)
	}
	if got := len(idx.Snapshot("t3", "room")); got != 0 {
		t.Fatalf("snapshot(t3,room)=%d want 0", got)
	}
	if ok, err := idx.Unsubscribe("s1", "room"); err != nil || !ok {
		t.Fatalf("unsubscribe: ok=%v err=%v", ok, err)
	}
	if got := len(idx.Snapshot("t2", "room")); got != 1 {
		t.Fatalf("t2 snapshot after t1 unsubscribe=%d want 1", got)
	}
	if got := idx.Stats().Subscriptions; got != 1 {
		t.Fatalf("subscriptions=%d want 1", got)
	}
}

func TestIndexLimits(t *testing.T) {
	idx := NewIndex(IndexLimits{MaxTopicsPerSession: 1, MaxSubscribersPerTopic: 1, MaxTopicBytes: 8, MaxGlobalSubscriptions: 1})
	if _, err := idx.Subscribe("s1", claims("t1", "long-topic-name"), factory); !errors.Is(err, ErrBadTopic) {
		t.Fatalf("topic length err=%v", err)
	}
	_, _ = idx.Subscribe("s1", claims("t1", "a"), factory)
	if _, err := idx.Subscribe("s1", claims("t1", "b"), factory); !errors.Is(err, ErrSessionFull) {
		t.Fatalf("session limit err=%v", err)
	}
	if _, err := idx.Subscribe("s2", claims("t1", "a"), factory); !errors.Is(err, ErrTopicFull) {
		t.Fatalf("topic limit err=%v", err)
	}
	if _, err := idx.Subscribe("s3", claims("t1", "c"), factory); !errors.Is(err, ErrIndexFull) {
		t.Fatalf("global limit err=%v", err)
	}
	if idx.Stats().FullEvents != 3 {
		t.Fatalf("full events=%d want 3", idx.Stats().FullEvents)
	}
}
