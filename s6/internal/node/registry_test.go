package node

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"relayx/s6/internal/backend"
	"relayx/s6/internal/clock"
	"relayx/s6/internal/model"
)

var (
	epA = model.EndpointKey{TenantID: "t1", UserID: "u1", DeviceID: "d1"}
	epB = model.EndpointKey{TenantID: "t1", UserID: "u2", DeviceID: "d2"}
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newTestRegistry(b backend.Backend, clk clock.Clock, selfID string) *Registry {
	return NewRegistry(Options{
		Backend: b, Clock: clk,
		Self: model.NodeInfo{NodeID: selfID, Addr: "127.0.0.1:9803"},
		TTL:  10 * time.Second, SuspectAfter: 5 * time.Second,
		Log: testLogger(),
	})
}

func registerEndpoint(t *testing.T, b backend.Backend, nodeID string, ep model.EndpointKey) {
	t.Helper()
	ctx := context.Background()
	if err := b.AddEndpointIndex(ctx, nodeID, ep); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Bind(ctx, ep, nodeID, time.Unix(1000, 0)); err != nil {
		t.Fatal(err)
	}
}

func TestReclaimDeadDeletesOwnBindings(t *testing.T) {
	b := backend.NewMemoryBackend()
	clk := clock.NewFake(time.Unix(1000, 0))
	r := newTestRegistry(b, clk.Now, "gw1")
	registerEndpoint(t, b, "gw2", epA)
	registerEndpoint(t, b, "gw2", epB)

	n, err := r.ReclaimDead(context.Background(), "gw2")
	if err != nil || n != 2 {
		t.Fatalf("reclaim: n=%d err=%v", n, err)
	}
	for _, ep := range []model.EndpointKey{epA, epB} {
		if _, ok, _ := b.Lookup(context.Background(), ep); ok {
			t.Fatalf("binding %s should be gone", ep.String())
		}
	}
	eps, _ := b.EndpointIndex(context.Background(), "gw2")
	if len(eps) != 0 {
		t.Fatalf("reverse index should be empty: %v", eps)
	}
	if _, ok, _ := b.Node(context.Background(), "gw2", clk.Now()); ok {
		t.Fatal("dead node key should be deleted")
	}
}

func TestReclaimDeadSkipsReboundEndpoints(t *testing.T) {
	b := backend.NewMemoryBackend()
	clk := clock.NewFake(time.Unix(1000, 0))
	r := newTestRegistry(b, clk.Now, "gw1")
	registerEndpoint(t, b, "gw1", epA)
	registerEndpoint(t, b, "gw2", epA)

	n, err := r.ReclaimDead(context.Background(), "gw1")
	if err != nil || n != 0 {
		t.Fatalf("reclaim must skip rebound: n=%d err=%v", n, err)
	}
	binding, ok, _ := b.Lookup(context.Background(), epA)
	if !ok || binding.NodeID != "gw2" {
		t.Fatalf("gw2 binding must survive: %+v ok=%v", binding, ok)
	}
}

func TestScanDeadDetectsTTLExpiry(t *testing.T) {
	b := backend.NewMemoryBackend()
	clk := clock.NewFake(time.Unix(1000, 0))
	r := newTestRegistry(b, clk.Now, "gw1")
	if err := r.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	other := model.NodeInfo{NodeID: "gw2", Addr: "a", Status: model.NodeAlive}
	if err := b.SaveNode(context.Background(), other, 8*time.Second, clk.Now()); err != nil {
		t.Fatal(err)
	}
	r.ObserveKnown("gw2")
	registerEndpoint(t, b, "gw2", epA)

	clk.Advance(9 * time.Second)
	r.scanDead(context.Background(), clk.Now())
	if _, ok, _ := b.Lookup(context.Background(), epA); ok {
		t.Fatal("binding of expired node must be reclaimed")
	}
}

func TestLookupRejectsDeadNode(t *testing.T) {
	b := backend.NewMemoryBackend()
	clk := clock.NewFake(time.Unix(1000, 0))
	r := newTestRegistry(b, clk.Now, "gw1")
	info := model.NodeInfo{NodeID: "gw2", Addr: "a", Status: model.NodeDead}
	if err := b.SaveNode(context.Background(), info, 10*time.Second, clk.Now()); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := r.Lookup(context.Background(), "gw2"); !errors.Is(err, ErrNodeUnavailable) || ok {
		t.Fatalf("dead node lookup: ok=%v err=%v", ok, err)
	}
}

func TestAddEndpointIndexFailureStillAllowsBind(t *testing.T) {
	b := backend.NewMemoryBackend()
	clk := clock.NewFake(time.Unix(1000, 0))
	r := newTestRegistry(b, clk.Now, "gw1")
	registerEndpoint(t, b, "gw1", epA)

	b.SetFault("RemoveEndpointIndex", errors.New("SREM failed"))
	registerEndpoint(t, b, "gw2", epA)
	binding, ok, _ := b.Lookup(context.Background(), epA)
	if !ok || binding.NodeID != "gw2" {
		t.Fatalf("rebind must succeed despite SREM failure: %+v ok=%v", binding, ok)
	}
	b.SetFault("RemoveEndpointIndex", nil)
	n, err := r.ReclaimDead(context.Background(), "gw1")
	if err != nil || n != 0 {
		t.Fatalf("reclaim should skip rebound endpoint: n=%d err=%v", n, err)
	}
	binding, ok, _ = b.Lookup(context.Background(), epA)
	if !ok || binding.NodeID != "gw2" {
		t.Fatalf("gw2 route must survive reclaim: %+v ok=%v", binding, ok)
	}
}

// TestStopWithoutRegisterDoesNotBlock 守护未启动心跳时的 Stop 幂等性：
// done 只由 heartbeatLoop 关闭，没启动过就等待会永久阻塞停机路径。
func TestStopWithoutRegisterDoesNotBlock(t *testing.T) {
	b := backend.NewMemoryBackend()
	r := NewRegistry(Options{
		Backend: b, Clock: clock.NewFake(time.Unix(1000, 0)).Now,
		Self: model.NodeInfo{NodeID: "gw1"}, TTL: 10 * time.Second, SuspectAfter: 5 * time.Second,
		HeartbeatEvery: 10 * time.Millisecond, Log: testLogger(),
	})
	done := make(chan struct{})
	go func() {
		r.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop blocked when heartbeat was never started")
	}
}

// TestDeregisterStopsHeartbeatAndReclaims 守护注销顺序：先停心跳再回收，
// 否则下一次 tick 会把节点写回去，绑定回收结果随之失效。
func TestDeregisterStopsHeartbeatAndReclaims(t *testing.T) {
	b := backend.NewMemoryBackend()
	clk := clock.NewFake(time.Unix(1000, 0))
	r := NewRegistry(Options{
		Backend: b, Clock: clk.Now,
		Self: model.NodeInfo{NodeID: "gw1", Addr: "a"},
		TTL:  10 * time.Second, SuspectAfter: 5 * time.Second,
		HeartbeatEvery: 10 * time.Millisecond, Log: testLogger(),
	})
	ctx := context.Background()
	if err := r.Register(ctx); err != nil {
		t.Fatal(err)
	}
	registerEndpoint(t, b, "gw1", epA)

	if err := r.Deregister(ctx); err != nil {
		t.Fatalf("deregister: %v", err)
	}
	select {
	case <-r.done:
	default:
		t.Fatal("heartbeat loop still running after Deregister")
	}
	if _, ok, _ := b.Node(ctx, "gw1", clk.Now()); ok {
		t.Fatal("deregistered node key must be deleted and not re-registered")
	}
	if _, ok, _ := b.Lookup(ctx, epA); ok {
		t.Fatal("deregistered node bindings must be reclaimed")
	}
	r.Stop() // Deregister 已停心跳，Stop 必须幂等且不阻塞
}

// TestScanDeadRetriesIncompleteReclaim 守护部分失败：保留反向索引并重试，
// 不能删除索引/节点键后把剩余 binding 永久遗留。
func TestScanDeadRetriesIncompleteReclaim(t *testing.T) {
	b := backend.NewMemoryBackend()
	clk := clock.NewFake(time.Unix(1000, 0))
	r := newTestRegistry(b, clk.Now, "gw1")
	ctx := context.Background()
	registerEndpoint(t, b, "gw2", epA)
	r.ObserveKnown("gw2")

	b.SetFault("DeleteIfOwner", errors.New("backend down"))
	r.scanDead(ctx, clk.Now())
	if _, ok, _ := b.Lookup(ctx, epA); !ok {
		t.Fatal("binding must survive while reclaim fails")
	}
	eps, _ := b.EndpointIndex(ctx, "gw2")
	if len(eps) != 1 {
		t.Fatalf("reverse index must be preserved for retry: %v", eps)
	}

	b.SetFault("DeleteIfOwner", nil)
	r.scanDead(ctx, clk.Now())
	if _, ok, _ := b.Lookup(ctx, epA); ok {
		t.Fatal("retry must reclaim the binding")
	}
	if eps, _ := b.EndpointIndex(ctx, "gw2"); len(eps) != 0 {
		t.Fatalf("retry must clear reverse index: %v", eps)
	}
}

// TestLookupObservesKnown 守护转发路径发现的新节点也会被纳入 DEAD 候选。
func TestLookupObservesKnown(t *testing.T) {
	b := backend.NewMemoryBackend()
	clk := clock.NewFake(time.Unix(1000, 0))
	r := newTestRegistry(b, clk.Now, "gw1")
	info := model.NodeInfo{NodeID: "gw2", Addr: "a", Status: model.NodeAlive}
	if err := b.SaveNode(context.Background(), info, 10*time.Second, clk.Now()); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := r.Lookup(context.Background(), "gw2"); err != nil || !ok {
		t.Fatalf("lookup: ok=%v err=%v", ok, err)
	}
	r.mu.Lock()
	known := r.known["gw2"]
	r.mu.Unlock()
	if !known {
		t.Fatal("lookup must observe node as known for later DEAD detection")
	}
}
