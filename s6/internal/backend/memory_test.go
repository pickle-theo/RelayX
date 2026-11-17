package backend

import (
	"context"
	"errors"
	"testing"
	"time"

	"relayx/s6/internal/model"
)

var (
	epA = model.EndpointKey{TenantID: "t1", UserID: "u1", DeviceID: "d1"}
	epB = model.EndpointKey{TenantID: "t1", UserID: "u2", DeviceID: "d2"}
)

func TestBindVersionMonotonicAndOldNodeReturned(t *testing.T) {
	b := NewMemoryBackend()
	ctx := context.Background()
	now := time.Unix(1000, 0)

	v1, old, err := b.Bind(ctx, epA, "gw1", now)
	if err != nil || v1 != 1 || old != "" {
		t.Fatalf("first bind: v=%d old=%q err=%v", v1, old, err)
	}
	v2, old, err := b.Bind(ctx, epA, "gw2", now.Add(time.Second))
	if err != nil || v2 != 2 || old != "gw1" {
		t.Fatalf("rebind: v=%d old=%q err=%v", v2, old, err)
	}
	v3, old, err := b.Bind(ctx, epB, "gw1", now)
	if err != nil || v3 != 1 || old != "" {
		t.Fatalf("other endpoint must have own seq: v=%d old=%q err=%v", v3, old, err)
	}
	binding, ok, err := b.Lookup(ctx, epA)
	if err != nil || !ok || binding.NodeID != "gw2" || binding.Version != 2 {
		t.Fatalf("lookup: %+v ok=%v err=%v", binding, ok, err)
	}
}

func TestUnbindVersionGuard(t *testing.T) {
	b := NewMemoryBackend()
	ctx := context.Background()
	now := time.Unix(1000, 0)

	v1, _, _ := b.Bind(ctx, epA, "gw1", now)
	v2, _, _ := b.Bind(ctx, epA, "gw2", now.Add(time.Second))

	// 旧连接的延迟清理事件（v1）不得删除新连接（v2）的路由。
	deleted, err := b.UnbindIfVersion(ctx, epA, v1)
	if err != nil || deleted {
		t.Fatalf("stale unbind should be ignored: deleted=%v err=%v", deleted, err)
	}
	binding, ok, _ := b.Lookup(ctx, epA)
	if !ok || binding.Version != v2 || binding.NodeID != "gw2" {
		t.Fatalf("new binding must survive: %+v ok=%v", binding, ok)
	}
	// 持有当前 version 的连接可以解绑。
	deleted, err = b.UnbindIfVersion(ctx, epA, v2)
	if err != nil || !deleted {
		t.Fatalf("current unbind: deleted=%v err=%v", deleted, err)
	}
	if _, ok, _ := b.Lookup(ctx, epA); ok {
		t.Fatal("binding should be gone")
	}
}

func TestDeleteIfOwnerGuard(t *testing.T) {
	b := NewMemoryBackend()
	ctx := context.Background()
	now := time.Unix(1000, 0)

	_, _, _ = b.Bind(ctx, epA, "gw1", now)
	_, _, _ = b.Bind(ctx, epA, "gw2", now.Add(time.Second)) // 已重绑到 gw2

	// gw1 死亡后的回收必须跳过已重绑的端点。
	deleted, err := b.DeleteIfOwner(ctx, epA, "gw1")
	if err != nil || deleted {
		t.Fatalf("reclaim of dead node should skip rebound endpoint: deleted=%v err=%v", deleted, err)
	}
	binding, ok, _ := b.Lookup(ctx, epA)
	if !ok || binding.NodeID != "gw2" {
		t.Fatalf("gw2 binding must survive: %+v ok=%v", binding, ok)
	}
	deleted, err = b.DeleteIfOwner(ctx, epA, "gw2")
	if err != nil || !deleted {
		t.Fatalf("owner reclaim: deleted=%v err=%v", deleted, err)
	}
}

func TestNodeTTLExpiry(t *testing.T) {
	b := NewMemoryBackend()
	ctx := context.Background()
	now := time.Unix(1000, 0)
	info := model.NodeInfo{NodeID: "gw1", Addr: "127.0.0.1:9803", Status: model.NodeAlive}

	if err := b.SaveNode(ctx, info, 10*time.Second, now); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := b.Node(ctx, "gw1", now.Add(9*time.Second)); !ok {
		t.Fatal("node should be alive before TTL")
	}
	if _, ok, _ := b.Node(ctx, "gw1", now.Add(10*time.Second)); ok {
		t.Fatal("expired node must not be returned")
	}
	refreshed, err := b.RefreshNode(ctx, "gw1", 10*time.Second, now.Add(9*time.Second))
	if err != nil || !refreshed {
		t.Fatalf("refresh: ok=%v err=%v", refreshed, err)
	}
	if _, ok, _ := b.Node(ctx, "gw1", now.Add(15*time.Second)); !ok {
		t.Fatal("heartbeat should extend TTL")
	}
}

// TestSetNodeStatusPreservesTTL 守护 SUSPECT 判定：只改状态，不能像 SaveNode
// 那样把过期时间推后（否则 DEAD 回收会被无限推迟）。
func TestSetNodeStatusPreservesTTL(t *testing.T) {
	b := NewMemoryBackend()
	ctx := context.Background()
	now := time.Unix(1000, 0)
	info := model.NodeInfo{NodeID: "gw1", Addr: "a", Status: model.NodeAlive}
	if err := b.SaveNode(ctx, info, 10*time.Second, now); err != nil {
		t.Fatal(err)
	}

	ok, err := b.SetNodeStatus(ctx, "gw1", model.NodeSuspect, now.Add(9*time.Second))
	if err != nil || !ok {
		t.Fatalf("set status: ok=%v err=%v", ok, err)
	}
	got, ok, err := b.Node(ctx, "gw1", now.Add(9*time.Second))
	if err != nil || !ok || got.Status != model.NodeSuspect {
		t.Fatalf("status not updated: %+v ok=%v err=%v", got, ok, err)
	}
	// 原始 TTL 到期（t0+10s）必须过期，而不是被续到 t0+19s。
	if _, ok, _ := b.Node(ctx, "gw1", now.Add(10*time.Second)); ok {
		t.Fatal("SetNodeStatus must not extend TTL")
	}
}

// TestRefreshNodeResetsAliveAndRejectsExpired 守护心跳恢复与"不复活"语义。
func TestRefreshNodeResetsAliveAndRejectsExpired(t *testing.T) {
	b := NewMemoryBackend()
	ctx := context.Background()
	now := time.Unix(1000, 0)
	info := model.NodeInfo{NodeID: "gw1", Addr: "a", Status: model.NodeAlive}
	if err := b.SaveNode(ctx, info, 10*time.Second, now); err != nil {
		t.Fatal(err)
	}
	if _, err := b.SetNodeStatus(ctx, "gw1", model.NodeSuspect, now); err != nil {
		t.Fatal(err)
	}
	refreshed, err := b.RefreshNode(ctx, "gw1", 10*time.Second, now.Add(time.Second))
	if err != nil || !refreshed {
		t.Fatalf("refresh: ok=%v err=%v", refreshed, err)
	}
	got, _, _ := b.Node(ctx, "gw1", now.Add(time.Second))
	if got.Status != model.NodeAlive {
		t.Fatalf("heartbeat must reset status to ALIVE: %+v", got)
	}

	// TTL 过期后 refresh 必须失败，且不能重新创建节点键。
	refreshed, err = b.RefreshNode(ctx, "gw1", 10*time.Second, now.Add(20*time.Second))
	if err != nil || refreshed {
		t.Fatalf("expired refresh must fail: ok=%v err=%v", refreshed, err)
	}
	if _, ok, _ := b.Node(ctx, "gw1", now.Add(20*time.Second)); ok {
		t.Fatal("refresh must not resurrect an expired node")
	}
}

func TestEndpointIndexSetSemantics(t *testing.T) {
	b := NewMemoryBackend()
	ctx := context.Background()

	if err := b.AddEndpointIndex(ctx, "gw1", epA); err != nil {
		t.Fatal(err)
	}
	if err := b.AddEndpointIndex(ctx, "gw1", epA); err != nil {
		t.Fatal(err) // 幂等
	}
	eps, err := b.EndpointIndex(ctx, "gw1")
	if err != nil || len(eps) != 1 {
		t.Fatalf("index: %v err=%v", eps, err)
	}
	if err := b.RemoveEndpointIndex(ctx, "gw1", epA); err != nil {
		t.Fatal(err)
	}
	eps, _ = b.EndpointIndex(ctx, "gw1")
	if len(eps) != 0 {
		t.Fatalf("index should be empty: %v", eps)
	}
}

func TestDownAndFaultInjection(t *testing.T) {
	b := NewMemoryBackend()
	ctx := context.Background()
	now := time.Unix(1000, 0)

	if !b.Available(ctx) {
		t.Fatal("fresh backend should be available")
	}
	b.SetDown(true)
	if b.Available(ctx) {
		t.Fatal("backend must report degraded")
	}
	if _, _, err := b.Bind(ctx, epA, "gw1", now); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("bind down: %v", err)
	}
	b.SetDown(false)

	injected := errors.New("injected SADD failure")
	b.SetFault("AddEndpointIndex", injected)
	if err := b.AddEndpointIndex(ctx, "gw1", epA); !errors.Is(err, injected) {
		t.Fatalf("fault injection: %v", err)
	}
	b.SetFault("AddEndpointIndex", nil)
	if err := b.AddEndpointIndex(ctx, "gw1", epA); err != nil {
		t.Fatal(err)
	}
}
