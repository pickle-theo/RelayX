package registry

import (
	"context"
	"testing"
	"time"
)

func TestMemoryNodeLifecycle(t *testing.T) {
	ctx := context.Background()
	reg := NewMemory(time.Second)
	defer reg.Close()

	started := time.Now().Add(-time.Minute)
	if err := reg.RegisterNode(ctx, Node{ID: "node-a", Addr: "127.0.0.1:9100", StartedAt: started}); err != nil {
		t.Fatalf("RegisterNode() error = %v", err)
	}

	nodes, err := reg.ListNodes(ctx)
	if err != nil {
		t.Fatalf("ListNodes() error = %v", err)
	}
	if len(nodes) != 1 || nodes[0].ID != "node-a" || nodes[0].Addr != "127.0.0.1:9100" {
		t.Fatalf("ListNodes() = %+v, want node-a at 127.0.0.1:9100", nodes)
	}
	if !nodes[0].StartedAt.Equal(started) {
		t.Fatalf("StartedAt = %v, want %v", nodes[0].StartedAt, started)
	}

	alive, err := reg.NodeAlive(ctx, "node-a")
	if err != nil || !alive {
		t.Fatalf("NodeAlive(node-a) = %v, %v; want true, nil", alive, err)
	}

	if err := reg.DeregisterNode(ctx, "node-a"); err != nil {
		t.Fatalf("DeregisterNode() error = %v", err)
	}
	alive, err = reg.NodeAlive(ctx, "node-a")
	if err != nil || alive {
		t.Fatalf("NodeAlive(node-a) = %v, %v; want false, nil", alive, err)
	}
	nodes, err = reg.ListNodes(ctx)
	if err != nil || len(nodes) != 0 {
		t.Fatalf("ListNodes() = %+v, %v; want empty", nodes, err)
	}
}

func TestMemoryNodeExpires(t *testing.T) {
	ctx := context.Background()
	reg := NewMemory(40 * time.Millisecond)
	defer reg.Close()

	if err := reg.RegisterNode(ctx, Node{ID: "node-a", Addr: "addr"}); err != nil {
		t.Fatalf("RegisterNode() error = %v", err)
	}
	waitFor(t, time.Second, func() bool {
		alive, err := reg.NodeAlive(ctx, "node-a")
		return err == nil && !alive
	})

	nodes, err := reg.ListNodes(ctx)
	if err != nil || len(nodes) != 0 {
		t.Fatalf("ListNodes() = %+v, %v; want empty after expiry", nodes, err)
	}
}

func TestMemoryLookupNode(t *testing.T) {
	ctx := context.Background()
	reg := NewMemory(40 * time.Millisecond)
	defer reg.Close()

	if _, ok, err := reg.LookupNode(ctx, "node-a"); err != nil || ok {
		t.Fatalf("LookupNode(unknown) = _, %v, %v; want missing", ok, err)
	}

	started := time.Now().Add(-time.Minute).Truncate(time.Second)
	if err := reg.RegisterNode(ctx, Node{ID: "node-a", Addr: "127.0.0.1:9100", StartedAt: started}); err != nil {
		t.Fatalf("RegisterNode() error = %v", err)
	}
	got, ok, err := reg.LookupNode(ctx, "node-a")
	if err != nil || !ok {
		t.Fatalf("LookupNode(node-a) = _, %v, %v; want found", ok, err)
	}
	if got.ID != "node-a" || got.Addr != "127.0.0.1:9100" || !got.StartedAt.Equal(started) {
		t.Fatalf("LookupNode(node-a) = %+v, want node-a at 127.0.0.1:9100", got)
	}

	waitFor(t, time.Second, func() bool {
		_, ok, err := reg.LookupNode(ctx, "node-a")
		return err == nil && !ok
	})
}

func TestMemoryNextVersionMonotonic(t *testing.T) {
	ctx := context.Background()
	reg := NewMemory(time.Second)
	defer reg.Close()

	for want := uint64(1); want <= 3; want++ {
		got, err := reg.NextVersion(ctx, "user", "device")
		if err != nil {
			t.Fatalf("NextVersion() error = %v", err)
		}
		if got != want {
			t.Fatalf("NextVersion() = %d, want %d", got, want)
		}
	}
	other, err := reg.NextVersion(ctx, "user", "other-device")
	if err != nil || other != 1 {
		t.Fatalf("NextVersion(other) = %d, %v; want 1, nil", other, err)
	}
}

func TestMemoryRouteLookupAndCAS(t *testing.T) {
	ctx := context.Background()
	reg := NewMemory(time.Second)
	defer reg.Close()

	created := time.Now().Truncate(time.Second)
	rt := Route{
		UserID:    "user",
		DeviceID:  "device",
		NodeID:    "node-a",
		SessionID: 42,
		Version:   2,
		CreatedAt: created,
	}
	if err := reg.UpsertRoute(ctx, rt); err != nil {
		t.Fatalf("UpsertRoute() error = %v", err)
	}

	got, ok, err := reg.LookupRoute(ctx, "user", "device")
	if err != nil || !ok {
		t.Fatalf("LookupRoute() = _, %v, %v; want found", ok, err)
	}
	if got != rt {
		t.Fatalf("LookupRoute() = %+v, want %+v", got, rt)
	}

	stale := rt
	stale.Version = 1
	if deleted, err := reg.DeleteRouteIfVersion(ctx, stale); err != nil || deleted {
		t.Fatalf("DeleteRouteIfVersion(stale) = %v, %v; want false, nil", deleted, err)
	}

	wrongNode := rt
	wrongNode.NodeID = "node-b"
	if deleted, err := reg.DeleteRouteIfVersion(ctx, wrongNode); err != nil || deleted {
		t.Fatalf("DeleteRouteIfVersion(wrong node) = %v, %v; want false, nil", deleted, err)
	}

	if deleted, err := reg.DeleteRouteIfVersion(ctx, rt); err != nil || !deleted {
		t.Fatalf("DeleteRouteIfVersion() = %v, %v; want true, nil", deleted, err)
	}
	if _, ok, err := reg.LookupRoute(ctx, "user", "device"); err != nil || ok {
		t.Fatalf("LookupRoute() after delete = %v, %v; want missing", ok, err)
	}
	if deleted, err := reg.DeleteRouteIfVersion(ctx, rt); err != nil || deleted {
		t.Fatalf("DeleteRouteIfVersion() twice = %v, %v; want false, nil", deleted, err)
	}
}

func TestMemorySweepDeadNodes(t *testing.T) {
	ctx := context.Background()
	reg := NewMemory(40 * time.Millisecond)
	defer reg.Close()

	if err := reg.RegisterNode(ctx, Node{ID: "node-a", Addr: "a"}); err != nil {
		t.Fatalf("RegisterNode(a) error = %v", err)
	}
	if err := reg.RegisterNode(ctx, Node{ID: "node-b", Addr: "b"}); err != nil {
		t.Fatalf("RegisterNode(b) error = %v", err)
	}

	routeA := Route{UserID: "user-a", DeviceID: "device", NodeID: "node-a", Version: 1, CreatedAt: time.Now()}
	routeB := Route{UserID: "user-b", DeviceID: "device", NodeID: "node-b", Version: 1, CreatedAt: time.Now()}
	routeMoved := Route{UserID: "user-c", DeviceID: "device", NodeID: "node-a", Version: 1, CreatedAt: time.Now()}
	for _, rt := range []Route{routeA, routeB, routeMoved} {
		if err := reg.UpsertRoute(ctx, rt); err != nil {
			t.Fatalf("UpsertRoute(%s) error = %v", rt.UserID, err)
		}
	}
	// The moved route is taken over by node-b, so node-a must no longer own it.
	routeMoved.NodeID = "node-b"
	routeMoved.Version = 2
	if err := reg.UpsertRoute(ctx, routeMoved); err != nil {
		t.Fatalf("UpsertRoute(moved) error = %v", err)
	}

	waitFor(t, time.Second, func() bool {
		alive, err := reg.NodeAlive(ctx, "node-a")
		return err == nil && !alive
	})
	// Re-register node-b to keep it alive after its first TTL lapsed.
	if err := reg.RegisterNode(ctx, Node{ID: "node-b", Addr: "b"}); err != nil {
		t.Fatalf("RegisterNode(b) error = %v", err)
	}

	cleaned, err := reg.SweepDeadNodes(ctx)
	if err != nil {
		t.Fatalf("SweepDeadNodes() error = %v", err)
	}
	if len(cleaned) != 1 || cleaned[0] != "node-a" {
		t.Fatalf("SweepDeadNodes() = %v, want [node-a]", cleaned)
	}

	if _, ok, err := reg.LookupRoute(ctx, "user-a", "device"); err != nil || ok {
		t.Fatalf("dead node route still present (ok=%v, err=%v)", ok, err)
	}
	if got, ok, err := reg.LookupRoute(ctx, "user-b", "device"); err != nil || !ok || got.NodeID != "node-b" {
		t.Fatalf("live node route missing or wrong: %+v, %v, %v", got, ok, err)
	}
	if got, ok, err := reg.LookupRoute(ctx, "user-c", "device"); err != nil || !ok || got.NodeID != "node-b" {
		t.Fatalf("moved route was deleted: %+v, %v, %v", got, ok, err)
	}

	cleaned, err = reg.SweepDeadNodes(ctx)
	if err != nil || len(cleaned) != 0 {
		t.Fatalf("second SweepDeadNodes() = %v, %v; want empty", cleaned, err)
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}
