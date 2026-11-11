package registry

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// newTestRedis returns a Redis registry under a unique key prefix so parallel
// test runs (and leftover data) cannot interfere. The prefix is scanned and
// deleted when the test finishes.
func newTestRedis(t *testing.T, nodeTTL time.Duration) *Redis {
	t.Helper()

	addr := os.Getenv("RELAYX_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("RELAYX_TEST_REDIS_ADDR not set")
	}
	password := os.Getenv("RELAYX_TEST_REDIS_PASSWORD")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("relayx-test:%d:", time.Now().UnixNano())
	reg, err := NewRedis(ctx, RedisConfig{
		Addr:     addr,
		Password: password,
		Prefix:   prefix,
		NodeTTL:  nodeTTL,
	})
	if err != nil {
		t.Fatalf("NewRedis() error = %v", err)
	}
	t.Cleanup(func() {
		cleanupRedisPrefix(t, addr, password, prefix)
		_ = reg.Close()
	})
	return reg
}

func cleanupRedisPrefix(t *testing.T, addr, password, prefix string) {
	t.Helper()

	client := redis.NewClient(&redis.Options{Addr: addr, Password: password})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, prefix+"*", 100).Result()
		if err != nil {
			t.Logf("cleanup scan %q: %v", prefix, err)
			return
		}
		if len(keys) > 0 {
			if err := client.Del(ctx, keys...).Err(); err != nil {
				t.Logf("cleanup del %q: %v", prefix, err)
			}
		}
		cursor = next
		if cursor == 0 {
			return
		}
	}
}

func TestRedisNodeLifecycle(t *testing.T) {
	ctx := context.Background()
	reg := newTestRedis(t, time.Second)

	started := time.Now().Truncate(time.Second)
	if err := reg.RegisterNode(ctx, Node{ID: "node-a", Addr: "127.0.0.1:9100", StartedAt: started}); err != nil {
		t.Fatalf("RegisterNode() error = %v", err)
	}
	if err := reg.RegisterNode(ctx, Node{ID: "node-b", Addr: "127.0.0.1:9101"}); err != nil {
		t.Fatalf("RegisterNode(b) error = %v", err)
	}

	nodes, err := reg.ListNodes(ctx)
	if err != nil {
		t.Fatalf("ListNodes() error = %v", err)
	}
	if len(nodes) != 2 || nodes[0].ID != "node-a" || nodes[1].ID != "node-b" {
		t.Fatalf("ListNodes() = %+v, want node-a and node-b", nodes)
	}
	if nodes[0].Addr != "127.0.0.1:9100" || !nodes[0].StartedAt.Equal(started) {
		t.Fatalf("node-a = %+v, want addr 127.0.0.1:9100 started %v", nodes[0], started)
	}

	// Re-registering must keep the node visible and refresh its TTL.
	if err := reg.RegisterNode(ctx, Node{ID: "node-a", Addr: "127.0.0.1:9100", StartedAt: started}); err != nil {
		t.Fatalf("RegisterNode() again error = %v", err)
	}
	if alive, err := reg.NodeAlive(ctx, "node-a"); err != nil || !alive {
		t.Fatalf("NodeAlive(node-a) = %v, %v; want true", alive, err)
	}

	if err := reg.DeregisterNode(ctx, "node-a"); err != nil {
		t.Fatalf("DeregisterNode() error = %v", err)
	}
	if alive, err := reg.NodeAlive(ctx, "node-a"); err != nil || alive {
		t.Fatalf("NodeAlive(node-a) = %v, %v; want false", alive, err)
	}
	nodes, err = reg.ListNodes(ctx)
	if err != nil || len(nodes) != 1 || nodes[0].ID != "node-b" {
		t.Fatalf("ListNodes() = %+v, %v; want only node-b", nodes, err)
	}
}

// TestRedisRegisterNodeRebuildsDeletedKey verifies the self-healing path used
// by the keeper: a node hash deleted from Redis is recreated by the next
// RegisterNode call.
func TestRedisRegisterNodeRebuildsDeletedKey(t *testing.T) {
	ctx := context.Background()
	reg := newTestRedis(t, time.Second)

	node := Node{ID: "node-a", Addr: "127.0.0.1:9100", StartedAt: time.Now().Truncate(time.Second)}
	if err := reg.RegisterNode(ctx, node); err != nil {
		t.Fatalf("RegisterNode() error = %v", err)
	}
	if err := reg.client.Del(ctx, reg.nodeKey(node.ID)).Err(); err != nil {
		t.Fatalf("delete node key: %v", err)
	}
	if _, ok, err := reg.LookupNode(ctx, node.ID); err != nil || ok {
		t.Fatalf("LookupNode() after delete = _, %v, %v; want missing", ok, err)
	}

	if err := reg.RegisterNode(ctx, node); err != nil {
		t.Fatalf("RegisterNode() after delete error = %v", err)
	}
	got, ok, err := reg.LookupNode(ctx, node.ID)
	if err != nil || !ok {
		t.Fatalf("LookupNode() after re-register = _, %v, %v; want found", ok, err)
	}
	if got.Addr != node.Addr {
		t.Fatalf("LookupNode().Addr = %q, want %q", got.Addr, node.Addr)
	}
}

// TestRedisKeeperRebuildsNodeAfterKeyLoss exercises the keeper against Redis:
// deleting the node key must not strand the node until process restart.
func TestRedisKeeperRebuildsNodeAfterKeyLoss(t *testing.T) {
	ctx := context.Background()
	reg := newTestRedis(t, 200*time.Millisecond)

	k := NewKeeper(reg, KeeperConfig{
		NodeID:    "node-a",
		Addr:      "127.0.0.1:9100",
		Heartbeat: 50 * time.Millisecond,
		Cleanup:   time.Hour,
		Logger:    testLogger(),
	})
	if err := k.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer func() {
		if err := k.Stop(); err != nil {
			t.Errorf("Stop() error = %v", err)
		}
	}()

	if err := reg.client.Del(ctx, reg.nodeKey("node-a")).Err(); err != nil {
		t.Fatalf("delete node key: %v", err)
	}
	// The 50ms heartbeat should rebuild the hash well within this window.
	waitFor(t, time.Second, func() bool {
		got, ok, err := reg.LookupNode(ctx, "node-a")
		return err == nil && ok && got.Addr == "127.0.0.1:9100"
	})

	if err := k.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if _, ok, err := reg.LookupNode(ctx, "node-a"); err != nil || ok {
		t.Fatalf("LookupNode() after Stop = _, %v, %v; want missing", ok, err)
	}
}

func TestRedisLookupNode(t *testing.T) {
	ctx := context.Background()
	const nodeTTL = 500 * time.Millisecond
	reg := newTestRedis(t, nodeTTL)

	if _, ok, err := reg.LookupNode(ctx, "node-missing"); err != nil || ok {
		t.Fatalf("LookupNode(missing) = _, %v, %v; want missing", ok, err)
	}

	started := time.Now().Truncate(time.Second)
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

	waitFor(t, 3*time.Second, func() bool {
		_, ok, err := reg.LookupNode(ctx, "node-a")
		return err == nil && !ok
	})
}

func TestRedisRouteFields(t *testing.T) {
	ctx := context.Background()
	reg := newTestRedis(t, time.Second)

	v1, err := reg.NextVersion(ctx, "user", "device")
	if err != nil || v1 != 1 {
		t.Fatalf("NextVersion() = %d, %v; want 1", v1, err)
	}
	v2, err := reg.NextVersion(ctx, "user", "device")
	if err != nil || v2 != 2 {
		t.Fatalf("NextVersion() = %d, %v; want 2", v2, err)
	}

	created := time.Now().Truncate(time.Second)
	rt := Route{
		UserID:    "user",
		DeviceID:  "device",
		NodeID:    "node-a",
		SessionID: 42,
		Version:   v2,
		CreatedAt: created,
	}
	if err := reg.UpsertRoute(ctx, rt); err != nil {
		t.Fatalf("UpsertRoute() error = %v", err)
	}

	got, ok, err := reg.LookupRoute(ctx, "user", "device")
	if err != nil || !ok {
		t.Fatalf("LookupRoute() = _, %v, %v; want found", ok, err)
	}
	if got.UserID != rt.UserID || got.DeviceID != rt.DeviceID || got.NodeID != rt.NodeID ||
		got.SessionID != rt.SessionID || got.Version != rt.Version {
		t.Fatalf("LookupRoute() = %+v, want %+v", got, rt)
	}
	if !got.CreatedAt.Equal(created) {
		t.Fatalf("CreatedAt = %v, want %v", got.CreatedAt, created)
	}

	if _, ok, err := reg.LookupRoute(ctx, "user", "missing"); err != nil || ok {
		t.Fatalf("LookupRoute(missing) = %v, %v; want missing", ok, err)
	}
}

func TestRedisDeleteRouteIfVersion(t *testing.T) {
	ctx := context.Background()
	reg := newTestRedis(t, time.Second)

	v1, err := reg.NextVersion(ctx, "user", "device")
	if err != nil {
		t.Fatalf("NextVersion() error = %v", err)
	}
	rt := Route{UserID: "user", DeviceID: "device", NodeID: "node-a", SessionID: 7, Version: v1, CreatedAt: time.Now()}
	if err := reg.UpsertRoute(ctx, rt); err != nil {
		t.Fatalf("UpsertRoute() error = %v", err)
	}

	stale := rt
	stale.Version = v1 - 1
	if deleted, err := reg.DeleteRouteIfVersion(ctx, stale); err != nil || deleted {
		t.Fatalf("DeleteRouteIfVersion(stale) = %v, %v; want false", deleted, err)
	}

	wrongNode := rt
	wrongNode.NodeID = "node-b"
	if deleted, err := reg.DeleteRouteIfVersion(ctx, wrongNode); err != nil || deleted {
		t.Fatalf("DeleteRouteIfVersion(wrong node) = %v, %v; want false", deleted, err)
	}
	if _, ok, err := reg.LookupRoute(ctx, "user", "device"); err != nil || !ok {
		t.Fatalf("route vanished after failed CAS: ok=%v err=%v", ok, err)
	}

	if deleted, err := reg.DeleteRouteIfVersion(ctx, rt); err != nil || !deleted {
		t.Fatalf("DeleteRouteIfVersion() = %v, %v; want true", deleted, err)
	}
	if _, ok, err := reg.LookupRoute(ctx, "user", "device"); err != nil || ok {
		t.Fatalf("LookupRoute() after delete = %v, %v; want missing", ok, err)
	}
	if deleted, err := reg.DeleteRouteIfVersion(ctx, rt); err != nil || deleted {
		t.Fatalf("DeleteRouteIfVersion(again) = %v, %v; want false", deleted, err)
	}
}

func TestRedisSweepDeadNodes(t *testing.T) {
	ctx := context.Background()
	const nodeTTL = 200 * time.Millisecond
	reg := newTestRedis(t, nodeTTL)

	if err := reg.RegisterNode(ctx, Node{ID: "node-dead", Addr: "dead"}); err != nil {
		t.Fatalf("RegisterNode(dead) error = %v", err)
	}
	if err := reg.RegisterNode(ctx, Node{ID: "node-alive", Addr: "alive"}); err != nil {
		t.Fatalf("RegisterNode(alive) error = %v", err)
	}

	routeDead := Route{UserID: "user-dead", DeviceID: "device", NodeID: "node-dead", Version: 1, CreatedAt: time.Now()}
	routeAlive := Route{UserID: "user-alive", DeviceID: "device", NodeID: "node-alive", Version: 1, CreatedAt: time.Now()}
	routeMoved := Route{UserID: "user-moved", DeviceID: "device", NodeID: "node-dead", Version: 1, CreatedAt: time.Now()}
	for _, rt := range []Route{routeDead, routeAlive, routeMoved} {
		if err := reg.UpsertRoute(ctx, rt); err != nil {
			t.Fatalf("UpsertRoute(%s) error = %v", rt.UserID, err)
		}
	}
	// node-alive takes the route over; Sweep must not delete it.
	routeMoved.NodeID = "node-alive"
	routeMoved.Version = 2
	if err := reg.UpsertRoute(ctx, routeMoved); err != nil {
		t.Fatalf("UpsertRoute(moved) error = %v", err)
	}

	// Keep node-alive fresh while node-dead's TTL lapses.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if err := reg.RegisterNode(ctx, Node{ID: "node-alive", Addr: "alive"}); err != nil {
					t.Logf("RegisterNode(alive): %v", err)
				}
			}
		}
	}()
	defer func() {
		close(stop)
		wg.Wait()
	}()

	waitFor(t, 3*time.Second, func() bool {
		alive, err := reg.NodeAlive(ctx, "node-dead")
		return err == nil && !alive
	})

	cleaned, err := reg.SweepDeadNodes(ctx)
	if err != nil {
		t.Fatalf("SweepDeadNodes() error = %v", err)
	}
	if !slices.Contains(cleaned, "node-dead") || slices.Contains(cleaned, "node-alive") {
		t.Fatalf("SweepDeadNodes() = %v, want only node-dead", cleaned)
	}

	if _, ok, err := reg.LookupRoute(ctx, "user-dead", "device"); err != nil || ok {
		t.Fatalf("dead node route still present: ok=%v err=%v", ok, err)
	}
	if got, ok, err := reg.LookupRoute(ctx, "user-alive", "device"); err != nil || !ok || got.NodeID != "node-alive" {
		t.Fatalf("alive node route = %+v, %v, %v; want node-alive", got, ok, err)
	}
	if got, ok, err := reg.LookupRoute(ctx, "user-moved", "device"); err != nil || !ok || got.NodeID != "node-alive" {
		t.Fatalf("moved route = %+v, %v, %v; want node-alive", got, ok, err)
	}
	if alive, err := reg.NodeAlive(ctx, "node-alive"); err != nil || !alive {
		t.Fatalf("NodeAlive(alive) = %v, %v; want true", alive, err)
	}

	cleaned, err = reg.SweepDeadNodes(ctx)
	if err != nil {
		t.Fatalf("second SweepDeadNodes() error = %v", err)
	}
	if slices.Contains(cleaned, "node-dead") {
		t.Fatalf("second SweepDeadNodes() = %v, want no dead node", cleaned)
	}
}
