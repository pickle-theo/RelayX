package registry

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestKeeperStartStop(t *testing.T) {
	ctx := context.Background()
	reg := NewMemory(time.Second)
	defer reg.Close()

	k := NewKeeper(reg, KeeperConfig{
		NodeID:    "node-a",
		Addr:      "127.0.0.1:9100",
		Heartbeat: 10 * time.Millisecond,
		Cleanup:   time.Hour,
		Logger:    testLogger(),
	})

	if err := k.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	// A second Start must not register a duplicate or spawn another loop.
	if err := k.Start(ctx); err != nil {
		t.Fatalf("second Start() error = %v", err)
	}
	nodes, err := reg.ListNodes(ctx)
	if err != nil || len(nodes) != 1 || nodes[0].ID != "node-a" {
		t.Fatalf("ListNodes() = %+v, %v; want node-a", nodes, err)
	}

	if err := k.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if alive, err := reg.NodeAlive(ctx, "node-a"); err != nil || alive {
		t.Fatalf("NodeAlive() after Stop = %v, %v; want false", alive, err)
	}
	// Repeated Stop is a no-op.
	if err := k.Stop(); err != nil {
		t.Fatalf("second Stop() error = %v", err)
	}

	// The keeper can be restarted after a stop.
	if err := k.Start(ctx); err != nil {
		t.Fatalf("restart Start() error = %v", err)
	}
	if alive, err := reg.NodeAlive(ctx, "node-a"); err != nil || !alive {
		t.Fatalf("NodeAlive() after restart = %v, %v; want true", alive, err)
	}
	if err := k.Stop(); err != nil {
		t.Fatalf("final Stop() error = %v", err)
	}
}

func TestKeeperHeartbeatKeepsNodeAlive(t *testing.T) {
	ctx := context.Background()
	reg := NewMemory(30 * time.Millisecond)
	defer reg.Close()

	k := NewKeeper(reg, KeeperConfig{
		NodeID:    "node-a",
		Addr:      "addr",
		Heartbeat: 10 * time.Millisecond,
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

	// Without heartbeats the 30ms TTL would expire well within this window.
	time.Sleep(120 * time.Millisecond)
	alive, err := reg.NodeAlive(ctx, "node-a")
	if err != nil {
		t.Fatalf("NodeAlive() error = %v", err)
	}
	if !alive {
		t.Fatal("NodeAlive() = false; heartbeat did not re-register the node")
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
