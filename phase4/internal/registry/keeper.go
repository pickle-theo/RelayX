package registry

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

const (
	defaultHeartbeat = 5 * time.Second
	defaultCleanup   = 10 * time.Second
)

// KeeperConfig configures a node's registry heartbeat and stale-node sweeper.
type KeeperConfig struct {
	NodeID    string
	Addr      string
	Heartbeat time.Duration // defaults to 5s
	Cleanup   time.Duration // defaults to 10s
	Logger    *slog.Logger
}

// Keeper periodically re-registers this node and sweeps nodes that stopped
// heartbeating. Re-registering every tick lets a live node rebuild its hash
// after Redis data loss or TTL expiry. One goroutine drives both tickers so
// Stop has a single thing to wait for.
type Keeper struct {
	reg  Registry
	cfg  KeeperConfig
	node Node

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// NewKeeper returns a keeper. Call Start to register the node and begin the
// background loop.
func NewKeeper(reg Registry, cfg KeeperConfig) *Keeper {
	if cfg.Heartbeat <= 0 {
		cfg.Heartbeat = defaultHeartbeat
	}
	if cfg.Cleanup <= 0 {
		cfg.Cleanup = defaultCleanup
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Keeper{reg: reg, cfg: cfg}
}

// Start registers the node and starts the heartbeat/sweep loop. Repeated calls
// are no-ops. A registration failure is returned and leaves the keeper stopped.
func (k *Keeper) Start(ctx context.Context) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.running {
		return nil
	}
	node := Node{ID: k.cfg.NodeID, Addr: k.cfg.Addr, StartedAt: time.Now()}
	if err := k.reg.RegisterNode(ctx, node); err != nil {
		return fmt.Errorf("registry: keeper register node %s: %w", k.cfg.NodeID, err)
	}
	k.node = node

	k.stopCh = make(chan struct{})
	k.running = true
	k.wg.Add(1)
	go k.loop(ctx)
	return nil
}

// Stop terminates the loop, waits for it to exit, then deregisters the node.
// Deregistering after the wait prevents a final heartbeat from writing the node
// back. Repeated calls are no-ops.
func (k *Keeper) Stop() error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if !k.running {
		return nil
	}
	close(k.stopCh)
	k.running = false
	k.wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := k.reg.DeregisterNode(ctx, k.cfg.NodeID); err != nil {
		return fmt.Errorf("registry: keeper deregister node %s: %w", k.cfg.NodeID, err)
	}
	return nil
}

func (k *Keeper) loop(ctx context.Context) {
	defer k.wg.Done()

	heartbeat := time.NewTicker(k.cfg.Heartbeat)
	defer heartbeat.Stop()
	cleanup := time.NewTicker(k.cfg.Cleanup)
	defer cleanup.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-k.stopCh:
			return
		case <-heartbeat.C:
			// Register rather than just extend the TTL so a node hash lost to
			// Redis eviction or TTL expiry is rebuilt automatically. A failed
			// heartbeat is transient; the next tick retries.
			if err := k.reg.RegisterNode(ctx, k.node); err != nil {
				k.cfg.Logger.Warn("registry: node heartbeat failed",
					"node", k.cfg.NodeID, "error", err)
			}
		case <-cleanup.C:
			if _, err := k.reg.SweepDeadNodes(ctx); err != nil {
				k.cfg.Logger.Warn("registry: node sweep failed",
					"node", k.cfg.NodeID, "error", err)
			}
		}
	}
}
