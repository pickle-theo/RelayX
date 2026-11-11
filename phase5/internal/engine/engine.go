// Package engine runs the Phase 5 delivery scheduler: it leases due deliveries
// from PostgreSQL, tests the tenant quota, resolves the endpoint route and
// pushes through the local session manager or a peer gateway node.
package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"relayx/phase5/internal/delivery"
	"relayx/phase5/internal/protocol"
	"relayx/phase5/internal/registry"
	"relayx/phase5/internal/session"
	"relayx/phase5/internal/store"
	"relayx/phase5/internal/tenant"
)

// Config controls one delivery worker.
type Config struct {
	WorkerID     string
	Shards       int
	ShardIndex   int
	Batch        int
	LeaseTTL     time.Duration
	ScanInterval time.Duration
	MaxAttempts  int
	BackoffBase  time.Duration
	BackoffMax   time.Duration
	ExpireBatch  int
	Logger       *slog.Logger
}

// Stats is a point-in-time counter snapshot.
type Stats struct {
	Claimed   int64
	Sent      int64
	Retried   int64
	Failed    int64
	Throttled int64
	Expired   int64
	NoRoute   int64
	Released  int64
}

// Forwarder pushes one delivery to the node that owns the session. *cluster.Hub
// satisfies it; tests supply a fake.
type Forwarder interface {
	ForwardDeliver(ctx context.Context, nodeID, addr string, msgID uint64, body *protocol.NodeDeliverBody) error
}

// Engine is the lease-based delivery scheduler.
type Engine struct {
	cfg       Config
	store     *store.Store
	sessions  *session.Manager
	deliverer *delivery.Deliverer
	routes    registry.Registry
	forwarder Forwarder
	limiter   *tenant.Limiter

	claimed   atomic.Int64
	sent      atomic.Int64
	retried   atomic.Int64
	failed    atomic.Int64
	throttled atomic.Int64
	expired   atomic.Int64
	noRoute   atomic.Int64
	released  atomic.Int64

	wake chan struct{}
}

// New returns an engine. routes/forwarder may both be nil for a worker that can
// only reach local sessions; limiter may be nil to skip tenant throttling.
func New(
	cfg Config,
	st *store.Store,
	sessions *session.Manager,
	deliverer *delivery.Deliverer,
	routes registry.Registry,
	forwarder Forwarder,
	limiter *tenant.Limiter,
) *Engine {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Batch <= 0 {
		cfg.Batch = 100
	}
	if cfg.Shards <= 0 {
		cfg.Shards = 1
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 30 * time.Second
	}
	if cfg.ScanInterval <= 0 {
		cfg.ScanInterval = time.Second
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = time.Second
	}
	if cfg.BackoffMax < cfg.BackoffBase {
		cfg.BackoffMax = cfg.BackoffBase
	}
	if cfg.ExpireBatch <= 0 {
		cfg.ExpireBatch = 100
	}
	return &Engine{
		cfg:       cfg,
		store:     st,
		sessions:  sessions,
		deliverer: deliverer,
		routes:    routes,
		forwarder: forwarder,
		limiter:   limiter,
		wake:      make(chan struct{}, 1),
	}
}

// Notify asks the run loop for an early scan. It never blocks; a pending wake-up
// satisfies any number of callers.
func (e *Engine) Notify() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

// Run scans until ctx is cancelled.
func (e *Engine) Run(ctx context.Context) error {
	ticker := time.NewTicker(e.cfg.ScanInterval)
	defer ticker.Stop()
	for {
		if err := e.RunOnce(ctx); err != nil && ctx.Err() == nil {
			e.cfg.Logger.Warn("delivery scan failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-e.wake:
		}
	}
}

// RunOnce performs one claim-and-push cycle plus one expiry sweep.
func (e *Engine) RunOnce(ctx context.Context) error {
	if e.store == nil {
		return fmt.Errorf("engine: store unavailable")
	}
	now := time.Now()
	items, err := e.store.ClaimDueDeliveries(ctx, now, e.cfg.Shards, e.cfg.ShardIndex, e.cfg.Batch, e.cfg.WorkerID, e.cfg.LeaseTTL)
	if err != nil {
		return err
	}
	e.claimed.Add(int64(len(items)))
	for i := range items {
		e.process(ctx, &items[i], now)
	}
	e.expireDue(ctx, now)
	return nil
}

// process handles one leased delivery. Every branch clears or renews the lease
// through a conditional update, so a row taken over by another worker is never
// double-counted.
func (e *Engine) process(ctx context.Context, d *store.Delivery, now time.Time) {
	if !d.ExpireAt.After(now) {
		// The expiry sweep owns the transition; releasing the lease keeps the
		// row available if the sweep runs on another worker.
		if err := e.store.ReleaseLease(ctx, d.ID, e.cfg.WorkerID); err != nil {
			e.cfg.Logger.Warn("release expired delivery lease failed", "delivery_id", d.ID, "error", err)
		}
		e.released.Add(1)
		return
	}

	if e.limiter != nil {
		rel, retryAfter, ok, err := e.limiter.Acquire(ctx, d.TenantID, now)
		if err != nil {
			e.cfg.Logger.Warn("tenant quota lookup failed", "tenant_id", d.TenantID, "error", err)
			e.retry(ctx, d, now.Add(e.cfg.BackoffBase), false)
			return
		}
		if !ok {
			e.throttled.Add(1)
			e.retry(ctx, d, now.Add(retryAfter), false)
			return
		}
		defer rel()
	}

	outcome := e.push(ctx, d)
	switch outcome {
	case pushSent:
		owned, attempts, err := e.store.MarkDeliverySentLeased(ctx, d.ID, e.cfg.WorkerID)
		if err != nil {
			e.cfg.Logger.Warn("mark delivery sent failed", "delivery_id", d.ID, "error", err)
			return
		}
		if !owned {
			e.released.Add(1)
			return
		}
		e.sent.Add(1)
		e.cfg.Logger.Debug("delivery sent", "delivery_id", d.ID, "message_id", d.MessageID,
			"user_id", d.UserID, "device_id", d.DeviceID, "attempts", attempts)
	case pushNoRoute:
		e.noRoute.Add(1)
		e.retry(ctx, d, now.Add(e.cfg.BackoffBase), false)
	case pushFailed:
		e.retry(ctx, d, now.Add(e.backoff(d.Attempts)), true)
	}
}

type pushOutcome int

const (
	pushSent pushOutcome = iota
	pushNoRoute
	pushFailed
)

// push delivers one row through the local session manager or a peer node.
func (e *Engine) push(ctx context.Context, d *store.Delivery) pushOutcome {
	if e.sessions != nil && e.deliverer != nil {
		if sess := e.sessions.FindByDevice(d.UserID, d.DeviceID); sess != nil {
			if err := e.deliverer.Deliver(sess, d.MessageID, d.FromUser, d.FromDevice, d.Payload); err != nil {
				e.cfg.Logger.Debug("local delivery failed", "delivery_id", d.ID, "error", err)
				return pushFailed
			}
			return pushSent
		}
	}

	if e.routes == nil || e.forwarder == nil {
		return pushNoRoute
	}
	route, ok, err := e.routes.LookupRoute(ctx, d.UserID, d.DeviceID)
	if err != nil {
		e.cfg.Logger.Warn("route lookup failed", "delivery_id", d.ID, "error", err)
		return pushFailed
	}
	if !ok {
		return pushNoRoute
	}
	alive, err := e.routes.NodeAlive(ctx, route.NodeID)
	if err != nil {
		e.cfg.Logger.Warn("node liveness lookup failed", "node_id", route.NodeID, "error", err)
		return pushFailed
	}
	if !alive {
		return pushNoRoute
	}
	node, ok, err := e.routes.LookupNode(ctx, route.NodeID)
	if err != nil {
		e.cfg.Logger.Warn("node lookup failed", "node_id", route.NodeID, "error", err)
		return pushFailed
	}
	if !ok {
		return pushNoRoute
	}
	err = e.forwarder.ForwardDeliver(ctx, node.ID, node.Addr, d.MessageID, &protocol.NodeDeliverBody{
		UserID:        d.UserID,
		DeviceID:      d.DeviceID,
		FromSessionID: 0,
		FromUser:      d.FromUser,
		FromDevice:    d.FromDevice,
		Payload:       d.Payload,
	})
	if err != nil {
		e.cfg.Logger.Debug("remote delivery forward failed", "delivery_id", d.ID,
			"node_id", node.ID, "error", err)
		return pushFailed
	}
	return pushSent
}

func (e *Engine) retry(ctx context.Context, d *store.Delivery, nextAt time.Time, increment bool) {
	failed, err := e.store.RetryLeased(ctx, d.ID, e.cfg.WorkerID, nextAt, increment, e.cfg.MaxAttempts)
	if err != nil {
		e.cfg.Logger.Warn("retry delivery failed", "delivery_id", d.ID, "error", err)
		return
	}
	if failed {
		e.failed.Add(1)
		return
	}
	e.retried.Add(1)
}

func (e *Engine) backoff(attempts int) time.Duration {
	d := e.cfg.BackoffBase
	for i := 0; i < attempts; i++ {
		d *= 2
		if d >= e.cfg.BackoffMax {
			return e.cfg.BackoffMax
		}
	}
	return d
}

func (e *Engine) expireDue(ctx context.Context, now time.Time) {
	ids, err := e.store.ListExpiredMessages(ctx, now, e.cfg.ExpireBatch)
	if err != nil {
		e.cfg.Logger.Warn("scan expired messages failed", "error", err)
		return
	}
	for _, id := range ids {
		if err := e.store.ExpireMessage(ctx, id); err != nil {
			e.cfg.Logger.Warn("expire message failed", "message_id", id, "error", err)
			continue
		}
		e.expired.Add(1)
	}
}

// Snapshot returns the current counters.
func (e *Engine) Snapshot() Stats {
	return Stats{
		Claimed:   e.claimed.Load(),
		Sent:      e.sent.Load(),
		Retried:   e.retried.Load(),
		Failed:    e.failed.Load(),
		Throttled: e.throttled.Load(),
		Expired:   e.expired.Load(),
		NoRoute:   e.noRoute.Load(),
		Released:  e.released.Load(),
	}
}
