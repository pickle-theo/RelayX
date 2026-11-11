// Package engine runs the Phase 5 delivery scheduler: it leases due deliveries
// from PostgreSQL, tests the tenant quota, resolves the endpoint route and
// pushes through the local session manager or a peer gateway node.
package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"relayx/phase5.5/internal/delivery"
	"relayx/phase5.5/internal/protocol"
	"relayx/phase5.5/internal/registry"
	"relayx/phase5.5/internal/session"
	"relayx/phase5.5/internal/store"
	"relayx/phase5.5/internal/tenant"
)

// Config controls one delivery worker.
type Config struct {
	WorkerID     string
	Batch        int
	LeaseTTL     time.Duration
	ScanInterval time.Duration
	MaxAttempts  int
	BackoffBase  time.Duration
	BackoffMax   time.Duration
	ExpireBatch  int
	Logger       *slog.Logger
}

// ShardProvider returns the virtual shard interval this worker owns. The
// interval comes from the online shard assignment, so the worker set can grow
// or shrink without a restart and without moving rows between workers.
type ShardProvider interface {
	ShardRange(ctx context.Context) (start, end int, err error)
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
	shards    ShardProvider

	claimed   atomic.Int64
	sent      atomic.Int64
	retried   atomic.Int64
	failed    atomic.Int64
	throttled atomic.Int64
	expired   atomic.Int64
	noRoute   atomic.Int64
	released  atomic.Int64
	renewed   atomic.Int64

	stopping atomic.Bool
	done     chan struct{}

	mu       sync.Mutex
	inFlight map[uint64]struct{}

	wake chan struct{}
}

// New returns an engine. routes/forwarder may both be nil for a worker that can
// only reach local sessions; limiter may be nil to skip tenant throttling;
// shards may be nil to scan the whole virtual shard space.
func New(
	cfg Config,
	st *store.Store,
	sessions *session.Manager,
	deliverer *delivery.Deliverer,
	routes registry.Registry,
	forwarder Forwarder,
	limiter *tenant.Limiter,
	shards ShardProvider,
) *Engine {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Batch <= 0 {
		cfg.Batch = 100
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
		shards:    shards,
		done:      make(chan struct{}),
		inFlight:  make(map[uint64]struct{}),
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

// Run scans until ctx is cancelled or the engine is stopped. The lease
// renewer and the expiry sweeper run in their own goroutines so a slow sweep
// never delays a delivery.
func (e *Engine) Run(ctx context.Context) error {
	defer close(e.done)

	renewCtx, cancelRenew := context.WithCancel(ctx)
	defer cancelRenew()
	go e.runLeaseRenewer(renewCtx)
	go e.runExpirySweeper(renewCtx)

	ticker := time.NewTicker(e.cfg.ScanInterval)
	defer ticker.Stop()
	for {
		if e.stopping.Load() {
			return nil
		}
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

// RunOnce performs one claim-and-push cycle.
func (e *Engine) RunOnce(ctx context.Context) error {
	if e.store == nil {
		return fmt.Errorf("engine: store unavailable")
	}
	start, end := 0, store.DefaultShardSpace
	if e.shards != nil {
		s, en, err := e.shards.ShardRange(ctx)
		if err != nil {
			// Without an interval this worker must not claim anything: the
			// assignment is still being computed or the worker lost it. Waiting
			// for the next scan is cheaper than scanning the whole space and
			// competing with the workers that do own an interval.
			e.cfg.Logger.Debug("shard range unavailable, skipping this scan", "error", err)
			return nil
		}
		if en > s {
			start, end = s, en
		}
	}
	items, err := e.store.ClaimDueDeliveries(ctx, start, end, e.cfg.Batch, e.cfg.WorkerID, e.cfg.LeaseTTL)
	if err != nil {
		return err
	}
	e.claimed.Add(int64(len(items)))
	for i := range items {
		e.process(ctx, &items[i])
	}
	return nil
}

// process handles one leased delivery. Every branch clears or renews the lease
// through a conditional update, so a row taken over by another worker is never
// double-counted.
func (e *Engine) process(ctx context.Context, d *store.Delivery) {
	e.track(d.ID)
	defer e.untrack(d.ID)

	if e.limiter != nil {
		rel, retryAfter, ok, err := e.limiter.Acquire(ctx, d.TenantID, time.Now())
		if err != nil {
			e.cfg.Logger.Warn("tenant quota lookup failed", "tenant_id", d.TenantID, "error", err)
			e.retry(ctx, d, e.cfg.BackoffBase, false)
			return
		}
		if !ok {
			e.throttled.Add(1)
			e.retry(ctx, d, retryAfter, false)
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
		e.retry(ctx, d, e.cfg.BackoffBase, false)
	case pushFailed:
		e.retry(ctx, d, e.backoff(d.Attempts), true)
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

// retry postpones a leased delivery by delay. The retry time is computed by
// the database clock, so a skewed worker clock cannot move a row into the past.
func (e *Engine) retry(ctx context.Context, d *store.Delivery, delay time.Duration, increment bool) {
	failed, err := e.store.RetryLeased(ctx, d.ID, e.cfg.WorkerID, delay, increment, e.cfg.MaxAttempts)
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

// runExpirySweeper expires the messages whose TTL has passed. It runs in its
// own goroutine so a large backlog of expired rows never delays a delivery.
func (e *Engine) runExpirySweeper(ctx context.Context) {
	interval := e.cfg.ScanInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.expireDue(ctx)
		}
	}
}

// expireDue expires the messages whose TTL has passed. The comparison happens
// inside PostgreSQL, so the database clock decides what is expired.
func (e *Engine) expireDue(ctx context.Context) {
	ids, err := e.store.ListExpiredMessages(ctx, e.cfg.ExpireBatch)
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

// runLeaseRenewer extends the leases of the in-flight deliveries. Without it a
// slow push would lose its lease to another worker and be delivered twice.
func (e *Engine) runLeaseRenewer(ctx context.Context) {
	interval := e.cfg.LeaseTTL / 3
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.renewLeases(ctx)
		}
	}
}

// renewLeases extends every lease this worker still holds.
func (e *Engine) renewLeases(ctx context.Context) {
	ids := e.inFlightIDs()
	if len(ids) == 0 {
		return
	}
	if err := e.store.RenewLeases(ctx, e.cfg.WorkerID, ids, e.cfg.LeaseTTL); err != nil {
		e.cfg.Logger.Warn("renew leases failed", "count", len(ids), "error", err)
		return
	}
	e.renewed.Add(int64(len(ids)))
}

// track records that a delivery is being processed.
func (e *Engine) track(id uint64) {
	e.mu.Lock()
	e.inFlight[id] = struct{}{}
	e.mu.Unlock()
}

// untrack records that a delivery is no longer being processed.
func (e *Engine) untrack(id uint64) {
	e.mu.Lock()
	delete(e.inFlight, id)
	e.mu.Unlock()
}

// inFlightIDs returns the deliveries this worker currently holds a lease on.
func (e *Engine) inFlightIDs() []uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.inFlight) == 0 {
		return nil
	}
	ids := make([]uint64, 0, len(e.inFlight))
	for id := range e.inFlight {
		ids = append(ids, id)
	}
	return ids
}

// Stop asks the run loop to stop claiming new work. The deliveries that are
// already in flight are finished first.
func (e *Engine) Stop() {
	e.stopping.Store(true)
}

// Wait blocks until the run loop has stopped or the context expires.
func (e *Engine) Wait(ctx context.Context) error {
	select {
	case <-e.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Shutdown stops the engine and releases every lease it still holds, so the
// next worker can pick the rows up immediately instead of waiting for the
// lease to expire.
func (e *Engine) Shutdown(ctx context.Context) error {
	e.Stop()
	if err := e.Wait(ctx); err != nil {
		return err
	}
	if e.store == nil {
		return nil
	}
	released, err := e.store.ReleaseAllLeases(ctx, e.cfg.WorkerID)
	if err != nil {
		return err
	}
	if released > 0 {
		e.cfg.Logger.Info("released leases on shutdown", "count", released)
	}
	return nil
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
