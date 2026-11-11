// Package shard keeps the virtual shard assignment in sync with the live
// delivery workers.
//
// The virtual shard space is fixed. Every delivery row is mapped to a stable
// virtual shard by its message id, and a worker owns a half-open interval of
// that space. Growing or shrinking the worker set therefore only rewrites the
// interval table: no row is moved, because the shard of a row never changes.
package shard

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"relayx/phase5.5/internal/store"
)

// Rebalancer splits the virtual shard space across the live workers and
// rewrites the assignment whenever the worker set changes.
type Rebalancer struct {
	store    *store.Store
	space    int
	interval time.Duration
	leaseTTL time.Duration
	logger   *slog.Logger
}

// NewRebalancer returns a rebalancer. A space of zero selects the default
// virtual shard space and an interval of zero selects the default period.
func NewRebalancer(st *store.Store, space int, interval, leaseTTL time.Duration, logger *slog.Logger) *Rebalancer {
	if space <= 0 {
		space = store.DefaultShardSpace
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Rebalancer{store: st, space: space, interval: interval, leaseTTL: leaseTTL, logger: logger}
}

// Run rebalances until the context is cancelled.
func (r *Rebalancer) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := r.Rebalance(ctx); err != nil && ctx.Err() == nil {
				r.logger.Warn("shard rebalance failed", "error", err)
			}
		}
	}
}

// Rebalance performs one rebalance pass. It is safe to run on every worker:
// the assignment is derived from the same ordered worker list, so all workers
// compute the same result.
func (r *Rebalancer) Rebalance(ctx context.Context) error {
	if r.store == nil {
		return fmt.Errorf("shard: store is not configured")
	}
	workers, err := r.store.ListActiveShardWorkers(ctx)
	if err != nil {
		return err
	}
	if len(workers) == 0 {
		return nil
	}
	ids := make([]string, 0, len(workers))
	for _, w := range workers {
		ids = append(ids, w.WorkerID)
	}

	targets := ComputeAssignments(ids, r.space)
	current, err := r.store.ListShardOwners(ctx)
	if err != nil {
		return err
	}
	currentByWorker := make(map[string]store.ShardOwner, len(current))
	for _, o := range current {
		currentByWorker[o.WorkerID] = o
	}

	for _, t := range targets {
		cur, ok := currentByWorker[t.WorkerID]
		if ok && cur.ShardStart == t.ShardStart && cur.ShardEnd == t.ShardEnd {
			continue
		}
		t.Version = cur.Version + 1
		if err := r.store.UpsertShardOwner(ctx, t); err != nil {
			return err
		}
		r.logger.Info("shard assignment updated",
			"worker_id", t.WorkerID, "shard_start", t.ShardStart, "shard_end", t.ShardEnd,
			"version", t.Version)
	}

	if _, err := r.store.DeleteShardOwners(ctx, ids); err != nil {
		return err
	}
	return nil
}

// ComputeAssignments splits [0, space) across the workers. The workers are
// sorted first, so the same worker set always produces the same intervals.
// A worker that would receive an empty interval is left without an
// assignment: there is nothing for it to claim.
func ComputeAssignments(workers []string, space int) []store.ShardOwner {
	if len(workers) == 0 || space <= 0 {
		return nil
	}
	sorted := append([]string(nil), workers...)
	sort.Strings(sorted)

	n := len(sorted)
	base := space / n
	rem := space % n
	out := make([]store.ShardOwner, 0, n)
	start := 0
	for i, w := range sorted {
		size := base
		if i < rem {
			size++
		}
		if size == 0 {
			continue
		}
		out = append(out, store.ShardOwner{
			WorkerID:   w,
			ShardStart: start,
			ShardEnd:   start + size,
			Version:    1,
		})
		start += size
	}
	return out
}
