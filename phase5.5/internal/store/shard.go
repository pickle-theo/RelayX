package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ShardOwner is the virtual shard interval owned by one worker.
type ShardOwner struct {
	WorkerID   string
	ShardStart int
	ShardEnd   int
	Version    int64
	LeaseUntil time.Time
}

// Contains reports whether a virtual shard id falls into the interval.
func (o ShardOwner) Contains(shardID int) bool {
	return shardID >= o.ShardStart && shardID < o.ShardEnd
}

// ShardWorker is the liveness record of one delivery worker.
type ShardWorker struct {
	WorkerID   string
	LeaseUntil time.Time
}

// HeartbeatShardWorker registers or refreshes the liveness lease of a worker.
func (s *Store) HeartbeatShardWorker(ctx context.Context, workerID string, leaseTTL time.Duration) error {
	if _, err := s.db.ExecContext(ctx, `
INSERT INTO shard_workers (worker_id, lease_until, updated_at)
VALUES ($1, now() + make_interval(secs => $2::float8), now())
ON CONFLICT (worker_id) DO UPDATE SET
    lease_until = EXCLUDED.lease_until,
    updated_at = now()`,
		workerID, leaseTTL.Seconds()); err != nil {
		return fmt.Errorf("store: heartbeat shard worker %s: %w", workerID, err)
	}
	return nil
}

// ListActiveShardWorkers returns the workers whose liveness lease is still
// valid, ordered by their id so the assignment is deterministic.
func (s *Store) ListActiveShardWorkers(ctx context.Context) ([]ShardWorker, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT worker_id, lease_until
FROM shard_workers
WHERE lease_until > now()
ORDER BY worker_id`)
	if err != nil {
		return nil, fmt.Errorf("store: list active shard workers: %w", err)
	}
	defer rows.Close()

	var out []ShardWorker
	for rows.Next() {
		var w ShardWorker
		if err := rows.Scan(&w.WorkerID, &w.LeaseUntil); err != nil {
			return nil, fmt.Errorf("store: scan shard worker: %w", err)
		}
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate shard workers: %w", err)
	}
	return out, nil
}

// RemoveShardWorker drops the liveness record of a worker.
func (s *Store) RemoveShardWorker(ctx context.Context, workerID string) error {
	if _, err := s.db.ExecContext(ctx, `
DELETE FROM shard_workers WHERE worker_id = $1`, workerID); err != nil {
		return fmt.Errorf("store: remove shard worker %s: %w", workerID, err)
	}
	return nil
}

// ListShardOwners returns every shard assignment, valid or not.
func (s *Store) ListShardOwners(ctx context.Context) ([]ShardOwner, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT worker_id, shard_start, shard_end, version, COALESCE(lease_until, 'epoch'::timestamptz)
FROM shard_assignments
ORDER BY worker_id`)
	if err != nil {
		return nil, fmt.Errorf("store: list shard owners: %w", err)
	}
	defer rows.Close()

	var out []ShardOwner
	for rows.Next() {
		var o ShardOwner
		if err := rows.Scan(&o.WorkerID, &o.ShardStart, &o.ShardEnd, &o.Version, &o.LeaseUntil); err != nil {
			return nil, fmt.Errorf("store: scan shard owner: %w", err)
		}
		out = append(out, o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate shard owners: %w", err)
	}
	return out, nil
}

// UpsertShardOwner writes the interval of one worker. The version is bumped so a
// later write always wins over an older one.
func (s *Store) UpsertShardOwner(ctx context.Context, owner ShardOwner) error {
	if owner.ShardEnd <= owner.ShardStart {
		return fmt.Errorf("store: invalid shard interval [%d,%d)", owner.ShardStart, owner.ShardEnd)
	}
	if _, err := s.db.ExecContext(ctx, `
INSERT INTO shard_assignments (worker_id, shard_start, shard_end, version, lease_until, updated_at)
VALUES ($1, $2, $3, $4, now() + make_interval(secs => $5::float8), now())
ON CONFLICT (worker_id) DO UPDATE SET
    shard_start = EXCLUDED.shard_start,
    shard_end = EXCLUDED.shard_end,
    version = EXCLUDED.version,
    lease_until = EXCLUDED.lease_until,
    updated_at = now()
WHERE shard_assignments.version <= EXCLUDED.version`,
		owner.WorkerID, owner.ShardStart, owner.ShardEnd, owner.Version, defaultShardLeaseSeconds); err != nil {
		return fmt.Errorf("store: upsert shard owner %s: %w", owner.WorkerID, err)
	}
	return nil
}

// ShardOwnerOf returns the interval of one worker.
func (s *Store) ShardOwnerOf(ctx context.Context, workerID string) (ShardOwner, bool, error) {
	var o ShardOwner
	err := s.db.QueryRowContext(ctx, `
SELECT worker_id, shard_start, shard_end, version, COALESCE(lease_until, 'epoch'::timestamptz)
FROM shard_assignments
WHERE worker_id = $1`, workerID).Scan(&o.WorkerID, &o.ShardStart, &o.ShardEnd, &o.Version, &o.LeaseUntil)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ShardOwner{}, false, nil
		}
		return ShardOwner{}, false, fmt.Errorf("store: get shard owner %s: %w", workerID, err)
	}
	return o, true, nil
}

// DeleteShardOwners removes the assignments that are not owned by any of the
// given workers. It returns the number of removed rows.
func (s *Store) DeleteShardOwners(ctx context.Context, keep []string) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
DELETE FROM shard_assignments
WHERE NOT (worker_id = ANY($1::text[]))`, keep)
	if err != nil {
		return 0, fmt.Errorf("store: delete stale shard owners: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("store: count deleted shard owners: %w", err)
	}
	return n, nil
}

// defaultShardLeaseSeconds is the liveness lease of a shard assignment.
const defaultShardLeaseSeconds = 30.0

// ShardRangeProvider caches the virtual shard interval of one worker. The
// interval is refreshed from the assignment table, so a worker picks up a new
// interval after an online rebalance without a restart.
type ShardRangeProvider struct {
	store    *Store
	workerID string
	cacheTTL time.Duration

	mu        sync.Mutex
	owner     ShardOwner
	fetchedAt time.Time
}

// NewShardRangeProvider returns a provider that caches the interval for
// cacheTTL. A cacheTTL of zero disables the cache.
func NewShardRangeProvider(st *Store, workerID string, cacheTTL time.Duration) *ShardRangeProvider {
	return &ShardRangeProvider{store: st, workerID: workerID, cacheTTL: cacheTTL}
}

// ShardRange returns the half-open interval [start, end) owned by the worker.
func (p *ShardRangeProvider) ShardRange(ctx context.Context) (int, int, error) {
	if p == nil || p.store == nil {
		return 0, 0, fmt.Errorf("store: shard range provider is not configured")
	}

	p.mu.Lock()
	if p.owner.WorkerID != "" && p.cacheTTL > 0 && time.Since(p.fetchedAt) < p.cacheTTL {
		owner := p.owner
		p.mu.Unlock()
		return owner.ShardStart, owner.ShardEnd, nil
	}
	p.mu.Unlock()

	owner, ok, err := p.store.ShardOwnerOf(ctx, p.workerID)
	if err != nil {
		return 0, 0, err
	}
	if !ok {
		return 0, 0, fmt.Errorf("store: no shard assignment for worker %s", p.workerID)
	}
	p.mu.Lock()
	p.owner = owner
	p.fetchedAt = time.Now()
	p.mu.Unlock()
	return owner.ShardStart, owner.ShardEnd, nil
}
