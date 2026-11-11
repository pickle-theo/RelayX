package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ClaimDueDeliveries leases up to limit due deliveries owned by the given scan
// shard. A row is due when it is PENDING or SENT, its retry time is absent or
// reached, and its lease is absent or expired. FOR UPDATE SKIP LOCKED lets
// several workers scan concurrently without claiming the same row.
//
// The shard is a half-open virtual shard interval [shardStart, shardEnd). The
// interval comes from the online shard assignment, so the worker set can grow
// or shrink without a restart and without moving rows.
//
// Every time comparison happens inside PostgreSQL, so the database clock is the
// only arbiter of a lease. A worker with a skewed clock can neither steal a
// live lease nor lose its own lease early.
//
// The returned rows carry the tenant and message fields the engine needs to
// route and push them.
func (s *Store) ClaimDueDeliveries(
	ctx context.Context,
	shardStart, shardEnd int,
	limit int,
	workerID string,
	leaseTTL time.Duration,
) ([]Delivery, error) {
	if limit <= 0 || shardEnd <= shardStart {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
WITH due AS (
    SELECT d.id
    FROM deliveries d
    WHERE d.state IN (0, 1)
      AND (d.next_retry_at IS NULL OR d.next_retry_at <= now())
      AND (d.lease_until IS NULL OR d.lease_until <= now())
      AND d.shard_id >= $1 AND d.shard_id < $2
    ORDER BY d.next_retry_at NULLS FIRST, d.id
    LIMIT $3
    FOR UPDATE SKIP LOCKED
), leased AS (
    UPDATE deliveries d
    SET lease_until = now() + make_interval(secs => $4::float8),
        worker_id = $5,
        updated_at = now()
    FROM due
    WHERE d.id = due.id
    RETURNING d.id, d.message_id, d.user_id, d.device_id, d.state, d.attempts
)
SELECT l.id, l.message_id, m.tenant_id, l.user_id, l.device_id, l.state, l.attempts,
       m.from_user, m.from_device, m.payload, m.created_at, m.expire_at
FROM leased l
JOIN messages m ON m.id = l.message_id
ORDER BY l.id`,
		shardStart, shardEnd, limit, leaseTTL.Seconds(), workerID)
	if err != nil {
		return nil, fmt.Errorf("store: claim due deliveries: %w", err)
	}
	defer rows.Close()

	var items []Delivery
	for rows.Next() {
		var d Delivery
		var state, attempts int16
		if err := rows.Scan(&d.ID, &d.MessageID, &d.TenantID, &d.UserID, &d.DeviceID,
			&state, &attempts, &d.FromUser, &d.FromDevice, &d.Payload, &d.CreatedAt, &d.ExpireAt); err != nil {
			return nil, fmt.Errorf("store: scan claimed delivery: %w", err)
		}
		d.State = DeliveryState(state)
		d.Attempts = int(attempts)
		items = append(items, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate claimed deliveries: %w", err)
	}
	return items, nil
}

// RenewLeases extends the lease of the given rows by ttl when they are still
// owned by workerID. Rows whose lease was taken over are silently skipped.
//
// The new deadline is computed by the database clock, so a renewal never
// shortens or extends a lease because of a skewed worker clock.
func (s *Store) RenewLeases(ctx context.Context, workerID string, ids []uint64, ttl time.Duration) error {
	if len(ids) == 0 {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `
UPDATE deliveries
SET lease_until = now() + make_interval(secs => $3::float8)
WHERE worker_id = $1 AND id = ANY($2::bigint[]) AND state IN (0, 1)`,
		workerID, ids, ttl.Seconds()); err != nil {
		return fmt.Errorf("store: renew leases: %w", err)
	}
	return nil
}

// ReleaseLease drops the lease of one row when workerID still owns it.
func (s *Store) ReleaseLease(ctx context.Context, id uint64, workerID string) error {
	if _, err := s.db.ExecContext(ctx, `
UPDATE deliveries
SET lease_until = NULL, worker_id = NULL, updated_at = now()
WHERE id = $1 AND worker_id = $2 AND state IN (0, 1)`, int64(id), workerID); err != nil {
		return fmt.Errorf("store: release lease %d: %w", id, err)
	}
	return nil
}

// ReleaseAllLeases drops every lease owned by workerID and returns the number
// of released rows. A graceful shutdown calls it so the next worker can pick
// the rows up immediately instead of waiting for the lease to expire.
func (s *Store) ReleaseAllLeases(ctx context.Context, workerID string) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
UPDATE deliveries
SET lease_until = NULL, worker_id = NULL, updated_at = now()
WHERE worker_id = $1 AND state IN (0, 1)`, workerID)
	if err != nil {
		return 0, fmt.Errorf("store: release all leases of %s: %w", workerID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("store: count released leases: %w", err)
	}
	return n, nil
}

// MarkDeliverySentLeased records a successful push of a leased delivery:
// PENDING/SENT becomes SENT with attempts+1 and the usual exponential retry
// schedule. It reports whether this worker still owned the row and the resulting
// attempt count.
func (s *Store) MarkDeliverySentLeased(ctx context.Context, id uint64, workerID string) (bool, int, error) {
	base, max := s.retrySeconds()
	var attempts int
	err := s.db.QueryRowContext(ctx, `
UPDATE deliveries
SET state = 1,
    attempts = attempts + 1,
    next_retry_at = now() + make_interval(secs => LEAST(
        $3::float8 * power(2::float8, attempts::float8), $4::float8)),
    lease_until = NULL,
    worker_id = NULL,
    updated_at = now()
WHERE id = $1 AND worker_id = $2 AND state IN (0, 1)
RETURNING attempts`, int64(id), workerID, base, max).Scan(&attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return false, 0, nil
	}
	if err != nil {
		return false, 0, fmt.Errorf("store: mark delivery %d sent: %w", id, err)
	}
	return true, attempts, nil
}

// RetryLeased reschedules a leased delivery by delay. When incrementAttempts is
// true the attempt counter grows and a delivery that reaches maxAttempts
// becomes FAILED; otherwise only the retry time moves and the state is
// preserved, which is how no-route and tenant-throttled rows back off without
// consuming attempts.
//
// The new retry time is computed by the database clock, so a skewed worker
// clock cannot move a delivery into the past or push it too far into the
// future.
func (s *Store) RetryLeased(
	ctx context.Context,
	id uint64,
	workerID string,
	delay time.Duration,
	incrementAttempts bool,
	maxAttempts int,
) (failed bool, err error) {
	var state int16
	err = s.db.QueryRowContext(ctx, `
UPDATE deliveries
SET state = CASE
        WHEN $4 AND attempts + 1 >= $5 THEN 6
        ELSE state
    END,
    attempts = attempts + CASE WHEN $4 THEN 1 ELSE 0 END,
    next_retry_at = now() + make_interval(secs => $3::float8),
    lease_until = NULL,
    worker_id = NULL,
    updated_at = now()
WHERE id = $1 AND worker_id = $2 AND state IN (0, 1)
RETURNING state`,
		int64(id), workerID, delay.Seconds(), incrementAttempts, maxAttempts).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: retry delivery %d: %w", id, err)
	}
	return DeliveryState(state) == DeliveryFailed, nil
}

// RetryDelay returns the exponential backoff for the given attempt count using
// the store's configured bounds.
func (s *Store) RetryDelay(attempts int) time.Duration {
	base, max := s.retrySeconds()
	secs := base
	for i := 0; i < attempts; i++ {
		secs *= 2
		if secs >= max {
			secs = max
			break
		}
	}
	return time.Duration(secs * float64(time.Second))
}
