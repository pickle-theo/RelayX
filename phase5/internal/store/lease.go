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
// The returned rows carry the tenant and message fields the engine needs to
// route and push them.
func (s *Store) ClaimDueDeliveries(
	ctx context.Context,
	now time.Time,
	shards, shardIndex, limit int,
	workerID string,
	leaseTTL time.Duration,
) ([]Delivery, error) {
	if limit <= 0 || shards < 1 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
WITH due AS (
    SELECT d.id
    FROM deliveries d
    WHERE d.state IN (0, 1)
      AND (d.next_retry_at IS NULL OR d.next_retry_at <= $1)
      AND (d.lease_until IS NULL OR d.lease_until <= $1)
      AND mod(d.id, $2) = $3
    ORDER BY d.next_retry_at NULLS FIRST, d.id
    LIMIT $4
    FOR UPDATE SKIP LOCKED
), leased AS (
    UPDATE deliveries d
    SET lease_until = $5, worker_id = $6, updated_at = now()
    FROM due
    WHERE d.id = due.id
    RETURNING d.id, d.message_id, d.user_id, d.device_id, d.state, d.attempts
)
SELECT l.id, l.message_id, m.tenant_id, l.user_id, l.device_id, l.state, l.attempts,
       m.from_user, m.from_device, m.payload, m.created_at, m.expire_at
FROM leased l
JOIN messages m ON m.id = l.message_id
ORDER BY l.id`,
		now, int64(shards), int64(shardIndex), limit, now.Add(leaseTTL), workerID)
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

// RenewLeases extends the lease of the given rows when they are still owned by
// workerID. Rows whose lease was taken over are silently skipped.
func (s *Store) RenewLeases(ctx context.Context, workerID string, ids []uint64, until time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `
UPDATE deliveries
SET lease_until = $1
WHERE worker_id = $2 AND id = ANY($3::bigint[]) AND state IN (0, 1)`,
		until, workerID, ids); err != nil {
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

// RetryLeased reschedules a leased delivery. When incrementAttempts is true the
// attempt counter grows and a delivery that reaches maxAttempts becomes FAILED;
// otherwise only the retry time moves and the state is preserved, which is how
// no-route and tenant-throttled rows back off without consuming attempts.
func (s *Store) RetryLeased(
	ctx context.Context,
	id uint64,
	workerID string,
	nextAt time.Time,
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
    next_retry_at = $3,
    lease_until = NULL,
    worker_id = NULL,
    updated_at = now()
WHERE id = $1 AND worker_id = $2 AND state IN (0, 1)
RETURNING state`,
		int64(id), workerID, nextAt, incrementAttempts, maxAttempts).Scan(&state)
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
