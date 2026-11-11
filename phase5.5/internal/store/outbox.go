package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// RelayOutbox claims one batch of due outbox rows with FOR UPDATE SKIP LOCKED,
// publishes each event and marks it published. A publish failure records the
// attempt and a backoff on the row, stops the batch and is returned to the
// caller so it can wait before the next attempt.
//
// The row locks are held until the transaction commits, so two relay instances
// can never publish the same row at the same time.
func (s *Store) RelayOutbox(
	ctx context.Context,
	limit int,
	backoffBase, backoffMax time.Duration,
	publish func(ctx context.Context, ev OutboxEvent) error,
) (published int, err error) {
	if limit <= 0 {
		return 0, nil
	}
	var publishErr error

	txErr := s.withTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `
SELECT id, topic, partition_key, payload, attempts
FROM outbox
WHERE state = 0 AND next_attempt_at <= now()
ORDER BY id
LIMIT $1
FOR UPDATE SKIP LOCKED`, limit)
		if err != nil {
			return fmt.Errorf("select pending outbox: %w", err)
		}
		defer rows.Close()

		var pending []OutboxEvent
		for rows.Next() {
			var ev OutboxEvent
			if err := rows.Scan(&ev.ID, &ev.Topic, &ev.PartitionKey, &ev.Payload, &ev.Attempts); err != nil {
				return fmt.Errorf("scan outbox: %w", err)
			}
			pending = append(pending, ev)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate outbox: %w", err)
		}

		for i := range pending {
			if err := publish(ctx, pending[i]); err != nil {
				publishErr = err
				if _, uerr := tx.ExecContext(ctx, `
UPDATE outbox
SET attempts = attempts + 1,
    next_attempt_at = now() + make_interval(secs => LEAST(
        $2::float8 * power(2::float8, attempts::float8),
        $3::float8))
WHERE id = $1`,
					int64(pending[i].ID), backoffBase.Seconds(), backoffMax.Seconds()); uerr != nil {
					return fmt.Errorf("record outbox failure: %w", uerr)
				}
				return nil
			}
			if _, err := tx.ExecContext(ctx, `
UPDATE outbox
SET state = 1, published_at = now(), attempts = attempts + 1
WHERE id = $1`, int64(pending[i].ID)); err != nil {
				return fmt.Errorf("mark outbox published: %w", err)
			}
			published++
		}
		return nil
	})
	if txErr != nil {
		return 0, fmt.Errorf("store: relay outbox: %w", txErr)
	}
	return published, publishErr
}

// CountPendingOutbox returns the number of outbox rows still waiting to be
// published.
func (s *Store) CountPendingOutbox(ctx context.Context) (int64, error) {
	var n int64
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM outbox WHERE state = 0`).Scan(&n); err != nil {
		return 0, fmt.Errorf("store: count pending outbox: %w", err)
	}
	return n, nil
}

// CountLeasedDeliveries returns the number of deliveries currently under a
// valid lease.
func (s *Store) CountLeasedDeliveries(ctx context.Context, now time.Time) (int64, error) {
	var n int64
	if err := s.db.QueryRowContext(ctx, `
SELECT count(*) FROM deliveries WHERE lease_until IS NOT NULL AND lease_until > $1`, now).Scan(&n); err != nil {
		return 0, fmt.Errorf("store: count leased deliveries: %w", err)
	}
	return n, nil
}
