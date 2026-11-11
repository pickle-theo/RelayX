package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// rowQuerier is satisfied by both *sql.DB and *sql.Tx.
type rowQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// ApplyReceiptAck moves a SENT delivery to RECEIPT_ACKED and clears its retry
// schedule. It reports whether a real transition happened; a repeated ACK is a
// no-op. The message aggregation is recomputed in the same transaction.
func (s *Store) ApplyReceiptAck(ctx context.Context, userID, deviceID string, messageID uint64) (changed bool, err error) {
	// One statement (an implicit transaction) instead of BEGIN + two UPDATEs +
	// COMMIT: the ACK path is the hottest write path of the gateway. The
	// aggregation still reads the pre-update snapshot; RECEIPT_ACKED is not a
	// terminal state, so the outcome only depends on whether the message already
	// has a terminal delivery.
	res, err := s.db.ExecContext(ctx, `
WITH upd AS (
    UPDATE deliveries
    SET state = 2, next_retry_at = NULL, updated_at = now()
    WHERE message_id = $1 AND user_id = $2 AND device_id = $3 AND state = 1
    RETURNING message_id
)
UPDATE messages m
SET state = CASE
    WHEN m.state = 3 THEN 3
    WHEN EXISTS (SELECT 1 FROM deliveries d
                 WHERE d.message_id = $1 AND d.state IN (3, 4, 5, 6)) THEN 1
    ELSE 0
END
FROM upd
WHERE m.id = upd.message_id`, int64(messageID), userID, deviceID)
	if err != nil {
		return false, fmt.Errorf("store: apply receipt ack: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: apply receipt ack: rows affected: %w", err)
	}
	return affected > 0, nil
}

// ApplyBusinessAck moves a SENT or RECEIPT_ACKED delivery to BUSINESS_ACKED
// (status 0) or BUSINESS_FAILED (status non-zero). It reports whether a real
// transition happened and recomputes the message aggregation in the same
// transaction.
func (s *Store) ApplyBusinessAck(ctx context.Context, userID, deviceID string, messageID uint64, status uint8) (changed bool, err error) {
	target := int16(DeliveryBusinessAcked)
	if status != 0 {
		target = int16(DeliveryBusinessFailed)
	}
	// See ApplyReceiptAck: one implicit-transaction statement. BUSINESS_ACKED
	// and BUSINESS_FAILED are terminal, so the snapshot's terminal count plus
	// this row decides whether the message is COMPLETED or only PARTIAL.
	res, err := s.db.ExecContext(ctx, `
WITH upd AS (
    UPDATE deliveries
    SET state = $4, next_retry_at = NULL, updated_at = now()
    WHERE message_id = $1 AND user_id = $2 AND device_id = $3 AND state IN (1, 2)
    RETURNING message_id
)
UPDATE messages m
SET state = CASE
    WHEN m.state = 3 THEN 3
    WHEN (SELECT count(*) FROM deliveries d WHERE d.message_id = $1) =
         (SELECT count(*) FROM deliveries d
          WHERE d.message_id = $1 AND d.state IN (3, 4, 5, 6)) + 1 THEN 2
    ELSE 1
END
FROM upd
WHERE m.id = upd.message_id`, int64(messageID), userID, deviceID, target)
	if err != nil {
		return false, fmt.Errorf("store: apply business ack: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: apply business ack: rows affected: %w", err)
	}
	return affected > 0, nil
}

// PullPending returns up to limit pending-or-sent deliveries for one endpoint
// with id > cursor. The returned rows are marked SENT, get attempts+1 and a new
// retry time; the extra probe row used for hasMore is neither updated nor
// returned.
func (s *Store) PullPending(ctx context.Context, userID, deviceID string, cursor uint64, limit int) ([]Delivery, bool, error) {
	if limit <= 0 {
		return nil, false, nil
	}
	base, max := s.retrySeconds()

	var (
		items   []Delivery
		hasMore bool
	)
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		rows, err := queryDeliveries(ctx, tx, `
SELECT d.id, d.message_id, d.user_id, d.device_id, d.state, d.attempts,
       m.from_user, m.from_device, m.payload, m.created_at, m.expire_at
FROM deliveries d
JOIN messages m ON m.id = d.message_id
WHERE d.user_id = $1 AND d.device_id = $2 AND d.state IN (0, 1) AND d.id > $3
ORDER BY d.id
LIMIT $4
FOR UPDATE OF d`, userID, deviceID, int64(cursor), limit+1)
		if err != nil {
			return fmt.Errorf("select pending deliveries: %w", err)
		}
		if len(rows) > limit {
			hasMore = true
			rows = rows[:limit]
		}
		for i := range rows {
			if _, err := tx.ExecContext(ctx, `
UPDATE deliveries
SET state = 1, attempts = attempts + 1,
    next_retry_at = now() + make_interval(secs => LEAST($2::float8 * power(2::float8, attempts::float8), $3::float8)),
    updated_at = now()
WHERE id = $1`, int64(rows[i].ID), base, max); err != nil {
				return fmt.Errorf("mark delivery sent: %w", err)
			}
			rows[i].State = DeliverySent
			rows[i].Attempts++
		}
		items = rows
		return nil
	})
	if err != nil {
		return nil, false, fmt.Errorf("store: pull pending: %w", err)
	}
	return items, hasMore, nil
}

// MarkDeliverySent records one realtime push of a PENDING delivery: the row
// moves to SENT, attempts is incremented and the retry schedule is set with the
// same exponential backoff PULL uses. Rows that are no longer PENDING are left
// untouched, so a concurrent PULL or ACK always wins.
func (s *Store) MarkDeliverySent(ctx context.Context, messageID uint64, userID, deviceID string) error {
	base, max := s.retrySeconds()
	if _, err := s.db.ExecContext(ctx, `
UPDATE deliveries
SET state = 1, attempts = attempts + 1,
    next_retry_at = now() + make_interval(secs => LEAST($4::float8 * power(2::float8, attempts::float8), $5::float8)),
    updated_at = now()
WHERE message_id = $1 AND user_id = $2 AND device_id = $3 AND state = 0`,
		int64(messageID), userID, deviceID, base, max); err != nil {
		return fmt.Errorf("store: mark delivery sent for message %d: %w", messageID, err)
	}
	return nil
}

// DueRetries returns SENT deliveries whose next_retry_at is due, ordered by
// next_retry_at then id. It does not change any row.
func (s *Store) DueRetries(ctx context.Context, now time.Time, limit int) ([]Delivery, error) {
	items, err := queryDeliveries(ctx, s.db, `
SELECT d.id, d.message_id, d.user_id, d.device_id, d.state, d.attempts,
       m.from_user, m.from_device, m.payload, m.created_at, m.expire_at
FROM deliveries d
JOIN messages m ON m.id = d.message_id
WHERE d.state = 1 AND d.next_retry_at IS NOT NULL AND d.next_retry_at <= $1
ORDER BY d.next_retry_at, d.id
LIMIT $2`, now, limit)
	if err != nil {
		return nil, fmt.Errorf("store: due retries: %w", err)
	}
	return items, nil
}

// MarkDeliveryRetried records one retry push for a still-SENT delivery.
func (s *Store) MarkDeliveryRetried(ctx context.Context, id uint64) error {
	base, max := s.retrySeconds()
	if _, err := s.db.ExecContext(ctx, `
UPDATE deliveries
SET attempts = attempts + 1,
    next_retry_at = now() + make_interval(secs => LEAST($2::float8 * power(2::float8, attempts::float8), $3::float8)),
    updated_at = now()
WHERE id = $1 AND state = 1`, int64(id), base, max); err != nil {
		return fmt.Errorf("store: mark delivery %d retried: %w", id, err)
	}
	return nil
}

// ClearRetrySchedule stops retries for a still-SENT delivery, typically because
// its target is offline and will recover through PULL.
func (s *Store) ClearRetrySchedule(ctx context.Context, id uint64) error {
	if _, err := s.db.ExecContext(ctx, `
UPDATE deliveries
SET next_retry_at = NULL
WHERE id = $1 AND state = 1`, int64(id)); err != nil {
		return fmt.Errorf("store: clear retry schedule for delivery %d: %w", id, err)
	}
	return nil
}

// MarkDeliveryFailed moves a PENDING or SENT delivery to FAILED and recomputes
// the owning message aggregation in the same transaction.
func (s *Store) MarkDeliveryFailed(ctx context.Context, id uint64) error {
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		var messageID int64
		err := tx.QueryRowContext(ctx, `
UPDATE deliveries
SET state = 6, next_retry_at = NULL, updated_at = now()
WHERE id = $1 AND state IN (0, 1)
RETURNING message_id`, int64(id)).Scan(&messageID)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("update delivery: %w", err)
		}
		return recomputeMessageState(ctx, tx, uint64(messageID))
	})
	if err != nil {
		return fmt.Errorf("store: mark delivery %d failed: %w", id, err)
	}
	return nil
}

// CountPendingDeliveries counts deliveries that still need attention
// (PENDING or SENT).
func (s *Store) CountPendingDeliveries(ctx context.Context) (int64, error) {
	var count int64
	if err := s.db.QueryRowContext(ctx, `
SELECT count(*) FROM deliveries WHERE state IN (0, 1)`).Scan(&count); err != nil {
		return 0, fmt.Errorf("store: count pending deliveries: %w", err)
	}
	return count, nil
}

// retrySeconds exposes the retry backoff bounds in seconds for SQL.
func (s *Store) retrySeconds() (base, max float64) {
	return s.retryBase.Seconds(), s.retryMax.Seconds()
}

func queryDeliveries(ctx context.Context, q rowQuerier, query string, args ...any) ([]Delivery, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []Delivery
	for rows.Next() {
		item, err := scanDelivery(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func scanDelivery(rows *sql.Rows) (Delivery, error) {
	var (
		item    Delivery
		state   int16
		payload []byte
	)
	if err := rows.Scan(&item.ID, &item.MessageID, &item.UserID, &item.DeviceID, &state, &item.Attempts,
		&item.FromUser, &item.FromDevice, &payload, &item.CreatedAt, &item.ExpireAt); err != nil {
		return Delivery{}, err
	}
	item.State = DeliveryState(state)
	item.Payload = append([]byte(nil), payload...)
	return item, nil
}
