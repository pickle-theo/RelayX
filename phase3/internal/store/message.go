package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// AcceptMessage persists one logical message together with all delivery rows
// in a single statement (implicit transaction). When (from_user, client_msg_id)
// already exists the original message ID is returned with Duplicate set and
// nothing is written: neither the original message nor its deliveries are
// touched.
func (s *Store) AcceptMessage(ctx context.Context, req AcceptRequest) (AcceptResult, error) {
	if len(req.Endpoints) == 0 {
		return AcceptResult{}, ErrNoRecipients
	}
	payload := req.Payload
	if payload == nil {
		payload = []byte{}
	}

	// Deduplicate the recipient list: the statement below inserts one delivery
	// row per element of the three arrays.
	users := make([]string, 0, len(req.Endpoints))
	devices := make([]string, 0, len(req.Endpoints))
	online := make([]bool, 0, len(req.Endpoints))
	seen := make(map[Endpoint]struct{}, len(req.Endpoints))
	for _, ep := range req.Endpoints {
		if _, ok := seen[ep]; ok {
			continue
		}
		seen[ep] = struct{}{}
		users = append(users, ep.UserID)
		devices = append(devices, ep.DeviceID)
		online = append(online, req.Online[ep])
	}
	base, _ := s.retrySeconds()

	// One statement keeps the message and its deliveries atomic without the
	// BEGIN/COMMIT round trips: on a busy gateway those round trips, not the
	// SQL itself, dominate the per-message cost.
	var messageID uint64
	err := s.db.QueryRowContext(ctx, `
WITH msg AS (
    INSERT INTO messages (client_msg_id, from_user, from_device, target_type, target, payload, expire_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (from_user, client_msg_id) DO NOTHING
    RETURNING id
), created AS (
    INSERT INTO deliveries (message_id, user_id, device_id, state, attempts, next_retry_at)
    SELECT msg.id, u.user_id, u.device_id,
           CASE WHEN u.online THEN 1 ELSE 0 END,
           CASE WHEN u.online THEN 1 ELSE 0 END,
           CASE WHEN u.online THEN now() + make_interval(secs => $8) ELSE NULL END
    FROM msg
    CROSS JOIN unnest($9::text[], $10::text[], $11::boolean[]) AS u(user_id, device_id, online)
    ON CONFLICT (message_id, user_id, device_id) DO NOTHING
    RETURNING 1
)
SELECT id FROM msg`,
		int64(req.ClientMsgID), req.FromUser, req.FromDevice, int16(req.TargetType),
		req.Target, payload, req.ExpireAt, base, users, devices, online).Scan(&messageID)
	if errors.Is(err, sql.ErrNoRows) {
		// The unique key already exists: report the original message ID without
		// touching the stored message or its deliveries.
		if err := s.db.QueryRowContext(ctx, `
SELECT id FROM messages WHERE from_user = $1 AND client_msg_id = $2`,
			req.FromUser, int64(req.ClientMsgID)).Scan(&messageID); err != nil {
			return AcceptResult{}, fmt.Errorf("store: accept message: select existing message: %w", err)
		}
		return AcceptResult{MessageID: messageID, Duplicate: true}, nil
	}
	if err != nil {
		return AcceptResult{}, fmt.Errorf("store: accept message: %w", err)
	}
	return AcceptResult{MessageID: messageID}, nil
}

// ListExpiredMessages returns the IDs of messages whose expire_at has passed
// and which are still non-terminal, ordered by expire_at.
func (s *Store) ListExpiredMessages(ctx context.Context, now time.Time, limit int) ([]uint64, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id
FROM messages
WHERE expire_at <= $1 AND state IN (0, 1)
ORDER BY expire_at
LIMIT $2`, now, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list expired messages: %w", err)
	}
	defer rows.Close()

	var ids []uint64
	for rows.Next() {
		var id uint64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: scan expired message: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list expired messages: %w", err)
	}
	return ids, nil
}

// ExpireMessage expires one message: every non-terminal delivery becomes
// EXPIRED and the message itself becomes EXPIRED, in a single transaction.
func (s *Store) ExpireMessage(ctx context.Context, messageID uint64) error {
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
UPDATE deliveries
SET state = 5, next_retry_at = NULL, updated_at = now()
WHERE message_id = $1 AND state IN (0, 1, 2)`, int64(messageID)); err != nil {
			return fmt.Errorf("expire deliveries: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE messages SET state = 3 WHERE id = $1`, int64(messageID)); err != nil {
			return fmt.Errorf("expire message: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("store: expire message %d: %w", messageID, err)
	}
	return nil
}

// recomputeMessageState recomputes messages.state from its delivery rows. It
// must run inside the transaction that changed a delivery. An EXPIRED message
// is never overwritten, terminal delivery states are 3..6.
func recomputeMessageState(ctx context.Context, tx *sql.Tx, messageID uint64) error {
	if _, err := tx.ExecContext(ctx, `
WITH agg AS (
    SELECT count(*) AS total,
           count(*) FILTER (WHERE state IN (3, 4, 5, 6)) AS terminal
    FROM deliveries
    WHERE message_id = $1
)
UPDATE messages
SET state = CASE
    WHEN messages.state = 3 THEN 3
    WHEN agg.total > 0 AND agg.terminal = agg.total THEN 2
    WHEN agg.terminal > 0 THEN 1
    ELSE 0
END
FROM agg
WHERE messages.id = $1`, int64(messageID)); err != nil {
		return fmt.Errorf("recompute message state: %w", err)
	}
	return nil
}
