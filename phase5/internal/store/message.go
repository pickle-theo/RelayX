package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// AcceptMessage persists one logical message, the resolved explicit endpoint
// (if any) and its outbox event in a single transaction. No delivery rows are
// written: the fan-out worker creates them after the event is published.
//
// When (tenant_id, from_user, client_msg_id) already exists the original
// message ID is returned with Duplicate set and nothing is written.
func (s *Store) AcceptMessage(ctx context.Context, req AcceptRequest) (AcceptResult, error) {
	tenant := strings.TrimSpace(req.TenantID)
	if tenant == "" {
		tenant = DefaultTenant
	}
	payload := req.Payload
	if payload == nil {
		payload = []byte{}
	}
	if req.OutboxTopic == "" || req.OutboxKey == "" || req.OutboxPayload == nil {
		return AcceptResult{}, errors.New("store: accept message: outbox event is required")
	}

	var (
		messageID uint64
		duplicate bool
	)
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		err := tx.QueryRowContext(ctx, `
INSERT INTO messages (tenant_id, client_msg_id, from_user, from_device, target_type, target, payload, expire_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (tenant_id, from_user, client_msg_id) DO NOTHING
RETURNING id`,
			tenant, int64(req.ClientMsgID), req.FromUser, req.FromDevice, int16(req.TargetType),
			req.Target, payload, req.ExpireAt).Scan(&messageID)
		if errors.Is(err, sql.ErrNoRows) {
			duplicate = true
			if err := tx.QueryRowContext(ctx, `
SELECT id FROM messages
WHERE tenant_id = $1 AND from_user = $2 AND client_msg_id = $3`,
				tenant, req.FromUser, int64(req.ClientMsgID)).Scan(&messageID); err != nil {
				return fmt.Errorf("select existing message: %w", err)
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("insert message: %w", err)
		}

		// An explicitly addressed endpoint must exist in the endpoints table so
		// PULL and the fan-out worker can find it even before a LOGIN.
		if req.TargetUser != "" && req.TargetDevice != "" {
			if _, err := tx.ExecContext(ctx, `
INSERT INTO endpoints (user_id, device_id)
VALUES ($1, $2)
ON CONFLICT (user_id, device_id) DO NOTHING`, req.TargetUser, req.TargetDevice); err != nil {
				return fmt.Errorf("insert endpoint: %w", err)
			}
		}

		body, err := req.OutboxPayload(messageID)
		if err != nil {
			return fmt.Errorf("build outbox payload: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO outbox (topic, partition_key, payload)
VALUES ($1, $2, $3::jsonb)`,
			req.OutboxTopic, req.OutboxKey, string(body)); err != nil {
			return fmt.Errorf("insert outbox: %w", err)
		}
		return nil
	})
	if err != nil {
		return AcceptResult{}, fmt.Errorf("store: accept message: %w", err)
	}
	return AcceptResult{MessageID: messageID, Duplicate: duplicate}, nil
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
// EXPIRED with its lease cleared and the message itself becomes EXPIRED, in a
// single transaction.
func (s *Store) ExpireMessage(ctx context.Context, messageID uint64) error {
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
UPDATE deliveries
SET state = 5, next_retry_at = NULL, lease_until = NULL, worker_id = NULL, updated_at = now()
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
