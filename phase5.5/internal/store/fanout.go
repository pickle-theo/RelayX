package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// GetMessage returns one message by ID. It is used by the fan-out worker when
// it needs the authoritative message row rather than the Kafka event body.
func (s *Store) GetMessage(ctx context.Context, messageID uint64) (Message, error) {
	var (
		msg        Message
		targetType int16
		state      int16
	)
	err := s.db.QueryRowContext(ctx, `
SELECT id, tenant_id, client_msg_id, from_user, from_device, target_type, target,
       payload, state, created_at, expire_at
FROM messages
WHERE id = $1`, int64(messageID)).Scan(
		&msg.ID, &msg.TenantID, &msg.ClientMsgID, &msg.FromUser, &msg.FromDevice,
		&targetType, &msg.Target, &msg.Payload, &state, &msg.CreatedAt, &msg.ExpireAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Message{}, ErrMessageNotFound
	}
	if err != nil {
		return Message{}, fmt.Errorf("store: get message %d: %w", messageID, err)
	}
	msg.TargetType = TargetType(targetType)
	msg.State = MessageState(state)
	return msg, nil
}

// ErrMessageNotFound reports that a fan-out event references a missing message.
var ErrMessageNotFound = errors.New("store: message not found")

// EndpointExists reports whether (userID, deviceID) is registered.
func (s *Store) EndpointExists(ctx context.Context, userID, deviceID string) (bool, error) {
	var exists bool
	if err := s.db.QueryRowContext(ctx, `
SELECT EXISTS (SELECT 1 FROM endpoints WHERE user_id = $1 AND device_id = $2)`,
		userID, deviceID).Scan(&exists); err != nil {
		return false, fmt.Errorf("store: check endpoint %s/%s: %w", userID, deviceID, err)
	}
	return exists, nil
}

// ListEndpointsPage returns at most limit endpoints of userID whose device_id
// sorts after afterDevice. It is the paginated form used by the fan-out worker
// so a user with a huge device fleet is processed in bounded batches.
func (s *Store) ListEndpointsPage(ctx context.Context, userID, afterDevice string, limit int) ([]Endpoint, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT user_id, device_id
FROM endpoints
WHERE user_id = $1 AND device_id > $2
ORDER BY device_id
LIMIT $3`, userID, afterDevice, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list endpoints page for %s: %w", userID, err)
	}
	defer rows.Close()

	var endpoints []Endpoint
	for rows.Next() {
		var ep Endpoint
		if err := rows.Scan(&ep.UserID, &ep.DeviceID); err != nil {
			return nil, fmt.Errorf("store: scan endpoint: %w", err)
		}
		endpoints = append(endpoints, ep)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list endpoints page for %s: %w", userID, err)
	}
	return endpoints, nil
}

// InsertDeliveries idempotently creates one PENDING delivery per endpoint for
// messageID. A repeated fan-out event conflicts with the existing rows and
// inserts nothing, so redelivery never duplicates deliveries.
func (s *Store) InsertDeliveries(ctx context.Context, messageID uint64, endpoints []Endpoint) (int, error) {
	if len(endpoints) == 0 {
		return 0, nil
	}
	users := make([]string, 0, len(endpoints))
	devices := make([]string, 0, len(endpoints))
	seen := make(map[Endpoint]struct{}, len(endpoints))
	for _, ep := range endpoints {
		if _, ok := seen[ep]; ok {
			continue
		}
		seen[ep] = struct{}{}
		users = append(users, ep.UserID)
		devices = append(devices, ep.DeviceID)
	}
	res, err := s.db.ExecContext(ctx, `
INSERT INTO deliveries (message_id, user_id, device_id, state, shard_id)
SELECT $1, u.user_id, u.device_id, 0, mod($1::bigint, $4::bigint)::int
FROM unnest($2::text[], $3::text[]) AS u(user_id, device_id)
ON CONFLICT (message_id, user_id, device_id) DO NOTHING`,
		int64(messageID), users, devices, DefaultShardSpace)
	if err != nil {
		return 0, fmt.Errorf("store: insert deliveries for message %d: %w", messageID, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("store: insert deliveries rows affected: %w", err)
	}
	return int(affected), nil
}
