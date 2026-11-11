package store

import (
	"context"
	"fmt"
)

// UpsertEndpoint records a login for (userID, deviceID), refreshing
// last_seen_at when the endpoint already exists.
func (s *Store) UpsertEndpoint(ctx context.Context, userID, deviceID string) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO endpoints (user_id, device_id)
VALUES ($1, $2)
ON CONFLICT (user_id, device_id) DO UPDATE SET last_seen_at = now()`, userID, deviceID)
	if err != nil {
		return fmt.Errorf("store: upsert endpoint %s/%s: %w", userID, deviceID, err)
	}
	return nil
}

// ListEndpoints returns every endpoint of userID ordered by device_id.
func (s *Store) ListEndpoints(ctx context.Context, userID string) ([]Endpoint, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT user_id, device_id
FROM endpoints
WHERE user_id = $1
ORDER BY device_id`, userID)
	if err != nil {
		return nil, fmt.Errorf("store: list endpoints for %s: %w", userID, err)
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
		return nil, fmt.Errorf("store: list endpoints for %s: %w", userID, err)
	}
	return endpoints, nil
}
