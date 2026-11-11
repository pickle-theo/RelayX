package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// GetTenant returns the delivery quota of one tenant. A missing row is reported
// as (Tenant{}, false, nil) so the caller can apply defaults.
func (s *Store) GetTenant(ctx context.Context, tenantID string) (Tenant, bool, error) {
	var t Tenant
	err := s.db.QueryRowContext(ctx, `
SELECT id, rate_per_sec, burst, max_inflight
FROM tenants
WHERE id = $1`, tenantID).Scan(&t.ID, &t.RatePerSec, &t.Burst, &t.MaxInflight)
	if errors.Is(err, sql.ErrNoRows) {
		return Tenant{}, false, nil
	}
	if err != nil {
		return Tenant{}, false, fmt.Errorf("store: get tenant %s: %w", tenantID, err)
	}
	return t, true, nil
}

// UpsertTenant creates or updates a tenant quota.
func (s *Store) UpsertTenant(ctx context.Context, t Tenant) error {
	if _, err := s.db.ExecContext(ctx, `
INSERT INTO tenants (id, rate_per_sec, burst, max_inflight)
VALUES ($1, $2, $3, $4)
ON CONFLICT (id) DO UPDATE SET
    rate_per_sec = EXCLUDED.rate_per_sec,
    burst = EXCLUDED.burst,
    max_inflight = EXCLUDED.max_inflight,
    updated_at = now()`, t.ID, t.RatePerSec, t.Burst, t.MaxInflight); err != nil {
		return fmt.Errorf("store: upsert tenant %s: %w", t.ID, err)
	}
	return nil
}
