package postgres

import (
	"context"
	"sort"

	"relayx/s2/internal/store"
)

type endpointView struct{ view }

func (v *endpointView) Resolve(ctx context.Context, tenant, user string, devices []string, mode string, limit int32) ([]store.Endpoint, error) {
	query := `SELECT tenant_id, user_id, device_id, status, session_id, last_seen_at
	          FROM endpoints WHERE tenant_id=$1 AND user_id=$2`
	args := []any{tenant, user}
	if len(devices) > 0 {
		query += ` AND device_id = ANY($3::text[])`
		args = append(args, devices)
	}
	rows, err := v.q().Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]store.Endpoint, 0)
	for rows.Next() {
		var e store.Endpoint
		if err := rows.Scan(&e.TenantID, &e.UserID, &e.DeviceID, &e.Status, &e.SessionID, &e.LastSeenAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		if mode == store.ModeAllEndpoints {
			return out[i].DeviceID < out[j].DeviceID
		}
		if out[i].LastSeenAt.Equal(out[j].LastSeenAt) {
			return out[i].DeviceID < out[j].DeviceID
		}
		return out[i].LastSeenAt.After(out[j].LastSeenAt)
	})
	if limit > 0 && int32(len(out)) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (v *endpointView) MarkOnline(ctx context.Context, tenant, user, device, sessionID string) error {
	_, err := v.q().Exec(ctx, `
		INSERT INTO endpoints (tenant_id, user_id, device_id, status, session_id, last_seen_at)
		VALUES ($1,$2,$3,'ONLINE',$4,now())
		ON CONFLICT (tenant_id, user_id, device_id)
		DO UPDATE SET status='ONLINE', session_id=EXCLUDED.session_id, last_seen_at=now()`,
		tenant, user, device, nullableString(sessionID, sessionID != ""))
	return err
}

func (v *endpointView) MarkOffline(ctx context.Context, tenant, user, device, sessionID string) error {
	if sessionID == "" {
		_, err := v.q().Exec(ctx, `
			UPDATE endpoints SET status='OFFLINE', session_id=NULL, last_seen_at=now()
			WHERE tenant_id=$1 AND user_id=$2 AND device_id=$3`, tenant, user, device)
		return err
	}
	_, err := v.q().Exec(ctx, `
		UPDATE endpoints SET status='OFFLINE', session_id=NULL, last_seen_at=now()
		WHERE tenant_id=$1 AND user_id=$2 AND device_id=$3 AND session_id=$4`,
		tenant, user, device, sessionID)
	return err
}

func (v *endpointView) ResetAllOffline(ctx context.Context) error {
	_, err := v.q().Exec(ctx, `UPDATE endpoints SET status='OFFLINE', session_id=NULL, last_seen_at=now()`)
	return err
}

func (v *endpointView) ListInconsistentOnline(ctx context.Context, isOnline func(tenant, user, device string) bool) ([]store.Endpoint, error) {
	rows, err := v.q().Query(ctx, `
		SELECT tenant_id, user_id, device_id, status, session_id, last_seen_at
		FROM endpoints WHERE status='ONLINE'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]store.Endpoint, 0)
	for rows.Next() {
		var e store.Endpoint
		if err := rows.Scan(&e.TenantID, &e.UserID, &e.DeviceID, &e.Status, &e.SessionID, &e.LastSeenAt); err != nil {
			return nil, err
		}
		if !isOnline(e.TenantID, e.UserID, e.DeviceID) {
			out = append(out, e)
		}
	}
	return out, rows.Err()
}
