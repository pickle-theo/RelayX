package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"relayx/s2/internal/store"
)

const deliveryColumns = `id, tenant_id, message_id, user_id, device_id, endpoint_id,
	delivery_state, state_version, attempts, next_attempt_at, application_status,
	application_acked_at, last_error, created_at, updated_at, terminal_at`

type deliveryView struct{ view }

func scanDelivery(row pgx.Row) (*store.Delivery, error) {
	var d store.Delivery
	err := row.Scan(
		&d.ID, &d.TenantID, &d.MessageID, &d.UserID, &d.DeviceID, &d.EndpointID,
		&d.DeliveryState, &d.StateVersion, &d.Attempts, &d.NextAttemptAt,
		&d.ApplicationStatus, &d.ApplicationAckedAt, &d.LastError,
		&d.CreatedAt, &d.UpdatedAt, &d.TerminalAt,
	)
	if err != nil {
		return nil, err
	}
	return &d, nil
}

func scanDeliveries(rows pgx.Rows) ([]*store.Delivery, error) {
	defer rows.Close()
	out := make([]*store.Delivery, 0)
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (v *deliveryView) InsertBatch(ctx context.Context, _ store.Tx, ds []*store.Delivery) error {
	for _, d := range ds {
		_, err := v.q().Exec(ctx, `
			INSERT INTO deliveries (id, tenant_id, message_id, user_id, device_id,
				endpoint_id, delivery_state, attempts, next_attempt_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7::delivery_state,$8,
			        CASE WHEN $9::bool THEN now() ELSE NULL END)
			ON CONFLICT (tenant_id, message_id, user_id, device_id) DO NOTHING`,
			d.ID, d.TenantID, d.MessageID, d.UserID, d.DeviceID, d.EndpointID,
			d.DeliveryState, d.Attempts, d.Online,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (v *deliveryView) GetByKey(ctx context.Context, tenant, msgID, user, device string) (*store.Delivery, error) {
	d, err := scanDelivery(v.q().QueryRow(ctx,
		`SELECT `+deliveryColumns+` FROM deliveries
		 WHERE tenant_id=$1 AND message_id=$2 AND user_id=$3 AND device_id=$4`,
		tenant, msgID, user, device))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return d, err
}

func (v *deliveryView) ListByMessage(ctx context.Context, msgID string) ([]*store.Delivery, error) {
	rows, err := v.q().Query(ctx,
		`SELECT `+deliveryColumns+` FROM deliveries WHERE message_id=$1 ORDER BY id`, msgID)
	if err != nil {
		return nil, err
	}
	return scanDeliveries(rows)
}

func (v *deliveryView) ListDue(ctx context.Context, limit int) ([]*store.Delivery, error) {
	rows, err := v.q().Query(ctx,
		`SELECT `+deliveryColumns+` FROM deliveries
		 WHERE delivery_state IN ('PENDING','SENT')
		   AND next_attempt_at IS NOT NULL
		   AND next_attempt_at <= now()
		 ORDER BY next_attempt_at LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	return scanDeliveries(rows)
}

func (v *deliveryView) ListPendingByEndpoint(ctx context.Context, tenant, user, device string) ([]*store.Delivery, error) {
	rows, err := v.q().Query(ctx,
		`SELECT `+deliveryColumns+` FROM deliveries
		 WHERE tenant_id=$1 AND user_id=$2 AND device_id=$3
		   AND delivery_state IN ('PENDING','SENT')
		 ORDER BY created_at`, tenant, user, device)
	if err != nil {
		return nil, err
	}
	return scanDeliveries(rows)
}

func (v *deliveryView) ActivatePending(ctx context.Context, _ store.Tx, tenant, user, device string) error {
	_, err := v.q().Exec(ctx, `
		UPDATE deliveries
		SET next_attempt_at = now(), updated_at = now()
		WHERE tenant_id=$1 AND user_id=$2 AND device_id=$3
		  AND delivery_state IN ('PENDING','SENT') AND next_attempt_at IS NULL`,
		tenant, user, device)
	return err
}

// MarkSent 推进投递进入 SENT，并由 SQL now()
// 派生 next_attempt_at，调用方从不提供判定时间戳。
func (v *deliveryView) MarkSent(ctx context.Context, _ store.Tx, id string, attempts int32, backoffSeconds int32) (int, bool, error) {
	var version int
	err := v.q().QueryRow(ctx, `
		UPDATE deliveries
		SET delivery_state='SENT', state_version=state_version+1,
		    attempts=$2, next_attempt_at=now()+make_interval(secs => $3::int),
		    updated_at=now()
		WHERE id=$1 AND delivery_state IN ('PENDING','SENT')
		RETURNING state_version`, id, attempts, backoffSeconds).Scan(&version)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return version, true, nil
}

// Transition 是投递状态的唯一受守卫写入路径。fromStates
// 按文本比较，避免不同驱动间枚举数组绑定差异。
// 未指定的 patch 字段保持不变；terminal_at 由 SQL now() 派生。
func (v *deliveryView) Transition(ctx context.Context, _ store.Tx, id string, from []string, to string, patch store.DeliveryPatch, version int) (bool, error) {
	var attempts any
	if patch.Attempts != nil {
		attempts = *patch.Attempts
	}
	var status any
	if patch.ApplicationStatus != nil && !patch.ClearApplicationStatus {
		status = *patch.ApplicationStatus
	}
	var lastErr any
	if patch.LastError != nil && !patch.ClearLastError {
		lastErr = *patch.LastError
	}
	tag, err := v.q().Exec(ctx, `
		UPDATE deliveries
		SET delivery_state=$2::delivery_state, state_version=state_version+1,
		    updated_at=now(),
		    next_attempt_at = CASE WHEN $3::bool THEN NULL
		                           WHEN $4::timestamptz IS NULL THEN next_attempt_at
		                           ELSE $4 END,
		    attempts = coalesce($5::int, attempts),
		    application_status = CASE WHEN $6::bool THEN NULL
		                              WHEN $7::text IS NULL THEN application_status
		                              ELSE $7 END,
		    application_acked_at = coalesce($8::timestamptz, application_acked_at),
		    last_error = CASE WHEN $9::bool THEN NULL
		                      WHEN $10::text IS NULL THEN last_error
		                      ELSE $10 END,
		    terminal_at = CASE WHEN $2::text IN ('RECEIPT_ACKED','FAILED','EXPIRED')
		                       THEN now() ELSE terminal_at END
		WHERE id=$1
		  AND delivery_state::text = ANY($11::text[])
		  AND state_version=$12`,
		id, to, patch.ClearNextAttemptAt, patch.NextAttemptAt, attempts,
		patch.ClearApplicationStatus, status, patch.ApplicationAckedAt,
		patch.ClearLastError, lastErr, from, version)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (v *deliveryView) ExpireIfEligible(ctx context.Context, _ store.Tx, id string) (int, bool, error) {
	var version int
	err := v.q().QueryRow(ctx, `
		UPDATE deliveries
		SET delivery_state='EXPIRED', state_version=state_version+1,
		    next_attempt_at=NULL, terminal_at=now(), updated_at=now()
		WHERE id=$1 AND delivery_state IN ('PENDING','SENT')
		RETURNING state_version`, id).Scan(&version)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return version, true, nil
}

func (v *deliveryView) SetApplicationAck(ctx context.Context, _ store.Tx, id string, status string, at time.Time) error {
	_, err := v.q().Exec(ctx, `
		UPDATE deliveries
		SET application_status=$2, application_acked_at=$3
		WHERE id=$1`, id, status, at)
	return err
}
