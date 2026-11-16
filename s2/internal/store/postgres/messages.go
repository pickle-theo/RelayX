package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"relayx/s2/internal/store"
)

const messageColumns = `id, tenant_id, producer_id, producer_kind, sender, source_device_id,
	client_msg_id, payload, payload_size, delivery_mode, fanout, ttl_seconds,
	max_attempts, platform_state, state_version, fanout_done_at, created_at,
	expires_at, settled_at`

type messageView struct{ view }

func scanMessage(row pgx.Row) (*store.Message, error) {
	var m store.Message
	var sender *string
	err := row.Scan(
		&m.ID, &m.TenantID, &m.ProducerID, &m.ProducerKind, &sender, &m.SourceDeviceID,
		&m.ClientMsgID, &m.Payload, &m.PayloadSize, &m.DeliveryMode, &m.Fanout,
		&m.TTLSeconds, &m.MaxAttempts, &m.PlatformState, &m.StateVersion,
		&m.FanoutDoneAt, &m.CreatedAt, &m.ExpiresAt, &m.SettledAt,
	)
	if err != nil {
		return nil, err
	}
	if sender != nil {
		m.Sender = *sender
	}
	return &m, nil
}

func scanMessages(rows pgx.Rows) ([]*store.Message, error) {
	defer rows.Close()
	out := make([]*store.Message, 0)
	for rows.Next() {
		m, err := scanMessage(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (v *messageView) Insert(ctx context.Context, _ store.Tx, m *store.Message) (bool, *store.Message, error) {
	var id string
	err := v.q().QueryRow(ctx, `
		INSERT INTO messages (id, tenant_id, producer_id, producer_kind, sender,
			source_device_id, client_msg_id, payload, payload_size, delivery_mode,
			fanout, ttl_seconds, max_attempts, expires_at)
		VALUES ($1,$2,$3,$4::producer_kind,NULLIF($5,''),$6,$7,$8,$9,
			$10::delivery_mode,$11,$12,$13,
			now() + make_interval(secs => $14::int))
		ON CONFLICT (tenant_id, producer_id, client_msg_id) DO NOTHING
		RETURNING id`,
		m.ID, m.TenantID, m.ProducerID, m.ProducerKind, m.Sender, m.SourceDeviceID,
		m.ClientMsgID, m.Payload, m.PayloadSize, m.DeliveryMode, m.Fanout,
		m.TTLSeconds, m.MaxAttempts, m.TTLSeconds,
	).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		// 键已存在：DO NOTHING 已等待并发事务落定，同一事务内回读权威消息。
		existing, err := v.GetByIdempotencyKey(ctx, m.TenantID, m.ProducerID, m.ClientMsgID)
		return false, existing, err
	}
	if err != nil {
		return false, nil, err
	}
	return true, nil, nil
}

func (v *messageView) GetByID(ctx context.Context, id string) (*store.Message, error) {
	m, err := scanMessage(v.q().QueryRow(ctx, `SELECT `+messageColumns+` FROM messages WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return m, err
}

func (v *messageView) GetByIdempotencyKey(ctx context.Context, tenant, producerID, clientMsgID string) (*store.Message, error) {
	m, err := scanMessage(v.q().QueryRow(ctx,
		`SELECT `+messageColumns+` FROM messages
		 WHERE tenant_id=$1 AND producer_id=$2 AND client_msg_id=$3`,
		tenant, producerID, clientMsgID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return m, err
}

func (v *messageView) ListExpired(ctx context.Context, limit int) ([]*store.Message, error) {
	rows, err := v.q().Query(ctx,
		`SELECT `+messageColumns+` FROM messages
		 WHERE platform_state='ACCEPTED' AND expires_at <= now()
		 ORDER BY expires_at LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	return scanMessages(rows)
}

func (v *messageView) MarkState(ctx context.Context, _ store.Tx, id, state string, version int) error {
	var err error
	if version >= 0 {
		_, err = v.q().Exec(ctx,
			`UPDATE messages SET platform_state=$2::message_state, state_version=state_version+1
			 WHERE id=$1 AND state_version=$3`, id, state, version)
	} else {
		_, err = v.q().Exec(ctx,
			`UPDATE messages SET platform_state=$2::message_state, state_version=state_version+1
			 WHERE id=$1`, id, state)
	}
	return err
}

func (v *messageView) MarkFanoutDone(ctx context.Context, _ store.Tx, id string) (bool, error) {
	tag, err := v.q().Exec(ctx,
		`UPDATE messages SET fanout_done_at=now()
		 WHERE id=$1 AND fanout_done_at IS NULL`, id)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

const settleCAS = `
WITH c AS (
    SELECT count(*) AS total,
           count(*) FILTER (WHERE d.delivery_state = 'RECEIPT_ACKED') AS receipt_acked,
           count(*) FILTER (WHERE d.delivery_state = 'FAILED') AS failed,
           count(*) FILTER (WHERE d.delivery_state = 'EXPIRED') AS expired
    FROM deliveries d WHERE d.message_id = $1
)
UPDATE messages m
SET platform_state = 'SETTLED',
    state_version  = state_version + 1,
    settled_at     = now()
FROM c
WHERE m.id = $1
  AND m.platform_state = 'ACCEPTED'
  AND m.fanout_done_at IS NOT NULL
  AND NOT EXISTS (
        SELECT 1 FROM deliveries d
        WHERE d.message_id = m.id AND d.delivery_state IN ('PENDING','SENT')
      )
RETURNING m.state_version, m.settled_at,
          c.total, c.receipt_acked, c.failed, c.expired`

const expireCAS = `
WITH c AS (
    SELECT count(*) AS total,
           count(*) FILTER (WHERE d.delivery_state = 'RECEIPT_ACKED') AS receipt_acked,
           count(*) FILTER (WHERE d.delivery_state = 'FAILED') AS failed,
           count(*) FILTER (WHERE d.delivery_state = 'EXPIRED') AS expired
    FROM deliveries d WHERE d.message_id = $1
)
UPDATE messages m
SET platform_state = 'EXPIRED',
    state_version  = state_version + 1
FROM c
WHERE m.id = $1
  AND m.platform_state = 'ACCEPTED'
  AND m.expires_at <= now()
RETURNING m.state_version, m.expires_at,
          c.total, c.receipt_acked, c.failed, c.expired`

func scanCAS(row pgx.Row) (store.CASResult, bool, error) {
	var r store.CASResult
	var terminal time.Time
	err := row.Scan(&r.StateVersion, &terminal,
		&r.Counts.Total, &r.Counts.ReceiptAcked, &r.Counts.Failed, &r.Counts.Expired)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, false, nil
	}
	if err != nil {
		return r, false, err
	}
	r.TerminalAt = terminal
	return r, true, nil
}

func (v *messageView) SettleIfEligible(ctx context.Context, _ store.Tx, id string) (store.CASResult, bool, error) {
	return scanCAS(v.q().QueryRow(ctx, settleCAS, id))
}

func (v *messageView) ExpireIfEligible(ctx context.Context, _ store.Tx, id string) (store.CASResult, bool, error) {
	return scanCAS(v.q().QueryRow(ctx, expireCAS, id))
}
