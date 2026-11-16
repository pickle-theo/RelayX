package postgres

import (
	"context"
	"encoding/json"
	"time"

	"relayx/s2/internal/facts"
	"relayx/s2/internal/store"
)

type factView struct{ view }

func (v *factView) InsertBatch(ctx context.Context, _ store.Tx, fs []*store.Fact) error {
	for _, f := range fs {
		eventID := f.EventID
		if eventID == "" {
			eventID = facts.Derive(f.TenantID, f.MessageID, f.FactType, f.EndpointID, f.StateVersion)
		}
		summary := []byte("{}")
		if f.PayloadSummary != nil {
			b, err := json.Marshal(f.PayloadSummary)
			if err != nil {
				return err
			}
			summary = b
		}
		_, err := v.q().Exec(ctx, `
			INSERT INTO delivery_facts (event_id, tenant_id, message_id, endpoint_id,
				fact_type, state_version, occurred_at, payload_summary)
			VALUES ($1,$2,$3,$4,$5::fact_type,$6,coalesce($7::timestamptz,now()),$8::jsonb)
			ON CONFLICT (event_id) DO NOTHING`,
			eventID, f.TenantID, f.MessageID, f.EndpointID, f.FactType,
			f.StateVersion, nullableTime(f.OccurredAt), summary,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

func (v *factView) ListByMessage(ctx context.Context, msgID string) ([]*store.Fact, error) {
	rows, err := v.q().Query(ctx, `
		SELECT event_id, tenant_id, message_id, endpoint_id, fact_type,
		       state_version, occurred_at, payload_summary, published_at
		FROM delivery_facts WHERE message_id=$1
		ORDER BY occurred_at, event_id`, msgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]*store.Fact, 0)
	for rows.Next() {
		var f store.Fact
		var summary []byte
		if err := rows.Scan(&f.EventID, &f.TenantID, &f.MessageID, &f.EndpointID,
			&f.FactType, &f.StateVersion, &f.OccurredAt, &summary, &f.PublishedAt); err != nil {
			return nil, err
		}
		if len(summary) > 0 {
			if err := json.Unmarshal(summary, &f.PayloadSummary); err != nil {
				return nil, err
			}
		}
		out = append(out, &f)
	}
	return out, rows.Err()
}
