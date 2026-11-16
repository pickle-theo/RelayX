package engine

import (
	"context"
	"errors"

	"relayx/s2/internal/protocol"
	"relayx/s2/internal/store"
)

// dispatchOne 在一个事务内写 P1 的 DELIVER 并推进投递状态。
// 离线端点停在 next_attempt_at = NULL，不产生事实、
// 不触发重试，也不改变状态。
func (e *Engine) dispatchOne(ctx context.Context, m *store.Message, d *store.Delivery, attempts int32) error {
	sink, ok := e.resolver.Lookup(d.TenantID, d.UserID, d.DeviceID)
	if !ok {
		_ = e.parkOffline(ctx, d)
		return store.ErrOffline
	}
	body := protocol.EncodeDeliver(protocol.DeliverBody{
		MessageID:  m.ID,
		ProducerID: m.ProducerID,
		Sender:     m.Sender,
		Payload:    m.Payload,
		ExpiresAt:  m.ExpiresAt.UnixMilli(),
	})
	frame, err := protocol.Encode(protocol.Frame{Type: protocol.TypeDeliver, Body: body}, nil)
	if err != nil {
		return err
	}
	if err := sink.EnqueueReliable(ctx, frame); err != nil {
		return err
	}
	var version int
	err = e.st.WithinTx(ctx, func(tx store.Tx) error {
		v, ok, err := tx.Deliveries().MarkSent(ctx, tx, d.ID, attempts, e.backoffSeconds())
		version = v
		if err != nil || !ok {
			return err
		}
		return tx.Facts().InsertBatch(ctx, tx, []*store.Fact{
			e.deliveryFact(d, store.FactDeliverySent, version),
		})
	})
	return err
}

// parkOffline 将到期投递移出扫描窗口且不改变状态。
// 端点上线钩子会重新激活它。
func (e *Engine) parkOffline(ctx context.Context, d *store.Delivery) error {
	return e.st.WithinTx(ctx, func(tx store.Tx) error {
		_, err := tx.Deliveries().Transition(ctx, tx, d.ID,
			[]string{d.DeliveryState}, d.DeliveryState,
			store.DeliveryPatch{ClearNextAttemptAt: true}, d.StateVersion)
		return err
	})
}

// ResumeEndpoint 重新激活所有等待中的投递并再次派发。
// 在 LOGIN（重连）时调用，而非内存回调，因此重启
// 不会丢失触发信号。
func (e *Engine) ResumeEndpoint(ctx context.Context, tenant, user, device string) error {
	if err := e.st.WithinTx(ctx, func(tx store.Tx) error {
		return tx.Deliveries().ActivatePending(ctx, tx, tenant, user, device)
	}); err != nil {
		return err
	}
	ds, err := e.st.Deliveries().ListPendingByEndpoint(ctx, tenant, user, device)
	if err != nil {
		return err
	}
	for _, d := range ds {
		m, err := e.st.Messages().GetByID(ctx, d.MessageID)
		if err != nil {
			continue
		}
		if m.PlatformState != store.MessageAccepted {
			continue
		}
		attempts := d.Attempts + 1
		if attempts < 1 {
			attempts = 1
		}
		if err := e.dispatchOne(ctx, m, d, attempts); err != nil && !errors.Is(err, store.ErrOffline) {
			e.log.Printf("resume %s: %v", d.ID, err)
		}
	}
	return nil
}
