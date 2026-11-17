package engine

import (
	"context"
	"errors"

	"relayx/s2/internal/store"
)

// HandleReceiptAck 是驱动投递进入 RECEIPT_ACKED 的唯一路径。
// 端点身份由会话提供，绝不来自帧内容。
func (e *Engine) HandleReceiptAck(ctx context.Context, tenant, messageID, user, device string) error {
	return e.st.WithinTx(ctx, func(tx store.Tx) error {
		// 在同一事务中读取投递及其版本，避免与并发 MarkSent（重试）之间
		// 出现读-改-写竞态：旧版本会让 CAS 静默失败，ACK 永久丢失。
		d, err := tx.Deliveries().GetByKey(ctx, tenant, messageID, user, device)
		if err != nil {
			// 只把「确实不存在」映射为 UnknownMessage；DB 故障/超时原样上抛。
			if errors.Is(err, store.ErrNotFound) {
				return store.ErrUnknownMessage
			}
			return err
		}
		hit, err := tx.Deliveries().Transition(ctx, tx, d.ID,
			[]string{store.DeliverySent}, store.DeliveryReceiptAcked,
			store.DeliveryPatch{ClearNextAttemptAt: true}, d.StateVersion)
		if err != nil || !hit {
			return err // duplicate / late ACK: no state change, no fact
		}
		if err := tx.Facts().InsertBatch(ctx, tx, []*store.Fact{
			e.deliveryFact(d, store.FactDeliveryReceiptAcked, d.StateVersion+1),
		}); err != nil {
			return err
		}
		_, err = e.trySettle(ctx, tx, d.MessageID)
		return err
	})
}
