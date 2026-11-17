package engine

import (
	"context"
	"errors"

	"relayx/s2/internal/store"
)

// HandleBusinessAck 只写业务层透传列。
// 刻意不触碰 delivery_state、重试计划、事实或
// 消息聚合：业务成功不是传输终态。
func (e *Engine) HandleBusinessAck(ctx context.Context, tenant, messageID, user, device, status string) error {
	return e.st.WithinTx(ctx, func(tx store.Tx) error {
		d, err := tx.Deliveries().GetByKey(ctx, tenant, messageID, user, device)
		if err != nil {
			// 只把「确实不存在」映射为 UnknownMessage；DB 故障/超时必须原样上抛，
			// 否则会被误报成 404 并掩盖真实故障。
			if errors.Is(err, store.ErrNotFound) {
				return store.ErrUnknownMessage
			}
			return err
		}
		// application_acked_at 由 store 时钟派生（语义契约 §6：接口不传 now）。
		return tx.Deliveries().SetApplicationAck(ctx, tx, d.ID, status)
	})
}
