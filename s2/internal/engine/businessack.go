package engine

import (
	"context"
	"time"

	"relayx/s2/internal/store"
)

// HandleBusinessAck 只写业务层透传列。
// 刻意不触碰 delivery_state、重试计划、事实或
// 消息聚合：业务成功不是传输终态。
func (e *Engine) HandleBusinessAck(ctx context.Context, tenant, messageID, user, device, status string) error {
	d, err := e.st.Deliveries().GetByKey(ctx, tenant, messageID, user, device)
	if err != nil {
		return store.ErrUnknownMessage
	}
	// 该时间戳仅用于展示/审计，不作为时间仲裁输入。
	at := time.Now().UTC()
	return e.st.WithinTx(ctx, func(tx store.Tx) error {
		return tx.Deliveries().SetApplicationAck(ctx, tx, d.ID, status, at)
	})
}
