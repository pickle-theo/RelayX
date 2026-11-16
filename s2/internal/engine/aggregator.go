package engine

import (
	"context"

	"relayx/s2/internal/store"
)

// OnFanoutDone 标记投递集合已最终确定，并在扇出事务内
// 立即尝试结算（包含零端点场景）。
func (e *Engine) OnFanoutDone(ctx context.Context, tx store.Tx, messageID string) (bool, error) {
	if _, err := tx.Messages().MarkFanoutDone(ctx, tx, messageID); err != nil {
		return false, err
	}
	return e.trySettle(ctx, tx, messageID)
}

// OnDeliveryTerminal 是重试耗尽场景使用的通用聚合钩子。
func (e *Engine) OnDeliveryTerminal(ctx context.Context, tx store.Tx, messageID string) error {
	_, err := e.trySettle(ctx, tx, messageID)
	return err
}

// ExpireMessage 应用 expire_message CAS，仅当 CAS 成功时
// 写入 message.expired。它与 trySettle 对称：二者竞争同一个
// platform_state='ACCEPTED' 前置条件，因此两条事实互斥。
func (e *Engine) ExpireMessage(ctx context.Context, messageID string) error {
	return e.st.WithinTx(ctx, func(tx store.Tx) error {
		m, err := tx.Messages().GetByID(ctx, messageID)
		if err != nil {
			return err
		}
		res, hit, err := tx.Messages().ExpireIfEligible(ctx, tx, messageID)
		if err != nil || !hit {
			return err
		}
		return tx.Facts().InsertBatch(ctx, tx, []*store.Fact{
			e.messageFact(m, store.FactMessageExpired, res.StateVersion, res.Counts),
		})
	})
}
