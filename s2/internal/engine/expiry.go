package engine

import (
	"context"
	"time"

	"relayx/s2/internal/store"
)

// RunExpiryScanner 在 S2 中是单 goroutine。本地 ticker 只决定
// 扫描频率，终态判定全部来自 store 的 CAS 谓词。
func (e *Engine) RunExpiryScanner(ctx context.Context) error {
	ticker := time.NewTicker(e.cfg.ScanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			e.scanExpiry(ctx)
		}
	}
}

func (e *Engine) scanExpiry(ctx context.Context) {
	msgs, err := e.st.Messages().ListExpired(ctx, e.cfg.ExpiryLimit)
	if err != nil {
		e.log.Printf("expiry scan: %v", err)
		return
	}
	for _, m := range msgs {
		if ctx.Err() != nil {
			return
		}
		if err := e.ExpireMessageWithDeliveries(ctx, m.ID); err != nil {
			e.log.Printf("expire message %s: %v", m.ID, err)
		}
	}
}

// 上方的 ExpireMessage 处理消息 CAS；本函数在同一事务中
// 先执行投递级过期。供 maintenance CLI 与确定性驱动
// 单条消息的测试使用。
func (e *Engine) ExpireMessageWithDeliveries(ctx context.Context, messageID string) error {
	return e.st.WithinTx(ctx, func(tx store.Tx) error {
		m, err := tx.Messages().GetByID(ctx, messageID)
		if err != nil {
			return err
		}
		ds, err := tx.Deliveries().ListByMessage(ctx, messageID)
		if err != nil {
			return err
		}
		for _, d := range ds {
			if d.DeliveryState != store.DeliveryPending && d.DeliveryState != store.DeliverySent {
				continue
			}
			version, hit, err := tx.Deliveries().ExpireIfEligible(ctx, tx, d.ID)
			if err != nil {
				return err
			}
			if hit {
				if err := tx.Facts().InsertBatch(ctx, tx, []*store.Fact{
					e.deliveryFact(d, store.FactDeliveryExpired, version),
				}); err != nil {
					return err
				}
			}
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
