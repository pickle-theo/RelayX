package engine

import (
	"context"
	"time"

	"relayx/s2/internal/store"
)

// RunRetryScheduler 以单 goroutine 启动。本地 ticker 只决定
// 扫描频率；“是否到期”由 ListDue 中的 SQL now() 判定。
func (e *Engine) RunRetryScheduler(ctx context.Context) error {
	ticker := time.NewTicker(e.cfg.ScanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			e.scanRetries(ctx)
		}
	}
}

func (e *Engine) scanRetries(ctx context.Context) {
	ds, err := e.st.Deliveries().ListDue(ctx, e.cfg.RetryLimit)
	if err != nil {
		e.log.Printf("retry scan: %v", err)
		return
	}
	for _, d := range ds {
		if ctx.Err() != nil {
			return
		}
		m, err := e.st.Messages().GetByID(ctx, d.MessageID)
		if err != nil || m.PlatformState != store.MessageAccepted {
			continue
		}
		if d.Attempts >= m.MaxAttempts {
			e.failDelivery(ctx, d, "attempts exhausted")
			continue
		}
		if err := e.dispatchOne(ctx, m, d, d.Attempts+1); err != nil {
			e.log.Printf("retry %s: %v", d.ID, err)
		}
	}
}

// failDelivery 是重试耗尽路径：投递终态 CAS + 事实 +
// 聚合，全部在同一事务内。
func (e *Engine) failDelivery(ctx context.Context, d *store.Delivery, reason string) {
	err := e.st.WithinTx(ctx, func(tx store.Tx) error {
		hit, err := tx.Deliveries().Transition(ctx, tx, d.ID,
			[]string{store.DeliveryPending, store.DeliverySent}, store.DeliveryFailed,
			store.DeliveryPatch{ClearNextAttemptAt: true, LastError: &reason}, d.StateVersion)
		if err != nil || !hit {
			return err
		}
		if err := tx.Facts().InsertBatch(ctx, tx, []*store.Fact{
			e.deliveryFact(d, store.FactDeliveryFailed, d.StateVersion+1),
		}); err != nil {
			return err
		}
		return e.OnDeliveryTerminal(ctx, tx, d.MessageID)
	})
	if err != nil {
		e.log.Printf("fail delivery %s: %v", d.ID, err)
	}
}
