// Package engine 实现 S2 投递引擎：派发、回执、
// 重试、过期、聚合与事实写入。所有终态写入均为 CAS，
// 每条事实与其状态变更在同一事务内写入。
package engine

import (
	"context"
	"log"

	"relayx/s2/internal/config"
	"relayx/s2/internal/protocol"
	"relayx/s2/internal/store"
)

// Sink 是单条活跃连接的 P1 可靠通道入口。
// 传入未编码帧：连接内序号分配与帧编码由连接层完成（唯一 Writer 仍然是
// 唯一接触 socket 的地方）。若在引擎侧预编码，出站 Sequence 将无人分配，
// 违反 S0 冻结的连接内单调序号语义。
type Sink interface {
	EnqueueReliable(ctx context.Context, f protocol.Frame) error
}

// OnlineResolver 把端点映射到其活跃 Sink。会话注册表是
// 权威来源，endpoints 表只是投影。
type OnlineResolver interface {
	Lookup(tenant, user, device string) (Sink, bool)
}

// Engine 承载单机投递编排。
type Engine struct {
	st       store.Store
	cfg      config.Config
	resolver OnlineResolver
	log      *log.Logger
}

func New(st store.Store, cfg config.Config, resolver OnlineResolver, logger *log.Logger) *Engine {
	if logger == nil {
		logger = log.Default()
	}
	return &Engine{st: st, cfg: cfg, resolver: resolver, log: logger}
}

// Store 向 ingress 服务与查询 API 暴露 store 句柄。
func (e *Engine) Store() store.Store { return e.st }

func (e *Engine) backoffSeconds() int32 {
	sec := int32(e.cfg.RetryBackoff.Seconds())
	if sec < 1 {
		return 1
	}
	return sec
}

// DispatchMessage 推动某条消息当前所有待派发的投递。
func (e *Engine) DispatchMessage(ctx context.Context, messageID string) error {
	m, err := e.st.Messages().GetByID(ctx, messageID)
	if err != nil {
		return err
	}
	if m.PlatformState != store.MessageAccepted {
		return nil
	}
	ds, err := e.st.Deliveries().ListByMessage(ctx, messageID)
	if err != nil {
		return err
	}
	for _, d := range ds {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if d.DeliveryState != store.DeliveryPending {
			continue
		}
		if err := e.dispatchOne(ctx, m, d, 1); err != nil {
			e.log.Printf("dispatch %s/%s: %v", d.ID, d.EndpointID, err)
		}
	}
	return nil
}

// trySettle 是聚合守卫：结算 CAS + 同事务事实。
// CAS 零行影响是正常结果，不是错误。
func (e *Engine) trySettle(ctx context.Context, tx store.Tx, messageID string) (bool, error) {
	m, err := tx.Messages().GetByID(ctx, messageID)
	if err != nil {
		return false, err
	}
	res, hit, err := tx.Messages().SettleIfEligible(ctx, tx, messageID)
	if err != nil || !hit {
		return hit, err
	}
	f := e.messageFact(m, store.FactMessageSettled, res.StateVersion, res.Counts)
	if err := tx.Facts().InsertBatch(ctx, tx, []*store.Fact{f}); err != nil {
		return false, err
	}
	return true, nil
}
