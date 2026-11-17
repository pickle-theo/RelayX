package broadcast

import (
	"context"
	"sync/atomic"

	"relayx/s3/internal/config"
	"relayx/s3/internal/connection"
	"relayx/s3/internal/identity"
)

// Component 把 S3 广播子系统装配成一组可注入 Gateway 的组件（LLD §2.2）。
// 所有组件在同一进程内，不新增进程、不落库。
type Component struct {
	Index    *Index
	Verifier *TokenVerifier
	KeyRing  *TokenKeyRing
	Encoder  *Encoder
	Fanout   *Fanout
	Ingress  *Ingress
	Metrics  *Metrics
	Auth     *identity.Authenticator

	threshold  int64
	slowCount  atomic.Int64
	maxPayload int
}

func NewComponent(cfg config.Config, auth *identity.Authenticator) *Component {
	ring := NewTokenKeyRing(cfg.BroadcastTokenKeys)
	metrics := &Metrics{}
	idx := NewIndex(IndexLimits{
		MaxTopicsPerSession:    cfg.MaxTopicsPerSession,
		MaxSubscribersPerTopic: cfg.MaxSubscribersPerTopic,
		MaxTopicBytes:          cfg.MaxTopicBytes,
		MaxGlobalSubscriptions: cfg.MaxGlobalSubscriptions,
	})
	enc := NewSharedEncoder(cfg.BroadcastPayloadMax)
	fanout := NewFanout(idx, enc, metrics)
	ingress := NewIngress(fanout, metrics, cfg.BroadcastPayloadMax, cfg.PublishIdemTTL)
	return &Component{
		Index: idx, Verifier: NewTokenVerifier(ring), KeyRing: ring,
		Encoder: enc, Fanout: fanout, Ingress: ingress, Metrics: metrics, Auth: auth,
		threshold: cfg.BroadcastSlowDropThreshold, maxPayload: cfg.BroadcastPayloadMax,
	}
}

// SenderFactoryFor 为一条连接生成订阅发送器工厂。
// 每个 (会话, topic, version) 绑定一个 connSender，负责投递前的状态/版本校验。
func (c *Component) SenderFactoryFor(conn *connection.Connection) SenderFactory {
	return func(sessID, topic string, version uint64) P2Sender {
		return NewConnSender(conn, c.Index, sessID, topic, version, c.threshold, func(on bool) {
			if on {
				c.slowCount.Add(1)
			} else {
				c.slowCount.Add(-1)
			}
			c.Metrics.SetSlowConsumers(c.slowCount.Load())
		})
	}
}

// Publish 是 HTTP 受理的统一入口，并在每次发布后刷新索引/慢消费者指标。
func (c *Component) Publish(ctx context.Context, in BroadcastInput) (Accepted, error) {
	accepted, err := c.Ingress.Publish(ctx, in)
	c.Metrics.SetIndexStats(c.Index.Stats())
	c.Metrics.SetSlowConsumers(c.slowCount.Load())
	return accepted, err
}

// MaxPayload 返回广播 payload 上限（HTTP 层校验用）。
func (c *Component) MaxPayload() int { return c.maxPayload }

// RenderMetrics 返回广播指标的 Prometheus 文本片段（合并到 /metrics）。
func (c *Component) RenderMetrics() string {
	c.Metrics.SetIndexStats(c.Index.Stats())
	c.Metrics.SetSlowConsumers(c.slowCount.Load())
	return c.Metrics.Render()
}
