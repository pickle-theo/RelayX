package broadcast

import (
	"errors"
	"sync/atomic"

	"relayx/s4/internal/connection"
	"relayx/s4/internal/sendpath"
)

// BroadcastSink 是 connSender 对连接的最小依赖面（生产实现为 *connection.Connection，
// 单测可注入假实现，无需真实 socket）。
type BroadcastSink interface {
	State() connection.ConnState
	TrySendBroadcast(frame *sendpath.ImmutableFrame) error
	BroadcastPending() int64
}

// connSender 绑定 (会话, topic, 订阅版本)，把 P2Sender 接口适配到具体连接。
// 投递前的两道校验（LLD §9.1）：
//  1. 连接仍在线（StateOpen）
//  2. 订阅版本未变（未退订/重订阅）
//
// 两者任一失效则跳过，绝不写已关闭连接。
type connSender struct {
	conn    BroadcastSink
	idx     *Index
	sessID  string
	topic   string
	version uint64

	threshold int64
	drops     atomic.Int64
	degraded  atomic.Bool
	onDegrade func(bool) // 降级状态切换回调（聚合慢消费者指标），可为 nil
}

// NewConnSender 构造一个绑定订阅版本的 P2Sender。
// threshold <= 0 时不启用慢消费者降级；onDegrade 在降级状态切换时回调。
func NewConnSender(conn BroadcastSink, idx *Index, sessID, topic string, version uint64, threshold int64, onDegrade func(bool)) P2Sender {
	return &connSender{
		conn: conn, idx: idx, sessID: sessID, topic: topic,
		version: version, threshold: threshold, onDegrade: onDegrade,
	}
}

func (s *connSender) setDegraded(v bool) {
	if s.degraded.Swap(v) != v && s.onDegrade != nil {
		s.onDegrade(v)
	}
}

func (s *connSender) TrySendBroadcast(frame *sendpath.ImmutableFrame) error {
	if s.conn == nil || s.conn.State() != connection.StateOpen {
		return ErrStaleSubscriber
	}
	if !s.idx.VersionMatches(s.sessID, s.topic, s.version) {
		return ErrStaleSubscriber
	}
	// 已降级且 P2 仍有积压时不再尝试入队；积压排空后放行一帧探测，
	// 成功即恢复广播投递（LLD §8.2「预算回落后自动恢复」/ B3「消费恢复后回升」）。
	if s.degraded.Load() && s.conn.BroadcastPending() > 0 {
		return ErrDegraded
	}
	err := s.conn.TrySendBroadcast(frame)
	switch {
	case err == nil:
		if s.drops.Load() != 0 || s.degraded.Load() {
			s.drops.Store(0)
			s.setDegraded(false)
		}
		return nil
	case errors.Is(err, sendpath.ErrBudgetExhausted):
		n := s.drops.Add(1)
		if s.threshold > 0 && n >= s.threshold {
			s.setDegraded(true)
		}
		return err
	default:
		// 连接关闭等：视为失效订阅者，交由扇出跳过。
		return ErrStaleSubscriber
	}
}

// IsDegraded 返回该连接是否已降级为只保留可靠消息（供指标）。
func (s *connSender) IsDegraded() bool { return s.degraded.Load() }

// Drops 返回当前连续丢弃计数（供指标）。
func (s *connSender) Drops() int64 { return s.drops.Load() }
