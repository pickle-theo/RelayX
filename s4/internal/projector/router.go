package projector

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"relayx/s4/internal/sendpath"
	"relayx/s4/protocol"
)

// ClientEvent 是投影层产出的服务端事件。Seq 在投影阶段恒为 0，
// 唯一分配点是 Router 在确认会话在线后调用 SessionSeqAllocator（LLD §5.3）。
//
// ProducerID / SourceDeviceID 是路由键，不进入线上帧——它们来自
// 事实 payload_summary（S7）或 messages.source_device_id（S4 单机）。
type ClientEvent struct {
	EventID        string
	Seq            uint64
	Type           protocol.ClientEventType
	TenantID       string
	MessageID      string
	StateVersion   int
	Counts         *protocol.DeliveryCounts
	OccurredAt     time.Time
	ProducerID     string
	SourceDeviceID string
}

// EventSink 是一条已登录会话的事件出口（P3 通道）。
type EventSink interface {
	SessionID() string
	EventMask() protocol.EventMask
	// EnqueueClientEvent 入队 P3；预算耗尽返回 sendpath.ErrBudgetExhausted，
	// 连接已关闭返回 sendpath.ErrClosed。
	EnqueueClientEvent(f protocol.Frame) error
}

// SessionLocator 按路由键定位发送设备的当前会话。
type SessionLocator interface {
	Locate(tenantID, producerID, sourceDeviceID string) (EventSink, bool)
}

// SessionSeqAllocator 是 per-session 序号的唯一分配点。
type SessionSeqAllocator interface {
	Next(sessionID string) uint64
}

// SeqAllocator 为每个会话维护独立计数器，从 1 起单调，会话结束即销毁。
type SeqAllocator struct {
	mu       sync.Mutex
	counters map[string]uint64
}

func NewSeqAllocator() *SeqAllocator {
	return &SeqAllocator{counters: make(map[string]uint64)}
}

func (a *SeqAllocator) Next(sessionID string) uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.counters[sessionID]++
	return a.counters[sessionID]
}

// Delete 在会话结束时销毁计数器，避免长跑进程无界增长。
func (a *SeqAllocator) Delete(sessionID string) {
	a.mu.Lock()
	delete(a.counters, sessionID)
	a.mu.Unlock()
}

// Router 把 self-scoped 事件投递到发送设备自己的会话。
// 离线直接丢弃，不重试、不落库、不降级为"推给全部设备"。
type Router struct {
	locator   SessionLocator
	allocator SessionSeqAllocator
	metrics   *Metrics
	log       *slog.Logger

	// injectedSkip 是验证钩子：分配 seq 后丢弃 N 条事件，用于制造 gap。
	injectedSkip atomic.Int64
}

func NewRouter(locator SessionLocator, allocator SessionSeqAllocator, metrics *Metrics, log *slog.Logger) *Router {
	if log == nil {
		log = slog.Default()
	}
	return &Router{locator: locator, allocator: allocator, metrics: metrics, log: log}
}

// SkipNext 让接下来 n 条事件在分配 seq 后被丢弃（仅 -test-hooks 开启时调用）。
func (r *Router) SkipNext(n int64) {
	r.injectedSkip.Add(n)
}

func (r *Router) takeInjected() bool {
	for {
		cur := r.injectedSkip.Load()
		if cur <= 0 {
			return false
		}
		if r.injectedSkip.CompareAndSwap(cur, cur-1) {
			return true
		}
	}
}

// Route 执行 self-scoped 路由。返回 error 仅代表"本批不应推进游标"
// （例如设备在线但入队出现非预算类错误）；离线/未订阅属于正常丢弃。
func (r *Router) Route(_ context.Context, ev ClientEvent) error {
	if ev.SourceDeviceID == "" {
		// HTTP 提交 / 服务令牌 / 令牌未绑定设备：无设备可路由。
		// 绝不降级为"推给该用户全部设备"。
		r.metrics.NoRouteInc()
		r.log.Debug("client event has no route: empty source device",
			"event_id", ev.EventID, "message_id", ev.MessageID)
		return nil
	}
	sink, ok := r.locator.Locate(ev.TenantID, ev.ProducerID, ev.SourceDeviceID)
	if !ok {
		r.metrics.RouteFailedInc()
		r.metrics.DroppedInc(DropOffline)
		return nil
	}
	bit, known := protocol.EventTypeMask(ev.Type)
	if !known {
		r.metrics.DroppedInc(DropNotSubscribed)
		return nil
	}
	if sink.EventMask()&bit == 0 {
		r.metrics.DroppedInc(DropNotSubscribed)
		return nil
	}

	sessionID := sink.SessionID()
	seq := r.allocator.Next(sessionID)
	if r.takeInjected() {
		r.metrics.DroppedInc(DropInjected)
		return nil
	}

	frame := protocol.Frame{
		Type: protocol.TypeClientEvent,
		Body: protocol.EncodeClientEvent(protocol.ClientEventBody{
			EventID:      ev.EventID,
			Seq:          seq,
			Type:         ev.Type,
			TenantID:     ev.TenantID,
			MessageID:    ev.MessageID,
			StateVersion: int32(ev.StateVersion),
			Counts:       ev.Counts,
			OccurredAt:   ev.OccurredAt.UnixMilli(),
		}),
	}
	if err := sink.EnqueueClientEvent(frame); err != nil {
		switch {
		case errors.Is(err, sendpath.ErrBudgetExhausted):
			// P3 预算独立：丢弃本事件，不断连、不影响 P1/P2。
			r.metrics.DroppedInc(DropQueueFull)
			return nil
		case errors.Is(err, sendpath.ErrClosed):
			r.metrics.DroppedInc(DropOffline)
			return nil
		default:
			r.metrics.RouteFailedInc()
			return err
		}
	}
	r.metrics.DeliveredInc()
	return nil
}
