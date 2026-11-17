package projector

import (
	"fmt"
	"sync/atomic"

	"relayx/s4/protocol"
)

// 丢弃原因（LLD §13 指标维度）。标签集合是冻结的：即使某个原因恒为 0 也要输出，
// 保证仪表盘不受服务端行为变化影响。
//
// DropStale 由端侧去重判定（state_version 回退），服务端不产生该丢弃，
// 恒 0 是有意为之（见 docs/client-events.md §指标）。
const (
	DropQueueFull     = "queue_full"
	DropOffline       = "offline"
	DropStale         = "stale"
	DropNotSubscribed = "not_subscribed"
	DropInjected      = "injected"
)

type eventCounter struct {
	typ protocol.ClientEventType
	n   atomic.Int64
}

// Metrics 是 S4 事件面指标。所有方法并发安全，可直接被 Gateway
// 合并进 /metrics 文本。
type Metrics struct {
	produced  [6]eventCounter
	delivered atomic.Int64

	queueFull     atomic.Int64
	offline       atomic.Int64
	stale         atomic.Int64
	notSubscribed atomic.Int64
	injected      atomic.Int64
	routeFailed   atomic.Int64
	noRoute       atomic.Int64

	queueDepth func() int64
}

func NewMetrics(queueDepth func() int64) *Metrics {
	return &Metrics{
		queueDepth: queueDepth,
		produced: [6]eventCounter{
			{typ: protocol.EvMessageSettled},
			{typ: protocol.EvMessageExpired},
			{typ: protocol.EvDeliveryReceiptAcked},
			{typ: protocol.EvDeliveryFailed},
			{typ: protocol.EvDeliveryExpired},
			{typ: protocol.EvMessagePartial},
		},
	}
}

func (m *Metrics) ProducedInc(t protocol.ClientEventType) {
	for i := range m.produced {
		if m.produced[i].typ == t {
			m.produced[i].n.Add(1)
			return
		}
	}
}

func (m *Metrics) DeliveredInc() { m.delivered.Add(1) }

func (m *Metrics) DroppedInc(reason string) {
	switch reason {
	case DropQueueFull:
		m.queueFull.Add(1)
	case DropOffline:
		m.offline.Add(1)
	case DropStale:
		m.stale.Add(1)
	case DropNotSubscribed:
		m.notSubscribed.Add(1)
	case DropInjected:
		m.injected.Add(1)
	}
}

func (m *Metrics) RouteFailedInc() { m.routeFailed.Add(1) }
func (m *Metrics) NoRouteInc() {
	m.noRoute.Add(1)
	m.routeFailed.Add(1)
}

func (m *Metrics) Produced() int64 {
	var n int64
	for i := range m.produced {
		n += m.produced[i].n.Load()
	}
	return n
}

// Render 输出 Prometheus 文本片段。
func (m *Metrics) Render() string {
	var b []byte
	for i := range m.produced {
		b = fmt.Appendf(b, "client_event_produced_total{type=%q} %d\n",
			string(m.produced[i].typ), m.produced[i].n.Load())
	}
	b = fmt.Appendf(b, "client_event_delivered_total %d\n", m.delivered.Load())
	b = fmt.Appendf(b, "client_event_dropped_total{reason=%q} %d\n", DropQueueFull, m.queueFull.Load())
	b = fmt.Appendf(b, "client_event_dropped_total{reason=%q} %d\n", DropOffline, m.offline.Load())
	b = fmt.Appendf(b, "client_event_dropped_total{reason=%q} %d\n", DropStale, m.stale.Load())
	b = fmt.Appendf(b, "client_event_dropped_total{reason=%q} %d\n", DropNotSubscribed, m.notSubscribed.Load())
	b = fmt.Appendf(b, "client_event_dropped_total{reason=%q} %d\n", DropInjected, m.injected.Load())
	b = fmt.Appendf(b, "client_event_route_failed_total %d\n", m.routeFailed.Load())
	b = fmt.Appendf(b, "client_event_no_route_total %d\n", m.noRoute.Load())
	if m.queueDepth != nil {
		b = fmt.Appendf(b, "client_event_queue_depth %d\n", m.queueDepth())
	}
	return string(b)
}
