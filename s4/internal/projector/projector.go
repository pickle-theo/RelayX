package projector

import (
	"context"
	"log/slog"
	"time"

	"relayx/s4/internal/core"
	"relayx/s4/protocol"
)

// ProjectorName 是 projection_cursor 表中的投影器标识。
const ProjectorName = "client_event"

// FactSource 是只读事实源（游标键集分页）。单机阶段由内部事实内核实现；
// S7 起换成 Kafka facts 总线，本接口与线上帧 schema 零改动。
type FactSource interface {
	ListFactsAfter(afterAt time.Time, afterID string, types []string, limit int) []core.Fact
}

// MessageRoute 是 S4 单机阶段的等价回查：来源设备优先取事实
// payload_summary，缺失时回查 messages.source_device_id（LLD §5.3）。
type MessageRoute interface {
	Route(messageID string) (producerID, sourceDeviceID string, ok bool)
}

// EventRouter 是事件分发接口（*Router 实现）。
type EventRouter interface {
	Route(ctx context.Context, ev ClientEvent) error
}

// TypesProvider 返回当前需要投影的事实类型并集（来自各会话已授予的
// 事件位）。空实现只投影聚合终态。
type TypesProvider func() []string

// Options 是投影器依赖。
type Options struct {
	Source   FactSource
	Messages MessageRoute
	Router   EventRouter
	Cursor   CursorStore
	Interval time.Duration
	Batch    int
	Types    TypesProvider
	// Signal 是可选的"有新事实"唤醒通道（低延迟优化）。
	Signal  func() <-chan struct{}
	Metrics *Metrics
	Log     *slog.Logger
}

// Projector 轮询事实并投影为客户端事件（S4 LLD §5.4）。
// 它只读事实，不写状态、不改 S2 状态机。
type Projector struct {
	src      FactSource
	messages MessageRoute
	router   EventRouter
	cursor   CursorStore
	interval time.Duration
	batch    int
	types    TypesProvider
	signal   func() <-chan struct{}
	metrics  *Metrics
	log      *slog.Logger
}

func New(o Options) *Projector {
	interval := o.Interval
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	batch := o.Batch
	if batch <= 0 {
		batch = 500
	}
	logger := o.Log
	if logger == nil {
		logger = slog.Default()
	}
	types := o.Types
	if types == nil {
		types = func() []string {
			return []string{core.FactMessageSettled, core.FactMessageExpired}
		}
	}
	metrics := o.Metrics
	if metrics == nil {
		metrics = NewMetrics(nil)
	}
	return &Projector{
		src: o.Source, messages: o.Messages, router: o.Router, cursor: o.Cursor,
		interval: interval, batch: batch, types: types, signal: o.Signal,
		metrics: metrics, log: logger,
	}
}

// Project 把单条事实投影为零或多条客户端事件。投影阶段 Seq 恒为 0。
func (p *Projector) Project(f core.Fact) ([]ClientEvent, error) {
	var typ protocol.ClientEventType
	var withCounts bool
	switch f.FactType {
	case core.FactMessageSettled:
		typ, withCounts = protocol.EvMessageSettled, true
	case core.FactMessageExpired:
		typ, withCounts = protocol.EvMessageExpired, true
	case core.FactDeliveryReceiptAcked:
		typ = protocol.EvDeliveryReceiptAcked
	case core.FactDeliveryFailed:
		typ = protocol.EvDeliveryFailed
	case core.FactDeliveryExpired:
		typ = protocol.EvDeliveryExpired
	default:
		return nil, nil // 非终态事实不投影（accepted/created/sent 不产生客户端事件）
	}

	producerID, sourceDevice := routeFromSummary(f.PayloadSummary)
	if producerID == "" || sourceDevice == "" {
		// S4 单机等价实现：回查 messages.source_device_id。
		// 回查失败不放大到全设备，路由键保持空值由 Router 计 no_route。
		if p.messages != nil {
			if pid, sd, ok := p.messages.Route(f.MessageID); ok {
				if producerID == "" {
					producerID = pid
				}
				if sourceDevice == "" {
					sourceDevice = sd
				}
			}
		}
	}

	ev := ClientEvent{
		EventID:        f.EventID,
		Seq:            0, // §5.3：投影阶段不分配
		Type:           typ,
		TenantID:       f.TenantID,
		MessageID:      f.MessageID,
		StateVersion:   f.StateVersion,
		OccurredAt:     f.OccurredAt,
		ProducerID:     producerID,
		SourceDeviceID: sourceDevice,
	}
	if withCounts {
		c, ok := CountsFromSummary(f.PayloadSummary)
		if !ok {
			p.log.Warn("client event fact missing counts summary",
				"event_id", f.EventID, "message_id", f.MessageID, "fact_type", f.FactType)
		}
		ev.Counts = &c
	}
	return []ClientEvent{ev}, nil
}

// RunOnce 执行一轮：Load → 读一批 → Project/Route → 全部成功后 Save。
// 任何路由错误都不推进游标，下一轮重放（崩溃只导致重复，不导致漏读）。
func (p *Projector) RunOnce(ctx context.Context) (int, error) {
	cur, err := p.cursor.Load(ctx, ProjectorName)
	if err != nil {
		return 0, err
	}
	facts := p.src.ListFactsAfter(cur.At, cur.EventID, p.types(), p.batch)
	if len(facts) == 0 {
		return 0, nil
	}
	var last Cursor
	for _, f := range facts {
		events, err := p.Project(f)
		if err != nil {
			return 0, err
		}
		for _, ev := range events {
			p.metrics.ProducedInc(ev.Type)
			if err := p.router.Route(ctx, ev); err != nil {
				return 0, err
			}
		}
		last = Cursor{At: f.OccurredAt, EventID: f.EventID}
	}
	if err := p.cursor.Save(ctx, ProjectorName, last); err != nil {
		return 0, err
	}
	return len(facts), nil
}

// Run 是 S4 的唯一触发源：轮询游标扫描（信号只是加速唤醒）。
func (p *Projector) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	var signal <-chan struct{}
	if p.signal != nil {
		signal = p.signal()
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		case <-signal:
		}
		for {
			n, err := p.RunOnce(ctx)
			if err != nil {
				p.log.Warn("client event projection batch failed", "err", err)
				break
			}
			if n < p.batch {
				break
			}
		}
	}
}
