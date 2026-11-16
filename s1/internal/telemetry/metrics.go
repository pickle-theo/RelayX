// Package telemetry 实现 S1 最小可观测集（LLD §13.1）：
// /metrics 暴露连接数、四通道队列深度、丢弃计数、frame_gap/out_of_order；
// 结构化日志字段约定见 log.go。
package telemetry

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Metrics 是进程级指标聚合。各组件持有指针并更新，HTTP handler 读取。
type Metrics struct {
	startTime   time.Time
	connections atomic.Int64
	totalLogin  atomic.Int64
	authFailed  atomic.Int64
	frameGap    atomic.Int64
	outOfOrder  atomic.Int64
	deliveries  atomic.Int64
	routeMisses atomic.Int64
	replaced    atomic.Int64

	// 四通道的队列深度与丢弃计数由 SendPath 提供；这里用函数注入以解耦。
	depthFn   func(class int) int64
	droppedFn func(class int) int64
	bytesFn   func(class int) int64
}

// New 创建 Metrics。三个函数用于读取某条连接（聚合视图）的通道指标；
// 传 nil 时对应指标不输出。S1 单机取第一条连接的 SendPath 或聚合所有连接。
func New(depth, dropped, bytes func(class int) int64) *Metrics {
	return &Metrics{
		startTime: time.Now(),
		depthFn:   depth, droppedFn: dropped, bytesFn: bytes,
	}
}

func (m *Metrics) ConnInc()       { m.connections.Add(1) }
func (m *Metrics) ConnDec()       { m.connections.Add(-1) }
func (m *Metrics) LoginInc()      { m.totalLogin.Add(1) }
func (m *Metrics) AuthFailedInc() { m.authFailed.Add(1) }
func (m *Metrics) FrameGapInc()   { m.frameGap.Add(1) }
func (m *Metrics) OutOfOrderInc() { m.outOfOrder.Add(1) }
func (m *Metrics) DeliveryInc()   { m.deliveries.Add(1) }
func (m *Metrics) RouteMissInc()  { m.routeMisses.Add(1) }
func (m *Metrics) ReplacedInc()   { m.replaced.Add(1) }

func (m *Metrics) Connections() int64 { return m.connections.Load() }

// Render 输出 Prometheus 风格的纯文本指标。
func (m *Metrics) Render() string {
	classes := []string{"CONTROL", "RELIABLE", "BROADCAST", "EVENT"}
	var b []byte
	appendI := func(name string, v int64) {
		b = fmt.Appendf(b, "%s %d\n", name, v)
	}
	appendI("gateway_uptime_seconds", int64(time.Since(m.startTime).Seconds()))
	appendI("gateway_connections", m.connections.Load())
	appendI("gateway_login_total", m.totalLogin.Load())
	appendI("gateway_auth_failed_total", m.authFailed.Load())
	appendI("gateway_frame_gap_total", m.frameGap.Load())
	appendI("gateway_frame_out_of_order_total", m.outOfOrder.Load())
	appendI("gateway_deliveries_total", m.deliveries.Load())
	appendI("gateway_route_misses_total", m.routeMisses.Load())
	appendI("gateway_sessions_replaced_total", m.replaced.Load())
	for i, c := range classes {
		if m.depthFn != nil {
			b = fmt.Appendf(b, "gateway_channel_depth{class=%q} %d\n", c, m.depthFn(i))
		}
		if m.droppedFn != nil {
			b = fmt.Appendf(b, "gateway_channel_dropped{class=%q} %d\n", c, m.droppedFn(i))
		}
		if m.bytesFn != nil {
			b = fmt.Appendf(b, "gateway_channel_bytes{class=%q} %d\n", c, m.bytesFn(i))
		}
	}
	return string(b)
}
