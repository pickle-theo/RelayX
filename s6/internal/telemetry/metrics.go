// Package telemetry 实现 S6 可观测集（LLD §12）。
// 沿用 S1–S4 的手写 Prometheus 文本指标约定，不引入第三方依赖。
package telemetry

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics 是进程级指标聚合。各组件持有指针并更新，HTTP handler 读取。
type Metrics struct {
	startTime time.Time

	nodesAlive         atomic.Int64
	endpointBindings   atomic.Int64
	crossNodeForwards  atomic.Int64
	forwardFailures    atomic.Int64
	staleUnbindIgnored atomic.Int64
	bindingReclaimed   atomic.Int64
	redisDegraded      atomic.Int64

	routeLookups atomic.Int64
	routeMisses  atomic.Int64
	routeLatSum  atomic.Int64 // 纳秒
	routeLatCnt  atomic.Int64

	mu        sync.RWMutex
	nackByRes map[string]int64
}

// New 创建 Metrics。
func New() *Metrics { return &Metrics{startTime: time.Now(), nackByRes: make(map[string]int64)} }

// ---- NodeRegistry 钩子 ----
func (m *Metrics) NodesAliveSet(n int)             { m.nodesAlive.Store(int64(n)) }
func (m *Metrics) BindingReclaimedInc()            { m.bindingReclaimed.Add(1) }
func (m *Metrics) EndpointBindingsSet(n int64)     { m.endpointBindings.Store(n) }
func (m *Metrics) EndpointBindingsAdd(delta int64) { m.endpointBindings.Add(delta) }

// ---- Router 钩子 ----
func (m *Metrics) RouteLookupInc() { m.routeLookups.Add(1) }
func (m *Metrics) RouteMissInc()   { m.routeMisses.Add(1) }
func (m *Metrics) RouteLookupLatency(d time.Duration) {
	if d < 0 {
		d = 0
	}
	m.routeLatSum.Add(int64(d))
	m.routeLatCnt.Add(1)
}
func (m *Metrics) StaleUnbindIgnoredInc() { m.staleUnbindIgnored.Add(1) }
func (m *Metrics) RedisDegradedSet(on bool) {
	if on {
		m.redisDegraded.Store(1)
	} else {
		m.redisDegraded.Store(0)
	}
}

// ---- Forwarder 钩子 ----
func (m *Metrics) CrossNodeForwardsInc()        { m.crossNodeForwards.Add(1) }
func (m *Metrics) CrossNodeForwardFailuresInc() { m.forwardFailures.Add(1) }
func (m *Metrics) ForwardNackInc(reason string) {
	m.mu.Lock()
	m.nackByRes[reason]++
	m.mu.Unlock()
}

// Render 输出 Prometheus 风格纯文本指标。
func (m *Metrics) Render() string {
	var b []byte
	appendI := func(name string, v int64) {
		b = fmt.Appendf(b, "%s %d\n", name, v)
	}
	appendI("gateway_uptime_seconds", int64(time.Since(m.startTime).Seconds()))
	appendI("gateway_nodes_alive", m.nodesAlive.Load())
	appendI("gateway_endpoint_bindings", m.endpointBindings.Load())
	appendI("gateway_cross_node_forwards_total", m.crossNodeForwards.Load())
	appendI("gateway_cross_node_forward_failures_total", m.forwardFailures.Load())
	appendI("gateway_route_lookup_total", m.routeLookups.Load())
	appendI("gateway_route_misses_total", m.routeMisses.Load())
	appendI("gateway_stale_unbind_ignored_total", m.staleUnbindIgnored.Load())
	appendI("gateway_endpoint_binding_reclaimed_total", m.bindingReclaimed.Load())
	appendI("gateway_redis_degraded", m.redisDegraded.Load())

	cnt := m.routeLatCnt.Load()
	if cnt > 0 {
		avg := time.Duration(m.routeLatSum.Load() / cnt)
		b = fmt.Appendf(b, "gateway_route_lookup_avg_ns %d\n", int64(avg))
	}

	m.mu.RLock()
	reasons := make([]string, 0, len(m.nackByRes))
	for r := range m.nackByRes {
		reasons = append(reasons, r)
	}
	sort.Strings(reasons)
	for _, r := range reasons {
		b = fmt.Appendf(b, "gateway_forward_nack_total{reason=%q} %d\n", r, m.nackByRes[r])
	}
	m.mu.RUnlock()
	return string(b)
}
