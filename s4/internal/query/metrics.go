package query

import (
	"fmt"
	"sync/atomic"
)

// latencyBuckets 是 client_query_latency_seconds 的桶边界（秒）。
var latencyBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1}

// Metrics 是查询 API 的延迟直方图（Prometheus 文本格式）。
type Metrics struct {
	counts [9]atomic.Int64 // 8 个显式桶 + +Inf
	sumUs  atomic.Int64
	total  atomic.Int64
}

func (m *Metrics) Observe(seconds float64) {
	m.total.Add(1)
	m.sumUs.Add(int64(seconds * 1e6))
	idx := len(latencyBuckets) // +Inf
	for i, b := range latencyBuckets {
		if seconds <= b {
			idx = i
			break
		}
	}
	// 累积桶语义：一次观测同时计入所有 >= 它的桶。
	for i := idx; i < len(m.counts); i++ {
		m.counts[i].Add(1)
	}
}

// Render 输出查询延迟直方图。
func (m *Metrics) Render() string {
	var b []byte
	for i, le := range latencyBuckets {
		b = fmt.Appendf(b, "client_query_latency_seconds_bucket{le=%q} %d\n",
			fmt.Sprintf("%g", le), m.counts[i].Load())
	}
	b = fmt.Appendf(b, "client_query_latency_seconds_bucket{le=\"+Inf\"} %d\n", m.counts[len(m.counts)-1].Load())
	b = fmt.Appendf(b, "client_query_latency_seconds_sum %g\n", float64(m.sumUs.Load())/1e6)
	b = fmt.Appendf(b, "client_query_latency_seconds_count %d\n", m.total.Load())
	return string(b)
}
