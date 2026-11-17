package broadcast

import (
	"fmt"
	"sync/atomic"
	"time"
)

// latencyBuckets 是扇出延迟直方图的上限（微秒）。
var latencyBuckets = [...]int64{10, 50, 100, 500, 1000, 5000, 10000, 50000}

type latencyHistogram struct {
	buckets [len(latencyBuckets)]atomic.Int64
	count   atomic.Int64
	sum     atomic.Int64
}

func (h *latencyHistogram) Observe(d time.Duration) {
	us := d.Microseconds()
	if us < 0 {
		us = 0
	}
	for i, b := range latencyBuckets {
		if us <= b {
			h.buckets[i].Add(1)
			break
		}
	}
	h.count.Add(1)
	h.sum.Add(us)
}

// Metrics 实现 LLD §11 的广播可观测指标，全部用原子计数，无外部依赖。
type Metrics struct {
	subscribers   atomic.Int64
	activeTopics  atomic.Int64
	publishMsgs   atomic.Int64
	publishBytes  atomic.Int64
	fanoutFrames  atomic.Int64
	droppedBudget atomic.Int64
	droppedStale  atomic.Int64
	droppedDegr   atomic.Int64
	slowConsumers atomic.Int64
	indexFull     atomic.Int64
	subscribeFail atomic.Int64
	latency       latencyHistogram
}

func (m *Metrics) SetIndexStats(s IndexStats) {
	m.subscribers.Store(int64(s.Subscriptions))
	m.activeTopics.Store(int64(s.Topics))
	m.indexFull.Store(s.FullEvents)
}

func (m *Metrics) PublishInc(payloadBytes int) {
	m.publishMsgs.Add(1)
	m.publishBytes.Add(int64(payloadBytes))
}

func (m *Metrics) FanoutInc(targets, enqueued int) {
	m.fanoutFrames.Add(int64(enqueued))
	_ = targets
}

func (m *Metrics) FanoutLatency(d time.Duration) { m.latency.Observe(d) }

func (m *Metrics) DroppedBudgetInc()        { m.droppedBudget.Add(1) }
func (m *Metrics) DroppedStaleInc()         { m.droppedStale.Add(1) }
func (m *Metrics) DroppedDegradedInc()      { m.droppedDegr.Add(1) }
func (m *Metrics) SetSlowConsumers(n int64) { m.slowConsumers.Store(n) }
func (m *Metrics) SubscribeFailInc()        { m.subscribeFail.Add(1) }

var metricNames = [...]string{
	"broadcast_subscribers",
	"broadcast_active_topics",
	"broadcast_publish_tps_total",
	"broadcast_publish_bytes_total",
	"broadcast_fanout_frames_total",
	"broadcast_slow_consumers",
	"broadcast_index_full_total",
	"broadcast_subscribe_failed_total",
}

// Render 输出 Prometheus 文本格式片段。
func (m *Metrics) Render() string {
	var b []byte
	vals := [...]int64{
		m.subscribers.Load(), m.activeTopics.Load(), m.publishMsgs.Load(),
		m.publishBytes.Load(), m.fanoutFrames.Load(),
		m.slowConsumers.Load(), m.indexFull.Load(), m.subscribeFail.Load(),
	}
	for i, name := range metricNames {
		b = fmt.Appendf(b, "%s %d\n", name, vals[i])
	}
	b = fmt.Appendf(b, "broadcast_dropped_total{reason=%q} %d\n", "budget", m.droppedBudget.Load())
	b = fmt.Appendf(b, "broadcast_dropped_total{reason=%q} %d\n", "stale", m.droppedStale.Load())
	b = fmt.Appendf(b, "broadcast_dropped_total{reason=%q} %d\n", "degraded", m.droppedDegr.Load())
	// Prometheus 的 _bucket 是累积语义（le=X 表示 ≤X 的观测数）：
	// 必须输出前缀和，否则直方图非单调，histogram_quantile 得到错误分位。
	cumulative := int64(0)
	for i, upper := range latencyBuckets {
		cumulative += m.latency.buckets[i].Load()
		b = fmt.Appendf(b, "broadcast_fanout_latency_us_bucket{le=%q} %d\n",
			fmt.Sprintf("%d", upper), cumulative)
	}
	b = fmt.Appendf(b, "broadcast_fanout_latency_us_bucket{le=%q} %d\n", "+Inf", m.latency.count.Load())
	b = fmt.Appendf(b, "broadcast_fanout_latency_us_sum %d\n", m.latency.sum.Load())
	b = fmt.Appendf(b, "broadcast_fanout_latency_us_count %d\n", m.latency.count.Load())
	return string(b)
}
