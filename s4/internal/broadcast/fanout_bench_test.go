package broadcast

import (
	"context"
	"fmt"
	"testing"
)

// BenchmarkFanout 是 S5 性能工程的基线骨架（S3 LLD §7.1）：
// 观测每次 publish 的 allocs/op 与扇出延迟；共享编码不应随订阅者数线性增长。
func BenchmarkFanout(b *testing.B) {
	for _, subs := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("subs=%d", subs), func(b *testing.B) {
			idx := NewIndex(IndexLimits{
				MaxTopicsPerSession:    100,
				MaxSubscribersPerTopic: subs + 1,
				MaxTopicBytes:          256,
				MaxGlobalSubscriptions: subs + 1,
			})
			fan := NewFanout(idx, NewSharedEncoder(DefaultMaxPayload), &Metrics{})
			for i := 0; i < subs; i++ {
				_, _ = idx.Subscribe(fmt.Sprintf("s%d", i), claims("t1", "bench"),
					func(string, string, uint64) P2Sender { return fakeSender{} })
			}
			in := BroadcastInput{TenantID: "t1", Topic: "bench", Payload: []byte("benchmark-payload")}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				fan.Fanout(context.Background(), in)
			}
		})
	}
}
