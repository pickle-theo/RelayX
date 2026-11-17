package benchmark

import (
	"fmt"
	"testing"
	"time"

	base "relayx/s5/benchmark/baseline"
	"relayx/s5/internal/sendpath"
)

// 扇出基准（S5 §3.1 BenchmarkFanout）：单 topic 1 个/100 个/1000 个/10000 个订阅者。
// 观测 ns/op 与 allocs/op —— 共享编码的前提是「allocs 不随订阅者数线性增长」。

const fanoutDepth = 8

// sharedSink 模拟一条订阅连接上的 P2 队列：满则丢弃（S3 §8.2，不断连）。
type sharedSink struct {
	q chan *sendpath.ImmutableFrame
}

func newSharedSinks(n int) []*sharedSink {
	sinks := make([]*sharedSink, n)
	for i := range sinks {
		sinks[i] = &sharedSink{q: make(chan *sendpath.ImmutableFrame, fanoutDepth)}
	}
	return sinks
}

func (s *sharedSink) offer(f *sendpath.ImmutableFrame) bool {
	select {
	case s.q <- f:
		f.Retain() // 队列持有 1 个引用，写完后 Release
		return true
	default:
		return false
	}
}

func (s *sharedSink) drain() int {
	n := 0
	for {
		select {
		case f := <-s.q:
			f.Release()
			n++
		default:
			return n
		}
	}
}

// byteSink 是「每订阅者各编码一次」的对照组队列。
type byteSink struct {
	q chan []byte
}

func newByteSinks(n int) []*byteSink {
	sinks := make([]*byteSink, n)
	for i := range sinks {
		sinks[i] = &byteSink{q: make(chan []byte, fanoutDepth)}
	}
	return sinks
}

func (s *byteSink) offer(b []byte) bool {
	select {
	case s.q <- b:
		return true
	default:
		return false
	}
}

func (s *byteSink) drain() int {
	n := 0
	for {
		select {
		case <-s.q:
			n++
		default:
			return n
		}
	}
}

// BenchmarkFanout_Shared 测 S3 共享帧扇出：一次编码 + N 次引用计数入队。
func BenchmarkFanout_Shared(b *testing.B) {
	payload := benchPayload(256)
	for _, subs := range []int{1, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("subs=%d", subs), func(b *testing.B) {
			sinks := newSharedSinks(subs)
			enc := newSharedEncoder(len(payload))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				f, err := enc.encode("topic.fanout", payload, uint64(i))
				if err != nil {
					b.Fatal(err)
				}
				delivered := 0
				for _, s := range sinks {
					if s.offer(f) {
						delivered++
					}
				}
				f.Release() // 扇出方释放自己那 1 个引用
				drained := 0
				for _, s := range sinks {
					drained += s.drain()
				}
				if delivered != drained {
					b.Fatalf("delivered=%d drained=%d", delivered, drained)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*subs), "ns/sub")
		})
	}
}

// BenchmarkFanout_PerSubscriber 是共享编码之前的做法：每个订阅者各编码一次。
// 它随订阅者数线性放大分配与 CPU，是 S3「一次编码」决策的量化依据。
func BenchmarkFanout_PerSubscriber(b *testing.B) {
	payload := benchPayload(256)
	now := time.Now()
	for _, subs := range []int{1, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("subs=%d", subs), func(b *testing.B) {
			sinks := newByteSinks(subs)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, s := range sinks {
					raw, err := base.EncodePerSubscriber("topic.fanout", payload, now, uint64(i))
					if err != nil {
						b.Fatal(err)
					}
					s.offer(raw)
				}
				for _, s := range sinks {
					s.drain()
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*subs), "ns/sub")
		})
	}
}
