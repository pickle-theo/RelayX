package benchmark

import (
	"context"
	"testing"
	"time"

	base "relayx/s5/benchmark/baseline"
	"relayx/s5/internal/sendpath"
	"relayx/s5/protocol"
)

// 写路径基准（S5 §3.1 BenchmarkSend / BenchmarkWriteBatch）：
// 同一份四通道骨架、同一批帧、同一个假连接，唯一变量是 Writer 是否批量写。
// 观测 ns/frame 与 syscalls/frame —— 后者是 §12.1「系统调用数量量化下降」的证据。

const (
	benchBurstFrames  = 8
	benchFrameBytes   = 64
	benchWriteTimeout = 5 * time.Second
)

type writerRunner interface {
	Run()
	Stop()
	Closed() <-chan struct{}
}

// runnerAdapter 统一两套 Writer 的统计口径（writes, deadlines, frames）。
type runnerAdapter struct {
	runner writerRunner
	stat   func() (writes, deadlines, frames int64)
}

func newBatchRunner(nc *benchConn, sp *sendpath.SendPath) runnerAdapter {
	w := sendpath.NewWriter(sp, nc, sendpath.WriteConfig{WriteDeadline: benchWriteTimeout}, nil)
	return runnerAdapter{runner: w, stat: func() (int64, int64, int64) {
		st := w.Stats()
		return st.WriteCalls, st.DeadlineCalls, st.Frames
	}}
}

func newPerFrameRunner(nc *benchConn, sp *sendpath.SendPath) runnerAdapter {
	w := base.NewWriter(sp, nc, benchWriteTimeout)
	return runnerAdapter{runner: w, stat: w.Stats}
}

func runSend(b *testing.B, mk func(*benchConn, *sendpath.SendPath) runnerAdapter) {
	nc := newBenchConn()
	sp := sendpath.New(benchChannels())
	adapter := mk(nc, sp)
	done := make(chan struct{})
	go func() { defer close(done); adapter.runner.Run() }()
	defer func() {
		adapter.runner.Stop()
		<-done
		sp.Close()
	}()

	frames := make([][]byte, benchBurstFrames)
	for i := range frames {
		raw, err := encodePingFrame(benchFrameBytes, uint32(i+1))
		if err != nil {
			b.Fatal(err)
		}
		frames[i] = raw
	}
	ctx := context.Background()
	total := b.N * benchBurstFrames

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 一次突发 8 帧：模拟扇出/投递批次的真实入队形态。
		for _, f := range frames {
			if err := sp.EnqueueReliable(ctx, f); err != nil {
				b.Fatalf("enqueue: %v", err)
			}
		}
	}
	waitWritten(b, sp, total)
	b.StopTimer()

	elapsed := b.Elapsed()
	nsPerFrame := float64(elapsed.Nanoseconds()) / float64(total)
	writes, deadlines, written := adapter.stat()
	b.ReportMetric(nsPerFrame, "ns/frame")
	if written > 0 {
		b.ReportMetric(float64(writes+deadlines)/float64(written), "syscalls/frame")
		b.ReportMetric(float64(writes)*1000/float64(written), "writes/frame*1e3")
		b.ReportMetric(float64(deadlines)*1000/float64(written), "deadlines/frame*1e3")
	}
}

// BenchmarkSend 是 S5 批量写 Writer。
func BenchmarkSend(b *testing.B) {
	runSend(b, newBatchRunner)
}

// BenchmarkSend_PerFrame 是 S1–S4 逐帧写 Writer（对照组）。
func BenchmarkSend_PerFrame(b *testing.B) {
	runSend(b, newPerFrameRunner)
}

// BenchmarkWriteBatch 直接量化「合并 N 帧为一次 Write」的批大小收益：
// 批越大，每帧系统调用越少；P0 延迟风险随批变大（见 §5 隔离验证）。
func BenchmarkWriteBatch(b *testing.B) {
	for _, size := range []int{1, 4, 8, 16, 32} {
		b.Run(batchName(size), func(b *testing.B) {
			nc := newBenchConn()
			sp := sendpath.New(benchChannels())
			w := sendpath.NewWriter(sp, nc, sendpath.WriteConfig{
				WriteDeadline: benchWriteTimeout, MaxBatchFrames: size,
			}, nil)
			done := make(chan struct{})
			go func() { defer close(done); w.Run() }()
			defer func() { w.Stop(); <-done; sp.Close() }()

			frame, err := encodePingFrame(benchFrameBytes, 1)
			if err != nil {
				b.Fatal(err)
			}
			ctx := context.Background()
			total := b.N * size
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := 0; j < size; j++ {
					if err := sp.EnqueueReliable(ctx, frame); err != nil {
						b.Fatalf("enqueue: %v", err)
					}
				}
			}
			waitWritten(b, sp, total)
			b.StopTimer()

			st := w.Stats()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(total), "ns/frame")
			if st.Frames > 0 {
				b.ReportMetric(float64(st.WriteCalls+st.DeadlineCalls)/float64(st.Frames), "syscalls/frame")
			}
		})
	}
}

func batchName(n int) string {
	switch n {
	case 1:
		return "batch=1"
	case 4:
		return "batch=4"
	case 8:
		return "batch=8"
	case 16:
		return "batch=16"
	case 32:
		return "batch=32"
	default:
		return "batch=n"
	}
}

func encodePingFrame(bodyLen int, seq uint32) ([]byte, error) {
	body := make([]byte, bodyLen)
	for i := range body {
		body[i] = byte(i)
	}
	return protocol.AppendFrame(nil, protocol.TypePing, seq, body)
}

func waitWritten(b *testing.B, sp *sendpath.SendPath, want int) {
	b.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if sp.Written(sendpath.ChReliable) >= int64(want) {
			return
		}
		time.Sleep(50 * time.Microsecond)
	}
	b.Fatalf("only %d/%d frames written", sp.Written(sendpath.ChReliable), want)
}
