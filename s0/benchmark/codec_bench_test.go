// Package benchmark 提供 S0 编解码与写循环的基准骨架（S5 优化基线）。
//
// 运行：go test -bench=. -benchmem ./benchmark/...
// S0 只建骨架不优化；S5 在此基线上做前后对比。
package benchmark

import (
	"io"
	"testing"

	"relayx/s0/internal/protocol"
)

func sampleFrame(size int) protocol.Frame {
	return protocol.Frame{
		Type:     protocol.TypeEcho,
		Sequence: 1,
		Body:     make([]byte, size),
	}
}

func BenchmarkEncode(b *testing.B) {
	for _, size := range []int{0, 64, 1024, 8192} {
		b.Run(sizeName(size), func(b *testing.B) {
			f := sampleFrame(size)
			dst := make([]byte, 0, protocol.MaxFrameSize)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = protocol.Encode(f, dst)
			}
		})
	}
}

func BenchmarkDecode(b *testing.B) {
	for _, size := range []int{0, 64, 1024, 8192} {
		b.Run(sizeName(size), func(b *testing.B) {
			encoded, _ := protocol.Encode(sampleFrame(size), nil)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, _ = protocol.Decode(encoded)
			}
		})
	}
}

// discardWriter 模拟只写目标，用于 BenchmarkSend 测量写循环开销。
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
func (discardWriter) Close() error                { return nil }

var _ io.Writer = discardWriter{}

func BenchmarkSend(b *testing.B) {
	encoded, _ := protocol.Encode(sampleFrame(1024), nil)
	w := discardWriter{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := w.Write(encoded); err != nil {
			b.Fatal(err)
		}
	}
}

func sizeName(n int) string {
	switch {
	case n == 0:
		return "empty"
	case n < 1024:
		return "64B"
	case n < 8192:
		return "1KiB"
	default:
		return "8KiB"
	}
}
