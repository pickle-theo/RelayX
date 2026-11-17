package benchmark

import (
	"bytes"
	"sync"
	"testing"
	"time"

	base "relayx/s5/benchmark/baseline"
	"relayx/s5/internal/pool"
	"relayx/s5/protocol"
)

// 基准清单（S5 §3.1）：Encode/Decode 的 ns/op、B/op、allocs/op 是编码路径
// 是否值得池化的唯一依据。

func benchPayload(n int) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = byte(i)
	}
	return p
}

// BenchmarkEncode 测两段式编码（S0 冻结实现）：Body 先分配、再拷进帧缓冲。
func BenchmarkEncode(b *testing.B) {
	body := benchPayload(512)
	f := protocol.Frame{Type: protocol.TypeDeliver, Sequence: 1, Body: body}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := protocol.Encode(f, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEncodeIntoBuffer 测「调用方复用缓冲」的两段式编码。
func BenchmarkEncodeIntoBuffer(b *testing.B) {
	body := benchPayload(512)
	f := protocol.Frame{Type: protocol.TypeDeliver, Sequence: 1, Body: body}
	buf := make([]byte, 0, protocol.HeaderSize+len(body))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := protocol.Encode(f, buf[:0]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAppendEncode 测 S5 追加式编码（写入池化缓冲，零额外分配）。
func BenchmarkAppendEncode(b *testing.B) {
	body := benchPayload(512)
	p := pool.New(pool.Config{})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := p.Get(protocol.HeaderSize + len(body))
		buf, err := protocol.AppendFrame(h.Slice(), protocol.TypeDeliver, 1, body)
		if err != nil {
			b.Fatal(err)
		}
		h.Set(buf)
		p.Put(h)
	}
}

// BenchmarkEncodeBroadcast_Baseline 是 S3/S4 的共享帧编码（两段式）。
func BenchmarkEncodeBroadcast_Baseline(b *testing.B) {
	payload := benchPayload(256)
	p := &sync.Pool{New: func() any { return make([]byte, 0, protocol.MaxFrameSize) }}
	now := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f, err := base.EncodeSharedFrame(p, "topic.loadgen", payload, now, uint64(i))
		if err != nil {
			b.Fatal(err)
		}
		_ = f.Len()
		f.Release()
	}
}

// BenchmarkEncodeBroadcast_Append 是 S5 的共享帧编码（一次写入池化缓冲）。
func BenchmarkEncodeBroadcast_Append(b *testing.B) {
	payload := benchPayload(256)
	enc := newSharedEncoder(len(payload))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f, err := enc.encode("topic.loadgen", payload, uint64(i))
		if err != nil {
			b.Fatal(err)
		}
		_ = f.Len()
		f.Release()
	}
}

// BenchmarkDecode 测零拷贝解码（Frame.Body 引用输入缓冲，不额外分配）。
func BenchmarkDecode(b *testing.B) {
	body := benchPayload(512)
	raw, err := protocol.Encode(protocol.Frame{Type: protocol.TypeDeliver, Sequence: 1, Body: body}, nil)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f, used, err := protocol.Decode(raw)
		if err != nil {
			b.Fatal(err)
		}
		if used != len(raw) || len(f.Body) != len(body) {
			b.Fatalf("decode mismatch used=%d body=%d", used, len(f.Body))
		}
	}
}

// BenchmarkDecodeBatch 测「一次读缓冲内连续解 N 帧」，是读循环的真实形态。
func BenchmarkDecodeBatch(b *testing.B) {
	const frames = 16
	body := benchPayload(64)
	var raw []byte
	for i := 0; i < frames; i++ {
		var err error
		raw, err = protocol.AppendFrame(raw, protocol.TypePing, uint32(i+1), body)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pos := 0
		n := 0
		for pos < len(raw) {
			_, used, err := protocol.Decode(raw[pos:])
			if err != nil {
				if err == protocol.ErrIncomplete {
					break
				}
				b.Fatal(err)
			}
			pos += used
			n++
		}
		if n != frames {
			b.Fatalf("decoded %d frames, want %d", n, frames)
		}
	}
}

// BenchmarkPoolGetPut 测分级池取还（与 make 对照）。
func BenchmarkPoolGetPut(b *testing.B) {
	p := pool.New(pool.Config{})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := p.Get(600)
		h.Set(append(h.Slice(), 0))
		p.Put(h)
	}
}

// allocSink 阻止编译器把对照组缓冲优化掉（保证真的发生堆分配）。
var allocSink []byte

// BenchmarkMakeBuffer 是分级池的对照组：每次 make 一块 512B 缓冲。
func BenchmarkMakeBuffer(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := make([]byte, 0, 600)
		buf = append(buf, 0)
		allocSink = buf
	}
}

// TestAppendEncodeEqualsEncode 是基准目录内的等价性复核：
// 优化后的编码与冻结编码必须逐字节一致，否则基准结论无意义。
func TestAppendEncodeEqualsEncode(t *testing.T) {
	body := benchPayload(1024)
	want, err := protocol.Encode(protocol.Frame{Type: protocol.TypeBroadcast, Sequence: 5, Body: body}, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := protocol.AppendFrame(nil, protocol.TypeBroadcast, 5, body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("append-encode differs from frozen Encode")
	}
}
