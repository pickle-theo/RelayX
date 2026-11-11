package protocol

import (
	"bufio"
	"bytes"
	"io"
	"testing"
)

const (
	benchMaxBody      = 1 << 20
	benchDecodeFrames = 1024
)

func benchFrame(bodySize int) *Frame {
	return &Frame{Type: Data, MsgID: 1, Body: bytes.Repeat([]byte{0xab}, bodySize)}
}

func benchmarkEncode(b *testing.B, bodySize int) {
	b.Helper()
	f := benchFrame(bodySize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := Encode(io.Discard, f, benchMaxBody); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEncode64 measures encoding a DATA frame with a 64-byte body.
func BenchmarkEncode64(b *testing.B) { benchmarkEncode(b, 64) }

// BenchmarkEncode1024 measures encoding a DATA frame with a 1024-byte body.
func BenchmarkEncode1024(b *testing.B) { benchmarkEncode(b, 1024) }

// benchStream encodes n DATA frames back to back, returning the raw stream.
func benchStream(b *testing.B, bodySize, n int) []byte {
	b.Helper()
	var buf bytes.Buffer
	for i := 0; i < n; i++ {
		f := &Frame{Type: Data, MsgID: uint64(i), Body: bytes.Repeat([]byte{0xcd}, bodySize)}
		if err := Encode(&buf, f, benchMaxBody); err != nil {
			b.Fatal(err)
		}
	}
	return buf.Bytes()
}

// BenchmarkDecode64 decodes 1024 back-to-back 64-byte-body frames per
// iteration, so the reported cost is the steady-state per-frame decode.
func BenchmarkDecode64(b *testing.B) {
	data := benchStream(b, 64, benchDecodeFrames)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := bufio.NewReader(bytes.NewReader(data))
		for j := 0; j < benchDecodeFrames; j++ {
			if _, err := Decode(r, benchMaxBody); err != nil {
				b.Fatal(err)
			}
		}
	}
}
