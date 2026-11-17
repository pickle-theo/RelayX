package benchmark

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"

	base "relayx/s5/benchmark/baseline"
	"relayx/s5/internal/pool"
	"relayx/s5/internal/readloop"
	"relayx/s5/protocol"
)

// 读路径基准（S5 §4.3）：同一份字节流、同样的帧数，唯一变量是
// 「每帧刷新时钟/Deadline + 每连接分配缓冲」还是「每批一次 + 池化缓冲」。

const readBenchFrames = 64

// testHelper 让流构造辅助既能用于 *testing.B 也能用于 *testing.T。
type testHelper interface {
	Helper()
	Fatal(args ...any)
}

func readBenchStream(tb testHelper, frameBody int) []byte {
	tb.Helper()
	var raw []byte
	for i := 0; i < readBenchFrames; i++ {
		var err error
		raw, err = protocol.AppendFrame(raw, protocol.TypePing, uint32(i+1), make([]byte, frameBody))
		if err != nil {
			tb.Fatal(err)
		}
	}
	return raw
}

// memConn 是基准用的内存连接：每次 Run 前重置 Reader，读到 EOF 即结束。
type memConn struct {
	data []byte
	off  int
}

func (c *memConn) reset(data []byte) { c.data, c.off = data, 0 }

func (c *memConn) Read(p []byte) (int, error) {
	if c.off >= len(c.data) {
		return 0, io.EOF
	}
	n := copy(p, c.data[c.off:])
	c.off += n
	return n, nil
}

func (c *memConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *memConn) Close() error                     { return nil }
func (c *memConn) LocalAddr() net.Addr              { return benchAddr{} }
func (c *memConn) RemoteAddr() net.Addr             { return benchAddr{} }
func (c *memConn) SetDeadline(time.Time) error      { return nil }
func (c *memConn) SetReadDeadline(time.Time) error  { return nil }
func (c *memConn) SetWriteDeadline(time.Time) error { return nil }

// BenchmarkReadLoop_Baseline 是每帧刷新时钟与读 Deadline 的朴素实现。
func BenchmarkReadLoop_Baseline(b *testing.B) {
	stream := readBenchStream(b, 32)
	nc := &memConn{}
	nc.reset(stream)
	r := base.NewReader(nc, 0)
	r.IdleTimeout = time.Second
	count := 0
	b.SetBytes(int64(len(stream)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nc.reset(stream)
		r.TimeCalls, r.DeadlineRefreshes, r.Frames = 0, 0, 0
		err := r.Run(func(protocol.Frame) { count++ })
		if !errors.Is(err, io.EOF) {
			b.Fatalf("run err=%v", err)
		}
	}
	b.StopTimer()
	if count != b.N*readBenchFrames {
		b.Fatalf("frames=%d, want %d", count, b.N*readBenchFrames)
	}
	perFrame := float64(b.Elapsed().Nanoseconds()) / float64(b.N*readBenchFrames)
	b.ReportMetric(perFrame, "ns/frame")
	b.ReportMetric(float64(r.TimeCalls)/readBenchFrames, "timecalls/frame")
	b.ReportMetric(float64(r.DeadlineRefreshes)/readBenchFrames, "deadlines/frame")
}

// BenchmarkReadLoop_Pooled 是 S5 实现：池化缓冲 + 每批一次时钟 + 粗粒度 Deadline。
func BenchmarkReadLoop_Pooled(b *testing.B) {
	stream := readBenchStream(b, 32)
	nc := &memConn{}
	nc.reset(stream)
	p := pool.New(pool.Config{})
	count := 0
	b.SetBytes(int64(len(stream)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nc.reset(stream)
		r := readloop.New(nc, readloop.Config{
			Pool: p, IdleTimeout: time.Second, RefreshSlack: time.Second,
		})
		err := r.Run(readFunc(func(protocol.Frame) { count++ }))
		r.Close()
		if !errors.Is(err, io.EOF) {
			b.Fatalf("run err=%v", err)
		}
	}
	b.StopTimer()
	if count != b.N*readBenchFrames {
		b.Fatalf("frames=%d, want %d", count, b.N*readBenchFrames)
	}
	perFrame := float64(b.Elapsed().Nanoseconds()) / float64(b.N*readBenchFrames)
	b.ReportMetric(perFrame, "ns/frame")
	// 粗粒度 Deadline：每连接 1 次（首个窗口），与帧数无关。
	b.ReportMetric(1.0/float64(readBenchFrames), "deadlines/frame")
}

type readFunc func(protocol.Frame)

func (f readFunc) HandleFrame(fr protocol.Frame) { f(fr) }

// TestReadLoopEquivalence 保证两种读循环解出的帧一致（字节级等价是 A/B 的前提）。
func TestReadLoopEquivalence(t *testing.T) {
	stream := readBenchStream(t, 48)
	var gotBase, gotNew []uint32
	nc := &memConn{}
	nc.reset(stream)
	br := base.NewReader(nc, 0)
	if err := br.Run(func(f protocol.Frame) { gotBase = append(gotBase, f.Sequence) }); !errors.Is(err, io.EOF) {
		t.Fatalf("baseline err=%v", err)
	}
	nc2 := &memConn{}
	nc2.reset(stream)
	nr := readloop.New(nc2, readloop.Config{})
	if err := nr.Run(readFunc(func(f protocol.Frame) { gotNew = append(gotNew, f.Sequence) })); !errors.Is(err, io.EOF) {
		t.Fatalf("s5 err=%v", err)
	}
	nr.Close()
	if len(gotBase) != len(gotNew) || len(gotBase) != readBenchFrames {
		t.Fatalf("frames base=%d new=%d", len(gotBase), len(gotNew))
	}
	for i := range gotBase {
		if gotBase[i] != gotNew[i] {
			t.Fatalf("frame %d seq base=%d new=%d", i, gotBase[i], gotNew[i])
		}
	}
}
