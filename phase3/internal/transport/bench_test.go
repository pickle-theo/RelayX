package transport

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"relayx/phase3/internal/config"
	"relayx/phase3/internal/protocol"
)

// countingConn is a net.Conn that counts Write calls and discards all data.
// It is used to observe how many syscalls the writer path issues.
type countingConn struct {
	writes atomic.Int64
}

func (c *countingConn) Read([]byte) (int, error) { return 0, io.EOF }

func (c *countingConn) Write(p []byte) (int, error) {
	c.writes.Add(1)
	return len(p), nil
}

func (c *countingConn) Close() error                     { return nil }
func (c *countingConn) LocalAddr() net.Addr              { return testAddr("local") }
func (c *countingConn) RemoteAddr() net.Addr             { return testAddr("remote") }
func (c *countingConn) SetDeadline(time.Time) error      { return nil }
func (c *countingConn) SetReadDeadline(time.Time) error  { return nil }
func (c *countingConn) SetWriteDeadline(time.Time) error { return nil }

// BenchmarkSend64 measures Conn.Send: protocol encoding plus the non-blocking
// enqueue. The queue item is drained every iteration so the channel never
// fills, which isolates Send from the writer goroutine.
func BenchmarkSend64(b *testing.B) {
	cfg := config.ServerConfig{MaxBody: testMaxBody, SendQueueSize: 4096}
	c := newConn(1, &countingConn{}, cfg, nil, testLogger(), nil)
	defer c.Close()

	f := &protocol.Frame{Type: protocol.Data, MsgID: 1, Body: make([]byte, 64)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Send(f); err != nil {
			b.Fatal(err)
		}
		buf := <-c.sendCh
		c.queuedBytes.Add(-int64(len(buf)))
	}
}

// BenchmarkSend64ReuseFrame measures the pooled Send path: one Frame and Body
// are allocated up front, and the drained buffer is returned to the pool the
// same way writeLoop does, so no per-send allocation is expected.
func BenchmarkSend64ReuseFrame(b *testing.B) {
	cfg := config.ServerConfig{MaxBody: testMaxBody, SendQueueSize: 4096}
	c := newConn(1, &countingConn{}, cfg, nil, testLogger(), nil)
	defer c.Close()

	f := &protocol.Frame{Type: protocol.Data, MsgID: 1, Body: make([]byte, 64)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Send(f); err != nil {
			b.Fatal(err)
		}
		buf := <-c.sendCh
		c.queuedBytes.Add(-int64(len(buf)))
		putEncodeBuf(buf)
	}
}

// TestBaselineWriteCallsPerFrame was replaced by TestWriteCallsBatched in
// conn_test.go: the writer now coalesces data frames, so a fixed one Write per
// frame expectation no longer applies.
