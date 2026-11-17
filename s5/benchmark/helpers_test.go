package benchmark

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"relayx/s5/internal/sendpath"
	"relayx/s5/protocol"
)

// benchConn 是基准用的假连接：不放数据，只统计系统调用次数与字节数。
// 这样「每帧系统调用数」是可测量的量，而不是靠日志猜测（S5 §3.1）。
type benchConn struct {
	writes    atomic.Int64
	deadlines atomic.Int64
	bytes     atomic.Int64
	fail      atomic.Bool
}

func newBenchConn() *benchConn { return &benchConn{} }

func (c *benchConn) Write(p []byte) (int, error) {
	if c.fail.Load() {
		return 0, errors.New("bench conn: write failure")
	}
	c.writes.Add(1)
	c.bytes.Add(int64(len(p)))
	return len(p), nil
}

func (c *benchConn) Read([]byte) (int, error)         { return 0, net.ErrClosed }
func (c *benchConn) Close() error                     { return nil }
func (c *benchConn) LocalAddr() net.Addr              { return benchAddr{} }
func (c *benchConn) RemoteAddr() net.Addr             { return benchAddr{} }
func (c *benchConn) SetDeadline(time.Time) error      { return nil }
func (c *benchConn) SetReadDeadline(time.Time) error  { return nil }
func (c *benchConn) SetWriteDeadline(time.Time) error { c.deadlines.Add(1); return nil }

func (c *benchConn) syscalls() (writes, deadlines int64) {
	return c.writes.Load(), c.deadlines.Load()
}

type benchAddr struct{}

func (benchAddr) Network() string { return "bench" }
func (benchAddr) String() string  { return "bench" }

// benchChannels 返回与 S1 缺省预算同量级的四通道（P1 队列放大以容纳突发）。
func benchChannels() [4]sendpath.Channel {
	return sendpath.DefaultChannels(64, 8192, 16<<20, 512, 4<<20, 512, 4<<20)
}

// sharedEncoder 用 S5 的追加式编码 + sync.Pool 生成共享帧（P2 扇出用）。
// 池类型沿用 S3/S4 的 *sync.Pool：共享帧的归还接口在 S3 已冻结为 sync.Pool，
// 分级池只用于「独占字节」路径（编码/批量写/读缓冲）。
type sharedEncoder struct {
	pool *sync.Pool
}

func newSharedEncoder(payloadCap int) *sharedEncoder {
	size := protocol.BroadcastFrameSize("topic", payloadCap)
	return &sharedEncoder{pool: &sync.Pool{New: func() any { return make([]byte, 0, size) }}}
}

// encode 返回 refs=1 的共享帧；调用方负责 Release。
func (e *sharedEncoder) encode(topic string, payload []byte, seq uint64) (*sendpath.ImmutableFrame, error) {
	buf := e.pool.Get().([]byte)
	out, err := protocol.AppendBroadcast(buf[:0], topic, time.Now().UnixMilli(), seq, payload)
	if err != nil {
		e.pool.Put(buf)
		return nil, err
	}
	return sendpath.NewImmutableFrame(e.pool, out), nil
}
