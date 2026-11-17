package baseline

import (
	"log"
	"net"
	"sync/atomic"
	"time"

	"relayx/s5/internal/sendpath"
)

// Writer 是 S1–S4 的写循环（单 Writer、按 P0→P1→P2→P3 取帧），
// 但每帧各做一次 SetWriteDeadline 与一次 Write。逐字节语义与
// internal/sendpath.Writer 相同，只是没有批量合并。
type Writer struct {
	sp *sendpath.SendPath
	nc net.Conn

	deadline time.Duration
	done     chan struct{}
	closed   chan struct{}

	writeCalls    atomic.Int64
	deadlineCalls atomic.Int64
	frames        atomic.Int64
}

// NewWriter 创建基线 Writer。
func NewWriter(sp *sendpath.SendPath, nc net.Conn, deadline time.Duration) *Writer {
	if deadline <= 0 {
		deadline = 10 * time.Second
	}
	return &Writer{
		sp: sp, nc: nc, deadline: deadline,
		done: make(chan struct{}), closed: make(chan struct{}),
	}
}

// Run 启动写循环，阻塞直到 Stop 或 SendPath 关闭。
func (w *Writer) Run() {
	defer close(w.closed)
	defer func() { _ = w.nc.Close() }()
	for {
		for {
			it, ok := w.sp.Pop()
			if !ok {
				break
			}
			if !w.writeOne(it) {
				return
			}
			w.sp.RecordWritten(it.Class())
			w.frames.Add(1)
		}
		select {
		case <-w.done:
			for {
				it, ok := w.sp.Pop()
				if !ok {
					return
				}
				if !w.writeOne(it) {
					return
				}
				w.sp.RecordWritten(it.Class())
				w.frames.Add(1)
			}
		case <-w.sp.Done():
			return
		case <-w.sp.Wake():
		}
	}
}

func (w *Writer) writeOne(it sendpath.Outbound) bool {
	_ = w.nc.SetWriteDeadline(time.Now().Add(w.deadline))
	w.deadlineCalls.Add(1)
	w.writeCalls.Add(1)
	if it.Shared != nil {
		if _, err := it.Shared.WriteTo(w.nc); err != nil {
			log.Default().Printf("baseline writer: write shared error: %v", err)
			it.Shared.Release()
			return false
		}
		it.Shared.Release()
		return true
	}
	if _, err := w.nc.Write(it.Raw); err != nil {
		log.Default().Printf("baseline writer: write error: %v", err)
		return false
	}
	return true
}

// Stats 返回 (write 系统调用, deadline 系统调用, 帧数)。
func (w *Writer) Stats() (writes, deadlines, frames int64) {
	return w.writeCalls.Load(), w.deadlineCalls.Load(), w.frames.Load()
}

// Stop 通知退出（幂等）。
func (w *Writer) Stop() {
	select {
	case <-w.done:
	default:
		close(w.done)
	}
}

// Closed 在 Writer 退出后关闭。
func (w *Writer) Closed() <-chan struct{} { return w.closed }
