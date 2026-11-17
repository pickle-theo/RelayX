package sendpath

import (
	"log"
	"net"
	"time"
)

// Writer 是唯一写 Socket 的 goroutine。它按 P0→P1→P2→P3 优先级从 SendPath 取帧。
type Writer struct {
	sp     *SendPath
	nc     net.Conn
	cfg    WriteConfig
	log    *log.Logger
	done   chan struct{}
	closed chan struct{}
}

// WriteConfig 是 Writer 的写参数。
type WriteConfig struct {
	WriteDeadline time.Duration
	OnClose       func() // Socket 关闭后的回调（用于连接层清理），可为 nil
}

// NewWriter 创建 Writer（尚未启动）。
func NewWriter(sp *SendPath, nc net.Conn, cfg WriteConfig, logger *log.Logger) *Writer {
	if logger == nil {
		logger = log.Default()
	}
	return &Writer{
		sp: sp, nc: nc, cfg: cfg, log: logger,
		done: make(chan struct{}), closed: make(chan struct{}),
	}
}

// Run 启动写循环，阻塞直到 Stop 或 SendPath 关闭。仅应在一个 goroutine 中调用。
func (w *Writer) Run() {
	defer close(w.closed)
	defer func() {
		_ = w.nc.Close()
		if w.cfg.OnClose != nil {
			w.cfg.OnClose()
		}
	}()

	for {
		// 优先排空所有帧；有帧时不阻塞。
		for {
			it, ok := w.sp.Pop()
			if !ok {
				break
			}
			if !w.writeFrame(it) {
				return
			}
		}
		select {
		case <-w.done:
			// 退出前再排空一次已入队的帧（与 S0 一致：错误响应先送达）。
			for {
				it, ok := w.sp.Pop()
				if !ok {
					return
				}
				if !w.writeFrame(it) {
					return
				}
			}
		case <-w.sp.Done():
			return
		case <-w.sp.Wake():
			// 有新帧，回到顶部按优先级取。
		}
	}
}

// writeFrame 写出一帧。共享帧写完立即 Release（含写失败路径），
// 保证引用不会因 Writer 提前退出而泄漏。
func (w *Writer) writeFrame(it Outbound) bool {
	_ = w.nc.SetWriteDeadline(time.Now().Add(w.cfg.WriteDeadline))
	if it.Shared != nil {
		_, err := it.Shared.WriteTo(w.nc)
		it.Shared.Release()
		if err != nil {
			w.log.Printf("sendpath writer: write shared frame error: %v", err)
			return false
		}
		return true
	}
	if _, err := w.nc.Write(it.Raw); err != nil {
		w.log.Printf("sendpath writer: write error: %v", err)
		return false
	}
	return true
}

// Stop 通知 Writer 退出并关闭 Socket（幂等）。
func (w *Writer) Stop() {
	select {
	case <-w.done:
		return
	default:
		close(w.done)
	}
}

// Closed 在 Writer 退出（Socket 已关闭）后关闭。
func (w *Writer) Closed() <-chan struct{} { return w.closed }
