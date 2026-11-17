package baseline

import (
	"errors"
	"io"
	"net"
	"time"

	"relayx/s5/protocol"
)

// Reader 是「需要读空闲检测时最直观的实现」（S5 §4.3 优化前）：
//   - 每帧调用一次 time.Now 判断空闲；
//   - 每帧调用一次 SetReadDeadline 刷新窗口；
//   - 读缓冲按连接各自 make（64KiB+14）。
//
// S1–S4 的读循环没有读 Deadline（断连由心跳负责），所以本对照组对应的是
// 「引入空闲检测后的朴素写法」，用于量化粗粒度扫描 + 池化缓冲的收益。
type Reader struct {
	nc  net.Conn
	buf []byte

	IdleTimeout time.Duration

	TimeCalls         int64
	DeadlineRefreshes int64
	Frames            int64
	Reads             int64
}

// NewReader 创建基线读循环（读缓冲按连接分配）。
func NewReader(nc net.Conn, bufferSize int) *Reader {
	if bufferSize <= 0 {
		bufferSize = protocol.MaxFrameSize
	}
	return &Reader{nc: nc, buf: make([]byte, bufferSize)}
}

// Run 阻塞执行读循环。
func (r *Reader) Run(h func(protocol.Frame)) error {
	pending := 0
	if r.IdleTimeout > 0 {
		r.DeadlineRefreshes++
		r.TimeCalls++
		_ = r.nc.SetReadDeadline(time.Now().Add(r.IdleTimeout))
	}
	for {
		if pending >= len(r.buf) {
			return errors.New("baseline reader: buffer full")
		}
		n, err := r.nc.Read(r.buf[pending:])
		r.Reads++
		if n > 0 {
			pending += n
			pos := 0
			for pos < pending {
				f, used, derr := protocol.Decode(r.buf[pos:pending])
				if errors.Is(derr, protocol.ErrIncomplete) {
					break
				}
				if derr != nil {
					return derr
				}
				pos += used
				r.Frames++
				if r.IdleTimeout > 0 {
					// 朴素实现：每帧刷新一次 Deadline 与一次时钟。
					r.DeadlineRefreshes++
					r.TimeCalls++
					_ = r.nc.SetReadDeadline(time.Now().Add(r.IdleTimeout))
				}
				h(f)
			}
			remain := copy(r.buf, r.buf[pos:pending])
			pending = remain
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return io.EOF
			}
			return err
		}
	}
}
