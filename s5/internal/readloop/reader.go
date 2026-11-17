// Package readloop 实现 S5 §4.3 的读路径细化（只改实现效率，不改帧语义）。
//
// 与 S1–S4 读循环的差异（均为实现层）：
//   - 时间读取按「每批一次」而非「每帧一次」：一次 Read 返回的 N 帧共用一个
//     now，供心跳判活使用（S4 是每帧 time.Now + UnixNano）。
//   - 读 Deadline 粗粒度刷新：只在「距上次刷新超过 RefreshSlack」时调用
//     SetReadDeadline，而不是每帧刷新（每帧一次是 runtime timer 与锁的高频开销）。
//   - 读缓冲来自分级池：连接建立/销毁不再各自分配 64KiB+14B 的大对象。
//
// 帧解码顺序、零拷贝约定（Frame.Body 引用读缓冲）、粘包半包处理、
// 帧超限判定与「解码失败即断连」的策略全部与 S0/S4 保持一致。
package readloop

import (
	"errors"
	"io"
	"net"
	"sync/atomic"
	"time"

	"relayx/s5/internal/pool"
	"relayx/s5/protocol"
)

// ErrBufferFull 表示缓冲区已满但仍无法解出完整帧：说明对端发了超长帧或垃圾流。
var ErrBufferFull = errors.New("readloop: read buffer full without complete frame")

// Handler 处理入站帧。Frame.Body 引用读缓冲，跨循环持有必须自行拷贝。
type Handler interface {
	HandleFrame(f protocol.Frame)
}

// Config 是读循环参数。
type Config struct {
	// BufferSize 是单连接读缓冲（默认 protocol.MaxFrameSize）。
	// 调小可省内存，但半包拼接会多几次 copy（见 benchmark 的 readloop 用例）。
	BufferSize int
	// IdleTimeout > 0 时启用读空闲兜底：超过该时长无任何入站字节即返回
	// ErrIdleTimeout。0 表示不设（断连完全由心跳负责，与 S1–S4 一致）。
	IdleTimeout time.Duration
	// RefreshSlack 是读 Deadline 的最小刷新间隔（默认 1s）。
	// 只有「距上次刷新已过 RefreshSlack」才再调 SetReadDeadline，即粗粒度扫描。
	RefreshSlack time.Duration
	// Pool 是读缓冲来源（nil 时新建连接级缓冲，不池化）。
	Pool *pool.Pool
	// Now 可注入时钟（测试用），nil 时用 time.Now。
	Now func() time.Time
}

// ErrIdleTimeout 表示读空闲超过 IdleTimeout。
var ErrIdleTimeout = errors.New("readloop: read idle timeout")

// DefaultRefreshSlack 是缺省 Deadline 刷新窗口。
const DefaultRefreshSlack = time.Second

// Stats 是读循环统计（单 goroutine 写入，读取方拿快照）。
type Stats struct {
	Reads             int64
	Bytes             int64
	Frames            int64
	DeadlineRefreshes int64
	TimeCalls         int64
	DecodeErrors      int64
	LastActivityUnix  int64
}

// Reader 是一条连接的读循环。
type Reader struct {
	nc  net.Conn
	cfg Config

	buf     []byte
	handle  *pool.Buf // 池化句柄（Pool 为 nil 时为空）
	size    int       // 有效读缓冲长度（buf 的 len 恒为 0，容量为 size）
	pending int

	stats Stats
	// lastActivity 供心跳 goroutine 并发读取，必须原子访问。
	lastActivity atomic.Int64

	// 读 Deadline 状态（单 goroutine 访问）。
	haveDeadline bool
	lastDeadline time.Time
}

// New 创建读循环。
func New(nc net.Conn, cfg Config) *Reader {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = protocol.MaxFrameSize
	}
	if cfg.RefreshSlack <= 0 {
		cfg.RefreshSlack = DefaultRefreshSlack
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	r := &Reader{nc: nc, cfg: cfg, size: cfg.BufferSize}
	if cfg.Pool != nil {
		r.handle = cfg.Pool.Get(cfg.BufferSize)
		r.buf = r.handle.Slice()
	} else {
		r.buf = make([]byte, 0, cfg.BufferSize)
	}
	if cap(r.buf) < r.size {
		r.size = cap(r.buf)
	}
	return r
}

// Buffer 返回内部读缓冲（供「处理完即归还」的调用方复核容量，测试用）。
func (r *Reader) Buffer() []byte { return r.buf }

// Close 归还读缓冲。必须在 Run 返回后调用，且之后不得再用该 Reader。
func (r *Reader) Close() {
	if r.buf == nil {
		return
	}
	if r.cfg.Pool != nil && r.handle != nil {
		r.cfg.Pool.Put(r.handle)
		r.handle = nil
	}
	r.buf = nil
	r.size = 0
}

// Stats 返回统计快照。stats 由 Run 所在 goroutine 无锁写入，因此只能在
// Run 返回后调用；并发采样应使用 LastActivity（原子）而不是本方法。
func (r *Reader) Stats() Stats {
	st := r.stats
	st.LastActivityUnix = r.lastActivity.Load()
	return st
}

// LastActivity 返回最近一次读到字节的时刻（UnixNano，0 表示尚未读到）。
// 心跳据此判活，避免每帧调用 time.Now。
func (r *Reader) LastActivity() int64 { return r.lastActivity.Load() }

// Run 阻塞执行读循环，直到连接错误或帧解码失败。返回的错误原样交给调用方，
// 由连接层决定是否记录日志与断连（S0：解码失败即断连，不回 ERROR 帧）。
func (r *Reader) Run(h Handler) error {
	// buf 的 len 恒为 0、cap 为 r.size（池化语义），读循环内部统一用全长视图。
	buf := r.buf[:r.size]
	for {
		if r.pending >= r.size {
			return ErrBufferFull
		}
		r.refreshDeadline()
		n, err := r.nc.Read(buf[r.pending:r.size])
		now := r.now()
		r.stats.Reads++
		if n > 0 {
			r.stats.Bytes += int64(n)
			r.lastActivity.Store(now.UnixNano())
			r.pending += n
			pos := 0
			for pos < r.pending {
				f, used, derr := protocol.Decode(buf[pos:r.pending])
				if errors.Is(derr, protocol.ErrIncomplete) {
					break
				}
				if derr != nil {
					r.stats.DecodeErrors++
					return derr
				}
				pos += used
				r.stats.Frames++
				h.HandleFrame(f)
			}
			remain := copy(buf, buf[pos:r.pending])
			r.pending = remain
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return io.EOF
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				// 粗粒度 Deadline 到期：这是设计内的周期唤醒，不是连接错误。
				if r.cfg.IdleTimeout > 0 && now.Sub(time.Unix(0, r.lastActivity.Load())) >= r.cfg.IdleTimeout {
					return ErrIdleTimeout
				}
				r.haveDeadline = false // 下一轮必须重设
				continue
			}
			return err
		}
	}
}

// refreshDeadline 按 RefreshSlack 粗粒度刷新读 Deadline。
//
// 语义：只有配置了 IdleTimeout 才设读 Deadline。刷新频率从「每帧一次」降到
// 「每个 RefreshSlack 至多一次」，把 SetReadDeadline（runtime timer + 锁）
// 从热路径上移走；对端不读/静默时的判活精度仍是 RefreshSlack 级别，不影响
// 心跳超时（心跳由 P0 帧与 PongTimeout 判定，与此无关）。
func (r *Reader) refreshDeadline() {
	if r.cfg.IdleTimeout <= 0 {
		return
	}
	now := r.now()
	if r.haveDeadline && now.Sub(r.lastDeadline) < r.cfg.RefreshSlack {
		return
	}
	r.stats.DeadlineRefreshes++
	_ = r.nc.SetReadDeadline(now.Add(r.cfg.IdleTimeout))
	r.lastDeadline = now
	r.haveDeadline = true
}

// now 包装一次时钟读取并计数，使「每帧多次时间读取」可被基准与测试量化。
func (r *Reader) now() time.Time {
	r.stats.TimeCalls++
	return r.cfg.Now()
}
