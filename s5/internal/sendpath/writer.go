package sendpath

import (
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"relayx/s5/internal/pool"
)

// Writer 是唯一写 Socket 的 goroutine（S1 §4.3 单 Writer 原则不变）。
//
// S5 只改写出方式：一次取多帧、一次 SetWriteDeadline、合并为一次 Write。
// 隔离语义完全不变——取帧顺序仍是 P0→P1→P2→P3，每条通道的帧数/字节预算
// 仍由 SendPath 独立扣减，Writer 只是把「已取出的帧」合并落盘。
type Writer struct {
	sp  *SendPath
	nc  net.Conn
	cfg WriteConfig
	log *log.Logger

	done     chan struct{}
	closed   chan struct{}
	stopOnce sync.Once

	// 批量收集缓冲（仅 Writer goroutine 访问，无需锁）。
	batch      []Outbound
	pending    Outbound
	hasPending bool

	// 统计：平均每帧系统调用数 = (writeCalls+deadlineCalls) / framesWritten。
	batches        atomic.Int64
	framesWritten  atomic.Int64
	writeCalls     atomic.Int64
	deadlineCalls  atomic.Int64
	coalescedBytes atomic.Int64
}

// DefaultWriteDeadline 是 WriteConfig.WriteDeadline 为零值时的缺省写超时。
const DefaultWriteDeadline = 5 * time.Second

// WriteConfig 是 Writer 的写参数。
type WriteConfig struct {
	// WriteDeadline 是单批写出的超时；<=0 使用 DefaultWriteDeadline。
	// 不能用零值直接调 SetWriteDeadline：那等价于「立即超时」，所有写都会失败。
	WriteDeadline time.Duration
	OnClose       func() // Socket 关闭后的回调（用于连接层清理），可为 nil

	// MaxBatchFrames 是单批最多帧数（默认 32）。1 表示退回逐帧写。
	MaxBatchFrames int
	// MaxBatchBytes 是单批合并上限（默认 64KiB）；超过则本批退化为逐帧写，
	// 避免为「少量大帧」付一次全量拷贝。
	MaxBatchBytes int
	// BufferPool 是合并缓冲的来源；nil 时使用包内缺省分级池。
	BufferPool *pool.Pool
}

// DefaultBatchFrames / DefaultBatchBytes 是缺省批量参数（基准标定见
// docs/benchmark-report.md：批大小超过 32 帧后 syscall 收益饱和，而 P0 延迟变大）。
const (
	DefaultBatchFrames = 32
	DefaultBatchBytes  = 64 << 10
)

var defaultPool = pool.New(pool.Config{})

// WriteStats 是 Writer 的写出统计（供压测报告计算「每帧系统调用数」）。
type WriteStats struct {
	Batches       int64
	Frames        int64
	WriteCalls    int64
	DeadlineCalls int64
	// SyscallsPerFrame 是每帧平均系统调用次数（Write + SetWriteDeadline）。
	SyscallsPerFrame float64
}

// NewWriter 创建 Writer（尚未启动）。
func NewWriter(sp *SendPath, nc net.Conn, cfg WriteConfig, logger *log.Logger) *Writer {
	if logger == nil {
		logger = log.Default()
	}
	if cfg.MaxBatchFrames <= 0 {
		cfg.MaxBatchFrames = DefaultBatchFrames
	}
	if cfg.MaxBatchBytes <= 0 {
		cfg.MaxBatchBytes = DefaultBatchBytes
	}
	if cfg.WriteDeadline <= 0 {
		cfg.WriteDeadline = DefaultWriteDeadline
	}
	if cfg.BufferPool == nil {
		cfg.BufferPool = defaultPool
	}
	return &Writer{
		sp: sp, nc: nc, cfg: cfg, log: logger,
		done: make(chan struct{}), closed: make(chan struct{}),
		batch: make([]Outbound, 0, cfg.MaxBatchFrames),
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
	// 退出清理：已取出但未写出的共享帧必须释放引用，否则池中 buffer 永不归还。
	// pending 是「本批放不下、留待下批」的那一帧；batch 是当前批的残余槽位。
	defer func() {
		if w.hasPending {
			releaseShared([]Outbound{w.pending})
			w.hasPending = false
		}
		releaseShared(w.batch)
	}()

	for {
		// 优先排空所有帧；有帧时不阻塞。
		for {
			batch := w.collect()
			if len(batch) == 0 {
				break
			}
			if !w.writeBatch(batch) {
				return
			}
		}
		select {
		case <-w.done:
			// 退出前再排空一次已入队的帧（与 S0 一致：错误响应先送达）。
			for {
				batch := w.collect()
				if len(batch) == 0 {
					return
				}
				if !w.writeBatch(batch) {
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

// collect 按优先级取一批帧，返回内部复用切片。
// 若下一帧会超出批量预算，则暂存为 pending 留到下一批（不丢帧、不阻塞）。
func (w *Writer) collect() []Outbound {
	w.batch = w.batch[:0]
	total := 0
	for len(w.batch) < w.cfg.MaxBatchFrames {
		var it Outbound
		if w.hasPending {
			it, w.hasPending = w.pending, false
		} else {
			next, ok := w.sp.Pop()
			if !ok {
				break
			}
			it = next
		}
		size := it.Len()
		if len(w.batch) > 0 && total+size > w.cfg.MaxBatchBytes {
			w.pending, w.hasPending = it, true
			break
		}
		w.batch = append(w.batch, it)
		total += size
	}
	return w.batch
}

// writeBatch 写出一个批次。返回 false 表示 Socket 已不可用，Writer 应退出。
//
// 两条路径：
//  1. 单帧 → 直接写（共享帧走 WriteTo，保持 S3 零拷贝；独占字节走 Write）；
//  2. 多帧 → 合并进池化缓冲，一次 Write（省 N-1 次 Write 系统调用）。
//
// 注意 collect 已保证多帧批次的累计字节 ≤ MaxBatchBytes，因此合并缓冲不会
// 为大帧付拷贝代价：单帧永远独占一批。
func (w *Writer) writeBatch(batch []Outbound) bool {
	if len(batch) == 0 {
		return true
	}
	w.batches.Add(1)
	// 写 Deadline 由「每帧一次」改为「每批一次」，语义不变（对端不读仍会被超时打断）。
	_ = w.nc.SetWriteDeadline(time.Now().Add(w.cfg.WriteDeadline))
	w.deadlineCalls.Add(1)

	if len(batch) == 1 {
		if !w.writeOne(&batch[0]) {
			return false
		}
		w.sp.markWritten(batch[0].Class())
		w.framesWritten.Add(1)
		return true
	}

	total := 0
	for _, it := range batch {
		total += it.Len()
	}
	handle := w.cfg.BufferPool.Get(total)
	buf := handle.Slice()
	for _, it := range batch {
		if it.Shared != nil {
			buf = it.Shared.AppendTo(buf)
			continue
		}
		buf = append(buf, it.Raw...)
	}
	handle.Set(buf)
	_, err := w.nc.Write(buf)
	w.writeCalls.Add(1)
	w.coalescedBytes.Add(int64(len(buf)))
	w.cfg.BufferPool.Put(handle)
	if err != nil {
		releaseShared(batch)
		w.log.Printf("sendpath writer: write batch error: %v", err)
		return false
	}
	for _, it := range batch {
		w.sp.markWritten(it.Class())
		w.framesWritten.Add(1)
	}
	releaseShared(batch)
	return true
}

// writeOne 写出一帧；共享帧写完立即 Release（含失败路径），并清空该槽位，
// 避免 Writer 退出清理时二次 Release（双重释放会 panic）。
func (w *Writer) writeOne(it *Outbound) bool {
	w.writeCalls.Add(1)
	if it.Shared != nil {
		f := it.Shared
		it.Shared = nil
		_, err := f.WriteTo(w.nc)
		f.Release()
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

// releaseShared 释放批次中所有共享帧引用。
// 批量路径无论写出成功与否都必须释放，否则池中 buffer 永不归还。
func releaseShared(batch []Outbound) {
	for i := range batch {
		if batch[i].Shared != nil {
			batch[i].Shared.Release()
			batch[i].Shared = nil
		}
	}
}

// Stats 返回写出统计。
func (w *Writer) Stats() WriteStats {
	frames := w.framesWritten.Load()
	st := WriteStats{
		Batches:       w.batches.Load(),
		Frames:        frames,
		WriteCalls:    w.writeCalls.Load(),
		DeadlineCalls: w.deadlineCalls.Load(),
	}
	if frames > 0 {
		st.SyscallsPerFrame = float64(st.WriteCalls+st.DeadlineCalls) / float64(frames)
	}
	return st
}

// Stop 通知 Writer 退出并关闭 Socket（幂等，可由多个 goroutine 并发调用）。
func (w *Writer) Stop() {
	// 不能用 select 检查再 close：两个并发 Stop 可能同时看到 done 未关闭，
	// 第二个 close 会 panic。sync.Once 保证只关闭一次。
	w.stopOnce.Do(func() { close(w.done) })
}

// Closed 在 Writer 退出（Socket 已关闭）后关闭。
func (w *Writer) Closed() <-chan struct{} { return w.closed }
