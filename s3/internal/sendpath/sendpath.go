package sendpath

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// reliablePollInterval 是 P1 背压等待的轮询兜底间隔：即使 space 信号在多个
// 并发等待者间被合并，也能在有限时间内重试，避免永久阻塞。
const reliablePollInterval = 20 * time.Millisecond

// ErrQueueFull 表示 P1 背压等待超时：慢消费者，应断连。
var ErrQueueFull = errors.New("sendpath: reliable queue full, slow consumer")

// ErrClosed 表示 SendPath 已关闭。
var ErrClosed = errors.New("sendpath: closed")

// ErrBudgetExhausted 表示 P2 广播预算耗尽：丢弃当前帧，连接保持（S3 LLD §8.2）。
// 与 P1 的 ErrQueueFull 语义不同——广播永不因慢消费者断连。
var ErrBudgetExhausted = errors.New("sendpath: broadcast budget exhausted")

// Outbound 是 Pop 交给 Writer 的写出单元。
// 恰好一个字段非 nil：Raw 用于 P0/P1/P3 的独占字节，Shared 用于 P2 的共享只读帧。
type Outbound struct {
	Raw    []byte
	Shared *ImmutableFrame
}

// Len 返回写出字节数（预算计数用）。
func (o Outbound) Len() int {
	if o.Shared != nil {
		return o.Shared.Len()
	}
	return len(o.Raw)
}

// channelState 是一条通道的运行时状态：队列 + 预算计数 + 丢弃计数。
type channelState struct {
	ch      Channel
	queue   chan Outbound
	frames  atomic.Int64
	bytes   atomic.Int64
	dropped atomic.Int64
}

func newChannelState(c Channel) *channelState {
	size := c.MaxFrames
	if size <= 0 {
		size = 64 // 无上限时给一个有界的物理队列，避免无限增长
	}
	return &channelState{ch: c, queue: make(chan Outbound, size)}
}

// canEnqueue 检查预算是否允许入队（在持 sendMu 时调用，计数安全）。
func (s *channelState) canEnqueue(frameLen int) bool {
	if s.ch.MaxFrames > 0 && s.frames.Load() >= int64(s.ch.MaxFrames) {
		return false
	}
	if s.ch.MaxBytes > 0 && s.bytes.Load()+int64(frameLen) > s.ch.MaxBytes {
		return false
	}
	return true
}

func (s *channelState) onEnqueue(frameLen int) {
	s.frames.Add(1)
	s.bytes.Add(int64(frameLen))
}

func (s *channelState) onDequeue(frameLen int) {
	s.frames.Add(-1)
	s.bytes.Add(-int64(frameLen))
}

// SendPath 是一条连接唯一的出站发送路径。四个通道物理隔离、预算独立。
// 入队由任意 goroutine 调用（线程安全），出队由唯一 Writer 完成。
type SendPath struct {
	states    [4]*channelState
	mu        sync.Mutex    // 保护预算检查→入队的原子性
	wake      chan struct{} // 帧到达信号，仅供 Writer 等待
	space     chan struct{} // 队列腾出空间信号，仅供 P1 背压入队方等待
	done      chan struct{}
	closeOnce sync.Once

	// 出站序号：连接内单调，S0 冻结语义。
	seq   uint32
	seqMu sync.Mutex
}

// New 创建四通道发送路径。channels 必须按 ChControl..ChEvent 顺序给出。
func New(channels [4]Channel) *SendPath {
	sp := &SendPath{
		wake:  make(chan struct{}, 1),
		space: make(chan struct{}, 1),
		done:  make(chan struct{}),
	}
	for i := range sp.states {
		sp.states[i] = newChannelState(channels[i])
	}
	return sp
}

// Wake 返回 Writer 等待唤醒的 channel（仅供 writer.go 使用）。
func (s *SendPath) Wake() <-chan struct{} { return s.wake }

// Done 返回关闭信号。
func (s *SendPath) Done() <-chan struct{} { return s.done }

// Pop 按 P0→P1→P2→P3 优先级取出一帧；全部为空时返回 ok=false。
// 仅由唯一 Writer 调用。取帧时同步扣减对应通道预算，并唤醒 P1 背压入队方。
func (s *SendPath) Pop() (Outbound, bool) {
	for i := 0; i < 4; i++ {
		st := s.states[i]
		select {
		case it := <-st.queue:
			st.onDequeue(it.Len())
			select {
			case s.space <- struct{}{}:
			default:
			}
			return it, true
		default:
		}
	}
	return Outbound{}, false
}

// enqueueRaw 是 P0/P1/P3 独占字节的入队实现。返回 false 表示预算/关闭未入队。
func (s *SendPath) enqueueRaw(cls ChannelClass, frame []byte, block bool) error {
	st := s.states[cls]
	s.mu.Lock()
	if s.isClosed() {
		s.mu.Unlock()
		return ErrClosed
	}
	if !st.canEnqueue(len(frame)) {
		s.mu.Unlock()
		if cls == ChReliable && block {
			return ErrQueueFull // 背压交由调用方决定是否断连
		}
		st.dropped.Add(1)
		return nil // P3：丢弃 + 计数，不断连
	}
	st.queue <- Outbound{Raw: frame}
	st.onEnqueue(len(frame))
	s.mu.Unlock()
	s.signal()
	return nil
}

// TrySendBroadcast 把共享只读帧入队 P2：成功时 +1 引用（队列持有），
// 预算耗尽时返回 ErrBudgetExhausted 并计数、不持有引用（扇出方仍持有其那 1 个）。
// frame 为 nil 视为编程错误，panic 而非静默丢弃。
func (s *SendPath) TrySendBroadcast(frame *ImmutableFrame) error {
	if frame == nil {
		panic("broadcast: TrySendBroadcast nil frame")
	}
	st := s.states[ChBroadcast]
	size := frame.Len()
	s.mu.Lock()
	if s.isClosed() {
		s.mu.Unlock()
		return ErrClosed
	}
	if !st.canEnqueue(size) {
		st.dropped.Add(1)
		s.mu.Unlock()
		return ErrBudgetExhausted // 丢弃 + 计数，不断连；未 Retain，无需回退
	}
	frame.Retain()
	st.queue <- Outbound{Shared: frame}
	st.onEnqueue(size)
	s.mu.Unlock()
	s.signal()
	return nil
}

func (s *SendPath) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *SendPath) isClosed() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// EnqueueControl 入队 P0 帧（不丢弃）。
func (s *SendPath) EnqueueControl(frame []byte) error { return s.enqueueRaw(ChControl, frame, false) }

// EnqueueReliable 入队 P1 帧。满时按 LLD 为背压：等待 Writer 取帧腾出空间后重试，
// ctx 超时则返回 ErrQueueFull 表示慢消费者，调用方应断连。
func (s *SendPath) EnqueueReliable(ctx context.Context, frame []byte) error {
	poll := time.NewTicker(reliablePollInterval)
	defer poll.Stop()
	for {
		err := s.enqueueRaw(ChReliable, frame, true)
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrQueueFull) {
			return err
		}
		select {
		case <-s.space:
		case <-poll.C:
		case <-ctx.Done():
			return ErrQueueFull
		case <-s.done:
			return ErrClosed
		}
	}
}

// EnqueueBroadcast 入队 P2 独占字节帧（兼容 S1 调用方）。S3 广播走 TrySendBroadcast。
func (s *SendPath) EnqueueBroadcast(frame []byte) error {
	return s.enqueueRaw(ChBroadcast, frame, false)
}

// EnqueueEvent 入队 P3 帧，满则丢弃 + 计数。
func (s *SendPath) EnqueueEvent(frame []byte) error { return s.enqueueRaw(ChEvent, frame, false) }

// Dropped 返回指定通道的丢弃计数（供 /metrics）。
func (s *SendPath) Dropped(cls ChannelClass) int64 { return s.states[cls].dropped.Load() }

// Depth 返回指定通道当前帧数（供 /metrics）。
func (s *SendPath) Depth(cls ChannelClass) int64 { return s.states[cls].frames.Load() }

// Bytes 返回指定通道当前字节数（供 /metrics）。
func (s *SendPath) Bytes(cls ChannelClass) int64 { return s.states[cls].bytes.Load() }

// NextSequence 分配连接内单调递增序号（不回绕，S0 冻结语义）。
func (s *SendPath) NextSequence() (uint32, error) {
	s.seqMu.Lock()
	defer s.seqMu.Unlock()
	if s.seq >= ^uint32(0) {
		return 0, errors.New("sendpath: sequence exhausted")
	}
	s.seq++
	return s.seq, nil
}

// Close 关闭 SendPath，唤醒 Writer 与背压入队方，并排空残留共享帧的引用，
// 避免因连接提前关闭导致 pool 中的 buffer 永久泄漏。幂等。
//
// 排空必须与入队互斥：入队方在 mu 内先 Retain 再写入队列，若不持 mu 就排空，
// 可能在其 Retain 之后、入队之前完成排空，使该引用永久留在队列里（buffer 不归还）。
func (s *SendPath) Close() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		close(s.done)
		s.signal()
		for i := 0; i < len(s.states); i++ {
			for {
				select {
				case it := <-s.states[i].queue:
					if it.Shared != nil {
						it.Shared.Release()
					}
				default:
					goto nextChannel
				}
			}
		nextChannel:
		}
	})
}
