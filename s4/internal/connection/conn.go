// Package connection 实现单条连接：读循环（粘包半包 + Sequence 观测）与
// SendPath/Writer 的生命周期编排（S1 LLD §8 / §10）。
package connection

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"relayx/s4/internal/config"
	"relayx/s4/internal/sendpath"
	"relayx/s4/internal/telemetry"
	"relayx/s4/protocol"
)

// Handler 由 gateway 层实现，处理入站帧与连接关闭。connection 包不依赖业务包，
// 避免循环依赖。
type Handler interface {
	// OnOpen 在连接注册后、Serve 启动前调用（gateway 可登记 SendPath 供指标聚合）。
	OnOpen(c *Connection)
	// HandleFrame 处理一帧入站帧。seqGap/outOfOrder 为本帧的 Sequence 观测结果
	// （S0 §3.1：只观测、改变指标，不改变业务处理）。
	HandleFrame(c *Connection, f protocol.Frame, seqGap, outOfOrder bool)
	// OnClose 在连接关闭、goroutine 回收后调用（gateway 负责注销 Session）。
	OnClose(c *Connection)
}

// ConnState 是连接状态。
type ConnState int

const (
	StateOpen ConnState = iota
	StateClosing
	StateClosed
)

// Connection 是一条 TCP 连接及其唯一的 SendPath/Writer。
type Connection struct {
	ID      string
	NetConn net.Conn
	Send    *sendpath.SendPath
	Version uint64 // per-endpoint 单调（S1 本地分配；S6 用 Redis INCR）

	cfg     config.Config
	handler Handler
	metrics *telemetry.Metrics
	log     *log.Logger

	ctx    context.Context
	cancel context.CancelFunc

	stateMu sync.RWMutex
	state   ConnState

	writer *sendpath.Writer
	once   sync.Once
	wg     sync.WaitGroup

	// 入站 Sequence 观测：记录已收到的最大序号。
	seqMu     sync.Mutex
	lastInSeq uint32
	seenInSeq bool
	lastInAt  atomic.Int64 // 最近一次读到任意字节的时间（UnixNano），心跳用

	// closeReason 是 S4 引入的 CLOSE 原因（LLD §3.4）。默认 NORMAL；
	// 由 gateway 在断连前显式设置（REPLACED/KICKED/...），
	// 心跳超时由连接自身设置（HEARTBEAT_TIMEOUT）。
	closeReason atomic.Uint32
}

// Options 是 newConnection 的依赖注入参数。
type Options struct {
	ID      string
	NC      net.Conn
	Cfg     config.Config
	Handler Handler
	Metrics *telemetry.Metrics
	Log     *log.Logger
	Version uint64
}

// New 创建连接（尚未启动）。
func New(o Options) *Connection {
	channels := sendpath.DefaultChannels(
		o.Cfg.ControlMaxFrames, o.Cfg.ReliableMaxFrames, o.Cfg.ReliableMaxBytes,
		o.Cfg.BroadcastMaxFrames, o.Cfg.BroadcastMaxBytes,
		o.Cfg.EventMaxFrames, o.Cfg.EventMaxBytes,
	)
	sp := sendpath.New(channels)
	ctx, cancel := context.WithCancel(context.Background())
	logger := o.Log
	if logger == nil {
		logger = log.Default()
	}
	c := &Connection{
		ID: o.ID, NetConn: o.NC, Send: sp, Version: o.Version,
		cfg: o.Cfg, handler: o.Handler, metrics: o.Metrics, log: logger,
		ctx: ctx, cancel: cancel, state: StateOpen,
	}
	c.lastInAt.Store(time.Now().UnixNano())
	c.closeReason.Store(uint32(protocol.CloseNormal))
	c.writer = sendpath.NewWriter(sp, o.NC, sendpath.WriteConfig{
		WriteDeadline: o.Cfg.WriteDeadline,
		OnClose:       c.onSocketClosed,
	}, logger)
	return c
}

// Serve 启动读循环与 Writer，阻塞直到连接结束。
func (c *Connection) Serve() {
	c.metrics.ConnInc()
	c.wg.Add(3)
	go c.readLoop()
	go func() { defer c.wg.Done(); c.writer.Run() }()
	go c.heartbeatLoop()
	c.wg.Wait()
	// 不能复用 c.once：Close() 已占用它，复用会导致 state 永远停在 StateClosing。
	// wg.Wait() 后所有内部 goroutine 已退出，此处直接加锁置终态即可。
	c.stateMu.Lock()
	c.state = StateClosed
	c.stateMu.Unlock()
	c.handler.OnClose(c)
	c.metrics.ConnDec()
}

// heartbeatLoop 在空闲超过 heartbeat_interval 后通过 P0 发 PING；
// pong_timeout 内无任意入站活动则关闭连接。P0 绝对优先，心跳不被数据淹没。
func (c *Connection) heartbeatLoop() {
	defer c.wg.Done()
	defer c.Close()

	ticker := time.NewTicker(c.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			pingSentAt := c.lastInAt.Load()
			if time.Since(time.Unix(0, pingSentAt)) < c.cfg.HeartbeatInterval {
				continue
			}
			c.enqueueControl(protocol.Frame{Type: protocol.TypePing})
			deadline := time.NewTimer(c.cfg.PongTimeout)
			poll := time.NewTicker(20 * time.Millisecond)
		wait:
			for {
				select {
				case <-c.ctx.Done():
					poll.Stop()
					deadline.Stop()
					return
				case <-deadline.C:
					poll.Stop()
					c.log.Printf("conn %s: heartbeat timeout", c.ID)
					// S4：先补 CLOSE 原因码再断连，端侧才能区分心跳超时与网络断开。
					c.SetCloseReason(protocol.CloseHeartbeatTimeout)
					c.ReplyClose()
					c.flushWindow(20 * time.Millisecond)
					return
				case <-poll.C:
					if c.lastInAt.Load() > pingSentAt {
						poll.Stop()
						deadline.Stop()
						break wait
					}
				}
			}
		}
	}
}

// State 返回连接状态。
func (c *Connection) State() ConnState {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.state
}

// Close 幂等关闭：通知 Writer 退出、打断读、关闭 SendPath。
func (c *Connection) Close() {
	c.once.Do(func() {
		c.stateMu.Lock()
		if c.state == StateOpen {
			c.state = StateClosing
		}
		c.stateMu.Unlock()
		c.cancel()
		c.Send.Close()
		c.writer.Stop()
		_ = c.NetConn.SetReadDeadline(time.Now())
	})
}

// Context 返回连接的生命周期 context（鉴权/背压使用）。
func (c *Connection) Context() context.Context { return c.ctx }

// onSocketClosed 在 Writer 关闭 Socket 后调用，确保读循环被打破。
func (c *Connection) onSocketClosed() {
	c.cancel()
	_ = c.NetConn.SetReadDeadline(time.Now())
	c.Send.Close()
}

// EnqueueControl/Reliable/Broadcast/Event 是发送路径的四入口，供 gateway 调用。
func (c *Connection) EnqueueControl(f protocol.Frame) error {
	b, err := c.encode(f)
	if err != nil {
		return err
	}
	return c.Send.EnqueueControl(b)
}

func (c *Connection) EnqueueReliable(ctx context.Context, f protocol.Frame) error {
	b, err := c.encode(f)
	if err != nil {
		return err
	}
	return c.Send.EnqueueReliable(ctx, b)
}

func (c *Connection) EnqueueBroadcast(f protocol.Frame) error {
	b, err := c.encode(f)
	if err != nil {
		return err
	}
	return c.Send.EnqueueBroadcast(b)
}

// TrySendBroadcast 把共享只读帧交给本连接的 P2 通道（S3）。
// 连接非 Open 时直接拒绝，避免向已关闭连接入队。
func (c *Connection) TrySendBroadcast(frame *sendpath.ImmutableFrame) error {
	if c.State() != StateOpen {
		return sendpath.ErrClosed
	}
	return c.Send.TrySendBroadcast(frame)
}

// BroadcastPending 返回 P2 通道当前待写出的帧数。
// 慢消费者降级后据此判断积压是否已排空，从而放行恢复探测（S3 LLD §8.2）。
func (c *Connection) BroadcastPending() int64 {
	return c.Send.Depth(sendpath.ChBroadcast)
}

func (c *Connection) EnqueueEvent(f protocol.Frame) error {
	b, err := c.encode(f)
	if err != nil {
		return err
	}
	return c.Send.EnqueueEvent(b)
}

// TryEnqueueEvent 把 P3 事件帧入队，并在预算耗尽时返回 ErrBudgetExhausted，
// 供 S4 事件分发区分"已入队"与"已丢弃"（丢弃不断连，只计数）。
func (c *Connection) TryEnqueueEvent(f protocol.Frame) error {
	if c.State() != StateOpen {
		return sendpath.ErrClosed
	}
	b, err := c.encode(f)
	if err != nil {
		return err
	}
	return c.Send.TryEnqueueEvent(b)
}

// encode 分配连接内出站序号并编码帧。
func (c *Connection) encode(f protocol.Frame) ([]byte, error) {
	seq, err := c.Send.NextSequence()
	if err != nil {
		return nil, err
	}
	f.Sequence = seq
	return protocol.Encode(f, nil)
}

// ReplyError 通过 P0 通道发送 ERROR 帧（继承 S0 错误码）。
func (c *Connection) ReplyError(code uint16) {
	body := []byte{byte(code >> 8), byte(code)}
	c.enqueueControl(protocol.Frame{Type: protocol.TypeError, Body: body})
}

// SendControl 入队 P0 帧并记录失败（gateway 层的握手确认也走这里）。
func (c *Connection) SendControl(f protocol.Frame) { c.enqueueControl(f) }

// enqueueControl 入队 P0 帧并记录失败。P0 不允许静默消失：
// 预算耗尽时 SendPath 返回错误，这里必须留下痕迹而不是假装已发送。
func (c *Connection) enqueueControl(f protocol.Frame) {
	if err := c.EnqueueControl(f); err != nil && !errors.Is(err, sendpath.ErrClosed) {
		c.log.Printf("conn %s: control frame 0x%02x not enqueued: %v", c.ID, f.Type, err)
	}
}

// SetCloseReason 记录本次断连的 CLOSE 原因（可由 gateway 在 Close 前设置）。
func (c *Connection) SetCloseReason(r protocol.CloseReason) {
	c.closeReason.Store(uint32(r))
}

// CloseReason 返回本次断连的原因码；连接建立后默认 NORMAL。
func (c *Connection) CloseReason() protocol.CloseReason {
	return protocol.CloseReason(c.closeReason.Load())
}

// ReplyClose 通过 P0 发送 CLOSE 帧，body 携带当前原因码（S4 LLD §3.2）。
// 旧客户端忽略 body，行为与 S1 一致。
func (c *Connection) ReplyClose() {
	code := c.CloseReason()
	c.enqueueControl(protocol.Frame{
		Type: protocol.TypeClose,
		Body: protocol.EncodeCloseBody(code, code.String()),
	})
}

// CloseWithReason 是 S4 引入的断连入口：先置原因、发 CLOSE（P0 尽力而为），
// 再关闭连接。用于 REPLACED / KICKED / TOKEN_EXPIRED / SERVER_DRAINING 等
// 需要端侧区分的场景。
func (c *Connection) CloseWithReason(r protocol.CloseReason, text string) {
	c.SetCloseReason(r)
	c.enqueueControl(protocol.Frame{
		Type: protocol.TypeClose,
		Body: protocol.EncodeCloseBody(r, text),
	})
	// P0 帧由唯一 Writer 异步写出；给写线程一个极短窗口把 CLOSE 送出，
	// 否则 Close 会立刻撤掉 SendPath，端侧只能看到 EOF。
	c.flushWindow(50 * time.Millisecond)
	c.Close()
}

// flushWindow 等待「调用时已入队的 P0 帧」全部写出，或超时（不影响幂等关闭）。
// 判据是 Writer 的写成功计数而非队列深度：Depth 归零只说明帧被取走，
// 不代表已落到 Socket，用深度判断会让 Close 抢在 CLOSE 帧之前撤掉连接。
func (c *Connection) flushWindow(d time.Duration) {
	target := c.Send.Enqueued(sendpath.ChControl)
	if c.Send.Written(sendpath.ChControl) >= target {
		return
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		select {
		case <-c.Send.WriteSignal():
		case <-c.Send.Done():
			return
		case <-timer.C:
			return
		}
		if c.Send.Written(sendpath.ChControl) >= target {
			return
		}
	}
}

// replyPong 回 PONG（P0）。
func (c *Connection) replyPong() {
	c.enqueueControl(protocol.Frame{Type: protocol.TypePong})
}

// readLoop 复用单连接读缓冲，处理粘包半包，做 Sequence 观测并分发帧。
func (c *Connection) readLoop() {
	defer c.wg.Done()
	defer c.Close()

	buf := make([]byte, protocol.MaxFrameSize)
	pending := buf[:0]
	for {
		if len(pending) >= cap(buf) {
			c.log.Printf("conn %s: read buffer full without complete frame", c.ID)
			return
		}
		n, err := c.NetConn.Read(buf[len(pending):cap(buf)])
		if n > 0 {
			c.lastInAt.Store(time.Now().UnixNano())
			pending = pending[:len(pending)+n]
			pos := 0
			for pos < len(pending) {
				f, used, derr := protocol.Decode(pending[pos:])
				if errors.Is(derr, protocol.ErrIncomplete) {
					break
				}
				if derr != nil {
					// S2 语义：帧解码失败直接断连，不回 ERROR——ERROR 码表
					// 已归 S2 的策略/幂等/未知消息使用，不再承载传输层错误。
					c.log.Printf("conn %s: decode error: %v", c.ID, derr)
					return
				}
				pos += used
				gap, ooo := c.observe(f.Sequence)
				// Body 引用读缓冲；handler 若需跨循环持有须自行拷贝（S0 零拷贝约定）。
				c.handler.HandleFrame(c, f, gap, ooo)
				if c.State() != StateOpen {
					break
				}
			}
			remaining := copy(buf, pending[pos:])
			pending = buf[:remaining]
		}
		if err != nil {
			if !errors.Is(err, io.EOF) && c.ctx.Err() == nil && !isTimeout(err) {
				c.log.Printf("conn %s: read error: %v", c.ID, err)
			}
			return
		}
	}
}

// observe 执行一次入站 Sequence 观测。返回 (gap, outOfOrder)。
// 规则：seq 严格递增；seq 增大超过 1 → gap；seq 不大于上次 → outOfOrder。
// 仅更新指标，不改变业务处理（S0 §3.1）。
func (c *Connection) observe(seq uint32) (bool, bool) {
	c.seqMu.Lock()
	defer c.seqMu.Unlock()
	if !c.seenInSeq {
		c.seenInSeq = true
		c.lastInSeq = seq
		return false, false
	}
	var gap, ooo bool
	if seq > c.lastInSeq {
		gap = seq > c.lastInSeq+1
		c.lastInSeq = seq
	} else {
		ooo = true
	}
	if gap {
		c.metrics.FrameGapInc()
	}
	if ooo {
		c.metrics.OutOfOrderInc()
	}
	return gap, ooo
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}
