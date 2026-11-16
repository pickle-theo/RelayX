package server

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"relayx/s0/internal/protocol"
)

// conn 封装一条 TCP 连接。
//
// 并发模型（S0 冻结）：
//   - 单个读 goroutine：io.ReadFull 语义的累积缓冲处理粘包半包，缓冲按连接复用。
//   - 单个写 goroutine：唯一 Socket 写者，从 send 通道取已编码字节写出，杜绝并发写冲突。
//   - 心跳 goroutine：空闲超时发 PING，超时未收到 PONG 则关闭连接。
//   - 三者通过 ctx 统一取消；任一退出即关闭连接并通知其余 goroutine，避免泄漏。
type conn struct {
	nc   net.Conn
	cfg  Config
	log  *log.Logger
	addr string

	ctx    context.Context
	cancel context.CancelFunc

	send chan []byte // 已编码帧字节；写 goroutine 独占消费
	done chan struct{}
	once sync.Once
	wg   sync.WaitGroup

	// enqueueMu 串行化「分配序号 → 编码 → 入队」：readLoop 与 heartbeatLoop
	// 都会调用 enqueue，若不加锁则 tracker.Next() 存在数据竞争，且序号顺序
	// 可能与入队顺序不一致（对端会观测到假乱序）。
	enqueueMu sync.Mutex

	tracker   protocol.SequenceTracker // 出站序号，连接内从 1 单调分配
	lastActiv atomicInt64              // 最近一次读到合法帧的时间戳（UnixNano）
}

// atomicInt64 用互斥锁实现时间戳的原子读写。
type atomicInt64 struct {
	mu sync.Mutex
	v  int64
}

func (a *atomicInt64) Store(v int64) { a.mu.Lock(); a.v = v; a.mu.Unlock() }
func (a *atomicInt64) Load() int64   { a.mu.Lock(); v := a.v; a.mu.Unlock(); return v }

func newConn(nc net.Conn, cfg Config, logger *log.Logger) *conn {
	ctx, cancel := context.WithCancel(context.Background())
	c := &conn{
		nc:     nc,
		cfg:    cfg,
		log:    logger,
		addr:   nc.RemoteAddr().String(),
		ctx:    ctx,
		cancel: cancel,
		send:   make(chan []byte, 128),
		done:   make(chan struct{}),
	}
	c.lastActiv.Store(time.Now().UnixNano())
	return c
}

// serve 启动读/写/心跳 goroutine 并阻塞直到连接结束。
func (c *conn) serve() {
	c.wg.Add(3)
	go c.readLoop()
	go c.writeLoop()
	go c.heartbeatLoop()
	c.wg.Wait()
}

// close 幂等关闭：通知所有 goroutine 退出并打断阻塞的读。
// Socket 的实际关闭交给 writeLoop，在排空出站队列后执行，保证已入队的
// ERROR/PONG 等帧能在断连前送达对端（修复解码错误时 ERROR 被关连吞掉的问题）。
func (c *conn) close() {
	c.once.Do(func() {
		c.cancel()
		close(c.done)
		_ = c.nc.SetReadDeadline(time.Now())
	})
}

// readLoop 用按连接复用的缓冲处理粘包半包。
func (c *conn) readLoop() {
	defer c.wg.Done()
	defer c.close() // 读循环退出即断开连接，通知其余 goroutine

	buf := make([]byte, protocol.MaxFrameSize)
	pending := buf[:0]

	for {
		// 缓冲已满仍无法解出完整帧：声明长度与实际送达不符，断开。
		if len(pending) >= cap(buf) {
			c.log.Printf("conn %s: read buffer full without a complete frame", c.addr)
			return
		}

		n, err := c.nc.Read(buf[len(pending):cap(buf)])
		if n > 0 {
			pending = pending[:len(pending)+n]
			consumed := c.consume(pending)
			// 把未消费字节 pending[consumed:] 搬到缓冲前部，保持缓冲复用。
			remaining := copy(buf, pending[consumed:])
			pending = buf[:remaining]
			c.lastActiv.Store(time.Now().UnixNano())
		}
		if err != nil {
			if !errors.Is(err, io.EOF) && c.ctx.Err() == nil && !isTimeout(err) {
				c.log.Printf("conn %s read error: %v", c.addr, err)
			}
			return
		}
	}
}

// consume 解析 pending 中的全部完整帧，返回未消费字节数。
func (c *conn) consume(pending []byte) int {
	pos := 0
	for pos < len(pending) {
		f, used, err := protocol.Decode(pending[pos:])
		if errors.Is(err, protocol.ErrIncomplete) {
			break
		}
		if err != nil {
			c.log.Printf("conn %s decode error: %v", c.addr, err)
			c.replyError(protocol.ErrorBadFrame)
			c.close()
			return pos
		}
		pos += used

		switch f.Type {
		case protocol.TypePing:
			c.enqueue(protocol.TypePong, nil)
		case protocol.TypePong:
			// 活跃时间由 readLoop 统一刷新
		case protocol.TypeEcho:
			if c.cfg.EnableEcho {
				// Body 来自共享读缓冲；enqueue 会编码拷贝，但此处仍显式复制以隔离生命周期。
				body := append([]byte(nil), f.Body...)
				c.enqueue(protocol.TypeEcho, body)
			} else {
				c.replyError(protocol.ErrorUnknownFrame)
			}
		default:
			c.replyError(protocol.ErrorUnknownFrame)
		}
	}
	return pos
}

// enqueue 分配序号、编码帧并送入写队列。队列满时按超时背压（不无限阻塞）。
// 整个「分配序号 → 编码 → 入队」在 enqueueMu 下串行，保证序号单调且与入队顺序一致。
func (c *conn) enqueue(frameType byte, body []byte) {
	c.enqueueMu.Lock()
	defer c.enqueueMu.Unlock()

	seq, err := c.tracker.Next()
	if err != nil {
		c.log.Printf("conn %s: %v, closing", c.addr, err)
		c.close()
		return
	}
	frame, err := protocol.Encode(protocol.Frame{Type: frameType, Sequence: seq, Body: body}, nil)
	if err != nil {
		c.log.Printf("conn %s encode error: %v", c.addr, err)
		return
	}
	// 快路径：队列有空位时直接入队，避免每次调用都分配一个 timer。
	select {
	case c.send <- frame:
		return
	default:
	}
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case c.send <- frame:
	case <-c.done:
	case <-timer.C:
		c.log.Printf("conn %s send queue full, backpressure", c.addr)
	}
}

func (c *conn) replyError(code uint16) {
	body := []byte{byte(code >> 8), byte(code)}
	c.enqueue(protocol.TypeError, body)
}

// writeLoop 是唯一写 Socket 的 goroutine，也是唯一关闭 Socket 的地方。
// 收到关闭信号后先把 send 队列中已入队的帧写完再关闭，保证解码错误时
// 入队的 ERROR 帧先送达对端，然后才断开连接。
func (c *conn) writeLoop() {
	defer c.wg.Done()
	defer func() { _ = c.nc.Close() }()
	// 写循环退出（含写错误）时同样通知其余 goroutine，避免它们等到心跳超时才退出。
	// defer 为 LIFO：先 c.close() 打断阻塞的读，再关闭 Socket。
	defer c.close()

	for {
		select {
		case frame := <-c.send:
			if !c.writeFrame(frame) {
				return
			}
		case <-c.done:
			// 排空已入队的帧后再退出；队列为空则直接返回并关闭 Socket。
			for {
				select {
				case frame := <-c.send:
					if !c.writeFrame(frame) {
						return
					}
				default:
					return
				}
			}
		}
	}
}

// writeFrame 写一帧到 Socket，返回 false 表示写失败应终止写循环。
func (c *conn) writeFrame(frame []byte) bool {
	_ = c.nc.SetWriteDeadline(time.Now().Add(c.cfg.WriteDeadline))
	if _, err := c.nc.Write(frame); err != nil {
		if c.ctx.Err() == nil && !isTimeout(err) {
			c.log.Printf("conn %s write error: %v", c.addr, err)
		}
		return false
	}
	return true
}

// heartbeatLoop 在空闲超过 heartbeat_interval 后发 PING；pingSentAt 之后
// lastActiv 未被刷新且超过 pong_timeout 则关闭连接。
func (c *conn) heartbeatLoop() {
	defer c.wg.Done()
	defer c.close()

	ticker := time.NewTicker(c.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			idle := time.Since(time.Unix(0, c.lastActiv.Load()))
			if idle < c.cfg.HeartbeatInterval {
				continue
			}
			pingSentAt := c.lastActiv.Load()
			c.enqueue(protocol.TypePing, nil)
			if !c.waitPong(pingSentAt) {
				return
			}
		}
	}
}

// waitPong 等待对端活动（PONG 或任意帧刷新了 lastActiv）。返回 false 表示连接已关闭。
func (c *conn) waitPong(pingSentAt int64) bool {
	deadline := time.NewTimer(c.cfg.PongTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return false
		case <-deadline.C:
			c.log.Printf("conn %s heartbeat timeout", c.addr)
			c.close()
			return false
		case <-ticker.C:
			if c.lastActiv.Load() > pingSentAt {
				return true
			}
		}
	}
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}
