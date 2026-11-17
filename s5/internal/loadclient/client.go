package loadclient

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"relayx/s5/internal/heartbeat"
	"relayx/s5/internal/pool"
	"relayx/s5/internal/readloop"
	"relayx/s5/internal/sendpath"
	"relayx/s5/protocol"
)

// ErrClosed 表示客户端已关闭。
var ErrClosed = errors.New("loadclient: closed")

// ErrTimeout 表示等待 ACK 超时（慢消费者或服务端过载的可观测信号）。
var ErrTimeout = errors.New("loadclient: ack timeout")

// readPool 是压测进程内所有连接共享的读缓冲池（S5 §4.3）。
// 10 万连接若不池化，每连接 64KiB 读缓冲就是 6.4GiB 常驻内存。
var readPool = pool.New(pool.Config{})

// Config 是压测客户端配置。
type Config struct {
	Addr          string
	Token         string // 登录令牌（IssueLoginToken 签发）
	DeviceID      string
	Tenant        string
	User          string
	WriteDeadline time.Duration
	ReadBuffer    int
	// AckTimeout 是等待 SEND_ACK / SUBSCRIBE_ACK 的上限。
	AckTimeout time.Duration
	// InboundQueue 是入站投递/广播队列深度；满则丢弃并计数（压测端不拖慢服务端）。
	InboundQueue int
	// Wheel 是可选的共享时间轮（多连接共用一个 tick goroutine）。
	Wheel *heartbeat.Wheel
	// HeartbeatInterval/PongTimeout 为 0 时使用缺省（30s/10s）。
	HeartbeatInterval time.Duration
	PongTimeout       time.Duration
}

// Stats 是客户端侧统计。
type Stats struct {
	Frames         int64
	Delivers       int64
	Broadcasts     int64
	DeliverDropped int64
	BroadcastDrops int64
	Acks           int64
	Errors         int64
	CloseReasons   int64
}

// Client 是一条压测连接（L1 层：状态机 + 定型 API，不暴露裸帧）。
type Client struct {
	cfg Config

	nc     net.Conn
	sp     *sendpath.SendPath
	writer *sendpath.Writer
	reader *readloop.Reader
	probe  *heartbeat.Probe

	loginCh chan protocol.LoginAckBody
	subAck  chan protocol.SubscribeAckBody
	deliver chan protocol.DeliverBody
	bc      chan protocol.BroadcastBody
	pong    chan struct{}
	closed  chan struct{}

	mu      sync.Mutex
	pending map[string]chan protocol.SendAckBody

	done     chan struct{}
	closeOne sync.Once

	frames         atomic.Int64
	delivers       atomic.Int64
	broadcasts     atomic.Int64
	deliverDropped atomic.Int64
	broadcastDrops atomic.Int64
	acks           atomic.Int64
	errCount       atomic.Int64
	closeReasons   atomic.Int64
	pongSeen       atomic.Int64
}

// Dial 建立连接并启动读/写/心跳。
func Dial(cfg Config) (*Client, error) {
	if cfg.Addr == "" {
		return nil, errors.New("loadclient: empty addr")
	}
	if cfg.WriteDeadline <= 0 {
		cfg.WriteDeadline = 5 * time.Second
	}
	if cfg.AckTimeout <= 0 {
		cfg.AckTimeout = 5 * time.Second
	}
	if cfg.InboundQueue <= 0 {
		cfg.InboundQueue = 1024
	}
	nc, err := net.DialTimeout("tcp", cfg.Addr, 3*time.Second)
	if err != nil {
		return nil, err
	}
	// 大 TCP 缓冲：压测端不应因本地 socket 缓冲过小而成为瓶颈。
	if tc, ok := nc.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
		_ = tc.SetReadBuffer(256 << 10)
		_ = tc.SetWriteBuffer(256 << 10)
	}
	sp := sendpath.New(sendpath.DefaultChannels(64, 512, 4<<20, 256, 2<<20, 256, 2<<20))
	c := &Client{
		cfg: cfg, nc: nc, sp: sp,
		loginCh: make(chan protocol.LoginAckBody, 1),
		subAck:  make(chan protocol.SubscribeAckBody, 4),
		deliver: make(chan protocol.DeliverBody, cfg.InboundQueue),
		bc:      make(chan protocol.BroadcastBody, cfg.InboundQueue),
		pong:    make(chan struct{}, 1),
		closed:  make(chan struct{}, 1),
		pending: make(map[string]chan protocol.SendAckBody),
		done:    make(chan struct{}),
	}
	c.writer = sendpath.NewWriter(sp, nc, sendpath.WriteConfig{
		WriteDeadline: cfg.WriteDeadline,
		OnClose:       c.onSocketClosed,
	}, nil)
	c.reader = readloop.New(nc, readloop.Config{BufferSize: cfg.ReadBuffer, Pool: readPool})

	go c.writer.Run()
	go c.readLoop()
	if cfg.Wheel != nil {
		c.probe = heartbeat.NewProbe(cfg.Wheel, heartbeat.ProbeConfig{
			Interval:     cfg.HeartbeatInterval,
			PongTimeout:  cfg.PongTimeout,
			LastActivity: func() time.Time { return time.Unix(0, c.reader.LastActivity()) },
			SendPing:     func() { _ = c.Ping() },
			OnTimeout:    func() { c.Close() },
		})
		c.probe.Start()
	}
	return c, nil
}

// Login 发送 LOGIN 并等待 LOGIN_ACK，返回服务端宣告的能力位。
func (c *Client) Login() (uint32, error) {
	body, err := protocol.EncodeLogin(c.cfg.Token, c.cfg.DeviceID)
	if err != nil {
		return 0, err
	}
	raw, err := c.encode(protocol.TypeLogin, body)
	if err != nil {
		return 0, err
	}
	if err := c.sp.EnqueueControl(raw); err != nil {
		return 0, err
	}
	timer := time.NewTimer(c.cfg.AckTimeout)
	defer timer.Stop()
	select {
	case ack := <-c.loginCh:
		return ack.Capabilities, nil
	case <-c.done:
		return 0, ErrClosed
	case <-timer.C:
		return 0, ErrTimeout
	}
}

// Subscribe 订阅一个 topic（需先用 IssueSubscribeToken 签名）。
func (c *Client) Subscribe(topic, token string) (protocol.SubscribeResult, error) {
	body, err := protocol.EncodeSubscribe(topic, token)
	if err != nil {
		return 0, err
	}
	raw, err := c.encode(protocol.TypeSubscribe, body)
	if err != nil {
		return 0, err
	}
	// 入队与等待 ACK 共用同一个 deadline：AckTimeout 是整条调用的上限，
	// 慢消费者造成的 P1 背压也不能突破它（否则 context.Background 会无限阻塞）。
	deadline := time.Now().Add(c.cfg.AckTimeout)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	if err := c.sp.EnqueueReliable(ctx, raw); err != nil {
		return 0, err
	}
	select {
	case ack := <-c.subAck:
		return ack.Result, nil
	case <-c.done:
		return 0, ErrClosed
	case <-ctx.Done():
		return 0, ErrTimeout
	}
}

// SendResult 是 SEND 的结果（耗时用于客户端侧延迟分解）。
type SendResult struct {
	MessageID  string
	State      string
	Duplicate  bool
	AcceptedAt int64 // 服务端受理时刻（Unix 毫秒，协议字段原值）
	// AckAt 是客户端收到 SEND_ACK 的本地时刻（UnixNano）。
	// 客户端侧延迟分解用它把「受理+入站网络」与「投递+出站网络」切开，
	// 精度是纳秒级（服务端 accepted_at 只有毫秒，无法支撑亚毫秒分解）。
	AckAt    int64
	EncodeNs int64 // 客户端编码 + 入队耗时
	WaitNs   int64 // 等待 SEND_ACK 的耗时（含服务端受理与网络）
}

// Send 发送一条点对点消息并等待 SEND_ACK。
func (c *Client) Send(targetUser string, targetDevices []string, payload []byte) (SendResult, error) {
	clientMsgID := newClientMsgID()
	sb, err := protocol.EncodeSend(protocol.SendBody{
		ClientMsgID:   clientMsgID,
		TargetUserID:  targetUser,
		TargetDevices: targetDevices,
		Payload:       payload,
	})
	if err != nil {
		return SendResult{}, err
	}
	raw, err := c.encode(protocol.TypeSend, sb)
	if err != nil {
		return SendResult{}, err
	}
	ch := make(chan protocol.SendAckBody, 1)
	c.mu.Lock()
	c.pending[clientMsgID] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, clientMsgID)
		c.mu.Unlock()
	}()

	start := time.Now()
	// 入队与等待 ACK 共用同一个 deadline：AckTimeout 是整条调用的上限，
	// 慢消费者造成的 P1 背压也不能突破它。
	deadline := start.Add(c.cfg.AckTimeout)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	if err := c.sp.EnqueueReliable(ctx, raw); err != nil {
		return SendResult{}, err
	}
	encodeNs := time.Since(start).Nanoseconds()

	select {
	case ack := <-ch:
		c.acks.Add(1)
		ackAt := time.Now().UnixNano()
		return SendResult{
			MessageID: ack.MessageID, State: ack.PlatformState, Duplicate: ack.Duplicate,
			AcceptedAt: ack.AcceptedAt, AckAt: ackAt,
			EncodeNs: encodeNs, WaitNs: ackAt - start.UnixNano(),
		}, nil
	case <-c.done:
		return SendResult{}, ErrClosed
	case <-ctx.Done():
		return SendResult{}, ErrTimeout
	}
}

// ReceiptAck 上报投递回执（P1）。
func (c *Client) ReceiptAck(messageID string) error {
	raw, err := c.encode(protocol.TypeReceiptAck, protocol.EncodeReceiptAck(messageID))
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.AckTimeout)
	defer cancel()
	return c.sp.EnqueueReliable(ctx, raw)
}

// Ping 通过 P0 发心跳。
func (c *Client) Ping() error { return c.sendControl(protocol.TypePing, nil) }

// Pong 回应服务端 PING（P0）。
func (c *Client) Pong() error { return c.sendControl(protocol.TypePong, nil) }

// PingWait 发 PING 并等待 PONG，用于心跳延迟采样。
//
// 用「已见 PONG 计数」而不是裸 channel 判据：PONG 会因超时/重排而滞留，
// 只等 channel 会把上一个 PING 的响应算成本次的 0 延迟（曾实测 P50=0µs）。
// 单连接内 PONG 有序到达，因此计数比较是精确的。
func (c *Client) PingWait(timeout time.Duration) (time.Duration, error) {
	before := c.pongSeen.Load()
	start := time.Now()
	if err := c.Ping(); err != nil {
		return 0, err
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		if c.pongSeen.Load() > before {
			return time.Since(start), nil
		}
		select {
		case <-c.pong:
		case <-c.done:
			return 0, ErrClosed
		case <-timer.C:
			return 0, ErrTimeout
		}
	}
}

// Deliver 返回入站投递队列（只读，容量由 Config.InboundQueue 决定）。
func (c *Client) Deliver() <-chan protocol.DeliverBody { return c.deliver }

// Broadcast 返回入站广播队列。
func (c *Client) Broadcast() <-chan protocol.BroadcastBody { return c.bc }

// Closed 在连接关闭后关闭。
func (c *Client) Closed() <-chan struct{} { return c.done }

// Stats 返回客户端统计快照。
func (c *Client) Stats() Stats {
	return Stats{
		Frames: c.frames.Load(), Delivers: c.delivers.Load(), Broadcasts: c.broadcasts.Load(),
		DeliverDropped: c.deliverDropped.Load(), BroadcastDrops: c.broadcastDrops.Load(),
		Acks: c.acks.Load(), Errors: c.errCount.Load(), CloseReasons: c.closeReasons.Load(),
	}
}

// WriterStats 暴露写路径统计（压测报告需要客户端侧 syscalls/frame）。
func (c *Client) WriterStats() sendpath.WriteStats { return c.writer.Stats() }

// Close 关闭客户端（幂等）。
func (c *Client) Close() {
	c.closeOne.Do(func() {
		close(c.done)
		if c.probe != nil {
			c.probe.Stop()
		}
		c.sp.Close()
		c.writer.Stop()
		_ = c.nc.Close()
		// 读缓冲由 readLoop 在 Reader.Run 返回后归还：此处提前 Put 会让池
		// 立即把 buffer 复用于其他连接，而读循环可能仍在使用同一块内存。
	})
}

func (c *Client) onSocketClosed() { c.Close() }

// encode 分配连接内单调序号并编码一帧（S0：客户端 Sequence 从 1 单调递增）。
func (c *Client) encode(typ byte, body []byte) ([]byte, error) {
	seq, err := c.sp.NextSequence()
	if err != nil {
		return nil, err
	}
	return protocol.AppendFrame(nil, typ, seq, body)
}

func (c *Client) sendControl(typ byte, body []byte) error {
	raw, err := c.encode(typ, body)
	if err != nil {
		return err
	}
	return c.sp.EnqueueControl(raw)
}

func (c *Client) readLoop() {
	defer c.Close()
	// defer 是 LIFO：先归还读缓冲，再执行 Close（Close 不再触碰 reader）。
	// Reader.Close 的契约是「必须在 Run 返回后调用」，只有这里能保证。
	defer c.reader.Close()
	err := c.reader.Run(c)
	if err != nil && !errors.Is(err, ErrClosed) {
		c.errCount.Add(1)
	}
}

// HandleFrame 实现 readloop.Handler：解码后分发到对应队列。
// 所有队列都非阻塞写入，压测端绝不因为自己消费不及而拖慢服务端。
func (c *Client) HandleFrame(f protocol.Frame) {
	c.frames.Add(1)
	switch f.Type {
	case protocol.TypeLoginAck:
		ack, err := protocol.DecodeLoginAck(f.Body)
		if err != nil {
			c.errCount.Add(1)
			return
		}
		select {
		case c.loginCh <- ack:
		default:
		}
	case protocol.TypeSendAck:
		ack, err := protocol.DecodeSendAck(f.Body)
		if err != nil {
			c.errCount.Add(1)
			return
		}
		c.mu.Lock()
		ch := c.pending[ack.ClientMsgID]
		c.mu.Unlock()
		if ch != nil {
			select {
			case ch <- ack:
			default:
			}
		}
	case protocol.TypeDeliver:
		d, err := protocol.DecodeDeliver(f.Body)
		if err != nil {
			c.errCount.Add(1)
			return
		}
		c.delivers.Add(1)
		select {
		case c.deliver <- d:
		default:
			c.deliverDropped.Add(1)
		}
	case protocol.TypeBroadcast:
		b, err := protocol.DecodeBroadcast(f.Body)
		if err != nil {
			c.errCount.Add(1)
			return
		}
		// DecodeBroadcast 与 DecodeDeliver 不同：Payload 直接引用读缓冲
		// （rest[16:]），只有 Topic 被拷贝。入队后读循环会继续复用该缓冲，
		// 必须在这里复制，否则消费者读到的是下一批字节（数据竞争 + 脏延迟）。
		b.Payload = append([]byte(nil), b.Payload...)
		c.broadcasts.Add(1)
		select {
		case c.bc <- b:
		default:
			c.broadcastDrops.Add(1)
		}
	case protocol.TypeSubscribeAck:
		ack, err := protocol.DecodeSubscribeAck(f.Body)
		if err != nil {
			c.errCount.Add(1)
			return
		}
		select {
		case c.subAck <- ack:
		default:
		}
	case protocol.TypePong:
		c.pongSeen.Add(1)
		select {
		case c.pong <- struct{}{}:
		default:
		}
	case protocol.TypePing:
		_ = c.Pong()
	case protocol.TypeClose:
		c.closeReasons.Add(1)
		select {
		case c.closed <- struct{}{}:
		default:
		}
	case protocol.TypeError:
		c.errCount.Add(1)
	}
}

var msgSeq atomic.Uint64

// newClientMsgID 生成压测用的 client_msg_id（进程内唯一即可；
// 幂等键是 (tenant, producer, client_msg_id)，同一 producer 不会重复）。
func newClientMsgID() string {
	return "lg-" + itoa(msgSeq.Add(1))
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
