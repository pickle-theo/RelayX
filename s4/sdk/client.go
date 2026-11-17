package sdk

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"relayx/s4/protocol"
)

var (
	// ErrNotReady 表示当前状态不可收发。
	ErrNotReady = errors.New("sdk: connection not ready")
	// ErrClosed 表示客户端已关闭。
	ErrClosed = errors.New("sdk: client closed")
	// ErrAuthFailed 表示令牌被服务端拒绝。
	ErrAuthFailed = errors.New("sdk: authentication failed")
	// ErrRequestTimeout 表示等待 ACK 超时。
	ErrRequestTimeout = errors.New("sdk: request timeout")
	// ErrServerError 表示服务端返回 ERROR 帧。
	ErrServerError = errors.New("sdk: server error")
)

// SDKClient 是 S4 冻结的 SDK 公共接口（LLD §2.3）。
type SDKClient interface {
	Connect(ctx context.Context) error
	Close() error
	Send(ctx context.Context, to string, payload []byte, opts ...SendOption) (string, error)
	Subscribe(ctx context.Context, topic, token string) error
	QueryMessage(ctx context.Context, id string) (*ClientMessageView, error)
	State() ConnState
}

// Config 是 SDK 配置。
type Config struct {
	Addr     string
	Token    string
	DeviceID string
	// TenantID / ProducerID 仅用于 hook 展示与日志，不参与鉴权
	// （身份一律由服务端从令牌派生）。
	TenantID   string
	ProducerID string

	// QueryBaseURL 是端侧查询 API 的 base URL（例如 http://127.0.0.1:9802）。
	// 为空时 PendingTracker 保持关闭（LLD §11 硬规则）。
	QueryBaseURL string

	// EventMask 是客户端想收的事件集合；默认只订阅 MESSAGE_SETTLED。
	EventMask protocol.EventMask

	HookQueue        int
	HandshakeTimeout time.Duration
	RequestTimeout   time.Duration
	DialTimeout      time.Duration
	QueryTimeout     time.Duration

	// MaxReconnectAttempts <= 0 表示无限重连。
	MaxReconnectAttempts int
	Backoff              Backoff
	PendingLimit         int

	Hooks  Hooks
	Logger func(format string, args ...any)
}

func (c *Config) applyDefaults() {
	if c.EventMask == 0 {
		c.EventMask = protocol.EventMaskDefault
	}
	if c.HookQueue <= 0 {
		c.HookQueue = 1024
	}
	if c.HandshakeTimeout <= 0 {
		c.HandshakeTimeout = 5 * time.Second
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 5 * time.Second
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = 3 * time.Second
	}
	if c.QueryTimeout <= 0 {
		c.QueryTimeout = 3 * time.Second
	}
	if c.PendingLimit <= 0 {
		c.PendingLimit = 10000
	}
	if c.Backoff.Initial <= 0 {
		c.Backoff.Initial = 500 * time.Millisecond
	}
	if c.Backoff.Max <= 0 {
		c.Backoff.Max = 30 * time.Second
	}
}

type closeInfo struct {
	code   uint16
	reason string
	text   string
	known  bool
}

type sessionEnd struct {
	err   error
	close *closeInfo
}

// connSession 是一条 TCP 连接及其帧读写状态。重连会创建全新 session，
// 旧 session 的任何异步结果都不会污染新会话。
type connSession struct {
	nc      net.Conn
	writeMu sync.Mutex
	seq     protocol.SequenceTracker

	mu       sync.Mutex
	sendAcks map[string]chan protocol.SendAckBody
	loginAck chan loginResult
	eventAck chan protocol.SubscribeEventsAckBody
	subAck   chan protocol.SubscribeAckBody
	unsubAck chan protocol.UnsubscribeAckBody

	endOnce sync.Once
	end     sessionEnd
	ended   chan struct{}
}

type loginResult struct {
	body  protocol.LoginAckBody
	err   error
	close *closeInfo
}

func newConnSession(nc net.Conn) *connSession {
	return &connSession{
		nc:       nc,
		sendAcks: make(map[string]chan protocol.SendAckBody),
		loginAck: make(chan loginResult, 1),
		eventAck: make(chan protocol.SubscribeEventsAckBody, 1),
		subAck:   make(chan protocol.SubscribeAckBody, 1),
		unsubAck: make(chan protocol.UnsubscribeAckBody, 1),
		ended:    make(chan struct{}),
	}
}

// write 编码并写出一帧。序号分配必须在 writeMu 之内：Send / Pong 回应 /
// RECEIPT_ACK 来自不同 goroutine，锁外分配会同时造成 SequenceTracker 数据竞争
// 与线上序号逆序（服务端会把它记为 frame_gap / out_of_order）。
func (s *connSession) write(f protocol.Frame) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	seq, err := s.seq.Next()
	if err != nil {
		return err
	}
	f.Sequence = seq
	b, err := protocol.Encode(f, nil)
	if err != nil {
		return err
	}
	_, err = s.nc.Write(b)
	return err
}

func (s *connSession) finish(err error, ci *closeInfo) {
	s.endOnce.Do(func() {
		s.end = sessionEnd{err: err, close: ci}
		close(s.ended)
	})
}

func (s *connSession) End() sessionEnd {
	select {
	case <-s.ended:
		return s.end
	default:
		return sessionEnd{}
	}
}

func (s *connSession) waitSendAck(clientMsgID string) chan protocol.SendAckBody {
	ch := make(chan protocol.SendAckBody, 1)
	s.mu.Lock()
	s.sendAcks[clientMsgID] = ch
	s.mu.Unlock()
	return ch
}

func (s *connSession) dropSendAck(clientMsgID string) {
	s.mu.Lock()
	delete(s.sendAcks, clientMsgID)
	s.mu.Unlock()
}

func (s *connSession) deliverSendAck(ack protocol.SendAckBody) {
	s.mu.Lock()
	ch := s.sendAcks[ack.ClientMsgID]
	delete(s.sendAcks, ack.ClientMsgID)
	s.mu.Unlock()
	if ch != nil {
		select {
		case ch <- ack:
		default:
		}
	}
}

// Client 是 SDK 实现。
type Client struct {
	cfg        Config
	hooks      Hooks
	dispatcher *HookDispatcher
	sm         *StateMachine
	deduper    *EventDeduper
	pending    *PendingTracker
	query      *QueryClient
	backoff    Backoff

	ctx    context.Context
	cancel context.CancelFunc

	mu             sync.Mutex
	sess           *connSession
	caps           uint32
	granted        protocol.EventMask
	pendingEnabled bool

	controlMu  sync.Mutex
	backfillMu sync.Mutex

	terminalMu    sync.Mutex
	terminals     map[string]struct{}
	terminalOrder []string
	terminalCap   int

	closedByUser atomic.Bool
	closedOnce   atomic.Bool
	gapQueries   atomic.Int64
	reconnects   atomic.Int64
	backfills    atomic.Int64
	stopOnce     sync.Once
}

// New 创建 SDK 客户端（尚不连接）。
func New(cfg Config) *Client {
	cfg.applyDefaults()
	ctx, cancel := context.WithCancel(context.Background())
	logf := cfg.Logger
	if logf == nil {
		logf = func(string, ...any) {}
	}
	hooks := cfg.Hooks
	c := &Client{
		cfg:         cfg,
		hooks:       hooks,
		sm:          NewStateMachine(),
		deduper:     NewEventDeduper(0),
		pending:     NewPendingTracker(cfg.PendingLimit),
		backoff:     cfg.Backoff,
		ctx:         ctx,
		cancel:      cancel,
		terminals:   make(map[string]struct{}),
		terminalCap: terminalWindow(cfg.PendingLimit),
	}
	c.dispatcher = NewHookDispatcher(cfg.HookQueue,
		func(d DropInfo) {
			if hooks.EventDropped != nil {
				hooks.EventDropped(d)
			}
		},
		func(hook string, v any) {
			logf("sdk: hook %s panicked: %v", hook, v)
		})
	if cfg.QueryBaseURL != "" {
		c.query = NewQueryClient(cfg.QueryBaseURL, cfg.Token, cfg.QueryTimeout)
	}
	return c
}

var _ SDKClient = (*Client)(nil)

// State 返回连接状态。
func (c *Client) State() ConnState { return c.sm.State() }

// Caps 返回最近一次 LOGIN_ACK 宣告的能力位。
func (c *Client) Caps() uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.caps
}

// GrantedEventMask 返回服务端实际授予的事件位。
func (c *Client) GrantedEventMask() protocol.EventMask {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.granted
}

// PendingEnabled 表示「能力位 + 查询 API」双条件是否满足（LLD §11）。
func (c *Client) PendingEnabled() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pendingEnabled
}

// PendingLen 返回当前 pending 数。
func (c *Client) PendingLen() int { return c.pending.Len() }

// GapQueries 返回因 seq gap 触发的查询次数（验证用例断言）。
func (c *Client) GapQueries() int64 { return c.gapQueries.Load() }

// Reconnects 返回重连尝试次数。
func (c *Client) Reconnects() int64 { return c.reconnects.Load() }

// Backfills 返回查询补齐次数。
func (c *Client) Backfills() int64 { return c.backfills.Load() }

// Connect 建立连接、完成鉴权与能力协商。成功后进入 READY。
func (c *Client) Connect(ctx context.Context) error {
	if c.cfg.Addr == "" || c.cfg.Token == "" || c.cfg.DeviceID == "" {
		return fmt.Errorf("sdk: Addr/Token/DeviceID are required")
	}
	if st := c.sm.State(); st != StateIdle {
		return fmt.Errorf("sdk: Connect called in state %s", st)
	}
	c.dispatcher.Start()
	if err := c.sm.Transition(StateConnecting); err != nil {
		return err
	}
	c.fire("connecting", false, func() {
		if c.hooks.Connecting != nil {
			c.hooks.Connecting(ConnectingInfo{Addr: c.cfg.Addr, Attempt: 0})
		}
	})

	sess, err := c.establish(ctx)
	if err != nil {
		if errors.Is(err, ErrAuthFailed) {
			c.notifyClosed(uint16(protocol.CloseAuthFailed), protocol.CloseAuthFailed.String(), err.Error(), false)
		} else {
			c.notifyClosed(uint16(protocol.CloseNormal), protocol.CloseNormal.String(), err.Error(), false)
		}
		c.dispatcher.Stop(2 * time.Second)
		return err
	}
	c.install(sess)
	go c.supervise(sess)
	return nil
}

// install 把新会话设为当前会话。
func (c *Client) install(sess *connSession) {
	c.mu.Lock()
	c.sess = sess
	c.mu.Unlock()
}

func (c *Client) currentSession() *connSession {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sess
}

// establish 拨号 + LOGIN + SUBSCRIBE_EVENTS + 查询 API 探测。
// 它启动该会话的读循环，返回时状态已进入 READY。
func (c *Client) establish(ctx context.Context) (*connSession, error) {
	// 新会话的 seq 从 1 重新开始。必须在读循环启动前重置，
	// 否则第一个事件可能被旧 lastSeq 误判为 gap（LLD §8.3）。
	c.deduper.ResetSeq()
	dialer := net.Dialer{Timeout: c.cfg.DialTimeout}
	nc, err := dialer.DialContext(ctx, "tcp", c.cfg.Addr)
	if err != nil {
		return nil, err
	}
	sess := newConnSession(nc)
	go c.readLoop(sess)

	if err := c.sm.Transition(StateConnected); err != nil {
		nc.Close()
		return nil, err
	}
	c.fire("connected", false, func() {
		if c.hooks.Connected != nil {
			c.hooks.Connected(ConnectedInfo{Remote: nc.RemoteAddr().String()})
		}
	})
	if err := c.sm.Transition(StateAuthenticating); err != nil {
		nc.Close()
		return nil, err
	}

	body, _ := protocol.EncodeLogin(c.cfg.Token, c.cfg.DeviceID)
	if err := sess.write(protocol.Frame{Type: protocol.TypeLogin, Body: body}); err != nil {
		nc.Close()
		return nil, err
	}

	timer := time.NewTimer(c.cfg.HandshakeTimeout)
	defer timer.Stop()
	select {
	case res := <-sess.loginAck:
		if res.err != nil {
			nc.Close()
			return nil, res.err
		}
		c.mu.Lock()
		c.caps = res.body.Capabilities
		c.mu.Unlock()
	case <-sess.ended:
		end := sess.End()
		nc.Close()
		if end.close != nil && end.close.code == uint16(protocol.CloseAuthFailed) {
			return nil, ErrAuthFailed
		}
		if end.err != nil {
			return nil, end.err
		}
		return nil, ErrAuthFailed
	case <-timer.C:
		nc.Close()
		return nil, fmt.Errorf("%w: login", ErrRequestTimeout)
	case <-ctx.Done():
		nc.Close()
		return nil, ctx.Err()
	}

	c.mu.Lock()
	caps := c.caps
	c.mu.Unlock()

	c.fire("authenticated", false, func() {
		if c.hooks.Authenticated != nil {
			c.hooks.Authenticated(AuthenticatedInfo{
				TenantID: c.cfg.TenantID, ProducerID: c.cfg.ProducerID,
				DeviceID: c.cfg.DeviceID, Capabilities: caps,
			})
		}
	})

	// 旧服务端不置 CAP_CLIENT_EVENTS：SDK 降级为无事件模式，绝不发订阅帧。
	if caps&protocol.CapClientEvents != 0 {
		if err := c.subscribeEvents(sess, c.cfg.EventMask); err != nil {
			c.logf("sdk: subscribe events failed: %v", err)
		}
	}

	c.mu.Lock()
	if c.query != nil && caps&protocol.CapClientEvents != 0 {
		c.mu.Unlock()
		probeCtx, cancel := context.WithTimeout(ctx, c.cfg.QueryTimeout)
		err := c.query.Probe(probeCtx)
		cancel()
		c.mu.Lock()
		c.pendingEnabled = err == nil
		if err != nil {
			c.logf("sdk: query API probe failed, PendingTracker disabled: %v", err)
		}
	} else {
		c.pendingEnabled = false
	}
	c.mu.Unlock()

	if err := c.sm.Transition(StateReady); err != nil {
		nc.Close()
		return nil, err
	}
	c.fire("ready", false, func() {
		if c.hooks.Ready != nil {
			c.hooks.Ready()
		}
	})
	return sess, nil
}

func (c *Client) subscribeEvents(sess *connSession, mask protocol.EventMask) error {
	if err := sess.write(protocol.Frame{
		Type: protocol.TypeSubscribeEvents,
		Body: protocol.EncodeSubscribeEvents(mask),
	}); err != nil {
		return err
	}
	timer := time.NewTimer(c.cfg.RequestTimeout)
	defer timer.Stop()
	select {
	case ack := <-sess.eventAck:
		c.mu.Lock()
		c.granted = ack.GrantedMask
		c.mu.Unlock()
		if ack.Result != protocol.SubscribeEventsOK {
			return fmt.Errorf("sdk: subscribe events result=%d", ack.Result)
		}
		return nil
	case <-sess.ended:
		return fmt.Errorf("sdk: connection closed during subscribe_events")
	case <-timer.C:
		return fmt.Errorf("%w: subscribe_events", ErrRequestTimeout)
	}
}

// readLoop 是单连接唯一读循环。
// 它持有该会话 Socket 的关闭责任：读循环因错误退出即会话结束，
// 若只在 establish/Close 路径关闭，重连链上每次意外断链都会留下一枚未关闭的 fd。
func (c *Client) readLoop(sess *connSession) {
	defer sess.nc.Close()
	buf := make([]byte, protocol.MaxFrameSize)
	for {
		f, err := readFrame(sess.nc, buf)
		if err != nil {
			sess.finish(err, nil)
			return
		}
		switch f.Type {
		case protocol.TypeLoginAck:
			ack, derr := protocol.DecodeLoginAck(f.Body)
			if derr != nil {
				sess.loginAck <- loginResult{err: derr}
				continue
			}
			sess.loginAck <- loginResult{body: ack}
		case protocol.TypeSendAck:
			ack, derr := protocol.DecodeSendAck(f.Body)
			if derr != nil {
				c.logf("sdk: bad send_ack: %v", derr)
				continue
			}
			sess.deliverSendAck(ack)
		case protocol.TypeDeliver:
			d, derr := protocol.DecodeDeliver(f.Body)
			if derr != nil {
				c.logf("sdk: bad deliver: %v", derr)
				continue
			}
			info := DeliveredInfo{
				MessageID: d.MessageID, ProducerID: d.ProducerID, Sender: d.Sender,
				Payload: d.Payload, ExpiresAt: time.UnixMilli(d.ExpiresAt),
			}
			c.fire("delivered", true, func() {
				if c.hooks.Delivered != nil {
					c.hooks.Delivered(info)
				}
			})
		case protocol.TypeSubscribeAck:
			ack, derr := protocol.DecodeSubscribeAck(f.Body)
			if derr != nil {
				c.logf("sdk: bad subscribe_ack: %v", derr)
				continue
			}
			select {
			case sess.subAck <- ack:
			default:
			}
		case protocol.TypeUnsubscribeAck:
			ack, derr := protocol.DecodeUnsubscribeAck(f.Body)
			if derr != nil {
				continue
			}
			select {
			case sess.unsubAck <- ack:
			default:
			}
		case protocol.TypeSubscribeEventsAck:
			ack, derr := protocol.DecodeSubscribeEventsAck(f.Body)
			if derr != nil {
				c.logf("sdk: bad subscribe_events_ack: %v", derr)
				continue
			}
			select {
			case sess.eventAck <- ack:
			default:
			}
		case protocol.TypeClientEvent:
			ev, derr := protocol.DecodeClientEvent(f.Body)
			if derr != nil {
				c.logf("sdk: bad client_event: %v", derr)
				continue
			}
			c.handleClientEvent(ev)
		case protocol.TypeBroadcast:
			b, derr := protocol.DecodeBroadcast(f.Body)
			if derr != nil {
				continue
			}
			info := BroadcastInfo{
				Topic: b.Topic, Payload: b.Payload, Seq: b.Seq,
				ServerReceivedAt: time.UnixMilli(b.ServerReceivedAt),
			}
			c.fire("broadcast.received", false, func() {
				if c.hooks.BroadcastReceived != nil {
					c.hooks.BroadcastReceived(info)
				}
			})
		case protocol.TypePing:
			_ = sess.write(protocol.Frame{Type: protocol.TypePong})
		case protocol.TypePong:
			// 任意入站帧都已刷新服务端心跳窗口；SDK 无需额外处理。
		case protocol.TypeClose:
			cb, _ := protocol.DecodeCloseBody(f.Body)
			reason, known := protocol.ParseCloseReason(cb.ReasonCode)
			if !known {
				// E7：未知原因码按 NORMAL 处理并记录。
				c.logf("sdk: unknown close reason %d, treated as NORMAL", cb.ReasonCode)
				reason = protocol.CloseNormal
			}
			sess.finish(nil, &closeInfo{
				code: uint16(reason), reason: reason.String(), text: cb.ReasonText, known: known,
			})
			_ = sess.nc.Close()
			return
		case protocol.TypeError:
			code := uint16(0)
			if len(f.Body) >= 2 {
				code = binary.BigEndian.Uint16(f.Body[:2])
			}
			c.logf("sdk: server error frame code=%d", code)
		default:
			// 未注册帧按 S0 语义忽略（不改变连接状态）。
		}
	}
}

// readFrame 读取一整帧。Body 为零拷贝切片，仅在本次循环内有效。
func readFrame(nc net.Conn, buf []byte) (protocol.Frame, error) {
	header := buf[:protocol.HeaderSize]
	if _, err := io.ReadFull(nc, header); err != nil {
		return protocol.Frame{}, err
	}
	length := binary.BigEndian.Uint32(header[4:8])
	if length > protocol.MaxBodySize {
		return protocol.Frame{}, fmt.Errorf("sdk: frame too large: %d", length)
	}
	total := protocol.HeaderSize + int(length)
	if total > len(buf) {
		return protocol.Frame{}, fmt.Errorf("sdk: frame buffer too small")
	}
	if _, err := io.ReadFull(nc, buf[protocol.HeaderSize:total]); err != nil {
		return protocol.Frame{}, err
	}
	f, _, err := protocol.Decode(buf[:total])
	return f, err
}

func (c *Client) handleClientEvent(body protocol.ClientEventBody) {
	ev := Event{
		EventID: body.EventID, Seq: body.Seq, Type: body.Type,
		TenantID: body.TenantID, MessageID: body.MessageID,
		StateVersion: int(body.StateVersion), Counts: body.Counts,
		OccurredAt: time.UnixMilli(body.OccurredAt),
	}
	accept, gap := c.deduper.OnEvent(ev)
	c.logf("sdk: client_event type=%s seq=%d message=%s version=%d accept=%v gap=%v",
		ev.Type, ev.Seq, ev.MessageID, ev.StateVersion, accept, gap)
	if !accept {
		return
	}
	if gap {
		c.gapQueries.Add(1)
		go c.backfill("seq-gap")
	}
	switch ev.Type {
	case protocol.EvMessageSettled, protocol.EvMessageExpired:
		c.fireTerminal(ev, false)
	case protocol.EvDeliveryReceiptAcked, protocol.EvDeliveryFailed, protocol.EvDeliveryExpired:
		info := DeliveryEventInfo{
			EventID: ev.EventID, MessageID: ev.MessageID,
			StateVersion: ev.StateVersion, OccurredAt: ev.OccurredAt,
		}
		name := "delivery." + string(ev.Type)
		c.fire(name, false, func() {
			switch ev.Type {
			case protocol.EvDeliveryReceiptAcked:
				if c.hooks.DeliveryReceiptAcked != nil {
					c.hooks.DeliveryReceiptAcked(info)
				}
			case protocol.EvDeliveryFailed:
				if c.hooks.DeliveryFailed != nil {
					c.hooks.DeliveryFailed(info)
				}
			case protocol.EvDeliveryExpired:
				if c.hooks.DeliveryExpired != nil {
					c.hooks.DeliveryExpired(info)
				}
			}
		})
	default:
		c.logf("sdk: unknown client event type %q ignored", ev.Type)
	}
}

// fireTerminal 保证每条消息最多一次终态 hook（事件与查询补齐共用）。
func (c *Client) fireTerminal(ev Event, fromQuery bool) {
	if ev.MessageID == "" {
		return
	}
	if !c.markTerminal(ev.MessageID) {
		return
	}
	c.pending.Remove(ev.MessageID)
	counts := DeliveryCounts{}
	if ev.Counts != nil {
		counts = *ev.Counts
	}
	info := MessageSettledInfo{
		MessageID: ev.MessageID, EventID: ev.EventID,
		StateVersion: ev.StateVersion, Counts: counts,
		OccurredAt: ev.OccurredAt, FromQuery: fromQuery,
	}
	if ev.Type == protocol.EvMessageExpired {
		c.fire("message.expired", true, func() {
			if c.hooks.MessageExpired != nil {
				c.hooks.MessageExpired(info)
			}
		})
		return
	}
	c.fire("message.settled", true, func() {
		if c.hooks.MessageSettled != nil {
			c.hooks.MessageSettled(info)
		}
	})
}

// terminalWindow 返回终态去重窗口大小：至少覆盖去重器窗口，且随 pending
// 上限放大。窗口外若再收到同一消息的迟到事件，可能重复触发终态 hook——
// 这与 pending FIFO 淘汰的取舍一致，避免长跑进程无界增长。
func terminalWindow(pendingLimit int) int {
	n := pendingLimit * 4
	if n < 8192 {
		n = 8192
	}
	return n
}

// markTerminal 标记消息终态；首次返回 true，重复返回 false。
func (c *Client) markTerminal(messageID string) bool {
	c.terminalMu.Lock()
	defer c.terminalMu.Unlock()
	if _, ok := c.terminals[messageID]; ok {
		return false
	}
	c.terminals[messageID] = struct{}{}
	c.terminalOrder = append(c.terminalOrder, messageID)
	if len(c.terminalOrder) > c.terminalCap {
		oldest := c.terminalOrder[0]
		c.terminalOrder = c.terminalOrder[1:]
		delete(c.terminals, oldest)
	}
	return true
}

// backfill 用查询 API 补齐 pending 的终态（重连/seq gap 触发）。
func (c *Client) backfill(reason string) {
	c.mu.Lock()
	enabled := c.pendingEnabled
	query := c.query
	c.mu.Unlock()
	if !enabled || query == nil {
		return
	}
	if !c.backfillMu.TryLock() {
		return // 已有补齐在执行，避免查询风暴
	}
	defer c.backfillMu.Unlock()

	ids := c.pending.IDs()
	if len(ids) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(c.ctx, c.cfg.QueryTimeout)
	defer cancel()
	views, err := query.GetBatch(ctx, ids)
	if err != nil {
		c.logf("sdk: backfill (%s) failed: %v", reason, err)
		return
	}
	c.backfills.Add(1)
	for _, v := range views {
		var typ protocol.ClientEventType
		switch v.PlatformState {
		case "SETTLED":
			typ = protocol.EvMessageSettled
		case "EXPIRED":
			typ = protocol.EvMessageExpired
		default:
			continue
		}
		counts := v.Counts
		c.fireTerminal(Event{
			EventID: "query:" + v.MessageID, MessageID: v.MessageID, Type: typ,
			StateVersion: v.StateVersion, Counts: &counts, OccurredAt: time.Now().UTC(),
		}, true)
	}
}

// Send 发送一条不透明消息，返回服务端分配的 message_id。
func (c *Client) Send(ctx context.Context, to string, payload []byte, opts ...SendOption) (string, error) {
	if c.sm.State() != StateReady {
		return "", fmt.Errorf("%w: state=%s", ErrNotReady, c.sm.State())
	}
	sess := c.currentSession()
	if sess == nil {
		return "", ErrNotReady
	}
	var o sendOptions
	for _, opt := range opts {
		opt(&o)
	}
	if o.clientMsgID == "" {
		o.clientMsgID = fmt.Sprintf("c-%d-%d", time.Now().UnixNano(), c.seqHint())
	}
	ackCh := sess.waitSendAck(o.clientMsgID)
	defer sess.dropSendAck(o.clientMsgID)

	body, err := protocol.EncodeSend(protocol.SendBody{
		ClientMsgID: o.clientMsgID, TargetUserID: to,
		TargetDevices: o.devices, Payload: payload, Policy: o.policy,
	})
	if err != nil {
		return "", err
	}
	if err := sess.write(protocol.Frame{Type: protocol.TypeSend, Body: body}); err != nil {
		return "", err
	}

	timer := time.NewTimer(c.cfg.RequestTimeout)
	defer timer.Stop()
	select {
	case ack := <-ackCh:
		c.fire("message.accepted", false, func() {
			if c.hooks.MessageAccepted != nil {
				c.hooks.MessageAccepted(MessageAcceptedInfo{
					ClientMsgID: ack.ClientMsgID, MessageID: ack.MessageID,
					PlatformState: ack.PlatformState, Duplicate: ack.Duplicate,
				})
			}
		})
		if ack.PlatformState == "SETTLED" {
			// 目标无端点：S2 已同事务结算，直接触发终态 hook 且不加入 pending。
			counts := DeliveryCounts{}
			c.fireTerminal(Event{
				EventID: "send-ack:" + ack.MessageID, MessageID: ack.MessageID,
				Type: protocol.EvMessageSettled, OccurredAt: time.Now().UTC(), Counts: &counts,
			}, false)
		} else if c.PendingEnabled() {
			if evicted := c.pending.Add(ack.MessageID, ack.ClientMsgID); evicted != "" {
				c.logf("sdk: pending evicted %s", evicted)
			}
		}
		return ack.MessageID, nil
	case <-sess.ended:
		return "", fmt.Errorf("%w: connection closed while awaiting send_ack", ErrNotReady)
	case <-timer.C:
		return "", fmt.Errorf("%w: send_ack", ErrRequestTimeout)
	case <-ctx.Done():
		return "", ctx.Err()
	case <-c.ctx.Done():
		return "", ErrClosed
	}
}

var clientSeq atomic.Uint64

func (c *Client) seqHint() uint64 { return clientSeq.Add(1) }

// SendOption 调整单次发送。
type SendOption func(*sendOptions)

type sendOptions struct {
	clientMsgID string
	devices     []string
	policy      *protocol.SendPolicy
}

// WithClientMsgID 指定幂等键（重试安全）。
func WithClientMsgID(id string) SendOption {
	return func(o *sendOptions) { o.clientMsgID = id }
}

// WithDevices 指定目标设备；为空表示该用户全部端点。
func WithDevices(devices ...string) SendOption {
	return func(o *sendOptions) { o.devices = append([]string(nil), devices...) }
}

// WithPolicy 指定消息级策略（S2 §5.2 平台硬上限内）。
func WithPolicy(p protocol.SendPolicy) SendOption {
	return func(o *sendOptions) { o.policy = &p }
}

// Subscribe 订阅广播 topic（S3 能力；token 由业务签发，平台只验签）。
func (c *Client) Subscribe(ctx context.Context, topic, token string) error {
	if c.sm.State() != StateReady {
		return fmt.Errorf("%w: state=%s", ErrNotReady, c.sm.State())
	}
	sess := c.currentSession()
	if sess == nil {
		return ErrNotReady
	}
	c.controlMu.Lock()
	defer c.controlMu.Unlock()

	body, err := protocol.EncodeSubscribe(topic, token)
	if err != nil {
		return err
	}
	if err := sess.write(protocol.Frame{Type: protocol.TypeSubscribe, Body: body}); err != nil {
		return err
	}
	timer := time.NewTimer(c.cfg.RequestTimeout)
	defer timer.Stop()
	select {
	case ack := <-sess.subAck:
		if ack.Result != protocol.SubscribeOK {
			return fmt.Errorf("sdk: subscribe result=%d reason=%s", ack.Result, ack.Reason)
		}
		return nil
	case <-sess.ended:
		return fmt.Errorf("%w: connection closed during subscribe", ErrNotReady)
	case <-timer.C:
		return fmt.Errorf("%w: subscribe_ack", ErrRequestTimeout)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SendReceipt 发送 RECEIPT_ACK 并触发 receipt.ack_sent hook（接收方本地 hook）。
func (c *Client) SendReceipt(messageID string) error {
	sess := c.currentSession()
	if sess == nil || c.sm.State() != StateReady {
		return ErrNotReady
	}
	if err := sess.write(protocol.Frame{
		Type: protocol.TypeReceiptAck, Body: protocol.EncodeReceiptAck(messageID),
	}); err != nil {
		return err
	}
	c.fire("receipt.ack_sent", false, func() {
		if c.hooks.ReceiptAckSent != nil {
			c.hooks.ReceiptAckSent(ReceiptAckInfo{MessageID: messageID})
		}
	})
	return nil
}

// QueryMessage 查询自己发送消息的权威传输状态（不含 payload）。
func (c *Client) QueryMessage(ctx context.Context, id string) (*ClientMessageView, error) {
	if c.query == nil {
		return nil, ErrQueryUnavailable
	}
	v, err := c.query.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return &v, nil
}

// Close 关闭连接：进入终态 CLOSED，触发 closed(NORMAL)。
func (c *Client) Close() error {
	c.closedByUser.Store(true)
	c.stopOnce.Do(func() { c.cancel() })
	if sess := c.currentSession(); sess != nil {
		_ = sess.write(protocol.Frame{Type: protocol.TypeClose})
		_ = sess.nc.Close()
	}
	c.notifyClosed(uint16(protocol.CloseNormal), protocol.CloseNormal.String(), "client close", false)
	c.dispatcher.Stop(2 * time.Second)
	return nil
}

// notifyClosed 保证 closed 只触发一次，且按原因码先触发细分 hook。
func (c *Client) notifyClosed(code uint16, reason, text string, recoverable bool) {
	if c.closedOnce.Swap(true) {
		return
	}
	c.sm.ForceTransition(StateClosed)
	ci := CloseInfo{ReasonCode: code, Reason: reason, Text: text, Recoverable: recoverable}
	reasonEnum := protocol.CloseReason(code)
	switch reasonEnum {
	case protocol.CloseReplaced:
		c.fire("session.replaced", true, func() {
			if c.hooks.SessionReplaced != nil {
				c.hooks.SessionReplaced(ci)
			}
		})
	case protocol.CloseKicked:
		c.fire("session.kicked", true, func() {
			if c.hooks.SessionKicked != nil {
				c.hooks.SessionKicked(ci)
			}
		})
	case protocol.CloseTokenExpired:
		c.fire("session.token_expired", true, func() {
			if c.hooks.SessionTokenExpired != nil {
				c.hooks.SessionTokenExpired(ci)
			}
		})
	case protocol.CloseAuthFailed:
		c.fire("auth_failed", true, func() {
			if c.hooks.AuthFailed != nil {
				c.hooks.AuthFailed(AuthFailedInfo{Err: ErrAuthFailed})
			}
		})
	case protocol.CloseServerDraining:
		c.fire("server.draining", true, func() {
			if c.hooks.ServerDraining != nil {
				c.hooks.ServerDraining(ci)
			}
		})
	case protocol.CloseHeartbeatTimeout:
		c.fire("heartbeat_timeout", true, func() {
			if c.hooks.HeartbeatTimeout != nil {
				c.hooks.HeartbeatTimeout()
			}
		})
	}
	c.fire("closed", true, func() {
		if c.hooks.Closed != nil {
			c.hooks.Closed(ci)
		}
	})
	c.dispatcher.Stop(2 * time.Second)
}

// supervise 在会话结束后决定「重连」还是「终态关闭」。
func (c *Client) supervise(sess *connSession) {
	<-sess.ended
	if c.closedByUser.Load() {
		return
	}
	end := sess.End()
	reasonText := "network error"
	if end.err != nil {
		reasonText = end.err.Error()
	}
	_ = c.sm.Transition(StateDisconnected)

	if end.close != nil {
		code := end.close.code
		switch protocol.CloseReason(code) {
		case protocol.CloseServerDraining, protocol.CloseHeartbeatTimeout:
			// 服务端排空与心跳超时是预期行为，必须重连。
			c.notifyRecoverableClose(code, end.close)
		default:
			c.notifyClosed(code, end.close.reason, end.close.text, false)
			return
		}
	} else {
		c.fire("disconnected", false, func() {
			if c.hooks.Disconnected != nil {
				c.hooks.Disconnected(DisconnectInfo{Reason: reasonText, Err: end.err})
			}
		})
	}

	attempt := 0
	for {
		attempt++
		if c.cfg.MaxReconnectAttempts > 0 && attempt > c.cfg.MaxReconnectAttempts {
			c.notifyClosed(uint16(protocol.CloseNormal), "RECONNECT_EXHAUSTED", "max reconnect attempts", true)
			return
		}
		delay := c.backoff.Delay(attempt)
		c.reconnects.Add(1)
		c.fire("reconnecting", false, func() {
			if c.hooks.Reconnecting != nil {
				c.hooks.Reconnecting(ReconnectInfo{Attempt: attempt, Delay: delay, Reason: reasonText})
			}
		})
		_ = c.sm.Transition(StateReconnecting)
		timer := time.NewTimer(delay)
		select {
		case <-c.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		_ = c.sm.Transition(StateConnecting)
		newSess, err := c.establish(c.ctx)
		if err != nil {
			if errors.Is(err, ErrAuthFailed) {
				c.notifyClosed(uint16(protocol.CloseAuthFailed), protocol.CloseAuthFailed.String(), err.Error(), false)
				return
			}
			reasonText = err.Error()
			_ = c.sm.Transition(StateDisconnected)
			continue
		}
		c.install(newSess)
		c.logf("sdk: reconnected after %d attempt(s)", attempt)
		go c.supervise(newSess)
		go c.backfill("reconnect")
		return
	}
}

// notifyRecoverableClose 触发可恢复的关闭类 hook，并由调用方继续重连。
func (c *Client) notifyRecoverableClose(code uint16, ci *closeInfo) {
	info := CloseInfo{ReasonCode: code, Reason: ci.reason, Text: ci.text, Recoverable: true}
	switch protocol.CloseReason(code) {
	case protocol.CloseServerDraining:
		c.fire("server.draining", true, func() {
			if c.hooks.ServerDraining != nil {
				c.hooks.ServerDraining(info)
			}
		})
	case protocol.CloseHeartbeatTimeout:
		c.fire("heartbeat_timeout", true, func() {
			if c.hooks.HeartbeatTimeout != nil {
				c.hooks.HeartbeatTimeout()
			}
		})
	}
	c.fire("disconnected", false, func() {
		if c.hooks.Disconnected != nil {
			c.hooks.Disconnected(DisconnectInfo{Reason: ci.reason, Err: nil})
		}
	})
}

func (c *Client) fire(name string, critical bool, fn func()) {
	c.dispatcher.Enqueue(name, critical, fn)
}

func (c *Client) logf(format string, args ...any) {
	if c.cfg.Logger != nil {
		c.cfg.Logger(format, args...)
	}
}
