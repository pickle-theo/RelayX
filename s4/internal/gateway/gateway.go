// Package gateway 组装 S4 单机 Gateway：S2 兼容可靠内核 + S3 订阅广播 +
// S4 事件投影 / self-scoped 路由 / 端侧查询 API / CLOSE 原因码。
//
// 它是唯一把帧翻译为内核/投影调用的地方；投影层只读事实，不写状态。
package gateway

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"relayx/s4/internal/broadcast"
	"relayx/s4/internal/config"
	"relayx/s4/internal/connection"
	"relayx/s4/internal/core"
	"relayx/s4/internal/identity"
	"relayx/s4/internal/projector"
	"relayx/s4/internal/query"
	"relayx/s4/internal/sendpath"
	"relayx/s4/internal/session"
	"relayx/s4/internal/telemetry"
	"relayx/s4/protocol"
)

// Gateway 实现 connection.Handler、core.OnlineResolver 与 projector.SessionLocator。
type Gateway struct {
	cfg      config.Config
	auth     *identity.Authenticator
	registry *session.Registry
	conns    *connection.Manager
	metrics  *telemetry.Metrics
	log      *slog.Logger

	core      *core.Kernel
	clock     core.Clock
	seqAlloc  *projector.SeqAllocator
	events    *projector.Metrics
	router    *projector.Router
	projector *projector.Projector
	query     *query.Service
	bc        *broadcast.Component

	mu        sync.RWMutex
	sessions  map[string]*connection.Connection // sessionID → Connection
	byConn    map[string]*session.Session
	sendpaths map[string]*sendpath.SendPath
	reasons   map[string]int64

	sessionSeq atomic.Uint64
	legacyMode atomic.Bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Options 是 Gateway 构造依赖。
type Options struct {
	Cfg config.Config
	Log *slog.Logger
}

// New 创建 Gateway。
func New(o Options) *Gateway {
	logger := o.Log
	if logger == nil {
		logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	g := &Gateway{
		cfg: o.Cfg, auth: identity.New(o.Cfg.TokenSecret),
		registry: session.NewRegistry(), log: logger,
		sessions:  make(map[string]*connection.Connection),
		byConn:    make(map[string]*session.Session),
		sendpaths: make(map[string]*sendpath.SendPath),
		reasons:   make(map[string]int64),
		ctx:       ctx, cancel: cancel,
	}
	g.metrics = telemetry.New(
		func(class int) int64 { return g.aggregateChannel(class, 0) },
		func(class int) int64 { return g.aggregateChannel(class, 1) },
		func(class int) int64 { return g.aggregateChannel(class, 2) },
	)
	g.metrics.SetExtra(g.RenderServiceMetrics)
	if o.Cfg.LegacyServer {
		g.legacyMode.Store(true)
	}

	g.clock = core.SystemClock{}
	if o.Cfg.TestHooks {
		g.clock = core.NewSkewClock()
	}
	g.core = core.New(core.Options{
		MaxPayload:         o.Cfg.MaxPayload,
		MaxTTL:             o.Cfg.MaxTTL,
		MaxAttempts:        o.Cfg.MaxAttempts,
		DefaultTTL:         o.Cfg.DefaultTTL,
		DefaultMaxAttempts: o.Cfg.DefaultMaxAttempts,
	}, g.clock)
	g.core.SetOnlineResolver(g)

	g.events = projector.NewMetrics(func() int64 { return g.aggregateChannel(3, 0) })
	g.seqAlloc = projector.NewSeqAllocator()
	g.router = projector.NewRouter(g, g.seqAlloc, g.events, logger)
	g.projector = projector.New(projector.Options{
		Source:   g.core,
		Messages: g.core,
		Router:   g.router,
		Cursor:   g.newCursor(),
		Interval: o.Cfg.EventPollInterval,
		Batch:    o.Cfg.EventBatch,
		Types:    g.projectedFactTypes,
		Signal:   g.core.Signal,
		Metrics:  g.events,
		Log:      logger,
	})
	g.query = query.New(g.core, g.auth)
	g.bc = broadcast.NewComponent(o.Cfg, g.auth)
	g.conns = connection.NewManager(o.Cfg, g, g.metrics, slogToStd(logger))
	return g
}

// newCursor 根据配置选择内存或文件游标。
func (g *Gateway) newCursor() projector.CursorStore {
	if g.cfg.CursorKind == "file" && g.cfg.CursorPath != "" {
		c, err := projector.NewFileCursor(g.cfg.CursorPath)
		if err != nil {
			g.log.Error("load projection cursor, falling back to memory", "err", err)
			return projector.NewMemoryCursor()
		}
		return c
	}
	return projector.NewMemoryCursor()
}

// aggregateChannel 聚合所有连接的通道指标。kind: 0=Depth,1=Dropped,2=Bytes。
func (g *Gateway) aggregateChannel(class, kind int) int64 {
	g.mu.RLock()
	paths := make([]*sendpath.SendPath, 0, len(g.sendpaths))
	for _, sp := range g.sendpaths {
		paths = append(paths, sp)
	}
	g.mu.RUnlock()
	cl := sendpath.ChannelClass(class)
	var total int64
	for _, sp := range paths {
		switch kind {
		case 0:
			total += sp.Depth(cl)
		case 1:
			total += sp.Dropped(cl)
		case 2:
			total += sp.Bytes(cl)
		}
	}
	return total
}

// Metrics 返回内部指标实例（cmd/gateway 构造 HTTP server 用）。
func (g *Gateway) Metrics() *telemetry.Metrics { return g.metrics }

// ConnManager 返回连接管理器。
func (g *Gateway) ConnManager() *connection.Manager { return g.conns }

// Registry 返回会话注册表（测试断言用）。
func (g *Gateway) Registry() *session.Registry { return g.registry }

// Core 返回事实内核（测试/场景断言用）。
func (g *Gateway) Core() *core.Kernel { return g.core }

// Projector 返回事件投影器（测试用）。
func (g *Gateway) Projector() *projector.Projector { return g.projector }

// Events 返回事件面指标。
func (g *Gateway) Events() *projector.Metrics { return g.events }

// Broadcast 返回 S3 广播子系统。
func (g *Gateway) Broadcast() *broadcast.Component { return g.bc }

// Query 返回端侧查询服务。
func (g *Gateway) Query() *query.Service { return g.query }

// Router 返回事件路由器（测试钩子用）。
func (g *Gateway) Router() *projector.Router { return g.router }

// Start 启动后台任务：投影游标轮询 + 过期扫描。
func (g *Gateway) Start() {
	g.wg.Add(2)
	go func() { defer g.wg.Done(); _ = g.projector.Run(g.ctx) }()
	go func() { defer g.wg.Done(); _ = g.core.RunExpiryScanner(g.ctx, g.cfg.ScanInterval) }()
}

// Shutdown 关闭后台任务与全部连接。
func (g *Gateway) Shutdown(ctx context.Context) error {
	g.cancel()
	err := g.conns.Shutdown(ctx)
	g.wg.Wait()
	return err
}

// Drain 向所有存活会话发送 SERVER_DRAINING 后断连，监听器保持不变。
// 客户端按契约重连，用于验证「服务端排空是预期行为」。
func (g *Gateway) Drain() int {
	g.mu.RLock()
	conns := make([]*connection.Connection, 0, len(g.sessions))
	for _, c := range g.sessions {
		conns = append(conns, c)
	}
	g.mu.RUnlock()
	for _, c := range conns {
		c.CloseWithReason(protocol.CloseServerDraining, protocol.CloseServerDraining.String())
	}
	if g.cfg.DrainGrace > 0 {
		time.Sleep(g.cfg.DrainGrace)
	}
	return len(conns)
}

// SetLegacyMode 切换"旧服务端"能力宣告（仅验证钩子）。
func (g *Gateway) SetLegacyMode(on bool) { g.legacyMode.Store(on) }

// SkipNextEvents 注入丢事件（仅验证钩子，用于制造 seq gap）。
func (g *Gateway) SkipNextEvents(n int64) { g.router.SkipNext(n) }

// ---------- core.OnlineResolver ----------

// Lookup 把端点解析为其活跃连接的 P1 出口。
func (g *Gateway) Lookup(tenant, user, device string) (core.Sink, bool) {
	s, err := g.registry.Lookup(session.Endpoint{TenantID: tenant, UserID: user, DeviceID: device})
	if err != nil {
		return nil, false
	}
	g.mu.RLock()
	c, ok := g.sessions[s.ID]
	g.mu.RUnlock()
	if !ok || c.State() != connection.StateOpen {
		return nil, false
	}
	return connSink{c: c, timeout: g.cfg.EnqueueTimeout}, true
}

// ---------- projector.SessionLocator ----------

// Locate 按 (tenant, producer_id, source_device_id) 定位发送设备的当前会话。
// 来源设备来自事实（不是进程内连接上下文）；跨租户自然不命中。
func (g *Gateway) Locate(tenantID, producerID, sourceDeviceID string) (projector.EventSink, bool) {
	if sourceDeviceID == "" {
		return nil, false
	}
	userID := producerUserID(producerID)
	if userID == "" {
		return nil, false
	}
	s, err := g.registry.Lookup(session.Endpoint{
		TenantID: tenantID, UserID: userID, DeviceID: sourceDeviceID,
	})
	if err != nil || s.ProducerID != producerID {
		return nil, false
	}
	g.mu.RLock()
	c, ok := g.sessions[s.ID]
	g.mu.RUnlock()
	if !ok || c.State() != connection.StateOpen {
		return nil, false
	}
	return eventSink{sess: s, conn: c}, true
}

func producerUserID(producerID string) string {
	switch {
	case len(producerID) > 2 && producerID[:2] == "u:":
		return producerID[2:]
	case len(producerID) > 4 && producerID[:4] == "svc:":
		return producerID[4:]
	default:
		return ""
	}
}

// projectedFactTypes 返回所有已订阅会话的事件位并集对应的事实类型。
// 没有任何订阅时只投影聚合终态（默认位）。
func (g *Gateway) projectedFactTypes() []string {
	var mask protocol.EventMask = protocol.EventMaskRequired
	g.mu.RLock()
	for _, s := range g.byConn {
		mask |= protocol.EventMask(s.EventMask())
	}
	g.mu.RUnlock()
	types := make([]string, 0, 5)
	for _, et := range protocol.EventTypesForMask(mask) {
		if ft := factTypeForEvent(et); ft != "" {
			types = append(types, ft)
		}
	}
	return types
}

func factTypeForEvent(t protocol.ClientEventType) string {
	switch t {
	case protocol.EvMessageSettled:
		return core.FactMessageSettled
	case protocol.EvMessageExpired:
		return core.FactMessageExpired
	case protocol.EvDeliveryReceiptAcked:
		return core.FactDeliveryReceiptAcked
	case protocol.EvDeliveryFailed:
		return core.FactDeliveryFailed
	case protocol.EvDeliveryExpired:
		return core.FactDeliveryExpired
	default:
		return ""
	}
}

// ---------- connection.Handler ----------

// OnOpen 登记连接的 SendPath 供 /metrics 聚合。
func (g *Gateway) OnOpen(c *connection.Connection) {
	g.mu.Lock()
	g.sendpaths[c.ID] = c.Send
	g.mu.Unlock()
}

// HandleFrame 按帧类型分发入站帧。
func (g *Gateway) HandleFrame(c *connection.Connection, f protocol.Frame, seqGap, outOfOrder bool) {
	if seqGap || outOfOrder {
		g.log.Debug("frame sequence anomaly",
			telemetry.FieldConnID, c.ID, "seq", f.Sequence, "gap", seqGap, "ooo", outOfOrder)
	}
	switch f.Type {
	case protocol.TypeLogin:
		g.handleLogin(c, f)
	case protocol.TypeSend:
		g.handleSend(c, f)
	case protocol.TypeReceiptAck:
		g.handleReceiptAck(c, f)
	case protocol.TypeBusinessAck:
		g.handleBusinessAck(c, f)
	case protocol.TypeSubscribe:
		g.handleSubscribe(c, f)
	case protocol.TypeUnsubscribe:
		g.handleUnsubscribe(c, f)
	case protocol.TypeSubscribeEvents:
		g.handleSubscribeEvents(c, f)
	case protocol.TypePing:
		c.SendControl(protocol.Frame{Type: protocol.TypePong})
	case protocol.TypePong:
	case protocol.TypeClose:
		c.Close()
	default:
		// S2 语义：未注册帧只忽略并记录，不改变连接状态。
		g.log.Debug("unregistered frame", telemetry.FieldConnID, c.ID, "type", f.Type)
	}
}

func (g *Gateway) handleLogin(c *connection.Connection, f protocol.Frame) {
	lb, err := protocol.DecodeLogin(f.Body)
	if err != nil {
		c.CloseWithReason(protocol.CloseAuthFailed, "malformed login")
		return
	}
	producer, err := g.auth.Authenticate(lb.Token)
	if err != nil {
		g.metrics.AuthFailedInc()
		c.CloseWithReason(protocol.CloseAuthFailed, "authentication failed")
		return
	}
	device := lb.DeviceID
	if producer.Kind == core.ProducerService {
		device = ""
	}
	if producer.Kind == core.ProducerUser && device == "" {
		c.CloseWithReason(protocol.CloseAuthFailed, "missing device id")
		return
	}

	ep := session.Endpoint{TenantID: producer.TenantID, UserID: producer.UserID(), DeviceID: device}
	s := session.NewSession(fmt.Sprintf("s%d", g.sessionSeq.Add(1)), ep, producer.ID, g.capabilities())
	s.ProducerKind = producer.Kind

	g.mu.Lock()
	g.sessions[s.ID] = c
	g.byConn[c.ID] = s
	g.mu.Unlock()

	old, _ := g.registry.Register(&session.Entry{Session: s})
	if old != nil && old.Session != s && old.Session.State() == session.Active {
		g.metrics.ReplacedInc()
		old.Session.MarkReplaced()
		if oldConn, ok := g.sessionConn(old.Session.ID); ok && oldConn == c {
			// 同一连接重复登录：摘掉旧映射，不关闭自身。
			g.mu.Lock()
			delete(g.sessions, old.Session.ID)
			g.mu.Unlock()
			g.bc.Index.RemoveSession(old.Session.ID)
		} else if oldConn, ok := g.sessionConn(old.Session.ID); ok {
			// 旧会话收到 REPLACED(1001)：是否提示"已替换其他设备"留给业务。
			oldConn.CloseWithReason(protocol.CloseReplaced, protocol.CloseReplaced.String())
		}
	}
	s.Activate()
	g.metrics.LoginInc()
	g.core.MarkOnline(ep.TenantID, ep.UserID, ep.DeviceID, s.ID)

	g.log.Info("session login",
		telemetry.FieldTenantID, ep.TenantID,
		telemetry.FieldEndpoint, ep.Key(),
		telemetry.FieldSessionID, s.ID)

	c.SendControl(protocol.Frame{
		Type: protocol.TypeLoginAck,
		Body: protocol.EncodeLoginAck(s.Capabilities),
	})

	// 重连恢复：把该端点等待中的投递重新派发（持久化触发，不依赖回调）。
	go g.resumeEndpoint(ep)
}

func (g *Gateway) resumeEndpoint(ep session.Endpoint) {
	for _, id := range g.core.PendingMessagesFor(ep.TenantID, ep.UserID, ep.DeviceID) {
		if err := g.core.Dispatch(g.ctx, id); err != nil {
			g.log.Warn("resume dispatch failed", "message_id", id, "err", err)
		}
	}
}

// capabilities 返回 LOGIN_ACK 能力位；legacyMode 模拟旧服务端。
func (g *Gateway) capabilities() uint32 {
	if g.legacyMode.Load() {
		return protocol.LegacyCapabilities
	}
	return protocol.S4Capabilities
}

func (g *Gateway) sessionConn(sessID string) (*connection.Connection, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	conn, ok := g.sessions[sessID]
	return conn, ok
}

func (g *Gateway) sessionOf(c *connection.Connection) (*session.Session, bool) {
	g.mu.RLock()
	s := g.byConn[c.ID]
	g.mu.RUnlock()
	if s == nil || s.State() != session.Active {
		return nil, false
	}
	return s, true
}

func (g *Gateway) handleSend(c *connection.Connection, f protocol.Frame) {
	s, ok := g.sessionOf(c)
	if !ok {
		// 与 S2 一致：未登录的 SEND 不进业务路径。ERROR 码表（1001/1003/1004/1005）
		// 里没有"未鉴权"，用 1005 会把端侧误导成可重试的存储故障。
		g.log.Warn("send without active session", telemetry.FieldConnID, c.ID)
		c.Close()
		return
	}
	sb, err := protocol.DecodeSend(f.Body)
	if err != nil {
		// S4 语义：Body 解码失败属传输层错误，ERROR 码表不承载它，直接断连。
		g.log.Warn("bad send body", telemetry.FieldConnID, c.ID, "err", err)
		c.Close()
		return
	}
	var policy core.Policy
	if sb.Policy != nil {
		mode := core.ModeAllEndpoints
		if sb.Policy.DeliveryMode == 1 {
			mode = core.ModeLastActive
		}
		policy = core.Policy{
			TTLSeconds: sb.Policy.TTLSeconds, MaxAttempts: sb.Policy.MaxAttempts,
			DeliveryMode: mode, Fanout: sb.Policy.Fanout,
		}
	}
	sourceDevice := ""
	if s.ProducerKind == core.ProducerUser {
		sourceDevice = s.Endpoint.DeviceID
	}
	res, err := g.core.Submit(g.ctx, core.Input{
		TenantID: s.Endpoint.TenantID, ProducerID: s.ProducerID, ProducerKind: s.ProducerKind,
		Sender: s.Endpoint.UserID, SourceDeviceID: sourceDevice,
		ClientMsgID: sb.ClientMsgID, TargetUserID: sb.TargetUserID,
		TargetDevices: sb.TargetDevices, Payload: sb.Payload, Policy: policy,
	})
	if err != nil {
		g.replySubmitError(c, err)
		return
	}
	c.SendControl(protocol.Frame{
		Type: protocol.TypeSendAck,
		Body: protocol.EncodeSendAck(protocol.SendAckBody{
			ClientMsgID: sb.ClientMsgID, MessageID: res.MessageID,
			PlatformState: res.State, AcceptedAt: res.AcceptedAt.UnixMilli(),
			Duplicate: res.Duplicate,
		}),
	})
	if res.State == core.MessageAccepted {
		go func(id string) {
			if err := g.core.Dispatch(g.ctx, id); err != nil {
				g.log.Warn("dispatch message failed", "message_id", id, "err", err)
			}
		}(res.MessageID)
	}
}

func (g *Gateway) replySubmitError(c *connection.Connection, err error) {
	switch {
	case errors.Is(err, core.ErrPolicyLimit):
		c.ReplyError(protocol.ErrorPolicyLimit)
	case errors.Is(err, core.ErrDuplicateMismatch):
		c.ReplyError(protocol.ErrorIdempotencyConflict)
	default:
		c.ReplyError(protocol.ErrorDBUnavailable)
	}
}

func (g *Gateway) handleReceiptAck(c *connection.Connection, f protocol.Frame) {
	s, ok := g.sessionOf(c)
	if !ok {
		return
	}
	rb, err := protocol.DecodeReceiptAck(f.Body)
	if err != nil {
		return
	}
	if err := g.core.ReceiptAck(g.ctx, s.Endpoint.TenantID, rb.MessageID,
		s.Endpoint.UserID, s.Endpoint.DeviceID); err != nil {
		g.log.Warn("receipt ack failed", "message_id", rb.MessageID,
			"endpoint", s.Endpoint.Key(), "err", err)
		if errors.Is(err, core.ErrUnknownMessage) {
			c.ReplyError(protocol.ErrorUnknownMessage)
		}
		return
	}
	g.log.Debug("receipt acked", "message_id", rb.MessageID, "endpoint", s.Endpoint.Key())
}

func (g *Gateway) handleBusinessAck(c *connection.Connection, f protocol.Frame) {
	s, ok := g.sessionOf(c)
	if !ok {
		return
	}
	bb, err := protocol.DecodeBusinessAck(f.Body)
	if err != nil {
		return
	}
	if err := g.core.BusinessAck(g.ctx, s.Endpoint.TenantID, bb.MessageID,
		s.Endpoint.UserID, s.Endpoint.DeviceID, string(bb.ApplicationData)); err != nil {
		if errors.Is(err, core.ErrUnknownMessage) {
			c.ReplyError(protocol.ErrorUnknownMessage)
		}
	}
}

// handleSubscribeEvents 处理 SUBSCRIBE_EVENTS（S4 门控：未订阅则不推事件）。
func (g *Gateway) handleSubscribeEvents(c *connection.Connection, f protocol.Frame) {
	s, ok := g.sessionOf(c)
	if !ok {
		g.replyEventsAck(c, protocol.SubscribeEventsNotLoggedIn, 0)
		return
	}
	sb, err := protocol.DecodeSubscribeEvents(f.Body)
	if err != nil {
		g.replyEventsAck(c, protocol.SubscribeEventsNotLoggedIn, 0)
		return
	}
	if s.Capabilities&protocol.CapClientEvents == 0 {
		g.replyEventsAck(c, protocol.SubscribeEventsNotLoggedIn, 0)
		return
	}
	granted := protocol.SanitizeRequestedMask(sb.Mask)
	s.SetEventMask(uint32(granted))
	g.log.Info("client events subscribed",
		telemetry.FieldSessionID, s.ID,
		"requested", uint32(sb.Mask), "granted", uint32(granted))
	g.replyEventsAck(c, protocol.SubscribeEventsOK, granted)
}

// replyEventsAck 走 P0：它是握手确认而非事件流，不能被 P3 预算丢弃。
func (g *Gateway) replyEventsAck(c *connection.Connection, result protocol.SubscribeEventsResult, granted protocol.EventMask) {
	c.SendControl(protocol.Frame{
		Type: protocol.TypeSubscribeEventsAck,
		Body: protocol.EncodeSubscribeEventsAck(result, granted),
	})
}

// handleSubscribe 处理 SUBSCRIBE：只验签、不查业务成员库（S3 契约）。
func (g *Gateway) handleSubscribe(c *connection.Connection, f protocol.Frame) {
	sb, err := protocol.DecodeSubscribe(f.Body)
	if err != nil {
		g.log.Warn("bad subscribe body", telemetry.FieldConnID, c.ID, "err", err)
		c.Close()
		return
	}
	s, ok := g.sessionOf(c)
	if !ok {
		g.replySubscribeAck(c, sb.Topic, protocol.SubscribeNotLoggedIn, "not logged in")
		return
	}
	claims, err := g.bc.Verifier.VerifyFor(c.Context(), sb.Token, s.Endpoint.TenantID, sb.Topic)
	if err != nil {
		g.bc.Metrics.SubscribeFailInc()
		result := protocol.SubscribeBadToken
		switch {
		case errors.Is(err, broadcast.ErrWrongTenant):
			result = protocol.SubscribeBadTenant
		case errors.Is(err, broadcast.ErrWrongTopic):
			result = protocol.SubscribeBadTopic
		}
		g.replySubscribeAck(c, sb.Topic, result, err.Error())
		return
	}
	created, err := g.bc.Index.Subscribe(s.ID, claims, g.bc.SenderFactoryFor(c))
	if err != nil {
		g.bc.Metrics.SubscribeFailInc()
		result := protocol.SubscribeOverLimit
		switch {
		case errors.Is(err, broadcast.ErrBadTopic):
			result = protocol.SubscribeBadToken
		case errors.Is(err, broadcast.ErrSessionFull), errors.Is(err, broadcast.ErrTopicFull), errors.Is(err, broadcast.ErrIndexFull):
			result = protocol.SubscribeOverLimit
		}
		g.replySubscribeAck(c, sb.Topic, result, err.Error())
		return
	}
	if !created {
		g.replySubscribeAck(c, sb.Topic, protocol.SubscribeDuplicate, "already subscribed")
		return
	}
	g.replySubscribeAck(c, sb.Topic, protocol.SubscribeOK, "ok")
}

func (g *Gateway) replySubscribeAck(c *connection.Connection, topic string, result protocol.SubscribeResult, reason string) {
	c.SendControl(protocol.Frame{
		Type: protocol.TypeSubscribeAck,
		Body: protocol.EncodeSubscribeAck(topic, result, reason),
	})
}

func (g *Gateway) handleUnsubscribe(c *connection.Connection, f protocol.Frame) {
	ub, err := protocol.DecodeUnsubscribe(f.Body)
	if err != nil {
		g.log.Warn("bad unsubscribe body", telemetry.FieldConnID, c.ID, "err", err)
		c.Close()
		return
	}
	s, ok := g.sessionOf(c)
	result := protocol.UnsubscribeNotExists
	if ok {
		if removed, err := g.bc.Index.Unsubscribe(s.ID, ub.Topic); err == nil && removed {
			result = protocol.UnsubscribeOK
		}
	}
	c.SendControl(protocol.Frame{
		Type: protocol.TypeUnsubscribeAck,
		Body: protocol.EncodeUnsubscribeAck(ub.Topic, result),
	})
}

// OnClose 注销会话、清理订阅与 per-session seq 计数器。
func (g *Gateway) OnClose(c *connection.Connection) {
	g.mu.Lock()
	s := g.byConn[c.ID]
	delete(g.byConn, c.ID)
	delete(g.sendpaths, c.ID)
	if s != nil {
		delete(g.sessions, s.ID)
	}
	g.mu.Unlock()

	reason := c.CloseReason().String()
	g.mu.Lock()
	g.reasons[reason]++
	g.mu.Unlock()

	if s == nil {
		return
	}
	g.bc.Index.RemoveSession(s.ID)
	g.registry.Unregister(s.Endpoint, s)
	s.MarkClosed()
	g.core.MarkOffline(s.Endpoint.TenantID, s.Endpoint.UserID, s.Endpoint.DeviceID, s.ID)
	g.seqAlloc.Delete(s.ID)
	g.log.Info("session closed",
		telemetry.FieldSessionID, s.ID, telemetry.FieldConnID, c.ID, "reason", reason)
}

// RenderServiceMetrics 合并广播、客户端事件与查询延迟指标。
func (g *Gateway) RenderServiceMetrics() string {
	return g.bc.RenderMetrics() + g.events.Render() + g.query.Metrics().Render() + g.renderReasons()
}

func (g *Gateway) renderReasons() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	var b []byte
	for reason, n := range g.reasons {
		b = fmt.Appendf(b, "reconnect_reason_total{reason=%q} %d\n", reason, n)
	}
	return string(b)
}

// connSink 把内核的 P1 出口适配到具体连接，并给背压加时间上限：
// 引擎/派发是有限并发，不能被单个慢消费者无限期拖住。
type connSink struct {
	c       *connection.Connection
	timeout time.Duration
}

func (s connSink) EnqueueReliable(ctx context.Context, f protocol.Frame) error {
	if s.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}
	if err := s.c.EnqueueReliable(ctx, f); err != nil {
		if errors.Is(err, sendpath.ErrQueueFull) {
			s.c.CloseWithReason(protocol.ClosePolicyViolation, "slow consumer")
		}
		return err
	}
	return nil
}

var _ core.Sink = connSink{}

// eventSink 是 projector.EventSink 的网关适配。
type eventSink struct {
	sess *session.Session
	conn *connection.Connection
}

func (e eventSink) SessionID() string { return e.sess.ID }

func (e eventSink) EventMask() protocol.EventMask {
	return protocol.EventMask(e.sess.EventMask())
}

func (e eventSink) EnqueueClientEvent(f protocol.Frame) error {
	return e.conn.TryEnqueueEvent(f)
}

// slogToStd 把 *slog.Logger 适配成 *log.Logger。
func slogToStd(l *slog.Logger) *log.Logger {
	return slog.NewLogLogger(l.Handler(), slog.LevelInfo)
}

var _ connection.Handler = (*Gateway)(nil)
var _ core.OnlineResolver = (*Gateway)(nil)
var _ projector.SessionLocator = (*Gateway)(nil)
