// Package gateway 把继承的 S1 传输层接到 S2 可靠内核上。
// 这里是唯一把帧翻译为 IngressService/Engine 调用的地方。
package gateway

import (
	"context"
	"errors"
	"log"
	"net"
	"sync"
	"time"

	"relayx/s2/internal/config"
	"relayx/s2/internal/connection"
	"relayx/s2/internal/engine"
	"relayx/s2/internal/identity"
	"relayx/s2/internal/idgen"
	"relayx/s2/internal/ingress"
	"relayx/s2/internal/protocol"
	"relayx/s2/internal/sendpath"
	"relayx/s2/internal/session"
	"relayx/s2/internal/store"
	"relayx/s2/internal/telemetry"
)

// Gateway 同时实现 connection.Handler 与 engine.OnlineResolver。
type Gateway struct {
	cfg      config.Config
	auth     *identity.Authenticator
	registry *session.Registry
	conns    *connection.Manager
	metrics  *telemetry.Metrics
	log      *log.Logger

	st  store.Store
	ing *ingress.Service
	eng *engine.Engine

	mu        sync.RWMutex
	byConn    map[*connection.Connection]*session.Session
	bySession map[string]*connection.Connection
	producers map[string]identity.Producer

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type Options struct {
	Cfg     config.Config
	Store   store.Store
	Metrics *telemetry.Metrics
	Log     *log.Logger
}

func New(o Options) *Gateway {
	ctx, cancel := context.WithCancel(context.Background())
	logger := o.Log
	if logger == nil {
		logger = log.Default()
	}
	g := &Gateway{
		cfg:       o.Cfg,
		auth:      identity.New(o.Cfg.TokenSecret),
		registry:  session.NewRegistry(),
		log:       logger,
		st:        o.Store,
		byConn:    make(map[*connection.Connection]*session.Session),
		bySession: make(map[string]*connection.Connection),
		producers: make(map[string]identity.Producer),
		ctx:       ctx,
		cancel:    cancel,
	}
	g.metrics = telemetry.New(
		func(class int) int64 { return g.aggregateChannel(class, 0) },
		func(class int) int64 { return g.aggregateChannel(class, 1) },
		func(class int) int64 { return g.aggregateChannel(class, 2) },
	)
	_ = o.Metrics
	g.conns = connection.NewManager(o.Cfg, g, g.metrics, logger)
	g.eng = engine.New(o.Store, o.Cfg, g, logger)
	g.ing = ingress.New(o.Store, o.Cfg, g.eng)
	return g
}

// Metrics 暴露继承的四通道观测接口。
func (g *Gateway) Metrics() *telemetry.Metrics { return g.metrics }

// aggregateChannel 汇总所有活跃 SendPath 的某项指标。kind：0=depth，
// 1=dropped（丢弃数），2=bytes（字节数）。
func (g *Gateway) aggregateChannel(class, kind int) int64 {
	g.mu.RLock()
	conns := make([]*connection.Connection, 0, len(g.byConn))
	for c := range g.byConn {
		conns = append(conns, c)
	}
	g.mu.RUnlock()
	cl := sendpath.ChannelClass(class)
	var total int64
	for _, c := range conns {
		switch kind {
		case 0:
			total += c.Send.Depth(cl)
		case 1:
			total += c.Send.Dropped(cl)
		case 2:
			total += c.Send.Bytes(cl)
		}
	}
	return total
}

func (g *Gateway) Engine() *engine.Engine                 { return g.eng }
func (g *Gateway) Ingress() *ingress.Service              { return g.ing }
func (g *Gateway) Authenticator() *identity.Authenticator { return g.auth }
func (g *Gateway) Store() store.Store                     { return g.st }

// Lookup 是引擎判断“端点是否在线”的权威来源。
func (g *Gateway) Lookup(tenant, user, device string) (engine.Sink, bool) {
	s, err := g.registry.Lookup(session.Endpoint{TenantID: tenant, UserID: user, DeviceID: device})
	if err != nil {
		return nil, false
	}
	g.mu.RLock()
	c, ok := g.bySession[s.ID]
	g.mu.RUnlock()
	if !ok || c.State() != connection.StateOpen {
		return nil, false
	}
	return c.Send, true
}

func (g *Gateway) OnOpen(_ *connection.Connection) {}

func (g *Gateway) OnClose(c *connection.Connection) {
	g.mu.Lock()
	s := g.byConn[c]
	delete(g.byConn, c)
	if s != nil {
		delete(g.bySession, s.ID)
		delete(g.producers, s.ID)
	}
	g.mu.Unlock()
	if s == nil {
		return
	}
	s.MarkClosed()
	g.registry.Unregister(s.Endpoint, s)
	_ = g.st.Endpoints().MarkOffline(context.Background(),
		s.Endpoint.TenantID, s.Endpoint.UserID, s.Endpoint.DeviceID, s.ID)
}

func (g *Gateway) HandleFrame(c *connection.Connection, f protocol.Frame, _, _ bool) {
	switch f.Type {
	case protocol.TypeLogin:
		g.handleLogin(c, f.Body)
	case protocol.TypeSend:
		g.handleSend(c, f.Body)
	case protocol.TypeReceiptAck:
		g.handleReceiptAck(c, f.Body)
	case protocol.TypeBusinessAck:
		g.handleBusinessAck(c, f.Body)
	case protocol.TypePing:
		_ = c.EnqueueControl(protocol.Frame{Type: protocol.TypePong})
	case protocol.TypePong:
	case protocol.TypeClose:
		c.Close()
	default:
		g.log.Printf("conn %s: unregistered frame 0x%02x", c.ID, f.Type)
	}
}

func (g *Gateway) sessionOf(c *connection.Connection) (*session.Session, bool) {
	g.mu.RLock()
	s := g.byConn[c]
	g.mu.RUnlock()
	if s == nil || s.State() != session.Active {
		return nil, false
	}
	return s, true
}

func (g *Gateway) handleLogin(c *connection.Connection, body []byte) {
	lb, err := protocol.DecodeLogin(body)
	if err != nil {
		c.Close()
		return
	}
	p, err := g.auth.Authenticate(lb.Token)
	if err != nil {
		c.ReplyError(protocol.ErrorDBUnavailable) // auth failure never enters business path
		c.Close()
		return
	}
	if _, ok := g.sessionOf(c); ok {
		c.Close()
		return
	}
	device := lb.DeviceID
	if p.Kind == store.ProducerService {
		device = "" // service tokens have no source device
	}
	ep := session.Endpoint{TenantID: p.TenantID, UserID: p.UserID(), DeviceID: device}
	s := session.NewSession(idgen.New("s"), ep, p.ID, protocol.S2Capabilities)
	if !s.Activate() {
		c.Close()
		return
	}
	old, _ := g.registry.Replace(&session.Entry{Session: s})
	g.mu.Lock()
	g.byConn[c] = s
	g.bySession[s.ID] = c
	g.producers[s.ID] = p
	g.mu.Unlock()
	if old != nil && old.Session != s {
		old.Session.MarkReplaced()
		if oc, ok := g.bySessionOf(old); ok {
			_ = oc.EnqueueControl(protocol.Frame{Type: protocol.TypeClose})
			oc.Close()
		}
	}
	_ = g.st.Endpoints().MarkOnline(context.Background(), ep.TenantID, ep.UserID, ep.DeviceID, s.ID)
	_ = c.EnqueueControl(protocol.Frame{Type: protocol.TypeLoginAck, Body: protocol.EncodeLoginAck(protocol.S2Capabilities)})

	// 重连触发是持久化的：激活等待中的投递并重新派发。
	go func() {
		if err := g.eng.ResumeEndpoint(g.ctx, ep.TenantID, ep.UserID, ep.DeviceID); err != nil {
			g.log.Printf("resume endpoint %s: %v", ep.Key(), err)
		}
	}()
}

func (g *Gateway) bySessionOf(old *session.Entry) (*connection.Connection, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	c, ok := g.bySession[old.Session.ID]
	return c, ok
}

func (g *Gateway) handleSend(c *connection.Connection, body []byte) {
	s, ok := g.sessionOf(c)
	if !ok {
		g.log.Printf("conn %s: send without active session", c.ID)
		c.Close()
		return
	}
	sb, err := protocol.DecodeSend(body)
	if err != nil {
		g.log.Printf("conn %s: bad send body: %v", c.ID, err)
		c.Close()
		return
	}
	g.mu.RLock()
	p := g.producers[s.ID]
	g.mu.RUnlock()
	policy := config.DefaultPolicy()
	if sb.Policy != nil {
		mode := "all_endpoints"
		if sb.Policy.DeliveryMode == 1 {
			mode = "last_active"
		}
		policy = config.Policy{
			TTLSeconds: sb.Policy.TTLSeconds, MaxAttempts: sb.Policy.MaxAttempts,
			DeliveryMode: mode, Fanout: sb.Policy.Fanout,
		}
	}
	source := ""
	if p.Kind == store.ProducerUser {
		source = s.Endpoint.DeviceID
	}
	res, err := g.ing.Submit(g.ctx, ingress.SubmitInput{
		TenantID: p.TenantID, ProducerID: p.ID, ProducerKind: p.Kind,
		Sender: p.UserID(), SourceDeviceID: source,
		ClientMsgID: sb.ClientMsgID, TargetUserID: sb.TargetUserID,
		TargetDevices: sb.TargetDevices, Payload: sb.Payload, Policy: policy,
	})
	if err != nil {
		g.replySubmitError(c, err)
		return
	}
	_ = c.EnqueueControl(protocol.Frame{Type: protocol.TypeSendAck, Body: protocol.EncodeSendAck(protocol.SendAckBody{
		ClientMsgID: sb.ClientMsgID, MessageID: res.MessageID,
		PlatformState: res.State, AcceptedAt: res.AcceptedAt.UnixMilli(), Duplicate: res.Duplicate,
	})})
	if res.State == store.MessageAccepted {
		go func(id string) {
			if err := g.eng.DispatchMessage(g.ctx, id); err != nil {
				g.log.Printf("dispatch message %s: %v", id, err)
			}
		}(res.MessageID)
	}
}

func (g *Gateway) replySubmitError(c *connection.Connection, err error) {
	switch {
	case errors.Is(err, store.ErrPolicyLimit):
		c.ReplyError(protocol.ErrorPolicyLimit)
	case errors.Is(err, store.ErrDuplicateMismatch):
		c.ReplyError(protocol.ErrorIdempotencyConflict)
	default:
		c.ReplyError(protocol.ErrorDBUnavailable)
	}
}

func (g *Gateway) handleReceiptAck(c *connection.Connection, body []byte) {
	s, ok := g.sessionOf(c)
	if !ok {
		return
	}
	rb, err := protocol.DecodeReceiptAck(body)
	if err != nil {
		return
	}
	if err := g.eng.HandleReceiptAck(g.ctx, s.Endpoint.TenantID, rb.MessageID,
		s.Endpoint.UserID, s.Endpoint.DeviceID); err != nil {
		if errors.Is(err, store.ErrUnknownMessage) {
			c.ReplyError(protocol.ErrorUnknownMessage)
		}
	}
}

func (g *Gateway) handleBusinessAck(c *connection.Connection, body []byte) {
	s, ok := g.sessionOf(c)
	if !ok {
		return
	}
	bb, err := protocol.DecodeBusinessAck(body)
	if err != nil {
		return
	}
	if err := g.eng.HandleBusinessAck(g.ctx, s.Endpoint.TenantID, bb.MessageID,
		s.Endpoint.UserID, s.Endpoint.DeviceID, string(bb.ApplicationData)); err != nil {
		if errors.Is(err, store.ErrUnknownMessage) {
			c.ReplyError(protocol.ErrorUnknownMessage)
		}
	}
}

// Start 启动后台扫描器与漂移修复。
func (g *Gateway) Start() {
	g.wg.Add(3)
	go func() { defer g.wg.Done(); _ = g.eng.RunRetryScheduler(g.ctx) }()
	go func() { defer g.wg.Done(); _ = g.eng.RunExpiryScanner(g.ctx) }()
	go func() { defer g.wg.Done(); g.driftLoop() }()
}

func (g *Gateway) driftLoop() {
	ticker := time.NewTicker(g.cfg.DriftCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-g.ctx.Done():
			return
		case <-ticker.C:
			bad, err := g.st.Endpoints().ListInconsistentOnline(g.ctx, func(t, u, d string) bool {
				_, ok := g.Lookup(t, u, d)
				return ok
			})
			if err != nil {
				continue
			}
			for _, e := range bad {
				_ = g.st.Endpoints().MarkOffline(g.ctx, e.TenantID, e.UserID, e.DeviceID, "")
				g.log.Printf("endpoint projection drift corrected: %s/%s/%s", e.TenantID, e.UserID, e.DeviceID)
			}
		}
	}
}

// ServeTCP 阻塞接收客户端连接。
func (g *Gateway) ServeTCP() error { return g.conns.ListenAndServe() }

// Serve 在注入的 listener 上接收连接（测试或自定义装配）。
func (g *Gateway) Serve(ln net.Listener) error { return g.conns.Serve(ln) }

func (g *Gateway) Shutdown(ctx context.Context) error {
	g.cancel()
	err := g.conns.Shutdown(ctx)
	g.wg.Wait()
	return err
}

var _ engine.OnlineResolver = (*Gateway)(nil)
var _ connection.Handler = (*Gateway)(nil)
