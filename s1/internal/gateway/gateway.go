// Package gateway 组装 S1 单机 Gateway：实现 connection.Handler，把鉴权、
// 会话、路由与四通道发送路径串起来（LLD §5/§7/§9）。
package gateway

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"sync"
	"sync/atomic"

	"relayx/s1/internal/config"
	"relayx/s1/internal/connection"
	"relayx/s1/internal/identity"
	"relayx/s1/internal/protocol"
	"relayx/s1/internal/route"
	"relayx/s1/internal/sendpath"
	"relayx/s1/internal/session"
	"relayx/s1/internal/telemetry"
)

// Gateway 实现 connection.Handler。
type Gateway struct {
	cfg      config.Config
	auth     *identity.Authenticator
	registry *session.Registry
	router   *route.Router
	conns    *connection.Manager
	metrics  *telemetry.Metrics
	log      *slog.Logger

	mu         sync.RWMutex
	sessions   map[string]*connection.Connection // sessionID → Connection
	byConn     map[string]*session.Session       // connectionID → Session（登录时绑定）
	sendpaths  map[string]*sendpath.SendPath     // connID → SendPath（/metrics 聚合）
	sessionSeq atomic.Uint64
}

// Options 是 Gateway 构造依赖。
type Options struct {
	Cfg     config.Config
	Metrics *telemetry.Metrics
	Log     *slog.Logger
}

// New 创建 Gateway。
func New(o Options) *Gateway {
	logger := o.Log
	if logger == nil {
		logger = slog.Default()
	}
	g := &Gateway{
		cfg: o.Cfg, auth: identity.New(o.Cfg.TokenSecret),
		registry: session.NewRegistry(), log: logger,
		sessions:  make(map[string]*connection.Connection),
		byConn:    make(map[string]*session.Session),
		sendpaths: make(map[string]*sendpath.SendPath),
	}
	// 四通道指标：聚合当前所有连接的 SendPath。若外部传入 Metrics（测试），
	// 仍用本 Gateway 的聚合闭包覆盖通道读取，保证 /metrics 反映真实队列。
	metrics := telemetry.New(
		func(class int) int64 { return g.aggregateChannel(class, 0) },
		func(class int) int64 { return g.aggregateChannel(class, 1) },
		func(class int) int64 { return g.aggregateChannel(class, 2) },
	)
	if o.Metrics != nil {
		// 保留外部指标计数器引用不现实（各自独立），这里统一使用内部实例。
		_ = o.Metrics
	}
	g.metrics = metrics
	g.router = route.New(g.registry, g.itinerary)
	g.conns = connection.NewManager(o.Cfg, g, metrics, slogToStd(logger))
	return g
}

// aggregateChannel 聚合所有连接指定通道的指标。kind: 0=Depth,1=Dropped,2=Bytes。
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

// itinerary 把 Session 解析为可投递目标（route.Router 注入点）。
func (g *Gateway) itinerary(s *session.Session) (route.Destination, bool) {
	g.mu.RLock()
	conn, ok := g.sessions[s.ID]
	g.mu.RUnlock()
	if !ok || conn.State() != connection.StateOpen {
		return route.Destination{}, false
	}
	return route.Destination{
		Session: s,
		Deliver: func(from string, payload []byte) error {
			body, err := protocol.EncodeDeliver(from, payload)
			if err != nil {
				return err
			}
			ctx := conn.Context()
			if g.cfg.EnqueueTimeout > 0 {
				// P1 背压必须有上限：用连接生命周期 context 会让慢消费者的发送
				// 方读循环无限期阻塞（跨连接队头阻塞），并把心跳一起拖死。
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, g.cfg.EnqueueTimeout)
				defer cancel()
			}
			if err := conn.EnqueueReliable(ctx, protocol.Frame{
				Type: protocol.TypeDeliver, Body: body,
			}); err != nil {
				if errors.Is(err, sendpath.ErrQueueFull) {
					// 背压等待超时 = 慢消费者，按 S1 契约断连（LLD §8.2）。
					conn.Close()
				}
				return err
			}
			return nil
		},
	}, true
}

// ConnManager 返回连接管理器（cmd/gateway 用于 ListenAndServe/Shutdown）。
func (g *Gateway) ConnManager() *connection.Manager { return g.conns }

// Registry 返回会话注册表（测试断言用）。
func (g *Gateway) Registry() *session.Registry { return g.registry }

// ---------- connection.Handler ----------

// OnOpen 登记连接的 SendPath 供 /metrics 聚合。
func (g *Gateway) OnOpen(c *connection.Connection) {
	g.mu.Lock()
	g.sendpaths[c.ID] = c.Send
	g.mu.Unlock()
}

// HandleFrame 按帧类型分发入站帧。入站帧不经过 SendPath（LLD §4.4）。
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
	case protocol.TypePing:
		_ = c.EnqueueControl(protocol.Frame{Type: protocol.TypePong})
	case protocol.TypePong:
		// 活跃时间由 Connection 读循环维护
	default:
		c.ReplyError(protocol.ErrorUnknownFrame)
	}
}

func (g *Gateway) handleLogin(c *connection.Connection, f protocol.Frame) {
	lb, err := protocol.DecodeLogin(f.Body)
	if err != nil {
		c.ReplyError(protocol.ErrorBadFrame)
		return
	}
	producer, err := g.auth.Authenticate(lb.Token)
	if err != nil {
		g.metrics.AuthFailedInc()
		c.ReplyClose() // S1: CLOSE body 为空，不携带原因码
		c.Close()
		return
	}
	if lb.DeviceID == "" {
		c.ReplyError(protocol.ErrorBadFrame)
		return
	}

	ep := session.Endpoint{TenantID: producer.TenantID, UserID: producer.UserID(), DeviceID: lb.DeviceID}
	sid := fmt.Sprintf("s%d", g.sessionSeq.Add(1))
	s := session.NewSession(sid, ep, producer.ID, 0) // S1: capabilities=0

	g.mu.Lock()
	g.sessions[sid] = c
	g.byConn[c.ID] = s
	g.mu.Unlock()

	old, _ := g.registry.Register(&session.Entry{Session: s})
	if old != nil && old.Session != s && old.Session.State() == session.Active {
		g.metrics.ReplacedInc()
		old.Session.MarkReplaced()
		if oldConn, ok := g.sessionConn(old.Session.ID); ok && oldConn == c {
			// 同一连接重复登录：关闭自身会打掉刚建立的新会话，且旧会话映射
			// 永远无人清理。只摘掉旧会话映射即可。
			g.mu.Lock()
			delete(g.sessions, old.Session.ID)
			g.mu.Unlock()
		} else {
			g.closeReplaced(old.Session)
		}
	}
	s.Activate()
	g.metrics.LoginInc()

	g.log.Info("session login",
		telemetry.FieldTenantID, ep.TenantID,
		telemetry.FieldEndpoint, ep.Key(),
		telemetry.FieldSessionID, sid)

	_ = c.EnqueueControl(protocol.Frame{
		Type: protocol.TypeLoginAck,
		Body: protocol.EncodeLoginAck(s.Capabilities),
	})
}

// sessionConn 返回会话当前绑定的连接。
func (g *Gateway) sessionConn(sessID string) (*connection.Connection, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	conn, ok := g.sessions[sessID]
	return conn, ok
}

// closeReplaced 给被替换的旧会话发 CLOSE（body 空），然后关闭其连接。
func (g *Gateway) closeReplaced(old *session.Session) {
	if conn, ok := g.sessionConn(old.ID); ok {
		conn.ReplyClose()
		conn.Close()
		// 不立即清理索引：旧连接的 OnClose 会注销其 byConn/sessions 映射。
	}
}

func (g *Gateway) handleSend(c *connection.Connection, f protocol.Frame) {
	sb, err := protocol.DecodeSend(f.Body)
	if err != nil {
		c.ReplyError(protocol.ErrorBadFrame)
		return
	}
	// 发送方必须已登录（身份由令牌派生，SEND body 无 producer 字段）。
	g.mu.RLock()
	sender := g.byConn[c.ID]
	g.mu.RUnlock()
	if sender == nil {
		c.ReplyError(protocol.ErrorUnknownFrame)
		return
	}
	// 租户由登录令牌决定：SEND body 里的 tenant 只是寻址字段，必须与已认证租户一致。
	// 否则任何合法用户都能指定别的 tenant_id 完成跨租户投递（数据注入）。
	if sb.TenantID != sender.Endpoint.TenantID {
		g.metrics.RouteMissInc()
		c.ReplyError(protocol.ErrorUnknownFrame)
		return
	}
	dsts, err := g.router.Resolve(sb)
	if err != nil {
		g.metrics.RouteMissInc()
		c.ReplyError(protocol.ErrorUnknownFrame) // S1 无「目标离线」错误码，用通用错误
		return
	}
	for _, d := range dsts {
		if err := d.Deliver(sender.ProducerID, sb.Payload); err != nil {
			g.log.Warn("deliver failed", telemetry.FieldSessionID, d.Session.ID, "err", err)
			continue
		}
		g.metrics.DeliveryInc()
	}
}

// OnClose 在连接关闭后注销会话（仅当仍为当前 Session）。
func (g *Gateway) OnClose(c *connection.Connection) {
	g.mu.Lock()
	s := g.byConn[c.ID]
	delete(g.byConn, c.ID)
	delete(g.sendpaths, c.ID)
	if s != nil {
		delete(g.sessions, s.ID)
	}
	g.mu.Unlock()
	if s != nil {
		g.registry.Unregister(s.Endpoint, s)
		s.MarkClosed()
		g.log.Info("session closed", telemetry.FieldSessionID, s.ID, telemetry.FieldConnID, c.ID)
	}
}

// slogToStd 把 *slog.Logger 适配成 *log.Logger（connection.Manager 使用标准 log）。
func slogToStd(l *slog.Logger) *log.Logger {
	return slog.NewLogLogger(l.Handler(), slog.LevelInfo)
}
