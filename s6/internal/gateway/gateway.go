// Package gateway 编排 S6 节点的全部组件：节点注册、路由、转发服务端、
// 客户端接入与观测（LLD §2.1 组件视图）。
//
// S6 不改任何冻结语义：客户端 TCP 通道沿用 S1 的四通道/帧语义，
// 本包只在 LOGIN 成功后把端点绑定到 Redis 路由，并在投递前查询路由。
package gateway

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"relayx/s6/internal/backend"
	"relayx/s6/internal/config"
	"relayx/s6/internal/dispatch"
	"relayx/s6/internal/forward"
	"relayx/s6/internal/model"
	"relayx/s6/internal/node"
	"relayx/s6/internal/route"
	"relayx/s6/internal/telemetry"
	"relayx/s6/protocol"
)

// Options 是 Gateway 构造参数。
type Options struct {
	Cfg   config.Config
	Log   *slog.Logger
	Clock func() time.Time
	// Backend 注入时优先使用；为空时按 Cfg.Backend 构造。
	Backend backend.Backend
	// TLSConfig 由调用方构造（mTLS）。为空时本节点只做客户端接入+路由，
	// 不启动内部帧服务端（单节点模式）。
	TLSConfig *tls.Config
}

// Gateway 编排所有组件。
type Gateway struct {
	cfg      config.Config
	log      *slog.Logger
	metrics  *telemetry.Metrics
	backend  backend.Backend
	router   *route.Router
	registry *node.Registry

	forwarder  *forward.Forwarder
	intServer  *forward.Server
	ownBackend bool

	mu       sync.Mutex
	sessions map[string]*session // 本地活跃会话：endpoint -> session
	// clients 跟踪所有已接受连接（含未登录/空闲连接），Shutdown 时必须全部关闭，
	// 否则 handleClient 阻塞在 Read 上导致 wg.Wait 永不返回。
	clients map[net.Conn]struct{}
	// consecutiveErr 记录每个端点的连续转发失败次数，供 §4.4 D（连续 3 次不可达回落离线）判定。
	consecutiveErr map[string]int
	listener       net.Listener
	wg             sync.WaitGroup
	stopped        bool

	// dialTLS 由调用方注入，用于 mTLS 拨号（测试可替换为内存管道）。
	dialTLS func(context.Context, string) (net.Conn, error)
}

// connWriter 是连接上唯一的写者：所有帧（LOGIN_ACK/PONG/DELIVER）都经它，
// 避免 handleClient 与 forward.Enqueue 并发写同一 socket 造成帧交错（S1 单 Writer 约束）。
type connWriter struct {
	mu       sync.Mutex
	conn     net.Conn
	w        *bufio.Writer
	seq      uint32
	deadline time.Duration
}

func (cw *connWriter) write(frame protocol.Frame) error {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	// S1 冻结语义要求序号单调不 wrap；耗尽后必须失败而不是从 0 重新开始。
	if cw.seq == ^uint32(0) {
		return protocol.ErrSequenceExhausted
	}
	cw.seq++
	frame.Sequence = cw.seq
	raw, err := protocol.Encode(frame, nil)
	if err != nil {
		return err
	}
	if cw.deadline > 0 {
		_ = cw.conn.SetWriteDeadline(time.Now().Add(cw.deadline))
		defer cw.conn.SetWriteDeadline(time.Time{})
	}
	if _, err := cw.w.Write(raw); err != nil {
		return err
	}
	return cw.w.Flush()
}

type session struct {
	ep      model.EndpointKey
	binding model.NodeBinding
	conn    net.Conn
	out     *connWriter
}

// New 创建 Gateway（尚未监听）。
func New(o Options) (*Gateway, error) {
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.Clock == nil {
		o.Clock = time.Now
	}
	m := telemetry.New()
	b := o.Backend
	own := false
	if b == nil {
		switch o.Cfg.Backend {
		case "redis":
			b = backend.NewRedisBackend(o.Cfg.RedisAddr, o.Cfg.RedisPassword, o.Cfg.RedisDB, o.Cfg.RouteTimeout, o.Clock)
		default:
			b = backend.NewMemoryBackend()
		}
		own = true
	}

	gw := &Gateway{
		cfg: o.Cfg, log: o.Log, metrics: m, backend: b, ownBackend: own,
		sessions:       make(map[string]*session),
		clients:        make(map[net.Conn]struct{}),
		consecutiveErr: make(map[string]int),
	}
	gw.router = route.NewRouter(route.Options{
		Backend: b, Clock: o.Clock, NodeID: o.Cfg.NodeID,
		CacheTTL: o.Cfg.LocalCacheTTL, Log: o.Log, Metrics: m,
	})
	self := model.NodeInfo{NodeID: o.Cfg.NodeID, Addr: o.Cfg.InternalAddr, Status: model.NodeAlive}
	gw.registry = node.NewRegistry(node.Options{
		Backend: b, Clock: o.Clock, Self: self,
		TTL: o.Cfg.NodeTTL, SuspectAfter: o.Cfg.SuspectAfter,
		HeartbeatEvery: o.Cfg.HeartbeatInterval, Log: o.Log, Metrics: m,
	})
	gw.forwarder = forward.NewForwarder(gw.registry, o.Log, m, gw, forward.WithWriteTimeout(o.Cfg.ForwardTimeout))
	if o.TLSConfig != nil {
		gw.intServer = forward.NewServer(o.TLSConfig, gw, gw.router, o.Log, m, forward.WithHandshakeTimeout(o.Cfg.DialTimeout))
	}
	gw.dialTLS = func(ctx context.Context, addr string) (net.Conn, error) {
		// tls.Dialer.DialContext 的 ctx 同时约束 TCP 拨号与 TLS 握手；
		// 上层传入的是 Background，必须在这里补上超时，否则握手半开可无限阻塞。
		if o.Cfg.DialTimeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, o.Cfg.DialTimeout)
			defer cancel()
		}
		var d tls.Dialer
		d.Config = o.TLSConfig
		d.NetDialer = &net.Dialer{Timeout: o.Cfg.DialTimeout}
		return d.DialContext(ctx, "tcp", addr)
	}
	return gw, nil
}

// Metrics 返回指标实例。
func (g *Gateway) Metrics() *telemetry.Metrics { return g.metrics }

// Router 返回路由实例（供测试/上层使用）。
func (g *Gateway) Router() *route.Router { return g.router }

// Registry 返回节点注册表（供测试/上层使用）。
func (g *Gateway) Registry() *node.Registry { return g.registry }

// Start 注册本节点并启动客户端监听与内部帧服务端。
//
// 两个 listener 都先同步 Bind 成功再注册节点：内部端口占用等错误必须在
// Start 返回前暴露，不能注册完节点后才发现内部服务起不来。
func (g *Gateway) Start() error {
	ctx := context.Background()
	ln, err := net.Listen("tcp", g.cfg.ClientAddr)
	if err != nil {
		return fmt.Errorf("gateway: listen client: %w", err)
	}
	var intLn net.Listener
	if g.intServer != nil {
		intLn, err = net.Listen("tcp", g.cfg.InternalAddr)
		if err != nil {
			_ = ln.Close()
			return fmt.Errorf("gateway: listen internal: %w", err)
		}
	}
	if err := g.registry.Register(ctx); err != nil {
		_ = ln.Close()
		if intLn != nil {
			_ = intLn.Close()
		}
		return fmt.Errorf("gateway: register node: %w", err)
	}
	g.mu.Lock()
	if g.stopped {
		g.mu.Unlock()
		_ = ln.Close()
		if intLn != nil {
			_ = intLn.Close()
		}
		_ = g.registry.Deregister(ctx) // 撤销刚完成的注册，避免留下无监听节点的路由
		return net.ErrClosed
	}
	g.listener = ln
	g.wg.Add(1)
	if intLn != nil {
		g.wg.Add(1) // 与客户端 loop 一起在锁内登记，避免与 Shutdown 的 Wait 并发
	}
	g.mu.Unlock()
	go g.serveClients(ln)

	if intLn != nil {
		go func() {
			defer g.wg.Done()
			if err := g.intServer.Serve(intLn); err != nil && !errors.Is(err, net.ErrClosed) {
				g.log.Error("internal server exited", "err", err)
			}
		}()
	}
	return nil
}

func (g *Gateway) serveClients(ln net.Listener) {
	defer g.wg.Done()
	for {
		c, err := ln.Accept()
		if err != nil {
			g.mu.Lock()
			stopped := g.stopped
			g.mu.Unlock()
			if stopped {
				return
			}
			g.log.Error("client accept", "err", err)
			return
		}
		// 与 Shutdown 共享同一把锁：置 stopped 之后不再 Add，Wait 开始后不再有
		// 新 handler；同时把连接登记进 clients，Shutdown 能关闭空闲连接。
		g.mu.Lock()
		if g.stopped {
			g.mu.Unlock()
			_ = c.Close()
			return
		}
		g.clients[c] = struct{}{}
		g.wg.Add(1)
		g.mu.Unlock()
		go g.handleClient(c)
	}
}

func (g *Gateway) handleClient(c net.Conn) {
	defer g.wg.Done()
	defer c.Close()
	defer func() {
		g.mu.Lock()
		delete(g.clients, c)
		g.mu.Unlock()
	}()
	r := bufio.NewReader(c)
	out := &connWriter{conn: c, w: bufio.NewWriter(c), deadline: g.cfg.WriteDeadline}
	var current *session
	defer func() {
		if current != nil {
			g.unbind(current)
		}
	}()

	for {
		frame, _, err := readFrame(r)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				g.log.Debug("client read ended", "remote", c.RemoteAddr(), "err", err)
			}
			return
		}
		switch frame.Type {
		case protocol.TypeLogin:
			login, err := protocol.DecodeLogin(frame.Body)
			if err != nil {
				g.writeError(out, 1003)
				continue
			}
			ep := model.EndpointKey{TenantID: "default", UserID: login.Token, DeviceID: login.DeviceID}
			if !ep.Valid() {
				g.writeError(out, 1003)
				continue
			}
			binding, err := g.router.Bind(context.Background(), ep)
			if err != nil {
				g.writeError(out, 1005) // 依赖不可用：新登录被拒（§7.2）
				continue
			}
			next := &session{ep: ep, binding: binding, conn: c, out: out}
			// 先登记新会话再解绑旧会话：unbind 按指针条件删除，不会误删
			// 刚插入的新会话；反过来则存在 sessions 短暂缺项的窗口。
			g.mu.Lock()
			g.sessions[ep.String()] = next
			g.metrics.EndpointBindingsSet(int64(len(g.sessions)))
			g.mu.Unlock()
			if current != nil {
				g.unbind(current)
			}
			current = next
			ack := protocol.EncodeLoginAck(protocol.S4Capabilities)
			_ = out.write(protocol.Frame{Type: protocol.TypeLoginAck, Body: ack})
		case protocol.TypePing:
			_ = out.write(protocol.Frame{Type: protocol.TypePong})
		case protocol.TypeSend:
			// S6 不实现业务受理语义（S2 负责）；这里只把帧按目标端点路由。
			g.handleSend(current, frame)
		default:
			// 其余帧由 S1–S4 语义层处理；S6 本地快照暂不展开。
		}
	}
}

// handleSend 演示路由选择：本地端点直接投递，远端端点尽力转发。
// 真正的 S2 受理/回执/事实写入由上层语义层复用本路由完成。
func (g *Gateway) handleSend(from *session, frame protocol.Frame) {
	if from == nil {
		return
	}
	sb, err := protocol.DecodeSend(frame.Body)
	if err != nil {
		return
	}
	targets := sb.TargetDevices
	if len(targets) == 0 {
		return
	}
	for _, dev := range targets {
		ep := model.EndpointKey{TenantID: from.ep.TenantID, UserID: sb.TargetUserID, DeviceID: dev}
		binding, ok, err := g.router.Lookup(context.Background(), ep)
		if err != nil || !ok {
			continue // 无路由 → S2 离线路径（由上层语义层处理）
		}
		// 把受理的 SEND 转成 S2 冻结的 DELIVER 帧体（生产者/expiry 由上层语义层填；
		// 这里用已知字段构造，帧类型与 body 布局不变）。
		deliverBody := protocol.EncodeDeliver(protocol.DeliverBody{
			MessageID:  sb.ClientMsgID,
			ProducerID: from.ep.UserID,
			Sender:     from.ep.UserID,
			Payload:    sb.Payload,
		})
		inner := protocol.Frame{Type: protocol.TypeDeliver, Body: deliverBody}
		if binding.NodeID == g.cfg.NodeID {
			_ = g.Enqueue(context.Background(), ep.TenantID, ep.UserID, ep.DeviceID, inner.Type, inner.Body)
			continue
		}
		// 远端：尽力转发。失败按 §4.4 映射；本层落实「传输侧」动作
		// （计数、D 类清本地缓存），「状态/attempts/事实」交给 S2 语义层消费 Outcome。
		err = g.forwarder.Forward(context.Background(), binding.NodeID, ep, inner.Type, inner.Body, binding.Version, g.dialTLS)
		if err != nil {
			g.metrics.CrossNodeForwardFailuresInc()
			kind := classifyForwardErr(err)
			g.mu.Lock()
			if kind == dispatch.ErrNodeUnreachable {
				g.consecutiveErr[ep.String()]++
			} else {
				g.consecutiveErr[ep.String()] = 0 // 非不可达失败不累积到 D 类计数
			}
			count := g.consecutiveErr[ep.String()]
			g.mu.Unlock()
			outcome := dispatch.ClassifyFailure(kind, count, g.cfg.NodeUnreachableLimit, int32(g.cfg.RetryBackoff/time.Second))
			if outcome.Kind == dispatch.OutcomeOfflineHold {
				// §4.4 D：清本节点本地缓存（不删 Redis，交由 endpoint 重绑）。
				g.router.DeleteLocalCache(ep)
				g.log.Warn("cross-node route held offline after repeated unreachable",
					"node", binding.NodeID, "endpoint", ep.String(), "count", count)
			} else {
				g.log.Warn("cross-node forward failed", "node", binding.NodeID, "endpoint", ep.String(), "err", err)
			}
		} else {
			g.mu.Lock()
			delete(g.consecutiveErr, ep.String())
			g.mu.Unlock()
		}
	}
}

// classifyForwardErr 把转发错误映射到 §4.4 的 ErrKind（供退避/离线判定）。
func classifyForwardErr(err error) dispatch.ErrKind {
	switch {
	case errors.Is(err, forward.ErrNodeUnavailable):
		return dispatch.ErrNodeUnreachable
	default:
		// mTLS 握手/拨号等其它失败按 B 类处理，不累积到"不可达"计数。
		return dispatch.ErrMTLSFailure
	}
}

func (g *Gateway) writeError(out *connWriter, code uint16) {
	// ERROR body 沿用 S0 冻结布局：2 字节大端错误码。
	body := []byte{byte(code >> 8), byte(code)}
	_ = out.write(protocol.Frame{Type: protocol.TypeError, Body: body})
}

func (g *Gateway) unbind(s *session) {
	_, _ = g.router.Unbind(context.Background(), s.ep, s.binding.Version)
	g.mu.Lock()
	// 只有 map 里仍是本会话时才删除：迟到的旧连接清理不能移除同端点的新会话。
	if g.sessions[s.ep.String()] == s {
		delete(g.sessions, s.ep.String())
		g.metrics.EndpointBindingsSet(int64(len(g.sessions)))
	}
	g.mu.Unlock()
}

// Shutdown 优雅停机：先注销节点（触发绑定回收），再关闭监听与连接。
func (g *Gateway) Shutdown(ctx context.Context) error {
	g.mu.Lock()
	g.stopped = true
	ln := g.listener
	clients := make([]net.Conn, 0, len(g.clients))
	for c := range g.clients {
		clients = append(clients, c)
	}
	g.mu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
	_ = g.registry.Deregister(ctx)
	if g.intServer != nil {
		_ = g.intServer.Close()
	}
	g.forwarder.Close()
	// 关闭全部客户端连接（含未登录/空闲连接），让 handleClient 的 Read 返回。
	for _, c := range clients {
		_ = c.Close()
	}
	g.wg.Wait()
	g.registry.Stop()
	if g.ownBackend {
		if r, ok := g.backend.(interface{ Close() }); ok {
			r.Close()
		}
	}
	return nil
}

// ---- forward.LocalSink 实现：在目标节点本地投递 ----

// Enqueue 把 FORWARD 帧里的原始业务帧写入目标端点在本节点的连接。
// 这是 §4.1 步骤 5「按 S1 路由在本节点投递」的最小实现。
// 所有写都经 connWriter（单 Writer），序号由本连接分配（S1 冻结语义）。
func (g *Gateway) Enqueue(_ context.Context, tenant, user, device string, innerType byte, innerBody []byte) error {
	ep := model.EndpointKey{TenantID: tenant, UserID: user, DeviceID: device}
	g.mu.Lock()
	s := g.sessions[ep.String()]
	g.mu.Unlock()
	if s == nil {
		return forward.ErrNodeUnavailable // 映射为 NACK NO_SESSION
	}
	return s.out.write(protocol.Frame{Type: innerType, Body: innerBody})
}

// ---- forward.NackHandler 实现 ----

// OnNack 处理异步回执（LLD §4.4 C）：刷新本地缓存，由重试调度器下次重新查路由。
// 这里刻意不改变任何投递状态/事实——那是 S2 语义层的职责。
func (g *Gateway) OnNack(_ context.Context, from string, nack protocol.NackBody) {
	outcome := dispatch.ClassifyNack(nack.Reason, int32(g.cfg.RetryBackoff/time.Second))
	ep := model.EndpointKey{TenantID: nack.TenantID, UserID: nack.UserID, DeviceID: nack.DeviceID}
	if ep.Valid() {
		// 对端已证明本节点手里的路由过期：清本地缓存，下一次重试重新走 Redis。
		g.router.DeleteLocalCache(ep)
	}
	g.log.Warn("forward nack", "from", from, "reason", protocol.NackReasonName(nack.Reason), "outcome", outcome.Kind)
}

func readFrame(r *bufio.Reader) (protocol.Frame, int, error) {
	header := make([]byte, protocol.HeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return protocol.Frame{}, 0, err
	}
	// 先按 uint32 判定上限再转 int：32 位平台上直接转成 int 会让高位置 1 的
	// Length 变成负数，绕过 MaxBodySize 守卫并使 make 以负长度 panic。
	length := uint32(header[4])<<24 | uint32(header[5])<<16 | uint32(header[6])<<8 | uint32(header[7])
	if length > protocol.MaxBodySize {
		return protocol.Frame{}, 0, protocol.ErrFrameTooLarge
	}
	buf := make([]byte, protocol.HeaderSize+int(length))
	copy(buf, header)
	if _, err := io.ReadFull(r, buf[protocol.HeaderSize:]); err != nil {
		return protocol.Frame{}, 0, err
	}
	return protocol.Decode(buf)
}
